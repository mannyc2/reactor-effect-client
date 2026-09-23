//! The media a far peer sends: a precomputed BGRA pattern and a 440 Hz tone,
//! each pushed on its own schedule.

use reactor_webrtc::{AudioFrame, AudioTrack, VideoFrame, VideoTrack};
use std::env;
use std::f64::consts::TAU;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SAMPLE_RATE: u32 = 48_000;
/// 10 ms of mono samples.
const BLOCK_SAMPLES: u32 = SAMPLE_RATE / 100;
const BLOCK_INTERVAL: Duration = Duration::from_millis(10);
const TONE_HZ: f64 = 440.0;
const TONE_AMPLITUDE: f64 = 8_000.0;

/// The video's frame size and rate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Shape {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) fps: u32,
}

impl Shape {
    /// `--width`, `--height` and `--fps`, defaulting to 1344x768 at 24 fps. A
    /// missing, unparsable or zero value keeps its default.
    pub(crate) fn from_args() -> Self {
        let args: Vec<String> = env::args().collect();
        let value = |name: &str, default: u32| {
            args.iter()
                .position(|arg| arg == name)
                .and_then(|index| args.get(index + 1))
                .and_then(|value| value.parse().ok())
                .filter(|value| *value > 0)
                .unwrap_or(default)
        };
        Self {
            width: value("--width", 1344),
            height: value("--height", 768),
            fps: value("--fps", 24),
        }
    }

    fn frame_interval(self) -> Duration {
        Duration::from_secs(1) / self.fps
    }
}

/// `frames` frames of a moving gradient with a noisy middle band, so the
/// encoder spends real bits.
pub(crate) fn pattern(shape: Shape, frames: u32) -> Vec<Vec<u8>> {
    let Shape { width, height, .. } = shape;
    let mut noise = XorShift(0x9e37_79b9);
    (0..frames)
        .map(|index| {
            let mut bgra = vec![255; width as usize * height as usize * 4];
            let positions = (0..height).flat_map(|y| (0..width).map(move |x| (x, y)));
            for ((x, y), [blue, green, red, _alpha]) in positions.zip(bgra.as_chunks_mut().0) {
                // The gradient wraps: only each sum's low byte is used.
                let mut pixel = [x + index * 8, y + index * 4, (x ^ y) + index * 16].map(low_byte);
                if y > height / 3 && y < height / 3 * 2 {
                    let noise = noise.next();
                    for (shift, channel) in (0..).step_by(8).zip(&mut pixel) {
                        *channel = channel.wrapping_add(low_byte(noise >> shift) & 0x3f);
                    }
                }
                [*blue, *green, *red] = pixel;
            }
            bgra
        })
        .collect()
}

fn low_byte(value: u32) -> u8 {
    value.to_le_bytes()[0]
}

struct XorShift(u32);

impl XorShift {
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
}

/// What the pump achieved against its schedule, reported with the stats.
#[derive(Debug, Default)]
pub(crate) struct Pacing {
    pub(crate) pushed: AtomicU64,
    /// Pushes that ran more than one frame interval behind schedule.
    pub(crate) late: AtomicU64,
    /// The furthest behind schedule any push ran, in microseconds.
    pub(crate) lag_max_us: AtomicU64,
}

/// Push video at `shape.fps` and a 10 ms audio block every 10 ms until
/// `stop`. Each frame's metadata is `[wall-clock microseconds u64 LE]
/// [sequence u64 LE]`.
pub(crate) fn pump(
    video: &VideoTrack,
    audio: &AudioTrack,
    frames: &[Vec<u8>],
    shape: Shape,
    stop: &AtomicBool,
    pacing: &Pacing,
) {
    let frame_interval = shape.frame_interval();
    let started = Instant::now();
    let (mut next_video, mut next_audio) = (started, started);
    let mut tone = Tone::default();
    let mut pattern = frames.iter().cycle();
    let mut sequence = 0u64;
    while !stop.load(Ordering::Acquire) {
        let now = Instant::now();
        if now >= next_video {
            let lag = now - next_video;
            if lag > frame_interval {
                pacing.late.fetch_add(1, Ordering::Relaxed);
            }
            pacing.lag_max_us.fetch_max(micros(lag), Ordering::Relaxed);
            let mut metadata = [0; 16];
            metadata[..8].copy_from_slice(&micros(wall_clock()).to_le_bytes());
            metadata[8..].copy_from_slice(&sequence.to_le_bytes());
            if let Some(bgra) = pattern.next() {
                let frame = VideoFrame::new(bgra, shape.width, shape.height);
                if video.push_frame_with_metadata(frame, &metadata).is_ok() {
                    pacing.pushed.fetch_add(1, Ordering::Relaxed);
                }
            }
            sequence += 1;
            next_video += frame_interval;
        }
        if now >= next_audio {
            let pcm = tone.block();
            let block = AudioFrame {
                pcm: &pcm,
                sample_rate: SAMPLE_RATE,
                channels: 1,
                frames: BLOCK_SAMPLES,
            };
            #[expect(
                clippy::let_underscore_must_use,
                reason = "audio keeps flowing even if one block is refused"
            )]
            let _ = audio.push_frame(block);
            next_audio += BLOCK_INTERVAL;
        }
        let wake = next_video.min(next_audio);
        let now = Instant::now();
        if wake > now {
            thread::sleep((wake - now).min(Duration::from_millis(5)));
        }
    }
}

/// A 440 Hz sine, one 10 ms block at a time.
#[derive(Debug, Default)]
struct Tone {
    /// The phase of the next sample, in radians.
    phase: f64,
}

impl Tone {
    fn block(&mut self) -> Vec<i16> {
        let step = TAU * TONE_HZ / f64::from(SAMPLE_RATE);
        (0..BLOCK_SAMPLES)
            .map(|_| {
                let sample = pcm_sample(self.phase.sin() * TONE_AMPLITUDE);
                self.phase = (self.phase + step) % TAU;
                sample
            })
            .collect()
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "the tone's amplitude keeps every sample within i16"
)]
fn pcm_sample(value: f64) -> i16 {
    value as i16
}

fn wall_clock() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHAPE: Shape = Shape {
        width: 8,
        height: 6,
        fps: 24,
    };

    #[test]
    fn the_pattern_fills_every_frame_and_moves_between_frames() {
        let frames = pattern(SHAPE, 3);
        assert_eq!(frames.len(), 3);
        for frame in &frames {
            assert_eq!(frame.len(), 8 * 6 * 4);
            assert!(frame.chunks_exact(4).all(|pixel| pixel[3] == 255), "opaque");
        }
        assert_ne!(frames[0], frames[1], "the gradient must move");
    }

    #[test]
    fn a_tone_block_is_10_ms_within_its_amplitude() {
        let mut tone = Tone::default();
        let first = tone.block();
        assert_eq!(first.len(), 480);
        assert!(first.iter().all(|sample| i32::from(*sample).abs() <= 8_000));
        assert_ne!(first, tone.block(), "440 Hz does not repeat every 10 ms");
    }

    #[test]
    fn the_frame_interval_follows_the_rate() {
        assert_eq!(
            Shape { fps: 25, ..SHAPE }.frame_interval(),
            Duration::from_millis(40)
        );
    }
}
