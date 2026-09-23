//! Shared loopback helpers for the native-host spikes. Not a product surface.
//!
//! Everything here drives reactor-webrtc at the same pinned revision as
//! packages/native, so a crash or a pass is a statement about that revision.

use reactor_webrtc::{
    AudioFrame, AudioTrack, AudioTrackOptions, AudioTrackSource, DataChannel, DataChannelState,
    IceCandidate, MediaKind, PeerConnection, PeerConnectionFactory, PeerConnectionObserver,
    PeerConnectionState, RemoteTrack, RtcConfiguration, SdpType, SessionDescription,
    TransceiverDirection, VideoFrame, VideoTrack,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn factory() -> PeerConnectionFactory {
    PeerConnectionFactory::builder()
        .with_synthetic_adm()
        .build()
        .expect("PeerConnectionFactory")
}

pub fn wall_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Default)]
pub struct Counters {
    pub video: AtomicU64,
    pub audio: AtomicU64,
    pub metadata: AtomicU64,
    pub connected: AtomicBool,
}

/// The receive side, shaped like packages/native: two binary channels and
/// one recvonly video plus one recvonly audio transceiver.
pub struct Offerer {
    pub pc: PeerConnection,
    pub channels: Vec<DataChannel>,
    pub remote: Arc<Mutex<Vec<RemoteTrack>>>,
    pub ice: Arc<Mutex<VecDeque<IceCandidate>>>,
    pub counters: Arc<Counters>,
}

impl Offerer {
    pub fn new(factory: &PeerConnectionFactory) -> (Self, SessionDescription) {
        let ice = Arc::new(Mutex::new(VecDeque::new()));
        let counters = Arc::new(Counters::default());
        let remote = Arc::new(Mutex::new(Vec::new()));
        let observer = PeerConnectionObserver::new()
            .on_ice_candidate({
                let ice = Arc::clone(&ice);
                move |c| lock(&ice).push_back(c)
            })
            .on_connection_state_change({
                let counters = Arc::clone(&counters);
                move |s| {
                    if s == PeerConnectionState::Connected {
                        counters.connected.store(true, Ordering::Release)
                    }
                }
            })
            .on_track({
                let counters = Arc::clone(&counters);
                let remote = Arc::clone(&remote);
                move |track| {
                    match &track {
                        RemoteTrack::Video(v) => {
                            let counters = Arc::clone(&counters);
                            v.on_frame(move |f| {
                                counters.video.fetch_add(1, Ordering::Relaxed);
                                if f.metadata.is_some() {
                                    counters.metadata.fetch_add(1, Ordering::Relaxed);
                                }
                            });
                        }
                        RemoteTrack::Audio(a) => {
                            let counters = Arc::clone(&counters);
                            a.on_frame(move |_| {
                                counters.audio.fetch_add(1, Ordering::Relaxed);
                            });
                        }
                    }
                    lock(&remote).push(track);
                }
            });
        let pc = factory
            .create_peer_connection(&RtcConfiguration::default(), observer)
            .expect("offerer PC");
        let channels = vec![
            pc.create_data_channel("control").expect("control"),
            pc.create_data_channel("data").expect("data"),
        ];
        pc.add_transceiver(MediaKind::Video, TransceiverDirection::RecvOnly)
            .expect("video transceiver");
        pc.add_transceiver(MediaKind::Audio, TransceiverDirection::RecvOnly)
            .expect("audio transceiver");
        let offer = pc.create_offer().expect("offer");
        pc.set_local_description(&offer).expect("local offer");
        (
            Self {
                pc,
                channels,
                remote,
                ice,
                counters,
            },
            offer,
        )
    }
}

/// The sending side: the role Reactor's libwebrtc server plays.
pub struct Answerer {
    pub pc: PeerConnection,
    pub channels: Arc<Mutex<Vec<DataChannel>>>,
    pub video: Arc<VideoTrack>,
    pub audio: Arc<AudioTrack>,
    pub ice: Arc<Mutex<VecDeque<IceCandidate>>>,
    pub gathered: Arc<AtomicBool>,
    pub connected: Arc<AtomicBool>,
}

impl Answerer {
    pub fn new(
        factory: &PeerConnectionFactory,
        config: &RtcConfiguration,
        offer: &SessionDescription,
        label: &str,
    ) -> (Self, SessionDescription) {
        let ice = Arc::new(Mutex::new(VecDeque::new()));
        let channels = Arc::new(Mutex::new(Vec::new()));
        let gathered = Arc::new(AtomicBool::new(false));
        let connected = Arc::new(AtomicBool::new(false));
        let observer = PeerConnectionObserver::new()
            .on_ice_candidate({
                let ice = Arc::clone(&ice);
                move |c| lock(&ice).push_back(c)
            })
            .on_ice_gathering_change({
                let gathered = Arc::clone(&gathered);
                move |s| {
                    if s == reactor_webrtc::IceGatheringState::Complete {
                        gathered.store(true, Ordering::Release)
                    }
                }
            })
            .on_connection_state_change({
                let connected = Arc::clone(&connected);
                move |s| {
                    connected.store(s == PeerConnectionState::Connected, Ordering::Release)
                }
            })
            .on_data_channel({
                let channels = Arc::clone(&channels);
                move |channel| lock(&channels).push(channel)
            });
        let pc = factory
            .create_peer_connection(config, observer)
            .expect("answerer PC");
        pc.set_remote_description(offer).expect("remote offer");
        let video = factory
            .create_video_track(&format!("{label}-video"))
            .expect("video track");
        let audio = factory
            .create_audio_track_with_options(&format!("{label}-audio"), {
                let mut o = AudioTrackOptions::default();
                o.source = AudioTrackSource::LocalPush;
                o
            })
            .expect("audio track");
        for t in pc.transceivers() {
            match t.kind() {
                MediaKind::Video => t.set_track(&video).expect("set video"),
                MediaKind::Audio => t.set_track(&audio).expect("set audio"),
                MediaKind::Unknown => continue,
            }
            t.set_direction(TransceiverDirection::SendOnly)
                .expect("send direction");
        }
        let answer = pc.create_answer().expect("answer");
        pc.set_local_description(&answer).expect("local answer");
        (
            Self {
                pc,
                channels,
                video: Arc::new(video),
                audio: Arc::new(audio),
                ice,
                gathered,
                connected,
            },
            answer,
        )
    }
}

/// Forward trickled candidates in both directions until both sides report
/// connected and the offerer's two channels are open.
pub fn connect(off: &Offerer, ans: &Answerer, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        while let Some(c) = lock(&off.ice).pop_front() {
            let _ = ans.pc.add_ice_candidate(&c);
        }
        while let Some(c) = lock(&ans.ice).pop_front() {
            let _ = off.pc.add_ice_candidate(&c);
        }
        let open = off
            .channels
            .iter()
            .all(|c| c.state() == DataChannelState::Open);
        if open
            && off.counters.connected.load(Ordering::Acquire)
            && ans.connected.load(Ordering::Acquire)
        {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

pub fn answer_description(sdp: String) -> SessionDescription {
    SessionDescription {
        kind: SdpType::Answer,
        sdp,
    }
}

/// Synthetic media: a moving gradient plus a noisy band so libvpx has to
/// spend real bits, and 10 ms of 48 kHz mono PCM per audio push.
pub struct Pattern {
    frames: Vec<Vec<u8>>,
    pub width: u32,
    pub height: u32,
}

impl Pattern {
    pub fn new(width: u32, height: u32, distinct: usize, noisy: bool) -> Self {
        let mut seed: u32 = 0x9e37_79b9;
        let mut frames = Vec::with_capacity(distinct);
        for index in 0..distinct {
            let mut bgra = vec![0u8; (width * height * 4) as usize];
            for y in 0..height {
                for x in 0..width {
                    let o = ((y * width + x) * 4) as usize;
                    let mut b = ((x + index as u32 * 8) & 0xff) as u8;
                    let mut g = ((y + index as u32 * 4) & 0xff) as u8;
                    let mut r = (((x ^ y) + index as u32 * 16) & 0xff) as u8;
                    if noisy && y > height / 3 && y < height / 3 * 2 {
                        seed ^= seed << 13;
                        seed ^= seed >> 17;
                        seed ^= seed << 5;
                        b = b.wrapping_add((seed & 0x3f) as u8);
                        g = g.wrapping_add(((seed >> 8) & 0x3f) as u8);
                        r = r.wrapping_add(((seed >> 16) & 0x3f) as u8);
                    }
                    bgra[o] = b;
                    bgra[o + 1] = g;
                    bgra[o + 2] = r;
                    bgra[o + 3] = 255;
                }
            }
            frames.push(bgra);
        }
        Self {
            frames,
            width,
            height,
        }
    }

    pub fn frame(&self, sequence: u64) -> VideoFrame<'_> {
        let bgra = &self.frames[(sequence as usize) % self.frames.len()];
        VideoFrame::new(bgra, self.width, self.height)
    }
}

pub fn tone(sequence: u64) -> Vec<i16> {
    (0..480u64)
        .map(|i| {
            let t = (sequence * 480 + i) as f64 / 48_000.0;
            ((t * 440.0 * std::f64::consts::TAU).sin() * 8_000.0) as i16
        })
        .collect()
}

#[derive(Default)]
pub struct PumpCounters {
    pub video: AtomicU64,
    pub audio: AtomicU64,
    pub video_errors: AtomicU64,
}

/// Push video at `fps` with a 16-byte user_data [wall_us u64 LE][seq u64 LE]
/// and 10 ms audio frames until `stop` is set.
pub fn pump(
    video: Arc<VideoTrack>,
    audio: Arc<AudioTrack>,
    pattern: Arc<Pattern>,
    fps: u32,
    stop: Arc<AtomicBool>,
    counters: Arc<PumpCounters>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let start = Instant::now();
        let frame_interval = Duration::from_micros(1_000_000 / fps as u64);
        let audio_interval = Duration::from_millis(10);
        let mut next_video = start;
        let mut next_audio = start;
        let mut video_seq = 0u64;
        let mut audio_seq = 0u64;
        while !stop.load(Ordering::Acquire) {
            let now = Instant::now();
            if now >= next_video {
                let mut user = [0u8; 16];
                user[..8].copy_from_slice(&wall_micros().to_le_bytes());
                user[8..].copy_from_slice(&video_seq.to_le_bytes());
                if video
                    .push_frame_with_metadata(pattern.frame(video_seq), &user)
                    .is_err()
                {
                    counters.video_errors.fetch_add(1, Ordering::Relaxed);
                }
                counters.video.fetch_add(1, Ordering::Relaxed);
                video_seq += 1;
                next_video += frame_interval;
            }
            if now >= next_audio {
                let pcm = tone(audio_seq);
                let _ = audio.push_frame(AudioFrame {
                    pcm: &pcm,
                    sample_rate: 48_000,
                    channels: 1,
                    frames: 480,
                });
                counters.audio.fetch_add(1, Ordering::Relaxed);
                audio_seq += 1;
                next_audio += audio_interval;
            }
            let wake = next_video.min(next_audio);
            let now = Instant::now();
            if wake > now {
                thread::sleep((wake - now).min(Duration::from_millis(5)));
            }
        }
    })
}
