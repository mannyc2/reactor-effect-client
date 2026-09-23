//! Test-only libwebrtc far peer standing in for Reactor's media server. It is a
//! Cargo example so it never enters the staged library or its build identity.
//!
//! Line-delimited JSON on stdin/stdout, one factory for the whole process:
//!
//!   <- {"op":"ready"}
//!   -> {"op":"offer","id":"a","sdp":"..."}   <- {"op":"answer","id":"a","sdp":"..."}
//!   -> {"op":"candidate","id":"a","candidate":"...","sdpMid":"0","sdpMLineIndex":0}
//!   -> {"op":"stats","id":"a"}               <- {"op":"stats","id":"a",...}
//!   -> {"op":"close","id":"a"}               <- {"op":"closed","id":"a"}
//!
//! The answer carries the far peer's gathered candidates, as Reactor's does.
//! Once connected, each session sends BGRA video at --width x --height and
//! --fps, every frame carrying a 16-byte metadata user_data of [wall-clock
//! microseconds u64 LE][sequence u64 LE], plus 10 ms blocks of 48 kHz mono PCM.
//! Both data channels echo every binary message. Closing stdin ends the process.

use reactor_webrtc::{
    AudioFrame, AudioTrack, AudioTrackOptions, AudioTrackSource, DataChannel, IceCandidate,
    IceGatheringState, MediaKind, PeerConnection, PeerConnectionFactory, PeerConnectionObserver,
    PeerConnectionState, RtcConfiguration, SdpType, SessionDescription, StreamKind,
    TransceiverDirection, VideoFrame, VideoTrack,
};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

fn out(value: Value) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}

fn arg(name: &str, default: u32) -> u32 {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn wall_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// A moving gradient with a noisy band, so the encoder spends real bits.
fn pattern(width: u32, height: u32, frames: usize) -> Vec<Vec<u8>> {
    let mut seed: u32 = 0x9e37_79b9;
    (0..frames as u32)
        .map(|index| {
            let mut bgra = vec![255u8; (width * height * 4) as usize];
            for y in 0..height {
                for x in 0..width {
                    let o = ((y * width + x) * 4) as usize;
                    let mut pixel = [
                        (x + index * 8) as u8,
                        (y + index * 4) as u8,
                        ((x ^ y) + index * 16) as u8,
                    ];
                    if y > height / 3 && y < height / 3 * 2 {
                        seed ^= seed << 13;
                        seed ^= seed >> 17;
                        seed ^= seed << 5;
                        for (channel, value) in pixel.iter_mut().enumerate() {
                            *value = value.wrapping_add((seed >> (channel * 8)) as u8 & 0x3f);
                        }
                    }
                    bgra[o..o + 3].copy_from_slice(&pixel);
                }
            }
            bgra
        })
        .collect()
}

#[derive(Default)]
struct Signals {
    ice: Mutex<VecDeque<IceCandidate>>,
    gathered: AtomicBool,
    connected: AtomicBool,
    channels: Mutex<Vec<DataChannel>>,
}

struct Session {
    peer: PeerConnection,
    signals: Arc<Signals>,
    stop: Arc<AtomicBool>,
    pushed: Arc<AtomicU64>,
    pump: Option<JoinHandle<()>>,
    echoer: Option<JoinHandle<()>>,
}

impl Session {
    fn open(
        factory: &PeerConnectionFactory,
        offer: String,
        frames: Arc<Vec<Vec<u8>>>,
        (width, height, fps): (u32, u32, u32),
    ) -> (Self, String) {
        let signals = Arc::new(Signals::default());
        // Echo off libwebrtc's callback thread, which must not send re-entrantly.
        let (echo, echoes) = mpsc::channel::<(String, Vec<u8>)>();
        let observer = PeerConnectionObserver::new()
            .on_ice_candidate({
                let signals = Arc::clone(&signals);
                move |candidate| lock(&signals.ice).push_back(candidate)
            })
            .on_ice_gathering_change({
                let signals = Arc::clone(&signals);
                move |state| {
                    if state == IceGatheringState::Complete {
                        signals.gathered.store(true, Ordering::Release);
                    }
                }
            })
            .on_connection_state_change({
                let signals = Arc::clone(&signals);
                move |state| {
                    signals
                        .connected
                        .store(state == PeerConnectionState::Connected, Ordering::Release);
                }
            })
            .on_data_channel({
                let signals = Arc::clone(&signals);
                let echo = echo.clone();
                move |mut channel| {
                    let label = channel.label();
                    let echo = echo.clone();
                    channel.on_message(move |bytes, _binary| {
                        let _ = echo.send((label.clone(), bytes.to_vec()));
                    });
                    lock(&signals.channels).push(channel);
                }
            });
        let peer = factory
            .create_peer_connection(&RtcConfiguration::default(), observer)
            .expect("far peer connection");
        peer.set_remote_description(&SessionDescription {
            kind: SdpType::Offer,
            sdp: offer,
        })
        .expect("far peer accepts the bridge offer");
        let video = factory
            .create_video_track("far-video")
            .expect("far video track");
        let audio = factory
            .create_audio_track_with_options("far-audio", {
                let mut options = AudioTrackOptions::default();
                options.source = AudioTrackSource::LocalPush;
                options
            })
            .expect("far audio track");
        for transceiver in peer.transceivers() {
            match transceiver.kind() {
                MediaKind::Video => transceiver.set_track(&video).expect("send video"),
                MediaKind::Audio => transceiver.set_track(&audio).expect("send audio"),
                MediaKind::Unknown => continue,
            }
            transceiver
                .set_direction(TransceiverDirection::SendOnly)
                .expect("send direction");
            if transceiver.kind() == MediaKind::Video {
                transceiver
                    .set_send_bitrate(Some(300_000), Some(8_000_000))
                    .expect("video bitrate");
            }
        }
        let answer = peer.create_answer().expect("far answer");
        peer.set_local_description(&answer)
            .expect("far local answer");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !signals.gathered.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let candidates: Vec<IceCandidate> = lock(&signals.ice).drain(..).collect();
        let sdp = with_candidates(&answer.sdp, &candidates);

        let stop = Arc::new(AtomicBool::new(false));
        let pushed = Arc::new(AtomicU64::new(0));
        let echoer = {
            let signals = Arc::clone(&signals);
            thread::spawn(move || {
                while let Ok((label, bytes)) = echoes.recv() {
                    let channels = lock(&signals.channels);
                    if let Some(channel) = channels.iter().find(|channel| channel.label() == label)
                    {
                        let _ = channel.send(&bytes, true);
                    }
                }
            })
        };
        let pump = {
            let (stop, pushed, connected) =
                (Arc::clone(&stop), Arc::clone(&pushed), Arc::clone(&signals));
            thread::spawn(move || {
                while !connected.connected.load(Ordering::Acquire) {
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                pump(
                    &video,
                    &audio,
                    &frames,
                    (width, height, fps),
                    &stop,
                    &pushed,
                );
            })
        };
        (
            Self {
                peer,
                signals,
                stop,
                pushed,
                pump: Some(pump),
                echoer: Some(echoer),
            },
            sdp,
        )
    }

    fn stats(&self) -> Value {
        let mut video = json!({});
        if let Ok(report) = self.peer.get_stats() {
            if let Some(outbound) = report
                .outbound_rtp
                .iter()
                .find(|entry| entry.kind == StreamKind::Video)
            {
                video = json!({
                    "framesSent": outbound.frames_sent,
                    "frameWidth": outbound.frame_width,
                    "frameHeight": outbound.frame_height,
                    "targetBitrate": outbound.target_bitrate_bps,
                });
            }
        }
        json!({
            "connected": self.signals.connected.load(Ordering::Acquire),
            "framesPushed": self.pushed.load(Ordering::Relaxed),
            "video": video,
        })
    }

    fn close(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(pump) = self.pump.take() {
            let _ = pump.join();
        }
        // Dropping the connection drops its observer and with it the echo
        // sender; the channels hold the others.
        lock(&self.signals.channels).clear();
        drop(self.peer);
        if let Some(echoer) = self.echoer.take() {
            let _ = echoer.join();
        }
    }
}

fn pump(
    video: &VideoTrack,
    audio: &AudioTrack,
    frames: &[Vec<u8>],
    (width, height, fps): (u32, u32, u32),
    stop: &AtomicBool,
    pushed: &AtomicU64,
) {
    let started = Instant::now();
    let frame_interval = Duration::from_micros(1_000_000 / u64::from(fps));
    let (mut next_video, mut next_audio) = (started, started);
    let (mut sequence, mut block) = (0u64, 0u64);
    while !stop.load(Ordering::Acquire) {
        let now = Instant::now();
        if now >= next_video {
            let mut user_data = [0u8; 16];
            user_data[..8].copy_from_slice(&wall_micros().to_le_bytes());
            user_data[8..].copy_from_slice(&sequence.to_le_bytes());
            let bgra = &frames[sequence as usize % frames.len()];
            if video
                .push_frame_with_metadata(VideoFrame::new(bgra, width, height), &user_data)
                .is_ok()
            {
                pushed.fetch_add(1, Ordering::Relaxed);
            }
            sequence += 1;
            next_video += frame_interval;
        }
        if now >= next_audio {
            let pcm: Vec<i16> = (0..480u64)
                .map(|i| {
                    let t = (block * 480 + i) as f64 / 48_000.0;
                    ((t * 440.0 * std::f64::consts::TAU).sin() * 8_000.0) as i16
                })
                .collect();
            let _ = audio.push_frame(AudioFrame {
                pcm: &pcm,
                sample_rate: 48_000,
                channels: 1,
                frames: 480,
            });
            block += 1;
            next_audio += Duration::from_millis(10);
        }
        let wake = next_video.min(next_audio);
        let now = Instant::now();
        if wake > now {
            thread::sleep((wake - now).min(Duration::from_millis(5)));
        }
    }
}

/// Insert gathered candidates into their m-sections: the bridge's C ABI has no
/// remote-candidate call, exactly like an answer from Reactor.
fn with_candidates(sdp: &str, candidates: &[IceCandidate]) -> String {
    let mut sections: Vec<Vec<String>> = vec![Vec::new()];
    for line in sdp.split("\r\n").filter(|line| !line.is_empty()) {
        if line.starts_with("m=") {
            sections.push(Vec::new());
        }
        sections.last_mut().expect("section").push(line.to_owned());
    }
    for candidate in candidates {
        let Some(index) = candidate.sdp_mline_index else {
            continue;
        };
        if let Some(section) = sections.get_mut(usize::from(index) + 1) {
            section.push(format!(
                "a={}",
                candidate.candidate.trim_start_matches("a=")
            ));
        }
    }
    for section in sections.iter_mut().skip(1) {
        section.push("a=end-of-candidates".into());
    }
    let mut sdp = sections.concat().join("\r\n");
    sdp.push_str("\r\n");
    sdp
}

fn main() {
    let shape = (arg("--width", 1344), arg("--height", 768), arg("--fps", 24));
    let frames = Arc::new(pattern(shape.0, shape.1, 12));
    let factory = PeerConnectionFactory::builder()
        .with_synthetic_adm()
        .build()
        .expect("far peer factory");
    let mut sessions: HashMap<String, Session> = HashMap::new();
    out(json!({ "op": "ready" }));
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = message["id"].as_str().unwrap_or_default().to_owned();
        match message["op"].as_str().unwrap_or_default() {
            "offer" => {
                let offer = message["sdp"].as_str().unwrap_or_default().to_owned();
                let (session, sdp) = Session::open(&factory, offer, Arc::clone(&frames), shape);
                sessions.insert(id.clone(), session);
                out(json!({ "op": "answer", "id": id, "sdp": sdp }));
            }
            "candidate" => {
                if let Some(session) = sessions.get(&id) {
                    let candidate = IceCandidate {
                        candidate: message["candidate"].as_str().unwrap_or_default().to_owned(),
                        sdp_mid: message["sdpMid"].as_str().map(str::to_owned),
                        sdp_mline_index: message["sdpMLineIndex"].as_u64().map(|v| v as u16),
                    };
                    if !candidate.candidate.is_empty() {
                        let _ = session.peer.add_ice_candidate(&candidate);
                    }
                }
            }
            "stats" => {
                let stats = sessions.get(&id).map_or(Value::Null, Session::stats);
                out(json!({ "op": "stats", "id": id, "stats": stats }));
            }
            "close" => {
                if let Some(session) = sessions.remove(&id) {
                    session.close();
                }
                out(json!({ "op": "closed", "id": id }));
            }
            _ => {}
        }
    }
    for (_, session) in sessions.drain() {
        session.close();
    }
}
