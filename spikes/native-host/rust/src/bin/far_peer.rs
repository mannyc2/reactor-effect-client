//! A libwebrtc far peer standing in for Reactor's server (reactor-webrtc at
//! the pinned revision, one factory for the whole process as upstream
//! recommends). Line-delimited JSON on stdin/stdout:
//!
//!   -> {"op":"offer","id":"s1","sdp":"..."}
//!   <- {"op":"answer","id":"s1","sdp":"..."}          (after ICE gathering completes)
//!   -> {"op":"candidate","id":"s1","candidate":"...","sdpMid":"0","sdpMLineIndex":0}
//!   -> {"op":"stats"}            <- {"op":"stats","sessions":[...]}
//!   -> {"op":"close","id":"s1"}  <- {"op":"closed","id":"s1"}
//!   -> {"op":"quit"}
//!
//! Media: BGRA video at --width x --height, --fps, each frame carrying a
//! 16-byte user_data [wall-clock micros u64 LE][sequence u64 LE] in the
//! reactor frame-metadata trailer; 10 ms 48 kHz mono PCM. Both data channels
//! echo every binary message on the channel it arrived on.
//!
//! --loss P --delay-ms D inserts a UDP relay in front of every host candidate
//! that drops each packet with probability P and delays it by D ms, in both
//! directions. The offer's own candidates are then ignored, so the only path
//! is through the relay.

use native_host_spikes::{factory, pump, wall_micros, Answerer, Pattern, PumpCounters};
use reactor_webrtc::{
    DataChannel, IceCandidate, PeerConnectionFactory, RtcConfiguration, SdpType, SessionDescription,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn out(value: Value) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}

fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Lossy {
    loss: f64,
    delay: Duration,
    seed: Mutex<u64>,
    dropped: AtomicU64,
    forwarded: AtomicU64,
}

impl Lossy {
    fn drop_packet(&self) -> bool {
        if self.loss <= 0.0 {
            return false;
        }
        let mut s = self.seed.lock().unwrap();
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        let r = (*s >> 11) as f64 / (1u64 << 53) as f64;
        r < self.loss
    }
}

/// Constant-delay forwarder: FIFO order is preserved because every packet
/// waits the same amount of time.
fn delayed_sender(socket: Arc<UdpSocket>, lossy: Arc<Lossy>) -> mpsc::Sender<(Instant, Vec<u8>, SocketAddr)> {
    let (tx, rx) = mpsc::channel::<(Instant, Vec<u8>, SocketAddr)>();
    thread::spawn(move || {
        while let Ok((due, bytes, to)) = rx.recv() {
            let now = Instant::now();
            if due > now {
                thread::sleep(due - now);
            }
            let _ = socket.send_to(&bytes, to);
            lossy.forwarded.fetch_add(1, Ordering::Relaxed);
        }
    });
    tx
}

/// Relay in front of one far-peer host candidate `target`. Returns the relay's
/// public address. Client -> front -> back -> target, and back again.
fn relay(target: SocketAddr, lossy: Arc<Lossy>) -> SocketAddr {
    let front = Arc::new(UdpSocket::bind(SocketAddr::new(target.ip(), 0)).expect("relay front"));
    let back = Arc::new(UdpSocket::bind(SocketAddr::new(target.ip(), 0)).expect("relay back"));
    let public = front.local_addr().unwrap();
    let client: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
    let to_target = delayed_sender(Arc::clone(&back), Arc::clone(&lossy));
    let to_client = delayed_sender(Arc::clone(&front), Arc::clone(&lossy));
    {
        let front = Arc::clone(&front);
        let client = Arc::clone(&client);
        let lossy = Arc::clone(&lossy);
        thread::spawn(move || {
            let mut buf = vec![0u8; 65536];
            while let Ok((n, from)) = front.recv_from(&mut buf) {
                *client.lock().unwrap() = Some(from);
                if lossy.drop_packet() {
                    lossy.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let _ = to_target.send((Instant::now() + lossy.delay, buf[..n].to_vec(), target));
            }
        });
    }
    {
        let back = Arc::clone(&back);
        thread::spawn(move || {
            let mut buf = vec![0u8; 65536];
            while let Ok((n, _)) = back.recv_from(&mut buf) {
                let Some(to) = *client.lock().unwrap() else { continue };
                if lossy.drop_packet() {
                    lossy.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let _ = to_client.send((Instant::now() + lossy.delay, buf[..n].to_vec(), to));
            }
        });
    }
    public
}

/// Insert gathered candidates into their m-sections (the answerer has no
/// trickle path back through the bridge's C ABI, exactly like Reactor's answer).
fn with_candidates(sdp: &str, candidates: &[IceCandidate], lossy: Option<&Arc<Lossy>>) -> String {
    let mut sections: Vec<Vec<String>> = vec![Vec::new()];
    for line in sdp.split("\r\n").filter(|l| !l.is_empty()) {
        if line.starts_with("m=") {
            sections.push(Vec::new());
        }
        sections.last_mut().unwrap().push(line.to_owned());
    }
    let mut relays: HashMap<SocketAddr, SocketAddr> = HashMap::new();
    for c in candidates {
        let Some(index) = c.sdp_mline_index else { continue };
        let Some(section) = sections.get_mut(index as usize + 1) else { continue };
        let mut text = c.candidate.clone();
        if let Some(lossy) = lossy {
            // candidate:<foundation> <component> <proto> <priority> <ip> <port> typ <type> ...
            let mut parts: Vec<String> = text.split(' ').map(str::to_owned).collect();
            if parts.len() < 8 || !parts[2].eq_ignore_ascii_case("udp") || parts[7] != "host" {
                continue;
            }
            let Ok(target) = format!("{}:{}", parts[4], parts[5]).parse::<SocketAddr>() else { continue };
            let public = *relays
                .entry(target)
                .or_insert_with(|| relay(target, Arc::clone(lossy)));
            parts[5] = public.port().to_string();
            text = parts.join(" ");
        }
        section.push(format!("a={}", text.trim_start_matches("a=")));
    }
    for section in sections.iter_mut().skip(1) {
        section.push("a=end-of-candidates".into());
    }
    let mut out = sections.concat().join("\r\n");
    out.push_str("\r\n");
    out
}

struct Session {
    ans: Answerer,
    stop: Arc<AtomicBool>,
    pump: Option<thread::JoinHandle<()>>,
    echo: Option<thread::JoinHandle<()>>,
    pumped: Arc<PumpCounters>,
    echoed: Arc<AtomicU64>,
    opened: Instant,
}

impl Session {
    fn stats(&self, id: &str) -> Value {
        let mut outbound = Vec::new();
        if let Ok(report) = self.ans.pc.get_stats() {
            for o in report.outbound_rtp {
                outbound.push(json!({
                    "kind": format!("{:?}", o.kind),
                    "packetsSent": o.packets_sent,
                    "bytesSent": o.bytes_sent,
                    "retransmittedPacketsSent": o.retransmitted_packets_sent,
                    "nackCount": o.nack_count,
                    "pliCount": o.pli_count,
                    "targetBitrate": o.target_bitrate_bps,
                    "framesSent": o.frames_sent,
                    "frameWidth": o.frame_width,
                    "frameHeight": o.frame_height,
                    "fractionLost": o.fraction_lost,
                    "roundTripTime": o.round_trip_time_s,
                }));
            }
        }
        json!({
            "id": id,
            "connected": self.ans.connected.load(Ordering::Acquire),
            "videoPushed": self.pumped.video.load(Ordering::Relaxed),
            "audioPushed": self.pumped.audio.load(Ordering::Relaxed),
            "echoed": self.echoed.load(Ordering::Relaxed),
            "seconds": self.opened.elapsed().as_secs_f64(),
            "outbound": outbound,
        })
    }

    fn close(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(p) = self.pump.take() {
            let _ = p.join();
        }
        if let Some(e) = self.echo.take() {
            let _ = e.join();
        }
        drop(self.ans);
    }
}

fn open(
    factory: &PeerConnectionFactory,
    id: &str,
    offer_sdp: &str,
    pattern: &Arc<Pattern>,
    fps: u32,
    min_kbps: i32,
    max_kbps: i32,
    lossy: Option<&Arc<Lossy>>,
) -> (Session, String) {
    let offer_sdp = if lossy.is_some() {
        offer_sdp
            .split("\r\n")
            .filter(|l| !l.starts_with("a=candidate:"))
            .collect::<Vec<_>>()
            .join("\r\n")
    } else {
        offer_sdp.to_owned()
    };
    let offer = SessionDescription { kind: SdpType::Offer, sdp: offer_sdp };
    let (ans, answer) = Answerer::new(factory, &RtcConfiguration::default(), &offer, id);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ans.gathered.load(Ordering::Acquire) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    let candidates: Vec<IceCandidate> = ans.ice.lock().unwrap().drain(..).collect();
    let sdp = with_candidates(&answer.sdp, &candidates, lossy);
    for t in ans.pc.transceivers() {
        if t.kind() == reactor_webrtc::MediaKind::Video {
            let _ = t.set_send_bitrate(Some(min_kbps * 1000), Some(max_kbps * 1000));
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let pumped = Arc::new(PumpCounters::default());
    // Media starts once ICE/DTLS connect so pushed counts reflect sendable frames.
    let pump = {
        let video = Arc::clone(&ans.video);
        let audio = Arc::clone(&ans.audio);
        let pattern = Arc::clone(pattern);
        let stop = Arc::clone(&stop);
        let pumped = Arc::clone(&pumped);
        let connected = Arc::clone(&ans.connected);
        thread::spawn(move || {
            while !connected.load(Ordering::Acquire) && !stop.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(5));
            }
            if !stop.load(Ordering::Acquire) {
                pump(video, audio, pattern, fps, stop, pumped).join().ok();
            }
        })
    };
    let echoed = Arc::new(AtomicU64::new(0));
    let echo = {
        let channels = Arc::clone(&ans.channels);
        let stop = Arc::clone(&stop);
        let echoed = Arc::clone(&echoed);
        thread::spawn(move || {
            let (tx, rx) = mpsc::channel::<(String, Vec<u8>)>();
            let mut wired = 0usize;
            while !stop.load(Ordering::Acquire) {
                {
                    let mut list = channels.lock().unwrap();
                    for channel in list.iter_mut().skip(wired) {
                        let label = channel.label();
                        let tx = tx.clone();
                        channel.on_message(move |bytes, _binary| {
                            let _ = tx.send((label.clone(), bytes.to_vec()));
                        });
                        wired += 1;
                    }
                }
                while let Ok((label, bytes)) = rx.recv_timeout(Duration::from_millis(2)) {
                    let list = channels.lock().unwrap();
                    if let Some(channel) = list.iter().find(|c: &&DataChannel| c.label() == label) {
                        if channel.send(&bytes, true).is_ok() {
                            echoed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        })
    };
    (
        Session {
            ans,
            stop,
            pump: Some(pump),
            echo: Some(echo),
            pumped,
            echoed,
            opened: Instant::now(),
        },
        sdp,
    )
}

fn main() {
    let width: u32 = arg("--width", 1344);
    let height: u32 = arg("--height", 768);
    let fps: u32 = arg("--fps", 24);
    let max_kbps: i32 = arg("--max-kbps", 8000);
    let min_kbps: i32 = arg("--min-kbps", 300);
    let loss: f64 = arg("--loss", 0.0);
    let delay_ms: u64 = arg("--delay-ms", 0);
    let distinct: usize = arg("--distinct", 48);
    let lossy = (loss > 0.0 || delay_ms > 0).then(|| {
        Arc::new(Lossy {
            loss,
            delay: Duration::from_millis(delay_ms),
            seed: Mutex::new(0x2545_f491_4f6c_dd1d),
            dropped: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
        })
    });
    let pattern = Arc::new(Pattern::new(width, height, distinct, true));
    let factory = factory();
    let mut sessions: HashMap<String, Session> = HashMap::new();
    out(json!({ "op": "ready", "width": width, "height": height, "fps": fps, "loss": loss, "delayMs": delay_ms, "minKbps": min_kbps, "maxKbps": max_kbps }));
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(msg) = serde_json::from_str::<Value>(&line) else { continue };
        let op = msg["op"].as_str().unwrap_or("");
        let id = msg["id"].as_str().unwrap_or("").to_owned();
        match op {
            "offer" => {
                let (session, sdp) = open(
                    &factory,
                    &id,
                    msg["sdp"].as_str().unwrap_or(""),
                    &pattern,
                    fps,
                    min_kbps,
                    max_kbps,
                    lossy.as_ref(),
                );
                sessions.insert(id.clone(), session);
                out(json!({ "op": "answer", "id": id, "sdp": sdp }));
            }
            "candidate" => {
                if lossy.is_some() {
                    continue;
                }
                if let Some(s) = sessions.get(&id) {
                    let c = IceCandidate {
                        candidate: msg["candidate"].as_str().unwrap_or("").to_owned(),
                        sdp_mid: msg["sdpMid"].as_str().map(str::to_owned),
                        sdp_mline_index: msg["sdpMLineIndex"].as_u64().map(|v| v as u16),
                    };
                    if !c.candidate.is_empty() {
                        let _ = s.ans.pc.add_ice_candidate(&c);
                    }
                }
            }
            "stats" => {
                let list: Vec<Value> = sessions.iter().map(|(k, s)| s.stats(k)).collect();
                let relay = lossy.as_ref().map(|l| {
                    json!({ "dropped": l.dropped.load(Ordering::Relaxed), "forwarded": l.forwarded.load(Ordering::Relaxed) })
                });
                out(json!({ "op": "stats", "wallMicros": wall_micros(), "sessions": list, "relay": relay }));
            }
            "close" => {
                if let Some(s) = sessions.remove(&id) {
                    s.close();
                }
                out(json!({ "op": "closed", "id": id }));
            }
            "quit" => break,
            _ => {}
        }
    }
    for (_, s) in sessions.drain() {
        s.close();
    }
    drop(factory);
    out(json!({ "op": "bye" }));
}
