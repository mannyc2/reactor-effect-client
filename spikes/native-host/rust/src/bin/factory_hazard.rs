//! Reproduction for reactor-webrtc docs/architecture.md "One factory per
//! process" at bebf63e. Each mode prints one JSON line per iteration and exits
//! 0 when every iteration completed; a crash surfaces as a signal to the
//! driver script (run-factory-hazard.sh).
//!
//!   shared    N   one factory for both ends, N sequential sessions (the documented contract)
//!   per-peer  N   bridge pattern: a new factory per peer, offerer and answerer alive together
//!   renewal   N   the-show renewal: session k+1 on a new factory while session k streams,
//!                 then drop session k and its factory while k+1 streams
//!   seq       N   create factory + PC + offer, drop, repeat (no overlap)
//!   churn     N T T threads each create/drop factories N times concurrently
//!   drop-live N   drop a factory whose PC is still connected and receiving media

use native_host_spikes::{connect, factory, pump, Answerer, Offerer, Pattern, PumpCounters};
use reactor_webrtc::{MediaKind, PeerConnectionFactory, PeerConnectionObserver, RtcConfiguration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

struct Session {
    off: Offerer,
    ans: Answerer,
    stop: Arc<AtomicBool>,
    pump: Option<thread::JoinHandle<()>>,
    pumped: Arc<PumpCounters>,
}

impl Session {
    fn open(client: &PeerConnectionFactory, far: &PeerConnectionFactory, pattern: &Arc<Pattern>) -> Self {
        let (off, offer) = Offerer::new(client);
        let (ans, answer) = Answerer::new(far, &RtcConfiguration::default(), &offer, "hazard");
        off.pc.set_remote_description(&answer).expect("remote answer");
        assert!(connect(&off, &ans, Duration::from_secs(20)), "session did not connect");
        let stop = Arc::new(AtomicBool::new(false));
        let pumped = Arc::new(PumpCounters::default());
        let pump = pump(
            Arc::clone(&ans.video),
            Arc::clone(&ans.audio),
            Arc::clone(pattern),
            30,
            Arc::clone(&stop),
            Arc::clone(&pumped),
        );
        Self { off, ans, stop, pump: Some(pump), pumped }
    }

    fn wait_media(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.off.counters.video.load(Ordering::Relaxed) > 3
                && self.off.counters.audio.load(Ordering::Relaxed) > 3
            {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    fn close(mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Release);
        if let Some(p) = self.pump.take() {
            p.join().expect("pump thread");
        }
        let got = (
            self.off.counters.video.load(Ordering::Relaxed),
            self.off.counters.audio.load(Ordering::Relaxed),
        );
        let _ = self.pumped.video.load(Ordering::Relaxed);
        // Drop order mirrors packages/native WorkerState::shutdown: channels,
        // remote tracks, peer, and only then the factory (by the caller).
        drop(self.off.channels);
        self.off.remote.lock().unwrap().clear();
        drop(self.off.pc);
        drop(self.ans);
        got
    }
}

fn line(mode: &str, iteration: usize, extra: &str) {
    println!("{{\"mode\":\"{mode}\",\"iteration\":{iteration}{extra}}}");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("shared");
    let n: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(10);
    let threads: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(2);
    let pattern = Arc::new(Pattern::new(320, 180, 8, false));
    let started = Instant::now();
    match mode {
        "shared" => {
            let f = factory();
            for i in 0..n {
                let s = Session::open(&f, &f, &pattern);
                assert!(s.wait_media(Duration::from_secs(10)), "no media");
                let (v, a) = s.close();
                line(mode, i, &format!(",\"video\":{v},\"audio\":{a}"));
            }
        }
        "per-peer" => {
            for i in 0..n {
                let client = factory();
                let far = factory();
                let s = Session::open(&client, &far, &pattern);
                assert!(s.wait_media(Duration::from_secs(10)), "no media");
                let (v, a) = s.close();
                drop(client);
                drop(far);
                line(mode, i, &format!(",\"video\":{v},\"audio\":{a}"));
            }
        }
        "renewal" => {
            let far = factory();
            let mut current_factory = factory();
            let mut current = Session::open(&current_factory, &far, &pattern);
            assert!(current.wait_media(Duration::from_secs(10)), "no media");
            for i in 0..n {
                // Open the replacement on its own factory while the old one streams.
                let next_factory = factory();
                let next = Session::open(&next_factory, &far, &pattern);
                assert!(next.wait_media(Duration::from_secs(10)), "replacement had no media");
                thread::sleep(Duration::from_millis(300));
                // Drain: drop the old session and its factory while the replacement streams.
                let before = next.off.counters.video.load(Ordering::Relaxed);
                let (v, a) = current.close();
                drop(current_factory);
                thread::sleep(Duration::from_millis(200));
                let after = next.off.counters.video.load(Ordering::Relaxed);
                assert!(after > before, "replacement stalled while the old factory was destroyed");
                line(mode, i, &format!(",\"oldVideo\":{v},\"oldAudio\":{a},\"replacementFramesDuringDrop\":{}", after - before));
                current = next;
                current_factory = next_factory;
            }
            current.close();
            drop(current_factory);
            drop(far);
        }
        "seq" => {
            for i in 0..n {
                let f = factory();
                let pc = f
                    .create_peer_connection(&RtcConfiguration::default(), PeerConnectionObserver::new())
                    .expect("pc");
                pc.add_transceiver(MediaKind::Video, reactor_webrtc::TransceiverDirection::RecvOnly)
                    .expect("transceiver");
                let _ = pc.create_data_channel("control").expect("dc");
                let offer = pc.create_offer().expect("offer");
                pc.set_local_description(&offer).expect("local");
                drop(pc);
                drop(f);
                if i % 50 == 0 {
                    line(mode, i, "");
                }
            }
        }
        "churn" => {
            let handles: Vec<_> = (0..threads)
                .map(|t| {
                    thread::spawn(move || {
                        for i in 0..n {
                            let f = factory();
                            let pc = f
                                .create_peer_connection(&RtcConfiguration::default(), PeerConnectionObserver::new())
                                .expect("pc");
                            let _ = pc.create_data_channel("control").expect("dc");
                            let offer = pc.create_offer().expect("offer");
                            pc.set_local_description(&offer).expect("local");
                            drop(pc);
                            drop(f);
                            if i % 50 == 0 {
                                println!("{{\"mode\":\"churn\",\"thread\":{t},\"iteration\":{i}}}");
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("churn thread panicked");
            }
        }
        "drop-live" => {
            let far = factory();
            for i in 0..n {
                let client = factory();
                let s = Session::open(&client, &far, &pattern);
                assert!(s.wait_media(Duration::from_secs(10)), "no media");
                // Drop the client factory first: objects keep it alive, so the
                // native threads must survive until the session is closed.
                drop(client);
                thread::sleep(Duration::from_millis(100));
                let (v, a) = s.close();
                line(mode, i, &format!(",\"video\":{v},\"audio\":{a}"));
            }
        }
        other => panic!("unknown mode {other}"),
    }
    println!(
        "{{\"mode\":\"{mode}\",\"done\":true,\"iterations\":{n},\"seconds\":{:.1}}}",
        started.elapsed().as_secs_f64()
    );
}
