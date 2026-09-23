//! One answered connection: its libwebrtc peer, the echo on both data
//! channels, and the pump that sends its media.

use crate::media::{self, Pacing, Shape};
use crate::sdp::with_candidates;
use reactor_webrtc::{
    AudioTrack, AudioTrackOptions, AudioTrackSource, DataChannel, IceCandidate, IceGatheringState,
    MediaKind, PeerConnection, PeerConnectionFactory, PeerConnectionObserver, PeerConnectionState,
    RtcConfiguration, SdpType, SessionDescription, StreamKind, TransceiverDirection, VideoTrack,
};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Each connection holds its congestion controller at this rate. On loopback
/// its estimate follows only how promptly the host schedules both processes,
/// and on a busy runner it backs off until the encoder drops most frames; the
/// load tests need the load they name.
const LOAD_BPS: i32 = 8_000_000;
const MIN_VIDEO_BPS: i32 = 300_000;
/// How long an answer waits for the far peer's candidates.
const GATHER_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// A message to echo: its channel's label and its bytes.
type Echo = (String, Vec<u8>);

/// What the connection's callbacks observed.
#[derive(Default)]
struct Signals {
    candidates: Mutex<Vec<IceCandidate>>,
    gathered: AtomicBool,
    connected: AtomicBool,
    channels: Mutex<Vec<DataChannel>>,
}

pub(crate) struct Session {
    peer: PeerConnection,
    signals: Arc<Signals>,
    stop: Arc<AtomicBool>,
    pacing: Arc<Pacing>,
    pump_thread: JoinHandle<()>,
    echo_thread: JoinHandle<()>,
}

impl Session {
    /// Answer the bridge's offer and publish media on every transceiver it
    /// opened. Returns the session and its answer, which carries the far
    /// peer's gathered candidates.
    pub(crate) fn open(
        factory: &PeerConnectionFactory,
        offer: String,
        frames: Arc<Vec<Vec<u8>>>,
        shape: Shape,
    ) -> (Self, String) {
        let signals = Arc::new(Signals::default());
        // Echo off libwebrtc's callback thread, which must not send re-entrantly.
        let (echo, echoes) = mpsc::channel();
        let peer = factory
            .create_peer_connection(&RtcConfiguration::default(), observer(&signals, echo))
            .expect("far peer connection");
        peer.set_bitrate(Some(LOAD_BPS), Some(LOAD_BPS), Some(LOAD_BPS))
            .expect("far peer bitrate");
        let offer = SessionDescription {
            kind: SdpType::Offer,
            sdp: offer,
        };
        peer.set_remote_description(&offer)
            .expect("far peer accepts the bridge offer");
        let (video, audio) = publish(factory, &peer);
        let answer = peer.create_answer().expect("far answer");
        peer.set_local_description(&answer)
            .expect("far local answer");
        let answer = with_candidates(&answer.sdp, &gathered(&signals));

        let stop = Arc::new(AtomicBool::new(false));
        let pacing = Arc::new(Pacing::default());
        let echo_thread = thread::spawn({
            let signals = Arc::clone(&signals);
            move || echo_messages(&signals, &echoes)
        });
        let pump_thread = thread::spawn({
            let (signals, stop, pacing) =
                (Arc::clone(&signals), Arc::clone(&stop), Arc::clone(&pacing));
            move || {
                if connected_before_stop(&signals, &stop) {
                    media::pump(&video, &audio, &frames, shape, &stop, &pacing);
                }
            }
        });
        let session = Self {
            peer,
            signals,
            stop,
            pacing,
            pump_thread,
            echo_thread,
        };
        (session, answer)
    }

    pub(crate) fn add_candidate(&self, candidate: &IceCandidate) {
        // A refused candidate only loses that one path.
        let _ = self.peer.add_ice_candidate(candidate);
    }

    /// The pump's pacing, the video encoder's counters and the selected
    /// path, which the load tests print.
    pub(crate) fn stats(&self) -> Value {
        let report = self.peer.get_stats().ok();
        let video = report
            .as_ref()
            .and_then(|report| {
                report
                    .outbound_rtp
                    .iter()
                    .find(|entry| entry.kind == StreamKind::Video)
            })
            .map_or_else(
                || json!({}),
                |outbound| {
                    json!({
                        "framesSent": outbound.frames_sent,
                        "frameWidth": outbound.frame_width,
                        "frameHeight": outbound.frame_height,
                        "targetBitrate": outbound.target_bitrate_bps,
                        "packetsLost": outbound.packets_lost,
                        "nackCount": outbound.nack_count,
                        "pliCount": outbound.pli_count,
                    })
                },
            );
        let path = report
            .as_ref()
            .and_then(|report| report.candidate_pairs.iter().find(|pair| pair.nominated))
            .map_or_else(
                || json!({}),
                |pair| {
                    json!({
                        "availableOutgoingBitrate": pair.available_outgoing_bitrate_bps,
                        "currentRoundTripTime": pair.current_round_trip_time_s,
                    })
                },
            );
        let lag_max = Duration::from_micros(self.pacing.lag_max_us.load(Ordering::Relaxed));
        json!({
            "connected": self.signals.connected.load(Ordering::Acquire),
            "framesPushed": self.pacing.pushed.load(Ordering::Relaxed),
            "latePushes": self.pacing.late.load(Ordering::Relaxed),
            "pushLagMaxMs": lag_max.as_secs_f64() * 1000.0,
            "video": video,
            "path": path,
        })
    }

    pub(crate) fn close(self) {
        self.stop.store(true, Ordering::Release);
        // Only the pump's stopping matters here, not how it ended.
        let _ = self.pump_thread.join();
        // Dropping the connection drops its observer, and with it one echo
        // sender; the channels hold the others.
        lock(&self.signals.channels).clear();
        drop(self.peer);
        let _ = self.echo_thread.join();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Record candidates and state, and echo each data channel's messages.
fn observer(signals: &Arc<Signals>, echo: Sender<Echo>) -> PeerConnectionObserver {
    PeerConnectionObserver::new()
        .on_ice_candidate({
            let signals = Arc::clone(signals);
            move |candidate| lock(&signals.candidates).push(candidate)
        })
        .on_ice_gathering_change({
            let signals = Arc::clone(signals);
            move |state| {
                if state == IceGatheringState::Complete {
                    signals.gathered.store(true, Ordering::Release);
                }
            }
        })
        .on_connection_state_change({
            let signals = Arc::clone(signals);
            move |state| {
                let connected = state == PeerConnectionState::Connected;
                signals.connected.store(connected, Ordering::Release);
            }
        })
        .on_data_channel({
            let signals = Arc::clone(signals);
            move |mut channel| {
                let (label, echo) = (channel.label(), echo.clone());
                channel.on_message(move |bytes, _binary| {
                    // The echo thread only stops once the session closes.
                    let _ = echo.send((label.clone(), bytes.to_vec()));
                });
                lock(&signals.channels).push(channel);
            }
        })
}

/// Send one video and one audio track on every transceiver of the offer, as
/// Reactor publishes into the bridge's receive tracks.
fn publish(factory: &PeerConnectionFactory, peer: &PeerConnection) -> (VideoTrack, AudioTrack) {
    let video = factory
        .create_video_track("far-video")
        .expect("far video track");
    let mut options = AudioTrackOptions::default();
    options.source = AudioTrackSource::LocalPush;
    let audio = factory
        .create_audio_track_with_options("far-audio", options)
        .expect("far audio track");
    for transceiver in peer.transceivers() {
        let kind = transceiver.kind();
        match kind {
            MediaKind::Video => transceiver.set_track(&video).expect("send video"),
            MediaKind::Audio => transceiver.set_track(&audio).expect("send audio"),
            MediaKind::Unknown => continue,
        }
        transceiver
            .set_direction(TransceiverDirection::SendOnly)
            .expect("send direction");
        if kind == MediaKind::Video {
            transceiver
                .set_send_bitrate(Some(MIN_VIDEO_BPS), Some(LOAD_BPS))
                .expect("video bitrate");
        }
    }
    (video, audio)
}

/// The far peer's candidates once gathering completes, or those it has
/// gathered by [`GATHER_TIMEOUT`].
fn gathered(signals: &Signals) -> Vec<IceCandidate> {
    let deadline = Instant::now() + GATHER_TIMEOUT;
    while !signals.gathered.load(Ordering::Acquire) && Instant::now() < deadline {
        thread::sleep(POLL_INTERVAL);
    }
    lock(&signals.candidates).drain(..).collect()
}

/// Whether the connection came up before the session was told to stop.
fn connected_before_stop(signals: &Signals, stop: &AtomicBool) -> bool {
    while !signals.connected.load(Ordering::Acquire) {
        if stop.load(Ordering::Acquire) {
            return false;
        }
        thread::sleep(POLL_INTERVAL);
    }
    true
}

/// Send each message back on its channel, until every echo sender is gone.
fn echo_messages(signals: &Signals, echoes: &Receiver<Echo>) {
    while let Ok((label, bytes)) = echoes.recv() {
        let channels = lock(&signals.channels);
        if let Some(channel) = channels.iter().find(|channel| channel.label() == label) {
            // A closing channel drops its echo, as a remote peer's would.
            let _ = channel.send(&bytes, true);
        }
    }
}
