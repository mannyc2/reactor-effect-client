//! A peer driven through its handle, the way the addon drives it.

use super::*;
use crate::protocol::{Direction, Mapping, TrackKind, TrackSpec};
use crate::test_support::{wait_until, with_candidates};
use reactor_webrtc::{
    AudioFrame, AudioTrack, AudioTrackOptions, AudioTrackSource, DataChannel, DataChannelState,
    IceCandidate, IceGatheringState, PeerConnection, PeerConnectionObserver, PeerConnectionState,
    RtcConfiguration, SdpType, SessionDescription, TransceiverDirection, VideoFrame, VideoTrack,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(5);
const MEDIA_TIMEOUT: Duration = Duration::from_secs(20);

/// Submit a call and wait for its answer, as the addon's promise would.
fn wait<T: Send + 'static>(submit: impl FnOnce(Done<T>)) -> Result<T, BridgeError> {
    let (sender, receiver) = mpsc::sync_channel(1);
    submit(Box::new(move |result| {
        sender.send(result).expect("the test waits for the answer");
    }));
    receiver
        .recv_timeout(MEDIA_TIMEOUT)
        .expect("every call is answered")
}

fn track(name: &str, kind: TrackKind, direction: Direction) -> TrackSpec {
    TrackSpec {
        name: name.into(),
        kind,
        direction,
    }
}

#[test]
fn a_closed_peer_refuses_calls_and_sends_before_queueing_them() {
    let peer = Peer::create().expect("a peer");
    peer.close();
    assert_eq!(
        wait(|done| peer.stats(done)).unwrap_err(),
        BridgeError::closed()
    );
    assert_eq!(
        wait(|done| peer.send(Channel::Data, b"late".to_vec(), done)).unwrap_err(),
        BridgeError::closed()
    );
    peer.shutdown().expect("shutdown");
}

#[test]
fn a_send_over_the_message_bound_or_before_the_channel_opens_is_refused() {
    let peer = Peer::create().expect("a peer");
    let error =
        wait(|done| peer.send(Channel::Data, vec![0; MAX_MESSAGE_BYTES + 1], done)).unwrap_err();
    assert_eq!(error.class, FailureClass::Overflow);
    assert_eq!(
        wait(|done| peer.send(Channel::Control, b"early".to_vec(), done)).unwrap_err(),
        BridgeError::new(FailureClass::ChannelClosed, "control channel is not open")
    );
    peer.shutdown().expect("shutdown");
}

#[test]
fn a_call_is_pending_from_admission_until_the_owner_takes_it_up() {
    let peer = Peer::create().expect("a peer");
    // The first call's answer runs on the owner thread and holds it there.
    let (release, held) = mpsc::sync_channel::<()>(0);
    let (answered, first) = mpsc::sync_channel(1);
    peer.stats(Box::new(move |result| {
        held.recv().expect("the test releases the owner");
        answered
            .send(result)
            .expect("the test waits for the answer");
    }));
    let (queued, second) = mpsc::sync_channel(1);
    peer.stats(Box::new(move |result| {
        queued.send(result).expect("the test waits for the answer");
    }));
    wait_until("the second call to queue", TIMEOUT, || {
        peer.snapshot().pending_requests == 1
    });
    release.send(()).expect("the owner is held");
    for answer in [first, second] {
        let refused = answer.recv_timeout(TIMEOUT).expect("answered");
        assert_eq!(refused.expect_err("unprepared"), BridgeError::closed());
    }
    assert_eq!(peer.snapshot().pending_requests, 0);
    peer.shutdown().expect("shutdown");
}

#[test]
fn shutdown_is_idempotent_and_leaves_every_queue_closed() {
    let peer = Peer::create().expect("a peer");
    peer.shutdown().expect("first shutdown");
    peer.shutdown().expect("second shutdown");
    let shared = peer.shared();
    assert_eq!(shared.events.take(|_| true), Taken::Closed);
    assert_eq!(shared.video.take(|_| true), Taken::Closed);
    assert_eq!(shared.audio.take(|_| true), Taken::Closed);
}

#[test]
fn an_event_wakes_the_host_once_until_it_takes_the_readiness() {
    let peer = Peer::create().expect("a peer");
    let wakes = Arc::new(AtomicUsize::new(0));
    peer.set_wake(Box::new({
        let wakes = Arc::clone(&wakes);
        move || {
            wakes.fetch_add(1, Ordering::SeqCst);
        }
    }));
    peer.shared().emit(Event::Ice { candidate: None });
    peer.shared().emit(Event::Ice { candidate: None });
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(peer.take_ready(), crate::sync::Ready::Events.bit());
    assert_eq!(peer.take_event(), Some(Event::Ice { candidate: None }));
    peer.shutdown().expect("shutdown");
    peer.shared().emit(Event::Ice { candidate: None });
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "a closed peer wakes no one"
    );
}

/// What the remote end of a loopback observed.
#[derive(Default)]
struct RemoteSignals {
    candidates: Mutex<Vec<IceCandidate>>,
    gathered: AtomicBool,
    connected: AtomicBool,
    channels: Mutex<HashMap<String, DataChannel>>,
    /// Each message's channel, bytes and whether it was binary.
    inbox: Mutex<Vec<(String, Vec<u8>, bool)>>,
}

impl RemoteSignals {
    /// Records what the remote peer sees. Assertions stay on the test thread:
    /// a panic on a libwebrtc thread would abort the process.
    fn observer(self: &Arc<Self>) -> PeerConnectionObserver {
        PeerConnectionObserver::new()
            .on_ice_candidate({
                let signals = Arc::clone(self);
                move |candidate| lock(&signals.candidates).push(candidate)
            })
            .on_ice_gathering_change({
                let signals = Arc::clone(self);
                move |state| {
                    if state == IceGatheringState::Complete {
                        signals.gathered.store(true, Ordering::Release);
                    }
                }
            })
            .on_connection_state_change({
                let signals = Arc::clone(self);
                move |state| {
                    let connected = state == PeerConnectionState::Connected;
                    signals.connected.store(connected, Ordering::Release);
                }
            })
            .on_data_channel({
                let signals = Arc::clone(self);
                move |mut channel| {
                    let label = channel.label();
                    channel.on_message({
                        let (signals, label) = (Arc::clone(&signals), label.clone());
                        move |bytes, binary| {
                            lock(&signals.inbox).push((label.clone(), bytes.to_vec(), binary));
                        }
                    });
                    lock(&signals.channels).insert(label, channel);
                }
            })
    }
}

/// What the host has seen of the bridge through its events.
#[derive(Debug, Default)]
struct HostView {
    connected: bool,
    open_channels: HashSet<String>,
    /// `(kind, name)` of each decoded event.
    decoded: Vec<(String, String)>,
    messages: Vec<(String, Vec<u8>)>,
}

/// Media the remote peer publishes into the bridge's receive tracks.
struct Sources {
    video_a: VideoTrack,
    video_b: VideoTrack,
    audio: AudioTrack,
}

/// A bridge peer connected over loopback to a libwebrtc peer that answers
/// its offer, as Reactor's media server would. Fields drop in order: the
/// sources before the remote connection, and that before its channels.
struct Loopback {
    bridge: Peer,
    mapping: Vec<Mapping>,
    sources: Sources,
    remote: PeerConnection,
    signals: Arc<RemoteSignals>,
    host: HostView,
}

impl Loopback {
    /// Prepare the bridge with two video and one audio receive track and one
    /// send track, answer from a remote peer publishing into them, and wait
    /// until both ends are connected with both channels open.
    fn connect() -> Self {
        let bridge = Peer::create().expect("a bridge peer");
        let request = PrepareRequest::new(
            vec![],
            vec![
                track("video-a", TrackKind::Video, Direction::RecvOnly),
                track("video-b", TrackKind::Video, Direction::RecvOnly),
                track("audio-a", TrackKind::Audio, Direction::RecvOnly),
                track("outgoing-video", TrackKind::Video, Direction::SendOnly),
            ],
        )
        .expect("a request within the bounds");
        let prepared = wait(|done| bridge.prepare(request, done)).expect("prepare");
        let offer = SessionDescription {
            kind: SdpType::Offer,
            sdp: prepared.sdp,
        };
        assert!(
            offer.declares_frame_metadata(),
            "the offer must negotiate frame metadata"
        );
        let mapping: Vec<Mapping> = prepared.mapping;

        // The remote peer shares the bridge's process-wide factory, as
        // reactor-webrtc requires of every peer in one process.
        let factory = owner::factory().expect("the process factory");
        let signals = Arc::new(RemoteSignals::default());
        let remote = factory
            .create_peer_connection(&RtcConfiguration::default(), signals.observer())
            .expect("a remote peer");
        remote
            .set_remote_description(&offer)
            .expect("the remote accepts the offer");
        let sources = Sources {
            video_a: factory.create_video_track("fixture-video-a").unwrap(),
            video_b: factory.create_video_track("fixture-video-b").unwrap(),
            audio: factory
                .create_audio_track_with_options("fixture-audio", {
                    let mut options = AudioTrackOptions::default();
                    options.source = AudioTrackSource::LocalPush;
                    options
                })
                .unwrap(),
        };
        for transceiver in remote.transceivers() {
            let mid = transceiver.mid().expect("a negotiated MID");
            let Some(entry) = mapping.iter().find(|entry| entry.mid == mid) else {
                continue;
            };
            if entry.direction != Direction::RecvOnly {
                continue;
            }
            match entry.name.as_str() {
                "video-a" => transceiver.set_track(&sources.video_a).unwrap(),
                "video-b" => transceiver.set_track(&sources.video_b).unwrap(),
                "audio-a" => transceiver.set_track(&sources.audio).unwrap(),
                other => panic!("unexpected receive mapping {other}"),
            }
            transceiver
                .set_direction(TransceiverDirection::SendOnly)
                .unwrap();
        }
        let answer = remote.create_answer().expect("an answer");
        assert!(
            answer.declares_frame_metadata(),
            "the answer must echo frame metadata support"
        );
        remote
            .set_local_description(&answer)
            .expect("the remote applies its answer");
        wait_until("the remote peer to gather candidates", TIMEOUT, || {
            signals.gathered.load(Ordering::Acquire)
        });
        let answer = with_candidates(&answer.sdp, &lock(&signals.candidates));
        wait(|done| bridge.answer(answer, done)).expect("the bridge applies the answer");

        let mut loopback = Self {
            bridge,
            mapping,
            sources,
            remote,
            signals,
            host: HostView::default(),
        };
        wait_until(
            "a connection with both channels open",
            MEDIA_TIMEOUT,
            || {
                loopback.pump_events();
                loopback.host.connected
                    && loopback.host.open_channels.len() == 2
                    && loopback.signals.connected.load(Ordering::Acquire)
                    && loopback.remote_channels_open()
            },
        );
        loopback
    }

    /// Take the bridge's events as the host would: forward its candidates to
    /// the remote peer and record what the events report.
    fn pump_events(&mut self) {
        while let Some(event) = self.bridge.take_event() {
            match event {
                Event::State { state } => self.host.connected = state == "connected",
                Event::Channel { channel, open } => {
                    let label = channel.label().to_owned();
                    if open {
                        self.host.open_channels.insert(label);
                    } else {
                        self.host.open_channels.remove(&label);
                    }
                }
                Event::Ice {
                    candidate: Some(candidate),
                } => {
                    let candidate = IceCandidate {
                        candidate: candidate.candidate,
                        sdp_mid: candidate.sdp_mid,
                        sdp_mline_index: candidate.sdp_mline_index,
                    };
                    self.remote
                        .add_ice_candidate(&candidate)
                        .expect("the remote accepts the bridge's candidate");
                }
                Event::Decoded { kind, name, .. } => {
                    let kind = match kind {
                        TrackKind::Video => "video",
                        TrackKind::Audio => "audio",
                    };
                    self.host.decoded.push((kind.to_owned(), name));
                }
                Event::Message { channel, bytes } => {
                    self.host.messages.push((channel.label().to_owned(), bytes));
                }
                Event::Error { class, message } => {
                    panic!("the bridge failed: {class:?} {message}")
                }
                Event::Ice { candidate: None } | Event::Track { .. } => {}
            }
        }
    }

    fn remote_channels_open(&self) -> bool {
        let channels = lock(&self.signals.channels);
        ["control", "data"].into_iter().all(|label| {
            channels
                .get(label)
                .is_some_and(|channel| channel.state() == DataChannelState::Open)
        })
    }

    fn remote_send(&self, label: &str, bytes: &[u8]) {
        lock(&self.signals.channels)[label]
            .send(bytes, true)
            .expect("the remote sends");
    }

    /// The messages the remote peer received on `label`, which must all have
    /// been binary.
    fn remote_received(&self, label: &str) -> Vec<Vec<u8>> {
        lock(&self.signals.inbox)
            .iter()
            .filter(|(channel, ..)| channel == label)
            .map(|(_, bytes, binary)| {
                assert!(binary, "bridge channels carry binary messages");
                bytes.clone()
            })
            .collect()
    }

    fn host_received(&self, label: &str) -> Vec<Vec<u8>> {
        self.host
            .messages
            .iter()
            .filter(|(channel, _)| channel == label)
            .map(|(_, bytes)| bytes.clone())
            .collect()
    }

    fn track_index(&self, name: &str) -> u32 {
        let position = self
            .mapping
            .iter()
            .position(|entry| entry.name == name)
            .expect("a mapped track");
        u32::try_from(position).unwrap()
    }
}

/// Video lanes carry distinct fills, so decoding cannot pass off one as the other.
const LANE_A_FILL: u8 = 0x21;
const LANE_B_FILL: u8 = 0x83;
const WIDTH: u32 = 64;
const HEIGHT: u32 = 48;
const FRAME_BYTES: usize = WIDTH as usize * HEIGHT as usize * 4;

impl Sources {
    /// Push a frame on each video lane, with that lane's metadata, and 10 ms
    /// of 48 kHz audio.
    fn push(&self) {
        let lanes = [
            (&self.video_a, LANE_A_FILL, b"meta-a"),
            (&self.video_b, LANE_B_FILL, b"meta-b"),
        ];
        for (track, fill, metadata) in lanes {
            let bgra = vec![fill; FRAME_BYTES];
            track
                .push_frame_with_metadata(VideoFrame::new(&bgra, WIDTH, HEIGHT), metadata)
                .expect("the remote pushes video");
        }
        let pcm: Vec<i16> = (0..480_i16)
            .map(|sample| (sample % 128 - 64) * 128)
            .collect();
        let block = AudioFrame {
            pcm: &pcm,
            sample_rate: 48_000,
            channels: 1,
            frames: 480,
        };
        self.audio
            .push_frame(block)
            .expect("the remote pushes audio");
    }
}

#[test]
fn messages_cross_a_loopback_connection_in_order_both_ways() {
    let mut loopback = Loopback::connect();
    for message in [b"one".as_slice(), b"two", b"three"] {
        wait(|done| loopback.bridge.send(Channel::Data, message.to_vec(), done))
            .expect("the bridge sends");
    }
    wait_until("three messages at the remote", TIMEOUT, || {
        loopback.remote_received("data").len() == 3
    });
    assert_eq!(
        loopback.remote_received("data"),
        [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );

    loopback.remote_send("control", b"alpha");
    loopback.remote_send("control", b"beta");
    wait_until("two control messages at the host", TIMEOUT, || {
        loopback.pump_events();
        loopback.host_received("control").len() == 2
    });
    assert_eq!(
        loopback.host_received("control"),
        [b"alpha".to_vec(), b"beta".to_vec()]
    );
}

#[test]
fn each_receive_lane_decodes_real_media_with_its_own_metadata() {
    let mut loopback = Loopback::connect();
    let audio_track = loopback.track_index("audio-a");
    let shared = Arc::clone(loopback.bridge.shared());
    let mut video = HashMap::new();
    let mut audio_blocks = 0;
    wait_until("decoded media on every receive lane", MEDIA_TIMEOUT, || {
        loopback.sources.push();
        while let Taken::Item(frame) = shared.video.take(|_| true) {
            assert_eq!((frame.width, frame.height), (WIDTH, HEIGHT));
            assert_eq!(frame.bgra.len(), FRAME_BYTES);
            video.insert(frame.track, frame);
        }
        while let Taken::Item(block) = shared.audio.take(|_| true) {
            assert_eq!(block.track, audio_track);
            assert_eq!((block.sample_rate, block.channels), (48_000, 1));
            assert!(!block.pcm.is_empty());
            audio_blocks += 1;
        }
        // Pace the pushes near real time.
        thread::sleep(Duration::from_millis(30));
        video.len() == 2 && audio_blocks > 0
    });

    let lane_a = &video[&loopback.track_index("video-a")];
    let lane_b = &video[&loopback.track_index("video-b")];
    // VP8 and H.264 are lossy, so exact pixels are not asserted; distinct
    // inputs must stay distinct through the real codec path.
    assert_ne!(
        lane_a.bgra[0], lane_b.bgra[0],
        "two video lanes collapsed into one"
    );
    assert_eq!(
        lane_a.metadata, b"meta-a",
        "lane A's metadata was misattributed"
    );
    assert_eq!(
        lane_b.metadata, b"meta-b",
        "lane B's metadata was misattributed"
    );

    loopback.pump_events();
    let decoded: HashSet<_> = loopback.host.decoded.iter().cloned().collect();
    let expected = [
        ("video", "video-a"),
        ("video", "video-b"),
        ("audio", "audio-a"),
    ]
    .map(|(kind, name)| (kind.to_owned(), name.to_owned()));
    assert_eq!(decoded, HashSet::from(expected));

    wait_until("stats for the live streams", TIMEOUT, || {
        let stats = wait(|done| loopback.bridge.stats(done)).expect("stats");
        let entries = stats.as_array().expect("a stats array");
        let inbound = |kind: &str| {
            entries
                .iter()
                .find(|entry| entry["type"] == "inbound-rtp" && entry["kind"] == kind)
        };
        let decoding =
            inbound("video").is_some_and(|entry| entry["framesDecoded"].as_u64().unwrap_or(0) > 0);
        let pair = entries
            .iter()
            .any(|entry| entry["type"] == "candidate-pair");
        decoding && inbound("audio").is_some() && pair
    });
}

#[test]
fn closing_a_live_connection_fences_late_frames() {
    let loopback = Loopback::connect();
    let shared = Arc::clone(loopback.bridge.shared());
    wait_until("a decoded frame", MEDIA_TIMEOUT, || {
        loopback.sources.push();
        thread::sleep(Duration::from_millis(30));
        shared.video.counts().queued > 0
    });

    // Close fences admission before the remote pushes more media; after
    // shutdown no late callback can reach a queue.
    loopback.bridge.close();
    loopback.sources.push();
    loopback.bridge.shutdown().expect("shutdown");
    assert_eq!(shared.video.take(|_| true), Taken::Closed);
    assert_eq!(shared.audio.take(|_| true), Taken::Closed);
    assert_eq!(shared.events.take(|_| true), Taken::Closed);
}
