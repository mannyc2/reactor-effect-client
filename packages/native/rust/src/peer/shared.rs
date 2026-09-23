//! The state a peer shares between its owner thread, libwebrtc callbacks and
//! the host's takes.

use super::media::{self, AudioItem, VideoItem};
use crate::abi::Ready;
use crate::error::FailureClass;
use crate::protocol::{DecimalU64, Direction, Event, Mapping, MediaSnapshot, TrackKind};
use crate::sync::{CallbackGate, Notifier, Push, Queue, lock};
use reactor_webrtc::RemoteTrack;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// Queue bounds. No single item can exceed its queue's byte bound, so the host
// sizes its take buffers from them: keep them in step with the MAX_*_BYTES and
// MAX_AUDIO_SAMPLES constants in packages/native/src/_internal/bridge.ts.
const EVENT_QUEUE_ITEMS: usize = 1024;
const EVENT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
/// A third of a second at 24 fps.
const VIDEO_QUEUE_FRAMES: usize = 8;
const VIDEO_QUEUE_BYTES: usize = 64 * 1024 * 1024;
/// 2.56 s of 10 ms blocks.
const AUDIO_QUEUE_BLOCKS: usize = 256;
const AUDIO_QUEUE_BYTES: usize = 4 * 1024 * 1024;

/// What a peer's threads share. libwebrtc callbacks copy into its queues and
/// signal readiness; the host takes from the queues.
pub(crate) struct Shared {
    /// Admits libwebrtc callbacks, and host calls, until the peer closes.
    pub(crate) gate: CallbackGate,
    /// Transport events, each an encoded packet.
    pub(crate) events: Queue<Vec<u8>>,
    pub(crate) video: Queue<VideoItem>,
    pub(crate) audio: Queue<AudioItem>,
    pub(crate) notifier: Notifier,
    /// Declared receive tracks not yet claimed by a remote track.
    bindings: Mutex<Bindings>,
    /// Remote tracks, kept alive so their sinks keep delivering.
    remote_tracks: Mutex<Vec<RemoteTrack>>,
    /// Set once the event queue overflows, which retires the connection.
    overflowed: AtomicBool,
}

/// A declared receive track waiting for its remote track.
#[derive(Debug)]
struct Binding {
    name: String,
    mid: String,
    /// The track's index in the prepare request.
    index: u32,
}

/// Unclaimed bindings per kind, in declaration order.
///
/// Pinned reactor-webrtc does not tell a remote track's MID, so a remote
/// track claims the first unclaimed binding of its kind. The host therefore
/// declares at most one receive track per kind.
#[derive(Debug, Default)]
struct Bindings {
    video: VecDeque<Binding>,
    audio: VecDeque<Binding>,
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            gate: CallbackGate::new(),
            events: Queue::new(EVENT_QUEUE_ITEMS, EVENT_QUEUE_BYTES),
            video: Queue::new(VIDEO_QUEUE_FRAMES, VIDEO_QUEUE_BYTES),
            audio: Queue::new(AUDIO_QUEUE_BLOCKS, AUDIO_QUEUE_BYTES),
            notifier: Notifier::default(),
            bindings: Mutex::default(),
            remote_tracks: Mutex::default(),
            overflowed: AtomicBool::new(false),
        }
    }

    /// Run a libwebrtc callback's body unless the peer has closed. Shutdown
    /// waits for the body to return.
    pub(crate) fn admit(&self, body: impl FnOnce()) {
        if let Some(_running) = self.gate.enter() {
            body();
        }
    }

    /// Queue a transport event for the host. A full queue retires the
    /// connection rather than lose the event.
    pub(crate) fn emit(&self, event: &Event<'_>) {
        match self.events.try_push(event.to_packet()) {
            Push::Accepted => self.notifier.signal(Ready::Events),
            Push::Closed => {}
            Push::Overflow => self.fail_overflow(),
        }
    }

    /// Report a failure of the connection itself as an `error` event.
    pub(crate) fn emit_error(&self, class: FailureClass, message: &str) {
        self.emit(&Event::error(class, message));
    }

    /// Retire the connection: fence admission and replace the event backlog
    /// with the one diagnostic that says why.
    fn fail_overflow(&self) {
        if self.overflowed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.gate.close();
        let diagnostic = Event::error(
            FailureClass::Overflow,
            "native transport event queue overflowed; connection retired",
        );
        self.events.replace(diagnostic.to_packet());
        self.notifier.signal(Ready::Events);
    }

    pub(crate) fn push_video(&self, frame: VideoItem) {
        if self.video.push_drop_oldest(frame) {
            self.notifier.signal(Ready::Video);
        }
    }

    pub(crate) fn push_audio(&self, block: AudioItem) {
        if self.audio.push_drop_oldest(block) {
            self.notifier.signal(Ready::Audio);
        }
    }

    /// Record which declared tracks remote tracks may claim.
    pub(crate) fn set_bindings(&self, mapping: &[Mapping]) {
        let mut bindings = Bindings::default();
        for (index, entry) in (0..).zip(mapping) {
            if entry.direction != Direction::RecvOnly {
                continue;
            }
            let binding = Binding {
                name: entry.name.clone(),
                mid: entry.mid.clone(),
                index,
            };
            match entry.kind {
                TrackKind::Video => bindings.video.push_back(binding),
                TrackKind::Audio => bindings.audio.push_back(binding),
            }
        }
        *lock(&self.bindings) = bindings;
    }

    fn claim_binding(&self, kind: TrackKind) -> Option<Binding> {
        let mut bindings = lock(&self.bindings);
        match kind {
            TrackKind::Video => bindings.video.pop_front(),
            TrackKind::Audio => bindings.audio.pop_front(),
        }
    }

    /// Bind a remote track to a declared receive track and route its decoded
    /// media to the queues. A track nobody declared breaks the negotiated
    /// contract.
    pub(crate) fn accept_remote(self: &Arc<Self>, track: RemoteTrack) {
        let kind = media::kind_of(&track);
        let Some(binding) = self.claim_binding(kind) else {
            self.emit_error(
                FailureClass::Protocol,
                "received native track without a declared receive mapping",
            );
            return;
        };
        media::route(&track, binding.index, self);
        let (name, mid) = (binding.name.as_str(), binding.mid.as_str());
        self.emit(&Event::Track { name, mid });
        self.emit(&Event::Decoded { kind, name, mid });
        lock(&self.remote_tracks).push(track);
    }

    /// Drop the remote tracks, which stops their sinks.
    pub(crate) fn release_remote_tracks(&self) {
        lock(&self.remote_tracks).clear();
    }

    pub(crate) fn snapshot(&self) -> MediaSnapshot {
        let (events, video, audio) = (
            self.events.counts(),
            self.video.counts(),
            self.audio.counts(),
        );
        MediaSnapshot {
            closed: !self.gate.is_open(),
            queued_control: events.queued,
            queued_video: video.queued,
            queued_audio: audio.queued,
            queued_bytes: events.bytes + video.bytes + audio.bytes,
            dropped_video: DecimalU64(video.dropped),
            dropped_audio: DecimalU64(audio.dropped),
            delivered_video: DecimalU64(video.taken),
            delivered_audio: DecimalU64(audio.taken),
            pending_requests: 0,
        }
    }

    /// Fence admission and discard what is queued: `reactor_effect_peer_close`.
    pub(crate) fn close(&self) {
        self.gate.close();
        self.close_queues();
    }

    /// Refuse later items, discard queued ones and stop the notifier thread.
    pub(crate) fn close_queues(&self) {
        self.events.close();
        self.video.close();
        self.audio.close();
        self.notifier.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::Status;
    use crate::sync::Taken;
    use crate::test_support::parse_packet;
    use serde_json::json;

    fn mapping(name: &str, kind: TrackKind, direction: Direction) -> Mapping {
        Mapping {
            name: name.into(),
            kind,
            direction,
            mid: format!("mid-{name}"),
        }
    }

    #[test]
    fn event_overflow_retires_the_connection_with_one_diagnostic() {
        let shared = Shared::new();
        for index in 0..EVENT_QUEUE_ITEMS {
            let name = index.to_string();
            shared.emit(&Event::Track {
                name: &name,
                mid: "0",
            });
        }
        assert!(shared.gate.is_open());

        shared.emit(&Event::Ice { candidate: None });
        assert!(
            !shared.gate.is_open(),
            "overflow must retire the connection"
        );
        assert_eq!(shared.events.counts().queued, 1, "the backlog collapses");
        let Taken::Item(packet) = shared.events.take(|_| true) else {
            panic!("the overflow diagnostic must be queued");
        };
        let (header, payload) = parse_packet(&packet);
        assert!(payload.is_empty());
        assert_eq!(header["type"], "error");
        assert_eq!(header["status"], Status::Overflow.code());
    }

    #[test]
    fn remote_tracks_claim_declared_receive_tracks_by_kind_in_order() {
        let shared = Shared::new();
        shared.set_bindings(&[
            mapping("out", TrackKind::Video, Direction::SendOnly),
            mapping("video", TrackKind::Video, Direction::RecvOnly),
            mapping("audio", TrackKind::Audio, Direction::RecvOnly),
            mapping("second", TrackKind::Video, Direction::RecvOnly),
        ]);
        let claimed = |kind| {
            shared
                .claim_binding(kind)
                .map(|binding| (binding.name, binding.index))
        };
        assert_eq!(claimed(TrackKind::Audio), Some(("audio".into(), 2)));
        assert_eq!(claimed(TrackKind::Video), Some(("video".into(), 1)));
        assert_eq!(claimed(TrackKind::Video), Some(("second".into(), 3)));
        assert_eq!(
            claimed(TrackKind::Video),
            None,
            "sending tracks are never claimed"
        );
        assert_eq!(claimed(TrackKind::Audio), None);
    }

    #[test]
    fn close_fences_events_and_media_and_the_snapshot_says_so() {
        let shared = Shared::new();
        shared.emit(&Event::Ice { candidate: None });
        shared.close();
        shared.emit(&Event::Ice { candidate: None });
        assert_eq!(shared.events.take(|_| true), Taken::Closed);
        let snapshot = serde_json::to_value(shared.snapshot()).unwrap();
        assert_eq!(
            snapshot,
            json!({
                "closed": true,
                "queuedControl": 0,
                "queuedVideo": 0,
                "queuedAudio": 0,
                "queuedBytes": 0,
                "droppedVideo": "0",
                "droppedAudio": "0",
                "deliveredVideo": "0",
                "deliveredAudio": "0",
                "pendingRequests": 0,
            })
        );
    }
}
