//! The state a peer shares between its owner thread, libwebrtc callbacks and
//! the host's takes.

use super::media::{self, AudioItem, VideoItem};
use crate::error::FailureClass;
use crate::protocol::{Direction, Event, Mapping, MediaSnapshot, TrackKind};
use crate::sync::{CallbackGate, Push, Queue, Readiness, Ready, lock};
use reactor_webrtc::RemoteTrack;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// Queue bounds. The event queue never evicts, so its bound retires a connection
// whose host stopped taking events; media queues evict the oldest items.
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
    /// Transport events, in the order they happened.
    pub(crate) events: Queue<Event>,
    pub(crate) video: Queue<VideoItem>,
    pub(crate) audio: Queue<AudioItem>,
    pub(crate) readiness: Readiness,
    /// Calls queued for the owner thread that it has not yet taken.
    pub(crate) in_flight: AtomicUsize,
    /// Declared receive tracks not yet claimed by a remote track.
    bindings: Mutex<Bindings>,
    /// Remote tracks, kept alive so their sinks keep delivering.
    remote_tracks: Mutex<Vec<RemoteTrack>>,
    /// Set once an event is lost, which retires the connection.
    retired: AtomicBool,
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
            readiness: Readiness::default(),
            in_flight: AtomicUsize::new(0),
            bindings: Mutex::default(),
            remote_tracks: Mutex::default(),
            retired: AtomicBool::new(false),
        }
    }

    /// Run a libwebrtc callback's body unless the peer has closed. Shutdown
    /// waits for the body to return.
    pub(crate) fn admit(&self, body: impl FnOnce()) {
        if let Some(_running) = self.gate.enter() {
            body();
        }
    }

    /// Queue a transport event for the host. An event that cannot be queued
    /// retires the connection rather than go missing.
    pub(crate) fn emit(&self, event: Event) {
        match self.events.try_push(event) {
            Push::Accepted => self.readiness.signal(Ready::Events),
            Push::Closed => {}
            Push::Overflow => self.retire(
                FailureClass::Overflow,
                "native transport event queue overflowed; connection retired",
            ),
        }
    }

    /// Report a failure of the connection itself as an `error` event.
    pub(crate) fn emit_error(&self, class: FailureClass, message: impl Into<String>) {
        self.emit(Event::error(class, message));
    }

    /// Retire the connection: fence admission and replace the event backlog
    /// with the one diagnostic that says why.
    fn retire(&self, class: FailureClass, message: &str) {
        if self.retired.swap(true, Ordering::AcqRel) {
            return;
        }
        self.gate.close();
        self.events.replace(Event::error(class, message));
        self.readiness.signal(Ready::Events);
    }

    pub(crate) fn push_video(&self, frame: VideoItem) {
        if self.video.push_drop_oldest(frame) {
            self.readiness.signal(Ready::Video);
        }
    }

    pub(crate) fn push_audio(&self, block: AudioItem) {
        if self.audio.push_drop_oldest(block) {
            self.readiness.signal(Ready::Audio);
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
        self.emit(Event::Track {
            name: binding.name.clone(),
            mid: binding.mid.clone(),
        });
        self.emit(Event::Decoded {
            kind,
            name: binding.name,
            mid: binding.mid,
        });
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
            pending_requests: self.in_flight.load(Ordering::Acquire),
            queued_control: events.queued,
            queued_video: video.queued,
            queued_audio: audio.queued,
            queued_bytes: events.bytes + video.bytes + audio.bytes,
            dropped_video: video.dropped,
            dropped_audio: audio.dropped,
            delivered_video: video.taken,
            delivered_audio: audio.taken,
        }
    }

    /// Fence admission and discard what is queued.
    pub(crate) fn close(&self) {
        self.gate.close();
        self.close_queues();
    }

    /// Refuse later items, discard queued ones and stop waking the host.
    pub(crate) fn close_queues(&self) {
        self.events.close();
        self.video.close();
        self.audio.close();
        self.readiness.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Taken;

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
            shared.emit(Event::Track {
                name: index.to_string(),
                mid: "0".into(),
            });
        }
        assert!(shared.gate.is_open());

        shared.emit(Event::Ice { candidate: None });
        assert!(
            !shared.gate.is_open(),
            "overflow must retire the connection"
        );
        assert_eq!(shared.events.counts().queued, 1, "the backlog collapses");
        let Taken::Item(Event::Error { class, .. }) = shared.events.take(|_| true) else {
            panic!("the overflow diagnostic must be queued");
        };
        assert_eq!(class, FailureClass::Overflow);
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
        shared.emit(Event::Ice { candidate: None });
        shared.close();
        shared.emit(Event::Ice { candidate: None });
        assert_eq!(shared.events.take(|_| true), Taken::Closed);
        let snapshot = shared.snapshot();
        assert!(snapshot.closed);
        assert_eq!(
            (snapshot.queued_control, snapshot.queued_bytes),
            (0, 0),
            "close discards the backlog"
        );
    }
}
