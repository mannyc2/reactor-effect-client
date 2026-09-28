//! A native peer: the handle the addon holds, and its owner thread.

mod callbacks;
mod media;
mod owner;
mod shared;

pub(crate) use media::{AudioItem, VideoItem};
pub(crate) use owner::{Done, Prepared};
pub(crate) use shared::Shared;

use crate::error::{BridgeError, FailureClass};
use crate::protocol::{
    BitrateRequest, Channel, Event, MAX_MESSAGE_BYTES, MediaSnapshot, PrepareRequest,
};
use crate::sync::{Taken, Wake, lock};
use owner::{Command, Owner};
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[cfg(test)]
mod tests;

/// The most calls a peer holds unanswered. A host that queues more is refused
/// with `Overflow` rather than growing the owner thread's backlog.
const MAX_IN_FLIGHT_CALLS: usize = 128;

/// A native peer. Calls and sends run one at a time on its owner thread, which
/// holds every libwebrtc object; each answers through its `Done`. Takes,
/// snapshots and close act on the shared queues from the host's thread.
pub(crate) struct Peer {
    shared: Arc<Shared>,
    commands: Sender<Command>,
    owner: Mutex<Option<JoinHandle<()>>>,
}

impl Peer {
    /// Start a peer's owner thread.
    pub(crate) fn create() -> Result<Self, BridgeError> {
        let shared = Arc::new(Shared::new());
        let (commands, received) = mpsc::channel();
        let owner = thread::Builder::new()
            .name("reactor-effect-native".into())
            .spawn({
                let shared = Arc::clone(&shared);
                move || Owner::new(shared).run(&received)
            })
            .map_err(|error| {
                BridgeError::new(
                    FailureClass::Native,
                    format!("could not start the native owner thread: {error}"),
                )
            })?;
        Ok(Self {
            shared,
            commands,
            owner: Mutex::new(Some(owner)),
        })
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// How the peer wakes its host when a queue has items.
    pub(crate) fn set_wake(&self, wake: Wake) {
        self.shared.readiness.set_wake(wake);
    }

    /// The readiness bits raised since the host last took them.
    pub(crate) fn take_ready(&self) -> u32 {
        self.shared.readiness.take()
    }

    /// Create the connection and its local offer.
    pub(crate) fn prepare(&self, request: PrepareRequest, done: Done<Prepared>) {
        self.submit(Command::Prepare { request, done });
    }

    /// Apply the remote answer, which carries the remote peer's candidates.
    pub(crate) fn answer(&self, sdp: String, done: Done<()>) {
        self.submit(Command::Answer { sdp, done });
    }

    /// Pause a declared track, or resume it in its declared direction.
    pub(crate) fn direction(&self, name: String, active: bool, done: Done<()>) {
        self.submit(Command::Direction { name, active, done });
    }

    /// Cap an outgoing track's send bitrate.
    pub(crate) fn max_bitrate(&self, request: BitrateRequest, done: Done<()>) {
        self.submit(Command::MaxBitrate { request, done });
    }

    /// Read WebRTC statistics.
    pub(crate) fn stats(&self, done: Done<Value>) {
        self.submit(Command::Stats { done });
    }

    /// Send one binary message on a bridge data channel.
    pub(crate) fn send(&self, channel: Channel, bytes: Vec<u8>, done: Done<()>) {
        if bytes.len() > MAX_MESSAGE_BYTES {
            done(Err(BridgeError::overflow(
                "native data channel message exceeds local bound",
            )));
            return;
        }
        self.submit(Command::Send {
            channel,
            bytes,
            done,
        });
    }

    /// Queue a command for the owner thread, or refuse it at once: when the
    /// peer has closed, or too many calls are unanswered.
    fn submit(&self, command: Command) {
        if !self.shared.gate.is_open() {
            command.refuse(BridgeError::closed());
            return;
        }
        let in_flight = &self.shared.in_flight;
        if in_flight.fetch_add(1, Ordering::AcqRel) >= MAX_IN_FLIGHT_CALLS {
            in_flight.fetch_sub(1, Ordering::AcqRel);
            command.refuse(BridgeError::overflow(
                "native call admission bound exceeded",
            ));
            return;
        }
        // A failed send means the owner thread has stopped: the peer is closed.
        if let Err(mpsc::SendError(command)) = self.commands.send(command) {
            in_flight.fetch_sub(1, Ordering::AcqRel);
            command.refuse(BridgeError::closed());
        }
    }

    /// What each queue dropped, delivered and still holds.
    pub(crate) fn snapshot(&self) -> MediaSnapshot {
        self.shared.snapshot()
    }

    pub(crate) fn take_event(&self) -> Option<Event> {
        taken(self.shared.events.take(|_| true))
    }

    pub(crate) fn take_video(&self) -> Option<VideoItem> {
        taken(self.shared.video.take(|_| true))
    }

    pub(crate) fn take_audio(&self) -> Option<AudioItem> {
        taken(self.shared.audio.take(|_| true))
    }

    /// Fence callback admission and host calls at once, and discard what is
    /// queued.
    pub(crate) fn close(&self) {
        self.shared.close();
    }

    /// Close, then join the owner thread, which releases the libwebrtc objects
    /// and waits for every admitted callback to return. It blocks, so the addon
    /// runs it off the JavaScript thread. Idempotent: a concurrent shutdown
    /// waits for this one's join.
    pub(crate) fn shutdown(&self) -> Result<(), BridgeError> {
        self.close();
        let mut owner = lock(&self.owner);
        let Some(handle) = owner.take() else {
            return Ok(());
        };
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a failed send means the owner has stopped already; it is joined either way"
        )]
        let _ = self.commands.send(Command::Shutdown);
        handle.join().map_err(|_panic| {
            BridgeError::new(FailureClass::Native, "native owner thread panicked")
        })
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        // The addon shuts a peer down before dropping it. One dropped without
        // that is closed here and its owner joined on a thread of its own, so
        // a garbage-collector finalizer never waits on libwebrtc.
        self.close();
        let Some(handle) = self.owner.get_mut().ok().and_then(Option::take) else {
            return;
        };
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a failed send means the owner has stopped already"
        )]
        let _ = self.commands.send(Command::Shutdown);
        #[expect(
            clippy::let_underscore_must_use,
            reason = "without a joiner thread the owner still exits on Shutdown, detached"
        )]
        let _ = thread::Builder::new()
            .name("reactor-effect-native-drop".into())
            .spawn(move || drop(handle.join()));
    }
}

fn taken<T>(taken: Taken<T>) -> Option<T> {
    match taken {
        Taken::Item(item) => Some(item),
        Taken::TooSmall | Taken::Empty | Taken::Closed => None,
    }
}
