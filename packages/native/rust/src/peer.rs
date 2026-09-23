//! A native peer: the handle the host holds, its owner thread and its
//! notifier thread.

mod callbacks;
mod media;
mod owner;
mod shared;

#[cfg(test)]
pub(crate) use media::{AudioItem, VideoItem};
pub(crate) use shared::Shared;

use crate::abi::{Channel, Operation};
use crate::error::{BridgeError, FailureClass};
use crate::ffi::ReactorEffectNotify;
use crate::protocol;
use crate::sync::lock;
use owner::{Command, Owner, Reply};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[cfg(test)]
mod tests;

/// A native peer, behind the opaque `ReactorEffectPeer *` of the C ABI.
///
/// Calls and sends run one at a time on the peer's owner thread, which holds
/// every libwebrtc object. Closing and the takes act on the shared queues
/// directly, from whichever thread the host uses.
pub struct ReactorEffectPeer {
    shared: Arc<Shared>,
    commands: Sender<Command>,
    threads: Mutex<Threads>,
}

// The host calls entry points from several threads at once.
const _: () = {
    const fn thread_safe<T: Send + Sync>() {}
    thread_safe::<ReactorEffectPeer>();
};

/// The peer's threads, until shutdown joins them.
#[derive(Default)]
struct Threads {
    owner: Option<JoinHandle<()>>,
    notifier: Option<JoinHandle<()>>,
}

impl ReactorEffectPeer {
    /// Start a peer's owner thread and, when the host passed a callback, its
    /// notifier thread. `None` when a thread cannot start.
    pub(crate) fn create(notify: Option<ReactorEffectNotify>) -> Option<Self> {
        let shared = Arc::new(Shared::new());
        let (commands, received) = mpsc::channel();
        let owner = thread::Builder::new()
            .name("reactor-effect-native".into())
            .spawn({
                let shared = Arc::clone(&shared);
                move || Owner::new(shared).run(&received)
            })
            .ok()?;
        let peer = Self {
            shared,
            commands,
            threads: Mutex::new(Threads {
                owner: Some(owner),
                notifier: None,
            }),
        };
        if let Some(notify) = notify {
            let shared = Arc::clone(&peer.shared);
            // On failure `peer` drops, which joins the owner thread.
            #[expect(
                clippy::redundant_closure,
                reason = "an extern \"C\" fn pointer does not implement FnMut"
            )]
            let notifier = thread::Builder::new()
                .name("reactor-effect-notify".into())
                .spawn(move || shared.notifier.run(|ready| notify(ready)))
                .ok()?;
            lock(&peer.threads).notifier = Some(notifier);
        }
        Some(peer)
    }

    pub(crate) fn shared(&self) -> &Shared {
        &self.shared
    }

    /// Run one operation. A snapshot is answered here, never queued behind a
    /// blocking libwebrtc call on the owner thread.
    pub(crate) fn call(&self, operation: u32, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        self.ensure_open()?;
        match Operation::try_from(operation)? {
            Operation::MediaSnapshot => protocol::encode(&self.shared.snapshot()),
            operation @ (Operation::Prepare
            | Operation::Answer
            | Operation::Direction
            | Operation::MaxBitrate
            | Operation::Stats) => self.on_owner(|reply| Command::Call {
                operation,
                request: request.to_vec(),
                reply,
            }),
        }
    }

    /// Send one binary message on a bridge data channel. The C ABI has already
    /// held it to `MAX_MESSAGE_BYTES`.
    pub(crate) fn send(&self, channel: u32, bytes: &[u8]) -> Result<(), BridgeError> {
        self.ensure_open()?;
        let channel = Channel::try_from(channel)?;
        self.on_owner(|reply| Command::Send {
            channel,
            bytes: bytes.to_vec(),
            reply,
        })
    }

    /// Fence callback admission and host calls at once, and discard what is
    /// queued.
    pub(crate) fn close(&self) {
        self.shared.close();
    }

    /// Close, then join the owner thread, every admitted callback and the
    /// notifier thread. Idempotent: holding the thread lock throughout makes
    /// a concurrent shutdown wait for this one to finish joining.
    pub(crate) fn shutdown(&self) -> Result<(), BridgeError> {
        self.close();
        let mut threads = lock(&self.threads);
        let mut result = Ok(());
        if let Some(owner) = threads.owner.take() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a failed send means the owner has stopped already; it is joined either way"
            )]
            let _ = self.commands.send(Command::Shutdown);
            if owner.join().is_err() {
                result = Err(BridgeError::new(
                    FailureClass::Native,
                    "native owner thread panicked",
                ));
            }
        }
        // The notifier may be inside the host callback, waiting for the host
        // to run it, so the host joins from a thread other than the one that
        // runs its callback.
        if let Some(notifier) = threads.notifier.take()
            && notifier.join().is_err()
            && result.is_ok()
        {
            result = Err(BridgeError::new(
                FailureClass::Native,
                "native notifier thread panicked",
            ));
        }
        result
    }

    fn ensure_open(&self) -> Result<(), BridgeError> {
        if self.shared.gate.is_open() {
            Ok(())
        } else {
            Err(BridgeError::closed())
        }
    }

    /// Queue a command for the owner thread and wait for its reply. Either
    /// channel failing means the owner thread has stopped: the peer is closed.
    fn on_owner<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T, BridgeError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.commands.send(command(reply))?;
        response.recv()?
    }
}

impl Drop for ReactorEffectPeer {
    fn drop(&mut self) {
        // A host that needs the result calls `reactor_effect_peer_shutdown`
        // first, and this is then a no-op.
        #[expect(
            clippy::let_underscore_must_use,
            reason = "destruction has no failure channel"
        )]
        let _ = self.shutdown();
    }
}
