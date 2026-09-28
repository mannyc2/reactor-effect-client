//! The hand-off between libwebrtc callbacks, a peer's owner thread and its
//! host.

mod gate;
mod queue;
mod readiness;

pub(crate) use gate::CallbackGate;
pub(crate) use queue::{Push, Queue, QueueItem, Taken};
pub(crate) use readiness::{Readiness, Ready, Wake};

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Lock `mutex`, recovering its data if a panic poisoned it.
///
/// No critical section in this crate can panic after it starts mutating, so a
/// poisoned lock still guards consistent data, and a peer keeps serving its
/// host after an entry point caught a panic.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
