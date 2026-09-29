//! Readiness: which queues hold items the host has not been told about.

use super::lock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// A queue that has items for the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ready {
    Events = 1,
    Video = 2,
    Audio = 4,
}

impl Ready {
    pub(crate) const fn bit(self) -> u32 {
        self as u32
    }
}

/// Wakes the host, which then takes the pending bits. It must return at once:
/// the addon's wake queues a call on the JavaScript thread without waiting.
pub(crate) type Wake = Box<dyn Fn() + Send + Sync>;

/// Readiness bits waiting for the host. libwebrtc threads set bits and wake
/// the host only when none were pending, so wakes coalesce and none is lost:
/// a bit set after the host took the pending bits wakes it again.
#[derive(Default)]
pub(crate) struct Readiness {
    pending: AtomicU32,
    closed: AtomicBool,
    wake: Mutex<Option<Wake>>,
}

impl Readiness {
    /// Install the host's wake. A peer without one is driven by polling.
    pub(crate) fn set_wake(&self, wake: Wake) {
        *lock(&self.wake) = Some(wake);
    }

    /// Mark a queue ready and wake the host if nothing was pending. Never
    /// waits for the host.
    pub(crate) fn signal(&self, ready: Ready) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if self.pending.fetch_or(ready.bit(), Ordering::AcqRel) == 0
            && let Some(wake) = lock(&self.wake).as_ref()
        {
            wake();
        }
    }

    /// The pending bits, which the host now handles.
    pub(crate) fn take(&self) -> u32 {
        self.pending.swap(0, Ordering::AcqRel)
    }

    /// Stop waking the host and release its wake. Later signals are ignored.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        drop(lock(&self.wake).take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    fn counting() -> (Readiness, Arc<AtomicUsize>) {
        let readiness = Readiness::default();
        let wakes = Arc::new(AtomicUsize::new(0));
        readiness.set_wake(Box::new({
            let wakes = Arc::clone(&wakes);
            move || {
                wakes.fetch_add(1, Ordering::SeqCst);
            }
        }));
        (readiness, wakes)
    }

    #[test]
    fn signals_before_the_host_takes_them_wake_it_once() {
        let (readiness, wakes) = counting();
        readiness.signal(Ready::Video);
        readiness.signal(Ready::Video);
        readiness.signal(Ready::Audio);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(readiness.take(), Ready::Video.bit() | Ready::Audio.bit());
        assert_eq!(readiness.take(), 0);
    }

    #[test]
    fn a_signal_after_the_host_took_the_bits_wakes_it_again() {
        let (readiness, wakes) = counting();
        readiness.signal(Ready::Events);
        assert_eq!(readiness.take(), Ready::Events.bit());
        readiness.signal(Ready::Events);
        assert_eq!(wakes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn close_releases_the_wake_and_ignores_later_signals() {
        let (readiness, wakes) = counting();
        readiness.close();
        readiness.signal(Ready::Events);
        assert_eq!(wakes.load(Ordering::SeqCst), 0);
        assert_eq!(readiness.take(), 0);
        assert!(lock(&readiness.wake).is_none(), "close must drop the wake");
    }
}
