//! Admission of libwebrtc callbacks, fenced by close and awaited by shutdown.

use super::lock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};

/// Admits libwebrtc callbacks until closed, and counts the admitted ones so
/// shutdown can wait for every callback that is still running.
pub(crate) struct CallbackGate {
    open: AtomicBool,
    running: Mutex<usize>,
    idle: Condvar,
}

impl CallbackGate {
    /// An open gate.
    pub(crate) fn new() -> Self {
        Self {
            open: AtomicBool::new(true),
            running: Mutex::new(0),
            idle: Condvar::new(),
        }
    }

    /// Whether the gate still admits callbacks and host calls.
    pub(crate) fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Admit one callback, or refuse it once the gate has closed. The callback
    /// counts as running until the guard drops.
    pub(crate) fn enter(&self) -> Option<CallbackGuard<'_>> {
        if !self.is_open() {
            return None;
        }
        let mut running = lock(&self.running);
        // `wait_idle` takes this lock after `close`, so re-checking under it
        // means a callback is either refused or counted before anyone waits.
        if !self.is_open() {
            return None;
        }
        *running += 1;
        Some(CallbackGuard { gate: self })
    }

    /// Stop admitting callbacks. Callbacks already admitted keep running.
    pub(crate) fn close(&self) {
        self.open.store(false, Ordering::Release);
    }

    /// Block until no admitted callback is running. Called after [`close`],
    /// it returns once no callback can run again.
    ///
    /// [`close`]: Self::close
    pub(crate) fn wait_idle(&self) {
        let _idle = self
            .idle
            .wait_while(lock(&self.running), |running| *running > 0)
            .unwrap_or_else(PoisonError::into_inner);
    }
}

impl Default for CallbackGate {
    fn default() -> Self {
        Self::new()
    }
}

/// One admitted callback. Dropping it lets a waiting shutdown proceed.
pub(crate) struct CallbackGuard<'gate> {
    gate: &'gate CallbackGate,
}

impl Drop for CallbackGuard<'_> {
    fn drop(&mut self) {
        let mut running = lock(&self.gate.running);
        *running -= 1;
        if *running == 0 {
            self.gate.idle.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Defer, wait_until};
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn close_refuses_new_callbacks_and_wait_idle_waits_for_admitted_ones() {
        let gate = CallbackGate::new();
        let guard = gate.enter().expect("an open gate admits");
        gate.close();
        assert!(!gate.is_open());
        assert!(gate.enter().is_none(), "a closed gate must refuse");

        thread::scope(|scope| {
            let waiter = scope.spawn(|| gate.wait_idle());
            thread::sleep(Duration::from_millis(10));
            assert!(
                !waiter.is_finished(),
                "wait_idle returned while a callback was running"
            );
            drop(guard);
            waiter.join().unwrap();
        });
    }

    #[test]
    fn wait_idle_returns_at_once_when_nothing_was_admitted() {
        let gate = CallbackGate::default();
        gate.close();
        gate.wait_idle();
    }

    #[test]
    fn no_callback_runs_once_close_and_wait_idle_return() {
        let gate = CallbackGate::new();
        let running = AtomicUsize::new(0);
        let admitted = AtomicUsize::new(0);
        let stop = AtomicBool::new(false);
        thread::scope(|scope| {
            let _stop_callers = Defer(|| stop.store(true, Ordering::Relaxed));
            for _ in 0..4 {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        if let Some(_guard) = gate.enter() {
                            running.fetch_add(1, Ordering::SeqCst);
                            admitted.fetch_add(1, Ordering::Relaxed);
                            thread::yield_now();
                            running.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                });
            }
            wait_until("admitted callbacks", Duration::from_secs(5), || {
                admitted.load(Ordering::Relaxed) >= 1_000
            });

            gate.close();
            gate.wait_idle();
            assert_eq!(
                running.load(Ordering::SeqCst),
                0,
                "a callback outlived wait_idle"
            );
            let admitted_at_close = admitted.load(Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            assert_eq!(
                admitted.load(Ordering::SeqCst),
                admitted_at_close,
                "a callback was admitted after close"
            );
        });
    }
}
