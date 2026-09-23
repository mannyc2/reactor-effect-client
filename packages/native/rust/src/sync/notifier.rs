//! The readiness hand-off to the host's notify callback.

use super::lock;
use crate::abi::Ready;
use std::mem;
use std::sync::{Condvar, Mutex, PoisonError};

/// Readiness waiting for the host. libwebrtc threads only set bits here; a
/// peer's notifier thread is the one thread that ever waits on the host.
#[derive(Default)]
pub(crate) struct Notifier {
    state: Mutex<State>,
    wake: Condvar,
}

#[derive(Default)]
struct State {
    /// Readiness bits the host has not been passed yet.
    ready: u32,
    closed: bool,
}

impl Notifier {
    /// Mark a queue ready, waking the notifier thread if nothing was pending.
    /// Never waits for the host.
    pub(crate) fn signal(&self, ready: Ready) {
        let mut state = lock(&self.state);
        if state.closed || state.ready & ready.bit() != 0 {
            return;
        }
        let was_idle = state.ready == 0;
        state.ready |= ready.bit();
        if was_idle {
            self.wake.notify_one();
        }
    }

    /// Stop the notifier thread. Later signals are ignored.
    pub(crate) fn close(&self) {
        lock(&self.state).closed = true;
        self.wake.notify_all();
    }

    /// The notifier thread's loop: pass pending readiness bits to `notify`
    /// until closed. Signals raised while `notify` runs coalesce into its next
    /// call.
    pub(crate) fn run(&self, mut notify: impl FnMut(u32)) {
        loop {
            let ready = {
                let mut state = self
                    .wake
                    .wait_while(lock(&self.state), |state| state.ready == 0 && !state.closed)
                    .unwrap_or_else(PoisonError::into_inner);
                if state.closed {
                    return;
                }
                mem::take(&mut state.ready)
            };
            notify(ready);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Defer;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_secs(5);

    #[test]
    fn readiness_raised_before_the_host_runs_arrives_as_one_call() {
        let notifier = Notifier::default();
        notifier.signal(Ready::Video);
        notifier.signal(Ready::Video);
        notifier.signal(Ready::Audio);
        let (calls, received) = mpsc::channel();
        thread::scope(|scope| {
            let _stop_runner = Defer(|| notifier.close());
            let runner = scope.spawn(|| notifier.run(|ready| calls.send(ready).unwrap()));
            assert_eq!(
                received.recv_timeout(TIMEOUT),
                Ok(Ready::Video.bit() | Ready::Audio.bit())
            );
            notifier.signal(Ready::Events);
            assert_eq!(received.recv_timeout(TIMEOUT), Ok(Ready::Events.bit()));
            notifier.close();
            runner.join().unwrap();
        });
        notifier.signal(Ready::Events);
        assert!(
            received.try_recv().is_err(),
            "a signal after close must not reach the host"
        );
    }

    #[test]
    fn signals_never_wait_for_a_busy_host_and_coalesce_behind_it() {
        let notifier = Notifier::default();
        let (entered, host_entered) = mpsc::channel();
        let (release, host_released) = mpsc::channel::<()>();
        let (calls, received) = mpsc::channel();
        let notifier = &notifier;
        thread::scope(|scope| {
            let _stop_runner = Defer(|| notifier.close());
            let runner = scope.spawn(move || {
                notifier.run(|ready| {
                    calls.send(ready).unwrap();
                    if ready == Ready::Video.bit() {
                        entered.send(()).unwrap();
                        host_released.recv_timeout(TIMEOUT).unwrap();
                    }
                });
            });
            notifier.signal(Ready::Video);
            host_entered.recv_timeout(TIMEOUT).unwrap();

            // The host is still inside its callback: these must not block.
            notifier.signal(Ready::Audio);
            notifier.signal(Ready::Events);
            notifier.signal(Ready::Audio);
            release.send(()).unwrap();

            assert_eq!(received.recv_timeout(TIMEOUT), Ok(Ready::Video.bit()));
            assert_eq!(
                received.recv_timeout(TIMEOUT),
                Ok(Ready::Audio.bit() | Ready::Events.bit())
            );
            notifier.close();
            runner.join().unwrap();
        });
    }
}
