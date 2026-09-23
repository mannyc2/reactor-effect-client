//! Helpers shared by this crate's tests.

use reactor_webrtc::IceCandidate;
use serde_json::Value;
use std::thread;
use std::time::{Duration, Instant};

/// Poll `done` until it holds, failing the test after `timeout`.
pub(crate) fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        thread::sleep(Duration::from_millis(2));
    }
}

/// Runs its closure when dropped, including while a failed assertion
/// unwinds: a test releases the threads its scope joins with one, so a failure
/// fails instead of hanging.
pub(crate) struct Defer<F: FnMut()>(pub(crate) F);

impl<F: FnMut()> Drop for Defer<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// Split an event packet into its JSON header and its payload.
pub(crate) fn parse_packet(packet: &[u8]) -> (Value, &[u8]) {
    let (length, rest) = packet
        .split_first_chunk::<4>()
        .expect("a packet starts with its header length");
    let (header, payload) = rest
        .split_at_checked(u32::from_le_bytes(*length) as usize)
        .expect("the header fits in the packet");
    (
        serde_json::from_slice(header).expect("the header is JSON"),
        payload,
    )
}

/// A deterministic xorshift generator: a failing sequence reproduces from
/// its seed.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    /// A value in `0..bound`.
    pub(crate) fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        usize::try_from(self.0 % bound as u64).unwrap()
    }
}

/// Add gathered candidates to their m-sections of `sdp` and end each
/// section's candidates, as an answer from Reactor arrives: the bridge's C ABI
/// has no call for remote candidates. The far peer example has its own copy,
/// since an example cannot use a library's test code.
pub(crate) fn with_candidates(sdp: &str, candidates: &[IceCandidate]) -> String {
    let mut sections: Vec<Vec<String>> = vec![Vec::new()];
    for line in sdp.split("\r\n").filter(|line| !line.is_empty()) {
        if line.starts_with("m=") {
            sections.push(Vec::new());
        }
        sections
            .last_mut()
            .expect("there is always a session section")
            .push(line.to_owned());
    }
    for candidate in candidates {
        let section = candidate
            .sdp_mline_index
            .and_then(|index| sections.get_mut(usize::from(index) + 1));
        if let Some(section) = section {
            let attribute = candidate.candidate.trim_start_matches("a=");
            section.push(format!("a={attribute}"));
        }
    }
    for section in sections.iter_mut().skip(1) {
        section.push("a=end-of-candidates".to_owned());
    }
    let mut sdp = sections.concat().join("\r\n");
    sdp.push_str("\r\n");
    sdp
}
