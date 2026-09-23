//! The native WebRTC transport of `reactor-effect-native`, exported as a C ABI.
//!
//! `include/reactor_effect_native.h` is the contract and [`ffi`] implements
//! it. Each peer runs on three kinds of threads:
//!
//! - its **owner thread** holds the libwebrtc peer connection and runs the
//!   host's calls and sends one at a time;
//! - **libwebrtc threads** run callbacks, which only copy into bounded queues
//!   and set readiness bits: they never call or wait on the host;
//! - its **notifier thread** is the one thread that calls the host, passing
//!   it those readiness bits.
//!
//! The host drains the queues with nonblocking takes. Closing a peer fences
//! callback admission at once; shutting it down joins the owner thread, every
//! admitted callback and the notifier thread.
//!
//! The source tree follows those roles:
//!
//! - `ffi`: the exported functions and C structs;
//! - `abi`: the header's constants, checked against it by a test;
//! - `peer`: the peer handle, its owner thread and its libwebrtc callbacks;
//! - `protocol`: the JSON of requests, responses and event headers;
//! - `sync`: the queues, callback gate and notifier the threads share;
//! - `error`: failures and the ABI failure class of each.

mod abi;
mod error;
pub mod ffi;
mod peer;
mod protocol;
mod sync;

#[cfg(test)]
mod test_support;
