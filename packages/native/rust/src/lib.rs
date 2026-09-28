//! The native WebRTC transport of `reactor-effect-native`, a Node-API addon.
//!
//! [`binding`] is the surface Node.js and Bun load. Each peer runs on two
//! kinds of threads:
//!
//! - its **owner thread** holds the libwebrtc peer connection and runs the
//!   host's calls and sends one at a time, answering each through a promise;
//! - **libwebrtc threads** run callbacks, which only copy into bounded queues
//!   and raise readiness: they never call or wait on JavaScript. The first
//!   readiness bit queues a non-blocking wake on the JavaScript thread, where
//!   the host takes the items.
//!
//! Closing a peer fences callback admission at once; shutting it down joins
//! the owner thread and every admitted callback, off the JavaScript thread.
//!
//! The source tree follows those roles:
//!
//! - `binding`: the Node-API classes, objects and conversions;
//! - `peer`: the peer handle, its owner thread and its libwebrtc callbacks;
//! - `protocol`: requests and their bounds, events and statistics;
//! - `sync`: the queues, callback gate and readiness the threads share;
//! - `error`: failures and their classes.

pub mod binding;
mod error;
mod peer;
mod protocol;
mod sync;

#[cfg(test)]
mod test_support;
