//! The exported C ABI. `include/reactor_effect_native.h` is its contract.
//!
//! Every entry point catches panics, since unwinding into C is undefined: a
//! caught panic reports a native failure.
//!
//! A misaligned pointer argument is invalid input, and a length over its bound
//! is an overflow. Both are refused before any caller memory is touched, so
//! the `# Safety` sections below constrain only aligned pointers and lengths
//! within their bounds.

mod memory;
#[cfg(test)]
mod tests;

pub use crate::peer::ReactorEffectPeer;

use crate::abi::{
    ABI_VERSION, CALL_BUFFER_MIN, FAILURE_MESSAGE_BYTES, MAX_MESSAGE_BYTES, MAX_REQUEST_BYTES,
    Status,
};
use crate::error::{BridgeError, FailureClass};
use crate::sync::Taken;
use memory::{Out, OutSlice, input, peer_ref};
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

/// Called on a peer's notifier thread with the readiness bits of every queue
/// that received an item since the previous call (`ReactorEffectNotify`).
pub type ReactorEffectNotify = extern "C" fn(ready: u32);

/// The header of one decoded BGRA frame, written by
/// [`reactor_effect_peer_take_video`].
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReactorEffectVideoHeader {
    /// Frame width in pixels.
    pub width: u32,
    /// Frame height in pixels.
    pub height: u32,
    /// BGRA bytes: `width * height * 4`.
    pub data_len: u32,
    /// Bytes of the sender's frame-metadata user data.
    pub metadata_len: u32,
    /// The sender's frame ID; 0 when it supplied none.
    pub frame_id: u64,
    /// The sender's capture time in microseconds; 0 when absent.
    pub timestamp_us: u64,
    /// The track's index in the prepare request.
    pub track: u32,
    /// Always 0.
    pub reserved: u32,
    /// The frame's admission sequence on its track, from 0, stamped before
    /// the queue can drop it: a gap is a frame the queue dropped.
    pub sequence: u64,
}

/// The header of one interleaved PCM block, written by
/// [`reactor_effect_peer_take_audio`].
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReactorEffectAudioHeader {
    /// Samples per second, per channel.
    pub sample_rate: u32,
    /// Interleaved channels.
    pub channels: u32,
    /// Interleaved `int16_t` samples: frames times channels.
    pub samples: u32,
    /// The track's index in the prepare request.
    pub track: u32,
    /// The block's admission sequence on its track, from 0, stamped before
    /// the queue can drop it: a gap is a block the queue dropped.
    pub sequence: u64,
}

/// Diagnostic text beside a failure status. Never match on it: the status is
/// the failure class.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct ReactorEffectFailure {
    /// Bytes of `message` in use.
    pub message_len: u32,
    /// UTF-8, truncated on a character boundary.
    pub message: [u8; FAILURE_MESSAGE_BYTES],
}

impl ReactorEffectFailure {
    /// The diagnostic for `message`, truncated to fit.
    fn new(message: &str) -> Self {
        let text = truncate(message, FAILURE_MESSAGE_BYTES).as_bytes();
        let mut failure = Self {
            message_len: 0,
            message: [0; FAILURE_MESSAGE_BYTES],
        };
        // `truncate` keeps the text within the array, so this always copies.
        if let (Some(prefix), Ok(len)) = (
            failure.message.get_mut(..text.len()),
            u32::try_from(text.len()),
        ) {
            prefix.copy_from_slice(text);
            failure.message_len = len;
        }
        failure
    }
}

/// The longest prefix of `text` within `max` bytes that ends on a character
/// boundary.
fn truncate(text: &str, max: usize) -> &str {
    (0..=max.min(text.len()))
        .rev()
        .find_map(|end| text.get(..end))
        .unwrap_or_default()
}

/// This image's source and build identity: a NUL-terminated C string inside a
/// marker that packaging finds by scanning the file, without loading a
/// foreign-platform library.
static BUILD_IDENTITY: &CStr = {
    let marked = concat!(
        "reactor-effect-native:build-identity:",
        env!("REACTOR_EFFECT_BUILD_IDENTITY"),
        ":end\0"
    );
    match CStr::from_bytes_with_nul(marked.as_bytes()) {
        Ok(identity) => identity,
        Err(_) => panic!("the build identity must be one NUL-terminated C string"),
    }
};

/// The ABI version this library implements. The host refuses any other.
#[unsafe(no_mangle)]
pub extern "C" fn reactor_effect_abi_version() -> u32 {
    ABI_VERSION
}

/// The static source and build identity of this loaded image. Never free it.
#[unsafe(no_mangle)]
pub extern "C" fn reactor_effect_build_identity() -> *const c_char {
    BUILD_IDENTITY.as_ptr()
}

/// Allocate a peer, or return null when its threads cannot start.
///
/// When `notify` is non-null, a notifier thread calls it with the readiness
/// bits of the queues that received items since its previous call, until
/// shutdown joins that thread.
#[unsafe(no_mangle)]
pub extern "C" fn reactor_effect_peer_create(
    notify: Option<ReactorEffectNotify>,
) -> *mut ReactorEffectPeer {
    catch_unwind(|| ReactorEffectPeer::create(notify))
        .ok()
        .flatten()
        .map_or(ptr::null_mut(), |peer| Box::into_raw(Box::new(peer)))
}

/// Run one serialized peer operation. `request` is UTF-8; on success the
/// response is UTF-8 JSON and `response_cap` must be at least 4 MiB, which
/// `BUFFER_TOO_SMALL` reports through `response_len`.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`].
/// A nonzero `request_len` of at most 1 MiB needs `request` valid for reads of
/// that many bytes. A non-null `response` must be valid for writes of
/// `response_cap` bytes, a non-null `response_len` for one `usize`, and a
/// non-null `failure` for one [`ReactorEffectFailure`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_call(
    peer: *mut ReactorEffectPeer,
    operation: u32,
    request: *const u8,
    request_len: usize,
    response: *mut u8,
    response_cap: usize,
    response_len: *mut usize,
    failure: *mut ReactorEffectFailure,
) -> i32 {
    // SAFETY: a non-null `failure` is writable for the call, per the contract.
    let failure = unsafe { Out::new(failure) };
    // SAFETY: a non-null `response_len` is writable for the call.
    let response_len = unsafe { Out::new(response_len) };
    // SAFETY: a non-null `response` is writable for `response_cap` bytes.
    let response = unsafe { OutSlice::new(response, response_cap) };
    status_of(failure, || {
        // SAFETY: `peer` is null or live for the call.
        let peer = unsafe { peer_ref(peer) }?;
        let mut response = response?;
        let Some(mut response_len) = response_len?.filter(|_| !response.is_null()) else {
            return Err(BridgeError::invalid("call requires a response buffer"));
        };
        response_len.write(0);
        if response_cap < CALL_BUFFER_MIN {
            response_len.write(CALL_BUFFER_MIN);
            return Ok(Status::BufferTooSmall);
        }
        // SAFETY: a nonzero `request_len` within its bound makes `request`
        // readable for it.
        let request = unsafe { input(request, request_len, MAX_REQUEST_BYTES) }?;
        let bytes = peer.call(operation, request)?;
        if !response.holds(bytes.len()) {
            return Err(BridgeError::overflow(
                "native call response exceeds its buffer",
            ));
        }
        response.copy_from(&bytes)?;
        response_len.write(bytes.len());
        Ok(Status::Ok)
    })
}

/// Send one binary SCTP message on a bridge-owned data channel.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`].
/// A nonzero `data_len` of at most 256 KiB needs `data` valid for reads of
/// that many bytes, and a non-null `failure` must be writable for one
/// [`ReactorEffectFailure`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_send(
    peer: *mut ReactorEffectPeer,
    channel: u32,
    data: *const u8,
    data_len: usize,
    failure: *mut ReactorEffectFailure,
) -> i32 {
    // SAFETY: a non-null `failure` is writable for the call.
    let failure = unsafe { Out::new(failure) };
    status_of(failure, || {
        // SAFETY: `peer` is null or live for the call.
        let peer = unsafe { peer_ref(peer) }?;
        // SAFETY: a nonzero `data_len` within its bound makes `data` readable
        // for it.
        let bytes = unsafe { input(data, data_len, MAX_MESSAGE_BYTES) }?;
        peer.send(channel, bytes)?;
        Ok(Status::Ok)
    })
}

/// Nonblocking: copy the oldest transport event into caller memory and
/// remove it. `BUFFER_TOO_SMALL` writes the required size to `out_len` and
/// keeps the event queued; `AGAIN` means the queue is empty.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`].
/// A non-null `out_len` must be writable for one `usize`, and a nonzero
/// `out_cap` needs `out` valid for writes of that many bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_take_event(
    peer: *mut ReactorEffectPeer,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    // SAFETY: a non-null `out_len` is writable for the call.
    let out_len = unsafe { Out::new(out_len) };
    // SAFETY: a nonzero `out_cap` makes `out` writable for it; `holds` never
    // accepts bytes for a null `out`.
    let out = unsafe { OutSlice::new(out, out_cap) };
    status_of(Ok(None), || {
        // SAFETY: `peer` is null or live for the call.
        let peer = unsafe { peer_ref(peer) }?;
        let mut out = out?;
        let Some(mut out_len) = out_len? else {
            return Err(BridgeError::invalid("take_event requires out_len"));
        };
        if out_cap != 0 && out.is_null() {
            return Err(BridgeError::invalid("null event buffer with a capacity"));
        }
        let taken = peer.shared().events.take(|packet| {
            out_len.write(packet.len());
            out.holds(packet.len())
        });
        Ok(match taken {
            Taken::Item(packet) => {
                out.copy_from(&packet)?;
                Status::Ok
            }
            Taken::TooSmall => Status::BufferTooSmall,
            Taken::Empty => {
                out_len.write(0);
                Status::Again
            }
            Taken::Closed => {
                out_len.write(0);
                Status::Closed
            }
        })
    })
}

/// Nonblocking: copy the oldest decoded frame into caller memory and remove it.
///
/// The header is written for `OK` and for `BUFFER_TOO_SMALL`, which keeps the
/// frame queued; a null `bgra` asks for the header alone.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`].
/// A non-null `header` must be writable for one [`ReactorEffectVideoHeader`],
/// a non-null `bgra` for `bgra_cap` bytes and a non-null `metadata` for
/// `metadata_cap` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_take_video(
    peer: *mut ReactorEffectPeer,
    header: *mut ReactorEffectVideoHeader,
    bgra: *mut u8,
    bgra_cap: usize,
    metadata: *mut u8,
    metadata_cap: usize,
) -> i32 {
    // SAFETY: a non-null `header` is writable for the call.
    let header = unsafe { Out::new(header) };
    // SAFETY: a non-null `bgra` is writable for `bgra_cap` bytes.
    let bgra = unsafe { OutSlice::new(bgra, bgra_cap) };
    // SAFETY: a non-null `metadata` is writable for `metadata_cap` bytes.
    let metadata = unsafe { OutSlice::new(metadata, metadata_cap) };
    status_of(Ok(None), || {
        // SAFETY: `peer` is null or live for the call.
        let peer = unsafe { peer_ref(peer) }?;
        let (mut bgra, mut metadata) = (bgra?, metadata?);
        let Some(mut header) = header? else {
            return Err(BridgeError::invalid("take_video requires a header"));
        };
        let taken = peer.shared().video.take(|frame| {
            header.write(frame.header());
            !bgra.is_null() && bgra.holds(frame.bgra.len()) && metadata.holds(frame.metadata.len())
        });
        Ok(match taken {
            Taken::Item(frame) => {
                bgra.copy_from(&frame.bgra)?;
                metadata.copy_from(&frame.metadata)?;
                Status::Ok
            }
            Taken::TooSmall => Status::BufferTooSmall,
            Taken::Empty => Status::Again,
            Taken::Closed => Status::Closed,
        })
    })
}

/// Nonblocking: copy the oldest PCM block into caller memory and remove it.
///
/// The block is native-endian interleaved `int16_t`. The header is written
/// for `OK` and for `BUFFER_TOO_SMALL`; a null `pcm` asks for the header alone.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`].
/// A non-null `header` must be writable for one [`ReactorEffectAudioHeader`],
/// and a non-null `pcm` for `pcm_cap` samples.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_take_audio(
    peer: *mut ReactorEffectPeer,
    header: *mut ReactorEffectAudioHeader,
    pcm: *mut i16,
    pcm_cap: usize,
) -> i32 {
    // SAFETY: a non-null `header` is writable for the call.
    let header = unsafe { Out::new(header) };
    // SAFETY: a non-null `pcm` is writable for `pcm_cap` samples.
    let pcm = unsafe { OutSlice::new(pcm, pcm_cap) };
    status_of(Ok(None), || {
        // SAFETY: `peer` is null or live for the call.
        let peer = unsafe { peer_ref(peer) }?;
        let mut pcm = pcm?;
        let Some(mut header) = header? else {
            return Err(BridgeError::invalid("take_audio requires a header"));
        };
        let taken = peer.shared().audio.take(|block| {
            header.write(block.header());
            !pcm.is_null() && pcm.holds(block.pcm.len())
        });
        Ok(match taken {
            Taken::Item(block) => {
                pcm.copy_from(&block.pcm)?;
                Status::Ok
            }
            Taken::TooSmall => Status::BufferTooSmall,
            Taken::Empty => Status::Again,
            Taken::Closed => Status::Closed,
        })
    })
}

/// Fence callback and event admission at once. No callback or event is
/// admitted after it returns.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_close(peer: *mut ReactorEffectPeer) {
    // SAFETY: `peer` is null or live for the call.
    if let Ok(peer) = unsafe { peer_ref(peer) } {
        // Closing only stores flags and clears queues.
        discard_panic(|| peer.close());
    }
}

/// Join native ownership: close, then drop the libwebrtc objects on the owner
/// thread, wait for every admitted callback, and join the owner and notifier
/// threads. Safe to call more than once.
///
/// # Safety
/// `peer` must be null or a live handle from [`reactor_effect_peer_create`],
/// and a non-null `failure` must be writable for one [`ReactorEffectFailure`].
/// The notifier may be waiting for the host to run its callback, so never
/// call this from the thread that runs that callback.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_shutdown(
    peer: *mut ReactorEffectPeer,
    failure: *mut ReactorEffectFailure,
) -> i32 {
    // SAFETY: a non-null `failure` is writable for the call.
    let failure = unsafe { Out::new(failure) };
    status_of(failure, || {
        // SAFETY: `peer` is null or live for the call.
        unsafe { peer_ref(peer) }?.shutdown()?;
        Ok(Status::Ok)
    })
}

/// Shut a peer down if needed, so the same thread rule applies, then free its
/// handle.
///
/// # Safety
/// `peer` must be null or a handle from [`reactor_effect_peer_create`] not
/// passed here before. Every foreign call using the handle must have
/// returned, including calls still queued in a host FFI executor: joining the
/// native owner alone does not establish that.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reactor_effect_peer_destroy(peer: *mut ReactorEffectPeer) {
    // A misaligned pointer never came from `reactor_effect_peer_create`, and
    // this call has no status to refuse it with.
    if peer.is_null() || !peer.is_aligned() {
        return;
    }
    // SAFETY: `peer` came from `Box::into_raw` in `reactor_effect_peer_create`,
    // is passed here once, and no other call still uses it.
    let peer = unsafe { Box::from_raw(peer) };
    // Dropping shuts the peer down.
    discard_panic(move || drop(peer));
}

/// Run the body of an entry point that returns no status: a panic there has
/// nothing to report it and must not unwind into C.
fn discard_panic(body: impl FnOnce()) {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "the entry point has no status to report a caught panic with"
    )]
    let _ = catch_unwind(AssertUnwindSafe(body));
}

/// The status of an entry point's body. A failure's diagnostic goes to
/// `failure` when the caller passed one, and a caught panic is a native
/// failure. A misaligned `failure` fails the call before the body runs, since
/// it cannot hold its own diagnostic.
fn status_of(
    failure: Result<Option<Out<ReactorEffectFailure>>, BridgeError>,
    body: impl FnOnce() -> Result<Status, BridgeError>,
) -> i32 {
    let failure = match failure {
        Ok(failure) => failure,
        Err(misaligned) => return misaligned.class.status().code(),
    };
    let error = match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(status)) => return status.code(),
        Ok(Err(error)) => error,
        Err(_) => BridgeError::new(FailureClass::Native, "native bridge panicked"),
    };
    if let Some(mut failure) = failure {
        failure.write(ReactorEffectFailure::new(&error.message));
    }
    error.class.status().code()
}
