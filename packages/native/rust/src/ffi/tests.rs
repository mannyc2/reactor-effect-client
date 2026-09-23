//! The C ABI, exercised the way a host uses it: through raw pointers and
//! status codes.

use super::*;
use crate::abi::{Channel, Operation, Ready};
use crate::peer::{AudioItem, Shared, VideoItem};
use crate::protocol::Event;
use crate::sync::Push;
use crate::test_support::wait_until;
use serde_json::Value;
use std::mem::offset_of;
use std::panic::resume_unwind;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

/// A peer handle owned by a test and destroyed on drop, even when an
/// assertion fails first.
struct TestPeer(NonNull<ReactorEffectPeer>);

// SAFETY: a host may call a live handle's entry points from any thread, which
// the static assertion that `ReactorEffectPeer: Send + Sync` backs.
unsafe impl Send for TestPeer {}
// SAFETY: as for `Send`.
unsafe impl Sync for TestPeer {}

/// What `reactor_effect_peer_call` produced.
#[derive(Debug)]
struct Called {
    status: i32,
    response_len: usize,
    response: Vec<u8>,
    failure: String,
}

impl TestPeer {
    fn new() -> Self {
        Self::notifying(None)
    }

    fn notifying(notify: Option<ReactorEffectNotify>) -> Self {
        Self(NonNull::new(reactor_effect_peer_create(notify)).expect("a peer handle"))
    }

    fn handle(&self) -> *mut ReactorEffectPeer {
        self.0.as_ptr()
    }

    fn shared(&self) -> &Shared {
        // SAFETY: the handle stays live until this `TestPeer` drops.
        unsafe { self.0.as_ref() }.shared()
    }

    fn call(&self, operation: u32, request: &[u8]) -> Called {
        self.call_into(operation, request, CALL_BUFFER_MIN)
    }

    fn call_into(&self, operation: u32, request: &[u8], response_cap: usize) -> Called {
        let mut response = vec![0; response_cap];
        let mut response_len = usize::MAX;
        let mut failure = ReactorEffectFailure::new("");
        // SAFETY: the handle is live and every buffer is as long as passed.
        let status = unsafe {
            reactor_effect_peer_call(
                self.handle(),
                operation,
                request.as_ptr(),
                request.len(),
                response.as_mut_ptr(),
                response.len(),
                &raw mut response_len,
                &raw mut failure,
            )
        };
        response.truncate(response_len.min(response_cap));
        Called {
            status,
            response_len,
            response,
            failure: text(&failure).to_owned(),
        }
    }

    fn send(&self, channel: u32, data: &[u8]) -> (i32, String) {
        let mut failure = ReactorEffectFailure::new("");
        // SAFETY: the handle is live and `data` is readable for its length.
        let status = unsafe {
            reactor_effect_peer_send(
                self.handle(),
                channel,
                data.as_ptr(),
                data.len(),
                &raw mut failure,
            )
        };
        (status, text(&failure).to_owned())
    }

    /// Take an event into a buffer of `capacity` bytes: its status, the
    /// length it reported and the bytes it copied.
    fn take_event(&self, capacity: usize) -> (i32, usize, Vec<u8>) {
        let mut out = vec![0; capacity];
        let mut out_len = usize::MAX;
        let out_ptr = if capacity == 0 {
            ptr::null_mut()
        } else {
            out.as_mut_ptr()
        };
        // SAFETY: the handle is live and `out` is writable for `capacity`.
        let status = unsafe {
            reactor_effect_peer_take_event(self.handle(), out_ptr, capacity, &raw mut out_len)
        };
        out.truncate(out_len.min(capacity));
        (status, out_len, out)
    }

    fn close(&self) {
        // SAFETY: the handle is live.
        unsafe { reactor_effect_peer_close(self.handle()) };
    }

    fn shutdown(&self) -> (i32, String) {
        let mut failure = ReactorEffectFailure::new("");
        // SAFETY: the handle is live and `failure` is writable.
        let status = unsafe { reactor_effect_peer_shutdown(self.handle(), &raw mut failure) };
        (status, text(&failure).to_owned())
    }
}

impl Drop for TestPeer {
    fn drop(&mut self) {
        // SAFETY: the handle came from `reactor_effect_peer_create`, is
        // destroyed only here, and every call on it has returned.
        unsafe { reactor_effect_peer_destroy(self.handle()) };
    }
}

fn text(failure: &ReactorEffectFailure) -> &str {
    std::str::from_utf8(&failure.message[..failure.message_len as usize])
        .expect("failure text is UTF-8")
}

fn video_item(track: u32, fill: u8, metadata: &[u8]) -> VideoItem {
    VideoItem {
        track,
        width: 2,
        height: 1,
        frame_id: u64::MAX,
        timestamp_us: 9_007_199_254_740_993,
        sequence: u64::MAX - 1,
        bgra: vec![fill; 8],
        metadata: metadata.to_vec(),
    }
}

const OK: i32 = Status::Ok.code();
const AGAIN: i32 = Status::Again.code();
const BUFFER_TOO_SMALL: i32 = Status::BufferTooSmall.code();
const CLOSED: i32 = Status::Closed.code();
const INVALID_INPUT: i32 = Status::InvalidInput.code();
const OVERFLOW: i32 = Status::Overflow.code();

#[test]
fn c_structs_match_the_header_layout() {
    assert_eq!(size_of::<ReactorEffectVideoHeader>(), 48);
    assert_eq!(offset_of!(ReactorEffectVideoHeader, frame_id), 16);
    assert_eq!(offset_of!(ReactorEffectVideoHeader, timestamp_us), 24);
    assert_eq!(offset_of!(ReactorEffectVideoHeader, track), 32);
    assert_eq!(offset_of!(ReactorEffectVideoHeader, sequence), 40);
    assert_eq!(size_of::<ReactorEffectAudioHeader>(), 24);
    assert_eq!(offset_of!(ReactorEffectAudioHeader, track), 12);
    assert_eq!(offset_of!(ReactorEffectAudioHeader, sequence), 16);
    assert_eq!(size_of::<ReactorEffectFailure>(), 1024);
}

#[test]
fn the_build_identity_is_marked_json_for_this_abi() {
    // SAFETY: the export returns a pointer to a static C string.
    let exported = unsafe { CStr::from_ptr(reactor_effect_build_identity()) };
    assert_eq!(exported, BUILD_IDENTITY);
    assert_eq!(reactor_effect_abi_version(), ABI_VERSION);

    let identity = exported.to_str().expect("UTF-8");
    let json = identity
        .strip_prefix("reactor-effect-native:build-identity:")
        .and_then(|rest| rest.strip_suffix(":end"))
        .expect("the identity is inside its marker");
    let build: Value = serde_json::from_str(json).expect("the identity is JSON");
    assert_eq!(build["schemaVersion"], 1);
    assert_eq!(
        build["abiVersion"], ABI_VERSION,
        "build.rs must declare this ABI"
    );
    let source = build["sourceSha256"].as_str().unwrap();
    assert!(source.len() == 64 && source.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

#[test]
fn take_event_reports_the_size_then_copies_the_event_once() {
    let peer = TestPeer::new();
    let packet = Event::Message {
        channel: Channel::Control,
        bytes: b"abc",
    }
    .to_packet()
    .unwrap();
    assert_eq!(
        peer.shared().events.try_push(packet.clone()),
        Push::Accepted
    );

    assert_eq!(peer.take_event(0), (BUFFER_TOO_SMALL, packet.len(), vec![]));
    assert_eq!(
        peer.shared().events.counts().queued,
        1,
        "a short take keeps the event"
    );
    assert_eq!(
        peer.take_event(packet.len()),
        (OK, packet.len(), packet.clone())
    );
    assert_eq!(peer.take_event(packet.len()), (AGAIN, 0, vec![]));
}

#[test]
fn media_takes_copy_typed_frames_into_caller_buffers() {
    let peer = TestPeer::new();
    peer.shared().push_video(video_item(3, 0x21, b"meta"));
    let pcm = vec![1i16, -2, 300, -400];
    peer.shared().push_audio(AudioItem {
        track: 1,
        sample_rate: 48_000,
        channels: 2,
        sequence: 9,
        pcm: pcm.clone(),
    });

    let mut header = ReactorEffectVideoHeader::default();
    let mut bgra = vec![0u8; 8];
    let mut metadata = vec![0u8; 3];
    let mut take_video = |bgra: &mut [u8], metadata: &mut [u8]| {
        // SAFETY: the handle is live and each buffer is writable for its length.
        unsafe {
            reactor_effect_peer_take_video(
                peer.handle(),
                &raw mut header,
                bgra.as_mut_ptr(),
                bgra.len(),
                metadata.as_mut_ptr(),
                metadata.len(),
            )
        }
    };
    assert_eq!(take_video(&mut bgra, &mut metadata), BUFFER_TOO_SMALL);
    assert_eq!(
        peer.shared().video.counts().queued,
        1,
        "a short take keeps the frame"
    );
    metadata.resize(4, 0);
    assert_eq!(take_video(&mut bgra, &mut metadata), OK);
    assert_eq!(take_video(&mut bgra, &mut metadata), AGAIN);
    assert_eq!(header, video_item(3, 0x21, b"meta").header());
    assert_eq!(bgra, [0x21; 8]);
    assert_eq!(metadata, b"meta");

    let mut audio = ReactorEffectAudioHeader::default();
    let mut samples = vec![0i16; 3];
    let mut take_audio = |samples: &mut [i16]| {
        // SAFETY: the handle is live and `samples` is writable for its length.
        unsafe {
            reactor_effect_peer_take_audio(
                peer.handle(),
                &raw mut audio,
                samples.as_mut_ptr(),
                samples.len(),
            )
        }
    };
    assert_eq!(take_audio(&mut samples), BUFFER_TOO_SMALL);
    assert_eq!(
        peer.shared().audio.counts().queued,
        1,
        "a short take keeps the block"
    );
    samples.resize(4, 0);
    assert_eq!(take_audio(&mut samples), OK);
    assert_eq!(take_audio(&mut samples), AGAIN);
    assert_eq!(
        audio,
        ReactorEffectAudioHeader {
            sample_rate: 48_000,
            channels: 2,
            samples: 4,
            track: 1,
            sequence: 9,
        }
    );
    assert_eq!(samples, pcm);
}

#[test]
fn a_null_media_buffer_asks_for_the_header_alone() {
    let peer = TestPeer::new();
    peer.shared().push_video(video_item(0, 1, b""));
    let mut header = ReactorEffectVideoHeader::default();
    // SAFETY: the handle is live, `header` is writable and the buffers are null.
    let status = unsafe {
        reactor_effect_peer_take_video(
            peer.handle(),
            &raw mut header,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0,
        )
    };
    assert_eq!(status, BUFFER_TOO_SMALL);
    assert_eq!((header.data_len, header.metadata_len), (8, 0));
}

#[test]
fn closing_fences_every_take_and_host_call() {
    let peer = TestPeer::new();
    peer.shared().push_video(video_item(0, 1, b""));
    peer.close();

    let snapshot = peer.call(Operation::MediaSnapshot as u32, b"");
    assert_eq!(
        (snapshot.status, snapshot.failure.as_str()),
        (CLOSED, "native peer is closed")
    );
    assert_eq!(peer.send(Channel::Data as u32, b"late").0, CLOSED);
    assert_eq!(peer.take_event(0), (CLOSED, 0, vec![]));

    let mut header = ReactorEffectVideoHeader::default();
    let mut bgra = [0u8; 8];
    // SAFETY: the handle is live and each buffer is writable for its length.
    let video = unsafe {
        reactor_effect_peer_take_video(
            peer.handle(),
            &raw mut header,
            bgra.as_mut_ptr(),
            bgra.len(),
            ptr::null_mut(),
            0,
        )
    };
    assert_eq!(video, CLOSED, "closing discards queued media");
    let mut audio_header = ReactorEffectAudioHeader::default();
    let mut pcm = [0i16; 4];
    // SAFETY: the handle is live and each buffer is writable for its length.
    let audio = unsafe {
        reactor_effect_peer_take_audio(
            peer.handle(),
            &raw mut audio_header,
            pcm.as_mut_ptr(),
            pcm.len(),
        )
    };
    assert_eq!(audio, CLOSED);
    assert_eq!(peer.shutdown(), (OK, String::new()));
}

#[test]
fn readiness_reaches_the_host_on_the_notifier_thread() {
    static READY: AtomicU32 = AtomicU32::new(0);
    static THREADS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    extern "C" fn record(ready: u32) {
        let name = thread::current().name().unwrap_or_default().to_owned();
        THREADS.lock().unwrap().push(name);
        READY.fetch_or(ready, Ordering::AcqRel);
    }

    let peer = TestPeer::notifying(Some(record));
    peer.shared().emit(&Event::Ice { candidate: None });
    wait_until("event readiness", Duration::from_secs(5), || {
        READY.load(Ordering::Acquire) & Ready::Events.bit() != 0
    });
    peer.shared().push_video(video_item(0, 1, b""));
    wait_until("video readiness", Duration::from_secs(5), || {
        READY.load(Ordering::Acquire) & Ready::Video.bit() != 0
    });
    assert_eq!(peer.shutdown(), (OK, String::new()));
    assert_eq!(
        peer.shutdown(),
        (OK, String::new()),
        "shutdown is idempotent"
    );
    let threads = THREADS.lock().unwrap();
    assert!(
        threads.iter().all(|name| name == "reactor-effect-notify"),
        "only the notifier thread calls the host: {threads:?}"
    );
}

#[test]
fn calls_report_their_failure_class_and_diagnostic() {
    let peer = TestPeer::new();

    let (status, failure) = peer.send(Channel::Data as u32, b"early");
    assert_eq!(
        (status, failure.as_str()),
        (Status::ChannelClosed.code(), "data channel is not open")
    );
    // SAFETY: the handle is live; a null failure pointer is allowed.
    let unknown_channel =
        unsafe { reactor_effect_peer_send(peer.handle(), 7, ptr::null(), 0, ptr::null_mut()) };
    assert_eq!(unknown_channel, INVALID_INPUT);

    let unknown = peer.call(99, b"");
    assert_eq!(
        (
            unknown.status,
            unknown.response_len,
            unknown.failure.as_str()
        ),
        (INVALID_INPUT, 0, "unknown native call operation")
    );

    let oversized = peer.call(
        Operation::Prepare as u32,
        &vec![b' '; MAX_REQUEST_BYTES + 1],
    );
    assert_eq!(oversized.status, Status::Overflow.code());

    let short = peer.call_into(Operation::MediaSnapshot as u32, b"", 16);
    assert_eq!(
        (short.status, short.response_len),
        (BUFFER_TOO_SMALL, CALL_BUFFER_MIN)
    );

    let snapshot = peer.call(Operation::MediaSnapshot as u32, b"");
    assert_eq!(snapshot.status, OK);
    assert_eq!(snapshot.response_len, snapshot.response.len());
    let snapshot: Value = serde_json::from_slice(&snapshot.response).expect("snapshot JSON");
    assert_eq!(snapshot["closed"], false);
    assert_eq!(snapshot["droppedVideo"], "0");

    let bad_answer = peer.call(Operation::Answer as u32, b"");
    assert_eq!(
        (bad_answer.status, bad_answer.failure.as_str()),
        (INVALID_INPUT, "answer SDP is empty")
    );
}

#[test]
fn a_null_handle_or_required_pointer_is_invalid_input() {
    let null = ptr::null_mut();
    let mut response = vec![0u8; CALL_BUFFER_MIN];
    let mut len = 0usize;
    let mut failure = ReactorEffectFailure::new("");
    let mut video = ReactorEffectVideoHeader::default();
    let mut audio = ReactorEffectAudioHeader::default();

    // SAFETY: a null handle is rejected before any other argument is used.
    let call = unsafe {
        reactor_effect_peer_call(
            null,
            Operation::MediaSnapshot as u32,
            ptr::null(),
            0,
            response.as_mut_ptr(),
            response.len(),
            &raw mut len,
            &raw mut failure,
        )
    };
    assert_eq!(
        (call, text(&failure)),
        (INVALID_INPUT, "null native peer handle")
    );
    // SAFETY: as above.
    let send = unsafe { reactor_effect_peer_send(null, 0, ptr::null(), 0, ptr::null_mut()) };
    // SAFETY: as above.
    let event = unsafe { reactor_effect_peer_take_event(null, ptr::null_mut(), 0, &raw mut len) };
    // SAFETY: as above.
    let take_video = unsafe {
        reactor_effect_peer_take_video(null, &raw mut video, ptr::null_mut(), 0, ptr::null_mut(), 0)
    };
    // SAFETY: as above.
    let take_audio =
        unsafe { reactor_effect_peer_take_audio(null, &raw mut audio, ptr::null_mut(), 0) };
    // SAFETY: as above.
    let shutdown = unsafe { reactor_effect_peer_shutdown(null, ptr::null_mut()) };
    assert_eq!(
        [send, event, take_video, take_audio, shutdown],
        [INVALID_INPUT; 5]
    );
    // SAFETY: closing or destroying a null handle does nothing.
    unsafe { reactor_effect_peer_close(null) };
    // SAFETY: as above.
    unsafe { reactor_effect_peer_destroy(null) };

    let peer = TestPeer::new();
    // SAFETY: the handle is live and the buffers are writable; the null
    // response length is rejected.
    let no_length = unsafe {
        reactor_effect_peer_call(
            peer.handle(),
            Operation::MediaSnapshot as u32,
            ptr::null(),
            0,
            response.as_mut_ptr(),
            response.len(),
            ptr::null_mut(),
            &raw mut failure,
        )
    };
    assert_eq!(
        (no_length, text(&failure)),
        (INVALID_INPUT, "call requires a response buffer")
    );
    // SAFETY: the handle is live; the null request with a length is rejected.
    let no_request = unsafe {
        reactor_effect_peer_call(
            peer.handle(),
            Operation::Prepare as u32,
            ptr::null(),
            8,
            response.as_mut_ptr(),
            response.len(),
            &raw mut len,
            &raw mut failure,
        )
    };
    assert_eq!(
        (no_request, text(&failure)),
        (INVALID_INPUT, "null input with a nonzero length")
    );
    // SAFETY: the handle is live; the null data with a length is rejected.
    let no_data =
        unsafe { reactor_effect_peer_send(peer.handle(), 0, ptr::null(), 4, ptr::null_mut()) };
    // SAFETY: the handle is live; the null length pointer is rejected.
    let no_out_len = unsafe {
        reactor_effect_peer_take_event(peer.handle(), ptr::null_mut(), 0, ptr::null_mut())
    };
    // SAFETY: the handle is live; a null buffer with a capacity is rejected.
    let no_out =
        unsafe { reactor_effect_peer_take_event(peer.handle(), ptr::null_mut(), 8, &raw mut len) };
    // SAFETY: the handle is live; the null header is rejected.
    let no_video_header = unsafe {
        reactor_effect_peer_take_video(
            peer.handle(),
            ptr::null_mut(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0,
        )
    };
    // SAFETY: the handle is live; the null header is rejected.
    let no_audio_header = unsafe {
        reactor_effect_peer_take_audio(peer.handle(), ptr::null_mut(), ptr::null_mut(), 0)
    };
    assert_eq!(
        [
            no_data,
            no_out_len,
            no_out,
            no_video_header,
            no_audio_header
        ],
        [INVALID_INPUT; 5]
    );
}

#[test]
fn a_length_over_its_bound_is_refused_before_caller_memory_is_read() {
    let peer = TestPeer::new();
    // Nothing is readable at a dangling address, so only the bound checks
    // keep these calls from reading it.
    let nowhere = NonNull::<u8>::dangling().as_ptr();
    let mut response = vec![0u8; CALL_BUFFER_MIN];
    let mut len = 0usize;
    let mut failure = ReactorEffectFailure::new("");

    // SAFETY: the handle is live and the buffers are writable; a request
    // length over its bound is refused before `request` is read.
    let call = unsafe {
        reactor_effect_peer_call(
            peer.handle(),
            Operation::Prepare as u32,
            nowhere,
            MAX_REQUEST_BYTES + 1,
            response.as_mut_ptr(),
            response.len(),
            &raw mut len,
            &raw mut failure,
        )
    };
    assert_eq!(call, OVERFLOW);
    // SAFETY: the handle is live; a message length over its bound is refused
    // before `data` is read.
    let send = unsafe {
        reactor_effect_peer_send(
            peer.handle(),
            Channel::Data as u32,
            nowhere,
            MAX_MESSAGE_BYTES + 1,
            &raw mut failure,
        )
    };
    assert_eq!(
        (send, text(&failure)),
        (
            OVERFLOW,
            "input of 262145 bytes exceeds its 262144-byte bound"
        )
    );
}

/// A pointer one byte into `words`, misaligned for any type wider than a byte.
fn misaligned<T>(words: &mut [u64]) -> *mut T {
    words.as_mut_ptr().cast::<u8>().wrapping_add(1).cast()
}

#[test]
fn a_misaligned_pointer_is_invalid_input_and_nothing_is_written() {
    let peer = TestPeer::new();
    let mut words = [0u64; 8];
    let mut response = vec![0u8; CALL_BUFFER_MIN];
    let mut len = usize::MAX;
    let mut audio_header = ReactorEffectAudioHeader::default();

    // SAFETY: the handle is live and the buffers are writable; the misaligned
    // failure pointer fails the call before its body runs.
    let call = unsafe {
        reactor_effect_peer_call(
            peer.handle(),
            Operation::MediaSnapshot as u32,
            ptr::null(),
            0,
            response.as_mut_ptr(),
            response.len(),
            &raw mut len,
            misaligned(&mut words),
        )
    };
    assert_eq!((call, len), (INVALID_INPUT, usize::MAX), "the call ran");
    // SAFETY: the handle is live; the misaligned response length is refused.
    let response_len = unsafe {
        reactor_effect_peer_call(
            peer.handle(),
            Operation::MediaSnapshot as u32,
            ptr::null(),
            0,
            response.as_mut_ptr(),
            response.len(),
            misaligned(&mut words),
            ptr::null_mut(),
        )
    };
    // SAFETY: the handle is live; the misaligned length pointer is refused.
    let event = unsafe {
        reactor_effect_peer_take_event(peer.handle(), ptr::null_mut(), 0, misaligned(&mut words))
    };
    // SAFETY: the handle is live; the misaligned header is refused.
    let video = unsafe {
        reactor_effect_peer_take_video(
            peer.handle(),
            misaligned(&mut words),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0,
        )
    };
    // SAFETY: the handle is live; the misaligned sample buffer is refused.
    let audio = unsafe {
        reactor_effect_peer_take_audio(
            peer.handle(),
            &raw mut audio_header,
            misaligned(&mut words),
            4,
        )
    };
    // SAFETY: a misaligned handle is refused before it is dereferenced.
    let send = unsafe {
        reactor_effect_peer_send(misaligned(&mut words), 0, ptr::null(), 0, ptr::null_mut())
    };
    // SAFETY: as above.
    let shutdown = unsafe { reactor_effect_peer_shutdown(misaligned(&mut words), ptr::null_mut()) };
    assert_eq!(
        [response_len, event, video, audio, send, shutdown],
        [INVALID_INPUT; 6]
    );
    // SAFETY: closing or destroying a misaligned handle does nothing.
    unsafe { reactor_effect_peer_close(misaligned(&mut words)) };
    // SAFETY: as above.
    unsafe { reactor_effect_peer_destroy(misaligned(&mut words)) };
    assert_eq!(words, [0; 8], "a misaligned pointer was written");
}

#[test]
fn a_panic_is_a_native_failure_not_an_unwind_into_c() {
    let mut failure = ReactorEffectFailure::new("");
    // SAFETY: `failure` is writable for the call.
    let out = unsafe { Out::new(&raw mut failure) };
    // `resume_unwind` panics without running the panic hook's report.
    let status = status_of(out, || resume_unwind(Box::new("boom")));
    assert_eq!(status, Status::Native.code());
    assert_eq!(text(&failure), "native bridge panicked");
}

#[test]
fn failure_text_is_truncated_on_a_character_boundary() {
    let long = "é".repeat(FAILURE_MESSAGE_BYTES);
    let failure = ReactorEffectFailure::new(&long);
    assert_eq!(failure.message_len as usize, FAILURE_MESSAGE_BYTES);
    assert_eq!(text(&failure), "é".repeat(FAILURE_MESSAGE_BYTES / 2));

    assert_eq!(truncate("aéb", 2), "a", "a split character is dropped");
    assert_eq!(truncate("aéb", 3), "aé");
    assert_eq!(truncate("short", 64), "short");
    assert_eq!(truncate("", 0), "");
}
