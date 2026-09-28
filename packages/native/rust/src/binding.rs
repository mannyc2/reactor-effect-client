//! The addon's Node-API surface, which `napi build` also writes out as the
//! TypeScript declarations the host package loads it with.
//!
//! Every value here is created on the JavaScript thread: queues hold plain
//! Rust values, and a take converts one item when the host asks for it. A call
//! that runs on the owner thread answers through a promise, settled from that
//! thread without waiting, and a failure is a [`Reply`] naming its
//! [`FailureClass`], never text for the host to match.

#![expect(
    clippy::missing_errors_doc,
    reason = "a method fails only when Node-API cannot create its promise or wake, which the \
              host treats as a defect; call failures are Replies"
)]

use crate::error::{self, BridgeError};
use crate::peer::{Done, Peer};
use crate::protocol::{self, BitrateRequest, Event, IceServerSpec, PrepareRequest, TrackSpec};
use napi::bindgen_prelude::{BigInt, Function, Int16Array, Object, Uint8Array};
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi::{Env, JsDeferred, Status};
use napi_derive::napi;
use serde_json::Value;
use std::sync::Arc;
use std::thread;

/// The build identity JSON: the source hash, target, profile and toolchain
/// the addon was built from, which staging checks against the checkout.
#[napi]
#[must_use]
pub fn build_identity() -> String {
    env!("REACTOR_EFFECT_BUILD_IDENTITY").to_owned()
}

/// The media a track carries.
#[napi(string_enum = "lowercase")]
pub enum TrackKind {
    /// Video.
    Video,
    /// Audio.
    Audio,
}

/// Which way a track's media flows, seen from the addon.
#[napi(string_enum = "lowercase")]
pub enum Direction {
    /// The remote peer sends and the addon decodes.
    RecvOnly,
    /// The addon sends.
    SendOnly,
}

/// A data channel the addon owns.
#[napi(string_enum = "lowercase")]
pub enum Channel {
    /// Reactor's control channel.
    Control,
    /// Reactor's data channel.
    Data,
}

/// What kind of failure a call or the connection met. The host maps each class
/// to its own error reason.
#[napi(string_enum)]
pub enum FailureClass {
    /// The peer is closed or shut down.
    Closed,
    /// An argument or request was rejected.
    InvalidInput,
    /// libwebrtc or the addon failed in a way it cannot classify.
    Native,
    /// A queue, buffer or message bound was exceeded.
    Overflow,
    /// The remote peer broke the negotiated contract.
    Protocol,
    /// libwebrtc refused to create or apply an SDP.
    SdpRejected,
    /// The data channel is not open.
    ChannelClosed,
}

/// A STUN or TURN server.
#[napi(object)]
pub struct IceServer {
    /// The server's URLs.
    pub urls: Vec<String>,
    /// The TURN username, if any.
    pub username: Option<String>,
    /// The TURN credential, if any.
    pub credential: Option<String>,
}

/// A track to negotiate.
#[napi(object)]
pub struct Track {
    /// The track's name, unique in its request.
    pub name: String,
    /// Its media.
    pub kind: TrackKind,
    /// Its direction.
    pub direction: Direction,
}

/// A declared track and the MID libwebrtc gave its transceiver.
#[napi(object)]
pub struct Mapping {
    /// The declared name.
    pub name: String,
    /// Its media.
    pub kind: TrackKind,
    /// Its direction.
    pub direction: Direction,
    /// Its media section.
    pub mid: String,
}

/// A local ICE candidate, as the host forwards it to signaling.
#[napi(object)]
pub struct Candidate {
    /// The candidate line.
    pub candidate: String,
    /// Its media section, if known.
    pub sdp_mid: Option<String>,
    /// Its m-line index, if known.
    #[napi(js_name = "sdpMLineIndex")]
    pub sdp_mline_index: Option<u32>,
}

/// A classified failure.
#[napi(object)]
pub struct Failure {
    /// What kind of failure it is.
    pub class: FailureClass,
    /// Diagnostic text. It can quote libwebrtc and so peer SDP: the host keeps
    /// it private and never matches on it.
    pub message: String,
}

/// The local offer and each declared track's mapping.
#[napi(object)]
pub struct Prepared {
    /// The offer.
    pub sdp: String,
    /// The mapping of each declared track, in request order.
    pub mapping: Vec<Mapping>,
}

/// A call's answer: its failure, or the value a call returns.
#[napi(object)]
pub struct Reply {
    /// Present when the call failed.
    pub failure: Option<Failure>,
    /// `prepare`'s offer and mapping.
    pub prepared: Option<Prepared>,
    /// `stats`: an `RTCStatsReport`-shaped array; counters beyond double
    /// precision are decimal strings.
    #[napi(ts_type = "Array<Record<string, unknown>>")]
    pub stats: Option<Value>,
}

impl Reply {
    fn ok() -> Self {
        Self {
            failure: None,
            prepared: None,
            stats: None,
        }
    }

    fn failed(error: BridgeError) -> Self {
        Self {
            failure: Some(Failure {
                class: error.class.into(),
                message: error.message,
            }),
            ..Self::ok()
        }
    }
}

/// A transport event, in the order it happened.
#[napi(discriminant = "type", discriminant_case = "lowercase")]
pub enum PeerEvent {
    /// The aggregate connection state changed.
    State {
        /// `new`, `connecting`, `connected`, `disconnected`, `failed` or `closed`.
        state: String,
    },
    /// A local ICE candidate, or with none, the end of gathering.
    Ice {
        /// The candidate.
        candidate: Option<Candidate>,
    },
    /// A data channel opened or closed.
    Channel {
        /// Which channel.
        channel: Channel,
        /// Whether it is now open.
        open: bool,
    },
    /// A binary message arrived on a data channel.
    Message {
        /// Which channel.
        channel: Channel,
        /// The message, in an `ArrayBuffer` of its own.
        bytes: Uint8Array,
    },
    /// A remote track arrived for a declared receive track.
    Track {
        /// The declared name.
        name: String,
        /// Its media section.
        mid: String,
    },
    /// That track's decoded media now reaches its queue.
    Decoded {
        /// Its media.
        kind: TrackKind,
        /// The declared name.
        name: String,
        /// Its media section.
        mid: String,
    },
    /// The connection failed.
    Error {
        /// Why.
        failure: Failure,
    },
}

/// One decoded BGRA frame.
#[napi(object)]
pub struct Video {
    /// The track's index in the prepare request.
    pub track: u32,
    /// Width in pixels.
    pub width: u32,
    /// Height in pixels.
    pub height: u32,
    /// The sender's frame id; 0 when it supplied none.
    pub frame_id: BigInt,
    /// The sender's capture time in microseconds; 0 when absent.
    pub timestamp_us: BigInt,
    /// The frame's admission sequence on its track: a gap counts drops.
    pub sequence: BigInt,
    /// `width * height * 4` bytes, in an `ArrayBuffer` of their own.
    pub data: Uint8Array,
    /// The sender's metadata, in an `ArrayBuffer` of its own.
    pub metadata: Uint8Array,
}

/// One block of decoded, interleaved PCM.
#[napi(object)]
pub struct Audio {
    /// The track's index in the prepare request.
    pub track: u32,
    /// Samples per second.
    pub sample_rate: u32,
    /// Interleaved channels.
    pub channels: u32,
    /// The block's admission sequence on its track: a gap counts drops.
    pub sequence: BigInt,
    /// Signed 16-bit samples, in an `ArrayBuffer` of their own.
    pub samples: Int16Array,
}

/// What each queue dropped, delivered and still holds.
#[napi(object)]
pub struct Pressure {
    /// Whether the peer is closed.
    pub closed: bool,
    /// Events waiting.
    pub queued_control: u32,
    /// Frames waiting.
    pub queued_video: u32,
    /// Audio blocks waiting.
    pub queued_audio: u32,
    /// Bytes waiting across the queues.
    pub queued_bytes: u32,
    /// Frames the video queue evicted.
    pub dropped_video: BigInt,
    /// Blocks the audio queue evicted.
    pub dropped_audio: BigInt,
    /// Frames the host took.
    pub delivered_video: BigInt,
    /// Blocks the host took.
    pub delivered_audio: BigInt,
}

/// A native WebRTC peer. Every call that runs on its owner thread returns a
/// promise of a [`Reply`]; takes, `pressure` and `close` answer at once.
#[napi]
pub struct NativePeer {
    peer: Arc<Peer>,
}

/// How a promise is settled: on the JavaScript thread, with the reply the
/// owner thread produced.
type Settle = Box<dyn FnOnce(Env) -> napi::Result<Reply> + Send>;

/// A promise and the completion that settles it with `answer`'s reply.
fn promise<T: 'static>(
    env: &Env,
    answer: impl FnOnce(T) -> Reply + Send + 'static,
) -> napi::Result<(Done<T>, Object<'_>)> {
    let (deferred, promise): (JsDeferred<Reply, Settle>, _) = env.create_deferred()?;
    let done: Done<T> = Box::new(move |result| {
        let reply = result.map_or_else(Reply::failed, answer);
        deferred.resolve(Box::new(move |_env| Ok(reply)));
    });
    Ok((done, promise))
}

#[napi]
impl NativePeer {
    /// Start a peer. `ready` runs on the JavaScript thread with the bits of
    /// the queues that have items (events 1, video 2, audio 4); it is only a
    /// wake, and the host takes the items itself.
    #[napi(constructor)]
    pub fn new(ready: Function<'_, u32, ()>) -> napi::Result<Self> {
        let peer = Arc::new(Peer::create().map_err(to_napi)?);
        let taken = Arc::clone(&peer);
        let wake = ready
            .build_threadsafe_function::<()>()
            .callee_handled::<false>()
            .build_callback(move |_context| Ok(taken.take_ready()))?;
        peer.set_wake(Box::new(move || {
            // Non-blocking: a libwebrtc callback never waits for JavaScript.
            // A refused call means the addon is shutting down.
            let _status: Status = wake.call((), ThreadsafeFunctionCallMode::NonBlocking);
        }));
        Ok(Self { peer })
    }

    /// Create the connection and its local offer.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn prepare<'env>(
        &self,
        env: &'env Env,
        servers: Vec<IceServer>,
        tracks: Vec<Track>,
    ) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |prepared: crate::peer::Prepared| Reply {
            prepared: Some(Prepared {
                sdp: prepared.sdp,
                mapping: prepared.mapping.into_iter().map(Mapping::from).collect(),
            }),
            ..Reply::ok()
        })?;
        let servers = servers.into_iter().map(IceServerSpec::from).collect();
        let tracks = tracks.into_iter().map(TrackSpec::from).collect();
        match PrepareRequest::new(servers, tracks) {
            Ok(request) => self.peer.prepare(request, done),
            Err(error) => done(Err(error)),
        }
        Ok(promise)
    }

    /// Apply the remote answer.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn answer<'env>(&self, env: &'env Env, sdp: String) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |()| Reply::ok())?;
        self.peer.answer(sdp, done);
        Ok(promise)
    }

    /// Pause a declared track, or resume it in its declared direction.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn direction<'env>(
        &self,
        env: &'env Env,
        name: String,
        active: bool,
    ) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |()| Reply::ok())?;
        self.peer.direction(name, active, done);
        Ok(promise)
    }

    /// Cap an outgoing track's send bitrate.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn max_bitrate<'env>(
        &self,
        env: &'env Env,
        name: String,
        bits_per_second: u32,
    ) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |()| Reply::ok())?;
        match BitrateRequest::new(name, bits_per_second) {
            Ok(request) => self.peer.max_bitrate(request, done),
            Err(error) => done(Err(error)),
        }
        Ok(promise)
    }

    /// Read WebRTC statistics.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn stats<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |stats| Reply {
            stats: Some(stats),
            ..Reply::ok()
        })?;
        self.peer.stats(done);
        Ok(promise)
    }

    /// Send one binary message. The bytes are copied before this returns, so
    /// later changes to them never reach the peer.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn send<'env>(
        &self,
        env: &'env Env,
        channel: Channel,
        bytes: Uint8Array,
    ) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |()| Reply::ok())?;
        let copied = bytes.to_vec();
        // The copy is what the owner thread sends; the JavaScript array is
        // released now rather than when this call returns.
        drop(bytes);
        self.peer.send(channel.into(), copied, done);
        Ok(promise)
    }

    /// The next transport event, if one is queued.
    #[napi]
    #[must_use]
    pub fn take_event(&self) -> Option<PeerEvent> {
        self.peer.take_event().map(PeerEvent::from)
    }

    /// The next decoded frame, if one is queued.
    #[napi]
    #[must_use]
    pub fn take_video(&self) -> Option<Video> {
        self.peer.take_video().map(|frame| Video {
            track: frame.track,
            width: frame.width,
            height: frame.height,
            frame_id: frame.frame_id.into(),
            timestamp_us: frame.timestamp_us.into(),
            sequence: frame.sequence.into(),
            data: Uint8Array::new(frame.bgra),
            metadata: Uint8Array::new(frame.metadata),
        })
    }

    /// The next decoded audio block, if one is queued.
    #[napi]
    #[must_use]
    pub fn take_audio(&self) -> Option<Audio> {
        self.peer.take_audio().map(|block| Audio {
            track: block.track,
            sample_rate: block.sample_rate,
            channels: block.channels,
            sequence: block.sequence.into(),
            samples: Int16Array::new(block.pcm),
        })
    }

    /// What each queue dropped, delivered and still holds.
    #[napi]
    #[must_use]
    pub fn pressure(&self) -> Pressure {
        let snapshot = self.peer.snapshot();
        let count = |value: usize| u32::try_from(value).unwrap_or(u32::MAX);
        Pressure {
            closed: snapshot.closed,
            queued_control: count(snapshot.queued_control),
            queued_video: count(snapshot.queued_video),
            queued_audio: count(snapshot.queued_audio),
            queued_bytes: count(snapshot.queued_bytes),
            dropped_video: snapshot.dropped_video.into(),
            dropped_audio: snapshot.dropped_audio.into(),
            delivered_video: snapshot.delivered_video.into(),
            delivered_audio: snapshot.delivered_audio.into(),
        }
    }

    /// Fence callback admission and calls at once, and discard what is queued.
    #[napi]
    pub fn close(&self) {
        self.peer.close();
    }

    /// Close, then join the owner thread and every admitted callback, on a
    /// thread of its own. The promise settles when the join completes.
    #[napi(ts_return_type = "Promise<Reply>")]
    pub fn shutdown<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let (done, promise) = promise(env, |()| Reply::ok())?;
        let peer = Arc::clone(&self.peer);
        let joiner = thread::Builder::new()
            .name("reactor-effect-native-join".into())
            .spawn(move || done(peer.shutdown()));
        if let Err(error) = joiner {
            return Err(napi::Error::from_reason(format!(
                "could not start the native join thread: {error}"
            )));
        }
        Ok(promise)
    }
}

fn to_napi(error: BridgeError) -> napi::Error {
    napi::Error::new(Status::GenericFailure, error.message)
}

impl From<error::FailureClass> for FailureClass {
    fn from(class: error::FailureClass) -> Self {
        match class {
            error::FailureClass::Closed => Self::Closed,
            error::FailureClass::InvalidInput => Self::InvalidInput,
            error::FailureClass::Native => Self::Native,
            error::FailureClass::Overflow => Self::Overflow,
            error::FailureClass::Protocol => Self::Protocol,
            error::FailureClass::SdpRejected => Self::SdpRejected,
            error::FailureClass::ChannelClosed => Self::ChannelClosed,
        }
    }
}

impl From<TrackKind> for protocol::TrackKind {
    fn from(kind: TrackKind) -> Self {
        match kind {
            TrackKind::Video => Self::Video,
            TrackKind::Audio => Self::Audio,
        }
    }
}

impl From<protocol::TrackKind> for TrackKind {
    fn from(kind: protocol::TrackKind) -> Self {
        match kind {
            protocol::TrackKind::Video => Self::Video,
            protocol::TrackKind::Audio => Self::Audio,
        }
    }
}

impl From<Direction> for protocol::Direction {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::RecvOnly => Self::RecvOnly,
            Direction::SendOnly => Self::SendOnly,
        }
    }
}

impl From<protocol::Direction> for Direction {
    fn from(direction: protocol::Direction) -> Self {
        match direction {
            protocol::Direction::RecvOnly => Self::RecvOnly,
            protocol::Direction::SendOnly => Self::SendOnly,
        }
    }
}

impl From<Channel> for protocol::Channel {
    fn from(channel: Channel) -> Self {
        match channel {
            Channel::Control => Self::Control,
            Channel::Data => Self::Data,
        }
    }
}

impl From<protocol::Channel> for Channel {
    fn from(channel: protocol::Channel) -> Self {
        match channel {
            protocol::Channel::Control => Self::Control,
            protocol::Channel::Data => Self::Data,
        }
    }
}

impl From<IceServer> for IceServerSpec {
    fn from(server: IceServer) -> Self {
        Self {
            urls: server.urls,
            username: server.username.unwrap_or_default(),
            credential: server.credential.unwrap_or_default(),
        }
    }
}

impl From<Track> for TrackSpec {
    fn from(track: Track) -> Self {
        Self {
            name: track.name,
            kind: track.kind.into(),
            direction: track.direction.into(),
        }
    }
}

impl From<protocol::Mapping> for Mapping {
    fn from(mapping: protocol::Mapping) -> Self {
        Self {
            name: mapping.name,
            kind: mapping.kind.into(),
            direction: mapping.direction.into(),
            mid: mapping.mid,
        }
    }
}

impl From<Event> for PeerEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::State { state } => Self::State {
                state: state.to_owned(),
            },
            Event::Ice { candidate } => Self::Ice {
                candidate: candidate.map(|candidate| Candidate {
                    candidate: candidate.candidate,
                    sdp_mid: candidate.sdp_mid,
                    sdp_mline_index: candidate.sdp_mline_index.map(u32::from),
                }),
            },
            Event::Channel { channel, open } => Self::Channel {
                channel: channel.into(),
                open,
            },
            Event::Message { channel, bytes } => Self::Message {
                channel: channel.into(),
                bytes: Uint8Array::new(bytes),
            },
            Event::Track { name, mid } => Self::Track { name, mid },
            Event::Decoded { kind, name, mid } => Self::Decoded {
                kind: kind.into(),
                name,
                mid,
            },
            Event::Error { class, message } => Self::Error {
                failure: Failure {
                    class: class.into(),
                    message,
                },
            },
        }
    }
}
