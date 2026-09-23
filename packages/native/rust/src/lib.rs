use reactor_webrtc::{
    DataChannel, DataChannelState, IceCandidateType, IceGatheringState, IceServer, MediaKind,
    PeerConnection, PeerConnectionFactory, PeerConnectionObserver, PeerConnectionState,
    RelayProtocol, RemoteTrack, RtcConfiguration, SdpType, SessionDescription, StatsReport,
    Transceiver, TransceiverDirection,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

const ABI_VERSION: u32 = 3;

// Non-negative statuses are outcomes. Negative statuses are failure classes,
// closed for this ABI; the host maps each one to its own error type.
const STATUS_OK: i32 = 0;
const STATUS_AGAIN: i32 = 1;
const STATUS_BUFFER_TOO_SMALL: i32 = 2;
const STATUS_CLOSED: i32 = 3;
const STATUS_INVALID_INPUT: i32 = -1;
const STATUS_NATIVE: i32 = -2;
const STATUS_OVERFLOW: i32 = -3;
const STATUS_PROTOCOL: i32 = -4;
const STATUS_SDP_REJECTED: i32 = -5;
const STATUS_CHANNEL_CLOSED: i32 = -6;

const CALL_PREPARE: u32 = 1;
const CALL_ANSWER: u32 = 2;
const CALL_DIRECTION: u32 = 3;
const CALL_MAX_BITRATE: u32 = 4;
const CALL_STATS: u32 = 5;
const CALL_MEDIA_SNAPSHOT: u32 = 6;

const CHANNEL_CONTROL: u32 = 0;
const CHANNEL_DATA: u32 = 1;

const READY_EVENTS: u32 = 1;
const READY_VIDEO: u32 = 2;
const READY_AUDIO: u32 = 4;

const CALL_BUFFER_MIN: usize = 4 * 1024 * 1024;
const FAILURE_MESSAGE_BYTES: usize = 1020;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 262_144;
const MAX_BUFFERED_SEND_BYTES: u64 = 1_048_576;

/// The failure class of a bridge error, and the status the C ABI reports for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Closed,
    InvalidInput,
    Native,
    Overflow,
    Protocol,
    SdpRejected,
    ChannelClosed,
}

impl Class {
    fn status(self) -> i32 {
        match self {
            Class::Closed => STATUS_CLOSED,
            Class::InvalidInput => STATUS_INVALID_INPUT,
            Class::Native => STATUS_NATIVE,
            Class::Overflow => STATUS_OVERFLOW,
            Class::Protocol => STATUS_PROTOCOL,
            Class::SdpRejected => STATUS_SDP_REJECTED,
            Class::ChannelClosed => STATUS_CHANNEL_CLOSED,
        }
    }
}

#[derive(Debug, Clone)]
struct BridgeError {
    class: Class,
    message: String,
}

impl BridgeError {
    fn new(class: Class, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(Class::InvalidInput, message)
    }

    fn closed() -> Self {
        Self::new(Class::Closed, "native peer is closed")
    }
}

/// reactor-webrtc reports every libwebrtc failure as an untyped string, so the
/// class comes from the operation that failed rather than from the error.
fn webrtc(
    class: Class,
    operation: &'static str,
) -> impl FnOnce(reactor_webrtc::Error) -> BridgeError {
    move |error| BridgeError::new(class, format!("{operation}: {error}"))
}

#[derive(Default)]
struct CallbackGate {
    accepting: AtomicBool,
    active: Mutex<usize>,
    zero: Condvar,
}

impl CallbackGate {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            active: Mutex::new(0),
            zero: Condvar::new(),
        }
    }

    fn accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    fn enter(self: &Arc<Self>) -> Option<CallbackGuard> {
        if !self.accepting() {
            return None;
        }
        let mut active = lock(&self.active);
        if !self.accepting() {
            return None;
        }
        *active += 1;
        drop(active);
        Some(CallbackGuard {
            gate: Arc::clone(self),
        })
    }

    fn close(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn wait_zero(&self) {
        let mut active = lock(&self.active);
        while *active != 0 {
            active = self.zero.wait(active).unwrap_or_else(|p| p.into_inner());
        }
    }
}

struct CallbackGuard {
    gate: Arc<CallbackGate>,
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        let mut active = lock(&self.gate.active);
        *active -= 1;
        if *active == 0 {
            self.gate.zero.notify_all();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Debug, PartialEq, Eq)]
enum Push {
    Accepted,
    Closed,
    Overflow,
}

enum Taken<T> {
    Item(T),
    TooSmall,
    Empty,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counts {
    dropped: u64,
    taken: u64,
    queued: usize,
    bytes: usize,
}

struct QueueState<T> {
    items: VecDeque<(T, usize)>,
    bytes: usize,
    closed: bool,
}

/// A bounded FIFO between libwebrtc producers and one host reader. Every item
/// is observed exactly once and then either taken, dropped or still queued.
struct Queue<T> {
    state: Mutex<QueueState<T>>,
    max_items: usize,
    max_bytes: usize,
    observed: AtomicU64,
    dropped: AtomicU64,
    taken: AtomicU64,
}

impl<T> Queue<T> {
    fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            state: Mutex::new(QueueState {
                items: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            max_items,
            max_bytes,
            observed: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            taken: AtomicU64::new(0),
        }
    }

    /// Media: the newest item always wins. Returns whether it was queued.
    fn push_drop_oldest(&self, item: T, size: usize) -> bool {
        let mut state = lock(&self.state);
        if state.closed {
            return false;
        }
        self.observed.fetch_add(1, Ordering::Relaxed);
        if size > self.max_bytes {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        while state.items.len() >= self.max_items || state.bytes + size > self.max_bytes {
            let Some((_, evicted)) = state.items.pop_front() else {
                break;
            };
            state.bytes -= evicted;
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        state.bytes += size;
        state.items.push_back((item, size));
        true
    }

    /// Transport events are never evicted: a full queue refuses the event and
    /// the caller retires the connection.
    fn push(&self, item: T, size: usize) -> Push {
        let mut state = lock(&self.state);
        if state.closed {
            return Push::Closed;
        }
        self.observed.fetch_add(1, Ordering::Relaxed);
        if state.items.len() >= self.max_items || state.bytes + size > self.max_bytes {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Push::Overflow;
        }
        state.bytes += size;
        state.items.push_back((item, size));
        Push::Accepted
    }

    /// Discard the backlog, counting it as dropped, and keep only `item`.
    fn replace(&self, item: T, size: usize) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        self.observed.fetch_add(1, Ordering::Relaxed);
        self.dropped
            .fetch_add(state.items.len() as u64, Ordering::Relaxed);
        state.items.clear();
        state.bytes = size;
        state.items.push_back((item, size));
    }

    /// Remove the front item when `fits` accepts it. `fits` sees the item under
    /// the queue lock so it can report its sizes; the caller copies after the
    /// lock is released, so a producer never waits for that copy.
    fn take(&self, fits: impl FnOnce(&T) -> bool) -> Taken<T> {
        let mut state = lock(&self.state);
        let Some((front, _)) = state.items.front() else {
            return if state.closed {
                Taken::Closed
            } else {
                Taken::Empty
            };
        };
        if !fits(front) {
            return Taken::TooSmall;
        }
        let (item, size) = state.items.pop_front().expect("front item exists");
        state.bytes -= size;
        self.taken.fetch_add(1, Ordering::Relaxed);
        Taken::Item(item)
    }

    /// Closing discards what the reader never took; those items count as dropped.
    fn close(&self) {
        let mut state = lock(&self.state);
        self.dropped
            .fetch_add(state.items.len() as u64, Ordering::Relaxed);
        state.items.clear();
        state.bytes = 0;
        state.closed = true;
    }

    fn counts(&self) -> Counts {
        let state = lock(&self.state);
        Counts {
            dropped: self.dropped.load(Ordering::Relaxed),
            taken: self.taken.load(Ordering::Relaxed),
            queued: state.items.len(),
            bytes: state.bytes,
        }
    }
}

/// Header of one decoded BGRA frame, written by `reactor_effect_peer_take_video`.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReactorEffectVideoHeader {
    pub width: u32,
    pub height: u32,
    pub data_len: u32,
    pub metadata_len: u32,
    pub frame_id: u64,
    pub timestamp_us: u64,
    pub track: u32,
    pub reserved: u32,
}

/// Header of one interleaved PCM block, written by `reactor_effect_peer_take_audio`.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReactorEffectAudioHeader {
    pub sample_rate: u32,
    pub channels: u32,
    pub samples: u32,
    pub track: u32,
}

/// Diagnostic text beside a failure status. It is never matched on: the status
/// is the failure class.
#[repr(C)]
pub struct ReactorEffectFailure {
    pub message_len: u32,
    pub message: [u8; FAILURE_MESSAGE_BYTES],
}

struct VideoItem {
    track: u32,
    width: u32,
    height: u32,
    frame_id: u64,
    timestamp_us: u64,
    bgra: Vec<u8>,
    metadata: Vec<u8>,
}

impl VideoItem {
    fn header(&self) -> ReactorEffectVideoHeader {
        ReactorEffectVideoHeader {
            width: self.width,
            height: self.height,
            data_len: self.bgra.len() as u32,
            metadata_len: self.metadata.len() as u32,
            frame_id: self.frame_id,
            timestamp_us: self.timestamp_us,
            track: self.track,
            reserved: 0,
        }
    }
}

struct AudioItem {
    track: u32,
    sample_rate: u32,
    channels: u32,
    pcm: Vec<i16>,
}

impl AudioItem {
    fn header(&self) -> ReactorEffectAudioHeader {
        ReactorEffectAudioHeader {
            sample_rate: self.sample_rate,
            channels: self.channels,
            samples: self.pcm.len() as u32,
            track: self.track,
        }
    }
}

/// Called with the queues that became readable since the previous call.
type NotifyFn = extern "C" fn(u32);

#[derive(Default)]
struct NotifierState {
    ready: u32,
    closed: bool,
}

/// Readiness hand-off to the host. libwebrtc threads only set bits here; the
/// peer's notifier thread is the one thread that ever waits on the host.
#[derive(Default)]
struct Notifier {
    state: Mutex<NotifierState>,
    wake: Condvar,
}

impl Notifier {
    fn signal(&self, bit: u32) {
        let mut state = lock(&self.state);
        if state.closed || state.ready & bit != 0 {
            return;
        }
        let idle = state.ready == 0;
        state.ready |= bit;
        if idle {
            self.wake.notify_one();
        }
    }

    fn close(&self) {
        lock(&self.state).closed = true;
        self.wake.notify_all();
    }

    fn run(&self, notify: NotifyFn) {
        loop {
            let ready = {
                let mut state = lock(&self.state);
                while state.ready == 0 && !state.closed {
                    state = self.wake.wait(state).unwrap_or_else(|p| p.into_inner());
                }
                if state.closed {
                    return;
                }
                std::mem::take(&mut state.ready)
            };
            notify(ready);
        }
    }
}

#[derive(Clone)]
struct Binding {
    name: String,
    mid: String,
    index: u32,
}

#[derive(Default)]
struct Bindings {
    video: VecDeque<Binding>,
    audio: VecDeque<Binding>,
}

struct Shared {
    gate: Arc<CallbackGate>,
    events: Queue<Vec<u8>>,
    video: Queue<VideoItem>,
    audio: Queue<AudioItem>,
    notifier: Notifier,
    bindings: Mutex<Bindings>,
    remote_tracks: Mutex<Vec<RemoteTrack>>,
    overflowed: AtomicBool,
}

impl Shared {
    fn new() -> Self {
        Self {
            gate: Arc::new(CallbackGate::new()),
            events: Queue::new(1024, 16 * 1024 * 1024),
            video: Queue::new(8, 64 * 1024 * 1024),
            audio: Queue::new(256, 4 * 1024 * 1024),
            notifier: Notifier::default(),
            bindings: Mutex::new(Bindings::default()),
            remote_tracks: Mutex::new(Vec::new()),
            overflowed: AtomicBool::new(false),
        }
    }

    fn emit(&self, header: Value, payload: &[u8]) {
        let packet = packet(header, payload);
        let size = packet.len();
        match self.events.push(packet, size) {
            Push::Accepted => self.notifier.signal(READY_EVENTS),
            Push::Closed => {}
            Push::Overflow => self.fail_overflow(),
        }
    }

    fn emit_error(&self, class: Class, message: impl Into<String>) {
        self.emit(
            json!({ "type": "error", "status": class.status(), "message": message.into() }),
            &[],
        );
    }

    fn fail_overflow(&self) {
        if self.overflowed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.gate.close();
        let diagnostic = packet(
            json!({
                "type": "error",
                "status": STATUS_OVERFLOW,
                "message": "native transport event queue overflowed; connection retired"
            }),
            &[],
        );
        let size = diagnostic.len();
        self.events.replace(diagnostic, size);
        self.notifier.signal(READY_EVENTS);
    }

    fn set_bindings(&self, mappings: &[MappingSpec]) {
        let mut bindings = lock(&self.bindings);
        bindings.video.clear();
        bindings.audio.clear();
        for (index, mapping) in mappings.iter().enumerate() {
            if mapping.direction != "recvonly" {
                continue;
            }
            let binding = Binding {
                name: mapping.name.clone(),
                mid: mapping.mid.clone(),
                index: index as u32,
            };
            if mapping.kind == "video" {
                bindings.video.push_back(binding);
            } else {
                bindings.audio.push_back(binding);
            }
        }
    }

    fn take_binding(&self, kind: MediaKind) -> Option<Binding> {
        let mut bindings = lock(&self.bindings);
        match kind {
            MediaKind::Video => bindings.video.pop_front(),
            MediaKind::Audio => bindings.audio.pop_front(),
            MediaKind::Unknown => None,
        }
    }

    fn accept_remote(self: &Arc<Self>, track: RemoteTrack) {
        let Some(_guard) = self.gate.enter() else {
            return;
        };
        let kind = track.kind();
        let Some(binding) = self.take_binding(kind) else {
            self.emit_error(
                Class::Protocol,
                "received native track without a declared receive mapping",
            );
            return;
        };

        match &track {
            RemoteTrack::Video(video) => {
                let shared = Arc::clone(self);
                let index = binding.index;
                video.on_frame(move |frame| {
                    let Some(_guard) = shared.gate.enter() else {
                        return;
                    };
                    let (frame_id, timestamp_us, metadata) = frame
                        .metadata
                        .map(|m| (m.frame_id, m.capture_time_us, m.user_data))
                        .unwrap_or_default();
                    // The one native copy: libwebrtc lends the pixels only for
                    // the duration of this callback.
                    let item = VideoItem {
                        track: index,
                        width: frame.width,
                        height: frame.height,
                        frame_id,
                        timestamp_us,
                        bgra: frame.bgra.to_vec(),
                        metadata,
                    };
                    let size = item.bgra.len() + item.metadata.len();
                    if shared.video.push_drop_oldest(item, size) {
                        shared.notifier.signal(READY_VIDEO);
                    }
                });
            }
            RemoteTrack::Audio(audio) => {
                let shared = Arc::clone(self);
                let index = binding.index;
                audio.on_frame(move |frame| {
                    let Some(_guard) = shared.gate.enter() else {
                        return;
                    };
                    let item = AudioItem {
                        track: index,
                        sample_rate: frame.sample_rate,
                        channels: frame.channels,
                        pcm: frame.pcm.to_vec(),
                    };
                    let size = item.pcm.len() * 2;
                    if shared.audio.push_drop_oldest(item, size) {
                        shared.notifier.signal(READY_AUDIO);
                    }
                });
            }
        }

        self.emit(
            json!({ "type": "track", "name": binding.name, "mid": binding.mid }),
            &[],
        );
        self.emit(
            json!({
                "type": "decoded",
                "kind": match kind { MediaKind::Video => "video", MediaKind::Audio => "audio", MediaKind::Unknown => "unknown" },
                "name": binding.name,
                "mid": binding.mid
            }),
            &[],
        );
        lock(&self.remote_tracks).push(track);
    }

    fn pressure(&self) -> Value {
        let events = self.events.counts();
        let video = self.video.counts();
        let audio = self.audio.counts();
        json!({
            "closed": !self.gate.accepting(),
            "queuedControl": events.queued,
            "queuedVideo": video.queued,
            "queuedAudio": audio.queued,
            "queuedBytes": events.bytes + video.bytes + audio.bytes,
            "droppedVideo": video.dropped.to_string(),
            "droppedAudio": audio.dropped.to_string(),
            "deliveredVideo": video.taken.to_string(),
            "deliveredAudio": audio.taken.to_string(),
            "pendingRequests": 0,
        })
    }

    fn close_queues(&self) {
        self.events.close();
        self.video.close();
        self.audio.close();
        self.notifier.close();
    }
}

fn packet(header: Value, payload: &[u8]) -> Vec<u8> {
    let header = serde_json::to_vec(&header).unwrap_or_else(|_| b"{}".to_vec());
    let mut out = Vec::with_capacity(4 + header.len() + payload.len());
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(payload);
    out
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrepareRequest {
    servers: Vec<IceServerSpec>,
    tracks: Vec<TrackSpec>,
}

#[derive(Debug, Deserialize)]
struct IceServerSpec {
    urls: Vec<String>,
    #[serde(default)]
    username: String,
    #[serde(default)]
    credential: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TrackSpec {
    name: String,
    kind: String,
    direction: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MappingSpec {
    name: String,
    kind: String,
    direction: String,
    mid: String,
}

#[derive(Debug, Deserialize)]
struct DirectionRequest {
    name: String,
    active: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitrateRequest {
    name: String,
    bits_per_second: u32,
}

struct NativeTrack {
    spec: TrackSpec,
    transceiver: Transceiver,
}

struct Channels {
    control: DataChannel,
    data: DataChannel,
}

// libwebrtc's threads are process-global: reactor-webrtc requires one factory
// per process (docs/architecture.md, "One factory per process"). Every peer
// shares this one; it is created on first use and never destroyed.
static FACTORY: Mutex<Option<&'static PeerConnectionFactory>> = Mutex::new(None);

fn factory() -> Result<&'static PeerConnectionFactory, BridgeError> {
    let mut slot = lock(&FACTORY);
    if let Some(factory) = *slot {
        return Ok(factory);
    }
    let factory = PeerConnectionFactory::builder()
        .with_synthetic_adm()
        .build()
        .map_err(webrtc(Class::Native, "create_factory"))?;
    let factory: &'static PeerConnectionFactory = Box::leak(Box::new(factory));
    *slot = Some(factory);
    Ok(factory)
}

struct WorkerState {
    shared: Arc<Shared>,
    peer: Option<PeerConnection>,
    channels: Option<Channels>,
    tracks: HashMap<String, NativeTrack>,
}

impl WorkerState {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            peer: None,
            channels: None,
            tracks: HashMap::new(),
        }
    }

    fn prepare(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        if self.peer.is_some() {
            return Err(BridgeError::invalid("native peer is already prepared"));
        }
        let request: PrepareRequest = decode_json(request)?;
        validate_prepare(&request)?;

        let config = RtcConfiguration {
            ice_servers: request
                .servers
                .iter()
                .map(|server| IceServer {
                    urls: server.urls.clone(),
                    username: server.username.clone(),
                    password: server.credential.clone(),
                })
                .collect(),
            ..RtcConfiguration::default()
        };

        let peer = factory()?
            .create_peer_connection(&config, observer(Arc::clone(&self.shared)))
            .map_err(webrtc(Class::Native, "create_peer_connection"))?;

        let mut control = peer
            .create_data_channel("control")
            .map_err(webrtc(Class::Native, "create_data_channel"))?;
        let mut data = peer
            .create_data_channel("data")
            .map_err(webrtc(Class::Native, "create_data_channel"))?;
        wire_channel(&mut control, "control", Arc::clone(&self.shared));
        wire_channel(&mut data, "data", Arc::clone(&self.shared));

        let mut tracks = HashMap::with_capacity(request.tracks.len());
        let mut order = Vec::with_capacity(request.tracks.len());
        for spec in request.tracks {
            let kind = media_kind(&spec.kind)?;
            let direction = direction(&spec.direction)?;
            let transceiver = peer
                .add_transceiver(kind, direction)
                .map_err(webrtc(Class::Native, "add_transceiver"))?;
            order.push(spec.name.clone());
            tracks.insert(spec.name.clone(), NativeTrack { spec, transceiver });
        }

        let offer = peer
            .create_offer()
            .map_err(webrtc(Class::SdpRejected, "create_offer"))?;
        peer.set_local_description(&offer)
            .map_err(webrtc(Class::SdpRejected, "set_local_description"))?;

        let mut mappings = Vec::with_capacity(order.len());
        for name in &order {
            let entry = tracks.get(name).ok_or_else(|| {
                BridgeError::new(Class::Native, "track map changed while preparing")
            })?;
            let mid = entry.transceiver.mid().ok_or_else(|| {
                BridgeError::new(
                    Class::Native,
                    format!("missing MID after local description: {name}"),
                )
            })?;
            mappings.push(MappingSpec {
                name: entry.spec.name.clone(),
                kind: entry.spec.kind.clone(),
                direction: entry.spec.direction.clone(),
                mid,
            });
        }
        self.shared.set_bindings(&mappings);

        self.peer = Some(peer);
        self.channels = Some(Channels { control, data });
        self.tracks = tracks;

        serde_json::to_vec(&json!({ "sdp": offer.sdp, "mapping": mappings })).map_err(|e| {
            BridgeError::new(Class::Native, format!("serialize prepare response: {e}"))
        })
    }

    fn answer(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let sdp = std::str::from_utf8(request)
            .map_err(|_| BridgeError::invalid("answer SDP is not UTF-8"))?;
        if sdp.is_empty() {
            return Err(BridgeError::invalid("answer SDP is empty"));
        }
        self.require_peer()?
            .set_remote_description(&SessionDescription {
                kind: SdpType::Answer,
                sdp: sdp.to_owned(),
            })
            .map_err(webrtc(Class::SdpRejected, "set_remote_description"))?;
        Ok(b"{}".to_vec())
    }

    fn set_direction(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let request: DirectionRequest = decode_json(request)?;
        let entry = self
            .tracks
            .get(&request.name)
            .ok_or_else(|| BridgeError::invalid(format!("unknown track: {}", request.name)))?;
        let value = if request.active {
            direction(&entry.spec.direction)?
        } else {
            TransceiverDirection::Inactive
        };
        entry
            .transceiver
            .set_direction(value)
            .map_err(webrtc(Class::Native, "set_direction"))?;
        Ok(b"{}".to_vec())
    }

    fn max_bitrate(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let request: BitrateRequest = decode_json(request)?;
        if request.bits_per_second == 0 || request.bits_per_second > i32::MAX as u32 {
            return Err(BridgeError::invalid(
                "bitsPerSecond must be an integer in 1..=2147483647",
            ));
        }
        let entry = self
            .tracks
            .get(&request.name)
            .ok_or_else(|| BridgeError::invalid(format!("unknown track: {}", request.name)))?;
        if entry.spec.direction != "sendonly" {
            return Err(BridgeError::invalid(format!(
                "{} is not an outgoing track",
                request.name
            )));
        }
        entry
            .transceiver
            .set_send_bitrate(None, Some(request.bits_per_second as i32))
            .map_err(webrtc(Class::Native, "set_send_bitrate"))?;
        Ok(b"{}".to_vec())
    }

    fn stats(&mut self) -> Result<Vec<u8>, BridgeError> {
        let report = self
            .require_peer()?
            .get_stats()
            .map_err(webrtc(Class::Native, "get_stats"))?;
        serde_json::to_vec(&stats_json(report))
            .map_err(|e| BridgeError::new(Class::Native, format!("serialize stats response: {e}")))
    }

    fn send(&mut self, channel: u32, bytes: &[u8]) -> Result<(), BridgeError> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(BridgeError::new(
                Class::Overflow,
                format!("data channel message exceeds {MAX_MESSAGE_BYTES} bytes"),
            ));
        }
        let name = match channel {
            CHANNEL_CONTROL => "control",
            CHANNEL_DATA => "data",
            _ => return Err(BridgeError::invalid("unknown data channel")),
        };
        let channel = self.channels.as_ref().map(|channels| match channel {
            CHANNEL_CONTROL => &channels.control,
            _ => &channels.data,
        });
        let Some(channel) = channel.filter(|channel| channel.state() == DataChannelState::Open)
        else {
            return Err(BridgeError::new(
                Class::ChannelClosed,
                format!("{name} channel is not open"),
            ));
        };
        if channel.buffered_amount().saturating_add(bytes.len() as u64) > MAX_BUFFERED_SEND_BYTES {
            return Err(BridgeError::new(
                Class::Overflow,
                "native data channel buffered amount bound exceeded",
            ));
        }
        channel
            .send(bytes, true)
            .map_err(webrtc(Class::Native, "send"))
    }

    fn call(&mut self, operation: u32, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        match operation {
            CALL_PREPARE => self.prepare(request),
            CALL_ANSWER => self.answer(request),
            CALL_DIRECTION => self.set_direction(request),
            CALL_MAX_BITRATE => self.max_bitrate(request),
            CALL_STATS => self.stats(),
            _ => Err(BridgeError::invalid("unknown native call operation")),
        }
    }

    fn require_peer(&self) -> Result<&PeerConnection, BridgeError> {
        self.peer.as_ref().ok_or_else(BridgeError::closed)
    }

    fn shutdown(mut self) {
        self.channels.take();
        lock(&self.shared.remote_tracks).clear();
        self.tracks.clear();
        self.peer.take();
        self.shared.gate.wait_zero();
        self.shared.close_queues();
    }
}

fn validate_prepare(request: &PrepareRequest) -> Result<(), BridgeError> {
    if request.servers.len() > 64 {
        return Err(BridgeError::invalid("at most 64 ICE servers are supported"));
    }
    if request.tracks.len() > 64 {
        return Err(BridgeError::invalid("at most 64 tracks are supported"));
    }
    let mut names = std::collections::HashSet::new();
    for server in &request.servers {
        if server.urls.is_empty() || server.urls.len() > 16 {
            return Err(BridgeError::invalid(
                "each ICE server must contain 1..=16 URLs",
            ));
        }
        for url in &server.urls {
            if url.is_empty() || url.len() > 2048 {
                return Err(BridgeError::invalid("invalid ICE URL length"));
            }
        }
    }
    for track in &request.tracks {
        if track.name.is_empty() || track.name.len() > 256 || track.name.contains('\0') {
            return Err(BridgeError::invalid("invalid track name"));
        }
        media_kind(&track.kind)?;
        direction(&track.direction)?;
        if !names.insert(track.name.as_str()) {
            return Err(BridgeError::invalid("duplicate track name"));
        }
    }
    Ok(())
}

fn media_kind(kind: &str) -> Result<MediaKind, BridgeError> {
    match kind {
        "video" => Ok(MediaKind::Video),
        "audio" => Ok(MediaKind::Audio),
        _ => Err(BridgeError::invalid(format!("unknown track kind: {kind}"))),
    }
}

fn direction(direction: &str) -> Result<TransceiverDirection, BridgeError> {
    match direction {
        "recvonly" => Ok(TransceiverDirection::RecvOnly),
        "sendonly" => Ok(TransceiverDirection::SendOnly),
        _ => Err(BridgeError::invalid(format!(
            "unknown track direction: {direction}"
        ))),
    }
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, BridgeError> {
    serde_json::from_slice(bytes).map_err(|e| BridgeError::invalid(format!("invalid JSON: {e}")))
}

fn observer(shared: Arc<Shared>) -> PeerConnectionObserver {
    PeerConnectionObserver::new()
        .on_connection_state_change({
            let shared = Arc::clone(&shared);
            move |state| {
                let Some(_guard) = shared.gate.enter() else {
                    return;
                };
                shared.emit(
                    json!({ "type": "state", "state": connection_state(state) }),
                    &[],
                );
            }
        })
        .on_ice_gathering_change({
            let shared = Arc::clone(&shared);
            move |state| {
                let Some(_guard) = shared.gate.enter() else {
                    return;
                };
                if state == IceGatheringState::Complete {
                    shared.emit(json!({ "type": "ice" }), &[]);
                }
            }
        })
        .on_ice_candidate({
            let shared = Arc::clone(&shared);
            move |candidate| {
                let Some(_guard) = shared.gate.enter() else {
                    return;
                };
                shared.emit(
                    json!({
                        "type": "ice",
                        "candidate": {
                            "candidate": candidate.candidate,
                            "sdp_mid": candidate.sdp_mid,
                            "sdp_mline_index": candidate.sdp_mline_index,
                        }
                    }),
                    &[],
                );
            }
        })
        .on_track({
            let shared = Arc::clone(&shared);
            move |track| shared.accept_remote(track)
        })
}

fn wire_channel(channel: &mut DataChannel, name: &'static str, shared: Arc<Shared>) {
    channel.on_message({
        let shared = Arc::clone(&shared);
        move |bytes, binary| {
            let Some(_guard) = shared.gate.enter() else {
                return;
            };
            if !binary {
                shared.emit_error(
                    Class::Protocol,
                    format!("{name} data channel delivered a nonbinary message"),
                );
                return;
            }
            if bytes.len() > MAX_MESSAGE_BYTES {
                shared.emit_error(
                    Class::Overflow,
                    format!("{name} data channel message exceeds local bound"),
                );
                return;
            }
            shared.emit(json!({ "type": "message", "channel": name }), bytes);
        }
    });
    channel.on_state_change(move |state| {
        let Some(_guard) = shared.gate.enter() else {
            return;
        };
        match state {
            DataChannelState::Open => shared.emit(
                json!({ "type": "channel", "channel": name, "open": true }),
                &[],
            ),
            DataChannelState::Closed => shared.emit(
                json!({ "type": "channel", "channel": name, "open": false }),
                &[],
            ),
            DataChannelState::Connecting | DataChannelState::Closing => {}
        }
    });
}

fn connection_state(state: PeerConnectionState) -> &'static str {
    match state {
        PeerConnectionState::New => "new",
        PeerConnectionState::Connecting => "connecting",
        PeerConnectionState::Connected => "connected",
        PeerConnectionState::Disconnected => "disconnected",
        PeerConnectionState::Failed => "failed",
        PeerConnectionState::Closed => "closed",
    }
}

fn stats_json(report: StatsReport) -> Value {
    let mut entries = Vec::new();
    for entry in report.inbound_rtp {
        entries.push(json!({
            "id": format!("inbound-rtp-{}", entry.ssrc),
            "type": "inbound-rtp",
            "ssrc": entry.ssrc,
            "kind": stream_kind(entry.kind),
            "packetsReceived": entry.packets_received,
            "bytesReceived": entry.bytes_received.to_string(),
            "jitter": entry.jitter_s,
            "packetsLost": entry.packets_lost,
            "nackCount": entry.nack_count,
            "pliCount": entry.pli_count,
            "firCount": entry.fir_count,
            "totalDecodeTime": entry.total_decode_time_s,
            "framesPerSecond": entry.frames_per_second,
            "framesDecoded": entry.frames_decoded,
            "framesDropped": entry.frames_dropped,
            "frameWidth": entry.frame_width,
            "frameHeight": entry.frame_height,
        }));
    }
    for entry in report.outbound_rtp {
        entries.push(json!({
            "id": format!("outbound-rtp-{}", entry.ssrc),
            "type": "outbound-rtp",
            "ssrc": entry.ssrc,
            "kind": stream_kind(entry.kind),
            "packetsSent": entry.packets_sent.to_string(),
            "bytesSent": entry.bytes_sent.to_string(),
            "targetBitrate": entry.target_bitrate_bps,
            "roundTripTime": entry.round_trip_time_s,
            "totalRoundTripTime": entry.total_round_trip_time_s,
            "fractionLost": entry.fraction_lost,
            "packetsLost": entry.packets_lost,
            "retransmittedPacketsSent": entry.retransmitted_packets_sent.to_string(),
            "nackCount": entry.nack_count,
            "pliCount": entry.pli_count,
            "firCount": entry.fir_count,
            "framesPerSecond": entry.frames_per_second,
            "framesSent": entry.frames_sent,
            "frameWidth": entry.frame_width,
            "frameHeight": entry.frame_height,
        }));
    }
    for (index, entry) in report.candidate_pairs.into_iter().enumerate() {
        let local_id = format!("local-candidate-{index}");
        entries.push(json!({
            "id": local_id,
            "type": "local-candidate",
            "candidateType": candidate_type(entry.local_candidate_type),
            "relayProtocol": relay_protocol(entry.local_relay_protocol),
        }));
        entries.push(json!({
            "id": format!("candidate-pair-{index}"),
            "type": "candidate-pair",
            "state": pair_state(entry.state),
            "nominated": entry.nominated,
            "writable": entry.writable,
            "priority": entry.priority.to_string(),
            "bytesSent": entry.bytes_sent.to_string(),
            "bytesReceived": entry.bytes_received.to_string(),
            "packetsSent": entry.packets_sent.to_string(),
            "packetsReceived": entry.packets_received.to_string(),
            "currentRoundTripTime": entry.current_round_trip_time_s,
            "totalRoundTripTime": entry.total_round_trip_time_s,
            "availableOutgoingBitrate": entry.available_outgoing_bitrate_bps,
            "availableIncomingBitrate": entry.available_incoming_bitrate_bps,
            "localCandidateId": format!("local-candidate-{index}"),
        }));
    }
    Value::Array(entries)
}

fn stream_kind(kind: reactor_webrtc::StreamKind) -> &'static str {
    match kind {
        reactor_webrtc::StreamKind::Audio => "audio",
        reactor_webrtc::StreamKind::Video => "video",
        reactor_webrtc::StreamKind::Unknown => "unknown",
    }
}

fn pair_state(state: reactor_webrtc::IceCandidatePairState) -> &'static str {
    match state {
        reactor_webrtc::IceCandidatePairState::Waiting => "waiting",
        reactor_webrtc::IceCandidatePairState::InProgress => "in-progress",
        reactor_webrtc::IceCandidatePairState::Failed => "failed",
        reactor_webrtc::IceCandidatePairState::Succeeded => "succeeded",
        reactor_webrtc::IceCandidatePairState::Cancelled => "cancelled",
    }
}

fn candidate_type(value: IceCandidateType) -> &'static str {
    match value {
        IceCandidateType::Host => "host",
        IceCandidateType::Srflx => "srflx",
        IceCandidateType::Prflx => "prflx",
        IceCandidateType::Relay => "relay",
        IceCandidateType::Unknown => "unknown",
    }
}

fn relay_protocol(value: RelayProtocol) -> &'static str {
    match value {
        RelayProtocol::Udp => "udp",
        RelayProtocol::Tcp => "tcp",
        RelayProtocol::Tls => "tls",
        RelayProtocol::NotRelayed => "",
    }
}

type Reply = SyncSender<Result<Vec<u8>, BridgeError>>;

enum Command {
    Call {
        operation: u32,
        request: Vec<u8>,
        reply: Reply,
    },
    Send {
        channel: u32,
        bytes: Vec<u8>,
        reply: SyncSender<Result<(), BridgeError>>,
    },
    Shutdown,
}

fn worker_loop(shared: Arc<Shared>, commands: Receiver<Command>) {
    let mut state = WorkerState::new(Arc::clone(&shared));
    while let Ok(command) = commands.recv() {
        match command {
            Command::Call {
                operation,
                request,
                reply,
            } => {
                let result = if shared.gate.accepting() {
                    state.call(operation, &request)
                } else {
                    Err(BridgeError::closed())
                };
                let _ = reply.send(result);
            }
            Command::Send {
                channel,
                bytes,
                reply,
            } => {
                let result = if shared.gate.accepting() {
                    state.send(channel, &bytes)
                } else {
                    Err(BridgeError::closed())
                };
                let _ = reply.send(result);
            }
            Command::Shutdown => break,
        }
    }
    state.shutdown();
}

#[derive(Default)]
struct Threads {
    worker: Option<JoinHandle<()>>,
    notifier: Option<JoinHandle<()>>,
}

pub struct ReactorEffectPeer {
    shared: Arc<Shared>,
    commands: Sender<Command>,
    threads: Mutex<Threads>,
}

impl ReactorEffectPeer {
    fn create(notify: Option<NotifyFn>) -> Option<Self> {
        let shared = Arc::new(Shared::new());
        let (commands, receiver) = mpsc::channel();
        let worker_shared = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("reactor-effect-native".into())
            .spawn(move || worker_loop(worker_shared, receiver))
            .ok()?;
        let peer = Self {
            shared: Arc::clone(&shared),
            commands,
            threads: Mutex::new(Threads {
                worker: Some(worker),
                notifier: None,
            }),
        };
        if let Some(notify) = notify {
            let spawned = thread::Builder::new()
                .name("reactor-effect-notify".into())
                .spawn(move || shared.notifier.run(notify));
            match spawned {
                Ok(notifier) => lock(&peer.threads).notifier = Some(notifier),
                Err(_) => {
                    // Joins the worker; no notifier exists to join.
                    let _ = peer.shutdown();
                    return None;
                }
            }
        }
        Some(peer)
    }

    fn request(&self, operation: u32, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        if !self.shared.gate.accepting() {
            return Err(BridgeError::closed());
        }
        if operation == CALL_MEDIA_SNAPSHOT {
            // Counters need no libwebrtc work; never queue them behind a
            // blocking operation on the owner thread.
            return serde_json::to_vec(&self.shared.pressure()).map_err(|e| {
                BridgeError::new(Class::Native, format!("serialize media snapshot: {e}"))
            });
        }
        let (tx, rx) = mpsc::sync_channel(1);
        self.commands
            .send(Command::Call {
                operation,
                request: request.to_vec(),
                reply: tx,
            })
            .map_err(|_| BridgeError::closed())?;
        rx.recv().map_err(|_| BridgeError::closed())?
    }

    fn send(&self, channel: u32, bytes: &[u8]) -> Result<(), BridgeError> {
        if !self.shared.gate.accepting() {
            return Err(BridgeError::closed());
        }
        let (tx, rx) = mpsc::sync_channel(1);
        self.commands
            .send(Command::Send {
                channel,
                bytes: bytes.to_vec(),
                reply: tx,
            })
            .map_err(|_| BridgeError::closed())?;
        rx.recv().map_err(|_| BridgeError::closed())?
    }

    fn close(&self) {
        self.shared.gate.close();
        self.shared.close_queues();
    }

    /// Holding the thread lock throughout makes a concurrent shutdown wait for
    /// this one to finish joining rather than return early.
    fn shutdown(&self) -> Result<(), BridgeError> {
        self.close();
        let mut threads = lock(&self.threads);
        let mut result = Ok(());
        if let Some(worker) = threads.worker.take() {
            let _ = self.commands.send(Command::Shutdown);
            if worker.join().is_err() {
                result = Err(BridgeError::new(
                    Class::Native,
                    "native owner thread panicked",
                ));
            }
        }
        // The notifier may be inside the host callback, waiting for the host
        // to run it. The host therefore joins from a thread other than the one
        // that runs its callback.
        if let Some(notifier) = threads.notifier.take() {
            if notifier.join().is_err() && result.is_ok() {
                result = Err(BridgeError::new(
                    Class::Native,
                    "native notifier thread panicked",
                ));
            }
        }
        result
    }
}

#[no_mangle]
pub extern "C" fn reactor_effect_abi_version() -> u32 {
    ABI_VERSION
}

// A unique marker makes the same identity inspectable without executing a
// foreign-platform library during package staging. The exported function lets
// runtime preflight compare the loaded image with that inspected artifact.
static BUILD_IDENTITY: &str = concat!(
    "reactor-effect-native:build-identity:",
    env!("REACTOR_EFFECT_BUILD_IDENTITY"),
    ":end\0"
);

#[no_mangle]
pub extern "C" fn reactor_effect_build_identity() -> *const std::ffi::c_char {
    BUILD_IDENTITY.as_ptr().cast()
}

#[no_mangle]
/// Allocate a peer. When `notify` is non-null, a notifier thread calls it with
/// the readiness bits of the queues that received items since its previous
/// call, until shutdown joins that thread.
pub extern "C" fn reactor_effect_peer_create(notify: Option<NotifyFn>) -> *mut ReactorEffectPeer {
    catch_unwind(|| ReactorEffectPeer::create(notify))
        .ok()
        .flatten()
        .map_or(ptr::null_mut(), |peer| Box::into_raw(Box::new(peer)))
}

#[no_mangle]
/// Invoke one serialized peer operation through the C ABI.
///
/// # Safety
/// `peer` must be a live handle returned by [`reactor_effect_peer_create`].
/// Non-null input/output pointers must reference at least their declared byte
/// lengths, `response_len` must be writable for one `usize`, and `failure`
/// must be null or writable for one [`ReactorEffectFailure`].
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
    with_failure(failure, || {
        let peer = peer_ref(peer)?;
        if response.is_null() || response_len.is_null() {
            return Err(BridgeError::invalid("call requires a response buffer"));
        }
        *response_len = 0;
        if response_cap < CALL_BUFFER_MIN {
            *response_len = CALL_BUFFER_MIN;
            return Ok(STATUS_BUFFER_TOO_SMALL);
        }
        let request = input(request, request_len)?;
        if request.len() > MAX_REQUEST_BYTES {
            return Err(BridgeError::new(
                Class::Overflow,
                "native request exceeds 1 MiB",
            ));
        }
        let bytes = peer.request(operation, request)?;
        if bytes.len() > response_cap {
            return Err(BridgeError::new(
                Class::Overflow,
                "native call response exceeds its buffer",
            ));
        }
        ptr::copy_nonoverlapping(bytes.as_ptr(), response, bytes.len());
        *response_len = bytes.len();
        Ok(STATUS_OK)
    })
}

#[no_mangle]
/// Send one binary SCTP message on a bridge-owned channel.
///
/// # Safety
/// `peer` must be live. `data` must reference `data_len` readable bytes when
/// nonempty, and `failure` must be null or writable for one
/// [`ReactorEffectFailure`].
pub unsafe extern "C" fn reactor_effect_peer_send(
    peer: *mut ReactorEffectPeer,
    channel: u32,
    data: *const u8,
    data_len: usize,
    failure: *mut ReactorEffectFailure,
) -> i32 {
    with_failure(failure, || {
        let peer = peer_ref(peer)?;
        peer.send(channel, input(data, data_len)?)?;
        Ok(STATUS_OK)
    })
}

#[no_mangle]
/// Nonblocking: copy the oldest transport event into caller memory.
///
/// # Safety
/// `peer` must be live, `out_len` writable for one `usize`, and `out` valid
/// for `out_cap` writable bytes when `out_cap` is nonzero.
pub unsafe extern "C" fn reactor_effect_peer_take_event(
    peer: *mut ReactorEffectPeer,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer).ok()?;
        if out_len.is_null() || (out_cap != 0 && out.is_null()) {
            return None;
        }
        Some(
            match peer.shared.events.take(|packet| {
                *out_len = packet.len();
                packet.len() <= out_cap
            }) {
                Taken::Item(packet) => {
                    if !packet.is_empty() {
                        ptr::copy_nonoverlapping(packet.as_ptr(), out, packet.len());
                    }
                    STATUS_OK
                }
                Taken::TooSmall => STATUS_BUFFER_TOO_SMALL,
                Taken::Empty => {
                    *out_len = 0;
                    STATUS_AGAIN
                }
                Taken::Closed => {
                    *out_len = 0;
                    STATUS_CLOSED
                }
            },
        )
    })
}

#[no_mangle]
/// Nonblocking: copy the oldest decoded frame into caller memory. The header
/// is written for OK and BUFFER_TOO_SMALL; the latter keeps the frame queued.
///
/// # Safety
/// `peer` must be live, `header` writable, `bgra` valid for `bgra_cap` bytes
/// and `metadata` for `metadata_cap` bytes when they are non-null.
pub unsafe extern "C" fn reactor_effect_peer_take_video(
    peer: *mut ReactorEffectPeer,
    header: *mut ReactorEffectVideoHeader,
    bgra: *mut u8,
    bgra_cap: usize,
    metadata: *mut u8,
    metadata_cap: usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer).ok()?;
        if header.is_null() {
            return None;
        }
        Some(
            match peer.shared.video.take(|item| {
                *header = item.header();
                !bgra.is_null()
                    && item.bgra.len() <= bgra_cap
                    && item.metadata.len() <= metadata_cap
                    && (item.metadata.is_empty() || !metadata.is_null())
            }) {
                Taken::Item(item) => {
                    ptr::copy_nonoverlapping(item.bgra.as_ptr(), bgra, item.bgra.len());
                    if !item.metadata.is_empty() {
                        ptr::copy_nonoverlapping(
                            item.metadata.as_ptr(),
                            metadata,
                            item.metadata.len(),
                        );
                    }
                    STATUS_OK
                }
                Taken::TooSmall => STATUS_BUFFER_TOO_SMALL,
                Taken::Empty => STATUS_AGAIN,
                Taken::Closed => STATUS_CLOSED,
            },
        )
    })
}

#[no_mangle]
/// Nonblocking: copy the oldest PCM block (native-endian interleaved `int16_t`)
/// into caller memory. The header is written for OK and BUFFER_TOO_SMALL.
///
/// # Safety
/// `peer` must be live, `header` writable, and `pcm` valid for `pcm_cap`
/// samples when non-null.
pub unsafe extern "C" fn reactor_effect_peer_take_audio(
    peer: *mut ReactorEffectPeer,
    header: *mut ReactorEffectAudioHeader,
    pcm: *mut i16,
    pcm_cap: usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer).ok()?;
        if header.is_null() {
            return None;
        }
        Some(
            match peer.shared.audio.take(|item| {
                *header = item.header();
                !pcm.is_null() && item.pcm.len() <= pcm_cap
            }) {
                Taken::Item(item) => {
                    ptr::copy_nonoverlapping(item.pcm.as_ptr(), pcm, item.pcm.len());
                    STATUS_OK
                }
                Taken::TooSmall => STATUS_BUFFER_TOO_SMALL,
                Taken::Empty => STATUS_AGAIN,
                Taken::Closed => STATUS_CLOSED,
            },
        )
    })
}

#[no_mangle]
/// Fence callback/event admission immediately.
///
/// # Safety
/// `peer` must be null or a live handle returned by
/// [`reactor_effect_peer_create`]. It must not have been destroyed.
pub unsafe extern "C" fn reactor_effect_peer_close(peer: *mut ReactorEffectPeer) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(peer) = peer.as_ref() {
            peer.close();
        }
    }));
}

#[no_mangle]
/// Join native ownership: the owner thread, every admitted libwebrtc callback
/// and the notifier thread.
///
/// # Safety
/// `peer` must be live and `failure` null or writable for one
/// [`ReactorEffectFailure`]. The caller must not be the thread that runs the
/// notify callback.
pub unsafe extern "C" fn reactor_effect_peer_shutdown(
    peer: *mut ReactorEffectPeer,
    failure: *mut ReactorEffectFailure,
) -> i32 {
    with_failure(failure, || {
        peer_ref(peer)?.shutdown()?;
        Ok(STATUS_OK)
    })
}

#[no_mangle]
/// Shut down and free an opaque peer handle.
///
/// # Safety
/// `peer` must be null or a handle returned by [`reactor_effect_peer_create`]
/// that has not already been passed to this function. All foreign calls using
/// the handle, including calls queued in a host FFI executor, must have returned.
/// Joining the native owner alone does not establish that host-side condition.
/// It shuts the peer down if needed, so it must not run on the thread that runs
/// the notify callback either.
pub unsafe extern "C" fn reactor_effect_peer_destroy(peer: *mut ReactorEffectPeer) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if peer.is_null() {
            return;
        }
        let peer = Box::from_raw(peer);
        let _ = peer.shutdown();
    }));
}

/// Run an FFI body that reports failures through a [`ReactorEffectFailure`].
/// A caught panic is a native failure.
unsafe fn with_failure(
    failure: *mut ReactorEffectFailure,
    body: impl FnOnce() -> Result<i32, BridgeError>,
) -> i32 {
    let error = match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(status)) => return status,
        Ok(Err(error)) => error,
        Err(_) => BridgeError::new(Class::Native, "native bridge panicked"),
    };
    if let Some(failure) = failure.as_mut() {
        let message = truncate(&error.message, FAILURE_MESSAGE_BYTES);
        failure.message[..message.len()].copy_from_slice(message.as_bytes());
        failure.message_len = message.len() as u32;
    }
    error.class.status()
}

/// Run an FFI body without a failure channel; `None` means invalid arguments.
fn ffi_status(body: impl FnOnce() -> Option<i32>) -> i32 {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Some(status)) => status,
        Ok(None) => STATUS_INVALID_INPUT,
        Err(_) => STATUS_NATIVE,
    }
}

fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

unsafe fn peer_ref<'a>(peer: *mut ReactorEffectPeer) -> Result<&'a ReactorEffectPeer, BridgeError> {
    peer.as_ref()
        .ok_or_else(|| BridgeError::invalid("null native peer handle"))
}

unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a [u8], BridgeError> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(BridgeError::invalid("null input with a nonzero length"));
    }
    Ok(slice::from_raw_parts(data, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reactor_webrtc::{
        AudioFrame, AudioTrackOptions, AudioTrackSource, IceCandidate, VideoFrame,
    };
    use std::sync::atomic::AtomicU32;
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct AnswererSignals {
        ice: Mutex<VecDeque<IceCandidate>>,
        connected: AtomicBool,
        channels: Mutex<HashMap<String, DataChannel>>,
        inbox: Mutex<Vec<(String, Vec<u8>)>>,
    }

    fn answerer_observer(signals: Arc<AnswererSignals>) -> PeerConnectionObserver {
        PeerConnectionObserver::new()
            .on_ice_candidate({
                let signals = Arc::clone(&signals);
                move |candidate| lock(&signals.ice).push_back(candidate)
            })
            .on_connection_state_change({
                let signals = Arc::clone(&signals);
                move |state| {
                    if state == PeerConnectionState::Connected {
                        signals.connected.store(true, Ordering::Release);
                    }
                }
            })
            .on_data_channel(move |mut channel| {
                let label = channel.label();
                let inbox = Arc::clone(&signals);
                let message_label = label.clone();
                channel.on_message(move |bytes, binary| {
                    assert!(binary, "bridge channels must remain binary");
                    lock(&inbox.inbox).push((message_label.clone(), bytes.to_vec()));
                });
                lock(&signals.channels).insert(label, channel);
            })
    }

    fn packet_parts(packet: &[u8]) -> (Value, &[u8]) {
        assert!(packet.len() >= 4, "packet missing header length");
        let header_len = u32::from_le_bytes(packet[0..4].try_into().unwrap()) as usize;
        assert!(4 + header_len <= packet.len(), "header exceeds packet");
        let header =
            serde_json::from_slice(&packet[4..4 + header_len]).expect("packet header JSON");
        (header, &packet[4 + header_len..])
    }

    fn take_any<T>(queue: &Queue<T>) -> Option<T> {
        match queue.take(|_| true) {
            Taken::Item(item) => Some(item),
            Taken::TooSmall => unreachable!("take_any accepts every size"),
            Taken::Empty | Taken::Closed => None,
        }
    }

    fn observed<T>(queue: &Queue<T>) -> u64 {
        queue.observed.load(Ordering::Relaxed)
    }

    fn drain_bridge_events(
        shared: &Shared,
        answerer: &PeerConnection,
        bridge_connected: &mut bool,
        bridge_messages: &mut Vec<(String, Vec<u8>)>,
    ) {
        while let Some(packet) = take_any(&shared.events) {
            let (header, payload) = packet_parts(&packet);
            match header.get("type").and_then(Value::as_str) {
                Some("state")
                    if header.get("state").and_then(Value::as_str) == Some("connected") =>
                {
                    *bridge_connected = true;
                }
                Some("ice") => {
                    if let Some(candidate) = header.get("candidate").and_then(Value::as_object) {
                        let candidate = IceCandidate {
                            candidate: candidate
                                .get("candidate")
                                .and_then(Value::as_str)
                                .expect("candidate text")
                                .to_owned(),
                            sdp_mid: candidate
                                .get("sdp_mid")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            sdp_mline_index: candidate
                                .get("sdp_mline_index")
                                .and_then(Value::as_u64)
                                .map(|value| value as u16),
                        };
                        answerer
                            .add_ice_candidate(&candidate)
                            .expect("answerer accepts bridge ICE");
                    }
                }
                Some("message") => bridge_messages.push((
                    header
                        .get("channel")
                        .and_then(Value::as_str)
                        .expect("message channel")
                        .to_owned(),
                    payload.to_vec(),
                )),
                Some("error") => panic!("bridge emitted error event: {header}"),
                _ => {}
            }
        }
    }

    fn forward_answerer_ice(signals: &AnswererSignals, bridge: &PeerConnection) {
        while let Some(candidate) = lock(&signals.ice).pop_front() {
            bridge
                .add_ice_candidate(&candidate)
                .expect("bridge accepts answerer ICE");
        }
    }

    fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn failure() -> ReactorEffectFailure {
        ReactorEffectFailure {
            message_len: 0,
            message: [0; FAILURE_MESSAGE_BYTES],
        }
    }

    fn failure_text(failure: &ReactorEffectFailure) -> &str {
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
            bgra: vec![fill; 8],
            metadata: metadata.to_vec(),
        }
    }

    #[test]
    fn c_structs_match_the_header_layout() {
        use std::mem::{offset_of, size_of};
        assert_eq!(size_of::<ReactorEffectVideoHeader>(), 40);
        assert_eq!(offset_of!(ReactorEffectVideoHeader, frame_id), 16);
        assert_eq!(offset_of!(ReactorEffectVideoHeader, timestamp_us), 24);
        assert_eq!(offset_of!(ReactorEffectVideoHeader, track), 32);
        assert_eq!(size_of::<ReactorEffectAudioHeader>(), 16);
        assert_eq!(offset_of!(ReactorEffectAudioHeader, track), 12);
        assert_eq!(size_of::<ReactorEffectFailure>(), 1024);
    }

    #[test]
    fn packet_framing_is_length_prefixed_and_lossless() {
        let payload = [0, 1, 2, 255];
        let packet = packet(json!({ "type": "message", "channel": "data" }), &payload);
        let (header, body) = packet_parts(&packet);
        assert_eq!(header["type"], "message");
        assert_eq!(body, payload);
    }

    #[test]
    fn callback_gate_fences_new_callbacks_and_waits_for_inflight() {
        let gate = Arc::new(CallbackGate::new());
        let guard = gate.enter().expect("first callback admitted");
        gate.close();
        assert!(gate.enter().is_none(), "close must fence new callbacks");
        let waiter = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || gate.wait_zero())
        };
        thread::sleep(Duration::from_millis(10));
        assert!(
            !waiter.is_finished(),
            "wait_zero returned while callback was active"
        );
        drop(guard);
        waiter.join().unwrap();
    }

    #[test]
    fn media_queue_evicts_the_oldest_and_accounts_for_every_item() {
        let queue = Queue::new(2, 32);
        for fill in 1..=3u8 {
            assert!(queue.push_drop_oldest(vec![fill; 8], 8));
        }
        assert_eq!(take_any(&queue), Some(vec![2; 8]));
        assert_eq!(take_any(&queue), Some(vec![3; 8]));
        assert_eq!(take_any(&queue), None);

        // The byte bound evicts too, and an item above it is dropped unqueued.
        assert!(queue.push_drop_oldest(vec![4; 20], 20));
        assert!(queue.push_drop_oldest(vec![5; 20], 20));
        assert!(!queue.push_drop_oldest(vec![6; 33], 33));
        let counts = queue.counts();
        assert_eq!(counts.queued, 1);
        assert_eq!(counts.bytes, 20);
        assert_eq!(counts.dropped, 3);
        assert_eq!(counts.taken, 2);
        assert_eq!(
            observed(&queue),
            counts.dropped + counts.taken + counts.queued as u64
        );
    }

    #[test]
    fn take_keeps_an_item_the_reader_cannot_hold() {
        let queue = Queue::new(4, 64);
        assert!(queue.push_drop_oldest(vec![7; 12], 12));
        let mut seen = 0;
        assert!(matches!(
            queue.take(|item: &Vec<u8>| {
                seen = item.len();
                false
            }),
            Taken::TooSmall
        ));
        assert_eq!(seen, 12);
        assert_eq!(queue.counts().queued, 1);
        assert_eq!(take_any(&queue), Some(vec![7; 12]));
        assert_eq!(queue.counts().taken, 1);
    }

    #[test]
    fn close_counts_untaken_media_as_dropped_and_refuses_later_items() {
        let queue = Queue::new(4, 64);
        assert!(queue.push_drop_oldest(vec![1; 4], 4));
        assert!(queue.push_drop_oldest(vec![2; 4], 4));
        queue.close();
        assert!(matches!(queue.take(|_| true), Taken::Closed));
        assert!(!queue.push_drop_oldest(vec![3; 4], 4));
        let counts = queue.counts();
        assert_eq!((counts.dropped, counts.queued, counts.bytes), (2, 0, 0));
        assert_eq!(observed(&queue), 2, "a closed queue observes nothing");
    }

    #[test]
    fn event_overflow_retires_the_connection_with_one_typed_diagnostic() {
        let shared = Shared::new();
        for index in 0..1024 {
            shared.emit(json!({ "type": "probe", "index": index }), &[]);
        }
        assert!(shared.gate.accepting());

        shared.emit(json!({ "type": "overflow-trigger" }), &[]);
        assert!(!shared.gate.accepting());
        assert_eq!(
            shared.events.counts().queued,
            1,
            "overflow must collapse the event backlog"
        );
        let packet = take_any(&shared.events).expect("overflow diagnostic");
        let (header, payload) = packet_parts(&packet);
        assert!(payload.is_empty());
        assert_eq!(header["type"], "error");
        assert_eq!(header["status"], STATUS_OVERFLOW);
    }

    #[test]
    fn notifier_coalesces_readiness_until_the_host_runs() {
        static CALLS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
        extern "C" fn record(ready: u32) {
            lock(&CALLS).push(ready);
        }
        let notifier = Arc::new(Notifier::default());
        notifier.signal(READY_VIDEO);
        notifier.signal(READY_VIDEO);
        notifier.signal(READY_AUDIO);
        let runner = {
            let notifier = Arc::clone(&notifier);
            thread::spawn(move || notifier.run(record))
        };
        until("coalesced readiness", || !lock(&CALLS).is_empty());
        notifier.signal(READY_EVENTS);
        until("second readiness", || lock(&CALLS).len() == 2);
        notifier.close();
        runner.join().unwrap();
        notifier.signal(READY_EVENTS);
        assert_eq!(
            *lock(&CALLS),
            [READY_VIDEO | READY_AUDIO, READY_EVENTS],
            "signals after close must not reach the host"
        );
    }

    #[test]
    fn c_abi_take_event_reports_its_size_then_copies_once() {
        unsafe {
            let peer = reactor_effect_peer_create(None);
            assert!(!peer.is_null());
            let shared = &(*peer).shared;
            let packet = packet(json!({ "type": "message", "channel": "control" }), b"abc");
            assert_eq!(
                shared.events.push(packet.clone(), packet.len()),
                Push::Accepted
            );

            let mut required = 0usize;
            let status = reactor_effect_peer_take_event(peer, ptr::null_mut(), 0, &mut required);
            assert_eq!(status, STATUS_BUFFER_TOO_SMALL);
            assert_eq!(required, packet.len());

            let mut output = vec![0u8; required];
            let mut copied = 0usize;
            let status = reactor_effect_peer_take_event(
                peer,
                output.as_mut_ptr(),
                output.len(),
                &mut copied,
            );
            assert_eq!(status, STATUS_OK);
            assert_eq!(copied, packet.len());
            assert_eq!(output, packet);
            assert_eq!(
                reactor_effect_peer_take_event(
                    peer,
                    output.as_mut_ptr(),
                    output.len(),
                    &mut copied
                ),
                STATUS_AGAIN
            );
            assert_eq!(copied, 0);
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn c_abi_takes_typed_media_into_caller_buffers() {
        unsafe {
            let peer = reactor_effect_peer_create(None);
            assert!(!peer.is_null());
            let shared = &(*peer).shared;
            assert!(shared
                .video
                .push_drop_oldest(video_item(3, 0x21, b"meta"), 12));
            let pcm = vec![1i16, -2, 300, -400];
            assert!(shared.audio.push_drop_oldest(
                AudioItem {
                    track: 1,
                    sample_rate: 48_000,
                    channels: 2,
                    pcm: pcm.clone(),
                },
                8
            ));

            let mut header = ReactorEffectVideoHeader::default();
            let mut bgra = vec![0u8; 8];
            let mut metadata = vec![0u8; 3];
            assert_eq!(
                reactor_effect_peer_take_video(
                    peer,
                    &mut header,
                    bgra.as_mut_ptr(),
                    bgra.len(),
                    metadata.as_mut_ptr(),
                    metadata.len()
                ),
                STATUS_BUFFER_TOO_SMALL
            );
            assert_eq!((header.data_len, header.metadata_len), (8, 4));
            assert_eq!(
                shared.video.counts().queued,
                1,
                "a short take keeps the frame"
            );

            metadata.resize(4, 0);
            assert_eq!(
                reactor_effect_peer_take_video(
                    peer,
                    &mut header,
                    bgra.as_mut_ptr(),
                    bgra.len(),
                    metadata.as_mut_ptr(),
                    metadata.len()
                ),
                STATUS_OK
            );
            assert_eq!(
                header,
                ReactorEffectVideoHeader {
                    width: 2,
                    height: 1,
                    data_len: 8,
                    metadata_len: 4,
                    frame_id: u64::MAX,
                    timestamp_us: 9_007_199_254_740_993,
                    track: 3,
                    reserved: 0,
                }
            );
            assert_eq!(bgra, [0x21; 8]);
            assert_eq!(metadata, b"meta");

            let mut audio = ReactorEffectAudioHeader::default();
            let mut samples = vec![0i16; 4];
            assert_eq!(
                reactor_effect_peer_take_audio(peer, &mut audio, samples.as_mut_ptr(), 4),
                STATUS_OK
            );
            assert_eq!(
                audio,
                ReactorEffectAudioHeader {
                    sample_rate: 48_000,
                    channels: 2,
                    samples: 4,
                    track: 1,
                }
            );
            assert_eq!(samples, pcm);
            assert_eq!(
                reactor_effect_peer_take_audio(peer, &mut audio, samples.as_mut_ptr(), 4),
                STATUS_AGAIN
            );

            reactor_effect_peer_close(peer);
            assert_eq!(
                reactor_effect_peer_take_video(
                    peer,
                    &mut header,
                    bgra.as_mut_ptr(),
                    8,
                    ptr::null_mut(),
                    0
                ),
                STATUS_CLOSED
            );
            assert_eq!(
                reactor_effect_peer_take_audio(peer, &mut audio, samples.as_mut_ptr(), 4),
                STATUS_CLOSED
            );
            assert_eq!(
                reactor_effect_peer_take_video(
                    peer,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    0,
                    ptr::null_mut(),
                    0
                ),
                STATUS_INVALID_INPUT
            );
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn c_abi_notifies_readiness_from_its_own_thread() {
        static READY: AtomicU32 = AtomicU32::new(0);
        extern "C" fn record(ready: u32) {
            READY.fetch_or(ready, Ordering::AcqRel);
        }
        unsafe {
            let peer = reactor_effect_peer_create(Some(record));
            assert!(!peer.is_null());
            let shared = &(*peer).shared;
            shared.emit(json!({ "type": "ice" }), &[]);
            until("event readiness", || {
                READY.load(Ordering::Acquire) & READY_EVENTS != 0
            });
            if shared.video.push_drop_oldest(video_item(0, 1, &[]), 8) {
                shared.notifier.signal(READY_VIDEO);
            }
            until("video readiness", || {
                READY.load(Ordering::Acquire) & READY_VIDEO != 0
            });
            let mut failure = failure();
            assert_eq!(reactor_effect_peer_shutdown(peer, &mut failure), STATUS_OK);
            assert_eq!(
                reactor_effect_peer_shutdown(peer, &mut failure),
                STATUS_OK,
                "shutdown is idempotent"
            );
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn shutdown_joins_a_notifier_that_is_still_inside_the_host_callback() {
        static ENTERED: AtomicBool = AtomicBool::new(false);
        static RELEASE: AtomicBool = AtomicBool::new(false);
        extern "C" fn blocking(_: u32) {
            ENTERED.store(true, Ordering::Release);
            while !RELEASE.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
        }
        unsafe {
            let peer = reactor_effect_peer_create(Some(blocking));
            assert!(!peer.is_null());
            let shared = &(*peer).shared;
            shared.emit(json!({ "type": "ice" }), &[]);
            until("the host callback", || ENTERED.load(Ordering::Acquire));

            let address = peer as usize;
            let joiner = thread::spawn(move || {
                let mut failure = failure();
                reactor_effect_peer_shutdown(address as *mut ReactorEffectPeer, &mut failure)
            });
            thread::sleep(Duration::from_millis(30));
            assert!(
                !joiner.is_finished(),
                "shutdown returned while the host callback could still run"
            );
            RELEASE.store(true, Ordering::Release);
            assert_eq!(joiner.join().unwrap(), STATUS_OK);
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn c_abi_reports_failure_classes_and_closes_every_entry_point() {
        unsafe {
            let peer = reactor_effect_peer_create(None);
            assert!(!peer.is_null());
            let mut failure = failure();

            let status =
                reactor_effect_peer_send(peer, CHANNEL_DATA, b"early".as_ptr(), 5, &mut failure);
            assert_eq!(status, STATUS_CHANNEL_CLOSED);
            assert_eq!(failure_text(&failure), "data channel is not open");
            assert_eq!(
                reactor_effect_peer_send(peer, 7, ptr::null(), 0, ptr::null_mut()),
                STATUS_INVALID_INPUT,
                "a null failure pointer is allowed"
            );

            let mut response = vec![0u8; CALL_BUFFER_MIN];
            let mut response_len = usize::MAX;
            let status = reactor_effect_peer_call(
                peer,
                99,
                ptr::null(),
                0,
                response.as_mut_ptr(),
                response.len(),
                &mut response_len,
                &mut failure,
            );
            assert_eq!(status, STATUS_INVALID_INPUT);
            assert_eq!(response_len, 0);
            assert_eq!(failure_text(&failure), "unknown native call operation");

            let oversized = vec![b' '; MAX_REQUEST_BYTES + 1];
            let status = reactor_effect_peer_call(
                peer,
                CALL_PREPARE,
                oversized.as_ptr(),
                oversized.len(),
                response.as_mut_ptr(),
                response.len(),
                &mut response_len,
                &mut failure,
            );
            assert_eq!(status, STATUS_OVERFLOW);

            let mut short = [0u8; 16];
            let status = reactor_effect_peer_call(
                peer,
                CALL_MEDIA_SNAPSHOT,
                ptr::null(),
                0,
                short.as_mut_ptr(),
                short.len(),
                &mut response_len,
                &mut failure,
            );
            assert_eq!(status, STATUS_BUFFER_TOO_SMALL);
            assert_eq!(response_len, CALL_BUFFER_MIN);

            let status = reactor_effect_peer_call(
                peer,
                CALL_MEDIA_SNAPSHOT,
                ptr::null(),
                0,
                response.as_mut_ptr(),
                response.len(),
                &mut response_len,
                &mut failure,
            );
            assert_eq!(status, STATUS_OK);
            let snapshot: Value =
                serde_json::from_slice(&response[..response_len]).expect("snapshot JSON");
            assert_eq!(snapshot["closed"], false);
            assert_eq!(snapshot["droppedVideo"], "0");

            reactor_effect_peer_close(peer);
            let status = reactor_effect_peer_call(
                peer,
                CALL_MEDIA_SNAPSHOT,
                ptr::null(),
                0,
                response.as_mut_ptr(),
                response.len(),
                &mut response_len,
                &mut failure,
            );
            assert_eq!(status, STATUS_CLOSED);
            assert_eq!(failure_text(&failure), "native peer is closed");
            assert_eq!(
                reactor_effect_peer_send(peer, CHANNEL_DATA, b"late".as_ptr(), 4, &mut failure),
                STATUS_CLOSED
            );
            let mut length = usize::MAX;
            assert_eq!(
                reactor_effect_peer_take_event(peer, ptr::null_mut(), 0, &mut length),
                STATUS_CLOSED
            );
            assert_eq!(length, 0);
            assert_eq!(
                reactor_effect_peer_shutdown(peer, ptr::null_mut()),
                STATUS_OK
            );
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn failure_text_is_truncated_on_a_character_boundary() {
        let text = "é".repeat(FAILURE_MESSAGE_BYTES);
        let cut = truncate(&text, FAILURE_MESSAGE_BYTES);
        assert_eq!(cut.len(), FAILURE_MESSAGE_BYTES);
        assert!(
            truncate(&text, 5).len() == 4,
            "a split character is dropped"
        );
    }

    #[test]
    fn a_rejected_answer_is_classified_as_sdp_rejected() {
        let shared = Arc::new(Shared::new());
        let mut bridge = WorkerState::new(Arc::clone(&shared));
        let prepare = serde_json::to_vec(&json!({
            "servers": [],
            "tracks": [{ "name": "video", "kind": "video", "direction": "recvonly" }]
        }))
        .unwrap();
        bridge.prepare(&prepare).expect("bridge prepare");
        let error = bridge
            .answer(b"v=0\r\nthis is not an answer\r\n")
            .expect_err("libwebrtc must reject a malformed answer");
        assert_eq!(error.class, Class::SdpRejected);
        assert!(error.message.starts_with("set_remote_description: "));
        bridge.shutdown();
    }

    #[test]
    fn transport_loopback_exchanges_ordered_binary_and_real_decoded_media() {
        let shared = Arc::new(Shared::new());
        let gate = Arc::clone(&shared.gate);
        let mut bridge = WorkerState::new(Arc::clone(&shared));

        let prepare = serde_json::to_vec(&json!({
            "servers": [],
            "tracks": [
                { "name": "video-a", "kind": "video", "direction": "recvonly" },
                { "name": "video-b", "kind": "video", "direction": "recvonly" },
                { "name": "audio-a", "kind": "audio", "direction": "recvonly" },
                { "name": "outgoing-video", "kind": "video", "direction": "sendonly" }
            ]
        }))
        .unwrap();
        let response = bridge.prepare(&prepare).expect("bridge prepare");
        let prepared: Value = serde_json::from_slice(&response).expect("prepare JSON");
        let offer = SessionDescription {
            kind: SdpType::Offer,
            sdp: prepared["sdp"].as_str().expect("offer SDP").to_owned(),
        };
        let mappings: Vec<MappingSpec> =
            serde_json::from_value(prepared["mapping"].clone()).expect("prepare mappings");
        assert_eq!(mappings.len(), 4);
        assert!(
            offer.declares_frame_metadata(),
            "bridge offer must negotiate metadata"
        );

        // Direction and sender bitrate are local transceiver capabilities and do
        // not depend on a Reactor account or a browser MediaStreamTrack.
        bridge
            .set_direction(br#"{"name":"outgoing-video","active":false}"#)
            .expect("pause outgoing transceiver");
        bridge
            .set_direction(br#"{"name":"outgoing-video","active":true}"#)
            .expect("resume outgoing transceiver");
        bridge
            .max_bitrate(br#"{"name":"outgoing-video","bitsPerSecond":2000000}"#)
            .expect("set outgoing bitrate");

        // The answerer shares the bridge's process-wide factory, as reactor-webrtc
        // requires of every peer in one process.
        let factory = factory().expect("process factory");
        let signals = Arc::new(AnswererSignals::default());
        let answerer = factory
            .create_peer_connection(
                &RtcConfiguration::default(),
                answerer_observer(Arc::clone(&signals)),
            )
            .expect("answerer PC");
        answerer
            .set_remote_description(&offer)
            .expect("answerer remote offer");

        let video_a = factory
            .create_video_track("fixture-video-a")
            .expect("video a");
        let video_b = factory
            .create_video_track("fixture-video-b")
            .expect("video b");
        let audio = factory
            .create_audio_track_with_options("fixture-audio", {
                let mut options = AudioTrackOptions::default();
                options.source = AudioTrackSource::LocalPush;
                options
            })
            .expect("fixture audio");

        for transceiver in answerer.transceivers() {
            let mid = transceiver.mid().expect("answerer transceiver MID");
            let Some(mapping) = mappings.iter().find(|mapping| mapping.mid == mid) else {
                continue;
            };
            if mapping.direction != "recvonly" {
                continue;
            }
            match mapping.name.as_str() {
                "video-a" => transceiver.set_track(&video_a).expect("publish video a"),
                "video-b" => transceiver.set_track(&video_b).expect("publish video b"),
                "audio-a" => transceiver.set_track(&audio).expect("publish audio"),
                other => panic!("unexpected receive mapping {other}"),
            }
            transceiver
                .set_direction(TransceiverDirection::SendOnly)
                .expect("answerer send direction");
        }

        let answer = answerer.create_answer().expect("answer");
        assert!(
            answer.declares_frame_metadata(),
            "answer must echo metadata support"
        );
        answerer
            .set_local_description(&answer)
            .expect("answerer local answer");
        bridge
            .answer(answer.sdp.as_bytes())
            .expect("bridge remote answer");

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut bridge_connected = false;
        let mut bridge_messages = Vec::new();
        loop {
            drain_bridge_events(
                &shared,
                &answerer,
                &mut bridge_connected,
                &mut bridge_messages,
            );
            forward_answerer_ice(&signals, bridge.require_peer().unwrap());
            let channels_open = bridge.channels.as_ref().is_some_and(|channels| {
                channels.control.state() == DataChannelState::Open
                    && channels.data.state() == DataChannelState::Open
            });
            let remote_channels = {
                let channels = lock(&signals.channels);
                channels
                    .get("control")
                    .is_some_and(|channel| channel.state() == DataChannelState::Open)
                    && channels
                        .get("data")
                        .is_some_and(|channel| channel.state() == DataChannelState::Open)
            };
            if bridge_connected
                && signals.connected.load(Ordering::Acquire)
                && channels_open
                && remote_channels
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "loopback did not connect/open both channels"
            );
            thread::sleep(Duration::from_millis(20));
        }

        for bytes in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            bridge.send(CHANNEL_DATA, bytes).expect("bridge data send");
        }
        let data_deadline = Instant::now() + Duration::from_secs(5);
        while lock(&signals.inbox)
            .iter()
            .filter(|(channel, _)| channel == "data")
            .count()
            < 3
        {
            assert!(
                Instant::now() < data_deadline,
                "answerer did not receive ordered data"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let received: Vec<Vec<u8>> = lock(&signals.inbox)
            .iter()
            .filter(|(channel, _)| channel == "data")
            .map(|(_, bytes)| bytes.clone())
            .collect();
        assert_eq!(
            received,
            [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
        );

        {
            let channels = lock(&signals.channels);
            let control = channels.get("control").expect("remote control channel");
            control.send(b"alpha", true).expect("answerer alpha");
            control.send(b"beta", true).expect("answerer beta");
        }
        let reverse_deadline = Instant::now() + Duration::from_secs(5);
        while bridge_messages
            .iter()
            .filter(|(channel, _)| channel == "control")
            .count()
            < 2
        {
            drain_bridge_events(
                &shared,
                &answerer,
                &mut bridge_connected,
                &mut bridge_messages,
            );
            assert!(
                Instant::now() < reverse_deadline,
                "bridge did not receive control messages"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let reverse: Vec<Vec<u8>> = bridge_messages
            .iter()
            .filter(|(channel, _)| channel == "control")
            .map(|(_, bytes)| bytes.clone())
            .collect();
        assert_eq!(reverse, [b"alpha".to_vec(), b"beta".to_vec()]);

        let (width, height) = (64u32, 48u32);
        let bgra_a = vec![0x21; (width * height * 4) as usize];
        let bgra_b = vec![0x83; (width * height * 4) as usize];
        let pcm: Vec<i16> = (0..480)
            .map(|sample| (((sample % 128) as i16) - 64) * 128)
            .collect();
        let index = |name: &str| {
            mappings
                .iter()
                .position(|mapping| mapping.name == name)
                .expect("mapped track") as u32
        };
        let media_deadline = Instant::now() + Duration::from_secs(20);
        let mut got_video: HashMap<u32, (Vec<u8>, Vec<u8>)> = HashMap::new();
        let mut got_audio = false;
        while got_video.len() < 2 || !got_audio {
            video_a
                .push_frame_with_metadata(VideoFrame::new(&bgra_a, width, height), b"meta-a")
                .expect("push video a");
            video_b
                .push_frame_with_metadata(VideoFrame::new(&bgra_b, width, height), b"meta-b")
                .expect("push video b");
            audio
                .push_frame(AudioFrame {
                    pcm: &pcm,
                    sample_rate: 48_000,
                    channels: 1,
                    frames: pcm.len() as u32,
                })
                .expect("push audio");

            while let Some(frame) = take_any(&shared.video) {
                let header = frame.header();
                assert_eq!((header.width, header.height), (width, height));
                assert_eq!(header.data_len as usize, bgra_a.len());
                got_video.insert(frame.track, (frame.bgra, frame.metadata));
            }
            while let Some(block) = take_any(&shared.audio) {
                assert_eq!(block.track, index("audio-a"));
                assert_eq!(block.sample_rate, 48_000);
                assert_eq!(block.channels, 1);
                assert!(!block.pcm.is_empty());
                got_audio = true;
            }
            assert!(
                Instant::now() < media_deadline,
                "real codecs did not deliver all decoded media"
            );
            thread::sleep(Duration::from_millis(30));
        }

        let (decoded_a, metadata_a) = got_video.get(&index("video-a")).expect("decoded video-a");
        let (decoded_b, metadata_b) = got_video.get(&index("video-b")).expect("decoded video-b");
        // VP8/H264 are lossy, so exact pixels are not asserted. Distinct luma
        // inputs should remain observably distinct after the real codec path.
        assert_ne!(
            decoded_a[0], decoded_b[0],
            "two decoded video lanes collapsed into one"
        );
        assert_eq!(
            metadata_a, b"meta-a",
            "same-kind lane A metadata was misattributed"
        );
        assert_eq!(
            metadata_b, b"meta-b",
            "same-kind lane B metadata was misattributed"
        );
        assert_ne!(observed(&shared.video), 0, "no frame reached the queue");

        let stats_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats: Value =
                serde_json::from_slice(&bridge.stats().expect("bridge stats")).expect("stats JSON");
            let entries = stats.as_array().expect("stats array");
            let video = entries.iter().any(|entry| {
                entry["type"] == "inbound-rtp"
                    && entry["kind"] == "video"
                    && entry["framesDecoded"].as_u64().unwrap_or(0) > 0
            });
            let audio = entries
                .iter()
                .any(|entry| entry["type"] == "inbound-rtp" && entry["kind"] == "audio");
            let pair = entries
                .iter()
                .any(|entry| entry["type"] == "candidate-pair");
            if video && audio && pair {
                break;
            }
            assert!(
                Instant::now() < stats_deadline,
                "bridge stats omitted live media/candidate data"
            );
            thread::sleep(Duration::from_millis(100));
        }

        // Fence admission before the remote tries one more frame. The queue is
        // then closed by shutdown, establishing that no late callback can become
        // observable after the finalizer returns.
        gate.close();
        video_a
            .push_frame_with_metadata(VideoFrame::new(&bgra_a, width, height), b"late")
            .expect("remote source can still push after local fence");
        bridge.shutdown();
        assert!(matches!(shared.video.take(|_| true), Taken::Closed));
        assert!(matches!(shared.audio.take(|_| true), Taken::Closed));
        assert!(matches!(shared.events.take(|_| true), Taken::Closed));
    }
}
