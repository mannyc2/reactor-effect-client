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
use std::time::{Duration, Instant};

const NOTIFY_EVENTS: u32 = 1;
const NOTIFY_VIDEO: u32 = 2;
const NOTIFY_AUDIO: u32 = 4;

const ABI_VERSION: u32 = 3;
const STATUS_OK: i32 = 0;
const STATUS_AGAIN: i32 = 1;
const STATUS_BUFFER_TOO_SMALL: i32 = 2;
const STATUS_CLOSED: i32 = 3;
const STATUS_INVALID: i32 = -1;
const STATUS_NATIVE: i32 = -2;
const STATUS_OVERFLOW: i32 = -3;

const CALL_PREPARE: u32 = 1;
const CALL_ANSWER: u32 = 2;
const CALL_DIRECTION: u32 = 3;
const CALL_MAX_BITRATE: u32 = 4;
const CALL_STATS: u32 = 5;
const CALL_MEDIA_SNAPSHOT: u32 = 6;

const CHANNEL_CONTROL: u32 = 0;
const CHANNEL_DATA: u32 = 1;

const CALL_BUFFER_MIN: usize = 4 * 1024 * 1024;
const ERROR_BUFFER_MIN: usize = 4096;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 262_144;
const MAX_BUFFERED_SEND_BYTES: u64 = 1_048_576;

#[derive(Debug, Clone)]
struct BridgeError {
    code: &'static str,
    message: String,
}

impl BridgeError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "InvalidInput",
            message: message.into(),
        }
    }

    fn native(message: impl Into<String>) -> Self {
        Self {
            code: "Native",
            message: message.into(),
        }
    }

    fn closed() -> Self {
        Self {
            code: "Closed",
            message: "native peer is closed".into(),
        }
    }

    fn overflow(message: impl Into<String>) -> Self {
        Self {
            code: "Overflow",
            message: message.into(),
        }
    }

    fn json(&self) -> Vec<u8> {
        serde_json::to_vec(&json!({ "code": self.code, "message": self.message })).unwrap_or_else(
            |_| br#"{"code":"Native","message":"failed to serialize native error"}"#.to_vec(),
        )
    }
}

impl From<reactor_webrtc::Error> for BridgeError {
    fn from(value: reactor_webrtc::Error) -> Self {
        Self::native(value.to_string())
    }
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

    fn enter(self: &Arc<Self>) -> Option<CallbackGuard> {
        if !self.accepting.load(Ordering::Acquire) {
            return None;
        }
        let mut active = lock(&self.active);
        if !self.accepting.load(Ordering::Acquire) {
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

#[derive(Debug)]
enum PushResult {
    Accepted,
    Closed,
    Overflow,
}

struct PacketQueue {
    inner: Mutex<PacketQueueInner>,
    wake: Condvar,
    max_items: usize,
    max_bytes: usize,
}

struct PacketQueueInner {
    packets: VecDeque<Vec<u8>>,
    // A size probe transfers the front packet into the reader's retained slot.
    // Producers may evict queued media, but must never replace this packet.
    // Retained packets count toward both bounds until copy or close.
    retained: Option<Vec<u8>>,
    bytes: usize,
    closed: bool,
}

impl PacketQueueInner {
    fn len(&self) -> usize {
        self.packets.len() + usize::from(self.retained.is_some())
    }
}

#[derive(Debug, Clone, Copy)]
struct QueueSnapshot {
    queued: usize,
    bytes: usize,
    closed: bool,
}

impl PacketQueue {
    fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(PacketQueueInner {
                packets: VecDeque::new(),
                retained: None,
                bytes: 0,
                closed: false,
            }),
            wake: Condvar::new(),
            max_items,
            max_bytes,
        }
    }

    fn push(&self, packet: Vec<u8>) -> PushResult {
        let mut inner = lock(&self.inner);
        if inner.closed {
            return PushResult::Closed;
        }
        if packet.len() > self.max_bytes
            || inner.len() >= self.max_items
            || inner.bytes.saturating_add(packet.len()) > self.max_bytes
        {
            return PushResult::Overflow;
        }
        inner.bytes += packet.len();
        inner.packets.push_back(packet);
        self.wake.notify_one();
        PushResult::Accepted
    }

    fn replace_with(&self, packet: Vec<u8>) {
        let mut inner = lock(&self.inner);
        if inner.closed {
            return;
        }
        inner.packets.clear();
        inner.bytes = inner.retained.as_ref().map_or(0, Vec::len);
        // The critical-event queue has room for its small terminal diagnostic
        // even with one maximum-sized transport event retained by its reader.
        if inner.len() < self.max_items
            && inner.bytes.saturating_add(packet.len()) <= self.max_bytes
        {
            inner.bytes += packet.len();
            inner.packets.push_back(packet);
        }
        self.wake.notify_all();
    }

    fn close(&self) {
        let mut inner = lock(&self.inner);
        inner.closed = true;
        inner.packets.clear();
        inner.retained = None;
        inner.bytes = 0;
        self.wake.notify_all();
    }

    fn snapshot(&self) -> QueueSnapshot {
        let inner = lock(&self.inner);
        QueueSnapshot {
            queued: inner.len(),
            bytes: inner.bytes,
            closed: inner.closed,
        }
    }

    fn poll(&self, timeout: Duration, capacity: usize) -> PollResult {
        let started = Instant::now();
        let mut inner = lock(&self.inner);
        loop {
            if inner.retained.is_none() {
                inner.retained = inner.packets.pop_front();
            }
            if let Some(packet) = inner.retained.as_ref() {
                if capacity < packet.len() {
                    return PollResult::Need(packet.len());
                }
                let packet = inner.retained.take().expect("retained packet exists");
                inner.bytes -= packet.len();
                return PollResult::Packet(packet);
            }
            if inner.closed {
                return PollResult::Closed;
            }
            if timeout.is_zero() {
                return PollResult::Again;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return PollResult::Again;
            }
            let (next, timed) = self
                .wake
                .wait_timeout(inner, remaining)
                .unwrap_or_else(|p| p.into_inner());
            inner = next;
            if timed.timed_out() && inner.packets.is_empty() {
                return PollResult::Again;
            }
        }
    }
}

enum PollResult {
    Packet(Vec<u8>),
    Need(usize),
    Again,
    Closed,
}


/// Spike B: media is stored typed, copied once from the libwebrtc callback
/// and once into JavaScript-owned memory by a synchronous take.
struct VideoItem {
    width: u32,
    height: u32,
    frame_id: u64,
    timestamp_us: u64,
    track: u32,
    bgra: Vec<u8>,
    meta: Vec<u8>,
}

struct AudioItem {
    sample_rate: u32,
    channels: u32,
    track: u32,
    pcm: Vec<i16>,
}

#[repr(C)]
pub struct VideoHeader {
    width: u32,
    height: u32,
    data_len: u32,
    meta_len: u32,
    frame_id: u64,
    timestamp_us: u64,
    track: u32,
    reserved: u32,
}

#[repr(C)]
pub struct AudioHeader {
    sample_rate: u32,
    channels: u32,
    samples: u32,
    track: u32,
}

struct TypedInner<T> {
    items: VecDeque<(T, usize)>,
    bytes: usize,
    closed: bool,
}

struct TypedQueue<T> {
    inner: Mutex<TypedInner<T>>,
    max_items: usize,
    max_bytes: usize,
    observed: AtomicU64,
    dropped: AtomicU64,
    taken: AtomicU64,
}

enum Take {
    Copied,
    TooSmall,
}

impl<T> TypedQueue<T> {
    fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(TypedInner {
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

    /// Drop-oldest with every eviction counted. Returns whether the reader
    /// should be signalled.
    fn push_drop_oldest(&self, item: T, size: usize) -> bool {
        let mut inner = lock(&self.inner);
        if inner.closed {
            return false;
        }
        self.observed.fetch_add(1, Ordering::Relaxed);
        if size > self.max_bytes {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        while !inner.items.is_empty()
            && (inner.items.len() >= self.max_items || inner.bytes + size > self.max_bytes)
        {
            if let Some((_, old)) = inner.items.pop_front() {
                inner.bytes -= old;
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        inner.bytes += size;
        inner.items.push_back((item, size));
        true
    }

    fn take(&self, copy: impl FnOnce(&T) -> Take) -> i32 {
        let mut inner = lock(&self.inner);
        let Some((front, _)) = inner.items.front() else {
            return if inner.closed { STATUS_CLOSED } else { STATUS_AGAIN };
        };
        match copy(front) {
            Take::TooSmall => STATUS_BUFFER_TOO_SMALL,
            Take::Copied => {
                let (_, size) = inner.items.pop_front().expect("front exists");
                inner.bytes -= size;
                self.taken.fetch_add(1, Ordering::Relaxed);
                STATUS_OK
            }
        }
    }

    fn close(&self) {
        let mut inner = lock(&self.inner);
        let discarded = inner.items.len() as u64;
        self.dropped.fetch_add(discarded, Ordering::Relaxed);
        inner.items.clear();
        inner.bytes = 0;
        inner.closed = true;
    }

    fn snapshot(&self) -> Value {
        let inner = lock(&self.inner);
        json!({
            "observed": self.observed.load(Ordering::Relaxed).to_string(),
            "dropped": self.dropped.load(Ordering::Relaxed).to_string(),
            "delivered": self.taken.load(Ordering::Relaxed).to_string(),
            "queued": inner.items.len(),
            "queuedBytes": inner.bytes,
        })
    }
}

type NotifyFn = extern "C" fn(u32);

#[derive(Default)]
struct NotifyState {
    pending: u32,
    callback: Option<NotifyFn>,
    closed: bool,
    delivered: u64,
    coalesced: u64,
}

/// One thread per peer owns the only blocking hand-off to JavaScript. When
/// the event loop stalls, this thread waits inside Koffi's threadsafe call;
/// libwebrtc threads only set bits here and never wait on JavaScript.
#[derive(Default)]
struct Notifier {
    state: Mutex<NotifyState>,
    wake: Condvar,
}

impl Notifier {
    fn signal(&self, bit: u32) {
        let mut state = lock(&self.state);
        if state.pending & bit == 0 {
            state.pending |= bit;
            self.wake.notify_one();
        } else {
            state.coalesced += 1;
        }
    }

    fn close(&self) {
        let mut state = lock(&self.state);
        state.closed = true;
        self.wake.notify_all();
    }

    fn run(&self) {
        loop {
            let (mask, callback) = {
                let mut state = lock(&self.state);
                while state.pending == 0 && !state.closed {
                    state = self.wake.wait(state).unwrap_or_else(|p| p.into_inner());
                }
                if state.closed {
                    return;
                }
                let mask = std::mem::take(&mut state.pending);
                state.delivered += 1;
                (mask, state.callback)
            };
            if let Some(callback) = callback {
                callback(mask);
            }
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
    events: PacketQueue,
    video: TypedQueue<VideoItem>,
    audio: TypedQueue<AudioItem>,
    notifier: Notifier,
    bindings: Mutex<Bindings>,
    remote_tracks: Mutex<Vec<RemoteTrack>>,
    overflowed: AtomicBool,
}

impl Shared {
    fn new(gate: Arc<CallbackGate>) -> Self {
        Self {
            gate,
            events: PacketQueue::new(1024, 16 * 1024 * 1024),
            video: TypedQueue::new(8, 64 * 1024 * 1024),
            audio: TypedQueue::new(256, 4 * 1024 * 1024),
            notifier: Notifier::default(),
            bindings: Mutex::new(Bindings::default()),
            remote_tracks: Mutex::new(Vec::new()),
            overflowed: AtomicBool::new(false),
        }
    }

    fn emit(&self, header: Value, payload: &[u8]) {
        let packet = packet(header, payload);
        match self.events.push(packet) {
            PushResult::Accepted => self.notifier.signal(NOTIFY_EVENTS),
            PushResult::Closed => {}
            PushResult::Overflow => self.fail_overflow(),
        }
    }

    fn fail_overflow(&self) {
        if self.overflowed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.gate.close();
        self.events.replace_with(packet(
            json!({
                "type": "error",
                "code": "Overflow",
                "message": "native transport event queue overflowed; connection retired"
            }),
            &[],
        ));
        self.notifier.signal(NOTIFY_EVENTS);
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
            self.emit(
                json!({
                    "type": "error",
                    "code": "Protocol",
                    "message": "received native track without a declared receive mapping"
                }),
                &[],
            );
            return;
        };

        match &track {
            RemoteTrack::Video(video) => {
                let shared = Arc::clone(self);
                let frame_binding = binding.clone();
                video.on_frame(move |frame| {
                    let Some(_guard) = shared.gate.enter() else {
                        return;
                    };
                    let (frame_id, timestamp_us, meta) = frame
                        .metadata
                        .as_ref()
                        .map(|m| (m.frame_id, m.capture_time_us, m.user_data.clone()))
                        .unwrap_or((0, 0, Vec::new()));
                    let size = frame.bgra.len() + meta.len();
                    let item = VideoItem {
                        width: frame.width,
                        height: frame.height,
                        frame_id,
                        timestamp_us,
                        track: frame_binding.index,
                        bgra: frame.bgra.to_vec(),
                        meta,
                    };
                    if shared.video.push_drop_oldest(item, size) {
                        shared.notifier.signal(NOTIFY_VIDEO);
                    }
                });
            }
            RemoteTrack::Audio(audio) => {
                let shared = Arc::clone(self);
                let frame_binding = binding.clone();
                audio.on_frame(move |frame| {
                    let Some(_guard) = shared.gate.enter() else {
                        return;
                    };
                    let item = AudioItem {
                        sample_rate: frame.sample_rate,
                        channels: frame.channels,
                        track: frame_binding.index,
                        pcm: frame.pcm.to_vec(),
                    };
                    if shared.audio.push_drop_oldest(item, frame.pcm.len() * 2) {
                        shared.notifier.signal(NOTIFY_AUDIO);
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

// Spike B: one factory per process, created on first use and never destroyed,
// as reactor-webrtc docs/architecture.md "One factory per process" requires.
static FACTORY: Mutex<Option<&'static PeerConnectionFactory>> = Mutex::new(None);

fn shared_factory() -> Result<&'static PeerConnectionFactory, BridgeError> {
    let mut slot = lock(&FACTORY);
    if let Some(factory) = *slot {
        return Ok(factory);
    }
    let factory = PeerConnectionFactory::builder()
        .with_synthetic_adm()
        .build()
        .map_err(BridgeError::from)?;
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

        let factory = shared_factory()?;

        let observer = observer(Arc::clone(&self.shared));
        let peer = factory
            .create_peer_connection(&config, observer)
            .map_err(BridgeError::from)?;

        let mut control = peer
            .create_data_channel("control")
            .map_err(BridgeError::from)?;
        let mut data = peer
            .create_data_channel("data")
            .map_err(BridgeError::from)?;
        wire_channel(&mut control, "control", Arc::clone(&self.shared));
        wire_channel(&mut data, "data", Arc::clone(&self.shared));

        let mut tracks = HashMap::with_capacity(request.tracks.len());
        let mut order = Vec::with_capacity(request.tracks.len());
        for spec in request.tracks {
            let kind = media_kind(&spec.kind)?;
            let direction = direction(&spec.direction)?;
            let transceiver = peer
                .add_transceiver(kind, direction)
                .map_err(BridgeError::from)?;
            order.push(spec.name.clone());
            tracks.insert(spec.name.clone(), NativeTrack { spec, transceiver });
        }

        let offer = peer.create_offer().map_err(BridgeError::from)?;
        peer.set_local_description(&offer)
            .map_err(BridgeError::from)?;

        let mut mappings = Vec::with_capacity(order.len());
        for name in &order {
            let entry = tracks
                .get(name)
                .ok_or_else(|| BridgeError::native("track map changed while preparing"))?;
            let mid = entry.transceiver.mid().ok_or_else(|| {
                BridgeError::native(format!("missing MID after local description: {name}"))
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

        serde_json::to_vec(&json!({ "sdp": offer.sdp, "mapping": mappings }))
            .map_err(|e| BridgeError::native(format!("serialize prepare response: {e}")))
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
            .map_err(BridgeError::from)?;
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
            .map_err(BridgeError::from)?;
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
            .map_err(BridgeError::from)?;
        Ok(b"{}".to_vec())
    }

    fn stats(&mut self) -> Result<Vec<u8>, BridgeError> {
        let report = self
            .require_peer()?
            .get_stats()
            .map_err(BridgeError::from)?;
        serde_json::to_vec(&stats_json(report))
            .map_err(|e| BridgeError::native(format!("serialize stats response: {e}")))
    }

    fn media_snapshot(&self) -> Result<Vec<u8>, BridgeError> {
        let notifier = lock(&self.shared.notifier.state);
        serde_json::to_vec(&json!({
            "closed": !self.shared.gate.accepting.load(Ordering::Acquire),
            "queuedControl": self.shared.events.snapshot().queued,
            "video": self.shared.video.snapshot(),
            "audio": self.shared.audio.snapshot(),
            "notifications": notifier.delivered,
            "coalescedSignals": notifier.coalesced,
        }))
        .map_err(|e| BridgeError::native(format!("serialize media snapshot: {e}")))
    }

    fn send(&mut self, channel: u32, bytes: &[u8]) -> Result<(), BridgeError> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(BridgeError::overflow(format!(
                "data channel message exceeds {MAX_MESSAGE_BYTES} bytes"
            )));
        }
        let channels = self.channels.as_ref().ok_or_else(BridgeError::closed)?;
        let channel = match channel {
            CHANNEL_CONTROL => &channels.control,
            CHANNEL_DATA => &channels.data,
            _ => return Err(BridgeError::invalid("unknown data channel")),
        };
        if channel.state() != DataChannelState::Open {
            return Err(BridgeError::closed());
        }
        if channel.buffered_amount().saturating_add(bytes.len() as u64) > MAX_BUFFERED_SEND_BYTES {
            return Err(BridgeError::overflow(
                "native data channel buffered amount bound exceeded",
            ));
        }
        channel.send(bytes, true).map_err(BridgeError::from)
    }

    fn call(&mut self, operation: u32, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        match operation {
            CALL_PREPARE => self.prepare(request),
            CALL_ANSWER => self.answer(request),
            CALL_DIRECTION => self.set_direction(request),
            CALL_MAX_BITRATE => self.max_bitrate(request),
            CALL_STATS => self.stats(),
            CALL_MEDIA_SNAPSHOT => self.media_snapshot(),
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
                shared.emit(
                    json!({
                        "type": "error",
                        "code": "Protocol",
                        "message": format!("{name} data channel delivered a nonbinary message")
                    }),
                    &[],
                );
                return;
            }
            if bytes.len() > MAX_MESSAGE_BYTES {
                shared.emit(
                    json!({
                        "type": "error",
                        "code": "Overflow",
                        "message": format!("{name} data channel message exceeds local bound")
                    }),
                    &[],
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
                let result = if shared.gate.accepting.load(Ordering::Acquire) {
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
                let result = if shared.gate.accepting.load(Ordering::Acquire) {
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

#[repr(C)]
pub struct ReactorEffectPeer {
    gate: Arc<CallbackGate>,
    shared: Arc<Shared>,
    commands: Sender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    notifier: Mutex<Option<JoinHandle<()>>>,
}

impl ReactorEffectPeer {
    fn request(&self, operation: u32, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        if !self.gate.accepting.load(Ordering::Acquire) {
            return Err(BridgeError::closed());
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
        if !self.gate.accepting.load(Ordering::Acquire) {
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
        self.gate.close();
        self.shared.close_queues();
    }

    fn shutdown(&self) -> Result<(), BridgeError> {
        self.close();
        let mut worker = lock(&self.worker);
        let Some(handle) = worker.take() else {
            return Ok(());
        };
        let _ = self.commands.send(Command::Shutdown);
        let joined = handle
            .join()
            .map_err(|_| BridgeError::native("native owner thread panicked"));
        // The notifier may be waiting for JavaScript to run its last callback;
        // shutdown runs as a Koffi async call so the event loop stays free.
        self.shared.notifier.close();
        if let Some(notifier) = lock(&self.notifier).take() {
            let _ = notifier.join();
        }
        joined
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
pub extern "C" fn reactor_effect_peer_create() -> *mut ReactorEffectPeer {
    catch_unwind(AssertUnwindSafe(|| {
        let gate = Arc::new(CallbackGate::new());
        let shared = Arc::new(Shared::new(Arc::clone(&gate)));
        let (commands, receiver) = mpsc::channel();
        let worker_shared = Arc::clone(&shared);
        let handle = match thread::Builder::new()
            .name("reactor-effect-native".into())
            .spawn(move || worker_loop(worker_shared, receiver))
        {
            Ok(handle) => handle,
            Err(_) => return ptr::null_mut(),
        };
        Box::into_raw(Box::new(ReactorEffectPeer {
            gate,
            shared,
            commands,
            worker: Mutex::new(Some(handle)),
            notifier: Mutex::new(None),
        }))
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
/// Invoke one serialized peer operation through the C ABI.
///
/// # Safety
/// `peer` must be a live handle returned by [`reactor_effect_peer_create`].
/// Non-null input/output pointers must reference at least their declared byte
/// lengths, and `response_len` must be writable for one `usize`.
pub unsafe extern "C" fn reactor_effect_peer_call(
    peer: *mut ReactorEffectPeer,
    operation: u32,
    request: *const u8,
    request_len: usize,
    response: *mut u8,
    response_cap: usize,
    response_len: *mut usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer)?;
        require_call_buffer(response, response_cap, response_len)?;
        let request = input(request, request_len)?;
        if request.len() > MAX_REQUEST_BYTES {
            let native = BridgeError::overflow("native request exceeds 1 MiB");
            let bytes = native.json();
            copy_out(&bytes, response, response_cap, response_len);
            return Ok(status_for(&native));
        }
        let (status, bytes) = match peer.request(operation, request) {
            Ok(bytes) => (STATUS_OK, bytes),
            Err(error) => (status_for(&error), error.json()),
        };
        if bytes.len() > response_cap {
            let native = BridgeError::overflow("native call response exceeds 4 MiB");
            let bytes = native.json();
            copy_out(&bytes, response, response_cap, response_len);
            return Ok(status_for(&native));
        }
        copy_out(&bytes, response, response_cap, response_len);
        Ok(status)
    })
}

#[no_mangle]
/// Send one binary SCTP message on a bridge-owned channel.
///
/// # Safety
/// `peer` must be live. `data` must reference `data_len` readable bytes when
/// nonempty; `error` must reference `error_cap` writable bytes and `error_len`
/// must be writable for one `usize`.
pub unsafe extern "C" fn reactor_effect_peer_send(
    peer: *mut ReactorEffectPeer,
    channel: u32,
    data: *const u8,
    data_len: usize,
    error: *mut u8,
    error_cap: usize,
    error_len: *mut usize,
) -> i32 {
    ffi_status(|| {
        require_error_buffer(error, error_cap, error_len)?;
        let peer = peer_ref(peer)?;
        let data = input(data, data_len)?;
        match peer.send(channel, data) {
            Ok(()) => {
                copy_out(&[], error, error_cap, error_len);
                Ok(STATUS_OK)
            }
            Err(native) => {
                let bytes = native.json();
                copy_out(&bytes, error, error_cap, error_len);
                Ok(status_for(&native))
            }
        }
    })
}

#[no_mangle]
/// Poll the critical transport event queue.
///
/// # Safety
/// `peer` must be live. When `out_cap` is nonzero, `out` must reference that
/// many writable bytes; `out_len` must be writable for one `usize`.
pub unsafe extern "C" fn reactor_effect_peer_poll_event(
    peer: *mut ReactorEffectPeer,
    timeout_ms: u32,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_poll(
        peer,
        &|p| &p.shared.events,
        timeout_ms,
        out,
        out_cap,
        out_len,
    )
}

#[no_mangle]
/// Register the readiness callback and start this peer's notifier thread.
///
/// # Safety
/// `peer` must be live; `callback` must stay callable until shutdown returns.
pub unsafe extern "C" fn reactor_effect_peer_set_notify(
    peer: *mut ReactorEffectPeer,
    callback: Option<NotifyFn>,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer)?;
        let Some(callback) = callback else {
            return Err(FfiError::Invalid);
        };
        let mut slot = lock(&peer.notifier);
        if slot.is_some() {
            return Err(FfiError::Invalid);
        }
        lock(&peer.shared.notifier.state).callback = Some(callback);
        let shared = Arc::clone(&peer.shared);
        let handle = thread::Builder::new()
            .name("reactor-effect-notify".into())
            .spawn(move || shared.notifier.run())
            .map_err(|_| FfiError::Status(STATUS_NATIVE))?;
        *slot = Some(handle);
        Ok(STATUS_OK)
    })
}

#[no_mangle]
/// Synchronous, nonblocking: copy the oldest decoded frame into caller memory.
/// BUFFER_TOO_SMALL fills `header` with the required sizes and keeps the frame.
///
/// # Safety
/// `peer` must be live; `header` writable; buffers valid for their capacities.
pub unsafe extern "C" fn reactor_effect_peer_take_video(
    peer: *mut ReactorEffectPeer,
    header: *mut VideoHeader,
    bgra: *mut u8,
    bgra_cap: usize,
    meta: *mut u8,
    meta_cap: usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer)?;
        if header.is_null() {
            return Err(FfiError::Invalid);
        }
        Ok(peer.shared.video.take(|item| {
            *header = VideoHeader {
                width: item.width,
                height: item.height,
                data_len: item.bgra.len() as u32,
                meta_len: item.meta.len() as u32,
                frame_id: item.frame_id,
                timestamp_us: item.timestamp_us,
                track: item.track,
                reserved: 0,
            };
            if bgra.is_null() || bgra_cap < item.bgra.len() || meta_cap < item.meta.len()
                || (!item.meta.is_empty() && meta.is_null())
            {
                return Take::TooSmall;
            }
            ptr::copy_nonoverlapping(item.bgra.as_ptr(), bgra, item.bgra.len());
            if !item.meta.is_empty() {
                ptr::copy_nonoverlapping(item.meta.as_ptr(), meta, item.meta.len());
            }
            Take::Copied
        }))
    })
}

#[no_mangle]
/// Synchronous, nonblocking: copy the oldest PCM block (native-endian i16).
///
/// # Safety
/// `peer` must be live; `header` writable; `pcm` valid for `pcm_cap` samples.
pub unsafe extern "C" fn reactor_effect_peer_take_audio(
    peer: *mut ReactorEffectPeer,
    header: *mut AudioHeader,
    pcm: *mut i16,
    pcm_cap: usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer)?;
        if header.is_null() {
            return Err(FfiError::Invalid);
        }
        Ok(peer.shared.audio.take(|item| {
            *header = AudioHeader {
                sample_rate: item.sample_rate,
                channels: item.channels,
                samples: item.pcm.len() as u32,
                track: item.track,
            };
            if pcm.is_null() || pcm_cap < item.pcm.len() {
                return Take::TooSmall;
            }
            ptr::copy_nonoverlapping(item.pcm.as_ptr(), pcm, item.pcm.len());
            Take::Copied
        }))
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
/// Join native ownership after callback admission has been fenced.
///
/// # Safety
/// `peer` must be live. `error` must reference `error_cap` writable bytes and
/// `error_len` must be writable for one `usize`.
pub unsafe extern "C" fn reactor_effect_peer_shutdown(
    peer: *mut ReactorEffectPeer,
    error: *mut u8,
    error_cap: usize,
    error_len: *mut usize,
) -> i32 {
    ffi_status(|| {
        require_error_buffer(error, error_cap, error_len)?;
        let peer = peer_ref(peer)?;
        match peer.shutdown() {
            Ok(()) => {
                copy_out(&[], error, error_cap, error_len);
                Ok(STATUS_OK)
            }
            Err(native) => {
                let bytes = native.json();
                copy_out(&bytes, error, error_cap, error_len);
                Ok(status_for(&native))
            }
        }
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
pub unsafe extern "C" fn reactor_effect_peer_destroy(peer: *mut ReactorEffectPeer) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if peer.is_null() {
            return;
        }
        let peer = Box::from_raw(peer);
        let _ = peer.shutdown();
    }));
}

unsafe fn ffi_poll(
    peer: *mut ReactorEffectPeer,
    queue: &dyn Fn(&ReactorEffectPeer) -> &PacketQueue,
    timeout_ms: u32,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_status(|| {
        let peer = peer_ref(peer)?;
        if out_len.is_null() || (out_cap != 0 && out.is_null()) {
            return Err(FfiError::Invalid);
        }
        match queue(peer).poll(Duration::from_millis(timeout_ms as u64), out_cap) {
            PollResult::Packet(packet) => {
                if out.is_null() && !packet.is_empty() {
                    return Err(FfiError::Invalid);
                }
                copy_out(&packet, out, out_cap, out_len);
                Ok(STATUS_OK)
            }
            PollResult::Need(required) => {
                *out_len = required;
                Ok(STATUS_BUFFER_TOO_SMALL)
            }
            PollResult::Again => {
                *out_len = 0;
                Ok(STATUS_AGAIN)
            }
            PollResult::Closed => {
                *out_len = 0;
                Ok(STATUS_CLOSED)
            }
        }
    })
}

enum FfiError {
    Invalid,
    Status(i32),
}

fn ffi_status(body: impl FnOnce() -> Result<i32, FfiError>) -> i32 {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(status)) => status,
        Ok(Err(FfiError::Invalid)) => STATUS_INVALID,
        Ok(Err(FfiError::Status(status))) => status,
        Err(_) => STATUS_NATIVE,
    }
}

unsafe fn peer_ref<'a>(peer: *mut ReactorEffectPeer) -> Result<&'a ReactorEffectPeer, FfiError> {
    peer.as_ref().ok_or(FfiError::Invalid)
}

unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a [u8], FfiError> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(FfiError::Invalid);
    }
    Ok(slice::from_raw_parts(data, len))
}

unsafe fn require_call_buffer(out: *mut u8, cap: usize, len: *mut usize) -> Result<(), FfiError> {
    if len.is_null() || out.is_null() {
        return Err(FfiError::Invalid);
    }
    if cap < CALL_BUFFER_MIN {
        *len = CALL_BUFFER_MIN;
        return Err(FfiError::Status(STATUS_BUFFER_TOO_SMALL));
    }
    Ok(())
}

unsafe fn require_error_buffer(out: *mut u8, cap: usize, len: *mut usize) -> Result<(), FfiError> {
    if len.is_null() || out.is_null() {
        return Err(FfiError::Invalid);
    }
    if cap < ERROR_BUFFER_MIN {
        *len = ERROR_BUFFER_MIN;
        return Err(FfiError::Status(STATUS_BUFFER_TOO_SMALL));
    }
    Ok(())
}

unsafe fn copy_out(bytes: &[u8], out: *mut u8, cap: usize, len: *mut usize) {
    *len = bytes.len();
    if !bytes.is_empty() {
        debug_assert!(!out.is_null());
        debug_assert!(cap >= bytes.len());
        ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len());
    }
}

fn status_for(error: &BridgeError) -> i32 {
    match error.code {
        "InvalidInput" | "Protocol" => STATUS_INVALID,
        "Closed" => STATUS_CLOSED,
        "Overflow" => STATUS_OVERFLOW,
        _ => STATUS_NATIVE,
    }
}


