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

const ABI_VERSION: u32 = 2;
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

#[derive(Default)]
struct MediaCounters {
    observed: AtomicU64,
    dropped: AtomicU64,
}

struct MediaQueue {
    queue: PacketQueue,
    counters: MediaCounters,
}

impl MediaQueue {
    fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            queue: PacketQueue::new(max_items, max_bytes),
            counters: MediaCounters::default(),
        }
    }

    fn push_drop_oldest(&self, packet: Vec<u8>) {
        let mut inner = lock(&self.queue.inner);
        if inner.closed {
            return;
        }
        self.counters.observed.fetch_add(1, Ordering::Relaxed);
        if packet.len() > self.queue.max_bytes {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        while !inner.packets.is_empty()
            && (inner.len() >= self.queue.max_items
                || inner.bytes.saturating_add(packet.len()) > self.queue.max_bytes)
        {
            if let Some(old) = inner.packets.pop_front() {
                inner.bytes -= old.len();
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        // A reader can occupy the remaining budget. Drop incoming media rather
        // than evict retained bytes or exceed either queue bound.
        if inner.len() >= self.queue.max_items
            || inner.bytes.saturating_add(packet.len()) > self.queue.max_bytes
        {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        inner.bytes += packet.len();
        inner.packets.push_back(packet);
        self.queue.wake.notify_one();
    }

    fn snapshot(&self) -> Value {
        let inner = self.queue.snapshot();
        let observed = self.counters.observed.load(Ordering::Relaxed);
        let dropped = self.counters.dropped.load(Ordering::Relaxed);
        let queued = inner.queued as u64;
        let delivered = observed.saturating_sub(dropped).saturating_sub(queued);
        json!({
            "observed": observed.to_string(),
            "dropped": dropped.to_string(),
            "delivered": delivered.to_string(),
            "queued": inner.queued,
            "queuedBytes": inner.bytes,
            "closed": inner.closed,
        })
    }

    fn close(&self) {
        let mut inner = lock(&self.queue.inner);
        let discarded = inner.len() as u64;
        if discarded != 0 {
            self.counters
                .dropped
                .fetch_add(discarded, Ordering::Relaxed);
        }
        inner.closed = true;
        inner.packets.clear();
        inner.retained = None;
        inner.bytes = 0;
        self.queue.wake.notify_all();
    }
}

#[derive(Clone)]
struct Binding {
    name: String,
    mid: String,
}

#[derive(Default)]
struct Bindings {
    video: VecDeque<Binding>,
    audio: VecDeque<Binding>,
}

struct Shared {
    gate: Arc<CallbackGate>,
    events: PacketQueue,
    video: MediaQueue,
    audio: MediaQueue,
    bindings: Mutex<Bindings>,
    remote_tracks: Mutex<Vec<RemoteTrack>>,
    overflowed: AtomicBool,
}

impl Shared {
    fn new(gate: Arc<CallbackGate>) -> Self {
        Self {
            gate,
            events: PacketQueue::new(1024, 16 * 1024 * 1024),
            video: MediaQueue::new(8, 64 * 1024 * 1024),
            audio: MediaQueue::new(256, 4 * 1024 * 1024),
            bindings: Mutex::new(Bindings::default()),
            remote_tracks: Mutex::new(Vec::new()),
            overflowed: AtomicBool::new(false),
        }
    }

    fn emit(&self, header: Value, payload: &[u8]) {
        let packet = packet(header, payload);
        match self.events.push(packet) {
            PushResult::Accepted | PushResult::Closed => {}
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
    }

    fn set_bindings(&self, mappings: &[MappingSpec]) {
        let mut bindings = lock(&self.bindings);
        bindings.video.clear();
        bindings.audio.clear();
        for mapping in mappings.iter().filter(|m| m.direction == "recvonly") {
            let binding = Binding {
                name: mapping.name.clone(),
                mid: mapping.mid.clone(),
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
                    let (frame_id, timestamp_micros, metadata) = frame
                        .metadata
                        .as_ref()
                        .map(|m| (m.frame_id, m.capture_time_us, m.user_data.as_slice()))
                        .unwrap_or((0, 0, &[]));
                    let header = json!({
                        "type": "video",
                        "track": frame_binding.name,
                        "mid": frame_binding.mid,
                        "width": frame.width,
                        "height": frame.height,
                        "frameId": frame_id.to_string(),
                        "timestampMicros": timestamp_micros.to_string(),
                        "dataLength": frame.bgra.len(),
                        "metadataLength": metadata.len(),
                        "format": "BGRA"
                    });
                    let mut payload = Vec::with_capacity(frame.bgra.len() + metadata.len());
                    payload.extend_from_slice(frame.bgra);
                    payload.extend_from_slice(metadata);
                    shared.video.push_drop_oldest(packet(header, &payload));
                });
            }
            RemoteTrack::Audio(audio) => {
                let shared = Arc::clone(self);
                let frame_binding = binding.clone();
                audio.on_frame(move |frame| {
                    let Some(_guard) = shared.gate.enter() else {
                        return;
                    };
                    let mut payload = Vec::with_capacity(frame.pcm.len() * 2);
                    for sample in frame.pcm {
                        payload.extend_from_slice(&sample.to_le_bytes());
                    }
                    shared.audio.push_drop_oldest(packet(
                        json!({
                            "type": "audio",
                            "track": frame_binding.name,
                            "mid": frame_binding.mid,
                            "sampleRate": frame.sample_rate,
                            "channels": frame.channels,
                            "frames": frame.frames,
                            "samples": frame.pcm.len(),
                            "format": "s16le"
                        }),
                        &payload,
                    ));
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

struct WorkerState {
    shared: Arc<Shared>,
    factory: Option<PeerConnectionFactory>,
    peer: Option<PeerConnection>,
    channels: Option<Channels>,
    tracks: HashMap<String, NativeTrack>,
}

impl WorkerState {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            factory: None,
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

        let factory = PeerConnectionFactory::builder()
            .with_synthetic_adm()
            .build()
            .map_err(BridgeError::from)?;

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

        self.factory = Some(factory);
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
        let control = self.shared.events.snapshot();
        let video = self.shared.video.snapshot();
        let audio = self.shared.audio.snapshot();
        let video_bytes = video
            .get("queuedBytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let audio_bytes = audio
            .get("queuedBytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        serde_json::to_vec(&json!({
            "closed": !self.shared.gate.accepting.load(Ordering::Acquire),
            "queuedControl": control.queued,
            "queuedVideo": video.get("queued").and_then(Value::as_u64).unwrap_or(0),
            "queuedAudio": audio.get("queued").and_then(Value::as_u64).unwrap_or(0),
            "queuedBytes": (control.bytes as u64).saturating_add(video_bytes).saturating_add(audio_bytes),
            "droppedVideo": video.get("dropped").and_then(Value::as_str).unwrap_or("0"),
            "droppedAudio": audio.get("dropped").and_then(Value::as_str).unwrap_or("0"),
            "deliveredVideo": video.get("delivered").and_then(Value::as_str).unwrap_or("0"),
            "deliveredAudio": audio.get("delivered").and_then(Value::as_str).unwrap_or("0"),
            "pendingRequests": 0,
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
        self.factory.take();
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
        handle
            .join()
            .map_err(|_| BridgeError::native("native owner thread panicked"))
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
/// Poll the decoded-video queue.
///
/// # Safety
/// `peer` must be live. When `out_cap` is nonzero, `out` must reference that
/// many writable bytes; `out_len` must be writable for one `usize`.
pub unsafe extern "C" fn reactor_effect_peer_poll_video(
    peer: *mut ReactorEffectPeer,
    timeout_ms: u32,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_poll(
        peer,
        &|p| &p.shared.video.queue,
        timeout_ms,
        out,
        out_cap,
        out_len,
    )
}

#[no_mangle]
/// Poll the decoded-audio queue.
///
/// # Safety
/// `peer` must be live. When `out_cap` is nonzero, `out` must reference that
/// many writable bytes; `out_len` must be writable for one `usize`.
pub unsafe extern "C" fn reactor_effect_peer_poll_audio(
    peer: *mut ReactorEffectPeer,
    timeout_ms: u32,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_poll(
        peer,
        &|p| &p.shared.audio.queue,
        timeout_ms,
        out,
        out_cap,
        out_len,
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use reactor_webrtc::{
        AudioFrame, AudioTrackOptions, AudioTrackSource, IceCandidate, VideoFrame,
    };
    use std::sync::atomic::AtomicBool;

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

    fn drain_bridge_events(
        shared: &Shared,
        answerer: &PeerConnection,
        bridge_connected: &mut bool,
        bridge_messages: &mut Vec<(String, Vec<u8>)>,
    ) {
        loop {
            let packet = match shared.events.poll(Duration::ZERO, 2 * 1024 * 1024) {
                PollResult::Packet(packet) => packet,
                PollResult::Again | PollResult::Closed => break,
                PollResult::Need(size) => panic!("unexpected oversized bridge event: {size}"),
            };
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

    #[test]
    fn packet_framing_is_length_prefixed_and_lossless() {
        let payload = [0, 1, 2, 255];
        let packet = packet(json!({ "type": "message", "channel": "data" }), &payload);
        let header_len = u32::from_le_bytes(packet[0..4].try_into().unwrap()) as usize;
        let header: Value = serde_json::from_slice(&packet[4..4 + header_len]).unwrap();
        assert_eq!(header["type"], "message");
        assert_eq!(&packet[4 + header_len..], payload);
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
    fn media_queue_drops_oldest_under_pressure() {
        let queue = MediaQueue::new(2, 32);
        queue.push_drop_oldest(vec![1; 8]);
        queue.push_drop_oldest(vec![2; 8]);
        queue.push_drop_oldest(vec![3; 8]);
        assert_eq!(queue.counters.observed.load(Ordering::Relaxed), 3);
        assert_eq!(queue.counters.dropped.load(Ordering::Relaxed), 1);
        match queue.queue.poll(Duration::ZERO, 32) {
            PollResult::Packet(packet) => assert_eq!(packet, vec![2; 8]),
            _ => panic!("expected retained packet"),
        }
    }

    #[test]
    fn media_probe_owns_packet_across_differently_sized_eviction() {
        // Both directions matter: a larger successor formerly failed the copy,
        // while a smaller successor changed the size after JS allocated output.
        for (first_size, replacement_size) in [(8, 13), (13, 8)] {
            let queue = MediaQueue::new(2, 32);
            queue.push_drop_oldest(vec![1; first_size]);
            queue.push_drop_oldest(vec![2; 7]);
            assert!(matches!(
                queue.queue.poll(Duration::ZERO, 0),
                PollResult::Need(size) if size == first_size
            ));

            // The producer runs between the size probe and the copying call.
            queue.push_drop_oldest(vec![3; replacement_size]);
            assert_eq!(queue.counters.dropped.load(Ordering::Relaxed), 1);
            let snapshot = queue.queue.snapshot();
            assert_eq!(snapshot.queued, 2);
            assert_eq!(snapshot.bytes, first_size + replacement_size);
            match queue.queue.poll(Duration::ZERO, first_size) {
                PollResult::Packet(bytes) => assert_eq!(bytes, vec![1; first_size]),
                _ => panic!("producer evicted a packet already retained by a reader"),
            }
            match queue.queue.poll(Duration::ZERO, replacement_size) {
                PollResult::Packet(bytes) => assert_eq!(bytes, vec![3; replacement_size]),
                _ => panic!("replacement packet was lost or reordered"),
            }
        }
    }

    #[test]
    fn retained_media_stays_within_capacity_and_close_discards_it() {
        let queue = MediaQueue::new(1, 8);
        queue.push_drop_oldest(vec![1; 8]);
        assert!(matches!(
            queue.queue.poll(Duration::ZERO, 0),
            PollResult::Need(8)
        ));
        queue.push_drop_oldest(vec![2; 7]);
        assert_eq!(queue.queue.snapshot().bytes, 8);
        assert_eq!(queue.queue.snapshot().queued, 1);
        assert_eq!(queue.counters.dropped.load(Ordering::Relaxed), 1);
        queue.close();
        assert_eq!(queue.queue.snapshot().bytes, 0);
        assert_eq!(queue.counters.dropped.load(Ordering::Relaxed), 2);
        assert!(matches!(
            queue.queue.poll(Duration::ZERO, 8),
            PollResult::Closed
        ));
    }

    #[test]
    fn critical_overflow_preserves_a_probed_packet_before_the_diagnostic() {
        let queue = PacketQueue::new(2, 128);
        assert!(matches!(queue.push(vec![1; 7]), PushResult::Accepted));
        assert!(matches!(queue.poll(Duration::ZERO, 0), PollResult::Need(7)));
        queue.replace_with(vec![2; 13]);
        match queue.poll(Duration::ZERO, 7) {
            PollResult::Packet(bytes) => assert_eq!(bytes, vec![1; 7]),
            _ => panic!("critical overflow invalidated a reader's retained packet"),
        }
        match queue.poll(Duration::ZERO, 13) {
            PollResult::Packet(bytes) => assert_eq!(bytes, vec![2; 13]),
            _ => panic!("critical overflow lost its diagnostic"),
        }
    }

    #[test]
    fn packet_queue_size_probe_retains_packet_and_fifo_order() {
        let queue = PacketQueue::new(4, 128);
        assert!(matches!(queue.push(vec![1, 2, 3]), PushResult::Accepted));
        assert!(matches!(queue.push(vec![4, 5]), PushResult::Accepted));

        assert!(matches!(queue.poll(Duration::ZERO, 0), PollResult::Need(3)));
        match queue.poll(Duration::ZERO, 3) {
            PollResult::Packet(packet) => assert_eq!(packet, vec![1, 2, 3]),
            _ => panic!("size probe consumed or reordered the first packet"),
        }
        match queue.poll(Duration::ZERO, 2) {
            PollResult::Packet(packet) => assert_eq!(packet, vec![4, 5]),
            _ => panic!("second packet did not remain FIFO"),
        }
    }

    #[test]
    fn critical_event_overflow_fences_connection_and_retains_one_error() {
        let gate = Arc::new(CallbackGate::new());
        let shared = Shared::new(Arc::clone(&gate));
        for index in 0..1024 {
            shared.emit(json!({ "type": "probe", "index": index }), &[]);
        }
        assert!(gate.accepting.load(Ordering::Acquire));

        shared.emit(json!({ "type": "overflow-trigger" }), &[]);
        assert!(!gate.accepting.load(Ordering::Acquire));
        let snapshot = shared.events.snapshot();
        assert_eq!(
            snapshot.queued, 1,
            "overflow must collapse critical backlog"
        );
        match shared.events.poll(Duration::ZERO, 4096) {
            PollResult::Packet(packet) => {
                let (header, payload) = packet_parts(&packet);
                assert!(payload.is_empty());
                assert_eq!(header["type"], "error");
                assert_eq!(header["code"], "Overflow");
            }
            _ => panic!("overflow diagnostic was not retained"),
        }
    }

    #[test]
    fn c_abi_size_probe_retains_packet_until_copy() {
        unsafe {
            let peer = reactor_effect_peer_create();
            assert!(!peer.is_null());
            let packet = packet(json!({ "type": "message", "channel": "control" }), b"abc");
            let peer_ref = &*peer;
            assert!(matches!(
                peer_ref.shared.events.push(packet.clone()),
                PushResult::Accepted
            ));

            let mut required = 0usize;
            let status = reactor_effect_peer_poll_event(peer, 0, ptr::null_mut(), 0, &mut required);
            assert_eq!(status, STATUS_BUFFER_TOO_SMALL);
            assert_eq!(required, packet.len());

            let mut output = vec![0u8; required];
            let mut copied = 0usize;
            let status = reactor_effect_peer_poll_event(
                peer,
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut copied,
            );
            assert_eq!(status, STATUS_OK);
            assert_eq!(copied, packet.len());
            assert_eq!(output, packet);
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn c_abi_video_probe_survives_producer_eviction_between_calls() {
        for (first_size, replacement_size) in [(8, 13), (13, 8)] {
            unsafe {
                let peer = reactor_effect_peer_create();
                assert!(!peer.is_null());
                let inner = &*peer;
                let media = &inner.shared.video;
                let first = packet(json!({ "type": "fixture" }), &vec![1; first_size]);
                let replacement = packet(json!({ "type": "fixture" }), &vec![3; replacement_size]);
                media.push_drop_oldest(first.clone());
                // Fill the production queue to capacity before retaining its
                // first packet through the exported C ABI.
                for _ in 1..8 {
                    media.push_drop_oldest(packet(json!({ "type": "fixture" }), &[2; 7]));
                }
                let mut required = 0;
                assert_eq!(
                    reactor_effect_peer_poll_video(peer, 0, ptr::null_mut(), 0, &mut required),
                    STATUS_BUFFER_TOO_SMALL
                );
                assert_eq!(required, first.len());
                media.push_drop_oldest(replacement);
                assert_eq!(media.counters.dropped.load(Ordering::Relaxed), 1);
                let mut copied = 0;
                let mut output = vec![0; required];
                assert_eq!(
                    reactor_effect_peer_poll_video(
                        peer,
                        0,
                        output.as_mut_ptr(),
                        output.len(),
                        &mut copied
                    ),
                    STATUS_OK
                );
                assert_eq!(copied, first.len());
                assert_eq!(output, first);
                reactor_effect_peer_destroy(peer);
            }
        }
    }

    #[test]
    fn close_wakes_inflight_poll_and_post_close_operations_stay_closed() {
        unsafe {
            let peer = reactor_effect_peer_create();
            assert!(!peer.is_null());
            let address = peer as usize;
            let waiter = thread::spawn(move || {
                let peer = address as *mut ReactorEffectPeer;
                let mut length = 0usize;
                reactor_effect_peer_poll_event(peer, 10_000, ptr::null_mut(), 0, &mut length)
            });

            thread::sleep(Duration::from_millis(20));
            reactor_effect_peer_close(peer);
            assert_eq!(waiter.join().expect("poll thread"), STATUS_CLOSED);

            let mut response = vec![0u8; CALL_BUFFER_MIN];
            let mut response_len = 0usize;
            let status = reactor_effect_peer_call(
                peer,
                CALL_MEDIA_SNAPSHOT,
                ptr::null(),
                0,
                response.as_mut_ptr(),
                response.len(),
                &mut response_len,
            );
            assert_eq!(status, STATUS_CLOSED);
            let error: Value =
                serde_json::from_slice(&response[..response_len]).expect("closed JSON");
            assert_eq!(error["code"], "Closed");

            let mut send_error = vec![0u8; ERROR_BUFFER_MIN];
            let mut send_error_len = 0usize;
            let status = reactor_effect_peer_send(
                peer,
                CHANNEL_DATA,
                b"late".as_ptr(),
                4,
                send_error.as_mut_ptr(),
                send_error.len(),
                &mut send_error_len,
            );
            assert_eq!(status, STATUS_CLOSED);
            let error: Value =
                serde_json::from_slice(&send_error[..send_error_len]).expect("closed send JSON");
            assert_eq!(error["code"], "Closed");

            let mut poll_len = usize::MAX;
            assert_eq!(
                reactor_effect_peer_poll_event(peer, 0, ptr::null_mut(), 0, &mut poll_len),
                STATUS_CLOSED
            );
            assert_eq!(poll_len, 0);
            reactor_effect_peer_destroy(peer);
        }
    }

    #[test]
    fn transport_loopback_exchanges_ordered_binary_and_real_decoded_media() {
        let gate = Arc::new(CallbackGate::new());
        let shared = Arc::new(Shared::new(Arc::clone(&gate)));
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

        let factory = PeerConnectionFactory::builder()
            .with_synthetic_adm()
            .build()
            .expect("answerer factory");
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
        let media_deadline = Instant::now() + Duration::from_secs(20);
        let mut got_video: HashMap<String, (Vec<u8>, Vec<u8>)> = HashMap::new();
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

            while let PollResult::Packet(packet) =
                shared.video.queue.poll(Duration::ZERO, 1024 * 1024)
            {
                let (header, payload) = packet_parts(&packet);
                let name = header["track"].as_str().expect("video track").to_owned();
                let data_len = header["dataLength"].as_u64().expect("data length") as usize;
                assert_eq!(header["format"], "BGRA");
                assert_eq!(header["width"], width);
                assert_eq!(header["height"], height);
                assert_eq!(data_len, bgra_a.len());
                got_video.insert(
                    name,
                    (payload[..data_len].to_vec(), payload[data_len..].to_vec()),
                );
            }
            while let PollResult::Packet(packet) =
                shared.audio.queue.poll(Duration::ZERO, 1024 * 1024)
            {
                let (header, payload) = packet_parts(&packet);
                assert_eq!(header["track"], "audio-a");
                assert_eq!(header["format"], "s16le");
                assert_eq!(header["sampleRate"], 48_000);
                assert_eq!(header["channels"], 1);
                assert!(!payload.is_empty());
                got_audio = true;
            }
            assert!(
                Instant::now() < media_deadline,
                "real codecs did not deliver all decoded media"
            );
            thread::sleep(Duration::from_millis(30));
        }

        let (decoded_a, metadata_a) = got_video.get("video-a").expect("decoded video-a");
        let (decoded_b, metadata_b) = got_video.get("video-b").expect("decoded video-b");
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
        assert_ne!(
            bigint_from_header(&shared.video),
            0,
            "metadata path never observed frames"
        );

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
        assert!(matches!(
            shared.video.queue.poll(Duration::ZERO, 1024),
            PollResult::Closed
        ));
        assert!(matches!(
            shared.audio.queue.poll(Duration::ZERO, 1024),
            PollResult::Closed
        ));
        assert!(matches!(
            shared.events.poll(Duration::ZERO, 1024),
            PollResult::Closed
        ));
    }

    fn bigint_from_header(media: &MediaQueue) -> u64 {
        media.counters.observed.load(Ordering::Relaxed)
    }
}
