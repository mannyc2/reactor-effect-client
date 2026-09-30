/**
 * The browser half of the public session qualification, run by headless Chrome from
 * `scripts/browser-native.ts`. It makes two checks:
 *
 * - the public BrowserPeer session against a provider on a second RTCPeerConnection in this
 *   page, through an in-page coordinator fake;
 * - the far side of the runner's NativePeer session: this page plays the Reactor provider over a
 *   real RTCPeerConnection and trades SDP and ICE with the runner over HTTP.
 *
 * The provider speaks the wire protocol with its own codec, generated from the same protos as the
 * client's. Everything under test is imported through its public entry point.
 */
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { DescMessage, MessageInitShape, MessageShape } from "@bufbuild/protobuf";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Peer from "reactor-effect-client/Peer";
import * as Reactor from "reactor-effect-client/Reactor";
import type { SessionEvent } from "reactor-effect-client/Session";
import { BrowserMedia, BrowserPeer } from "reactor-effect-browser";
import { MessageKind } from "./proto/reactor_wire/v1/common_pb.js";
import {
  ControlClientMessageSchema,
  ControlServerMessageSchema,
} from "./proto/reactor_wire/v1/control_pb.js";
import {
  DataClientMessageSchema,
  DataServerMessageSchema,
} from "./proto/reactor_wire/v1/data_pb.js";

/** A check the page makes did not hold, or a browser API it drives failed. */
class Failed extends Schema.TaggedError<Failed>(
  "reactor-effect-integration/browser/native-connectivity/Failed",
)("Failed", { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }) {}

const ensure = Effect.fnUntraced(function* (holds: boolean, message: string) {
  if (!holds) return yield* Failed.make({ message });
});

/** Bounds a wait, failing with what did not happen in time. */
const within =
  (label: string, duration: Duration.Input) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | Failed, R> =>
    Effect.timeoutOrElse(effect, {
      duration,
      orElse: () => Effect.fail(Failed.make({ message: `${label}: deadline` })),
    });

/** A browser API's promise, failing with what the page was doing. */
const promised = <A>(doing: string, evaluate: () => Promise<A>): Effect.Effect<A, Failed> =>
  Effect.tryPromise({ try: evaluate, catch: (cause) => Failed.make({ message: doing, cause }) });

/** Waits until `holds`, checking it now and after each `event` from `target`. */
const whenEvent = (target: EventTarget, event: string, holds: () => boolean): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    const check = (): void => {
      if (!holds()) return;
      target.removeEventListener(event, check);
      resume(Effect.void);
    };
    target.addEventListener(event, check);
    check();
    return Effect.sync(() => target.removeEventListener(event, check));
  });

/** Waits until `ref` holds a value `holds` accepts. */
const until = <A>(ref: SubscriptionRef.SubscriptionRef<A>, holds: (value: A) => boolean) =>
  SubscriptionRef.changes(ref).pipe(Stream.filter(holds), Stream.runHead, Effect.asVoid);

const iceGathered = (peer: RTCPeerConnection) =>
  whenEvent(peer, "icegatheringstatechange", () => peer.iceGatheringState === "complete").pipe(
    within("browser ICE gathering", "5 seconds"),
  );

const echoed = { bytes: [0xa2, 0x20, 0x21, 0x22] };

// --- The provider's wire codec ------------------------------------------------------------------

/** The largest message a channel carries: the SCTP bound both Reactor peers hold. */
const maxMessageBytes = 262_144;

/** A message from the owner: larger than a channel carries, or malformed, it fails. */
const decode = <Desc extends DescMessage>(
  schema: Desc,
  bytes: Uint8Array,
): Effect.Effect<MessageShape<Desc>, Failed> =>
  bytes.byteLength > maxMessageBytes
    ? Effect.fail(
        Failed.make({ message: `provider received a message over ${maxMessageBytes} bytes` }),
      )
    : Effect.try({
        try: () => fromBinary(schema, bytes),
        catch: (cause) =>
          Failed.make({ message: `provider received a malformed ${schema.typeName}`, cause }),
      });

/** A reply built from its fields and encoded, if a channel can carry it. */
const encode = <Desc extends DescMessage>(
  schema: Desc,
  init: MessageInitShape<Desc>,
): Effect.Effect<Uint8Array<ArrayBuffer>, Failed> =>
  Effect.try({
    try: () => toBinary(schema, create(schema, init)),
    catch: (cause) =>
      Failed.make({ message: `provider could not encode ${schema.typeName}`, cause }),
  }).pipe(
    Effect.filterOrFail(
      (bytes) => bytes.byteLength <= maxMessageBytes,
      () => Failed.make({ message: `provider reply exceeds ${maxMessageBytes} bytes` }),
    ),
  );

// --- The provider -------------------------------------------------------------------------------

/** A request the provider answered, as the report shows it. */
interface ProviderRequest {
  readonly channel: "control" | "data";
  readonly type: string;
  readonly requestId: string;
  readonly bytes: number;
}

/** What the provider peer hears: a channel the owner opened, a message on it, or its failure. */
type Heard =
  | { readonly _tag: "Channel"; readonly channel: RTCDataChannel }
  | { readonly _tag: "Message"; readonly channel: RTCDataChannel; readonly data: unknown }
  | { readonly _tag: "Broken"; readonly channel: RTCDataChannel };

/** The raw WebRTC peer as the unpaid provider, never the SDK client. */
interface Provider {
  readonly channels: SubscriptionRef.SubscriptionRef<ReadonlyMap<string, RTCDataChannel>>;
  readonly requests: SubscriptionRef.SubscriptionRef<ReadonlyArray<ProviderRequest>>;
  /** Fails with the first protocol failure the provider saw. */
  readonly failed: Effect.Effect<never, Failed>;
  /** Fails now if the provider has seen a protocol failure. */
  readonly surfaced: Effect.Effect<void, Failed>;
  /** Stops answering: what the owner's channels do afterwards is not the provider's failure. */
  readonly retire: Effect.Effect<void>;
}

/**
 * Answers the owner's control and data channels on `peer` until the scope ends or `retire`.
 * The listeners are registered before this returns, so no channel opens unheard.
 */
const provide = Effect.fnUntraced(function* (peer: RTCPeerConnection) {
  const scope = yield* Scope.fork(yield* Effect.scope);
  const heard = yield* Queue.unbounded<Heard>();
  const channels = yield* SubscriptionRef.make<ReadonlyMap<string, RTCDataChannel>>(new Map());
  const requests = yield* SubscriptionRef.make<ReadonlyArray<ProviderRequest>>([]);
  const failure = yield* Deferred.make<never, Failed>();

  // Chrome calls these outside any fiber; they only hand the event to the queue.
  const opened = ({ channel }: RTCDataChannelEvent): void => {
    channel.binaryType = "arraybuffer";
    channel.onmessage = (event: MessageEvent<unknown>) => {
      Queue.offerUnsafe(heard, { _tag: "Message", channel, data: event.data });
    };
    channel.onerror = () => {
      Queue.offerUnsafe(heard, { _tag: "Broken", channel });
    };
    Queue.offerUnsafe(heard, { _tag: "Channel", channel });
  };
  yield* Effect.acquireRelease(
    Effect.sync(() => peer.addEventListener("datachannel", opened)),
    () =>
      Effect.gen(function* () {
        peer.removeEventListener("datachannel", opened);
        for (const channel of (yield* SubscriptionRef.get(channels)).values()) {
          channel.onmessage = null;
          channel.onerror = null;
        }
        yield* Queue.shutdown(heard);
      }),
  ).pipe(Scope.provide(scope));

  const record = (request: ProviderRequest) =>
    SubscriptionRef.update(requests, (seen) => [...seen, request]);
  const send = (channel: RTCDataChannel, bytes: Uint8Array<ArrayBuffer>) =>
    Effect.try({
      try: () => channel.send(bytes),
      catch: (cause) =>
        Failed.make({ message: `provider could not reply on ${channel.label}`, cause }),
    });

  const control = Effect.fnUntraced(function* (channel: RTCDataChannel, bytes: Uint8Array) {
    const request = yield* decode(ControlClientMessageSchema, bytes);
    if (request.kind !== MessageKind.REQUEST || request.payload.case === undefined) return;
    yield* record({
      channel: "control",
      type: request.payload.case,
      requestId: request.requestId,
      bytes: bytes.byteLength,
    });
    switch (request.payload.case) {
      case "requestSchema":
        return yield* send(
          channel,
          yield* encode(ControlServerMessageSchema, {
            requestId: request.requestId,
            kind: MessageKind.RESPONSE,
            payload: { case: "modelSchema", value: { openapi: { openapi: "3.1.0", paths: {} } } },
          }),
        );
      case "publishTrack":
        return yield* send(
          channel,
          yield* encode(ControlServerMessageSchema, {
            requestId: request.requestId,
            kind: MessageKind.RESPONSE,
            payload: { case: "publishTrack", value: { name: request.payload.value.name } },
          }),
        );
      default:
        // The provider answers only what the checks ask of it.
        return;
    }
  });

  const command = Effect.fnUntraced(function* (channel: RTCDataChannel, bytes: Uint8Array) {
    const request = yield* decode(DataClientMessageSchema, bytes);
    if (request.kind !== MessageKind.REQUEST || request.payload.case !== "command")
      return yield* Failed.make({ message: "provider received an invalid model command" });
    const received = request.payload.value;
    yield* record({
      channel: "data",
      type: received.type,
      requestId: request.requestId,
      bytes: bytes.byteLength,
    });
    const reply: MessageInitShape<typeof DataServerMessageSchema> =
      received.type === "ack"
        ? { requestId: request.requestId, kind: MessageKind.RESPONSE }
        : {
            requestId: request.requestId,
            kind: MessageKind.RESPONSE,
            payload: {
              case: "message",
              value: {
                type: received.type,
                ...(received.data === undefined ? {} : { data: received.data }),
              },
            },
          };
    yield* send(channel, yield* encode(DataServerMessageSchema, reply));
    if (received.type === "echo")
      yield* ensure(
        Equal.equals(received.data, echoed),
        "provider command data changed in transit",
      );
  });

  const answer = Effect.fnUntraced(function* (channel: RTCDataChannel, data: unknown) {
    if (!(data instanceof ArrayBuffer))
      return yield* Failed.make({ message: "provider received a nonbinary SCTP message" });
    const bytes = new Uint8Array(data);
    switch (channel.label) {
      case "control":
        return yield* control(channel, bytes);
      case "data":
        return yield* command(channel, bytes);
      default:
        return yield* Failed.make({ message: "provider received an unknown data channel" });
    }
  });

  const step = (event: Heard): Effect.Effect<void> => {
    switch (event._tag) {
      case "Channel":
        return SubscriptionRef.update(
          channels,
          (open) => new Map([...open, [event.channel.label, event.channel]]),
        );
      case "Broken":
        return Deferred.fail(
          failure,
          Failed.make({ message: `provider ${event.channel.label} channel failed` }),
        ).pipe(Effect.asVoid);
      case "Message":
        return answer(event.channel, event.data).pipe(
          Effect.catch((error) => Deferred.fail(failure, error)),
          Effect.asVoid,
        );
    }
  };
  yield* Stream.fromQueue(heard).pipe(Stream.runForEach(step), Effect.forkIn(scope));

  return {
    channels,
    requests,
    failed: Deferred.await(failure),
    surfaced: Deferred.isDone(failure).pipe(
      Effect.flatMap((done) => (done ? Deferred.await(failure) : Effect.void)),
    ),
    retire: Scope.close(scope, Exit.void),
  } satisfies Provider;
});

const channelNames = (provider: Provider) =>
  SubscriptionRef.get(provider.channels).pipe(Effect.map((open) => [...open.keys()].sort()));

// --- The page's media ---------------------------------------------------------------------------

/** The page's sources: a changing canvas and a 440 Hz tone, stopped with the scope. */
interface SourceTracks {
  readonly video: MediaStreamTrack;
  readonly audio: MediaStreamTrack;
}

const canvasVideo = Effect.gen(function* () {
  const canvas = document.createElement("canvas");
  canvas.width = 160;
  canvas.height = 96;
  const context = canvas.getContext("2d");
  if (context === null)
    return yield* Failed.make({ message: "Canvas 2D unavailable for browser/native media source" });
  const paint = (frame: number): void => {
    context.fillStyle = `rgb(${(frame * 29) % 255},${(frame * 47) % 255},${(frame * 71) % 255})`;
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = "white";
    context.fillRect((frame * 7) % 140, 18, 20, 24);
  };
  yield* Stream.fromSchedule(Schedule.spaced("50 millis")).pipe(
    Stream.runForEach((count) => Effect.sync(() => paint(count + 1))),
    Effect.forkScoped,
  );
  const track = Arr.head(canvas.captureStream(20).getVideoTracks());
  if (Option.isNone(track))
    return yield* Failed.make({ message: "canvas.captureStream produced no video track" });
  yield* Effect.addFinalizer(() => Effect.sync(() => track.value.stop()));
  return track.value;
});

const toneAudio = Effect.gen(function* () {
  const audioContext = yield* Effect.acquireRelease(
    Effect.sync(() => new AudioContext({ sampleRate: 48_000 })),
    (opened) => Effect.promise(() => opened.close()),
  );
  yield* promised("Web Audio did not resume", () => audioContext.resume());
  const oscillator = audioContext.createOscillator();
  const gain = audioContext.createGain();
  const destination = audioContext.createMediaStreamDestination();
  oscillator.frequency.value = 440;
  gain.gain.value = 0.2;
  oscillator.connect(gain);
  gain.connect(destination);
  oscillator.start();
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      oscillator.stop();
      oscillator.disconnect();
      gain.disconnect();
      destination.disconnect();
    }),
  );
  const track = Arr.head(destination.stream.getAudioTracks());
  if (Option.isNone(track))
    return yield* Failed.make({ message: "Web Audio destination produced no audio track" });
  yield* Effect.addFinalizer(() => Effect.sync(() => track.value.stop()));
  return track.value;
});

const sourceTracks: Effect.Effect<SourceTracks, Failed, Scope.Scope> = Effect.all({
  video: canvasVideo,
  audio: toneAudio,
});

// --- The local BrowserPeer check ----------------------------------------------------------------

/** The canonical factory over the browser peer, its host layer built in the caller's scope. */
const browserClient = (settings: CoordinatorClient.Options & Reactor.Options) =>
  Layer.build(Layer.merge(BrowserPeer.layer, CoordinatorClient.layer(settings))).pipe(
    Effect.flatMap((services) => Reactor.make(settings).pipe(Effect.provide(services))),
  );

const localTracks: ReadonlyArray<Peer.Track> = [
  { name: "browser_video", kind: "video", direction: "recvonly" },
  { name: "browser_audio", kind: "audio", direction: "recvonly" },
  { name: "input_audio", kind: "audio", direction: "sendonly" },
];

/** What the in-page coordinator has done for the owned session and the failing one. */
interface Fake {
  readonly closed: boolean;
  readonly failedClosed: boolean;
  readonly deletes: number;
  readonly failedDeletes: number;
  readonly answer: string | undefined;
  readonly remoteReady: boolean;
  readonly pendingIce: ReadonlyArray<RTCIceCandidateInit>;
}

const Offer = Schema.Struct({
  sdp_offer: Schema.String,
  track_mapping: Schema.Array(Peer.Mapping),
});
const IceBatch = Schema.Struct({ candidates: Schema.Array(Peer.IceCandidate) });

const bodyText = (request: HttpClientRequest.HttpClientRequest): string => {
  const body = request.body;
  if (body._tag === "Uint8Array") return new TextDecoder().decode(body.body);
  return body._tag === "Raw" && Predicate.isString(body.body) ? body.body : "";
};

const requestBody = <A>(
  schema: Schema.ConstraintCodec<A, string>,
  request: HttpClientRequest.HttpClientRequest,
) =>
  request.pipe(
    bodyText,
    Schema.decodeEffect(schema),
    Effect.mapError((cause) =>
      Failed.make({ message: "browser fixture received a malformed body", cause }),
    ),
  );

const iceInit = (candidate: Peer.IceCandidate): RTCIceCandidateInit => ({
  candidate: candidate.candidate,
  ...(candidate.sdp_mid === undefined ? {} : { sdpMid: candidate.sdp_mid }),
  ...(candidate.sdp_mline_index === undefined ? {} : { sdpMLineIndex: candidate.sdp_mline_index }),
});

const reply = <A>(schema: Schema.ConstraintCodec<A, unknown, unknown>, body: A) =>
  HttpServerResponse.schemaJson(schema)(body).pipe(
    Effect.mapError((cause) => Failed.make({ message: "browser fixture could not reply", cause })),
  );

/**
 * The coordinator the local owner talks to, answered in this page: `failure` sessions are
 * allocated but get an answer no peer accepts.
 */
const coordinatorFake = (
  provider: RTCPeerConnection,
  source: SourceTracks,
  fake: Ref.Ref<Fake>,
): HttpClient.HttpClient => {
  const descriptor = (failed: boolean) =>
    Ref.get(fake).pipe(
      Effect.flatMap((state) =>
        reply(CoordinatorClient.Descriptor, {
          session_id: failed ? "sess_browser_failure" : "sess_browser_local",
          state: (failed ? state.failedClosed : state.closed) ? "CLOSED" : "ACTIVE",
          capabilities: {
            protocol_version: "1.0",
            tracks: localTracks,
            commands: [
              { name: "echo", schema: {} },
              { name: "ack", schema: {} },
            ],
          },
          selected_transport: { protocol: "webrtc", version: "1.0" },
        }),
      ),
    );

  const offered = Effect.fnUntraced(function* (request: HttpClientRequest.HttpClientRequest) {
    const offer = yield* requestBody(Schema.fromJsonString(Offer), request);
    yield* promised("browser fixture could not take the offer", () =>
      provider.setRemoteDescription({ type: "offer", sdp: offer.sdp_offer }),
    );
    const pending = yield* Ref.modify(fake, (state) => [
      state.pendingIce,
      { ...state, remoteReady: true, pendingIce: [] },
    ]);
    for (const candidate of pending)
      yield* promised("browser fixture could not add a candidate", () =>
        provider.addIceCandidate(candidate),
      );
    for (const mapping of offer.track_mapping) {
      const transceiver = provider.getTransceivers().find((entry) => entry.mid === mapping.mid);
      if (transceiver === undefined)
        return yield* Failed.make({ message: "browser fixture lost a negotiated transceiver" });
      if (mapping.direction === "recvonly") {
        yield* promised("browser fixture could not send its source", () =>
          transceiver.sender.replaceTrack(mapping.kind === "video" ? source.video : source.audio),
        );
        transceiver.direction = "sendonly";
      } else transceiver.direction = "recvonly";
    }
    const answer = yield* promised("browser fixture could not answer", () =>
      provider.createAnswer(),
    );
    yield* promised("browser fixture could not apply its answer", () =>
      provider.setLocalDescription(answer),
    );
    yield* iceGathered(provider);
    const sdp = provider.localDescription?.sdp;
    if (sdp === undefined)
      return yield* Failed.make({ message: "browser fixture answer has no gathered SDP" });
    yield* Ref.update(fake, (state) => ({ ...state, answer: sdp }));
  });

  const candidates = Effect.fnUntraced(function* (request: HttpClientRequest.HttpClientRequest) {
    const batch = yield* requestBody(Schema.fromJsonString(IceBatch), request);
    for (const candidate of batch.candidates) {
      const init = iceInit(candidate);
      const now = yield* Ref.modify(fake, (state) =>
        state.remoteReady
          ? [true, state]
          : [false, { ...state, pendingIce: [...state.pendingIce, init] }],
      );
      if (now)
        yield* promised("browser fixture could not add a candidate", () =>
          provider.addIceCandidate(init),
        );
    }
  });

  const route = Effect.fnUntraced(function* (
    request: HttpClientRequest.HttpClientRequest,
    url: URL,
  ) {
    const path = url.pathname;
    const failed = path.startsWith("/failure");
    if (path.endsWith("/sessions") && request.method === "POST") return yield* descriptor(failed);
    if (/\/sessions\/sess_browser_(local|failure)$/.test(path)) {
      if (request.method !== "DELETE") return yield* descriptor(failed);
      yield* Ref.update(fake, (state) =>
        failed
          ? { ...state, failedClosed: true, failedDeletes: state.failedDeletes + 1 }
          : { ...state, closed: true, deletes: state.deletes + 1 },
      );
      return HttpServerResponse.empty({ status: 202 });
    }
    if (path.endsWith("/ice_servers"))
      return yield* reply(CoordinatorClient.IceServersReply, { ice_servers: [] });
    if (path.endsWith("/connections"))
      return yield* reply(CoordinatorClient.Registered, { connection_id: 1001 });
    if (path.endsWith("/ice_candidates")) {
      if (!failed) yield* candidates(request);
      return HttpServerResponse.empty({ status: 204 });
    }
    if (path.endsWith("/sdp_params")) {
      if (request.method === "GET") {
        const { answer } = yield* Ref.get(fake);
        if (!failed && answer === undefined)
          return yield* Failed.make({ message: "browser fixture has no answer yet" });
        return yield* reply(CoordinatorClient.SdpAnswer, {
          sdp_answer: failed || answer === undefined ? "fixture deliberately invalid SDP" : answer,
        });
      }
      if (!failed) yield* offered(request);
      return HttpServerResponse.empty({ status: 204 });
    }
    return HttpServerResponse.jsonUnsafe(
      { error: "unhandled browser fixture request" },
      { status: 404 },
    );
  });

  return HttpClient.make((request, url) =>
    route(request, url).pipe(
      Effect.map((response) => HttpServerResponse.toClientResponse(response, { request })),
      // A fixture failure reaches the owner as a request that never completed.
      Effect.mapError(
        (failure) =>
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({
              request,
              cause: failure,
              description: failure.message,
            }),
          }),
      ),
    ),
  );
};

/** Records every RTCPeerConnection the public owner makes; Chrome's own class does the work. */
const trackedPeers = Effect.acquireRelease(
  Effect.sync(() => {
    const original = globalThis.RTCPeerConnection;
    // Chrome calls this constructor outside any fiber, so it records into a plain array.
    const made: RTCPeerConnection[] = [];
    globalThis.RTCPeerConnection = class extends original {
      constructor(configuration?: RTCConfiguration) {
        super(configuration);
        made.push(this);
      }
    };
    return { original, made };
  }),
  ({ original, made }) =>
    Effect.sync(() => {
      globalThis.RTCPeerConnection = original;
      for (const peer of made) if (peer.connectionState !== "closed") peer.close();
    }),
).pipe(Effect.map(({ made }): ReadonlyArray<RTCPeerConnection> => made));

const ownedSession = Effect.fnUntraced(function* (
  provider: Provider,
  source: SourceTracks,
  clients: ReadonlyArray<RTCPeerConnection>,
) {
  const factory = yield* browserClient({
    apiUrl: "http://browser.fixture",
    replyTimeout: "5 seconds",
  });
  const session = yield* factory.create({ model: "fixture/browser" });
  const ready = yield* session.ready;
  yield* ensure(ready.remote.ownership === "owned", "browser acquisition lost remote ownership");
  const media = yield* BrowserMedia.tracks(session);
  yield* ensure(
    media.generation === ready.generation && media.tracks.length === localTracks.length,
    "browser media did not capture negotiated generation/tracks",
  );
  const lease = yield* media.track("browser_video");
  yield* ensure(
    lease instanceof MediaStreamTrack && lease.kind === "video" && lease.readyState === "live",
    "browser public media lease is not a live real track",
  );
  yield* media.publish("input_audio", source.audio);
  yield* media.setMaxBitrate("input_audio", 96_000);
  yield* media.unpublish("input_audio");
  yield* ensure(
    source.audio.readyState === "live",
    "browser publication stopped the borrowed source",
  );
  const events = yield* SubscriptionRef.make<ReadonlyArray<SessionEvent>>([]);
  yield* session.events().pipe(
    Stream.runForEach((event) => SubscriptionRef.update(events, (seen) => [...seen, event])),
    Effect.forkScoped,
  );
  yield* Effect.yieldNow;
  const schema = yield* session.schema;
  const ack = yield* session.command("ack", {});
  const answered = yield* session.command("echo", echoed);
  yield* until(events, (seen) => seen.includes(ack) && seen.includes(answered)).pipe(
    within("browser model observation delivery", "1 second"),
  );
  yield* ensure(
    schema.openapi?.openapi === "3.1.0",
    "browser control channel lost schema response",
  );
  yield* ensure(
    ack.kind === "ack" && answered.kind === "message",
    "browser session collapsed ACK and model reply",
  );
  yield* ensure(
    answered.generation === ready.generation && ack.sequence < answered.sequence,
    "browser reply ordering/generation invalid",
  );
  yield* ensure(
    answered.kind === "message" && Equal.equals(answered.data, echoed),
    "browser echo payload changed",
  );
  yield* provider.surfaced;
  yield* provider.retire;
  const report = yield* session.close;
  yield* ensure(
    report.localClosed && report.localErrors.length === 0 && report.remote.confirmed,
    "browser owned close did not complete and confirm termination",
  );
  yield* ensure(
    lease.readyState === "ended",
    "browser session close did not retire its leased track",
  );
  const stale = yield* Effect.result(media.setTrackActive("browser_video", true));
  yield* ensure(
    Result.isFailure(stale),
    "retired browser media generation accepted a track operation",
  );
  yield* ensure(
    clients.length === 1 && clients[0]?.connectionState === "closed",
    "browser client retained its peer after close",
  );
  return {
    generation: String(ready.generation),
    ack: ack.kind,
    reply: answered.kind,
    sameAttributedObjects: true,
    media: {
      realTrackLease: true,
      publicationBorrowedSourcePreserved: true,
      leaseRetired: true,
      retiredOperationRejected: true,
    },
    close: report,
  };
});

const failedAcquisition = Effect.fnUntraced(function* (
  fake: Ref.Ref<Fake>,
  clients: ReadonlyArray<RTCPeerConnection>,
) {
  const factory = yield* browserClient({ apiUrl: "http://browser.fixture/failure" });
  const failed = yield* Effect.result(factory.create({ model: "fixture/browser-failure" }));
  yield* ensure(Result.isFailure(failed), "invalid browser answer unexpectedly connected");
  const state = yield* Ref.get(fake);
  yield* ensure(
    state.failedDeletes === 1 && state.failedClosed,
    "failed acquisition did not release owned remote lifetime before returning",
  );
  yield* ensure(
    clients.length === 2 && clients[1]?.connectionState === "closed",
    "failed acquisition retained its real browser peer",
  );
});

/** The public BrowserPeer session and a failing acquisition, against a provider in this page. */
const localBrowserPeerCheck = Effect.gen(function* () {
  const peer = yield* Effect.acquireRelease(
    Effect.sync(() => new RTCPeerConnection()),
    (opened) => Effect.sync(() => opened.close()),
  );
  const provider = yield* provide(peer);
  const source = yield* sourceTracks;
  const fake = yield* Ref.make<Fake>({
    closed: false,
    failedClosed: false,
    deletes: 0,
    failedDeletes: 0,
    answer: undefined,
    remoteReady: false,
    pendingIce: [],
  });
  const client = coordinatorFake(peer, source, fake);
  const clients = yield* trackedPeers;
  const result = yield* ownedSession(provider, source, clients).pipe(
    Effect.scoped,
    Effect.provideService(HttpClient.HttpClient, client),
  );
  yield* failedAcquisition(fake, clients).pipe(
    Effect.scoped,
    Effect.provideService(HttpClient.HttpClient, client),
  );
  const { deletes, failedDeletes } = yield* Ref.get(fake);
  yield* ensure(deletes === 1, "browser owned session terminated more than once");
  yield* provider.surfaced;
  return {
    ...result,
    channels: yield* channelNames(provider),
    requests: yield* SubscriptionRef.get(provider.requests),
    failureCleanup: { localClosed: true, remoteDeleteCount: failedDeletes },
    realPeerHandlesClosed: clients.length,
  };
}).pipe(Effect.scoped);

// --- The native session's far side --------------------------------------------------------------

const NativeOffer = Schema.Struct({
  sdp: Schema.String,
  mapping: Schema.Array(Peer.Mapping),
  fixture: Schema.Struct({
    forceRelay: Schema.Boolean,
    iceServers: Schema.Array(
      Schema.Struct({
        urls: Schema.Array(Schema.String),
        username: Schema.String,
        credential: Schema.String,
      }),
    ),
  }),
});
const NativeIce = Schema.Struct({
  candidates: Schema.Array(
    Schema.Struct({
      candidate: Schema.String,
      sdpMid: Schema.optionalKey(Schema.String),
      sdpMLineIndex: Schema.optionalKey(Schema.Int),
    }),
  ),
  complete: Schema.Boolean,
});
const Finish = Schema.Struct({ finish: Schema.Literal(true) });

/** Reads one of the runner's endpoints, which answers once the fact it serves exists. */
const fetchJson = <A>(path: string, schema: Schema.ConstraintDecoder<A>) =>
  HttpClient.get(path).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)),
  );

const postJson = (path: string, body: unknown) =>
  HttpClientRequest.post(path).pipe(
    HttpClientRequest.bodyJson(body),
    Effect.flatMap(HttpClient.execute),
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.asVoid,
  );

/** Adds the native owner's candidates as the runner learns them, until the owner's last. */
const forwardNativeIce = (
  peer: RTCPeerConnection,
  cursor: number,
): Effect.Effect<
  void,
  Failed | HttpClientError.HttpClientError | Schema.SchemaError,
  HttpClient.HttpClient
> =>
  Effect.gen(function* () {
    const batch = yield* fetchJson(`/native-ice?cursor=${cursor}`, NativeIce);
    for (const candidate of batch.candidates)
      yield* promised("browser could not add a native candidate", () =>
        peer.addIceCandidate(candidate),
      );
    if (!batch.complete) yield* forwardNativeIce(peer, cursor + batch.candidates.length);
  });

const TransportStats = Schema.Struct({
  type: Schema.Literal("transport"),
  selectedCandidatePairId: Schema.String,
});
const PairStats = Schema.Struct({
  id: Schema.String,
  localCandidateId: Schema.String,
  remoteCandidateId: Schema.String,
});
const CandidateStats = Schema.Struct({
  candidateType: Schema.String,
  protocol: Schema.optionalKey(Schema.String),
  relayProtocol: Schema.optionalKey(Schema.String),
});

/** A stats string worth reporting: present and not empty. */
const reported = Schema.is(Schema.NonEmptyString);

/** The ICE candidate pair Chrome's transport selected, from its statistics. */
const selectedCandidates = Effect.fnUntraced(function* (peer: RTCPeerConnection) {
  const report = yield* promised("browser stats", () => peer.getStats());
  const entries = new Map<string, unknown>(report);
  const selected = Arr.findFirst([...entries.values()], (entry) =>
    Schema.decodeUnknownOption(TransportStats)(entry).pipe(
      Option.flatMap(({ selectedCandidatePairId }) =>
        Option.fromNullishOr(entries.get(selectedCandidatePairId)),
      ),
    ),
  );
  const pair = yield* selected.pipe(
    Option.getOrUndefined,
    Schema.decodeUnknownEffect(PairStats),
    Effect.mapError(() =>
      Failed.make({
        message:
          "browser stats omitted transport.selectedCandidatePairId or its selected ICE candidate pair",
      }),
    ),
  );
  const [local, remote] = yield* Effect.all([
    Schema.decodeUnknownEffect(CandidateStats)(entries.get(pair.localCandidateId)),
    Schema.decodeUnknownEffect(CandidateStats)(entries.get(pair.remoteCandidateId)),
  ]).pipe(
    Effect.mapError(() =>
      Failed.make({ message: "browser stats omitted selected ICE candidate details" }),
    ),
  );
  return {
    pairId: pair.id,
    localCandidateId: pair.localCandidateId,
    remoteCandidateId: pair.remoteCandidateId,
    localCandidateType: local.candidateType,
    remoteCandidateType: remote.candidateType,
    ...(reported(local.protocol) ? { localProtocol: local.protocol } : {}),
    ...(reported(remote.protocol) ? { remoteProtocol: remote.protocol } : {}),
    ...(reported(local.relayProtocol) ? { localRelayProtocol: local.relayProtocol } : {}),
    ...(reported(remote.relayProtocol) ? { remoteRelayProtocol: remote.relayProtocol } : {}),
  };
});

/**
 * Answers the runner's native owner as its provider: the offer and the owner's candidates come
 * from the runner, the answer goes back to it, and media and replies flow over the peer. The
 * peer, its media and the provider last as long as the caller's scope.
 */
const nativeCheck = Effect.gen(function* () {
  const offer = yield* fetchJson("/native-offer", NativeOffer).pipe(
    within("native public session offer", "20 seconds"),
  );
  const configuration: RTCConfiguration = {
    iceServers: offer.fixture.iceServers.map((server) => ({ ...server, urls: [...server.urls] })),
    ...(offer.fixture.forceRelay ? { iceTransportPolicy: "relay" } : {}),
  };
  const peer = yield* Effect.acquireRelease(
    Effect.sync(() => new RTCPeerConnection(configuration)),
    (opened) => Effect.sync(() => opened.close()),
  );
  const source = yield* sourceTracks;
  const provider = yield* provide(peer);
  yield* promised("browser could not take the native offer", () =>
    peer.setRemoteDescription({ type: "offer", sdp: offer.sdp }),
  );
  for (const mapping of offer.mapping) {
    if (mapping.direction !== "recvonly") continue;
    const transceiver = peer.getTransceivers().find((entry) => entry.mid === mapping.mid);
    if (transceiver === undefined)
      return yield* Failed.make({
        message: `browser answerer has no transceiver for native MID ${mapping.mid}`,
      });
    yield* promised("browser could not send its source", () =>
      transceiver.sender.replaceTrack(mapping.kind === "video" ? source.video : source.audio),
    );
    transceiver.direction = "sendonly";
  }
  const forwarding = yield* forwardNativeIce(peer, 0).pipe(
    within("native candidate forwarding", "15 seconds"),
    Effect.forkScoped,
  );
  const answer = yield* promised("browser could not answer the native offer", () =>
    peer.createAnswer(),
  );
  yield* promised("browser could not apply its answer", () => peer.setLocalDescription(answer));
  yield* iceGathered(peer);
  const sdp = peer.localDescription?.sdp;
  if (sdp === undefined)
    return yield* Failed.make({ message: "browser/native answer has no gathered SDP" });
  yield* postJson("/native-answer", { sdp });
  yield* Fiber.join(forwarding);

  yield* Effect.gen(function* () {
    yield* whenEvent(peer, "connectionstatechange", () => peer.connectionState === "connected");
    yield* until(provider.channels, (open) => open.size === 2);
    for (const channel of (yield* SubscriptionRef.get(provider.channels)).values())
      yield* whenEvent(channel, "open", () => channel.readyState === "open");
  }).pipe(
    Effect.raceFirst(provider.failed),
    within("browser/native WebRTC connection and channels", "10 seconds"),
  );
  yield* provider.surfaced;
  yield* until(
    provider.requests,
    (seen) =>
      seen.some((request) => request.channel === "control" && request.type === "requestSchema") &&
      seen.filter((request) => request.channel === "data").length >= 2,
  ).pipe(
    Effect.raceFirst(provider.failed),
    within("native public session control/model exchanges", "7 seconds"),
  );
  yield* provider.surfaced;
  const requests = yield* SubscriptionRef.get(provider.requests);
  const model = requests.filter((request) => request.channel === "data");
  yield* ensure(
    model[0]?.type === "ack" && model[1]?.type === "echo",
    "model commands lost ordered channel delivery",
  );
  yield* ensure(
    new Set(model.map((request) => request.requestId)).size === 2,
    "model requests lost unique correlation identities",
  );
  for (const channel of (yield* SubscriptionRef.get(provider.channels)).values())
    yield* ensure(channel.ordered, "native negotiated an unordered channel");

  const relay = offer.fixture.forceRelay ? yield* selectedCandidates(peer) : undefined;
  if (
    relay !== undefined &&
    (relay.localCandidateType !== "relay" || relay.remoteCandidateType !== "relay")
  )
    return yield* Failed.make({
      message: `forced TURN selected non-relay browser pair: local=${relay.localCandidateType} remote=${relay.remoteCandidateType}`,
    });

  return {
    connectionState: peer.connectionState,
    channels: yield* channelNames(provider),
    requests,
    replies: { control: "schema", data: ["ack", "echo"] },
    orderedBinaryChannels: true,
    relay: relay === undefined ? { forced: false } : { forced: true, selected: relay },
    source: {
      video: { width: 160, height: 96, framesPerSecond: 20 },
      audio: { sampleRate: 48_000, frequencyHz: 440 },
    },
  };
});

// --- The page -----------------------------------------------------------------------------------

const main = Effect.gen(function* () {
  if (!Predicate.isFunction(BrowserMedia.tracks) || !Layer.isLayer(BrowserPeer.layer))
    return yield* Failed.make({
      message: "bundled browser public entry did not load its host surface",
    });
  const localPeer = yield* localBrowserPeerCheck;
  const native = yield* nativeCheck;
  yield* postJson("/browser-report", {
    ok: true,
    browserEntry: { tracks: "function", layer: "present" },
    localPeer,
    native,
  });
  yield* fetchJson("/finish", Finish).pipe(
    within("browser/native runner finish signal", "15 seconds"),
  );
}).pipe(
  Effect.scoped,
  Effect.catchCause((cause) =>
    postJson("/browser-report", { ok: false, error: Cause.pretty(cause) }).pipe(Effect.ignore),
  ),
  Effect.ensuring(postJson("/browser-closed", { closed: true }).pipe(Effect.ignore)),
);

Effect.runFork(
  main.pipe(
    // The page's entry point.
    // @effect-diagnostics-next-line strictEffectProvide:off
    Effect.provide(FetchHttpClient.layer),
  ),
);
