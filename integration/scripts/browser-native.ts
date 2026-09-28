/**
 * The public NativePeer session against real Chrome. This runner serves the browser bundle and
 * plays Reactor's coordinator over HTTP; headless Chrome loads the page, which plays the provider
 * over a real RTCPeerConnection. It prints the evidence summary and exits 0 when every check
 * passes.
 *
 *   node scripts/browser-native.ts <browser bundle>
 *
 * BROWSER_NATIVE_FORCE_RELAY=1 with BROWSER_NATIVE_TURN_URL, BROWSER_NATIVE_TURN_USERNAME and
 * BROWSER_NATIVE_TURN_PASSWORD forces both peers through that TURN relay.
 * BROWSER_NATIVE_NO_SANDBOX=1 runs Chrome without its sandbox, and BROWSER_EXECUTABLE names the
 * Chrome to run.
 */
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ByteSize from "effect/ByteSize";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpIncomingMessage from "effect/unstable/http/HttpIncomingMessage";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
// NodeHttpServer listens with the Node server this factory makes.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createServer } from "node:http";
import { createRequire } from "node:module";
import * as Coordinator from "reactor-effect-client/Coordinator";
import type { AudioFrame, VideoFrame } from "reactor-effect-client/Media";
import * as Reactor from "reactor-effect-client/Reactor";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import { CloseReport } from "reactor-effect-client/Session";
import type { CommandReply, Statistics } from "reactor-effect-client/Session";
import * as NativePeer from "reactor-effect-native/NativePeer";

/** A check the qualification makes did not hold. */
class Failed extends Schema.TaggedError<Failed>(
  "reactor-effect-integration/scripts/browser-native/Failed",
)("Failed", { message: Schema.String }) {}

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

/** Waits until `ref` holds a value `holds` accepts. */
const until = <A>(ref: SubscriptionRef.SubscriptionRef<A>, holds: (value: A) => boolean) =>
  SubscriptionRef.changes(ref).pipe(Stream.filter(holds), Stream.runHead, Effect.asVoid);

/** The deferred's value if it is already there. */
const peek = <A>(deferred: Deferred.Deferred<A>): Effect.Effect<Option.Option<A>> =>
  Deferred.isDone(deferred).pipe(
    Effect.flatMap((done) =>
      done ? Deferred.await(deferred).pipe(Effect.asSome) : Effect.succeedNone,
    ),
  );

// --- Configuration ------------------------------------------------------------------------------

interface Settings {
  readonly forceRelay: boolean;
  readonly turnUrl: string;
  readonly turnUsername: string;
  readonly turnPassword: Redacted.Redacted<string>;
  readonly noSandbox: boolean;
  readonly executable: Option.Option<string>;
}

const settings = Effect.gen(function* () {
  const forceRelay = yield* Config.Literals(["0", "1"], "BROWSER_NATIVE_FORCE_RELAY").pipe(
    Config.withDefault("0"),
    Effect.mapError(() => Failed.make({ message: "BROWSER_NATIVE_FORCE_RELAY must be 0 or 1" })),
  );
  const { turnUrl, turnUsername, turnPassword, noSandbox, executable } = yield* Config.all({
    turnUrl: Config.String("BROWSER_NATIVE_TURN_URL").pipe(Config.withDefault("")),
    turnUsername: Config.String("BROWSER_NATIVE_TURN_USERNAME").pipe(Config.withDefault("")),
    turnPassword: Config.Redacted("BROWSER_NATIVE_TURN_PASSWORD").pipe(
      Config.withDefault(Redacted.make("")),
    ),
    noSandbox: Config.String("BROWSER_NATIVE_NO_SANDBOX").pipe(Config.withDefault("")),
    executable: Config.String("BROWSER_EXECUTABLE").pipe(Config.option),
  });
  const password = Redacted.value(turnPassword);
  const configuredTurn = [turnUrl, turnUsername, password].some((value) => value.length > 0);
  if (forceRelay === "0" && configuredTurn)
    return yield* Failed.make({
      message: "TURN fixture credentials require BROWSER_NATIVE_FORCE_RELAY=1",
    });
  if (
    forceRelay === "1" &&
    (!/^turns?:/i.test(turnUrl) || turnUsername.length === 0 || password.length === 0)
  )
    return yield* Failed.make({
      message: "forced TURN requires a TURN URL, username and password",
    });
  return {
    forceRelay: forceRelay === "1",
    turnUrl,
    turnUsername,
    turnPassword,
    noSandbox: noSandbox === "1",
    executable,
  } satisfies Settings;
});

/** The Chrome to run: the configured one, else the first installed of the usual places. */
const browserExecutable = Effect.fnUntraced(function* (configured: Option.Option<string>) {
  const fs = yield* FileSystem.FileSystem;
  const candidates = [
    ...Option.toArray(configured),
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ].filter((candidate) => candidate.length > 0);
  const found = yield* Effect.findFirst(candidates, (candidate) => fs.exists(candidate));
  if (Option.isNone(found))
    return yield* Failed.make({ message: "no reusable Chrome/Chromium executable found" });
  return found.value;
});

// --- What the page and the native owner send ----------------------------------------------------

/** What the page reports once its checks end; the rest of a success is evidence it prints. */
const BrowserReport = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), localPeer: Schema.Json, native: Schema.Json }),
  Schema.Struct({ ok: Schema.Literal(false), error: Schema.String }),
]);
type BrowserReport = typeof BrowserReport.Type;

/** The browser's selected candidate pair, from its report of the native session. */
const BrowserRelay = Schema.Struct({ relay: Schema.Struct({ selected: Schema.Json }) });
const RelayPair = Schema.Struct({
  localCandidateType: Schema.Literal("relay"),
  remoteCandidateType: Schema.Literal("relay"),
});

const Offer = Schema.Struct({
  sdp_offer: Schema.String,
  track_mapping: Schema.Array(Coordinator.Mapping),
});
const IceBatch = Schema.Struct({
  candidates: Schema.Array(Coordinator.IceCandidate),
  is_final: Schema.optionalKey(Schema.Boolean),
});
const Answer = Schema.Struct({ sdp: Schema.String });
const Cursor = Schema.Struct({
  cursor: Schema.optionalKey(
    Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  ),
});

/** The addon the native package loaded: its hash and the build it came from. */
const NativeIdentity = Schema.Struct({
  sha256: Schema.String,
  build: Schema.Struct({ sourceSha256: Schema.String, target: Schema.String }),
});

// --- What the runner sends the page -------------------------------------------------------------

/** An ICE server as the page's RTCPeerConnection takes it. */
const PageIceServer = Schema.Struct({
  urls: Schema.Array(Schema.String),
  username: Schema.String,
  credential: Schema.String,
});
const NativeOffer = Schema.Struct({
  sdp: Schema.String,
  mapping: Schema.Array(Coordinator.Mapping),
  fixture: Schema.Struct({ forceRelay: Schema.Boolean, iceServers: Schema.Array(PageIceServer) }),
});
const PageCandidate = Schema.Struct({
  candidate: Schema.String,
  sdpMid: Schema.optionalKey(Schema.String),
  sdpMLineIndex: Schema.optionalKey(Schema.Int),
});
type PageCandidate = typeof PageCandidate.Type;
const NativeIce = Schema.Struct({
  candidates: Schema.Array(PageCandidate),
  complete: Schema.Boolean,
});

// --- Evidence -----------------------------------------------------------------------------------

/** Counters and flags as the native host reports them: 64-bit counts print as decimal strings. */
const Counters = Schema.Record(
  Schema.String,
  Schema.Union([Schema.Boolean, Schema.Finite, Schema.String, Schema.BigInt]),
);
const NativeEvidence = Schema.Struct({
  generation: Schema.String,
  schema: Schema.String,
  ack: Schema.String,
  reply: Schema.String,
  sameAttributedObjects: Schema.Boolean,
  close: CloseReport,
  pressure: Counters,
  relay: Schema.optionalKey(
    Schema.Struct({
      forced: Schema.Boolean,
      native: Counters,
      browser: Schema.Json,
      gatheredRelayCandidates: Schema.Int,
      filteredNonRelayCandidates: Schema.Int,
      writableProof: Schema.String,
    }),
  ),
});
const printEvidence = Schema.encodeEffect(
  Schema.fromJsonString(Schema.toCodecJson(NativeEvidence)),
);
const printJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));

/** FNV-1a of a frame's bytes: frames that differ in content hash apart. */
const fnv = (bytes: Uint8Array): string => {
  let value = 2166136261;
  for (const byte of bytes) value = Math.imul(value ^ byte, 16777619);
  return (value >>> 0).toString(16);
};

const pcmRms = (samples: Int16Array): number => {
  let sum = 0;
  for (const sample of samples) sum += (sample / 32768) ** 2;
  return Math.sqrt(sum / Math.max(samples.length, 1));
};

/** The page's canvas source, decoded: 160 by 96 RGBA frames. */
const SourceVideo = Schema.TaggedStruct("VideoFrame", {
  width: Schema.Literal(160),
  height: Schema.Literal(96),
});
const sourceVideoBytes = 160 * 96 * 4;
const FrameIds = Schema.Struct({ frameId: Schema.BigInt, timestampMicros: Schema.BigInt });
/** The page's tone, decoded: mono 48 kHz PCM. */
const SourceAudio = Schema.TaggedStruct("AudioFrame", {
  sampleRate: Schema.Literal(48000),
  channels: Schema.Literal(1),
});
const isSourceVideo = Schema.is(SourceVideo);
const hasFrameIds = Schema.is(FrameIds);
const isSourceAudio = Schema.is(SourceAudio);

/** What the native owner decoded from the page's media. */
interface Tally {
  readonly videoFrames: number;
  readonly hashes: ReadonlySet<string>;
  readonly metadataFrames: number;
  readonly metadataBytes: number;
  readonly audioPackets: number;
  readonly maxAudioRms: number;
  readonly first?: { readonly frame: VideoFrame; readonly hash: string };
}
const noMedia: Tally = {
  videoFrames: 0,
  hashes: new Set(),
  metadataFrames: 0,
  metadataBytes: 0,
  audioPackets: 0,
  maxAudioRms: 0,
};
const enoughMedia = (tally: Tally): boolean =>
  tally.videoFrames >= 12 &&
  tally.hashes.size >= 3 &&
  tally.audioPackets >= 12 &&
  tally.maxAudioRms > 0.001;

// --- The coordinator and the page's endpoints ---------------------------------------------------

const sessionId = "sess_native_browser_fixture";
const tracks: ReadonlyArray<Coordinator.Track> = [
  { name: "browser_video", kind: "video", direction: "recvonly" },
  { name: "browser_audio", kind: "audio", direction: "recvonly" },
];
const page =
  "<!doctype html><meta charset=utf-8><title>Reactor public session local qualification</title><script type=module src=/browser.js></script>";
const noStore = { "cache-control": "no-store" };
const relayCandidate = /(?:^|\s)typ relay(?:\s|$)/i;

/** The native owner's candidates as the page receives them, and how many were relayed. */
interface NativeIceState {
  readonly candidates: ReadonlyArray<PageCandidate>;
  readonly complete: boolean;
  readonly relay: number;
  readonly filtered: number;
}

/** What the fixture's endpoints learn while the run goes on. */
interface Fixture {
  readonly settings: Settings;
  readonly bundle: Uint8Array;
  readonly remote: Ref.Ref<{
    readonly closed: boolean;
    readonly deletes: number;
    readonly allocations: number;
  }>;
  readonly offer: Deferred.Deferred<typeof NativeOffer.Type>;
  readonly answer: Ref.Ref<string | undefined>;
  readonly nativeIce: SubscriptionRef.SubscriptionRef<NativeIceState>;
  readonly report: Deferred.Deferred<BrowserReport>;
  readonly browserClosed: Deferred.Deferred<void>;
  readonly finish: Deferred.Deferred<void>;
  /** The first failure the host saw outside the native session's own calls. */
  readonly hostFailure: Deferred.Deferred<never, Failed | ReactorError>;
}

const json = (body: unknown, status = 200) =>
  HttpServerResponse.jsonUnsafe(body, { status, headers: noStore });

/** A route's failure answers 500 with its message, as the coordinator's errors do. */
const answered = <E extends { readonly message: string }, R>(
  handler: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
) => handler.pipe(Effect.catch((error) => Effect.succeed(json({ error: error.message }, 500))));

const coordinatorReply =
  <A>(schema: Schema.ConstraintCodec<A, unknown, unknown>) =>
  (body: A, status = 200) =>
    HttpServerResponse.schemaJson(schema)(body, { status, headers: noStore });

const body = <A>(schema: Schema.ConstraintDecoder<A>, message: string) =>
  HttpServerRequest.schemaBodyJson(schema).pipe(Effect.mapError(() => Failed.make({ message })));

const routes = (fixture: Fixture) => {
  const { settings } = fixture;
  const descriptor = Effect.map(Ref.get(fixture.remote), ({ closed }) => ({
    session_id: sessionId,
    state: closed ? "CLOSED" : "ACTIVE",
    capabilities: {
      protocol_version: "1.0",
      tracks,
      commands: [
        { name: "ack", schema: {} },
        { name: "echo", schema: {} },
      ],
    },
    selected_transport: { protocol: "webrtc", version: "1.0" },
  }));
  const describe = (status: number) =>
    descriptor.pipe(
      Effect.flatMap((value) => coordinatorReply(Coordinator.Descriptor)(value, status)),
    );
  const transport = `/sessions/${sessionId}/transport/webrtc`;
  const connection = `${transport}/connections/:connection`;
  const offered = answered(
    Effect.gen(function* () {
      const offer = yield* body(Offer, "public native owner submitted invalid SDP/mapping");
      yield* Deferred.succeed(fixture.offer, {
        sdp: offer.sdp_offer,
        mapping: offer.track_mapping,
        fixture: {
          forceRelay: settings.forceRelay,
          iceServers: settings.forceRelay
            ? [
                {
                  urls: [settings.turnUrl],
                  username: settings.turnUsername,
                  credential: Redacted.value(settings.turnPassword),
                },
              ]
            : [],
        },
      });
      return json({});
    }),
  );
  return [
    HttpRouter.route("GET", "/", HttpServerResponse.html(page)),
    HttpRouter.route(
      "GET",
      "/browser.js",
      HttpServerResponse.uint8Array(fixture.bundle, {
        contentType: "text/javascript",
        headers: noStore,
      }),
    ),
    HttpRouter.route(
      "POST",
      "/sessions",
      answered(
        Ref.update(fixture.remote, (remote) => ({
          ...remote,
          allocations: remote.allocations + 1,
        })).pipe(Effect.andThen(describe(200))),
      ),
    ),
    HttpRouter.route("GET", `/sessions/${sessionId}`, answered(describe(200))),
    HttpRouter.route(
      "DELETE",
      `/sessions/${sessionId}`,
      Ref.update(fixture.remote, (remote) => ({
        ...remote,
        closed: true,
        deletes: remote.deletes + 1,
      })).pipe(Effect.as(json({}, 202))),
    ),
    HttpRouter.route(
      "GET",
      `${transport}/ice_servers`,
      answered(
        coordinatorReply(Coordinator.IceServersReply)({
          ice_servers: settings.forceRelay
            ? [
                {
                  uris: [settings.turnUrl],
                  credentials: {
                    username: settings.turnUsername,
                    password: Redacted.value(settings.turnPassword),
                  },
                },
              ]
            : [],
        }),
      ),
    ),
    HttpRouter.route(
      "POST",
      `${transport}/connections`,
      answered(coordinatorReply(Coordinator.Registered)({ connection_id: 1001 })),
    ),
    HttpRouter.route("POST", `${connection}/sdp_params`, offered),
    HttpRouter.route("PUT", `${connection}/sdp_params`, offered),
    HttpRouter.route(
      "GET",
      `${connection}/sdp_params`,
      answered(
        Effect.gen(function* () {
          const report = yield* peek(fixture.report);
          if (Option.isSome(report) && !report.value.ok)
            return json({ error: "browser fixture failed before answering" }, 500);
          const answer = yield* Ref.get(fixture.answer);
          if (answer === undefined) return json({}, 202);
          return yield* coordinatorReply(Coordinator.SdpAnswer)({ sdp_answer: answer });
        }),
      ),
    ),
    HttpRouter.route(
      "POST",
      `${connection}/ice_candidates`,
      answered(
        Effect.gen(function* () {
          const batch = yield* body(IceBatch, "native owner omitted ICE candidate batch");
          for (const candidate of batch.candidates) {
            const relay = relayCandidate.test(candidate.candidate);
            const kept = !settings.forceRelay || relay;
            const bounded = yield* SubscriptionRef.modify(fixture.nativeIce, (ice) => {
              const next = {
                ...ice,
                relay: ice.relay + (relay ? 1 : 0),
                filtered: ice.filtered + (kept ? 0 : 1),
              };
              if (!kept) return [true, next];
              if (ice.candidates.length >= 1024) return [false, ice];
              return [true, { ...next, candidates: [...ice.candidates, pageCandidate(candidate)] }];
            });
            yield* ensure(bounded, "native candidate fixture bound exceeded");
          }
          if (batch.is_final === true)
            yield* SubscriptionRef.update(fixture.nativeIce, (ice) => ({ ...ice, complete: true }));
          return json({});
        }),
      ),
    ),
    // The page's own endpoints answer once the fact it asks for exists.
    HttpRouter.route(
      "GET",
      "/native-offer",
      Deferred.await(fixture.offer).pipe(
        Effect.flatMap((offer) =>
          HttpServerResponse.schemaJson(NativeOffer)(offer, { headers: noStore }),
        ),
        answered,
      ),
    ),
    HttpRouter.route(
      "GET",
      "/native-ice",
      answered(
        Effect.gen(function* () {
          const { cursor = 0 } = yield* HttpServerRequest.schemaSearchParams(Cursor).pipe(
            Effect.mapError(() => Failed.make({ message: "invalid ICE cursor" })),
          );
          const known = yield* SubscriptionRef.get(fixture.nativeIce);
          yield* ensure(cursor <= known.candidates.length, "invalid ICE cursor");
          yield* until(fixture.nativeIce, (ice) => ice.complete || ice.candidates.length > cursor);
          const ice = yield* SubscriptionRef.get(fixture.nativeIce);
          return yield* HttpServerResponse.schemaJson(NativeIce)(
            { candidates: ice.candidates.slice(cursor), complete: ice.complete },
            { headers: noStore },
          );
        }),
      ),
    ),
    HttpRouter.route(
      "POST",
      "/native-answer",
      answered(
        body(Answer, "browser omitted answer SDP").pipe(
          Effect.flatMap(({ sdp }) => Ref.set(fixture.answer, sdp)),
          Effect.as(json({ stored: true })),
        ),
      ),
    ),
    HttpRouter.route(
      "POST",
      "/browser-report",
      answered(
        Effect.gen(function* () {
          const report = yield* body(BrowserReport, "browser sent an invalid report");
          yield* Deferred.succeed(fixture.report, report);
          if (!report.ok)
            yield* Deferred.fail(
              fixture.hostFailure,
              Failed.make({ message: `browser qualification failed: ${report.error}` }),
            );
          return json({ stored: true });
        }),
      ),
    ),
    HttpRouter.route(
      "POST",
      "/browser-closed",
      Deferred.succeed(fixture.browserClosed, undefined).pipe(Effect.as(json({ stored: true }))),
    ),
    HttpRouter.route(
      "GET",
      "/finish",
      Deferred.await(fixture.finish).pipe(Effect.as(json({ finish: true }))),
    ),
    HttpRouter.route("*", "*", json({ error: "unhandled local fixture route" }, 404)),
  ];
};

const pageCandidate = (candidate: Coordinator.IceCandidate): PageCandidate => ({
  candidate: candidate.candidate,
  ...(candidate.sdp_mid === undefined ? {} : { sdpMid: candidate.sdp_mid }),
  ...(candidate.sdp_mline_index === undefined ? {} : { sdpMLineIndex: candidate.sdp_mline_index }),
});

/** Serves the fixture on a free loopback port for as long as the scope lasts. */
const serve = Effect.fnUntraced(function* (fixture: Fixture) {
  const app = yield* HttpRouter.toHttpEffect(HttpRouter.addAll(routes(fixture)));
  const server = yield* NodeHttpServer.make(createServer, { host: "127.0.0.1", port: 0 });
  yield* server.serve(
    app.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.mebibytes(2))),
  );
  if (server.address._tag !== "InetAddressV4")
    return yield* Failed.make({ message: "fixture server did not bind IPv4" });
  return `http://127.0.0.1:${server.address.port}`;
});

// --- Chrome -------------------------------------------------------------------------------------

/**
 * Headless Chrome on the page, in a profile directory of its own, for as long as the scope
 * lasts. Chrome stays in this runner's process group, so a caller that kills the group reaches
 * it too.
 */
const chrome = Effect.fnUntraced(function* (executable: string, url: string, fixture: Fixture) {
  const fs = yield* FileSystem.FileSystem;
  const profile = yield* Effect.acquireRelease(
    fs.makeTempDirectory({ prefix: "reactor-public-browser-native-" }),
    (directory) =>
      fs.remove(directory, { recursive: true, force: true }).pipe(
        // Chrome's helper processes can still be writing the profile cache for a moment after
        // the main process has exited; retry instead of racing them.
        Effect.retry({ schedule: Schedule.spaced("250 millis"), times: 20 }),
        Effect.orDie,
      ),
  );
  const handle = yield* ChildProcess.make(
    executable,
    [
      "--headless=new",
      "--autoplay-policy=no-user-gesture-required",
      "--disable-background-networking",
      "--disable-component-update",
      "--disable-default-apps",
      "--disable-sync",
      "--no-first-run",
      "--no-default-browser-check",
      "--metrics-recording-only",
      "--disable-features=WebRtcHideLocalIpsWithMdns",
      "--disable-dev-shm-usage",
      ...(fixture.settings.noSandbox ? ["--no-sandbox"] : []),
      `--user-data-dir=${profile}`,
      `${url}/`,
    ],
    {
      stdin: "ignore",
      stdout: "ignore",
      detached: false,
      killSignal: "SIGTERM",
      forceKillAfter: "1 second",
    },
  );
  const stderr = yield* Ref.make("");
  yield* handle.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((text) => Ref.update(stderr, (kept) => `${kept}${text}`.slice(-16_384))),
    Effect.ignore,
    Effect.forkScoped,
  );
  const exited = yield* Deferred.make<void>();
  yield* handle.exitCode.pipe(
    Effect.match({
      onFailure: (error) => error.message,
      onSuccess: (code) => `exit code ${code}`,
    }),
    Effect.flatMap((exit) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(exited, undefined);
        if (yield* Deferred.isDone(fixture.browserClosed)) return;
        yield* Deferred.fail(
          fixture.hostFailure,
          Failed.make({
            message: `Chrome exited before cleanup: ${exit}; ${yield* Ref.get(stderr)}`,
          }),
        );
      }),
    ),
    Effect.forkScoped,
  );
  return exited;
});

// --- The native session -------------------------------------------------------------------------

/** Both ends' proof that the forced relay carried the session. */
const relayEvidence = Effect.fnUntraced(function* (fixture: Fixture, statistics: Statistics) {
  const ice = yield* SubscriptionRef.get(fixture.nativeIce);
  yield* ensure(ice.relay > 0, "native gathered no relay candidates");
  const pair = statistics.pair;
  if (pair?.localCandidateType !== "relay" || pair.remoteCandidateType !== "relay")
    return yield* Failed.make({
      message: "native active nominated/succeeded pair is not relay/relay",
    });
  const report = yield* Deferred.await(fixture.report);
  if (!report.ok) return yield* Failed.make({ message: "browser omitted successful relay report" });
  const selected = yield* Schema.decodeUnknownEffect(BrowserRelay)(report.native).pipe(
    Effect.tap(({ relay }) => Schema.decodeUnknownEffect(RelayPair)(relay.selected)),
    Effect.mapError(() => Failed.make({ message: "browser selected ICE pair is not relay/relay" })),
  );
  return {
    forced: true,
    native: { ...pair },
    browser: selected.relay.selected,
    gatheredRelayCandidates: ice.relay,
    filteredNonRelayCandidates: ice.filtered,
    writableProof: "bidirectional SCTP plus decoded RTP",
  };
});

const nativeSession = Effect.fnUntraced(function* (
  url: string,
  fixture: Fixture,
  chromeExited: Deferred.Deferred<void>,
) {
  const services = yield* Layer.build(
    Layer.merge(NativePeer.layer(), Coordinator.layer({ apiUrl: url })),
  );
  const factory = yield* Reactor.make({
    connectTimeout: "45 seconds",
    readyTimeout: "45 seconds",
    replyTimeout: "5 seconds",
  }).pipe(Effect.provide(services));
  const session = yield* factory.create({ model: "fixture/native-browser" });
  const ready = yield* session.ready;
  yield* ensure(ready.remote.ownership === "owned", "native public session lost ownership");

  const events = yield* SubscriptionRef.make<ReadonlyArray<CommandReply>>([]);
  const observing = yield* session.events().pipe(
    Stream.runForEach((event) =>
      event._tag === "Model"
        ? SubscriptionRef.modify(events, (seen) =>
            seen.length < 64 ? [true, [...seen, event]] : [false, seen],
          ).pipe(Effect.flatMap((kept) => ensure(kept, "model observation fixture bound exceeded")))
        : Effect.void,
    ),
    Effect.catch((error) => Deferred.fail(fixture.hostFailure, error)),
    Effect.forkScoped,
  );

  const media = yield* session.decoded;
  yield* ensure(
    media.generation === ready.generation,
    "native public media did not capture ready generation",
  );
  const tally = yield* SubscriptionRef.make(noMedia);
  const video = (frame: VideoFrame) =>
    Effect.gen(function* () {
      yield* ensure(
        isSourceVideo(frame) && frame.data.length === sourceVideoBytes,
        "native decoded video shape mismatch",
      );
      yield* ensure(hasFrameIds(frame), "native video lost 64-bit metadata");
      const hash = fnv(frame.data);
      yield* SubscriptionRef.update(tally, (seen) => ({
        ...seen,
        videoFrames: seen.videoFrames + 1,
        hashes: seen.hashes.size < 256 ? new Set([...seen.hashes, hash]) : seen.hashes,
        metadataBytes: seen.metadataBytes + frame.metadata.length,
        metadataFrames: seen.metadataFrames + (frame.metadata.length > 0 ? 1 : 0),
        first: seen.first ?? { frame, hash },
      }));
    });
  const audio = (frame: AudioFrame) =>
    ensure(isSourceAudio(frame), "native decoded PCM shape mismatch").pipe(
      Effect.andThen(
        SubscriptionRef.update(tally, (seen) => ({
          ...seen,
          audioPackets: seen.audioPackets + 1,
          maxAudioRms: Math.max(seen.maxAudioRms, pcmRms(frame.samples)),
        })),
      ),
    );
  const decoding = yield* Effect.all(
    [
      media.video("browser_video").pipe(Stream.runForEach(video)),
      media.audio("browser_audio").pipe(Stream.runForEach(audio)),
    ],
    { concurrency: "unbounded", discard: true },
  ).pipe(
    Effect.catch((error) => Deferred.fail(fixture.hostFailure, error)),
    Effect.forkScoped,
  );
  yield* Effect.yieldNow;

  const schema = yield* session.schema;
  const ack = yield* session.command("ack", {});
  const reply = yield* session.command("echo", { bytes: [0xa2, 0x20, 0x21, 0x22] });
  yield* until(events, (seen) => seen.includes(ack) && seen.includes(reply)).pipe(
    within("native model observation delivery", "1 second"),
  );
  const openapi = schema.openapi?.openapi;
  if (openapi !== "3.1.0")
    return yield* Failed.make({ message: "native control channel lost schema response" });
  yield* ensure(
    ack.kind === "ack" && reply.kind === "message" && reply.type === "echo",
    "native command lost ACK/reply distinction",
  );
  yield* ensure(
    ack.sequence < reply.sequence && reply.generation === ready.generation,
    "native reply sequence/generation invalid",
  );
  yield* ensure(
    reply.kind === "message" && Equal.equals(reply.data, { bytes: [0xa2, 0x20, 0x21, 0x22] }),
    "native echo data changed",
  );

  yield* Effect.all([Deferred.await(fixture.report), until(tally, enoughMedia)], {
    concurrency: "unbounded",
    discard: true,
  }).pipe(within("public native/browser messages and decoded media", "15 seconds"));

  const statistics = yield* session.stats;
  const relay = fixture.settings.forceRelay ? yield* relayEvidence(fixture, statistics) : undefined;

  const pressure = yield* media.pressure;
  // Closing ends the session's events and media; what their readers see then is not a failure.
  yield* Fiber.interruptAll([observing, decoding]);
  const close = yield* session.close;
  yield* ensure(
    close.localClosed && close.localErrors.length === 0 && close.remote.confirmed,
    "native joined cleanup/remote confirmation failed",
  );
  const remote = yield* Ref.get(fixture.remote);
  yield* ensure(
    remote.deletes === 1 && remote.allocations === 1,
    "native session ownership did not allocate/terminate exactly once",
  );
  const { first } = yield* SubscriptionRef.get(tally);
  yield* ensure(
    first !== undefined && fnv(first.frame.data) === first.hash,
    "decoded frame bytes did not survive native destruction",
  );
  yield* Deferred.succeed(fixture.finish, undefined);
  yield* Effect.raceFirst(Deferred.await(fixture.browserClosed), Deferred.await(chromeExited)).pipe(
    within("browser cleanup report", "5 seconds"),
  );
  yield* ensure(
    yield* Deferred.isDone(fixture.browserClosed),
    "browser fixture did not join its media cleanup",
  );
  return {
    evidence: {
      generation: String(ready.generation),
      schema: openapi,
      ack: ack.kind,
      reply: reply.kind,
      sameAttributedObjects: true,
      close,
      pressure: { ...pressure },
      ...(relay === undefined ? {} : { relay }),
    },
    tally: yield* SubscriptionRef.get(tally),
  };
});

// --- The run ------------------------------------------------------------------------------------

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const args = yield* (yield* Stdio.Stdio).args;
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const configured = yield* settings;
  const bundle = path.resolve(args[0] ?? path.join(root, "../.check/browser-native/browser.js"));
  if (!(yield* fs.exists(bundle)))
    return yield* Failed.make({ message: `browser bundle missing: ${bundle}` });
  const executable = yield* browserExecutable(configured.executable);
  const fixture: Fixture = {
    settings: configured,
    bundle: yield* fs.readFile(bundle),
    remote: yield* Ref.make({ closed: false, deletes: 0, allocations: 0 }),
    offer: yield* Deferred.make<typeof NativeOffer.Type>(),
    answer: yield* Ref.make<string | undefined>(undefined),
    nativeIce: yield* SubscriptionRef.make<NativeIceState>({
      candidates: [],
      complete: false,
      relay: 0,
      filtered: 0,
    }),
    report: yield* Deferred.make<BrowserReport>(),
    browserClosed: yield* Deferred.make<void>(),
    finish: yield* Deferred.make<void>(),
    hostFailure: yield* Deferred.make<never, Failed | ReactorError>(),
  };

  const lines = yield* Effect.gen(function* () {
    const url = yield* serve(fixture);
    const chromeExited = yield* chrome(executable, url, fixture);
    const native = yield* Effect.raceFirst(
      nativeSession(url, fixture, chromeExited).pipe(Effect.scoped),
      Deferred.await(fixture.hostFailure),
    );
    const identityFile = yield* Effect.try({
      try: () =>
        createRequire(import.meta.url).resolve(
          `reactor-effect-native-${process.platform === "darwin" ? "darwin-arm64" : "linux-x64-gnu"}/native-identity.json`,
          { paths: [path.join(root, "../packages/native")] },
        ),
      catch: () => Failed.make({ message: "the staged native addon has no identity file" }),
    });
    const identity = yield* fs
      .readFileString(identityFile)
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(NativeIdentity))));
    const report = yield* peek(fixture.report);
    if (Option.isNone(report) || !report.value.ok)
      return yield* Failed.make({
        message: "browser omitted its successful public session report",
      });
    const { tally } = native;
    return [
      "browser-native-ok",
      `host ${process.platform}-${process.arch}`,
      `browser ${executable}`,
      `native-artifact sha256=${identity.sha256} sourceSha256=${identity.build.sourceSha256} target=${identity.build.target}`,
      `browser-public ${yield* printJson(report.value.localPeer)}`,
      `browser-native ${yield* printJson(report.value.native)}`,
      `native-public ${yield* printEvidence(native.evidence)}`,
      `native-video frames=${tally.videoFrames} distinctHashes=${tally.hashes.size} metadataFrames=${tally.metadataFrames} metadataBytes=${tally.metadataBytes}`,
      `native-audio packets=${tally.audioPackets} maxRms=${tally.maxAudioRms.toFixed(6)}`,
      `turn-relay ${configured.forceRelay ? "qualified through public session statistics and media" : "disabled"}`,
      `metadata-limit ${tally.metadataBytes === 0 ? "standards browser emitted no Reactor custom frame metadata; decoded A/V is real" : "custom frame metadata delivered"}`,
    ];
  }).pipe(
    Effect.tapCause(() =>
      peek(fixture.report).pipe(
        Effect.flatMap((report) =>
          Option.isSome(report) && !report.value.ok
            ? Console.error(`browser-failure ${report.value.error}`)
            : Effect.void,
        ),
      ),
    ),
    Effect.scoped,
  );
  // The summary prints only once Chrome, the server and the profile are cleaned up.
  for (const line of lines) yield* Console.log(line);
});

program.pipe(
  // The runner's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Layer.merge(NodeServices.layer, FetchHttpClient.layer)),
  NodeRuntime.runMain,
);
