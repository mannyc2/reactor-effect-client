/**
 * What the native tests share: the scripted fake addon, a coordinator
 * stand-in, the far peer process the media tests receive from, and small
 * assertions.
 */
import { fileURLToPath } from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import type * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";
import * as Coordinator from "reactor-effect-client/Coordinator";
import { IceCandidate } from "reactor-effect-client/Coordinator";
import type { Track } from "reactor-effect-client/Coordinator";
import type { DecodedMedia } from "reactor-effect-client/Media";
import type { Peer } from "reactor-effect-client/Peer";
import * as Reactor from "reactor-effect-client/Reactor";
import * as NativePeer from "../src/NativePeer.js";
import { load } from "../src/internal/addon.js";
import type { Addon } from "../src/internal/addon.js";
import { local } from "../src/internal/local.js";
import * as InProcess from "../src/internal/peer.js";
import { make } from "./fixtures/addon.mjs";

const fixture = fileURLToPath(new URL("./fixtures/addon.mts", import.meta.url));

/** Poll `condition` on the live clock until it holds, or die with `message`. */
export const eventually = (check: {
  readonly condition: () => boolean;
  readonly message: string;
  readonly timeout?: Duration.Input;
}) =>
  Effect.suspend(() => (check.condition() ? Effect.void : Effect.fail(check.message))).pipe(
    Effect.retry({ schedule: Schedule.spaced("5 millis") }),
    Effect.timeoutOrElse({
      duration: check.timeout ?? "5 seconds",
      orElse: () => Effect.die(new Error(check.message)),
    }),
    Effect.orDie,
  );

/**
 * The scripted fake addon in a directory of its own, removed with the scope.
 * `path` is a module another process can load; `module` is its twin in this
 * process. Holding a call keeps it from answering in either.
 */
export const fakeAddon = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "reactor-native-addon-" });
  const entry = path.join(directory, "addon.cjs");
  const quoted = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.String))(fixture);
  yield* fs.writeFileString(entry, `module.exports = require(${quoted}).make(__dirname);\n`);
  const marker = (name: string) => path.join(directory, name);
  const log = marker("calls.log");
  /** Every call that reached the fake, in this process or a child. */
  const calls = Effect.flatMap(fs.exists(log), (written) =>
    written ? fs.readFileString(log) : Effect.succeed(""),
  ).pipe(
    Effect.map((text) => text.split("\n").filter((line) => line !== "")),
    Effect.orDie,
  );
  return {
    path: entry,
    module: make(directory),
    hold: (call: "stats" | "shutdown", held: boolean) =>
      (held
        ? fs.writeFileString(marker(`hold-${call}`), "")
        : fs.remove(marker(`hold-${call}`), { force: true })
      ).pipe(Effect.orDie),
    calls,
    /** Wait until `count` entries named `call` are in the fake's log. */
    reached: (call: string, count = 1) =>
      calls.pipe(
        Effect.filterOrFail((all) => all.filter((name) => name === call).length >= count),
        Effect.retry({ schedule: Schedule.spaced("5 millis") }),
        Effect.timeoutOrElse({
          duration: "5 seconds",
          orElse: () => Effect.die(new Error(`${call} never reached the fake addon`)),
        }),
        Effect.asVoid,
      ),
  };
});

export type FakeAddon = Effect.Success<typeof fakeAddon>;

/** A native peer on `addon`, the installed one by default, made as the layer makes it. */
export const nativePeer = (
  options: { readonly addon?: Addon; readonly shutdownTimeout?: Duration.Duration } = {},
) =>
  (options.addon === undefined ? load(undefined) : Effect.succeed(options.addon)).pipe(
    Effect.flatMap(local),
    Effect.flatMap((handle) => InProcess.make(handle, options.shutdownTimeout)),
  );

/**
 * The canonical factory over the native peer, with its host layer built in the
 * caller's scope, as `Reactor.layer(configuration).pipe(Layer.provide(NativePeer.layer(options)))`
 * does for an application.
 */
export const nativeClient = (
  input: {
    readonly settings?: Coordinator.Options & Reactor.Options;
    readonly options?: NativePeer.Options;
  } = {},
) =>
  Layer.build(Layer.merge(NativePeer.layer(input.options), Coordinator.layer(input.settings))).pipe(
    Effect.flatMap((services) => Reactor.make(input.settings).pipe(Effect.provide(services))),
  );

/** A reply the coordinator stand-in sends: a status, and JSON when there is a body. */
interface Reply {
  readonly status: number;
  readonly body?: unknown;
}

const Offer = Schema.Struct({ sdp_offer: Schema.String });
const Candidates = Schema.Struct({ candidates: Schema.Array(IceCandidate) });

/** A request's JSON body; the canonical client only sends ones that decode. */
const body = <S extends Schema.Codec<unknown, unknown>>(
  schema: S,
  request: HttpClientRequest.HttpClientRequest,
) =>
  (request.body._tag === "Uint8Array"
    ? Effect.succeed(new TextDecoder().decode(request.body.body))
    : Effect.die(new Error(`unexpected ${request.body._tag} request body`))
  ).pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(schema))), Effect.orDie);

/**
 * A coordinator stand-in. It allocates one session per POST, named after
 * `sessionId`, reads a deleted session as absent, answers each offer with
 * `answer`, a fixed SDP by default, and hands each ICE candidate the client
 * sends to `candidate`, which drops it by default. `client` is the HTTP client
 * the canonical client reaches it with.
 */
export const coordinator = Effect.fnUntraced(function* (input: {
  readonly sessionId: string;
  readonly tracks: ReadonlyArray<Track>;
  readonly answer?: (offer: string) => Effect.Effect<string>;
  readonly candidate?: (candidate: IceCandidate) => Effect.Effect<void>;
}) {
  const allocated = yield* Ref.make<ReadonlyArray<string>>([]);
  const deleted = yield* Ref.make<ReadonlySet<string>>(new Set());
  const answered = yield* Ref.make<string | undefined>(undefined);
  const answer = input.answer ?? (() => Effect.succeed("fixture native answer"));
  const descriptor = (id: string) => ({
    session_id: id,
    state: "ACTIVE",
    capabilities: {
      protocol_version: "1.0",
      tracks: input.tracks,
      commands: [{ name: "echo", schema: {} }],
    },
    selected_transport: { protocol: "webrtc", version: "1.0" },
  });
  const route = (request: HttpClientRequest.HttpClientRequest, url: URL) =>
    Effect.gen(function* () {
      const path = url.pathname;
      const method = request.method;
      const id = /^\/sessions\/([^/]+)/.exec(path)?.[1] ?? input.sessionId;
      if (path === "/sessions" && method === "POST") {
        const allocation = yield* Ref.modify(allocated, (all) => {
          const next = all.length === 0 ? input.sessionId : `${input.sessionId}_${all.length}`;
          return [next, [...all, next]] as const;
        });
        return { status: 200, body: descriptor(allocation) };
      }
      if (path === `/sessions/${id}` && method === "GET")
        return (yield* Ref.get(deleted)).has(id)
          ? { status: 404, body: { error: "session not found" } }
          : { status: 200, body: descriptor(id) };
      if (path === `/sessions/${id}` && method === "DELETE") {
        yield* Ref.update(deleted, (all) => new Set(all).add(id));
        return { status: 202 };
      }
      if (path.endsWith("/ice_servers")) return { status: 200, body: { ice_servers: [] } };
      if (path.endsWith("/connections")) return { status: 200, body: { connection_id: 1001 } };
      if (path.endsWith("/ice_candidates")) {
        const sent = yield* body(Candidates, request);
        if (input.candidate !== undefined)
          yield* Effect.forEach(sent.candidates, input.candidate, { discard: true });
        return { status: 204 };
      }
      if (path.endsWith("/sdp_params") && method === "GET") {
        const sdp = yield* Ref.get(answered);
        return sdp === undefined ? { status: 202 } : { status: 200, body: { sdp_answer: sdp } };
      }
      if (path.endsWith("/sdp_params")) {
        const offer = yield* body(Offer, request);
        yield* Ref.set(answered, yield* answer(offer.sdp_offer));
        return { status: 204 };
      }
      return { status: 404, body: { error: `unhandled route ${path}` } };
    });
  const client = HttpClient.make((request, url) =>
    Effect.map(route(request, url), (reply: Reply) =>
      HttpServerResponse.toClientResponse(
        reply.body === undefined
          ? HttpServerResponse.empty({ status: reply.status })
          : HttpServerResponse.jsonUnsafe(reply.body, { status: reply.status }),
        { request },
      ),
    ),
  );
  return { client, allocated: Ref.get(allocated), deleted: Ref.get(deleted) };
});

export type Message = { readonly [x: PropertyKey]: unknown };

/** One line of the far peer's JSON protocol, either way. */
const FarPeerMessage = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));

/** `value` when it is an object, or an empty one: a far peer or statistics entry, read loosely. */
export const record = (value: unknown): Message => (Predicate.isObject(value) ? value : {});

/**
 * The far peer process: a libwebrtc sender on the pinned reactor-webrtc that
 * answers offers, sends 1344x768 BGRA at 24 fps with per-frame metadata plus
 * 48 kHz PCM, and echoes both channels, one session per id. `bun run
 * native:test` builds it; `REACTOR_NATIVE_FAR_PEER` names another.
 */
export class FarPeer extends Context.Service<
  FarPeer,
  {
    readonly pid: number;
    readonly answer: (id: string, sdp: string) => Effect.Effect<string>;
    readonly candidate: (id: string, candidate: IceCandidate) => Effect.Effect<void>;
    /** One session's stats as far_peer.rs reports them: pacing, encoder and path. */
    readonly stats: (id: string) => Effect.Effect<Message>;
    /** Frames the far peer's encoder actually sent, and their size. */
    readonly sent: (id: string) => Effect.Effect<{
      readonly frames: number;
      readonly width: number;
      readonly height: number;
    }>;
    readonly close: (id: string) => Effect.Effect<void>;
  }
>()("reactor-effect-native/test/support/FarPeer") {
  static readonly layer = Layer.effect(
    FarPeer,
    Effect.gen(function* () {
      const path = yield* Config.String("REACTOR_NATIVE_FAR_PEER").pipe(
        Config.withDefault(
          fileURLToPath(new URL("../rust/target/release/examples/far_peer", import.meta.url)),
        ),
      );
      const spawner = yield* ChildProcessSpawner;
      const outgoing = yield* Queue.unbounded<string, Cause.Done>();
      const waiters = yield* Ref.make<
        ReadonlyArray<{
          readonly op: string;
          readonly id: string;
          readonly reply: Deferred.Deferred<Message>;
        }>
      >([]);
      const child = yield* spawner.spawn(
        ChildProcess.make(path, [], { stdin: "pipe", stdout: "pipe", stderr: "inherit" }),
      );
      // Stopping sends end of input, and the far peer exits on it.
      yield* Effect.addFinalizer(() => Queue.end(outgoing));
      yield* Stream.fromQueue(outgoing).pipe(
        Stream.encodeText,
        Stream.run(child.stdin),
        Effect.forkScoped,
      );
      const deliver = (message: Message) =>
        Ref.modify(waiters, (all) => {
          const index = all.findIndex(
            (waiter) => waiter.op === message.op && waiter.id === (message.id ?? ""),
          );
          return index < 0
            ? [undefined, all]
            : [all[index], [...all.slice(0, index), ...all.slice(index + 1)]];
        }).pipe(
          Effect.flatMap((waiter) =>
            waiter === undefined ? Effect.void : Deferred.succeed(waiter.reply, message),
          ),
        );
      yield* child.stdout.pipe(
        Stream.decodeText,
        Stream.splitLines,
        Stream.mapEffect((line) => Schema.decodeEffect(FarPeerMessage)(line)),
        Stream.runForEach(deliver),
        Effect.forkScoped,
      );
      /** One line of JSON to the far peer. */
      const send = (message: Message) =>
        Schema.encodeEffect(FarPeerMessage)(message).pipe(
          Effect.flatMap((line) => Queue.offer(outgoing, `${line}\n`)),
          Effect.asVoid,
          // The test builds every message; one that cannot encode is a defect in it.
          Effect.orDie,
        );
      /** Sends `message` and waits for the reply named `op` for session `id`. */
      const request = (op: string, id: string, message?: Message) =>
        Effect.gen(function* () {
          const reply = yield* Deferred.make<Message>();
          yield* Ref.update(waiters, (all) => [...all, { op, id, reply }]);
          if (message !== undefined) yield* send(message);
          return yield* Deferred.await(reply);
        });
      yield* request("ready", "");
      const stats = (id: string) =>
        Effect.map(request("stats", id, { op: "stats", id }), (reply) => record(reply.stats));
      return FarPeer.of({
        pid: child.pid,
        answer: (id, sdp) =>
          Effect.map(request("answer", id, { op: "offer", id, sdp }), (reply) => String(reply.sdp)),
        candidate: (id, candidate) =>
          send({
            op: "candidate",
            id,
            candidate: candidate.candidate,
            sdpMid: candidate.sdp_mid,
            sdpMLineIndex: candidate.sdp_mline_index,
          }),
        stats,
        sent: (id) =>
          Effect.map(stats(id), (all) => {
            const video = record(all.video);
            return {
              frames: Number(video.framesSent ?? 0),
              width: Number(video.frameWidth ?? 0),
              height: Number(video.frameHeight ?? 0),
            };
          }),
        close: (id) => Effect.asVoid(request("closed", id, { op: "close", id })),
      });
    }).pipe(Effect.orDie),
  ).pipe(Layer.provide(NodeServices.layer));
}

/**
 * Forwards a peer's ICE candidates to the far peer's session `id`, in order,
 * for the scope's life: the peer's event callback offers each one.
 */
export const candidateRelay = (input: { readonly far: FarPeer["Service"]; readonly id: string }) =>
  Effect.gen(function* () {
    const candidates = yield* Queue.unbounded<IceCandidate>();
    yield* Stream.fromQueue(candidates).pipe(
      Stream.runForEach((candidate) => input.far.candidate(input.id, candidate)),
      Effect.forkScoped,
    );
    return (candidate: IceCandidate): void => {
      Queue.offerUnsafe(candidates, candidate);
    };
  });

/** A peer's decoded media: every native peer has it. */
export const decoded = (peer: Peer): Omit<DecodedMedia, "generation" | "tracks" | "retired"> => {
  if (peer.media._tag !== "Decoded") throw new Error("expected a peer with decoded media");
  return peer.media;
};

/**
 * Each frame's bytes are the whole of an ArrayBuffer of their own: offset 0,
 * no slack and no buffer shared with another frame, so a transfer moves only
 * that frame.
 */
export const assertExactFrames = <A>(check: {
  readonly frames: ReadonlyArray<A>;
  readonly bytes: (frame: A) => ArrayBufferView;
}): void => {
  const seen = new Set<ArrayBufferLike>();
  check.frames.forEach((frame, index) => {
    const view = check.bytes(frame);
    if (view.byteOffset !== 0 || view.buffer.byteLength !== view.byteLength)
      throw new Error(`frame ${index} is not the whole of its buffer`);
    if (seen.has(view.buffer)) throw new Error(`frame ${index} shares its buffer with another`);
    seen.add(view.buffer);
  });
};
