/**
 * What the native tests share: the scripted fake addon, a fixture coordinator,
 * the far peer process the media tests receive from, and small assertions.
 */
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the fake addon is steered through files another process reads
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the fake addon's directory is made synchronously, beside the module it loads
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";
import * as Coordinator from "reactor-effect-client/Coordinator";
import type { IceCandidate } from "reactor-effect-client/Coordinator";
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

/** What a test that makes a canonical client needs: an HTTP client over `Fetch`, and Node's services. */
export const clientServices = Layer.merge(FetchHttpClient.layer, NodeServices.layer);

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
 * The scripted fake addon in a directory of its own. `path` is a module
 * another process can load; `module` is its twin in this process. Holding a
 * call keeps it from answering in either.
 */
const makeFakeAddon = () => {
  const directory = mkdtempSync(join(tmpdir(), "reactor-native-addon-"));
  const path = join(directory, "addon.cjs");
  writeFileSync(path, `module.exports = require(${JSON.stringify(fixture)}).make(__dirname);\n`);
  const marker = (name: string) => join(directory, name);
  const calls = (): ReadonlyArray<string> =>
    existsSync(marker("calls.log"))
      ? readFileSync(marker("calls.log"), "utf8")
          .split("\n")
          .filter((line) => line !== "")
      : [];
  return {
    directory,
    path,
    module: make(directory),
    hold: (call: "stats" | "shutdown", held: boolean): void =>
      held
        ? writeFileSync(marker(`hold-${call}`), "")
        : rmSync(marker(`hold-${call}`), { force: true }),
    calls,
    /** Wait until `count` calls named `call` have reached the fake. */
    reached: (call: string, count = 1) =>
      eventually({
        condition: () => calls().filter((name) => name === call).length >= count,
        message: `${call} never reached the fake addon`,
      }),
    remove: () => rmSync(directory, { recursive: true, force: true }),
  };
};

/** The fake addon, removed with the scope. */
export const fakeAddon = Effect.acquireRelease(Effect.sync(makeFakeAddon), (addon) =>
  Effect.sync(addon.remove),
);

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

const fixtureDescriptor = (id: string, tracks: ReadonlyArray<unknown>) => ({
  session_id: id,
  state: "ACTIVE",
  capabilities: {
    protocol_version: "1.0",
    tracks,
    commands: [{ name: "echo", schema: {} }],
  },
  selected_transport: { protocol: "webrtc", version: "1.0" },
});

/**
 * A coordinator that allocates one session per POST, named after `sessionId`,
 * and reads a deleted session as absent; its `fetch` stands in for the network.
 */
export const fixtureCoordinator = (input: {
  readonly sessionId: string;
  readonly tracks: ReadonlyArray<unknown>;
}) => {
  const allocated: Array<string> = [],
    deleted = new Set<string>();
  const route = (request: Request): Response => {
    const path = new URL(request.url).pathname;
    const id = /^\/sessions\/([^/]+)/.exec(path)?.[1] ?? input.sessionId;
    if (path === "/sessions" && request.method === "POST") {
      const allocation =
        allocated.length === 0 ? input.sessionId : `${input.sessionId}_${allocated.length}`;
      allocated.push(allocation);
      return Response.json(fixtureDescriptor(allocation, input.tracks));
    }
    if (path === `/sessions/${id}` && request.method === "GET")
      return deleted.has(id)
        ? Response.json({ error: "session not found" }, { status: 404 })
        : Response.json(fixtureDescriptor(id, input.tracks));
    if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
    if (path.endsWith("/connections")) return Response.json({ connection_id: 1001 });
    if (path.endsWith("/ice_candidates")) return new Response(null, { status: 204 });
    if (path.endsWith("/sdp_params"))
      return request.method === "GET"
        ? Response.json({ sdp_answer: "fixture native answer" })
        : new Response(null, { status: 204 });
    if (path === `/sessions/${id}` && request.method === "DELETE") {
      deleted.add(id);
      return new Response(null, { status: 202 });
    }
    return Response.json({ error: `unhandled fixture route ${path}` }, { status: 404 });
  };
  const fetch: typeof globalThis.fetch = (url, init) =>
    Promise.resolve(route(new Request(url, init)));
  return { fetch, allocated, deleted };
};

/**
 * A coordinator that allocates one session, `id`, and relays its signaling to
 * the far peer, which drops candidates sent before the offer; its own arrive
 * in its answer.
 */
export const farPeerCoordinator = (input: {
  readonly far: FarPeer["Service"];
  readonly id: string;
  readonly tracks: ReadonlyArray<unknown>;
}) => {
  const { far, id } = input;
  const descriptor = fixtureDescriptor(id, input.tracks);
  const state: { answer: Promise<string> | undefined; deleted: boolean } = {
    answer: undefined,
    deleted: false,
  };
  const route = (request: Request): Promise<Response> => {
    const path = new URL(request.url).pathname;
    if (path === "/sessions" && request.method === "POST")
      return Promise.resolve(Response.json(descriptor));
    if (path === `/sessions/${id}` && request.method === "GET")
      return Promise.resolve(
        state.deleted
          ? Response.json({ error: "session not found" }, { status: 404 })
          : Response.json(descriptor),
      );
    if (path === `/sessions/${id}` && request.method === "DELETE") {
      state.deleted = true;
      return Promise.resolve(new Response(null, { status: 202 }));
    }
    if (path.endsWith("/ice_servers")) return Promise.resolve(Response.json({ ice_servers: [] }));
    if (path.endsWith("/connections")) return Promise.resolve(Response.json({ connection_id: 1 }));
    if (path.endsWith("/ice_candidates"))
      return request
        .json()
        .then((body: unknown) => {
          const candidates = record(body).candidates;
          return Effect.runPromise(
            Effect.forEach(Array.isArray(candidates) ? candidates : [], (candidate) =>
              far.candidate(id, candidate as IceCandidate),
            ),
          );
        })
        .then(() => new Response(null, { status: 204 }));
    if (path.endsWith("/sdp_params"))
      return request.method === "GET"
        ? (state.answer ?? Promise.resolve(undefined)).then((sdp) =>
            Response.json({ sdp_answer: sdp }),
          )
        : request.json().then((body: unknown) => {
            state.answer = Effect.runPromise(far.answer(id, String(record(body).sdp_offer)));
            return new Response(null, { status: 204 });
          });
    return Promise.resolve(Response.json({ error: `unhandled route ${path}` }, { status: 404 }));
  };
  const fetch: typeof globalThis.fetch = (url, init) => route(new Request(url, init));
  return { fetch };
};

/** Runs `effect` with `fetch` as the network the canonical client's HTTP client uses. */
export const withFetch =
  (fetch: typeof globalThis.fetch) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.provideService(effect, FetchHttpClient.Fetch, fetch);

export type Message = Readonly<Record<string, unknown>>;

/** One line of the far peer's JSON protocol, either way. */
const FarPeerMessage = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));

export const record = (value: unknown): Message =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Message) : {};

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
    readonly sent: (
      id: string,
    ) => Effect.Effect<{
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
        Stream.runForEach((message) => deliver(record(message))),
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

/** An error with its Redacted diagnostic detail revealed, for assertions on what it recorded. */
export const revealed = <E extends { readonly context: { readonly detail?: unknown } }>(
  error: E,
) => {
  const detail = error.context.detail;
  return {
    ...error,
    reason: (error as { readonly reason?: unknown }).reason,
    message: (error as { readonly message?: unknown }).message,
    context: {
      ...error.context,
      detail: Redacted.isRedacted(detail) ? Redacted.value(detail) : detail,
    },
  };
};

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
