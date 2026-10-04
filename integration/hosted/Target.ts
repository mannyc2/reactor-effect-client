/**
 * Where a check runs: hosted Reactor for money, or `ReactorTest` for free. The
 * check itself is the same program either way; only the network edge, the
 * clock and the way the takeover's owner dies differ.
 */
import * as Console from "effect/Console";
import * as Context from "effect/Context";
import type * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type * as H3 from "reactor-effect-client/H3";
import * as H3Source from "reactor-effect-client/H3Source";
import { PeerFactory } from "reactor-effect-client/Peer";
import type { Peer, PeerEvent } from "reactor-effect-client/Peer";
import { ItemKey } from "reactor-effect-client/Playout";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import * as ReactorTest from "reactor-effect-client/ReactorTest";
import * as NativePeer from "reactor-effect-native/NativePeer";
import * as Family from "./Family.js";
import * as Media from "./Media.js";
import { models, Refused, sessionSeconds, type ModelKey } from "./Spend.js";

export const prompt = "A slow camera move across a sunlit table with a glass of water.";

/** What the takeover's owner reports once it streams: its record, and the clips it queued. */
export const Streaming = Schema.Struct({
  allocation: H3Source.Allocation,
  playing: Schema.String,
  queued: Schema.String,
});
export type Streaming = typeof Streaming.Type;

/** What the owner process reports once it can run, before it reads its grant: its runtime and native peer. */
const Started = Schema.Struct({ started: Schema.String });
/** What the owner process reports as soon as it allocates, before it connects. */
const Allocated = Schema.Struct({ allocated: H3Source.Allocation });
/** What an idle owner reports once its session is connected and set up. */
const Connected = Schema.Struct({ connected: H3Source.Allocation });
/** A line of the owner process's output that its parent reads; any other line is ignored. */
const OwnerLine = Schema.fromJsonString(Schema.Union([Started, Allocated, Streaming, Connected]));

/** The idle owner's grant: its cap stays absent for an uncapped session. */
const IdleGrant = Schema.Struct({
  jwt: Schema.String,
  expiresAt: Schema.Finite,
  maxSessionSeconds: Schema.optionalKey(Schema.Int),
});

/** The grant the owner runs under, handed to it without the API key. */
const OwnerGrant = Schema.Struct({
  jwt: Schema.String,
  expiresAt: Schema.Finite,
  maxSessionSeconds: Schema.Int,
  marker: Schema.String,
});

/** The takeover's owner could not start. */
export class OwnerFailed extends Schema.TaggedError<OwnerFailed>(
  "reactor-effect-integration/hosted/Target/OwnerFailed",
)("OwnerFailed", { message: Schema.String }) {}

/** An owner that connected its session and plays nothing. */
export interface IdleOwner {
  readonly allocation: H3Source.Allocation;
  /** Where the owner ran, as it reported it: its runtime and native peer. */
  readonly host: string | undefined;
  /** Ends the owner as a crash does: nothing it holds is closed or terminated. */
  readonly kill: Effect.Effect<void>;
}

export interface Owner extends IdleOwner, Streaming {}

/** A photo `avatar` and `character` make their avatar from, as its first bytes show it. */
export interface Photo {
  readonly bytes: Uint8Array;
  readonly type: "png" | "jpeg" | "webp";
}

export interface OwnerOptions {
  readonly model?: ModelKey | undefined;
  /**
   * A paid owner runs under Node (`node` on the PATH, 22.18 or newer) with each
   * connection's native peer in a child process of its own; a rehearsal's
   * owner runs in this process on ReactorTest's peers either way.
   */
  readonly isolated?: boolean;
  /** How long the queued clip asks for: the selected model's shortest length unless given. */
  readonly queuedSeconds?: number;
  /**
   * Runs as soon as the owner allocates, before it connects, so a session
   * whose owner fails before streaming is still known and can be ended.
   */
  readonly onAllocated?: (allocation: H3Source.Allocation) => Effect.Effect<void>;
}

export class Target extends Context.Service<
  Target,
  {
    readonly mode: "paid" | "rehearsal";
    readonly apiKey: Redacted.Redacted<string>;
    readonly apiUrl: string;
    readonly network: string;
    /** Where seam frames are written for a person to look at; none in rehearsal. */
    readonly seams: string | undefined;
    /**
     * A prompt meant to be flagged by content moderation, read from a file the
     * operator names; `cut` then ends with it. Never logged or saved.
     */
    readonly moderationPrompt: Redacted.Redacted<string> | undefined;
    /**
     * The photo `avatar` and `character` make their avatar from, read from a
     * file the operator names; a paid run of either refuses without one, and a rehearsal uses a
     * black 64x64 PNG unless one is given. Never logged or saved.
     */
    readonly photo: Photo | undefined;
    /**
     * Why this machine cannot record a showreel, or undefined when its ffmpeg
     * can: asked once, by the run before its clock starts, since a rehearsal's
     * clock runs ahead while a process does.
     */
    readonly cannotRecord: Effect.Effect<string | undefined>;
    /**
     * How long after the owner is killed the adopter starts (takeover, resume,
     * tokens), when a rehearsal sets it; otherwise at once, or for `tokens` once
     * the creating token expired.
     */
    readonly adoptAfterMs: number | undefined;
    /** Starts the takeover's owner on the grant; it returns once the owner streams. */
    readonly owner: (
      grant: CoordinatorClient.TokenGrant,
      marker: string,
      options?: OwnerOptions,
    ) => Effect.Effect<Owner, OwnerFailed, Scope.Scope | Crypto.Crypto>;
    /** Starts an owner that plays nothing; returns once its session is connected and set up. */
    readonly idleOwner: (
      grant: CoordinatorClient.TokenGrant,
      options?: Pick<OwnerOptions, "isolated" | "onAllocated">,
    ) => Effect.Effect<IdleOwner, OwnerFailed, Scope.Scope | Crypto.Crypto>;
    /**
     * Drops every live connection at once, as a network fault would: each
     * session sees its connection go down, and nothing ends remotely. The
     * number of connections dropped.
     */
    readonly sever: Effect.Effect<number>;
  }
>()("reactor-effect-integration/hosted/Target") {}

/** How `Target.sever` reaches the peers the check's sessions hold. */
class Severable extends Context.Service<Severable, { readonly sever: Effect.Effect<number> }>()(
  "reactor-effect-integration/hosted/Target/Severable",
) {}

/**
 * The host's peers, each of which `sever` can cut off as a lost network would:
 * the peer closes, which fences it so it emits and sends nothing more, and the
 * session hears that its connection went down. Nothing ends remotely, and a
 * peer that is never cut is the host's own.
 */
const severable = Layer.effectContext(
  Effect.gen(function* () {
    const host = yield* PeerFactory;
    const live = yield* Ref.make<ReadonlySet<Effect.Effect<boolean>>>(new Set());
    const make = Effect.gen(function* () {
      const peer = yield* host.make;
      const report = yield* Ref.make<((event: PeerEvent) => void) | undefined>(undefined);
      const cut = yield* Ref.make(false);
      const sever = Effect.gen(function* () {
        if (yield* Ref.getAndSet(cut, true)) return false;
        yield* peer.close;
        const emit = yield* Ref.get(report);
        // The session's callback, called synchronously as a host calls it.
        yield* Effect.sync(() => emit?.({ type: "state", state: "disconnected" }));
        return true;
      });
      yield* Effect.acquireRelease(
        Ref.update(live, (all) => new Set(all).add(sever)),
        () => Ref.update(live, (all) => new Set([...all].filter((other) => other !== sever))),
      );
      return {
        ...peer,
        prepare: (servers, tracks, emit) =>
          Effect.andThen(Ref.set(report, emit), peer.prepare(servers, tracks, emit)),
      } satisfies Peer;
    });
    const sever = Effect.flatMap(Ref.get(live), (all) =>
      Effect.map(
        Effect.forEach(all, (one) => one),
        (cut) => cut.filter(Boolean).length,
      ),
    );
    return Context.make(PeerFactory, PeerFactory.of({ check: host.check, make })).pipe(
      Context.add(Severable, Severable.of({ sever })),
    );
  }),
);

/**
 * The isolated owner's arguments to `node`: `script`, this harness's
 * `main.ts`, run from its TypeScript sources, with the isolated native peer.
 */
export const nodeOwnerArgs = (script: string): ReadonlyArray<string> => [
  "--import",
  new URL("./node.ts", import.meta.url).href,
  script,
  "owner",
  "--isolated",
];

/** Model and clip length passed to either paid owner's runtime. */
export const ownerArgs = (
  options: Pick<OwnerOptions, "model" | "queuedSeconds">,
): ReadonlyArray<string> => [
  ...(options.queuedSeconds === undefined
    ? []
    : ["--queued-seconds", String(options.queuedSeconds)]),
  ...(options.model === undefined ? [] : ["--model", options.model]),
];

/** How long a killed owner may take to exit before the check stops waiting for it. */
const ownerExitWait = Duration.seconds(5);

/** A foreign process or owner failure, translated at the host boundary. */
const ownerFailure = (cause: unknown) =>
  Schema.is(OwnerFailed)(cause) ? cause : OwnerFailed.make({ message: String(cause) });

/** The oldest Node that runs this harness from its TypeScript sources. */
const nodeMinimum = { major: 22, minor: 18 };

/**
 * What a paid `adoption` needs of this machine, checked for free: Node 22.18
 * or newer, and the isolated owner command, which given no grant must report
 * that it runs (its native peer's probe child forked and opened a peer) and
 * then exit on the missing grant. Its error output is never read.
 */
export const probeOwner = Effect.fnUntraced(function* (script: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const refuse = (message: string) => Refused.make({ message });
  const node = (yield* spawner
    .string(ChildProcess.make("node", ["--version"]))
    .pipe(Effect.mapError(() => refuse("adoption's owner needs node on the PATH")))).trim();
  const [, major = 0, minor = 0] = /^v(\d+)\.(\d+)\./.exec(node)?.map(Number) ?? [];
  if (major < nodeMinimum.major || (major === nodeMinimum.major && minor < nodeMinimum.minor))
    return yield* refuse(
      `adoption's owner needs node ${nodeMinimum.major}.${nodeMinimum.minor} or newer; the PATH has ${node}`,
    );
  const { lines, exitCode } = yield* Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* spawner.spawn(
        ChildProcess.make("node", nodeOwnerArgs(script), {
          env: { REACTOR_API_KEY: undefined },
          extendEnv: true,
          stdin: "ignore",
          stderr: "ignore",
          killSignal: "SIGKILL",
        }),
      );
      return yield* Effect.all(
        {
          lines: handle.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runCollect),
          exitCode: handle.exitCode,
        },
        { concurrency: "unbounded" },
      );
    }),
  ).pipe(
    Effect.timeout(Duration.seconds(30)),
    Effect.mapError((error) => refuse(`the isolated owner did not exit: ${error.message}`)),
  );
  const started = lines
    .map((line) => Option.getOrUndefined(Schema.decodeOption(Schema.fromJsonString(Started))(line)))
    .find(Predicate.isNotUndefined);
  if (started === undefined)
    return yield* refuse(
      `the isolated owner exited ${exitCode} before it could run: its modules or its native peer failed under ${node}`,
    );
  if (exitCode === 0) return yield* refuse("the isolated owner exited 0 without a grant");
  return { node, host: started.started, exitCode };
});

/** Opens the owner's source and reports its allocation before it connects. */
const opened = Effect.fnUntraced(function* <Req extends Family.RequestInput, C extends H3.Clip>(
  family: Family.Family<Req, C>,
  grant: CoordinatorClient.TokenGrant,
  allocated: ((allocation: H3Source.Allocation) => Effect.Effect<void>) | undefined,
) {
  const recorded = yield* Deferred.make<H3Source.Allocation>();
  const source = yield* family.open({
    tokens: CoordinatorClient.fixedTokens(grant),
    onAllocated: ({ allocation }) =>
      Deferred.succeed(recorded, allocation).pipe(
        Effect.andThen(allocated?.(allocation) ?? Effect.void),
      ),
  });
  return { source, allocation: yield* Deferred.await(recorded) };
});

/**
 * The owner's whole life: open its model's source, play its longest clip with
 * its shortest (or `queuedSeconds`) queued behind it, stream, report, and wait
 * to be killed. It holds the grant, never the API key.
 */
export const own = (input: {
  readonly grant: CoordinatorClient.TokenGrant;
  readonly marker: string;
  readonly model?: ModelKey | undefined;
  readonly queuedSeconds?: number | undefined;
  readonly allocated?: ((allocation: H3Source.Allocation) => Effect.Effect<void>) | undefined;
  readonly announce: (streaming: Streaming) => Effect.Effect<void>;
}) =>
  Family.withFamily({ model: models[input.model ?? "h3"] }, (family) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { grant, marker, announce } = input;
        const { source, allocation } = yield* opened(family, grant, input.allocated);
        yield* source.setAutoplay(true);
        // The attacher knows the playing clip by its id; the queued clip carries its marker.
        const playing = yield* source.enqueue(
          family.request({
            prompt: Family.prompt,
            seconds: family.lengths.long,
            metadata: `${marker}:playing`,
          }),
          { _tag: "Item", key: ItemKey.make("playing") },
        );
        const queued = yield* source.enqueue(
          family.request({
            prompt: Family.prompt,
            seconds: input.queuedSeconds ?? family.lengths.short,
            metadata: `${marker}:queued`,
          }),
          { _tag: "Item", key: ItemKey.make("queued") },
        );
        yield* source.events.pipe(
          Stream.filter(
            (event) => event._tag === "State" && event.state.playing?.clipId === playing,
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        yield* source.video.pipe(Stream.take(24), Stream.runDrain);
        yield* announce({ allocation, playing, queued });
        return yield* Effect.never;
      }),
    ),
  ).pipe(Effect.timeout(Duration.seconds((input.grant.maxSessionSeconds ?? sessionSeconds) - 10)));

/** The idle owner's scoped life: connect, set up, report, then wait to be killed. */
export const idle = (input: {
  readonly grant: CoordinatorClient.TokenGrant;
  readonly allocated?: ((allocation: H3Source.Allocation) => Effect.Effect<void>) | undefined;
  readonly announce: (allocation: H3Source.Allocation) => Effect.Effect<void>;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { allocation } = yield* opened(Family.h3, input.grant, input.allocated);
      yield* input.announce(allocation);
      return yield* Effect.never;
    }),
  ).pipe(Effect.timeout(Duration.seconds((input.grant.maxSessionSeconds ?? sessionSeconds) - 10)));

/** Hosted Reactor over the native peer, in this process. */
export const paid = (input: {
  readonly apiKey: Redacted.Redacted<string>;
  readonly apiUrl: string;
  readonly network: string;
  readonly seams: string;
  readonly script: string;
  readonly moderationPrompt: Redacted.Redacted<string> | undefined;
  readonly photo: Photo | undefined;
}) =>
  Layer.effect(
    Target,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      // A child holds only its grant on stdin; the key and error output stay out of its reports.
      const spawnOwner = Effect.fnUntraced(function* <A>(owner: {
        readonly flags: ReadonlyArray<string>;
        readonly isolated: boolean;
        readonly grant: CoordinatorClient.TokenGrant;
        readonly line: string;
        readonly onAllocated:
          | ((allocation: H3Source.Allocation) => Effect.Effect<void>)
          | undefined;
        readonly reported: (line: typeof OwnerLine.Type) => A | undefined;
        readonly before: string;
      }) {
        const environment = {
          env: { REACTOR_API_KEY: undefined },
          extendEnv: true,
          stderr: "ignore",
        } satisfies ChildProcess.CommandOptions;
        const handle = yield* spawner.spawn(
          owner.isolated
            ? // A handled signal would let the owner close its session, which a crash cannot.
              ChildProcess.make("node", [...nodeOwnerArgs(input.script), ...owner.flags], {
                ...environment,
                killSignal: "SIGKILL",
              })
            : ChildProcess.make(
                process.execPath,
                [input.script, "owner", ...owner.flags],
                environment,
              ),
        );
        yield* Stream.make(new TextEncoder().encode(`${owner.line}\n`)).pipe(
          Stream.run(handle.stdin),
        );
        const host = yield* Ref.make<string | undefined>(undefined);
        const reported = yield* handle.stdout.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.mapEffect((line) => Effect.option(Schema.decodeEffect(OwnerLine)(line))),
          Stream.filter(Option.isSome),
          Stream.map((line) => line.value),
          Stream.tap((line) => {
            if ("started" in line) return Ref.set(host, line.started);
            if ("allocated" in line) return owner.onAllocated?.(line.allocated) ?? Effect.void;
            return Effect.void;
          }),
          Stream.map(owner.reported),
          Stream.filter(Predicate.isNotUndefined),
          Stream.runHead,
          Effect.timeout(Duration.seconds((owner.grant.maxSessionSeconds ?? sessionSeconds) + 5)),
        );
        if (Option.isNone(reported))
          return yield* OwnerFailed.make({ message: `the owner exited before ${owner.before}` });
        return {
          reported: reported.value,
          host: yield* Ref.get(host),
          kill: handle
            .kill({ killSignal: "SIGKILL" })
            .pipe(Effect.timeout(ownerExitWait), Effect.ignore),
        };
      }, Effect.mapError(ownerFailure));
      return Target.of({
        mode: "paid",
        apiKey: input.apiKey,
        apiUrl: input.apiUrl,
        network: input.network,
        adoptAfterMs: undefined,
        seams: input.seams,
        moderationPrompt: input.moderationPrompt,
        photo: input.photo,
        cannotRecord: yield* Effect.cached(
          Effect.provideService(
            Media.cannotRecord,
            ChildProcessSpawner.ChildProcessSpawner,
            spawner,
          ),
        ),
        sever: (yield* Severable).sever,
        owner: (grant, marker, options) =>
          Effect.gen(function* () {
            const owner = yield* spawnOwner({
              flags: ownerArgs(options ?? {}),
              isolated: options?.isolated === true,
              grant,
              line: yield* Schema.encodeEffect(Schema.fromJsonString(OwnerGrant))({
                jwt: Redacted.value(grant.jwt),
                expiresAt: grant.expiresAt,
                maxSessionSeconds: grant.maxSessionSeconds ?? sessionSeconds,
                marker,
              }),
              onAllocated: options?.onAllocated,
              reported: (line) => (Schema.is(Streaming)(line) ? line : undefined),
              before: "streaming",
            });
            return { ...owner.reported, host: owner.host, kill: owner.kill };
          }).pipe(Effect.mapError(ownerFailure)),
        idleOwner: (grant, options) =>
          Effect.gen(function* () {
            const owner = yield* spawnOwner({
              flags: ["--idle"],
              isolated: options?.isolated === true,
              grant,
              line: yield* Schema.encodeEffect(Schema.fromJsonString(IdleGrant))({
                jwt: Redacted.value(grant.jwt),
                expiresAt: grant.expiresAt,
                ...(grant.maxSessionSeconds === undefined
                  ? {}
                  : { maxSessionSeconds: grant.maxSessionSeconds }),
              }),
              onAllocated: options?.onAllocated,
              reported: (line) => ("connected" in line ? line.connected : undefined),
              before: "connecting",
            });
            return { allocation: owner.reported, host: owner.host, kill: owner.kill };
          }).pipe(Effect.mapError(ownerFailure)),
      });
    }),
  ).pipe(
    Layer.provideMerge(Reactor.layer()),
    Layer.provideMerge(CoordinatorClient.layer({ apiUrl: input.apiUrl, apiKey: input.apiKey })),
    Layer.provideMerge(severable),
    Layer.provideMerge(NativePeer.layer()),
    Layer.provideMerge(FetchHttpClient.layer),
  );

/**
 * The owner process's side of a paid takeover: report that it runs, read its
 * grant, then own the session, reporting its allocation and then its record,
 * or its connection when it plays nothing.
 */
export const ownerProcess = <E>(input: {
  readonly lines: Stream.Stream<string, E>;
  /** Its runtime and native peer. */
  readonly host: string;
  readonly queuedSeconds?: number | undefined;
  readonly model?: ModelKey | undefined;
  readonly idle: boolean;
}) =>
  Effect.gen(function* () {
    const report = (line: typeof OwnerLine.Type) =>
      Schema.encodeEffect(OwnerLine)(line).pipe(
        Effect.flatMap((text) => Console.log(text)),
        Effect.orDie,
      );
    yield* report({ started: input.host });
    const first = Option.getOrElse(yield* Stream.runHead(input.lines), () => "");
    const allocated = (allocation: H3Source.Allocation) => report({ allocated: allocation });
    if (input.idle) {
      const owned = yield* Schema.decodeEffect(Schema.fromJsonString(IdleGrant))(first);
      return yield* idle({
        grant: {
          jwt: Redacted.make(owned.jwt),
          expiresAt: owned.expiresAt,
          maxSessionSeconds: owned.maxSessionSeconds,
        },
        allocated,
        announce: (allocation) => report({ connected: allocation }),
      });
    }
    const owned = yield* Schema.decodeEffect(Schema.fromJsonString(OwnerGrant))(first);
    const grant: CoordinatorClient.TokenGrant = {
      jwt: Redacted.make(owned.jwt),
      expiresAt: owned.expiresAt,
      maxSessionSeconds: owned.maxSessionSeconds,
    };
    return yield* own({
      grant,
      marker: owned.marker,
      queuedSeconds: input.queuedSeconds,
      model: input.model,
      allocated,
      announce: report,
    });
  });

/**
 * The simulated Reactor, at the timing two paid runs measured. The owner runs
 * in this process: its kill cuts its network, so neither its connection nor
 * its coordinator requests reach the simulated Reactor, then interrupts it.
 * Run it under `movingClock`, or a test's own `TestClock` kept moving.
 */
export const rehearsal = (input: {
  readonly faults: ReadonlyArray<ReactorTest.Fault>;
  readonly candidate: "host" | "relay";
  readonly adoptAfterMs?: number | undefined;
  readonly moderationPrompt?: Redacted.Redacted<string> | undefined;
  /** The photo `avatar` and `character` make their avatar from; a black 64x64 PNG unless one is given. */
  readonly photo?: Photo | undefined;
  /**
   * Whether the simulated deployment records, so a clip request gets a
   * playlist; by default it answers as a disabled recorder does.
   */
  readonly recorder?: boolean | undefined;
  /** The simulated Reactor's timing: what paid runs measured, unless a test needs other. */
  readonly timing?: ReactorTest.Timing | undefined;
}) =>
  Layer.effect(
    Target,
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const http = yield* HttpClient.HttpClient;
      const peers = yield* PeerFactory;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const host = "in the rehearsal's process, on ReactorTest's peers";
      const inProcess = Effect.fnUntraced(function* <A, E>(
        life: (
          announce: (reported: A) => Effect.Effect<void>,
        ) => Effect.Effect<unknown, E, Reactor.Reactor | Crypto.Crypto>,
      ) {
        const cut = yield* Ref.make(false);
        const gone = (message: string) => ReactorError.fromCode("ChannelClosed", message);
        const client = HttpClient.transform(http, (effect, request) =>
          Effect.flatMap(Ref.get(cut), (isCut) =>
            isCut
              ? Effect.fail(
                  new HttpClientError.HttpClientError({
                    reason: new HttpClientError.TransportError({
                      request,
                      description: "the owner process is gone",
                    }),
                  }),
                )
              : effect,
          ),
        );
        const factory = PeerFactory.of({
          check: peers.check,
          make: Effect.map(peers.make, (peer) => ({
            ...peer,
            send: (channel, bytes) =>
              Effect.flatMap(Ref.get(cut), (isCut) =>
                isCut ? Effect.fail(gone("the owner process is gone")) : peer.send(channel, bytes),
              ),
          })),
        });
        const coordinator = yield* CoordinatorClient.make().pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.mapError((error) => OwnerFailed.make({ message: error.message })),
        );
        const reactor = yield* Reactor.make().pipe(
          Effect.provideService(CoordinatorClient.CoordinatorClient, coordinator),
          Effect.provideService(PeerFactory, factory),
          Effect.mapError((error) => OwnerFailed.make({ message: error.message })),
        );
        const announced = yield* Deferred.make<A, OwnerFailed>();
        const fiber = yield* life((reported) => Deferred.succeed(announced, reported)).pipe(
          Effect.provideService(Reactor.Reactor, reactor),
          Effect.catch((cause) => Deferred.fail(announced, ownerFailure(cause))),
          Effect.forkScoped,
        );
        // Registered after the fiber, so the scope cuts its requests before interrupting it.
        yield* Effect.addFinalizer(() => Ref.set(cut, true));
        return {
          reported: yield* Deferred.await(announced),
          kill: Ref.set(cut, true).pipe(Effect.andThen(Fiber.interrupt(fiber))),
        };
      });
      return Target.of({
        mode: "rehearsal",
        apiKey: test.apiKey,
        apiUrl: CoordinatorClient.defaultApiUrl,
        network: "rehearsal against ReactorTest",
        adoptAfterMs: input.adoptAfterMs,
        seams: undefined,
        moderationPrompt: input.moderationPrompt,
        photo: input.photo ?? {
          bytes: ReactorTest.pngBytes({ width: 64, height: 64 }),
          type: "png",
        },
        cannotRecord: yield* Effect.cached(
          Effect.provideService(
            Media.cannotRecord,
            ChildProcessSpawner.ChildProcessSpawner,
            spawner,
          ),
        ),
        sever: (yield* Severable).sever,
        owner: (grant, marker, options) =>
          Effect.map(
            inProcess((announce: (streaming: Streaming) => Effect.Effect<void>) =>
              own({
                grant,
                marker,
                queuedSeconds: options?.queuedSeconds,
                model: options?.model,
                allocated: options?.onAllocated,
                announce,
              }),
            ),
            ({ reported, kill }) => ({ ...reported, host, kill }),
          ),
        idleOwner: (grant, options) =>
          Effect.map(
            inProcess((announce: (allocation: H3Source.Allocation) => Effect.Effect<void>) =>
              idle({ grant, allocated: options?.onAllocated, announce }),
            ),
            ({ reported, kill }) => ({ allocation: reported, host, kill }),
          ),
      });
    }),
  ).pipe(
    Layer.provideMerge(Reactor.layer()),
    Layer.provideMerge(CoordinatorClient.layer()),
    Layer.provideMerge(severable),
    Layer.provideMerge(
      ReactorTest.layer({
        timing: input.timing ?? ReactorTest.Timing.hosted,
        faults: input.faults,
        candidate: input.candidate,
        recorder: input.recorder === true,
        // H3's published rate on September 30, 2026, so a rehearsal is admitted, or refused,
        // as a paid run is today.
        creditsPerSecond: 350,
        width: 64,
        height: 36,
      }),
    ),
  );

/**
 * A test clock that keeps moving in small steps, so a rehearsal that waits out
 * a session plays in moments and repeats exactly.
 */
export const movingClock = Layer.effectDiscard(ReactorTest.flow().pipe(Effect.forkScoped)).pipe(
  Layer.provideMerge(TestClock.layer()),
);
