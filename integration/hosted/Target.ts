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
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as H3Source from "reactor-effect-client/H3Source";
import { PeerFactory } from "reactor-effect-client/Peer";
import { ItemKey } from "reactor-effect-client/Playout";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import * as ReactorTest from "reactor-effect-client/ReactorTest";
import * as NativePeer from "reactor-effect-native/NativePeer";
import { workSeconds } from "./Spend.js";

export const prompt = "A slow camera move across a sunlit table with a glass of water.";

/** What the takeover's owner reports once it streams: its record, and the clips it queued. */
export const Streaming = Schema.Struct({
  allocation: H3Source.Allocation,
  playing: Schema.String,
  queued: Schema.String,
});
export type Streaming = typeof Streaming.Type;

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

export interface Owner extends Streaming {
  /** Ends the owner as a crash does: nothing it holds is closed or terminated. */
  readonly kill: Effect.Effect<void>;
}

export class Target extends Context.Service<
  Target,
  {
    readonly mode: "paid" | "rehearsal";
    readonly apiKey: Redacted.Redacted<string>;
    readonly apiUrl: string;
    readonly network: string;
    /** How long a clip's media is read once it started, and fresh frames after an attach. */
    readonly windowMs: number;
    /** Where seam frames are written for a person to look at; none in rehearsal. */
    readonly seams: string | undefined;
    /** Starts the takeover's owner on the grant; it returns once the owner streams. */
    readonly owner: (
      grant: Coordinator.TokenGrant,
      marker: string,
    ) => Effect.Effect<Owner, OwnerFailed, Scope.Scope | Crypto.Crypto>;
  }
>()("reactor-effect-integration/hosted/Target") {}

/**
 * The owner's whole life: open an H3 source, play a 15 s clip with a 5 s one
 * queued behind it, stream, report, and wait to be killed. It holds the grant,
 * never the API key.
 */
export const own = (input: {
  readonly grant: Coordinator.TokenGrant;
  readonly marker: string;
  readonly announce: (streaming: Streaming) => Effect.Effect<void>;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { grant, marker, announce } = input;
      const recorded = yield* Deferred.make<H3Source.Allocation>();
      const source = yield* H3Source.open({
        mint: Effect.succeed(grant),
        onAllocated: ({ allocation }) => Deferred.succeed(recorded, allocation),
      });
      const allocation = yield* Deferred.await(recorded);
      yield* source.setAutoplay(true);
      // H3 keeps no history, so an attacher knows the playing clip only by its id; the clip
      // queued behind it is the one whose metadata it can read.
      const playing = yield* source.enqueue(
        { prompt, seconds: 15, metadata: `${marker}:playing` },
        { _tag: "Item", key: ItemKey.make("playing") },
      );
      const queued = yield* source.enqueue(
        { prompt, seconds: 5, metadata: `${marker}:queued` },
        { _tag: "Item", key: ItemKey.make("queued") },
      );
      yield* source.events.pipe(
        Stream.filter((event) => event._tag === "State" && event.state.playing?.clipId === playing),
        Stream.take(1),
        Stream.runDrain,
      );
      yield* source.video.pipe(Stream.take(24), Stream.runDrain);
      yield* announce({ allocation, playing, queued });
      return yield* Effect.never;
    }),
  ).pipe(Effect.timeout(Duration.seconds(workSeconds)));

/** Hosted Reactor over the native peer, in this process. */
export const paid = (input: {
  readonly apiKey: Redacted.Redacted<string>;
  readonly apiUrl: string;
  readonly network: string;
  readonly seams: string;
  readonly script: string;
}) =>
  Layer.effect(
    Target,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      return Target.of({
        mode: "paid",
        apiKey: input.apiKey,
        apiUrl: input.apiUrl,
        network: input.network,
        windowMs: 6_000,
        seams: input.seams,
        // The owner is a child process, so killing it is a real crash. It gets the grant on
        // its stdin and never the API key.
        owner: (grant, marker) =>
          Effect.gen(function* () {
            const handle = yield* spawner.spawn(
              ChildProcess.make(process.execPath, [input.script, "owner"], {
                env: { REACTOR_API_KEY: undefined },
                extendEnv: true,
              }),
            );
            const text = yield* Schema.encodeEffect(Schema.fromJsonString(OwnerGrant))({
              jwt: Redacted.value(grant.jwt),
              expiresAt: grant.expiresAt,
              maxSessionSeconds: grant.granted.maxSessionSeconds,
              marker,
            });
            yield* Stream.make(new TextEncoder().encode(`${text}\n`)).pipe(
              Stream.run(handle.stdin),
            );
            const streaming = yield* handle.stdout.pipe(
              Stream.decodeText(),
              Stream.splitLines,
              Stream.mapEffect((line) =>
                Effect.option(Schema.decodeEffect(Schema.fromJsonString(Streaming))(line)),
              ),
              Stream.filter(Option.isSome),
              Stream.map((line) => line.value),
              Stream.runHead,
              Effect.timeout(Duration.seconds(workSeconds + 15)),
            );
            if (Option.isNone(streaming))
              return yield* OwnerFailed.make({ message: "the owner exited before streaming" });
            return {
              ...streaming.value,
              kill: Effect.ignore(handle.kill({ killSignal: "SIGKILL" })),
            };
          }).pipe(
            Effect.mapError((cause) =>
              Schema.is(OwnerFailed)(cause) ? cause : OwnerFailed.make({ message: String(cause) }),
            ),
          ),
      });
    }),
  ).pipe(
    Layer.provideMerge(Reactor.layer()),
    Layer.provideMerge(Coordinator.layer({ apiUrl: input.apiUrl, apiKey: input.apiKey })),
    Layer.provideMerge(NativePeer.layer()),
    Layer.provideMerge(FetchHttpClient.layer),
  );

/** The owner process's side of a paid takeover: read its grant, then own the session. */
export const ownerProcess = <E>(lines: Stream.Stream<string, E>) =>
  Effect.gen(function* () {
    const first = yield* Stream.runHead(lines);
    const input = yield* Schema.decodeEffect(Schema.fromJsonString(OwnerGrant))(
      Option.getOrElse(first, () => ""),
    );
    const grant: Coordinator.TokenGrant = {
      jwt: Redacted.make(input.jwt),
      expiresAt: input.expiresAt,
      granted: { maxSessions: 1, maxSessionSeconds: input.maxSessionSeconds },
    };
    return yield* own({
      grant,
      marker: input.marker,
      announce: (streaming) =>
        Schema.encodeEffect(Schema.fromJsonString(Streaming))(streaming).pipe(
          Effect.flatMap((text) => Console.log(text)),
          Effect.orDie,
        ),
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
}) =>
  Layer.effect(
    Target,
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const http = yield* HttpClient.HttpClient;
      const peers = yield* PeerFactory;
      return Target.of({
        mode: "rehearsal",
        apiKey: test.apiKey,
        apiUrl: Coordinator.defaultApiUrl,
        network: "rehearsal against ReactorTest",
        windowMs: 1_500,
        seams: undefined,
        owner: (grant, marker) =>
          Effect.gen(function* () {
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
                    isCut
                      ? Effect.fail(gone("the owner process is gone"))
                      : peer.send(channel, bytes),
                  ),
              })),
            });
            const coordinator = yield* Coordinator.make().pipe(
              Effect.provideService(HttpClient.HttpClient, client),
              Effect.mapError((error) => OwnerFailed.make({ message: error.message })),
            );
            const reactor = yield* Reactor.make().pipe(
              Effect.provideService(Coordinator.Coordinator, coordinator),
              Effect.provideService(PeerFactory, factory),
              Effect.mapError((error) => OwnerFailed.make({ message: error.message })),
            );
            const announced = yield* Deferred.make<Streaming, OwnerFailed>();
            const fiber = yield* own({
              grant,
              marker,
              announce: (streaming) => Deferred.succeed(announced, streaming),
            }).pipe(
              Effect.provideService(Reactor.Reactor, reactor),
              Effect.catch((cause) =>
                Deferred.fail(announced, OwnerFailed.make({ message: String(cause) })),
              ),
              Effect.forkDetach,
            );
            const streaming = yield* Deferred.await(announced);
            return {
              ...streaming,
              kill: Ref.set(cut, true).pipe(Effect.andThen(Fiber.interrupt(fiber))),
            };
          }),
      });
    }),
  ).pipe(
    Layer.provideMerge(Reactor.layer()),
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(
      ReactorTest.layer({
        timing: ReactorTest.Timing.hosted,
        faults: input.faults,
        candidate: input.candidate,
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
