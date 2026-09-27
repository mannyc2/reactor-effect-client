/**
 * Reactor in a box: Reactor's coordinator and an H3 model simulated in memory
 * at the network edge, so an application and every SDK layer above it run
 * unchanged without a paid session. Provide `layer()` beneath `Reactor.layer()`
 * in place of an HTTP client and a host. Every delay is an `Effect.sleep`:
 * under `TestClock`, fork `flow()` and the run is deterministic; on the live
 * clock it plays in real time.
 */
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as Coordinator from "./internal/reactorTest/coordinator.js";
import { clipId } from "./internal/reactorTest/h3.js";
import * as Media from "./internal/reactorTest/media.js";
import * as Peer from "./internal/reactorTest/peer.js";
import * as Sessions from "./internal/reactorTest/sessions.js";
import type { VideoFrame } from "./Media.js";
import { PeerFactory } from "./Peer.js";

const Count = Schema.Int.check(Schema.isGreaterThan(0));
const nth = { nth: Schema.optionalKey(Count) };

/**
 * A fault the simulated Reactor injects. `nth` counts the occurrences the
 * fault matches since it was added, from 1; without it every one matches.
 */
export const Fault = Schema.Union([
  /** `POST /sessions` is refused with 403. */
  Schema.TaggedStruct("RefuseAllocation", nth),
  /** Registering a WebRTC connection is refused with 403. */
  Schema.TaggedStruct("RefuseConnect", nth),
  /** The model never answers the command; with `applied` it takes effect and only the reply is lost. */
  Schema.TaggedStruct("DropReply", {
    ...nth,
    command: Schema.String,
    applied: Schema.optionalKey(Schema.Boolean),
  }),
  /** A build never finishes and holds the build slot. */
  Schema.TaggedStruct("StallBuild", nth),
  /** A build fails: the model broadcasts `clip_failed` and drops the clip. */
  Schema.TaggedStruct("FailBuild", { ...nth, reason: Schema.optionalKey(Schema.String) }),
  /** A connection drops this long after its channels open; the session goes on. */
  Schema.TaggedStruct("Disconnect", { ...nth, after: Schema.Duration }),
  /** Sessions end this long after they become ready, when that is sooner than their grant. */
  Schema.TaggedStruct("Expire", { after: Schema.Duration }),
  /** DELETE is accepted and the session reads STOPPING this long before it closes. */
  Schema.TaggedStruct("SlowDelete", { for: Schema.Duration }),
  /** DELETE is accepted and the session runs on until its grant ends. */
  Schema.TaggedStruct("IgnoreDelete", {}),
  /** While a clip plays, video is absent, black or one repeated frame. */
  Schema.TaggedStruct("Video", { video: Schema.Literals(["absent", "black", "frozen"]) }),
  /** The session offers audio and sends none. */
  Schema.TaggedStruct("NoAudio", {}),
]);
export type Fault = typeof Fault.Type;

const millis = (value: number) =>
  Schema.Duration.pipe(Schema.withConstructorDefault(Effect.succeed(Duration.millis(value))));
const count = (value: number) => Count.pipe(Schema.withConstructorDefault(Effect.succeed(value)));

/**
 * How the simulated Reactor behaves. Defaults marked measured come from paid
 * hosted H3 runs in September 2026; the others are assumptions.
 */
export const Options = Schema.Struct({
  /** The key `POST /tokens` accepts. */
  apiKey: Schema.String.pipe(Schema.withConstructorDefault(Effect.succeed("reactor-test-api-key"))),
  /** Each coordinator request, one way; 40 ms (assumed). */
  httpLatency: millis(40),
  /** Each channel message and media frame, one way; 20 ms (assumed). */
  channelLatency: millis(20),
  /** From `POST /sessions` until the session is ACTIVE and billed; 1 s (assumed). */
  allocation: millis(1_000),
  /** From the SDP offer until its answer is ready; 50 ms (assumed). */
  negotiation: millis(50),
  /** From the SDP answer until both channels open; 100 ms (assumed). */
  connect: millis(100),
  /** Seconds of video built per second; 2.4 (measured: a 5 s clip about every 2.1 s while playing). */
  buildSpeed: Schema.Finite.check(Schema.isGreaterThan(0)).pipe(
    Schema.withConstructorDefault(Effect.succeed(2.4)),
  ),
  /** From `clip_finished` to the next `clip_started` under autoplay; 30–110 ms (measured). */
  seamMin: millis(30),
  seamMax: millis(110),
  /** The queue capacities H3's state reports. */
  generationCapacity: count(20),
  playoutCapacity: count(10),
  /** The published rate: 125 credits a second at 10,000 a dollar, $0.75 a minute (measured). */
  creditsPerSecond: count(125),
  creditsPerDollar: count(10_000),
  /** Decoded frame size. */
  width: count(16),
  height: count(16),
  /** Seeds the seam delays, so a run repeats. */
  seed: Schema.Int.pipe(Schema.withConstructorDefault(Effect.succeed(1))),
  faults: Schema.Array(Fault).pipe(Schema.withConstructorDefault(Effect.succeed([]))),
});
export type Options = typeof Options.Type;

export interface SessionInfo {
  readonly id: string;
  readonly state: "PENDING" | "ACTIVE" | "STOPPING" | "CLOSED";
  /** A peer is bound and both its channels are open. */
  readonly connected: boolean;
  /** DELETE requests received, repeats and ignored ones included. */
  readonly deletes: number;
  readonly grant: { readonly maxSessionSeconds: number; readonly expiresAt: number };
}

export interface Billing {
  /** From ACTIVE until the session ended, or now. */
  readonly seconds: number;
  /** Whole billed minutes, counted per session. */
  readonly minutes: number;
  readonly usd: number;
}

/** A command the model received, a message it sent, or a session's lifecycle step. */
export interface Entry {
  /** Monotonic milliseconds. */
  readonly at: number;
  readonly sessionId: string;
  readonly kind: "session" | "command" | "message";
  readonly name: string;
  readonly clipId?: string;
  /** What a `DropReply` fault took: the reply only, or the whole command. */
  readonly dropped?: "reply" | "command";
}

/** Inspects and disturbs the simulated Reactor. */
export class ReactorTest extends Context.Service<
  ReactorTest,
  {
    /** The key the simulated `POST /tokens` accepts. */
    readonly apiKey: Redacted.Redacted<string>;
    readonly sessions: Effect.Effect<ReadonlyArray<SessionInfo>>;
    readonly billing: Effect.Effect<Billing>;
    /** Commands received, messages sent and session lifecycle, in order. */
    readonly log: Effect.Effect<ReadonlyArray<Entry>>;
    /** Arms a fault from now on; its `nth` counts from this call. */
    readonly inject: (fault: Fault) => Effect.Effect<void>;
  }
>()("reactor-effect-client/ReactorTest") {}

/** The simulated Reactor, as the network edge `Reactor.layer` needs. */
export const layer = (
  input: Schema.Struct.MakeIn<typeof Options.fields> = {},
): Layer.Layer<ReactorTest | HttpClient.HttpClient | PeerFactory> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const options = Options.make(input);
      const sessions = yield* Sessions.make(options);
      return Context.make(
        ReactorTest,
        ReactorTest.of({
          apiKey: Redacted.make(options.apiKey),
          sessions: sessions.info,
          billing: sessions.billing,
          log: sessions.log,
          inject: sessions.inject,
        }),
      ).pipe(
        Context.add(HttpClient.HttpClient, Coordinator.client(sessions)),
        Context.add(
          PeerFactory,
          PeerFactory.of({
            check: Effect.void,
            make: Effect.acquireRelease(
              Effect.sync(() => Peer.make(sessions)),
              (peer) => peer.close,
            ),
          }),
        ),
      );
    }),
  );

/** The clip a simulated frame belongs to and its index in the clip; undefined when black. */
export const frameOf = (
  frame: VideoFrame,
): { readonly clipId: string; readonly index: number } | undefined => {
  const decoded = Media.decode(frame.data);
  return decoded && { clipId: clipId(decoded.ordinal), index: decoded.index };
};

/**
 * Keeps a `TestClock` moving in `step`s (5 ms by default) while it runs, so a
 * scenario written as straight-line code advances in virtual time and each
 * message chain lands within a step of when it would. Fork it into the test's
 * scope; one large `TestClock.adjust` can run ahead of a fiber still being
 * woken.
 */
export const flow = (step: Duration.Input = "5 millis"): Effect.Effect<never> =>
  Effect.forever(TestClock.adjust(step));
