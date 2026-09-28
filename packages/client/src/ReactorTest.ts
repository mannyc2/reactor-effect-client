/**
 * Reactor in a box: Reactor's coordinator and an H3 model simulated in memory
 * at the network edge, so an application and every SDK layer above it run
 * unchanged without a paid session. Provide `layer({ timing })` beneath
 * `Reactor.layer()` in place of an HTTP client and a host.
 *
 * It models what H3 does, not how fast it does it: every delay comes from the
 * `timing` the caller chooses. A scenario test states the timing it relies on
 * with `Timing.fixed`; a simulation test draws from wide ranges with
 * `Timing.random`, reproducibly for its seed; `Timing.hosted` replays what two
 * paid runs measured, for demos. Every delay is an `Effect.sleep`: under
 * `TestClock`, fork `flow()` and the run is deterministic; on the live clock
 * it plays in real time.
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
import * as Sampler from "./internal/reactorTest/timing.js";
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
  /** An enqueue with reference images is refused because one of them is invalid. */
  Schema.TaggedStruct("InvalidImage", nth),
  /** `POST /tokens` grants a longer session than was asked for. */
  Schema.TaggedStruct("OverGrant", nth),
]);
export type Fault = typeof Fault.Type;

/** A delay drawn uniformly from `min` to `max`; equal ends make it fixed. */
export interface Range {
  readonly min: Duration.Duration;
  readonly max: Duration.Duration;
}

/** Where each simulated delay comes from. */
export interface Timing {
  /** Where the numbers come from, for a reader of a failing run. */
  readonly label: string;
  /** Seeds every draw, so a run repeats. */
  readonly seed: number;
  /** Each coordinator request, once. */
  readonly http: Range;
  /**
   * Each channel message, one way. A connection's media arrives at one delay
   * from this range, drawn when the connection opens.
   */
  readonly channel: Range;
  /** From `POST /sessions` until the session is ACTIVE and billed. */
  readonly allocation: Range;
  /** From the SDP offer until its answer is ready. */
  readonly negotiation: Range;
  /** From the SDP answer until both channels open. */
  readonly connect: Range;
  /** From `clip_finished` to the next `clip_started` under autoplay. */
  readonly seam: Range;
  /** Seconds of video built per second of build time; below 1 the queue starves. */
  readonly buildSpeed: { readonly min: number; readonly max: number };
}

const point = (input: Duration.Input | undefined): Range => {
  const value = Duration.fromInputUnsafe(input ?? 0);
  return { min: value, max: value };
};
const range = (bounds: readonly [Duration.Input, Duration.Input]): Range => ({
  min: Duration.fromInputUnsafe(bounds[0]),
  max: Duration.fromInputUnsafe(bounds[1]),
});

export const Timing = {
  /**
   * Every delay fixed: zero unless named. A scenario test names the ones its
   * outcome depends on.
   */
  fixed: (input: {
    readonly buildSpeed: number;
    readonly http?: Duration.Input;
    readonly channel?: Duration.Input;
    readonly allocation?: Duration.Input;
    readonly negotiation?: Duration.Input;
    readonly connect?: Duration.Input;
    readonly seam?: Duration.Input;
  }): Timing => ({
    label: "fixed",
    seed: 1,
    http: point(input.http),
    channel: point(input.channel),
    allocation: point(input.allocation),
    negotiation: point(input.negotiation),
    connect: point(input.connect),
    seam: point(input.seam),
    buildSpeed: { min: input.buildSpeed, max: input.buildSpeed },
  }),
  /**
   * Every delay drawn from a range, deliberately wider than anything measured
   * so that code tuned to one provider speed fails: builds from a quarter of
   * real time to ten times it, requests and messages up to 2 s, seams up to
   * half a second. Name a narrower range to explore one.
   */
  random: (input: {
    readonly seed: number;
    readonly http?: readonly [Duration.Input, Duration.Input];
    readonly channel?: readonly [Duration.Input, Duration.Input];
    readonly allocation?: readonly [Duration.Input, Duration.Input];
    readonly negotiation?: readonly [Duration.Input, Duration.Input];
    readonly connect?: readonly [Duration.Input, Duration.Input];
    readonly seam?: readonly [Duration.Input, Duration.Input];
    readonly buildSpeed?: readonly [number, number];
  }): Timing => ({
    label: `random seed ${input.seed}`,
    seed: input.seed,
    http: range(input.http ?? [0, "2 seconds"]),
    channel: range(input.channel ?? [0, "2 seconds"]),
    allocation: range(input.allocation ?? [0, "5 seconds"]),
    negotiation: range(input.negotiation ?? [0, "2 seconds"]),
    connect: range(input.connect ?? [0, "2 seconds"]),
    seam: range(input.seam ?? [0, "500 millis"]),
    buildSpeed: {
      min: input.buildSpeed?.[0] ?? 0.25,
      max: input.buildSpeed?.[1] ?? 10,
    },
  }),
  /**
   * What two paid hosted H3 runs measured on 2026-09-27 (0.6.0 evidence):
   * connect steps of 0.2–0.9 s, eleven 5 s clips built about every 2.1 s
   * while playing, five seams of 30–110 ms and command round trips of 60–90
   * ms. Two runs are a small sample: use it for demos and realism checks, not
   * as what a test depends on.
   */
  hosted: {
    label: "hosted H3, 2 paid runs, 2026-09-27",
    seed: 1,
    http: range(["200 millis", "300 millis"]),
    channel: range(["30 millis", "45 millis"]),
    allocation: range(["300 millis", "500 millis"]),
    negotiation: range(["500 millis", "650 millis"]),
    connect: range(["600 millis", "900 millis"]),
    seam: range(["30 millis", "110 millis"]),
    buildSpeed: { min: 2.3, max: 2.6 },
  } satisfies Timing,
};

const count = (value: number) => Count.pipe(Schema.withConstructorDefault(Effect.succeed(value)));

/** The simulated Reactor's fixed facts; its delays come from `Timing`. */
export const Options = Schema.Struct({
  /** The key `POST /tokens` accepts. */
  apiKey: Schema.String.pipe(Schema.withConstructorDefault(Effect.succeed("reactor-test-api-key"))),
  /** The queue capacities H3's state reports. */
  generationCapacity: count(20),
  playoutCapacity: count(10),
  /**
   * The rate the pricing API publishes, in credits a minute: 7,500 at 10,000
   * a dollar is $0.75 a minute.
   */
  creditsPerMinute: count(7_500),
  creditsPerDollar: count(10_000),
  /** Decoded frame size. */
  width: count(16),
  height: count(16),
  /**
   * The local candidate type of the pair a peer's statistics report carrying
   * its media: `relay` stands for a connection through TURN.
   */
  candidate: Schema.Literals(["host", "srflx", "relay"]).pipe(
    Schema.withConstructorDefault(Effect.succeed("host" as const)),
  ),
  /** Whether the deployment's `enqueue` declares `reference_audios`. */
  referenceAudio: Schema.Boolean.pipe(Schema.withConstructorDefault(Effect.succeed(true))),
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
  readonly kind: "session" | "command" | "message" | "upload" | "build";
  readonly name: string;
  readonly clipId?: string;
  /**
   * A build that asked to continue from this clip: its name says whether it
   * `continued` or built `independent`ly, which H3 itself never reports.
   */
  readonly continuedFrom?: string;
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
  input: Schema.Struct.MakeIn<typeof Options.fields> & { readonly timing: Timing },
): Layer.Layer<ReactorTest | HttpClient.HttpClient | PeerFactory> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const { timing, ...rest } = input;
      const options = Options.make(rest);
      const sessions = yield* Sessions.make(options, yield* Sampler.make(timing));
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
            make: Effect.acquireRelease(Peer.make(sessions), (peer) => peer.close),
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
