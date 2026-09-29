/**
 * Reactor in a box: Reactor's coordinator and an H3 model simulated in memory
 * at the network edge, so an application and every SDK layer above it run
 * unchanged without a paid session. Provide `layer({ timing })` beneath
 * `Reactor.layer()` in place of an HTTP client and a host; `layerCoordinator`
 * is the coordinator's HTTP API alone, with no host.
 *
 * It models what H3 does, not how fast it does it: every delay comes from the
 * `timing` the caller chooses. A scenario test states the timing it relies on
 * with `Timing.fixed`; a simulation test draws from wide ranges with
 * `Timing.random`, reproducibly for its seed; `Timing.hosted` draws from
 * ranges paid runs measured, for demos. Every delay is an `Effect.sleep`: under
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
  /**
   * `POST /sessions` is refused with 403, or with `status`, allocating nothing
   * either way: from a 5xx the client cannot tell that.
   */
  Schema.TaggedStruct("RefuseAllocation", {
    ...nth,
    status: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 400, maximum: 599 }))),
  }),
  /**
   * `POST /sessions` on a token whose sessions are used allocates a session
   * all the same, where it is otherwise refused 403 `session_limit`. What
   * hosted Reactor answers a spent token is unobserved.
   */
  Schema.TaggedStruct("IgnoreSessionLimit", nth),
  /**
   * `POST /sessions` on a token whose sessions are used answers with the
   * session the token created last, where it is otherwise refused 403
   * `session_limit`. What hosted Reactor answers a spent token is unobserved.
   */
  Schema.TaggedStruct("RepeatSession", nth),
  /** Registering a WebRTC connection is refused with 403. */
  Schema.TaggedStruct("RefuseConnect", nth),
  /**
   * A reconnect is refused with 503, or with `status`: the offer that replaces a connection's SDP
   * (`PUT sdp_params`) takes no effect.
   */
  Schema.TaggedStruct("RefuseReconnect", {
    ...nth,
    status: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 400, maximum: 599 }))),
  }),
  /** The model never answers the command; with `applied` it takes effect and only the reply is lost. */
  Schema.TaggedStruct("DropReply", {
    ...nth,
    command: Schema.String,
    applied: Schema.optionalKey(Schema.Boolean),
  }),
  /**
   * The model takes the command on time and answers it this long late, over
   * the connection the command came on, whichever transport then carries it.
   */
  Schema.TaggedStruct("LateReply", { ...nth, command: Schema.String, after: Schema.Duration }),
  /** A build never finishes and holds the build slot. */
  Schema.TaggedStruct("StallBuild", nth),
  /** A build fails: the model broadcasts `clip_failed` and drops the clip. */
  Schema.TaggedStruct("FailBuild", { ...nth, reason: Schema.optionalKey(Schema.String) }),
  /** A connection drops this long after its channels open; the session goes on. */
  Schema.TaggedStruct("Disconnect", { ...nth, after: Schema.Duration }),
  /** Sessions end this long after they become ready, when that is sooner than their grant. */
  Schema.TaggedStruct("Expire", { after: Schema.Duration }),
  /**
   * No session ends at its grant's cap: each runs until it is terminated, or
   * until 30 s after its last connection drops. Whether hosted Reactor ends a
   * session nothing ever connected to at its cap is unobserved.
   */
  Schema.TaggedStruct("IgnoreCap", {}),
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
  /**
   * `POST /tokens` misgrants: a longer session than was asked for (the
   * default), none of the cap asked for, a session not asked to be bound, a
   * token already expired, or no echo of what it granted.
   */
  Schema.TaggedStruct("OverGrant", {
    ...nth,
    grant: Schema.optionalKey(
      Schema.Literals(["longer", "uncapped", "bound", "expired", "silent"]),
    ),
  }),
  /** A recording's playlist is ready this long after the time its clip predicted. */
  Schema.TaggedStruct("LateRecording", { ...nth, by: Schema.Duration }),
  /**
   * Content moderation flags an enqueue, every one or those with `prompt`.
   * The enqueue is answered as usual; `timing.moderation` later the verdict
   * arrives on the control channel naming no category, input, command or
   * request, as it did in a paid run, and on `terminate` (the default) the
   * session ends, as Reactor documents. `warn` only reports it.
   */
  Schema.TaggedStruct("Moderate", {
    ...nth,
    prompt: Schema.optionalKey(Schema.String),
    action: Schema.optionalKey(Schema.Literals(["terminate", "warn"])),
    /** False ends the session with no verdict sent, which Reactor's docs allow. */
    verdict: Schema.optionalKey(Schema.Boolean),
  }),
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
  /** From a `stop`'s acknowledgement until it takes effect and its clip ends. */
  readonly stop: Range;
  /** From a flagged enqueue until content moderation's verdict, and the session's end on `terminate`. */
  readonly moderation: Range;
  /** Seconds of video built per second of build time; below 1 the queue starves. */
  readonly buildSpeed: { readonly min: number; readonly max: number };
  /** The same for a clip built continuing from another, which hosted H3 built slower. */
  readonly continuedBuildSpeed: { readonly min: number; readonly max: number };
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
    /** `buildSpeed` unless named. */
    readonly continuedBuildSpeed?: number;
    readonly http?: Duration.Input;
    readonly channel?: Duration.Input;
    readonly allocation?: Duration.Input;
    readonly negotiation?: Duration.Input;
    readonly connect?: Duration.Input;
    readonly seam?: Duration.Input;
    readonly stop?: Duration.Input;
    readonly moderation?: Duration.Input;
  }): Timing => ({
    label: "fixed",
    seed: 1,
    http: point(input.http),
    channel: point(input.channel),
    allocation: point(input.allocation),
    negotiation: point(input.negotiation),
    connect: point(input.connect),
    seam: point(input.seam),
    stop: point(input.stop),
    moderation: point(input.moderation),
    buildSpeed: { min: input.buildSpeed, max: input.buildSpeed },
    continuedBuildSpeed: {
      min: input.continuedBuildSpeed ?? input.buildSpeed,
      max: input.continuedBuildSpeed ?? input.buildSpeed,
    },
  }),
  /**
   * Every delay drawn from a range, deliberately wider than anything measured
   * so that code tuned to one provider speed fails: builds from a quarter of
   * real time to ten times it, requests and messages up to 2 s, seams up to
   * half a second, stops landing up to a second after their acknowledgement.
   * Name a narrower range to explore one.
   */
  random: (input: {
    readonly seed: number;
    readonly http?: readonly [Duration.Input, Duration.Input];
    readonly channel?: readonly [Duration.Input, Duration.Input];
    readonly allocation?: readonly [Duration.Input, Duration.Input];
    readonly negotiation?: readonly [Duration.Input, Duration.Input];
    readonly connect?: readonly [Duration.Input, Duration.Input];
    readonly seam?: readonly [Duration.Input, Duration.Input];
    readonly stop?: readonly [Duration.Input, Duration.Input];
    readonly moderation?: readonly [Duration.Input, Duration.Input];
    readonly buildSpeed?: readonly [number, number];
    readonly continuedBuildSpeed?: readonly [number, number];
  }): Timing => ({
    label: `random seed ${input.seed}`,
    seed: input.seed,
    http: range(input.http ?? [0, "2 seconds"]),
    channel: range(input.channel ?? [0, "2 seconds"]),
    allocation: range(input.allocation ?? [0, "5 seconds"]),
    negotiation: range(input.negotiation ?? [0, "2 seconds"]),
    connect: range(input.connect ?? [0, "2 seconds"]),
    seam: range(input.seam ?? [0, "500 millis"]),
    stop: range(input.stop ?? [0, "1 second"]),
    moderation: range(input.moderation ?? [0, "2 seconds"]),
    buildSpeed: {
      min: input.buildSpeed?.[0] ?? 0.25,
      max: input.buildSpeed?.[1] ?? 10,
    },
    continuedBuildSpeed: {
      min: input.continuedBuildSpeed?.[0] ?? 0.25,
      max: input.continuedBuildSpeed?.[1] ?? 10,
    },
  }),
  /**
   * What two paid hosted H3 runs measured on 2026-09-27 (0.6.0 evidence):
   * connect steps of 0.2–0.9 s, eleven 5 s clips built about every 2.1 s
   * while playing, five seams of 30–110 ms and command round trips of 60–90
   * ms; from 0.7.0's two runs on 2026-09-28, a stop landing about 20 ms
   * after its acknowledgement and one continued 5 s clip built in 5.45 s; and
   * from 0.8.0's cut run, a moderation verdict 1.01 s after its enqueue. These
   * are small samples, drawn as ranges and not replayed as a trace: use them
   * for demos and realism checks, not as what a test depends on.
   */
  hosted: {
    label: "hosted H3, paid runs of 2026-09-27 and 2026-09-28",
    seed: 1,
    http: range(["200 millis", "300 millis"]),
    channel: range(["30 millis", "45 millis"]),
    allocation: range(["300 millis", "500 millis"]),
    negotiation: range(["500 millis", "650 millis"]),
    connect: range(["600 millis", "900 millis"]),
    seam: range(["30 millis", "110 millis"]),
    stop: point("20 millis"),
    moderation: point("1 second"),
    buildSpeed: { min: 2.3, max: 2.6 },
    continuedBuildSpeed: { min: 0.92, max: 0.92 },
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
   * The rate the pricing API publishes, in credits a second, as it stated
   * H3's for the paid runs: 125 at 10,000 a dollar is $0.0125 a second.
   */
  creditsPerSecond: count(125),
  creditsPerDollar: count(10_000),
  /** Sessions the account may run at once, as Reactor's default quota; more are refused with 429. */
  concurrentSessions: count(5),
  /**
   * Sessions the account may create a minute, three back to back, as Reactor's
   * default quota; more are refused with 429 and a `Retry-After`.
   */
  sessionsPerMinute: count(10),
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
  /**
   * Whether the deployment records. A recorder answers a clip or recording
   * request with an HLS playlist, ready when the clip says; without one the
   * request fails as a disabled recorder does. Whether hosted H3 records is
   * unobserved.
   */
  recorder: Schema.Boolean.pipe(Schema.withConstructorDefault(Effect.succeed(false))),
  faults: Schema.Array(Fault).pipe(Schema.withConstructorDefault(Effect.succeed([]))),
});
export type Options = typeof Options.Type;

export interface SessionInfo {
  readonly id: string;
  /** INACTIVE: its last connection dropped; it ends 30 s later unless one returns. */
  readonly state: "PENDING" | "ACTIVE" | "INACTIVE" | "STOPPING" | "CLOSED";
  /** One of its connections at least is connected with both channels open. */
  readonly connected: boolean;
  /** DELETE requests received, repeats and ignored ones included. */
  readonly deletes: number;
  /** The creating token's cap, undefined for none, and when that token expires. */
  readonly grant: { readonly maxSessionSeconds: number | undefined; readonly expiresAt: number };
  /** The SDK that created it, as its `client_info` named itself. */
  readonly client: { readonly sdkVersion: string; readonly sdkType: string } | undefined;
}

/** What the sessions cost, billed per second as the pricing API states H3's rate. */
export interface Billing {
  /** From ACTIVE until each session ended, or now. */
  readonly seconds: number;
  readonly usd: number;
}

/**
 * A command the model received, a message it sent, a track paused or resumed,
 * a session's lifecycle step, or a request the coordinator or storage served.
 */
export interface Entry {
  /** Monotonic milliseconds. */
  readonly at: number;
  /** The session it concerns; empty for a request that names none. */
  readonly sessionId: string;
  readonly kind: "session" | "command" | "message" | "upload" | "build" | "track" | "request";
  /** A request's is its method and URL. */
  readonly name: string;
  /** The bearer a request carried: the API key or a token. */
  readonly bearer?: "key" | "token";
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

type Input = Schema.Struct.MakeIn<typeof Options.fields> & { readonly timing: Timing };

/** The simulated coordinator's state, and the services over it. */
const simulate = Effect.fnUntraced(function* (input: Input) {
  const { timing, ...rest } = input;
  const options = Options.make(rest);
  const sessions = yield* Sessions.make(options, yield* Sampler.make(timing));
  const context = Context.make(
    ReactorTest,
    ReactorTest.of({
      apiKey: Redacted.make(options.apiKey),
      sessions: sessions.info,
      billing: sessions.billing,
      log: sessions.log,
      inject: sessions.inject,
    }),
  ).pipe(Context.add(HttpClient.HttpClient, Coordinator.client(sessions)));
  return { sessions, context };
});

/**
 * The simulated coordinator alone: tokens, sessions and their lifetimes,
 * uploads and recordings over its HTTP API, with no host. Its sessions run
 * without a connection, as a session does before its first or after its last.
 */
export const layerCoordinator = (input: Input): Layer.Layer<ReactorTest | HttpClient.HttpClient> =>
  Layer.effectContext(Effect.map(simulate(input), ({ context }) => context));

/** The simulated Reactor, as the network edge `Reactor.layer` needs. */
export const layer = (
  input: Input,
): Layer.Layer<ReactorTest | HttpClient.HttpClient | PeerFactory> =>
  Layer.effectContext(
    Effect.map(simulate(input), ({ sessions, context }) =>
      Context.add(
        context,
        PeerFactory,
        PeerFactory.of({
          check: Effect.void,
          make: Effect.acquireRelease(Peer.make(sessions), (peer) => peer.close),
        }),
      ),
    ),
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

/**
 * Reference media H3's local checks accept, for tests and demos:
 * `pngBytes({ width, height })` is a black PNG, `wavBytes({ seconds })` a WAV tone.
 */
export { pngBytes, wavBytes, type WavOptions } from "./internal/reactorTest/references.js";
