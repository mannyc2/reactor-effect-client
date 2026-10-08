import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/http-api";

/**
 * The channel's HTTP contract, kept apart from the server so a client (the
 * page, the test, or another service) can derive a typed client from it
 * without the server's dependencies.
 */

export const PromptRequest = Schema.Struct({
  prompt: Schema.Trimmed.check(Schema.isBetweenLength(1, 2000)),
});

/**
 * The playout admitted the prompt as an item under `key`. Building, airing
 * and how it ended follow in the status; the server never sends it twice.
 */
export const Submitted = Schema.TaggedStruct("Accepted", { key: Schema.String }).pipe(
  HttpApiSchema.status(202),
);
export type Submitted = typeof Submitted.Type;

export class ChannelBusy extends Schema.TaggedError<ChannelBusy>()(
  "ChannelBusy",
  { message: Schema.String, retryAfterSeconds: Schema.Int },
  { httpApiStatus: 429 },
) {}

export class ChannelUnavailable extends Schema.TaggedError<ChannelUnavailable>()(
  "ChannelUnavailable",
  { message: Schema.String },
  { httpApiStatus: 503 },
) {}

export class PromptRejected extends Schema.TaggedError<PromptRejected>()(
  "PromptRejected",
  { message: Schema.String },
  { httpApiStatus: 422 },
) {}

/** Whose a clip is: a viewer's prompt, the house rotation, or a clip this channel did not send. */
const Clip = {
  /** The viewer item's key. */
  key: Schema.NullOr(Schema.String),
  /** The house rotation's index for the clip, once the playout has named it. */
  house: Schema.NullOr(Schema.Int),
  origin: Schema.Literals(["viewer", "house", "other"]),
  /** The words it was asked for with; null where the channel does not know them. */
  prompt: Schema.NullOr(Schema.String),
};

/** How a clip left the air, or settled without airing, in the playout's words. Times are epoch ms. */
export const Outcome = Schema.Union([
  Schema.TaggedStruct("Ended", {
    at: Schema.Finite,
    /** Reported for a viewer's clip; the playout reports a house clip's end without one. */
    termination: Schema.NullOr(Schema.Literals(["finished", "stopped"])),
    /** The provider's count for a viewer's clip. */
    airedSeconds: Schema.NullOr(Schema.Finite),
  }),
  Schema.TaggedStruct("Dropped", {
    at: Schema.Finite,
    reason: Schema.Literals(["late", "withdrawn", "replaced", "displaced"]),
  }),
  Schema.TaggedStruct("Failed", {
    at: Schema.Finite,
    reason: Schema.Literals(["Clip", "Command", "Lost", "Moderated", "Closed"]),
  }),
  /** Acknowledged, its start never seen. */
  Schema.TaggedStruct("Unobserved", { at: Schema.Finite }),
  /** Sent, its acknowledgement never seen, and nothing can settle it any more. */
  Schema.TaggedStruct("Unknown", { at: Schema.Finite }),
]);
export type Outcome = typeof Outcome.Type;

/** One clip of the as-run log: what aired, or settled without airing, and how. */
export const AsRunEntry = Schema.Struct({
  ...Clip,
  /** When the session reported its start; null for a clip that settled without one. */
  startedAt: Schema.NullOr(Schema.Finite),
  /** Its length as the provider built it, when reported. */
  seconds: Schema.NullOr(Schema.Finite),
  session: Schema.NullOr(Schema.String),
  /** The first decoded frame the broadcast received after the reported start. */
  pictureAt: Schema.NullOr(Schema.Finite),
  /** Null while it is on air. */
  outcome: Schema.NullOr(Outcome),
});
export type AsRunEntry = typeof AsRunEntry.Type;

export const SessionStatus = Schema.Struct({
  sessionId: Schema.String,
  role: Schema.Literals(["on-air", "replacement", "retiring"]),
  /** When the playout reported it open; null if that was before the channel listened. */
  openedAt: Schema.NullOr(Schema.Finite),
  /** When its granted length runs out; null for an uncapped session, or one opened unseen. */
  endsAt: Schema.NullOr(Schema.Finite),
  /** When the playout opens its replacement: the renewal lead before `endsAt`. */
  renewsAt: Schema.NullOr(Schema.Finite),
  /** Its connection dropped and the session is reconnecting it. */
  reconnecting: Schema.Boolean,
  reconnects: Schema.Int,
});
export type SessionStatus = typeof SessionStatus.Type;

export const Switch = Schema.Struct({
  from: Schema.String,
  to: Schema.String,
  at: Schema.Finite,
  /** The retiring session never started a clip, or its last one left the air and the grace passed. */
  decision: Schema.Literals(["no-observed-start", "grace-elapsed"]),
});
export type Switch = typeof Switch.Type;

/**
 * The channel as the server knows it, read from the playout, the programme
 * and the broadcast. Times are epoch milliseconds on the server's clock. It
 * carries identities, phases and the channel's own prompts, never provider text.
 */
export class ChannelStatus extends Schema.Class<ChannelStatus>("ChannelStatus")({
  name: Schema.String,
  mode: Schema.Literals(["simulated", "live"]),
  /** When this status was read. */
  at: Schema.Finite,
  /** Why the channel went off air; null while it is on air. */
  offAir: Schema.NullOr(Schema.String),
  /** Browsers receiving the broadcast now. */
  viewers: Schema.Int,
  /**
   * The clip on air, as the session reported its start. `pictureAt` stays null
   * until the broadcast has received a decoded frame after that start.
   */
  playing: Schema.NullOr(
    Schema.Struct({
      ...Clip,
      startedAt: Schema.Finite,
      seconds: Schema.NullOr(Schema.Finite),
      pictureAt: Schema.NullOr(Schema.Finite),
    }),
  ),
  /**
   * What the playout projects to air after the clip on air, in the order it
   * airs, house clips included, then the viewers' items it projects to go
   * without airing. A projection from its forecast, not a promise: it changes
   * as builds are measured and prompts arrive.
   */
  upNext: Schema.Array(
    Schema.Struct({
      ...Clip,
      phase: Schema.Literals(["Accepted", "Building", "Ready"]),
      /** On the replacement, which takes the air at the switch. */
      afterSwitch: Schema.Boolean,
      /** When it is projected to start; null for an item projected not to air. */
      startsAt: Schema.NullOr(Schema.Finite),
    }),
  ),
  /** The latest clips aired or settled, newest first. */
  asRun: Schema.Array(AsRunEntry),
  sessions: Schema.Array(SessionStatus),
  /** Switches from a retiring session to its replacement, newest first. */
  switches: Schema.Array(Switch),
  /** Sessions lost before a planned switch, whose unaired clips were rebuilt on the next. */
  replaced: Schema.Int,
  /** Seconds of air secured: the playing clip's rest and the Ready clips after it. */
  runwaySeconds: Schema.Finite,
  /** Times nothing was left to play. */
  starved: Schema.Int,
  /** A prompt is taken only if it can start within this many seconds. */
  startWithinSeconds: Schema.Finite,
}) {}

export class OffAir extends Schema.TaggedError<OffAir>()(
  "OffAir",
  { message: Schema.String },
  { httpApiStatus: 503 },
) {}

export class ChannelApi extends HttpApiGroup.make("channel")
  .add(
    HttpApiEndpoint.post("submit", "/prompts", {
      payload: PromptRequest,
      success: Submitted,
      error: [ChannelBusy, ChannelUnavailable, PromptRejected],
    }),
    HttpApiEndpoint.get("status", "/status", { success: ChannelStatus }),
    // Server-Sent Events: the current status first, then a status at every change.
    HttpApiEndpoint.get("events", "/events", {
      success: HttpApiSchema.StreamSse({ data: ChannelStatus }),
    }),
  )
  .prefix("/api") {}

/** The broadcast itself: one long fragmented MP4 a `<video>` element plays as it arrives. */
export class MediaApi extends HttpApiGroup.make("media", { topLevel: true }).add(
  HttpApiEndpoint.get("live", "/live.mp4", {
    success: HttpApiSchema.StreamUint8Array({ contentType: "video/mp4" }),
    error: OffAir,
  }),
) {}

export class Api extends HttpApi.make("reactor-live-channel")
  .add(ChannelApi)
  .add(MediaApi)
  .annotateMerge(OpenApi.annotations({ title: "Reactor live channel" })) {}
