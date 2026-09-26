import { Schema } from "effect";
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
  OpenApi,
} from "effect/unstable/httpapi";

/**
 * The channel's HTTP contract, kept apart from the server so a client (the
 * test, or another service) can derive a typed client from it without the
 * server's dependencies.
 */

export const PromptRequest = Schema.Struct({
  prompt: Schema.Trimmed.check(Schema.isLengthBetween(1, 2000)),
});

/**
 * Either the provider accepted the prompt as a clip, or the enqueue was sent
 * and its outcome is unknown: it may still play, and the event stream will
 * show it if it does. Neither is retried by the server.
 */
export const Submitted = Schema.Union([
  Schema.TaggedStruct("Accepted", { clipId: Schema.String }),
  Schema.TaggedStruct("Unconfirmed", {}),
]).pipe(HttpApiSchema.status(202));
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

export const ClipSummary = Schema.Struct({
  clipId: Schema.String,
  prompt: Schema.NullOr(Schema.String),
});
export type ClipSummary = typeof ClipSummary.Type;

export class ChannelStatus extends Schema.Class<ChannelStatus>("ChannelStatus")({
  mode: Schema.Literals(["simulated", "live"]),
  session: Schema.NullOr(Schema.String),
  media: Schema.Literals(["Ready", "Recovering", "Failed", "Closed"]),
  playing: Schema.NullOr(ClipSummary),
  upcoming: Schema.Array(ClipSummary),
  /** The media output's loss so far, or null when a total could not be read. */
  loss: Schema.NullOr(
    Schema.Struct({
      droppedVideo: Schema.Int,
      droppedAudio: Schema.Int,
      readerOverflows: Schema.Int,
    }),
  ),
}) {}

/**
 * What `GET /api/events` sends, one Server-Sent Event per change, in the order
 * the orchestration observed them. It carries identities and phases only:
 * never provider text, which the SDK keeps out of everything it reports.
 */
export const ChannelEvent = Schema.Union([
  Schema.TaggedStruct("Clip", {
    clipId: Schema.String,
    phase: Schema.Literals(["Queued", "Building", "Ready", "Started", "Ended", "Failed"]),
    prompt: Schema.NullOr(Schema.String),
  }),
  Schema.TaggedStruct("Starved", {}),
  Schema.TaggedStruct("Renewal", {
    phase: Schema.Literals([
      "Opened",
      "Prepared",
      "HandoffReady",
      "SetupFailed",
      "Recovering",
      "Reconnected",
      "Switched",
      "Replaced",
      "Failed",
    ]),
    session: Schema.NullOr(Schema.String),
    lostClips: Schema.NullOr(Schema.Int),
  }),
  Schema.TaggedStruct("Media", {
    state: Schema.Literals(["Ready", "Recovering", "Failed", "Closed"]),
    session: Schema.NullOr(Schema.String),
  }),
  Schema.TaggedStruct("OffAir", { reason: Schema.String }),
]);
export type ChannelEvent = typeof ChannelEvent.Type;

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
    // Server-Sent Events: the current state first, then every change.
    HttpApiEndpoint.get("events", "/events", {
      success: HttpApiSchema.StreamSse({ data: ChannelEvent }),
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
