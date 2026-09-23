import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ReactorError } from "../errors.js";
import { jsonObject } from "../json.js";
import type { JsonObject } from "../json.js";

const NumberValue = Schema.Number.check(Schema.isFinite());
const Integer = NumberValue.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const Positive = Integer.check(Schema.isGreaterThan(0));
const Id = Schema.String.check(Schema.isUUID());

/** Required fields follow the H3 Reference Turbo Realtime 0.5.5 documentation. */
export const Clip = Schema.Struct({
  clip_id: Id,
  prompt: Schema.String,
  metadata: Schema.String,
  frames: Positive,
  seconds: NumberValue.check(Schema.isGreaterThan(0)),
  seed: Integer,
  ready: Schema.Boolean,
  has_reference_image: Schema.optionalKey(Schema.Boolean),
  reference_image_count: Schema.optionalKey(Integer),
  has_reference_audio: Schema.optionalKey(Schema.Boolean),
  reference_audio_count: Schema.optionalKey(Integer),
});
export type Clip = typeof Clip.Type;

/** The first repeated clip id in `clips`, as its index. */
const repeated = (clips: readonly Clip[], seen = new Set<string>()): number => {
  for (const [index, clip] of clips.entries()) {
    if (seen.has(clip.clip_id)) return index;
    seen.add(clip.clip_id);
  }
  return -1;
};

/**
 * A clip is in at most one queue position. A retained history entry may
 * describe a clip that is still in playout, but duplicate positions within the
 * queues, or within the history, are ambiguous.
 */
export const Queue = Schema.Struct({
  generation: Schema.Array(Clip),
  playout: Schema.Array(Clip),
  history: Schema.Array(Clip),
}).check(
  Schema.makeFilter((queue) => {
    const queued = new Set<string>();
    const generation = repeated(queue.generation, queued);
    if (generation >= 0)
      return { path: ["generation", generation, "clip_id"], issue: "clip is queued twice" };
    const playout = repeated(queue.playout, queued);
    if (playout >= 0)
      return { path: ["playout", playout, "clip_id"], issue: "clip is queued twice" };
    const history = repeated(queue.history);
    if (history >= 0)
      return { path: ["history", history, "clip_id"], issue: "clip is in history twice" };
    return undefined;
  }),
);
export type Queue = typeof Queue.Type;

export const State = Schema.Struct({
  clip_seconds: NumberValue,
  clip_seconds_min: NumberValue,
  clip_seconds_max: NumberValue,
  seed: Integer,
  autoplay: Schema.Boolean,
  flush_on_clip_end: Schema.Boolean,
  aspect: Schema.String,
  width: Positive,
  height: Positive,
  playing: Schema.Boolean,
  playing_clip_id: Schema.NullOr(Id),
  generation_queued: Integer,
  generation_capacity: Positive,
  playout_queued: Integer,
  playout_capacity: Positive,
  clips_played: Integer,
  seconds_sent: NumberValue.check(Schema.isGreaterThanOrEqualTo(0)),
  valid_commands: Schema.Array(Schema.String),
}).check(
  Schema.makeFilter((state) => {
    const issues: Schema.FilterIssue[] = [];
    if (state.clip_seconds_min <= 0)
      issues.push({ path: ["clip_seconds_min"], issue: "must be positive" });
    if (state.clip_seconds_max < state.clip_seconds_min)
      issues.push({ path: ["clip_seconds_max"], issue: "must be at least clip_seconds_min" });
    if (state.clip_seconds <= 0) issues.push({ path: ["clip_seconds"], issue: "must be positive" });
    if (state.playing !== (state.playing_clip_id !== null))
      issues.push({ path: ["playing_clip_id"], issue: "must be set exactly while playing" });
    return issues;
  }),
);
export type State = typeof State.Type;

export const Payloads = {
  clip_queued: Schema.Struct({ clip: Clip }),
  clip_moved: Schema.Struct({
    clip: Clip,
    queue: Schema.Literals(["generation", "playout"]),
    position: Integer,
  }),
  clip_popped: Schema.Struct({ clip: Clip }),
  clip_generated: Schema.Struct({ clip: Clip }),
  clip_failed: Schema.Struct({ clip: Clip, reason: Schema.String }),
  clip_started: Schema.Struct({ clip: Clip }),
  clip_finished: Schema.Struct({ clip: Clip, seconds_sent: NumberValue }),
  clip_stopped: Schema.Struct({ clip: Clip, seconds_sent: NumberValue }),
  queue_update: Queue,
  state_update: State,
  command_error: Schema.Struct({ command: Schema.String, reason: Schema.String }),
  seed_accepted: Schema.Struct({ seed: Integer }),
  clip_length_accepted: Schema.Struct({ clip_seconds: NumberValue, frames: Positive }),
  canvas_accepted: Schema.Struct({ aspect: Schema.String, width: Positive, height: Positive }),
  autoplay_accepted: Schema.Struct({ enabled: Schema.Boolean }),
  flush_accepted: Schema.Struct({ enabled: Schema.Boolean }),
  session_reset: Schema.Struct({ cleared_clips: Integer, was_playing: Schema.Boolean }),
} as const;
export type MessageType = keyof typeof Payloads;
export type Payload<K extends MessageType> = (typeof Payloads)[K]["Type"];
export type Message = {
  [K in MessageType]: { readonly type: K; readonly data: Payload<K> };
}[MessageType];
export type DecodedMessage =
  | Message
  | { readonly type: "unknown"; readonly name: string; readonly data: JsonObject | undefined };

const freeze = <A>(value: A): A => {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};

/** The `SchemaError`, which names the path but no input value, stays in `detail`. */
const malformed = (type: string, cause: Schema.SchemaError): ReactorError =>
  ReactorError.fromCode("Protocol", `H3 ${type} payload is malformed`, {
    operation: "h3 observation",
    detail: cause,
  });

/**
 * Unknown events remain observable. A known message never accepts a partial
 * payload: its Schema, cross-field rules included, rejects it as Protocol, and
 * a bug in these checks stays a defect.
 */
export const decodeMessage = (type: string, input: unknown): DecodedMessage => {
  if (!Object.hasOwn(Payloads, type))
    return Object.freeze({
      type: "unknown",
      name: type,
      data: input === undefined ? undefined : jsonObject(input),
    });
  const decoded = Schema.decodeUnknownResult(Payloads[type as MessageType])(input);
  if (Result.isFailure(decoded)) throw malformed(type, decoded.failure);
  return freeze({ type, data: decoded.success } as Message);
};
