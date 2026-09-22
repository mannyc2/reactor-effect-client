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

export const Queue = Schema.Struct({
  generation: Schema.Array(Clip),
  playout: Schema.Array(Clip),
  history: Schema.Array(Clip),
});
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
});
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

/** Unknown events remain observable. A known message never accepts a partial payload. */
export const decodeMessage = (type: string, input: unknown): DecodedMessage => {
  if (!Object.hasOwn(Payloads, type))
    return Object.freeze({
      type: "unknown",
      name: type,
      data: input === undefined ? undefined : jsonObject(input),
    });
  try {
    const schema = Payloads[type as MessageType];
    const data: unknown = Schema.decodeUnknownSync(schema as Schema.ConstraintDecoder<unknown>)(
      input,
    );
    const message = { type, data } as Message;
    if (message.type === "state_update") {
      const state = message.data;
      if (
        state.clip_seconds_min <= 0 ||
        state.clip_seconds_max < state.clip_seconds_min ||
        state.clip_seconds <= 0 ||
        state.playing !== (state.playing_clip_id !== null)
      )
        throw new Error("inconsistent state");
    }
    if (message.type === "queue_update") {
      const queued = [...message.data.generation, ...message.data.playout].map(
        (clip) => clip.clip_id,
      );
      const history = message.data.history.map((clip) => clip.clip_id);
      // A retained history entry may describe a clip that is still in playout;
      // duplicate positions within queues/history remain ambiguous.
      if (new Set(queued).size !== queued.length || new Set(history).size !== history.length)
        throw new Error("duplicate queue identity");
    }
    return freeze(message);
  } catch {
    throw new ReactorError("Protocol", `H3 ${type} payload is malformed`, {
      operation: "h3 observation",
    });
  }
};
