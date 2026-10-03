import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ReactorError } from "../../ReactorError.js";

const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const Id = Schema.String.check(Schema.isUUID());
/** Required fields follow the H3 Reference Turbo Realtime 0.5.5 documentation. */
export const Clip = Schema.Struct({
  clip_id: Id,
  prompt: Schema.String,
  metadata: Schema.String,
  frames: Positive,
  seconds: Schema.Finite.check(Schema.isGreaterThan(0)),
  seed: Schema.Natural,
  ready: Schema.Boolean,
  has_reference_image: Schema.optionalKey(Schema.Boolean),
  reference_image_count: Schema.optionalKey(Schema.Natural),
  has_reference_audio: Schema.optionalKey(Schema.Boolean),
  reference_audio_count: Schema.optionalKey(Schema.Natural),
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
const queueFor = <S extends Schema.Codec<Clip, unknown>>(clip: S) =>
  Schema.Struct({
    generation: Schema.Array(clip),
    playout: Schema.Array(clip),
    history: Schema.Array(clip),
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
export const Queue = queueFor(Clip);
export interface Queue<C extends Clip = Clip> {
  readonly generation: ReadonlyArray<C>;
  readonly playout: ReadonlyArray<C>;
  readonly history: ReadonlyArray<C>;
}

export const State = Schema.Struct({
  clip_seconds: Schema.Finite,
  clip_seconds_min: Schema.Finite,
  clip_seconds_max: Schema.Finite,
  seed: Schema.Natural,
  autoplay: Schema.Boolean,
  flush_on_clip_end: Schema.Boolean,
  aspect: Schema.String,
  width: Positive,
  height: Positive,
  playing: Schema.Boolean,
  playing_clip_id: Schema.NullOr(Id),
  generation_queued: Schema.Natural,
  generation_capacity: Positive,
  playout_queued: Schema.Natural,
  playout_capacity: Positive,
  clips_played: Schema.Natural,
  seconds_sent: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  valid_commands: Schema.Array(Schema.String),
}).check(
  Schema.makeFilter((state) => {
    const issues: Schema.FilterIssue[] = [];
    if (state.clip_seconds_min <= 0)
      issues.push({ path: ["clip_seconds_min"], issue: "must be positive" });
    if (state.clip_seconds_max < state.clip_seconds_min)
      issues.push({ path: ["clip_seconds_max"], issue: "must be at least clip_seconds_min" });
    if (state.clip_seconds <= 0) issues.push({ path: ["clip_seconds"], issue: "must be positive" });
    // `playing` also covers a clip armed for its seam, and H3 does not say whether
    // `playing_clip_id` names that clip yet, so the two are not held to agree.
    return issues;
  }),
);
export type State = typeof State.Type;

export const payloadsFor = <S extends Schema.Codec<Clip, unknown>>(clip: S) => ({
  clip_queued: Schema.Struct({ clip }),
  clip_moved: Schema.Struct({
    clip,
    queue: Schema.Literals(["generation", "playout"]),
    position: Schema.Natural,
  }),
  clip_popped: Schema.Struct({ clip }),
  clip_generated: Schema.Struct({ clip }),
  clip_failed: Schema.Struct({ clip, reason: Schema.String }),
  clip_started: Schema.Struct({ clip }),
  clip_finished: Schema.Struct({ clip, seconds_sent: Schema.Finite }),
  clip_stopped: Schema.Struct({ clip, seconds_sent: Schema.Finite }),
  queue_update: queueFor(clip),
  state_update: State,
  command_error: Schema.Struct({ command: Schema.String, reason: Schema.String }),
  seed_accepted: Schema.Struct({ seed: Schema.Natural }),
  clip_length_accepted: Schema.Struct({ clip_seconds: Schema.Finite, frames: Positive }),
  canvas_accepted: Schema.Struct({ aspect: Schema.String, width: Positive, height: Positive }),
  autoplay_accepted: Schema.Struct({ enabled: Schema.Boolean }),
  flush_accepted: Schema.Struct({ enabled: Schema.Boolean }),
  session_reset: Schema.Struct({ cleared_clips: Schema.Natural, was_playing: Schema.Boolean }),
});
export const Payloads = payloadsFor(Clip);
export type PayloadTable<C extends Clip = Clip> = ReturnType<
  typeof payloadsFor<Schema.Codec<C, unknown>>
>;
export type MessageType = keyof typeof Payloads;
export type Payload<K extends MessageType, C extends Clip = Clip> = PayloadTable<C>[K]["Type"];
export type Message<K extends MessageType = MessageType, C extends Clip = Clip> = {
  [P in K]: { readonly type: P; readonly data: Payload<P, C> };
}[K];
export type DecodedMessage<C extends Clip = Clip> =
  | Message<MessageType, C>
  | {
      readonly type: "unknown";
      readonly name: string;
      readonly data: Schema.JsonObject | undefined;
    };

/**
 * Unknown events stay observable. A known message never accepts a partial
 * payload: its Schema, cross-field rules included, rejects it as `Protocol`,
 * and the `SchemaError`, which names the path but no input value, stays in
 * the error's detail.
 */
export const decodeMessageFor =
  <C extends Clip>(payloads: PayloadTable<C>) =>
  ({
    type,
    data,
  }: {
    readonly type: string;
    readonly data?: Schema.JsonObject | undefined;
  }): Result.Result<DecodedMessage<C>, ReactorError> => {
    if (!isMessageType(type)) return Result.succeed({ type: "unknown", name: type, data });
    return decodeKnown(payloads, type, data);
  };
export const decodeMessage = decodeMessageFor(Payloads);

const decodeKnown = <C extends Clip, K extends MessageType>(
  payloads: PayloadTable<C>,
  type: K,
  data: Schema.JsonObject | undefined,
): Result.Result<Message<K, C>, ReactorError> =>
  Result.mapBoth(Schema.decodeUnknownResult(payloads[type])(data), {
    onFailure: (cause) =>
      ReactorError.fromCode("Protocol", `H3 ${type} payload is malformed`, {
        operation: "H3 observation",
        detail: cause,
      }),
    onSuccess: (decoded): Message<K, C> => ({ type, data: decoded }),
  });

const isMessageType = (type: string): type is MessageType => Object.hasOwn(Payloads, type);
