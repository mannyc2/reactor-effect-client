/**
 * Vidu S2-Avatar's messages as Reactor documents them, decoded once where they
 * come in. A field the model leaves out reads as it documents one that does
 * not apply: `null`, or `false` for a flag.
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const Phase = Schema.Literals([
  "idle",
  "preparing_avatar",
  "avatar_ready",
  "starting",
  "warming_up",
  "live",
  "ending",
  "ended",
  "failed",
]);
export type Phase = typeof Phase.Type;

const flag = Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false)));
const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
/** A code the model documents, or one the generation service passes through: never free text. */
const Code = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_.:-]{1,64}$/));
/** Fields that do not apply: absent or `null`, read as `null`. */
const unset = Effect.succeed(null);
const text = Schema.NullOr(Schema.String).pipe(Schema.withDecodingDefaultKey(unset));
const code = Schema.NullOr(Code).pipe(Schema.withDecodingDefaultKey(unset));
const number = Schema.NullOr(Schema.Finite).pipe(Schema.withDecodingDefaultKey(unset));
const integer = Schema.NullOr(Schema.Int).pipe(Schema.withDecodingDefaultKey(unset));

/** A refused command or a failed call. Its `reason` is provider text and stays redacted. */
export const CommandError = Schema.Struct({
  /** The command that failed, or `session` when nothing a client sent caused it. */
  command: Schema.String,
  /** `request`, `state`, `upstream` or `platform`, as documented. */
  origin: Schema.String,
  /** Stable: branch on it. */
  code: Code,
  reason: Schema.RedactedFromValue(Schema.String),
  retryable: flag,
  upstream_status: integer,
  upstream_code: code,
  trace_id: text,
});
export type CommandError = typeof CommandError.Type;

/** The authoritative snapshot: Reactor's documentation says to drive a UI from it alone. */
export const State = Schema.Struct({
  phase: Phase,
  avatar_id: text,
  avatar_status: text,
  avatar_name: text,
  /** The voice in effect for the current or next call. */
  voice: text,
  persona_set: flag,
  call_mode: text,
  warmup_attempts: Count.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  /** Unix seconds at which the call became live. */
  call_started_at: number,
  /** The longest the call may run before it ends on its own. */
  call_max_seconds: number,
  call_elapsed_seconds: number,
  /** Commands such as `say` and `interrupt` are accepted. */
  control_ready: flag,
  video_receiving: flag,
  audio_receiving: flag,
  mic_forwarding: flag,
  camera_forwarding: flag,
  last_frame_age_ms: number,
  reference_images: Schema.Struct({ image_id: Schema.String, kind: text }).pipe(
    Schema.Array,
    Schema.NullOr,
    Schema.withDecodingDefaultKey(unset),
  ),
  /**
   * Why the last call ended: `ended_by_client`, `max_duration`,
   * `idle_timeout`, `content_policy`, `upstream_quota`,
   * `upstream_interrupted`, `media_lost` or `bridge_failed`, as documented.
   */
  end_reason: text,
  /** The most recent `command_error`, cleared when a call starts. */
  last_error: Schema.NullOr(CommandError).pipe(Schema.withDecodingDefaultKey(unset)),
});
export type State = typeof State.Type;

/** A sentence the caller or the character finished, while the call keeps transcripts. */
export const Transcript = Schema.Struct({
  speaker: Schema.Literals(["user", "character"]),
  text: Schema.String,
  /** True once the text is settled. */
  final: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(true))),
});
export type Transcript = typeof Transcript.Type;

export const Voice = Schema.Struct({
  /** What `startCall` and `updateCall` take. */
  voice: Schema.String,
  description: text,
  accent: text,
});
export type Voice = typeof Voice.Type;

export const Voices = Schema.Struct({
  system: Schema.Array(Voice),
  /** The account's own voices, where the deployment lists them. */
  cloned: Schema.Array(Voice).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
  default_voice: text,
});
export type Voices = typeof Voices.Type;

export const CallEnded = Schema.Struct({
  end_reason: Schema.String,
  /** How long the call was live; 0 when it never was. */
  duration_seconds: Schema.Finite,
});
export type CallEnded = typeof CallEnded.Type;

export const CallUpdated = Schema.Struct({ applied: Schema.Array(Schema.String) });
export const ReferenceImagesApplied = Schema.Struct({ image_ids: Schema.Array(Schema.String) });

/** Every message type the model documents. */
export const messageTypes: ReadonlySet<string> = new Set([
  "session_state",
  "command_error",
  "transcript",
  "voices",
  "call_updated",
  "reference_images_applied",
  "call_ended",
]);
