/**
 * The simulated Vidu S2-Avatar model as a pure state machine: a command, a
 * finished timer or a new connection goes in; the next state comes out with
 * the messages to send and the timers, picture and speech to start.
 *
 * It states Vidu S2-Avatar as Reactor documents it and takes no rule from the
 * client. Where the documentation leaves something open (the voice catalog,
 * what the character answers, which check a refusal meets first), the choice
 * is this simulation's and says so.
 */
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

/** Vidu S2-Avatar's documented facts. */
export const documented = {
  modelName: "reactor/vidu-s2-avatar",
  version: "0.4.2",
  /** Reactor's pricing endpoint stated 70 credits a second on October 1, 2026. */
  creditsPerSecond: 70,
  fps: 25,
  tracks: { mic: "mic", webcam: "webcam", video: "main_video", audio: "main_audio" },
  soundtrack: { sampleRate: 48_000, channels: 1 },
  avatarName: 100,
  greeting: 200,
  language: 40,
  persona: 50_000,
  say: 2_000,
  references: 3,
  imageId: 128,
  referenceText: 200,
  /** This simulation's catalog: Reactor documents the reply's shape, not its voices. */
  voices: [
    { voice: "Ethan", description: "A calm adult male voice", accent: "American" },
    { voice: "Tina", description: "A warm adult female voice", accent: "British" },
  ],
  defaultVoice: "Tina",
} as const;

export type Phase =
  | "idle"
  | "preparing_avatar"
  | "avatar_ready"
  | "starting"
  | "warming_up"
  | "live"
  | "ending"
  | "ended"
  | "failed";

/** A call is active from `starting` until it has ended. */
const active = (phase: Phase): boolean =>
  phase === "starting" || phase === "warming_up" || phase === "live" || phase === "ending";

export interface Avatar {
  readonly id: string;
  readonly name: string | null;
}

/** The `command_error` payload, also kept as the snapshot's `last_error`. */
export interface CommandError {
  readonly command: string;
  readonly origin: "request" | "state" | "upstream" | "platform";
  readonly code: string;
  readonly reason: string;
  readonly retryable: boolean;
  readonly upstream_status: number | null;
  readonly upstream_code: string | null;
  readonly trace_id: string | null;
}

/** The documented `session_state` snapshot, every field present. */
export interface Snapshot {
  readonly phase: Phase;
  readonly avatar_id: string | null;
  readonly avatar_status: "processing" | "ready" | "failed" | null;
  readonly avatar_name: string | null;
  readonly voice: string | null;
  readonly persona_set: boolean;
  readonly call_mode: "audio" | "video" | null;
  readonly warmup_attempts: number;
  readonly call_started_at: number | null;
  readonly call_max_seconds: number | null;
  readonly call_elapsed_seconds: number | null;
  readonly control_ready: boolean;
  readonly video_receiving: boolean;
  readonly audio_receiving: boolean;
  readonly mic_forwarding: boolean;
  readonly camera_forwarding: boolean;
  readonly last_frame_age_ms: number | null;
  readonly reference_images: ReadonlyArray<{
    readonly image_id: string;
    readonly kind: string;
  }> | null;
  readonly end_reason: string | null;
  readonly last_error: CommandError | null;
}

/** Each message Vidu S2-Avatar documents, and its payload. */
export type Message =
  | { readonly type: "session_state"; readonly data: Snapshot }
  | { readonly type: "command_error"; readonly data: CommandError }
  | {
      readonly type: "transcript";
      readonly data: {
        readonly speaker: "user" | "character";
        readonly text: string;
        readonly final: boolean;
      };
    }
  | {
      readonly type: "voices";
      readonly data: {
        readonly system: ReadonlyArray<(typeof documented.voices)[number]>;
        readonly cloned: ReadonlyArray<(typeof documented.voices)[number]>;
        readonly default_voice: string;
      };
    }
  | { readonly type: "call_updated"; readonly data: { readonly applied: ReadonlyArray<string> } }
  | {
      readonly type: "reference_images_applied";
      readonly data: { readonly image_ids: ReadonlyArray<string> };
    }
  | {
      readonly type: "call_ended";
      readonly data: { readonly end_reason: string; readonly duration_seconds: number };
    };

const Upload = Schema.Struct({
  upload_id: Schema.String,
  name: Schema.String,
  mime_type: Schema.String,
  size: Schema.Int,
});
const nullable = <S extends Schema.Top>(schema: S) =>
  schema.pipe(Schema.NullOr, Schema.optionalKey);
const Text = (most: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(most));
const Settings = Schema.Record(Schema.String, Schema.Json);
const Empty = Schema.Struct({});

/**
 * Each command's parameters as the deployment declares them. The runtime
 * refuses a payload that breaks them with an error frame before the model
 * sees it; what the model then refuses arrives as `command_error`.
 */
export const Arguments = {
  create_avatar: Schema.Struct({
    name: nullable(Schema.String),
    image: nullable(Upload),
    image_url: nullable(Schema.String),
  }),
  attach_avatar: Schema.Struct({ avatar_id: Text(128) }),
  list_voices: Empty,
  // Hosted Vidu acknowledged a name outside the package's documented pattern, so the
  // deployment's parameters don't constrain it.
  clone_voice: Schema.Struct({
    name: Schema.String,
    audio_url: Schema.String,
    language: nullable(Schema.String),
  }),
  start_call: Schema.Struct({
    persona: Text(documented.persona),
    voice: nullable(Schema.String),
    greeting: nullable(Schema.String),
    language: nullable(Schema.String),
    call_mode: nullable(Schema.String),
    transcripts: nullable(Schema.Boolean),
    persona_enhance: Schema.optionalKey(Schema.Boolean),
    vad: nullable(Settings),
    llm: nullable(Settings),
  }),
  say: Schema.Struct({ text: Text(documented.say) }),
  interrupt: Empty,
  update_call: Schema.Struct({
    voice: nullable(Schema.String),
    persona: nullable(Schema.String),
    vad: nullable(Settings),
    llm: nullable(Settings),
  }),
  set_reference_images: Schema.Struct({ images: Schema.Array(Schema.Json) }),
  clear_reference_images: Schema.Struct({ image_ids: Schema.Json.pipe(Schema.Array, nullable) }),
  end_call: Empty,
  get_state: Empty,
} as const;
type Command = keyof typeof Arguments;
const isCommand = (name: string): name is Command => Object.hasOwn(Arguments, name);

/** Turn-taking settings' documented ranges: the model refuses a value outside them. */
const Vad = Schema.Struct({
  type: Schema.optionalKey(Schema.Literals(["server", "semantic"])),
  threshold: Schema.optionalKey(Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }))),
  silence_duration_ms: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 200, maximum: 6000 })),
  ),
});
/** Reply settings' documented ranges. */
const Llm = Schema.Struct({
  temperature: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2, exclusiveMaximum: true })),
  ),
  top_p: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1, exclusiveMinimum: true })),
  ),
  top_k: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100 }))),
  max_tokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  frequency_penalty: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
  presence_penalty: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2 })),
  ),
  seed: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1))),
});

/** Why the model refuses a `vad` or `llm` setting, by its documented ranges. */
const settingsRefusal = (vad: unknown, llm: unknown): string | undefined =>
  vad != null && Result.isFailure(Schema.decodeResult(Vad)(vad))
    ? "vad is outside its documented ranges"
    : llm != null && Result.isFailure(Schema.decodeResult(Llm)(llm))
      ? "llm is outside its documented ranges"
      : undefined;

const isVoice = (voice: string): boolean =>
  documented.voices.some((entry) => entry.voice === voice);

/**
 * One `set_reference_images` entry. A `kind` left out is inferred from the
 * image; this simulation infers `object`.
 */
const Reference = Schema.Struct({
  image_url: Schema.NonEmptyString,
  image_id: Text(documented.imageId),
  kind: Schema.optionalKey(Schema.Literals(["object", "garment", "background"])),
  text: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(documented.referenceText))),
});
const References = Schema.Array(Reference).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(documented.references),
);
const ImageIds = Schema.Array(Schema.String).check(Schema.isMaxLength(documented.references));

interface Call {
  readonly token: number;
  /** Calls started in the session, from 1: it numbers the call's picture. */
  readonly ordinal: number;
  readonly mode: "audio" | "video";
  readonly voice: string;
  readonly transcripts: boolean;
  readonly greeting: string | undefined;
  /** Monotonic and Unix milliseconds at which it became live. */
  readonly live: { readonly at: number; readonly unix: number } | undefined;
  /** What the caller said that the character has not answered yet. */
  readonly heard: ReadonlyArray<string>;
  /** The answer being thought of or spoken, by its token. */
  readonly answering:
    | {
        readonly token: number;
        readonly turn: number;
        readonly speaking: boolean;
        readonly text: string;
      }
    | undefined;
  readonly references: ReadonlyArray<{ readonly image_id: string; readonly kind: string }>;
  /** The ids the most recent `set_reference_images` applied, which a bare clear undoes. */
  readonly lastSet: ReadonlyArray<string>;
}

export interface Vidu {
  readonly phase: Phase;
  readonly avatar: Avatar | undefined;
  /** The avatar being created and the token of its timer. */
  readonly preparing: { readonly token: number; readonly avatar: Avatar } | undefined;
  readonly call: Call | undefined;
  /** The voice the current or next call uses. */
  readonly voice: string | null;
  readonly warmupAttempts: number;
  readonly endReason: string | null;
  readonly lastError: CommandError | null;
  readonly calls: number;
  /** Answers given in the session, which number each answer's sound. */
  readonly turns: number;
  readonly errors: number;
  /** Timer tokens issued, so a stale timer is ignored. */
  readonly tokens: number;
}

export const initial: Vidu = {
  phase: "idle",
  avatar: undefined,
  preparing: undefined,
  call: undefined,
  voice: null,
  warmupAttempts: 0,
  endReason: null,
  lastError: null,
  calls: 0,
  turns: 0,
  errors: 0,
  tokens: 0,
};

export type Input =
  | { readonly _tag: "Connected" }
  | {
      readonly _tag: "Command";
      readonly requestId: string;
      readonly name: string;
      readonly args: Schema.JsonObject;
      /** For `attach_avatar`: the saved avatar its id names, if Reactor keeps one. */
      readonly saved?: Avatar | undefined;
      /** For `create_avatar`: the id Reactor gives the avatar it makes. */
      readonly newAvatarId?: string;
    }
  | { readonly _tag: "AvatarReady"; readonly token: number }
  | { readonly _tag: "WarmingUp"; readonly token: number }
  | { readonly _tag: "Live"; readonly token: number }
  /** The character has thought of its answer and starts to speak it. */
  | { readonly _tag: "Answer"; readonly token: number }
  /** The character finished speaking its answer. */
  | { readonly _tag: "Spoken"; readonly token: number }
  | { readonly _tag: "Released"; readonly token: number; readonly requestId: string };

export type Output =
  | { readonly _tag: "Reply"; readonly requestId: string; readonly message: Message }
  | { readonly _tag: "Ack"; readonly requestId: string }
  /** The runtime's error frame for a payload the deployment refuses before the model runs. */
  | {
      readonly _tag: "Reject";
      readonly requestId: string;
      readonly code: string;
      readonly message: string;
    }
  | { readonly _tag: "Broadcast"; readonly message: Message }
  /** The avatar is prepared after the avatar delay. */
  | { readonly _tag: "Prepare"; readonly token: number }
  /** The call warms up halfway through the call delay and is live at its end. */
  | { readonly _tag: "Warm"; readonly token: number }
  /** The character answers after the answer delay. */
  | { readonly _tag: "Think"; readonly token: number }
  /** The character's answer, `turn`, sounds for the speech delay and is then `Spoken`. */
  | { readonly _tag: "Speak"; readonly token: number; readonly turn: number }
  /** The answer stops sounding: interrupted or the call ending. */
  | { readonly _tag: "Hush"; readonly token: number }
  /** The call is released after the hangup delay. */
  | { readonly _tag: "Hangup"; readonly token: number; readonly requestId: string }
  /** The character's picture runs from `at`, numbered by the call. */
  | { readonly _tag: "Picture"; readonly token: number; readonly call: number; readonly at: number }
  | { readonly _tag: "Dark"; readonly token: number }
  /** Reactor keeps the avatar for later sessions. */
  | { readonly _tag: "Save"; readonly avatar: Avatar };

export interface Env {
  /** Monotonic milliseconds. */
  readonly now: number;
  /** Unix milliseconds. */
  readonly unix: number;
}

type Step = readonly [Vidu, ReadonlyArray<Output>];

/**
 * What the character answers, which the simulation makes up: the documented
 * contract carries the text, not what a persona would say.
 */
const answerText = (turn: number, heard: string | undefined): string =>
  heard === undefined
    ? `Greeting ${turn}.`
    : `Answer ${turn} to: ${heard.length > 60 ? `${heard.slice(0, 60)}...` : heard}`;

export const step = ({
  model,
  input,
  env,
}: {
  readonly model: Vidu;
  readonly input: Input;
  readonly env: Env;
}): Step => {
  let s = model;
  const out: Output[] = [];
  const set = (patch: Partial<Vidu>): void => {
    s = { ...s, ...patch };
  };
  const setCall = (patch: Partial<Call>): void => {
    if (s.call !== undefined) set({ call: { ...s.call, ...patch } });
  };
  const token = (): number => {
    set({ tokens: s.tokens + 1 });
    return s.tokens;
  };
  const emit = (output: Output): void => {
    out.push(output);
  };
  const broadcast = (message: Message) => emit({ _tag: "Broadcast", message });
  const snapshot = (): Snapshot => {
    const live = s.phase === "live" ? s.call?.live : undefined;
    const references = s.call?.references ?? [];
    return {
      phase: s.phase,
      avatar_id: s.preparing === undefined ? (s.avatar?.id ?? null) : null,
      avatar_status:
        s.preparing !== undefined ? "processing" : s.avatar === undefined ? null : "ready",
      avatar_name: (s.preparing?.avatar ?? s.avatar)?.name ?? null,
      voice: s.call?.voice ?? s.voice,
      persona_set: s.call !== undefined,
      call_mode: s.call?.mode ?? null,
      warmup_attempts: s.warmupAttempts,
      call_started_at: s.call?.live === undefined ? null : Math.floor(s.call.live.unix / 1000),
      // Reactor documents the limit as the snapshot's to state; this simulation sets none.
      call_max_seconds: null,
      call_elapsed_seconds: live === undefined ? null : Math.floor((env.now - live.at) / 1000),
      control_ready: s.phase === "live",
      video_receiving: s.phase === "live",
      audio_receiving: s.phase === "live",
      // ReactorTest's peers send no media, so neither track reaches the character.
      mic_forwarding: false,
      camera_forwarding: false,
      last_frame_age_ms: live === undefined ? null : 0,
      reference_images: references.length === 0 ? null : references,
      end_reason: s.endReason,
      last_error: s.lastError,
    };
  };
  const changed = (): void => broadcast({ type: "session_state", data: snapshot() });
  /**
   * The model refuses: `command_error` goes to every client, the snapshot
   * keeps it as `last_error`, and the command is acknowledged with nothing
   * done, as hosted Vidu answered its refusals: the `command_error` first.
   */
  const refuse = (
    requestId: string,
    command: string,
    origin: CommandError["origin"],
    code: string,
    reason: string,
    retryable = false,
  ): void => {
    set({ errors: s.errors + 1 });
    const error: CommandError = {
      command,
      origin,
      code,
      reason,
      retryable,
      upstream_status: null,
      upstream_code: null,
      trace_id: `trace_reactor_test_${s.errors}`,
    };
    set({ lastError: error });
    broadcast({ type: "command_error", data: error });
    changed();
    emit({ _tag: "Ack", requestId });
  };
  const ack = (requestId: string) => emit({ _tag: "Ack", requestId });
  const reply = (requestId: string, message: Message) =>
    emit({ _tag: "Reply", requestId, message });

  /** The next answer the character owes, thought of after the answer delay. */
  const think = (): void => {
    const call = s.call;
    if (call === undefined || call.answering !== undefined || s.phase !== "live") return;
    const [heard, ...rest] = call.heard;
    if (heard === undefined) return;
    const t = token();
    set({ turns: s.turns + 1 });
    setCall({
      heard: rest,
      answering: { token: t, turn: s.turns, speaking: false, text: answerText(s.turns, heard) },
    });
    emit({ _tag: "Think", token: t });
  };
  const hush = (): void => {
    const answering = s.call?.answering;
    if (answering?.speaking === true) emit({ _tag: "Hush", token: answering.token });
    setCall({ answering: undefined });
  };

  const createAvatar = (id: string, args: Schema.JsonObject, avatarId: string): void => {
    const decoded = Schema.decodeResult(Arguments.create_avatar)(args);
    if (Result.isFailure(decoded))
      return emit({
        _tag: "Reject",
        requestId: id,
        code: "invalid_command",
        message: "create_avatar",
      });
    if (active(s.phase) || s.preparing !== undefined)
      return refuse(
        id,
        "create_avatar",
        "state",
        "BUSY",
        "a call is active or an avatar is being prepared",
      );
    const { image, image_url: url, name } = decoded.success;
    if ((image == null) === (url == null))
      return refuse(
        id,
        "create_avatar",
        "request",
        "INVALID_INPUT",
        "give exactly one of image or image_url",
      );
    if (name != null && name.length > documented.avatarName)
      return refuse(
        id,
        "create_avatar",
        "request",
        "INVALID_INPUT",
        "name is at most 100 characters",
      );
    const t = token();
    set({
      phase: "preparing_avatar",
      preparing: { token: t, avatar: { id: avatarId, name: name ?? null } },
      avatar: undefined,
    });
    ack(id);
    changed();
    emit({ _tag: "Prepare", token: t });
  };

  const attachAvatar = (id: string, args: Schema.JsonObject, saved: Avatar | undefined): void => {
    const decoded = Schema.decodeUnknownResult(Arguments.attach_avatar)(args);
    if (Result.isFailure(decoded))
      return emit({
        _tag: "Reject",
        requestId: id,
        code: "invalid_command",
        message: "attach_avatar",
      });
    if (active(s.phase) || s.preparing !== undefined)
      return refuse(
        id,
        "attach_avatar",
        "state",
        "BUSY",
        "a call is active or an avatar is being prepared",
      );
    if (saved === undefined)
      return refuse(id, "attach_avatar", "request", "AVATAR_NOT_FOUND", "no avatar has that id");
    set({ phase: "avatar_ready", avatar: saved });
    ack(id);
    changed();
  };

  const startCall = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeUnknownResult(Arguments.start_call)(args);
    if (Result.isFailure(decoded))
      return emit({
        _tag: "Reject",
        requestId: id,
        code: "invalid_command",
        message: "start_call",
      });
    if (active(s.phase)) return refuse(id, "start_call", "state", "BUSY", "a call is active");
    if (s.avatar === undefined || s.preparing !== undefined)
      return refuse(id, "start_call", "state", "NO_AVATAR", "no avatar is ready");
    const a = decoded.success;
    const invalid =
      a.voice != null && !isVoice(a.voice)
        ? "no voice has that name"
        : a.greeting != null && a.greeting.length > documented.greeting
          ? "greeting is at most 200 characters"
          : a.language != null && a.language.length > documented.language
            ? "language is at most 40 characters"
            : a.call_mode != null && a.call_mode !== "audio" && a.call_mode !== "video"
              ? "call_mode is audio or video"
              : settingsRefusal(a.vad, a.llm);
    if (invalid !== undefined) return refuse(id, "start_call", "request", "INVALID_INPUT", invalid);
    const t = token();
    set({
      phase: "starting",
      calls: s.calls + 1,
      warmupAttempts: 0,
      endReason: null,
      lastError: null,
      call: {
        token: t,
        ordinal: s.calls + 1,
        mode: a.call_mode === "video" ? "video" : "audio",
        voice: a.voice ?? s.voice ?? documented.defaultVoice,
        transcripts: a.transcripts ?? true,
        greeting: a.greeting ?? undefined,
        live: undefined,
        heard: [],
        answering: undefined,
        references: [],
        lastSet: [],
      },
    });
    ack(id);
    changed();
    emit({ _tag: "Warm", token: t });
  };

  const say = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeUnknownResult(Arguments.say)(args);
    if (Result.isFailure(decoded))
      return emit({ _tag: "Reject", requestId: id, code: "invalid_command", message: "say" });
    const call = s.call;
    if (s.phase !== "live" || call === undefined)
      return refuse(id, "say", "state", "NOT_LIVE", "the call is not live");
    setCall({ heard: [...call.heard, decoded.success.text] });
    ack(id);
    if (call.transcripts)
      broadcast({
        type: "transcript",
        data: { speaker: "user", text: decoded.success.text, final: true },
      });
    think();
  };

  const interrupt = (id: string): void => {
    if (s.phase !== "live")
      return refuse(id, "interrupt", "state", "NOT_LIVE", "the call is not live");
    hush();
    setCall({ heard: [] });
    ack(id);
  };

  const updateCall = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeResult(Arguments.update_call)(args);
    if (Result.isFailure(decoded))
      return emit({
        _tag: "Reject",
        requestId: id,
        code: "invalid_command",
        message: "update_call",
      });
    if (s.phase !== "live")
      return refuse(id, "update_call", "state", "NOT_LIVE", "the call is not live");
    const a = decoded.success;
    const applied = Struct.keys(a).filter((key) => a[key] != null);
    const invalid =
      applied.length === 0
        ? "give at least one field"
        : a.voice != null && !isVoice(a.voice)
          ? "no voice has that name"
          : a.persona != null && (a.persona.length < 1 || a.persona.length > documented.persona)
            ? "persona is 1 to 50,000 characters"
            : settingsRefusal(a.vad, a.llm);
    if (invalid !== undefined)
      return refuse(id, "update_call", "request", "INVALID_INPUT", invalid);
    if (a.voice != null) setCall({ voice: a.voice });
    reply(id, { type: "call_updated", data: { applied } });
    if (a.voice != null) changed();
  };

  const setReferences = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeUnknownResult(Arguments.set_reference_images)(args);
    if (Result.isFailure(decoded))
      return emit({
        _tag: "Reject",
        requestId: id,
        code: "invalid_command",
        message: "set_reference_images",
      });
    const call = s.call;
    if (s.phase !== "live" || call === undefined)
      return refuse(id, "set_reference_images", "state", "NOT_LIVE", "the call is not live");
    const entries = Schema.decodeUnknownResult(References)(decoded.success.images);
    if (Result.isFailure(entries))
      return refuse(
        id,
        "set_reference_images",
        "request",
        "INVALID_INPUT",
        "give one to three valid images",
      );
    const given = entries.success.map(({ image_id, kind }) => ({
      image_id,
      kind: kind ?? "object",
    }));
    const ids = given.map((entry) => entry.image_id);
    if (new Set(ids).size !== ids.length)
      return refuse(id, "set_reference_images", "request", "INVALID_INPUT", "image_id repeats");
    // An id already in effect is replaced.
    const references = [
      ...call.references.filter((entry) => !ids.includes(entry.image_id)),
      ...given,
    ];
    setCall({ references, lastSet: ids });
    reply(id, {
      type: "reference_images_applied",
      data: { image_ids: references.map((entry) => entry.image_id) },
    });
    changed();
  };

  const clearReferences = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeResult(Arguments.clear_reference_images)(args);
    if (Result.isFailure(decoded))
      return emit({
        _tag: "Reject",
        requestId: id,
        code: "invalid_command",
        message: "clear_reference_images",
      });
    const call = s.call;
    if (s.phase !== "live" || call === undefined)
      return refuse(id, "clear_reference_images", "state", "NOT_LIVE", "the call is not live");
    const given = decoded.success.image_ids;
    const wanted = given == null ? undefined : Schema.decodeUnknownResult(ImageIds)(given);
    if (wanted !== undefined && Result.isFailure(wanted))
      return refuse(
        id,
        "clear_reference_images",
        "request",
        "INVALID_INPUT",
        "give up to three image ids",
      );
    const cleared: ReadonlyArray<string> = wanted === undefined ? call.lastSet : wanted.success;
    const references = call.references.filter((entry) => !cleared.includes(entry.image_id));
    setCall({ references, lastSet: wanted === undefined ? [] : call.lastSet });
    reply(id, {
      type: "reference_images_applied",
      data: { image_ids: references.map((entry) => entry.image_id) },
    });
    changed();
  };

  const endCall = (id: string): void => {
    const call = s.call;
    if (!active(s.phase) || call === undefined || s.phase === "ending")
      return refuse(id, "end_call", "state", "NOT_LIVE", "no call is active");
    hush();
    set({ phase: "ending" });
    emit({ _tag: "Dark", token: call.token });
    changed();
    emit({ _tag: "Hangup", token: call.token, requestId: id });
  };

  const command = (input: Extract<Input, { readonly _tag: "Command" }>): void => {
    const { requestId: id, name, args } = input;
    if (!isCommand(name))
      return emit({ _tag: "Reject", requestId: id, code: "invalid_command", message: name });
    // Reactor documents a command carrying an explicit null as dropped whole, with no
    // `command_error`; hosted Vidu answered one with an error frame on 2026-10-01.
    if (Object.values(args).some((value) => value === null))
      return emit({ _tag: "Reject", requestId: id, code: "invalid_command", message: name });
    switch (name) {
      case "create_avatar":
        return createAvatar(id, args, input.newAvatarId ?? "");
      case "attach_avatar":
        return attachAvatar(id, args, input.saved);
      case "list_voices":
        return reply(id, {
          type: "voices",
          data: { system: documented.voices, cloned: [], default_voice: documented.defaultVoice },
        });
      case "clone_voice":
        // Hosted Vidu refused it so on 2026-10-01 and 2026-10-02: cloning was off there.
        return refuse(
          id,
          "clone_voice",
          "state",
          "CLONING_DISABLED",
          "voice cloning is disabled",
          true,
        );
      case "start_call":
        return startCall(id, args);
      case "say":
        return say(id, args);
      case "interrupt":
        return interrupt(id);
      case "update_call":
        return updateCall(id, args);
      case "set_reference_images":
        return setReferences(id, args);
      case "clear_reference_images":
        return clearReferences(id, args);
      case "end_call":
        return endCall(id);
      case "get_state":
        return reply(id, { type: "session_state", data: snapshot() });
    }
  };

  switch (input._tag) {
    case "Connected":
      // A client that connects hears the snapshot, as each one does on connect.
      changed();
      break;
    case "Command":
      command(input);
      break;
    case "AvatarReady": {
      const preparing = s.preparing;
      if (preparing?.token !== input.token) break;
      set({ phase: "avatar_ready", preparing: undefined, avatar: preparing.avatar });
      emit({ _tag: "Save", avatar: preparing.avatar });
      changed();
      break;
    }
    case "WarmingUp":
      if (s.call?.token !== input.token || s.phase !== "starting") break;
      set({ phase: "warming_up", warmupAttempts: 1 });
      changed();
      break;
    case "Live": {
      const call = s.call;
      if (call?.token !== input.token || s.phase !== "warming_up") break;
      set({ phase: "live" });
      setCall({ live: { at: env.now, unix: env.unix } });
      changed();
      emit({ _tag: "Picture", token: call.token, call: call.ordinal, at: env.now });
      // A greeting is the character's first answer, before anything is said to it.
      if (call.greeting !== undefined) {
        const t = token();
        set({ turns: s.turns + 1 });
        setCall({
          answering: {
            token: t,
            turn: s.turns,
            speaking: false,
            text: answerText(s.turns, undefined),
          },
        });
        emit({ _tag: "Think", token: t });
      }
      break;
    }
    case "Answer": {
      const answering = s.call?.answering;
      if (answering?.token !== input.token || s.phase !== "live") break;
      setCall({ answering: { ...answering, speaking: true } });
      emit({ _tag: "Speak", token: answering.token, turn: answering.turn });
      break;
    }
    case "Spoken": {
      const call = s.call;
      const answering = call?.answering;
      if (call === undefined || answering?.token !== input.token) break;
      setCall({ answering: undefined });
      if (call.transcripts)
        broadcast({
          type: "transcript",
          data: { speaker: "character", text: answering.text, final: true },
        });
      think();
      break;
    }
    case "Released": {
      const call = s.call;
      if (call?.token !== input.token || s.phase !== "ending") break;
      const duration = call.live === undefined ? 0 : Math.floor((env.now - call.live.at) / 1000);
      set({ phase: "ended", call: undefined, voice: call.voice, endReason: "ended_by_client" });
      reply(input.requestId, {
        type: "call_ended",
        data: { end_reason: "ended_by_client", duration_seconds: duration },
      });
      changed();
      break;
    }
  }
  return [s, out];
};

/**
 * The deployment document `request_schema` answers with: each command and
 * its parameters, from this simulation's own statement of them.
 */
export const deployment = () => ({
  openapi: "3.1.0",
  info: { title: "Vidu S2-Avatar (ReactorTest)", version: documented.version },
  paths: Object.fromEntries(
    Struct.keys(Arguments).map((name) => [
      `/events/${name}`,
      {
        post: {
          operationId: name,
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: Schema.toJsonSchemaDocument(Arguments[name]).schema },
            },
          },
          responses: { "200": { description: replies[name] ?? "no reply" } },
        },
      },
    ]),
  ),
});

const replies: Partial<Record<Command, string>> = {
  list_voices: "voices",
  clone_voice: "voice_cloned",
  update_call: "call_updated",
  set_reference_images: "reference_images_applied",
  clear_reference_images: "reference_images_applied",
  end_call: "call_ended",
  get_state: "session_state",
};
