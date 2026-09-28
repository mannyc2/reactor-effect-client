/**
 * The simulated H3 model's queues, builds and playout as a pure state machine:
 * a command, a finished timer or a new connection goes in; the next state
 * comes out with the messages to send and the timers to start.
 *
 * It states H3 as Reactor documents it and paid runs measured it, and takes
 * no rule from the client: a client limit that drifts from the documentation
 * then fails against the simulation instead of passing with it.
 */
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

/** H3 Reference Turbo Realtime's documented facts, and the grid and audio paid runs measured. */
export const documented = {
  modelName: "reactor/h3-reference-to-video-turbo-realtime",
  version: "0.5.5",
  fps: 24,
  /** Built lengths: 124 frames and each 17 more up to 362, so 5.167 to 15.083 seconds. */
  frames: { min: 124, step: 17, max: 362 },
  /** Requested lengths, before they are aligned up to the grid. */
  seconds: { min: 5, max: 15.084 },
  images: 9,
  audio: 3,
  /** A continued clip spends one audio slot on the soundtrack it continues. */
  continuedAudio: 2,
  /** Images, audio and a continuation together. */
  references: 12,
  metadataChars: 2_000,
  /**
   * The text budget, about 2,000 tokens; past it a clip fails as it would
   * build. H3 does not document its tokenizer: 3.5 characters a token is this
   * simulation's.
   */
  text: { tokens: 2_000, charactersPerToken: 3.5 },
  canvases: {
    "16:9": { width: 1344, height: 768 },
    "1:1": { width: 768, height: 768 },
    "9:16": { width: 768, height: 1344 },
    "4:3": { width: 1024, height: 768 },
  },
  tracks: { video: "main_video", audio: "main_audio" },
  /** The soundtrack hosted H3 sent in paid runs. */
  soundtrack: { sampleRate: 48_000, channels: 1 },
} as const;

export type Aspect = keyof typeof documented.canvases;
const isAspect = (aspect: string): aspect is Aspect => Object.hasOwn(documented.canvases, aspect);

/** The frames a request of `seconds` builds: rounded to frames, then aligned up to the grid. */
export const framesFor = (seconds: number): number => {
  const { min, step, max } = documented.frames;
  const frames = Math.ceil(seconds * documented.fps - 1e-6);
  return Math.min(max, min + Math.ceil(Math.max(0, frames - min) / step) * step);
};

const requestable = (seconds: number): boolean =>
  seconds >= documented.seconds.min && seconds <= documented.seconds.max;

/** The clip object H3's messages and queues carry. */
export interface Clip {
  readonly clip_id: string;
  readonly prompt: string;
  readonly metadata: string;
  readonly frames: number;
  readonly seconds: number;
  readonly seed: number;
  readonly ready: boolean;
  readonly has_reference_image: boolean;
  readonly reference_image_count: number;
  readonly has_reference_audio: boolean;
  readonly reference_audio_count: number;
}

interface Queue {
  readonly generation: ReadonlyArray<Clip>;
  readonly playout: ReadonlyArray<Clip>;
  /** Always empty: a played clip is not listed. */
  readonly history: ReadonlyArray<Clip>;
}

interface State {
  readonly clip_seconds: number;
  readonly clip_seconds_min: number;
  readonly clip_seconds_max: number;
  readonly seed: number;
  readonly autoplay: boolean;
  readonly flush_on_clip_end: boolean;
  readonly aspect: string;
  readonly width: number;
  readonly height: number;
  readonly playing: boolean;
  readonly playing_clip_id: string | null;
  readonly generation_queued: number;
  readonly generation_capacity: number;
  readonly playout_queued: number;
  readonly playout_capacity: number;
  readonly clips_played: number;
  readonly seconds_sent: number;
  readonly valid_commands: ReadonlyArray<string>;
}

/** Each message H3 documents, and its payload. */
export type Message =
  | {
      readonly type: "clip_queued" | "clip_popped" | "clip_generated" | "clip_started";
      readonly data: { readonly clip: Clip };
    }
  | {
      readonly type: "clip_moved";
      readonly data: {
        readonly clip: Clip;
        readonly queue: "generation" | "playout";
        readonly position: number;
      };
    }
  | {
      readonly type: "clip_failed";
      readonly data: { readonly clip: Clip; readonly reason: string };
    }
  | {
      readonly type: "clip_finished" | "clip_stopped";
      readonly data: { readonly clip: Clip; readonly seconds_sent: number };
    }
  | { readonly type: "queue_update"; readonly data: Queue }
  | { readonly type: "state_update"; readonly data: State }
  | {
      readonly type: "command_error";
      readonly data: { readonly command: string; readonly reason: string };
    }
  | { readonly type: "seed_accepted"; readonly data: { readonly seed: number } }
  | {
      readonly type: "clip_length_accepted";
      readonly data: { readonly clip_seconds: number; readonly frames: number };
    }
  | {
      readonly type: "canvas_accepted";
      readonly data: { readonly aspect: string; readonly width: number; readonly height: number };
    }
  | {
      readonly type: "autoplay_accepted" | "flush_accepted";
      readonly data: { readonly enabled: boolean };
    }
  | {
      readonly type: "session_reset";
      readonly data: { readonly cleared_clips: number; readonly was_playing: boolean };
    };

const Upload = Schema.Struct({
  upload_id: Schema.String,
  name: Schema.String,
  mime_type: Schema.String,
  size: Schema.Int,
});
/** A parameter that may be omitted, and null as well where H3 documents it nullable. */
const omitted = Schema.optionalKey;
const nullable = <S extends Schema.Top>(schema: S) =>
  schema.pipe(Schema.NullOr, Schema.optionalKey);
const Empty = Schema.Struct({});

/**
 * Each command's parameters as H3 documents them; one omitted takes its
 * documented default. A value of the wrong type is refused as invalid.
 */
export const Arguments = {
  enqueue: Schema.Struct({
    prompt: omitted(Schema.String),
    reference_image: nullable(Upload),
    reference_images: nullable(Upload.pipe(Schema.Array)),
    reference_audio: nullable(Upload),
    reference_audios: nullable(Upload.pipe(Schema.Array)),
    seconds: nullable(Schema.Finite),
    seed: nullable(Schema.Int),
    position: nullable(Schema.Int),
    metadata: omitted(Schema.String),
    continue_from_clip_id: omitted(Schema.String),
  }),
  move: Schema.Struct({ clip_id: omitted(Schema.String), position: omitted(Schema.Int) }),
  pop: Schema.Struct({ clip_id: omitted(Schema.String) }),
  play: Schema.Struct({ clip_id: omitted(Schema.String) }),
  stop: Empty,
  set_seed: Schema.Struct({ seed: omitted(Schema.Int) }),
  set_clip_seconds: Schema.Struct({ seconds: omitted(Schema.Finite) }),
  set_canvas: Schema.Struct({ aspect: omitted(Schema.String) }),
  set_autoplay: Schema.Struct({ enabled: omitted(Schema.Boolean) }),
  set_flush_on_clip_end: Schema.Struct({ enabled: omitted(Schema.Boolean) }),
  get_queue: Empty,
  get_state: Empty,
  reset: Empty,
} as const;
type Enqueue = (typeof Arguments.enqueue)["Type"];

export interface H3 {
  readonly generation: ReadonlyArray<Clip>;
  readonly playout: ReadonlyArray<Clip>;
  /** A popped build keeps the build slot until it finishes, then is discarded. */
  readonly building:
    | { readonly clipId: string; readonly token: number; readonly discarded: boolean }
    | undefined;
  readonly playing:
    | { readonly clip: Clip; readonly startedAt: number; readonly token: number }
    | undefined;
  /**
   * The ready clip waiting out its seam before it starts. H3 counts an armed
   * clip as playing: `state_update` reports it, `play` refuses and `stop` cuts it.
   */
  readonly arming: { readonly token: number; readonly clipId: string } | undefined;
  /**
   * A stop acknowledged and not yet landed, and the clip it cuts: hosted H3
   * answered `stop` before the clip ended (0.7.0 `scheduler-cut` run), so for
   * that moment the clip still plays and `play` refuses.
   */
  readonly stopping: { readonly token: number; readonly clipId: string } | undefined;
  /**
   * Stops sent while one lands. Hosted H3 handled such a stop once the next
   * clip had started, so it stopped that clip.
   */
  readonly deferred: ReadonlyArray<string>;
  readonly autoplay: boolean;
  /**
   * The documented boundary: true flushes the video to black when a clip ends,
   * false holds its last frame. 0.7.0's paid runs used the default and saw the
   * black frame at every seam, the last of each clip; 0.6.0's reading of none
   * came from a pause measure that passed over a single dark frame.
   */
  readonly flush: boolean;
  readonly seed: number;
  readonly clipFrames: number;
  readonly aspect: Aspect;
  readonly clipsPlayed: number;
  readonly secondsSent: number;
  /** Clips created, which numbers the next clip id. */
  readonly clips: number;
  /** Clips that finished generating; a continuation can follow one. */
  readonly generated: ReadonlySet<string>;
  /** Clips popped, failed or reset away; a continuation from one falls back. */
  readonly dropped: ReadonlySet<string>;
  /** The clip each enqueue asked to continue from. */
  readonly continuations: ReadonlyMap<string, string>;
  /** Timer tokens issued, so a stale timer is ignored. */
  readonly tokens: number;
}

export const initial: H3 = {
  generation: [],
  playout: [],
  building: undefined,
  playing: undefined,
  arming: undefined,
  stopping: undefined,
  deferred: [],
  autoplay: false,
  flush: true,
  seed: 1000,
  clipFrames: framesFor(15),
  aspect: "16:9",
  clipsPlayed: 0,
  secondsSent: 0,
  clips: 0,
  generated: new Set(),
  dropped: new Set(),
  continuations: new Map(),
  tokens: 0,
};

export type Input =
  | { readonly _tag: "Connected" }
  | {
      readonly _tag: "Command";
      readonly requestId: string;
      readonly name: string;
      readonly args: Schema.JsonObject;
      /** A fault's refusal, returned in place of the command. */
      readonly refuse?: string;
    }
  | { readonly _tag: "Built"; readonly token: number }
  | { readonly _tag: "BuildFailed"; readonly token: number; readonly reason: string }
  | { readonly _tag: "Start"; readonly token: number }
  | { readonly _tag: "Finish"; readonly token: number }
  | { readonly _tag: "Landed"; readonly token: number };

export type Output =
  | { readonly _tag: "Reply"; readonly requestId: string; readonly message: Message }
  | { readonly _tag: "Ack"; readonly requestId: string }
  | { readonly _tag: "Unknown"; readonly requestId: string; readonly command: string }
  | { readonly _tag: "Broadcast"; readonly message: Message }
  /** Build the clip, continuing another or not; the runner finishes, fails or stalls it. */
  | {
      readonly _tag: "Build";
      readonly token: number;
      readonly seconds: number;
      readonly continued: boolean;
    }
  /** Start the first ready clip once the seam has passed. */
  | { readonly _tag: "Arm"; readonly token: number }
  /** An acknowledged stop takes effect after its lag. */
  | { readonly _tag: "Land"; readonly token: number }
  | {
      readonly _tag: "Play";
      readonly clip: Clip;
      readonly startedAt: number;
      readonly token: number;
    }
  /** Playback of the clip started with `token` stopped before its last frame. */
  | { readonly _tag: "Halt"; readonly token: number }
  /** The video flushes to black at a boundary. */
  | { readonly _tag: "Flush"; readonly clip: Clip }
  /**
   * A clip asked to continue from another starts building: the continuation
   * applies only if that clip finished generating and was not dropped, and
   * otherwise the clip builds independently, which H3 does not report.
   */
  | {
      readonly _tag: "Continuation";
      readonly clipId: string;
      readonly from: string;
      readonly applied: boolean;
    };

export interface Env {
  /** Monotonic milliseconds. */
  readonly now: number;
  readonly generationCapacity: number;
  readonly playoutCapacity: number;
}

const idPrefix = "00000000-0000-4000-8000-";

/** Clip ids are UUIDs numbered in creation order, so a frame can name its clip. */
export const clipId = (ordinal: number): string =>
  `${idPrefix}${ordinal.toString(16).padStart(12, "0")}`;

/** Whether the session created `id` and has not dropped it. */
const holds = (model: H3, id: string): boolean => {
  const ordinal = id.startsWith(idPrefix) ? Number.parseInt(id.slice(idPrefix.length), 16) : 0;
  return ordinal >= 1 && ordinal <= model.clips && !model.dropped.has(id);
};

const adding = (set: ReadonlySet<string>, ids: ReadonlyArray<string>) => new Set([...set, ...ids]);

/** An enqueue's references: images, audio, and whether it continues a clip. */
const referencesOf = (args: Enqueue) => ({
  images: args.reference_images?.length ?? (args.reference_image == null ? 0 : 1),
  audio: args.reference_audios?.length ?? (args.reference_audio == null ? 0 : 1),
  continued: (args.continue_from_clip_id ?? "") !== "",
});

/** Why H3 refuses an enqueue, by its documented rules. */
const refusal = (args: Enqueue, queued: number, env: Env): string | undefined => {
  const { images, audio, continued } = referencesOf(args);
  if (args.reference_image != null && args.reference_images != null)
    return "reference_image and reference_images together";
  if (args.reference_audio != null && args.reference_audios != null)
    return "reference_audio and reference_audios together";
  if ((args.prompt ?? "").trim() === "") return "the prompt is empty";
  if (args.seconds != null && !requestable(args.seconds)) return "seconds out of range";
  if ((args.seed ?? 0) < 0 || (args.position ?? 0) < 0) return "seed and position must be natural";
  if (Array.from(args.metadata ?? "").length > documented.metadataChars)
    return "metadata is too long";
  if (images > documented.images) return "too many reference images";
  if (audio > documented.audio || (audio > 0 && images === 0 && !continued))
    return "invalid reference audio";
  if (continued && audio > documented.continuedAudio)
    return "a continued clip takes at most two audio references";
  if (images + audio + (continued ? 1 : 0) > documented.references) return "too many references";
  if (queued >= env.generationCapacity) return "the generation queue is full";
  return undefined;
};

type Step = readonly [H3, ReadonlyArray<Output>];

export const step = ({
  model,
  input,
  env,
}: {
  readonly model: H3;
  readonly input: Input;
  readonly env: Env;
}): Step => {
  let s = model;
  const out: Output[] = [];
  const set = (patch: Partial<H3>): void => {
    s = { ...s, ...patch };
  };
  const token = (): number => {
    set({ tokens: s.tokens + 1 });
    return s.tokens;
  };
  const emit = (output: Output): void => {
    out.push(output);
  };
  const broadcast = (message: Message) => emit({ _tag: "Broadcast", message });
  const reply = (requestId: string, message: Message) =>
    emit({ _tag: "Reply", requestId, message });
  const refuse = (requestId: string, command: string, reason: string) =>
    reply(requestId, { type: "command_error", data: { command, reason } });
  const queue = (): Queue => ({ generation: s.generation, playout: s.playout, history: [] });
  const elapsed = (): number | undefined =>
    s.playing && Math.min(s.playing.clip.seconds, (env.now - s.playing.startedAt) / 1000);
  const state = (): State => {
    const idle = s.playing === undefined && s.arming === undefined;
    const queued = s.generation.length + s.playout.length;
    // Commands the state names valid; any command not listed here always is.
    const valid: Partial<Record<string, boolean>> = {
      enqueue: s.generation.length < env.generationCapacity,
      move: queued > 0,
      pop: queued > 0,
      play: idle && s.playout.length > 0,
      stop: !idle,
      set_canvas: idle && queued === 0,
    };
    return {
      clip_seconds: s.clipFrames / documented.fps,
      clip_seconds_min: documented.seconds.min,
      clip_seconds_max: documented.seconds.max,
      seed: s.seed,
      autoplay: s.autoplay,
      flush_on_clip_end: s.flush,
      aspect: s.aspect,
      ...documented.canvases[s.aspect],
      playing: !idle,
      playing_clip_id: s.playing?.clip.clip_id ?? s.arming?.clipId ?? null,
      generation_queued: s.generation.length,
      generation_capacity: env.generationCapacity,
      playout_queued: s.playout.length,
      playout_capacity: env.playoutCapacity,
      clips_played: s.clipsPlayed,
      seconds_sent: s.secondsSent + (elapsed() ?? 0),
      valid_commands: Object.keys(Arguments).filter((name) => valid[name] ?? true),
    };
  };
  const changed = (withState = true): void => {
    broadcast({ type: "queue_update", data: queue() });
    if (withState) broadcast({ type: "state_update", data: state() });
  };
  const fail = (clip: Clip, reason: string): void => {
    set({
      generation: s.generation.filter((entry) => entry !== clip),
      dropped: adding(s.dropped, [clip.clip_id]),
    });
    broadcast({ type: "clip_failed", data: { clip, reason } });
    changed();
  };
  /**
   * The next clip builds unless a build holds the slot or the ready queue is
   * full. A prompt past the model's text budget fails its clip when it would
   * build.
   */
  const build = (): void => {
    for (;;) {
      const next = s.generation[0];
      if (s.building !== undefined || next === undefined) return;
      if (s.playout.length >= env.playoutCapacity) return;
      if (next.prompt.length / documented.text.charactersPerToken > documented.text.tokens) {
        fail(next, "the prompt exceeds the model's text budget");
        continue;
      }
      const from = s.continuations.get(next.clip_id);
      const continued = from !== undefined && s.generated.has(from) && !s.dropped.has(from);
      if (from !== undefined)
        emit({ _tag: "Continuation", clipId: next.clip_id, from, applied: continued });
      const t = token();
      set({ building: { clipId: next.clip_id, token: t, discarded: false } });
      emit({ _tag: "Build", token: t, seconds: next.seconds, continued });
      return;
    }
  };
  const arm = (): void => {
    if (!s.autoplay || s.playing !== undefined || s.arming !== undefined) return;
    const next = s.playout[0];
    if (next === undefined) return;
    const t = token();
    set({ arming: { token: t, clipId: next.clip_id } });
    emit({ _tag: "Arm", token: t });
  };
  const start = (clip: Clip): void => {
    const t = token();
    set({
      arming: undefined,
      playout: s.playout.filter((entry) => entry !== clip),
      playing: { clip, startedAt: env.now, token: t },
    });
    emit({ _tag: "Play", clip, startedAt: env.now, token: t });
    broadcast({ type: "clip_started", data: { clip } });
    changed();
    build();
    if (s.stopping === undefined) resume();
  };
  const end = (how: "clip_finished" | "clip_stopped"): void => {
    const playing = s.playing;
    if (playing === undefined) return;
    const seconds = how === "clip_finished" ? playing.clip.seconds : (elapsed() ?? 0);
    set({
      playing: undefined,
      secondsSent: s.secondsSent + seconds,
      clipsPlayed: s.clipsPlayed + 1,
    });
    if (how === "clip_stopped") emit({ _tag: "Halt", token: playing.token });
    if (s.flush) emit({ _tag: "Flush", clip: playing.clip });
    broadcast({ type: how, data: { clip: playing.clip, seconds_sent: s.secondsSent } });
    broadcast({ type: "state_update", data: state() });
    arm();
  };
  /** Stop cuts the armed clip before it starts, as it would cut a playing one. */
  const cutArmed = (clipId: string): void => {
    const armed = s.playout.find((entry) => entry.clip_id === clipId);
    if (armed === undefined) return;
    set({
      arming: undefined,
      playout: s.playout.filter((entry) => entry !== armed),
      clipsPlayed: s.clipsPlayed + 1,
    });
    broadcast({ type: "clip_stopped", data: { clip: armed, seconds_sent: s.secondsSent } });
    changed();
    arm();
  };
  /**
   * A stop is acknowledged at once and takes effect after its lag, on the clip
   * that played or was armed when it arrived. One sent while another lands
   * waits for it.
   */
  const stop = (id: string): void => {
    if (s.stopping !== undefined) {
      set({ deferred: [...s.deferred, id] });
      return;
    }
    const clipId = s.playing?.clip.clip_id ?? s.arming?.clipId;
    if (clipId === undefined) return refuse(id, "stop", "nothing is playing");
    emit({ _tag: "Ack", requestId: id });
    const t = token();
    set({ stopping: { token: t, clipId } });
    emit({ _tag: "Land", token: t });
  };
  /** The stops that waited on a landing stop, handled now. */
  const resume = (): void => {
    const waiting = s.deferred;
    set({ deferred: [] });
    for (const id of waiting) stop(id);
  };
  const accepted = (id: string, message: Message): void => {
    reply(id, message);
    broadcast({ type: "state_update", data: state() });
  };

  const enqueue = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeResult(Arguments.enqueue)(args);
    if (Result.isFailure(decoded)) return refuse(id, "enqueue", "invalid arguments");
    const a = decoded.success;
    const problem = refusal(a, s.generation.length, env);
    if (problem !== undefined) return refuse(id, "enqueue", problem);
    const { images, audio } = referencesOf(a);
    // An unknown or dropped continuation falls back to an independent clip,
    // except for a clip whose audio would then have no image to go with.
    const from = a.continue_from_clip_id ?? "";
    if (from !== "" && !holds(s, from) && audio > 0 && images === 0)
      return refuse(id, "enqueue", "continue_from_clip_id names no clip the session holds");
    const frames = a.seconds == null ? s.clipFrames : framesFor(a.seconds);
    const clip: Clip = {
      clip_id: clipId(s.clips + 1),
      prompt: a.prompt ?? "",
      metadata: a.metadata ?? "",
      frames,
      seconds: frames / documented.fps,
      seed: a.seed ?? s.seed,
      ready: false,
      has_reference_image: images > 0,
      reference_image_count: images,
      has_reference_audio: audio > 0,
      reference_audio_count: audio,
    };
    // Position 0 goes ahead of every queued clip except the one building, as documented. One
    // hosted read (0.7.0 `scheduler-cut` run) listed it ahead of the running build as well.
    const first = s.building !== undefined && !s.building.discarded ? 1 : 0;
    const at = Math.min(Math.max(a.position ?? s.generation.length, first), s.generation.length);
    set({
      clips: s.clips + 1,
      seed: a.seed == null ? s.seed + 1 : s.seed,
      generation: [...s.generation.slice(0, at), clip, ...s.generation.slice(at)],
      ...(from === "" ? {} : { continuations: new Map(s.continuations).set(clip.clip_id, from) }),
    });
    // Hosted H3 broadcasts the queue that lists the clip before its reply.
    changed();
    reply(id, { type: "clip_queued", data: { clip } });
    build();
  };

  const move = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeResult(Arguments.move)(args);
    if (Result.isFailure(decoded) || (decoded.success.position ?? 0) < 0)
      return refuse(id, "move", "position must be a natural number");
    const { clip_id: wanted = "", position = 0 } = decoded.success;
    const target = s.generation.some((clip) => clip.clip_id === wanted) ? "generation" : "playout";
    const list = target === "generation" ? s.generation : s.playout;
    const clip = list.find((entry) => entry.clip_id === wanted);
    if (clip === undefined) return refuse(id, "move", "no queued clip has that id");
    const rest = list.filter((entry) => entry !== clip);
    // The clip building stays first; nothing moves ahead of it.
    const building = target === "generation" && s.building !== undefined && !s.building.discarded;
    const at =
      building && s.building?.clipId === wanted
        ? 0
        : Math.min(Math.max(position, building ? 1 : 0), rest.length);
    const moved = [...rest.slice(0, at), clip, ...rest.slice(at)];
    set(target === "generation" ? { generation: moved } : { playout: moved });
    reply(id, { type: "clip_moved", data: { clip, queue: target, position: at } });
    changed(false);
  };

  const pop = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeResult(Arguments.pop)(args);
    const wanted = Result.isSuccess(decoded) ? decoded.success.clip_id : undefined;
    const clip = [...s.generation, ...s.playout].find((entry) => entry.clip_id === wanted);
    if (clip === undefined) return refuse(id, "pop", "no queued clip has that id");
    set({
      generation: s.generation.filter((entry) => entry !== clip),
      playout: s.playout.filter((entry) => entry !== clip),
      dropped: adding(s.dropped, [clip.clip_id]),
      ...(s.building?.clipId === clip.clip_id
        ? { building: { ...s.building, discarded: true } }
        : {}),
    });
    reply(id, { type: "clip_popped", data: { clip } });
    changed();
    build();
  };

  const play = (id: string, args: Schema.JsonObject): void => {
    const decoded = Schema.decodeResult(Arguments.play)(args);
    if (Result.isFailure(decoded)) return refuse(id, "play", "invalid arguments");
    if (s.playing !== undefined || s.arming !== undefined)
      return refuse(id, "play", "a clip is already playing");
    const wanted = decoded.success.clip_id ?? "";
    const clip = wanted === "" ? s.playout[0] : s.playout.find((entry) => entry.clip_id === wanted);
    if (clip === undefined) return refuse(id, "play", "no matching ready clip");
    emit({ _tag: "Ack", requestId: id });
    start(clip);
  };

  const reset = (id: string): void => {
    const waiting = s.deferred;
    const armed = s.playout.find((entry) => entry.clip_id === s.arming?.clipId);
    const stopped = s.playing?.clip ?? armed;
    const queued = [...s.generation, ...s.playout].filter((clip) => clip !== armed);
    const halted = s.playing?.token;
    set({
      ...initial,
      clips: s.clips,
      tokens: s.tokens,
      generated: s.generated,
      dropped: adding(
        s.dropped,
        [...queued, ...(armed === undefined ? [] : [armed])].map((clip) => clip.clip_id),
      ),
      continuations: s.continuations,
      clipsPlayed: s.clipsPlayed + (stopped === undefined ? 0 : 1),
      secondsSent: s.secondsSent + (elapsed() ?? 0),
    });
    reply(id, {
      type: "session_reset",
      data: { cleared_clips: queued.length, was_playing: stopped !== undefined },
    });
    changed();
    if (stopped === undefined) return;
    if (halted !== undefined) emit({ _tag: "Halt", token: halted });
    // Reset always clears the tracks.
    emit({ _tag: "Flush", clip: stopped });
    broadcast({ type: "clip_stopped", data: { clip: stopped, seconds_sent: s.secondsSent } });
    // The landing stop went with the reset; the stops waiting on it find nothing playing.
    for (const deferred of waiting) stop(deferred);
  };

  const command = (id: string, name: string, args: Schema.JsonObject): void => {
    switch (name) {
      case "enqueue":
        return enqueue(id, args);
      case "move":
        return move(id, args);
      case "pop":
        return pop(id, args);
      case "play":
        return play(id, args);
      case "stop":
        return stop(id);
      case "set_seed": {
        const decoded = Schema.decodeResult(Arguments.set_seed)(args);
        const seed = Result.isSuccess(decoded) ? (decoded.success.seed ?? 1000) : -1;
        if (seed < 0) return refuse(id, name, "seed must be a natural number");
        set({ seed });
        return accepted(id, { type: "seed_accepted", data: { seed: s.seed } });
      }
      case "set_clip_seconds": {
        const decoded = Schema.decodeResult(Arguments.set_clip_seconds)(args);
        const seconds = Result.isSuccess(decoded) ? (decoded.success.seconds ?? 15) : 0;
        if (!requestable(seconds)) return refuse(id, name, "seconds out of range");
        set({ clipFrames: framesFor(seconds) });
        const data = { clip_seconds: s.clipFrames / documented.fps, frames: s.clipFrames };
        return accepted(id, { type: "clip_length_accepted", data });
      }
      case "set_canvas": {
        const decoded = Schema.decodeResult(Arguments.set_canvas)(args);
        const aspect = Result.isSuccess(decoded) ? (decoded.success.aspect ?? "16:9") : "";
        if (!isAspect(aspect)) return refuse(id, name, "unsupported aspect");
        if (s.playing !== undefined || s.generation.length + s.playout.length > 0)
          return refuse(id, name, "the canvas changes only while idle and empty");
        set({ aspect });
        const data = { aspect, ...documented.canvases[aspect] };
        return accepted(id, { type: "canvas_accepted", data });
      }
      case "set_autoplay":
      case "set_flush_on_clip_end": {
        const decoded = Schema.decodeResult(Arguments[name])(args);
        if (Result.isFailure(decoded)) return refuse(id, name, "enabled must be boolean");
        // Autoplay is off unless asked for, and flushing on.
        const enabled = decoded.success.enabled ?? name === "set_flush_on_clip_end";
        set(name === "set_autoplay" ? { autoplay: enabled } : { flush: enabled });
        const type = name === "set_autoplay" ? "autoplay_accepted" : "flush_accepted";
        accepted(id, { type, data: { enabled } });
        return arm();
      }
      case "get_queue":
        return reply(id, { type: "queue_update", data: queue() });
      case "get_state":
        return reply(id, { type: "state_update", data: state() });
      case "reset":
        return reset(id);
      default:
        emit({ _tag: "Unknown", requestId: id, command: name });
    }
  };

  switch (input._tag) {
    case "Connected":
      changed();
      break;
    case "Command":
      if (input.refuse !== undefined) refuse(input.requestId, input.name, input.refuse);
      else command(input.requestId, input.name, input.args);
      break;
    case "Built":
    case "BuildFailed": {
      const building = s.building;
      if (building?.token !== input.token) break;
      set({ building: undefined });
      const clip = s.generation.find((entry) => entry.clip_id === building.clipId);
      if (!building.discarded && clip !== undefined) {
        set({ generation: s.generation.filter((entry) => entry !== clip) });
        if (input._tag === "Built") {
          const ready: Clip = { ...clip, ready: true };
          set({ playout: [...s.playout, ready], generated: adding(s.generated, [clip.clip_id]) });
          broadcast({ type: "clip_generated", data: { clip: ready } });
        } else {
          set({ dropped: adding(s.dropped, [clip.clip_id]) });
          broadcast({ type: "clip_failed", data: { clip, reason: input.reason } });
        }
        changed();
        arm();
      }
      build();
      break;
    }
    case "Start": {
      const armed = s.arming;
      if (armed?.token !== input.token) break;
      set({ arming: undefined });
      const next = s.playout.find((entry) => entry.clip_id === armed.clipId) ?? s.playout[0];
      if (next !== undefined && s.autoplay && s.playing === undefined) start(next);
      break;
    }
    case "Finish":
      if (s.playing?.token === input.token) end("clip_finished");
      break;
    case "Landed": {
      const landing = s.stopping;
      if (landing?.token !== input.token) break;
      set({ stopping: undefined });
      if (s.playing?.clip.clip_id === landing.clipId) end("clip_stopped");
      else if (s.arming?.clipId === landing.clipId) cutArmed(landing.clipId);
      // The stops sent meanwhile wait for the next clip to start, if one is arming.
      if (s.arming === undefined) resume();
      break;
    }
  }
  return [s, out];
};

/** The message each command replies with, as H3's schema names it. */
const replies: Record<keyof typeof Arguments, string> = {
  enqueue: "clip_queued",
  move: "clip_moved",
  pop: "clip_popped",
  play: "accepted; emits clip_started",
  stop: "accepted; emits clip_stopped",
  set_seed: "seed_accepted",
  set_clip_seconds: "clip_length_accepted",
  set_canvas: "canvas_accepted",
  set_autoplay: "autoplay_accepted",
  set_flush_on_clip_end: "flush_accepted",
  get_queue: "queue_update",
  get_state: "state_update",
  reset: "session_reset",
};

/**
 * The deployment document `request_schema` answers with: each command and
 * its parameters, from this simulation's own statement of them. A deployment
 * without reference audio declares none.
 */
export const deployment = (referenceAudio: boolean) => ({
  openapi: "3.1.0",
  info: { title: "H3 Reference Turbo Realtime (ReactorTest)", version: documented.version },
  paths: Object.fromEntries(
    Struct.keys(Arguments).map((name) => {
      const args: Schema.Top =
        name === "enqueue" && !referenceAudio
          ? Arguments.enqueue.mapFields(Struct.omit(["reference_audio", "reference_audios"]))
          : Arguments[name];
      const schema = Schema.toJsonSchemaDocument(args).schema;
      return [
        `/events/${name}`,
        {
          post: {
            operationId: name,
            requestBody: { required: true, content: { "application/json": { schema } } },
            responses: { "200": { description: replies[name] } },
          },
        },
      ];
    }),
  ),
});
