/**
 * The simulated FastH3 model's queues, builds and playout as a pure state machine:
 * a command, a finished timer or a new connection goes in; the next state
 * comes out with the messages to send and the timers to start.
 *
 * It states FastH3 as Reactor documents it and paid runs measured it, and takes
 * no rule from the client: a client limit that drifts from the documentation
 * then fails against the simulation instead of passing with it.
 */
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as H3 from "./h3.js";

/** FastH3's own documented facts, with lengths observed on hosted FastH3. */
export const documented = {
  modelName: "reactor/fast-h3",
  version: "1.7.2",
  fps: 24,
  frames: { min: 124, step: 17, max: 345 },
  seconds: { min: 5.167, max: 14.375, default: 14.375 },
  metadataChars: 2_000,
  /** Reactor does not publish its tokenizer; this estimate belongs only to the simulation. */
  text: { tokens: 1_024, charactersPerToken: 3.5 },
  canvases: {
    "16:9": { width: 1344, height: 768 },
    // FastH3 names these aspects without pixel sizes; the simulation uses H3's sizes.
    "1:1": { width: 768, height: 768 },
    "9:16": { width: 768, height: 1344 },
    "4:3": { width: 1024, height: 768 },
  },
  tracks: { video: "main_video", audio: "main_audio" },
  soundtrack: { sampleRate: 48_000, channels: 1 },
} as const;

export type Aspect = keyof typeof documented.canvases;
const Aspect = Schema.Literals(["16:9", "1:1", "9:16", "4:3"]);

/** Accepted lengths align upward; the rounded minimum still names the first grid point. */
export const framesFor = (seconds: number): number => {
  const { min, step, max } = documented.frames;
  if (seconds <= documented.seconds.min) return min;
  const frames = Math.ceil(seconds * documented.fps);
  return Math.min(max, min + Math.ceil(Math.max(0, frames - min) / step) * step);
};

const requestable = (seconds: number): boolean =>
  seconds >= documented.seconds.min && seconds <= documented.seconds.max;

/** The clip object FastH3's messages and queues carry. */
export interface Clip {
  readonly clip_id: string;
  readonly prompt: string;
  readonly metadata: string;
  readonly frames: number;
  readonly seconds: number;
  readonly seed: number;
  readonly ready: boolean;
  readonly continue_from_clip_id: string | null;
  readonly has_starting_frame: boolean;
  readonly ending_from_clip_id: string | null;
  readonly has_ending_frame: boolean;
}

interface Queue {
  readonly generation: ReadonlyArray<Clip>;
  readonly playout: ReadonlyArray<Clip>;
  /** Built clips no longer queued or playing, retained oldest first. */
  readonly history: ReadonlyArray<Clip>;
}

type State = Extract<H3.Message, { readonly type: "state_update" }>["data"];

/** Each message FastH3 documents, and its payload. */
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
  width: Schema.optionalKey(Schema.Int),
  height: Schema.optionalKey(Schema.Int),
});
/** Omitted parameters, and nullable frame and source arguments. */
const omitted = Schema.optionalKey;
const nullable = <S extends Schema.Top>(schema: S) =>
  schema.pipe(Schema.NullOr, Schema.optionalKey);

/**
 * Each command's parameters as Reactor documents them; one omitted takes its
 * documented default. A value of the wrong type is refused as invalid.
 */
export const Arguments = {
  ...H3.Arguments,
  enqueue: Schema.Struct({
    prompt: omitted(Schema.String),
    starting_frame: nullable(Upload),
    ending_frame: nullable(Upload),
    continue_from_clip_id: nullable(Schema.String),
    ending_from_clip_id: nullable(Schema.String),
    seconds: nullable(Schema.Finite),
    seed: nullable(Schema.Int),
    position: nullable(Schema.Int),
    metadata: omitted(Schema.String),
  }),
} as const;
type Enqueue = (typeof Arguments.enqueue)["Type"];

export interface FastH3 {
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
  /** A stop awaiting H3's simulated landing delay; FastH3's media drain is unmeasured. */
  readonly stopping: { readonly token: number; readonly clipId: string } | undefined;
  /** Stops sent while one lands wait for the next clip, following H3's stop behavior. */
  readonly deferred: ReadonlyArray<string>;
  readonly autoplay: boolean;
  /** Flush boundaries to black, except autoplay into the just-ended clip's continuation. */
  readonly flush: boolean;
  readonly seed: number;
  readonly clipFrames: number;
  readonly aspect: Aspect;
  readonly clipsPlayed: number;
  readonly secondsSent: number;
  /** Clips created, which numbers the next clip id. */
  readonly clips: number;
  /** The retained built clips, including any still queued or playing; evicted on new builds. */
  readonly built: ReadonlyArray<Clip>;
  /** Timer tokens issued, so a stale timer is ignored. */
  readonly tokens: number;
}

export const initial: FastH3 = {
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
  clipFrames: framesFor(documented.seconds.default),
  aspect: "16:9",
  clipsPlayed: 0,
  secondsSent: 0,
  clips: 0,
  built: [],
  tokens: 0,
};

export type Input = H3.Input;

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
  /** A build starts with its retained clip source, or independently. */
  | {
      readonly _tag: "Continuation";
      readonly clipId: string;
      readonly from: string | undefined;
      readonly applied: boolean;
    };

export interface Env {
  /** Monotonic milliseconds. */
  readonly now: number;
  readonly generationCapacity: number;
  readonly playoutCapacity: number;
  readonly connected: boolean;
  readonly history: number;
}

export const clipId = H3.clipId;

const sourcesOf = (clip: Clip): ReadonlyArray<string> => [
  ...new Set([clip.continue_from_clip_id, clip.ending_from_clip_id].filter((id) => id !== null)),
];

/** Frame uploads alone activate the image-validation fault. */
export const hasImages = (args: Schema.JsonObject): boolean =>
  args.starting_frame != null || args.ending_frame != null;

const refusal = (args: Enqueue, queued: number, env: Env): string | undefined => {
  if (args.starting_frame != null && args.continue_from_clip_id != null)
    return "starting_frame and continue_from_clip_id together";
  if (args.ending_frame != null && args.ending_from_clip_id != null)
    return "ending_frame and ending_from_clip_id together";
  if ((args.prompt ?? "").trim() === "") return "the prompt is empty";
  if ((args.prompt ?? "").length / documented.text.charactersPerToken > documented.text.tokens)
    return "the prompt exceeds the simulation's text estimate";
  if (args.seconds != null && !requestable(args.seconds)) return "seconds out of range";
  if ((args.seed ?? 0) < 0 || (args.position ?? 0) < 0) return "seed and position must be natural";
  if (Array.from(args.metadata ?? "").length > documented.metadataChars)
    return "metadata is too long";
  if (queued >= env.generationCapacity) return "the generation queue is full";
  return undefined;
};

type Step = readonly [FastH3, ReadonlyArray<Output>];

export const step = ({
  model,
  input,
  env,
}: {
  readonly model: FastH3;
  readonly input: Input;
  readonly env: Env;
}): Step => {
  let s = model;
  const out: Output[] = [];
  const set = (patch: Partial<FastH3>): void => {
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
  const held = (id: string): Clip | undefined =>
    [
      ...s.generation,
      ...s.playout,
      ...s.built,
      ...(s.playing === undefined ? [] : [s.playing.clip]),
    ].find((clip) => clip.clip_id === id);
  const queue = (): Queue => ({
    generation: s.generation,
    playout: s.playout,
    history: s.built.filter(
      (clip) =>
        clip.clip_id !== s.playing?.clip.clip_id &&
        !s.playout.some((queued) => queued.clip_id === clip.clip_id),
    ),
  });
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
    const failed = new Set([clip.clip_id]);
    for (;;) {
      const dependent = s.generation.find(
        (entry) => !failed.has(entry.clip_id) && sourcesOf(entry).some((id) => failed.has(id)),
      );
      if (dependent === undefined) break;
      failed.add(dependent.clip_id);
    }
    const clips = [
      clip,
      ...s.generation.filter((entry) => entry !== clip && failed.has(entry.clip_id)),
    ];
    set({ generation: s.generation.filter((entry) => !failed.has(entry.clip_id)) });
    for (const entry of clips) broadcast({ type: "clip_failed", data: { clip: entry, reason } });
    changed();
  };
  /** A blocked dependency yields the build slot to the first eligible clip behind it. */
  const build = (): void => {
    if (!env.connected || s.building !== undefined || s.playout.length >= env.playoutCapacity)
      return;
    for (;;) {
      const missing = s.generation.find((clip) =>
        sourcesOf(clip).some((id) => held(id) === undefined),
      );
      if (missing === undefined) break;
      fail(missing, "a source clip is no longer retained");
    }
    const next = s.generation.find((clip) =>
      sourcesOf(clip).every((id) => held(id)?.ready === true),
    );
    if (next === undefined) return;
    const sources = sourcesOf(next);
    emit({
      _tag: "Continuation",
      clipId: next.clip_id,
      from: sources[0],
      applied: sources.length > 0,
    });
    const t = token();
    set({ building: { clipId: next.clip_id, token: t, discarded: false } });
    emit({ _tag: "Build", token: t, seconds: next.seconds, continued: sources.length > 0 });
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
    const continued = s.autoplay && s.playout[0]?.continue_from_clip_id === playing.clip.clip_id;
    if (s.flush && !continued) emit({ _tag: "Flush", clip: playing.clip });
    broadcast({ type: how, data: { clip: playing.clip, seconds_sent: s.secondsSent } });
    changed();
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
    const decoded = Schema.decodeResult(Arguments.enqueue, { onExcessProperty: "error" })(args);
    if (Result.isFailure(decoded)) return refuse(id, "enqueue", "invalid arguments");
    const a = decoded.success;
    const problem = refusal(a, s.generation.length, env);
    if (problem !== undefined) return refuse(id, "enqueue", problem);
    // This is the documented refusal oracle; hosted unknown-source error attribution is unproved.
    for (const source of [a.continue_from_clip_id, a.ending_from_clip_id])
      if (source != null && held(source) === undefined)
        return refuse(id, "enqueue", "a source clip is unknown or evicted");
    const frames = a.seconds == null ? s.clipFrames : framesFor(a.seconds);
    const clip: Clip = {
      clip_id: clipId(s.clips + 1),
      prompt: a.prompt ?? "",
      metadata: a.metadata ?? "",
      frames,
      seconds: frames / documented.fps,
      seed: a.seed ?? s.seed,
      ready: false,
      continue_from_clip_id: a.continue_from_clip_id ?? null,
      has_starting_frame: a.starting_frame != null,
      ending_from_clip_id: a.ending_from_clip_id ?? null,
      has_ending_frame: a.ending_frame != null,
    };
    const at = Math.min(a.position ?? s.generation.length, s.generation.length);
    set({
      clips: s.clips + 1,
      seed: a.seed == null ? s.seed + 1 : s.seed,
      generation: [...s.generation.slice(0, at), clip, ...s.generation.slice(at)],
    });
    // The queue announces the clip before its correlated acceptance.
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
    const at = Math.min(position, rest.length);
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
    if (!clip.ready && s.generation.some((entry) => sourcesOf(entry).includes(clip.clip_id)))
      return refuse(id, "pop", "an unbuilt source has queued dependents");
    set({
      generation: s.generation.filter((entry) => entry !== clip),
      playout: s.playout.filter((entry) => entry !== clip),
      // An armed clip popped in its seam never starts: unlike a stopped one it is not counted as
      // played, and nothing plays, as after a boundary, until the next Ready clip is armed and
      // waits out a seam of its own.
      ...(s.arming?.clipId === clip.clip_id ? { arming: undefined } : {}),
      ...(s.building?.clipId === clip.clip_id
        ? { building: { ...s.building, discarded: true } }
        : {}),
    });
    reply(id, { type: "clip_popped", data: { clip } });
    changed();
    build();
    arm();
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
        const seconds = Result.isSuccess(decoded)
          ? (decoded.success.seconds ?? documented.seconds.default)
          : 0;
        if (!requestable(seconds)) return refuse(id, name, "seconds out of range");
        set({ clipFrames: framesFor(seconds) });
        const data = { clip_seconds: s.clipFrames / documented.fps, frames: s.clipFrames };
        return accepted(id, { type: "clip_length_accepted", data });
      }
      case "set_canvas": {
        const decoded = Schema.decodeResult(Arguments.set_canvas)(args);
        const aspect = Result.isSuccess(decoded) ? (decoded.success.aspect ?? "16:9") : "";
        const canvas = Schema.decodeUnknownResult(Aspect)(aspect);
        if (Result.isFailure(canvas)) return refuse(id, name, "unsupported aspect");
        if (s.playing !== undefined || s.generation.length + s.playout.length > 0)
          return refuse(id, name, "the canvas changes only while idle and empty");
        set({ aspect: canvas.success });
        const data = { aspect: canvas.success, ...documented.canvases[canvas.success] };
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
      build();
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
          set({ playout: [...s.playout, ready], built: [...s.built, ready].slice(-env.history) });
          broadcast({ type: "clip_generated", data: { clip: ready } });
        } else {
          // Restore the failed anchor for the cascade to emit it and every dependent once.
          set({ generation: [clip, ...s.generation] });
          fail(clip, input.reason);
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

/** The reply kinds shared by these thirteen commands. */
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

/** The deployment declares only FastH3's frame and clip-source inputs. */
export const deployment = () => ({
  openapi: "3.1.0",
  info: { title: "FastH3 (ReactorTest)", version: documented.version },
  paths: Object.fromEntries(
    Struct.keys(Arguments).map((name) => {
      const schema = Schema.toJsonSchemaDocument(Arguments[name]).schema;
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

/** Logging names events and clip identity, never prompt or provider text. */
export const describe = (message: Message) =>
  message.type === "queue_update" || message.type === "state_update"
    ? undefined
    : {
        name: message.type,
        ...("clip" in message.data ? { clipId: message.data.clip.clip_id } : {}),
      };
