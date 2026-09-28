/**
 * The simulated H3 model's queues, builds and playout as a pure state machine:
 * a command, a finished timer or a new connection goes in; the next state
 * comes out with the messages to send and the timers to start. It speaks the
 * client's own message Schemas, so every reply type-checks against them.
 */
import { dual } from "effect/Function";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import { Commands } from "../h3/commands.js";
import { Payloads } from "../h3/messages.js";
import type { Clip, Message, Queue, State } from "../h3/messages.js";
import {
  alignFrames,
  audioReferenceLimits,
  canvases,
  documentedVersion,
  estimateTokens,
  h3ReferenceTurboRealtime as profile,
  metadataMaxChars,
  referenceLimits,
  requestSeconds,
} from "../h3/profile.js";
import type { CanvasAspect } from "../h3/profile.js";

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
  readonly aspect: CanvasAspect;
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
  clipFrames: alignFrames(profile, 15),
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
      readonly prompt: string;
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

const requestable = (seconds: number): boolean =>
  seconds >= requestSeconds.min && seconds <= requestSeconds.max;

/** Why H3 refuses an enqueue, as the provider checks it. */
const refusal = (
  args: (typeof Commands.enqueue.args)["Type"],
  queued: number,
  env: Env,
): string | undefined => {
  const images = args.reference_images?.length ?? 0;
  const audios = args.reference_audios?.length ?? 0;
  const continued = (args.continue_from_clip_id ?? "") !== "";
  if (args.prompt.trim() === "") return "the prompt is empty";
  if (args.seconds != null && !requestable(args.seconds)) return "seconds out of range";
  if ((args.seed ?? 0) < 0 || (args.position ?? 0) < 0) return "seed and position must be natural";
  if (Array.from(args.metadata).length > metadataMaxChars) return "metadata is too long";
  if (images > referenceLimits.maxImages) return "too many reference images";
  if (audios > audioReferenceLimits.maxAudio || (audios > 0 && images === 0 && !continued))
    return "invalid reference audio";
  if (continued && audios > audioReferenceLimits.maxAudioWithContinuation)
    return "a continued clip takes at most two audio references";
  if (images + audios + (continued ? 1 : 0) > audioReferenceLimits.maxTotal)
    return "too many references";
  if (queued >= env.generationCapacity) return "the generation queue is full";
  return undefined;
};

type Step = readonly [H3, ReadonlyArray<Output>];

export const step: {
  (input: Input, env: Env): (model: H3) => Step;
  (model: H3, input: Input, env: Env): Step;
} = dual(3, (model: H3, input: Input, env: Env): Step => {
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
      clip_seconds: s.clipFrames / profile.fps,
      clip_seconds_min: requestSeconds.min,
      clip_seconds_max: requestSeconds.max,
      seed: s.seed,
      autoplay: s.autoplay,
      flush_on_clip_end: s.flush,
      aspect: s.aspect,
      ...canvases[s.aspect],
      playing: !idle,
      playing_clip_id: s.playing?.clip.clip_id ?? s.arming?.clipId ?? null,
      generation_queued: s.generation.length,
      generation_capacity: env.generationCapacity,
      playout_queued: s.playout.length,
      playout_capacity: env.playoutCapacity,
      clips_played: s.clipsPlayed,
      seconds_sent: s.secondsSent + (elapsed() ?? 0),
      valid_commands: Object.keys(Commands).filter((name) => valid[name] ?? true),
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
      if (estimateTokens(profile, next.prompt) > profile.prompt.maxTokens) {
        fail(next, "the prompt exceeds the model's text budget");
        continue;
      }
      const from = s.continuations.get(next.clip_id);
      const continued = from !== undefined && s.generated.has(from) && !s.dropped.has(from);
      if (from !== undefined)
        emit({ _tag: "Continuation", clipId: next.clip_id, from, applied: continued });
      const t = token();
      set({ building: { clipId: next.clip_id, token: t, discarded: false } });
      emit({ _tag: "Build", token: t, seconds: next.seconds, continued, prompt: next.prompt });
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
    const decoded = Schema.decodeResult(Commands.enqueue.args)({
      prompt: "",
      reference_images: null,
      metadata: "",
      ...args,
    });
    if (Result.isFailure(decoded)) return refuse(id, "enqueue", "invalid arguments");
    const a = decoded.success;
    const problem = refusal(a, s.generation.length, env);
    if (problem !== undefined) return refuse(id, "enqueue", problem);
    // An unknown or dropped continuation falls back to an independent clip,
    // except for a clip whose audio would then have no image to go with.
    const from = a.continue_from_clip_id ?? "";
    const audioOnly =
      (a.reference_audios?.length ?? 0) > 0 && (a.reference_images?.length ?? 0) === 0;
    if (from !== "" && !holds(s, from) && audioOnly)
      return refuse(id, "enqueue", "continue_from_clip_id names no clip the session holds");
    const frames = a.seconds == null ? s.clipFrames : alignFrames(profile, a.seconds);
    const images = a.reference_images?.length ?? 0;
    const audios = a.reference_audios?.length ?? 0;
    const clip: Clip = {
      clip_id: clipId(s.clips + 1),
      prompt: a.prompt,
      metadata: a.metadata,
      frames,
      seconds: frames / profile.fps,
      seed: a.seed ?? s.seed,
      ready: false,
      has_reference_image: images > 0,
      reference_image_count: images,
      has_reference_audio: audios > 0,
      reference_audio_count: audios,
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
    const decoded = Schema.decodeUnknownResult(Commands.move.args)(args);
    if (Result.isFailure(decoded) || decoded.success.position < 0)
      return refuse(id, "move", "position must be a natural number");
    const { clip_id: wanted, position } = decoded.success;
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
    const decoded = Schema.decodeUnknownResult(Commands.pop.args)(args);
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
    const decoded = Schema.decodeResult(Commands.play.args)({ clip_id: "", ...args });
    if (Result.isFailure(decoded)) return refuse(id, "play", "invalid arguments");
    if (s.playing !== undefined || s.arming !== undefined)
      return refuse(id, "play", "a clip is already playing");
    const wanted = decoded.success.clip_id;
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
        const decoded = Schema.decodeUnknownResult(Commands.set_seed.args)(args);
        if (Result.isFailure(decoded) || decoded.success.seed < 0)
          return refuse(id, name, "seed must be a natural number");
        set({ seed: decoded.success.seed });
        return accepted(id, { type: "seed_accepted", data: { seed: s.seed } });
      }
      case "set_clip_seconds": {
        const decoded = Schema.decodeUnknownResult(Commands.set_clip_seconds.args)(args);
        if (Result.isFailure(decoded) || !requestable(decoded.success.seconds))
          return refuse(id, name, "seconds out of range");
        set({ clipFrames: alignFrames(profile, decoded.success.seconds) });
        const data = { clip_seconds: s.clipFrames / profile.fps, frames: s.clipFrames };
        return accepted(id, { type: "clip_length_accepted", data });
      }
      case "set_canvas": {
        const decoded = Schema.decodeUnknownResult(Commands.set_canvas.args)(args);
        const canvas = profile.canvases.find(
          (entry) => Result.isSuccess(decoded) && entry.aspect === decoded.success.aspect,
        );
        if (canvas === undefined) return refuse(id, name, "unsupported aspect");
        if (s.playing !== undefined || s.generation.length + s.playout.length > 0)
          return refuse(id, name, "the canvas changes only while idle and empty");
        set({ aspect: canvas.aspect });
        return accepted(id, { type: "canvas_accepted", data: canvas });
      }
      case "set_autoplay":
      case "set_flush_on_clip_end": {
        const decoded = Schema.decodeUnknownResult(Commands[name].args)(args);
        if (Result.isFailure(decoded)) return refuse(id, name, "enabled must be boolean");
        const { enabled } = decoded.success;
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
});

/**
 * The deployment document `request_schema` answers with, derived from the
 * client's own command and message Schemas, so the two cannot drift.
 */
export const deployment = (referenceAudio: boolean) => {
  const body = (schema: Schema.Top) => ({
    required: true,
    content: { "application/json": { schema: Schema.toJsonSchemaDocument(schema).schema } },
  });
  return {
    openapi: "3.1.0",
    info: { title: "H3 (ReactorTest)", version: documentedVersion },
    paths: Object.fromEntries(
      Object.entries(Commands).map(([name, { args, reply }]) => [
        `/events/${name}`,
        {
          post: {
            operationId: name,
            requestBody: body(
              name === "enqueue" && !referenceAudio
                ? Commands.enqueue.args.mapFields(Struct.omit(["reference_audios"]))
                : args,
            ),
            responses:
              reply === null
                ? { "202": { description: "accepted" } }
                : { "200": { description: reply, ...body(Payloads[reply]) } },
          },
        },
      ]),
    ),
    webhooks: Object.fromEntries(
      Object.entries(Payloads).map(([name, payload]) => [
        name,
        { post: { operationId: name, requestBody: body(payload) } },
      ]),
    ),
  };
};
