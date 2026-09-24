/**
 * The H3 Reference Turbo Realtime model behind a twin session's channels,
 * written from the provider's published 0.5.5 schema (the frozen copy is
 * packages/client/test/h3/upstream/h3-schema.md), not from the client adapter.
 * Commands answer on the data channel with their named reply, correlated by
 * request id, before the events they cause; `play` and `stop` answer with a
 * bodyless acknowledgement. `enqueue` is the exception, as hosted H3 showed
 * on September 24, 2026: it broadcasts the queue that lists the new clip
 * before its `clip_queued` reply, and the twin sends the state with that
 * queue. Queue and state changes are
 * broadcast as the provider documents, so a client that attaches later reads
 * the same facts. Defaults follow the documentation too: autoplay is off, so a
 * ready clip waits for `play` unless the client turns autoplay on. Each
 * connection receives no media until it resumes its tracks, as on hosted
 * Reactor.
 */
import { randomUUID } from "node:crypto";
import type { Json, JsonObject } from "reactor-effect-client";
import {
  ControlClientMessage,
  ControlServerMessage,
  DataClientMessage,
  DataServerMessage,
  MessageKind,
  objectFromStruct,
  structFromObject,
} from "reactor-effect-client/wire";
import type { ChannelName, Media } from "./protocol.js";

/** A clip builds for this long before it is ready to play. */
const buildMs = 500;
/** Autoplay arms a ready clip for this long before it starts. */
const armMs = 500;
const fps = 24;
const minFrames = 124;
const maxFrames = 362;
const frameStep = 17;
const requestSeconds = { min: 5, max: 15.084 };
const generationCapacity = 20;
const playoutCapacity = 10;
const canvases: Readonly<Record<string, { readonly width: number; readonly height: number }>> = {
  "16:9": { width: 1344, height: 768 },
  "1:1": { width: 768, height: 768 },
  "9:16": { width: 768, height: 1344 },
  "4:3": { width: 1024, height: 768 },
};

/** The receive tracks an H3 session describes, from the client's side. */
export const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
] as const;

/** Requested seconds, rounded to frames and aligned upward to a supported length. */
const alignedFrames = (seconds: number): number => {
  const frames = Math.ceil(seconds * fps - 1e-6);
  return Math.min(
    maxFrames,
    minFrames + Math.ceil(Math.max(0, frames - minFrames) / frameStep) * frameStep,
  );
};

interface Clip {
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

export interface ModelFaults {
  /** Receive every enqueue and never answer it. */
  readonly dropEnqueueReply?: boolean;
  readonly blackFrames?: boolean;
  readonly frozenFrames?: boolean;
  readonly noAudio?: boolean;
}

/** What the model needs from the session it runs in. */
export interface ModelHost {
  /** Deliver one message on the current connection, if one is open. */
  readonly send: (channel: ChannelName, bytes: Uint8Array<ArrayBuffer>) => void;
  /** What the peer should render changed. */
  readonly changed: () => void;
  readonly enqueued: () => void;
}

const isCount = (value: Json | undefined): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const isRequestable = (value: Json | undefined): value is number =>
  typeof value === "number" && value >= requestSeconds.min && value <= requestSeconds.max;
const absent = (value: Json | undefined): value is null | undefined =>
  value === undefined || value === null;
/** How many references one-or-a-list names; undefined when both are given, or either is malformed. */
const references = (single: Json | undefined, list: Json | undefined): number | undefined => {
  if (!absent(single))
    return absent(list) && typeof single === "object" && !Array.isArray(single) ? 1 : undefined;
  if (absent(list)) return 0;
  return Array.isArray(list) ? list.length : undefined;
};

/**
 * One session's model. Everything it does is driven by the commands it
 * receives and by its own timers, which `close` clears.
 */
export class H3Model {
  private generation: Clip[] = [];
  private playout: Clip[] = [];
  private playing: { readonly clip: Clip; readonly startedAt: number } | undefined;
  private held: { readonly id: string; readonly frame: number } | undefined;
  private building: string | undefined;
  private arming: ReturnType<typeof setTimeout> | undefined;
  private finishing: ReturnType<typeof setTimeout> | undefined;
  private autoplay = false;
  private flush = true;
  private seed = 1000;
  private clipFrames = alignedFrames(15);
  private aspect = "16:9";
  private clipsPlayed = 0;
  private secondsSent = 0;
  private readonly paused = new Set<string>();
  private readonly timers = new Set<ReturnType<typeof setTimeout>>();
  private closed = false;

  constructor(
    private readonly host: ModelHost,
    private readonly faults: ModelFaults = {},
  ) {}

  /**
   * A connection opened: the model broadcasts its full queues and state, as on
   * connect. Its tracks start paused: each connection subscribes on its own.
   */
  connected(): void {
    this.paused.clear();
    for (const track of tracks) this.paused.add(track.name);
    this.broadcast("queue_update", this.queue());
    this.broadcast("state_update", this.state());
  }

  /** What a peer renders now. */
  media(now: number): Media {
    const playing = this.playing;
    return {
      clip:
        playing === undefined
          ? null
          : { id: playing.clip.clip_id, elapsedMs: now - playing.startedAt },
      hold: playing === undefined && !this.flush ? (this.held ?? null) : null,
      paused: [...this.paused],
      video: this.faults.blackFrames ? "black" : this.faults.frozenFrames ? "frozen" : "live",
      audio: this.faults.noAudio !== true,
    };
  }

  close(): void {
    this.closed = true;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
  }

  /** One channel message from the connected peer. Malformed bytes throw the wire's error. */
  receive(channel: ChannelName, bytes: Uint8Array): void {
    if (this.closed) return;
    if (channel === "control") this.control(ControlClientMessage.decode(bytes));
    else {
      const message = DataClientMessage.decode(bytes);
      if (message.payload?.case !== "command") return;
      const { type, data } = message.payload.value;
      this.command(message.request_id, type, data === undefined ? {} : objectFromStruct(data));
    }
  }

  private control(message: ControlClientMessage): void {
    const reply = (payload: NonNullable<ControlServerMessage["payload"]>) =>
      this.host.send(
        "control",
        ControlServerMessage.encode({
          request_id: message.request_id,
          kind: MessageKind.MESSAGE_KIND_RESPONSE,
          payload,
        }),
      );
    switch (message.payload?.case) {
      case "request_schema":
        return reply({ case: "model_schema", value: { openapi: structFromObject(schema()) } });
      case "pause_track":
        this.paused.add(message.payload.value.name);
        return this.host.changed();
      case "resume_track":
        this.paused.delete(message.payload.value.name);
        return this.host.changed();
      case "publish_track":
        return reply({
          case: "error",
          value: { code: "unknown_track", message: "H3 has no input tracks" },
        });
      case "request_clip":
      case "request_recording":
        return reply({ case: "clip_failed", value: { reason: "recorder disabled" } });
      default:
        // Pings, upload and unpublish notifications need no answer.
        return;
    }
  }

  private later(ms: number, body: () => void): ReturnType<typeof setTimeout> {
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      if (!this.closed) body();
    }, ms);
    this.timers.add(timer);
    return timer;
  }

  private cancel(timer: ReturnType<typeof setTimeout> | undefined): void {
    if (timer === undefined) return;
    clearTimeout(timer);
    this.timers.delete(timer);
  }

  private data(requestId: string, kind: number, type: string, data: JsonObject): void {
    this.host.send(
      "data",
      DataServerMessage.encode({
        request_id: requestId,
        kind,
        payload: { case: "message", value: { type, data: structFromObject(data) } },
      }),
    );
  }

  private reply(requestId: string, type: string, data: JsonObject): void {
    this.data(requestId, MessageKind.MESSAGE_KIND_RESPONSE, type, data);
  }

  private broadcast(type: string, data: JsonObject): void {
    this.data("", MessageKind.MESSAGE_KIND_NOTIFICATION, type, data);
  }

  private acknowledge(requestId: string): void {
    this.host.send(
      "data",
      DataServerMessage.encode({ request_id: requestId, kind: MessageKind.MESSAGE_KIND_RESPONSE }),
    );
  }

  private refuse(requestId: string, command: string, reason: string): void {
    this.reply(requestId, "command_error", { command, reason });
  }

  private queue(): JsonObject {
    return {
      generation: this.generation.map((clip) => ({ ...clip })),
      playout: this.playout.map((clip) => ({ ...clip })),
      history: [],
    };
  }

  private elapsedSeconds(now = Date.now()): number {
    return this.playing === undefined ? 0 : (now - this.playing.startedAt) / 1000;
  }

  private state(): JsonObject {
    const canvas = canvases[this.aspect]!;
    const queued = this.generation.length + this.playout.length;
    const valid = [
      ...(this.generation.length < generationCapacity ? ["enqueue"] : []),
      ...(queued > 0 ? ["move", "pop"] : []),
      ...(this.playing === undefined && this.playout.length > 0 ? ["play"] : []),
      ...(this.playing === undefined ? [] : ["stop"]),
      "set_seed",
      "set_clip_seconds",
      ...(this.playing === undefined && queued === 0 ? ["set_canvas"] : []),
      "set_autoplay",
      "set_flush_on_clip_end",
      "get_queue",
      "get_state",
      "reset",
    ];
    return {
      clip_seconds: this.clipFrames / fps,
      clip_seconds_min: requestSeconds.min,
      clip_seconds_max: requestSeconds.max,
      seed: this.seed,
      autoplay: this.autoplay,
      flush_on_clip_end: this.flush,
      aspect: this.aspect,
      width: canvas.width,
      height: canvas.height,
      playing: this.playing !== undefined,
      playing_clip_id: this.playing?.clip.clip_id ?? null,
      generation_queued: this.generation.length,
      generation_capacity: generationCapacity,
      playout_queued: this.playout.length,
      playout_capacity: playoutCapacity,
      clips_played: this.clipsPlayed,
      seconds_sent: this.secondsSent + this.elapsedSeconds(),
      valid_commands: valid,
    };
  }

  private changedQueues(state = true): void {
    this.broadcast("queue_update", this.queue());
    if (state) this.broadcast("state_update", this.state());
  }

  private command(id: string, type: string, args: JsonObject): void {
    switch (type) {
      case "enqueue":
        this.host.enqueued();
        if (this.faults.dropEnqueueReply === true) return;
        return this.enqueue(id, args);
      case "move":
        return this.move(id, args);
      case "pop":
        return this.pop(id, args);
      case "play":
        return this.play(id, args);
      case "stop": {
        const playing = this.playing;
        if (playing === undefined) return this.refuse(id, type, "nothing is playing");
        this.acknowledge(id);
        return this.end(playing, "clip_stopped");
      }
      case "set_seed":
        if (!isCount(args.seed)) return this.refuse(id, type, "seed must be a nonnegative integer");
        this.seed = args.seed;
        this.reply(id, "seed_accepted", { seed: this.seed });
        return this.broadcast("state_update", this.state());
      case "set_clip_seconds": {
        const seconds = args.seconds;
        if (!isRequestable(seconds))
          return this.refuse(id, type, "seconds must be between 5 and 15.084");
        this.clipFrames = alignedFrames(seconds);
        this.reply(id, "clip_length_accepted", {
          clip_seconds: this.clipFrames / fps,
          frames: this.clipFrames,
        });
        return this.broadcast("state_update", this.state());
      }
      case "set_canvas": {
        const aspect = args.aspect;
        if (typeof aspect !== "string" || !Object.hasOwn(canvases, aspect))
          return this.refuse(id, type, "unsupported aspect");
        if (this.playing !== undefined || this.generation.length + this.playout.length > 0)
          return this.refuse(
            id,
            type,
            "the canvas changes only while queues and playback are empty",
          );
        this.aspect = aspect;
        this.reply(id, "canvas_accepted", { aspect, ...canvases[aspect]! });
        return this.broadcast("state_update", this.state());
      }
      case "set_autoplay":
        if (typeof args.enabled !== "boolean")
          return this.refuse(id, type, "enabled must be boolean");
        this.autoplay = args.enabled;
        this.reply(id, "autoplay_accepted", { enabled: this.autoplay });
        this.broadcast("state_update", this.state());
        return this.arm();
      case "set_flush_on_clip_end":
        if (typeof args.enabled !== "boolean")
          return this.refuse(id, type, "enabled must be boolean");
        this.flush = args.enabled;
        this.reply(id, "flush_accepted", { enabled: this.flush });
        this.broadcast("state_update", this.state());
        return this.host.changed();
      case "get_queue":
        return this.reply(id, "queue_update", this.queue());
      case "get_state":
        return this.reply(id, "state_update", this.state());
      case "reset":
        return this.reset(id);
      default:
        return this.host.send(
          "data",
          DataServerMessage.encode({
            request_id: id,
            kind: MessageKind.MESSAGE_KIND_RESPONSE,
            payload: { case: "error", value: { code: "unknown_command", message: type } },
          }),
        );
    }
  }

  private enqueue(id: string, args: JsonObject): void {
    const refuse = (reason: string) => this.refuse(id, "enqueue", reason);
    const { prompt, seconds, seed, position, metadata } = args;
    if (typeof prompt !== "string" || prompt.trim().length === 0)
      return refuse("the prompt is empty");
    if (!absent(seconds) && !isRequestable(seconds))
      return refuse("seconds must be between 5 and 15.084");
    if (!absent(seed) && !isCount(seed)) return refuse("seed must be a nonnegative integer");
    if (!absent(position) && !isCount(position))
      return refuse("position must be a nonnegative integer");
    if (!absent(metadata) && (typeof metadata !== "string" || Array.from(metadata).length > 2000))
      return refuse("metadata must be a string of at most 2,000 characters");
    const images = references(args.reference_image, args.reference_images);
    const audios = references(args.reference_audio, args.reference_audios);
    if (images === undefined || images > 9) return refuse("invalid reference images");
    if (audios === undefined || audios > 3) return refuse("invalid reference audio");
    const continued =
      typeof args.continue_from_clip_id === "string" && args.continue_from_clip_id !== "";
    if (audios > 0 && images === 0 && !continued)
      return refuse("audio needs an image or a continued clip");
    if (this.generation.length >= generationCapacity) return refuse("the generation queue is full");
    const frames = isRequestable(seconds) ? alignedFrames(seconds) : this.clipFrames;
    const clip: Clip = {
      clip_id: randomUUID(),
      prompt,
      metadata: typeof metadata === "string" ? metadata : "",
      frames,
      seconds: frames / fps,
      seed: isCount(seed) ? seed : this.seed++,
      ready: false,
      has_reference_image: images > 0,
      reference_image_count: images,
      has_reference_audio: audios > 0,
      reference_audio_count: audios,
    };
    // A clip placed ahead of the running build still waits for it.
    const first = this.building === undefined ? 0 : 1;
    const at = isCount(position)
      ? Math.min(Math.max(position, first), this.generation.length)
      : this.generation.length;
    this.generation.splice(at, 0, clip);
    // Hosted H3's order: the queue that lists the clip comes before the reply.
    this.changedQueues();
    this.reply(id, "clip_queued", { clip: { ...clip } });
    this.build();
  }

  private move(id: string, args: JsonObject): void {
    const { clip_id: wanted, position: requested } = args;
    const target = this.generation.some((clip) => clip.clip_id === wanted)
      ? "generation"
      : this.playout.some((clip) => clip.clip_id === wanted)
        ? "playout"
        : undefined;
    if (target === undefined) return this.refuse(id, "move", "no queued clip has that id");
    if (!isCount(requested))
      return this.refuse(id, "move", "position must be a nonnegative integer");
    const queue = target === "generation" ? this.generation : this.playout;
    const index = queue.findIndex((clip) => clip.clip_id === wanted);
    const [clip] = queue.splice(index, 1);
    const position = Math.min(requested, queue.length);
    queue.splice(position, 0, clip!);
    this.reply(id, "clip_moved", { clip: { ...clip! }, queue: target, position });
    this.changedQueues(false);
  }

  private pop(id: string, args: JsonObject): void {
    const clip = [...this.generation, ...this.playout].find(
      (entry) => entry.clip_id === args.clip_id,
    );
    if (clip === undefined) return this.refuse(id, "pop", "no queued clip has that id");
    this.generation = this.generation.filter((entry) => entry !== clip);
    this.playout = this.playout.filter((entry) => entry !== clip);
    // A popped build's result is discarded.
    if (this.building === clip.clip_id) this.building = undefined;
    this.reply(id, "clip_popped", { clip: { ...clip } });
    this.changedQueues();
    this.build();
  }

  private play(id: string, args: JsonObject): void {
    const wanted = args.clip_id;
    const clip =
      absent(wanted) || wanted === ""
        ? this.playout[0]
        : this.playout.find((entry) => entry.clip_id === wanted);
    if (this.playing !== undefined) return this.refuse(id, "play", "a clip is already playing");
    if (clip === undefined) return this.refuse(id, "play", "no matching ready clip");
    this.acknowledge(id);
    this.start(clip);
  }

  private reset(id: string): void {
    const cleared = this.generation.length + this.playout.length;
    const playing = this.playing;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    this.generation = [];
    this.playout = [];
    this.playing = undefined;
    this.held = undefined;
    this.building = undefined;
    this.arming = undefined;
    this.finishing = undefined;
    this.autoplay = false;
    this.flush = true;
    this.seed = 1000;
    this.clipFrames = alignedFrames(15);
    this.aspect = "16:9";
    if (playing !== undefined) {
      this.secondsSent += this.elapsedFrom(playing);
      this.clipsPlayed++;
    }
    this.reply(id, "session_reset", { cleared_clips: cleared, was_playing: playing !== undefined });
    this.changedQueues();
    if (playing !== undefined)
      this.broadcast("clip_stopped", { clip: { ...playing.clip }, seconds_sent: this.secondsSent });
    this.host.changed();
  }

  private elapsedFrom(playing: { readonly clip: Clip; readonly startedAt: number }): number {
    return Math.min(playing.clip.seconds, (Date.now() - playing.startedAt) / 1000);
  }

  /** Build the front clip; building pauses while the playout queue is full. */
  private build(): void {
    if (
      this.building !== undefined ||
      this.generation.length === 0 ||
      this.playout.length >= playoutCapacity
    )
      return;
    const id = this.generation[0]!.clip_id;
    this.building = id;
    this.later(buildMs, () => {
      if (this.building !== id) return this.build();
      this.building = undefined;
      const index = this.generation.findIndex((clip) => clip.clip_id === id);
      if (index < 0) return this.build();
      const [built] = this.generation.splice(index, 1);
      const clip: Clip = { ...built!, ready: true };
      this.playout.push(clip);
      this.broadcast("clip_generated", { clip: { ...clip } });
      this.changedQueues();
      this.arm();
      this.build();
    });
  }

  private arm(): void {
    if (
      !this.autoplay ||
      this.playing !== undefined ||
      this.arming !== undefined ||
      this.playout.length === 0
    )
      return;
    this.arming = this.later(armMs, () => {
      this.arming = undefined;
      const clip = this.playout[0];
      if (clip !== undefined && this.autoplay && this.playing === undefined) this.start(clip);
    });
  }

  private start(clip: Clip): void {
    this.cancel(this.arming);
    this.arming = undefined;
    this.playout = this.playout.filter((entry) => entry.clip_id !== clip.clip_id);
    const playing = { clip, startedAt: Date.now() };
    this.playing = playing;
    this.finishing = this.later(clip.seconds * 1000, () => this.end(playing, "clip_finished"));
    // The picture changes before the event that reports it.
    this.host.changed();
    this.broadcast("clip_started", { clip: { ...clip } });
    this.changedQueues();
    this.build();
  }

  /** A clip finished all its frames, or `stop` cut it. */
  private end(
    playing: { readonly clip: Clip; readonly startedAt: number },
    how: "clip_finished" | "clip_stopped",
  ): void {
    if (this.playing !== playing) return;
    this.cancel(this.finishing);
    this.finishing = undefined;
    this.playing = undefined;
    const seconds = how === "clip_finished" ? playing.clip.seconds : this.elapsedFrom(playing);
    this.secondsSent += seconds;
    this.clipsPlayed++;
    this.held = {
      id: playing.clip.clip_id,
      frame: Math.max(0, Math.min(playing.clip.frames - 1, Math.floor(seconds * fps))),
    };
    this.host.changed();
    this.broadcast(how, { clip: { ...playing.clip }, seconds_sent: this.secondsSent });
    this.broadcast("state_update", this.state());
    this.arm();
  }
}

/**
 * The model's OpenAPI document, copied from the hand-authored offline fixture
 * packages/client/test/h3/ProviderSchema.ts (itself written from the saved H3
 * 0.5.5 documentation and pinned reactor-runtime's ModelSchema.to_openapi
 * layout), with this twin's title.
 */
const schema = (): JsonObject => {
  const str = { type: "string" },
    int = { type: "integer" },
    num = { type: "number" },
    bool = { type: "boolean" };
  const obj = (properties: JsonObject, required = Object.keys(properties)): JsonObject => ({
    type: "object",
    properties,
    required,
  });
  const list = (items: JsonObject): JsonObject => ({ type: "array", items });
  const ref = (name: string): JsonObject => ({ $ref: `#/components/schemas/${name}` });
  const optional = (schema: JsonObject): JsonObject => ({
    anyOf: [schema, { type: "null" }],
    default: null,
  });
  const clip = obj(
    {
      clip_id: { type: "string", format: "uuid" },
      prompt: str,
      metadata: str,
      frames: int,
      seconds: num,
      seed: int,
      ready: bool,
      has_reference_image: bool,
      reference_image_count: int,
      has_reference_audio: bool,
      reference_audio_count: int,
    },
    ["clip_id", "prompt", "metadata", "frames", "seconds", "seed", "ready"],
  );
  const components: JsonObject = {
    ReactorUploadReference: obj({
      upload_id: { type: "string", format: "uuid" },
      name: str,
      mime_type: str,
      size: int,
    }),
    ClipInfo: clip,
    ClipQueued: obj({ clip: ref("ClipInfo") }),
    ClipMoved: obj({ clip: ref("ClipInfo"), queue: str, position: int }),
    ClipPopped: obj({ clip: ref("ClipInfo") }),
    ClipGenerated: obj({ clip: ref("ClipInfo") }),
    ClipFailed: obj({ clip: ref("ClipInfo"), reason: str }),
    ClipStarted: obj({ clip: ref("ClipInfo") }),
    ClipFinished: obj({ clip: ref("ClipInfo"), seconds_sent: num }),
    ClipStopped: obj({ clip: ref("ClipInfo"), seconds_sent: num }),
    QueueUpdate: obj({
      generation: list(ref("ClipInfo")),
      playout: list(ref("ClipInfo")),
      history: list(ref("ClipInfo")),
    }),
    StateUpdate: obj({
      clip_seconds: num,
      clip_seconds_min: num,
      clip_seconds_max: num,
      seed: int,
      autoplay: bool,
      flush_on_clip_end: bool,
      aspect: str,
      width: int,
      height: int,
      playing: bool,
      playing_clip_id: { anyOf: [str, { type: "null" }] },
      generation_queued: int,
      generation_capacity: int,
      playout_queued: int,
      playout_capacity: int,
      clips_played: int,
      seconds_sent: num,
      valid_commands: list(str),
    }),
    CommandError: obj({ command: str, reason: str }),
    SeedAccepted: obj({ seed: int }),
    ClipLengthAccepted: obj({ clip_seconds: num, frames: int }),
    CanvasAccepted: obj({ aspect: str, width: int, height: int }),
    AutoplayAccepted: obj({ enabled: bool }),
    FlushAccepted: obj({ enabled: bool }),
    SessionReset: obj({ cleared_clips: int, was_playing: bool }),
  };
  const path = (name: string, properties: JsonObject, reply: string | null): JsonObject => ({
    post: {
      operationId: name,
      requestBody: {
        required: true,
        content: { "application/json": { schema: obj(properties, []) } },
      },
      responses:
        reply === null
          ? { "202": { description: "Command accepted" } }
          : {
              "200": {
                description: reply,
                content: { "application/json": { schema: ref(reply) } },
              },
            },
    },
  });
  const events = {
    clip_queued: "ClipQueued",
    clip_moved: "ClipMoved",
    clip_popped: "ClipPopped",
    clip_generated: "ClipGenerated",
    clip_failed: "ClipFailed",
    clip_started: "ClipStarted",
    clip_finished: "ClipFinished",
    clip_stopped: "ClipStopped",
    queue_update: "QueueUpdate",
    state_update: "StateUpdate",
    command_error: "CommandError",
    seed_accepted: "SeedAccepted",
    clip_length_accepted: "ClipLengthAccepted",
    canvas_accepted: "CanvasAccepted",
    autoplay_accepted: "AutoplayAccepted",
    flush_accepted: "FlushAccepted",
    session_reset: "SessionReset",
  };
  return {
    openapi: "3.1.0",
    info: { title: "H3 Reference Turbo Realtime (reactor twin)", version: "0.5.5" },
    "x-reactor": {
      tracks: [
        { name: "main_video", kind: "video", direction: "out" },
        { name: "main_audio", kind: "audio", direction: "out" },
      ],
    },
    paths: {
      "/events/enqueue": path(
        "enqueue",
        {
          prompt: { ...str, default: "" },
          reference_image: optional(ref("ReactorUploadReference")),
          reference_images: optional(list(ref("ReactorUploadReference"))),
          reference_audio: optional(ref("ReactorUploadReference")),
          reference_audios: optional(list(ref("ReactorUploadReference"))),
          seconds: optional(num),
          seed: optional(int),
          position: optional(int),
          metadata: { ...str, default: "" },
          continue_from_clip_id: { ...str, default: "" },
        },
        "ClipQueued",
      ),
      "/events/move": path("move", { clip_id: str, position: int }, "ClipMoved"),
      "/events/pop": path("pop", { clip_id: str }, "ClipPopped"),
      "/events/play": path("play", { clip_id: { ...str, default: "" } }, null),
      "/events/stop": path("stop", {}, null),
      "/events/set_seed": path("set_seed", { seed: int }, "SeedAccepted"),
      "/events/set_clip_seconds": path("set_clip_seconds", { seconds: num }, "ClipLengthAccepted"),
      "/events/set_canvas": path("set_canvas", { aspect: str }, "CanvasAccepted"),
      "/events/set_autoplay": path("set_autoplay", { enabled: bool }, "AutoplayAccepted"),
      "/events/set_flush_on_clip_end": path(
        "set_flush_on_clip_end",
        { enabled: bool },
        "FlushAccepted",
      ),
      "/events/get_state": path("get_state", {}, "StateUpdate"),
      "/events/get_queue": path("get_queue", {}, "QueueUpdate"),
      "/events/reset": path("reset", {}, "SessionReset"),
    },
    webhooks: Object.fromEntries(
      Object.entries(events).map(([name, component]) => [
        name,
        {
          post: {
            operationId: name,
            requestBody: {
              required: true,
              content: { "application/json": { schema: ref(component) } },
            },
          },
        },
      ]),
    ),
    components: { schemas: components },
  };
};
