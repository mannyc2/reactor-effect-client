import { ReactorError } from "../../errors.js";
import type { CommandReply, SessionEvent } from "../../session/index.js";
import type { Clip, DecodedMessage, MessageType, Queue, State } from "../messages.js";
import type { ClipObservation, Facts, ProviderSnapshot } from "../types.js";

const rank = (type: MessageType | null): number =>
  type === "clip_generated"
    ? 1
    : type === "clip_started"
      ? 2
      : type === "clip_finished" ||
          type === "clip_stopped" ||
          type === "clip_failed" ||
          type === "clip_popped"
        ? 3
        : 0;
const ended = (type: MessageType | null) => rank(type) === 3;

/** Only the Session observation reader may supply protocol facts to this reducer. */
export class ProviderState {
  private state: State | undefined;
  private queue: Queue | undefined;
  private lastFacts: Facts | null = null;
  private cause: ReactorError | undefined;
  private readonly clips = new Map<string, ClipObservation>();
  private revision: bigint;
  private generation: bigint;
  private stateDirty = true;
  private queueDirty = true;
  private readonly bodies = new Map<string, bigint>();
  private availability: "Synchronizing" | "Ready" | "Unavailable" = "Synchronizing";

  constructor(
    readonly sessionId: string,
    generation: bigint,
    revision: bigint,
    private readonly maxClips: number,
  ) {
    this.generation = generation;
    this.revision = revision;
  }

  snapshot(): ProviderSnapshot {
    const base = {
      sessionId: this.sessionId,
      transportGeneration: this.generation,
      revision: this.revision,
      clips: Object.freeze([...this.clips.values()]),
    };
    if (this.availability === "Ready" && this.state !== undefined && this.queue !== undefined)
      return Object.freeze({ ...base, _tag: "Ready", state: this.state, queue: this.queue });
    if (this.availability === "Unavailable")
      return Object.freeze({
        ...base,
        _tag: "Unavailable",
        cause: this.cause!,
        lastFacts: this.lastFacts,
      });
    return Object.freeze({ ...base, _tag: "Synchronizing", lastFacts: this.lastFacts });
  }

  unavailable(cause: ReactorError): void {
    this.rememberFacts();
    this.availability = "Unavailable";
    this.cause = cause;
  }

  private rememberFacts(): void {
    if (this.state !== undefined && this.queue !== undefined && this.coherent())
      this.lastFacts = Object.freeze({ state: this.state, queue: this.queue });
  }

  admit(source: SessionEvent): "applied" | "duplicate" | "stale" {
    if (source.generation < this.generation) return "stale";
    if (source.sequence <= this.revision) return "duplicate";
    this.revision = source.sequence;
    if (source.generation > this.generation) {
      this.rememberFacts();
      this.generation = source.generation;
      this.state = undefined;
      this.queue = undefined;
      this.stateDirty = true;
      this.queueDirty = true;
      this.bodies.clear();
      this.availability = "Synchronizing";
      this.cause = undefined;
    }
    if (source._tag === "Model" && source.correlation === "stale-generation") return "stale";
    // The wire correlator may label a body following an ACK as duplicate.
    // Each source sequence is distinct; an ACK is never a model payload.
    if (source._tag === "Model" && source.kind === "message" && source.requestId !== "") {
      if (
        source.correlation === "duplicate" &&
        this.bodies.get(source.requestId) === source.generation
      )
        return "duplicate";
      this.bodies.set(source.requestId, source.generation);
      if (this.bodies.size > 256) {
        const first = this.bodies.keys().next();
        if (!first.done) this.bodies.delete(first.value);
      }
    }
    if (source._tag === "Status") {
      if (
        source.status === "disconnected" ||
        source.status === "closing" ||
        source.status === "closed"
      ) {
        this.unavailable(
          new ReactorError({
            code: source.status === "disconnected" ? "Disconnected" : "Closed",
            message: `H3 session is ${source.status}`,
            context: { operation: "H3 observation" },
          }),
        );
      } else if (source.status === "ready" && this.availability === "Unavailable") {
        this.state = undefined;
        this.queue = undefined;
        this.stateDirty = true;
        this.queueDirty = true;
        this.availability = "Synchronizing";
        this.cause = undefined;
      }
    }
    return "applied";
  }

  private put(clip: Clip, source: CommandReply, lifecycle: ClipObservation["lifecycle"]): void {
    if (!this.clips.has(clip.clip_id) && this.clips.size >= this.maxClips)
      throw new ReactorError({
        code: "Overflow",
        message: "H3 clip observation bound exceeded",
        context: { operation: "H3 observation" },
      });
    this.clips.set(clip.clip_id, Object.freeze({ clip, source, lifecycle }));
  }

  private invalidate(state: boolean, queue: boolean): void {
    this.rememberFacts();
    this.stateDirty ||= state;
    this.queueDirty ||= queue;
    if (this.availability !== "Unavailable") this.availability = "Synchronizing";
  }

  private coherent(): boolean {
    return (
      this.state !== undefined &&
      this.queue !== undefined &&
      !this.stateDirty &&
      !this.queueDirty &&
      this.state.generation_queued === this.queue.generation.length &&
      this.state.playout_queued === this.queue.playout.length &&
      ![...this.queue.generation, ...this.queue.playout].some(
        (clip) => clip.clip_id === this.state!.playing_clip_id,
      )
    );
  }

  apply(message: DecodedMessage, source: CommandReply): "applied" | "duplicate" {
    if (message.type === "unknown") return "applied";
    if (message.type === "state_update") {
      this.state = message.data;
      this.stateDirty = false;
    } else if (message.type === "queue_update") {
      const clips = [...message.data.generation, ...message.data.playout, ...message.data.history];
      const ids = new Set([...this.clips.keys(), ...clips.map((clip) => clip.clip_id)]);
      if (ids.size > this.maxClips)
        throw new ReactorError({
          code: "Overflow",
          message: "H3 clip observation bound exceeded",
          context: { operation: "H3 observation" },
        });
      this.queue = message.data;
      this.queueDirty = false;
      for (const clip of clips) {
        const previous = this.clips.get(clip.clip_id);
        this.put(clip, source, previous?.lifecycle ?? null);
      }
    } else if ("clip" in message.data) {
      const clip = message.data.clip,
        previous = this.clips.get(clip.clip_id);
      if (message.type === "clip_moved") {
        this.put(clip, source, previous?.lifecycle ?? null);
        if (this.queue?.[message.data.queue][message.data.position]?.clip_id !== clip.clip_id)
          this.invalidate(false, true);
      } else {
        const lifecycle = message.type as ClipObservation["lifecycle"];
        if (previous !== undefined && previous.lifecycle !== null) {
          if (
            previous.lifecycle === lifecycle ||
            ended(previous.lifecycle) ||
            rank(previous.lifecycle) > rank(lifecycle)
          )
            return "duplicate";
        }
        this.put(clip, source, lifecycle);
        const queued =
          this.queue?.generation.some((entry) => entry.clip_id === clip.clip_id) === true;
        const ready = this.queue?.playout.some((entry) => entry.clip_id === clip.clip_id) === true;
        switch (message.type) {
          case "clip_queued":
            if (!queued && !ready && this.state?.playing_clip_id !== clip.clip_id)
              this.invalidate(true, true);
            break;
          case "clip_generated":
            if (!ready && this.state?.playing_clip_id !== clip.clip_id) this.invalidate(true, true);
            break;
          case "clip_started":
            if (this.state?.playing_clip_id !== clip.clip_id) this.invalidate(true, false);
            if (queued || ready) this.invalidate(false, true);
            break;
          case "clip_failed":
          case "clip_popped":
            if (queued || ready) this.invalidate(true, true);
            break;
          case "clip_finished":
          case "clip_stopped":
            if (this.state?.playing_clip_id === clip.clip_id || this.state === undefined)
              this.invalidate(true, false);
            break;
          default:
            break;
        }
      }
    } else {
      switch (message.type) {
        case "seed_accepted":
          if (this.state?.seed !== message.data.seed) this.invalidate(true, false);
          break;
        case "clip_length_accepted":
          if (this.state?.clip_seconds !== message.data.clip_seconds) this.invalidate(true, false);
          break;
        case "canvas_accepted":
          if (
            this.state?.aspect !== message.data.aspect ||
            this.state?.width !== message.data.width ||
            this.state?.height !== message.data.height
          )
            this.invalidate(true, false);
          break;
        case "autoplay_accepted":
          if (this.state?.autoplay !== message.data.enabled) this.invalidate(true, false);
          break;
        case "flush_accepted":
          if (this.state?.flush_on_clip_end !== message.data.enabled) this.invalidate(true, false);
          break;
        case "session_reset":
          if (
            this.state?.playing !== false ||
            this.queue?.generation.length !== 0 ||
            this.queue.playout.length !== 0
          )
            this.invalidate(true, true);
          break;
        default:
          break;
      }
    }
    // Settings/stop ACKs never fabricate state. Only the model's complete
    // state and queue snapshots establish synchronized provider facts.
    if (this.coherent() && this.availability !== "Unavailable") {
      this.availability = "Ready";
      this.rememberFacts();
    } else if (this.availability !== "Unavailable") this.availability = "Synchronizing";
    return "applied";
  }
}
