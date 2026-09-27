/**
 * What a check records while its session runs, each summarized as it arrives:
 * the library's own spans, stats samples, media summaries, the provider's
 * messages and the termination trail. Nothing here keeps a frame, a token or
 * provider text.
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import { recorder, type Recorded } from "reactor-effect-client/Media";
import { type SessionEvent, type Statistics } from "reactor-effect-client/Session";
import type * as Reactor from "reactor-effect-client";
import type * as H3 from "reactor-effect-client/h3";
import type * as Orchestration from "reactor-effect-client/orchestration";
import type { AudioFrame, VideoFrame } from "reactor-effect-client/Media";
import { terminal } from "reactor-effect-client/host";
import type { AudioSummary, SeamPause, SpanRecord, StatsSample, VideoSummary } from "./evidence.js";
import { liveVideoMotionFrames, type ClipVideoSeen } from "./gates.js";

/** Milliseconds since `origin`, a `Date.now()` reading. */
export const since = (origin: number): number => Date.now() - origin;

/** A single monotonic origin for the renewal subtree; legacy wall offsets stay separate. */
export const elapsedClock = Effect.gen(function* () {
  const clock = yield* Clock.Clock;
  const origin = clock.monotonicTimeNanosUnsafe();
  const now = () => Number((clock.monotonicTimeNanosUnsafe() - origin) / 1_000_000n);
  return { now, elapsed: Effect.sync(now) };
});

interface FrameSource {
  readonly sessionId: string;
  readonly generation: bigint;
}

/** Tags the original objects at the public source boundary, including queued retiring frames. */
export class FrameAttribution {
  private readonly frames = new WeakMap<VideoFrame | AudioFrame, FrameSource | "ambiguous">();

  get(frame: VideoFrame | AudioFrame): FrameSource | undefined {
    const source = this.frames.get(frame);
    return source === "ambiguous" ? undefined : source;
  }

  tag<F extends VideoFrame | AudioFrame, E, R>(
    stream: Stream.Stream<F, E, R>,
    source: FrameSource,
  ) {
    return stream.pipe(
      Stream.tap((frame) =>
        Effect.sync(() => {
          // An object a second source re-emits has no single source; relabelling
          // it would credit one source with another's frame, so it stays untagged.
          const previous = this.frames.get(frame);
          this.frames.set(
            frame,
            previous === undefined ||
              (previous !== "ambiguous" &&
                previous.sessionId === source.sessionId &&
                previous.generation === source.generation)
              ? source
              : "ambiguous",
          );
        }),
      ),
    );
  }

  source(source: Orchestration.Source): Orchestration.Source {
    return {
      ...source,
      media: source.media.pipe(
        Effect.map((media) => {
          const tag = { sessionId: source.id, generation: media.generation };
          return { ...media, video: this.tag(media.video, tag), audio: this.tag(media.audio, tag) };
        }),
      ),
    };
  }

  /** Reset the existing recorder on every attributed boundary, even if the new sequence increases. */
  recorded<F extends VideoFrame | AudioFrame, E, R>(stream: Stream.Stream<F, E, R>) {
    type Boundary = { readonly source: FrameSource | undefined; readonly offset: bigint };
    return recorder(
      stream.pipe(
        Stream.mapAccum(
          (): Boundary | undefined => undefined,
          (
            previous,
            frame,
          ): readonly [Boundary, readonly { readonly sequence: bigint; readonly frame: F }[]] => {
            const source = this.get(frame);
            const same =
              source !== undefined &&
              previous?.source?.sessionId === source.sessionId &&
              previous.source.generation === source.generation;
            const offset = same ? previous.offset : frame.sequence;
            return [{ source, offset }, [{ sequence: frame.sequence - offset, frame }]];
          },
        ),
      ),
    ).pipe(
      Stream.map((element): Recorded<F> =>
        element._tag === "Frame" ? { _tag: "Frame", frame: element.frame.frame } : element,
      ),
    );
  }
}

const round = (value: number, places = 1): number => {
  const scale = 10 ** places;
  return Math.round(value * scale) / scale;
};

const spread = (values: readonly number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  const pick = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
  return { p50: round(pick(0.5)), p95: round(pick(0.95)), max: round(sorted.at(-1)!) };
};

/**
 * Records the library's spans (`reactor.*`) as they end. Only `reactor.*`
 * attributes and `error.type` are kept: the library puts no credential, input,
 * reply or provider text in those, and nothing else is copied.
 */
export const spanRecorder = (origin: number, bound = 4096) => {
  const spans: Tracer.NativeSpan[] = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      if (options.name.startsWith("reactor.") && spans.length < bound) spans.push(span);
      return span;
    },
  });
  const ms = (nanos: bigint): number => round(Number(nanos / 1000n) / 1000 - origin, 1);
  const records = (): SpanRecord[] =>
    spans.map((span) => {
      const status = span.status;
      const attributes: Record<string, string | number | boolean> = {};
      for (const [key, value] of span.attributes)
        if (key.startsWith("reactor.") || key === "error.type")
          if (typeof value === "string" || typeof value === "boolean") attributes[key] = value;
          else if (typeof value === "number" && Number.isFinite(value)) attributes[key] = value;
          else if (typeof value === "bigint") attributes[key] = String(value);
      return {
        name: span.name,
        startMs: ms(status.startTime),
        ...(status._tag === "Ended"
          ? { durationMs: round(Number(status.endTime - status.startTime) / 1e6, 1) }
          : {}),
        status: status._tag === "Started" ? "open" : Exit.isSuccess(status.exit) ? "ok" : "error",
        attributes,
        events: span.events.map(([name, time]) => ({ name, atMs: ms(time) })),
      };
    });
  return { tracer, records };
};

/** One stats sample, reduced to what the evidence keeps. */
export const statsSample = (stats: Statistics, atMs: number): StatsSample => ({
  atMs,
  ...(stats.pair?.localCandidateType === undefined ? {} : { local: stats.pair.localCandidateType }),
  ...(stats.pair?.remoteCandidateType === undefined
    ? {}
    : { remote: stats.pair.remoteCandidateType }),
  ...(stats.roundTripTimeSeconds === undefined
    ? {}
    : { rttMs: round(stats.roundTripTimeSeconds * 1000) }),
  ...(stats.rates === undefined
    ? {}
    : { receivedKbps: round(stats.rates.receivedBitsPerSecond / 1000) }),
  ...(stats.pair?.availableIncomingBitrate === undefined
    ? {}
    : { availableIncomingKbps: round(stats.pair.availableIncomingBitrate / 1000) }),
  ...(stats.framesPerSecond === undefined ? {} : { fps: round(stats.framesPerSecond) }),
  ...(stats.jitterSeconds === undefined ? {} : { jitterMs: round(stats.jitterSeconds * 1000) }),
  ...(stats.lossRatio === undefined ? {} : { lossRatio: round(stats.lossRatio, 4) }),
});

/** Milliseconds since `origin` on Effect's clock, which is wall time outside tests. */
const elapsed = (origin: number) => Effect.map(Clock.currentTimeMillis, (now) => now - origin);

/** Samples `session.stats` every second into `samples` until interrupted; a failed read is skipped. */
export const sampleStats = (
  session: Pick<Reactor.Session, "stats">,
  origin: number,
  samples: StatsSample[],
  bound = 120,
) =>
  Effect.zip(session.stats, elapsed(origin)).pipe(
    Effect.map(([stats, atMs]) => {
      if (samples.length < bound) samples.push(statsSample(stats, atMs));
    }),
    Effect.ignore,
    Effect.repeat(Schedule.spaced("1 second")),
  );

/** FNV-1a over a pixel sample of about 16K pixels, and their mean luma. */
const sampled = (data: Uint8Array): { readonly digest: number; readonly luma: number } => {
  const pixels = Math.floor(data.length / 4);
  const step = Math.max(1, Math.floor(pixels / 16_384));
  let hash = 2166136261,
    luma = 0,
    count = 0;
  for (let pixel = 0; pixel < pixels; pixel += step) {
    const index = pixel * 4;
    const b = data[index]!,
      g = data[index + 1]!,
      r = data[index + 2]!;
    hash = Math.imul(hash ^ b, 16777619);
    hash = Math.imul(hash ^ g, 16777619);
    hash = Math.imul(hash ^ r, 16777619);
    luma += 0.0722 * b + 0.7152 * g + 0.2126 * r;
    count++;
  }
  return { digest: hash >>> 0, luma: count === 0 ? 0 : luma / count };
};

/** Mean luma above which a frame counts as lit: a decoder's black is not exactly zero. */
const litLuma = 12;

/**
 * Summarizes a video track's frames as they arrive. It keeps no frame, only
 * each frame's arrival, sampled digest, format and whether it was lit, so a
 * clip is judged by the frames that arrived while it played.
 */
export class VideoReader {
  private frames = 0;
  private lit = 0;
  private lost = 0;
  private gaps = 0;
  private withMetadata = 0;
  private withFrameId = 0;
  private withTimestamp = 0;
  private lumaTotal = 0;
  private readonly formats = new Set<string>();
  private readonly sizes = new Set<string>();
  private readonly digests = new Set<number>();
  private readonly arrivals: number[] = [];
  private readonly seen: {
    readonly atMs: number;
    readonly digest: number;
    readonly lit: boolean;
    readonly format: string;
  }[] = [];

  add(element: Recorded<VideoFrame>, atMs: number): void {
    if (element._tag === "Lost") {
      this.gaps++;
      this.lost += Number(element.count);
      return;
    }
    const frame = element.frame;
    this.frames++;
    this.formats.add(frame.format);
    this.sizes.add(`${frame.width}x${frame.height}`);
    const { digest, luma } = sampled(frame.data);
    if (this.digests.size < 100_000) this.digests.add(digest);
    this.lumaTotal += luma;
    if (luma > litLuma) this.lit++;
    if (frame.metadata.byteLength > 0) this.withMetadata++;
    if (frame.frameId !== 0n) this.withFrameId++;
    if (frame.timestampMicros !== 0n) this.withTimestamp++;
    if (this.arrivals.length < 100_000) {
      this.arrivals.push(atMs);
      this.seen.push({ atMs, digest, lit: luma > litLuma, format: frame.format });
    }
  }

  /** What the frames arriving at or after `atMs` showed. */
  seenSince(atMs: number): ClipVideoSeen {
    const frames = this.seen.filter((frame) => frame.atMs >= atMs);
    const recentLit = frames.slice(-liveVideoMotionFrames).filter((frame) => frame.lit);
    let recentChanges = 0;
    for (let index = 1; index < recentLit.length; index++)
      if (recentLit[index]!.digest !== recentLit[index - 1]!.digest) recentChanges++;
    return {
      frames: frames.length,
      formats: [...new Set(frames.map((frame) => frame.format))],
      lit: frames.filter((frame) => frame.lit).length,
      distinct: new Set(frames.map((frame) => frame.digest)).size,
      recentLitFrames: recentLit.length,
      recentChanges,
    };
  }

  get count(): number {
    return this.frames;
  }

  /** The first frame's arrival at or after `atMs`. */
  firstAfter(atMs: number): number | undefined {
    return this.arrivals.find((at) => at >= atMs);
  }

  /** Last decoded frame arrival at or before a handoff observation. */
  lastBefore(atMs: number): number | undefined {
    return this.arrivals.findLast((at) => at <= atMs);
  }

  /**
   * The longest stretch between `fromMs` and `toMs` with no new picture: from
   * a frame that differed from the one before it to the next that did. Frames
   * carry no clip identity, so a caller bounds the window around one boundary.
   */
  pause(fromMs: number, toMs: number): SeamPause | undefined {
    const frames = this.seen.filter((frame) => frame.atMs >= fromMs && frame.atMs <= toMs);
    // The window's first frame has no predecessor to differ from.
    const changed: number[] = [];
    for (let index = 1; index < frames.length; index++)
      if (frames[index]!.digest !== frames[index - 1]!.digest) changed.push(index);
    let longest: SeamPause | undefined;
    for (let index = 1; index < changed.length; index++) {
      const last = frames[changed[index - 1]!]!;
      const first = frames[changed[index]!]!;
      const durationMs = first.atMs - last.atMs;
      if (longest !== undefined && durationMs <= longest.durationMs) continue;
      const inside = frames.slice(changed[index - 1]! + 1, changed[index]);
      longest = {
        lastNewFrameMs: last.atMs,
        firstNewFrameMs: first.atMs,
        durationMs,
        frames: inside.length,
        dark: inside.filter((frame) => !frame.lit).length,
      };
    }
    return longest;
  }

  summary(): VideoSummary {
    const arrivals = this.arrivals;
    const intervals = arrivals.slice(1).map((at, index) => at - arrivals[index]!);
    const elapsed = arrivals.length > 1 ? arrivals.at(-1)! - arrivals[0]! : 0;
    return {
      frames: this.frames,
      arrivalsMs: [...arrivals],
      formats: [...this.formats],
      sizes: [...this.sizes],
      ...(arrivals.length === 0 ? {} : { firstFrameMs: arrivals[0]! }),
      ...(elapsed > 0 ? { fps: round(((arrivals.length - 1) * 1000) / elapsed) } : {}),
      ...(intervals.length === 0 ? {} : { interval: spread(intervals) }),
      lit: this.lit,
      distinct: this.digests.size,
      ...(this.frames === 0 ? {} : { meanLuma: round(this.lumaTotal / this.frames) }),
      lost: this.lost,
      gaps: this.gaps,
      withMetadata: this.withMetadata,
      withFrameId: this.withFrameId,
      withTimestamp: this.withTimestamp,
    };
  }
}

/** Summarizes an audio track's blocks as they arrive; it keeps no samples. */
export class AudioReader {
  private blocks = 0;
  private lost = 0;
  private peak = 0;
  private first: number | undefined;
  private readonly rates = new Set<number>();
  private readonly channels = new Set<number>();
  private readonly lengths = new Set<number>();
  private readonly arrivals: number[] = [];

  add(element: Recorded<AudioFrame>, atMs: number): void {
    if (element._tag === "Lost") {
      this.lost += Number(element.count);
      return;
    }
    const block = element.frame;
    this.blocks++;
    if (this.arrivals.length < 100_000) this.arrivals.push(atMs);
    this.first ??= atMs;
    this.rates.add(block.sampleRate);
    this.channels.add(block.channels);
    if (this.lengths.size < 16)
      this.lengths.add(Math.floor(block.samples.length / Math.max(1, block.channels)));
    let sum = 0;
    for (const sample of block.samples) sum += (sample / 32768) ** 2;
    this.peak = Math.max(this.peak, Math.sqrt(sum / Math.max(1, block.samples.length)));
  }

  get count(): number {
    return this.blocks;
  }

  summary(): AudioSummary {
    return {
      blocks: this.blocks,
      arrivalsMs: [...this.arrivals],
      sampleRates: [...this.rates],
      channels: [...this.channels],
      samplesPerBlock: [...this.lengths],
      ...(this.first === undefined ? {} : { firstBlockMs: this.first }),
      peakRms: round(this.peak, 4),
      lost: this.lost,
    };
  }
}

/** Reads `stream` into `reader` until it ends, fails or is interrupted. */
export const readInto = <F extends { readonly sequence: bigint }, E>(
  stream: Stream.Stream<Recorded<F>, E>,
  reader: { add(element: Recorded<F>, atMs: number): void },
  time: number | Effect.Effect<number>,
) =>
  stream.pipe(
    Stream.runForEach((element) =>
      Effect.map(typeof time === "number" ? elapsed(time) : time, (atMs) =>
        reader.add(element, atMs),
      ),
    ),
  );

/** Every clip object in a decoded message's data, however deep. */
const clipsIn = (value: unknown, found: { clip_id: string; metadata: string }[] = []) => {
  if (Array.isArray(value)) for (const item of value) clipsIn(item, found);
  else if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (typeof record.clip_id === "string" && typeof record.metadata === "string")
      found.push({ clip_id: record.clip_id, metadata: record.metadata });
    else for (const child of Object.values(record)) clipsIn(child, found);
  }
  return found;
};

const bump = (counts: Record<string, number>, key: string) => {
  counts[key] = (counts[key] ?? 0) + 1;
};

/**
 * Tallies what the provider sends: message types, unknown ones, duplicate and
 * stale deliveries and diagnostics by reason, plus each message that lists the
 * watched clip with its submission metadata intact.
 */
export class ContractTally {
  readonly messages: Record<string, number> = {};
  readonly unknown: Record<string, number> = {};
  readonly diagnostics: Record<string, number> = {};
  readonly echoes: Record<string, number> = {};
  duplicates = 0;
  stale = 0;
  lastState: Record<string, unknown> | undefined;
  private watched: { readonly clipId: string; readonly marker: string } | undefined;

  /** Count later messages that list `clipId` with metadata containing `marker`. */
  watch(clipId: string, marker: string): void {
    this.watched = { clipId, marker };
  }

  /** The observation itself ended early, so the counts stop there. */
  failed(error: Reactor.ReactorError): void {
    bump(this.diagnostics, `observation:${error.reason._tag}`);
  }

  add(event: H3.ProviderEvent): void {
    if (event._tag === "Diagnostic") {
      bump(this.diagnostics, event.error.reason._tag);
      return;
    }
    if (event._tag !== "Message") return;
    const message = event.message;
    if (message.type === "unknown") bump(this.unknown, message.name);
    else bump(this.messages, message.type);
    if (event.disposition === "duplicate") this.duplicates++;
    if (event.disposition === "stale") this.stale++;
    if (message.type === "state_update") this.lastState = { ...message.data };
    const watched = this.watched;
    if (
      watched !== undefined &&
      clipsIn(message.data).some(
        (clip) => clip.clip_id === watched.clipId && clip.metadata.includes(watched.marker),
      )
    )
      bump(this.echoes, message.type === "unknown" ? message.name : message.type);
  }
}

/**
 * Counts how the session answered on its data channel: each reply as an
 * acknowledgement or by its message type, with how the correlator attributed
 * it. An enqueue acknowledged before its `clip_queued` shows `ack matched` and
 * `clip_queued late`; one answered only by a broadcast shows
 * `clip_queued unsolicited`.
 */
export const tallyReply = (counts: Record<string, number>, event: SessionEvent): void => {
  if (event._tag === "Model")
    bump(counts, `${event.kind === "ack" ? "ack" : event.type} ${event.correlation}`);
  else if (event._tag === "CommandError") bump(counts, `command_error ${event.correlation}`);
};

/**
 * Inspects the session every half second after termination was requested,
 * until the coordinator reports it terminal or gone, or the clock passes
 * `deadline`. Each distinct answer is recorded once, when first seen.
 */
export const terminationTrail = (
  coordinator: Pick<Reactor.Coordinator.Client, "inspect">,
  sessionId: string,
  origin: number,
  deadline: number,
) =>
  Effect.gen(function* () {
    const trail: { atMs: number; state: string }[] = [];
    let terminalMs: number | undefined;
    while (terminalMs === undefined && (yield* Clock.currentTimeMillis) < deadline) {
      const state = yield* coordinator.inspect(sessionId).pipe(
        Effect.map((inspection) => inspection.state),
        // A refused read names its status, so the trail shows what hosted
        // Reactor answers for a session that has ended; only 404 means gone.
        Effect.catch((error) =>
          Effect.succeed(
            error.reason._tag !== "Http" || error.reason.status === undefined
              ? `error:${error.reason._tag}`
              : error.reason.status === 404
                ? "gone"
                : `http:${error.reason.status}`,
          ),
        ),
      );
      const atMs = yield* elapsed(origin);
      if (trail.at(-1)?.state !== state) trail.push({ atMs, state });
      if (state === "gone" || terminal(state)) terminalMs = atMs;
      else yield* Effect.sleep("500 millis");
    }
    return { trail, terminalMs };
  });
