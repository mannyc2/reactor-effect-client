/** Documentation revision selected by this adapter, not an assertion about a server version. */
export const modelName = "reactor/h3-reference-to-video-turbo-realtime" as const;
export const documentedVersion = "0.5.5" as const;
export const source =
  "https://docs.reactor.inc/model-api-reference/h3-reference-to-video-turbo-realtime/schema";
export const requestSeconds = Object.freeze({ min: 5, max: 15.084 });
export const canvases = Object.freeze({
  "16:9": Object.freeze({ width: 1344, height: 768 }),
  "1:1": Object.freeze({ width: 768, height: 768 }),
  "9:16": Object.freeze({ width: 768, height: 1344 }),
  "4:3": Object.freeze({ width: 1024, height: 768 }),
});
/** SDK image safety bounds; byte validation does not replace the model's image decoder. */
export const referenceLimits = Object.freeze({
  maxImages: 9,
  maxBytes: 25 * 1024 * 1024,
  maxPixels: 25_000_000,
  minAspect: 0.25,
  maxAspect: 4,
});

/** Shared H3 timing data and explicit defaults used by offline orchestration. */
export type CanvasAspect = "16:9" | "1:1" | "9:16" | "4:3";

export interface ModelProfile {
  /** Wire model name passed to the SDK. */
  readonly modelName: string;
  readonly fps: number;
  /** Legal output lengths are `minFrames + k * frameStep`, `k >= 0`, up to `maxFrames`. */
  readonly minFrames: number;
  readonly maxFrames: number;
  readonly frameStep: number;
  /** Requested lengths the provider accepts before aligning them up to the grid. */
  readonly requestSeconds: { readonly min: number; readonly max: number };
  readonly prompt: {
    readonly maxChars: number;
    /** Provider-side text budget. It is not a local hard limit because the provider tokenizer is authoritative. */
    readonly maxTokens: number;
    /** Conservative characters-per-token used to estimate the token bound locally. */
    readonly charsPerTokenEstimate: number;
  };
  readonly references: {
    readonly min: number;
    readonly max: number;
    readonly maxBytes: number;
    readonly maxPixels: number;
    /** width / height must lie within [minAspect, maxAspect]. */
    readonly minAspect: number;
    readonly maxAspect: number;
    readonly mimeTypes: ReadonlyArray<string>;
  };
  readonly metadataMaxChars: number;
  readonly canvases: ReadonlyArray<{
    readonly aspect: CanvasAspect;
    readonly width: number;
    readonly height: number;
  }>;
  /** Defaults for offline simulation; live provider snapshots remain authoritative. */
  readonly expectedCapacities: { readonly generation: number; readonly playout: number };
  /** Bounded continuation hints for orchestration; not a claim that the deployment retains a clip. */
  readonly continuationWindow: number;
  readonly audio: { readonly sampleRate: number; readonly channels: number };
  readonly tracks: { readonly video: string; readonly audio: string };
}

export const h3ReferenceTurboRealtime: ModelProfile = {
  modelName,
  fps: 24,
  minFrames: 124,
  maxFrames: 362,
  frameStep: 17,
  requestSeconds,
  // No provider character limit; the text budget is ~2,000 tokens ("roughly 8,000 characters of English
  // prose") and exceeding it fails the clip at build. maxChars is our own sanity bound.
  prompt: { maxChars: 12_000, maxTokens: 2_000, charsPerTokenEstimate: 3.5 },
  references: {
    min: 0,
    max: referenceLimits.maxImages,
    maxBytes: referenceLimits.maxBytes,
    maxPixels: referenceLimits.maxPixels,
    minAspect: referenceLimits.minAspect,
    maxAspect: referenceLimits.maxAspect,
    mimeTypes: ["image/jpeg", "image/png", "image/webp"],
  },
  metadataMaxChars: 2_000,
  canvases: Object.entries(canvases).map(([aspect, size]) => ({
    aspect: aspect as CanvasAspect,
    ...size,
  })),
  expectedCapacities: { generation: 20, playout: 10 },
  continuationWindow: 8,
  audio: { sampleRate: 48_000, channels: 1 },
  tracks: { video: "main_video", audio: "main_audio" },
};

export const minSeconds = (p: ModelProfile) => p.minFrames / p.fps;
export const maxSeconds = (p: ModelProfile) => p.maxFrames / p.fps;

/** Snap upward on this profile's frame grid. Actual admitted lengths come from Clip.frames and Clip.seconds. */
export const alignFrames = (p: ModelProfile, seconds: number): number => {
  if (!Number.isFinite(seconds)) throw new RangeError("clip length must be finite");
  const frames = Math.ceil(seconds * p.fps - 1e-6);
  const steps = Math.ceil(Math.max(0, frames - p.minFrames) / p.frameStep);
  return Math.min(p.maxFrames, p.minFrames + steps * p.frameStep);
};

export const alignSecondsTo = (p: ModelProfile, seconds: number): number =>
  alignFrames(p, seconds) / p.fps;

/** Clamp into the accepted request range, then align. Always yields a length the provider accepts. */
export const clampSecondsTo = (p: ModelProfile, seconds: number): number =>
  alignSecondsTo(
    p,
    Math.min(
      p.requestSeconds.max,
      Math.max(p.requestSeconds.min, Number.isFinite(seconds) ? seconds : p.requestSeconds.min),
    ),
  );

/** True when `seconds` is a length the provider accepts as a request. */
export const isRequestableSeconds = (p: ModelProfile, seconds: number): boolean =>
  Number.isFinite(seconds) && seconds >= p.requestSeconds.min && seconds <= p.requestSeconds.max;

/** Advisory token-count estimate for caller budgeting. The adapter does not reject from this estimate; the provider tokenizer is authoritative. */
export const estimateTokens = (p: ModelProfile, prompt: string): number =>
  Math.ceil(prompt.length / p.prompt.charsPerTokenEstimate);

export const canvasSize = (p: ModelProfile, aspect: CanvasAspect) =>
  p.canvases.find((c) => c.aspect === aspect);
