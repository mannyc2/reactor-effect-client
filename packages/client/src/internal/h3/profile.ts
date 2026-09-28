/** The H3 Reference Turbo Realtime model: its documented limits and frame grid. */
import { dual } from "effect/Function";

/** Documentation revision selected by this adapter, not an assertion about a server version. */
export const modelName = "reactor/h3-reference-to-video-turbo-realtime" as const;
export const documentedVersion = "0.5.5" as const;
export const source =
  "https://docs.reactor.inc/model-api-reference/h3-reference-to-video-turbo-realtime/schema";
export const requestSeconds = { min: 5, max: 15.084 } as const;
export const canvases = {
  "16:9": { width: 1344, height: 768 },
  "1:1": { width: 768, height: 768 },
  "9:16": { width: 768, height: 1344 },
  "4:3": { width: 1024, height: 768 },
} as const;
export type CanvasAspect = keyof typeof canvases;

/** SDK image safety bounds; byte validation does not replace the model's image decoder. */
export const referenceLimits = {
  maxImages: 9,
  maxBytes: 25 * 1024 * 1024,
  maxPixels: 25_000_000,
  minAspect: 0.25,
  maxAspect: 4,
} as const;

/**
 * H3's audio reference bounds: at most three clips, or two when the clip
 * continues another, whose soundtrack takes the third; at most twelve
 * references in total, counting images, audio and a continuation; each 2–15 s
 * long, mono or stereo, and at most 25 MiB.
 */
export const audioReferenceLimits = {
  maxAudio: 3,
  maxAudioWithContinuation: 2,
  maxTotal: 12,
  maxBytes: 25 * 1024 * 1024,
  minSeconds: 2,
  maxSeconds: 15,
  maxChannels: 2,
} as const;

export const metadataMaxChars = 2_000;
export const imageMimeTypes = ["image/jpeg", "image/png", "image/webp"] as const;

/**
 * The audio formats H3 documents (WAV, MP3, AAC/M4A, OGG/Opus, FLAC and WebM),
 * by the MIME type an upload of each carries. Bytes are identified by their
 * container, which names the first type of each format; an existing upload
 * may carry any of them.
 */
export const audioMimeTypes = [
  "audio/wav",
  "audio/mpeg",
  "audio/aac",
  "audio/mp4",
  "audio/ogg",
  "audio/flac",
  "audio/webm",
  "audio/x-wav",
  "audio/wave",
  "audio/x-m4a",
  "audio/opus",
  "audio/x-flac",
] as const;

export interface ModelProfile {
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
    /** The provider's text budget; its tokenizer, not this estimate, is authoritative. */
    readonly maxTokens: number;
    /** Conservative characters per token for a local estimate. */
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
  /** Audio references the model takes beside its images; absent for a model that takes none. */
  readonly audioReferences?: {
    readonly max: number;
    /** A continued clip spends one audio slot on the previous clip's soundtrack. */
    readonly maxWithContinuation: number;
    /** Images, audio and a continuation together. */
    readonly maxTotal: number;
    readonly maxBytes: number;
    readonly minSeconds: number;
    readonly maxSeconds: number;
    readonly mimeTypes: ReadonlyArray<string>;
  };
  readonly metadataMaxChars: number;
  readonly canvases: ReadonlyArray<{
    readonly aspect: CanvasAspect;
    readonly width: number;
    readonly height: number;
  }>;
  /** The queue capacities H3 documents; live state snapshots are authoritative. */
  readonly expectedCapacities: { readonly generation: number; readonly playout: number };
  /** How many recent clips orchestration offers as continuations. */
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
  // The provider sets no character limit; its budget is about 2,000 tokens, and
  // a longer prompt fails the clip at build. maxChars is this SDK's own bound.
  prompt: { maxChars: 12_000, maxTokens: 2_000, charsPerTokenEstimate: 3.5 },
  references: {
    min: 0,
    max: referenceLimits.maxImages,
    maxBytes: referenceLimits.maxBytes,
    maxPixels: referenceLimits.maxPixels,
    minAspect: referenceLimits.minAspect,
    maxAspect: referenceLimits.maxAspect,
    mimeTypes: imageMimeTypes,
  },
  audioReferences: {
    max: audioReferenceLimits.maxAudio,
    maxWithContinuation: audioReferenceLimits.maxAudioWithContinuation,
    maxTotal: audioReferenceLimits.maxTotal,
    maxBytes: audioReferenceLimits.maxBytes,
    minSeconds: audioReferenceLimits.minSeconds,
    maxSeconds: audioReferenceLimits.maxSeconds,
    mimeTypes: audioMimeTypes,
  },
  metadataMaxChars,
  canvases: (["16:9", "1:1", "9:16", "4:3"] as const).map((aspect) => ({
    aspect,
    ...canvases[aspect],
  })),
  expectedCapacities: { generation: 20, playout: 10 },
  continuationWindow: 8,
  audio: { sampleRate: 48_000, channels: 1 },
  tracks: { video: "main_video", audio: "main_audio" },
};

export const minSeconds = (profile: ModelProfile): number => profile.minFrames / profile.fps;
export const maxSeconds = (profile: ModelProfile): number => profile.maxFrames / profile.fps;

/**
 * The frame count a request of `seconds` produces: rounded up onto the
 * profile's frame grid. A clip's own `frames` and `seconds` are authoritative.
 */
export const alignFrames: {
  (seconds: number): (profile: ModelProfile) => number;
  (profile: ModelProfile, seconds: number): number;
} = dual(2, (profile: ModelProfile, seconds: number): number => {
  const frames = Number.isFinite(seconds) ? Math.ceil(seconds * profile.fps - 1e-6) : 0;
  const steps = Math.ceil(Math.max(0, frames - profile.minFrames) / profile.frameStep);
  return Math.min(profile.maxFrames, profile.minFrames + steps * profile.frameStep);
});

export const alignSecondsTo: {
  (seconds: number): (profile: ModelProfile) => number;
  (profile: ModelProfile, seconds: number): number;
} = dual(
  2,
  (profile: ModelProfile, seconds: number): number => alignFrames(profile, seconds) / profile.fps,
);

/** Clamps into the accepted request range, then aligns: always a length the provider accepts. */
export const clampSecondsTo: {
  (seconds: number): (profile: ModelProfile) => number;
  (profile: ModelProfile, seconds: number): number;
} = dual(2, (profile: ModelProfile, seconds: number): number => {
  const { min, max } = profile.requestSeconds;
  return alignSecondsTo(
    profile,
    Math.min(max, Math.max(min, Number.isFinite(seconds) ? seconds : min)),
  );
});

/** True when `seconds` is a length the provider accepts as a request. */
export const isRequestableSeconds: {
  (seconds: number): (profile: ModelProfile) => boolean;
  (profile: ModelProfile, seconds: number): boolean;
} = dual(
  2,
  (profile: ModelProfile, seconds: number): boolean =>
    Number.isFinite(seconds) &&
    seconds >= profile.requestSeconds.min &&
    seconds <= profile.requestSeconds.max,
);

/** An advisory token estimate for budgeting; the provider's tokenizer decides. */
export const estimateTokens: {
  (prompt: string): (profile: ModelProfile) => number;
  (profile: ModelProfile, prompt: string): number;
} = dual(2, (profile: ModelProfile, prompt: string): number =>
  Math.ceil(prompt.length / profile.prompt.charsPerTokenEstimate),
);
