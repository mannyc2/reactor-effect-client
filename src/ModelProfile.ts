/**
 * Provider facts the rest of the package must not hard-code. The simulator and
 * the real adapter select the same profile, so what the scheduler learns about
 * durations and limits is the same in tests and in production.
 *
 * The H3 profile below is the user's stated model contract plus the provider
 * details recorded in the handoff. Where the running deployment can be asked
 * (`state_update` capacities, the session's OpenAPI document), the adapter
 * prefers the deployment's answer and rejects an incompatible one.
 */

export type CanvasAspect = "16:9" | "1:1" | "9:16" | "4:3"

export interface ModelProfile {
  /** Wire model name passed to the SDK. */
  readonly modelName: string
  readonly fps: number
  /** Legal output lengths are `minFrames + k * frameStep`, `k >= 0`, up to `maxFrames`. */
  readonly minFrames: number
  readonly maxFrames: number
  readonly frameStep: number
  /** Requested lengths the provider accepts before aligning them up to the grid. */
  readonly requestSeconds: { readonly min: number; readonly max: number }
  readonly prompt: {
    readonly maxChars: number
    /** Provider-side text budget. It is not a local hard limit because the provider tokenizer is authoritative. */
    readonly maxTokens: number
    /** Conservative characters-per-token used to estimate the token bound locally. */
    readonly charsPerTokenEstimate: number
  }
  readonly references: {
    readonly min: number
    readonly max: number
    readonly maxBytes: number
    readonly maxPixels: number
    /** width / height must lie within [minAspect, maxAspect]. */
    readonly minAspect: number
    readonly maxAspect: number
    readonly mimeTypes: ReadonlyArray<string>
  }
  readonly metadataMaxChars: number
  readonly canvases: ReadonlyArray<{ readonly aspect: CanvasAspect; readonly width: number; readonly height: number }>
  /** Expected queue capacities. The adapter verifies the session's own values. */
  readonly expectedCapacities: { readonly generation: number; readonly playout: number }
  /** Only this many recently generated clips remain valid `continue_from` targets. */
  readonly continuationWindow: number
  readonly audio: { readonly sampleRate: number; readonly channels: number }
  readonly tracks: { readonly video: string; readonly audio: string }
}

export const h3ReferenceTurboRealtime: ModelProfile = {
  modelName: "reactor/h3-reference-to-video-turbo-realtime",
  fps: 24,
  minFrames: 124,
  maxFrames: 362,
  frameStep: 17,
  requestSeconds: { min: 5.0, max: 15.084 },
  // No provider character limit; the text budget is ~2,000 tokens ("roughly 8,000 characters of English
  // prose") and exceeding it fails the clip at build. maxChars is our own sanity bound.
  prompt: { maxChars: 12_000, maxTokens: 2_000, charsPerTokenEstimate: 3.5 },
  references: {
    min: 1,
    max: 9,
    maxBytes: 25 * 1024 * 1024,
    maxPixels: 25_000_000,
    minAspect: 1 / 4,
    maxAspect: 4,
    mimeTypes: ["image/jpeg", "image/png", "image/webp"]
  },
  metadataMaxChars: 2_000,
  canvases: [
    { aspect: "16:9", width: 1344, height: 768 },
    { aspect: "1:1", width: 768, height: 768 },
    { aspect: "9:16", width: 768, height: 1344 },
    { aspect: "4:3", width: 1024, height: 768 }
  ],
  expectedCapacities: { generation: 20, playout: 10 },
  continuationWindow: 8,
  audio: { sampleRate: 48_000, channels: 1 },
  tracks: { video: "main_video", audio: "main_audio" }
}

export const minSeconds = (p: ModelProfile) => p.minFrames / p.fps
export const maxSeconds = (p: ModelProfile) => p.maxFrames / p.fps

/** Snap a requested length upward onto the frame grid, exactly like the provider does. Requires a finite input. */
export const alignFrames = (p: ModelProfile, seconds: number): number => {
  if (!Number.isFinite(seconds)) throw new RangeError("clip length must be finite")
  const frames = Math.ceil(seconds * p.fps - 1e-6)
  const steps = Math.ceil(Math.max(0, frames - p.minFrames) / p.frameStep)
  return Math.min(p.maxFrames, p.minFrames + steps * p.frameStep)
}

export const alignSecondsTo = (p: ModelProfile, seconds: number): number => alignFrames(p, seconds) / p.fps

/** Clamp into the accepted request range, then align. Always yields a length the provider accepts. */
export const clampSecondsTo = (p: ModelProfile, seconds: number): number =>
  alignSecondsTo(p, Math.min(p.requestSeconds.max, Math.max(p.requestSeconds.min, Number.isFinite(seconds) ? seconds : p.requestSeconds.min)))

/** True when `seconds` is a length the provider accepts as a request. */
export const isRequestableSeconds = (p: ModelProfile, seconds: number): boolean =>
  Number.isFinite(seconds) && seconds >= p.requestSeconds.min && seconds <= p.requestSeconds.max

/** Advisory token-count estimate for caller budgeting. The adapter does not reject from this estimate; the provider tokenizer is authoritative. */
export const estimateTokens = (p: ModelProfile, prompt: string): number =>
  Math.ceil(prompt.length / p.prompt.charsPerTokenEstimate)

export const canvasSize = (p: ModelProfile, aspect: CanvasAspect) => p.canvases.find((c) => c.aspect === aspect)
