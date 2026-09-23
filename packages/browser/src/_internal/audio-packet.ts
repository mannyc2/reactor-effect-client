import { ReactorError } from "reactor-effect-client";
import { record, finite } from "reactor-effect-client/host";
/** A Web Audio rendering-clock block, not an RTP or capture timestamp. */
export interface WebAudioSample {
  readonly format: "f32-planar";
  readonly planes: readonly Float32Array<ArrayBuffer>[];
  readonly sampleRate: number;
  readonly frames: number;
  readonly timestampUs: number;
  readonly contextFrame: number;
  readonly skippedFrames: number;
  readonly clock: "audio-context-render";
}
export const decodeAudioPacket = (input: unknown, maxBytes: number): WebAudioSample => {
  const p = record(input, "AudioWorklet packet");
  if (p.type !== "pcm") throw new ReactorError("Protocol", "unexpected AudioWorklet packet");
  const frames = finite(p.frames, "PCM frames");
  const sampleRate = finite(p.sampleRate, "sample rate");
  if (
    !Number.isSafeInteger(frames) ||
    frames < 1 ||
    frames > 16_384 ||
    !Number.isSafeInteger(sampleRate) ||
    sampleRate < 1 ||
    sampleRate > 384_000
  ) {
    throw new ReactorError("Protocol", "invalid PCM frame count or sample rate");
  }
  const frame = finite(p.frame, "context frame"),
    skipped = finite(p.skipped, "skipped frames");
  if (!Number.isSafeInteger(frame) || frame < 0 || !Number.isSafeInteger(skipped) || skipped < 0)
    throw new ReactorError("Protocol", "invalid render-clock frame counter");
  if (!Array.isArray(p.planes) || p.planes.length < 1 || p.planes.length > 32)
    throw new ReactorError("Protocol", "invalid PCM channel count");
  if (p.planes.length * frames * 4 > maxBytes)
    throw new ReactorError("Overflow", "PCM sample byte bound exceeded");
  const planes = p.planes.map((plane: unknown) => {
    if (!(plane instanceof ArrayBuffer) || plane.byteLength !== frames * 4)
      throw new ReactorError("Protocol", "PCM plane length/type mismatch");
    const data = new Float32Array(plane);
    if (!data.every(Number.isFinite)) throw new ReactorError("Protocol", "nonfinite PCM sample");
    return data;
  });
  return Object.freeze({
    format: "f32-planar",
    planes: Object.freeze(planes),
    frames,
    sampleRate,
    timestampUs: (frame * 1_000_000) / sampleRate,
    contextFrame: frame,
    skippedFrames: skipped,
    clock: "audio-context-render",
  });
};
