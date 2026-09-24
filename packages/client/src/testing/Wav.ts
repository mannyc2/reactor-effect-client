export interface WavOptions {
  /** Samples a second, 16,000 by default. */
  readonly sampleRate?: number;
  /** 1 (mono, the default) or 2. */
  readonly channels?: number;
  /** The tone's pitch in hertz, 440 by default; 0 writes silence. */
  readonly frequency?: number;
}

/**
 * A 16-bit PCM WAV of a steady tone, `seconds` long: an audio reference whose
 * header gives its length and channels, so H3's local checks read them.
 */
export const wavBytes = (seconds: number, options: WavOptions = {}): Uint8Array => {
  const sampleRate = options.sampleRate ?? 16_000;
  const channels = options.channels ?? 1;
  const frequency = options.frequency ?? 440;
  const frames = Math.round(seconds * sampleRate);
  const data = frames * channels * 2;
  const bytes = new Uint8Array(44 + data);
  const view = new DataView(bytes.buffer);
  const text = (offset: number, value: string) => {
    for (let i = 0; i < value.length; i++) bytes[offset + i] = value.charCodeAt(i);
  };
  text(0, "RIFF");
  view.setUint32(4, 36 + data, true);
  text(8, "WAVE");
  text(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true); // PCM
  view.setUint16(22, channels, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * channels * 2, true);
  view.setUint16(32, channels * 2, true);
  view.setUint16(34, 16, true);
  text(36, "data");
  view.setUint32(40, data, true);
  for (let frame = 0; frame < frames; frame++) {
    const sample = Math.round(8_000 * Math.sin((2 * Math.PI * frequency * frame) / sampleRate));
    for (let channel = 0; channel < channels; channel++)
      view.setInt16(44 + (frame * channels + channel) * 2, sample, true);
  }
  return bytes;
};
