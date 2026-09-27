/** Synthetic media: a frame carries its clip's ordinal and its own index in its first two pixels. */
import { h3ReferenceTurboRealtime as profile } from "../../h3/profile.js";

/** One audio block, as libwebrtc hosts deliver them. */
export const audioBlockMs = 10;
export const samplesPerBlock = (profile.audio.sampleRate * audioBlockMs) / 1000;

const ordinalOf = (clipId: string): number => Number.parseInt(clipId.slice(-12), 16);

export type Picture = "live" | "absent" | "black" | "frozen";

export const render = ({
  clipId,
  index,
  picture,
  width,
  height,
}: {
  readonly clipId: string;
  readonly index: number;
  readonly picture: Picture;
  readonly width: number;
  readonly height: number;
}): Uint8Array<ArrayBuffer> => {
  const data = new Uint8Array(width * height * 4);
  const ordinal = ordinalOf(clipId);
  const [blue, green, red] =
    picture === "black" ? [0, 0, 0] : [(ordinal * 67) % 256, (ordinal * 131) % 256, 96];
  for (let pixel = 0; pixel < width * height; pixel++) data.set([blue, green, red, 255], pixel * 4);
  if (picture === "black") return data;
  const frame = picture === "frozen" ? 0 : index;
  data.set([ordinal & 0xff, (ordinal >> 8) & 0xff, (ordinal >> 16) & 0xff, 255], 0);
  data.set([frame & 0xff, (frame >> 8) & 0xff, 0, 255], 4);
  return data;
};

/** The clip ordinal and frame index `render` wrote, or undefined for a black frame. */
export const decode = (
  data: Uint8Array,
): { readonly ordinal: number; readonly index: number } | undefined => {
  const ordinal = (data[0] ?? 0) | ((data[1] ?? 0) << 8) | ((data[2] ?? 0) << 16);
  return ordinal === 0 ? undefined : { ordinal, index: (data[4] ?? 0) | ((data[5] ?? 0) << 8) };
};

/** A block of the clip's tone, continuous from sample `first`. */
export const tone = ({ clipId, first }: { readonly clipId: string; readonly first: number }) => {
  const samples = new Int16Array(samplesPerBlock * profile.audio.channels);
  const frequency = 220 + ((ordinalOf(clipId) * 37) % 440);
  for (let index = 0; index < samplesPerBlock; index++)
    samples.fill(
      Math.round(
        6000 * Math.sin((2 * Math.PI * frequency * (first + index)) / profile.audio.sampleRate),
      ),
      index * profile.audio.channels,
      (index + 1) * profile.audio.channels,
    );
  return samples;
};
