import { Effect, Schema, Stream } from "effect";

/**
 * One joinable unit of a fragmented MP4 stream: the initialization segment
 * (`ftyp` + `moov`) and one fragment (`moof` + `mdat`). The encoder starts
 * every fragment on a keyframe, so a viewer can start at any segment by
 * receiving `init` first.
 */
export interface Segment {
  readonly init: Uint8Array;
  readonly fragment: Uint8Array;
}

export class Fmp4Error extends Schema.TaggedError<Fmp4Error>()("Fmp4Error", {
  message: Schema.String,
}) {}

interface Splitter {
  readonly pending: Uint8Array;
  readonly header: readonly Uint8Array[];
  readonly init: Uint8Array | undefined;
  readonly fragment: readonly Uint8Array[];
}

const concat = (parts: readonly Uint8Array[]): Uint8Array => {
  const out = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

const boxType = (bytes: Uint8Array, offset: number): string =>
  String.fromCharCode(...bytes.subarray(offset + 4, offset + 8));

/**
 * Splits an encoder's output into segments by its top-level boxes. Boxes are
 * copied out of the chunks, so a segment owns its bytes. Boxes other than
 * `ftyp`, `moov`, `moof` and `mdat` (such as the trailing `mfra`) are skipped.
 */
export const segments = <E, R>(
  bytes: Stream.Stream<Uint8Array, E, R>,
): Stream.Stream<Segment, E | Fmp4Error, R> =>
  bytes.pipe(
    Stream.mapAccumEffect(
      (): Splitter => ({
        pending: new Uint8Array(0),
        header: [],
        init: undefined,
        fragment: [],
      }),
      (state, chunk) =>
        Effect.gen(function* () {
          const buffer = concat([state.pending, chunk]);
          const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
          const out: Segment[] = [];
          let { header, init, fragment } = state;
          let offset = 0;
          while (buffer.length - offset >= 8) {
            // A 32-bit size of 0 or 1 marks a box to the end of the file or a
            // 64-bit size; neither occurs in ffmpeg's live fragments.
            const size = view.getUint32(offset);
            if (size < 8) return yield* new Fmp4Error({ message: `unsupported box size ${size}` });
            if (buffer.length - offset < size) break;
            const box = buffer.slice(offset, offset + size);
            const type = boxType(buffer, offset);
            offset += size;
            if (init === undefined && (type === "ftyp" || type === "moov")) {
              header = [...header, box];
              if (type === "moov") init = concat(header);
            } else if (type === "moof") {
              fragment = [box];
            } else if (type === "mdat" && init !== undefined && fragment.length === 1) {
              out.push({ init, fragment: concat([...fragment, box]) });
              fragment = [];
            }
          }
          const next: Splitter = { pending: buffer.slice(offset), header, init, fragment };
          return [next, out] as const;
        }),
    ),
  );
