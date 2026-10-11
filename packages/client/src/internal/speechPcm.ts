import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { AudioFrame } from "../Media.js";
import { SpeechError } from "../SpeechModel.js";

interface DecoderState {
  readonly pending: Uint8Array;
  readonly sequence: bigint;
}

/** Both providers return interleaved signed 16-bit little-endian PCM across arbitrary chunks. */
export const decode = (format: { readonly sampleRate: number; readonly channels: number }) => {
  const step = Effect.fnUntraced(function* (
    state: DecoderState,
    chunk: Uint8Array | undefined,
  ): Effect.fn.Return<readonly [DecoderState, ReadonlyArray<AudioFrame>], SpeechError> {
    if (chunk === undefined) {
      if (state.pending.byteLength !== 0 || state.sequence === 0n)
        return yield* SpeechError.make({
          reason: {
            _tag: "InvalidAudio",
            message: "Speech response is empty or ends with an incomplete PCM frame",
          },
          outcome: "replied",
        });
      return [state, []];
    }
    const bytes = new Uint8Array(state.pending.byteLength + chunk.byteLength);
    bytes.set(state.pending);
    bytes.set(chunk, state.pending.byteLength);
    const frameBytes = format.channels * 2;
    const completeBytes = bytes.byteLength - (bytes.byteLength % frameBytes);
    const samples = new Int16Array(completeBytes / 2);
    const view = new DataView(bytes.buffer);
    for (let index = 0; index < samples.length; index++)
      samples[index] = view.getInt16(index * 2, true);
    const pending = bytes.slice(completeBytes);
    if (samples.length === 0) return [{ ...state, pending }, []];
    return [
      { pending, sequence: state.sequence + 1n },
      [{ _tag: "AudioFrame", track: "speech", ...format, sequence: state.sequence, samples }],
    ];
  });
  return <E, R>(stream: Stream.Stream<Uint8Array, E, R>) =>
    stream.pipe(
      Stream.concat(Stream.succeed(undefined)),
      Stream.mapAccumEffect(
        (): DecoderState => ({ pending: new Uint8Array(), sequence: 0n }),
        step,
      ),
    );
};
