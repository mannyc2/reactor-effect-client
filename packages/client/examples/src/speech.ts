import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Clock, Config, Console, Effect, FileSystem, Layer, Path, Stream } from "effect";
import { Model } from "effect/ai";
import { FetchHttpClient } from "effect/http";
import { CartesiaSpeech, SpeechModel } from "reactor-effect-client";

const Speech = CartesiaSpeech.layerConfig.pipe(Layer.provide(FetchHttpClient.layer));

const program = Effect.gen(function* () {
  const speech = yield* SpeechModel.SpeechModel;
  const provider = yield* Model.ProviderName;
  const name = yield* Model.ModelName;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const text = yield* Config.String("SPEECH_TEXT").pipe(
    Config.withDefault("Welcome to our live programme. Here is what is happening next."),
  );
  const output = yield* Config.String("SPEECH_OUTPUT").pipe(
    Config.withDefault(".check/speech.pcm"),
  );
  yield* fs.makeDirectory(path.dirname(output), { recursive: true });
  const started = yield* Clock.monotonicTimeNanos;
  yield* speech.stream({ text }).pipe(
    Stream.tap((frame) =>
      frame.sequence === 0n
        ? Effect.flatMap(Clock.monotonicTimeNanos, (now) =>
            Console.log(
              `${provider}/${name}: first audio in ${Number(now - started) / 1e6} ms; ${frame.sampleRate} Hz, ${frame.channels} channel(s)`,
            ),
          )
        : Effect.void,
    ),
    Stream.map((frame) => {
      const bytes = new Uint8Array(frame.samples.length * 2);
      const view = new DataView(bytes.buffer);
      for (const [index, sample] of frame.samples.entries()) view.setInt16(index * 2, sample, true);
      return bytes;
    }),
    Stream.run(fs.sink(`${output}.partial`)),
    Effect.timeout("2 minutes"),
  );
  yield* fs.rename(`${output}.partial`, output);
  yield* Console.log(`Saved signed 16-bit little-endian PCM to ${output}`);
});

program.pipe(
  // The application's entry point selects the speech provider and host services.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Layer.mergeAll(Speech, NodeServices.layer)),
  NodeRuntime.runMain,
);
