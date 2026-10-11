/**
 * The character page: one voice, heard on its own or spoken by a Live2D
 * character. Both modes use the same speech model and the same speaker; the
 * character only adds a picture that follows what the speaker plays. One
 * utterance plays at a time, and a new one, or Stop, interrupts it.
 */
import { Cause, Clock, Effect, Fiber, FiberHandle, Layer, Schema, Stream } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { HttpApiClient } from "effect/http-api";
import type { AudioFrame } from "reactor-effect-client/Media";
import { SpeechModel } from "reactor-effect-client/SpeechModel";
import { Api } from "./Api.ts";
import type { Character } from "./Api.ts";
import * as Avatar from "./Avatar.ts";
import * as Speaker from "./Speaker.ts";
import * as Speech from "./Speech.ts";

/** The page's element with this id, checked to be the kind the page uses it as. */
const element = <E extends HTMLElement>(id: string, kind: new () => E): E => {
  const found = document.getElementById(id);
  if (!(found instanceof kind)) throw new Error(`the page has no ${kind.name} #${id}`);
  return found;
};

/** The elements the page reads or listens to; module scripts run once the page is parsed. */
const ui = {
  canvas: element("character", HTMLCanvasElement),
  setup: element("setup", HTMLElement),
  level: element("level", HTMLElement),
  avatar: element("avatar", HTMLButtonElement),
  audioOnly: element("audio-only", HTMLButtonElement),
  text: element("text", HTMLTextAreaElement),
  say: element("say", HTMLButtonElement),
  stop: element("stop", HTMLButtonElement),
  file: element("file", HTMLInputElement),
  speech: element("speech", HTMLElement),
  state: element("state", HTMLElement),
  log: element("log", HTMLOListElement),
};

const note = (text: string) =>
  Effect.sync(() => {
    const item = document.createElement("li");
    item.textContent = text;
    ui.log.prepend(item);
  });

/** A local audio file that could not be read as audio. */
class AudioFileError extends Schema.TaggedError<AudioFileError>()("AudioFileError", {
  cause: Schema.Defect(),
}) {}

/** Blocks of about 43 ms, as a streaming provider would deliver them. */
const blockFrames = 2048;

/**
 * A local file's audio as the speaker's blocks, so the page can be heard and
 * watched without a provider. The browser decodes the whole file first.
 */
const fileFrames = (file: File): Stream.Stream<AudioFrame, AudioFileError> =>
  Stream.unwrap(
    Effect.tryPromise({
      try: () => file.arrayBuffer(),
      catch: (cause) => AudioFileError.make({ cause }),
    }).pipe(
      Effect.flatMap((bytes) =>
        Effect.tryPromise({
          try: () => new OfflineAudioContext(1, 1, 48000).decodeAudioData(bytes),
          catch: (cause) => AudioFileError.make({ cause }),
        }),
      ),
      Effect.map((audio) => {
        const channels = audio.numberOfChannels;
        const data = Array.from({ length: channels }, (_, channel) =>
          audio.getChannelData(channel),
        );
        const blocks: Array<AudioFrame> = [];
        for (let start = 0; start < audio.length; start += blockFrames) {
          const length = Math.min(blockFrames, audio.length - start);
          const samples = new Int16Array(length * channels);
          for (let index = 0; index < length; index++)
            for (let channel = 0; channel < channels; channel++) {
              const sample = data[channel]?.[start + index] ?? 0;
              samples[index * channels + channel] = Math.round(
                Math.max(-1, Math.min(1, sample)) * 32767,
              );
            }
          blocks.push({
            _tag: "AudioFrame",
            track: file.name,
            sampleRate: audio.sampleRate,
            channels,
            sequence: BigInt(blocks.length),
            samples,
          });
        }
        return Stream.fromIterable(blocks);
      }),
    ),
  );

const page = Effect.gen(function* () {
  const api = yield* HttpApiClient.make(Api);
  const speech = yield* SpeechModel;
  const speaker = yield* Speaker.make;
  const setup = yield* api.character().pipe(
    Effect.orElseSucceed((): Character => ({
      speech: { _tag: "Unavailable" },
      avatar: { _tag: "Unavailable", missing: "a server that answers /api/character" },
    })),
  );

  const run = Effect.runForkWith(yield* Effect.context<never>());
  /**
   * Runs `effect` in place of the handle's fiber once that one has ended, so
   * the line or character it replaces is silenced and released first.
   */
  const replace = (handle: FiberHandle.FiberHandle<void, never>, effect: Effect.Effect<void>) =>
    run(Effect.andThen(FiberHandle.clear(handle), FiberHandle.run(handle, effect)));
  /** The line playing now. */
  const voice = yield* FiberHandle.make<void, never>();
  /** The character, or nothing in audio-only mode. */
  const display = yield* FiberHandle.make<void, never>();

  const utter = <E extends { readonly _tag: string }>(
    label: string,
    frames: Stream.Stream<AudioFrame, E>,
    describe: (error: E) => string,
  ) =>
    Effect.gen(function* () {
      ui.state.textContent = "speaking";
      const started = yield* Clock.monotonicTimeNanos;
      yield* speaker.play(
        frames.pipe(
          Stream.tap((block) =>
            block.sequence === 0n
              ? Effect.flatMap(Clock.monotonicTimeNanos, (now) =>
                  note(`${label}: first audio after ${String((now - started) / 1_000_000n)} ms`),
                )
              : Effect.void,
          ),
        ),
      );
      yield* note(`${label}: played to its end`);
    }).pipe(
      Effect.catch((error: E) => note(`${label}: failed, ${describe(error)}`)),
      Effect.onInterrupt(() => note(`${label}: interrupted`)),
      Effect.ensuring(Effect.sync(() => (ui.state.textContent = "idle"))),
    );

  const meter = Avatar.frames.pipe(
    Stream.runForEach(() =>
      Effect.map(speaker.level, (level) => {
        ui.level.style.width = `${String(Math.min(100, level * 400))}%`;
      }),
    ),
  );

  const audioOnly = Effect.sync(() => {
    ui.canvas.hidden = true;
    ui.avatar.classList.remove("on");
    ui.audioOnly.classList.add("on");
  }).pipe(Effect.andThen(Effect.never));

  const avatar = (model: string) =>
    Effect.gen(function* () {
      ui.canvas.hidden = false;
      ui.avatar.classList.add("on");
      ui.audioOnly.classList.remove("on");
      yield* note("avatar: loading the model");
      return yield* Avatar.show(ui.canvas, model, speaker.level);
    }).pipe(
      Effect.scoped,
      Effect.catchCause((cause) =>
        note(`avatar: ${Cause.pretty(cause).split("\n")[0] ?? "failed"}`).pipe(
          Effect.andThen(audioOnly),
        ),
      ),
      Effect.onInterrupt(() => note("avatar: released")),
    );

  switch (setup.speech._tag) {
    case "Available":
      ui.speech.textContent = `Speech: ${setup.speech.model} via ${setup.speech.provider}.`;
      break;
    case "Unavailable":
      ui.speech.textContent =
        "Speech isn't configured yet. You can try the character with a local audio file.";
      ui.say.disabled = true;
      break;
  }
  switch (setup.avatar._tag) {
    case "Available": {
      const { model } = setup.avatar;
      ui.setup.textContent = "";
      ui.avatar.addEventListener("click", () => replace(display, avatar(model)));
      replace(display, avatar(model));
      break;
    }
    case "Unavailable":
      ui.setup.textContent = `The avatar needs ${setup.avatar.missing}. The voice plays on its own meanwhile.`;
      ui.avatar.disabled = true;
      replace(display, audioOnly);
      break;
  }
  ui.audioOnly.addEventListener("click", () => replace(display, audioOnly));
  ui.say.addEventListener("click", () => {
    const text = ui.text.value.trim();
    if (text === "") return;
    replace(
      voice,
      utter(
        "speech",
        speech.stream({ text }),
        (error) => `${error.reason._tag}, outcome ${error.outcome}`,
      ),
    );
  });
  ui.stop.addEventListener("click", () => {
    run(FiberHandle.clear(voice));
  });
  ui.file.addEventListener("change", () => {
    const file = ui.file.files?.[0];
    if (file === undefined) return;
    ui.file.value = "";
    replace(
      voice,
      utter(file.name, fileFrames(file), () => "not readable as audio"),
    );
  });
  yield* note("page: ready");
  yield* meter;
});

const running = Effect.runFork(
  page.pipe(
    Effect.scoped,
    // The page's entry point chooses its speech model and HTTP client.
    // @effect-diagnostics-next-line strictEffectProvide:off
    Effect.provide(Speech.layer.pipe(Layer.provideMerge(FetchHttpClient.layer))),
  ),
);

// Leaving the page for good stops its sound and releases the character.
window.addEventListener("pagehide", (event) => {
  if (!event.persisted) Effect.runFork(Fiber.interrupt(running));
});
