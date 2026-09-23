import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";

/** @type {(condition: unknown, message: string) => asserts condition} */
const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};
const crypto = Crypto.make({
  randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
  digest: (algorithm, bytes) =>
    Effect.promise(
      async () =>
        new Uint8Array(await globalThis.crypto.subtle.digest(algorithm, Uint8Array.from(bytes))),
    ),
});

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const pixels = new Uint8Array(64 * 64 * 4).fill(7);
      const metadata = Uint8Array.of(9, 8, 7);
      const samples = new Int16Array(480).fill(100);
      let rendererClosed = false;
      /** @type {Orchestration.LocalClipRecord | undefined} */
      let rendered;
      const handle = yield* Simulation.make({
        buildRatio: 0,
        buildFixedMs: 0,
        present: (record, _startedAt, sink) =>
          Effect.gen(function* () {
            rendered = record;
            yield* sink.video({
              _tag: "VideoFrame",
              track: "main_video",
              width: 64,
              height: 64,
              frameId: 9007199254740993n,
              timestampMicros: 1234567890123456n,
              data: pixels,
              metadata,
            });
            yield* sink.audio({
              _tag: "AudioFrame",
              track: "main_audio",
              sampleRate: 48000,
              channels: 1,
              samples,
            });
            pixels.fill(0);
            metadata.fill(0);
            samples.fill(0);
            yield* Effect.never;
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                rendererClosed = true;
              }),
            ),
          ),
      });
      const { engine, media } = handle;
      const request = new Orchestration.ClipRequest({
        prompt: "Offline installed-package simulation",
        references: [],
        durationSeconds: 5,
        metadata: { fixture: "installed-simulation" },
        sequence: { id: "installed-sequence", memberId: "first", final: true },
      });
      const invalid = yield* Effect.result(
        engine.enqueue(new Orchestration.ClipRequest({ ...request, durationSeconds: 0 })),
      );
      assert(
        invalid._tag === "Failure" && invalid.failure.context.outcome === "not-submitted",
        "simulation did not preserve local admission outcome",
      );
      const video = yield* Effect.forkScoped(media.video.pipe(Stream.runHead));
      const audio = yield* Effect.forkScoped(media.audio.pipe(Stream.runHead));
      yield* Effect.yieldNow;
      const accepted = yield* engine.enqueue(request);
      assert(
        typeof accepted === "string" && accepted.length > 0,
        "simulation did not return its admitted orchestration clip identity",
      );
      const frame = Option.getOrThrow(yield* Fiber.join(video));
      const pcm = Option.getOrThrow(yield* Fiber.join(audio));
      assert(
        frame._tag === "VideoFrame" &&
          frame.width === 64 &&
          frame.height === 64 &&
          frame.data.byteLength === 64 * 64 * 4,
        "simulation did not emit owned decoded BGRA",
      );
      assert(
        frame.frameId === 9007199254740993n && frame.timestampMicros === 1234567890123456n,
        "simulation lost 64-bit media identity",
      );
      assert(
        pcm._tag === "AudioFrame" &&
          pcm.sampleRate === 48_000 &&
          pcm.channels === 1 &&
          pcm.samples.length > 0,
        "simulation did not emit decoded PCM",
      );
      assert(
        frame.data.every((byte) => byte === 7) &&
          frame.metadata[0] === 9 &&
          pcm.samples.every((sample) => sample === 100),
        "simulation retained renderer-owned buffers",
      );
      const state = yield* engine.state;
      assert(
        Option.isSome(state.playing) && state.playing.value.clipId === accepted,
        "simulation omitted its playing clip from the production engine state",
      );
      assert(
        rendered?.clipId === accepted &&
          rendered.request.metadata.fixture === "installed-simulation",
        "simulation renderer lost local request annotation",
      );
      const beforeClose = Uint8Array.from(frame.data);
      const closed = yield* handle.close;
      assert(
        closed.sessions.length === 1 &&
          closed.sessions.every(
            (source) =>
              source.lease.localClosed &&
              source.lease.localErrors.length === 0 &&
              source.lease.allocation === "none",
          ),
        "simulation did not join its source cleanup",
      );
      assert(rendererClosed, "simulation returned before joining its running renderer");
      assert(
        (yield* handle.close) === closed,
        "simulation close did not preserve its cleanup report",
      );
      assert(
        (yield* handle.mediaState)._tag === "Closed",
        "simulation retained an open media lifetime",
      );
      assert(
        beforeClose.every((byte, index) => frame.data[index] === byte),
        "simulation media bytes changed during owner cleanup",
      );
      const afterClose = yield* Effect.result(engine.enqueue(request));
      assert(
        afterClose._tag === "Failure" && afterClose.failure.context.outcome === "not-submitted",
        "closed simulation admitted new work",
      );
    }),
  ).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.timeout(10_000)),
);
console.log(
  "simulation-smoke-ok production-engine=joined decoded-media=owned admission=not-submitted",
);
