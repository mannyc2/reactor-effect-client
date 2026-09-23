/** Controlled native-host contracts; these do not establish browser AudioData support. */
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import { audioSamples } from "../src/_internal/media.js";
import { FakeTrack } from "reactor-effect-test-kit";
import { assert, equal, eventually, failure, run, test, withGlobals } from "./harness.js";

class AudioBlock {
  readonly timestamp = -123456;
  readonly numberOfFrames = 4;
  readonly numberOfChannels = 2;
  readonly sampleRate = 48000;
  readonly data = new Float32Array([0.25, -0.5, 0.75, -0.125]);
  copies = 0;
  closes = 0;
  copyError: Error | undefined;
  closeError: Error | undefined;
  copyTo(plane: Float32Array, options: { planeIndex: number; format: string }): void {
    this.copies++;
    if (this.copyError !== undefined) throw this.copyError;
    equal(options.format, "f32-planar");
    assert(options.planeIndex < 2);
    plane.set(this.data);
  }
  close(): void {
    this.closes++;
    if (this.closeError !== undefined) throw this.closeError;
  }
}

const host = (readable: ReadableStream<unknown>, body: () => Promise<void>): Promise<void> =>
  withGlobals(
    {
      AudioData: AudioBlock,
      MediaStreamTrackProcessor: class {
        readonly readable = readable;
      },
    },
    body,
  );

test("native AudioData: copied planes and signed timestamps survive native closure", async ({
  signal,
}) => {
  const block = new AudioBlock(),
    source = new FakeTrack("audio");
  const readable = new ReadableStream<unknown>(
    {
      start(c) {
        c.enqueue(block);
      },
    },
    { highWaterMark: 0 },
  );
  await host(readable, async () => {
    const samples = await run(audioSamples(source).pipe(Stream.take(1), Stream.runCollect), {
      signal,
    });
    const sample = samples[0];
    assert(sample !== undefined);
    block.data.fill(0);
    source.stop();
    equal(sample.timestampUs, -123456);
    equal(sample.clock, "audio-track-processor");
    equal(sample.frames, 4);
    equal([...(sample.planes[0] ?? [])], [0.25, -0.5, 0.75, -0.125]);
    equal([...(sample.planes[1] ?? [])], [0.25, -0.5, 0.75, -0.125]);
    equal(block.closes, 1);
    equal(block.copies, 2);
    equal(readable.locked, false);
  });
});

test("native AudioData: close failure prevents copied output from escaping", async ({ signal }) => {
  const block = new AudioBlock();
  block.closeError = new Error("close failed");
  const readable = new ReadableStream<unknown>(
    {
      start(c) {
        c.enqueue(block);
      },
    },
    { highWaterMark: 0 },
  );
  let delivered = 0;
  await host(readable, async () => {
    const error = await failure(
      audioSamples(new FakeTrack("audio")).pipe(
        Stream.tap(() =>
          Effect.sync(() => {
            delivered++;
          }),
        ),
        Stream.take(1),
        Stream.runCollect,
      ),
      { signal },
    );
    equal(delivered, 0);
    equal(error.reason._tag, "Protocol");
    equal(error.context.operation, "audioSamples.close-sample");
    equal(block.closes, 1);
    equal(readable.locked, false);
  });
});

test("native AudioData: stopped borrowed lease retires its live clone", async ({ signal }) => {
  const source = new FakeTrack("audio"),
    readable = new ReadableStream<unknown>({}, { highWaterMark: 0 });
  await host(readable, async () => {
    const task = Effect.runFork(
      Effect.result(audioSamples(source, { readTimeoutMs: 500 }).pipe(Stream.runDrain)),
    );
    await eventually(() => readable.locked);
    source.stop();
    const result = await run(Fiber.join(task), { signal });
    assert(result._tag === "Failure");
    equal(result.failure.reason._tag, "Disconnected");
    equal(source.clones[0]?.readyState, "ended");
    equal(readable.locked, false);
  });
});

test("native AudioData: a read deadline joins native cancellation and reader release", async ({
  signal,
}) => {
  let cancelled = false;
  const source = new FakeTrack("audio");
  const readable = new ReadableStream<unknown>(
    {
      async cancel() {
        await new Promise((resolve) => setTimeout(resolve, 20));
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  await host(readable, async () => {
    const error = await failure(audioSamples(source, { readTimeoutMs: 60 }).pipe(Stream.runDrain), {
      signal,
    });
    equal(error.reason._tag, "Timeout");
    equal(cancelled, true);
    equal(readable.locked, false);
    equal(source.clones[0]?.readyState, "ended");
  });
});

test("native AudioData: absent type does not allocate an idle processor", ({ signal }) => {
  let processors = 0;
  return withGlobals(
    {
      AudioData: undefined,
      MediaStreamTrackProcessor: class {
        constructor() {
          processors++;
        }
      },
    },
    async () => {
      const error = await failure(audioSamples(new FakeTrack("audio")).pipe(Stream.runDrain), {
        signal,
      });
      equal(error.reason._tag, "UnsupportedCapability");
      equal(error.context.operation, "audioSamples.sample-type");
      equal(processors, 0);
    },
  );
});

test("native AudioData: absent processor releases only its owned clone", ({ signal }) =>
  withGlobals(
    {
      AudioData: AudioBlock,
      MediaStreamTrackProcessor: undefined,
    },
    async () => {
      const source = new FakeTrack("audio");
      const error = await failure(audioSamples(source).pipe(Stream.runDrain), { signal });
      equal(error.reason._tag, "UnsupportedCapability");
      equal(error.context.operation, "audioSamples.processor");
      equal(source.readyState, "live");
      equal(source.clones[0]?.readyState, "ended");
    },
  ));

test("native AudioData: cannot steal a reader already owned by another consumer", async ({
  signal,
}) => {
  const readable = new ReadableStream<unknown>(),
    other = readable.getReader();
  try {
    await host(readable, async () => {
      const source = new FakeTrack("audio");
      const error = await failure(audioSamples(source).pipe(Stream.runDrain), { signal });
      equal(error.reason._tag, "InvalidState");
      equal(error.context.operation, "audioSamples.reader");
      equal(readable.locked, true);
      equal(source.clones[0]?.readyState, "ended");
    });
  } finally {
    await other.cancel();
    other.releaseLock();
  }
});

test("native AudioData: caller interruption remains interruption and joins cleanup", async () => {
  const source = new FakeTrack("audio"),
    readable = new ReadableStream<unknown>();
  await host(readable, async () => {
    const task = Effect.runFork(
      audioSamples(source, { readTimeoutMs: 5000 }).pipe(Stream.runDrain),
    );
    await eventually(() => readable.locked);
    await Effect.runPromise(Fiber.interrupt(task));
    const exit = await Effect.runPromise(Fiber.await(task));
    assert(exit._tag === "Failure");
    assert(String(exit.cause).includes("Interrupt"));
    equal(source.readyState, "live");
    equal(source.clones[0]?.readyState, "ended");
    equal(readable.locked, false);
  });
});

test("native AudioData: explicit source end terminates an outstanding read", async ({ signal }) => {
  const source = new FakeTrack("audio"),
    readable = new ReadableStream<unknown>();
  await host(readable, async () => {
    const task = Effect.runFork(
      Effect.result(audioSamples(source, { readTimeoutMs: 5000 }).pipe(Stream.runDrain)),
    );
    await eventually(() => readable.locked);
    source.readyState = "ended";
    source.dispatchEvent(new Event("ended"));
    const result = await run(Fiber.join(task), { signal });
    assert(result._tag === "Failure");
    equal(result.failure.reason._tag, "Disconnected");
    equal(result.failure.context.operation, "audioSamples.track");
    equal(readable.locked, false);
  });
});

test("native AudioData: normal EOF is successful completion", async ({ signal }) => {
  const readable = new ReadableStream<unknown>({
    start(c) {
      c.close();
    },
  });
  await host(readable, async () => {
    const source = new FakeTrack("audio");
    equal(await run(audioSamples(source).pipe(Stream.runCollect), { signal }), []);
    equal(source.readyState, "live");
    equal(source.clones[0]?.readyState, "ended");
    equal(readable.locked, false);
  });
});

test("native AudioData: copy failure closes once and emits no output", async ({ signal }) => {
  const block = new AudioBlock();
  block.copyError = new Error("copy failed");
  const readable = new ReadableStream<unknown>({
    start(c) {
      c.enqueue(block);
    },
  });
  await host(readable, async () => {
    const error = await failure(audioSamples(new FakeTrack("audio")).pipe(Stream.runDrain), {
      signal,
    });
    equal(error.reason._tag, "Protocol");
    equal(error.context.operation, "audioSamples.copy");
    equal(block.closes, 1);
    equal(readable.locked, false);
  });
});

test("native AudioData: a cleanup error does not replace the primary copy failure", async ({
  signal,
}) => {
  const block = new AudioBlock();
  block.copyError = new Error("primary copy cause");
  block.closeError = new Error("cleanup cause");
  const readable = new ReadableStream<unknown>({
    start(c) {
      c.enqueue(block);
    },
  });
  await host(readable, async () => {
    const error = await failure(audioSamples(new FakeTrack("audio")).pipe(Stream.runDrain), {
      signal,
    });
    equal(error.context.operation, "audioSamples.copy");
    equal(block.closes, 1);
    const detail = error.context.detail;
    assert(typeof detail === "object" && detail !== null && "cleanupFailures" in detail);
    equal(detail.cleanupFailures, [{ phase: "close-sample", message: "cleanup cause" }]);
  });
});

test("native AudioData: output allocation bound is checked before any plane copy", async ({
  signal,
}) => {
  const block = new AudioBlock(),
    readable = new ReadableStream<unknown>({
      start(c) {
        c.enqueue(block);
      },
    });
  await host(readable, async () => {
    const error = await failure(
      audioSamples(new FakeTrack("audio"), { maxSampleBytes: 8 }).pipe(Stream.runDrain),
      { signal },
    );
    equal(error.reason._tag, "Overflow");
    equal(block.closes, 1);
    equal(block.copies, 0);
    equal(readable.locked, false);
  });
});
