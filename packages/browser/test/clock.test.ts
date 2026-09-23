/** Deadlines around host promises run on the provided Clock, so a TestClock triggers them.
 * The host promises are faked and settle only when a test says so; no browser is involved. */
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { FakeTrack } from "reactor-effect-test-kit";
import { audioContext, play } from "../src/_internal/media.js";
import { assert, equal, test, withGlobals } from "./harness.js";

const host = (options: {
  readonly resume: "never" | "running";
  readonly close: "never" | "done";
}) => {
  const resuming = Deferred.makeUnsafe<void>();
  const closing = Deferred.makeUnsafe<void>();
  const calls = { close: 0 };
  class Context {
    state: AudioContextState = "suspended";
    resume(): Promise<void> {
      Deferred.doneUnsafe(resuming, Effect.void);
      if (options.resume === "never") return new Promise(() => {});
      this.state = "running";
      return Promise.resolve();
    }
    close(): Promise<void> {
      calls.close++;
      Deferred.doneUnsafe(closing, Effect.void);
      if (options.close === "never") return new Promise(() => {});
      this.state = "closed";
      return Promise.resolve();
    }
  }
  return {
    resuming,
    closing,
    calls,
    globals: { AudioContext: Context, AudioWorkletNode: class {} },
  };
};

test("Web Audio policy: the resume deadline runs on the provided Clock and closes the owned context", () => {
  const stub = host({ resume: "never", close: "done" });
  return withGlobals(stub.globals, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const opening = yield* Effect.forkChild(Effect.scoped(audioContext({ timeoutMs: 60_000 })));
        yield* Deferred.await(stub.resuming);
        yield* TestClock.adjust(60_000);
        const error = yield* Effect.flip(Fiber.join(opening));
        equal(error.code, "Timeout");
        assert(error.message.includes("user activation"));
        equal(stub.calls.close, 1);
      }).pipe(Effect.provide(TestClock.layer()), Effect.timeout(2_000)),
    ),
  );
});

test("Web Audio policy: the close deadline in the scope finalizer runs on the provided Clock", () => {
  const stub = host({ resume: "running", close: "never" });
  return withGlobals(stub.globals, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // The scope runs this release uninterruptibly. The deadline outlasts the live guard
        // below, so only the provided Clock can end the wait for close().
        const closing = yield* Effect.forkChild(Effect.scoped(audioContext({ timeoutMs: 3_000 })));
        yield* Deferred.await(stub.closing);
        yield* TestClock.adjust(3_000);
        yield* Fiber.join(closing);
        equal(stub.calls.close, 1);
      }).pipe(Effect.provide(TestClock.layer()), Effect.timeout(2_000)),
    ),
  );
});

test("playback policy: the play deadline runs on the provided Clock and detaches owned media", () => {
  const playing = Deferred.makeUnsafe<void>();
  const elements: Element[] = [];
  class Element {
    srcObject: MediaProvider | null = null;
    pauses = 0;
    constructor() {
      elements.push(this);
    }
    getAttribute(_name: string): string | null {
      return null;
    }
    play(): Promise<void> {
      Deferred.doneUnsafe(playing, Effect.void);
      return new Promise(() => {});
    }
    pause(): void {
      this.pauses++;
    }
  }
  const source = new FakeTrack("video");
  return withGlobals({ HTMLMediaElement: Element, MediaStream: class {} }, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const starting = yield* Effect.forkChild(
          Effect.scoped(play(source, new HTMLMediaElement(), 60_000)),
        );
        yield* Deferred.await(playing);
        yield* TestClock.adjust(60_000);
        const error = yield* Effect.flip(Fiber.join(starting));
        equal(error.code, "Timeout");
        equal(source.clones[0]?.readyState, "ended");
        equal(elements[0]?.srcObject, null);
        equal(elements[0]?.pauses, 1);
      }).pipe(Effect.provide(TestClock.layer()), Effect.timeout(2_000)),
    ),
  );
});
