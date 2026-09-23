/** One ordered handle observation, and renewal observers that never hold the command permit. */
import { expect, test } from "vitest";
import { Effect, Fiber, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import type { HandleEvent, HandleShape } from "../../src/orchestration/types.js";
import { renewalFixture } from "./RenewalFixture.js";
import {
  gate,
  member,
  readyState,
  record,
  run,
  runClock,
  sourceFixture,
  until,
} from "./SourceFixture.js";

/** Collect a handle's events, from a subscription taken now, until the stream ends. */
const collect = (handle: HandleShape) =>
  Effect.gen(function* () {
    const observed: HandleEvent[] = [];
    const { initial, events } = yield* handle.observe({ capacity: 1024 });
    const reader = yield* events.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          observed.push(event);
        }),
      ),
      Effect.forkScoped,
    );
    return { initial, observed, reader };
  });

test("a renewal observer that never returns delays no enqueue", () =>
  run(
    Effect.gen(function* () {
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          const entry = yield* sourceFixture("slow-observer");
          return { source: entry.source, lifetime: "Infinity" };
        }),
        onRenewal: () => Effect.never,
      });
      const clipId = yield* handle.engine
        .enqueue(member("while-observer-is-stuck"))
        .pipe(Effect.timeout("1 second"));
      expect(clipId).toEqual(expect.any(String));
      expect((yield* handle.mediaState)._tag).toBe("Ready");
    }),
  ));

test("a replacement follows the failures of the clips it lost, in one stream", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, awaitRenewal } = yield* renewalFixture(
        () => ({ reconnect: Effect.never }),
        { reconnectTimeout: 5_000 },
      );
      const { initial, observed } = yield* collect(handle);
      expect(initial.media._tag).toBe("Ready");
      yield* sources[0]!.setState(readyState({ ready: [record("lost-clip")] }));
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "stalled reconnect"));
      yield* sources[0]!.lifecycle.wait((event) => event._tag === "Reconnecting");
      yield* TestClock.adjust(1_100);
      yield* awaitRenewal((event) => event._tag === "Replaced");
      yield* until(() =>
        observed.some((event) => event._tag === "Renewal" && event.event._tag === "Replaced"),
      );
      const failed = observed.findIndex(
        (event) =>
          event._tag === "Engine" &&
          event.event._tag === "Failed" &&
          event.event.clipId === "lost-clip",
      );
      const replaced = observed.findIndex(
        (event) => event._tag === "Renewal" && event.event._tag === "Replaced",
      );
      expect(failed).toBeGreaterThanOrEqual(0);
      expect(failed).toBeLessThan(replaced);
      const media = observed.flatMap((event) => (event._tag === "Media" ? [event.state] : []));
      expect(media.map((state) => state._tag)).toEqual(["Recovering", "Ready"]);
      expect(media.at(-1)).toMatchObject({ sessionId: "source-2" });
    }),
  ));

test("a reconnect finishing after close never moves the media state from Closed to Ready", () =>
  runClock(
    Effect.gen(function* () {
      const held = yield* gate;
      const { handle, sources } = yield* renewalFixture(() => ({ reconnect: held.wait }));
      const { observed, reader } = yield* collect(handle);
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "controlled reconnect"));
      yield* sources[0]!.lifecycle.wait((event) => event._tag === "Reconnecting");
      yield* handle.close;
      yield* held.release;
      yield* TestClock.adjust(1);
      // The observation ends with the handle, after its Closed transition.
      yield* Fiber.join(reader);
      const media = observed.flatMap((event) => (event._tag === "Media" ? [event.state._tag] : []));
      expect(media).toEqual(["Recovering", "Closed"]);
      expect((yield* handle.mediaState)._tag).toBe("Closed");
    }),
  ));
