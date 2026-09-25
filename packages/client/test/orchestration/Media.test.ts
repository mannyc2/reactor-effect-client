import { expect, test } from "vitest";
import { Clock, Effect, Fiber, Option, Result, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import type { EngineEvent, HandleShape } from "../../src/orchestration/types.js";
import { renewalFixture } from "./RenewalFixture.js";
import {
  audioFrame,
  cleanPressure,
  failure,
  gate,
  member,
  readyState,
  record,
  request,
  run,
  runClock,
  until,
  untilEffect,
  videoFrame,
} from "./SourceFixture.js";

test("sequence renewal waits for every old video frame, reports audio unverified, and forwards the new source", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, renewals, warm } = yield* renewalFixture();
      const received: number[] = [];
      yield* handle.media.video.pipe(
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            received.push(frame.data[0]!);
          }),
        ),
        Effect.forkScoped,
      );
      const first = yield* handle.engine.enqueue(member("old", false));
      yield* warm;
      const final = yield* handle.engine.enqueue(member("old", true));
      const next = yield* handle.engine.enqueue(member("next"));
      expect(sources[0]!.sends).toHaveLength(2);
      expect(sources[1]!.sends).toHaveLength(1);
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      for (const clipId of [first, final]) {
        // Twenty-four frames is the explicit test contract, independent of provider duration quantization.
        yield* sources[0]!.emit({ _tag: "Started", clipId, durationSeconds: 1, at: 0 });
        yield* sources[0]!.emit({ _tag: "Started", clipId, durationSeconds: 1, at: 0 });
        for (let frame = 0; frame < 24; frame++) yield* sources[0]!.video(videoFrame(1));
        yield* sources[0]!.emit({ _tag: "Ended", clipId, termination: "finished" });
      }
      yield* until(() => received.length === 48);
      yield* sources[0]!.setState(readyState());
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(100),
      );
      const switched = renewals.find((event) => event._tag === "Switched");
      expect(switched).toBeDefined();
      if (switched?._tag === "Switched") {
        expect(switched.tail.video).toEqual({
          framesPerSecond: 24,
          expectedFrames: 48,
          receivedFrames: 48,
          status: "count-complete",
        });
        expect(switched.tail.audio).toEqual({ receivedSamples: 0, status: "unverified" });
        expect(switched.tail.sourceDrops).toEqual({ video: 0n, audio: 0n });
      }
      expect(sources[0]!.status().closed).toBe(true);
      expect(sources[1]!.controls.at(-1)).toEqual({ command: "autoplay", value: true });
      yield* sources[1]!.video(videoFrame(2));
      yield* until(() => received.at(-1) === 2);
      const cleanup = yield* handle.close;
      expect(cleanup.sessions.map((entry) => entry.lease)).toEqual(
        sources.map((source) => source.cleanup.lease),
      );
      expect(
        sources.every((source) => source.status().finalized && source.status().closes === 1),
      ).toBe(true);
    }),
  ));

test("planned handoff retains queued old video and audio instead of claiming the caller consumed them", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, renewals, warm } = yield* renewalFixture();
      const old = yield* handle.engine.enqueue(member("old"));
      yield* warm;
      const next = yield* handle.engine.enqueue(member("next"));
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* sources[0]!.emit({ _tag: "Started", clipId: old, durationSeconds: 0.125, at: 0 });
      for (const value of [1, 2, 3]) yield* sources[0]!.video(videoFrame(value));
      yield* sources[0]!.audio(audioFrame(7, 8));
      yield* untilEffect(
        handle.media.pressure.pipe(
          Effect.map((pressure) => pressure.queuedVideo === 3 && pressure.queuedAudio === 1),
        ),
      );
      yield* sources[0]!.setState(readyState());
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(100),
      );
      const switched = renewals.find((event) => event._tag === "Switched");
      if (switched?._tag !== "Switched") throw new Error("Expected an observed planned handoff");
      expect(switched.tail.video.status).toBe("count-complete");
      expect(switched.tail.audio).toEqual({ receivedSamples: 7, status: "unverified" });
      expect(switched.tail.forwarded).toEqual({ queuedVideoFrames: 3, queuedAudioSamples: 7 });
      yield* sources[1]!.video(videoFrame(4));
      const video = yield* handle.media.video.pipe(Stream.take(4), Stream.runCollect);
      expect(video.map((frame) => frame.data[0])).toEqual([1, 2, 3, 4]);
      const audio = yield* handle.media.audio.pipe(Stream.take(1), Stream.runCollect);
      expect([...audio[0]!.samples]).toEqual(new Array<number>(7).fill(8));
      expect((yield* handle.media.pressure).queuedVideo).toBe(0);
      expect((yield* handle.media.pressure).queuedAudio).toBe(0);
    }),
  ));

/** Every engine event's tag the handle republishes, in order, from now on. */
const collectEngineEvents = (handle: HandleShape) =>
  Effect.gen(function* () {
    const tags: EngineEvent["_tag"][] = [];
    const { events } = yield* handle.observe({ capacity: 1024 });
    yield* events.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          if (event._tag === "Engine") tags.push(event.event._tag);
        }),
      ),
      Effect.forkScoped,
    );
    return tags;
  });

// PR #34, 886091b: a frame the provider never sent cannot arrive later, so a
// short final clip held the switch until the retiring source expired.
test("a short final clip switches once the default grace past its Ended has elapsed", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, renewals, warm } = yield* renewalFixture();
      const observed = yield* collectEngineEvents(handle);
      const old = yield* handle.engine.enqueue(member("old"));
      yield* warm;
      const next = yield* handle.engine.enqueue(member("next"));
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* sources[0]!.emit({ _tag: "Started", clipId: old, durationSeconds: 1, at: 0 });
      for (let frame = 0; frame < 23; frame++) yield* sources[0]!.video(videoFrame());
      yield* sources[0]!.setState(readyState());
      yield* sources[0]!.emit({ _tag: "Ended", clipId: old, termination: "finished" });
      yield* sources[0]!.emit({ _tag: "Starved", at: 0 });
      // The handle republishes Ended only after the source's owner has timed it,
      // and the Queued marker only after it has judged the Starved before it.
      yield* sources[0]!.emit({ _tag: "Queued", clipId: old, durationSeconds: 1 });
      yield* until(() => observed.includes("Queued"));
      const endedAt = yield* Clock.currentTimeMillis;
      // The switch is imminent, so the retiring source running dry is not starvation.
      expect(observed).toContain("Ended");
      expect(observed).not.toContain("Starved");
      yield* TestClock.adjust(240);
      expect(renewals.some((event) => event._tag === "Switched")).toBe(false);
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(10),
        "short final clip never switched",
      );
      // Before the source's one-second expiry, which would have replaced it.
      const waited = (yield* Clock.currentTimeMillis) - endedAt;
      expect(waited).toBeGreaterThanOrEqual(250);
      expect(waited).toBeLessThanOrEqual(360);
      expect(renewals.some((event) => event._tag === "Replaced")).toBe(false);
      const switched = renewals.find((event) => event._tag === "Switched");
      if (switched?._tag !== "Switched") throw new Error("Expected a planned switch");
      expect(switched.tail.video).toEqual({
        framesPerSecond: 24,
        expectedFrames: 24,
        receivedFrames: 23,
        status: "incomplete",
      });
    }),
  ));

test("a final frame landing after Ended switches at once, inside the grace", () =>
  runClock(
    Effect.gen(function* () {
      // The longest grace, far past expiry at 1 s: only the completed count
      // can explain a switch.
      const { handle, sources, renewals, warm } = yield* renewalFixture(() => ({}), {
        handoffGrace: "5 seconds",
      });
      const observed = yield* collectEngineEvents(handle);
      const received: number[] = [];
      yield* handle.media.video.pipe(
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            received.push(frame.data[0]!);
          }),
        ),
        Effect.forkScoped,
      );
      const old = yield* handle.engine.enqueue(member("old"));
      yield* warm;
      const next = yield* handle.engine.enqueue(member("next"));
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* sources[0]!.emit({ _tag: "Started", clipId: old, durationSeconds: 1, at: 0 });
      for (let frame = 0; frame < 23; frame++) yield* sources[0]!.video(videoFrame(1));
      yield* sources[0]!.emit({ _tag: "Ended", clipId: old, termination: "finished" });
      yield* sources[0]!.setState(readyState());
      yield* until(() => observed.includes("Ended"));
      yield* TestClock.adjust(200);
      expect(renewals.some((event) => event._tag === "Switched")).toBe(false);
      yield* sources[0]!.video(videoFrame(2));
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(10),
        "completed final clip never switched",
      );
      const switched = renewals.find((event) => event._tag === "Switched");
      if (switched?._tag !== "Switched") throw new Error("Expected a planned switch");
      expect(switched.tail.video.status).toBe("count-complete");
      yield* until(() => received.length === 24);
      expect(received.at(-1)).toBe(2);
    }),
  ));

test("the handoff grace is validated when the orchestration is made", () =>
  run(
    Effect.gen(function* () {
      for (const invalid of [-1, Number.NaN, "Infinity", "5001 millis"] as const) {
        const refused = yield* Effect.flip(renewalFixture(() => ({}), { handoffGrace: invalid }));
        expect(refused.reason._tag).toBe("InvalidInput");
      }
      for (const valid of [0, "5 seconds"] as const) {
        const { handle } = yield* renewalFixture(() => ({}), { handoffGrace: valid });
        yield* handle.close;
      }
    }),
  ));

// A lost Ended must not hold the switch until expiry either: withdrawing the
// retiring source's filler relies on the switch following its last clip.
test("a final clip whose Ended is lost switches once the grace past observed idle elapses", () =>
  runClock(
    Effect.gen(function* () {
      // A short grace: this fixture's source expires 400 ms after warming.
      const { handle, sources, renewals, warm } = yield* renewalFixture(() => ({}), {
        handoffGrace: "150 millis",
      });
      const observed = yield* collectEngineEvents(handle);
      const old = yield* handle.engine.enqueue(member("old"));
      yield* warm;
      const next = yield* handle.engine.enqueue(member("next"));
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* sources[0]!.emit({ _tag: "Started", clipId: old, durationSeconds: 1, at: 0 });
      const expectedReceived = 23;
      for (let frame = 0; frame < expectedReceived; frame++) yield* sources[0]!.video(videoFrame());
      yield* untilEffect(
        handle.media.pressure.pipe(
          Effect.map((pressure) => pressure.queuedVideo === expectedReceived),
        ),
      );
      yield* sources[0]!.setState(readyState());
      // Before the grace has started, no switch is due yet: the source running
      // dry is reported as starvation.
      yield* sources[0]!.emit({ _tag: "Starved", at: 0 });
      yield* until(() => observed.includes("Starved"));
      const idleFrom = yield* Clock.currentTimeMillis;
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(10),
        "idle source with a lost Ended never switched",
      );
      // The first tick sees the idle source and starts the grace; the switch
      // lands within the grace plus a tick after that, before expiry at 1 s.
      const waited = (yield* Clock.currentTimeMillis) - idleFrom;
      expect(waited).toBeGreaterThanOrEqual(150);
      expect(waited).toBeLessThanOrEqual(360);
      expect(renewals.some((event) => event._tag === "Replaced")).toBe(false);
      const switched = renewals.find((event) => event._tag === "Switched");
      if (switched?._tag !== "Switched") throw new Error("Expected a planned switch");
      expect(switched.tail.video.receivedFrames).toBe(expectedReceived);
      expect(switched.tail.video.status).toBe("incomplete");
      expect(switched.tail.audio.status).toBe("unverified");
    }),
  ));

test("a frame-only terminal failure fails media readers without requiring an engine-event reader", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources } = yield* renewalFixture(
        () => ({
          reconnect: Effect.fail(ReactorError.fromCode("Closed", "source expired")),
        }),
        { maxSessions: 1 },
      );
      const reader = yield* handle.media.video.pipe(
        Stream.runDrain,
        Effect.result,
        Effect.forkScoped,
      );
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "video receiver failed"));
      const result = yield* Fiber.join(reader).pipe(Effect.timeout(1000));
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(result.failure.reason._tag).toBe("Overflow");
      expect((yield* handle.mediaState)._tag).toBe("Failed");
      expect((yield* handle.engine.failure).reason._tag).toBe("Overflow");
      const later = yield* Effect.result(handle.engine.enqueue(request()));
      expect(Result.isFailure(later) && later.failure.context.outcome).toBe("not-submitted");
      expect(sources[0]!.status().closes).toBe(1);
    }),
  ));

test("unrecoverable media reports each lost clip with its source and replacement accepts fresh work", () =>
  run(
    Effect.gen(function* () {
      const events: EngineEvent[] = [];
      const { handle, sources, renewals } = yield* renewalFixture(() => ({
        reconnect: Effect.fail(ReactorError.fromCode("Closed", "expired")),
      }));
      yield* handle.engine.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      const lost = yield* handle.engine.enqueue(member("lost"));
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "connection lost"));
      yield* until(() => renewals.some((event) => event._tag === "Replaced"));
      const fresh = yield* handle.engine.enqueue(member("fresh"));
      expect(fresh).not.toBe(lost);
      yield* until(() => events.some((event) => event._tag === "Failed"));
      expect(
        events
          .filter((event) => event._tag === "Failed")
          .map((event) => ({ id: event.clipId, session: event.sessionId })),
      ).toEqual([{ id: lost, session: "source-1" }]);
      expect(events.some((event) => event._tag === "SessionFailed")).toBe(false);
      const replacement = renewals.find((event) => event._tag === "Replaced");
      expect(replacement?._tag === "Replaced" && replacement.lostClips).toBe(1);
      expect(replacement?.sessionId).toBe("source-1");
      expect(sources[1]!.sends).toHaveLength(1);
      expect(yield* handle.sessionId).toEqual(Option.some("source-2"));
    }),
  ));

test("reconnect reports clips missing from refreshed active state without inventing their lifecycle", () =>
  run(
    Effect.gen(function* () {
      const held = yield* gate,
        events: EngineEvent[] = [];
      const { handle, sources, renewals } = yield* renewalFixture(() => ({ reconnect: held.wait }));
      yield* handle.engine.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ),
        Effect.forkScoped,
      );
      const lost = yield* handle.engine.enqueue(request()),
        retained = yield* handle.engine.enqueue(request());
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "observation gap"));
      yield* untilEffect(
        handle.mediaState.pipe(Effect.map((state) => state._tag === "Recovering")),
      );
      yield* until(() => sources[0]!.status().reconnects === 1);
      yield* sources[0]!.setState(
        readyState({ queued: [record(retained)], generationOrder: [retained] }),
      );
      yield* held.release;
      yield* until(() => renewals.some((event) => event._tag === "Reconnected"));
      yield* until(() => events.some((event) => event._tag === "Failed"));
      expect(
        events.filter((event) => event._tag === "Failed").map((event) => event.clipId),
      ).toEqual([lost]);
      expect(
        events.some((event) => ["Started", "Ended", "SessionFailed"].includes(event._tag)),
      ).toBe(false);
      expect((yield* handle.engine.state).generationOrder).toEqual([retained]);
      expect(yield* handle.sessionId).toEqual(Option.some("source-1"));
      expect(sources[0]!.sends).toHaveLength(2);
    }),
  ));

test("forwarded video and audio pressure counts held frames, samples and bytes exactly", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources } = yield* renewalFixture();
      yield* sources[0]!.video({ ...videoFrame(), metadata: new Uint8Array([1, 2]) });
      yield* sources[0]!.audio(audioFrame(9));
      yield* untilEffect(
        handle.media.pressure.pipe(
          Effect.map((pressure) => pressure.queuedVideo === 1 && pressure.queuedAudio === 1),
        ),
      );
      expect(yield* handle.media.pressure).toMatchObject({
        queuedVideo: 1,
        queuedAudio: 1,
        queuedBytes: 24,
        droppedVideo: 0n,
        droppedAudio: 0n,
      });
      yield* handle.media.video.pipe(Stream.take(1), Stream.runDrain);
      yield* handle.media.audio.pipe(Stream.take(1), Stream.runDrain);
      expect(yield* handle.media.pressure).toMatchObject({
        queuedVideo: 0,
        queuedAudio: 0,
        queuedBytes: 0,
      });
    }),
  ));

for (const kind of ["video", "audio"] as const)
  test(`unconsumed ${kind} is bounded and overflow becomes a terminal typed failure`, () =>
    run(
      Effect.gen(function* () {
        const { handle, sources } = yield* renewalFixture();
        if (kind === "video")
          for (let frame = 0; frame < 97; frame++) yield* sources[0]!.video(videoFrame());
        else yield* sources[0]!.audio(audioFrame(48000 * 4 + 1));
        const fatal = yield* handle.engine.failure.pipe(Effect.timeout(1000));
        expect(fatal.reason._tag).toBe("Overflow");
        expect((yield* handle.mediaState)._tag).toBe("Failed");
        const pressure = yield* handle.media.pressure;
        expect(pressure.queuedVideo).toBeLessThanOrEqual(96);
        expect(pressure.queuedAudio).toBe(0);
        expect(sources[0]!.status().reconnects).toBe(0);
      }),
    ));

test("the media tolerance is an orchestration option, validated when it is made", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources } = yield* renewalFixture(() => ({}), {
        maxQueuedVideoFrames: 2,
        maxQueuedAudioSamples: 10,
      });
      yield* sources[0]!.audio(audioFrame(10));
      for (let frame = 0; frame < 2; frame++) yield* sources[0]!.video(videoFrame());
      // Exactly the bound is retained without failing.
      yield* untilEffect(
        handle.media.pressure.pipe(
          Effect.map((pressure) => pressure.queuedVideo === 2 && pressure.queuedAudio === 1),
        ),
      );
      expect((yield* handle.mediaState)._tag).not.toBe("Failed");
      yield* sources[0]!.video(videoFrame());
      const fatal = yield* handle.engine.failure.pipe(Effect.timeout(1000));
      expect(fatal.reason._tag).toBe("Overflow");
      for (const invalid of [{ maxQueuedVideoFrames: 0 }, { maxQueuedAudioSamples: 1.5 }]) {
        const refused = yield* Effect.flip(renewalFixture(() => ({}), invalid));
        expect(refused.reason._tag).toBe("InvalidInput");
      }
    }),
  ));

for (const outcome of ["success", "replied", "unknown"] as const)
  test(`a committed ${outcome} result survives handle closure without a fresh dispatch`, () =>
    run(
      Effect.gen(function* () {
        const original = outcome === "success" ? undefined : failure(outcome);
        const { handle, sources } = yield* renewalFixture((index) =>
          index === 0 && original !== undefined ? { execute: () => Effect.fail(original) } : {},
        );
        const prepared = yield* handle.engine.prepare(member("retained-result"));
        const first = yield* Effect.result(prepared.submit);
        if (outcome === "unknown") yield* until(() => sources.length === 2);
        yield* handle.close;
        const again = yield* Effect.result(prepared.submit);
        expect(again).toEqual(first);
        if (Result.isFailure(first) && Result.isFailure(again))
          expect(again.failure).toBe(first.failure);
        expect((yield* prepared.state)._tag).toBe("Completed");
        expect(sources.flatMap((source) => source.sends)).toHaveLength(1);
      }),
    ));

test("reconnect retains known drop evidence from retired media generations", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources, renewals } = yield* renewalFixture(() => ({
        pressure: (generation) =>
          Effect.succeed({
            ...cleanPressure,
            droppedVideo: generation === 1n ? 2n : 0n,
            droppedAudio: generation === 1n ? 3n : 0n,
          }),
      }));
      expect(yield* handle.media.pressure).toMatchObject({ droppedVideo: 2n, droppedAudio: 3n });
      yield* sources[0]!.failVideo(
        ReactorError.fromCode("Disconnected", "replace receiver generation"),
      );
      yield* until(() => renewals.some((event) => event._tag === "Reconnected"));
      expect(yield* handle.mediaState).toEqual({
        _tag: "Ready",
        sessionId: "source-1",
        generation: 2n,
      });
      expect(yield* handle.media.pressure).toMatchObject({ droppedVideo: 2n, droppedAudio: 3n });
      expect(sources[0]!.status().reconnects).toBe(1);
    }),
  ));

test("the output's loss totals carry a lost owner's losses into its replacement", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources, renewals } = yield* renewalFixture(() => ({
        reconnect: Effect.fail(ReactorError.fromCode("Closed", "expired")),
      }));
      yield* sources[0]!.setPressure({ droppedVideo: 4n, droppedAudio: 1n, readerOverflows: 1n });
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "connection lost"));
      yield* until(() => renewals.some((event) => event._tag === "Replaced"));
      expect(yield* handle.sessionId).toEqual(Option.some("source-2"));
      // The replacement's own counters start at zero; the output keeps the loss.
      expect(yield* handle.media.pressure).toMatchObject({
        droppedVideo: 4n,
        droppedAudio: 1n,
        readerOverflows: 1n,
      });
      yield* sources[1]!.setPressure({ droppedVideo: 2n });
      expect(yield* handle.media.pressure).toMatchObject({ droppedVideo: 6n, droppedAudio: 1n });
    }),
  ));

test("a prepared replacement's losses before it feeds the output are not the output's", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, renewals, warm } = yield* renewalFixture();
      yield* handle.engine.enqueue(member("old"));
      yield* warm;
      // Loss on the prepared session while the old one still feeds the output.
      yield* sources[1]!.setPressure({ droppedVideo: 3n, readerOverflows: 2n });
      const next = yield* handle.engine.enqueue(member("next"));
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* sources[0]!.setState(readyState());
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(100),
      );
      expect(yield* handle.sessionId).toEqual(Option.some("source-2"));
      expect(yield* handle.media.pressure).toMatchObject({ droppedVideo: 0n, readerOverflows: 0n });
      yield* sources[1]!.setPressure({ droppedVideo: 5n, readerOverflows: 2n });
      expect(yield* handle.media.pressure).toMatchObject({ droppedVideo: 2n, readerOverflows: 0n });
    }),
  ));

test("unavailable source pressure stays unknown in handoff evidence instead of becoming zero", () =>
  runClock(
    Effect.gen(function* () {
      const unavailable = ReactorError.fromCode("Disconnected", "pressure sample unavailable");
      const { handle, sources, renewals, warm } = yield* renewalFixture((index) =>
        index === 0 ? { pressure: () => Effect.fail(unavailable) } : {},
      );
      const pressure = yield* Effect.result(handle.media.pressure);
      expect(Result.isFailure(pressure) && pressure.failure).toBe(unavailable);
      yield* handle.engine.enqueue(member("old"));
      yield* warm;
      const next = yield* handle.engine.enqueue(member("next"));
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* sources[0]!.setState(readyState());
      yield* TestClock.adjust(100);
      yield* until(
        () => renewals.some((event) => event._tag === "Switched"),
        TestClock.adjust(100),
      );
      const replaced = renewals.find((event) => event._tag === "Switched");
      if (replaced?._tag !== "Switched") throw new Error("The idle source must explicitly retire");
      expect(replaced.tail.sourceDrops).toEqual({ video: null, audio: null });
      expect(replaced.tail.audio.status).toBe("unverified");
    }),
  ));

test("terminal media failure is published once and later source failures cannot overwrite it", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources } = yield* renewalFixture();
      const events: EngineEvent[] = [];
      yield* handle.engine.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      yield* sources[0]!.audio(audioFrame(48000 * 4 + 1));
      const first = yield* handle.engine.failure.pipe(Effect.timeout(1000));
      yield* until(() => events.some((event) => event._tag === "SessionFailed"));
      yield* sources[0]!.emit({
        _tag: "SessionFailed",
        failure: ReactorError.fromCode("Disconnected", "later source error"),
      });
      yield* Effect.yieldNow;
      expect(yield* handle.engine.failure).toBe(first);
      expect(events.filter((event) => event._tag === "SessionFailed")).toHaveLength(1);
      const requestResult = yield* Effect.result(handle.engine.enqueue(request()));
      expect(Result.isFailure(requestResult) && requestResult.failure.context.outcome).toBe(
        "not-submitted",
      );
      expect(sources[0]!.sends).toEqual([]);
    }),
  ));

// Without a media gate, handoffReady is what lets a scheduler withdraw the
// retiring source's Ready filler. An open sequence keeps that source preferred,
// and its members still need the filler behind them.
test("handoffReady stays false while an open sequence keeps the retiring source preferred", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, warm } = yield* renewalFixture();
      yield* handle.engine.enqueue(member("old", false));
      yield* warm;
      yield* sources[1]!.setState(readyState());
      const open = yield* handle.engine.state;
      expect(Option.getOrUndefined(open.retiringSessionId)).toBe(sources[0]!.source.id);
      expect(Option.getOrUndefined(open.preferredSessionId)).toBe(sources[0]!.source.id);
      expect(open.handoffReady).toBe(false);
      yield* handle.engine.enqueue(member("old", true));
      const sealed = yield* handle.engine.state;
      expect(Option.getOrUndefined(sealed.preferredSessionId)).toBe(sources[1]!.source.id);
      expect(sealed.handoffReady).toBe(true);
    }),
  ));
