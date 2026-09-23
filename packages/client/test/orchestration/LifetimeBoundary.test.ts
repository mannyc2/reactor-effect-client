import { expect, test } from "vitest";
import { Effect, Fiber, Result } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import { renewalFixture } from "./RenewalFixture.js";
import { cleanPressure, gate, member, readyState, record, run, runClock } from "./SourceFixture.js";

test("expiry closes the remote lease while allowing bounded local accounting of an already known acceptance", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate;
      let accounted = false;
      const { handle, sources } = yield* renewalFixture(
        (index) =>
          index === 0
            ? {
                result: () =>
                  entered.release.pipe(
                    Effect.andThen(Effect.sleep(1_200)),
                    Effect.tap(() =>
                      Effect.sync(() => {
                        accounted = true;
                      }),
                    ),
                  ),
              }
            : {},
        { reconnectTimeout: 5_000 },
      );
      const prepared = yield* handle.engine.prepare(member("known-at-expiry", true));
      const pending = yield* prepared.submit.pipe(Effect.result, Effect.forkScoped);
      yield* entered.wait;
      yield* TestClock.adjust(1_100);
      expect(sources[0]!.status().closed).toBe(true);
      expect(sources[0]!.status().finalized).toBe(false);
      expect(accounted).toBe(false);
      yield* TestClock.adjust(100);
      const outcome = yield* Fiber.join(pending);
      expect(Result.isSuccess(outcome)).toBe(true);
      expect(accounted).toBe(true);
      expect(yield* Effect.result(prepared.submit)).toEqual(outcome);
      expect((yield* handle.sequences.get("known-at-expiry"))?.acceptedCount).toBe(1);
      expect(sources.flatMap((source) => source.sends)).toHaveLength(1);
    }),
  ));

test("a stalled reconnect is bounded by the source lifetime rather than the longer recovery budget", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, renewals, awaitRenewal } = yield* renewalFixture(
        () => ({ reconnect: Effect.never }),
        {
          reconnectTimeout: 5_000,
        },
      );
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "stalled reconnect"));
      yield* sources[0]!.lifecycle.wait((event) => event._tag === "Reconnecting");
      yield* TestClock.adjust(1_100);
      yield* awaitRenewal((event) => event._tag === "Replaced");
      expect(sources[0]!.status()).toMatchObject({ closed: true, finalized: true, reconnects: 1 });
      expect(renewals.some((event) => event._tag === "Reconnected")).toBe(false);
      expect((yield* handle.mediaState)._tag).toBe("Ready");
    }),
  ));

test("recovery waiting on a committed command closes its source at expiry and retains the unknown outcome", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate;
      const { handle, sources, awaitRenewal } = yield* renewalFixture(
        (index) =>
          index === 0
            ? {
                execute: () => entered.release.pipe(Effect.andThen(Effect.never)),
              }
            : {},
        { reconnectTimeout: 5_000 },
      );
      const prepared = yield* handle.engine.prepare(member("recovering-at-expiry"));
      const pending = yield* prepared.submit.pipe(Effect.result, Effect.forkScoped);
      yield* entered.wait;
      yield* sources[0]!.failVideo(
        ReactorError.fromCode("Disconnected", "recovering with committed work"),
      );
      yield* awaitRenewal((event) => event._tag === "Recovering");
      yield* TestClock.adjust(1_100);
      yield* awaitRenewal((event) => event._tag === "Replaced");
      const outcome = yield* Fiber.join(pending);
      expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("unknown");
      expect(yield* Effect.result(prepared.submit)).toEqual(outcome);
      expect((yield* handle.sequences.get("recovering-at-expiry"))?.pendingCount).toBe(0);
      expect(sources[0]!.status()).toMatchObject({ closed: true, finalized: true, reconnects: 0 });
      expect(sources.flatMap((source) => source.sends)).toHaveLength(1);
    }),
  ));

test("repeated pressure reads and reconnects count each retired generation's drops once", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources, awaitRenewal } = yield* renewalFixture(() => ({
        pressure: (generation) =>
          Effect.succeed({
            ...cleanPressure,
            droppedVideo: generation * 2n,
            droppedAudio: generation * 3n,
          }),
      }));
      for (const [generation, total] of [
        [1, 1n],
        [2, 3n],
        [3, 6n],
      ] as const) {
        if (generation !== 1) {
          yield* sources[0]!.failVideo(
            ReactorError.fromCode("Disconnected", "retire one receiver generation"),
          );
          yield* awaitRenewal(
            (event) => event._tag === "Reconnected" && event.generation === BigInt(generation),
          );
        }
        for (let sample = 0; sample < 2; sample++) {
          expect(yield* handle.media.pressure).toMatchObject({
            droppedVideo: total * 2n,
            droppedAudio: total * 3n,
          });
        }
      }
      expect(sources[0]!.status().reconnects).toBe(2);
    }),
  ));

test("an unreadable retired generation prevents a later zero-drop sample from establishing complete pressure", () =>
  run(
    Effect.gen(function* () {
      const unavailable = ReactorError.fromCode(
        "Disconnected",
        "old generation pressure unavailable",
      );
      const { handle, sources, awaitRenewal } = yield* renewalFixture(() => ({
        pressure: (generation) =>
          generation === 1n ? Effect.fail(unavailable) : Effect.succeed(cleanPressure),
      }));
      yield* sources[0]!.failVideo(
        ReactorError.fromCode("Disconnected", "retire unobserved generation"),
      );
      yield* awaitRenewal((event) => event._tag === "Reconnected");
      const pressure = yield* Effect.result(handle.media.pressure);
      expect(Result.isFailure(pressure)).toBe(true);
      if (Result.isFailure(pressure)) expect(pressure.failure.message).toContain("unknown");
    }),
  ));

test("a reconnect returning after explicit close cannot publish a ready handle or reopen readers", () =>
  runClock(
    Effect.gen(function* () {
      const held = yield* gate;
      const { handle, sources, renewals } = yield* renewalFixture(() => ({ reconnect: held.wait }));
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "controlled reconnect"));
      yield* sources[0]!.lifecycle.wait((event) => event._tag === "Reconnecting");
      const report = yield* handle.close;
      yield* held.release;
      yield* TestClock.adjust(1);
      expect((yield* handle.mediaState)._tag).toBe("Closed");
      expect(renewals.some((event) => event._tag === "Reconnected")).toBe(false);
      expect(yield* handle.close).toBe(report);
      expect(sources).toHaveLength(1);
      expect(sources[0]!.status()).toMatchObject({ closes: 1, finalized: true });
    }),
  ));

for (const operation of ["switch", "replace"] as const)
  test(`a ${operation} completing autoplay after close cannot restore Ready`, () =>
    runClock(
      Effect.gen(function* () {
        const entered = yield* gate;
        const held = yield* gate;
        const { handle, sources, warm } = yield* renewalFixture((index) =>
          index === 1
            ? {
                autoplay: (enabled) =>
                  enabled ? entered.release.pipe(Effect.andThen(held.wait)) : Effect.void,
              }
            : {},
        );
        yield* warm;
        if (operation === "switch")
          yield* sources[1]!.setState(readyState({ ready: [record("warm")] }));
        const advancing = yield* TestClock.adjust(operation === "switch" ? 100 : 500).pipe(
          Effect.forkScoped,
        );
        yield* entered.wait;
        const report = yield* handle.close;
        yield* held.release;
        yield* Fiber.join(advancing);
        expect((yield* handle.mediaState)._tag).toBe("Closed");
        expect(yield* handle.close).toBe(report);
        expect(sources.every((source) => source.status().closed && source.status().finalized)).toBe(
          true,
        );
        expect(sources.map((source) => source.status().closes)).toEqual([1, 1]);
      }),
    ));
