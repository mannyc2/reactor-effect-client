import { expect, test } from "vitest";
import { Cause, Effect, Exit, Fiber, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/ReactorError.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import {
  gate,
  readyState,
  record,
  request,
  runClock,
  sourceFixture,
  until,
} from "./SourceFixture.js";
import type { SourceFixture } from "./SourceFixture.js";

for (const constructor of ["legacy", "continuous"] as const) {
  for (const failure of ["defect", "interruption"] as const)
    test(`a ${constructor} recovery ${failure} settles readers and admission with its original Cause`, () =>
      runClock(
        Effect.gen(function* () {
          const owner = yield* Scope.make();
          yield* Effect.addFinalizer(() =>
            Scope.close(owner, Exit.void).pipe(Effect.exit, Effect.asVoid),
          );
          const cause =
            failure === "defect"
              ? Cause.die(new Error("controlled reconnect defect"))
              : Cause.interrupt();
          const sources: SourceFixture[] = [];
          const options: Renewal.Options = {
            open: Effect.gen(function* () {
              const fixture = yield* sourceFixture(`recovering-${sources.length}`, {
                reconnect: Effect.failCause(cause),
              });
              sources.push(fixture);
              return { source: fixture.source, lifetime: "Infinity" };
            }),
          };
          const handle =
            constructor === "legacy"
              ? yield* Renewal.make(options).pipe(Scope.provide(owner))
              : yield* Renewal.makeContinuous(options).pipe(Scope.provide(owner));
          const observation = yield* handle.engine.observe();
          const terminal = yield* Effect.forkScoped(Effect.exit(handle.engine.failure));
          const readers = yield* Effect.forEach(
            [
              Stream.runDrain(observation.events),
              Stream.runDrain(handle.media.video),
              Stream.runDrain(handle.media.audio),
            ],
            (reader) => Effect.forkScoped(Effect.exit(reader)),
          );
          const prepared = yield* handle.engine.prepare(request());
          yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "lost video"));
          yield* until(() => terminal.pollUnsafe() !== undefined);
          const terminalExit = yield* Fiber.join(terminal);
          expect(Exit.isFailure(terminalExit)).toBe(true);
          if (Exit.isFailure(terminalExit)) {
            if (failure === "defect")
              expect(Cause.squash(terminalExit.cause)).toBe(Cause.squash(cause));
            else expect(terminalExit.cause.reasons[0]?._tag).toBe("Interrupt");
            expect(terminalExit.cause.reasons).toHaveLength(1);
          }
          for (const reader of readers) expect(yield* Fiber.join(reader)).toEqual(terminalExit);
          for (const snapshot of [handle.engine.state, handle.mediaState])
            expect(yield* Effect.exit<unknown, never, never>(snapshot)).toEqual(terminalExit);
          expect(yield* Effect.exit(handle.engine.observe())).toEqual(terminalExit);
          expect(yield* Effect.exit(handle.observe())).toEqual(terminalExit);
          expect(yield* Effect.exit(prepared.submit)).toEqual(terminalExit);
          expect(yield* Effect.exit(handle.engine.setAutoplay(true))).toEqual(terminalExit);
          yield* TestClock.adjust(1_000);
          expect(sources).toHaveLength(1);
          expect(sources[0]!.sends).toEqual([]);
          yield* handle.close;
          expect(yield* handle.mediaState).toEqual({ _tag: "Closed" });
          expect(sources[0]!.status()).toMatchObject({ closes: 1, finalized: true });
          expect(yield* Effect.exit(handle.engine.failure)).toEqual(terminalExit);
        }),
      ));

  test(`a ${constructor} handoff defect stops the renewal timer and preserves its Cause`, () =>
    runClock(
      Effect.gen(function* () {
        const owner = yield* Scope.make();
        yield* Effect.addFinalizer(() =>
          Scope.close(owner, Exit.void).pipe(Effect.exit, Effect.asVoid),
        );
        const cause = Cause.die(new Error("controlled handoff activation defect"));
        const sources: SourceFixture[] = [];
        const prepared = yield* gate;
        const options: Renewal.Options = {
          lead: 500,
          open: Effect.gen(function* () {
            const index = sources.length;
            const fixture = yield* sourceFixture(`handoff-${index}`, {
              autoplay: (enabled) =>
                index === 1 && enabled ? Effect.failCause(cause) : Effect.void,
            });
            sources.push(fixture);
            return { source: fixture.source, lifetime: 1_000 };
          }),
          onRenewal: (event) => (event._tag === "Prepared" ? prepared.release : Effect.void),
        };
        const handle =
          constructor === "legacy"
            ? yield* Renewal.make(options).pipe(Scope.provide(owner))
            : yield* Renewal.makeContinuous(options).pipe(Scope.provide(owner));
        const terminal = yield* Effect.forkScoped(Effect.exit(handle.engine.failure));
        yield* TestClock.adjust(600);
        yield* prepared.wait;
        yield* sources[1]!.setState(readyState({ ready: [record("warm")] }));
        yield* TestClock.adjust(100);
        expect(terminal.pollUnsafe()).toBeDefined();
        const terminalExit = yield* Fiber.join(terminal);
        expect(Exit.isFailure(terminalExit)).toBe(true);
        if (Exit.isFailure(terminalExit)) {
          expect(Cause.squash(terminalExit.cause)).toBe(Cause.squash(cause));
          expect(terminalExit.cause.reasons).toHaveLength(1);
        }
        expect(yield* Effect.exit(handle.mediaState)).toEqual(terminalExit);
        yield* TestClock.adjust(10_000);
        expect(sources).toHaveLength(2);
        yield* handle.close;
        expect(yield* handle.mediaState).toEqual({ _tag: "Closed" });
        expect(sources.map((source) => source.status().closes)).toEqual([1, 1]);
        expect(sources.every((source) => source.status().finalized)).toBe(true);
      }),
    ));

  for (const strategy of ["sequential", "parallel"] as const)
    test(`closing a ${constructor} owner with ${strategy} finalizers cancels recovery without a terminal failure`, () =>
      runClock(
        Effect.gen(function* () {
          const owner = yield* Scope.make(strategy);
          yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void));
          const reconnecting = yield* gate;
          const interrupted = yield* gate;
          const sources: SourceFixture[] = [];
          const options: Renewal.Options = {
            open: Effect.gen(function* () {
              const fixture = yield* sourceFixture(`closing-${sources.length}`, {
                reconnect: reconnecting.release.pipe(
                  Effect.andThen(Effect.never),
                  Effect.onInterrupt(() => interrupted.release),
                ),
              });
              sources.push(fixture);
              return { source: fixture.source, lifetime: "Infinity" };
            }),
          };
          const handle =
            constructor === "legacy"
              ? yield* Renewal.make(options).pipe(Scope.provide(owner))
              : yield* Renewal.makeContinuous(options).pipe(Scope.provide(owner));
          const terminal = yield* Effect.forkScoped(Effect.exit(handle.engine.failure));
          yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "lost video"));
          yield* reconnecting.wait;
          yield* Scope.close(owner, Exit.void);
          yield* interrupted.wait;
          yield* TestClock.adjust(1);
          expect(terminal.pollUnsafe()).toBeUndefined();
          expect(yield* handle.mediaState).toEqual({ _tag: "Closed" });
          expect(sources).toHaveLength(1);
          expect(sources[0]!.status()).toMatchObject({ closes: 1, finalized: true, reconnects: 1 });
        }),
      ));
}
