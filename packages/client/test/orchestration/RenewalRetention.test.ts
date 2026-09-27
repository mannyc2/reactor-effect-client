import { expect, test } from "vitest";
import { Cause, Effect, Exit, Fiber, Option, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as Renewal from "../../src/orchestration/renewal.js";
import { AcquisitionFailure, ReactorError } from "../../src/errors.js";
import { ClipId } from "../../src/orchestration/request.js";
import {
  failure,
  gate,
  member,
  refusal,
  request,
  runClock,
  sourceFixture,
  until,
  untilEffect,
  videoFrame,
} from "./SourceFixture.js";
import type { SourceFixture } from "./SourceFixture.js";
import type { SourceCleanup } from "../../src/orchestration/types.js";

/** A close Exit's reasons, each defect by identity, so a duplicate or an Interrupt shows. */
const reasons = (exit: Exit.Exit<unknown, unknown>): unknown[] =>
  Exit.isSuccess(exit)
    ? []
    : exit.cause.reasons.map((reason) => (Cause.isDieReason(reason) ? reason.defect : reason._tag));

test("scope-finalizer defects preserve the canonical report and their original Cause on every close", () =>
  runClock(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const defect = new Error("retiring scope failed");
      let canonical: SourceCleanup | undefined;
      const handle = yield* Renewal.makeContinuous({
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.die(defect));
          const fixture = yield* sourceFixture("scope-defect");
          canonical = fixture.cleanup;
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      }).pipe(Scope.provide(scope));
      const first = yield* Effect.exit(handle.close);
      const repeated = yield* Effect.exit(handle.close);
      expect(first._tag).toBe("Failure");
      if (Exit.isFailure(first) && Exit.isFailure(repeated)) {
        expect(Cause.findDefect(first.cause)).toMatchObject({ _tag: "Success", success: defect });
        expect(repeated.cause).toBe(first.cause);
      }
      const summary = Option.getOrThrow(yield* handle.cleanup);
      expect(summary.retained[0]?.cleanup).toBe(canonical);
      expect(summary.retained[0]?.retirement.scope).toBe("failed");
      expect(summary.retained[0]?.disposition).toBe("incomplete");
      expect(summary.omittedComplete.attachedDetached).toBe(0n);
      yield* Effect.exit(Scope.close(scope, Exit.void));
    }),
  ));

test("continuous renewal exceeds 64 sources and preserves exact bounded cleanup evidence", () =>
  runClock(
    Effect.gen(function* () {
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.makeContinuous({
        lead: 0,
        reconnectTimeout: 50,
        retainedSuccessfulCleanups: 2,
        maxUnresolvedCleanups: 2,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(`continuous-${sources.length}`);
          sources.push(fixture);
          return { source: fixture.source, lifetime: "200 millis" };
        }),
      });
      let frames = 0;
      yield* handle.media.video.pipe(
        Stream.runForEach(() =>
          Effect.sync(() => {
            frames++;
          }),
        ),
        Effect.forkScoped,
      );
      const first = yield* handle.engine.prepare(member("first"));
      const firstResult = yield* first.submit;
      for (let index = 1; index < 80; index++) {
        yield* sources.at(-1)!.video(videoFrame());
        yield* until(() => frames === index);
        yield* until(() => sources.length > index, TestClock.adjust(100));
        expect(sources.filter((source) => !source.status().finalized).length).toBeLessThanOrEqual(
          2,
        );
      }
      expect(yield* first.submit).toBe(firstResult);
      expect((yield* first.state)._tag).toBe("Completed");
      expect((yield* handle.sequences.get("first"))?.status).toBe("retired");
      expect((yield* handle.sequences.get("first"))?.owner).toBe("continuous-0");
      expect(Option.isNone(yield* handle.cleanup)).toBe(true);
      yield* sources.at(-1)!.setPressure({ droppedVideo: 2n });
      const summary = yield* handle.close;
      // Retiring the last owner folds its loss into the output's totals once.
      expect(yield* handle.media.pressure).toMatchObject({ closed: true, droppedVideo: 2n });
      expect(summary.totalRetirements).toBe(BigInt(sources.length));
      expect(summary.retained).toHaveLength(2);
      expect(summary.omittedComplete.attachedDetached).toBe(BigInt(sources.length - 2));
      expect(summary.omittedComplete.ownedTerminated).toBe(0n);
      expect(summary.exhausted).toBe(false);
      expect(yield* handle.close).toBe(summary);
      expect(Option.getOrUndefined(yield* handle.cleanup)).toBe(summary);
      expect(sources.every((source) => source.status().finalized)).toBe(true);
    }),
  ));

test("continuous explicit maxSessions retains the successful-open lifetime cap", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.makeContinuous({
        maxSessions: 1,
        lead: 0,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(`bounded-${++opened}`);
          return { source: fixture.source, lifetime: 200 };
        }),
      });
      const failed = yield* handle.engine.failure.pipe(Effect.forkScoped);
      yield* TestClock.adjust(300);
      expect((yield* Fiber.join(failed)).reason._tag).toBe("Overflow");
      expect(opened).toBe(1);
      expect((yield* handle.close).totalRetirements).toBe(1n);
    }),
  ));

test("continuous close preserves its original defect while exposing incomplete factual cleanup", () =>
  runClock(
    Effect.gen(function* () {
      const owned = yield* Scope.make();
      const defect = new Error("source close defect");
      const handle = yield* Renewal.makeContinuous({
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture("defective", { close: Effect.die(defect) });
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      }).pipe(Scope.provide(owned));
      const first = yield* Effect.exit(handle.close);
      const second = yield* Effect.exit(handle.close);
      expect(first._tag).toBe("Failure");
      if (Exit.isFailure(first) && Exit.isFailure(second)) {
        expect(Cause.findDefect(first.cause)).toEqual(Cause.findDefect(second.cause));
        expect(first.cause).toBe(second.cause);
      }
      const summary = Option.getOrThrow(yield* handle.cleanup);
      expect(summary.retained[0]?.cleanup).toBeUndefined();
      expect(summary.retained[0]?.disposition).toBe("incomplete");
      expect(summary.retained[0]?.retirement.errors.length).toBeGreaterThan(0);
      expect((yield* Effect.exit(Scope.close(owned, Exit.void)))._tag).toBe("Failure");
    }),
  ));

// Review of ed88be7/4443f80: every waiter recorded its own Cause object for one
// cached retirement, and close added its own cancellation of an opening attempt.
test("a legacy recovery retirement defect is reported once by close and by its owner scope", () =>
  runClock(
    Effect.gen(function* () {
      const owner = yield* Scope.make();
      const defect = new Error("retiring source close defect");
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(
            `retiring-${sources.length}`,
            sources.length === 0 ? { close: Effect.die(defect) } : {},
          );
          sources.push(fixture);
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      }).pipe(Scope.provide(owner));
      yield* sources[0]!.failEvents(ReactorError.fromCode("Disconnected", "lost"));
      yield* until(() => sources[0]!.status().finalized);
      expect(reasons(yield* Effect.exit(handle.close))).toEqual([defect]);
      expect(reasons(yield* Effect.exit(Scope.close(owner, Exit.void)))).toEqual([defect]);
    }),
  ));

test("an opening attempt that close cancels reports only its own finalizer defect", () =>
  runClock(
    Effect.gen(function* () {
      const owner = yield* Scope.make();
      const defect = new Error("opening finalizer defect");
      const opening = yield* gate;
      let attempts = 0;
      const handle = yield* Renewal.makeContinuous({
        lead: 500,
        open: Effect.gen(function* () {
          if (++attempts === 2) {
            yield* Effect.addFinalizer(() => Effect.die(defect));
            yield* opening.wait;
          }
          const fixture = yield* sourceFixture(`opening-${attempts}`);
          return { source: fixture.source, lifetime: 1000 };
        }),
      }).pipe(Scope.provide(owner));
      yield* until(() => attempts === 2, TestClock.adjust(100));
      expect(reasons(yield* Effect.exit(handle.close))).toEqual([defect]);
      expect(reasons(yield* Effect.exit(Scope.close(owner, Exit.void)))).toEqual([defect]);
    }),
  ));

test("a stalled scope finalizer keeps close and its cleanup reservation pending", () =>
  runClock(
    Effect.gen(function* () {
      const barrier = yield* gate;
      const parent = yield* Scope.make("parallel");
      let entered = false;
      const handle = yield* Renewal.makeContinuous({
        open: Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              entered = true;
            }).pipe(Effect.andThen(barrier.wait)),
          );
          const fixture = yield* sourceFixture("stalled");
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      }).pipe(Scope.provide(parent));
      const close = yield* handle.close.pipe(Effect.forkScoped);
      yield* until(() => entered);
      const repeated = yield* handle.close.pipe(Effect.forkScoped);
      const parentClose = yield* Scope.close(parent, Exit.void).pipe(Effect.forkScoped);
      yield* TestClock.adjust("1 minute");
      expect(close.pollUnsafe()).toBeUndefined();
      expect(repeated.pollUnsafe()).toBeUndefined();
      expect(parentClose.pollUnsafe()).toBeUndefined();
      expect(Option.isNone(yield* handle.cleanup)).toBe(true);
      yield* barrier.release;
      const summary = yield* Fiber.join(close);
      expect(summary.retained[0]?.disposition).toBe("complete");
      expect(yield* Fiber.join(repeated)).toBe(summary);
      yield* Fiber.join(parentClose);
    }),
  ));

// Review of 4443f80: close froze its summary around acquisitions it did not
// track, so a source could open after close returned without being reported.
test("close cancels an inline replacement acquisition and reports its reservation", () =>
  runClock(
    Effect.gen(function* () {
      const opening = yield* gate;
      const sources: SourceFixture[] = [];
      let attempts = 0;
      const handle = yield* Renewal.makeContinuous({
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          if (++attempts === 2) yield* opening.wait;
          const fixture = yield* sourceFixture(`inline-${attempts}`);
          sources.push(fixture);
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      });
      // Replacing the lost source acquires on the recovery fiber, not as Opening.
      yield* sources[0]!.failEvents(ReactorError.fromCode("Disconnected", "lost"));
      yield* until(() => attempts === 2);
      const summary = yield* handle.close;
      yield* opening.release;
      yield* Effect.yieldNow;
      expect(sources).toHaveLength(1);
      expect(summary.totalRetirements).toBe(2n);
      expect(summary.omittedComplete.attachedDetached).toBe(1n);
      expect(summary.retained).toHaveLength(1);
      expect(summary.retained[0]).toMatchObject({ ordinal: 2n, disposition: "incomplete" });
      expect(summary.retained[0]?.cleanup).toBeUndefined();
    }),
  ));

test("close joins an opening replacement that stopRenewal is still cancelling", () =>
  runClock(
    Effect.gen(function* () {
      const opening = yield* gate;
      let attempts = 0;
      const handle = yield* Renewal.makeContinuous({
        lead: 500,
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          // An allocation request that cannot be abandoned midway.
          if (++attempts === 2) yield* Effect.uninterruptible(opening.wait);
          const fixture = yield* sourceFixture(`stopping-${attempts}`);
          return { source: fixture.source, lifetime: 1000 };
        }),
      });
      yield* until(() => attempts === 2, TestClock.adjust(100));
      const stopping = yield* Effect.forkScoped(handle.engine.stopRenewal);
      yield* Effect.yieldNow;
      const closing = yield* Effect.forkScoped(handle.close);
      yield* Effect.yieldNow;
      yield* opening.release;
      const summary = yield* Fiber.join(closing);
      yield* Fiber.join(stopping);
      // The cancelled attempt retires before close retires the current source.
      expect(summary.totalRetirements).toBe(2n);
      expect(summary.retained).toHaveLength(1);
      expect(summary.retained[0]).toMatchObject({ ordinal: 1n, disposition: "incomplete" });
      expect(summary.retained[0]?.cleanup).toBeUndefined();
    }),
  ));

for (const constructor of ["legacy", "continuous"] as const)
  test(`a ${constructor} replacement due after close begins is refused without failing the handle`, () =>
    runClock(
      Effect.gen(function* () {
        const owner = yield* Scope.make();
        const retiring = yield* gate;
        const sources: SourceFixture[] = [];
        let attempts = 0;
        const options: Renewal.Options = {
          open: Effect.gen(function* () {
            attempts++;
            const fixture = yield* sourceFixture(
              `refused-${attempts}`,
              attempts === 1 ? { close: retiring.wait } : {},
            );
            sources.push(fixture);
            return { source: fixture.source, lifetime: "Infinity" };
          }),
        };
        const handle =
          constructor === "legacy"
            ? yield* Renewal.make(options).pipe(Scope.provide(owner))
            : yield* Renewal.makeContinuous(options).pipe(Scope.provide(owner));
        // The lost source's retirement is still running when close begins, so
        // the replacement it leads to is due only after close has started.
        yield* sources[0]!.failEvents(ReactorError.fromCode("Disconnected", "lost"));
        yield* until(() => sources[0]!.status().closed);
        const closing = yield* Effect.forkScoped(handle.close);
        yield* Effect.yieldNow;
        yield* retiring.release;
        yield* Fiber.join(closing);
        // Closing the owner joins the recovery fiber that retired the source.
        yield* Scope.close(owner, Exit.void);
        // No source opens after close, and its refusal publishes no failure.
        expect(attempts).toBe(1);
        expect((yield* handle.mediaState)._tag).toBe("Closed");
      }),
    ));

test("unknown submissions exhaust their reserved history even after confirmed termination", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.makeContinuous({
        retainedSuccessfulCleanups: 0,
        maxUnresolvedCleanups: 2,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(`unknown-bound-${++opened}`, {
            execute: () => Effect.fail(failure("unknown")),
          });
          return {
            source: {
              ...fixture.source,
              close: fixture.source.close.pipe(
                Effect.map((cleanup) => ({
                  ...cleanup,
                  lease: {
                    ...cleanup.lease,
                    ownership: "owned" as const,
                    remote: {
                      ...cleanup.lease.remote,
                      attempted: true,
                      confirmed: true,
                      evidence: "terminal" as const,
                    },
                  },
                })),
              ),
            },
            lifetime: "Infinity",
          };
        }),
      });
      yield* Effect.result(handle.engine.enqueue(request()));
      yield* until(() => opened === 2);
      yield* Effect.result(handle.engine.enqueue(request()));
      expect((yield* handle.engine.failure).reason._tag).toBe("Overflow");
      expect(opened).toBe(2);
      const summary = yield* handle.close;
      expect(summary.exhausted).toBe(true);
      expect(summary.retained.map((row) => row.retirement.unknownSubmissions)).toEqual([1n, 1n]);
      expect(
        summary.retained.every(
          (row) => row.cleanup?.lease.remote.confirmed && row.retirement.errors.length === 0,
        ),
      ).toBe(true);
    }),
  ));

test("source-fenced enqueue captures the available incarnation before waiting on recovery", () =>
  runClock(
    Effect.gen(function* () {
      const closing = yield* gate;
      const release = yield* gate;
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.makeContinuous({
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(
            "raw-reused",
            sources.length === 0
              ? { close: closing.release.pipe(Effect.andThen(release.wait)) }
              : {},
          );
          sources.push(fixture);
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      });
      yield* sources[0]!.failEvents(ReactorError.fromCode("Disconnected", "replace source"));
      yield* closing.wait;
      const pending = yield* Effect.result(
        handle.engine.enqueueOnSource!(request(), "raw-reused"),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* release.release;
      const outcome = yield* Fiber.join(pending);
      expect(outcome._tag === "Failure" && refusal(outcome.failure)).toBe("RouteChanged");
      expect(sources.every((source) => source.sends.length === 0)).toBe(true);
    }),
  ));

test("an unbound submission chooses current at first submit after earlier history is evicted", () =>
  runClock(
    Effect.gen(function* () {
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.makeContinuous({
        lead: 0,
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(`unbound-${sources.length}`);
          sources.push(fixture);
          return { source: fixture.source, lifetime: 200 };
        }),
      });
      const pending = yield* handle.engine.prepare(request());
      yield* until(() => sources.length === 2, TestClock.adjust(100));
      const id = yield* pending.submit;
      expect(id.startsWith("unbound-1/")).toBe(true);
      expect(sources[0]!.sends).toHaveLength(0);
    }),
  ));

test("continuous identity validation closes an invalid acquired source before refusing it", () =>
  runClock(
    Effect.gen(function* () {
      let fixture: SourceFixture | undefined;
      const result = yield* Effect.result(
        Renewal.makeContinuous({
          open: Effect.gen(function* () {
            fixture = yield* sourceFixture("x".repeat(1025));
            return { source: fixture.source, lifetime: "Infinity" };
          }),
        }),
      );
      expect(result._tag).toBe("Failure");
      expect(fixture?.status()).toMatchObject({ closes: 1, finalized: true });
    }),
  ));

test("source cleanup with unresolved publications stays detailed after continuous close", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Renewal.makeContinuous({
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture("incomplete");
          return {
            source: {
              ...fixture.source,
              close: fixture.source.close.pipe(
                Effect.map((cleanup) => ({
                  ...cleanup,
                  lease: {
                    ...cleanup.lease,
                    unresolvedPublications: ["publication"],
                    localErrors: [ReactorError.fromCode("InvalidState", "unresolved")],
                  },
                })),
              ),
            },
            lifetime: "Infinity" as const,
          };
        }),
      });
      const summary = yield* handle.close;
      expect(summary.retained).toHaveLength(1);
      expect(summary.retained[0]?.cleanup?.lease.unresolvedPublications).toEqual(["publication"]);
      expect(summary.omittedComplete.attachedDetached).toBe(0n);
    }),
  ));

test("a selected preparation never rebinds to a reused physical ID after history eviction", () =>
  runClock(
    Effect.gen(function* () {
      const prework = yield* gate;
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.makeContinuous({
        lead: 0,
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(
            "reused",
            sources.length === 0 ? { prework: () => prework.wait } : {},
          );
          sources.push(fixture);
          return { source: fixture.source, lifetime: 200 };
        }),
      });
      const pending = yield* handle.engine.prepare(request());
      const attempted = yield* Effect.result(pending.submit).pipe(Effect.forkScoped);
      yield* until(() => sources[0]!.plans.length === 1);
      yield* until(() => sources.length === 2, TestClock.adjust(100));
      yield* prework.release;
      const refused = yield* Fiber.join(attempted);
      expect(refused._tag).toBe("Failure");
      expect(sources.every((source) => source.sends.length === 0)).toBe(true);
      const retried = yield* Effect.result(pending.submit);
      expect(retried._tag === "Failure" && refusal(retried.failure)).toBe("SessionRetired");
      // This adapter violates the opt-in precondition: a newly invoked raw-ID
      // request cannot be checked against historical identities already evicted.
      yield* handle.engine.enqueueOnSource!(request(), "reused");
      expect(sources[1]!.sends).toHaveLength(1);
    }),
  ));

// Review of 4443f80: a selection fenced to a still-live source was refused as retired.
test("a selected preparation refuses a changed route while its source is still live", () =>
  runClock(
    Effect.gen(function* () {
      const sources: SourceFixture[] = [];
      let preworks = 0;
      let prepared = false;
      const handle = yield* Renewal.makeContinuous({
        lead: 500,
        onRenewal: (event) =>
          Effect.sync(() => {
            if (event._tag === "Prepared") prepared = true;
          }),
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(`selected-${sources.length}`, {
            prework: () =>
              ++preworks === 1
                ? Effect.fail(failure("not-submitted", "upload failed"))
                : Effect.void,
          });
          sources.push(fixture);
          return { source: fixture.source, lifetime: 1000 };
        }),
      });
      const pending = yield* handle.engine.prepare(request());
      expect((yield* Effect.result(pending.submit))._tag).toBe("Failure");
      // The prepared replacement is now preferred; the selected source is live.
      yield* until(() => prepared, TestClock.adjust(100));
      expect((yield* handle.engine.state).sessions).toHaveLength(2);
      const retried = yield* Effect.result(pending.submit);
      expect(retried._tag === "Failure" && refusal(retried.failure)).toBe("RouteChanged");
      expect(sources.every((source) => source.sends.length === 0)).toBe(true);
    }),
  ));

test("simultaneously live duplicate physical IDs refuse and close the attempted replacement", () =>
  runClock(
    Effect.gen(function* () {
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.makeContinuous({
        lead: 500,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture("duplicate");
          sources.push(fixture);
          return { source: fixture.source, lifetime: 1000 };
        }),
      });
      yield* until(() => sources[1]?.status().finalized === true, TestClock.adjust(100));
      yield* handle.engine.stopRenewal;
      expect(sources[0]!.status().closed).toBe(false);
      expect(sources[1]!.status().closes).toBe(1);
      expect((yield* handle.engine.state).sessions).toHaveLength(1);
      expect((yield* handle.close).totalRetirements).toBe(2n);
    }),
  ));

test("unresolved ledger exhaustion refuses before open and still fits both remaining owners", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.makeContinuous({
        lead: 500,
        maxUnresolvedCleanups: 3,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(`unconfirmed-${++opened}`);
          return {
            source: {
              ...fixture.source,
              close: fixture.source.close.pipe(
                Effect.map((cleanup) => ({
                  ...cleanup,
                  lease: { ...cleanup.lease, allocation: "unknown" as const },
                })),
              ),
            },
            lifetime: 1000,
          };
        }),
      });
      const terminal = yield* handle.engine.failure.pipe(Effect.forkScoped);
      yield* until(() => terminal.pollUnsafe() !== undefined, TestClock.adjust(100));
      const failure = yield* Fiber.join(terminal);
      expect(failure.reason._tag).toBe("Overflow");
      expect(opened).toBe(3);
      const summary = yield* handle.close;
      expect(summary.exhausted).toBe(true);
      expect(summary.retained).toHaveLength(3);
      expect(summary.retained.every((row) => row.disposition === "incomplete")).toBe(true);
    }),
  ));

test("unknown unsequenced outcomes remain factual after successful physical cleanup", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.makeContinuous({
        retainedSuccessfulCleanups: 0,
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture(
            `unknown-${++opened}`,
            opened === 1 ? { execute: () => Effect.fail(failure("unknown")) } : {},
          );
          return {
            source: {
              ...fixture.source,
              prepareRouted: (plan, hooks) =>
                fixture.source.prepareRouted(plan, {
                  ...hooks,
                  result: (id, result) =>
                    Effect.gen(function* () {
                      yield* hooks?.result?.(id, result) ?? Effect.void;
                      yield* hooks?.result?.(id, result) ?? Effect.void;
                    }),
                }),
              close: fixture.source.close.pipe(
                Effect.map((cleanup) => ({
                  ...cleanup,
                  lease: {
                    ...cleanup.lease,
                    ownership: "owned" as const,
                    remote: {
                      ...cleanup.lease.remote,
                      attempted: true,
                      confirmed: true,
                      evidence: "absent" as const,
                    },
                  },
                })),
              ),
            },
            lifetime: "Infinity",
          };
        }),
      });
      const submitted = yield* handle.engine.prepare(request());
      expect((yield* Effect.result(submitted.submit))._tag).toBe("Failure");
      yield* until(() => opened === 2);
      const summary = yield* handle.close;
      expect(summary.retained).toHaveLength(1);
      expect(summary.retained[0]?.retirement.unknownSubmissions).toBe(1n);
      expect(summary.retained[0]?.disposition).toBe("incomplete");
      expect(summary.retained[0]?.cleanup?.lease.localClosed).toBe(true);
      expect(summary.retained[0]?.cleanup?.lease.remote.confirmed).toBe(true);
      expect(summary.retained[0]?.retirement.accounting).toBe("settled");
      expect(summary.retained[0]?.retirement.errors).toEqual([]);
      expect((yield* Effect.result(submitted.submit))._tag).toBe("Failure");
    }),
  ));

test("unknown failed acquisitions reserve cleanup capacity before an attempted retry", () =>
  runClock(
    Effect.gen(function* () {
      let attempts = 0;
      const handle = yield* Renewal.makeContinuous({
        lead: 9000,
        maxUnresolvedCleanups: 2,
        open: Effect.gen(function* () {
          attempts++;
          const fixture = yield* sourceFixture(`acquisition-${attempts}`);
          if (attempts > 1)
            return yield* AcquisitionFailure.from(
              ReactorError.fromCode("Disconnected", "allocation unknown"),
              { ...fixture.cleanup.lease, allocation: "unknown" },
            );
          return { source: fixture.source, lifetime: 10000 };
        }),
      });
      const terminal = yield* handle.engine.failure.pipe(Effect.forkScoped);
      yield* until(() => terminal.pollUnsafe() !== undefined, TestClock.adjust(100));
      expect((yield* Fiber.join(terminal)).reason._tag).toBe("Overflow");
      expect(attempts).toBe(2);
      const summary = yield* handle.close;
      const failed = summary.retained.find((row) => row.cleanup?.lease.allocation === "unknown");
      expect(failed?.retirement).toMatchObject({
        accounting: "not-applicable",
        affinity: "not-applicable",
        unknownSubmissions: 0n,
      });
    }),
  ));

test("accepted history reserves its final entry before dispatch and rejects further admissions", () =>
  runClock(
    Effect.gen(function* () {
      const last = yield* gate;
      let sent = 0;
      const handle = yield* Renewal.makeContinuous({
        open: Effect.gen(function* () {
          const fixture = yield* sourceFixture("history", {
            execute: () =>
              Effect.gen(function* () {
                const id = ClipId.make(`accepted-${++sent}`);
                if (sent === 4096) yield* last.wait;
                return id;
              }),
          });
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      });
      for (let index = 0; index < 4095; index++) yield* handle.engine.enqueue(request());
      const pending = yield* handle.engine.enqueue(request()).pipe(Effect.forkScoped);
      yield* until(() => sent === 4096);
      const refused = yield* Effect.result(handle.engine.enqueue(request()));
      expect(refused._tag === "Failure" && refusal(refused.failure)).toBe("SubmissionCapacity");
      expect(sent).toBe(4096);
      yield* last.release;
      yield* Fiber.join(pending);
      const stillFull = yield* Effect.result(handle.engine.enqueue(request()));
      expect(stillFull._tag === "Failure" && refusal(stillFull.failure)).toBe("SubmissionCapacity");
    }),
  ));

test("continuous Started history accepts duplicates at capacity and refuses one unseen identity", () =>
  runClock(
    Effect.gen(function* () {
      let fixture: SourceFixture | undefined;
      const handle = yield* Renewal.makeContinuous({
        open: Effect.gen(function* () {
          fixture = yield* sourceFixture("started-history");
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      });
      yield* handle.engine.stopRenewal;
      const source = fixture!;
      const observation = yield* handle.engine.observe({ capacity: 4096 });
      let observed = 0;
      yield* observation.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event._tag === "Started") observed++;
          }),
        ),
        Effect.forkScoped,
      );
      for (let index = 0; index < 4096; index++) {
        yield* source.emit({
          _tag: "Started",
          clipId: ClipId.make(`started-${index}`),
          durationSeconds: 1,
          at: 0,
        });
        yield* Effect.yieldNow;
      }
      yield* until(() => observed === 4096);
      yield* source.emit({
        _tag: "Started",
        clipId: ClipId.make("started-0"),
        durationSeconds: 1,
        at: 0,
      });
      yield* until(() => observed === 4097);
      yield* source.emit({
        _tag: "Started",
        clipId: ClipId.make("started-overflow"),
        durationSeconds: 1,
        at: 0,
      });
      expect((yield* handle.engine.failure).reason._tag).toBe("Overflow");
      const summary = yield* handle.close;
      expect(summary.retained[0]?.retirement.errors[0]?.reason._tag).toBe("Overflow");
    }),
  ));

test("completed scopes and recovery fibers detach across repeated reconnects", () =>
  runClock(
    Effect.gen(function* () {
      const parent = yield* Scope.make();
      let sourceScope: Scope.Scope | undefined;
      let fixture: SourceFixture | undefined;
      const handle = yield* Renewal.makeContinuous({
        open: Effect.gen(function* () {
          sourceScope = yield* Effect.scope;
          fixture = yield* sourceFixture("reconnect-scope");
          return { source: fixture.source, lifetime: "Infinity" };
        }),
      }).pipe(Scope.provide(parent));
      const finalizers = (scope: Scope.Scope) =>
        scope.state._tag === "Open"
          ? (scope.state.finalizers?.size ?? (scope.state.finalizer === undefined ? 0 : 1))
          : 0;
      const parentBaseline = finalizers(parent);
      const sourceBaseline = finalizers(sourceScope!);
      for (let generation = 2n; generation <= 12n; generation++) {
        yield* fixture!.failVideo(ReactorError.fromCode("Disconnected", "reconnect fixture"));
        yield* untilEffect(
          handle.mediaState.pipe(
            Effect.map((state) => state._tag === "Ready" && state.generation === generation),
          ),
        );
        yield* Effect.yieldNow;
        expect(finalizers(parent)).toBe(parentBaseline);
        expect(finalizers(sourceScope!)).toBe(sourceBaseline);
      }
      yield* handle.close;
      expect(sourceScope?.state._tag).toBe("Closed");
      yield* Scope.close(parent, Exit.void);
    }),
  ));
