import { expect, test } from "bun:test";
import { Effect, Fiber, Option, Result } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import { ClipId, PolicyFailure } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { AcquisitionFailure } from "../../src/session/index.js";
import {
  failure,
  gate,
  member,
  readyState,
  record,
  request,
  run,
  runClock,
  sourceFixture,
  until,
  untilEffect,
} from "./SourceFixture.js";
import { renewalFixture } from "./RenewalFixture.js";

test("failed and interrupted acquisition immediately join their owned source scopes", () =>
  run(
    Effect.gen(function* () {
      let finalized = 0,
        entered = false;
      const failed = yield* Effect.result(
        Renewal.make({
          open: Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                finalized++;
              }),
            );
            const fixture = yield* sourceFixture("cannot-pause", {
              autoplay: () => Effect.fail(failure("replied")),
            });
            return { source: fixture.source, maxSeconds: 90 };
          }),
        }),
      );
      expect(Result.isFailure(failed)).toBe(true);
      if (Result.isFailure(failed)) {
        expect(failed.failure).toBeInstanceOf(AcquisitionFailure);
        if (failed.failure instanceof AcquisitionFailure)
          expect(failed.failure.cleanup.ownership).toBe("attached");
      }
      expect(finalized).toBe(1);
      const acquiring = yield* Renewal.make({
        open: Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              finalized++;
            }),
          );
          entered = true;
          return yield* Effect.never;
        }),
      }).pipe(Effect.forkScoped);
      yield* until(() => entered);
      yield* Fiber.interrupt(acquiring);
      expect(finalized).toBe(2);
    }),
  ));

test("logical preparation is inert and selects the live source only when submitted after renewal", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, warm } = yield* renewalFixture();
      const metadata = { original: "before renewal" };
      const input = member("prepared", true, "final", {
        prompt: "captured before renewal",
        metadata,
      });
      const prepared = yield* handle.engine.prepare(input);
      metadata.original = "mutated";
      expect(sources[0]!.plans).toEqual([]);
      expect(sources[0]!.sends).toEqual([]);
      expect(yield* handle.sequences.get("prepared")).toBeUndefined();
      yield* warm;
      const nextClip = yield* handle.engine.enqueue(member("prime"));
      yield* sources[1]!.setState(readyState({ ready: [record(nextClip)] }));
      yield* until(() => sources[0]!.status().closed, TestClock.adjust(100));
      const first = yield* prepared.submit;
      expect(yield* prepared.submit).toBe(first);
      expect(sources[0]!.sends).toEqual([]);
      expect(sources[1]!.sends.map((plan) => plan.request.prompt)).toEqual([
        request().prompt,
        "captured before renewal",
      ]);
      expect(sources[1]!.sends[1]!.request.metadata).toEqual({ original: "before renewal" });
      expect((yield* prepared.state)._tag).toBe("Completed");
    }),
  ));

test("continuation and insertion remain on the old owner while independent requests use a warm replacement", () =>
  runClock(
    Effect.gen(function* () {
      const predecessor = record("predecessor"),
        anchor = record("anchor");
      const { handle, sources, warm } = yield* renewalFixture((index) =>
        index === 0
          ? {
              initial: readyState({
                queued: [anchor],
                generationOrder: [anchor.clipId],
                continuable: [predecessor.clipId],
              }),
            }
          : {},
      );
      yield* warm;
      const local = yield* handle.engine.enqueue(
        request({ continueFrom: predecessor.clipId, before: anchor.clipId, position: 0 }),
      );
      const next = yield* handle.engine.enqueue(request({ position: 9999 }));
      expect(sources[0]!.accepted.map((clip) => clip.clipId)).toEqual([local]);
      expect(sources[0]!.sends[0]!.position).toBe(0);
      expect(sources[0]!.sends[0]!.request.continueFrom).toBe(predecessor.clipId);
      expect(sources[1]!.accepted.map((clip) => clip.clipId)).toEqual([next]);
      expect(sources[1]!.sends[0]!.position).toBe(9999);
    }),
  ));

test("a sequence conflicting with a warm-source anchor or continuation sends nothing", () =>
  runClock(
    Effect.gen(function* () {
      const { handle, sources, warm } = yield* renewalFixture();
      yield* handle.engine.enqueue(member("old", false));
      yield* warm;
      const foreign = record("foreign-warm");
      yield* sources[1]!.setState(
        readyState({
          queued: [foreign],
          generationOrder: [foreign.clipId],
          continuable: [foreign.clipId],
        }),
      );
      for (const field of ["before", "continueFrom"] as const) {
        const result = yield* Effect.result(
          handle.engine.enqueue(member("old", true, "last", { [field]: foreign.clipId })),
        );
        expect(
          Result.isFailure(result) &&
            result.failure instanceof PolicyFailure &&
            result.failure.reason,
        ).toBe("owner_conflict");
        expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
      }
      expect(sources[0]!.sends).toHaveLength(1);
      expect(sources[1]!.plans).toEqual([]);
      expect((yield* handle.sequences.get("old"))?.status).toBe("open");
    }),
  ));

test("before position is revalidated after prework and a changed route never commits", () =>
  run(
    Effect.gen(function* () {
      const entered = yield* gate,
        held = yield* gate;
      const anchor = record("anchor"),
        preceding = record("inserted-later");
      const { handle, sources } = yield* renewalFixture(() => ({
        initial: readyState({ queued: [anchor], generationOrder: [anchor.clipId] }),
        prework: () => entered.release.pipe(Effect.andThen(held.wait)),
      }));
      const prepared = yield* handle.engine.prepare(
        member("moving-anchor", true, "last", { before: anchor.clipId }),
      );
      const pending = yield* prepared.submit.pipe(Effect.result, Effect.forkScoped);
      yield* entered.wait;
      yield* sources[0]!.setState(
        readyState({
          queued: [preceding, anchor],
          generationOrder: [preceding.clipId, anchor.clipId],
        }),
      );
      yield* held.release;
      const outcome = yield* Fiber.join(pending);
      expect(
        Result.isFailure(outcome) &&
          outcome.failure instanceof PolicyFailure &&
          outcome.failure.reason,
      ).toBe("route_changed");
      expect(sources[0]!.sends).toEqual([]);
      expect(yield* handle.sequences.get("moving-anchor")).toBeUndefined();
      expect((yield* prepared.state)._tag).toBe("Prepared");
      yield* prepared.submit;
      expect(sources[0]!.sends[0]!.position).toBe(1);
    }),
  ));

test("cancelling prework releases provisional resources and leaves no sequence or delayed dispatch", () =>
  run(
    Effect.gen(function* () {
      const entered = yield* gate,
        held = yield* gate;
      let releases = 0;
      const { handle, sources } = yield* renewalFixture(() => ({
        prework: () =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                releases++;
              }),
            );
            yield* entered.release;
            yield* held.wait;
          }),
      }));
      const prepared = yield* handle.engine.prepare(member("prework"));
      const caller = yield* prepared.submit.pipe(Effect.forkScoped);
      yield* entered.wait;
      yield* Fiber.interrupt(caller);
      yield* held.release;
      yield* Effect.yieldNow;
      expect(releases).toBe(1);
      expect(sources[0]!.sends).toEqual([]);
      expect((yield* prepared.state)._tag).toBe("Prepared");
      expect(yield* handle.sequences.get("prework")).toBeUndefined();
      yield* prepared.submit;
      expect(sources[0]!.sends).toHaveLength(1);
      expect(releases).toBe(2);
    }),
  ));

test("caller cancellation after commit keeps one dispatch and records final acceptance before sealing", () =>
  run(
    Effect.gen(function* () {
      const entered = yield* gate,
        held = yield* gate;
      const { handle, sources } = yield* renewalFixture(() => ({
        execute: (_, accept) =>
          entered.release.pipe(Effect.andThen(held.wait), Effect.andThen(accept)),
      }));
      const prepared = yield* handle.engine.prepare(member("committed"));
      const caller = yield* prepared.submit.pipe(Effect.forkScoped);
      yield* entered.wait;
      expect((yield* prepared.state)._tag).toBe("Committed");
      expect((yield* handle.sequences.get("committed"))?.pendingCount).toBe(1);
      yield* Fiber.interrupt(caller);
      yield* held.release;
      const accepted = yield* prepared.submit;
      expect(yield* prepared.submit).toBe(accepted);
      expect(sources[0]!.sends).toHaveLength(1);
      expect(sources[0]!.status().results).toBe(1);
      expect((yield* handle.sequences.get("committed"))?.status).toBe("sealed");
      expect((yield* handle.sequences.get("committed"))?.acceptedCount).toBe(1);
      const late = yield* Effect.result(handle.engine.enqueue(member("committed", false, "late")));
      expect(
        Result.isFailure(late) && late.failure instanceof PolicyFailure && late.failure.reason,
      ).toBe("sequence_sealed");
    }),
  ));

test("concurrent sequence members share their owner and sealing waits for earlier pending work", () =>
  run(
    Effect.gen(function* () {
      const entered = yield* gate,
        held = yield* gate;
      const { handle, sources } = yield* renewalFixture(() => ({
        execute: (plan, accept) =>
          plan.request.sequence?.final === false
            ? entered.release.pipe(Effect.andThen(held.wait), Effect.andThen(accept))
            : accept,
      }));
      const first = yield* handle.engine.prepare(member("concurrent", false));
      const last = yield* handle.engine.prepare(member("concurrent", true));
      const pending = yield* first.submit.pipe(Effect.forkScoped);
      yield* entered.wait;
      yield* last.submit;
      const sealing = yield* handle.sequences.get("concurrent");
      expect(sealing?.owner).toBe("source-1");
      expect(sealing?.status).toBe("open");
      expect(sealing?.sealRequested).toBe(true);
      expect(sealing?.pendingCount).toBe(1);
      yield* held.release;
      yield* Fiber.join(pending);
      expect(sources[0]!.sends).toHaveLength(2);
      expect(new Set(sources[0]!.accepted.map((clip) => clip.clipId)).size).toBe(2);
      expect((yield* handle.sequences.get("concurrent"))?.status).toBe("sealed");
      expect((yield* handle.sequences.get("concurrent"))?.acceptedCount).toBe(2);
    }),
  ));

test("a definitive rejected final member seals the sequence and frees the renewal boundary", () =>
  runClock(
    Effect.gen(function* () {
      const rejected = failure("replied", "provider refused final");
      const { handle, sources, warm } = yield* renewalFixture((index) =>
        index === 0 ? { execute: () => Effect.fail(rejected) } : {},
      );
      const outcome = yield* Effect.result(handle.engine.enqueue(member("rejected")));
      expect(Result.isFailure(outcome) && outcome.failure).toBe(rejected);
      expect((yield* handle.sequences.get("rejected"))?.status).toBe("sealed");
      expect((yield* handle.sequences.get("rejected"))?.rejectedCount).toBe(1);
      yield* warm;
      const next = yield* handle.engine.enqueue(member("next"));
      expect(sources[1]!.accepted.map((clip) => clip.clipId)).toEqual([next]);
      yield* sources[1]!.setState(readyState({ ready: [record(next)] }));
      yield* until(() => sources[0]!.status().closed, TestClock.adjust(100));
    }),
  ));

test("unknown commit outcome remains indeterminate through retirement, late events and explicit release", () =>
  run(
    Effect.gen(function* () {
      const unknown = failure("unknown", "reply lost after commit");
      const { handle, sources, renewals } = yield* renewalFixture((index) =>
        index === 0 ? { execute: () => Effect.fail(unknown) } : {},
      );
      const prepared = yield* handle.engine.prepare(member("unknown"));
      const first = yield* Effect.result(prepared.submit);
      expect(Result.isFailure(first) && first.failure).toBe(unknown);
      yield* until(
        () => sources.length === 2 && renewals.some((event) => event._tag === "Replaced"),
      );
      expect((yield* handle.sequences.get("unknown"))?.status).toBe("indeterminate");
      expect((yield* handle.sequences.get("unknown"))?.indeterminateCount).toBe(1);
      expect((yield* handle.sequences.get("unknown"))?.pendingCount).toBe(0);
      expect(Result.isFailure(yield* Effect.result(handle.sequences.release("unknown")))).toBe(
        true,
      );
      expect(yield* Effect.result(prepared.submit)).toEqual(first);
      yield* sources[0]!.emit({
        _tag: "Started",
        clipId: ClipId.make("late"),
        durationSeconds: 1,
        at: 1,
      });
      expect((yield* handle.engine.state).playing).toEqual(Option.none());
      expect(sources.flatMap((source) => source.sends)).toHaveLength(1);
      expect(sources[0]!.status().reconnects).toBe(0);
      yield* handle.sequences.acknowledgeIndeterminate("unknown");
      yield* handle.sequences.release("unknown");
      expect(yield* handle.sequences.get("unknown")).toBeUndefined();
    }),
  ));

test("close joins pending commit accounting and preserves each canonical attached cleanup exactly once", () =>
  run(
    Effect.gen(function* () {
      const entered = yield* gate;
      const { handle, sources } = yield* renewalFixture(() => ({
        execute: () => entered.release.pipe(Effect.andThen(Effect.never)),
      }));
      const prepared = yield* handle.engine.prepare(member("closing"));
      const caller = yield* prepared.submit.pipe(Effect.result, Effect.forkScoped);
      yield* entered.wait;
      const report = yield* handle.close;
      expect(report.sessions).toEqual([sources[0]!.cleanup]);
      expect(report.sessions[0]!.lease).toBe(sources[0]!.cleanup.lease);
      expect(report.sessions[0]!.lease.remote.attempted).toBe(false);
      expect(report.sessions[0]!.policy).toEqual([]);
      expect(yield* handle.close).toBe(report);
      expect(yield* handle.cleanup).toEqual(Option.some(report));
      expect(sources[0]!.status()).toMatchObject({
        closed: true,
        finalized: true,
        closes: 1,
        results: 1,
      });
      const outcome = yield* Fiber.join(caller);
      expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("unknown");
      expect((yield* handle.sequences.get("closing"))?.status).toBe("indeterminate");
      expect((yield* handle.mediaState)._tag).toBe("Closed");
    }),
  ));

test("pauseAndStop applies explicit policy in order and mutation routing uses the unique active owner", () =>
  runClock(
    Effect.gen(function* () {
      const a = record("old"),
        b = record("warm");
      const { handle, sources, warm } = yield* renewalFixture((index) => ({
        initial: readyState(
          index === 0
            ? { queued: [a], generationOrder: [a.clipId] }
            : { queued: [b], generationOrder: [b.clipId] },
        ),
      }));
      yield* warm;
      yield* handle.engine.pauseAndStop;
      for (const source of sources)
        expect(source.controls.slice(-2)).toEqual([
          { command: "autoplay", value: false },
          { command: "stop" },
        ]);
      yield* handle.engine.move(b.clipId, 9999, "generation");
      expect(sources[1]!.controls.at(-1)).toEqual({
        command: "move",
        value: { clipId: b.clipId, position: 9998 },
      });
      expect(yield* handle.engine.remove(b.clipId)).toBe("generation");
      const missing = yield* Effect.result(handle.engine.remove(ClipId.make("missing")));
      expect(
        Result.isFailure(missing) &&
          missing.failure instanceof PolicyFailure &&
          missing.failure.reason,
      ).toBe("not_found");
      const before = sources.flatMap((source) => source.controls).length;
      const busy = yield* Effect.result(handle.engine.setCanvas("9:16"));
      expect(
        Result.isFailure(busy) && busy.failure instanceof PolicyFailure && busy.failure.reason,
      ).toBe("busy");
      expect(sources.flatMap((source) => source.controls)).toHaveLength(before);
    }),
  ));

test("foreign startup playback and incomplete snapshots prohibit canvas mutation", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources } = yield* renewalFixture(() => ({
        initial: readyState({
          playing: Option.some({
            clipId: ClipId.make("foreign"),
            record: Option.none(),
            startedAt: Option.none(),
          }),
        }),
      }));
      for (const fields of [
        {},
        { playing: Option.none(), availability: "Synchronizing" as const },
      ]) {
        yield* sources[0]!.update(fields);
        const outcome = yield* Effect.result(handle.engine.setCanvas("1:1"));
        expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("not-submitted");
      }
      expect(sources[0]!.controls.some((call) => call.command === "canvas")).toBe(false);
      yield* sources[0]!.setState(readyState());
      yield* handle.engine.setCanvas("9:16");
      expect((yield* handle.engine.state).canvas).toEqual(Option.some("9:16"));
    }),
  ));

test("frame failure exposes recovery without an application control reader and reconnect preserves queued work", () =>
  run(
    Effect.gen(function* () {
      const held = yield* gate;
      const { handle, sources, renewals } = yield* renewalFixture(() => ({ reconnect: held.wait }));
      const accepted = yield* handle.engine.enqueue(member("retained", false));
      yield* sources[0]!.failVideo(
        new ReactorError({ code: "Disconnected", message: "frame receiver ended" }),
      );
      yield* untilEffect(
        handle.mediaState.pipe(Effect.map((state) => state._tag === "Recovering")),
      );
      expect(sources[0]!.sends).toHaveLength(1);
      expect(sources[0]!.status().closed).toBe(false);
      yield* held.release;
      yield* until(() => renewals.some((event) => event._tag === "Reconnected"));
      expect(sources).toHaveLength(1);
      expect(sources[0]!.status().reconnects).toBe(1);
      expect((yield* handle.engine.state).generationOrder).toContain(accepted);
      expect(yield* handle.mediaState).toEqual({
        _tag: "Ready",
        sessionId: "source-1",
        generation: 2n,
      });
    }),
  ));

test("queued removal and committed work cannot deadlock recovery", () =>
  run(
    Effect.gen(function* () {
      const held = yield* gate,
        entered = yield* gate;
      const { handle, sources, renewals } = yield* renewalFixture(() => ({
        execute: (plan, accept) =>
          plan.request.metadata.held === true
            ? entered.release.pipe(Effect.andThen(held.wait), Effect.andThen(accept))
            : accept,
      }));
      const first = yield* handle.engine.enqueue(request());
      const pending = yield* handle.engine
        .enqueue(request({ metadata: { held: true } }))
        .pipe(Effect.forkScoped);
      yield* entered.wait;
      const removal = yield* handle.engine.remove(first).pipe(Effect.result, Effect.forkScoped);
      yield* sources[0]!.failVideo(
        new ReactorError({
          code: "Disconnected",
          message: "reconnect during an outstanding mutation",
        }),
      );
      yield* held.release;
      yield* Fiber.join(pending).pipe(Effect.timeout(1000));
      yield* until(() => renewals.some((event) => event._tag === "Reconnected"));
      const outcome = yield* Fiber.join(removal).pipe(Effect.timeout(1000));
      if (Result.isFailure(outcome)) {
        // Either ordering is legal: admission before recovery succeeds; admission
        // during recovery is explicitly not submitted and requires a caller retry.
        expect(outcome.failure.context.outcome).toBe("not-submitted");
        expect(outcome.failure instanceof PolicyFailure && outcome.failure.reason).toBe(
          "session_recovering",
        );
        expect(yield* handle.engine.remove(first).pipe(Effect.timeout(1000))).toBe("generation");
      } else expect(outcome.success).toBe("generation");
      yield* handle.engine.setAutoplay(true).pipe(Effect.timeout(1000));
      expect(sources[0]!.status().reconnects).toBe(1);
      expect(sources[0]!.controls.filter((call) => call.command === "remove")).toHaveLength(1);
    }),
  ));

test("generation capacity is enforced before physical preparation and includes a true active build", () =>
  run(
    Effect.gen(function* () {
      const full = record("building");
      const { handle, sources } = yield* renewalFixture(() => ({
        initial: readyState({
          generationOrder: [full.clipId],
          building: Option.some({ record: full, startedAt: Option.some(0) }),
          capacities: { generation: 1, playout: 1 },
        }),
      }));
      const result = yield* Effect.result(handle.engine.enqueue(request()));
      expect(
        Result.isFailure(result) &&
          result.failure instanceof PolicyFailure &&
          result.failure.reason,
      ).toBe("queue_full");
      expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
      expect(sources[0]!.plans).toEqual([]);
      expect(sources[0]!.sends).toEqual([]);
    }),
  ));
