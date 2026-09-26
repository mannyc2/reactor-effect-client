import { expect, test } from "vitest";
import { Effect, Fiber, Option, Result } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import { ClipId } from "../../src/orchestration/request.js";
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
  refusal,
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
            return { source: fixture.source, lifetime: "90 seconds" };
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

const refusedAcquisition = (sessionId: string) =>
  AcquisitionFailure.from(
    ReactorError.fromCode("Http", "session allocation refused"),
    Object.freeze({
      localClosed: true,
      allocation: "known",
      sessionId,
      remote: Object.freeze({
        attempted: true,
        responseReceived: true,
        confirmed: true,
        evidence: null,
        deleteStatus: 204,
        state: null,
      }),
      unpublishSubmitted: Object.freeze([]),
      unresolvedPublications: Object.freeze([]),
      localErrors: Object.freeze([]),
    }),
  );

test("an AcquisitionFailure from open fails make unchanged, with its cleanup by reference", () =>
  run(
    Effect.gen(function* () {
      const refused = refusedAcquisition("refused-first");
      const failed = yield* Effect.flip(Renewal.make({ open: Effect.fail(refused) }));
      expect(failed).toBe(refused);
      expect(AcquisitionFailure.is(failed) && failed.cleanup).toBe(refused.cleanup);
    }),
  ));

test("a replacement's AcquisitionFailure is the terminal failure, and cleanup keeps its report", () =>
  runClock(
    Effect.gen(function* () {
      const refused = refusedAcquisition("refused-replacement");
      let opened = 0;
      const handle = yield* Renewal.make({
        lead: "500 millis",
        reconnectTimeout: 50,
        open: Effect.gen(function* () {
          if (opened++ > 0) return yield* refused;
          const entry = yield* sourceFixture("expiring");
          return { source: entry.source, lifetime: "1 second" };
        }),
      });
      yield* TestClock.adjust(1_500);
      const terminal = yield* handle.engine.failure;
      expect(terminal).toBe(refused);
      expect(yield* handle.mediaState).toEqual({ _tag: "Failed", cause: refused });
      const report = yield* handle.close;
      expect(report.sessions.some(({ lease }) => lease === refused.cleanup)).toBe(true);
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
        expect(Result.isFailure(result) && refusal(result.failure)).toBe("OwnerConflict");
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
      expect(Result.isFailure(outcome) && refusal(outcome.failure)).toBe("RouteChanged");
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
      expect(Result.isFailure(late) && refusal(late.failure)).toBe("Sequence:sealed");
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

test("flattened queued, building, ready and playing records name their owning session", () =>
  runClock(
    Effect.gen(function* () {
      const a = record("old"),
        b = record("warm"),
        oldReady = record("old-ready"),
        warmReady = record("warm-ready"),
        warmBuilding = record("warm-building"),
        oldPlaying = record("old-playing");
      const { handle, warm } = yield* renewalFixture((index) => ({
        initial: readyState(
          index === 0
            ? {
                queued: [a],
                generationOrder: [a.clipId],
                ready: [oldReady],
                playing: Option.some({
                  clipId: oldPlaying.clipId,
                  record: Option.some(oldPlaying),
                  startedAt: Option.some(0),
                }),
              }
            : {
                queued: [b],
                generationOrder: [b.clipId, warmBuilding.clipId],
                building: Option.some({ record: warmBuilding, startedAt: Option.some(0) }),
                ready: [warmReady],
              },
        ),
      }));
      yield* warm;
      const state = yield* handle.engine.state;
      expect(state.queued.map((clip) => [clip.clipId, clip.sessionId])).toEqual([
        [a.clipId, "source-1"],
        [b.clipId, "source-2"],
      ]);
      expect(state.building.pipe(Option.map(({ record }) => record.sessionId))).toEqual(
        Option.some("source-2"),
      );
      expect(state.ready.map((clip) => [clip.clipId, clip.sessionId])).toEqual([
        [oldReady.clipId, "source-1"],
        [warmReady.clipId, "source-2"],
      ]);
      expect(
        state.playing.pipe(
          Option.flatMap(({ record }) => record),
          Option.map((clip) => clip.sessionId),
        ),
      ).toEqual(Option.some("source-1"));
    }),
  ));

test("an empty replacement is visible as the preferred session while the old queue drains", () =>
  runClock(
    Effect.gen(function* () {
      const oldReady = record("old-ready");
      const { handle, warm } = yield* renewalFixture((index) => ({
        initial: readyState(index === 0 ? { ready: [oldReady] } : {}),
      }));
      const before = yield* handle.engine.state;
      expect(before.sessions).toEqual([{ sessionId: "source-1", availability: "Ready" }]);
      expect(before.preferredSessionId).toEqual(Option.some("source-1"));
      expect(before.retiringSessionId).toEqual(Option.none());
      yield* warm;
      const during = yield* handle.engine.state;
      expect(during.sessions).toEqual([
        { sessionId: "source-1", availability: "Ready" },
        { sessionId: "source-2", availability: "Ready" },
      ]);
      expect(during.ready.map((clip) => clip.clipId)).toEqual([oldReady.clipId]);
      expect(during.preferredSessionId).toEqual(Option.some("source-2"));
      expect(during.retiringSessionId).toEqual(Option.some("source-1"));
    }),
  ));

test("pauseAndStop applies explicit policy and moves refuse ranks in another session", () =>
  runClock(
    Effect.gen(function* () {
      const a = record("old"),
        b = record("warm"),
        warmReady = record("warm-ready");
      const { handle, sources, warm } = yield* renewalFixture((index) => ({
        initial: readyState(
          index === 0
            ? { queued: [a], generationOrder: [a.clipId], ready: [record("old-ready")] }
            : { queued: [b], generationOrder: [b.clipId], ready: [warmReady] },
        ),
      }));
      yield* warm;
      yield* handle.engine.pauseAndStop;
      for (const source of sources)
        expect(source.controls.slice(-2)).toEqual([
          { command: "autoplay", value: false },
          { command: "stop" },
        ]);
      const wrongGeneration = yield* Effect.result(handle.engine.move(b.clipId, 0, "generation"));
      expect(Result.isFailure(wrongGeneration) && refusal(wrongGeneration.failure)).toBe(
        "InvalidRequest",
      );
      const wrongPlayout = yield* Effect.result(handle.engine.move(warmReady.clipId, 0, "playout"));
      expect(Result.isFailure(wrongPlayout) && refusal(wrongPlayout.failure)).toBe(
        "InvalidRequest",
      );
      expect(sources[1]!.controls.filter((call) => call.command === "move")).toEqual([]);
      yield* handle.engine.move(b.clipId, 1, "generation");
      expect(sources[1]!.controls.at(-1)).toEqual({
        command: "move",
        value: { clipId: b.clipId, position: 0 },
      });
      expect(yield* handle.engine.remove(b.clipId)).toBe("generation");
      const missing = yield* Effect.result(handle.engine.remove(ClipId.make("missing")));
      expect(Result.isFailure(missing) && refusal(missing.failure)).toBe("NotFound");
      const before = sources.flatMap((source) => source.controls).length;
      const busy = yield* Effect.result(handle.engine.setCanvas("9:16"));
      expect(Result.isFailure(busy) && refusal(busy.failure)).toBe("Busy");
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
      yield* sources[0]!.failVideo(ReactorError.fromCode("Disconnected", "frame receiver ended"));
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
        ReactorError.fromCode("Disconnected", "reconnect during an outstanding mutation"),
      );
      yield* held.release;
      yield* Fiber.join(pending).pipe(Effect.timeout(1000));
      yield* until(() => renewals.some((event) => event._tag === "Reconnected"));
      const outcome = yield* Fiber.join(removal).pipe(Effect.timeout(1000));
      if (Result.isFailure(outcome)) {
        // Either ordering is legal: admission before recovery succeeds; admission
        // during recovery is explicitly not submitted and requires a caller retry.
        expect(outcome.failure.context.outcome).toBe("not-submitted");
        expect(refusal(outcome.failure)).toBe("SessionRecovering");
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
      expect(Result.isFailure(result) && refusal(result.failure)).toBe("QueueFull");
      expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
      expect(sources[0]!.plans).toEqual([]);
      expect(sources[0]!.sends).toEqual([]);
    }),
  ));
