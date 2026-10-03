/**
 * `dropped`: what Reactor does with a connected session whose connection is
 * gone for good, as when the application that owns it crashes: no DELETE, and
 * nobody reconnecting. Reactor's docs say a session waits 30 s for a
 * connection, yet paid `unconnected` found a session nothing ever connected to
 * still `ACTIVE` 30 s in, and running to its cap. An application's sessions
 * may have no cap, so this one has none: only Reactor, or the key once the
 * window closes, ends it.
 *
 * An owner under Node on the isolated native host (in this process on
 * ReactorTest's peers, when rehearsed) creates the session on an uncapped
 * token, connects it and sets it up, plays nothing, and is killed with
 * SIGKILL. The API key reads the session every 2 s from the kill until a read
 * finds it `CLOSED`, or the second in a row finds it gone, or the window
 * closes 60 s after the kill. The key then ends it either way; one still
 * running then was not ended by Reactor within the window.
 *
 * The reviewed hold reserves 80 s from allocation, $2.80 at H3's per-second
 * rate: the owner reports it within 1 s of its creation and
 * must be connected within 10 s of that; the window closes 60 s after the
 * kill, or 67 s after the allocation if that is sooner, so a slow owner
 * shortens the window rather than lengthening the hold; its last read takes
 * 1 s; and the key's first two tries take 11 s to the second DELETE's answer.
 * A third try, after two unconfirmed, runs past that: nothing else ends an
 * uncapped session.
 */
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type * as H3Source from "reactor-effect-client/H3Source";
import type { Pieces } from "../Checks.js";
import type { DroppedRecord } from "../Evidence.js";
import * as Probes from "../Probes.js";
import { Run } from "../Run.js";
import { holdsFor } from "../Spend.js";
import { Target } from "../Target.js";

/** How long the owner may take to start and allocate: nothing bills before the allocation. */
const ownerWithinMs = 30_000;
/** How long after its allocation the owner must be connected: paid runs connected within 3.2 s. */
const connectWithinMs = 10_000;
/** How long after the kill the window runs: twice the 30 s Reactor documents. */
const watchMs = 60_000;
/** How long after its creation the owner's report of the session may reach this process. */
const reportedWithinMs = 1_000;
/** How long the watch's last read may take: the key's reads time out at 1 s. */
const lastReadMs = 1_000;
/** The key's first two tries, to the second DELETE's answer: a DELETE and a read of 3 s each, 2 s apart, and a DELETE. */
const twoTriesMs = 3_000 + 3_000 + 2_000 + 3_000;
/** The window closes this long after the allocation at the latest, so the session's hold holds. */
const windowWithinMs =
  (holdsFor("dropped")[0] ?? 0) * 1000 - reportedWithinMs - lastReadMs - twoTriesMs;

/**
 * Ends a session with the key, and again, twice at most and 2 s apart, while
 * its end is unconfirmed: nothing here trusts a cap to end it. Each try, a
 * DELETE and a read, takes up to 6 s. Each unconfirmed one goes in the
 * timeline with what its DELETE and its read met; a save that fails there
 * stops no try.
 */
const endWithKey = (inspector: CoordinatorClient.CoordinatorClient["Service"], sessionId: string) =>
  Effect.flatMap(Run, (run) =>
    inspector.terminate(sessionId).pipe(
      Effect.tap((termination) =>
        termination.confirmed
          ? Effect.void
          : run
              .mark(
                "end unconfirmed",
                `${sessionId}: DELETE ${termination.deleteStatus ?? "unanswered"}, then ${termination.state === null ? "no state" : Probes.keptText(termination.state)}`,
              )
              .pipe(Effect.ignore),
      ),
      Effect.repeat({
        schedule: Schedule.spaced("2 seconds"),
        until: (termination) => termination.confirmed,
        times: 2,
      }),
    ),
  );

/** The states read so far, and one more read: the last state runs on, or a new one begins. */
const withRead = (
  states: DroppedRecord["states"],
  state: string,
  atMs: number,
): DroppedRecord["states"] => {
  const last = states.at(-1);
  return last?.state === state
    ? [...states.slice(0, -1), { ...last, lastMs: atMs, reads: last.reads + 1 }]
    : [...states, { state, firstMs: atMs, lastMs: atMs, reads: 1 }];
};

/** How often the key reads a session it watches for its end. */
const watchEveryMs = 2_000;

/**
 * Reads a session with the key every 2 s from `fromAt` until a read finds it
 * `CLOSED`, or the second in a row finds it gone, since one read answered 404
 * may be the coordinator's slip; or until the read at `endsAt`, both Clock
 * times. `each` records every read, with its state as the evidence keeps it,
 * and each new state goes in the timeline. An end is timed at the read that
 * found the session `CLOSED`, or at the first of the reads in a row that found
 * it gone; `goneMs` is that first read while the last read found it gone.
 */
const watchForEnd = Effect.fnUntraced(function* <E>(
  pieces: Pieces,
  inspector: CoordinatorClient.CoordinatorClient["Service"],
  sessionId: string,
  fromAt: number,
  endsAt: number,
  each: (
    read: Result.Result<CoordinatorClient.Inspection, ReactorError>,
    state: string,
    atMs: number,
  ) => Effect.Effect<void, E>,
) {
  const run = yield* Run;
  let readAt = fromAt;
  let last: { readonly state: string; readonly known: boolean } | undefined;
  let goneMs: number | undefined;
  let endedMs: number | undefined;
  for (;;) {
    yield* pieces.sleepUntil(readAt - run.origin, endsAt);
    const read = yield* Effect.result(inspector.inspect(sessionId));
    const atMs = yield* run.now;
    const state = Result.isSuccess(read)
      ? Probes.keptText(read.success.state)
      : pieces.failedRead(read.failure);
    yield* each(read, state, atMs);
    if (state !== last?.state) yield* run.mark(`read ${state}`);
    last = { state, known: Result.isSuccess(read) || state === "gone" };
    if (state !== "gone") goneMs = undefined;
    if (CoordinatorClient.isTerminal(state)) {
      endedMs = goneMs ?? atMs;
      break;
    }
    if (state === "gone") {
      if (goneMs !== undefined) {
        endedMs = goneMs;
        break;
      }
      goneMs = atMs;
    }
    if (readAt >= endsAt) break;
    readAt = Math.min(readAt + watchEveryMs, endsAt);
  }
  return { last, goneMs, endedMs };
});

export const dropped = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const instant = (atMs: number) => DateTime.formatIso(DateTime.makeUnsafe(run.origin + atMs));
  const grant = yield* pieces.mint("dropped");
  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        const startedMs = yield* run.now;
        const initial: DroppedRecord = { startedMs, startedAt: instant(startedMs), states: [] };
        yield* run.update((evidence) => ({ ...evidence, dropped: initial }));
        const record = (change: (probe: DroppedRecord) => DroppedRecord) =>
          run.update((evidence) => ({ ...evidence, dropped: change(evidence.dropped ?? initial) }));
        // Saved before the owner starts, so even a crash leaves when its session could have been
        // made.
        yield* run.mark("owner started");

        // The session is held as soon as the owner reports it, before it connects, so one whose
        // owner fails before connecting is still ended with the key.
        const held = yield* Deferred.make<{
          readonly sessionId: string;
          readonly allocatedMs: number;
        }>();
        const onAllocated = (allocation: H3Source.Allocation) =>
          Effect.gen(function* () {
            const allocatedAt = yield* Clock.currentTimeMillis;
            grants.set(allocation.sessionId, grant);
            yield* pieces.holding(allocation.sessionId, allocatedAt, undefined);
            const allocatedMs = allocatedAt - run.origin;
            yield* record((probe) => ({
              ...probe,
              sessionId: allocation.sessionId,
              allocatedAt: instant(allocatedMs),
            }));
            yield* Deferred.succeed(held, { sessionId: allocation.sessionId, allocatedMs });
          }).pipe(Effect.provideService(Run, run));
        const timeUp = Effect.gen(function* () {
          const allocated = yield* Deferred.await(held).pipe(
            Effect.timeoutOption(Duration.millis(ownerWithinMs)),
          );
          if (Option.isNone(allocated))
            return `the owner allocated nothing within ${ownerWithinMs / 1000} s`;
          yield* pieces.sleepUntil(
            allocated.value.allocatedMs + connectWithinMs,
            Number.POSITIVE_INFINITY,
          );
          return `the owner was not connected ${connectWithinMs / 1000} s after its allocation`;
        });
        const owner = yield* Effect.raceFirst(
          target.idleOwner(grant, { isolated: true, onAllocated }).pipe(
            Effect.mapError((error) => error.message),
            Effect.result,
          ),
          Effect.map(timeUp, Result.fail),
        );
        yield* pieces.judge("the owner connected", [
          Result.isSuccess(owner),
          Result.isFailure(owner) ? owner.failure : "",
        ]);
        if (Result.isFailure(owner)) return;
        const { sessionId, allocatedMs } = yield* Deferred.await(held);
        const { host, kill } = owner.success;
        const connectedMs = yield* run.now;
        yield* record((probe) => ({
          ...probe,
          ...(host === undefined ? {} : { ownerHost: host }),
          connectedMs,
        }));
        yield* run.mark("owner connected", host);

        // Killed as a crash kills it: its connection is gone for good, and nothing it held is
        // closed or terminated.
        const killedMs = yield* run.now;
        yield* kill;
        yield* record((probe) => ({ ...probe, killedMs, killedAt: instant(killedMs) }));
        yield* run.mark("owner killed");

        const inspector = yield* keyed;
        const windowEndsMs = Math.min(killedMs + watchMs, allocatedMs + windowWithinMs);
        yield* record((probe) => ({ ...probe, windowEndsMs }));
        const { last, endedMs } = yield* watchForEnd(
          pieces,
          inspector,
          sessionId,
          run.origin + killedMs,
          run.origin + windowEndsMs,
          (_read, state, atMs) =>
            record((probe) => ({ ...probe, states: withRead(probe.states, state, atMs) })),
        );

        // What the coordinator says of a session Reactor closed: why, if it says. One still
        // running is ended at once, since its hold leaves no room for another read.
        if (CoordinatorClient.isTerminal(last.state)) {
          const read = yield* Probes.readSession({
            apiUrl: target.apiUrl,
            sessionId,
            credential: target.apiKey,
          });
          const readMs = yield* run.now;
          yield* record((probe) => ({ ...probe, read: { atMs: readMs, ...read } }));
        }
        // The key ends the session whether or not Reactor did, so its end is confirmed as every
        // check confirms one. Ending a session Reactor has closed ends nothing more.
        const endRequestedMs = yield* run.now;
        const termination = yield* endWithKey(inspector, sessionId);
        const terminatedMs = yield* run.now;
        yield* pieces.closedWith(sessionId, endRequestedMs, { termination });
        if (endedMs !== undefined)
          yield* record((probe) => ({
            ...probe,
            ended: { by: "reactor", atMs: endedMs, at: instant(endedMs) },
          }));
        else if (termination.confirmed)
          yield* record((probe) => ({
            ...probe,
            ended: { by: "key", atMs: terminatedMs, at: instant(terminatedMs) },
          }));
        yield* pieces.judge("the watch completed", [
          last.known,
          `the last read of the session failed, with ${last.state}`,
        ]);
        yield* run.mark("dropped observed");
      }),
    // Whatever failed, the key ends the session the owner made, trying as above.
    pieces.endHeld(
      Effect.map(keyed, (coordinator) => ({
        ...coordinator,
        terminate: (sessionId: string) =>
          endWithKey(coordinator, sessionId).pipe(Effect.provideService(Run, run)),
      })),
    ),
  );
});
