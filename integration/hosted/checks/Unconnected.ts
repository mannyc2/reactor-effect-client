/**
 * `unconnected`: what becomes of a session nothing connects to, which
 * Reactor's docs leave open. Its Sessions page says billing runs "from
 * creation", and its Billing page that the meter starts when the session
 * reaches `ready`; neither says whether Reactor ends such a session, or when.
 * Nor do they say what a second create on a spent single-session token
 * answers.
 *
 * The check mints a token for one session capped at 60 s, allocates the
 * session without connecting, and at once sends a second create on the same
 * token. It then reads the session with the API key every 2 s until a read
 * finds it ended, or until its window closes, past every end Reactor
 * plausibly gives it: the cap counted from allocation, from `ACTIVE` or from
 * ready, and the 30 s after each that Reactor gives a connected session after
 * its last connection drops. The key then ends it either way, as it ends at
 * once any session the spent token allocated, and every end is confirmed.
 * What Reactor billed comes from its dashboard, against the window the
 * evidence records.
 *
 * Unlike every other check, its work runs past its session's cap: the cap is
 * what it watches.
 */
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as H3 from "reactor-effect-client/H3";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import type { Pieces } from "../Checks.js";
import type { UnconnectedRecord } from "../Evidence.js";
import * as Probes from "../Probes.js";
import { recorded, Run } from "../Run.js";
import { plans } from "../Spend.js";
import { Target } from "../Target.js";

/** How often the API key reads the session. */
const readEveryMs = 2_000;
/**
 * How long after the request the window allows for allocation, `ACTIVE` and
 * ready to come in: the create's own limit. Every paid run so far had its
 * session connected within 3.2 s of allocation.
 */
const startsWithinMs = 15_000;
/** The cap the token asks for, which bounds the one it is granted. */
const capMs = plans.unconnected.seconds * 1000;
/** How long Reactor gives a connected session after its last connection drops. */
const graceMs = 30_000;
/** How far the window runs past the latest end it allows for, for Reactor's own timers. */
const spareMs = 15_000;
/**
 * How long after the spent token's create a read must still find the session
 * running for its end to be taken for Reactor's own. An end that soon may be
 * that create's doing: every DELETE the paid runs so far confirmed read
 * `CLOSED` within 0.65 s of its request. The create counts from its answer,
 * or from the key's end of a session it allocated.
 */
const settlesMs = 10_000;

/**
 * Ends a session with the key, and again, twice at most and 2 s apart, while
 * its end is unconfirmed: nothing here trusts the cap to end it. Each try, a
 * DELETE and a read, takes up to 6 s. Each unconfirmed one goes in the
 * timeline with what its DELETE and its read met; a save that fails there
 * stops no try.
 */
const endWithKey = (inspector: Coordinator.Coordinator["Service"], sessionId: string) =>
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

/**
 * Creates a session on the token's signaling, and records it unless it is one
 * already held, as one step an interrupt waits for: one landing between the
 * reply and the record would leave a session nothing ends. The create's own
 * 15 s limit still holds, as a race runs its sides interruptibly.
 */
const allocate = Effect.fnUntraced(function* (
  pieces: Pieces,
  signaling: Coordinator.Signaling,
  grant: Coordinator.TokenGrant,
  grants: Map<string, Coordinator.TokenGrant>,
) {
  const allocation = yield* recorded(signaling.create({ name: H3.modelName }));
  const allocatedAt = yield* Clock.currentTimeMillis;
  if (!grants.has(allocation.sessionId)) {
    grants.set(allocation.sessionId, grant);
    yield* pieces.holding(allocation.sessionId, allocatedAt, allocatedAt + pieces.capMs(grant));
  }
  return allocation;
}, Effect.uninterruptible);

type States = UnconnectedRecord["states"];
type SpentToken = NonNullable<UnconnectedRecord["spentToken"]>;

/** The states read so far, and one more read: the last state runs on, or a new one begins. */
const withRead = (states: States, state: string, atMs: number): States => {
  const last = states.at(-1);
  return last?.state === state
    ? [...states.slice(0, -1), { ...last, lastMs: atMs, reads: last.reads + 1 }]
    : [...states, { state, firstMs: atMs, lastMs: atMs, reads: 1 }];
};

/** A reply body's key names and codes, as the evidence keeps them. */
const bodyOf = (body: unknown) => {
  const { keys, codes } = Probes.summarizeBody(body);
  return { keys, ...(codes === undefined ? {} : { codes }) };
};

const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

/**
 * A failed create as the evidence keeps it: its reason, outcome and status,
 * and its body's codes under whatever keys hold them. A reply that named no
 * session carries its body, redacted, in the failure's detail.
 */
const refusal = (error: ReactorError) => {
  const reason = error.reason;
  const http = reason._tag === "Http" ? reason : undefined;
  const detail =
    error.context.detail === undefined ? undefined : Redacted.value(error.context.detail);
  const body =
    http?.body !== undefined
      ? http.body.pipe(Redacted.value, decodeJson, Option.getOrUndefined)
      : Predicate.hasProperty(detail, "body")
        ? detail.body
        : undefined;
  return {
    answer: reason._tag,
    ...(error.context.outcome === undefined ? {} : { outcome: error.context.outcome }),
    ...(http?.status === undefined ? {} : { status: http.status }),
    ...Probes.summarizeRefusal(body),
  };
};

export const unconnected = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* Coordinator.Coordinator;
  const keyed = Coordinator.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const instant = (atMs: number) => DateTime.formatIso(DateTime.makeUnsafe(run.origin + atMs));
  const grant = yield* pieces.mint("unconnected");
  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        // The session's own seam, on purpose: its create allocates a session that nothing
        // owns, connects to or closes, and such a session is what this check asks about.
        // The session is recorded as the reply names it, so the key ends it however the
        // check ends.
        const signaling = coordinator.signaling(Effect.succeed(grant.jwt));
        const requestedMs = yield* run.now;
        const initial: UnconnectedRecord = {
          requestedMs,
          requestedAt: instant(requestedMs),
          states: [],
        };
        yield* run.update((evidence) => ({ ...evidence, unconnected: initial }));
        const record = (change: (probe: UnconnectedRecord) => UnconnectedRecord) =>
          run.update((evidence) => ({
            ...evidence,
            unconnected: change(evidence.unconnected ?? initial),
          }));
        // The record names the session, or keeps the create's failure, before an interrupt
        // can land.
        const { sessionId } = yield* allocate(pieces, signaling, grant, grants).pipe(
          Effect.tapError((error) => record((probe) => ({ ...probe, create: refusal(error) }))),
          Effect.tap((allocation) =>
            record((probe) => ({ ...probe, sessionId: allocation.sessionId })),
          ),
          Effect.uninterruptible,
        );
        yield* run.mark("allocated", sessionId);
        const recordSpent = (spentToken: SpentToken) =>
          record((probe) => ({ ...probe, spentToken })).pipe(
            Effect.andThen(run.mark("spent token answered", spentToken.answer)),
          );
        const inspector = yield* keyed;

        // A second create on the same token, sent while the first session is live.
        const sentMs = yield* run.now;
        const second = yield* allocate(pieces, signaling, grant, grants).pipe(Effect.result);
        const answeredMs = yield* run.now;
        // When whatever that create did had been done: its answer, or the key's end of a session
        // it allocated.
        let settledFromMs = answeredMs;
        if (Result.isSuccess(second) && second.success.sessionId === sessionId)
          // The session the token made, named again: nothing more was allocated, so the key
          // ends nothing here, and the watch goes on.
          yield* recordSpent({
            sentMs,
            answeredMs,
            answer: "the same session",
            ...bodyOf(second.success.reply),
            sessionId,
          });
        else if (Result.isSuccess(second)) {
          // A session nothing asked for, recorded as the reply named it: ended with the key at
          // once.
          const extra = second.success.sessionId;
          yield* run.mark("allocated", extra);
          yield* recordSpent({
            sentMs,
            answeredMs,
            answer: "allocated",
            ...bodyOf(second.success.reply),
            sessionId: extra,
          });
          const endRequestedMs = yield* run.now;
          const termination = yield* endWithKey(inspector, extra);
          settledFromMs = yield* run.now;
          yield* pieces.closedWith(extra, endRequestedMs, { termination });
        } else yield* recordSpent({ sentMs, answeredMs, ...refusal(second.failure) });

        // The key reads the session every 2 s until a read finds it ended, or until its window
        // closes, when it reads it once more.
        const windowEndsMs = requestedMs + startsWithinMs + capMs + graceMs + spareMs;
        yield* record((probe) => ({ ...probe, windowEndsMs }));
        const windowEndsAt = run.origin + windowEndsMs;
        let readAt = yield* Clock.currentTimeMillis;
        let last: { readonly state: string; readonly known: boolean } | undefined;
        let runningMs: number | undefined;
        // The first of the reads in a row that found the session gone.
        let goneMs: number | undefined;
        let endedMs: number | undefined;
        for (;;) {
          yield* pieces.sleepUntil(readAt - run.origin, windowEndsAt);
          const read = yield* Effect.result(inspector.inspect(sessionId));
          const atMs = yield* run.now;
          const state = Result.isSuccess(read)
            ? Probes.keptText(read.success.state)
            : pieces.failedRead(read.failure);
          yield* record((probe) => ({ ...probe, states: withRead(probe.states, state, atMs) }));
          if (state !== last?.state) yield* run.mark(`read ${state}`);
          last = { state, known: Result.isSuccess(read) || state === "gone" };
          if (Result.isSuccess(read) && !Coordinator.isTerminal(state)) {
            runningMs = atMs;
            goneMs = undefined;
          }
          // The SDK connects once the session publishes its capabilities and a transport.
          if (
            Result.isSuccess(read) &&
            read.success.hasCapabilities &&
            read.success.selectedTransport !== null
          )
            yield* record((probe) => ({ ...probe, connectableMs: probe.connectableMs ?? atMs }));
          if (Coordinator.isTerminal(state)) {
            endedMs = goneMs ?? atMs;
            break;
          }
          // One read answered 404 may be the coordinator's slip: a second in a row, or the read
          // at the end, tells.
          if (state === "gone") {
            if (goneMs !== undefined) {
              endedMs = goneMs;
              break;
            }
            goneMs = atMs;
          }
          if (readAt >= windowEndsAt) break;
          readAt = Math.min(readAt + readEveryMs, windowEndsAt);
        }

        // What the coordinator says of the session now: why it ended, if it says.
        const read = yield* Probes.readSession({
          apiUrl: target.apiUrl,
          sessionId,
          credential: target.apiKey,
        });
        const readMs = yield* run.now;
        yield* record((probe) => ({ ...probe, read: { atMs: readMs, ...read } }));
        // Found ended now, it ended after the watch's last read that found it running, and
        // before the key tried to. Found running, no end the watch found holds.
        const readEnded =
          read.status === 404 || (read.state !== undefined && Coordinator.isTerminal(read.state));
        const readRunning = read.status === 200 && !readEnded;
        if (endedMs === undefined && readEnded) endedMs = goneMs ?? readMs;
        const contradicted = endedMs !== undefined && readRunning;
        if (contradicted) endedMs = undefined;
        // The key ends the session whether or not Reactor did, so its end is confirmed as
        // every check confirms one. Ending a session Reactor has closed ends nothing more.
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
        // An end the spent token's create could have made is not taken for Reactor's own: one
        // before any read found the session running 10 s after it, or one about the 30 s Reactor
        // gives a session once its last connection drops after it.
        const sinceSettledMs = endedMs === undefined ? undefined : endedMs - settledFromMs;
        const unanswered = contradicted
          ? "The watch found the session ended and the read at the end found it running, so this run cannot say when Reactor ends it."
          : sinceSettledMs === undefined
            ? undefined
            : (runningMs ?? -Infinity) < settledFromMs + settlesMs
              ? `The spent token's second create may have ended the session: Reactor ended it before any read found it still running ${settlesMs / 1000} s after that create was answered.`
              : sinceSettledMs >= graceMs - readEveryMs &&
                  sinceSettledMs <= graceMs + spareMs + readEveryMs
                ? `The spent token's second create may have ended the session: Reactor ended it about ${graceMs / 1000} s after that create was answered, the time it gives a session once its last connection drops.`
                : undefined;
        if (unanswered !== undefined) yield* record((probe) => ({ ...probe, unanswered }));
        const spent = (yield* run.evidence).unconnected?.spentToken;
        yield* pieces.judge(
          "the probe completed",
          [
            spent?.answer === "allocated" ||
              spent?.answer === "the same session" ||
              spent?.outcome === "replied",
            spent === undefined
              ? "the spent token's second create went unrecorded"
              : `the spent token's second create has no known answer: ${spent.answer}${spent.status === undefined ? "" : ` ${spent.status}`}, outcome ${spent.outcome ?? "unknown"}`,
          ],
          [
            last.known || read.status === 404 || read.state !== undefined,
            `the last reads of the session failed, with ${last.state} and then status ${read.status}`,
          ],
        );
        yield* run.mark("unconnected observed");
      }),
    // Whatever failed, the key ends every session the check allocated, trying as above.
    pieces.endHeld(
      Effect.map(keyed, (coordinator) => ({
        ...coordinator,
        terminate: (sessionId: string) =>
          endWithKey(coordinator, sessionId).pipe(Effect.provideService(Run, run)),
      })),
    ),
  );
});
