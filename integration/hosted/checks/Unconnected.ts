/**
 * `unconnected`: what becomes of a session nothing connects to, which
 * Reactor's docs leave open. Its Sessions page says billing runs "from
 * creation", and its Billing page that the meter starts when the session
 * reaches `ready`; neither says whether Reactor ends such a session, or when.
 * Nor do they say what a second create on a spent single-session token
 * answers, which decides whether a create whose outcome is unknown may be
 * sent again.
 *
 * The check mints two tokens, each for one session capped at 60 s. The first
 * allocates the session the check watches, without connecting, and nothing
 * else is sent on it until the key has ended that session, so nothing the
 * check does on the second can end it. The
 * API key reads the watched session every 2 s until a read finds it ended, or
 * until its window closes, past every end Reactor plausibly gives it: the cap
 * counted from allocation, from `ACTIVE` or from ready, and the 30 s after
 * each that Reactor gives a connected session after its last connection
 * drops. The key then ends it either way.
 *
 * Beside the watch, as soon as the watched session is allocated, the second
 * token allocates a session, which spends it, and at once sends a second
 * create, as a retry of a create whose outcome is unknown would. The key then
 * ends at once that session and any the second create allocated. Every end is
 * confirmed. What Reactor billed for each session comes from its dashboard,
 * against the times the evidence records.
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

/** How often the API key reads the watched session. */
const readEveryMs = 2_000;
/**
 * How long after the request the window allows for allocation, `ACTIVE` and
 * ready to come in: the create's own limit. Every paid run that timed it so
 * far connected its session within 3.2 s of allocation.
 */
const startsWithinMs = 15_000;
/** The cap the token asks for, which bounds the one it is granted. */
const capMs = plans.unconnected.seconds * 1000;
/** How long Reactor gives a connected session after its last connection drops. */
const graceMs = 30_000;
/** How far the window runs past the latest end it allows for, for Reactor's own timers. */
const spareMs = 15_000;
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
 * Ends each session with the key, side by side, so none bills while another's
 * tries run, and records the ends once all are done, so a save that fails
 * skips none.
 */
const endTogether = Effect.fnUntraced(function* (
  pieces: Pieces,
  inspector: Coordinator.Coordinator["Service"],
  sessionIds: ReadonlyArray<string>,
) {
  const run = yield* Run;
  const ends = yield* Effect.forEach(
    sessionIds,
    Effect.fnUntraced(function* (sessionId) {
      const requestedMs = yield* run.now;
      const termination = yield* endWithKey(inspector, sessionId);
      return { sessionId, requestedMs, reportedMs: yield* run.now, termination };
    }),
    { concurrency: "unbounded" },
  );
  for (const { sessionId, requestedMs, reportedMs, termination } of ends)
    yield* pieces.closedWith(sessionId, requestedMs, { termination }, reportedMs);
});

type Answered = Result.Result<Coordinator.Allocation, ReactorError>;

/**
 * Creates a session on the token's signaling, records it unless it is one
 * already held, and hands `answered` what the create answered, as one step an
 * interrupt waits for: one landing between the reply and the record would
 * leave a session nothing ends, or a create whose answer nothing kept. The
 * create's own 15 s limit still holds, as a race runs its sides
 * interruptibly.
 */
const allocate = Effect.fnUntraced(function* (
  pieces: Pieces,
  signaling: Coordinator.Signaling,
  grant: Coordinator.TokenGrant,
  grants: Map<string, Coordinator.TokenGrant>,
  answered: (answer: Answered) => Effect.Effect<void>,
) {
  const answer = yield* signaling.create({ name: H3.modelName }).pipe(recorded, Effect.result);
  if (Result.isSuccess(answer) && !grants.has(answer.success.sessionId)) {
    const allocatedAt = yield* Clock.currentTimeMillis;
    grants.set(answer.success.sessionId, grant);
    yield* pieces.holding(answer.success.sessionId, allocatedAt, allocatedAt + pieces.capMs(grant));
  }
  yield* answered(answer);
  return answer;
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

/**
 * A create's answer as the evidence keeps it: `allocated`, or `the same
 * session` when its reply named `held`, the session its token had made, with
 * the reply's key names and codes; or its failure.
 */
const answerOf = (answer: Answered, held?: string) =>
  Result.isFailure(answer)
    ? refusal(answer.failure)
    : {
        answer: answer.success.sessionId === held ? "the same session" : "allocated",
        ...bodyOf(answer.success.reply),
      };

/** The session a create's reply named, if it named one. */
const namedBy = (answer: Answered) =>
  Result.isSuccess(answer) ? { sessionId: answer.success.sessionId } : {};

/** A failed answer, as a criterion's detail gives it. */
const failedWith = (answer: {
  readonly answer: string;
  readonly status?: number;
  readonly outcome?: string;
}) =>
  `${answer.answer}${answer.status === undefined ? "" : ` ${answer.status}`}, outcome ${answer.outcome ?? "unknown"}`;

export const unconnected = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* Coordinator.Coordinator;
  const keyed = Coordinator.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const instant = (atMs: number) => DateTime.formatIso(DateTime.makeUnsafe(run.origin + atMs));
  // A token each, both minted before anything is allocated. The watched session's is used for
  // nothing else, so no create on the other can end it.
  const watchedGrant = yield* pieces.mint("unconnected");
  const spentGrant = yield* pieces.mint("unconnected");
  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
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
        // Saved before each create, so even a crash leaves which create went unanswered.
        yield* run.mark("create sent");
        // The session's own seam, on purpose: its create allocates a session that nothing
        // owns, connects to or closes, and such a session is what this check asks about.
        // Each session is recorded as the reply names it, so the key ends it however the
        // check ends.
        const allocation = yield* allocate(
          pieces,
          coordinator.signaling(Effect.succeed(watchedGrant.jwt)),
          watchedGrant,
          grants,
          (answer) =>
            record((probe) => ({ ...probe, ...namedBy(answer), create: answerOf(answer) })),
        );
        if (Result.isFailure(allocation)) return yield* allocation.failure;
        const { sessionId } = allocation.success;
        yield* run.mark("allocated", sessionId);
        const inspector = yield* keyed;

        // The key reads the watched session every 2 s until a read finds it ended, or until its
        // window closes, when it reads it once more, and then ends it.
        const watching = Effect.gen(function* () {
          const windowEndsMs = requestedMs + startsWithinMs + capMs + graceMs + spareMs;
          yield* record((probe) => ({ ...probe, windowEndsMs }));
          const windowEndsAt = run.origin + windowEndsMs;
          let readAt = yield* Clock.currentTimeMillis;
          let last: { readonly state: string; readonly known: boolean } | undefined;
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
            if (Result.isSuccess(read) && !Coordinator.isTerminal(state)) goneMs = undefined;
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
          // Found CLOSED now, it ended after the watch's last read that found it running, and
          // before the key tried to. Found gone, it ended only if the watch's last read found it
          // gone too: one 404 may be the coordinator's slip here as in the watch. Found running,
          // no end the watch found holds.
          const readClosed = read.state !== undefined && Coordinator.isTerminal(read.state);
          const readGone = read.status === 404;
          const readRunning = read.status === 200 && !readClosed;
          if (endedMs === undefined && (readClosed || (readGone && goneMs !== undefined)))
            endedMs = goneMs ?? readMs;
          const contradicted = endedMs !== undefined && readRunning;
          if (contradicted) endedMs = undefined;
          const unanswered = contradicted
            ? "The watch found the session ended and the read at the end found it running, so this run cannot say when Reactor ends it."
            : endedMs === undefined && readGone
              ? "The watch's last read found the session running and the read at the end alone answered 404, which may be the coordinator's slip, so this run cannot say whether Reactor ended it."
              : undefined;
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
          if (unanswered !== undefined) yield* record((probe) => ({ ...probe, unanswered }));
          return { last, read };
        });

        // The second token allocates a session, which spends it, and at once a second create on
        // it asks what a retry of a create whose outcome is unknown would meet. The key then
        // ends at once that session and any the second create allocated.
        const spending = Effect.gen(function* () {
          const signaling = coordinator.signaling(Effect.succeed(spentGrant.jwt));
          const spentMs = yield* run.now;
          const unspent: SpentToken = { requestedMs: spentMs, requestedAt: instant(spentMs) };
          const recordSpent = (change: (spent: SpentToken) => SpentToken) =>
            record((probe) => ({ ...probe, spentToken: change(probe.spentToken ?? unspent) }));
          yield* recordSpent((spent) => spent);
          yield* run.mark("spent token's first create sent");
          const first = yield* allocate(pieces, signaling, spentGrant, grants, (answer) =>
            recordSpent((spent) => ({ ...spent, ...namedBy(answer), create: answerOf(answer) })),
          );
          // A create that failed named no session, so the token may be unspent: a second create
          // would not ask the question.
          if (Result.isFailure(first)) return;
          const spentSession = first.success.sessionId;
          yield* run.mark("allocated", spentSession);
          yield* run.mark("spent token's second create sent");
          const sentMs = yield* run.now;
          const second = yield* allocate(pieces, signaling, spentGrant, grants, (answer) =>
            Effect.flatMap(run.now, (answeredMs) =>
              recordSpent((spent) => ({
                ...spent,
                second: {
                  sentMs,
                  answeredMs,
                  ...answerOf(answer, spentSession),
                  ...namedBy(answer),
                },
              })),
            ),
          );
          yield* run.mark(
            "spent token's second create answered",
            answerOf(second, spentSession).answer,
          );
          // A session nothing asked for, recorded as the reply named it.
          const extra =
            Result.isSuccess(second) && second.success.sessionId !== spentSession
              ? second.success.sessionId
              : undefined;
          if (extra !== undefined) yield* run.mark("allocated", extra);
          yield* endTogether(
            pieces,
            inspector,
            extra === undefined ? [spentSession] : [spentSession, extra],
          );
        });

        // Side by side, so no read of the watched session waits on the spent token's creates or
        // on the key's ends of what they made.
        const [{ last, read }] = yield* Effect.all([watching, spending], {
          concurrency: "unbounded",
        });
        const spent = (yield* run.evidence).unconnected?.spentToken;
        const second = spent?.second;
        yield* pieces.judge("the watch completed", [
          last.known || read.status === 404 || read.state !== undefined,
          `the last reads of the session failed, with ${last.state} and then status ${read.status}`,
        ]);
        yield* pieces.judge(
          "the spent token's second create was answered",
          [
            spent?.sessionId !== undefined,
            spent?.create === undefined
              ? "the spent token's first create went unrecorded"
              : `no second create went out: its first create failed with ${failedWith(spent.create)}`,
          ],
          [
            second?.answer === "allocated" ||
              second?.answer === "the same session" ||
              second?.outcome === "replied",
            second === undefined
              ? "the spent token's second create went unrecorded"
              : `the spent token's second create has no known answer: ${failedWith(second)}`,
          ],
          // A quota refuses a create before it asks anything of the token.
          [
            second?.status !== 429,
            "the spent token's second create was refused 429, by a limit of the account's, which says nothing of a spent token",
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
