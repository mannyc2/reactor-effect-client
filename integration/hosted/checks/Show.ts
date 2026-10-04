/**
 * `show`: one playout over three sessions, run as a show runs, a phase at a
 * time. Filler keeps the air covered throughout (`Playout.lineup`). Session 1
 * loses its connection and recovers it by itself, then airs the edits; the
 * air switches to session 2 as planned; session 2 airs an `Asap` item ahead
 * of one waiting, cues and an item due `At` an instant, and is then ended
 * under a playing clip, by content moderation when the operator gives a
 * prompt and with the API key when not; session 3 rebuilds what was lost,
 * airs the cut and conflict lanes, and drains.
 *
 * Each phase judges its own criteria. A failure, or running past its session's
 * work deadline, is a failed criterion named after it, and the phases that
 * still make sense run.
 *
 * The planned timeline, from session 1's allocation A1, at the timing paid runs
 * measured (a connection 2.7 s after allocation, a 5 s clip built in about
 * 2.2 s and a continued one in about 5.5 s, a verdict 1 s after its enqueue).
 * Steps marked * wait on hosted latency:
 *
 *   A1+3    the playout opens on session 1 (O1); filler secures 8 s of air
 *   A1+8    with the runway full nothing is in flight: the connection is cut
 *   A1+11   * session 1 has reconnected and its picture resumes
 *   A1+11   a group of three and w1 go in; the inserts once p1 airs, and the
 *           batch a median build and a second before p3 ends: p1 p2 xc xn p3 y,
 *           from about A1+15 to A1+46 (* xc continues from p2 within p2's length)
 *   A1+48   * 30 s before session 1's grant ends, session 2 opens and takes new
 *           work: L1, cued a second after its start and a second before its
 *           end, and X at Asap; once X is Ready session 1's filler goes
 *   A1+55   the air switches to session 2 as session 1's last clip ends; X airs
 *   X airs  T is due once the air secured has played and one new filler clip
 *           has tiled 6 s more (* H3 aligns the tile up to its frame grid, so T
 *           airs up to 0.7 s and a seam after its time): about A1+77
 *   T airs  a 10 s guard and c1 (* the guard builds while T plays); once the
 *           guard airs and c1 is Ready, the flagged item goes in, or without a
 *           prompt the API key ends session 2: about A1+84, well before its own
 *           lead would open a replacement at A1+95
 *   loss    session 3 opens about 3 s later and rebuilds c1: about A1+90
 *   c1 airs a 10 s clip in the line, cut 2.5 s after it starts by one in the
 *           cut lane; then a replaced pair in the `replace` lane, and one item
 *           in the `skip` lane with a second refused behind it
 *   A1+117  a drain of what was accepted ends the show
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type * as H3 from "reactor-effect-client/H3";
import * as Playout from "reactor-effect-client/Playout";
import { isReactorFailure } from "reactor-effect-client/ReactorError";
import type { Air, Pieces } from "../Checks.js";
import type * as Evidence from "../Evidence.js";
import { failedOf } from "../Evidence.js";
import type { Seam } from "../Evidence.js";
import * as Family from "../Family.js";
import { SaveFailed } from "../Ledger.js";
import * as Probes from "../Probes.js";
import { recorded, Run } from "../Run.js";
import { workSecondsFor } from "../Spend.js";
import { Target } from "../Target.js";
/** Long enough that a verdict, a second after the flagged enqueue, lands while the guard airs. */
const guardSeconds = 10;
/**
 * Filler keeps this much air secured ahead, in seconds: a refill starts with a 5 s clip's build
 * to spare, and two items Ready are air enough, so no filler waits behind them.
 */
const runway = { floor: 5, target: 8 };
/**
 * Session 2 opens this long before session 1's 75 s grant ends: after the edits are built, and
 * late enough that session 2's own lead falls after its planned loss.
 */
const leadSeconds = 30;
/**
 * The gap before T that new filler tiles, past the air already secured: one clip at least 5 s
 * long, H3's shortest, with room for its measured length running over what was asked.
 */
const tileMs = 6_000;
/** The line clip the cutter stops, long enough that a cutter built after 2.5 s leaves it seconds to run. */
const longSeconds = 10;
/** How long after the long clip starts the cutter goes in, as `cut` sends it. */
const cutAfterMs = 2_500;
/** How long the flagged item is watched for a verdict or its session's end. */
const moderationWaitMs = 12_000;
/** How long a dropped connection may take to come back; `H3Source`'s default recovery. */
const recoveryMs = 20_000;
/** How long the show waits for filler to fill the runway before it drops the connection anyway. */
const quietMs = 20_000;
/** How long an open may take before the show gives up on it, the wait for a GPU included. */
const openMs = 180_000;
/** How late a cue may fire after its due time, measured from its clip's observed start. */
const cueToleranceMs = 500;
/** How early: the due time and the firing are rounded to a tenth of a millisecond apart. */
const cueEarlyMs = 1;
/**
 * The longest a clip's end may go unfollowed on one session's air. Hosted
 * clips followed each other within 40 ms, and a cut's within 150 ms.
 */
const gapMs = 1_500;
/**
 * How far apart two observers may see one moment: the session's status and
 * the playout's report of it arrive on separate fibers.
 */
const observedMs = 100;
/** How far the reconnect's measured length may stray from the time between its reports. */
const reconnectSkewMs = 500;
/** Less than any filler clip's length: two starts of filler closer than this are one clip's. */
const fillerApartMs = 4_000;

const fillerPrompt = "The same sunlit table seen from across the room, the light shifting slowly.";

/** The planned order of the edits on session 1, as `edits` plans it. */
const plannedEdits = ["p1", "p2", "xc", "xn", "p3", "y"] as const;

/** A phase's failure as its criterion states it: the error's tag and the library's message. */
const phaseFailure = (error: unknown): string => {
  if (Cause.isTimeoutError(error)) return "TimeoutError: it ran past its deadline";
  if (Cause.isNoSuchElementError(error)) return "NoSuchElementError: what it waited for never came";
  if (isReactorFailure(error)) return `${error.reason._tag}: ${error.message}`;
  if (Predicate.hasProperty(error, "_tag") && Predicate.isString(error._tag)) return error._tag;
  return "an unexpected failure";
};

/**
 * Why an as-run status says its item failed: content moderation flagged it,
 * the session it was on was lost, or anything else. The one place that reads
 * the playout's failure shape.
 */
const whyFailed = (
  status: Playout.AsRunStatus | undefined,
):
  | {
      readonly kind: "moderated" | "lost" | "failed";
      readonly lost?: string;
      readonly reason: string;
    }
  | undefined => {
  if (status?._tag !== "Failed") return undefined;
  const reason = failedOf(status.reason).reason;
  switch (status.reason._tag) {
    case "Moderated":
      return { kind: "moderated", reason };
    case "Lost":
      return { kind: "lost", lost: status.reason.sessionId, reason };
    default:
      return { kind: "failed", reason };
  }
};

/** What the evidence says beside a status: why it failed, or that nothing can settle it any more. */
const statusDetail = (status: Playout.AsRunStatus): string | undefined => {
  const why = whyFailed(status);
  if (why !== undefined) return `${why.kind}: ${why.reason}`;
  return status._tag === "Unknown" && status.terminal === true ? "terminal" : undefined;
};

/** A playout event the show keeps: its sessions, cues and starvation. */
interface Logged {
  readonly atMs: number;
  readonly event: Exclude<Playout.Event, { readonly _tag: "AsRun" }>;
}

/** The playout's session events among those the show kept. */
const sessionEvents = (all: ReadonlyArray<Logged>) =>
  all.flatMap(({ atMs, event }) =>
    event._tag === "Session" ? [{ atMs, event: event.event }] : [],
  );

/** Each reconnect the playout reported, with its session's next report of being back. */
const reconnectsOf = (all: ReadonlyArray<Logged>): Evidence.ShowRecord["reconnects"] => {
  const reconnects: Array<Evidence.ShowRecord["reconnects"][number]> = [];
  for (const { atMs, event } of sessionEvents(all)) {
    if (event._tag === "Reconnecting")
      reconnects.push({ sessionId: event.sessionId, reconnectingMs: atMs });
    if (event._tag !== "Reconnected") continue;
    const index = reconnects.findLastIndex(
      (open) => open.sessionId === event.sessionId && open.reconnectedMs === undefined,
    );
    const open = reconnects[index];
    if (open !== undefined)
      reconnects[index] = {
        ...open,
        reconnectedMs: atMs,
        afterMillis: Math.round(event.afterMillis),
      };
  }
  return reconnects;
};

/** A stretch on air: a clip from its reported start to its end, or its length when none came. */
interface Aired {
  readonly key: string;
  readonly fromMs: number;
  readonly toMs: number;
}

/** Every gap between one clip's end and the next one's start, in order. */
const gapsOf = (aired: ReadonlyArray<Aired>): Evidence.ShowRecord["gaps"] => {
  const gaps: Array<Evidence.ShowRecord["gaps"][number]> = [];
  let covered: Aired | undefined;
  for (const next of [...aired].sort((a, b) => a.fromMs - b.fromMs)) {
    if (covered !== undefined && next.fromMs > covered.toMs)
      gaps.push({
        fromMs: covered.toMs,
        toMs: next.fromMs,
        ending: covered.key,
        next: next.key,
      });
    if (covered === undefined || next.toMs > covered.toMs) covered = next;
  }
  return gaps;
};

export const show = (pieces: Pieces) =>
  Effect.flatMap(Run, (run) => Family.withFamily(run, (family) => showWith(family, pieces)));

const showWith = Effect.fnUntraced(function* <Req extends Family.RequestInput, C extends H3.Clip>(
  family: Family.Family<Req, C>,
  pieces: Pieces,
) {
  const clipSeconds = family.lengths.short;
  const item = (seconds = clipSeconds) => family.request({ prompt: Family.prompt, seconds });
  const flagged = (yield* Target).moderationPrompt;
  const fills: Array<Evidence.ShowRecord["fills"][number]> = [];
  const lineup = Playout.lineup({
    runway: { floor: `${runway.floor} seconds`, target: `${runway.target} seconds` },
    clip: (context) => {
      // What the playout asked for; the request is the same whatever is kept.
      fills.push({
        index: context.index,
        runwaySeconds: Math.round(context.runwaySeconds * 100) / 100,
        seconds: Math.round(context.seconds * 1000) / 1000,
      });
      return family.request({ prompt: fillerPrompt, seconds: context.seconds });
    },
  });
  return yield* pieces.onAirFor(
    family,
    "show",
    {
      // The lineup's lane and filler, a cut lane above them, and a lane of each other conflict.
      lanes: [
        { name: "urgent", cut: true },
        ...lineup.lanes,
        { name: "swap", conflict: "replace" },
        { name: "solo", conflict: "skip" },
      ],
      filler: lineup.filler,
      sessions: 3,
      renewal: { lead: `${leadSeconds} seconds`, grace: "250 millis" },
      // One session lost to moderation is the planned loss; a second ends the show.
      maxModerations: 2,
    },
    (air) =>
      Effect.gen(function* () {
        const run = yield* Run;
        const target = yield* Target;
        const log = yield* SubscriptionRef.make<ReadonlyArray<Logged>>([]);
        /** Milliseconds from the run's origin to `at`, epoch milliseconds, to a tenth. */
        const sinceOrigin = (at: number) => Math.round((at - run.origin) * 10) / 10;
        const playing = yield* Ref.make<Evidence.ShowRecord["playing"]>({
          named: 0,
          later: 0,
          mismatched: [],
        });
        /**
         * Reads the clip the state names on air as a start is reported: `own` is
         * how the state names it, `at` the reported start and `seconds` its length.
         */
        const comparePlaying = (
          key: string,
          own: string,
          at: number,
          seconds: number | undefined,
        ) =>
          Effect.flatMap(air.playout.state, ({ playing: named }) =>
            Ref.update(playing, (seen) => {
              if (named?.key === own && named.startedAt === at)
                return { ...seen, named: seen.named + 1 };
              // Filler clips share a name, and the next starts once one has run at least 5 s.
              const same =
                named?.key === own && (own !== "filler" || named.startedAt - at < fillerApartMs);
              if (named !== null && !same && named.startedAt > at)
                return { ...seen, later: seen.later + 1 };
              return {
                ...seen,
                mismatched: [
                  ...seen.mismatched,
                  {
                    key,
                    startedMs: sinceOrigin(at),
                    ...(seconds === undefined ? {} : { seconds }),
                    ...(named === null
                      ? {}
                      : {
                          stateKey: named.key,
                          stateStartedMs: sinceOrigin(named.startedAt),
                          ...(named.seconds === undefined ? {} : { stateSeconds: named.seconds }),
                        }),
                  },
                ],
              };
            }),
          );
        yield* air.playout.events.pipe(
          Stream.runForEach((event) => {
            if (event._tag === "AsRun") {
              const status = event.event.status;
              return status._tag === "Started"
                ? comparePlaying(event.event.key, event.event.key, status.at, status.seconds)
                : Effect.void;
            }
            const kept = Effect.flatMap(run.now, (atMs) =>
              SubscriptionRef.update(log, (all) => [...all, { atMs, event }]),
            );
            return event._tag === "Filler" && event.phase === "Started"
              ? Effect.andThen(
                  comparePlaying(`filler ${event.index}`, "filler", event.at, event.seconds),
                  kept,
                )
              : kept;
          }),
          Effect.forkScoped,
        );
        const shown = yield* Ref.make<Pick<Evidence.ShowRecord, "recovery" | "loss" | "at">>({});
        const recordShow = Effect.gen(function* () {
          const all = yield* SubscriptionRef.get(log);
          const items = yield* SubscriptionRef.get(air.items);
          const facts = yield* Ref.get(shown);
          const readings = yield* Ref.get(playing);
          const filler = all.flatMap(({ event }) =>
            event._tag === "Filler"
              ? [
                  {
                    index: event.index,
                    phase: event.phase,
                    atMs: sinceOrigin(event.at),
                    ...(event.seconds === undefined
                      ? {}
                      : { seconds: Math.round(event.seconds * 1000) / 1000 }),
                  },
                ]
              : [],
          );
          const failures = air.asRun().flatMap(({ atMs, key, status }) =>
            status._tag === "Failed"
              ? [
                  {
                    key,
                    atMs,
                    reason: status.reason._tag,
                    ...(status.reason._tag === "Lost"
                      ? { sessionId: status.reason.sessionId }
                      : {}),
                  },
                ]
              : [],
          );
          // A clip lost on air ended when it failed; one whose end never came, as a retiring
          // session's last may not, ran its length.
          const aired: Array<Aired> = [];
          for (const clip of items.values())
            if (clip.startedMs !== undefined)
              aired.push({
                key: clip.key,
                fromMs: clip.startedMs,
                toMs:
                  clip.endedMs ??
                  failures.find((failure) => failure.key === clip.key)?.atMs ??
                  clip.startedMs + (clip.seconds ?? clipSeconds) * 1000,
              });
          for (const start of filler) {
            if (start.phase !== "Started") continue;
            const end = filler.find(
              (clip) =>
                clip.phase === "Ended" && clip.index === start.index && clip.atMs >= start.atMs,
            );
            aired.push({
              key: `filler ${start.index}`,
              fromMs: start.atMs,
              toMs: end?.atMs ?? start.atMs + (start.seconds ?? clipSeconds) * 1000,
            });
          }
          const cues = all.flatMap(({ event }) => {
            if (event._tag !== "Cue") return [];
            const aired = items.get(event.event.key);
            const due =
              aired?.startedMs === undefined
                ? undefined
                : cueDue(event.event.name, aired, clipSeconds);
            return [
              {
                key: event.event.key,
                name: event.event.name,
                atMs: sinceOrigin(event.event.at),
                ...(due === undefined
                  ? {}
                  : { lateByMs: Math.round(event.event.at - run.origin - due) }),
              },
            ];
          });
          yield* run.update((evidence) => ({
            ...evidence,
            show: {
              fills: [...fills],
              starved: all.flatMap(({ atMs, event }) => (event._tag === "Starved" ? [atMs] : [])),
              sessions: sessionEvents(all).map(({ atMs, event }) => ({
                atMs,
                event: pieces.sessionEventText(event),
              })),
              cues,
              ...facts,
              filler,
              gaps: gapsOf(aired).map((gap) => ({
                ...gap,
                fromMs: Math.round(gap.fromMs * 10) / 10,
                toMs: Math.round(gap.toMs * 10) / 10,
              })),
              reconnects: reconnectsOf(all),
              readerOverflows: all.flatMap(({ event }) =>
                event._tag === "ReaderOverflow"
                  ? [
                      {
                        sessionId: event.sessionId,
                        track: event.track,
                        atMs: sinceOrigin(event.at),
                        readerOverflows: Number(event.pressure.readerOverflows),
                      },
                    ]
                  : [],
              ),
              failures,
              playing: readings,
            },
          }));
        });
        yield* Effect.addFinalizer(() => Effect.ignore(recordShow));

        /**
         * Runs one phase until `deadline`, a Clock time. A failure, or running out of
         * time, is a failed criterion named after it; a save that fails stops the run.
         */
        const phase = <A, E, R>(name: string, deadline: number, body: Effect.Effect<A, E, R>) =>
          Effect.flatMap(pieces.until(deadline), (left) =>
            body.pipe(
              Effect.timeout(left),
              Effect.asSome,
              Effect.catchIf(
                (error) => !Schema.is(SaveFailed)(error),
                (error) =>
                  Effect.as(pieces.judge(name, [false, phaseFailure(error)]), Option.none()),
              ),
            ),
          );
        /** A session's work deadline, a Clock time: 10 s before its cap. */
        const deadlineOf = Effect.fnUntraced(function* (sessionId: string | undefined) {
          const held = (yield* run.evidence).sessions.find((session) => session.id === sessionId);
          return held === undefined
            ? undefined
            : run.origin + held.allocatedMs + workSecondsFor("show") * 1000;
        });
        const started = (key: string, deadline: number) =>
          pieces.waitFor(air.items, (all) => all.get(key)?.startedMs, deadline);
        const sessionEvent = <B>(
          find: (logged: {
            readonly atMs: number;
            readonly event: Playout.SessionEvent;
          }) => B | undefined,
          deadline: number,
        ) =>
          pieces.waitFor(
            log,
            (all) => sessionEvents(all).map(find).find(Predicate.isNotUndefined),
            deadline,
          );

        // Session 1, once its playout opened: waiting for a GPU is not billed.
        const opening = (yield* Clock.currentTimeMillis) + openMs;
        yield* pieces
          .watch(
            Effect.map(run.evidence, (evidence) => evidence.sessions.length > 0),
            opening,
          )
          .pipe(Effect.raceFirst(Effect.asVoid(air.playout.failure)));
        const first = (yield* run.evidence).sessions[0]?.id;
        const firstDeadline = (yield* deadlineOf(first)) ?? opening;

        // Recovery: the connection drops while filler holds the air, and comes back by itself.
        yield* phase(
          "recovery on the same session",
          firstDeadline,
          Effect.gen(function* () {
            // A full runway leaves filler nothing to build, so no command is in flight.
            yield* pieces.watch(
              Effect.map(
                air.playout.state,
                (state) =>
                  state.playing?.key === "filler" && state.runwaySeconds >= runway.target - 0.5,
              ),
              Math.min(firstDeadline, (yield* Clock.currentTimeMillis) + quietMs),
            );
            const runwaySeconds = (yield* air.playout.state).runwaySeconds;
            const droppedMs = yield* run.now;
            const dropped = yield* target.sever;
            yield* run.mark("connection dropped", `${dropped}`);
            const back = () =>
              air
                .sessionLog()
                .find(
                  (logged) =>
                    logged.sessionId === first &&
                    logged.atMs >= droppedMs &&
                    logged.event === "status ready",
                )?.atMs;
            const give = (yield* Clock.currentTimeMillis) + recoveryMs;
            yield* pieces.watch(
              Effect.sync(() => back() !== undefined),
              Math.min(firstDeadline, give),
            );
            const readyMs = back();
            if (readyMs !== undefined)
              yield* pieces.watch(
                Effect.sync(() => air.video.firstAfter(readyMs) !== undefined),
                Math.min(firstDeadline, run.origin + readyMs + 6_000),
              );
            const firstFrameMs = readyMs === undefined ? undefined : air.video.firstAfter(readyMs);
            const seenMs = firstFrameMs ?? (yield* run.now);
            const replaced = sessionEvents(yield* SubscriptionRef.get(log)).some(
              ({ atMs, event }) =>
                atMs <= seenMs &&
                (event._tag === "Replaced" ||
                  (event._tag === "Opened" && event.sessionId !== first)),
            );
            yield* Ref.update(shown, (facts) => ({
              ...facts,
              recovery: {
                sessionId: first ?? "",
                droppedMs,
                dropped,
                runwaySeconds: Math.round(runwaySeconds * 100) / 100,
                statuses: air
                  .sessionLog()
                  .filter(
                    (logged) =>
                      logged.sessionId === first &&
                      logged.atMs >= droppedMs &&
                      logged.atMs <= (readyMs ?? Infinity) &&
                      logged.event.startsWith("status "),
                  )
                  .map(({ atMs, event }) => ({
                    atMs,
                    status: event.slice("status ".length),
                  })),
                ...(readyMs === undefined ? {} : { readyMs }),
                ...(firstFrameMs === undefined ? {} : { firstFrameMs }),
              },
            }));
            yield* run.mark("recovered", readyMs === undefined ? "not ready" : "ready");
            yield* pieces.judge(
              "recovery on the same session",
              [dropped === 1, `${dropped} connections were dropped, not one`],
              [readyMs !== undefined, "the session never read ready again"],
              [firstFrameMs !== undefined, "no frame reached the picture after the reconnect"],
              [!replaced, "the playout replaced the session instead"],
            );
            // The playout's own account of the reconnect agrees with what the session published.
            const reconnects = Effect.map(SubscriptionRef.get(log), (all) =>
              reconnectsOf(all).filter(
                (reconnect) =>
                  reconnect.sessionId === first && reconnect.reconnectingMs >= droppedMs,
              ),
            );
            yield* pieces.watch(
              Effect.map(reconnects, (all) =>
                all.some((reconnect) => reconnect.reconnectedMs !== undefined),
              ),
              Math.min(firstDeadline, give),
            );
            const reported = yield* reconnects;
            const [reconnect] = reported;
            const backMs = reconnect?.reconnectedMs;
            const tookMs =
              reconnect === undefined || backMs === undefined
                ? undefined
                : backMs - reconnect.reconnectingMs;
            yield* pieces.judge(
              "the playout reports the reconnect",
              [
                reported.length === 1,
                `the playout reported ${reported.length} reconnects of the session, not one`,
              ],
              [tookMs !== undefined, "the playout never reported the session back"],
              [
                readyMs === undefined ||
                  reconnect === undefined ||
                  (readyMs >= reconnect.reconnectingMs - observedMs &&
                    readyMs <= (backMs ?? Infinity) + observedMs),
                `the session read ready at ${readyMs} ms, outside the reconnect the playout reported from ${reconnect?.reconnectingMs} to ${backMs ?? "never"} ms`,
              ],
              [
                tookMs === undefined ||
                  reconnect?.afterMillis === undefined ||
                  Math.abs(reconnect.afterMillis - tookMs) <= reconnectSkewMs,
                `the reconnect was said to take ${reconnect?.afterMillis} ms, but its reports were ${Math.round(tookMs ?? 0)} ms apart`,
              ],
            );
          }),
        );

        // The edits, on session 1 as `edits` plans them.
        yield* phase(
          "edits",
          firstDeadline,
          Effect.gen(function* () {
            yield* recorded(
              air.playout.submitGroup({
                key: Playout.ItemKey.make("beats"),
                lane: "line",
                parts: [
                  { key: yield* air.track("p1"), request: item() },
                  { key: yield* air.track("p2"), request: item() },
                  { key: yield* air.track("p3"), request: item() },
                ],
              }),
            );
            yield* air.submit("w1", "line", item());
            yield* run.mark("edits submitted");
            yield* started("p1", firstDeadline);
            // A continued build takes about 2.5 times an independent one on hosted H3, so xc
            // continues from p2, which gives it p2's whole length to build in.
            yield* recorded(
              air.playout.insert({
                key: yield* air.track("xc"),
                request: item(),
                after: Playout.ItemKey.make("p2"),
                continuity: "previous",
              }),
            );
            yield* recorded(
              air.playout.insert({
                key: yield* air.track("xn"),
                request: item(),
                before: Playout.ItemKey.make("p3"),
              }),
            );
            yield* run.mark("inserted");
            const p3Started = yield* started("p3", firstDeadline);
            const p3 = yield* pieces.waitFor(air.items, (all) => all.get("p3"), firstDeadline);
            const perSecond = (yield* air.playout.state).estimates.build?.median ?? 0.42;
            const endsMs = p3Started + (p3.seconds ?? clipSeconds) * 1000;
            yield* pieces.sleepUntil(endsMs - 1000 - perSecond * clipSeconds * 1000, firstDeadline);
            const submittedMs = yield* run.now;
            const batch = yield* recorded(
              air.playout.edit([
                { _tag: "Withdraw", key: Playout.ItemKey.make("w1") },
                {
                  _tag: "Insert",
                  insert: {
                    key: yield* air.track("y"),
                    request: item(),
                    after: Playout.ItemKey.make("p3"),
                  },
                },
              ]),
            );
            const committedAt = yield* Ref.make<number | undefined>(undefined);
            yield* batch.committed.pipe(
              Effect.andThen(run.now),
              Effect.flatMap((atMs) => Ref.set(committedAt, atMs)),
              Effect.ignore,
              Effect.forkScoped,
            );
            yield* run.mark("batch submitted");
            yield* pieces.sleepUntil(
              (yield* started("y", firstDeadline)) + 2 * pieces.seamMs + 250,
              firstDeadline,
            );
            const committedMs = yield* Ref.get(committedAt);
            const all = yield* SubscriptionRef.get(air.items);
            const order = air.starts().filter((key) => plannedEdits.some((edit) => edit === key));
            const seams: Array<Seam> = [];
            for (let index = 0; index + 1 < order.length; index++) {
              const ending = order[index];
              const next = order[index + 1];
              if (ending !== undefined && next !== undefined)
                seams.push(yield* air.seam(ending, next, next === "xc"));
            }
            const p3End = all.get("p3")?.endedMs;
            yield* pieces.recordPlayout({
              seams,
              batch: {
                submittedMs,
                ...(committedMs === undefined ? {} : { committedMs }),
                ...(p3End === undefined ? {} : { boundaryMs: p3End }),
              },
            });
            yield* pieces.judge("inserts and the batch air in their planned places", [
              order.join(",") === plannedEdits.join(","),
              `started in the order ${order.join(", ")}`,
            ]);
            yield* pieces.judge(
              "a batch takes effect before its boundary",
              [committedMs !== undefined, "the batch never took effect"],
              [
                p3End === undefined || committedMs === undefined || committedMs < p3End,
                "the batch took effect after the playing clip ended",
              ],
            );
            const w1 = all.get("w1");
            yield* pieces.judge(
              "a batch's withdrawn clip never starts",
              [w1?.startedMs === undefined, "the withdrawn clip started"],
              [w1?.dropped === "withdrawn", `the withdrawn clip ended ${w1?.last ?? "untracked"}`],
            );
            yield* pieces.judge("every seam measured", [
              seams.length >= plannedEdits.length - 1 &&
                seams.every((seam) => seam.pause !== undefined && seam.jump !== undefined),
              "a boundary has no pause or join measurement",
            ]);
            yield* run.mark("edits observed");
          }),
        );

        // The planned switch: the replacement opens `lead` before session 1's grant ends. The
        // first items go to it at once, so once one is Ready session 1's filler goes.
        const switched = yield* phase(
          "planned switch",
          firstDeadline,
          Effect.gen(function* () {
            const opened = yield* sessionEvent(
              ({ event }) =>
                event._tag === "Opened" && event.sessionId !== first ? event.sessionId : undefined,
              firstDeadline,
            );
            yield* recorded(
              air.playout.submit({
                key: yield* air.track("L1"),
                lane: "line",
                request: item(),
                cues: [
                  { name: "in", at: { from: "start", offset: "1 second" } },
                  { name: "out", at: { from: "end", offset: "1 second" } },
                ],
              }),
            );
            yield* recorded(
              air.playout.submit({
                key: yield* air.track("X"),
                lane: "line",
                request: item(),
                start: { _tag: "Asap" },
              }),
            );
            yield* run.mark("L1 and X submitted", opened);
            const change = yield* sessionEvent(
              ({ atMs, event }) =>
                event._tag === "Switched" && event.from === first
                  ? { atMs, to: event.to }
                  : undefined,
              firstDeadline,
            );
            yield* run.mark("switched", change.to);
            return change;
          }),
        );
        const second =
          Option.getOrUndefined(switched)?.to ??
          (yield* run.evidence).sessions.find((session) => session.id !== first)?.id;
        const secondDeadline = (yield* deadlineOf(second)) ?? firstDeadline;

        // Session 2: X at Asap overtakes L1, L1's cues fire, and T is due At an instant.
        const lined = yield* phase(
          "start modes and cues",
          secondDeadline,
          Effect.gen(function* () {
            yield* pieces.waitFor(
              air.items,
              (all) =>
                ["X", "L1"].some((key) => all.get(key)?.startedMs !== undefined) ? true : undefined,
              secondDeadline,
            );
            // T is due once the air already secured has played and one new filler clip has
            // tiled the rest, asked for while no filler builds, so none lands unaccounted.
            yield* pieces.watch(
              Effect.map(air.playout.state, (state) => state.runwaySeconds >= runway.target),
              Math.min(secondDeadline, (yield* Clock.currentTimeMillis) + quietMs),
            );
            const secured = (yield* air.playout.state).runwaySeconds;
            const dueMs = Math.round((yield* run.now) + secured * 1000 + tileMs);
            yield* recorded(
              air.playout.submit({
                key: yield* air.track("T"),
                lane: "line",
                request: item(),
                // A show never drops its anchor: it airs at the first boundary from its time.
                start: {
                  _tag: "At",
                  time: run.origin + dueMs,
                  late: { _tag: "nextBoundary" },
                },
              }),
            );
            yield* run.mark("T submitted");
            const settled = yield* pieces.waitFor(
              air.items,
              (all) => {
                const t = all.get("T");
                return t?.startedMs !== undefined || t?.dropped !== undefined ? t : undefined;
              },
              secondDeadline,
            );
            yield* Ref.update(shown, (facts) => ({
              ...facts,
              at: {
                dueMs,
                ...(settled.startedMs === undefined
                  ? {}
                  : { lateByMs: Math.round(settled.startedMs - dueMs) }),
              },
            }));
            const order = air.starts();
            yield* pieces.judge("an Asap item airs ahead of one waiting", [
              order.includes("X") && order.indexOf("X") < order.indexOf("L1"),
              `started ${order.filter((key) => key === "X" || key === "L1").join(", ") || "neither"}`,
            ]);
            // How late it came is recorded: it measures how well filler tiled the gap.
            yield* pieces.judge(
              "an At item airs, never before its time",
              [settled.startedMs !== undefined, `T ended ${settled.dropped ?? settled.last}`],
              [
                (settled.startedMs ?? dueMs) >= dueMs,
                `T started ${Math.round(dueMs - (settled.startedMs ?? dueMs))} ms early`,
              ],
            );
            yield* recordShow;
            const cues = ((yield* run.evidence).show?.cues ?? []).filter((cue) => cue.key === "L1");
            const off = cues.filter(
              (cue) =>
                cue.lateByMs === undefined ||
                cue.lateByMs < -cueEarlyMs ||
                cue.lateByMs > cueToleranceMs,
            );
            yield* pieces.judge(
              "cues fire at their offsets",
              [
                cues.map((cue) => cue.name).join(",") === "in,out",
                `L1's cues fired as ${cues.map((cue) => cue.name).join(", ") || "none"}`,
              ],
              [
                off.length === 0,
                `${off.map((cue) => `${cue.name} ${String(cue.lateByMs ?? "?")} ms`).join(", ")} from due`,
              ],
            );
            yield* run.mark("start modes observed");
            return settled;
          }),
        );

        // The loss: session 2 ends under a playing clip, with a Ready one behind it.
        const lost = yield* phase(
          "a session ended mid-show",
          secondDeadline,
          Effect.gen(function* () {
            if (Option.isNone(lined)) yield* started("T", secondDeadline);
            yield* air.submit("guard", "line", item(guardSeconds));
            yield* air.submit("c1", "line", item());
            yield* pieces.waitFor(
              air.items,
              (all) =>
                all.get("guard")?.startedMs !== undefined && all.get("c1")?.readyMs !== undefined
                  ? true
                  : undefined,
              secondDeadline,
            );
            const lostId = (yield* SubscriptionRef.get(air.items)).get("guard")?.sessionId;
            yield* run.mark("guard on air, c1 Ready");
            const requestedMs = yield* run.now;
            // What was on air, and what waited, on the session when it was asked to end.
            const aboard = [...(yield* SubscriptionRef.get(air.items)).values()];
            const verdict =
              flagged === undefined
                ? "none"
                : yield* moderate(family, pieces, air, log, {
                    flagged,
                    sessionId: lostId,
                    deadline: secondDeadline,
                  });
            const by: "moderation" | "key" = verdict === "ended" ? "moderation" : "key";
            // Without a prompt, or with one moderation let through, the API key ends it mid-clip.
            const termination =
              by === "moderation"
                ? undefined
                : yield* Effect.flatMap(
                    CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey }),
                    (keyed) => keyed.terminate(lostId ?? ""),
                  );
            if (termination !== undefined)
              yield* run.mark(
                "ended with the API key",
                termination.confirmed ? "confirmed" : "unconfirmed",
              );
            const replaced = yield* sessionEvent(
              ({ atMs, event }) =>
                atMs >= requestedMs && event._tag === "Replaced" && event.from === lostId
                  ? { atMs, carried: event.carried }
                  : undefined,
              secondDeadline,
            );
            // The next session: opened after the loss, or already opened by renewal before it.
            const next = yield* sessionEvent(
              ({ atMs, event }) =>
                event._tag === "Opened" && event.sessionId !== lostId && event.sessionId !== first
                  ? { atMs, sessionId: event.sessionId }
                  : undefined,
              Math.max(secondDeadline, (yield* Clock.currentTimeMillis) + openMs),
            );
            yield* run.mark("replaced", next.sessionId);
            yield* Ref.update(shown, (facts) => ({
              ...facts,
              loss: {
                sessionId: lostId ?? "",
                by,
                requestedMs,
                ...(termination === undefined ? {} : { termination }),
                replacedMs: replaced.atMs,
                nextSessionId: next.sessionId,
                nextOpenedMs: next.atMs,
              },
            }));
            yield* pieces.judge(
              "the ended session is replaced",
              [replaced.carried >= 1, `${replaced.carried} clips were carried`],
              [
                termination === undefined || termination.confirmed,
                "the API key's termination was not confirmed",
              ],
            );
            const keys = (holds: (aired: Evidence.Item) => boolean) =>
              aboard
                .filter((aired) => aired.sessionId === lostId && aired.key !== "flagged")
                .filter(holds)
                .map((aired) => aired.key);
            const waiting = (aired: Evidence.Item) =>
              aired.startedMs === undefined &&
              (aired.last === "Ready" || aired.last === "Building");
            return {
              lostId,
              next: next.sessionId,
              by,
              playing: keys(
                (aired) => aired.startedMs !== undefined && aired.endedMs === undefined,
              ),
              waiting: keys(waiting),
            };
          }),
        );

        // Session 3's lanes: a cut lane stops a lower lane's clip, a replacing lane's new item
        // takes its waiting item's place, and a skipping lane refuses one while another waits.
        const loss = Option.getOrUndefined(lost);
        const thirdDeadline = (yield* deadlineOf(loss?.next)) ?? secondDeadline;
        yield* phase(
          "lanes",
          thirdDeadline,
          Effect.gen(function* () {
            yield* air.submit("long", "line", item(longSeconds));
            const longStarted = yield* started("long", thirdDeadline);
            yield* pieces.sleepUntil(longStarted + cutAfterMs, thirdDeadline);
            const cutFromMs = yield* run.now;
            yield* air.submit("cutter", "urgent", item());
            yield* run.mark("cutter submitted");
            // They build while the cutter airs, and air after it in their lanes' order.
            yield* air.submit("old", "swap", item());
            yield* air.submit("new", "swap", item());
            yield* air.submit("solo", "solo", item());
            const refusal = yield* air.playout
              .submit({ key: Playout.ItemKey.make("busy"), lane: "solo", request: item() })
              .pipe(
                Effect.match({
                  onFailure: (error) => error._tag,
                  onSuccess: () => undefined,
                }),
              );
            // Accepted after all, it goes before it can air.
            if (refusal === undefined) yield* air.playout.withdraw(Playout.ItemKey.make("busy"));
            yield* run.mark("lane conflicts submitted", refusal ?? "busy accepted");
            const cutterStarted = yield* started("cutter", thirdDeadline);
            yield* pieces.sleepUntil(cutterStarted + 2 * pieces.seamMs + 250, thirdDeadline);
            const seams = (yield* run.evidence).playout?.seams ?? [];
            yield* pieces.recordPlayout({
              seams: [...seams, yield* air.seam("long", "cutter", false)],
            });
            // H3's stop names no clip: a second one stops whatever plays by then.
            const stops = pieces.commandSpans(yield* run.evidence, cutFromMs, "stop").length;
            yield* pieces.recordPlayout({ stops });
            const cut = yield* SubscriptionRef.get(air.items);
            const long = cut.get("long");
            const cutter = cut.get("cutter");
            const order = air.starts();
            yield* pieces.judge(
              "a cut lane's clip stops a lower lane's playing clip",
              [
                long?.termination === "stopped",
                `the long clip ended ${long?.termination ?? long?.last ?? "untracked"}`,
              ],
              [
                order[order.indexOf("long") + 1] === "cutter",
                "the cut-lane clip did not start next",
              ],
              [
                cutter?.termination !== "stopped",
                `the cut-lane clip was itself stopped after ${String(cutter?.airedSeconds ?? "?")} s`,
              ],
              [stops === 1, `${stops} stops were sent for one cut`],
            );
            yield* run.mark("cut observed");
            yield* started("solo", thirdDeadline);
            const conflicts = yield* SubscriptionRef.get(air.items);
            const old = conflicts.get("old");
            yield* pieces.judge(
              "a replacing lane's new item takes its waiting item's place",
              [old?.startedMs === undefined, "the replaced item started"],
              [old?.dropped === "replaced", `the replaced item ended ${old?.last ?? "untracked"}`],
              [conflicts.get("new")?.startedMs !== undefined, "the new item never started"],
            );
            yield* pieces.judge(
              "a skipping lane refuses an item while one waits",
              [refusal === "LaneBusy", `the second item was ${refusal ?? "accepted"}`],
              [conflicts.get("solo")?.startedMs !== undefined, "the waiting item never started"],
            );
            yield* run.mark("lanes observed");
          }),
        );

        // The drain, on session 3: what was accepted airs, then the show ends.
        yield* phase(
          "drain",
          thirdDeadline,
          Effect.gen(function* () {
            yield* air.playout.drain({ finish: "accepted" });
            const drainedMs = yield* run.now;
            yield* pieces.recordPlayout({ drainedMs });
            yield* run.mark("drained");
            const all = yield* SubscriptionRef.get(air.items);
            const unfinished = (loss?.waiting ?? []).filter(
              (key) =>
                all.get(key)?.termination !== "finished" || all.get(key)?.sessionId !== loss?.next,
            );
            yield* pieces.judge("the drain completes on the last session", [
              unfinished.length === 0,
              `${unfinished.join(", ")} did not finish on the last session`,
            ]);
          }),
        );

        // What the whole show did, judged once every phase ran.
        const all = yield* SubscriptionRef.get(air.items);
        const asRun = air.asRun();
        const statusesOf = (key: string) =>
          asRun.filter((entry) => entry.key === key).map((entry) => entry.status);
        if (loss !== undefined) {
          // A clip lost on air fails; one not yet aired is accepted again and rebuilt.
          const failedLost = loss.playing.filter((key) => {
            const why = whyFailed(statusesOf(key).at(-1));
            return why?.kind === "lost" && why.lost === loss.lostId;
          });
          yield* pieces.judge(
            "a clip on air with the lost session fails as lost",
            [loss.playing.length > 0, "nothing was on air when the session ended"],
            [
              failedLost.length === loss.playing.length,
              `${loss.playing.filter((key) => !failedLost.includes(key)).join(", ")} did not fail as lost`,
            ],
          );
          const carried = loss.waiting.filter((key) => {
            const statuses = statusesOf(key);
            return (
              statuses.some(
                (status) => status._tag === "Accepted" && status.carried?.sessionId === loss.lostId,
              ) &&
              statuses.some((status) => status._tag === "Started" && status.sessionId === loss.next)
            );
          });
          yield* pieces.judge(
            "a clip not yet aired is carried to the next session",
            [loss.waiting.length > 0, "nothing waited on the session when it ended"],
            [
              carried.length === loss.waiting.length,
              `${loss.waiting.filter((key) => !carried.includes(key)).join(", ")} was not carried and aired`,
            ],
          );
          if (loss.by === "moderation") {
            const blamed = sessionEvents(yield* SubscriptionRef.get(log)).some(
              ({ event }) => event._tag === "Moderated" && event.key === "flagged",
            );
            const blamedItem = all.get("flagged");
            yield* pieces.judge(
              "moderation fails the item it blames",
              [blamed, "the moderation verdict blamed no item, or another"],
              [
                whyFailed(statusesOf("flagged").at(-1))?.kind === "moderated" &&
                  blamedItem?.startedMs === undefined,
                `the flagged item ended ${blamedItem?.last ?? "untracked"}`,
              ],
            );
          }
        }
        const events = sessionEvents(yield* SubscriptionRef.get(log));
        const switches = events.flatMap(({ atMs, event }) =>
          event._tag === "Switched"
            ? [{ atMs, from: event.from, to: event.to, decision: event.decision }]
            : [],
        );
        const lossMs = yield* Effect.map(Ref.get(shown), (facts) => facts.loss?.requestedMs);
        yield* pieces.judge(
          "one planned switch",
          [switches.length === 1, `${switches.length} switches`],
          [
            !events.some(
              ({ atMs, event }) => event._tag === "Replaced" && atMs < (lossMs ?? Infinity),
            ),
            "a session was replaced before the planned loss",
          ],
        );
        const switchMs = switches[0]?.atMs;
        const sessions = (yield* run.evidence).sessions.map((session) => session.id);
        // The session on air at `atMs`: the first until the switch, the second until the loss.
        const onAirAt = (atMs: number) => {
          if (switchMs === undefined || atMs < switchMs) return sessions[0];
          if (lossMs === undefined || atMs <= lossMs) return sessions[1];
          return sessions[2];
        };
        const misplaced = [...all.values()].filter(
          (aired) => aired.startedMs !== undefined && aired.sessionId !== onAirAt(aired.startedMs),
        );
        yield* pieces.judge("each item airs on the session on air", [
          misplaced.length === 0,
          `${misplaced.map((aired) => aired.key).join(", ")} aired on another session`,
        ]);
        const framesBySession = sessions.map((sessionId) =>
          [...all.values()]
            .filter((aired) => aired.sessionId === sessionId)
            .reduce(
              (total, aired) =>
                aired.startedMs === undefined || aired.endedMs === undefined
                  ? total
                  : total + air.video.framesBetween(aired.startedMs, aired.endedMs),
              0,
            ),
        );
        const framesByItem: Record<string, number> = {};
        for (const aired of all.values())
          if (aired.startedMs !== undefined && aired.endedMs !== undefined)
            framesByItem[aired.key] = air.video.framesBetween(aired.startedMs, aired.endedMs);
        yield* pieces.recordPlayout({ switches, framesByItem });
        yield* pieces.judge("video from every session", [
          framesBySession.length === 3 && framesBySession.every((frames) => frames > 0),
          `the picture carried ${framesBySession.join(", ")} item frames by session`,
        ]);
        // Filler covers the air, except where the show broke it on purpose: from the drop
        // until the picture is back, and from the ended session on, after which it drains.
        const recovery = (yield* Ref.get(shown)).recovery;
        const starved = yield* Effect.map(SubscriptionRef.get(log), (logged) =>
          logged.flatMap(({ atMs, event }) => (event._tag === "Starved" ? [atMs] : [])),
        );
        const broken = (atMs: number) =>
          (recovery !== undefined &&
            atMs >= recovery.droppedMs &&
            atMs <= (recovery.firstFrameMs ?? Infinity)) ||
          (lossMs !== undefined && atMs >= lossMs);
        const steady = starved.filter((atMs) => !broken(atMs));
        yield* pieces.judge(
          "filler keeps the air covered",
          [fills.length > 0, "the playout never asked for filler"],
          [steady.length === 0, `the air starved at ${steady.join(", ")} ms`],
        );
        yield* recordShow;
        const record = (yield* run.evidence).show;
        // A gap is the show's own doing where it reaches into the cut connection, or from the
        // loss until the lanes begin: session 3 rebuilds c1 and then builds the long clip in
        // H3's one build slot, so no filler can cover a late build. The switch's is recorded:
        // Playout promises only that it follows the grace.
        const resumedMs =
          all.get("long")?.startedMs ??
          Math.min(
            ...[...all.values()].flatMap((aired) =>
              aired.startedMs !== undefined && aired.startedMs >= (lossMs ?? Infinity)
                ? [aired.startedMs]
                : [],
            ),
            ...(record?.filler ?? []).flatMap((clip) =>
              clip.phase === "Started" && clip.atMs >= (lossMs ?? Infinity) ? [clip.atMs] : [],
            ),
          );
        const within = (gap: Evidence.ShowRecord["gaps"][number], fromMs: number, toMs: number) =>
          gap.toMs >= fromMs && gap.fromMs <= toMs;
        const openGaps = (record?.gaps ?? []).filter(
          (gap) =>
            gap.toMs - gap.fromMs > gapMs &&
            !(
              recovery !== undefined &&
              within(gap, recovery.droppedMs, recovery.firstFrameMs ?? Infinity)
            ) &&
            !(switchMs !== undefined && within(gap, switchMs, switchMs)) &&
            !(lossMs !== undefined && within(gap, lossMs, resumedMs)),
        );
        yield* pieces.judge("no gap on air between clips, filler included", [
          openGaps.length === 0,
          openGaps
            .map(
              (gap) =>
                `${gap.ending} to ${gap.next} left ${Math.round(gap.toMs - gap.fromMs)} ms at ${gap.fromMs} ms`,
            )
            .join(", "),
        ]);
        const readings = record?.playing;
        yield* pieces.judge(
          "the state names the clip on air as it started",
          [(readings?.named ?? 0) > 0, "the state never named a clip as it started"],
          [
            (readings?.mismatched ?? []).length === 0,
            (readings?.mismatched ?? [])
              .map(
                (reading) =>
                  `${reading.key} started at ${reading.startedMs} ms, the state named ${reading.stateKey === undefined ? "none" : `${reading.stateKey} from ${reading.stateStartedMs} ms`}`,
              )
              .join(", "),
          ],
        );
        const overflows = record?.readerOverflows ?? [];
        yield* pieces.judge("the show's own reader keeps up", [
          overflows.length === 0,
          overflows
            .map((overflow) => `the ${overflow.track} reader fell behind at ${overflow.atMs} ms`)
            .join(", "),
        ]);
        yield* run.mark("show observed");
      }),
  );
});

/** When a cue of `L1` fell due, from its observed start: a second after it, or before its end. */
const cueDue = (name: string, aired: Evidence.Item, clipSeconds: number): number | undefined => {
  if (aired.startedMs === undefined) return undefined;
  if (name === "in") return aired.startedMs + 1000;
  if (name === "out") return aired.startedMs + (aired.seconds ?? clipSeconds) * 1000 - 1000;
  return undefined;
};

/**
 * The loss by moderation: an item carrying the operator's prompt goes in
 * behind the guard on air and c1, so it could air only after both, and what
 * follows is watched for as `cut` watches it. What moderation does is
 * recorded, never judged; "ended" means the session is ending over it. Built
 * and unflagged, or carried after its session ended with no verdict, the item
 * is withdrawn, so it never airs and no later session builds it.
 */
const moderate = Effect.fnUntraced(function* <Req extends Family.RequestInput, C extends H3.Clip>(
  family: Family.Family<Req, C>,
  pieces: Pieces,
  air: Air<Req>,
  log: SubscriptionRef.SubscriptionRef<ReadonlyArray<Logged>>,
  input: {
    readonly flagged: Redacted.Redacted<string>;
    readonly sessionId: string | undefined;
    readonly deadline: number;
  },
) {
  const run = yield* Run;
  const target = yield* Target;
  const { flagged, sessionId, deadline } = input;
  yield* run.secret(flagged);
  const submittedMs = yield* run.now;
  yield* air.submit(
    "flagged",
    "line",
    family.request({ prompt: Redacted.value(flagged), seconds: family.lengths.short }),
  );
  yield* run.mark("flagged item queued behind the guard");
  const since = Effect.map(SubscriptionRef.get(log), (all) =>
    sessionEvents(all).filter(({ atMs }) => atMs >= submittedMs),
  );
  const ending = Effect.map(since, (events) =>
    events.some(
      ({ event }) =>
        (event._tag === "Moderated" && event.action === "terminate") ||
        (event._tag === "Replaced" && event.from === sessionId),
    ),
  );
  const verdictLog = () =>
    air.sessionLog().find((logged) => logged.atMs >= submittedMs && logged.verdict !== undefined);
  const settled = Effect.gen(function* () {
    const last = (yield* SubscriptionRef.get(air.items)).get("flagged")?.last;
    return (
      last === "Failed" ||
      last === "Unknown" ||
      last === "Ready" ||
      (yield* ending) ||
      verdictLog() !== undefined
    );
  });
  yield* pieces.watch(
    settled,
    Math.min(deadline, (yield* Clock.currentTimeMillis) + moderationWaitMs),
  );
  const ended = yield* ending;
  const unsettled = (yield* SubscriptionRef.get(air.items)).get("flagged");
  if (unsettled?.failed === undefined && unsettled?.startedMs === undefined)
    yield* air.playout.withdraw(Playout.ItemKey.make("flagged"));
  // A session ending over the item is closed once the playout gives up on it.
  const closed = Effect.map(
    run.evidence,
    (evidence) =>
      evidence.sessions.find((session) => session.id === sessionId)?.close !== undefined,
  );
  if (ended)
    yield* pieces.watch(
      closed,
      Math.min(deadline, (yield* Clock.currentTimeMillis) + moderationWaitMs),
    );
  const [enqueueSpan] = pieces.commandSpans(yield* run.evidence, submittedMs, "enqueue");
  const enqueueRequest = enqueueSpan?.attributes["reactor.request.id"];
  const verdictLogged = verdictLog();
  const verdict = verdictLogged?.verdict;
  const grant = sessionId === undefined ? undefined : air.grant(sessionId);
  const read =
    sessionId === undefined || grant === undefined
      ? undefined
      : {
          atMs: yield* run.now,
          ...(yield* Probes.readSession({
            apiUrl: target.apiUrl,
            sessionId,
            credential: grant.jwt,
          })),
        };
  const flaggedItem = (yield* SubscriptionRef.get(air.items)).get("flagged");
  const moderation: Evidence.ModerationRecord = {
    promptLength: Redacted.value(flagged).length,
    submittedMs,
    statuses: air
      .asRun()
      .filter((entry) => entry.key === "flagged")
      .map(({ atMs, status }) => {
        const detail = statusDetail(status);
        return { atMs, status: status._tag, ...(detail === undefined ? {} : { detail }) };
      }),
    ...(enqueueSpan === undefined
      ? {}
      : {
          enqueue: {
            startMs: enqueueSpan.startMs,
            ...(enqueueSpan.durationMs === undefined ? {} : { durationMs: enqueueSpan.durationMs }),
            status: enqueueSpan.status,
            ...(typeof enqueueRequest === "string" ? { requestId: enqueueRequest } : {}),
          },
        }),
    ...(verdictLogged === undefined || verdict === undefined
      ? {}
      : {
          verdict: {
            atMs: verdictLogged.atMs,
            action: pieces.identifier(verdict.action) ? verdict.action : "(text)",
            categories: verdict.categories.filter(pieces.identifier),
            ...(verdict.inputKind === undefined || !pieces.identifier(verdict.inputKind)
              ? {}
              : { inputKind: verdict.inputKind }),
            ...(verdict.command === undefined || !pieces.identifier(verdict.command)
              ? {}
              : { command: verdict.command }),
            ...(verdict.requestId === undefined ? {} : { requestId: verdict.requestId }),
            namesEnqueue: verdict.requestId !== undefined && verdict.requestId === enqueueRequest,
          },
        }),
    session: air
      .sessionLog()
      .filter((logged) => logged.atMs >= submittedMs)
      .map(({ atMs, event }) => ({ atMs, event })),
    playout: (yield* since).map(({ atMs, event }) => ({
      atMs,
      event: pieces.sessionEventText(event),
    })),
    ...(read === undefined ? {} : { read }),
    flagged: verdict !== undefined || ended,
    aired: flaggedItem?.startedMs !== undefined,
  };
  yield* run.update((evidence) => ({ ...evidence, moderation }));
  yield* run.mark(
    "moderation observed",
    moderation.flagged ? "flagged" : moderation.aired ? "aired" : "not flagged",
  );
  return ended ? ("ended" as const) : ("let through" as const);
});
