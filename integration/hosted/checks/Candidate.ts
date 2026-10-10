/** One bounded release qualification, over one H3 and one FastH3 allocation. */
import * as Clock from "effect/Clock";
import type * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type * as H3 from "reactor-effect-client/H3";
import * as Ledger from "reactor-effect-client/Ledger";
import * as Playout from "reactor-effect-client/Playout";
import type * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { Pieces } from "../Checks.js";
import type { Evidence, Item } from "../Evidence.js";
import { failedOf } from "../Evidence.js";
import * as Family from "../Family.js";
import * as Media from "../Media.js";
import { recorded, Run } from "../Run.js";
import { acceptGrant, provenGrant, tokenSecondsFor, workSecondsFor } from "../Spend.js";
import { Target } from "../Target.js";

type Phase = NonNullable<Evidence["candidate"]>["phases"][number];

/**
 * The readyBy judgment falls 8 s into the filler before it, which must still be on air then.
 * The family's longest clip ran hosted FastH3's phase past its 80 s deadline.
 */
const fillerSeconds = 10;

const phase = Effect.fnUntraced(function* <Req extends Family.RequestInput, C extends H3.Clip>(
  family: Family.Family<Req, C>,
  pieces: Pieces,
  grants: Map<string, CoordinatorClient.TokenGrant>,
) {
  const run = yield* Run;
  const phaseStartedMs = yield* run.now;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const ledger = yield* Ledger.Ledger;
  const video = Media.videoLog();
  const items = yield* SubscriptionRef.make<ReadonlyMap<string, Item>>(new Map());
  const starts: Array<string> = [];
  const filler = yield* SubscriptionRef.make<
    { readonly atMs: number; readonly seconds: number } | undefined
  >(undefined);
  const key = (name: string) => Playout.ItemKey.make(`${family.key}:${name}`);
  const track = Effect.fnUntraced(function* (name: string) {
    const itemKey = key(name);
    const submittedMs = yield* run.now;
    yield* SubscriptionRef.update(items, (all) =>
      new Map(all).set(itemKey, { key: itemKey, submittedMs, last: "Accepted" }),
    );
    return itemKey;
  });
  const required = Effect.fnUntraced(function* (name: string, holds: boolean) {
    yield* run.judge(
      `${family.key}: ${name}`,
      holds ? undefined : "required evidence was not observed",
    );
    if (!holds) return yield* ReactorError.fromCode("InvalidState", `candidate: ${name}`);
  });
  const update = (fields: Partial<Phase>) =>
    run.update((evidence) => ({
      ...evidence,
      candidate: {
        phases: (evidence.candidate?.phases ?? []).map((phase) =>
          phase.model === family.modelName ? { ...phase, ...fields } : phase,
        ),
      },
    }));
  let deadline = 0;
  let sessionId = "";
  const grant = yield* coordinator.mintToken({
    modelName: family.modelName,
    maxSessionDuration: "90 seconds",
    expiresAfter: `${tokenSecondsFor("candidate")} seconds`,
  });
  yield* run.secret(grant.jwt);
  const proven = yield* provenGrant({ jwt: Redacted.value(grant.jwt), granted: grant.granted });
  yield* acceptGrant({ check: "candidate", model: family.modelName, granted: proven });
  yield* run.update((evidence) => ({
    ...evidence,
    grants: [...evidence.grants, { ...proven, expiresAt: grant.expiresAt }],
  }));
  const tokens = CoordinatorClient.fixedTokens(grant);
  const ClipQueued = Schema.Struct({ clip: family.clip });
  // The public opener callback supplies the raw observer needed for acceptance evidence.
  const opener: Ledger.Opener<Req, Reactor.Reactor | Crypto.Crypto | Run> = {
    model: family.modelName,
    open: (record) =>
      family.open({
        tokens,
        holdLastFrame: true,
        onAllocated: ({ session, allocation }) =>
          Effect.gen(function* () {
            sessionId = session.id;
            grants.set(session.id, grant);
            deadline = yield* pieces.allocated(session.id, grant, family.modelName);
            yield* record({ sessionId: allocation.sessionId, endsAt: allocation.endsAt });
            const entries = yield* ledger.entries;
            const recordedBeforeConnect = entries.some(
              (entry) =>
                entry.sessionId === session.id &&
                entry.model === family.modelName &&
                entry.state === "live",
            );
            yield* run.update((evidence) => ({
              ...evidence,
              candidate: {
                phases: [
                  ...(evidence.candidate?.phases ?? []),
                  {
                    model: family.modelName,
                    sessionId: session.id,
                    recordedBeforeConnect,
                    forecastOrder: [],
                    startOrder: [],
                    items: [],
                    groups: [],
                    media: [],
                    video: video.summary(),
                    uploads: [],
                    endReads: [],
                  },
                ],
              },
            }));
            yield* pieces
              .recordSessionEvents(session)
              .pipe(Effect.forkScoped({ startImmediately: true }));
            const observation = yield* session.observe({ capacity: 1024 });
            yield* observation.events.pipe(
              Stream.runForEach((event) =>
                event._tag === "Model" && event.kind === "message" && event.type === "clip_queued"
                  ? Schema.decodeUnknownEffect(ClipQueued)(event.data).pipe(
                      Effect.flatMap(({ clip }) => {
                        const metadata = [
                          ...[
                            key("firm"),
                            key("p1"),
                            key("p2"),
                            key("i1"),
                            key("i2"),
                            key("manual"),
                            key("ready"),
                          ],
                        ].find((key) => clip.metadata.includes(key));
                        if (metadata === undefined) return Effect.void;
                        const uploads = family.uploadsOf(clip);
                        return run.update((evidence) => ({
                          ...evidence,
                          candidate: {
                            phases: (evidence.candidate?.phases ?? []).map((phase) =>
                              phase.model !== family.modelName
                                ? phase
                                : {
                                    ...phase,
                                    uploads: [
                                      ...phase.uploads,
                                      {
                                        metadata,
                                        ...(uploads._tag === "References" &&
                                        uploads.hasReferenceAudio !== null
                                          ? { hasReferenceAudio: uploads.hasReferenceAudio }
                                          : {}),
                                        ...(uploads._tag === "Frame"
                                          ? { hasStartingFrame: uploads.hasStartingFrame }
                                          : {}),
                                        continued: uploads._tag === "Frame" && uploads.continued,
                                      },
                                    ],
                                  },
                            ),
                          },
                        }));
                      }),
                    )
                  : Effect.void,
              ),
              Effect.orDie,
              Effect.forkScoped({ startImmediately: true }),
            );
          }),
      }),
    resume: (entry) =>
      family.resume({
        tokens,
        allocation: {
          sessionId: entry.sessionId,
          ownership: "owned",
          model: entry.model,
          ...(entry.endsAt === undefined ? {} : { endsAt: entry.endsAt }),
        },
      }),
  };
  const source = yield* recorded(ledger.source(opener, { resume: false })).pipe(
    Effect.tapError((error) =>
      sessionId === ""
        ? Effect.void
        : Effect.flatMap(run.now, (requestedMs) =>
            pieces.closedWith(sessionId, requestedMs, { report: error.cleanup }),
          ),
    ),
  );
  const close = yield* Effect.cached(
    Effect.gen(function* () {
      const requestedMs = yield* run.now;
      const report = yield* source.close;
      yield* pieces.closedWith(source.sessionId, requestedMs, { report });
      return report;
    }).pipe(Effect.uninterruptible, Effect.orDie, Effect.provideService(Run, run)),
  );
  yield* Effect.addFinalizer(() => Effect.asVoid(close).pipe(Effect.orDie));
  let opened = false;
  const open = Effect.gen(function* () {
    if (opened)
      return yield* ReactorError.fromCode(
        "InvalidState",
        "candidate cannot allocate a replacement",
      );
    opened = true;
    return { ...source, lifetime: Duration.infinity, close };
  });
  const playout = yield* Playout.make<never, Req>({
    model: family.clipModel,
    open,
    maxBuildsInFlight: 1,
    lanes: [{ name: "timed", strict: true }, { name: "line", strict: true }, { name: "manual" }],
    filler: {
      runway: { floor: "1 second", target: "1 second" },
      lengths: { min: fillerSeconds, max: fillerSeconds },
      clip: () => family.request({ prompt: Family.prompt, seconds: fillerSeconds }),
      protect: "order",
    },
  });
  yield* playout.events.pipe(
    Stream.runForEach((event) => {
      if (event._tag === "Filler" && event.phase === "Started" && event.seconds !== undefined)
        return SubscriptionRef.set(filler, { atMs: event.at - run.origin, seconds: event.seconds });
      return Effect.void;
    }),
    Effect.forkScoped({ startImmediately: true }),
  );
  yield* playout.asRun.pipe(
    Stream.runForEach((event) => {
      const atMs = event.at - run.origin;
      const status = event.status;
      if (status._tag === "Started") starts.push(event.key);
      const uncertain =
        status._tag === "Unknown"
          ? run.update((evidence) => ({ ...evidence, outcomes: [...evidence.outcomes, "unknown"] }))
          : Effect.void;
      return uncertain.pipe(
        Effect.andThen(
          SubscriptionRef.update(items, (all) => {
            const item = all.get(event.key);
            if (item === undefined) return all;
            const next = { ...item, last: status._tag };
            switch (status._tag) {
              case "Ready":
                return new Map(all).set(event.key, {
                  ...next,
                  readyMs: atMs,
                  sessionId: status.sessionId,
                });
              case "Started":
                return new Map(all).set(event.key, {
                  ...next,
                  startedMs: atMs,
                  seconds: status.seconds,
                  sessionId: status.sessionId,
                });
              case "Ended":
                return new Map(all).set(event.key, {
                  ...next,
                  endedMs: atMs,
                  termination: status.termination,
                  airedSeconds: status.airedSeconds,
                });
              case "Dropped":
                return new Map(all).set(event.key, { ...next, dropped: status.reason });
              case "Failed":
                return new Map(all).set(event.key, { ...next, failed: failedOf(status.reason) });
              default:
                return new Map(all).set(event.key, next);
            }
          }),
        ),
      );
    }),
    Effect.forkScoped({ startImmediately: true }),
  );
  yield* pieces.readInto(playout.video, video).pipe(Effect.forkScoped({ startImmediately: true }));
  const save = Effect.gen(function* () {
    const all = yield* SubscriptionRef.get(items);
    const observed = [...all.values()];
    const observedAt = yield* run.now;
    yield* update({
      items: observed,
      startOrder: [...starts],
      video: video.summary(),
      media: observed.flatMap((item) => {
        if (item.startedMs === undefined) return [];
        const end = item.endedMs ?? observedAt;
        const firstFrameMs = video.firstAfter(item.startedMs, end);
        const reason = video.live(item.startedMs, end);
        return [
          {
            key: item.key,
            startedMs: item.startedMs,
            ...(firstFrameMs === undefined ? {} : { firstFrameMs }),
            frames: video.framesBetween(item.startedMs, end),
            live: reason === undefined,
            ...(reason === undefined ? {} : { reason }),
          },
        ];
      }),
    });
  });
  yield* Effect.addFinalizer(() => save);
  const when = <A>(find: (items: ReadonlyMap<string, Item>) => A | undefined) =>
    pieces.waitFor(items, find, deadline);
  const request = () => family.request({ prompt: Family.prompt, seconds: family.lengths.short });
  const batch = yield* recorded(
    playout.edit([
      {
        _tag: "Submit",
        item: {
          key: yield* track("manual"),
          lane: "manual",
          request: request(),
          start: { _tag: "Manual" },
          // notBefore gates admission to a build; an old held batch would wait for this add.
          window: { firm: false, notBefore: "50 seconds" },
        },
      },
      {
        _tag: "Submit",
        item: {
          key: yield* track("firm"),
          lane: "line",
          request: { ...family.withUploads(key("firm")), seconds: family.lengths.long },
          window: { firm: true, startBy: "30 seconds" },
        },
      },
    ]),
  );
  const firmStartedMs = yield* when((all) => all.get(key("firm"))?.startedMs);
  const manualAtStart = (yield* SubscriptionRef.get(items)).get(key("manual"));
  yield* batch.committed.pipe(Effect.timeout(yield* pieces.until(deadline)));
  yield* update({
    firm: {
      startedMs: firmStartedMs,
      committedMs: yield* run.now,
      manualWasReady: manualAtStart?.readyMs !== undefined,
      manualBuildAfterMs: (manualAtStart?.submittedMs ?? 0) + 50_000,
    },
  });
  yield* required(
    "a firm batch airs before its Manual add is Ready",
    manualAtStart !== undefined &&
      manualAtStart.readyMs === undefined &&
      manualAtStart.startedMs === undefined,
  );
  const group = yield* recorded(
    playout.submitGroup({
      key: key("group"),
      lane: "line",
      parts: [
        { key: yield* track("p1"), request: request(), continuity: "previous" },
        { key: yield* track("p2"), request: request() },
      ],
    }),
  );
  const inserted = yield* recorded(
    playout.insertGroup({
      key: key("inserted"),
      after: key("p1"),
      parts: [
        { key: yield* track("i1"), request: request() },
        { key: yield* track("i2"), request: request() },
      ],
    }),
  );
  const planned: ReadonlyArray<string> = [key("p1"), key("i1"), key("i2"), key("p2")];
  const forecast = yield* playout.forecast;
  const forecastOrder = forecast.clips.flatMap((entry) =>
    entry.clip?._tag === "Item" && planned.includes(entry.clip.key) ? [entry.clip.key] : [],
  );
  yield* update({ forecastOrder });
  yield* required(
    "the forecast preserves strict places and inserted groups",
    forecastOrder.join(",") === planned.join(",") &&
      Playout.forecastGroup(forecast, key("group"))?.parts.length === 2 &&
      Playout.forecastGroup(forecast, key("inserted"))?.parts.length === 2,
  );
  const ended = yield* group.outcome.pipe(Effect.timeout(yield* pieces.until(deadline)));
  const insertedEnd = yield* inserted.outcome.pipe(Effect.timeout(yield* pieces.until(deadline)));
  const outcomes: ReadonlyArray<readonly [string, Playout.GroupOutcome]> = [
    ["group", ended],
    ["inserted", insertedEnd],
  ];
  yield* update({
    groups: outcomes.map(([name, outcome]) => ({
      key: key(name),
      outcome: outcome._tag,
      played: outcome._tag === "Aired" ? outcome.played : 0,
      parts: outcome._tag === "Aired" ? outcome.parts.map((part) => part._tag) : [],
      ...(outcome._tag === "Aired" && outcome.stopped !== undefined
        ? { stoppedPart: outcome.stopped.part }
        : {}),
    })),
  });
  yield* when((all) => all.get(key("p2"))?.endedMs);
  yield* required(
    "groups air all their places in strict order",
    starts.filter((started) => planned.includes(started)).join(",") === planned.join(",") &&
      ended._tag === "Aired" &&
      ended.played === 2 &&
      insertedEnd._tag === "Aired" &&
      insertedEnd.played === 2,
  );
  yield* save;
  const groupItems = yield* SubscriptionRef.get(items);
  yield* required(
    "decoded video arrives during each ordered clip",
    planned.every((key) => {
      const item = groupItems.get(key);
      return (
        item?.startedMs !== undefined &&
        item.endedMs !== undefined &&
        video.live(item.startedMs, item.endedMs) === undefined
      );
    }),
  );
  const p2End = (yield* SubscriptionRef.get(items)).get(key("p2"))?.endedMs ?? 0;
  const fillerBeforeReady = yield* pieces.waitFor(
    filler,
    (value) => (value !== undefined && value.atMs >= p2End ? value : undefined),
    deadline,
  );
  const now = yield* Clock.currentTimeMillis;
  const time = now + 1_000;
  const cutoff = time + 7_000;
  yield* recorded(
    playout.submit({
      key: yield* track("ready"),
      lane: "timed",
      request: request(),
      start: { _tag: "At", time, late: { _tag: "readyBy", by: "7 seconds" } },
    }),
  );
  const missed = yield* recorded(
    playout.submit({
      key: yield* track("missed"),
      lane: "timed",
      request: request(),
      start: { _tag: "At", time: now - 1_000, late: { _tag: "readyBy", by: 0 } },
    }),
  );
  const missing = yield* missed.outcome.pipe(Effect.timeout(yield* pieces.until(deadline)));
  yield* pieces.sleepUntil(cutoff - run.origin + 100, deadline);
  const waiting = (yield* SubscriptionRef.get(items)).get(key("ready"));
  yield* required(
    "readyBy keeps a Ready clip waiting past its cutoff for filler",
    waiting?.readyMs !== undefined &&
      waiting.readyMs <= cutoff - run.origin &&
      waiting.startedMs === undefined &&
      waiting.dropped === undefined,
  );
  yield* when((all) => all.get(key("ready"))?.startedMs);
  yield* required(
    "readyBy drops an overdue clip that was not Ready",
    missing._tag === "Dropped" && missing.reason === "late",
  );
  yield* recorded(playout.release(key("manual")));
  yield* when((all) => all.get(key("manual"))?.endedMs);
  const ready = (yield* SubscriptionRef.get(items)).get(key("ready"));
  yield* update({
    readyBy: {
      timeMs: time - run.origin,
      cutoffMs: cutoff - run.origin,
      ...(ready?.readyMs === undefined ? {} : { readyMs: ready.readyMs }),
      ...(ready?.startedMs === undefined ? {} : { startedMs: ready.startedMs }),
      missed: missing._tag,
      filler: {
        startedMs: fillerBeforeReady.atMs,
        seconds: fillerBeforeReady.seconds,
        predictedEndMs: fillerBeforeReady.atMs + fillerBeforeReady.seconds * 1_000,
      },
    },
  });
  yield* required(
    "readyBy airs after the cutoff once filler ends",
    ready?.startedMs !== undefined && ready.startedMs > cutoff - run.origin,
  );
  yield* playout.drain({ finish: "accepted" }).pipe(Effect.timeout(yield* pieces.until(deadline)));
  yield* save;
  const allItems = yield* SubscriptionRef.get(items);
  yield* required(
    "decoded video arrives during every accepted clip",
    ["firm", "p1", "p2", "i1", "i2", "ready", "manual"].every((name) => {
      const item = allItems.get(key(name));
      return (
        item?.startedMs !== undefined &&
        item.endedMs !== undefined &&
        video.live(item.startedMs, item.endedMs) === undefined
      );
    }),
  );
  const current = (yield* run.evidence).candidate?.phases.find(
    (phase) => phase.model === family.modelName,
  );
  yield* required("live decoded video", video.live(0) === undefined);
  const upload = current?.uploads.find((upload) => upload.metadata === key("firm"));
  yield* required(
    "the provider reports the uploaded input",
    family.key === "h3" ? upload?.hasReferenceAudio === true : upload?.hasStartingFrame === true,
  );
  if (family.key === "fast-h3")
    yield* required(
      "the continuation reaches the provider",
      current?.uploads.some((upload) => upload.continued) === true,
    );
  const report = yield* close;
  const evidence = yield* run.evidence;
  const unknown =
    evidence.outcomes.includes("unknown") ||
    evidence.spans.some(
      (span) =>
        span.startMs >= phaseStartedMs && span.attributes["reactor.command.outcome"] === "unknown",
    );
  if (unknown)
    yield* run.update((evidence) => ({ ...evidence, outcomes: [...evidence.outcomes, "unknown"] }));
  yield* required("every command has a known outcome", !unknown);
  yield* required("the source confirms termination", report.remote.confirmed);
  const endReads: Array<Phase["endReads"][number]> = [];
  const readEnd = Effect.gen(function* () {
    const state = yield* coordinator.inspect(sessionId).pipe(
      Effect.map((inspection) => (inspection.state === "CLOSED" ? "CLOSED" : "other")),
      Effect.catchIf(
        (error) => error.reason._tag === "Http" && error.reason.status === 404,
        () => Effect.succeed("absent" as const),
      ),
    );
    endReads.push({ atMs: yield* run.now, state });
    yield* update({ endReads: [...endReads] });
    return state;
  });
  const first = yield* readEnd;
  const second = first === "absent" ? yield* readEnd : undefined;
  yield* required(
    "an independent coordinator read confirms the end",
    first === "CLOSED" || second === "CLOSED" || (first === "absent" && second === "absent"),
  );
  const remaining = yield* ledger.entries;
  yield* update({ ledgerAfterClose: remaining.length });
  yield* required("the Ledger forgets the confirmed session", remaining.length === 0);
  yield* required(
    "the Ledger recorded its session before connection",
    current?.recordedBeforeConnect === true,
  );
  yield* run.mark(`${family.key} candidate passed`);
});

/** No phase follows a failed required predicate or an unconfirmed end. */
export const candidate = (pieces: Pieces) =>
  pieces
    .withSessions((grants) =>
      phase(Family.h3, pieces, grants).pipe(
        Effect.timeout(Duration.seconds(workSecondsFor("candidate"))),
        Effect.scoped,
        Effect.andThen(
          phase(Family.fastH3, pieces, grants).pipe(
            Effect.timeout(Duration.seconds(workSecondsFor("candidate"))),
            Effect.scoped,
          ),
        ),
      ),
    )
    .pipe(
      // Each CLI check is an application entrypoint; its Ledger owns this run's scope.
      // @effect-diagnostics-next-line strictEffectProvide:off
      Effect.provide(
        Layer.unwrap(
          Effect.gen(function* () {
            const run = yield* Run;
            const target = yield* Target;
            return Ledger.layerFile(`${run.file}.sessions`, { ending: Schedule.recurs(0) }).pipe(
              Layer.provideMerge(
                CoordinatorClient.layer({ apiUrl: target.apiUrl, apiKey: target.apiKey }),
              ),
            );
          }),
        ),
      ),
    );
