/**
 * The checks, each one Effect over the public API: it spends only through the
 * sessions it opens, records what it sees in the run's evidence as it goes,
 * and judges its criteria. `Target` decides whether that is hosted Reactor or
 * `ReactorTest`; nothing here knows which.
 */
import type * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as H3 from "reactor-effect-client/H3";
import type * as FastH3 from "reactor-effect-client/FastH3";
import { recorder } from "reactor-effect-client/Media";
import type { Recorded, VideoFrame } from "reactor-effect-client/Media";
import * as Playout from "reactor-effect-client/Playout";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { CommandFailure } from "reactor-effect-client/ReactorError";
import type * as Session from "reactor-effect-client/Session";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import { adoption } from "./checks/Adoption.js";
import { dropped } from "./checks/Dropped.js";
import { avatar } from "./checks/Avatar.js";
import { character } from "./checks/Character.js";
import { fastH3 } from "./checks/FastH3.js";
import { rejoin } from "./checks/Rejoin.js";
import { tour } from "./checks/Tour.js";
import { show } from "./checks/Show.js";
import { showreel } from "./checks/Showreel.js";
import { unconnected } from "./checks/Unconnected.js";
import type * as Evidence from "./Evidence.js";
import { failedOf } from "./Evidence.js";
import type { Item, Seam, StatsSample } from "./Evidence.js";
import * as Family from "./Family.js";
import * as Media from "./Media.js";
import * as Probes from "./Probes.js";
import { recorded, Run } from "./Run.js";
import type { Check, ModelKey } from "./Spend.js";
import {
  acceptGrant,
  billedUsd,
  provenGrant,
  plans,
  sessionSeconds,
  tokenSecondsFor,
  workSecondsFor,
} from "./Spend.js";
import { prompt, Target } from "./Target.js";

const round = (value: number, places = 1) => Math.round(value * 10 ** places) / 10 ** places;
/** Every playout and queue clip asks for this long: a short build, a boundary every few seconds. */
const clipSeconds = 5;
/** Around a boundary, frames are looked for from this long before the end to twice it after the start. */
const seamMs = 1_500;

/** The time left until `deadline`, a Clock time in milliseconds, never negative. */
const until = (deadline: number) =>
  Effect.map(Clock.currentTimeMillis, (now) => Duration.millis(Math.max(0, deadline - now)));

/**
 * How long a clip's media is read once it started, and fresh frames after an attach. It outlasts
 * a clip, so a clip's end is read too. Rehearsals read as long, on their simulated clock.
 */
const windowMs = 6_000;

/** How long a check reads media: the window, never past `deadline`. */
const window = (deadline: number) =>
  Effect.map(until(deadline), (left) => Duration.min(Duration.millis(windowMs), left));

/** Sleeps until `atMs` after the run's start, never past `deadline`. */
const sleepUntil = Effect.fnUntraced(function* (atMs: number, deadline: number) {
  const run = yield* Run;
  const left = Duration.toMillis(yield* until(deadline));
  yield* Effect.sleep(Duration.millis(Math.min(left, Math.max(0, atMs - (yield* run.now)))));
});

/** Checks `done` every 250 ms until it holds or `deadline`, a Clock time, passes. */
const watch = Effect.fnUntraced(function* (done: Effect.Effect<boolean>, deadline: number) {
  while (!(yield* done) && (yield* Clock.currentTimeMillis) < deadline)
    yield* Effect.sleep("250 millis");
});

/**
 * Judges criterion `name` from its checks, each a condition that must hold and
 * what its failure says: the first that fails is the criterion's detail.
 */
const judge = (
  name: string,
  ...checks: ReadonlyArray<readonly [holds: boolean, failure: string]>
) => Effect.flatMap(Run, (run) => run.judge(name, checks.find(([holds]) => !holds)?.[1]));

const bump = (counts: Record<string, number>, key: string) => {
  counts[key] = (counts[key] ?? 0) + 1;
};

/** The library's command spans from `sinceMs` on, of `operation` when one is named. */
const commandSpans = (evidence: Evidence.Evidence, sinceMs: number, operation?: string) =>
  evidence.spans.filter(
    (span) =>
      span.name === "Session.command" &&
      span.startMs >= sinceMs &&
      (operation === undefined || span.attributes["reactor.operation"] === operation),
  );

/** Commands the library sent from `sinceMs` on, by name. */
const commandsSince = (evidence: Evidence.Evidence, sinceMs: number) => {
  const commands: Record<string, number> = {};
  for (const span of commandSpans(evidence, sinceMs))
    bump(commands, String(span.attributes["reactor.operation"]));
  return commands;
};

/** The HTTP status a refusal carries, when a reply came. */
const statusOf = (error: ReactorError) =>
  error.reason._tag === "Http" ? error.reason.status : undefined;

/**
 * A session read that failed, as a trail records it. Only 404 means gone; any
 * other refusal names its status, or its reason when no reply came.
 */
const failedRead = (error: ReactorError) => {
  const status = statusOf(error);
  if (status === 404) return "gone";
  return status === undefined ? `error:${error.reason._tag}` : `http:${status}`;
};

/** What H3 last reported of its state and queue. */
const factsOf = (snapshot: H3.ProviderSnapshot) =>
  snapshot._tag === "Ready" ? snapshot : snapshot.lastFacts;

/**
 * The adopting process's Reactor. Every read it makes of the session's
 * descriptor goes into the evidence: the time since the owner was killed, the
 * status, the state, and key names and codes only (`Probes.summarize`). Paid
 * run tokens 83d17eb7 read INACTIVE there, which the SDK then counted as ended.
 */
const adopting = Effect.fnUntraced(function* (sessionId: string, killedMs: number) {
  const run = yield* Run;
  const target = yield* Target;
  const http = yield* HttpClient.HttpClient;
  const descriptor = `/sessions/${encodeURIComponent(sessionId)}`;
  const client = HttpClient.transform(http, (effect, request) =>
    request.method !== "GET" || !request.url.endsWith(descriptor)
      ? effect
      : Effect.flatMap(effect, (response) =>
          Effect.gen(function* () {
            const body = yield* Effect.orElseSucceed(response.json, () => undefined);
            const atMs = yield* run.now;
            yield* run.update((evidence) => ({
              ...evidence,
              adopterReads: [
                ...(evidence.adopterReads ?? []),
                {
                  atMs,
                  sinceKillMs: atMs - killedMs,
                  ...Probes.summarize({ status: response.status, body }),
                },
              ],
            }));
            // The body was read here, so the SDK gets a copy of the reply.
            const bytes = yield* response.arrayBuffer;
            return HttpClientResponse.fromWeb(
              request,
              new Response(bytes, { status: response.status, headers: response.headers }),
            );
          }),
        ),
  );
  const coordinator = yield* CoordinatorClient.make({
    apiUrl: target.apiUrl,
    apiKey: target.apiKey,
  }).pipe(Effect.provideService(HttpClient.HttpClient, client));
  return yield* Reactor.make().pipe(
    Effect.provideService(CoordinatorClient.CoordinatorClient, coordinator),
  );
});

/** The first value `find` picks from `ref` as it changes, waited for until `deadline`. */
const waitFor = <A, B>(
  ref: SubscriptionRef.SubscriptionRef<A>,
  find: (value: A) => B | undefined,
  deadline: number,
) =>
  Effect.flatMap(until(deadline), (left) =>
    SubscriptionRef.changes(ref).pipe(
      Stream.map(find),
      Stream.filter(Predicate.isNotUndefined),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
      Effect.timeout(left),
    ),
  );

/** The coordinator as a session's token sees it. */
const withToken = (jwt: Redacted.Redacted<string>) =>
  Effect.flatMap(Target, (target) =>
    CoordinatorClient.make({ apiUrl: target.apiUrl, credential: Effect.succeed(jwt) }),
  );

/** Mints one session's token: its grant goes into the evidence, its JWT never does. */
const mint = Effect.fnUntraced(function* (
  check: Check,
  expiresAfterSeconds = tokenSecondsFor(check),
) {
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const cap = plans[check].seconds;
  const grant = yield* coordinator.mintToken({
    apiKey: target.apiKey,
    modelName: run.model.name,
    maxSessionDuration: cap === "unlimited" ? cap : `${cap} seconds`,
    expiresAfter: `${expiresAfterSeconds} seconds`,
  });
  yield* run.secret(grant.jwt);
  const granted = yield* provenGrant({ jwt: Redacted.value(grant.jwt), granted: grant.granted });
  yield* acceptGrant({ check, granted });
  yield* run.update((evidence) => ({
    ...evidence,
    grants: [...evidence.grants, { ...granted, expiresAt: grant.expiresAt }],
  }));
  yield* run.mark("minted");
  return grant;
});

/**
 * Tokens bound to one session, minted with the key as a server adopting a
 * dead owner's session does; `minted` sees each, and when it was asked for.
 */
const binder = Effect.fnUntraced(function* (
  expiresAfterSeconds: number,
  minted: (grant: CoordinatorClient.TokenGrant, sentAt: number) => Effect.Effect<void> = () =>
    Effect.void,
) {
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const bind = Effect.fnUntraced(function* (sessionId: string) {
    const sentAt = yield* Clock.currentTimeMillis;
    const token = yield* coordinator.mintToken({
      apiKey: target.apiKey,
      modelName: run.model.name,
      bind: [sessionId],
      expiresAfter: `${expiresAfterSeconds} seconds`,
    });
    yield* run.secret(token.jwt);
    yield* minted(token, sentAt);
    return token;
  });
  return { bind } satisfies Pick<CoordinatorClient.Tokens, "bind">;
});

/** The grant each session the check holds runs on, by session id. */
type Grants = Map<string, CoordinatorClient.TokenGrant>;

/** A grant's session cap, which the grant was proven not to exceed, in milliseconds. */
const capMs = (grant: CoordinatorClient.TokenGrant) =>
  (grant.maxSessionSeconds ?? sessionSeconds) * 1000;

/** Records a session the check holds from `allocatedAt` until its cap ends it at `capEndsAt`; its work deadline. */
const holding = Effect.fnUntraced(function* (
  sessionId: string,
  allocatedAt: number,
  capEndsAt: number | undefined,
) {
  const run = yield* Run;
  yield* run.update((evidence) => ({
    ...evidence,
    sessions: [
      ...evidence.sessions,
      {
        id: sessionId,
        allocatedMs: allocatedAt - run.origin,
        ...(capEndsAt === undefined
          ? {}
          : { capEndsAt: DateTime.formatIso(DateTime.makeUnsafe(capEndsAt)) }),
        trail: [],
      },
    ],
  }));
  return allocatedAt + workSecondsFor(run.check) * 1000;
});

/** Records a session the check allocated, and when its grant's cap ends it. Returns its work deadline. */
const allocated = Effect.fnUntraced(function* (
  sessionId: string,
  grant: CoordinatorClient.TokenGrant,
) {
  const now = yield* Clock.currentTimeMillis;
  const deadline = yield* holding(sessionId, now, now + capMs(grant));
  yield* (yield* Run).mark("allocated", sessionId);
  return deadline;
});

/** Records how a session's close went: reported now, or at `reportedMs` when it came earlier. */
const closedWith = Effect.fnUntraced(function* (
  sessionId: string,
  requestedMs: number,
  close:
    | { readonly report: Session.CloseReport }
    | { readonly termination: CoordinatorClient.Termination },
  reportedAt?: number,
) {
  const run = yield* Run;
  const reportedMs = reportedAt ?? (yield* run.now);
  const confirmed = "report" in close ? close.report.remote.confirmed : close.termination.confirmed;
  // The state a read confirmed is the provider's text: kept as the evidence keeps any.
  const keep = (termination: CoordinatorClient.Termination): CoordinatorClient.Termination =>
    termination.state === null
      ? termination
      : { ...termination, state: Probes.keptText(termination.state) };
  const kept =
    "report" in close
      ? { report: { ...close.report, remote: keep(close.report.remote) } }
      : { termination: keep(close.termination) };
  yield* run.update((evidence) => ({
    ...evidence,
    sessions: evidence.sessions.map((session) =>
      session.id === sessionId
        ? { ...session, close: { requestedMs, reportedMs, confirmed, ...kept } }
        : session,
    ),
  }));
  yield* run.mark("closed", `${sessionId} ${confirmed ? "confirmed" : "unconfirmed"}`);
});

/** Closes a session the check allocated, recording the library's report. */
const close = Effect.fnUntraced(function* (session: Pick<Session.Session, "id" | "close">) {
  const requestedMs = yield* (yield* Run).now;
  const report = yield* session.close;
  yield* closedWith(session.id, requestedMs, { report });
});

/** A session created on `grant`, recorded in `grants` and closed with the scope, and its work deadline. */
const create = Effect.fnUntraced(function* (grant: CoordinatorClient.TokenGrant, grants: Grants) {
  const reactor = yield* Reactor.Reactor;
  const run = yield* Run;
  let deadline = 0;
  const session = yield* recorded(
    reactor.create({
      model: run.model.name,
      tokens: CoordinatorClient.fixedTokens(grant),
      onAllocated: (allocation) =>
        Effect.gen(function* () {
          yield* recordSessionEvents(allocation).pipe(
            Effect.forkScoped({ startImmediately: true }),
          );
          const at = yield* allocated(allocation.id, grant);
          deadline = at;
          grants.set(allocation.id, grant);
        }),
    }),
  );
  yield* Effect.addFinalizer(() => Effect.ignore(close(session)));
  return { session, deadline };
});

/** The takeover's owner started on `grant`, its session recorded in `grants`, and the work deadline. */
const owned = Effect.fnUntraced(function* (
  grant: CoordinatorClient.TokenGrant,
  marker: string,
  grants: Grants,
  model: ModelKey,
) {
  const owner = yield* (yield* Target).owner(grant, marker, { model });
  const sessionId = owner.allocation.sessionId;
  grants.set(sessionId, grant);
  const cap = capMs(grant);
  const endsAt = owner.allocation.endsAt ?? ((yield* Clock.currentTimeMillis) + cap) / 1000;
  const deadline = yield* holding(sessionId, endsAt * 1000 - cap, endsAt * 1000);
  return { owner, sessionId, deadline };
});

/**
 * Whatever failed, ends each session the check still holds with a coordinator
 * from `ender`. Every DELETE goes out at once, so no session bills while
 * another's tries run, and before any end is recorded, so a save that fails
 * skips no session. Each end is recorded as reported when it was confirmed.
 */
export const endHeld = Effect.fnUntraced(
  function* <E, R>(ender: Effect.Effect<CoordinatorClient.CoordinatorClient["Service"], E, R>) {
    const run = yield* Run;
    const open = (yield* run.evidence).sessions.filter((session) => session.close === undefined);
    if (open.length === 0) return;
    const coordinator = yield* ender;
    const ends = yield* Effect.forEach(
      open,
      Effect.fnUntraced(function* (session) {
        const requestedMs = yield* run.now;
        const termination = yield* coordinator.terminate(session.id);
        return { sessionId: session.id, requestedMs, reportedMs: yield* run.now, termination };
      }),
      { concurrency: "unbounded" },
    );
    for (const { sessionId, requestedMs, reportedMs, termination } of ends)
      yield* Effect.ignore(closedWith(sessionId, requestedMs, { termination }, reportedMs));
  },
  (effect) => Effect.ignore(effect),
);

/**
 * After the sessions: follow the coordinator's view of each until it is
 * terminal or gone, bounded by its token, then price the run and judge that
 * every session's end was confirmed.
 */
const settle = Effect.fnUntraced(
  function* (grants: Grants) {
    const run = yield* Run;
    for (const [sessionId, grant] of grants) {
      const inspector = yield* withToken(grant.jwt);
      const deadline = Math.min(
        grant.expiresAt * 1000 - 5_000,
        (yield* Clock.currentTimeMillis) + 20_000,
      );
      const trail: Array<{ readonly atMs: number; readonly state: string }> = [];
      let terminalMs: number | undefined;
      while (terminalMs === undefined && (yield* Clock.currentTimeMillis) < deadline) {
        const state = yield* inspector.inspect(sessionId).pipe(
          Effect.match({
            onFailure: failedRead,
            onSuccess: (inspection) => Probes.keptText(inspection.state),
          }),
        );
        const atMs = yield* run.now;
        if (trail.at(-1)?.state !== state) trail.push({ atMs, state });
        if (state === "gone" || CoordinatorClient.isTerminal(state)) terminalMs = atMs;
        else yield* Effect.sleep("500 millis");
      }
      yield* run.update((evidence) => ({
        ...evidence,
        sessions: evidence.sessions.map((session) =>
          session.id === sessionId
            ? { ...session, trail, ...(terminalMs === undefined ? {} : { terminalMs }) }
            : session,
        ),
      }));
    }
    const evidence = yield* run.evidence;
    // Reactor bills from `ready` to the end. Counting from allocation, which precedes ready, to
    // the confirmed report or the first terminal read, every started unit whole, bills no less.
    const rate = evidence.budget.rate;
    const spent = evidence.sessions.map((session) => {
      const endedMs =
        session.close?.confirmed === true ? session.close.reportedMs : session.terminalMs;
      return rate === undefined || endedMs === undefined
        ? undefined
        : billedUsd({ rate, seconds: (endedMs - session.allocatedMs) / 1000 });
    });
    if (spent.length > 0 && spent.every(Predicate.isNotUndefined))
      yield* run.update((evidence) => ({
        ...evidence,
        budget: {
          ...evidence.budget,
          estimatedUsd: round(
            spent.reduce((total, usd) => total + usd, 0),
            4,
          ),
        },
      }));
    const unconfirmed = evidence.sessions.filter((session) => session.close?.confirmed !== true);
    yield* judge(
      "confirmed termination",
      [evidence.sessions.length > 0, "no session was recorded"],
      [
        unconfirmed.length === 0,
        `the end of ${unconfirmed.map((session) => session.id).join(", ")} was not confirmed`,
      ],
    );
    yield* run.mark("settled");
  },
  (effect) => Effect.ignore(effect),
);

/**
 * Runs a check's sessions in a scope of their own. Once it ends, `cleanup`
 * ends any it left open, then each is followed to its end on its grant.
 */
const withSessions = <A, E, R, R2 = never>(
  body: (grants: Grants) => Effect.Effect<A, E, R>,
  cleanup?: Effect.Effect<void, never, R2>,
) =>
  Effect.suspend(() => {
    const grants: Grants = new Map();
    return Effect.scoped(body(grants)).pipe(
      Effect.ensuring(cleanup ?? Effect.void),
      Effect.ensuring(settle(grants)),
    );
  });

/** Every clip object in a message's data, however deep. */
const clipsIn = (
  value: unknown,
  found: Array<{ readonly clipId: string; readonly metadata: string }> = [],
) => {
  if (Array.isArray(value)) for (const item of value) clipsIn(item, found);
  else if (Predicate.isObject(value)) {
    if (Predicate.isString(value.clip_id) && Predicate.isString(value.metadata))
      found.push({ clipId: value.clip_id, metadata: value.metadata });
    for (const item of Object.values(value)) clipsIn(item, found);
  }
  return found;
};

/** What the provider sent: message types, unknown ones, duplicates, diagnostics, and echoes of one clip. */
const contractTally = () => {
  const messages: Record<string, number> = {};
  const unknown: Record<string, number> = {};
  const diagnostics: Record<string, number> = {};
  const echoes: Record<string, number> = {};
  let duplicates = 0;
  let stale = 0;
  let watched: { readonly clipId: string; readonly marker: string } | undefined;
  return {
    watch: (clipId: string, marker: string) => {
      watched = { clipId, marker };
    },
    add: (event: H3.ProviderEvent) => {
      if (event._tag === "Diagnostic") return bump(diagnostics, event.error.reason._tag);
      if (event._tag !== "Message") return;
      const message = event.message;
      if (message.type === "unknown") bump(unknown, message.name);
      else bump(messages, message.type);
      if (event.disposition === "duplicate") duplicates++;
      if (event.disposition === "stale") stale++;
      const clip = watched;
      if (
        clip !== undefined &&
        clipsIn(message.data).some(
          (listed) => listed.clipId === clip.clipId && listed.metadata.includes(clip.marker),
        )
      )
        bump(echoes, message.type === "unknown" ? message.name : message.type);
    },
    failed: (error: ReactorError) => bump(diagnostics, `observation:${error.reason._tag}`),
    summary: (provider: { readonly contract: H3.Contract | FastH3.Contract }) => ({
      deploymentTitle: provider.contract.deployment.title,
      deploymentVersion: provider.contract.deployment.version,
      documentedVersion: provider.contract.documentedVersion,
      referenceAudio: provider.contract.referenceAudio,
      messages: { ...messages },
      unknown: { ...unknown },
      diagnostics: { ...diagnostics },
      duplicates,
      stale,
    }),
    get echoes() {
      return { ...echoes };
    },
  };
};

/** Reads a track into a log until it ends, fails or is interrupted. */
const readInto = Effect.fnUntraced(function* <F extends { readonly sequence: bigint }, E>(
  frames: Stream.Stream<F, E>,
  log: { readonly add: (element: Recorded<F>, atMs: number) => void },
) {
  const run = yield* Run;
  yield* recorder(frames).pipe(
    Stream.runForEach((element) => Effect.map(run.now, (atMs) => log.add(element, atMs))),
    Effect.ignore,
  );
});

/** Reads up to 48 frames into `log`, for the target's window and never past `deadline`. */
const readFresh = Effect.fnUntraced(function* <E>(
  frames: Stream.Stream<VideoFrame, E>,
  log: Media.VideoLog,
  deadline: number,
) {
  const limit = yield* window(deadline);
  yield* readInto(frames.pipe(Stream.take(48)), log).pipe(Effect.timeout(limit), Effect.ignore);
});

/** One statistics sample a second, as far as the evidence keeps it. */
const sampleStats = (
  session: Pick<Session.Session, "stats" | "snapshot">,
  samples: Array<StatsSample>,
) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const snapshot = yield* session.snapshot;
    const stats = Option.getOrUndefined(yield* session.stats.pipe(Effect.option));
    const atMs = yield* run.now;
    if (samples.length < 600)
      samples.push({
        atMs,
        status: snapshot.status,
        generation: Number(snapshot.generation),
        ...(stats?.pair?.localCandidateType === undefined
          ? {}
          : { local: stats.pair.localCandidateType }),
        ...(stats?.pair?.remoteCandidateType === undefined
          ? {}
          : { remote: stats.pair.remoteCandidateType }),
        ...(stats?.roundTripTimeSeconds === undefined
          ? {}
          : { rttMs: round(stats.roundTripTimeSeconds * 1000) }),
        ...(stats?.rates === undefined
          ? {}
          : { receivedKbps: round(stats.rates.receivedBitsPerSecond / 1000) }),
        ...(stats?.framesPerSecond === undefined ? {} : { fps: round(stats.framesPerSecond) }),
        ...(stats?.jitterSeconds === undefined
          ? {}
          : { jitterMs: round(stats.jitterSeconds * 1000) }),
        ...(stats?.lossRatio === undefined ? {} : { lossRatio: round(stats.lossRatio, 4) }),
      });
  }).pipe(Effect.ignore, Effect.repeat(Schedule.spaced("1 second")));

/** The session's events from allocation, as tags and names only, never provider text or data. */
const recordSessionEvents = Effect.fnUntraced(function* (
  session: Pick<Session.Session, "observe">,
) {
  const run = yield* Run;
  const observation = yield* session.observe({ capacity: 256 });
  yield* observation.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        if (event._tag === "Upload") return;
        let detail: string;
        switch (event._tag) {
          case "Model":
            detail = event.kind === "ack" ? "ack" : `message ${Probes.keptText(event.type)}`;
            detail += ` ${event.correlation}`;
            break;
          case "Status":
            detail = event.status;
            break;
          case "Track":
            detail = Probes.keptText(event.name);
            break;
          case "Decoded":
            detail = `${event.kind} ${Probes.keptText(event.name)}`;
            break;
          case "Control":
            detail = event.message._tag;
            break;
          case "CommandError":
          case "Diagnostic":
            detail = event.error.reason._tag;
            break;
          case "Moderation":
            detail = Probes.keptText(event.action);
            break;
        }
        const atMs = yield* run.now;
        yield* run.update((evidence) => {
          const events = evidence.sessionEvents ?? [];
          if (events.length >= 2000) return evidence;
          return {
            ...evidence,
            sessionEvents: [
              ...events,
              { atMs, generation: Number(event.generation), tag: event._tag, detail },
            ],
          };
        });
      }),
    ),
    Effect.catch((error) => Effect.ignore(run.mark("session events unread", error.reason._tag))),
  );
});

/** A clip with a reference image and reference audio: H3 takes audio only beside an image or a continuation. */
const withReferences = (metadata: string): H3.Request => ({
  prompt: `Picture 1 is a plain gray backdrop. Audio 1 is a low, steady hum under the scene. ${prompt}`,
  seconds: clipSeconds,
  metadata,
  references: [{ _tag: "Bytes", bytes: Media.grayPng({ width: 256, height: 144 }) }],
  audio: [{ _tag: "Bytes", bytes: Media.tone({ seconds: 3, sampleRate: 48_000, frequency: 220 }) }],
});

/**
 * One session end to end through the public API: pricing, allocation,
 * connection, a clip from submission to playback, its media, and termination.
 * `turn` must be carried by a relay pair; `audio` sends a reference image and a
 * reference audio clip, and the clip must report the audio.
 */
const verticalFor = Effect.fnUntraced(function* <
  Req extends Family.RequestInput,
  C extends H3.Clip,
>(family: Family.Family<Req, C>, check: "vertical" | "turn" | "audio") {
  const run = yield* Run;
  const grant = yield* mint(check);
  const marker = `hosted-qualification:${run.runId}`;
  const tally = contractTally();
  const video = Media.videoLog();
  const audio = Media.audioLog();
  const samples: Array<StatsSample> = [];
  yield* withSessions((grants) =>
    Effect.gen(function* () {
      const { session, deadline } = yield* create(grant, grants);
      yield* run.mark("connected");
      const provider = yield* family.provider(session);
      yield* provider.events({ capacity: 4096 }).pipe(
        Stream.runForEach((event) => Effect.sync(() => tally.add(event))),
        Effect.catch((error) => Effect.sync(() => tally.failed(error))),
        Effect.forkScoped,
      );
      yield* sampleStats(session, samples).pipe(Effect.forkScoped);
      const inspection = yield* (yield* withToken(grant.jwt)).inspect(session.id);
      yield* run.update((evidence) => ({
        ...evidence,
        server: {
          cluster: inspection.cluster,
          zone: inspection.zone,
          serverVersion: inspection.serverVersion,
          transport:
            inspection.selectedTransport === null
              ? null
              : `${inspection.selectedTransport.protocol}/${inspection.selectedTransport.version}`,
        },
      }));
      const media = yield* session.decoded;
      const audioOffered = media.tracks.some(
        (track) => track.kind === "audio" && track.direction === "recvonly",
      );
      yield* readInto(media.video(family.tracks.video), video).pipe(Effect.forkScoped);
      if (audioOffered)
        yield* readInto(media.audio(family.tracks.audio), audio).pipe(Effect.forkScoped);
      // H3 holds a generated clip until it is played, unless autoplay is on.
      yield* recorded(provider.setAutoplay(true));
      const request =
        check === "audio"
          ? family.withUploads(marker)
          : family.request({
              prompt: Family.prompt,
              seconds: family.lengths.short,
              metadata: marker,
            });
      const submission = yield* provider.prepare(request);
      const submitMs = yield* run.now;
      const acceptance = yield* recorded(submission.submit);
      tally.watch(acceptance.clip.clip_id, marker);
      const clip = acceptance.clip;
      const lifecycle: { generatedMs?: number; startedMs?: number; endedMs?: number } = {};
      const record = Effect.gen(function* () {
        const summary = video.summary();
        const firstClipFrameMs =
          lifecycle.startedMs === undefined ? undefined : video.firstAfter(lifecycle.startedMs);
        const pressure = yield* media.pressure.pipe(Effect.option);
        const paired = samples.findLast(
          (sample) => sample.local !== undefined && (sample.receivedKbps ?? 0) > 0,
        );
        yield* run.update((evidence) => ({
          ...evidence,
          contract: tally.summary(provider),
          clip: {
            clipId: clip.clip_id,
            acceptance: acceptance.evidence.kind,
            submitMs,
            acceptedMs: evidence.clip?.acceptedMs ?? submitMs,
            ...lifecycle,
            echoes: tally.echoes,
            ...(check === "audio"
              ? {
                  references: {
                    images: family.uploadCounts.images,
                    audio: family.uploadCounts.audio,
                    reportedAudio: clip.reference_audio_count ?? null,
                    hasReferenceAudio: clip.has_reference_audio ?? null,
                  },
                }
              : {}),
          },
          media: {
            audioOffered,
            ...(firstClipFrameMs === undefined ? {} : { firstClipFrameMs }),
            video: summary,
            ...(audioOffered ? { audio: audio.summary() } : {}),
            ...(Option.isSome(pressure)
              ? {
                  pressure: {
                    deliveredVideo: String(pressure.value.deliveredVideo),
                    deliveredAudio: String(pressure.value.deliveredAudio),
                    droppedVideo: String(pressure.value.droppedVideo),
                    droppedAudio: String(pressure.value.droppedAudio),
                    readerOverflows: String(pressure.value.readerOverflows),
                  },
                }
              : {}),
          },
          network: {
            samples: [...samples],
            ...(paired?.local === undefined ? {} : { pair: paired.local }),
          },
        }));
      });
      const acceptedMs = yield* run.now;
      yield* run.update((evidence) => ({
        ...evidence,
        clip: {
          clipId: clip.clip_id,
          acceptance: acceptance.evidence.kind,
          submitMs,
          acceptedMs,
          echoes: {},
        },
      }));
      // The collectors' view goes into the evidence however the check ends.
      yield* Effect.addFinalizer(() => Effect.ignore(record));
      yield* run.mark("accepted", acceptance.evidence.kind);
      const operation = yield* provider.operation(submission);
      const reached = (phase: "generatedMs" | "startedMs" | "endedMs") =>
        Effect.flatMap(run.now, (atMs) =>
          Effect.sync(() => {
            lifecycle[phase] ??= atMs;
          }),
        );
      yield* operation
        .reached("generated")
        .pipe(Effect.andThen(reached("generatedMs")), Effect.ignore, Effect.forkScoped);
      yield* operation.ended.pipe(
        Effect.andThen(reached("endedMs")),
        Effect.ignore,
        Effect.forkScoped,
      );
      yield* operation
        .reached("started")
        .pipe(Effect.andThen(reached("startedMs")), Effect.timeout(yield* until(deadline)));
      yield* run.mark("clip started");
      yield* Effect.sleep(yield* window(deadline));
      yield* record;
      const evidence = yield* run.evidence;
      yield* judge("correlated acceptance", [
        acceptance.evidence.kind === "correlated",
        `acceptance was only by ${acceptance.evidence.kind}`,
      ]);
      yield* judge("lifecycle progression", [
        lifecycle.generatedMs !== undefined && lifecycle.startedMs !== undefined,
        "generated and started were not both observed",
      ]);
      // Started is a provider fact: idle frames can still arrive before the media updates.
      yield* run.judge("live video", video.live(lifecycle.startedMs ?? 0));
      yield* judge("audio when offered", [
        !audioOffered || (audio.count > 0 && audio.summary().peakRms > 0),
        "the session offered audio and no sound arrived",
      ]);
      yield* judge("metadata preserved", [
        Object.keys(evidence.clip?.echoes ?? {}).some((type) => type !== "clip_queued"),
        "no provider message after the reply listed the clip with its metadata",
      ]);
      if (check === "audio") {
        const references = evidence.clip?.references;
        yield* judge("reference audio reported", [
          references?.hasReferenceAudio === true && references.reportedAudio === references.audio,
          `the clip reports has_reference_audio ${String(references?.hasReferenceAudio ?? "absent")} and ${String(references?.reportedAudio ?? "no")} audio reference(s) for ${String(references?.audio ?? 0)} sent`,
        ]);
      }
      const pair = evidence.network?.pair;
      yield* judge(
        check === "turn" ? "relay pair selected" : "ICE pair selected",
        [pair !== undefined, "no statistics sample named a pair that was receiving"],
        [check !== "turn" || pair === "relay", `the pair carrying the media was ${String(pair)}`],
      );
    }),
  );
});

export const vertical = Effect.fnUntraced(function* (check: "vertical" | "turn" | "audio") {
  const run = yield* Run;
  return yield* Family.withFamily(run, (family) => verticalFor(family, check));
});

/**
 * A process owns a session, streams, and is killed mid-clip; this process
 * takes the session over. `takeover` attaches with the raw session API and
 * ends the session through the dead owner's record; `resume` adopts it with
 * `H3Source.resume` and ends it by closing the resumed source.
 */
const takeoverFor = Effect.fnUntraced(function* <
  Req extends Family.RequestInput,
  C extends H3.Clip,
>(family: Family.Family<Req, C>, check: "takeover" | "resume") {
  const run = yield* Run;
  const target = yield* Target;
  const grant = yield* mint(check);
  // The taker holds the key, as a server adopting a dead owner's session does: it mints a
  // token bound to the session, which allocates nothing and stays out of the evidence.
  const bound = yield* binder(tokenSecondsFor(check));
  const marker = `hosted-qualification:${run.runId}`;
  const video = Media.videoLog();
  yield* withSessions(
    (grants) =>
      Effect.gen(function* () {
        const { owner, sessionId, deadline } = yield* owned(grant, marker, grants, family.key);
        const ownerStreamingMs = yield* run.now;
        yield* owner.kill;
        const killedMs = yield* run.now;
        yield* run.update((evidence) => ({
          ...evidence,
          takeover: { ownerStreamingMs, killedMs },
        }));
        yield* run.mark("owner killed", owner.playing);
        if (target.adoptAfterMs !== undefined)
          yield* sleepUntil(killedMs + target.adoptAfterMs, Number.POSITIVE_INFINITY);
        // Everything after this is the taker's own doing.
        const takenMs = yield* run.now;
        const taken = yield* Effect.scoped(
          Effect.gen(function* () {
            const reactor = yield* adopting(sessionId, killedMs);
            if (check === "takeover") {
              const session = yield* reactor.attach({ sessionId, tokens: bound });
              const provider = yield* family.provider(session);
              const attachMs = (yield* run.now) - takenMs;
              yield* run.mark("attached");
              const facts = factsOf(yield* provider.snapshot);
              const queued = [
                ...(facts?.queue.generation ?? []),
                ...(facts?.queue.playout ?? []),
              ].find((clip) => clip.clip_id === owner.queued);
              const media = yield* session.decoded;
              const attachedMs = yield* run.now;
              yield* readFresh(media.video(family.tracks.video), video, deadline);
              return {
                attachMs,
                attachedMs,
                playingClipId: facts?.state.playing_clip_id ?? null,
                metadataPreserved: queued?.metadata.includes(`${marker}:queued`) === true,
              };
            }
            const source = yield* family
              .resume({
                allocation: owner.allocation,
                tokens: bound,
              })
              .pipe(Effect.provideService(Reactor.Reactor, reactor));
            const attachMs = (yield* run.now) - takenMs;
            yield* run.mark("resumed");
            const state = yield* source.events.pipe(
              Stream.filter((event) => event._tag === "State"),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
            const clips = [...state.state.building, ...state.state.ready];
            const attachedMs = yield* run.now;
            yield* readFresh(source.video, video, deadline);
            // The adopted session is owned: closing its source terminates it.
            yield* close({ id: sessionId, close: source.close });
            return {
              attachMs,
              attachedMs,
              playingClipId: state.state.playing?.clipId ?? null,
              metadataPreserved: clips.some(
                (clip) =>
                  clip.clipId === owner.queued &&
                  clip.tag?._tag === "Item" &&
                  clip.tag.key === "queued",
              ),
            };
          }),
        );
        const commands = commandsSince(yield* run.evidence, takenMs);
        const firstFreshFrameMs = video.firstAfter(taken.attachedMs);
        yield* run.update((evidence) => ({
          ...evidence,
          takeover: {
            ownerStreamingMs,
            killedMs,
            attachMs: taken.attachMs,
            playingClipId: taken.playingClipId,
            clipIdentified: taken.playingClipId === owner.playing,
            metadataPreserved: taken.metadataPreserved,
            commands,
            ...(firstFreshFrameMs === undefined ? {} : { firstFreshFrameMs }),
            video: video.summary(),
          },
        }));
        yield* run.mark("observed");
        if (check === "takeover") {
          const inspector = yield* withToken(grant.jwt);
          const requestedMs = yield* run.now;
          const termination = yield* inspector.terminate(sessionId);
          yield* closedWith(sessionId, requestedMs, { termination });
        } else {
          // A resume reads the session; the owner already gave it the defaults a playout needs.
          const writes = Object.keys(commands).filter(
            (name) => !["get_state", "get_queue"].includes(name),
          );
          yield* judge("only reads on resume", [
            writes.length === 0,
            `the resume sent ${writes.join(", ")}`,
          ]);
          const report = (yield* run.evidence).sessions[0]?.close?.report;
          yield* judge(
            "adopted close terminates",
            [report !== undefined, "the resumed source was never closed"],
            [
              report?.ownership === "owned",
              `the resumed session's close reported it ${report?.ownership ?? "without ownership"}`,
            ],
            [
              report?.remote.attempted === true,
              "closing the adopted session attempted no termination",
            ],
          );
        }
        yield* judge("attach within 5 s", [
          taken.attachMs <= 5_000,
          `attaching took ${Math.round(taken.attachMs)} ms`,
        ]);
        yield* judge("clip identified", [
          taken.playingClipId === owner.playing,
          `the attached state named ${taken.playingClipId ?? "no clip"} playing, not the owner's`,
        ]);
        yield* judge("metadata preserved", [
          taken.metadataPreserved,
          "the owner's queued clip lost its metadata, or was not listed",
        ]);
        yield* judge("no enqueue on attach", [
          (commands.enqueue ?? 0) === 0,
          `${commands.enqueue} enqueue(s) after attaching`,
        ]);
        yield* run.judge(
          "fresh frames",
          video.count === 0 ? "no frame arrived after attaching" : video.live(taken.attachedMs),
        );
      }),
    // Whatever failed, a session the dead owner allocated ends through its record.
    withToken(grant.jwt).pipe(endHeld),
  );
});

export const takeover = Effect.fnUntraced(function* (check: "takeover" | "resume") {
  const run = yield* Run;
  return yield* Family.withFamily(run, (family) => takeoverFor(family, check));
});

/** How long `tokens`' creating token lives: enough to connect and stream, not to outlive the session. */
const createSeconds = 20;
/** How long each bound token lives in `tokens`, so one is refreshed while the session runs. */
const boundSeconds = 12;
/** What `tokens` needs after it adopts the session: frames, a refresh, a clip and the probes. */
const afterAdoptMs = 16_000;

/**
 * A session outliving the token that created it, as authentication ›
 * "Keeping the token fresh for a whole session" and "Acting on a session
 * another token created" describe. The owner creates the session on a 20 s
 * token, streams and dies; once that token expired, this process, holding the
 * key, adopts the session with a token bound to it, refreshes that token
 * before the next call, enqueues a clip with a reference image and audio on
 * it, and ends the session with the API key as the bearer. Free probes of the
 * token and key rules go first.
 */
const tokensFor = Effect.fnUntraced(function* <Req extends Family.RequestInput, C extends H3.Clip>(
  family: Family.Family<Req, C>,
) {
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const record = (change: (tokens: Evidence.TokensRecord) => Evidence.TokensRecord) =>
    run.update((evidence) => ({
      ...evidence,
      tokens: change(evidence.tokens ?? { probes: [], mints: [] }),
    }));
  const minted = Effect.fnUntraced(function* (
    kind: "create" | "bind" | "unbound",
    grant: CoordinatorClient.TokenGrant,
    sentAt: number,
  ) {
    const atMs = yield* run.now;
    yield* record((tokens) => ({
      ...tokens,
      mints: [
        ...tokens.mints,
        {
          atMs,
          kind,
          lifetimeSeconds: round(grant.expiresAt - sentAt / 1000),
          echoed: grant.granted !== undefined,
          ...(grant.granted === undefined ? {} : { bound: grant.granted.bound.length }),
        },
      ],
    }));
  });
  // Free: minting allocates nothing, and the key probes name no real session.
  const probes = yield* Probes.run({
    apiUrl: target.apiUrl,
    apiKey: target.apiKey,
    model: run.model.name,
  });
  yield* record((tokens) => ({ ...tokens, probes: [...probes] }));
  yield* run.mark("probed");
  const createSentAt = yield* Clock.currentTimeMillis;
  const grant = yield* mint("tokens", createSeconds);
  yield* minted("create", grant, createSentAt);
  // The adopter's tokens, each bound to the session and short enough to be refreshed in it.
  const bound: Array<{ readonly grant: CoordinatorClient.TokenGrant; readonly sentAt: number }> =
    [];
  const binds = yield* binder(boundSeconds, (token, sentAt) =>
    Effect.sync(() => bound.push({ grant: token, sentAt })).pipe(
      Effect.andThen(minted("bind", token, sentAt)),
    ),
  );
  const marker = `hosted-qualification:${run.runId}`;
  const video = Media.videoLog();
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  yield* withSessions(
    (sessions) =>
      Effect.gen(function* () {
        const { owner, sessionId, deadline } = yield* owned(
          grant,
          marker,
          sessions,
          Family.familyOf(run).key,
        );
        yield* owner.kill;
        const ownerKilledMs = yield* run.now;
        const createExpiresMs = grant.expiresAt * 1000 - run.origin;
        yield* record((tokens) => ({ ...tokens, ownerKilledMs, createExpiresMs }));
        yield* run.mark("owner killed", owner.playing);
        // The session must outlive the token that created it before anyone adopts it.
        const adoptAfterMs = target.adoptAfterMs;
        if (adoptAfterMs === undefined)
          yield* sleepUntil(createExpiresMs + 500, deadline - afterAdoptMs);
        else
          yield* sleepUntil(
            Math.max(createExpiresMs + 500, ownerKilledMs + adoptAfterMs),
            Number.POSITIVE_INFINITY,
          );
        const resumeStartedMs = yield* run.now;
        const reactor = yield* adopting(sessionId, ownerKilledMs);
        const session = yield* reactor.attach({ sessionId, tokens: binds, adopt: true });
        const provider = yield* family.provider(session);
        const attachedMs = yield* run.now;
        yield* run.mark("adopted");
        const playingClipId = factsOf(yield* provider.snapshot)?.state.playing_clip_id ?? null;
        const media = yield* session.decoded;
        yield* readFresh(media.video(family.tracks.video), video, deadline);
        const firstFreshFrameMs = video.firstAfter(attachedMs);
        yield* record((tokens) => ({
          ...tokens,
          resumeStartedMs,
          attachedMs,
          ownerClipIds: { playing: owner.playing, queued: owner.queued },
          playingClipId,
          clipIdentified: playingClipId === owner.playing || playingClipId === owner.queued,
          ...(firstFreshFrameMs === undefined ? {} : { firstFreshFrameMs }),
          video: video.summary(),
        }));
        // The next call after the first bound token's refresh point mints the next one.
        const first = bound[0];
        if (first !== undefined) {
          const expiresAt = first.grant.expiresAt * 1000;
          const margin = Math.min(60_000, (expiresAt - first.sentAt) / 4);
          yield* sleepUntil(expiresAt - margin - run.origin + 250, deadline - 7_000);
        }
        const startedMs = yield* run.now;
        const acceptance = yield* recorded(
          Effect.flatMap(
            provider.prepare(family.withUploads(`${marker}:references`)),
            (submission) => submission.submit,
          ),
        );
        const acceptedMs = yield* run.now;
        const uploads = family.uploadsOf(acceptance.clip);
        const refreshedMs = (yield* run.evidence).tokens?.mints.filter(
          (mint) => mint.kind === "bind",
        )[1]?.atMs;
        yield* record((tokens) => ({
          ...tokens,
          ...(refreshedMs === undefined ? {} : { refreshedMs }),
          upload: {
            startedMs,
            acceptedMs,
            images: family.uploadCounts.images,
            audio: family.uploadCounts.audio,
            reportedAudio: uploads._tag === "References" ? uploads.reportedAudio : null,
            hasReferenceAudio: uploads._tag === "References" ? uploads.hasReferenceAudio : null,
            ...(uploads._tag === "Frame" ? { hasStartingFrame: uploads.hasStartingFrame } : {}),
          },
        }));
        yield* run.mark("enqueued on a refreshed token");
        // The documented refusals: a token past its expiry, and one not bound to the session.
        const readWith = (credential: Redacted.Redacted<string>) =>
          withToken(credential).pipe(
            Effect.flatMap((inspector) => inspector.inspect(sessionId)),
            Effect.as(200),
            Effect.catch((error) => Effect.succeed(statusOf(error) ?? 0)),
          );
        const expiredTokenStatus = yield* readWith(grant.jwt);
        const unboundSentAt = yield* Clock.currentTimeMillis;
        const unbound = yield* coordinator.mintToken({
          apiKey: target.apiKey,
          modelName: run.model.name,
          // It could create one session of a second; it creates none.
          maxSessionDuration: "1 second",
          expiresAfter: "15 seconds",
        });
        yield* run.secret(unbound.jwt);
        yield* minted("unbound", unbound, unboundSentAt);
        const unboundTokenStatus = yield* readWith(unbound.jwt);
        yield* record((tokens) => ({ ...tokens, expiredTokenStatus, unboundTokenStatus }));
        // The key ends any session of its account, with the independent read confirming it.
        const requestedMs = yield* run.now;
        const apiKeyTermination = yield* (yield* keyed).terminate(sessionId);
        yield* record((tokens) => ({ ...tokens, apiKeyTermination }));
        if (apiKeyTermination.confirmed)
          yield* closedWith(sessionId, requestedMs, { termination: apiKeyTermination });
        else yield* close(session);
        const commands = commandsSince(yield* run.evidence, resumeStartedMs);
        yield* record((tokens) => ({ ...tokens, commands }));
        const latest = bound.at(-1);
        if (latest !== undefined) sessions.set(sessionId, latest.grant);
        yield* judge("adopted after the creating token expired", [
          resumeStartedMs > createExpiresMs,
          `the creating token lived ${Math.round((createExpiresMs - resumeStartedMs) / 1000)} s past the adoption`,
        ]);
        // The owner's first clip may have ended while the creating token ran out, so the
        // clip queued behind it counts too.
        yield* judge("clip identified", [
          playingClipId === owner.playing || playingClipId === owner.queued,
          `the adopted state named ${playingClipId ?? "no clip"} playing, neither of the owner's clips`,
        ]);
        yield* run.judge(
          "fresh frames",
          video.count === 0 ? "no frame arrived after adopting" : video.live(attachedMs),
        );
        yield* judge(
          "a refreshed bound token carried the next call",
          [
            bound.length >= 2 && refreshedMs !== undefined,
            `${bound.length} bound token(s) were minted`,
          ],
          [
            refreshedMs !== undefined && refreshedMs >= startedMs && refreshedMs <= acceptedMs,
            "the refresh did not happen for the clip's upload",
          ],
        );
        if (uploads._tag === "Frame")
          yield* judge("starting frame reported", [
            uploads.hasStartingFrame,
            "the clip reports has_starting_frame false for the frame sent",
          ]);
        else
          yield* judge("reference audio reported", [
            acceptance.clip.has_reference_audio === true &&
              acceptance.clip.reference_audio_count === 1,
            `the clip reports has_reference_audio ${String(acceptance.clip.has_reference_audio ?? "absent")} and ${String(acceptance.clip.reference_audio_count ?? "no")} audio reference(s) for 1 sent`,
          ]);
        yield* judge("an expired token is refused", [
          expiredTokenStatus === 401,
          `reading with it answered ${expiredTokenStatus}`,
        ]);
        yield* judge("an unbound token is refused", [
          unboundTokenStatus === 403,
          `reading with it answered ${unboundTokenStatus}`,
        ]);
        yield* judge("the API key ends the session", [
          apiKeyTermination.confirmed,
          `DELETE answered ${String(apiKeyTermination.deleteStatus ?? "nothing")} and the read found ${apiKeyTermination.state ?? "no terminal state"}`,
        ]);
        yield* run.mark("tokens observed");
      }),
    // Whatever failed, the key ends a session the check allocated.
    endHeld(keyed),
  );
});

export const tokens = Effect.flatMap(Run, (run) => Family.withFamily(run, tokensFor));

/** The queue check's edit before each boundary, and how long before the playing clip's end it is sent. */
const boundaries = [
  { edit: "none", aimMs: 0 },
  { edit: "move", aimMs: 2_500 },
  { edit: "move", aimMs: 250 },
  { edit: "pop", aimMs: 1_000 },
  { edit: "pop", aimMs: 250 },
] as const;
/** An edit sent at least this long before the end must decide the next clip; closer ones are recorded. */
const judgedAimMs = 1_000;

interface Observed {
  readonly type: string;
  readonly clipId: string;
  readonly seconds: number;
  readonly metadata: string;
  readonly atMs: number;
}

/**
 * H3's own queue on one session, raw: whether a queue read sent right behind
 * an enqueue lists it, position zero behind a running build, a pop of the
 * build in flight, then with autoplay on a move or a pop before each of five
 * boundaries, at a set distance from the playing clip's end, and which clip
 * starts next.
 */
const queueFor = Effect.fnUntraced(function* <Req extends Family.RequestInput, C extends H3.Clip>(
  family: Family.Family<Req, C>,
) {
  const run = yield* Run;
  const grant = yield* mint("queue");
  const marker = `hosted-qualification:${run.runId}:queue`;
  const observed = yield* SubscriptionRef.make<ReadonlyArray<Observed>>([]);
  yield* withSessions((grants) =>
    Effect.gen(function* () {
      const { session, deadline } = yield* create(grant, grants);
      const provider = yield* family.provider(session);
      yield* provider.events({ capacity: 4096 }).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            if (event._tag !== "Message" || event.disposition !== "applied") return;
            const message = event.message;
            if (message.type === "unknown" || !("clip" in message.data)) return;
            const clip = message.data.clip;
            const atMs = yield* run.now;
            yield* SubscriptionRef.update(observed, (all) =>
              all.length >= 1024
                ? all
                : [
                    ...all,
                    {
                      type: message.type,
                      clipId: clip.clip_id,
                      seconds: clip.seconds,
                      metadata: clip.metadata,
                      atMs,
                    },
                  ],
            );
          }),
        ),
        Effect.forkScoped,
      );
      const video = Media.videoLog();
      const media = yield* session.decoded;
      yield* readInto(media.video(family.tracks.video), video).pipe(Effect.forkScoped);
      const seen = (type: string, clipId: string) =>
        waitFor(
          observed,
          (all) => all.find((event) => event.type === type && event.clipId === clipId),
          deadline,
        );
      const started = (all: ReadonlyArray<Observed>) =>
        all.filter((event) => event.type === "clip_started");
      // `sending` completes as the enqueue commits, just before it is written to the channel.
      const submit = Effect.fnUntraced(function* (
        name: string,
        position?: number,
        sending?: Deferred.Deferred<void>,
      ) {
        const submission = yield* provider.prepare(
          family.request({
            prompt: Family.prompt,
            seconds: family.lengths.short,
            metadata: `${marker}:${name}`,
            ...(position === undefined ? {} : { position }),
          }),
          sending === undefined ? {} : { commit: () => Deferred.succeed(sending, undefined) },
        );
        const submittedMs = yield* run.now;
        const acceptance = yield* recorded(submission.submit);
        return { clipId: acceptance.clip.clip_id, submittedMs };
      });
      const readQueue = Effect.map(recorded(provider.getQueue), (reply) => reply.value);
      /** An edit's outcome: `true` when the provider refused it. Its reason is provider text, never kept. */
      const edit = (command: Effect.Effect<unknown, CommandFailure>) =>
        recorded(command).pipe(
          Effect.as(false),
          Effect.catchIf(
            (error) =>
              error.reason._tag === "Remote" &&
              "outcome" in error.context &&
              error.context.outcome === "replied",
            () => Effect.succeed(true),
          ),
        );

      yield* recorded(provider.setAutoplay(false));
      // H3 documents position zero as next, behind the build that is running; `first` is that
      // build on a session with nothing else building.
      const first = yield* submit("first");
      const zero = yield* submit("position-zero", 0);
      const tail = yield* submit("tail");
      const generation = (yield* readQueue).generation;
      const generationOrder = generation.map((clip) => clip.clip_id);
      const place = (clipId: string) => generationOrder.indexOf(clipId);
      if (family.key === "fast-h3")
        yield* judge("position zero precedes a subsequently enqueued tail", [
          place(zero.clipId) >= 0 &&
            place(tail.clipId) >= 0 &&
            place(zero.clipId) < place(tail.clipId),
          "the position-zero clip and tail were not both present with zero before the tail",
        ]);
      else
        yield* judge(
          "position zero goes next, behind the build that is running",
          [
            place(zero.clipId) >= 0 && place(tail.clipId) >= place(zero.clipId),
            "the position-zero clip was not ahead of the clip queued after it",
          ],
          [
            place(first.clipId) <= place(zero.clipId),
            "the position-zero clip went ahead of the build that was running",
          ],
        );
      // A build popped while it runs must never reach playout; the clip behind it shows
      // how long the popped build keeps the build slot.
      const popped = first;
      const wasBuilding = generationOrder[0] === popped.clipId;
      const wasQueuedUnbuilt =
        family.key === "fast-h3"
          ? generation.some((clip) => clip.clip_id === popped.clipId && !clip.ready)
          : undefined;
      let popAccepted: boolean | undefined;
      if (family.key === "fast-h3")
        popAccepted = yield* provider
          .pop(popped.clipId)
          .pipe(recorded, Effect.match({ onFailure: () => false, onSuccess: () => true }));
      else yield* provider.pop(popped.clipId).pipe(recorded, Effect.ignore);
      const poppedMs = yield* run.now;
      // A queue read sent right behind an enqueue, long before its reply: does it list the clip?
      const ordering: Array<boolean> = [];
      const builds = [zero, tail];
      for (let index = 1; index <= 3; index++) {
        const name = `ordering-${index}`;
        const sending = yield* Deferred.make<void>();
        const enqueue = yield* submit(name, undefined, sending).pipe(
          Effect.result,
          Effect.forkChild,
        );
        yield* Deferred.await(sending);
        yield* Effect.sleep("1 millis");
        const listed = yield* Effect.map(readQueue, (value) =>
          [...value.generation, ...value.playout].some((clip) =>
            clip.metadata.includes(`${marker}:${name}`),
          ),
        );
        ordering.push(listed);
        const accepted = yield* Fiber.join(enqueue);
        if (accepted._tag === "Success") builds.push(accepted.success);
      }
      yield* judge("a queue read sent right behind an enqueue lists the new clip", [
        ordering.every(Boolean),
        `${ordering.filter((listed) => !listed).length} of ${ordering.length} reads did not list it`,
      ]);
      for (let index = 1; index <= 5; index++) builds.push(yield* submit(`clip-${index}`));
      yield* run.mark("clips submitted");
      yield* seen("clip_generated", zero.clipId);
      yield* recorded(provider.setAutoplay(true));
      yield* run.mark("autoplay on");

      type Boundary = { -readonly [K in keyof Evidence.Boundary]: Evidence.Boundary[K] };
      const recordedBoundaries: Array<Boundary> = [];
      const acceptedPops: Array<string> = [];
      const record = Effect.gen(function* () {
        const all = yield* SubscriptionRef.get(observed);
        const watched = new Set([popped.clipId, ...builds.map((build) => build.clipId)]);
        const counts: Record<string, number> = {};
        const mismatched: Record<string, number> = {};
        for (const event of all) {
          if (!watched.has(event.clipId)) continue;
          bump(counts, event.type);
          if (!event.metadata.includes(marker)) bump(mismatched, event.type);
        }
        const afterPop = all.filter(
          (event) => event.clipId === popped.clipId && event.atMs > poppedMs,
        );
        yield* run.update((evidence) => ({
          ...evidence,
          queue: {
            positionZero: {
              buildingClipId: first.clipId,
              requestedClipId: zero.clipId,
              generationOrder,
            },
            poppedBuild: {
              wasBuilding,
              ...(wasQueuedUnbuilt === undefined ? {} : { wasQueuedUnbuilt }),
              ...(popAccepted === undefined ? {} : { popAccepted }),
              generatedAfterPop: afterPop.some((event) => event.type === "clip_generated"),
              startedAfterPop: afterPop.some((event) => event.type === "clip_started"),
            },
            ordering,
            boundaries: recordedBoundaries.map((boundary) => ({ ...boundary })),
            builds: builds.flatMap(({ clipId, submittedMs }) => {
              const generated = all.find(
                (event) => event.type === "clip_generated" && event.clipId === clipId,
              );
              return generated === undefined ? [] : [round(generated.atMs - submittedMs)];
            }),
            metadata: { observed: counts, mismatched },
          },
        }));
      });
      yield* Effect.addFinalizer(() => Effect.ignore(record));
      for (const [index, planned] of boundaries.entries()) {
        const ending = yield* waitFor(observed, (all) => started(all)[index], deadline);
        const endsAtMs = ending.atMs + ending.seconds * 1000;
        const boundary: Boundary = {
          edit: planned.edit,
          aimMs: planned.aimMs,
          endingClipId: ending.clipId,
        };
        recordedBoundaries.push(boundary);
        if (planned.edit !== "none") {
          // The queue is read just ahead, so the edit itself leaves on its aim.
          yield* sleepUntil(endsAtMs - planned.aimMs - 600, deadline);
          const played = new Set(
            started(yield* SubscriptionRef.get(observed)).map((event) => event.clipId),
          );
          const waiting = (yield* readQueue).playout
            .map((clip) => clip.clip_id)
            .filter((id) => !played.has(id));
          // Without two clips waiting the edit cannot show anything, so it is not staged.
          const [firstWaiting, secondWaiting] = waiting;
          if (firstWaiting !== undefined && secondWaiting !== undefined) {
            const edited = planned.edit === "move" ? secondWaiting : firstWaiting;
            yield* sleepUntil(endsAtMs - planned.aimMs, deadline);
            boundary.sentMs = yield* run.now;
            boundary.refused = yield* edit(
              planned.edit === "move" ? provider.move(edited, 0) : provider.pop(edited),
            );
            boundary.editedClipId = edited;
            boundary.expectedClipId = secondWaiting;
            if (planned.edit === "pop" && !boundary.refused) acceptedPops.push(edited);
          }
          yield* run.mark(
            `boundary ${index + 1}: ${planned.edit} ${planned.aimMs} ms before the end`,
            boundary.sentMs === undefined
              ? "not staged"
              : boundary.refused === true
                ? "refused"
                : undefined,
          );
        }
        const finished = yield* waitFor(
          observed,
          (all) =>
            all.find(
              (event) =>
                (event.type === "clip_finished" || event.type === "clip_stopped") &&
                event.clipId === ending.clipId,
            ),
          deadline,
        );
        const next = yield* waitFor(observed, (all) => started(all)[index + 1], deadline);
        boundary.finishedMs = finished.atMs;
        boundary.nextClipId = next.clipId;
        boundary.nextStartedMs = next.atMs;
      }
      // The last seam's frames arrive after its clip starts.
      yield* sleepUntil((recordedBoundaries.at(-1)?.nextStartedMs ?? 0) + seamMs, deadline);
      for (const boundary of recordedBoundaries) {
        if (boundary.finishedMs === undefined || boundary.nextStartedMs === undefined) continue;
        const pause = video.pause(boundary.finishedMs - seamMs, boundary.nextStartedMs + seamMs);
        if (pause !== undefined) boundary.pause = pause;
      }
      yield* record;
      for (const [index, boundary] of recordedBoundaries.entries()) {
        if (boundary.edit === "none" || boundary.aimMs < judgedAimMs) continue;
        yield* judge(
          `${boundary.edit} ${boundary.aimMs} ms before boundary ${index + 1}`,
          [
            boundary.sentMs !== undefined,
            "the edit was not staged: fewer than two clips were waiting",
          ],
          [boundary.refused !== true, "the provider refused the edit"],
          [boundary.nextClipId === boundary.expectedClipId, "a different clip started next"],
        );
      }
      const startedIds = new Set(
        started(yield* SubscriptionRef.get(observed)).map((event) => event.clipId),
      );
      yield* judge("popped clips never start", [
        !acceptedPops.some((clipId) => startedIds.has(clipId)),
        "a clip started after its pop was accepted",
      ]);
      const evidence = yield* run.evidence;
      const poppedBuild = evidence.queue?.poppedBuild;
      if (family.key === "fast-h3")
        yield* judge(
          "a popped unbuilt clip never generates or starts",
          [
            poppedBuild?.wasQueuedUnbuilt === true,
            "the clip was not present and unbuilt in the generation queue before the pop",
          ],
          [poppedBuild?.popAccepted === true, "the provider did not accept the pop"],
          [
            poppedBuild?.generatedAfterPop !== true && poppedBuild?.startedAfterPop !== true,
            "the popped clip generated or started after the pop's reply",
          ],
        );
      else
        yield* judge(
          "pop the build in flight",
          [
            poppedBuild?.wasBuilding === true,
            "the clip was not at the head of the generation queue before the pop",
          ],
          [
            poppedBuild?.generatedAfterPop !== true && poppedBuild?.startedAfterPop !== true,
            "the popped build generated or started after the pop's reply",
          ],
        );
      const metadata = evidence.queue?.metadata;
      yield* judge("observed clip metadata", [
        metadata !== undefined &&
          Object.keys(metadata.observed).length > 0 &&
          Object.keys(metadata.mismatched).length === 0,
        "a watched clip message had missing metadata, or none was observed",
      ]);
      yield* run.mark("queue observed");
    }),
  );
});

export const queue = Effect.flatMap(Run, (run) => Family.withFamily(run, queueFor));

/** An event a playout's session published, as the evidence may keep it: no provider text. */
interface Logged {
  readonly atMs: number;
  readonly sessionId: string;
  readonly event: string;
  readonly verdict?: Extract<Session.EventPayload, { readonly _tag: "Moderation" }>;
}

/** A code or category plain enough to be an identifier rather than provider text. */
const identifier = (value: string) => /^[\w.:/-]{1,64}$/.test(value);

/** A session event as the evidence may keep it; replies and track noise are left out. */
const summarize = (event: Session.SessionEvent): Omit<Logged, "atMs" | "sessionId"> | undefined => {
  if (!("_tag" in event)) return undefined;
  switch (event._tag) {
    case "Status":
      return { event: `status ${event.status}` };
    case "Moderation":
      return {
        event: `moderation ${identifier(event.action) ? event.action : "(text)"}`,
        verdict: event,
      };
    case "Control":
      return { event: `control ${event.message._tag}` };
    case "CommandError":
      return { event: `command error ${event.error.reason._tag}` };
    case "Diagnostic":
      return { event: `diagnostic ${event.error.reason._tag}` };
    default:
      return undefined;
  }
};

/** What a playout check has while its playout runs. */
export interface Air<Req extends Playout.ClipRequest = H3.Request> {
  readonly playout: Playout.Service<Req>;
  readonly items: SubscriptionRef.SubscriptionRef<ReadonlyMap<string, Item>>;
  /** Every as-run event, in order. */
  readonly asRun: () => ReadonlyArray<{
    readonly atMs: number;
    readonly key: string;
    readonly status: Playout.AsRunStatus;
  }>;
  /** What each session published: statuses, control messages, verdicts and diagnostics. */
  readonly sessionLog: () => ReadonlyArray<Logged>;
  /** The token each session was created with. */
  readonly grant: (sessionId: string) => CoordinatorClient.TokenGrant | undefined;
  readonly starts: () => ReadonlyArray<string>;
  /** Submits an item under `key`, recording when. */
  readonly track: (key: string) => Effect.Effect<Playout.ItemKey>;
  /** Submits `request` under `key` to `lane`, tracked. */
  readonly submit: (
    key: string,
    lane: string,
    request: Req,
  ) => Effect.Effect<Playout.ItemHandle, Playout.SubmitError, Run>;
  /** The first value `find` picks from the items as they change, until the deadline. */
  readonly when: <B>(
    find: (items: ReadonlyMap<string, Item>) => B | undefined,
  ) => Effect.Effect<B, Cause.NoSuchElementError | Cause.TimeoutError>;
  readonly video: Media.VideoLog;
  /** The work deadline of the first session. */
  readonly deadline: () => number;
  /** Waits for the first session's allocation: its work deadline. */
  readonly allocated: Effect.Effect<number>;
  /** The seam between two items, measured from the decoded picture. */
  readonly seam: (
    ending: string,
    next: string,
    continued: boolean,
  ) => Effect.Effect<Seam, never, FileSystem.FileSystem | Path.Path>;
}

/** Adds `fields` to the playout's evidence. */
const recordPlayout = (fields: Partial<NonNullable<Evidence.Evidence["playout"]>>) =>
  Effect.flatMap(Run, (run) =>
    run.update((evidence) => ({
      ...evidence,
      playout: { items: [], startOrder: [], seams: [], ...evidence.playout, ...fields },
    })),
  );

/**
 * A playout over H3 sessions this check opens, at most `sessions` of them,
 * with its as-run and picture recorded. Sessions hold the last frame at
 * boundaries, as a show does. `renew` false keeps the one session for the
 * whole check: its grant ends it, so a replacement is never planned.
 */
export interface AirOptions<Req extends Playout.ClipRequest = H3.Request> {
  readonly lanes: ReadonlyArray<Playout.LaneSpec>;
  readonly filler?: Playout.Options<never, Req>["filler"];
  readonly sessions: number;
  readonly renewal?: Playout.Options<never, Req>["renewal"];
  readonly maxModerations?: number;
  readonly maxBuildsInFlight?: number;
}

const onAirFor = Effect.fnUntraced(function* <
  Req extends Family.RequestInput,
  C extends H3.Clip,
  A,
  E,
  R,
>(
  family: Family.Family<Req, C>,
  check: Check,
  options: AirOptions<Req>,
  scenario: (air: Air<Req>) => Effect.Effect<A, E, R>,
) {
  const run = yield* Run;
  const target = yield* Target;
  const video = Media.videoLog();
  const audio = Media.audioLog();
  const items = yield* SubscriptionRef.make<ReadonlyMap<string, Item>>(new Map());
  const starts: Array<string> = [];
  const timeline: Array<{
    readonly atMs: number;
    readonly key: string;
    readonly status: Playout.AsRunStatus;
  }> = [];
  const sessionLog: Array<Logged> = [];
  const windows = new Map<string, number>();
  let deadline = Number.POSITIVE_INFINITY;
  const firstAllocated = yield* Deferred.make<number>();
  let opened = 0;
  return yield* withSessions((grants) =>
    Effect.gen(function* () {
      const open = Effect.gen(function* () {
        if (opened++ >= options.sessions)
          return yield* ReactorError.fromCode(
            "InvalidState",
            `the ${check} check opens ${options.sessions} session(s)`,
          );
        const grant = yield* mint(check).pipe(
          Effect.mapError((error) =>
            error._tag === "Refused" || error._tag === "SaveFailed"
              ? ReactorError.fromCode("InvalidState", error.message)
              : error,
          ),
        );
        const source = yield* family.open({
          tokens: CoordinatorClient.fixedTokens(grant),
          holdLastFrame: true,
          onAllocated: ({ session }) =>
            Effect.gen(function* () {
              const at = yield* allocated(session.id, grant);
              deadline = Math.min(deadline, at);
              yield* Deferred.succeed(firstAllocated, deadline);
              grants.set(session.id, grant);
              yield* session.events({ capacity: 1024 }).pipe(
                Stream.runForEach((event) =>
                  Effect.map(run.now, (atMs) => {
                    const logged = summarize(event);
                    if (logged !== undefined)
                      sessionLog.push({ atMs, sessionId: session.id, ...logged });
                  }),
                ),
                Effect.ignore,
                Effect.forkScoped,
              );
            }),
        });
        const wrapped: Playout.Source<Req> = {
          ...source,
          ...(options.renewal === undefined ? { lifetime: Duration.infinity } : {}),
          // A failed playout closes its sessions on a fiber its scope may interrupt: the
          // close and its record finish together.
          close: Effect.uninterruptible(
            Effect.gen(function* () {
              const requestedMs = yield* run.now;
              const report = yield* source.close;
              yield* closedWith(source.sessionId, requestedMs, { report }).pipe(
                Effect.provideService(Run, run),
                Effect.ignore,
              );
              return report;
            }),
          ),
        };
        return wrapped;
      });
      const playout = yield* Playout.make({
        model: family.clipModel,
        open,
        lanes: options.lanes,
        ...(options.filler === undefined ? {} : { filler: options.filler }),
        ...(options.renewal === undefined ? {} : { renewal: options.renewal }),
        ...(options.maxModerations === undefined ? {} : { maxModerations: options.maxModerations }),
        ...(options.maxBuildsInFlight === undefined
          ? {}
          : { maxBuildsInFlight: options.maxBuildsInFlight }),
      });
      yield* playout.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            const atMs = round(event.at - run.origin);
            const status = event.status;
            timeline.push({ atMs, key: event.key, status });
            yield* SubscriptionRef.update(items, (all) => {
              const item = all.get(event.key);
              if (item === undefined) return all;
              const next = { ...item, last: status._tag };
              switch (status._tag) {
                case "Ready":
                  return new Map(all).set(event.key, {
                    ...next,
                    readyMs: item.readyMs ?? atMs,
                    sessionId: status.sessionId,
                  });
                case "Started":
                  starts.push(event.key);
                  if (target.seams !== undefined)
                    windows.set(
                      event.key,
                      video.watch(
                        atMs + status.seconds * 1000 - seamMs,
                        atMs + status.seconds * 1000 + 2 * seamMs,
                      ),
                    );
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
                    airedSeconds: round(status.airedSeconds, 3),
                    termination: status.termination,
                  });
                case "Dropped":
                  return new Map(all).set(event.key, { ...next, dropped: status.reason });
                case "Failed":
                  return new Map(all).set(event.key, { ...next, failed: failedOf(status.reason) });
                default:
                  return new Map(all).set(event.key, next);
              }
            });
          }),
        ),
        Effect.forkScoped,
      );
      yield* readInto(playout.video, video).pipe(Effect.forkScoped);
      yield* readInto(playout.audio, audio).pipe(Effect.forkScoped);
      yield* run.mark("opened");
      const record = Effect.flatMap(SubscriptionRef.get(items), (all) =>
        recordPlayout({
          items: [...all.values()],
          startOrder: [...starts],
          video: video.summary(),
          audio: audio.summary(),
        }),
      );
      yield* Effect.addFinalizer(() => Effect.ignore(record));
      const track = Effect.fnUntraced(function* (key: string) {
        const submittedMs = yield* run.now;
        yield* SubscriptionRef.update(items, (all) =>
          new Map(all).set(key, { key, submittedMs, last: "Accepted" }),
        );
        return Playout.ItemKey.make(key);
      });
      const air: Air<Req> = {
        playout,
        items,
        asRun: () => [...timeline],
        sessionLog: () => [...sessionLog],
        grant: (sessionId) => grants.get(sessionId),
        starts: () => [...starts],
        track,
        submit: (key, lane, request) =>
          Effect.flatMap(track(key), (tracked) =>
            recorded(playout.submit({ key: tracked, lane, request })),
          ),
        when: (find) => waitFor(items, find, deadline),
        video,
        deadline: () => deadline,
        allocated: Deferred.await(firstAllocated),
        seam: Effect.fnUntraced(function* (ending, next, continued) {
          const all = yield* SubscriptionRef.get(items);
          const endingItem = all.get(ending);
          const nextItem = all.get(next);
          const window = windows.get(ending);
          const endedMs = endingItem?.endedMs ?? nextItem?.startedMs;
          const startedMs = nextItem?.startedMs;
          if (endedMs === undefined || startedMs === undefined) {
            if (window !== undefined) video.release(window);
            return { ending, next, continued };
          }
          const fromMs = endedMs - seamMs;
          const toMs = startedMs + 2 * seamMs;
          const pause = video.pause(fromMs, toMs);
          const jump = video.jump(fromMs, toMs);
          const images = window === undefined ? undefined : video.release(window, jump?.atMs);
          const frames =
            images === undefined || target.seams === undefined
              ? undefined
              : yield* Media.writeSeam({
                  directory: target.seams,
                  label: `${run.runId}-${ending}-${next}`,
                  images,
                }).pipe(Effect.option, Effect.map(Option.getOrUndefined));
          return {
            ending,
            next,
            continued,
            endedMs,
            startedMs,
            ...(pause === undefined ? {} : { pause }),
            darkFrames: video.dark(fromMs, toMs),
            ...(jump === undefined ? {} : { jump }),
            ...(frames === undefined ? {} : { frames: [...frames] }),
          } satisfies Seam;
        }),
      };
      const result = yield* scenario(air);
      yield* record;
      const state = yield* playout.state;
      yield* recordPlayout({
        estimates: {
          ...(state.estimates.build === undefined
            ? {}
            : {
                buildMedian: round(state.estimates.build.median, 3),
                buildP95: round(state.estimates.build.p95, 3),
              }),
          length: round(state.estimates.length, 4),
        },
      });
      return result;
    }),
  );
});

/** The fixed H3 checks use the same playout workflow with H3's request type. */
const onAir = <A, E, R>(
  check: Check,
  options: AirOptions,
  scenario: (air: Air) => Effect.Effect<A, E, R>,
) => onAirFor(Family.h3, check, options, scenario);

const itemRequest = (seconds = clipSeconds): H3.Request => ({ prompt, seconds });

/** The planned order of `edits`: the line, the two inserts, and the batch's insert. */
const plannedEdits = ["p1", "p2", "xc", "xn", "p3", "y"] as const;

/**
 * The playout's edits on one session: a group of three beats and a fourth
 * item, a clip inserted after the second beat continuing from it, one inserted
 * before the third built on its own, and a batch that withdraws the fourth and
 * inserts a clip after the third, sent to take effect about a second before
 * the third ends. Every seam's pause and join are measured.
 */
export const edits = onAir("edits", { lanes: [{ name: "line" }], sessions: 1 }, (air) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const startedOf = (key: string) => air.when((all) => all.get(key)?.startedMs);
    yield* recorded(
      air.playout.submitGroup({
        key: Playout.ItemKey.make("line"),
        lane: "line",
        parts: [
          { key: yield* air.track("p1"), request: itemRequest() },
          { key: yield* air.track("p2"), request: itemRequest() },
          { key: yield* air.track("p3"), request: itemRequest() },
        ],
      }),
    );
    yield* air.submit("w1", "line", itemRequest());
    yield* run.mark("line submitted");
    yield* startedOf("p1");
    // A continued build takes about 2.5 times an independent one on hosted H3, so xc
    // continues from p2, which gives it p2's whole length to build in.
    yield* recorded(
      air.playout.insert({
        key: yield* air.track("xc"),
        request: itemRequest(),
        after: Playout.ItemKey.make("p2"),
        continuity: "previous",
      }),
    );
    yield* recorded(
      air.playout.insert({
        key: yield* air.track("xn"),
        request: itemRequest(),
        before: Playout.ItemKey.make("p3"),
      }),
    );
    yield* run.mark("inserted");
    // The batch goes one measured build plus a second before p3 ends.
    const p3Started = yield* startedOf("p3");
    const p3 = yield* air.when((all) => all.get("p3"));
    const perSecond = (yield* air.playout.state).estimates.build?.median ?? 0.42;
    const endsMs = p3Started + (p3.seconds ?? clipSeconds) * 1000;
    yield* sleepUntil(endsMs - 1000 - perSecond * clipSeconds * 1000, air.deadline());
    const submittedMs = yield* run.now;
    const batch = yield* recorded(
      air.playout.edit([
        { _tag: "Withdraw", key: Playout.ItemKey.make("w1") },
        {
          _tag: "Insert",
          insert: {
            key: yield* air.track("y"),
            request: itemRequest(),
            after: Playout.ItemKey.make("p3"),
          },
        },
      ]),
    );
    const committed = yield* batch.committed.pipe(
      Effect.andThen(run.now),
      Effect.option,
      Effect.forkScoped,
    );
    yield* run.mark("batch submitted");
    yield* sleepUntil((yield* startedOf("y")) + 2 * seamMs + 250, air.deadline());
    const committedMs = Option.getOrUndefined(yield* Fiber.join(committed));
    const all = yield* SubscriptionRef.get(air.items);
    const order = air.starts();
    const seams: Array<Seam> = [];
    for (let index = 0; index + 1 < order.length; index++) {
      const ending = order[index];
      const next = order[index + 1];
      if (ending !== undefined && next !== undefined)
        seams.push(yield* air.seam(ending, next, next === "xc"));
    }
    yield* recordPlayout({ seams });
    const p3End = all.get("p3")?.endedMs;
    yield* recordPlayout({
      batch: {
        submittedMs,
        ...(committedMs === undefined ? {} : { committedMs }),
        ...(p3End === undefined ? {} : { boundaryMs: p3End }),
      },
    });
    yield* judge("inserts and the batch air in their planned places", [
      order.join(",") === plannedEdits.join(","),
      `started in the order ${order.join(", ")}`,
    ]);
    yield* judge(
      "a batch takes effect before its boundary",
      [committedMs !== undefined, "the batch never took effect"],
      [
        p3End === undefined || committedMs === undefined || committedMs < p3End,
        "the batch took effect after the playing clip ended",
      ],
    );
    const w1 = all.get("w1");
    yield* judge(
      "a batch's withdrawn clip never starts",
      [w1?.startedMs === undefined, "the withdrawn clip started"],
      [w1?.dropped === "withdrawn", `the withdrawn clip ended ${w1?.last ?? "untracked"}`],
    );
    yield* judge("every seam measured", [
      seams.length >= plannedEdits.length - 1 &&
        seams.every((seam) => seam.pause !== undefined && seam.jump !== undefined),
      "a boundary has no pause or join measurement",
    ]);
    yield* run.mark("edits observed");
  }),
);

/** How long a held, flagged item is watched for a verdict or its session's end. */
const moderationWaitMs = 12_000;

/** A failed item's record as a line of evidence: whether moderation or a loss was why, and why. */
const failureDetail = (failed: NonNullable<Item["failed"]>): string =>
  `${failed.moderated ? "moderated" : failed.lost ? "lost" : "failed"}: ${failed.reason}`;

/** A playout session event, as the evidence keeps it. */
const sessionEventText = (event: Playout.SessionEvent): string => {
  switch (event._tag) {
    case "Opened":
      return `opened ${event.sessionId}`;
    case "SetupFailed":
      return `setup failed (${event.consecutive} in a row)`;
    case "Switched":
      return `switched ${event.from} to ${event.to}`;
    case "Replaced":
      return `replaced ${event.from}, ${event.carried} carried`;
    case "Moderated":
      return `moderated ${event.sessionId}${event.key === undefined ? "" : ` blaming ${event.key}`}`;
    case "Reconnecting":
      return `reconnecting ${event.sessionId}`;
    case "Reconnected":
      return `reconnected ${event.sessionId} after ${Math.round(event.afterMillis)} ms`;
  }
};

/**
 * The end of `cut` when the operator gives a prompt meant to be flagged: a
 * 15 s guard clip goes to the line, and the flagged item behind it, so the
 * flagged clip could air only after the watch ends; if it comes back Ready
 * unflagged it is withdrawn. The check records what hosted Reactor, the
 * session and the playout do. The playout may open no second session, and
 * fails after one moderation.
 */
const moderate = (air: Air, flagged: Redacted.Redacted<string>) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const target = yield* Target;
    yield* run.secret(flagged);
    const playoutLog: Array<{ readonly atMs: number; readonly event: string }> = [];
    yield* air.playout.events.pipe(
      Stream.runForEach((event) =>
        Effect.map(run.now, (atMs) => {
          if (event._tag === "Session")
            playoutLog.push({ atMs, event: sessionEventText(event.event) });
        }),
      ),
      Effect.forkScoped,
    );
    yield* air.playout.failure.pipe(
      Effect.flatMap((failure) =>
        Effect.map(run.now, (atMs) => {
          playoutLog.push({
            atMs,
            event: `failed ${failure._tag === "InvalidFiller" ? failure._tag : failure.reason._tag}`,
          });
        }),
      ),
      Effect.forkScoped,
    );
    yield* air.submit("guard", "line", itemRequest(15));
    const submittedMs = yield* run.now;
    yield* air.submit("flagged", "line", { prompt: Redacted.value(flagged), seconds: clipSeconds });
    yield* run.mark("flagged item queued behind the guard");
    // The session ended under the item: the playout lost it, or a verdict said so.
    const ended = () =>
      playoutLog.some(
        (logged) => logged.event.startsWith("replaced") || logged.event.startsWith("moderated"),
      );
    const settled = Effect.gen(function* () {
      const last = (yield* SubscriptionRef.get(air.items)).get("flagged")?.last;
      return (
        last === "Failed" ||
        last === "Unknown" ||
        last === "Ready" ||
        ended() ||
        air
          .sessionLog()
          .some((logged) => logged.atMs >= submittedMs && logged.verdict !== undefined)
      );
    });
    yield* watch(
      settled,
      Math.min(air.deadline(), (yield* Clock.currentTimeMillis) + moderationWaitMs),
    );
    // Built and unflagged: it goes before it can air.
    if ((yield* SubscriptionRef.get(air.items)).get("flagged")?.last === "Ready")
      yield* air.playout
        .edit([{ _tag: "Withdraw", key: Playout.ItemKey.make("flagged") }])
        .pipe(Effect.ignore);
    // A verdict can follow a built clip, and a close follows a verdict: watch a little longer.
    yield* Effect.sleep(Duration.min(Duration.seconds(3), yield* until(air.deadline())));
    const all = yield* SubscriptionRef.get(air.items);
    const flaggedItem = all.get("flagged");
    const sessionId = flaggedItem?.sessionId ?? all.get("long")?.sessionId;
    // A session that ended under the item is closed once the playout gives up on it.
    const closed = Effect.map(
      run.evidence,
      (evidence) =>
        evidence.sessions.find((session) => session.id === sessionId)?.close !== undefined,
    );
    if (ended()) yield* watch(closed, air.deadline());
    const [enqueueSpan] = commandSpans(yield* run.evidence, submittedMs, "enqueue");
    const enqueueRequest = enqueueSpan?.attributes["reactor.request.id"];
    const verdictLog = air
      .sessionLog()
      .find((logged) => logged.atMs >= submittedMs && logged.verdict !== undefined);
    const verdict = verdictLog?.verdict;
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
    const moderation: Evidence.ModerationRecord = {
      promptLength: Redacted.value(flagged).length,
      submittedMs,
      statuses: air
        .asRun()
        .filter((entry) => entry.key === "flagged")
        .map(({ atMs, status }) => ({
          atMs,
          status: status._tag,
          ...(status._tag === "Failed"
            ? { detail: failureDetail(failedOf(status.reason)) }
            : status._tag === "Unknown" && status.terminal === true
              ? { detail: "terminal" }
              : {}),
        })),
      ...(enqueueSpan === undefined
        ? {}
        : {
            enqueue: {
              startMs: enqueueSpan.startMs,
              ...(enqueueSpan.durationMs === undefined
                ? {}
                : { durationMs: enqueueSpan.durationMs }),
              status: enqueueSpan.status,
              ...(typeof enqueueRequest === "string" ? { requestId: enqueueRequest } : {}),
            },
          }),
      ...(verdictLog === undefined || verdict === undefined
        ? {}
        : {
            verdict: {
              atMs: verdictLog.atMs,
              action: identifier(verdict.action) ? verdict.action : "(text)",
              categories: verdict.categories.filter(identifier),
              ...(verdict.inputKind === undefined || !identifier(verdict.inputKind)
                ? {}
                : { inputKind: verdict.inputKind }),
              ...(verdict.command === undefined || !identifier(verdict.command)
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
      playout: [...playoutLog],
      ...(read === undefined ? {} : { read }),
      flagged: verdict !== undefined || ended(),
      aired: flaggedItem?.startedMs !== undefined,
    };
    yield* run.update((evidence) => ({ ...evidence, moderation }));
    yield* run.mark(
      "moderation observed",
      moderation.flagged ? "flagged" : moderation.aired ? "aired" : "not flagged",
    );
  }).pipe(
    // The cut is judged already; what moderation did is an observation, never its verdict.
    Effect.catch((error) =>
      Effect.gen(function* () {
        const run = yield* Run;
        yield* run.mark("moderation not observed", error.message);
      }),
    ),
  );

/**
 * A cut lane on one session: a 15 s clip plays in the line lane, and 2.5 s
 * after it starts a 5 s clip goes to a lane with `cut: true`, which must stop
 * the long one once it is Ready, with one `stop`, and start next. With a
 * moderation prompt, a held item carrying it follows.
 */
export const cut = Effect.gen(function* () {
  const flagged = (yield* Target).moderationPrompt;
  return yield* onAir(
    "cut",
    {
      lanes: [{ name: "urgent", cut: true }, { name: "line" }],
      sessions: 1,
      // The flagged enqueue goes out while the guard builds, so screening starts at once.
      ...(flagged === undefined ? {} : { maxModerations: 1, maxBuildsInFlight: 2 }),
    },
    (air) =>
      Effect.gen(function* () {
        const run = yield* Run;
        yield* air.submit("long", "line", itemRequest(15));
        const longStarted = yield* air.when((all) => all.get("long")?.startedMs);
        yield* sleepUntil(longStarted + 2_500, air.deadline());
        const cutFromMs = yield* run.now;
        yield* air.submit("cutter", "urgent", itemRequest());
        yield* run.mark("cutter submitted");
        const cutterStarted = yield* air.when((all) => all.get("cutter")?.startedMs);
        yield* sleepUntil(cutterStarted + 2 * seamMs + 250, air.deadline());
        const all = yield* SubscriptionRef.get(air.items);
        yield* recordPlayout({ seams: [yield* air.seam("long", "cutter", false)] });
        // H3's stop names no clip: a second one stops whatever plays by then.
        const stops = commandSpans(yield* run.evidence, cutFromMs, "stop").length;
        yield* recordPlayout({ stops });
        const long = all.get("long");
        const cutter = all.get("cutter");
        const order = air.starts();
        yield* judge(
          "a cut lane's clip stops a lower lane's playing clip",
          [
            long?.termination === "stopped",
            `the long clip ended ${long?.termination ?? long?.last ?? "untracked"}`,
          ],
          [order[order.indexOf("long") + 1] === "cutter", "the cut-lane clip did not start next"],
          [
            cutter?.termination !== "stopped",
            `the cut-lane clip was itself stopped after ${String(cutter?.airedSeconds ?? "?")} s`,
          ],
          [stops === 1, `${stops} stops were sent for one cut`],
        );
        yield* run.mark("cut observed");
        if (flagged !== undefined) yield* moderate(air, flagged);
      }),
  );
});

/**
 * Renewal across two capped sessions: the playout opens the replacement 40 s
 * before the first session's grant ends (10 s in), and switches at a boundary
 * once the replacement's clip is Ready and the first session's last clip
 * ended. A plays on the first session, B on the second; then an accepted
 * drain, and both sessions end.
 */
export const renewal = onAir(
  "renewal",
  { lanes: [{ name: "line" }], sessions: 2, renewal: { lead: "40 seconds", grace: "250 millis" } },
  (air) =>
    Effect.gen(function* () {
      const run = yield* Run;
      const sessionEvents = yield* SubscriptionRef.make<
        ReadonlyArray<{ readonly atMs: number; readonly event: Playout.SessionEvent }>
      >([]);
      yield* air.playout.events.pipe(
        Stream.runForEach((event) =>
          event._tag === "Session"
            ? Effect.flatMap(run.now, (atMs) =>
                SubscriptionRef.update(sessionEvents, (all) => [
                  ...all,
                  { atMs, event: event.event },
                ]),
              )
            : Effect.void,
        ),
        Effect.forkScoped,
      );
      yield* air.submit("A", "line", itemRequest());
      yield* run.mark("A submitted");
      yield* air.when((all) => all.get("A")?.endedMs);
      const first = (yield* SubscriptionRef.get(air.items)).get("A")?.sessionId;
      yield* waitFor(
        sessionEvents,
        (all) => all.find(({ event }) => event._tag === "Opened" && event.sessionId !== first),
        air.deadline(),
      );
      yield* run.mark("replacement opened");
      yield* air.submit("B", "line", itemRequest());
      yield* run.mark("B submitted");
      yield* air.playout
        .drain({ finish: "accepted" })
        .pipe(Effect.timeout(yield* until(air.deadline())));
      const drainedMs = yield* run.now;
      yield* run.mark("drained");
      const all = yield* SubscriptionRef.get(air.items);
      const events = yield* SubscriptionRef.get(sessionEvents);
      const switches = events.flatMap(({ atMs, event }) =>
        event._tag === "Switched"
          ? [{ atMs, from: event.from, to: event.to, decision: event.decision }]
          : [],
      );
      // The playout's one picture carries each session's clip in turn.
      const framesWhile = (key: string) => {
        const aired = all.get(key);
        return aired?.startedMs === undefined || aired.endedMs === undefined
          ? 0
          : air.video.framesBetween(aired.startedMs, aired.endedMs);
      };
      const framesByItem = { A: framesWhile("A"), B: framesWhile("B") };
      yield* recordPlayout({ switches, framesByItem, drainedMs });
      const a = all.get("A");
      const b = all.get("B");
      yield* judge("A then B finish", [
        a?.termination === "finished" &&
          b?.termination === "finished" &&
          (a.startedMs ?? 0) < (b.startedMs ?? 0),
        `A ended ${a?.termination ?? a?.last ?? "untracked"}, B ${b?.termination ?? b?.last ?? "untracked"}`,
      ]);
      yield* judge("B airs on the replacement", [
        a?.sessionId !== undefined && b?.sessionId !== undefined && a.sessionId !== b.sessionId,
        "A and B aired on the same session",
      ]);
      yield* judge(
        "one planned switch",
        [switches.length === 1, `${switches.length} switches`],
        [
          !events.some(({ event }) => event._tag === "Replaced"),
          "a session was replaced before a planned switch",
        ],
      );
      yield* judge("video from both sessions", [
        framesByItem.A > 0 && framesByItem.B > 0,
        `the playout's picture carried ${framesByItem.A} frames of A and ${framesByItem.B} of B`,
      ]);
      yield* run.mark("renewal observed");
    }),
);

/**
 * The pieces of these checks that a check in a module of its own reuses. This
 * module imports that one for `all`, and an import cycle is refused, so they
 * are handed to it.
 */
const pieces = {
  adopting,
  allocated,
  binder,
  capMs,
  close,
  closedWith,
  commandSpans,
  commandsSince,
  contractTally,
  endHeld,
  factsOf,
  failedRead,
  holding,
  identifier,
  judge,
  mint,
  onAir,
  onAirFor,
  readFresh,
  readInto,
  recordPlayout,
  recordSessionEvents,
  round,
  sampleStats,
  seamMs,
  sessionEventText,
  sleepUntil,
  statusOf,
  until,
  waitFor,
  watch,
  window,
  withReferences,
  withSessions,
  withToken,
};
export type Pieces = typeof pieces;

const all = {
  vertical: vertical("vertical"),
  turn: vertical("turn"),
  audio: vertical("audio"),
  takeover: takeover("takeover"),
  resume: takeover("resume"),
  queue,
  renewal,
  edits,
  cut,
  tokens,
  tour: tour(pieces),
  adoption: adoption(pieces),
  show: show(pieces),
  unconnected: unconnected(pieces),
  dropped: dropped(pieces),
  showreel: showreel(pieces),
  avatar: avatar(pieces),
  character: character(pieces),
  fasth3: fastH3(pieces),
  rejoin: rejoin(pieces),
};
/** What a check can fail with, and what it needs. */
export type CheckError = Effect.Error<(typeof all)[Check]>;
export type CheckServices = Effect.Services<(typeof all)[Check]>;

/** Every check by name. */
export const checks: Record<Check, Effect.Effect<void, CheckError, CheckServices>> = all;
