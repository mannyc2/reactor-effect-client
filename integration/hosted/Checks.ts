/**
 * The checks, each one Effect over the public API: it spends only through the
 * sessions it opens, records what it sees in the run's evidence as it goes,
 * and judges its criteria. `Target` decides whether that is hosted Reactor or
 * `ReactorTest`; nothing here knows which.
 */
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
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as H3 from "reactor-effect-client/H3";
import * as H3Source from "reactor-effect-client/H3Source";
import { recorder } from "reactor-effect-client/Media";
import type { Recorded } from "reactor-effect-client/Media";
import * as Playout from "reactor-effect-client/Playout";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { CommandFailure } from "reactor-effect-client/ReactorError";
import type * as Session from "reactor-effect-client/Session";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import type * as Evidence from "./Evidence.js";
import type { Item, Seam, StatsSample } from "./Evidence.js";
import * as Media from "./Media.js";
import * as Probes from "./Probes.js";
import { recorded, Run } from "./Run.js";
import type { Check } from "./Spend.js";
import {
  acceptGrant,
  billedUsd,
  provenGrant,
  sessionSeconds,
  tokenSeconds,
  workSeconds,
} from "./Spend.js";
import { prompt, Target } from "./Target.js";

const round = (value: number, places = 1) => Math.round(value * 10 ** places) / 10 ** places;
const tracks = H3.h3ReferenceTurboRealtime.tracks;
/** Every playout and queue clip asks for this long: a short build, a boundary every few seconds. */
const clipSeconds = 5;
/** Around a boundary, frames are looked for from this long before the end to twice it after the start. */
const seamMs = 1_500;

/** The time left until `deadline`, a Clock time in milliseconds, never negative. */
const until = (deadline: number) =>
  Effect.map(Clock.currentTimeMillis, (now) => Duration.millis(Math.max(0, deadline - now)));

/** Sleeps until `atMs` after the run's start, never past `deadline`. */
const sleepUntil = (atMs: number, deadline: number) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const left = Duration.toMillis(yield* until(deadline));
    yield* Effect.sleep(Duration.millis(Math.min(left, Math.max(0, atMs - (yield* run.now)))));
  });

/**
 * The adopting process's Reactor. Every read it makes of the session's
 * descriptor goes into the evidence: the time since the owner was killed, the
 * status, the state, and key names and codes only (`Probes.summarize`). Paid
 * run tokens 83d17eb7 read INACTIVE there, which the SDK then counted as ended.
 */
const adopting = (sessionId: string, killedMs: number) =>
  Effect.gen(function* () {
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
    const coordinator = yield* Coordinator.make({
      apiUrl: target.apiUrl,
      apiKey: target.apiKey,
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));
    return yield* Reactor.make().pipe(Effect.provideService(Coordinator.Coordinator, coordinator));
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

/** Mints one session's token: its grant goes into the evidence, its JWT never does. */
const mint = (check: Check, expiresAfterSeconds = tokenSeconds) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const target = yield* Target;
    const coordinator = yield* Coordinator.Coordinator;
    const grant = yield* coordinator.mintToken({
      apiKey: target.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: `${sessionSeconds} seconds`,
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

/** Records a session the check allocated, and when its grant's cap ends it. Returns its work deadline. */
const allocated = (sessionId: string, grant: Coordinator.TokenGrant) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const now = yield* Clock.currentTimeMillis;
    // The cap asked for, which the grant was proven not to exceed.
    const capEndsAt = DateTime.formatIso(
      DateTime.makeUnsafe(now + (grant.maxSessionSeconds ?? sessionSeconds) * 1000),
    );
    yield* run.update((evidence) => ({
      ...evidence,
      sessions: [
        ...evidence.sessions,
        { id: sessionId, allocatedMs: now - run.origin, capEndsAt, trail: [] },
      ],
    }));
    yield* run.mark("allocated", sessionId);
    return now + workSeconds * 1000;
  });

/** Records how a session's close went. */
const closedWith = (
  sessionId: string,
  requestedMs: number,
  close:
    | { readonly report: Session.CloseReport }
    | { readonly termination: Coordinator.Termination },
) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const reportedMs = yield* run.now;
    const confirmed =
      "report" in close ? close.report.remote.confirmed : close.termination.confirmed;
    yield* run.update((evidence) => ({
      ...evidence,
      sessions: evidence.sessions.map((session) =>
        session.id === sessionId
          ? { ...session, close: { requestedMs, reportedMs, confirmed, ...close } }
          : session,
      ),
    }));
    yield* run.mark("closed", `${sessionId} ${confirmed ? "confirmed" : "unconfirmed"}`);
  });

/** Closes a session the check allocated, recording the library's report. */
const close = (session: Pick<Session.Session, "id" | "close">) =>
  Effect.gen(function* () {
    const requestedMs = yield* (yield* Run).now;
    const report = yield* session.close;
    yield* closedWith(session.id, requestedMs, { report });
  });

/**
 * After the sessions: follow the coordinator's view of each until it is
 * terminal or gone, bounded by its token, then price the run and judge that
 * every session's end was confirmed.
 */
const settle = (tokens: ReadonlyMap<string, Coordinator.TokenGrant>) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const target = yield* Target;
    for (const [sessionId, grant] of tokens) {
      const inspector = yield* Coordinator.make({
        apiUrl: target.apiUrl,
        credential: Effect.succeed(grant.jwt),
      });
      const deadline = Math.min(
        grant.expiresAt * 1000 - 5_000,
        (yield* Clock.currentTimeMillis) + 20_000,
      );
      const trail: Array<{ readonly atMs: number; readonly state: string }> = [];
      let terminalMs: number | undefined;
      while (terminalMs === undefined && (yield* Clock.currentTimeMillis) < deadline) {
        const state = yield* inspector.inspect(sessionId).pipe(
          Effect.map((inspection) => inspection.state),
          // Only 404 means gone; any other refusal names its status, so the trail shows it.
          Effect.catch((error) =>
            Effect.succeed(
              error.reason._tag === "Http" && error.reason.status !== undefined
                ? error.reason.status === 404
                  ? "gone"
                  : `http:${error.reason.status}`
                : `error:${error.reason._tag}`,
            ),
          ),
        );
        const atMs = yield* run.now;
        if (trail.at(-1)?.state !== state) trail.push({ atMs, state });
        if (state === "gone" || Coordinator.isTerminal(state)) terminalMs = atMs;
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
    yield* run.judge(
      "confirmed termination",
      evidence.sessions.length === 0
        ? "no session was allocated"
        : unconfirmed.length === 0
          ? undefined
          : `the end of ${unconfirmed.map((session) => session.id).join(", ")} was not confirmed`,
    );
    yield* run.mark("settled");
  }).pipe(Effect.ignore);

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

const bump = (counts: Record<string, number>, key: string) => {
  counts[key] = (counts[key] ?? 0) + 1;
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
    summary: (provider: H3.Provider) => ({
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
const readInto = <F extends { readonly sequence: bigint }, E>(
  frames: Stream.Stream<F, E>,
  log: { readonly add: (element: Recorded<F>, atMs: number) => void },
) =>
  Effect.gen(function* () {
    const run = yield* Run;
    yield* recorder(frames).pipe(
      Stream.runForEach((element) => Effect.map(run.now, (atMs) => log.add(element, atMs))),
      Effect.ignore,
    );
  });

/** One statistics sample a second, as far as the evidence keeps it. */
const sampleStats = (session: Pick<Session.Session, "stats">, samples: Array<StatsSample>) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const stats = yield* session.stats;
    const atMs = yield* run.now;
    if (samples.length < 600)
      samples.push({
        atMs,
        ...(stats.pair?.localCandidateType === undefined
          ? {}
          : { local: stats.pair.localCandidateType }),
        ...(stats.pair?.remoteCandidateType === undefined
          ? {}
          : { remote: stats.pair.remoteCandidateType }),
        ...(stats.roundTripTimeSeconds === undefined
          ? {}
          : { rttMs: round(stats.roundTripTimeSeconds * 1000) }),
        ...(stats.rates === undefined
          ? {}
          : { receivedKbps: round(stats.rates.receivedBitsPerSecond / 1000) }),
        ...(stats.framesPerSecond === undefined ? {} : { fps: round(stats.framesPerSecond) }),
        ...(stats.jitterSeconds === undefined
          ? {}
          : { jitterMs: round(stats.jitterSeconds * 1000) }),
        ...(stats.lossRatio === undefined ? {} : { lossRatio: round(stats.lossRatio, 4) }),
      });
  }).pipe(Effect.ignore, Effect.repeat(Schedule.spaced("1 second")));

/**
 * One session end to end through the public API: pricing, allocation,
 * connection, a clip from submission to playback, its media, and termination.
 * `turn` must be carried by a relay pair; `audio` sends a reference image and a
 * reference audio clip, and the clip must report the audio.
 */
export const vertical = (check: "vertical" | "turn" | "audio") =>
  Effect.gen(function* () {
    const run = yield* Run;
    const target = yield* Target;
    const grant = yield* mint(check);
    const tokens = new Map<string, Coordinator.TokenGrant>();
    const marker = `hosted-qualification:${run.runId}`;
    const tally = contractTally();
    const video = Media.videoLog();
    const audio = Media.audioLog();
    const samples: Array<StatsSample> = [];
    yield* Effect.scoped(
      Effect.gen(function* () {
        const reactor = yield* Reactor.Reactor;
        let deadline = 0;
        const session = yield* recorded(
          reactor.create({
            model: H3.modelName,
            tokens: Coordinator.fixedTokens(grant),
            onAllocated: (allocation) =>
              Effect.map(allocated(allocation.id, grant), (at) => {
                deadline = at;
                tokens.set(allocation.id, grant);
              }),
          }),
        );
        yield* Effect.addFinalizer(() => Effect.ignore(close(session)));
        yield* run.mark("connected");
        const provider = yield* H3.make(session);
        yield* provider.events({ capacity: 4096 }).pipe(
          Stream.runForEach((event) => Effect.sync(() => tally.add(event))),
          Effect.catch((error) => Effect.sync(() => tally.failed(error))),
          Effect.forkScoped,
        );
        yield* sampleStats(session, samples).pipe(Effect.forkScoped);
        const inspector = yield* Coordinator.make({
          apiUrl: target.apiUrl,
          credential: Effect.succeed(grant.jwt),
        });
        const inspection = yield* inspector.inspect(session.id);
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
        yield* readInto(media.video(tracks.video), video).pipe(Effect.forkScoped);
        if (audioOffered) yield* readInto(media.audio(tracks.audio), audio).pipe(Effect.forkScoped);
        // H3 holds a generated clip until it is played, unless autoplay is on.
        yield* recorded(provider.setAutoplay(true));
        const request: H3.Request =
          check === "audio"
            ? {
                prompt: `Picture 1 is a plain gray backdrop. Audio 1 is a low, steady hum under the scene. ${prompt}`,
                seconds: clipSeconds,
                metadata: marker,
                // H3 takes audio only beside an image or a continuation.
                references: [{ _tag: "Bytes", bytes: Media.grayPng({ width: 256, height: 144 }) }],
                audio: [
                  {
                    _tag: "Bytes",
                    bytes: Media.tone({ seconds: 3, sampleRate: 48_000, frequency: 220 }),
                  },
                ],
              }
            : { prompt, seconds: clipSeconds, metadata: marker };
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
                      images: request.references?.length ?? 0,
                      audio: request.audio?.length ?? 0,
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
        yield* run.update((evidence) => ({
          ...evidence,
          clip: {
            clipId: clip.clip_id,
            acceptance: acceptance.evidence.kind,
            submitMs,
            acceptedMs: submitMs,
            echoes: {},
          },
        }));
        const acceptedMs = yield* run.now;
        yield* run.update((evidence) =>
          evidence.clip === undefined
            ? evidence
            : { ...evidence, clip: { ...evidence.clip, acceptedMs } },
        );
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
        yield* Effect.sleep(Duration.min(Duration.millis(target.windowMs), yield* until(deadline)));
        yield* record;
        const evidence = yield* run.evidence;
        yield* run.judge(
          "correlated acceptance",
          acceptance.evidence.kind === "correlated"
            ? undefined
            : `acceptance was only by ${acceptance.evidence.kind}`,
        );
        yield* run.judge(
          "lifecycle progression",
          lifecycle.generatedMs !== undefined && lifecycle.startedMs !== undefined
            ? undefined
            : "generated and started were not both observed",
        );
        // Started is a provider fact: idle frames can still arrive before the media updates.
        yield* run.judge("live video", video.live(lifecycle.startedMs ?? 0));
        yield* run.judge(
          "audio when offered",
          !audioOffered || (audio.count > 0 && audio.summary().peakRms > 0)
            ? undefined
            : "the session offered audio and no sound arrived",
        );
        yield* run.judge(
          "metadata preserved",
          Object.keys(evidence.clip?.echoes ?? {}).some((type) => type !== "clip_queued")
            ? undefined
            : "no provider message after the reply listed the clip with its metadata",
        );
        if (check === "audio") {
          const references = evidence.clip?.references;
          yield* run.judge(
            "reference audio reported",
            references?.hasReferenceAudio === true && references.reportedAudio === references.audio
              ? undefined
              : `the clip reports has_reference_audio ${String(references?.hasReferenceAudio ?? "absent")} and ${String(references?.reportedAudio ?? "no")} audio reference(s) for ${String(references?.audio ?? 0)} sent`,
          );
        }
        const pair = evidence.network?.pair;
        yield* run.judge(
          check === "turn" ? "relay pair selected" : "ICE pair selected",
          pair === undefined
            ? "no statistics sample named a pair that was receiving"
            : check === "turn" && pair !== "relay"
              ? `the pair carrying the media was ${pair}`
              : undefined,
        );
      }),
    ).pipe(Effect.ensuring(settle(tokens)));
  });

/**
 * A process owns a session, streams, and is killed mid-clip; this process
 * takes the session over. `takeover` attaches with the raw session API and
 * ends the session through the dead owner's record; `resume` adopts it with
 * `H3Source.resume` and ends it by closing the resumed source.
 */
export const takeover = (check: "takeover" | "resume") =>
  Effect.gen(function* () {
    const run = yield* Run;
    const target = yield* Target;
    const coordinator = yield* Coordinator.Coordinator;
    const grant = yield* mint(check);
    // The taker holds the key, as a server adopting a dead owner's session does: it mints a
    // token bound to the session, which allocates nothing and stays out of the evidence.
    const bound: Pick<Coordinator.Tokens, "bind"> = {
      bind: (sessionId) =>
        coordinator
          .mintToken({
            apiKey: target.apiKey,
            modelName: H3.modelName,
            bind: [sessionId],
            expiresAfter: `${tokenSeconds} seconds`,
          })
          .pipe(Effect.tap((token) => run.secret(token.jwt))),
    };
    const tokens = new Map<string, Coordinator.TokenGrant>();
    const marker = `hosted-qualification:${run.runId}`;
    const video = Media.videoLog();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* target.owner(grant, marker);
        const sessionId = owner.allocation.sessionId;
        tokens.set(sessionId, grant);
        const cap = (grant.maxSessionSeconds ?? sessionSeconds) * 1000;
        const endsAt = owner.allocation.endsAt ?? ((yield* Clock.currentTimeMillis) + cap) / 1000;
        const allocatedAt = endsAt * 1000 - cap;
        const deadline = allocatedAt + workSeconds * 1000;
        yield* run.update((evidence) => ({
          ...evidence,
          sessions: [
            ...evidence.sessions,
            {
              id: sessionId,
              allocatedMs: allocatedAt - run.origin,
              capEndsAt: DateTime.formatIso(DateTime.makeUnsafe(endsAt * 1000)),
              trail: [],
            },
          ],
        }));
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
              const provider = yield* H3.make(session);
              const attachMs = (yield* run.now) - takenMs;
              yield* run.mark("attached");
              const snapshot = yield* provider.snapshot;
              const facts = snapshot._tag === "Ready" ? snapshot : snapshot.lastFacts;
              const queued = [
                ...(facts?.queue.generation ?? []),
                ...(facts?.queue.playout ?? []),
              ].find((clip) => clip.clip_id === owner.queued);
              const media = yield* session.decoded;
              const attachedMs = yield* run.now;
              yield* readInto(media.video(tracks.video).pipe(Stream.take(48)), video).pipe(
                Effect.timeout(
                  Duration.min(Duration.millis(target.windowMs), yield* until(deadline)),
                ),
                Effect.ignore,
              );
              return {
                attachMs,
                attachedMs,
                playingClipId: facts?.state.playing_clip_id ?? null,
                metadataPreserved: queued?.metadata.includes(`${marker}:queued`) === true,
              };
            }
            const source = yield* H3Source.resume({
              allocation: owner.allocation,
              tokens: bound,
            }).pipe(Effect.provideService(Reactor.Reactor, reactor));
            const attachMs = (yield* run.now) - takenMs;
            yield* run.mark("resumed");
            const state = yield* source.events.pipe(
              Stream.filter((event) => event._tag === "State"),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
            const clips = [...state.state.building, ...state.state.ready];
            const attachedMs = yield* run.now;
            yield* readInto(source.video.pipe(Stream.take(48)), video).pipe(
              Effect.timeout(
                Duration.min(Duration.millis(target.windowMs), yield* until(deadline)),
              ),
              Effect.ignore,
            );
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
        const evidence = yield* run.evidence;
        const commands: Record<string, number> = {};
        for (const span of evidence.spans)
          if (span.name === "reactor.session.command" && span.startMs >= takenMs)
            bump(commands, String(span.attributes["reactor.operation"]));
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
          const inspector = yield* Coordinator.make({
            apiUrl: target.apiUrl,
            credential: Effect.succeed(grant.jwt),
          });
          const requestedMs = yield* run.now;
          const termination = yield* inspector.terminate(sessionId);
          yield* closedWith(sessionId, requestedMs, { termination });
        } else {
          // A resume reads the session; the owner already gave it the defaults a playout needs.
          const writes = Object.keys(commands).filter(
            (name) => !["get_state", "get_queue"].includes(name),
          );
          yield* run.judge(
            "only reads on resume",
            writes.length === 0 ? undefined : `the resume sent ${writes.join(", ")}`,
          );
          const report = (yield* run.evidence).sessions[0]?.close?.report;
          yield* run.judge(
            "adopted close terminates",
            report === undefined
              ? "the resumed source was never closed"
              : report.ownership !== "owned"
                ? `the resumed session's close reported it ${report.ownership ?? "without ownership"}`
                : report.remote.attempted
                  ? undefined
                  : "closing the adopted session attempted no termination",
          );
        }
        yield* run.judge(
          "attach within 5 s",
          taken.attachMs <= 5_000 ? undefined : `attaching took ${Math.round(taken.attachMs)} ms`,
        );
        yield* run.judge(
          "clip identified",
          taken.playingClipId === owner.playing
            ? undefined
            : `the attached state named ${taken.playingClipId ?? "no clip"} playing, not the owner's`,
        );
        yield* run.judge(
          "metadata preserved",
          taken.metadataPreserved
            ? undefined
            : "the owner's queued clip lost its metadata, or was not listed",
        );
        yield* run.judge(
          "no enqueue on attach",
          (commands.enqueue ?? 0) === 0
            ? undefined
            : `${commands.enqueue} enqueue(s) after attaching`,
        );
        yield* run.judge(
          "fresh frames",
          video.count === 0 ? "no frame arrived after attaching" : video.live(taken.attachedMs),
        );
      }),
    ).pipe(
      // Whatever failed, a session the dead owner allocated ends through its record.
      Effect.ensuring(
        Effect.gen(function* () {
          const evidence = yield* run.evidence;
          for (const session of evidence.sessions) {
            if (session.close !== undefined) continue;
            const inspector = yield* Coordinator.make({
              apiUrl: target.apiUrl,
              credential: Effect.succeed(grant.jwt),
            });
            const requestedMs = yield* run.now;
            yield* closedWith(session.id, requestedMs, {
              termination: yield* inspector.terminate(session.id),
            });
          }
        }).pipe(Effect.ignore),
      ),
      Effect.ensuring(settle(tokens)),
    );
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
export const tokens = Effect.gen(function* () {
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* Coordinator.Coordinator;
  const record = (change: (tokens: Evidence.TokensRecord) => Evidence.TokensRecord) =>
    run.update((evidence) => ({
      ...evidence,
      tokens: change(evidence.tokens ?? { probes: [], mints: [] }),
    }));
  const minted = (
    kind: "create" | "bind" | "unbound",
    grant: Coordinator.TokenGrant,
    sentAt: number,
  ) =>
    Effect.gen(function* () {
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
  const probes = yield* Probes.run({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  yield* record((tokens) => ({ ...tokens, probes: [...probes] }));
  yield* run.mark("probed");
  const createSentAt = yield* Clock.currentTimeMillis;
  const grant = yield* mint("tokens", createSeconds);
  yield* minted("create", grant, createSentAt);
  // The adopter's tokens, each bound to the session and short enough to be refreshed in it.
  const bound: Array<{ readonly grant: Coordinator.TokenGrant; readonly sentAt: number }> = [];
  const binder: Pick<Coordinator.Tokens, "bind"> = {
    bind: (sessionId) =>
      Effect.gen(function* () {
        const sentAt = yield* Clock.currentTimeMillis;
        const token = yield* coordinator.mintToken({
          apiKey: target.apiKey,
          modelName: H3.modelName,
          bind: [sessionId],
          expiresAfter: `${boundSeconds} seconds`,
        });
        yield* run.secret(token.jwt);
        bound.push({ grant: token, sentAt });
        yield* minted("bind", token, sentAt);
        return token;
      }),
  };
  const sessions = new Map<string, Coordinator.TokenGrant>();
  const marker = `hosted-qualification:${run.runId}`;
  const video = Media.videoLog();
  yield* Effect.scoped(
    Effect.gen(function* () {
      const owner = yield* target.owner(grant, marker);
      const sessionId = owner.allocation.sessionId;
      sessions.set(sessionId, grant);
      const cap = (grant.maxSessionSeconds ?? sessionSeconds) * 1000;
      const endsAt = owner.allocation.endsAt ?? ((yield* Clock.currentTimeMillis) + cap) / 1000;
      const allocatedAt = endsAt * 1000 - cap;
      const deadline = allocatedAt + workSeconds * 1000;
      yield* run.update((evidence) => ({
        ...evidence,
        sessions: [
          ...evidence.sessions,
          {
            id: sessionId,
            allocatedMs: allocatedAt - run.origin,
            capEndsAt: DateTime.formatIso(DateTime.makeUnsafe(endsAt * 1000)),
            trail: [],
          },
        ],
      }));
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
      const session = yield* reactor.attach({ sessionId, tokens: binder, adopt: true });
      const provider = yield* H3.make(session);
      const attachedMs = yield* run.now;
      yield* run.mark("adopted");
      const snapshot = yield* provider.snapshot;
      const facts = snapshot._tag === "Ready" ? snapshot : snapshot.lastFacts;
      const playingClipId = facts?.state.playing_clip_id ?? null;
      const media = yield* session.decoded;
      yield* readInto(media.video(tracks.video).pipe(Stream.take(48)), video).pipe(
        Effect.timeout(Duration.min(Duration.millis(target.windowMs), yield* until(deadline))),
        Effect.ignore,
      );
      const firstFreshFrameMs = video.firstAfter(attachedMs);
      yield* record((tokens) => ({
        ...tokens,
        resumeStartedMs,
        attachedMs,
        playingClipId,
        clipIdentified: playingClipId === owner.playing,
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
      const request: H3.Request = {
        prompt: `Picture 1 is a plain gray backdrop. Audio 1 is a low, steady hum under the scene. ${prompt}`,
        seconds: clipSeconds,
        metadata: `${marker}:references`,
        references: [{ _tag: "Bytes", bytes: Media.grayPng({ width: 256, height: 144 }) }],
        audio: [
          {
            _tag: "Bytes",
            bytes: Media.tone({ seconds: 3, sampleRate: 48_000, frequency: 220 }),
          },
        ],
      };
      const startedMs = yield* run.now;
      const acceptance = yield* recorded(
        Effect.flatMap(provider.prepare(request), (submission) => submission.submit),
      );
      const acceptedMs = yield* run.now;
      const refreshedMs = (yield* run.evidence).tokens?.mints.filter(
        (mint) => mint.kind === "bind",
      )[1]?.atMs;
      yield* record((tokens) => ({
        ...tokens,
        ...(refreshedMs === undefined ? {} : { refreshedMs }),
        upload: {
          startedMs,
          acceptedMs,
          images: 1,
          audio: 1,
          reportedAudio: acceptance.clip.reference_audio_count ?? null,
          hasReferenceAudio: acceptance.clip.has_reference_audio ?? null,
        },
      }));
      yield* run.mark("enqueued on a refreshed token");
      // The documented refusals: a token past its expiry, and one not bound to the session.
      const statusOf = (credential: Redacted.Redacted<string>) =>
        Coordinator.make({ apiUrl: target.apiUrl, credential: Effect.succeed(credential) }).pipe(
          Effect.flatMap((inspector) => inspector.inspect(sessionId)),
          Effect.as(200),
          Effect.catch((error) =>
            Effect.succeed(
              error.reason._tag === "Http" && error.reason.status !== undefined
                ? error.reason.status
                : 0,
            ),
          ),
        );
      const expiredTokenStatus = yield* statusOf(grant.jwt);
      const unboundSentAt = yield* Clock.currentTimeMillis;
      const unbound = yield* coordinator.mintToken({
        apiKey: target.apiKey,
        modelName: H3.modelName,
        // It could create one session of a second; it creates none.
        maxSessionDuration: "1 second",
        expiresAfter: "15 seconds",
      });
      yield* run.secret(unbound.jwt);
      yield* minted("unbound", unbound, unboundSentAt);
      const unboundTokenStatus = yield* statusOf(unbound.jwt);
      yield* record((tokens) => ({ ...tokens, expiredTokenStatus, unboundTokenStatus }));
      // The key ends any session of its account, with the independent read confirming it.
      const keyed = yield* Coordinator.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
      const requestedMs = yield* run.now;
      const apiKeyTermination = yield* keyed.terminate(sessionId);
      yield* record((tokens) => ({ ...tokens, apiKeyTermination }));
      if (apiKeyTermination.confirmed)
        yield* closedWith(sessionId, requestedMs, { termination: apiKeyTermination });
      else yield* close(session);
      const evidence = yield* run.evidence;
      const commands: Record<string, number> = {};
      for (const span of evidence.spans)
        if (span.name === "reactor.session.command" && span.startMs >= resumeStartedMs)
          bump(commands, String(span.attributes["reactor.operation"]));
      yield* record((tokens) => ({ ...tokens, commands }));
      const latest = bound.at(-1);
      if (latest !== undefined) sessions.set(sessionId, latest.grant);
      const binds = bound.length;
      yield* run.judge(
        "adopted after the creating token expired",
        resumeStartedMs > createExpiresMs
          ? undefined
          : `the creating token lived ${Math.round((createExpiresMs - resumeStartedMs) / 1000)} s past the adoption`,
      );
      yield* run.judge(
        "clip identified",
        playingClipId === owner.playing
          ? undefined
          : `the adopted state named ${playingClipId ?? "no clip"} playing, not the owner's`,
      );
      yield* run.judge(
        "fresh frames",
        video.count === 0 ? "no frame arrived after adopting" : video.live(attachedMs),
      );
      yield* run.judge(
        "a refreshed bound token carried the next call",
        binds < 2 || refreshedMs === undefined
          ? `${binds} bound token(s) were minted`
          : refreshedMs < startedMs || refreshedMs > acceptedMs
            ? "the refresh did not happen for the clip's upload"
            : undefined,
      );
      yield* run.judge(
        "reference audio reported",
        acceptance.clip.has_reference_audio === true && acceptance.clip.reference_audio_count === 1
          ? undefined
          : `the clip reports has_reference_audio ${String(acceptance.clip.has_reference_audio ?? "absent")} and ${String(acceptance.clip.reference_audio_count ?? "no")} audio reference(s) for 1 sent`,
      );
      yield* run.judge(
        "an expired token is refused",
        expiredTokenStatus === 401 ? undefined : `reading with it answered ${expiredTokenStatus}`,
      );
      yield* run.judge(
        "an unbound token is refused",
        unboundTokenStatus === 403 ? undefined : `reading with it answered ${unboundTokenStatus}`,
      );
      yield* run.judge(
        "the API key ends the session",
        apiKeyTermination.confirmed
          ? undefined
          : `DELETE answered ${String(apiKeyTermination.deleteStatus ?? "nothing")} and the read found ${apiKeyTermination.state ?? "no terminal state"}`,
      );
      yield* run.mark("tokens observed");
    }),
  ).pipe(
    // Whatever failed, the key ends a session the check allocated.
    Effect.ensuring(
      Effect.gen(function* () {
        const keyed = yield* Coordinator.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
        for (const session of (yield* run.evidence).sessions) {
          if (session.close !== undefined) continue;
          const requestedMs = yield* run.now;
          yield* closedWith(session.id, requestedMs, {
            termination: yield* keyed.terminate(session.id),
          });
        }
      }).pipe(Effect.ignore),
    ),
    Effect.ensuring(settle(sessions)),
  );
});

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
export const queue = Effect.gen(function* () {
  const run = yield* Run;
  const grant = yield* mint("queue");
  const tokens = new Map<string, Coordinator.TokenGrant>();
  const marker = `hosted-qualification:${run.runId}:queue`;
  const observed = yield* SubscriptionRef.make<ReadonlyArray<Observed>>([]);
  yield* Effect.scoped(
    Effect.gen(function* () {
      const reactor = yield* Reactor.Reactor;
      let deadline = 0;
      const session = yield* recorded(
        reactor.create({
          model: H3.modelName,
          tokens: Coordinator.fixedTokens(grant),
          onAllocated: (allocation) =>
            Effect.map(allocated(allocation.id, grant), (at) => {
              deadline = at;
              tokens.set(allocation.id, grant);
            }),
        }),
      );
      yield* Effect.addFinalizer(() => Effect.ignore(close(session)));
      const provider = yield* H3.make(session);
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
      yield* readInto(media.video(tracks.video), video).pipe(Effect.forkScoped);
      const seen = (type: string, clipId: string) =>
        waitFor(
          observed,
          (all) => all.find((event) => event.type === type && event.clipId === clipId),
          deadline,
        );
      const started = (all: ReadonlyArray<Observed>) =>
        all.filter((event) => event.type === "clip_started");
      // `sending` completes as the enqueue commits, just before it is written to the channel.
      const submit = (name: string, position?: number, sending?: Deferred.Deferred<void>) =>
        Effect.gen(function* () {
          const submission = yield* provider.prepare(
            {
              prompt,
              seconds: clipSeconds,
              metadata: `${marker}:${name}`,
              ...(position === undefined ? {} : { position }),
            },
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
      const pop = (clipId: string) => provider.pop(clipId).pipe(recorded, Effect.ignore);

      yield* recorded(provider.setAutoplay(false));
      // H3 documents position zero as next, behind the build that is running; `first` is that
      // build on a session with nothing else building.
      const first = yield* submit("first");
      const zero = yield* submit("position-zero", 0);
      const tail = yield* submit("tail");
      const generationOrder = (yield* readQueue).generation.map((clip) => clip.clip_id);
      const place = (clipId: string) => generationOrder.indexOf(clipId);
      yield* run.judge(
        "position zero goes next, behind the build that is running",
        place(zero.clipId) < 0 || place(tail.clipId) < place(zero.clipId)
          ? "the position-zero clip was not ahead of the clip queued after it"
          : place(first.clipId) > place(zero.clipId)
            ? "the position-zero clip went ahead of the build that was running"
            : undefined,
      );
      // A build popped while it runs must never reach playout; the clip behind it shows
      // how long the popped build keeps the build slot.
      const popped = first;
      const wasBuilding = generationOrder[0] === popped.clipId;
      yield* pop(popped.clipId);
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
      yield* run.judge(
        "a queue read sent right behind an enqueue lists the new clip",
        ordering.every(Boolean)
          ? undefined
          : `${ordering.filter((listed) => !listed).length} of ${ordering.length} reads did not list it`,
      );
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
        yield* run.judge(
          `${boundary.edit} ${boundary.aimMs} ms before boundary ${index + 1}`,
          boundary.sentMs === undefined
            ? "the edit was not staged: fewer than two clips were waiting"
            : boundary.refused === true
              ? "the provider refused the edit"
              : boundary.nextClipId !== boundary.expectedClipId
                ? "a different clip started next"
                : undefined,
        );
      }
      const all = yield* SubscriptionRef.get(observed);
      const startedIds = new Set(started(all).map((event) => event.clipId));
      yield* run.judge(
        "popped clips never start",
        acceptedPops.some((clipId) => startedIds.has(clipId))
          ? "a clip started after its pop was accepted"
          : undefined,
      );
      const evidence = yield* run.evidence;
      const poppedBuild = evidence.queue?.poppedBuild;
      yield* run.judge(
        "pop the build in flight",
        poppedBuild?.wasBuilding !== true
          ? "the clip was not at the head of the generation queue before the pop"
          : poppedBuild.generatedAfterPop || poppedBuild.startedAfterPop
            ? "the popped build generated or started after the pop's reply"
            : undefined,
      );
      const metadata = evidence.queue?.metadata;
      yield* run.judge(
        "observed clip metadata",
        metadata !== undefined &&
          Object.keys(metadata.observed).length > 0 &&
          Object.keys(metadata.mismatched).length === 0
          ? undefined
          : "a watched clip message had missing metadata, or none was observed",
      );
      yield* run.mark("queue observed");
    }),
  ).pipe(Effect.ensuring(settle(tokens)));
});

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
      return { event: `control ${event.message.payload.case ?? "unknown"}` };
    case "CommandError":
      return { event: `command error ${event.error.reason._tag}` };
    case "Diagnostic":
      return { event: `diagnostic ${event.error.reason._tag}` };
    default:
      return undefined;
  }
};

/** What a playout check has while its playout runs. */
interface Air {
  readonly playout: Playout.Playout["Service"];
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
  readonly grant: (sessionId: string) => Coordinator.TokenGrant | undefined;
  readonly starts: () => ReadonlyArray<string>;
  /** Submits an item under `key`, recording when. */
  readonly track: (key: string) => Effect.Effect<Playout.ItemKey>;
  readonly video: Media.VideoLog;
  /** The work deadline of the first session. */
  readonly deadline: () => number;
  /** The seam between two items, measured from the decoded picture. */
  readonly seam: (
    ending: string,
    next: string,
    continued: boolean,
  ) => Effect.Effect<Seam, never, FileSystem.FileSystem | Path.Path>;
}

/**
 * A playout over H3 sessions this check opens, at most `sessions` of them,
 * with its as-run and picture recorded. Sessions hold the last frame at
 * boundaries, as a show does. `renew` false keeps the one session for the
 * whole check: its grant ends it, so a replacement is never planned.
 */
const onAir = <A, E, R>(
  check: Check,
  options: {
    readonly lanes: ReadonlyArray<Playout.LaneSpec>;
    readonly sessions: number;
    readonly renewal?: Playout.Options["renewal"];
    readonly maxModerations?: number;
    readonly maxBuildsInFlight?: number;
  },
  scenario: (air: Air) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const run = yield* Run;
    const target = yield* Target;
    const tokens = new Map<string, Coordinator.TokenGrant>();
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
    let opened = 0;
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
      const source = yield* H3Source.open({
        tokens: Coordinator.fixedTokens(grant),
        holdLastFrame: true,
        onAllocated: ({ session }) =>
          Effect.gen(function* () {
            const at = yield* allocated(session.id, grant);
            deadline = Math.min(deadline, at);
            tokens.set(session.id, grant);
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
          }).pipe(Effect.mapError((error) => ReactorError.fromCode("InvalidState", error.message))),
      });
      const recordClose = (requestedMs: number, report: Session.CloseReport) =>
        closedWith(source.sessionId, requestedMs, { report }).pipe(
          Effect.provideService(Run, run),
          Effect.ignore,
        );
      const wrapped: Playout.Source = {
        ...source,
        ...(options.renewal === undefined ? { lifetime: Duration.infinity } : {}),
        // A failed playout closes its sessions on a fiber its scope may interrupt: the
        // close and its record finish together.
        close: Effect.uninterruptible(
          Effect.gen(function* () {
            const requestedMs = yield* run.now;
            const report = yield* source.close;
            yield* recordClose(requestedMs, report);
            return report;
          }),
        ),
      };
      return wrapped;
    });
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const playout = yield* Playout.make({
          open,
          lanes: options.lanes,
          ...(options.renewal === undefined ? {} : { renewal: options.renewal }),
          ...(options.maxModerations === undefined
            ? {}
            : { maxModerations: options.maxModerations }),
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
                    return new Map(all).set(event.key, {
                      ...next,
                      failed: {
                        reason: status.reason,
                        moderated: status.moderated === true,
                        lost: status.lost !== undefined,
                      },
                    });
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
        const record = Effect.gen(function* () {
          const all = yield* SubscriptionRef.get(items);
          yield* run.update((evidence) => ({
            ...evidence,
            playout: {
              seams: [],
              ...evidence.playout,
              items: [...all.values()],
              startOrder: [...starts],
              video: video.summary(),
              audio: audio.summary(),
            },
          }));
        });
        yield* Effect.addFinalizer(() => Effect.ignore(record));
        const air: Air = {
          playout,
          items,
          asRun: () => [...timeline],
          sessionLog: () => [...sessionLog],
          grant: (sessionId) => tokens.get(sessionId),
          starts: () => [...starts],
          track: (key) =>
            Effect.gen(function* () {
              const submittedMs = yield* run.now;
              yield* SubscriptionRef.update(items, (all) =>
                new Map(all).set(key, { key, submittedMs, last: "Accepted" }),
              );
              return Playout.ItemKey.make(key);
            }),
          video,
          deadline: () => deadline,
          seam: (ending, next, continued) =>
            Effect.gen(function* () {
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
        yield* run.update((evidence) => ({
          ...evidence,
          playout: {
            seams: [],
            items: [],
            startOrder: [],
            ...evidence.playout,
            estimates: {
              ...(state.estimates.build === undefined
                ? {}
                : {
                    buildMedian: round(state.estimates.build.median, 3),
                    buildP95: round(state.estimates.build.p95, 3),
                  }),
              length: round(state.estimates.length, 4),
            },
          },
        }));
        return result;
      }),
    ).pipe(Effect.ensuring(settle(tokens)));
  });

const setSeams = (seams: ReadonlyArray<Seam>) =>
  Effect.gen(function* () {
    const run = yield* Run;
    yield* run.update((evidence) => ({
      ...evidence,
      playout: { items: [], startOrder: [], ...evidence.playout, seams: [...seams] },
    }));
  });

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
    const item = (key: string) => (all: ReadonlyMap<string, Item>) => all.get(key);
    const startedOf = (key: string) => (all: ReadonlyMap<string, Item>) => all.get(key)?.startedMs;
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
    yield* recorded(
      air.playout.submit({ key: yield* air.track("w1"), lane: "line", request: itemRequest() }),
    );
    yield* run.mark("line submitted");
    yield* waitFor(air.items, startedOf("p1"), air.deadline());
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
    const p3Started = yield* waitFor(air.items, startedOf("p3"), air.deadline());
    const p3 = yield* waitFor(air.items, item("p3"), air.deadline());
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
    const yStarted = yield* waitFor(air.items, startedOf("y"), air.deadline());
    yield* sleepUntil(yStarted + 2 * seamMs + 250, air.deadline());
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
    yield* setSeams(seams);
    const p3End = all.get("p3")?.endedMs;
    yield* run.update((evidence) => ({
      ...evidence,
      playout: {
        items: [],
        startOrder: [],
        seams: [],
        ...evidence.playout,
        batch: {
          submittedMs,
          ...(committedMs === undefined ? {} : { committedMs }),
          ...(p3End === undefined ? {} : { boundaryMs: p3End }),
        },
      },
    }));
    yield* run.judge(
      "inserts and the batch air in their planned places",
      order.join(",") === plannedEdits.join(",")
        ? undefined
        : `started in the order ${order.join(", ")}`,
    );
    yield* run.judge(
      "a batch takes effect before its boundary",
      committedMs === undefined
        ? "the batch never took effect"
        : p3End !== undefined && committedMs >= p3End
          ? "the batch took effect after the playing clip ended"
          : undefined,
    );
    const w1 = all.get("w1");
    yield* run.judge(
      "a batch's withdrawn clip never starts",
      w1?.startedMs !== undefined
        ? "the withdrawn clip started"
        : w1?.dropped !== "withdrawn"
          ? `the withdrawn clip ended ${w1?.last ?? "untracked"}`
          : undefined,
    );
    yield* run.judge(
      "every seam measured",
      seams.length < plannedEdits.length - 1 ||
        seams.some((seam) => seam.pause === undefined || seam.jump === undefined)
        ? "a boundary has no pause or join measurement"
        : undefined,
    );
    yield* run.mark("edits observed");
  }),
);

/** How long a held, flagged item is watched for a verdict or its session's end. */
const moderationWaitMs = 12_000;

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
          playoutLog.push({ atMs, event: `failed ${failure.reason._tag}` });
        }),
      ),
      Effect.forkScoped,
    );
    yield* recorded(
      air.playout.submit({
        key: yield* air.track("guard"),
        lane: "line",
        request: itemRequest(15),
      }),
    );
    const submittedMs = yield* run.now;
    yield* recorded(
      air.playout.submit({
        key: yield* air.track("flagged"),
        lane: "line",
        request: { prompt: Redacted.value(flagged), seconds: clipSeconds },
      }),
    );
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
    const watchUntil = Math.min(
      air.deadline(),
      (yield* Clock.currentTimeMillis) + moderationWaitMs,
    );
    while (!(yield* settled) && (yield* Clock.currentTimeMillis) < watchUntil)
      yield* Effect.sleep("250 millis");
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
    if (ended())
      while (!(yield* closed) && (yield* Clock.currentTimeMillis) < air.deadline())
        yield* Effect.sleep("250 millis");
    const enqueueSpan = (yield* run.evidence).spans.find(
      (span) =>
        span.name === "reactor.session.command" &&
        span.attributes["reactor.operation"] === "enqueue" &&
        span.startMs >= submittedMs,
    );
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
            ? {
                detail: `${status.moderated === true ? "moderated" : status.lost === undefined ? "failed" : "lost"}: ${status.reason}`,
              }
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
        yield* recorded(
          air.playout.submit({
            key: yield* air.track("long"),
            lane: "line",
            request: itemRequest(15),
          }),
        );
        const longStarted = yield* waitFor(
          air.items,
          (all) => all.get("long")?.startedMs,
          air.deadline(),
        );
        yield* sleepUntil(longStarted + 2_500, air.deadline());
        const cutFromMs = yield* run.now;
        yield* recorded(
          air.playout.submit({
            key: yield* air.track("cutter"),
            lane: "urgent",
            request: itemRequest(),
          }),
        );
        yield* run.mark("cutter submitted");
        const cutterStarted = yield* waitFor(
          air.items,
          (all) => all.get("cutter")?.startedMs,
          air.deadline(),
        );
        yield* sleepUntil(cutterStarted + 2 * seamMs + 250, air.deadline());
        const all = yield* SubscriptionRef.get(air.items);
        const seam = yield* air.seam("long", "cutter", false);
        yield* setSeams([seam]);
        // H3's stop names no clip: a second one stops whatever plays by then.
        const stops = (yield* run.evidence).spans.filter(
          (span) =>
            span.name === "reactor.session.command" &&
            span.attributes["reactor.operation"] === "stop" &&
            span.startMs >= cutFromMs,
        ).length;
        yield* run.update((evidence) => ({
          ...evidence,
          playout: { items: [], startOrder: [], seams: [], ...evidence.playout, stops },
        }));
        const long = all.get("long");
        const cutter = all.get("cutter");
        const order = air.starts();
        yield* run.judge(
          "a cut lane's clip stops a lower lane's playing clip",
          long?.termination !== "stopped"
            ? `the long clip ended ${long?.termination ?? long?.last ?? "untracked"}`
            : order[order.indexOf("long") + 1] !== "cutter"
              ? "the cut-lane clip did not start next"
              : cutter?.termination === "stopped"
                ? `the cut-lane clip was itself stopped after ${String(cutter.airedSeconds ?? "?")} s`
                : stops !== 1
                  ? `${stops} stops were sent for one cut`
                  : undefined,
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
      yield* recorded(
        air.playout.submit({ key: yield* air.track("A"), lane: "line", request: itemRequest() }),
      );
      yield* run.mark("A submitted");
      yield* waitFor(air.items, (all) => all.get("A")?.endedMs, air.deadline());
      const first = (yield* SubscriptionRef.get(air.items)).get("A")?.sessionId;
      yield* waitFor(
        sessionEvents,
        (all) => all.find(({ event }) => event._tag === "Opened" && event.sessionId !== first),
        air.deadline(),
      );
      yield* run.mark("replacement opened");
      yield* recorded(
        air.playout.submit({ key: yield* air.track("B"), lane: "line", request: itemRequest() }),
      );
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
      yield* run.update((evidence) => ({
        ...evidence,
        playout: {
          items: [],
          startOrder: [],
          seams: [],
          ...evidence.playout,
          switches,
          framesByItem,
          drainedMs,
        },
      }));
      const a = all.get("A");
      const b = all.get("B");
      yield* run.judge(
        "A then B finish",
        a?.termination === "finished" &&
          b?.termination === "finished" &&
          (a.startedMs ?? 0) < (b.startedMs ?? 0)
          ? undefined
          : `A ended ${a?.termination ?? a?.last ?? "untracked"}, B ${b?.termination ?? b?.last ?? "untracked"}`,
      );
      yield* run.judge(
        "B airs on the replacement",
        a?.sessionId !== undefined && b?.sessionId !== undefined && a.sessionId !== b.sessionId
          ? undefined
          : "A and B aired on the same session",
      );
      yield* run.judge(
        "one planned switch",
        switches.length !== 1
          ? `${switches.length} switches`
          : events.some(({ event }) => event._tag === "Replaced")
            ? "a session was replaced before a planned switch"
            : undefined,
      );
      yield* run.judge(
        "video from both sessions",
        framesByItem.A > 0 && framesByItem.B > 0
          ? undefined
          : `the playout's picture carried ${framesByItem.A} frames of A and ${framesByItem.B} of B`,
      );
      yield* run.mark("renewal observed");
    }),
);

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
};
/** What a check can fail with, and what it needs. */
export type CheckError = Effect.Error<(typeof all)[Check]>;
export type CheckServices = Effect.Services<(typeof all)[Check]>;

/** Every check by name. */
export const checks: Record<Check, Effect.Effect<void, CheckError, CheckServices>> = all;
