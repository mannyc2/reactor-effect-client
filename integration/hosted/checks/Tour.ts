/**
 * `tour`: one session on the native host through the raw API, a phase at a
 * time, so hosted Reactor sees each interaction `CoordinatorClient`, `Reactor`,
 * `Session`, `Media` and `H3` make, once. A phase judges what Reactor or the
 * SDK documents and records what they leave open. It runs within its own time
 * limit, and a failure it meets becomes a failed criterion named after it
 * before the tour goes on.
 *
 * The session's tokens come from `CoordinatorClient.tokens` and live 30 s, so it
 * refreshes one to a token bound to itself while it runs. From allocation A,
 * at the timing paid runs measured (connected in about 2.7 s, a 5 s clip built
 * in about 2.2 s, 0.44 s per requested second):
 *
 *   A-0.3   the session's creating token is minted, before the allocation
 *   A+2.7   connected; H3 is set up and the canvas set to one it is not
 *   A+5     clip 1, with a seed and a reference image and audio sent as bytes, is
 *           accepted once both upload; clip 2 reuses those uploads
 *   A+5.5   a clip enqueued at position zero, one moved to the front, the first
 *           popped while it waits
 *   A+7.5   clip 1 is built and starts; the moved clip, now heading the queue, is
 *           popped; a clip past H3's text budget, then one to play by hand, are enqueued
 *   A+12.7  clip 2 starts; autoplay goes off, clip 2 is stopped and the waiting clip
 *           played, and the session resets while it plays
 *   A+14.5  with autoplay off, a 10 s clip is enqueued for the reconnect to play; a
 *           recording of the last 5 s is asked for, and downloaded if one comes
 *   A+22.45 at the token's refresh point, whatever the phases have reached, the
 *           session uploads a picture on its own token and so mints one bound to itself
 *   A+31.7  2 s after the creating token expired, the session reconnects, reads its
 *           state, and plays the 10 s clip on its new connection
 *   A+37    the API key ends the session, and the session's own close follows
 *   A+38    once the end is confirmed, an attach to the ended session and the key
 *           on an unknown one
 *
 * That ends about 40 s inside the 80 s work deadline. Hosted build latency
 * moves the phases before A+22, never the refresh, which runs on its own from
 * the session's creation, or the reconnect; the free mints come before the
 * session.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type * as H3 from "reactor-effect-client/H3";
import type { AudioFrame, DecodedMedia, Recorded, VideoFrame } from "reactor-effect-client/Media";
import * as Reactor from "reactor-effect-client/Reactor";
import { isReactorFailure, ReactorError } from "reactor-effect-client/ReactorError";
import type { CommandFailure, ReactorErrorReason } from "reactor-effect-client/ReactorError";
import type * as Session from "reactor-effect-client/Session";
import type { Pieces } from "../Checks.js";
import type * as Evidence from "../Evidence.js";
import * as Family from "../Family.js";
import { SaveFailed } from "../Ledger.js";
import * as Media from "../Media.js";
import { unknownSession } from "../Probes.js";
import { recorded, Run } from "../Run.js";
import { acceptGrant, plans, provenBind, provenGrant, Refused } from "../Spend.js";
import { prompt, Target } from "../Target.js";

const round = (value: number) => Math.round(value * 10) / 10;

/** How long each of the session's tokens lives: short enough that the session refreshes one. */
const tokenSeconds = 30;
/** The clip played after the reconnect: long enough to be playing still when the session resets. */
const longSeconds = 10;
/** The seed clip 1 asks for, in place of the session's advancing default. */
const seed = 4242;
/** The length of the recording clip asked for, in seconds. */
const snapSeconds = 5;
/** Tokens minted only for their echo expire this soon; none is ever used. */
const freeSeconds = 15;
/** How long frames are read on the new connection. */
const freshMs = 3_000;
/** The reconnect waits this long past the creating token's expiry, for clocks a little apart. */
const afterExpiryMs = 2_000;
/** The name of the upload that refreshes the session's token: it is no clip's. */
const refreshName = "refresh.png";
/**
 * A prompt about twice H3's text budget of about 2,000 tokens, which its
 * prompt guide puts at roughly 8,000 characters of English prose: 220 beats
 * of about 75 characters each.
 */
const overBudget = Array.from({ length: 220 }, (_, beat) => `Beat ${beat + 1}: ${prompt}`).join(
  " ",
);
/** The commands the tour sends the model. */
const sent = [
  "enqueue",
  "move",
  "pop",
  "play",
  "stop",
  "set_canvas",
  "set_autoplay",
  "get_queue",
  "get_state",
  "reset",
] as const;

type TourRecord = Evidence.TourRecord;
type Settings = NonNullable<TourRecord["reset"]>["before"];

/** A clip lifecycle message H3 sent, as the tour saw it. */
interface Seen {
  readonly type: string;
  readonly clipId: string;
  readonly atMs: number;
}

/** A clip the tour enqueued, and the facts of its acceptance. */
interface Submitted<Clip extends H3.Clip> {
  readonly name: string;
  readonly submission: { readonly id: string };
  readonly acceptance: Family.Acceptance<Clip>;
  readonly clipId: string;
  readonly submitMs: number;
  readonly acceptedMs: number;
}

/** A phase's failure as its criterion states it: the tag and a short message, never provider text. */
const failureOf = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause);
  if (Cause.isTimeoutError(error)) return "TimeoutError: the phase ran out of time";
  if (isReactorFailure(error)) return `${error.reason._tag}: ${error.message}`;
  if (Predicate.hasProperty(error, "_tag") && Predicate.isString(error._tag))
    return Predicate.hasProperty(error, "message") && Predicate.isString(error.message)
      ? `${error._tag}: ${error.message.slice(0, 200)}`
      : error._tag;
  return `unexpected: ${String(error).slice(0, 200)}`;
};

/** What H3's state and queue said, the playing clip by the tour's name for it. */
const settingsOf = (facts: H3.Facts, nameOf: (clipId: string) => string): Settings => ({
  aspect: facts.state.aspect,
  width: facts.state.width,
  height: facts.state.height,
  autoplay: facts.state.autoplay,
  flushOnClipEnd: facts.state.flush_on_clip_end,
  seed: facts.state.seed,
  clipSeconds: facts.state.clip_seconds,
  playing: facts.state.playing,
  playingClip: facts.state.playing_clip_id === null ? null : nameOf(facts.state.playing_clip_id),
  queued: facts.queue.generation.length + facts.queue.playout.length,
});

/** What a clip reports of the references it was sent, beside what the tour sent and uploaded. */
const referencesOf = <Req extends Family.RequestInput, Clip extends H3.Clip>(
  family: Family.Family<Req, Clip>,
  clip: Clip,
  sentRefs: { readonly images: number; readonly audio: number; readonly uploads: number },
) => {
  const facts = family.uploadsOf(clip);
  return {
    ...sentRefs,
    reportedImages: facts._tag === "References" ? facts.reportedImages : null,
    reportedAudio: facts._tag === "References" ? facts.reportedAudio : null,
    hasReferenceAudio: facts._tag === "References" ? facts.hasReferenceAudio : null,
    ...(facts._tag === "Frame" ? { hasStartingFrame: facts.hasStartingFrame } : {}),
  };
};

/** How a failure reads in the evidence: the library's reason tag, never provider text. */
const reasonOf = (error: unknown) => (isReactorFailure(error) ? error.reason._tag : "unexpected");

/** The HTTP status a failure carries, when a reply came. */
const httpStatusOf = (error: unknown) =>
  isReactorFailure(error) && error.reason._tag === "Http" ? error.reason.status : undefined;

/** Picture and sound from their start until they are closed. */
interface Window {
  readonly video: Media.VideoLog;
  readonly audio: Media.AudioLog;
}

/**
 * The tour. It takes the pieces it shares with the checks in `Checks.ts` from
 * there, since that module's table imports this one.
 */
const tourFor = Effect.fnUntraced(function* <Req extends Family.RequestInput, Clip extends H3.Clip>(
  pieces: Pieces,
  family: Family.Family<Req, Clip>,
) {
  const run = yield* Run;
  const target = yield* Target;
  const tracks = family.tracks;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const marker = `hosted-qualification:${run.runId}:tour`;
  const record = (change: (tour: TourRecord) => TourRecord) =>
    run.update((evidence) => ({
      ...evidence,
      tour: change(evidence.tour ?? { mints: [], freeMints: [] }),
    }));
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  let deadline = Number.POSITIVE_INFINITY;

  /** Runs a phase within `seconds` and the work deadline; a failure it meets is its criterion. */
  const phase = <A, E, R>(name: string, seconds: number, body: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const left = Duration.toMillis(yield* pieces.until(deadline));
      const exit = yield* body.pipe(
        Effect.timeout(Duration.millis(Math.max(0, Math.min(seconds * 1000, left)))),
        Effect.exit,
      );
      if (Exit.isSuccess(exit)) return Option.some(exit.value);
      if (Cause.hasInterruptsOnly(exit.cause)) return yield* Effect.interrupt;
      // A save that failed would fail again: the run stops.
      const error = Cause.squash(exit.cause);
      if (Schema.is(SaveFailed)(error)) return yield* error;
      yield* run.judge(name, failureOf(exit.cause));
      yield* run.mark(`${name} failed`);
      return Option.none<A>();
    });

  // Free: minting allocates nothing, and none of these tokens is ever used.
  yield* phase(
    "free mints",
    20,
    Effect.gen(function* () {
      const mints = [
        { name: "three sessions", options: { maxSessions: 3, maxSessionDuration: "1 second" } },
        { name: "no session cap", options: { maxSessionDuration: "unlimited" } },
      ] as const;
      for (const { name, options } of mints) {
        const sentAt = yield* Clock.currentTimeMillis;
        const minted = yield* Effect.result(
          coordinator.mintToken({
            apiKey: target.apiKey,
            modelName: run.model.name,
            expiresAfter: `${freeSeconds} seconds`,
            ...options,
          }),
        );
        if (Result.isFailure(minted)) {
          const outcome = minted.failure.reason._tag;
          yield* record((tour) => ({
            ...tour,
            freeMints: [...tour.freeMints, { name, outcome }],
          }));
          continue;
        }
        const grant = minted.success;
        yield* run.secret(grant.jwt);
        const granted = grant.granted;
        yield* record((tour) => ({
          ...tour,
          freeMints: [
            ...tour.freeMints,
            {
              name,
              outcome: "granted",
              lifetimeSeconds: round(grant.expiresAt - sentAt / 1000),
              echoed: granted !== undefined,
              ...(granted === undefined
                ? {}
                : {
                    maxSessions: granted.maxSessions ?? null,
                    // The SDK reads a null cap as unlimited; an absent one says nothing.
                    maxSessionSeconds: granted.maxSessionSeconds ?? "absent",
                    bound: granted.bound.length,
                  }),
            },
          ],
        }));
      }
      yield* run.mark("free mints");
    }),
  );

  const names = new Map<string, string>();
  const nameOf = (clipId: string) => names.get(clipId) ?? "other";

  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        // The session's own tokens, from the SDK: every grant is proven before it is used.
        const tokens = coordinator.tokens({
          apiKey: target.apiKey,
          modelName: run.model.name,
          maxSessionDuration: `${plans.tour.seconds} seconds`,
          expiresAfter: `${tokenSeconds} seconds`,
        });
        let created:
          | {
              readonly grant: CoordinatorClient.TokenGrant;
              readonly sentAt: number;
              readonly cap: number;
            }
          | undefined;
        /** The session's latest grant, for the calls made on its behalf and after its end. */
        let latest: CoordinatorClient.TokenGrant | undefined;
        /**
         * Proves, accepts and records a grant; `bound` is the session a bind is for. A grant
         * refused is recorded with its refusal, and never used.
         */
        const admit = Effect.fnUntraced(
          function* (
            grant: CoordinatorClient.TokenGrant,
            sentAt: number,
            bound: string | undefined,
          ) {
            yield* run.secret(grant.jwt);
            const proven = yield* Effect.result(
              Effect.gen(function* () {
                const granted =
                  bound === undefined
                    ? yield* provenGrant({
                        jwt: Redacted.value(grant.jwt),
                        granted: grant.granted,
                      })
                    : created === undefined
                      ? yield* Refused.make({ message: "a bind came before its session's cap" })
                      : yield* provenBind({
                          sessionId: bound,
                          sessionSeconds: created.cap,
                          granted: grant.granted,
                        });
                yield* acceptGrant({ check: "tour", granted });
                if (granted.maxSessionSeconds === "unlimited")
                  return yield* Refused.make({ message: "the tour requires a capped session" });
                return { ...granted, maxSessionSeconds: granted.maxSessionSeconds };
              }),
            );
            const named = grant.granted?.bound ?? [];
            const atMs = yield* run.now;
            yield* record((tour) => ({
              ...tour,
              mints: [
                ...tour.mints,
                {
                  sentMs: round(sentAt - run.origin),
                  atMs,
                  kind: bound === undefined ? "create" : "bind",
                  lifetimeSeconds: round(grant.expiresAt - sentAt / 1000),
                  echoed: grant.granted !== undefined,
                  bound: named.length,
                  ownSession: bound !== undefined && named.length === 1 && named[0] === bound,
                  ...(Result.isFailure(proven) ? { refused: proven.failure.message } : {}),
                },
              ],
            }));
            if (Result.isFailure(proven)) return yield* proven.failure;
            const granted = proven.success;
            if (bound === undefined) created = { grant, sentAt, cap: granted.maxSessionSeconds };
            else grants.set(bound, grant);
            latest = grant;
            yield* run.update((evidence) => ({
              ...evidence,
              grants: [...evidence.grants, { ...granted, expiresAt: grant.expiresAt }],
            }));
          },
          Effect.mapError((error) => ReactorError.fromCode("InvalidState", error.message)),
        );
        const sessionTokens: CoordinatorClient.Tokens = {
          create: Effect.gen(function* () {
            const sentAt = yield* Clock.currentTimeMillis;
            const grant = yield* tokens.create;
            yield* admit(grant, sentAt, undefined);
            return grant;
          }),
          bind: (id) =>
            Effect.gen(function* () {
              const sentAt = yield* Clock.currentTimeMillis;
              const grant = yield* tokens.bind(id);
              yield* admit(grant, sentAt, id);
              return grant;
            }),
        };

        // 1. The session, created on the SDK's tokens rather than a fixed grant.
        const creation = yield* phase(
          "creation",
          30,
          Effect.gen(function* () {
            const session = yield* recorded(
              reactor.create({
                model: run.model.name,
                tokens: sessionTokens,
                onAllocated: (allocated) =>
                  Effect.gen(function* () {
                    yield* pieces
                      .recordSessionEvents(allocated)
                      .pipe(Effect.forkScoped({ startImmediately: true }));
                    if (created === undefined)
                      return yield* ReactorError.fromCode(
                        "InvalidState",
                        "the session was allocated on no recorded token",
                      );
                    deadline = yield* pieces.allocated(allocated.id, created.grant);
                    grants.set(allocated.id, created.grant);
                  }),
              }),
            );
            yield* run.mark("connected");
            return session;
          }),
        );
        if (Option.isNone(creation) || created === undefined) return;
        const session = creation.value;
        const create = created;
        const createExpiresMs = create.grant.expiresAt * 1000 - run.origin;
        // The session's token module refreshes a quarter of a short token's life before it ends.
        const refreshDueMs = round(
          createExpiresMs - Math.min(60_000, (create.grant.expiresAt * 1000 - create.sentAt) / 4),
        );
        yield* record((tour) => ({ ...tour, createExpiresMs, refreshDueMs }));
        const grant = () => latest ?? create.grant;

        // What the session publishes: its uploads, its picture and sound, its statistics.
        const uploads = yield* SubscriptionRef.make<ReadonlyArray<Session.UploadReference>>([]);
        yield* session.events({ capacity: 256 }).pipe(
          Stream.runForEach((event) => {
            if (event._tag !== "Upload") return Effect.void;
            const file = event.progress.file;
            return event.progress.notification === "submitted" && file !== undefined
              ? SubscriptionRef.update(uploads, (all) => [...all, file])
              : Effect.void;
          }),
          Effect.ignore,
          Effect.forkScoped,
        );
        /** When the refresh's own upload ran, from its start to its end. */
        let refreshWindow: { readonly startedMs: number; readonly endedMs?: number } | undefined;
        const byRefresh = (atMs: number) =>
          refreshWindow !== undefined &&
          atMs >= refreshWindow.startedMs &&
          atMs <= (refreshWindow.endedMs ?? Number.POSITIVE_INFINITY);
        /** How many uploads the session made for its clips from `fromMs` to `toMs`. */
        const uploadsBetween = (fromMs: number, toMs: number) =>
          Effect.map(
            run.evidence,
            (evidence) =>
              evidence.spans.filter(
                (span) =>
                  span.name === "Session.upload" &&
                  span.startMs >= fromMs &&
                  span.startMs <= toMs &&
                  !byRefresh(span.startMs),
              ).length,
          );
        const video = Media.videoLog();
        const audio = Media.audioLog();
        const windows = new Set<Window>();
        /** Media from now until it is closed, in logs of its own. */
        const openWindow = (): Window => {
          const opened = { video: Media.videoLog(), audio: Media.audioLog() };
          windows.add(opened);
          return opened;
        };
        const videoFeed = {
          add: (element: Recorded<VideoFrame>, atMs: number) => {
            video.add(element, atMs);
            for (const open of windows) open.video.add(element, atMs);
          },
        };
        const audioFeed = {
          add: (element: Recorded<AudioFrame>) => {
            audio.add(element);
            for (const open of windows) open.audio.add(element);
          },
        };
        let media: DecodedMedia | undefined;
        let audioOffered = false;
        /** Reads the current generation's picture and sound into the logs until it retires. */
        const listen = Effect.gen(function* () {
          const decoded = yield* session.decoded;
          media = decoded;
          audioOffered = decoded.tracks.some(
            (track) => track.kind === "audio" && track.direction === "recvonly",
          );
          yield* pieces.readInto(decoded.video(tracks.video), videoFeed).pipe(Effect.forkScoped);
          if (audioOffered)
            yield* pieces.readInto(decoded.audio(tracks.audio), audioFeed).pipe(Effect.forkScoped);
        });
        const samples: Array<Evidence.StatsSample> = [];
        yield* pieces.sampleStats(session, samples).pipe(Effect.forkScoped);
        let provider: Family.Provider<Req, Clip> | undefined;
        const tally = pieces.contractTally();
        let clip1StartedMs: number | undefined;
        /** The media, network and contract sections, as they stand. */
        const recordSections = Effect.gen(function* () {
          const pressure =
            media === undefined ? Option.none() : yield* media.pressure.pipe(Effect.option);
          const paired = samples.findLast(
            (sample) => sample.local !== undefined && (sample.receivedKbps ?? 0) > 0,
          );
          const firstClipFrameMs =
            clip1StartedMs === undefined ? undefined : video.firstAfter(clip1StartedMs);
          const commands = pieces.commandsSince(yield* run.evidence, 0);
          yield* run.update((evidence) => ({
            ...evidence,
            ...(provider === undefined ? {} : { contract: tally.summary(provider) }),
            media: {
              audioOffered,
              ...(firstClipFrameMs === undefined ? {} : { firstClipFrameMs }),
              video: video.summary(),
              ...(audioOffered ? { audio: audio.summary() } : {}),
              // Once the session closed, its media reads nothing: the last reading stays.
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
                : evidence.media?.pressure === undefined
                  ? {}
                  : { pressure: evidence.media.pressure }),
            },
            network: {
              samples: [...samples],
              ...(paired?.local === undefined ? {} : { pair: paired.local }),
            },
          }));
          yield* record((tour) => ({ ...tour, commands }));
        });
        // The phase that ends the session records its end; this closes whatever is left.
        let closeRecorded = false;
        yield* Effect.addFinalizer(() =>
          Effect.ignore(
            Effect.gen(function* () {
              yield* recordSections;
              const requestedMs = yield* run.now;
              const report = yield* session.close;
              if (!closeRecorded && report.remote.confirmed)
                yield* pieces.closedWith(session.id, requestedMs, { report });
            }),
          ),
        );
        yield* listen;

        // The refresh, on its own from here: past the refresh point the session's next call on
        // its token mints one bound to itself, whatever the phases have reached. An upload is
        // such a call, and touches nothing H3 holds.
        const refreshing = yield* Effect.gen(function* () {
          yield* pieces.sleepUntil(refreshDueMs + 250, deadline);
          const startedMs = yield* run.now;
          refreshWindow = { startedMs };
          const upload = yield* session
            .upload(refreshName, "image/png", Media.grayPng({ width: 320, height: 180 }))
            .pipe(recorded, Effect.result);
          const endedMs = yield* run.now;
          refreshWindow = { startedMs, endedMs };
          const call = {
            what: "an upload",
            startedMs,
            endedMs,
            ok: Result.isSuccess(upload),
            ...(Result.isFailure(upload) ? { failure: reasonOf(upload.failure) } : {}),
          };
          yield* record((tour) => ({ ...tour, refreshCall: call }));
          return call;
        }).pipe(Effect.forkScoped);

        // H3's clip lifecycle as it arrives, and the reasons clips failed with, by length only.
        const observed = yield* SubscriptionRef.make<ReadonlyArray<Seen>>([]);
        const failedReasons = new Map<string, number>();
        const observe = (event: H3.ProviderEvent) =>
          Effect.gen(function* () {
            tally.add(event);
            if (event._tag !== "Message" || event.disposition !== "applied") return;
            const message = event.message;
            if (message.type === "unknown" || !("clip" in message.data)) return;
            const clipId = message.data.clip.clip_id;
            if (message.type === "clip_failed")
              failedReasons.set(clipId, message.data.reason.length);
            const atMs = yield* run.now;
            yield* SubscriptionRef.update(observed, (all) =>
              all.length >= 1024 ? all : [...all, { type: message.type, clipId, atMs }],
            );
          });
        const seen = (type: string, clipId: string) =>
          pieces.waitFor(
            observed,
            (all) => all.find((entry) => entry.type === type && entry.clipId === clipId),
            deadline,
          );
        const lifecycleOf = (clipId: string, fromMs = 0) =>
          Effect.map(SubscriptionRef.get(observed), (all) =>
            all.filter((entry) => entry.clipId === clipId && entry.atMs >= fromMs),
          );
        const startsBetween = (fromMs: number, toMs: number) =>
          Effect.map(SubscriptionRef.get(observed), (all) =>
            all
              .filter(
                (entry) =>
                  entry.type === "clip_started" && entry.atMs >= fromMs && entry.atMs <= toMs,
              )
              .map((entry) => nameOf(entry.clipId)),
          );

        // 2. H3 over the session: its schema, state and queue, and what its messages show.
        const setup = yield* phase(
          "H3 setup",
          20,
          Effect.gen(function* () {
            const made = yield* family.provider(session);
            yield* made.events({ capacity: 4096 }).pipe(
              Stream.runForEach(observe),
              Effect.catch((error) => Effect.sync(() => tally.failed(error))),
              Effect.forkScoped,
            );
            provider = made;
            const missing = sent.filter((name) => !made.contract.commands.has(name));
            yield* record((tour) => ({ ...tour, missingCommands: missing }));
            yield* pieces.judge("the deployment offers every command the tour sends", [
              missing.length === 0,
              `it does not offer ${missing.join(", ")}`,
            ]);
            // Where the session runs, when the coordinator says: a read that fails is noted only.
            const inspected = yield* pieces.withToken(create.grant.jwt).pipe(
              Effect.flatMap((coordinator) => coordinator.inspect(session.id)),
              Effect.result,
            );
            if (Result.isFailure(inspected))
              yield* run.mark("server unread", reasonOf(inspected.failure));
            else {
              const inspection = inspected.success;
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
            }
            yield* run.mark("H3 ready");
            return made;
          }),
        );
        if (Option.isNone(setup)) return;
        const model = setup.value;
        const facts = Effect.map(model.snapshot, pieces.factsOf);
        /** H3's state and queue as its replies give them now, ahead of its next messages. */
        const fresh = Effect.all([recorded(model.getState), recorded(model.getQueue)]).pipe(
          Effect.map(([state, queue]): H3.Facts => ({ state: state.value, queue: queue.value })),
        );

        /** Enqueues a clip under `name`, which queue reads then use for it. */
        const submit = Effect.fnUntraced(function* (
          name: string,
          request: Partial<Family.RequestInput> | Req = {},
        ) {
          const submission = yield* model.prepare({
            ...family.request({
              prompt,
              seconds: family.lengths.short,
              metadata: `${marker}:${name}`,
            }),
            ...request,
          });
          const submitMs = yield* run.now;
          const acceptance = yield* recorded(submission.submit);
          const acceptedMs = yield* run.now;
          names.set(acceptance.clip.clip_id, name);
          return {
            name,
            submission,
            acceptance,
            clipId: acceptance.clip.clip_id,
            submitMs,
            acceptedMs,
          } satisfies Submitted<Clip>;
        });
        /** A submitted clip's operation, and its lifecycle recorded as the operation reports it. */
        const follow = Effect.fnUntraced(function* (submitted: Submitted<Clip>) {
          const operation = yield* model.operation(submitted.submission);
          const lifecycle: {
            generatedMs?: number;
            startedMs?: number;
            endedMs?: number;
            ended?: string;
          } = {};
          const at = (key: "generatedMs" | "startedMs" | "endedMs") =>
            Effect.flatMap(run.now, (atMs) =>
              Effect.sync(() => {
                lifecycle[key] ??= atMs;
              }),
            );
          yield* operation
            .reached("generated")
            .pipe(Effect.andThen(at("generatedMs")), Effect.ignore, Effect.forkScoped);
          yield* operation
            .reached("started")
            .pipe(Effect.andThen(at("startedMs")), Effect.ignore, Effect.forkScoped);
          yield* operation.ended.pipe(
            Effect.tap((fact) =>
              Effect.sync(() => {
                lifecycle.ended = fact.message;
              }),
            ),
            Effect.andThen(at("endedMs")),
            Effect.ignore,
            Effect.forkScoped,
          );
          return { operation, lifecycle };
        });
        const clipRecord = (
          submitted: Submitted<Clip>,
          lifecycle: {
            readonly generatedMs?: number;
            readonly startedMs?: number;
            readonly endedMs?: number;
            readonly ended?: string;
          },
          references: ReturnType<typeof referencesOf>,
        ) => ({
          clipId: submitted.clipId,
          acceptance: submitted.acceptance.evidence.kind,
          submitMs: submitted.submitMs,
          acceptedMs: submitted.acceptedMs,
          ...lifecycle,
          references,
        });

        // 3. A canvas other than the session's, from those H3's schema allows, before any clip.
        yield* phase(
          "canvas",
          10,
          Effect.gen(function* () {
            const state = (yield* facts)?.state;
            const listed = state?.valid_commands.includes("set_canvas") === true;
            const choice = family.canvases.find((canvas) => canvas.aspect !== state?.aspect);
            if (choice === undefined)
              return yield* ReactorError.fromCode("InvalidState", "H3 documents one canvas");
            const reply = (yield* recorded(model.setCanvas(choice.aspect))).value;
            const after = (yield* facts)?.state;
            yield* record((tour) => ({
              ...tour,
              canvas: {
                requested: choice.aspect,
                listed,
                reply: { aspect: reply.aspect, width: reply.width, height: reply.height },
                ...(after === undefined
                  ? {}
                  : {
                      state: { aspect: after.aspect, width: after.width, height: after.height },
                    }),
              },
            }));
            yield* pieces.judge("canvas_accepted names the canvas asked for at its size", [
              reply.aspect === choice.aspect &&
                reply.width === choice.width &&
                reply.height === choice.height,
              `asked for ${choice.aspect}, accepted ${reply.aspect} at ${reply.width}x${reply.height}`,
            ]);
            yield* pieces.judge("the state reports the new canvas", [
              after?.aspect === choice.aspect &&
                after.width === choice.width &&
                after.height === choice.height,
              `the state reports ${after?.aspect ?? "no canvas"}`,
            ]);
            yield* run.mark("canvas set", choice.aspect);
          }),
        );

        // 4. Clip 1: a seed, and a reference image and reference audio as bytes, which upload.
        const clip1 = yield* phase(
          "clip 1",
          25,
          Effect.gen(function* () {
            yield* recorded(model.setAutoplay(true));
            const defaultBefore = (yield* facts)?.state.seed ?? null;
            const known = (yield* SubscriptionRef.get(uploads)).length;
            const fromMs = yield* run.now;
            const submitted = yield* submit("clip 1", {
              ...family.withUploads(`${marker}:clip 1`),
              seed,
            });
            tally.watch(submitted.clipId, `${marker}:clip 1`);
            const { operation, lifecycle } = yield* follow(submitted);
            // Its own window: from its start until it ends, or the media window closes. A clip
            // that fails before it starts fails the window too.
            const closed = yield* Deferred.make<Window, ReactorError | CommandFailure>();
            yield* Effect.gen(function* () {
              yield* operation.reached("started");
              clip1StartedMs = yield* run.now;
              const window = openWindow();
              yield* operation.ended.pipe(
                Effect.timeout(yield* pieces.window(deadline)),
                Effect.ignore,
              );
              windows.delete(window);
              return window;
            }).pipe(Deferred.into(closed), Effect.forkScoped);
            const files = yield* pieces.waitFor(
              uploads,
              (all) => {
                const made = all.slice(known).filter((file) => file.name !== refreshName);
                return made.length >= family.uploadCounts.images + family.uploadCounts.audio
                  ? made
                  : undefined;
              },
              deadline,
            );
            const defaultAfter = (yield* facts)?.state.seed ?? null;
            const made = yield* uploadsBetween(fromMs, submitted.acceptedMs);
            const clip = submitted.acceptance.clip;
            /** What the clip shows; its playback fills in the rest. */
            const write = (window: Window | undefined) =>
              record((tour) => ({
                ...tour,
                clip1: {
                  ...clipRecord(
                    submitted,
                    lifecycle,
                    referencesOf(family, clip, { ...family.uploadCounts, uploads: made }),
                  ),
                  seed: { sent: seed, echoed: clip.seed, defaultBefore, defaultAfter },
                  echoes: tally.echoes,
                  ...(window === undefined
                    ? {}
                    : {
                        video: window.video.summary(),
                        ...(audioOffered ? { audio: window.audio.summary() } : {}),
                      }),
                },
              }));
            yield* write(undefined);
            yield* run.mark("clip 1 accepted", submitted.acceptance.evidence.kind);
            return { submitted, files, closed, write, lifecycle };
          }),
        );

        // 5. Clip 2 reuses clip 1's uploads as `Uploaded` references: nothing uploads again.
        const clip2 = yield* phase(
          "clip 2",
          10,
          Effect.gen(function* () {
            if (Option.isNone(clip1))
              return yield* ReactorError.fromCode("InvalidState", "clip 1 left no uploads");
            const files = clip1.value.files;
            const reused = yield* family.withUploaded(`${marker}:clip 2`, files);
            const fromMs = yield* run.now;
            const submitted = yield* submit("clip 2", reused);
            const made = yield* uploadsBetween(fromMs, yield* run.now);
            const { operation, lifecycle } = yield* follow(submitted);
            const clip = submitted.acceptance.clip;
            const references = referencesOf(family, clip, {
              ...family.uploadCounts,
              uploads: made,
            });
            /** What the clip shows, its lifecycle as far as it has gone. */
            const write = record((tour) => ({
              ...tour,
              clip2: clipRecord(submitted, lifecycle, references),
            }));
            yield* write;
            yield* pieces.judge(
              "clip 2 is accepted on clip 1's uploads",
              [
                submitted.acceptance.evidence.kind === "correlated",
                `acceptance was only by ${submitted.acceptance.evidence.kind}`,
              ],
              [made === 0, `${made} upload(s) were made again`],
            );
            if (family.uploadsOf(clip)._tag === "References")
              yield* pieces.judge("clip 2 reports its reused references", [
                references.reportedImages === 1 && references.reportedAudio === 1,
                `the clip reports ${String(references.reportedImages ?? "no")} image and ${String(references.reportedAudio ?? "no")} audio reference(s) for one of each`,
              ]);
            else
              yield* pieces.judge("clip 2 reports its reused starting frame", [
                references.hasStartingFrame === true,
                "the clip does not report its reused starting frame",
              ]);
            yield* run.mark("clip 2 accepted");
            return { submitted, operation, lifecycle, write };
          }),
        );

        // 6. H3's queue by hand: position zero, a move, and pops of a waiting clip and of the
        // build in flight. Clip 1 builds meanwhile, so the moved clip builds next.
        const replies: Record<string, number> = {};
        /** Sends `command`, keeping its round trip under `name`. */
        const timed = <A, E, R>(name: string, command: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const sentMs = yield* run.now;
            const value = yield* recorded(command);
            replies[name] = round((yield* run.now) - sentMs);
            return value;
          });
        const generationNames = Effect.map(timed("get_queue", model.getQueue), (reply) =>
          reply.value.generation.map((clip) => nameOf(clip.clip_id)),
        );
        const popped: Array<{
          readonly name: string;
          readonly clipId: string;
          readonly repliedMs: number;
        }> = [];
        yield* phase(
          "queue",
          20,
          Effect.gen(function* () {
            type QueueRecord = NonNullable<TourRecord["queue"]>;
            let queue: QueueRecord = { enqueued: [], pops: [], replies };
            const save = (change: Partial<QueueRecord>) =>
              Effect.suspend(() => {
                queue = { ...queue, ...change, replies: { ...replies } };
                return record((tour) => ({ ...tour, queue }));
              });
            const zero = yield* submit("position zero", { position: 0 });
            const moved = yield* submit("moved");
            yield* save({ enqueued: yield* generationNames });
            const move = (yield* timed("move", model.move(moved.clipId, 0))).value;
            const movedMs = yield* run.now;
            const afterMove = yield* generationNames;
            yield* save({
              move: { queue: move.queue, position: move.position, generation: afterMove },
            });
            // The moved clip is listed ahead of each clip it was behind that is still listed.
            const ahead = (name: string) =>
              afterMove.includes("moved") &&
              (!afterMove.includes(name) || afterMove.indexOf("moved") < afterMove.indexOf(name));
            yield* pieces.judge("the queue read reflects the move", [
              move.queue === "generation" && ahead("position zero") && ahead("clip 2"),
              `after moving it to the front, the generation queue read ${afterMove.join(", ")}`,
            ]);
            const pop = Effect.fnUntraced(function* (
              clip: Submitted<Clip>,
              headOfGeneration: boolean,
            ) {
              const sentMs = yield* run.now;
              yield* timed(`pop ${clip.name}`, model.pop(clip.clipId));
              const repliedMs = yield* run.now;
              popped.push({ name: clip.name, clipId: clip.clipId, repliedMs });
              yield* save({
                pops: [...queue.pops, { name: clip.name, headOfGeneration, sentMs, repliedMs }],
              });
            });
            yield* pop(zero, false);
            // The build running when the clip moved finishes, and H3 builds the front clip next.
            yield* pieces.waitFor(
              observed,
              (all) =>
                all.find((entry) => entry.type === "clip_generated" && entry.atMs >= movedMs),
              deadline,
            );
            const head = (yield* timed("get_queue", model.getQueue)).value.generation[0]?.clip_id;
            yield* pop(moved, head === moved.clipId);
            yield* run.mark("queue edited");
            const after = yield* timed("get_queue", model.getQueue);
            const state = yield* timed("get_state", model.getState);
            // A refresh fails when H3's snapshots change between its two reads: kept, not judged.
            const refreshed = yield* Effect.result(timed("refresh", model.refresh));
            const refresh = Result.isSuccess(refreshed) ? "refreshed" : reasonOf(refreshed.failure);
            yield* save({
              after: {
                generation: after.value.generation.map((clip) => nameOf(clip.clip_id)),
                playout: after.value.playout.map((clip) => nameOf(clip.clip_id)),
                generationQueued: state.value.generation_queued,
                playoutQueued: state.value.playout_queued,
                refresh,
              },
            });
          }),
        );

        // 7. A clip whose prompt is past H3's text budget, about 16 KB of it.
        const over = yield* phase(
          "over budget",
          10,
          Effect.gen(function* () {
            if (family.key === "fast-h3") {
              const submission = yield* model.prepare(
                family.request({
                  prompt: overBudget,
                  seconds: family.lengths.short,
                  metadata: `${marker}:over budget`,
                }),
              );
              const submitMs = yield* run.now;
              const answer = yield* submission.submit.pipe(recorded, Effect.result);
              const endedMs = yield* run.now;
              const failure = Result.isFailure(answer) ? answer.failure : undefined;
              yield* record((tour) => ({
                ...tour,
                failedBuild: {
                  promptChars: overBudget.length,
                  submitMs,
                  endedMs,
                  ...(Result.isSuccess(answer)
                    ? { acceptedMs: endedMs, acceptance: answer.success.evidence.kind }
                    : { generatedFailure: reasonOf(answer.failure) }),
                  started: false,
                },
              }));
              yield* pieces.judge("a prompt past the text budget is refused at enqueue", [
                failure !== undefined &&
                  isReactorFailure(failure) &&
                  failure.reason._tag === "Remote" &&
                  failure.context.outcome === "replied",
                failure === undefined
                  ? "the enqueue was accepted"
                  : `the enqueue failed with ${reasonOf(failure)} and outcome ${failure.context.outcome}`,
              ]);
              yield* run.mark("over budget refused at enqueue");
              return;
            }
            const submitted = yield* submit("over budget", { prompt: overBudget });
            const followed = yield* follow(submitted);
            // What waiting for it to generate ends with: nothing, or the failure's reason.
            const generatedFailure = yield* Deferred.make<ReactorErrorReason | undefined>();
            yield* followed.operation.reached("generated").pipe(
              Effect.as(undefined),
              Effect.catch((error) => Effect.succeed(error.reason)),
              Effect.flatMap((reason) => Deferred.succeed(generatedFailure, reason)),
              Effect.forkScoped,
            );
            yield* run.mark("over budget accepted");
            return { submitted, ...followed, generatedFailure };
          }),
        );

        // 8. A clip to play by hand once autoplay is off.
        const byHand = yield* phase(
          "by hand",
          10,
          Effect.gen(function* () {
            const submitted = yield* submit("by hand");
            const followed = yield* follow(submitted);
            yield* run.mark("by hand accepted");
            return { submitted, ...followed };
          }),
        );

        // Clip 1 on air: the SDK's lifecycle, its picture and sound, and its metadata echoed.
        yield* phase(
          "clip 1 on air",
          25,
          Effect.gen(function* () {
            if (Option.isNone(clip1))
              return yield* ReactorError.fromCode("InvalidState", "clip 1 was not accepted");
            const { submitted, closed, write, lifecycle } = clip1.value;
            const window = yield* Deferred.await(closed);
            yield* write(window);
            const clip = submitted.acceptance.clip;
            yield* pieces.judge("clip 1: correlated acceptance", [
              submitted.acceptance.evidence.kind === "correlated",
              `acceptance was only by ${submitted.acceptance.evidence.kind}`,
            ]);
            yield* pieces.judge("clip 1: lifecycle progression", [
              lifecycle.generatedMs !== undefined && lifecycle.startedMs !== undefined,
              "generated and started were not both observed",
            ]);
            // Started is a provider fact: idle frames can still arrive before the media updates.
            yield* run.judge("clip 1: live video", window.video.live(lifecycle.startedMs ?? 0));
            yield* pieces.judge("clip 1: audio when offered", [
              !audioOffered ||
                (window.audio.summary().blocks > 0 && window.audio.summary().peakRms > 0),
              "the session offered audio and no sound arrived",
            ]);
            const uploadFacts = family.uploadsOf(clip);
            if (uploadFacts._tag === "References")
              yield* pieces.judge("clip 1: reference audio reported", [
                clip.has_reference_audio === true && clip.reference_audio_count === 1,
                `the clip reports has_reference_audio ${String(clip.has_reference_audio ?? "absent")} and ${String(clip.reference_audio_count ?? "no")} audio reference(s) for 1 sent`,
              ]);
            else
              yield* pieces.judge("clip 1: starting frame reported", [
                uploadFacts.hasStartingFrame,
                "the clip does not report the starting frame sent as bytes",
              ]);
            yield* pieces.judge("clip 1: metadata preserved", [
              Object.keys(tally.echoes).some((type) => type !== "clip_queued"),
              "no provider message after the reply listed the clip with its metadata",
            ]);
            yield* run.mark("clip 1 aired");
          }),
        );

        // 9. Stop and play: autoplay off while clip 2 plays, a stop, then the clip by hand.
        yield* phase(
          "stop and play",
          20,
          Effect.gen(function* () {
            if (Option.isNone(clip2) || Option.isNone(byHand))
              return yield* ReactorError.fromCode(
                "InvalidState",
                "clip 2 or the clip by hand is missing",
              );
            const hand = byHand.value;
            // A clip 2 that fails first ends the wait with ClipEnded.
            yield* clip2.value.operation.reached("started");
            yield* recorded(model.setAutoplay(false));
            yield* hand.operation.reached("generated");
            // What plays now, from H3's reply rather than its last messages.
            const playing = (yield* recorded(model.getState)).value.playing_clip_id;
            const stopSentMs = yield* run.now;
            const stop = yield* recorded(model.stop);
            const stopped = playing === null ? undefined : yield* seen("clip_stopped", playing);
            // Autoplay is off, so nothing may start before the play: half a second to see that.
            yield* Effect.sleep("500 millis");
            const playSentMs = yield* run.now;
            const startedBetween = yield* startsBetween(stopSentMs, playSentMs);
            const play = yield* recorded(model.play(hand.submitted.clipId));
            const started = yield* seen("clip_started", hand.submitted.clipId);
            yield* record((tour) => ({
              ...tour,
              stopPlay: {
                stopped: playing === null ? "nothing" : nameOf(playing),
                stopSentMs,
                stop: stop._tag,
                ...(stopped === undefined ? {} : { stoppedMs: stopped.atMs }),
                startedBetween,
                played: hand.submitted.name,
                playSentMs,
                play: play._tag,
                playStartedMs: started.atMs,
              },
            }));
            yield* pieces.judge(
              "stop cuts the playing clip",
              [
                playing === clip2.value.submitted.clipId,
                `${playing === null ? "nothing" : nameOf(playing)} was playing, not clip 2`,
              ],
              [
                stop._tag === "Acknowledged",
                "stop answered with a payload, not an acknowledgement",
              ],
              [stopped !== undefined, "no clip_stopped followed"],
            );
            yield* pieces.judge("nothing starts after a stop with autoplay off", [
              startedBetween.length === 0,
              `${startedBetween.join(", ")} started`,
            ]);
            yield* pieces.judge("play starts the clip it names", [
              play._tag === "Acknowledged",
              "play answered with a payload, not an acknowledgement",
            ]);
            yield* run.mark("stopped and played");
          }),
        );

        // 10. Reset while the clip by hand plays.
        yield* phase(
          "reset",
          10,
          Effect.gen(function* () {
            const read = yield* fresh;
            const before = settingsOf(read, nameOf);
            const playing = read.state.playing_clip_id;
            const sentMs = yield* run.now;
            const reply = (yield* recorded(model.reset)).value;
            const stopped =
              playing === null
                ? Option.none()
                : yield* seen("clip_stopped", playing).pipe(
                    Effect.timeout("3 seconds"),
                    Effect.option,
                  );
            const after = settingsOf(yield* fresh, nameOf);
            yield* record((tour) => ({
              ...tour,
              reset: {
                before,
                sentMs,
                clearedClips: reply.cleared_clips,
                wasPlaying: reply.was_playing,
                ...(Option.isSome(stopped) ? { stoppedMs: stopped.value.atMs } : {}),
                after,
              },
            }));
            yield* pieces.judge(
              "reset stops the playing clip",
              [playing !== null, "nothing was playing when the session reset"],
              [reply.was_playing, "session_reset says nothing was playing"],
              [Option.isSome(stopped), "no clip_stopped followed"],
            );
            yield* pieces.judge("reset leaves both queues empty and nothing playing", [
              after.queued === 0 && !after.playing,
              `after the reset ${after.queued} clip(s) were queued and ${after.playingClip ?? "nothing"} playing`,
            ]);
            yield* run.mark("reset");
          }),
        );

        // 11. The build that fails: H3 documents `clip_failed` past its text budget. It failed
        // long before now, so the wait is short.
        if (family.key === "h3")
          yield* phase(
            "a build that fails",
            5,
            Effect.gen(function* () {
              if (Option.isNone(over) || over.value === undefined)
                return yield* ReactorError.fromCode("InvalidState", "the clip was not accepted");
              const { submitted, operation, lifecycle, generatedFailure } = over.value;
              const ended = yield* operation.ended;
              const reason = yield* Deferred.await(generatedFailure);
              const clipEnded = reason?._tag === "ClipEnded" ? reason : undefined;
              const started = (yield* lifecycleOf(submitted.clipId)).some(
                (entry) => entry.type === "clip_started",
              );
              const reasonChars = failedReasons.get(submitted.clipId);
              yield* record((tour) => ({
                ...tour,
                failedBuild: {
                  promptChars: overBudget.length,
                  submitMs: submitted.submitMs,
                  acceptedMs: submitted.acceptedMs,
                  acceptance: submitted.acceptance.evidence.kind,
                  ...(lifecycle.endedMs === undefined ? {} : { endedMs: lifecycle.endedMs }),
                  ended: ended.message,
                  ...(reason === undefined ? {} : { generatedFailure: reason._tag }),
                  ...(clipEnded === undefined
                    ? {}
                    : {
                        clipEnded: {
                          lifecycle: clipEnded.lifecycle,
                          sameClip: clipEnded.clipId === submitted.clipId,
                          transportGeneration: String(clipEnded.transportGeneration),
                        },
                      }),
                  ...(reasonChars === undefined ? {} : { reasonChars }),
                  started,
                },
              }));
              yield* pieces.judge(
                "a prompt past H3's text budget fails its clip",
                [ended.message === "clip_failed", `the clip ended by ${ended.message}`],
                [!started, "the clip started"],
              );
              yield* pieces.judge("the operation reports the failure as ClipEnded", [
                clipEnded?.lifecycle === "clip_failed" && clipEnded.clipId === submitted.clipId,
                `waiting for it to generate failed with ${reason?._tag ?? "nothing"}`,
              ]);
              yield* run.mark("build failed as documented");
            }),
          );

        // 12. A 10 s clip for the reconnect to play, built now and held: autoplay goes off
        // first, whatever the phases before left it at.
        const long = yield* phase(
          "long clip",
          10,
          Effect.gen(function* () {
            yield* recorded(model.setAutoplay(false));
            const submitted = yield* submit("long", { seconds: longSeconds });
            const followed = yield* follow(submitted);
            yield* run.mark("long clip accepted");
            return { submitted, ...followed };
          }),
        );

        // 13. Recordings, which Reactor leaves to each deployment: recorded, never judged. Each
        // request and the download end by the creating token's expiry, so the reconnect after
        // it keeps its time, and running out of time is recorded too.
        const recordingEndMs = createExpiresMs;
        /** `effect`'s result, or `TimeoutError` once the recordings' time is up. */
        const inTime = <A, E extends { readonly reason: { readonly _tag: string } }>(
          effect: Effect.Effect<A, E>,
        ) =>
          Effect.flatMap(run.now, (now) =>
            effect.pipe(
              Effect.map((value) => Result.succeed(value)),
              Effect.catch((error) => Effect.succeed(Result.fail(error.reason._tag))),
              Effect.timeoutOrElse({
                duration: Duration.millis(Math.max(0, recordingEndMs - now)),
                orElse: () => Effect.succeed(Result.fail("TimeoutError")),
              }),
            ),
          );
        yield* phase(
          "recording",
          Math.max(1, (recordingEndMs + 500 - (yield* run.now)) / 1000),
          Effect.gen(function* () {
            const recordings: Array<NonNullable<TourRecord["recordings"]>[number]> = [];
            const save = record((tour) => ({ ...tour, recordings: [...recordings] }));
            const requests = [
              { request: "clip", ask: session.requestRecordingClip(snapSeconds) },
              { request: "recording", ask: session.recording },
            ] as const;
            for (const { request, ask } of requests) {
              const reply = yield* inTime(ask);
              if (Result.isFailure(reply)) {
                recordings.push({ request, outcome: reply.failure });
                yield* save;
                continue;
              }
              const clip = reply.success;
              const now = yield* Clock.currentTimeMillis;
              const entry = {
                request,
                outcome: "ClipReady",
                kind: clip.kind,
                markers: clip.endMarker - clip.startMarker,
                readyInMs: round(Number(clip.predictedReadyAtMs) - now),
              };
              recordings.push(entry);
              yield* save;
              // The clip asked for is short; the whole recording is left where it is.
              if (request !== "clip") continue;
              const startedMs = yield* run.now;
              const download = yield* inTime(
                (yield* pieces.withToken(grant().jwt)).downloadClip(clip),
              );
              const ms = round((yield* run.now) - startedMs);
              recordings[recordings.length - 1] = {
                ...entry,
                download: Result.isFailure(download)
                  ? { outcome: download.failure, ms }
                  : {
                      outcome: "downloaded",
                      ms,
                      bytes: download.success.bytes.byteLength,
                      segments: download.success.segments.length,
                      init: download.success.segments.some((segment) => segment.kind === "init"),
                    },
              };
              yield* save;
            }
            yield* run.mark("recordings asked for");
          }),
        );

        // 14. The refresh, judged once its upload is back: a token bound to the session, minted
        // before the creating one expired, and the call it carried answered.
        yield* phase(
          "refresh",
          Math.max(1, (createExpiresMs + afterExpiryMs - (yield* run.now)) / 1000),
          Effect.gen(function* () {
            const call = yield* Fiber.join(refreshing);
            const mints = (yield* run.evidence).tour?.mints ?? [];
            const refreshed = mints.find(
              (mint) => mint.kind === "bind" && mint.ownSession && mint.refused === undefined,
            );
            yield* pieces.judge(
              "the session refreshes to a token bound to itself before its token expires",
              [refreshed !== undefined, "no token bound to the session was accepted"],
              [
                refreshed !== undefined && refreshed.atMs < createExpiresMs,
                `the bound token came ${round(((refreshed?.atMs ?? 0) - createExpiresMs) / 1000)} s after the creating token expired`,
              ],
              [call.ok, `the upload at the refresh point failed with ${call.failure ?? "?"}`],
            );
            yield* run.mark("token refreshed");
          }),
        );
        yield* Fiber.interrupt(refreshing);

        // 15. Once the creating token expired, a reconnect: a new generation on the refreshed token.
        yield* pieces.sleepUntil(createExpiresMs + afterExpiryMs, deadline);
        yield* phase(
          "reconnect",
          20,
          Effect.gen(function* () {
            const generationBefore = (yield* session.ready).generation;
            const startedMs = yield* run.now;
            const reconnected = yield* Effect.exit(session.reconnect);
            const readyMs = yield* run.now;
            yield* record((tour) => ({
              ...tour,
              afterExpiryCall: {
                what: "reconnect",
                startedMs,
                endedMs: readyMs,
                ok: Exit.isSuccess(reconnected),
                ...(Exit.isFailure(reconnected)
                  ? { failure: reconnected.cause.pipe(Cause.squash, reasonOf) }
                  : {}),
              },
              reconnect: { startedMs, generationBefore: String(generationBefore) },
            }));
            yield* pieces.judge("a call after the creating token expired succeeds", [
              Exit.isSuccess(reconnected) && startedMs > createExpiresMs,
              Exit.isSuccess(reconnected)
                ? "the reconnect began before the creating token expired"
                : `the reconnect failed: ${failureOf(reconnected.cause)}`,
            ]);
            if (Exit.isFailure(reconnected)) return yield* Effect.failCause(reconnected.cause);
            const generationAfter = (yield* session.ready).generation;
            yield* pieces.judge("the reconnect makes the next generation", [
              generationAfter === generationBefore + 1n,
              `the generation went from ${generationBefore} to ${generationAfter}`,
            ]);
            yield* listen;
            yield* run.mark("reconnected", String(generationAfter));
            const state = yield* model.getState.pipe(recorded, Effect.result);
            if (Option.isNone(long))
              return yield* ReactorError.fromCode("InvalidState", "the long clip was not accepted");
            yield* long.value.operation.reached("generated");
            const kept = (yield* recorded(model.getQueue)).value.playout.some(
              (clip) => clip.clip_id === long.value.submitted.clipId,
            );
            const window = openWindow();
            const playSentMs = yield* run.now;
            yield* recorded(model.play(long.value.submitted.clipId));
            const started = yield* seen("clip_started", long.value.submitted.clipId);
            yield* pieces.sleepUntil(started.atMs + freshMs, deadline);
            windows.delete(window);
            const firstFreshFrameMs = window.video.firstAfter(started.atMs);
            yield* record((tour) => ({
              ...tour,
              reconnect: {
                startedMs,
                readyMs,
                generationBefore: String(generationBefore),
                generationAfter: String(generationAfter),
                keptClip: kept,
                playSentMs,
                playStartedMs: started.atMs,
                ...(firstFreshFrameMs === undefined ? {} : { firstFreshFrameMs }),
                video: window.video.summary(),
                stateRead: Result.isSuccess(state),
              },
            }));
            yield* pieces.judge(
              family.key === "h3"
                ? "H3 answers on the new connection with the session's state kept"
                : "FastH3 answers on the new connection with the session's state kept",
              [Result.isSuccess(state), "get_state failed after the reconnect"],
              [kept, "the clip built before the reconnect was no longer ready to play"],
            );
            yield* run.judge(
              "fresh frames on the new connection",
              window.video.count === 0
                ? "no frame arrived after the reconnect"
                : window.video.live(started.atMs),
            );
          }),
        );

        // Popped clips, judged once long enough has passed for their builds to have finished.
        yield* phase(
          "popped clips",
          5,
          Effect.gen(function* () {
            const outcomes = yield* Effect.forEach(popped, (clip) =>
              Effect.map(lifecycleOf(clip.clipId, clip.repliedMs), (after) => ({
                ...clip,
                generatedAfter: after.some((entry) => entry.type === "clip_generated"),
                startedAfter: after.some((entry) => entry.type === "clip_started"),
              })),
            );
            // Clip 2 builds after the moved clip: when it was built says when that build ended.
            const movedPop = popped.find((clip) => clip.name === "moved");
            const clip2Generated = Option.isSome(clip2)
              ? clip2.value.lifecycle.generatedMs
              : undefined;
            if (Option.isSome(clip2)) yield* clip2.value.write;
            yield* record((tour) =>
              tour.queue === undefined
                ? tour
                : {
                    ...tour,
                    queue: {
                      ...tour.queue,
                      ...(movedPop === undefined || clip2Generated === undefined
                        ? {}
                        : {
                            clip2GeneratedAfterPopMs: round(clip2Generated - movedPop.repliedMs),
                          }),
                      pops: tour.queue.pops.map((entry) => {
                        const outcome = outcomes.find((clip) => clip.name === entry.name);
                        return outcome === undefined
                          ? entry
                          : {
                              ...entry,
                              generatedAfter: outcome.generatedAfter,
                              startedAfter: outcome.startedAfter,
                            };
                      }),
                    },
                  },
            );
            const pops = (yield* run.evidence).tour?.queue?.pops ?? [];
            const after = outcomes
              .filter((clip) => clip.generatedAfter || clip.startedAfter)
              .map((clip) => `${clip.name} ${clip.startedAfter ? "started" : "was built"}`);
            // H3 documents a waiting clip popped as removed, and a running build's result as
            // discarded: neither is built or starts.
            yield* pieces.judge(
              "popped clips are never built or started",
              [pops.length === 2, `${pops.length} of the two pops were sent`],
              [after.length === 0, `${after.join(", ")} after the pop`],
            );
          }),
        );

        // 16. The API key ends the session; the session's own close then confirms it. Only a
        // confirmed end is recorded as the session's; otherwise the check's cleanup ends it.
        yield* phase(
          "end with the API key",
          15,
          Effect.gen(function* () {
            const requestedMs = yield* run.now;
            const termination = yield* (yield* keyed).terminate(session.id);
            yield* record((tour) => ({ ...tour, apiKeyTermination: termination }));
            if (termination.confirmed) {
              yield* pieces.closedWith(session.id, requestedMs, { termination });
              closeRecorded = true;
            }
            const closeRequestedMs = yield* run.now;
            const report = yield* session.close;
            yield* record((tour) => ({ ...tour, ownedClose: report }));
            if (!closeRecorded && report.remote.confirmed) {
              yield* pieces.closedWith(session.id, closeRequestedMs, { report });
              closeRecorded = true;
            }
            grants.set(session.id, grant());
            yield* pieces.judge("the API key ends the session", [
              termination.confirmed,
              `DELETE answered ${String(termination.deleteStatus ?? "nothing")} and the read found ${termination.state ?? termination.evidence ?? "no terminal state"}`,
            ]);
            yield* pieces.judge("the session's own close confirms it ended", [
              report.remote.confirmed,
              `the close reported ${report.remote.evidence ?? "no evidence"} after DELETE ${String(report.remote.deleteStatus ?? "unanswered")}`,
            ]);
            yield* run.mark("ended with the API key");
          }),
        );

        // 17. After a confirmed end, never against a live session: an attach to the ended
        // session, and the key on an unknown session. None of it allocates.
        yield* phase(
          "after the end",
          15,
          Effect.gen(function* () {
            if (!closeRecorded)
              return yield* ReactorError.fromCode(
                "InvalidState",
                "the session's end was not confirmed",
              );
            const attached = yield* Effect.scoped(
              reactor.attach({
                sessionId: session.id,
                tokens: CoordinatorClient.fixedTokens(grant()),
              }),
            ).pipe(
              Effect.as("attached"),
              Effect.catch((error) => Effect.succeed(error)),
            );
            const attachStatus = Predicate.isString(attached) ? undefined : httpStatusOf(attached);
            const inspected = yield* (yield* pieces.withToken(target.apiKey))
              .inspect(unknownSession)
              .pipe(Effect.result);
            const inspectStatus = Result.isFailure(inspected)
              ? pieces.statusOf(inspected.failure)
              : undefined;
            const terminateUnknown = yield* (yield* keyed).terminate(unknownSession);
            yield* record((tour) => ({
              ...tour,
              afterEnd: {
                attach: Predicate.isString(attached) ? attached : reasonOf(attached),
                ...(attachStatus === undefined ? {} : { attachStatus }),
                inspectUnknown: Result.isSuccess(inspected)
                  ? "found"
                  : inspected.failure.reason._tag,
                ...(inspectStatus === undefined ? {} : { inspectStatus }),
                terminateUnknown,
              },
            }));
            // The SDK documents no refusal in particular for an ended session: which one came
            // is recorded.
            yield* pieces.judge("attaching to the ended session is refused", [
              !Predicate.isString(attached),
              "the attach succeeded",
            ]);
            yield* pieces.judge(
              "the API key finds no unknown session",
              [Result.isFailure(inspected), "reading it succeeded"],
              [
                terminateUnknown.confirmed && terminateUnknown.evidence === "absent",
                `ending it was ${terminateUnknown.confirmed ? `confirmed ${String(terminateUnknown.evidence)}` : "unconfirmed"} after DELETE ${String(terminateUnknown.deleteStatus ?? "unanswered")}`,
              ],
            );
            yield* run.mark("after the end");
          }),
        );
      }),
    pieces.endHeld(keyed),
  );
});

export const tour = (pieces: Pieces) =>
  Effect.flatMap(Run, (run) => Family.withFamily(run, (family) => tourFor(pieces, family)));
