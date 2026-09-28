/**
 * `adoption`: three ways a process takes over one session, in turn. An owner
 * on the isolated native host (under Node, when paid) creates the session on a
 * token that lives 20 s, plays a 15 s clip with another queued behind it,
 * and is killed 12 s after it allocated. A raw attach, which does not adopt,
 * names the playing clip, reads the queued clip's metadata, receives fresh
 * frames and closes without ending the session. The session is read while
 * nothing is connected, until the creating token has expired. Then
 * `H3Source.resume` adopts it with tokens bound to it that live 12 s, names
 * the playing clip, receives fresh frames, and past its first token's refresh
 * point enqueues a clip with a reference image and reference audio. A token
 * past its expiry and one without the bind are refused, and closing the
 * resumed source ends the session.
 *
 * The owner's clips decide when the resume may come: its frames must arrive
 * while one of them still plays. Paid `tokens` found the owner's 5 s queued
 * clip already playing 24 s into its run, so here the queued clip lasts 15 s
 * too. From the owner's allocation A, at the timing paid runs measured (an
 * attach ready in 2.7 to 2.9 s, a 15 s clip built in about 6.5 s, the refresh
 * a quarter of a token's life before it expires):
 *
 *   A+10    the first 15 s clip starts, and plays until about A+25.5; the second
 *           then plays until about A+41
 *   A+12    the owner is killed
 *   A+15    the raw attach is ready; it reads 2 s of frames and closes
 *   A+17.5  the session is read every half second while nothing is connected
 *   A+20    the creating token has expired and nothing was connected for 1 s: the resume starts
 *   A+23    it is ready, and reads 2 s of frames while one of the owner's clips plays
 *   A+29.5  past the first bound token's refresh point, the clip with references
 *   A+31    the clip is accepted; the refusals are read
 *   A+32    closing the resumed source ends the session, 33 s inside the work deadline
 *
 * The creating token is minted before the owner starts, so it expires 18 to 20 s
 * after A, by how long the owner took to allocate. The kill waits for the owner
 * to stream, so a slower build moves every later step with it.
 *
 * Each step is a phase: it judges its own criteria, and a failure or running
 * out of time is a failed criterion named after it, after which the steps that
 * still make sense run.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as H3 from "reactor-effect-client/H3";
import * as H3Source from "reactor-effect-client/H3Source";
import { ItemKey } from "reactor-effect-client/Playout";
import * as Reactor from "reactor-effect-client/Reactor";
import type * as Session from "reactor-effect-client/Session";
import type { Pieces } from "../Checks.js";
import type { AdoptionRecord } from "../Evidence.js";
import * as Media from "../Media.js";
import * as Probes from "../Probes.js";
import { describe, recorded, Run } from "../Run.js";
import { acceptGrant, provenGrant, tokenSecondsFor } from "../Spend.js";
import { Target } from "../Target.js";

const tracks = H3.h3ReferenceTurboRealtime.tracks;

/** How long the creating token lives: past the raw attach, not the session. */
const createSeconds = 20;
/** How long each token bound for the resume lives, so one is refreshed while the session runs. */
const boundSeconds = 12;
/** The owner is killed this long after it allocated, with its 15 s clip playing. */
const killAfterMs = 12_000;
/** How long the owner's queued clip asks for: its clips must still play when the resume attaches. */
const queuedSeconds = 15;
/** How long after the run starts the owner must be streaming. */
const ownerWithinMs = 30_000;
/** Each takeover's time from its start: an attach ready within 5 s, then a few seconds of frames. */
const takeoverMs = 12_000;
/** How far apart the reads are while nothing is connected. */
const gapReadMs = 500;
/**
 * The least time nothing is connected. A paid owner starts under Node before
 * it allocates, which moves the creating token's expiry toward the attached
 * close, so the reads while nothing is connected cannot rest on that expiry alone.
 */
const gapMs = 1_000;

/** H3's reply to an enqueue: the clip, as H3 accepted it. */
const ClipQueued = Schema.Struct({ clip: H3.Clip });

/**
 * Runs one phase until `endsAt`, a Clock time. Whatever it fails with, running
 * out of time included, becomes a failed criterion named after it, and the
 * check goes on without its result. A save that failed is no phase's own: it
 * stops the run.
 */
const phase = Effect.fnUntraced(function* <A, E extends { readonly _tag: string }, R>(
  name: string,
  endsAt: number,
  body: Effect.Effect<A, E, R>,
) {
  const run = yield* Run;
  const left = Math.max(0, endsAt - (yield* Clock.currentTimeMillis));
  return yield* body.pipe(
    Effect.timeout(Duration.millis(left)),
    Effect.asSome,
    Effect.catchIf(
      (error) => error._tag !== "SaveFailed",
      (error) => Effect.as(run.judge(name, describe(Cause.fail(error))), Option.none<A>()),
    ),
  );
});

export const adoption = Effect.fnUntraced(function* (pieces: Pieces) {
  const { round } = pieces;
  const run = yield* Run;
  const target = yield* Target;
  const coordinator = yield* Coordinator.Coordinator;
  const record = (change: (adoption: AdoptionRecord) => AdoptionRecord) =>
    run.update((evidence) => ({
      ...evidence,
      adoption: change(evidence.adoption ?? { mints: [], gap: [] }),
    }));
  const minted = Effect.fnUntraced(function* (
    kind: AdoptionRecord["mints"][number]["kind"],
    grant: Coordinator.TokenGrant,
    sentAt: number,
  ) {
    const atMs = yield* run.now;
    yield* record((adoption) => ({
      ...adoption,
      mints: [
        ...adoption.mints,
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
  const createSentAt = yield* Clock.currentTimeMillis;
  const grant = yield* pieces.mint("adoption", createSeconds);
  yield* minted("create", grant, createSentAt);
  // This process holds the key, as a server adopting a dead owner's session does: each token
  // it mints is bound to the session, which allocates nothing.
  const readTokens = yield* pieces.binder(tokenSecondsFor("adoption"), (token, sentAt) =>
    minted("read", token, sentAt),
  );
  const attachTokens = yield* pieces.binder(tokenSecondsFor("adoption"), (token, sentAt) =>
    minted("attach", token, sentAt),
  );
  const resumeMints: Array<{ readonly grant: Coordinator.TokenGrant; readonly sentAt: number }> =
    [];
  const resumeTokens = yield* pieces.binder(boundSeconds, (token, sentAt) =>
    Effect.sync(() => resumeMints.push({ grant: token, sentAt })).pipe(
      Effect.andThen(minted("resume", token, sentAt)),
    ),
  );
  const marker = `hosted-qualification:${run.runId}`;
  const keyed = Coordinator.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        // The session is held from the moment the owner allocates it, so an owner that fails
        // before it streams still leaves a session the key can end.
        const held = yield* Deferred.make<{
          readonly sessionId: string;
          readonly allocatedAt: number;
          readonly deadline: number;
        }>();
        const onAllocated = (allocation: H3Source.Allocation) =>
          Effect.gen(function* () {
            const cap = pieces.capMs(grant);
            const endsAt =
              allocation.endsAt === undefined
                ? (yield* Clock.currentTimeMillis) + cap
                : allocation.endsAt * 1000;
            grants.set(allocation.sessionId, grant);
            const deadline = yield* pieces.holding(allocation.sessionId, endsAt - cap, endsAt);
            yield* Deferred.succeed(held, {
              sessionId: allocation.sessionId,
              allocatedAt: endsAt - cap,
              deadline,
            });
          }).pipe(Effect.provideService(Run, run));
        const started = yield* phase(
          "the owner",
          run.origin + ownerWithinMs,
          Effect.gen(function* () {
            const owner = yield* target.owner(grant, marker, {
              isolated: true,
              queuedSeconds,
              onAllocated,
            });
            // The owner reports its allocation before it streams.
            const { sessionId, allocatedAt, deadline } = yield* Deferred.await(held);
            const ownerStreamingMs = yield* run.now;
            yield* record((adoption) => ({
              ...adoption,
              ...(owner.host === undefined ? {} : { ownerHost: owner.host }),
              ownerClipIds: { playing: owner.playing, queued: owner.queued },
              ownerStreamingMs,
            }));
            yield* run.mark("owner streaming", owner.host);
            // A token of the check's own for its reads, which outlives the session.
            const reader = yield* readTokens.bind(sessionId);
            grants.set(sessionId, reader);
            return { owner, sessionId, allocatedAt, deadline, reader };
          }),
        );
        if (Option.isNone(started)) return;
        const { owner, sessionId, allocatedAt, deadline, reader } = started.value;

        yield* pieces.sleepUntil(allocatedAt - run.origin + killAfterMs, deadline);
        yield* owner.kill;
        const killedMs = yield* run.now;
        const createExpiresMs = grant.expiresAt * 1000 - run.origin;
        yield* record((adoption) => ({ ...adoption, killedMs, createExpiresMs }));
        yield* run.mark("owner killed", owner.playing);

        // The session as its coordinator describes it, with nothing of this check connected. A
        // read that gets no reply is recorded as status 0, so a read never fails.
        let lastConnectionMs = killedMs;
        const gapRead = Effect.gen(function* () {
          const read = yield* Probes.readSession({
            apiUrl: target.apiUrl,
            sessionId,
            credential: reader.jwt,
          });
          const atMs = yield* run.now;
          const entry = { atMs, sinceConnectionMs: round(atMs - lastConnectionMs), ...read };
          yield* record((adoption) => ({ ...adoption, gap: [...adoption.gap, entry] }));
          return entry;
        });

        // The raw attach: it joins the session without taking over its lifetime.
        const attachEndsAt = Math.min(run.origin + killedMs + takeoverMs, deadline);
        const attachVideo = Media.videoLog();
        const attached = yield* phase(
          "the attach",
          attachEndsAt,
          Effect.gen(function* () {
            const reactor = yield* pieces.adopting(sessionId, killedMs);
            const startedMs = yield* run.now;
            const session = yield* reactor.attach({ sessionId, tokens: attachTokens });
            const attachedMs = yield* run.now;
            yield* record((adoption) => ({ ...adoption, attach: { startedMs, attachedMs } }));
            yield* run.mark("attached");
            yield* pieces.judge("the attach came within 5 s of the kill", [
              attachedMs - killedMs <= 5_000,
              `it was ready ${Math.round(attachedMs - killedMs)} ms after the kill`,
            ]);
            return { session, startedMs, attachedMs };
          }),
        );
        if (Option.isSome(attached)) {
          const { session, startedMs, attachedMs } = attached.value;
          const updateAttach = (fields: Partial<NonNullable<AdoptionRecord["attach"]>>) =>
            record((adoption) => ({
              ...adoption,
              attach: { startedMs, attachedMs, ...adoption.attach, ...fields },
            }));
          yield* phase(
            "the attached session",
            attachEndsAt,
            Effect.gen(function* () {
              const provider = yield* H3.make(session);
              const facts = pieces.factsOf(yield* provider.snapshot);
              const playingClipId = facts?.state.playing_clip_id ?? null;
              const queuedMetadata = [
                ...(facts?.queue.generation ?? []),
                ...(facts?.queue.playout ?? []),
              ].some(
                (clip) =>
                  clip.clip_id === owner.queued && clip.metadata.includes(`${marker}:queued`),
              );
              yield* updateAttach({ playingClipId, queuedMetadata });
              const media = yield* session.decoded;
              yield* pieces.readFresh(media.video(tracks.video), attachVideo, deadline);
              const firstFreshFrameMs = attachVideo.firstAfter(attachedMs);
              yield* updateAttach({
                ...(firstFreshFrameMs === undefined ? {} : { firstFreshFrameMs }),
                video: attachVideo.summary(),
              });
              yield* pieces.judge("the attach names the owner's playing clip", [
                playingClipId === owner.playing,
                `the attached state named ${playingClipId ?? "no clip"} playing, not the owner's`,
              ]);
              yield* pieces.judge("the attach reads the queued clip's metadata", [
                queuedMetadata,
                "the owner's queued clip lost its metadata, or was not listed",
              ]);
              yield* run.judge(
                "fresh frames on attach",
                attachVideo.count === 0
                  ? "no frame arrived after attaching"
                  : attachVideo.live(attachedMs),
              );
            }),
          );
          yield* phase(
            "the attached close",
            deadline,
            Effect.gen(function* () {
              const requestedMs = yield* run.now;
              const report = yield* session.close;
              const reportedMs = yield* run.now;
              lastConnectionMs = reportedMs;
              const commands = pieces.commandsSince(yield* run.evidence, startedMs);
              yield* updateAttach({ commands, close: { requestedMs, reportedMs, report } });
              yield* run.mark(
                "attached session closed",
                report.remote.attempted ? "termination attempted" : "left running",
              );
              const read = yield* gapRead;
              yield* pieces.judge("no enqueue on attach", [
                (commands.enqueue ?? 0) === 0,
                `${commands.enqueue} enqueue(s) after attaching`,
              ]);
              yield* pieces.judge(
                "the attached close attempts no termination",
                [
                  report.ownership === "attached",
                  `the attached session's close reported it ${report.ownership ?? "without ownership"}`,
                ],
                [!report.remote.attempted, "closing the attached session attempted a termination"],
              );
              yield* pieces.judge("the session still reads live after the attached close", [
                read.status === 200 &&
                  read.state !== undefined &&
                  !Coordinator.isTerminal(read.state),
                `reading it answered ${read.status} ${read.state ?? "without a state"}`,
              ]);
            }),
          );
        }

        // Nothing is connected until the creating token has expired, and for the gap at least.
        // Nothing here can fail, so nothing cuts the wait for that expiry short.
        const gapEndsAt = Math.min(
          Math.max(grant.expiresAt * 1000 + 500, run.origin + lastConnectionMs + gapMs),
          deadline,
        );
        while ((yield* Clock.currentTimeMillis) + gapReadMs <= gapEndsAt) {
          yield* Effect.sleep(Duration.millis(gapReadMs));
          yield* gapRead;
        }
        yield* pieces.sleepUntil(gapEndsAt - run.origin, deadline);

        // The resume: it adopts the session from the owner's record, and then owns it.
        const resumeEndsAt = Math.min((yield* Clock.currentTimeMillis) + takeoverMs, deadline);
        const resumeVideo = Media.videoLog();
        const resumed = yield* phase(
          "the resume",
          resumeEndsAt,
          Effect.gen(function* () {
            const reactor = yield* pieces.adopting(sessionId, killedMs);
            // The source keeps its session to itself; this one hands it over for its replies.
            const captured = yield* Deferred.make<Session.Session>();
            const watched = Reactor.Reactor.of({
              create: reactor.create,
              attach: (options) =>
                Effect.tap(reactor.attach(options), (session) =>
                  Deferred.succeed(captured, session),
                ),
            });
            const startedMs = yield* run.now;
            const source = yield* H3Source.resume({
              allocation: owner.allocation,
              tokens: resumeTokens,
            }).pipe(Effect.provideService(Reactor.Reactor, watched));
            const session = yield* Deferred.await(captured);
            const attachedMs = yield* run.now;
            yield* record((adoption) => ({
              ...adoption,
              resume: { startedMs, attachedMs, ownership: session.ownership },
            }));
            yield* run.mark("resumed");
            yield* pieces.judge("the resume came after the creating token expired", [
              startedMs > createExpiresMs,
              `the creating token lived ${round((createExpiresMs - startedMs) / 1000)} s past the resume`,
            ]);
            yield* pieces.judge("the resume adopts the session", [
              session.ownership === "owned",
              `the resumed session is ${session.ownership}`,
            ]);
            return { source, session, startedMs, attachedMs };
          }),
        );
        if (Option.isSome(resumed)) {
          const { source, session, startedMs, attachedMs } = resumed.value;
          const updateResume = (fields: Partial<NonNullable<AdoptionRecord["resume"]>>) =>
            record((adoption) => ({
              ...adoption,
              resume: {
                startedMs,
                attachedMs,
                ownership: session.ownership,
                ...adoption.resume,
                ...fields,
              },
            }));
          yield* phase(
            "the resumed session",
            resumeEndsAt,
            Effect.gen(function* () {
              const state = yield* source.events.pipe(
                Stream.filter((event) => event._tag === "State"),
                Stream.runHead,
                Effect.flatMap(Effect.fromOption),
              );
              const playingClipId = state.state.playing?.clipId ?? null;
              yield* updateResume({ playingClipId });
              yield* pieces.readFresh(source.video, resumeVideo, deadline);
              const firstFreshFrameMs = resumeVideo.firstAfter(attachedMs);
              yield* updateResume({
                ...(firstFreshFrameMs === undefined ? {} : { firstFreshFrameMs }),
                video: resumeVideo.summary(),
              });
              // The resume comes after the creating token expired, when the owner's first clip
              // may have ended, so the clip queued behind it counts too.
              yield* pieces.judge("the resume names one of the owner's clips playing", [
                playingClipId === owner.playing || playingClipId === owner.queued,
                `the resumed state named ${playingClipId ?? "no clip"} playing, neither of the owner's clips`,
              ]);
              yield* run.judge(
                "fresh frames on resume",
                resumeVideo.count === 0
                  ? "no frame arrived after resuming"
                  : resumeVideo.live(attachedMs),
              );
            }),
          );
          yield* phase(
            "the refreshed clip",
            deadline - 10_000,
            Effect.scoped(
              Effect.gen(function* () {
                // The first call past the first bound token's refresh point mints the next one.
                const first = resumeMints[0];
                if (first !== undefined) {
                  const expiresAt = first.grant.expiresAt * 1000;
                  const margin = Math.min(60_000, (expiresAt - first.sentAt) / 4);
                  yield* pieces.sleepUntil(
                    expiresAt - margin - run.origin + 250,
                    deadline - 17_000,
                  );
                }
                // H3's reply names what it accepted; the source returns only the clip's id.
                const queued = yield* SubscriptionRef.make<ReadonlyArray<H3.Clip>>([]);
                const observation = yield* session.observe({ capacity: 1024 });
                yield* observation.events.pipe(
                  Stream.runForEach((event) =>
                    event._tag === "Model" &&
                    event.kind === "message" &&
                    event.type === "clip_queued"
                      ? Schema.decodeUnknownEffect(ClipQueued)(event.data).pipe(
                          Effect.flatMap(({ clip }) =>
                            SubscriptionRef.update(queued, (all) => [...all, clip]),
                          ),
                          Effect.ignore,
                        )
                      : Effect.void,
                  ),
                  Effect.ignore,
                  Effect.forkScoped,
                );
                const uploadStartedMs = yield* run.now;
                const clipId = yield* recorded(
                  source.enqueue(pieces.withReferences(`${marker}:references`), {
                    _tag: "Item",
                    key: ItemKey.make("references"),
                  }),
                );
                const acceptedMs = yield* run.now;
                const clip = Option.getOrUndefined(
                  yield* pieces
                    .waitFor(
                      queued,
                      (all) => all.find((listed) => listed.clip_id === clipId),
                      Math.min(deadline, run.origin + acceptedMs + 2_000),
                    )
                    .pipe(Effect.option),
                );
                const refreshedMs = (yield* run.evidence).adoption?.mints.filter(
                  (mint) => mint.kind === "resume",
                )[1]?.atMs;
                yield* updateResume({
                  ...(refreshedMs === undefined ? {} : { refreshedMs }),
                  upload: {
                    startedMs: uploadStartedMs,
                    acceptedMs,
                    images: 1,
                    audio: 1,
                    reportedAudio: clip?.reference_audio_count ?? null,
                    hasReferenceAudio: clip?.has_reference_audio ?? null,
                  },
                  commands: pieces.commandsSince(yield* run.evidence, startedMs),
                });
                yield* run.mark("enqueued on a refreshed token");
                yield* pieces.judge(
                  "a refreshed bound token carried the next call",
                  [
                    resumeMints.length >= 2 && refreshedMs !== undefined,
                    `${resumeMints.length} bound token(s) were minted for the resume`,
                  ],
                  [
                    refreshedMs !== undefined &&
                      refreshedMs >= uploadStartedMs &&
                      refreshedMs <= acceptedMs,
                    "the refresh did not happen for the clip's upload",
                  ],
                );
                yield* pieces.judge("reference audio reported", [
                  clip?.has_reference_audio === true && clip.reference_audio_count === 1,
                  `the clip reports has_reference_audio ${String(clip?.has_reference_audio ?? "absent")} and ${String(clip?.reference_audio_count ?? "no")} audio reference(s) for 1 sent`,
                ]);
              }),
            ),
          );
        }

        // The documented refusals: a token past its expiry, and one not bound to the session.
        yield* phase(
          "the refusals",
          deadline - 5_000,
          Effect.gen(function* () {
            const readWith = (credential: Redacted.Redacted<string>) =>
              pieces.withToken(credential).pipe(
                Effect.flatMap((inspector) => inspector.inspect(sessionId)),
                Effect.as(200),
                Effect.catch((error) => Effect.succeed(pieces.statusOf(error) ?? 0)),
              );
            const expiredTokenStatus = yield* readWith(grant.jwt);
            const unboundSentAt = yield* Clock.currentTimeMillis;
            // It could create one session of a second, so it is proven like any creating token;
            // it creates none.
            const unbound = yield* coordinator.mintToken({
              apiKey: target.apiKey,
              modelName: H3.modelName,
              maxSessionDuration: "1 second",
              expiresAfter: "15 seconds",
            });
            yield* run.secret(unbound.jwt);
            const granted = yield* provenGrant({
              jwt: Redacted.value(unbound.jwt),
              granted: unbound.granted,
            });
            yield* acceptGrant({ check: "adoption", granted });
            yield* run.update((evidence) => ({
              ...evidence,
              grants: [...evidence.grants, { ...granted, expiresAt: unbound.expiresAt }],
            }));
            yield* minted("unbound", unbound, unboundSentAt);
            const unboundTokenStatus = yield* readWith(unbound.jwt);
            yield* record((adoption) => ({ ...adoption, expiredTokenStatus, unboundTokenStatus }));
            yield* run.mark("refusals read");
            yield* pieces.judge("an expired token is refused", [
              expiredTokenStatus === 401,
              `reading with it answered ${expiredTokenStatus}`,
            ]);
            yield* pieces.judge("an unbound token is refused", [
              unboundTokenStatus === 403,
              `reading with it answered ${unboundTokenStatus}`,
            ]);
          }),
        );

        // The end: the resumed source owns the session, so closing it terminates it.
        if (Option.isSome(resumed))
          yield* phase(
            "the end",
            deadline,
            Effect.gen(function* () {
              yield* pieces.close({ id: sessionId, close: resumed.value.source.close });
              const report = (yield* run.evidence).sessions.find(
                (session) => session.id === sessionId,
              )?.close?.report;
              yield* pieces.judge(
                "closing the resumed source ends the session",
                [
                  report?.ownership === "owned",
                  `the resumed session's close reported it ${report?.ownership ?? "without ownership"}`,
                ],
                [report?.remote.attempted === true, "closing it attempted no termination"],
                [report?.remote.confirmed === true, "its termination was not confirmed"],
                [
                  report?.remote.state === "CLOSED",
                  `the session read ${report?.remote.state ?? "no state"} after it`,
                ],
              );
            }),
          );
        yield* run.mark("adoption observed");
      }),
    // Whatever failed, the key ends a session the check allocated.
    pieces.endHeld(keyed),
  );
});
