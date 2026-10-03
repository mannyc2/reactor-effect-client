/**
 * One reconnect during a live Vidu call, on one session capped at 120 s. At the slower paid
 * avatar run's timing (26.7 s to make the avatar, 13.2 s to end the call), the picture wait,
 * reconnect and fresh media window put its end about 70 s after allocation, within 110 s of work.
 * The reconnect has a 45 s harness bound so the SDK's 30 s negotiation deadline can be observed.
 * Media after reconnect is an answer, never a pass criterion: hosted Reactor decides it.
 */
import { Cause, Duration, Effect, Exit, Option, Stream } from "effect";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type { AudioFrame, Recorded } from "reactor-effect-client/Media";
import * as Reactor from "reactor-effect-client/Reactor";
import type * as Session from "reactor-effect-client/Session";
import * as ViduS2Avatar from "reactor-effect-client/ViduS2Avatar";
import type { Pieces } from "../Checks.js";
import type * as Evidence from "../Evidence.js";
import * as Media from "../Media.js";
import * as Probes from "../Probes.js";
import { describe, recorded, Run } from "../Run.js";
import { plans } from "../Spend.js";
import { Target } from "../Target.js";
import { maxPhotoBytes } from "./Avatar.js";

export const rejoin = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const photo = target.photo;
  if (photo === undefined)
    return yield* pieces.judge("a photo to make the avatar from", [
      false,
      "no photo was given: a paid run needs --avatar-image",
    ]);
  const initial: Evidence.RejoinRecord = {
    photo: { bytes: photo.bytes.length, type: photo.type },
    steps: [],
    phases: [],
    diagnostics: [],
  };
  yield* run.update((evidence) => ({ ...evidence, rejoin: initial }));
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const grant = yield* pieces.mint("rejoin");
  const reactor = yield* Reactor.make({ maxUploadBytes: maxPhotoBytes });

  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        let deadline = Number.POSITIVE_INFINITY;
        let allocatedMs = 0;
        const since = (atMs: number) => pieces.round(atMs - allocatedMs);
        const steps: Array<Evidence.RejoinRecord["steps"][number]> = [];
        const phases: Array<Evidence.RejoinRecord["phases"][number]> = [];
        const diagnostics: Array<Evidence.RejoinRecord["diagnostics"][number]> = [];
        const samples: Array<Evidence.StatsSample> = [];
        let session: Session.Session | undefined;
        let call: Evidence.RejoinRecord["call"];
        let reconnect: Evidence.RejoinRecord["reconnect"];
        let after: Evidence.RejoinRecord["after"];
        let ended: Option.Option<ViduS2Avatar.CallEnded> = Option.none();

        const recordAll = Effect.gen(function* () {
          const paired = samples.findLast(
            (sample) => sample.local !== undefined && (sample.receivedKbps ?? 0) > 0,
          );
          const end = Option.getOrUndefined(ended);
          const allocated = (yield* run.evidence).sessions[0]?.allocatedMs;
          yield* run.update((evidence) => ({
            ...evidence,
            network: {
              samples: [...samples],
              ...(paired?.local === undefined ? {} : { pair: paired.local }),
            },
            rejoin: {
              ...initial,
              ...(allocated === undefined ? {} : { allocatedMs: allocated }),
              steps: [...steps],
              phases: [...phases],
              diagnostics: [...diagnostics],
              ...(call === undefined ? {} : { call }),
              ...(reconnect === undefined ? {} : { reconnect }),
              ...(after === undefined ? {} : { after }),
              ...(end === undefined
                ? {}
                : {
                    end: {
                      endReason: Probes.keptText(end.end_reason),
                      durationSeconds: end.duration_seconds,
                    },
                  }),
            },
          }));
        });
        /** Every operation keeps its outcome within both its own bound and the allocation's. */
        const operate = <A, E, R>(name: string, seconds: number, body: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const startedMs = yield* run.now;
            const left = Duration.toMillis(yield* pieces.until(deadline));
            const exit = yield* recorded(body).pipe(
              Effect.timeout(Duration.millis(Math.max(0, Math.min(seconds * 1000, left)))),
              Effect.exit,
            );
            if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause))
              return yield* Effect.interrupt;
            const step = {
              name,
              startedMs: since(startedMs),
              endedMs: since(yield* run.now),
              outcome: Exit.isSuccess(exit) ? "ok" : describe(exit.cause),
            };
            steps.push(step);
            yield* recordAll;
            yield* run.mark(name, step.outcome);
            return {
              ...step,
              value: Exit.isSuccess(exit) ? Option.some(exit.value) : Option.none<A>(),
            };
          });
        let closed = false;
        const closeSession = Effect.gen(function* () {
          if (closed || session === undefined) return;
          closed = true;
          yield* pieces.close(session);
        });
        yield* Effect.addFinalizer(() =>
          recordAll.pipe(Effect.ensuring(closeSession.pipe(Effect.orDie)), Effect.orDie),
        );

        /** Readers belong to the caller's scope, including the fresh generation's 10 s window. */
        const listen = Effect.fnUntraced(function* (
          opened: Session.Session,
          video: Media.VideoLog,
          audio: Media.AudioLog,
          onBlock: (atMs: number) => void,
        ) {
          const media = yield* opened.decoded;
          yield* pieces
            .readInto(media.video(ViduS2Avatar.tracks.video), video)
            .pipe(Effect.forkScoped);
          yield* pieces
            .readInto(media.audio(ViduS2Avatar.tracks.audio), {
              add: (element: Recorded<AudioFrame>, atMs: number) => {
                audio.add(element);
                if (element._tag === "Frame") onBlock(atMs);
              },
            })
            .pipe(Effect.forkScoped);
          return Number(media.generation);
        });

        const converse = Effect.gen(function* () {
          const created = yield* operate(
            "session",
            30,
            reactor.create({
              model: plans.rejoin.model.name,
              tokens: CoordinatorClient.fixedTokens(grant),
              onAllocated: (allocation) =>
                Effect.gen(function* () {
                  deadline = yield* pieces.allocated(allocation.id, grant);
                  grants.set(allocation.id, grant);
                  allocatedMs =
                    (yield* run.evidence).sessions.find((held) => held.id === allocation.id)
                      ?.allocatedMs ?? (yield* run.now);
                  yield* pieces
                    .recordSessionEvents(allocation)
                    .pipe(Effect.forkScoped({ startImmediately: true }));
                }),
            }),
          );
          if (Option.isNone(created.value)) return;
          session = created.value.value;
          const opened = session;
          yield* pieces.sampleStats(opened, samples).pipe(Effect.forkScoped);
          const video = Media.videoLog();
          const audio = Media.audioLog();
          const firstBlocks: Array<number> = [];
          const generation = yield* listen(opened, video, audio, (atMs) => {
            if (firstBlocks.length === 0) firstBlocks.push(atMs);
          });

          const made = yield* operate(
            "make",
            10,
            ViduS2Avatar.make(opened, { callTimeout: "30 seconds" }),
          );
          if (Option.isNone(made.value)) return;
          const avatar = made.value.value;
          yield* avatar.changes.pipe(
            Stream.runForEach((state) =>
              Effect.flatMap(run.now, (atMs) =>
                Effect.sync(() => {
                  const phase = Probes.keptText(state.phase);
                  if (phases.at(-1)?.phase !== phase) phases.push({ phase, atMs: since(atMs) });
                }),
              ),
            ),
            Effect.forkScoped,
          );
          yield* avatar.events({ capacity: 256 }).pipe(
            Stream.runForEach((event) =>
              event._tag === "Diagnostic"
                ? Effect.flatMap(run.now, (atMs) =>
                    Effect.sync(() => {
                      diagnostics.push({ atMs: since(atMs), reason: event.error.reason._tag });
                    }),
                  )
                : Effect.void,
            ),
            Effect.catch((error) => run.mark("events unread", error.reason._tag)),
            Effect.orDie,
            Effect.forkScoped,
          );
          const ready = yield* operate(
            "createAvatar",
            45,
            avatar.createAvatar({ bytes: photo.bytes, type: `image/${photo.type}` }),
          );
          if (Option.isNone(ready.value)) return;
          const live = yield* operate(
            "startCall",
            30,
            avatar.startCall({
              persona: "You are Probe, a friendly guide. Answer in one short sentence.",
              greeting: "Say hello in one short sentence.",
              llm: { maxTokens: 30 },
            }),
          );
          if (Option.isNone(live.value)) return;
          const liveAtMs = yield* run.now;
          firstBlocks.length = 0;
          yield* pieces.watch(
            Effect.sync(
              () =>
                video.count >= 2 &&
                video.firstAfter(liveAtMs) !== undefined &&
                firstBlocks[0] !== undefined,
            ),
            Math.min(deadline, run.origin + liveAtMs + 10_000),
          );
          const firstFrameMs = video.firstAfter(liveAtMs);
          const firstBlockMs = firstBlocks[0];
          call = {
            liveAtMs: since(liveAtMs),
            generation,
            observedMs: pieces.round((yield* run.now) - liveAtMs),
            ...(firstFrameMs === undefined
              ? {}
              : { firstFrameMs: pieces.round(firstFrameMs - liveAtMs) }),
            ...(firstBlockMs === undefined
              ? {}
              : { firstBlockMs: pieces.round(firstBlockMs - liveAtMs) }),
            video: video.summary(),
            audio: audio.summary(),
          };
          reconnect = { startedMs: since(yield* run.now), fromGeneration: generation };
          yield* recordAll;
          yield* run.mark("reconnecting during the call");
          const returned = yield* operate("reconnect", 45, opened.reconnect);
          const phase = phases.at(-1)?.phase;
          reconnect = {
            ...reconnect,
            endedMs: returned.endedMs,
            outcome: returned.outcome,
            generation: Number((yield* opened.snapshot).generation),
            ...(phase === undefined ? {} : { phase }),
          };
          yield* recordAll;
          yield* run.mark("reconnect observed", returned.outcome);

          const returnedMs = allocatedMs + returned.endedMs;
          yield* Effect.scoped(
            Effect.gen(function* () {
              const freshVideo = Media.videoLog();
              const freshAudio = Media.audioLog();
              let firstFreshBlockMs: number | undefined;
              const fresh = yield* operate(
                "decoded after reconnect",
                10,
                listen(opened, freshVideo, freshAudio, (atMs) => {
                  firstFreshBlockMs ??= atMs;
                }),
              );
              if (Option.isNone(fresh.value)) return;
              const freshGeneration = fresh.value.value;
              const observedFromMs = yield* run.now;
              // A deadline can interrupt the window too; its partial answer still belongs in it.
              yield* Effect.addFinalizer(() =>
                Effect.gen(function* () {
                  const frameMs = freshVideo.firstAfter(returnedMs);
                  after = {
                    generation: freshGeneration,
                    observedMs: pieces.round((yield* run.now) - observedFromMs),
                    ...(frameMs === undefined
                      ? {}
                      : { firstFrameMs: pieces.round(frameMs - returnedMs) }),
                    ...(firstFreshBlockMs === undefined
                      ? {}
                      : { firstBlockMs: pieces.round(firstFreshBlockMs - returnedMs) }),
                    video: freshVideo.summary(),
                    audio: freshAudio.summary(),
                  };
                  const afterPhase = phases.at(-1)?.phase;
                  if (reconnect !== undefined && afterPhase !== undefined)
                    reconnect = { ...reconnect, phase: afterPhase };
                }),
              );
              yield* pieces.sleepUntil(returnedMs + 10_000, deadline);
            }),
          );
          yield* recordAll;
          yield* run.mark("media after reconnect observed");
          ended = (yield* operate("endCall", 20, avatar.endCall)).value;
        });
        yield* converse;
        yield* recordAll;
        yield* pieces.judge(
          "the call went live with picture",
          [call?.firstFrameMs !== undefined, "no frame observed after live"],
          [(call?.video.lit ?? 0) > 0, "every frame was black"],
          [(call?.video.distinct ?? 0) >= 2, "the frames never changed"],
        );
        yield* pieces.judge(
          "the reconnect returned within 30 s",
          [reconnect?.outcome === "ok", reconnect?.outcome ?? "reconnect never settled"],
          [
            reconnect?.endedMs !== undefined && reconnect.endedMs - reconnect.startedMs <= 30_000,
            "reconnect took more than 30 s",
          ],
        );
        yield* closeSession;
        const closedSessions = (yield* run.evidence).sessions;
        yield* pieces.judge("the session was confirmed ended", [
          closedSessions.length > 0 &&
            closedSessions.every((held) => held.close?.confirmed === true),
          "the session's end was not confirmed",
        ]);
        yield* run.mark("rejoin observed");
      }),
    pieces.endHeld(keyed),
  );
});
