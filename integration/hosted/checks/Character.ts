/**
 * `character`: the SDK's `ViduS2Avatar` provider on hosted Vidu S2-Avatar, in
 * one call on one session capped at 75 s. The provider sends every command:
 * it makes the avatar from the operator's photo, starts a call with a
 * greeting and returns it live with the character's tracks resumed, passes on
 * a `say`, and ends the call. The check reads the session's decoded picture
 * and sound and the provider's snapshots and events, and judges what a caller
 * of the provider meets: the avatar ready, the call live, the character's
 * picture and sound after live, an answer to the `say`, and the call's end.
 *
 * The planned timeline, from the allocation A, at the timing the second paid
 * `avatar` run measured, and the first's in brackets where it differed
 * (connected about 2.5 s in, an avatar ready 2.1 s [26.7 s] after
 * `create_avatar`, a call live 2.7 to 4.5 s after `start_call`, the picture
 * 0.2 s after the resume, the greeting's sound 4.4 s after live, an answer's
 * 3.1 to 3.4 s after a `say`, and `end_call` answered in 6.9 s [13.2 s]):
 *
 *   A+3          connected; the provider reads the snapshot, uploads the photo
 *                and makes the avatar from it
 *   A+6  [A+31]  startCall with a greeting; live about A+10 [A+35], when the
 *                first frame must come within 5 s
 *   A+15 [A+40]  the greeting's sound, then the silence after it
 *   A+17 [A+42]  say, and the answer's sound within 8 s
 *   A+21 [A+46]  endCall, until the model released the call
 *   A+28 [A+59]  the session is closed
 *
 * Both end inside the work deadline, 65 s after the allocation.
 */
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type { AudioFrame, Recorded, VideoFrame } from "reactor-effect-client/Media";
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

/** Who the character is: short, so that each answer is. */
const persona = "You are Probe, a friendly guide. Answer in one short sentence.";
const greeting = "Say hello in one short sentence.";
const question = "Say one short sentence about the sea.";
/** Caps each reply below the model's default of 50 tokens. */
const llm = { maxTokens: 30 };
/** The first frame must come this long after live. */
const frameWithinMs = 5_000;
/** How long the character's sound is waited for after live, and an answer's after a `say`. */
const onsetWithinMs = 8_000;
/** How long the greeting is given to fall silent once it sounded. */
const silenceWithinMs = 10_000;
/** A block of 0.01 RMS (-40 dBFS) or more is speech; 300 ms with none is silence. */
const speechRms = 0.01;
const silentMs = 300;
/** Loud blocks held in memory; a session that sends more is past what the check reads. */
const maxLoud = 100_000;

/** An operation the check ran, on the run's clock. */
interface Step {
  readonly name: string;
  readonly startedMs: number;
  readonly endedMs: number;
  readonly outcome: string;
}

/** Why a call's picture fails, or undefined when lit frames kept changing. */
const videoFailure = (video: Evidence.CharacterRecord["video"]): string | undefined => {
  if (video === undefined || video.frames === 0) return "no frame arrived while the call was live";
  if (video.lit === 0) return "every frame was black";
  return video.distinct < 2 ? "the frames never changed" : undefined;
};

export const character = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const photo = target.photo;
  // A paid run is refused without one before it starts; nothing is opened, so nothing is spent.
  if (photo === undefined)
    return yield* pieces.judge("a photo to make the avatar from", [
      false,
      "no photo was given: a paid run needs --avatar-image",
    ]);
  const initial: Evidence.CharacterRecord = {
    photo: { bytes: photo.bytes.length, type: photo.type },
    steps: [],
    phases: [],
    transcripts: [],
    commandErrors: [],
    diagnostics: [],
  };
  yield* run.update((evidence) => ({ ...evidence, character: initial }));
  const record = (change: (character: Evidence.CharacterRecord) => Evidence.CharacterRecord) =>
    run.update((evidence) => ({ ...evidence, character: change(evidence.character ?? initial) }));
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const grant = yield* pieces.mint("character");
  // The photo may be as large as Vidu takes, past the SDK's default bound on an upload.
  const reactor = yield* Reactor.make({ maxUploadBytes: maxPhotoBytes });

  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        let deadline = Number.POSITIVE_INFINITY;
        let allocatedMs = 0;
        /** A time on the run's clock as the record keeps it: from the allocation. */
        const since = (atMs: number) => pieces.round(atMs - allocatedMs);
        const now = run.now;
        const steps: Array<Step> = [];

        /** Runs an operation within `seconds` and the work deadline, and keeps how it settled. */
        const operate = <A, E, R>(name: string, seconds: number, body: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const startedMs = yield* now;
            const left = Duration.toMillis(yield* pieces.until(deadline));
            const exit = yield* recorded(body).pipe(
              Effect.timeout(Duration.millis(Math.max(0, Math.min(seconds * 1000, left)))),
              Effect.exit,
            );
            if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause))
              return yield* Effect.interrupt;
            const outcome = Exit.isSuccess(exit) ? "ok" : describe(exit.cause);
            steps.push({ name, startedMs, endedMs: yield* now, outcome });
            yield* run.mark(name, outcome);
            return Exit.isSuccess(exit) ? Option.some(exit.value) : Option.none<A>();
          });

        // The picture and sound from the connection to the close, and from live until endCall
        // apart; each block of speech's arrival.
        const video = Media.videoLog();
        const audio = Media.audioLog();
        const callVideo = Media.videoLog();
        const callAudio = Media.audioLog();
        let inCall = false;
        const loud: Array<number> = [];
        const videoFeed = {
          add: (element: Recorded<VideoFrame>, atMs: number) => {
            video.add(element, atMs);
            if (inCall) callVideo.add(element, atMs);
          },
        };
        const audioFeed = {
          add: (element: Recorded<AudioFrame>, atMs: number) => {
            audio.add(element);
            if (inCall) callAudio.add(element);
            if (element._tag !== "Frame" || loud.length >= maxLoud) return;
            const samples = element.frame.samples;
            let power = 0;
            for (const sample of samples) power += (sample / 32768) ** 2;
            if (Math.sqrt(power / Math.max(1, samples.length)) >= speechRms) loud.push(atMs);
          },
        };
        /** The first speech at or after `atMs`. */
        const onsetAfter = (atMs: number) => loud.find((at) => at >= atMs);
        /** The last speech before the first 300 ms with none, from `atMs`, as of `nowMs`. */
        const silenceAfter = (atMs: number, nowMs: number) => {
          const from = loud.findIndex((at) => at >= atMs);
          if (from < 0) return undefined;
          for (let index = from; index < loud.length; index++) {
            const at = loud[index] ?? 0;
            if ((loud[index + 1] ?? nowMs) - at >= silentMs) return at;
          }
          return undefined;
        };
        /** Waits until `find` names a time, at most `withinMs` past `fromMs`: that time, if any. */
        const awaitTime = Effect.fnUntraced(function* (
          find: (nowMs: number) => number | undefined,
          fromMs: number,
          withinMs: number,
        ) {
          let found: number | undefined;
          yield* pieces.watch(
            Effect.map(now, (nowMs) => {
              found = find(nowMs);
              return found !== undefined;
            }),
            Math.min(deadline, run.origin + fromMs + withinMs),
          );
          return found;
        });

        // What the provider reported: its phases, transcripts, command errors and diagnostics.
        const phases: Array<{ readonly phase: string; readonly atMs: number }> = [];
        const transcripts: Array<{
          readonly atMs: number;
          readonly speaker: "user" | "character";
          readonly final: boolean;
          readonly length: number;
        }> = [];
        const commandErrors: Array<Evidence.CharacterRecord["commandErrors"][number]> = [];
        const diagnostics: Array<{ readonly atMs: number; readonly reason: string }> = [];
        const after = (atMs: number, speaker: "user" | "character") =>
          transcripts.find((entry) => entry.atMs >= atMs && entry.speaker === speaker)?.atMs;

        const samples: Array<Evidence.StatsSample> = [];
        let session: Session.Session | undefined;
        let ready: Option.Option<ViduS2Avatar.State> = Option.none();
        let live: Option.Option<ViduS2Avatar.State> = Option.none();
        let said: Option.Option<void> = Option.none();
        let ended: Option.Option<ViduS2Avatar.CallEnded> = Option.none();
        let liveMs: number | undefined;
        let firstFrame: number | undefined;
        let greetingOnset: number | undefined;
        let greetingSilence: number | undefined;
        let sayMs: number | undefined;
        let answerOnset: number | undefined;

        /** Everything seen so far, into the record and the network section. */
        const recordAll = Effect.gen(function* () {
          const paired = samples.findLast(
            (sample) => sample.local !== undefined && (sample.receivedKbps ?? 0) > 0,
          );
          yield* run.update((evidence) => ({
            ...evidence,
            network: {
              samples: [...samples],
              ...(paired?.local === undefined ? {} : { pair: paired.local }),
            },
          }));
          const callMax = Option.getOrUndefined(live)?.call_max_seconds;
          const characterAnswer = sayMs === undefined ? undefined : after(sayMs, "character");
          const userLine = sayMs === undefined ? undefined : after(sayMs, "user");
          const end = Option.getOrUndefined(ended);
          yield* record((character) => ({
            ...character,
            steps: steps.map((step) => ({
              name: step.name,
              startedMs: since(step.startedMs),
              endedMs: since(step.endedMs),
              outcome: step.outcome,
            })),
            phases: phases.map((entry) => ({
              phase: Probes.keptText(entry.phase),
              atMs: since(entry.atMs),
            })),
            ...(liveMs === undefined
              ? {}
              : {
                  call: {
                    liveMs: since(liveMs),
                    ...(callMax === undefined || callMax === null
                      ? {}
                      : { callMaxSeconds: callMax }),
                    ...(firstFrame === undefined
                      ? {}
                      : { firstFrameMs: pieces.round(firstFrame - liveMs) }),
                    ...(greetingOnset === undefined
                      ? {}
                      : { greetingOnsetMs: pieces.round(greetingOnset - liveMs) }),
                    ...(greetingSilence === undefined
                      ? {}
                      : { greetingSilenceMs: pieces.round(greetingSilence - liveMs) }),
                    video: callVideo.summary(),
                    audio: callAudio.summary(),
                  },
                }),
            ...(sayMs === undefined
              ? {}
              : {
                  say: {
                    sentMs: since(sayMs),
                    ...(userLine === undefined
                      ? {}
                      : { userTranscriptMs: pieces.round(userLine - sayMs) }),
                    ...(answerOnset === undefined
                      ? {}
                      : { onsetMs: pieces.round(answerOnset - sayMs) }),
                    ...(characterAnswer === undefined
                      ? {}
                      : { characterTranscriptMs: pieces.round(characterAnswer - sayMs) }),
                  },
                }),
            ...(end === undefined
              ? {}
              : {
                  end: {
                    endReason: Probes.keptText(end.end_reason),
                    durationSeconds: end.duration_seconds,
                  },
                }),
            transcripts: transcripts.map((entry) => ({ ...entry, atMs: since(entry.atMs) })),
            commandErrors: commandErrors.map((entry) => ({ ...entry, atMs: since(entry.atMs) })),
            diagnostics: diagnostics.map((entry) => ({ ...entry, atMs: since(entry.atMs) })),
            video: video.summary(),
            audio: audio.summary(),
          }));
        });
        /** Closes the session the harness's way, which confirms its end, once. */
        let closed = false;
        const closeSession = Effect.gen(function* () {
          if (closed || session === undefined) return;
          closed = true;
          yield* pieces.close(session);
        });
        // Whatever happens, what was seen goes into the record, and the session is closed.
        yield* Effect.addFinalizer(() => Effect.ignore(Effect.andThen(recordAll, closeSession)));

        /** The call, each operation only once the one before it gave what it needs. */
        const converse = Effect.gen(function* () {
          const created = yield* operate(
            "session",
            30,
            Effect.gen(function* () {
              const opened = yield* reactor.create({
                model: plans.character.model.name,
                tokens: CoordinatorClient.fixedTokens(grant),
                onAllocated: (allocation) =>
                  Effect.gen(function* () {
                    deadline = yield* pieces.allocated(allocation.id, grant);
                    grants.set(allocation.id, grant);
                    allocatedMs =
                      (yield* run.evidence).sessions.find((held) => held.id === allocation.id)
                        ?.allocatedMs ?? (yield* now);
                    yield* record((character) => ({ ...character, allocatedMs }));
                  }),
              });
              session = opened;
              const media = yield* opened.decoded;
              yield* pieces
                .readInto(media.video(ViduS2Avatar.tracks.video), videoFeed)
                .pipe(Effect.forkScoped);
              yield* pieces
                .readInto(media.audio(ViduS2Avatar.tracks.audio), audioFeed)
                .pipe(Effect.forkScoped);
              return opened;
            }),
          );
          if (Option.isNone(created)) return;
          yield* pieces.sampleStats(created.value, samples).pipe(Effect.forkScoped);

          const made = yield* operate(
            "make",
            10,
            ViduS2Avatar.make(created.value, { callTimeout: "30 seconds" }),
          );
          if (Option.isNone(made)) return;
          const avatar = made.value;
          yield* avatar.changes.pipe(
            Stream.runForEach((state) =>
              Effect.flatMap(now, (atMs) =>
                Effect.sync(() => {
                  if (phases.at(-1)?.phase !== state.phase)
                    phases.push({ phase: state.phase, atMs });
                }),
              ),
            ),
            Effect.forkScoped,
          );
          yield* avatar.events({ capacity: 256 }).pipe(
            Stream.runForEach((event) =>
              Effect.flatMap(now, (atMs) =>
                Effect.sync(() => {
                  switch (event._tag) {
                    case "State":
                      // Phases come from the provider's changes, which start with the snapshot.
                      return;
                    case "Transcript":
                      transcripts.push({
                        atMs,
                        speaker: event.transcript.speaker,
                        final: event.transcript.final,
                        length: event.transcript.text.length,
                      });
                      return;
                    case "CommandError":
                      commandErrors.push({
                        atMs,
                        command: Probes.keptText(event.error.command),
                        origin: Probes.keptText(event.error.origin),
                        code: Probes.keptText(event.error.code),
                        retryable: event.error.retryable,
                      });
                      return;
                    case "Diagnostic":
                      diagnostics.push({ atMs, reason: event.error.reason._tag });
                      return;
                  }
                }),
              ),
            ),
            Effect.catch((error) => Effect.ignore(run.mark("events unread", error.reason._tag))),
            Effect.forkScoped,
          );

          ready = yield* operate(
            "createAvatar",
            45,
            avatar.createAvatar({ bytes: photo.bytes, type: `image/${photo.type}` }),
          );
          if (Option.isNone(ready)) return;

          live = yield* operate("startCall", 30, avatar.startCall({ persona, greeting, llm }));
          if (Option.isNone(live)) return;
          // The provider resumed the character's tracks before it returned the call live.
          const liveAt = yield* now;
          liveMs = liveAt;
          inCall = true;
          firstFrame = yield* awaitTime(() => video.firstAfter(liveAt), liveAt, frameWithinMs);
          greetingOnset = yield* awaitTime(() => onsetAfter(liveAt), liveAt, onsetWithinMs);
          const onset = greetingOnset;
          if (onset !== undefined)
            greetingSilence = yield* awaitTime(
              (nowMs) => silenceAfter(onset, nowMs),
              onset,
              silenceWithinMs,
            );

          const sentAt = yield* now;
          sayMs = sentAt;
          said = yield* operate("say", 10, avatar.say(question));
          if (Option.isSome(said))
            answerOnset = yield* awaitTime(() => onsetAfter(sentAt), sentAt, onsetWithinMs);

          inCall = false;
          ended = yield* operate("endCall", 30, avatar.endCall);
        });

        /** What a caller of the provider met, judged from the record. */
        const judgeAll = Effect.gen(function* () {
          /** Why an operation gave nothing: its failure, or that it never ran. */
          const why = (name: string) => {
            const outcome = steps.find((step) => step.name === name)?.outcome;
            return outcome === undefined ? `${name} never ran` : `${name}: ${outcome}`;
          };
          const kept = (yield* run.evidence).character;
          const call = kept?.call;
          const answer = kept?.say;
          yield* pieces.judge("the provider made the avatar from the photo", [
            Option.isSome(ready),
            why("createAvatar"),
          ]);
          yield* pieces.judge("the provider returned the call live", [
            Option.isSome(live),
            why("startCall"),
          ]);
          const pictureFailure = videoFailure(call?.video);
          yield* pieces.judge(
            "the character's picture came after live",
            [call !== undefined, why("startCall")],
            [call?.firstFrameMs !== undefined, `no frame within ${frameWithinMs / 1000} s of live`],
            [pictureFailure === undefined, pictureFailure ?? ""],
          );
          yield* pieces.judge(
            "the character's sound came after live",
            [call !== undefined, why("startCall")],
            [
              call?.greetingOnsetMs !== undefined,
              `no speech within ${onsetWithinMs / 1000} s of live`,
            ],
          );
          yield* pieces.judge(
            "the character answered a say",
            [Option.isSome(said), why("say")],
            [
              answer?.onsetMs !== undefined || answer?.characterTranscriptMs !== undefined,
              `neither speech nor a character transcript within ${onsetWithinMs / 1000} s of the say`,
            ],
          );
          yield* pieces.judge("endCall returned the call's end", [
            Option.isSome(ended),
            why("endCall"),
          ]);
        });
        yield* converse;
        yield* recordAll;
        yield* judgeAll;
        yield* closeSession;
        yield* run.mark("character observed");
      }),
    // Whatever failed, the key ends a session the check allocated.
    pieces.endHeld(keyed),
  );
});
