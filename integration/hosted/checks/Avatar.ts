/**
 * `avatar`: a question for Reactor, not a qualification. Vidu S2-Avatar
 * (`reactor/vidu-s2-avatar`) makes a character from a photo, and it answers on
 * `main_video` and `main_audio`. Reactor documents its commands and messages,
 * and leaves open what a refused command answers on the wire, whether its
 * `command_error` comes before that answer, what a command carrying an
 * explicit null does, how long its slower commands take to answer, and
 * whether a call's picture flows without the outputs being resumed after
 * `live`. The check drives one session through the raw `Session`, never a
 * module of the model's, and records what it meets, so that the SDK's module
 * for the model can be designed from it.
 *
 * Everything the session publishes is observed from its allocation, before it
 * connects, so the `session_state` it sends on connect is seen. Each step runs
 * within its own time limit; one that fails is a failed criterion named after
 * it, and the steps after it run where they still make sense. No command's
 * outcome stops the run, unknown or not: a command bills nothing beyond the
 * session's time, the session is ended either way, and some are sent to see
 * whether they are answered at all.
 *
 * The planned timeline, from the allocation A, at the timing Reactor documents
 * (an avatar ready within a few seconds, a call live usually within 5 s):
 *
 *   A+2    connected: the schema, the first snapshot, get_state, list_voices,
 *          clone_voice with a name it refuses, and say before any call
 *   A+5    the photo uploads, and create_avatar makes the avatar from it
 *   A+11   start_call with a greeting; live about A+16, when its picture must
 *          come within 5 s, after one reconnect at most
 *   A+17   the greeting; say, and an interrupt 1.5 s into the answer's sound
 *   A+27   update_call with no field, say with no text, update_call with a
 *          null voice, and update_call to another voice
 *   A+38   end_call, then attach_avatar with the avatar's id
 *   A+43   a second call without a greeting: live, its picture, a say, end_call
 *   A+55   get_state, and the session is closed
 *
 * That ends about 55 s inside the 110 s work deadline.
 */
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import type { AudioFrame, Recorded, VideoFrame } from "reactor-effect-client/Media";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { CommandFailure } from "reactor-effect-client/ReactorError";
import type * as Session from "reactor-effect-client/Session";
import type { Pieces } from "../Checks.js";
import type * as Evidence from "../Evidence.js";
import { SaveFailed } from "../Ledger.js";
import * as Media from "../Media.js";
import * as Probes from "../Probes.js";
import { describe, Run } from "../Run.js";
import { plans } from "../Spend.js";
import type { Photo } from "../Target.js";
import { Target } from "../Target.js";

/** Vidu S2-Avatar takes an uploaded image under 20 MB. */
export const maxPhotoBytes = 20_000_000;

/** How each photo type uploads. */
const kinds: Readonly<
  Record<Photo["type"], { readonly mimeType: string; readonly extension: string }>
> = {
  png: { mimeType: "image/png", extension: "png" },
  jpeg: { mimeType: "image/jpeg", extension: "jpg" },
  webp: { mimeType: "image/webp", extension: "webp" },
};

/** A photo's type by its first bytes: PNG's signature, JPEG's start of image, or WebP's RIFF header. */
export const photoType = (bytes: Uint8Array): Photo["type"] | undefined => {
  const at = (offset: number, expected: ReadonlyArray<number>) =>
    expected.every((byte, index) => bytes[offset + index] === byte);
  if (at(0, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])) return "png";
  if (at(0, [0xff, 0xd8, 0xff])) return "jpeg";
  // "RIFF", the file's length, then "WEBP".
  if (at(0, [0x52, 0x49, 0x46, 0x46]) && at(8, [0x57, 0x45, 0x42, 0x50])) return "webp";
  return undefined;
};

const tracks = { video: "main_video", audio: "main_audio" } as const;
/** Who the character is: short, so that each answer is. */
const persona = "You are Probe, a friendly guide. Answer in one short sentence.";
const greeting = "Say hello in one short sentence.";
/** Caps each reply below the model's default of 50 tokens. */
const llm = { max_tokens: 30 };
/** What the first call is asked: an answer long enough to be cut short. */
const counting = "Please count slowly from one to twenty.";
/** What the second call is asked. */
const sea = "Say one short sentence about the sea.";
/** How long after a command's answer the broadcasts that follow it are waited for. */
const windowMs = 2_000;
/** A call's first frame must come this long after live, or after the one reconnect's ready. */
const frameWithinMs = 5_000;
/** How long the character's sound is waited for after a call is live, or after a say. */
const onsetWithinMs = 8_000;
/** How long an answer is given to fall silent. */
const answerWithinMs = 10_000;
/** The interrupt goes this long into the counting answer's sound. */
const interruptAfterMs = 1_500;
/** How long a greeting's or a cut answer's transcript is waited for once its sound stopped. */
const transcriptWithinMs = 3_000;
/** How long a call is watched for picture and sound after `ended`. */
const afterEndedMs = 2_000;
/** How long a phase the docs promise is waited for: an avatar ready, a call live. */
const phaseWithinMs = 30_000;
/** The sound's level is taken over 100 ms; 0.01 RMS (-40 dBFS) or more is speech. */
const binMs = 100;
const speechRms = 0.01;
/** 300 ms below speech is silence. */
const silentBins = 3;
/** Events, frames and blocks held in memory; a session that sends more is past what the check reads. */
const maxHeard = 4_096;
const maxArrivals = 100_000;
/** The documented `session_state` fields, in the docs' order. */
const documented = [
  "phase",
  "avatar_id",
  "avatar_status",
  "avatar_name",
  "voice",
  "persona_set",
  "call_mode",
  "warmup_attempts",
  "call_started_at",
  "call_max_seconds",
  "call_elapsed_seconds",
  "control_ready",
  "video_receiving",
  "audio_receiving",
  "mic_forwarding",
  "camera_forwarding",
  "last_frame_age_ms",
  "reference_images",
  "end_reason",
  "last_error",
] as const;
const documentedFields: ReadonlySet<string> = new Set(documented);
/** Its text fields that are codes; any other text in it is the provider's. */
const codeFields: ReadonlySet<string> = new Set(["phase", "end_reason", "call_mode"]);
/** Its fields that change as a call runs on, left out when two snapshots are compared. */
const clockFields: ReadonlySet<string> = new Set(["call_elapsed_seconds", "last_frame_age_ms"]);

const round = (value: number) => Math.round(value * 10) / 10;

type AvatarRecord = Evidence.AvatarRecord;
type AvatarEvent = AvatarRecord["events"][number];
type Phases = Evidence.AvatarCall["phases"];

/** A session event as the check holds it: when, its place, its request, and a model message. */
interface Heard {
  readonly atMs: number;
  readonly sequence: bigint;
  readonly requestId: string | undefined;
  readonly event: Omit<AvatarEvent, "atMs">;
  /** A model message, held in memory and never kept: its type, its data, and whether it was broadcast. */
  readonly message:
    | { readonly type: string; readonly data: Schema.JsonObject; readonly broadcast: boolean }
    | undefined;
}

/** A transcript as the check holds it: never its text. */
interface Transcript {
  readonly atMs: number;
  readonly speaker: string | undefined;
  readonly final: boolean | null;
  readonly length: number;
}

/** A command as it was sent and settled, on the run's clock. */
interface Sent {
  readonly sentMs: number;
  readonly answeredMs: number;
  readonly result: Result.Result<Session.CommandReply, CommandFailure>;
  /** The request its answer named, when one came. */
  readonly answer: string | undefined;
}

/** Picture and sound from a call's live until `end_call` was sent. */
interface Window {
  readonly video: Media.VideoLog;
  readonly audio: Media.AudioLog;
}

/** A call as the check runs it, its times on the run's clock. */
interface Call {
  readonly start: Sent;
  phases: Phases;
  liveMs?: number;
  atLive?: Schema.JsonObject;
  window?: Window;
  /** When `end_call` was sent, which closed the window. */
  closedMs?: number;
  /** Until when its sound was watched. */
  untilMs?: number;
  reconnect?: {
    readonly startedMs: number;
    readonly readyMs?: number;
    readonly failure?: string;
    readonly firstFrameMs?: number;
  };
  say?: { readonly sent: Sent; onsetMs?: number };
  end?: {
    readonly sent: Sent;
    phases: Phases;
    afterEnded?: { readonly forMs: number; readonly frames: number; readonly blocks: number };
  };
}

/** A value's field, when the value is an object. */
const field = (value: unknown, key: string): unknown =>
  Predicate.isObject(value) ? value[key] : undefined;

/** A value's text field. */
const textOf = (value: unknown, key: string): string | undefined => {
  const text = field(value, key);
  return Predicate.isString(text) ? text : undefined;
};

const isList = Schema.is(Schema.Array(Schema.Unknown));

/** A text as the evidence keeps it, or nothing for a value that is no text. */
const codeOf = (value: unknown): string | undefined =>
  Predicate.isString(value) ? Probes.keptText(value) : undefined;

/** A session event as the evidence keeps it: tags, kinds, correlations and codes. */
const eventOf = (event: Session.SessionEvent): Omit<AvatarEvent, "atMs"> => {
  switch (event._tag) {
    case "Model":
      return {
        tag: event._tag,
        kind: event.kind,
        ...(event.kind === "message" ? { type: Probes.keptText(event.type) } : {}),
        correlation: event.correlation,
      };
    case "CommandError": {
      const reason = event.error.reason;
      const code =
        reason._tag === "Remote" && reason.remoteCode !== undefined
          ? reason.remoteCode.pipe(Redacted.value, Probes.keptText)
          : undefined;
      return {
        tag: event._tag,
        correlation: event.correlation,
        reason: reason._tag,
        ...(code === undefined ? {} : { code }),
      };
    }
    case "Control":
      return { tag: event._tag, correlation: event.correlation, detail: event.message._tag };
    case "Status":
      return { tag: event._tag, detail: event.status };
    case "Track":
    case "Decoded":
      return { tag: event._tag, detail: Probes.keptText(event.name) };
    case "Diagnostic":
      return { tag: event._tag, reason: event.error.reason._tag };
    case "Moderation":
      return { tag: event._tag, detail: Probes.keptText(event.action) };
    case "Upload":
      return { tag: event._tag, detail: event.progress.notification };
  }
};

const heardOf = (event: Session.SessionEvent, atMs: number): Heard => ({
  atMs,
  sequence: event.sequence,
  requestId: event._tag === "Model" || event._tag === "CommandError" ? event.requestId : undefined,
  event: eventOf(event),
  message:
    event._tag === "Model" && event.kind === "message"
      ? {
          type: event.type,
          data: event.data ?? {},
          broadcast: event.correlation === "unsolicited",
        }
      : undefined,
});

/** The data of a model message of `type` that was heard. */
const messageOf = (heard: Heard, type: string) =>
  heard.message?.type === type ? heard.message.data : undefined;

const phaseOf = (heard: Heard) => textOf(messageOf(heard, "session_state"), "phase");

/** Every transcript among what was heard. */
const transcriptsOf = (all: ReadonlyArray<Heard>): ReadonlyArray<Transcript> =>
  all.flatMap((heard) => {
    const data = messageOf(heard, "transcript");
    if (data === undefined) return [];
    const final = field(data, "final");
    return [
      {
        atMs: heard.atMs,
        speaker: textOf(data, "speaker"),
        final: Predicate.isBoolean(final) ? final : null,
        length: textOf(data, "text")?.length ?? 0,
      },
    ];
  });

/** A `session_state`'s booleans and numbers, and its code fields, under keys the evidence may keep. */
const valuesOf = (data: Schema.JsonObject): Record<string, string | number | boolean> => {
  const values: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(data)) {
    if (!Probes.readable(key)) continue;
    if (Predicate.isBoolean(value) || (Predicate.isNumber(value) && Number.isFinite(value)))
      values[key] = value;
    else if (Predicate.isString(value) && codeFields.has(key)) values[key] = Probes.keptText(value);
  }
  return values;
};

/** Which documented fields a `session_state` set, set null or left out, and what else it named. */
const fieldsOf = (data: Schema.JsonObject) => {
  const present: Array<string> = [];
  const nulls: Array<string> = [];
  const absent: Array<string> = [];
  for (const key of documented) {
    if (!Object.hasOwn(data, key)) absent.push(key);
    else if (data[key] === null) nulls.push(key);
    else present.push(key);
  }
  const undocumented = Object.keys(data)
    .filter((key) => !documentedFields.has(key))
    .map(Probes.keptText);
  return { present, nulls, absent, undocumented, values: valuesOf(data) };
};

/** The phases reported from `from` on, as each changed, from `sentMs`. */
const phasesFrom = (all: ReadonlyArray<Heard>, from: number, sentMs: number): Phases => {
  let phase = all.slice(0, from).map(phaseOf).findLast(Predicate.isNotUndefined);
  const phases: Array<Phases[number]> = [];
  for (const heard of all.slice(from)) {
    const next = phaseOf(heard);
    if (next === undefined || next === phase) continue;
    phase = next;
    phases.push({ phase: Probes.keptText(next), afterMs: round(heard.atMs - sentMs) });
  }
  return phases;
};

/** What a command met on the wire, its times from `since`. */
const wireOf = (sent: Sent, since: (atMs: number) => number): Evidence.AvatarWire => {
  const at = { sentMs: since(sent.sentMs), answeredMs: since(sent.answeredMs) };
  if (Result.isSuccess(sent.result)) {
    const reply = sent.result.success;
    return reply.kind === "ack"
      ? { ...at, wire: "ack" }
      : { ...at, wire: "message", type: Probes.keptText(reply.type) };
  }
  const { reason, context } = sent.result.failure;
  switch (reason._tag) {
    case "Remote":
      return {
        ...at,
        wire: "error",
        ...(reason.remoteCode === undefined
          ? {}
          : { code: reason.remoteCode.pipe(Redacted.value, Probes.keptText) }),
        outcome: context.outcome,
      };
    case "Timeout":
      return { ...at, wire: "timeout", outcome: context.outcome };
    default:
      return { ...at, wire: "failed", reason: reason._tag, outcome: context.outcome };
  }
};

/** A wire outcome in a criterion's words. */
const wireText = (wire: Evidence.AvatarWire) =>
  [wire.wire, wire.type ?? wire.code ?? wire.reason].filter(Predicate.isNotUndefined).join(" ");

/** The request a command's answer named: a reply, or an error frame; none when it was not answered. */
const answerOf = (result: Sent["result"]): string | undefined => {
  if (Result.isSuccess(result)) return result.success.requestId;
  return result.failure.context.outcome === "replied"
    ? result.failure.context.requestId
    : undefined;
};

/** The data of a command's reply, when it was a message. */
const replyOf = (sent: Sent) =>
  Result.isSuccess(sent.result) && sent.result.success.kind === "message"
    ? sent.result.success.data
    : undefined;

/** Why a call never went live, or undefined when it did. */
const notLive = (call: Call | undefined): string | undefined => {
  if (call === undefined) return "the call was never started";
  if (call.liveMs !== undefined) return undefined;
  return `start_call reached ${call.phases.at(-1)?.phase ?? "no phase"}`;
};

/** Why a call's window did not show the character's picture, or undefined when it did. */
const videoFailure = (call: Call | undefined): string | undefined => {
  if (call?.window === undefined) return notLive(call) ?? "the call never went live";
  const video = call.window.video.summary();
  if (video.frames === 0) return "no frame arrived while the call was live";
  if (video.lit === 0) return "every frame was black";
  return video.distinct < 2 ? "the frames never changed" : undefined;
};

export const avatar = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const photo = target.photo;
  // A paid run is refused without one before it starts; nothing is opened, so nothing is spent.
  if (photo === undefined)
    return yield* pieces.judge("a photo to make the avatar from", [
      false,
      "no photo was given: a paid run needs --avatar-image",
    ]);
  const initial: AvatarRecord = {
    photo: { bytes: photo.bytes.length, type: photo.type },
    refusals: [],
    calls: [],
    states: [],
    windows: [],
    transcripts: [],
    messages: {},
    events: [],
  };
  yield* run.update((evidence) => ({ ...evidence, avatar: initial }));
  const record = (change: (avatar: AvatarRecord) => AvatarRecord) =>
    run.update((evidence) => ({ ...evidence, avatar: change(evidence.avatar ?? initial) }));
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const grant = yield* pieces.mint("avatar");
  // The photo may be as large as Vidu takes, past the SDK's default bound on an upload.
  const reactor = yield* Reactor.make({ maxUploadBytes: maxPhotoBytes });

  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        const heard = yield* SubscriptionRef.make<ReadonlyArray<Heard>>([]);
        const heardNow = SubscriptionRef.get(heard);
        let deadline = Number.POSITIVE_INFINITY;
        let allocatedMs = 0;
        /** A time on the run's clock as the record keeps it: from the allocation. */
        const since = (atMs: number) => round(atMs - allocatedMs);
        const now = run.now;

        /** Runs a step within `seconds` and the work deadline; a failure it meets is its criterion. */
        const step = <A, E, R>(name: string, seconds: number, body: Effect.Effect<A, E, R>) =>
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
            yield* run.judge(name, describe(exit.cause));
            yield* run.mark(`${name} failed`);
            return Option.none<A>();
          });
        /** The first `find` picks among what was heard from `from` on, until `atMs` on the run's clock. */
        const awaitHeard = <B>(from: number, find: (heard: Heard) => B | undefined, atMs: number) =>
          pieces
            .waitFor(
              heard,
              (all) => {
                for (const each of all.slice(from)) {
                  const found = find(each);
                  if (found !== undefined) return found;
                }
                return undefined;
              },
              Math.min(deadline, run.origin + atMs),
            )
            .pipe(Effect.option);

        // The picture and sound, every frame's and block's arrival, and the sound's level each
        // 100 ms, from the connection to the close.
        const video = Media.videoLog();
        const audio = Media.audioLog();
        const frameTimes: Array<number> = [];
        const blockTimes: Array<number> = [];
        const bins = new Map<number, { readonly power: number; readonly samples: number }>();
        const windows = new Set<Window>();
        const videoFeed = {
          add: (element: Recorded<VideoFrame>, atMs: number) => {
            video.add(element, atMs);
            for (const open of windows) open.video.add(element, atMs);
            if (element._tag === "Frame" && frameTimes.length < maxArrivals) frameTimes.push(atMs);
          },
        };
        const audioFeed = {
          add: (element: Recorded<AudioFrame>, atMs: number) => {
            audio.add(element);
            for (const open of windows) open.audio.add(element);
            if (element._tag !== "Frame") return;
            if (blockTimes.length < maxArrivals) blockTimes.push(atMs);
            const index = Math.floor(atMs / binMs);
            const bin = bins.get(index) ?? { power: 0, samples: 0 };
            let power = bin.power;
            for (const sample of element.frame.samples) power += (sample / 32768) ** 2;
            bins.set(index, { power, samples: bin.samples + element.frame.samples.length });
          },
        };
        /** Reads a connection's picture and sound until it is replaced or closed. */
        const listen = (session: Session.Session) =>
          Effect.gen(function* () {
            const decoded = yield* session.decoded;
            yield* pieces.readInto(decoded.video(tracks.video), videoFeed).pipe(Effect.forkScoped);
            yield* pieces.readInto(decoded.audio(tracks.audio), audioFeed).pipe(Effect.forkScoped);
          });
        const between = (times: ReadonlyArray<number>, fromMs: number, toMs: number) =>
          times.filter((atMs) => atMs >= fromMs && atMs < toMs).length;
        const rmsOf = (index: number): number | undefined => {
          const bin = bins.get(index);
          return bin === undefined || bin.samples === 0
            ? undefined
            : Math.sqrt(bin.power / bin.samples);
        };
        const loud = (index: number) => (rmsOf(index) ?? 0) >= speechRms;
        /** The start of the first 100 ms of speech at or after `atMs`, among the levels closed by `nowMs`. */
        const onsetAfter = (atMs: number, nowMs: number): number | undefined => {
          for (let index = Math.ceil(atMs / binMs); (index + 1) * binMs <= nowMs; index++)
            if (loud(index)) return index * binMs;
          return undefined;
        };
        /**
         * The start of the first 300 ms of silence at or after `atMs`, among the levels closed
         * 100 ms before `nowMs`, so that a block still on its way is not taken for silence.
         */
        const silenceAfter = (atMs: number, nowMs: number): number | undefined => {
          const first = Math.ceil(atMs / binMs);
          for (let index = first; (index + silentBins + 1) * binMs <= nowMs; index++)
            if (!loud(index) && !loud(index + 1) && !loud(index + 2)) return index * binMs;
          return undefined;
        };
        /** Waits until `find` names a time, from `atMs` until `withinMs` after it: that time, if any. */
        const awaitLevel = Effect.fnUntraced(function* (
          find: (atMs: number, nowMs: number) => number | undefined,
          atMs: number,
          withinMs: number,
        ) {
          let found: number | undefined;
          yield* pieces.watch(
            Effect.map(now, (nowMs) => {
              found = find(atMs, nowMs);
              return found !== undefined;
            }),
            Math.min(deadline, run.origin + atMs + withinMs),
          );
          return found;
        });
        /** Each stretch of speech from `fromMs` to `toMs`: loud levels, ended by 300 ms of silence. */
        const speechBetween = (fromMs: number, toMs: number) => {
          const runs: Array<{ readonly fromMs: number; readonly toMs: number }> = [];
          let start: number | undefined;
          let lastLoud = 0;
          for (let index = Math.floor(fromMs / binMs); index * binMs < toMs; index++) {
            if (loud(index)) {
              start ??= index;
              lastLoud = index;
            } else if (start !== undefined && index - lastLoud >= silentBins) {
              runs.push({ fromMs: since(start * binMs), toMs: since((lastLoud + 1) * binMs) });
              start = undefined;
            }
          }
          if (start !== undefined)
            runs.push({ fromMs: since(start * binMs), toMs: since((lastLoud + 1) * binMs) });
          return runs;
        };

        // The session, observed from its allocation: the snapshot it sends on connect comes
        // before the create returns.
        const created = yield* step(
          "session",
          30,
          Effect.gen(function* () {
            const session = yield* reactor.create({
              model: plans.avatar.model.name,
              tokens: CoordinatorClient.fixedTokens(grant),
              onAllocated: (allocation) =>
                Effect.gen(function* () {
                  deadline = yield* pieces.allocated(allocation.id, grant);
                  grants.set(allocation.id, grant);
                  allocatedMs =
                    (yield* run.evidence).sessions.find((held) => held.id === allocation.id)
                      ?.allocatedMs ?? (yield* now);
                  yield* record((avatar) => ({ ...avatar, allocatedMs }));
                  const observation = yield* allocation.observe({ capacity: maxHeard });
                  yield* observation.events.pipe(
                    Stream.runForEach((event) =>
                      Effect.flatMap(now, (atMs) =>
                        SubscriptionRef.update(heard, (all) =>
                          all.length >= maxHeard ? all : [...all, heardOf(event, atMs)],
                        ),
                      ),
                    ),
                    Effect.catch((error) =>
                      Effect.ignore(run.mark("events unread", error.reason._tag)),
                    ),
                    Effect.forkScoped,
                  );
                }),
            });
            yield* run.mark("connected");
            yield* listen(session);
            return session;
          }),
        );
        if (Option.isNone(created)) return;
        const session = created.value;
        const samples: Array<Evidence.StatsSample> = [];
        yield* pieces.sampleStats(session, samples).pipe(Effect.forkScoped);

        const calls: Array<Call> = [];
        let avatarId: string | undefined;
        let voiceIds: ReadonlyArray<string> = [];
        /** `call_ended`'s reason as a code, and its duration. */
        const endOf = (sent: Sent) => {
          const data = replyOf(sent);
          const endReason = codeOf(field(data, "end_reason"));
          const duration = field(data, "duration_seconds");
          return {
            ...(endReason === undefined ? {} : { endReason }),
            ...(Predicate.isNumber(duration) && Number.isFinite(duration)
              ? { durationSeconds: duration }
              : {}),
          };
        };
        /** A call as the evidence keeps it, from what had arrived by `nowMs`. */
        const callRecord = (
          call: Call,
          transcripts: ReadonlyArray<Transcript>,
          nowMs: number,
        ): Evidence.AvatarCall => {
          const { liveMs, window, reconnect, say, end } = call;
          const untilMs = call.untilMs ?? call.closedMs ?? nowMs;
          const firstFrame = liveMs === undefined ? undefined : video.firstAfter(liveMs);
          const firstBlock =
            liveMs === undefined ? undefined : blockTimes.find((atMs) => atMs >= liveMs);
          const userTranscript =
            say === undefined
              ? undefined
              : transcripts.find(
                  (entry) => entry.atMs >= say.sent.sentMs && entry.speaker === "user",
                );
          const fromBin = Math.floor((liveMs ?? untilMs) / binMs);
          const toBin = Math.ceil(untilMs / binMs);
          return {
            start: wireOf(call.start, since),
            phases: call.phases,
            ...(liveMs === undefined ? {} : { liveMs: since(liveMs) }),
            ...(call.atLive === undefined ? {} : { atLive: valuesOf(call.atLive) }),
            ...(firstFrame === undefined || liveMs === undefined
              ? {}
              : { firstFrameMs: round(firstFrame - liveMs) }),
            ...(firstBlock === undefined || liveMs === undefined
              ? {}
              : { firstBlockMs: round(firstBlock - liveMs) }),
            ...(reconnect === undefined
              ? {}
              : {
                  reconnect: {
                    startedMs: since(reconnect.startedMs),
                    ...(reconnect.readyMs === undefined
                      ? {}
                      : { readyMs: since(reconnect.readyMs) }),
                    ...(reconnect.failure === undefined ? {} : { failure: reconnect.failure }),
                    ...(reconnect.firstFrameMs === undefined
                      ? {}
                      : { firstFrameMs: round(reconnect.firstFrameMs) }),
                  },
                }),
            ...(window === undefined
              ? {}
              : { video: window.video.summary(), audio: window.audio.summary() }),
            ...(liveMs === undefined
              ? {}
              : {
                  levels: {
                    fromMs: since(fromBin * binMs),
                    rms: Array.from({ length: Math.max(0, toBin - fromBin) }, (_, offset) => {
                      const rms = rmsOf(fromBin + offset);
                      return rms === undefined ? null : Math.round(rms * 1e4) / 1e4;
                    }),
                  },
                }),
            speech: liveMs === undefined ? [] : speechBetween(liveMs, untilMs),
            ...(say === undefined
              ? {}
              : {
                  say: {
                    ...wireOf(say.sent, since),
                    ...(userTranscript === undefined
                      ? {}
                      : { userTranscriptMs: round(userTranscript.atMs - say.sent.sentMs) }),
                    ...(say.onsetMs === undefined
                      ? {}
                      : { onsetMs: round(say.onsetMs - say.sent.sentMs) }),
                  },
                }),
            ...(end === undefined
              ? {}
              : {
                  end: {
                    ...wireOf(end.sent, since),
                    ...endOf(end.sent),
                    phases: end.phases,
                    ...(end.afterEnded === undefined ? {} : { afterEnded: end.afterEnded }),
                  },
                }),
          };
        };
        const saveCalls = Effect.gen(function* () {
          const transcripts = transcriptsOf(yield* heardNow);
          const nowMs = yield* now;
          yield* record((avatar) => ({
            ...avatar,
            calls: calls.map((call) => callRecord(call, transcripts, nowMs)),
          }));
        });
        /** Everything heard and seen so far, into the record and the network section. */
        const recordAll = Effect.gen(function* () {
          const all = yield* heardNow;
          const nowMs = yield* now;
          const messages: Record<string, number> = {};
          const changes: Array<{ readonly phase: string; readonly atMs: number }> = [];
          for (const each of all) {
            if (each.message !== undefined) {
              const type = Probes.keptText(each.message.type);
              messages[type] = (messages[type] ?? 0) + 1;
            }
            const phase = phaseOf(each);
            if (phase !== undefined && phase !== changes.at(-1)?.phase)
              changes.push({ phase, atMs: each.atMs });
          }
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
          const transcripts = transcriptsOf(all);
          yield* record((avatar) => ({
            ...avatar,
            calls: calls.map((call) => callRecord(call, transcripts, nowMs)),
            states: all.flatMap((each) => {
              const data = messageOf(each, "session_state");
              if (data === undefined) return [];
              const via: "broadcast" | "reply" =
                each.message?.broadcast === true ? "broadcast" : "reply";
              return [{ atMs: since(each.atMs), via, values: valuesOf(data) }];
            }),
            windows: changes.map((change, index) => {
              const toMs = changes[index + 1]?.atMs ?? nowMs;
              return {
                phase: Probes.keptText(change.phase),
                fromMs: since(change.atMs),
                toMs: since(toMs),
                frames: between(frameTimes, change.atMs, toMs),
                blocks: between(blockTimes, change.atMs, toMs),
              };
            }),
            transcripts: transcripts.map((entry) => ({
              atMs: since(entry.atMs),
              ...(entry.speaker === undefined ? {} : { speaker: Probes.keptText(entry.speaker) }),
              final: entry.final,
              length: entry.length,
            })),
            messages,
            events: all.map((each) => ({ atMs: since(each.atMs), ...each.event })),
            video: video.summary(),
            audio: audio.summary(),
          }));
        });
        // Whatever happens, what was seen goes into the record, and the session is closed the
        // harness's way, which confirms its end.
        let closed = false;
        yield* Effect.addFinalizer(() =>
          Effect.ignore(
            Effect.gen(function* () {
              yield* recordAll;
              if (closed) return;
              closed = true;
              yield* pieces.close(session);
            }),
          ),
        );

        /**
         * Sends a command and keeps what it met. Its answer reaches the observer a moment after
         * the command returns, so this waits until it is heard: whatever is read next is ordered
         * after it.
         */
        const send = Effect.fnUntraced(function* (
          name: string,
          data: Schema.JsonObject,
          options?: Session.CommandOptions,
        ) {
          const from = (yield* heardNow).length;
          const sentMs = yield* now;
          const result = yield* Effect.result(session.command(name, data, options));
          const answeredMs = yield* now;
          const answer = answerOf(result);
          if (answer !== undefined)
            yield* awaitHeard(
              from,
              (each) => (each.requestId === answer ? each : undefined),
              answeredMs + windowMs,
            );
          return { sentMs, answeredMs, result, answer } satisfies Sent;
        });
        /**
         * Sends a command that should be refused, and keeps what came back on the wire and what
         * was broadcast: its `command_error` and the next `session_state`, waited for until both
         * came or 2 s after its answer.
         */
        const refuse = Effect.fnUntraced(function* (
          probe: string,
          name: string,
          data: Schema.JsonObject,
          options?: Session.CommandOptions,
        ) {
          const from = (yield* heardNow).length;
          const sent = yield* send(name, data, options);
          const isError = (each: Heard) => messageOf(each, "command_error") !== undefined;
          const isState = (each: Heard) => messageOf(each, "session_state") !== undefined;
          yield* pieces
            .waitFor(
              heard,
              (all) => {
                const after = all.slice(from);
                return after.some(isError) && after.some(isState) ? true : undefined;
              },
              Math.min(deadline, run.origin + sent.answeredMs + windowMs),
            )
            .pipe(Effect.option);
          const after = (yield* heardNow).slice(from);
          const answer =
            sent.answer === undefined
              ? undefined
              : after.find((each) => each.requestId === sent.answer);
          const error = after.find(isError);
          const errorData = error === undefined ? undefined : messageOf(error, "command_error");
          const state = after.find(isState);
          const lastError = field(
            state === undefined ? undefined : messageOf(state, "session_state"),
            "last_error",
          );
          const retryable = field(errorData, "retryable");
          const code = codeOf(field(errorData, "code"));
          const origin = codeOf(field(errorData, "origin"));
          const command = codeOf(field(errorData, "command"));
          const lastCode = codeOf(field(lastError, "code"));
          return {
            probe,
            command: name,
            ...wireOf(sent, since),
            ...(error === undefined
              ? {}
              : {
                  commandError: {
                    atMs: since(error.atMs),
                    ...(code === undefined ? {} : { code }),
                    ...(origin === undefined ? {} : { origin }),
                    ...(command === undefined ? {} : { command }),
                    ...(Predicate.isBoolean(retryable) ? { retryable } : {}),
                    traceId: (textOf(errorData, "trace_id") ?? "").length > 0,
                    ...(answer === undefined
                      ? {}
                      : { beforeAnswer: error.sequence < answer.sequence }),
                  },
                }),
            ...(state === undefined
              ? {}
              : {
                  state: {
                    atMs: since(state.atMs),
                    lastError: lastError !== undefined && lastError !== null,
                    ...(lastCode === undefined ? {} : { code: lastCode }),
                  },
                }),
          } satisfies Evidence.AvatarRefusal;
        });
        const refused = (refusal: Evidence.AvatarRefusal) =>
          record((avatar) => ({ ...avatar, refusals: [...avatar.refusals, refusal] }));
        /** The session's state as `get_state` answers it now. */
        const stateNow = Effect.gen(function* () {
          const sent = yield* send("get_state", {});
          const data = replyOf(sent);
          if (data === undefined)
            return yield* ReactorError.fromCode(
              "InvalidState",
              `get_state answered ${wireText(wireOf(sent, since))}`,
            );
          return data;
        });
        /** The latest `session_state` heard. */
        const latestState = Effect.map(heardNow, (all) => {
          const last = all.findLast((each) => messageOf(each, "session_state") !== undefined);
          return last === undefined ? undefined : messageOf(last, "session_state");
        });
        /** The first call, once it went live, and when. */
        const firstLive = Effect.suspend(() => {
          const call = calls[0];
          return call?.liveMs === undefined
            ? Effect.fail(ReactorError.fromCode("InvalidState", "the first call never went live"))
            : Effect.succeed({ call, liveMs: call.liveMs });
        });

        /**
         * Waits for a call's first frame after live; with none in 5 s, reconnects once and waits
         * as long after the new connection's ready. Whether a call's picture needs its tracks
         * resumed once it is live is what this asks.
         */
        const awaitPicture = Effect.fnUntraced(function* (call: Call, liveMs: number) {
          yield* pieces.watch(
            Effect.sync(() => video.firstAfter(liveMs) !== undefined),
            Math.min(deadline, run.origin + liveMs + frameWithinMs),
          );
          if (video.firstAfter(liveMs) !== undefined) return;
          const startedMs = yield* now;
          call.reconnect = { startedMs };
          yield* run.mark("reconnecting for the picture");
          const reconnected = yield* Effect.exit(session.reconnect);
          if (Exit.isFailure(reconnected)) {
            call.reconnect = { startedMs, failure: describe(reconnected.cause) };
            return;
          }
          const readyMs = yield* now;
          call.reconnect = { startedMs, readyMs };
          yield* listen(session);
          yield* pieces.watch(
            Effect.sync(() => video.firstAfter(readyMs) !== undefined),
            Math.min(deadline, run.origin + readyMs + frameWithinMs),
          );
          const first = video.firstAfter(readyMs);
          if (first !== undefined)
            call.reconnect = { startedMs, readyMs, firstFrameMs: first - readyMs };
        });
        /**
         * Starts a call and follows it to live, failed or ended, or to a `command_error` that
         * refuses it; at live its window opens.
         */
        const startCall = Effect.fnUntraced(function* (data: Schema.JsonObject) {
          const from = (yield* heardNow).length;
          const sent = yield* send("start_call", data);
          const call: Call = { start: sent, phases: [] };
          calls.push(call);
          const settled = yield* awaitHeard(
            from,
            (each) => {
              if (textOf(messageOf(each, "command_error"), "command") === "start_call") return each;
              const phase = phaseOf(each);
              return phase === "live" || phase === "failed" || phase === "ended" ? each : undefined;
            },
            sent.sentMs + phaseWithinMs,
          );
          call.phases = phasesFrom(yield* heardNow, from, sent.sentMs);
          if (Option.isNone(settled) || phaseOf(settled.value) !== "live") return call;
          const live = settled.value;
          const atLive = messageOf(live, "session_state");
          call.liveMs = live.atMs;
          if (atLive !== undefined) call.atLive = atLive;
          const window = { video: Media.videoLog(), audio: Media.audioLog() };
          windows.add(window);
          call.window = window;
          yield* run.mark("live", `call ${calls.length}`);
          yield* awaitPicture(call, live.atMs);
          return call;
        });
        /** Ends a call: closes its window, sends `end_call`, and watches what follows `ended`. */
        const endCall = Effect.fnUntraced(function* (call: Call) {
          call.closedMs = yield* now;
          if (call.window !== undefined) windows.delete(call.window);
          const from = (yield* heardNow).length;
          const sent = yield* send("end_call", {});
          const end: NonNullable<Call["end"]> = { sent, phases: [] };
          call.end = end;
          const ended = yield* awaitHeard(
            from,
            (each) => (phaseOf(each) === "ended" ? each : undefined),
            sent.answeredMs + windowMs,
          );
          end.phases = phasesFrom(yield* heardNow, from, sent.sentMs);
          if (Option.isSome(ended)) {
            const endedMs = ended.value.atMs;
            yield* pieces.sleepUntil(endedMs + afterEndedMs, deadline);
            const untilMs = yield* now;
            end.afterEnded = {
              forMs: round(untilMs - endedMs),
              frames: between(frameTimes, endedMs, untilMs),
              blocks: between(blockTimes, endedMs, untilMs),
            };
            call.untilMs = untilMs;
          }
          yield* run.mark("call ended", wireText(wireOf(sent, since)));
        });
        /** A character's transcript heard at or after `atMs`, waited for until `untilMs`. */
        const awaitCharacter = (atMs: number, untilMs: number) =>
          awaitHeard(
            0,
            (each) =>
              each.atMs >= atMs && textOf(messageOf(each, "transcript"), "speaker") === "character"
                ? each
                : undefined,
            untilMs,
          );

        // 1. The deployment's schema, and where the session runs.
        yield* step(
          "contract",
          10,
          Effect.gen(function* () {
            const { openapi } = yield* session.schema;
            const info = field(openapi, "info");
            const paths = field(openapi, "paths");
            const commands = Predicate.isObject(paths)
              ? Object.keys(paths).flatMap((path) =>
                  path.startsWith("/events/") ? [path.slice("/events/".length)] : [],
                )
              : [];
            const title = codeOf(field(info, "title"));
            const version = codeOf(field(info, "version"));
            yield* record((avatar) => ({
              ...avatar,
              contract: {
                ...(title === undefined ? {} : { title }),
                ...(version === undefined ? {} : { version }),
                commands: commands.map(Probes.keptText),
                cloneVoice: commands.includes("clone_voice"),
              },
            }));
            // A read that fails is noted, and leaves the server section missing.
            const inspected = yield* pieces.withToken(grant.jwt).pipe(
              Effect.flatMap((coordinator) => coordinator.inspect(session.id)),
              Effect.result,
            );
            if (Result.isFailure(inspected))
              yield* run.mark("server unread", inspected.failure.reason._tag);
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
            yield* run.mark("contract read");
          }),
        );

        // 2. The snapshot the session sent on connect.
        yield* step(
          "first snapshot",
          10,
          Effect.gen(function* () {
            const first = yield* pieces.waitFor(
              heard,
              (all) => all.find((each) => messageOf(each, "session_state") !== undefined),
              deadline,
            );
            const data = messageOf(first, "session_state") ?? {};
            yield* record((avatar) => ({
              ...avatar,
              first: { atMs: since(first.atMs), ...fieldsOf(data) },
            }));
          }),
        );

        // 3. get_state and list_voices, answered at any time.
        yield* step(
          "get_state",
          12,
          Effect.gen(function* () {
            const sent = yield* send("get_state", {});
            yield* record((avatar) => ({ ...avatar, getState: wireOf(sent, since) }));
          }),
        );
        yield* step(
          "list_voices",
          12,
          Effect.gen(function* () {
            const sent = yield* send("list_voices", {});
            const data = replyOf(sent);
            const system = field(data, "system");
            const entries = isList(system) ? system : [];
            voiceIds = entries.flatMap((entry) => {
              const voice = textOf(entry, "voice");
              return voice === undefined ? [] : [voice];
            });
            yield* record((avatar) => ({
              ...avatar,
              voices: {
                ...wireOf(sent, since),
                system: entries.length,
                ids: voiceIds.filter(Probes.isCode),
                cloned: data !== undefined && Object.hasOwn(data, "cloned"),
                defaultVoice: data !== undefined && Object.hasOwn(data, "default_voice"),
              },
            }));
          }),
        );

        // 4. Refusals before any avatar: clone_voice with a name its schema refuses, so nothing
        // is cloned and its URL, which cannot resolve, is never fetched; and say before any call.
        yield* step(
          "clone_voice",
          15,
          Effect.flatMap(
            refuse("clone_voice with an invalid name", "clone_voice", {
              name: "bad name!",
              audio_url: "https://example.invalid/a.wav",
            }),
            refused,
          ),
        );
        yield* step(
          "say before a call",
          15,
          Effect.flatMap(refuse("say before any call", "say", { text: "hello" }), refused),
        );

        // 5. The avatar, from the photo uploaded for it.
        yield* step(
          "create_avatar",
          45,
          Effect.gen(function* () {
            const kind = kinds[photo.type];
            const startedMs = yield* now;
            const uploaded = yield* Effect.result(
              session.upload(`avatar.${kind.extension}`, kind.mimeType, photo.bytes),
            );
            const upload = {
              startedMs: since(startedMs),
              endedMs: since(yield* now),
              outcome: Result.isSuccess(uploaded) ? "submitted" : uploaded.failure.reason._tag,
            };
            yield* record((avatar) => ({ ...avatar, avatar: { upload, phases: [] } }));
            if (Result.isFailure(uploaded)) return yield* uploaded.failure;
            const from = (yield* heardNow).length;
            const sent = yield* send(
              "create_avatar",
              { name: "Probe" },
              { uploads: new Map([["image", uploaded.success.file]]) },
            );
            // Ready, failed, or refused: a failed avatar returns the phase to idle.
            const settled = yield* awaitHeard(
              from,
              (each) => {
                if (messageOf(each, "command_error") !== undefined) return each;
                const phase = phaseOf(each);
                return phase === "avatar_ready" || phase === "idle" ? each : undefined;
              },
              sent.sentMs + phaseWithinMs,
            );
            const ready = Option.isSome(settled)
              ? messageOf(settled.value, "session_state")
              : undefined;
            const id = textOf(ready, "avatar_id");
            const status = codeOf(field(ready ?? (yield* latestState), "avatar_status"));
            const phases = phasesFrom(yield* heardNow, from, sent.sentMs);
            yield* record((avatar) => ({
              ...avatar,
              avatar: {
                upload,
                create: wireOf(sent, since),
                phases,
                ...(status === undefined ? {} : { status }),
                ...(id === undefined ? {} : { idLength: id.length }),
              },
            }));
            if (textOf(ready, "phase") !== "avatar_ready" || id === undefined)
              return yield* ReactorError.fromCode(
                "InvalidState",
                `create_avatar reached ${phases.at(-1)?.phase ?? "no phase"} without an avatar id`,
              );
            avatarId = id;
            yield* run.mark("avatar ready");
          }),
        );

        // 6. The first call, with a greeting: live, and its picture.
        yield* step(
          "first call",
          50,
          Effect.gen(function* () {
            yield* startCall({ persona, greeting, llm });
            yield* saveCalls;
          }),
        );

        // 7. The greeting: its sound and its character's transcript, from live.
        yield* step(
          "greeting",
          25,
          Effect.gen(function* () {
            const { liveMs } = yield* firstLive;
            const onset = yield* awaitLevel(onsetAfter, liveMs, onsetWithinMs);
            const silence =
              onset === undefined
                ? undefined
                : yield* awaitLevel(silenceAfter, onset, answerWithinMs);
            yield* awaitCharacter(liveMs, (silence ?? (yield* now)) + transcriptWithinMs);
            const spoken = transcriptsOf(yield* heardNow).filter(
              (entry) => entry.atMs >= liveMs && entry.speaker === "character",
            );
            const first = spoken[0];
            yield* record((avatar) => ({
              ...avatar,
              greeting: {
                ...(onset === undefined ? {} : { onsetMs: round(onset - liveMs) }),
                ...(first === undefined ? {} : { transcriptMs: round(first.atMs - liveMs) }),
                transcripts: spoken.length,
                finals: spoken.map((entry) => entry.final),
              },
            }));
          }),
        );

        // 8. say, then interrupt 1.5 s into the answer's sound.
        yield* step(
          "say and interrupt",
          30,
          Effect.gen(function* () {
            const { call } = yield* firstLive;
            const said = yield* send("say", { text: counting });
            const say: NonNullable<Call["say"]> = { sent: said };
            call.say = say;
            const onset = yield* awaitLevel(onsetAfter, said.sentMs, onsetWithinMs);
            if (onset !== undefined) {
              say.onsetMs = onset;
              yield* pieces.sleepUntil(onset + interruptAfterMs, deadline);
            }
            const interrupted = yield* send("interrupt", {});
            const silence = yield* awaitLevel(silenceAfter, interrupted.sentMs, answerWithinMs);
            yield* awaitCharacter(
              said.sentMs,
              (silence ?? interrupted.answeredMs) + transcriptWithinMs,
            );
            const cut = transcriptsOf(yield* heardNow).find(
              (entry) => entry.atMs >= said.sentMs && entry.speaker === "character",
            );
            yield* record((avatar) => ({
              ...avatar,
              interrupt: {
                ...wireOf(interrupted, since),
                ...(onset === undefined ? {} : { afterOnsetMs: round(interrupted.sentMs - onset) }),
                ...(silence === undefined
                  ? {}
                  : { silenceMs: round(Math.max(0, silence - interrupted.sentMs)) }),
                ...(cut === undefined
                  ? {}
                  : {
                      cut: {
                        afterMs: round(cut.atMs - interrupted.sentMs),
                        final: cut.final,
                        length: cut.length,
                      },
                    }),
              },
            }));
            yield* saveCalls;
          }),
        );

        // 9. Refusals while live: update_call with no field, which the docs refuse, and say
        // with no text, below its declared length.
        yield* step(
          "refusals while live",
          20,
          Effect.gen(function* () {
            yield* firstLive;
            yield* refused(yield* refuse("update_call with no field", "update_call", {}));
            yield* refused(yield* refuse("say with no text", "say", { text: "" }));
          }),
        );

        // 10. A command carrying an explicit null, which the docs say is dropped whole with no
        // command_error: whether it is answered within 5 s, and what reads otherwise after it.
        yield* step(
          "explicit null",
          20,
          Effect.gen(function* () {
            yield* firstLive;
            const before = yield* stateNow;
            const refusal = yield* refuse(
              "update_call with a null voice",
              "update_call",
              { persona, voice: null },
              { replyTimeout: "5 seconds" },
            );
            const after = yield* stateNow;
            const changed = [...new Set([...Object.keys(before), ...Object.keys(after)])]
              .filter((key) => !clockFields.has(key) && !Equal.equals(before[key], after[key]))
              .map(Probes.keptText);
            yield* refused({ ...refusal, changed });
          }),
        );

        // 11. update_call to a voice list_voices named other than the one in effect.
        yield* step(
          "voice change",
          15,
          Effect.gen(function* () {
            yield* firstLive;
            const current = textOf(yield* latestState, "voice");
            const other = voiceIds.find((voice) => voice !== current);
            if (other === undefined) return yield* run.mark("no other voice to change to");
            const from = (yield* heardNow).length;
            const sent = yield* send("update_call", { voice: other });
            const applied = field(replyOf(sent), "applied");
            const changed = yield* awaitHeard(
              from,
              (each) => {
                const data = messageOf(each, "session_state");
                return data !== undefined && textOf(data, "voice") !== current ? each : undefined;
              },
              sent.answeredMs + windowMs,
            );
            yield* record((avatar) => ({
              ...avatar,
              voiceChange: {
                ...wireOf(sent, since),
                ...(Probes.isCode(other) ? { voice: other } : {}),
                applied: (isList(applied) ? applied : []).flatMap((name) => {
                  const code = codeOf(name);
                  return code === undefined ? [] : [code];
                }),
                changed: Option.isSome(changed),
              },
            }));
          }),
        );

        // 12. end_call, answered within the SDK's 10 s default reply deadline or not.
        yield* step(
          "end_call",
          25,
          Effect.gen(function* () {
            const call = calls[0];
            if (call === undefined)
              return yield* ReactorError.fromCode("InvalidState", "no call was started");
            yield* endCall(call);
            yield* saveCalls;
          }),
        );

        // 13. The avatar again, by the id its session_state reported.
        yield* step(
          "attach_avatar",
          15,
          Effect.gen(function* () {
            if (avatarId === undefined)
              return yield* ReactorError.fromCode("InvalidState", "no avatar id was reported");
            const from = (yield* heardNow).length;
            const sent = yield* send("attach_avatar", { avatar_id: avatarId });
            yield* awaitHeard(
              from,
              (each) => {
                if (messageOf(each, "command_error") !== undefined) return each;
                const phase = phaseOf(each);
                return phase === "avatar_ready" || phase === "idle" || phase === "failed"
                  ? each
                  : undefined;
              },
              sent.answeredMs + windowMs,
            );
            const phases = phasesFrom(yield* heardNow, from, sent.sentMs);
            yield* record((avatar) => ({
              ...avatar,
              attach: { ...wireOf(sent, since), phases },
            }));
          }),
        );

        // 14. A second call without a greeting: whether its picture flows with no resume after
        // its live, its answer to a say, and its end.
        yield* step(
          "second call",
          60,
          Effect.gen(function* () {
            const call = yield* startCall({ persona, llm });
            yield* saveCalls;
            if (call.liveMs !== undefined) {
              const said = yield* send("say", { text: sea });
              const say: NonNullable<Call["say"]> = { sent: said };
              call.say = say;
              const onset = yield* awaitLevel(onsetAfter, said.sentMs, onsetWithinMs);
              if (onset !== undefined) {
                say.onsetMs = onset;
                yield* awaitLevel(silenceAfter, onset, answerWithinMs);
              }
            }
            yield* endCall(call);
            yield* saveCalls;
          }),
        );

        // 15. The state once more; then the record, the criteria and the close.
        yield* step(
          "last state",
          12,
          Effect.gen(function* () {
            const sent = yield* send("get_state", {});
            const phase = codeOf(field(replyOf(sent), "phase"));
            yield* record((avatar) => ({
              ...avatar,
              lastState: { ...wireOf(sent, since), ...(phase === undefined ? {} : { phase }) },
            }));
          }),
        );
        yield* recordAll;
        const [first, second] = calls;
        yield* pieces.judge("the session's first snapshot arrived", [
          (yield* run.evidence).avatar?.first !== undefined,
          "no session_state arrived after the session connected",
        ]);
        yield* pieces.judge("the avatar became ready", [
          avatarId !== undefined,
          "no session_state reported the avatar ready with its id",
        ]);
        yield* run.judge("the first call went live", notLive(first));
        yield* run.judge("character video arrived during the first call", videoFailure(first));
        const firstAudio = first?.window?.audio.summary();
        yield* pieces.judge("character audio arrived during the first call", [
          (firstAudio?.blocks ?? 0) > 0 && (firstAudio?.peakRms ?? 0) > 0,
          "no sound arrived while the first call was live",
        ]);
        const ended = first?.end === undefined ? undefined : wireOf(first.end.sent, since);
        yield* pieces.judge("end_call was answered with call_ended", [
          ended?.wire === "message" && ended.type === "call_ended",
          ended === undefined ? "end_call was never sent" : `end_call answered ${wireText(ended)}`,
        ]);
        yield* run.judge("the second call went live with video", videoFailure(second));
        closed = true;
        yield* pieces.close(session);
        yield* run.mark("avatar observed");
      }),
    // Whatever failed, the key ends a session the check allocated.
    pieces.endHeld(keyed),
  );
});
