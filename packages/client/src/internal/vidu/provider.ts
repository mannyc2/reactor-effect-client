/**
 * Vidu S2-Avatar over a connected session: the snapshot the model reports,
 * what it says, and each command with the evidence of what answered it.
 *
 * The model refuses a command by sending `command_error`, which names the
 * command but no request. Commands therefore go out one at a time, and what
 * answers one is what arrived after the events the provider had applied when
 * it was sent: a `command_error` naming it, or the snapshots that settle it.
 * Another client's command of the same name in that window is mistaken for it.
 */
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { CommandFailure, ReactorError, Refused } from "../../ReactorError.js";
import type { CommandReply, Session, SessionEvent, UploadReference } from "../../Session.js";
import * as Deadline from "../deadline.js";
import * as Hub from "../hub.js";
import { AvatarImage, Call, CallUpdate, ReferenceImages, type ReferenceImage } from "./commands.js";
import {
  CallEnded,
  CallUpdated,
  CommandError,
  messageTypes,
  ReferenceImagesApplied,
  State,
  Transcript,
  Voices,
} from "./messages.js";

export interface Options {
  /** Each command until its reply; 10 seconds by default. */
  readonly replyTimeout?: Duration.Input | undefined;
  /** The avatar image's upload; 60 seconds by default. */
  readonly uploadTimeout?: Duration.Input | undefined;
  /** From `create_avatar` or `attach_avatar` until the avatar is ready; 60 seconds by default. */
  readonly avatarTimeout?: Duration.Input | undefined;
  /**
   * From `start_call` until the call is live, and from `end_call` until the model released the
   * call; 60 seconds by default. Hosted Vidu answered an `end_call` 13.2 s after it was sent.
   */
  readonly callTimeout?: Duration.Input | undefined;
}

export type ProviderEvent =
  | { readonly _tag: "State"; readonly state: State; readonly source: SessionEvent }
  | { readonly _tag: "Transcript"; readonly transcript: Transcript; readonly source: SessionEvent }
  /** A refusal or a failed call, this client's or another's. */
  | { readonly _tag: "CommandError"; readonly error: CommandError; readonly source: SessionEvent }
  /** A message the model does not document, or the character's tracks not resumed at `live`. */
  | { readonly _tag: "Diagnostic"; readonly error: ReactorError; readonly source?: SessionEvent };

export interface ObservationOptions {
  /** Events an observer may hold unread before it fails with `Overflow`; 64 by default. */
  readonly capacity?: number | undefined;
}

export interface Provider {
  readonly sessionId: string;
  /** The latest snapshot. */
  readonly state: Effect.Effect<State>;
  /** The snapshot at every change, starting with the current one. */
  readonly changes: Stream.Stream<State>;
  /** Snapshots, transcripts and command errors from now on. */
  readonly events: (options?: ObservationOptions) => Stream.Stream<ProviderEvent, ReactorError>;
  /**
   * Makes an avatar from one photo of one person and binds it to the session,
   * once the model says it is ready. Bytes are uploaded first; a URL the model
   * fetches itself. Save its `avatar_id` to attach it in a later session.
   */
  readonly createAvatar: (image: AvatarImage) => Effect.Effect<State, CommandFailure>;
  /** Binds an avatar made earlier, by the `avatar_id` it was reported with, once ready. */
  readonly attachAvatar: (avatarId: string) => Effect.Effect<State, CommandFailure>;
  readonly listVoices: Effect.Effect<Voices, CommandFailure>;
  /** Starts a call with the bound avatar; it succeeds once the call is live. */
  readonly startCall: (call: Call) => Effect.Effect<State, CommandFailure>;
  /** Text the character answers as if it had been spoken, while the call is live. */
  readonly say: (text: string) => Effect.Effect<void, CommandFailure>;
  /** Stops the character mid-sentence. */
  readonly interrupt: Effect.Effect<void, CommandFailure>;
  /** Changes the live call; it returns the settings the model says changed. */
  readonly updateCall: (update: CallUpdate) => Effect.Effect<ReadonlyArray<string>, CommandFailure>;
  /** Gives the character one to three images; it returns every image id now in effect. */
  readonly setReferenceImages: (
    images: ReadonlyArray<ReferenceImage>,
  ) => Effect.Effect<ReadonlyArray<string>, CommandFailure>;
  /**
   * Removes reference images by id, or undoes the most recent
   * `setReferenceImages` when given none; it returns the ids still in effect.
   */
  readonly clearReferenceImages: (
    imageIds?: ReadonlyArray<string>,
  ) => Effect.Effect<ReadonlyArray<string>, CommandFailure>;
  /** Ends the call and keeps the avatar; it returns once the model released the call. */
  readonly endCall: Effect.Effect<CallEnded, CommandFailure>;
  /** Asks the model for its snapshot. */
  readonly getState: Effect.Effect<State, CommandFailure>;
}

/** What the provider keeps of the session: its snapshot and a short, ordered history. */
interface Internal {
  /** Undefined until the first snapshot arrives, which `make` waits for. */
  readonly state: State | undefined;
  /** The last session event applied. */
  readonly applied: bigint;
  /** Recent snapshots and command errors, by the session event each arrived as. */
  readonly timeline: ReadonlyArray<Entry>;
  /** Why the provider stopped observing, for good. */
  readonly ended: ReactorError | undefined;
  /** The generation the character's tracks were last resumed on, as a call was live there. */
  readonly resumed: bigint | undefined;
}

type Entry =
  | { readonly _tag: "State"; readonly sequence: bigint; readonly state: State }
  | { readonly _tag: "CommandError"; readonly sequence: bigint; readonly error: CommandError };

const bounds = { observation: 256, timeline: 64 };

/** The character's picture and voice. */
const outputs = ["main_video", "main_audio"] as const;

type Command =
  | "create_avatar"
  | "attach_avatar"
  | "list_voices"
  | "start_call"
  | "say"
  | "interrupt"
  | "update_call"
  | "set_reference_images"
  | "clear_reference_images"
  | "end_call"
  | "get_state";

const evidence = (operation: Command, source: CommandReply) => ({
  operation,
  requestId: source.requestId,
  generation: source.generation,
});

/** Nothing was sent: the input was refused here, or what had to come first failed. */
const local = (operation: Command, cause: ReactorError | CommandFailure): CommandFailure =>
  CommandFailure.from(cause, { ...cause.context, operation, outcome: "not-submitted" });

/** The model answered in a way the documentation does not. */
const unexpected = (operation: Command, source: CommandReply, message: string): CommandFailure =>
  CommandFailure.from(ReactorError.fromCode("UnexpectedReply", message), {
    ...evidence(operation, source),
    outcome: "unknown",
  });

/** The model refused the command, or failed what it started. */
const refused = (operation: Command, source: CommandReply, error: CommandError): CommandFailure =>
  CommandFailure.make({
    reason: Refused.make({
      message: `Vidu S2-Avatar refused ${operation}`,
      code: error.code,
      origin: error.origin,
      retryable: error.retryable,
      ...(error.trace_id === null ? {} : { traceId: error.trace_id }),
      body: error.reason,
    }),
    context: { ...evidence(operation, source), outcome: "replied" },
  });

const invalid = (operation: Command, what: string, cause: unknown): CommandFailure =>
  local(
    operation,
    ReactorError.fromCode("InvalidInput", `Vidu S2-Avatar ${operation}: ${what} is invalid`, {
      detail: cause,
    }),
  );

const extensions: Record<string, string> = {
  "image/png": "png",
  "image/jpeg": "jpg",
  "image/webp": "webp",
  "image/heic": "heic",
};

const build = Effect.fnUntraced(function* (session: Session, options: Options) {
  const limits = {
    reply: yield* Deadline.decode("ViduS2Avatar replyTimeout")(
      options.replyTimeout ?? "10 seconds",
    ),
    upload: yield* Deadline.decode("ViduS2Avatar uploadTimeout")(
      options.uploadTimeout ?? "60 seconds",
    ),
    avatar: yield* Deadline.decode("ViduS2Avatar avatarTimeout")(
      options.avatarTimeout ?? "60 seconds",
    ),
    call: yield* Deadline.decode("ViduS2Avatar callTimeout")(options.callTimeout ?? "60 seconds"),
  };
  // A provider attaches to a connected session; it neither allocates nor closes it.
  yield* session.ready;
  const observation = yield* session.observe({ capacity: bounds.observation });
  const hub = yield* Hub.make<ProviderEvent>();
  const internal = yield* SubscriptionRef.make<Internal | undefined>(undefined);
  /** Commands go out one at a time, so a refusal names the one it answers. */
  const lock = yield* Semaphore.make(1);

  const record = (entry: Entry) => (current: Internal | undefined) =>
    current === undefined
      ? current
      : {
          ...current,
          ...(entry._tag === "State" ? { state: entry.state } : {}),
          timeline: [...current.timeline.slice(1 - bounds.timeline), entry],
        };
  const diagnose = (error: ReactorError, source?: SessionEvent) =>
    hub.publish({ _tag: "Diagnostic", error, ...(source === undefined ? {} : { source }) });

  /**
   * Resumes the character's tracks as a call goes live, and on a generation that replaced the one
   * it went live on, as Reactor's own tutorial does on every `live`. With only the resume each
   * connection makes as it becomes ready, the paid probe of 2026-10-01 had a call's voice and
   * none of its picture.
   */
  const resumeOutputs = Effect.fnUntraced(function* (event: SessionEvent) {
    const due = yield* SubscriptionRef.modify(internal, (current) =>
      current === undefined ||
      (current.state?.phase === "live" && current.resumed === event.generation)
        ? ([false, current] as const)
        : ([true, { ...current, resumed: event.generation }] as const),
    );
    if (!due) return;
    const resumed = yield* Effect.exit(
      Effect.gen(function* () {
        const media = yield* session.decoded.pipe(
          Effect.catchReason("ReactorError", "UnsupportedCapability", () => session.tracks),
        );
        // A generation since replaced resumes nothing; the live snapshot of its successor does.
        if (media.generation !== event.generation) return;
        for (const name of outputs) yield* media.setTrackActive(name, true);
      }),
    );
    if (Exit.isFailure(resumed))
      yield* diagnose(
        Cause.findErrorOption(resumed.cause).pipe(
          Option.getOrElse(() =>
            ReactorError.fromCode("InvalidState", "the character's tracks were not resumed", {
              detail: resumed.cause,
            }),
          ),
        ),
        event,
      );
  });

  /** Applies one session event: a snapshot, a transcript or a command error. */
  const apply = Effect.fnUntraced(function* (event: SessionEvent) {
    const sequence = event.sequence;
    if (event._tag === "Model" && event.kind === "message") {
      const malformed = (cause: unknown) =>
        diagnose(
          ReactorError.fromCode("Protocol", `Vidu S2-Avatar sent a malformed ${event.type}`, {
            detail: cause,
          }),
          event,
        );
      const data = event.data ?? {};
      switch (event.type) {
        case "session_state": {
          const decoded = Schema.decodeUnknownResult(State)(data);
          if (Result.isFailure(decoded)) yield* malformed(decoded.failure);
          else {
            const state = decoded.success;
            // Before the snapshot is applied, so a call is live to its caller once it can be seen.
            if (state.phase === "live") yield* resumeOutputs(event);
            yield* SubscriptionRef.update(internal, record({ _tag: "State", sequence, state }));
            yield* hub.publish({ _tag: "State", state, source: event });
          }
          break;
        }
        case "command_error": {
          const decoded = Schema.decodeUnknownResult(CommandError)(data);
          if (Result.isFailure(decoded)) yield* malformed(decoded.failure);
          else {
            const error = decoded.success;
            yield* SubscriptionRef.update(
              internal,
              record({ _tag: "CommandError", sequence, error }),
            );
            yield* hub.publish({ _tag: "CommandError", error, source: event });
          }
          break;
        }
        case "transcript": {
          const decoded = Schema.decodeUnknownResult(Transcript)(data);
          if (Result.isFailure(decoded)) yield* malformed(decoded.failure);
          else
            yield* hub.publish({ _tag: "Transcript", transcript: decoded.success, source: event });
          break;
        }
        default:
          // Other documented messages are replies, which their command decodes.
          if (!messageTypes.has(event.type))
            yield* diagnose(
              ReactorError.fromCode(
                "Protocol",
                "Vidu S2-Avatar sent a message it does not document",
              ),
              event,
            );
      }
    }
    yield* SubscriptionRef.update(internal, (current) =>
      current === undefined ? current : { ...current, applied: sequence },
    );
  });

  /** The provider stops for good: waiters fail with `error`, and so do observers. */
  const end = (error: ReactorError) =>
    SubscriptionRef.update(internal, (current) =>
      current === undefined || current.ended !== undefined ? current : { ...current, ended: error },
    ).pipe(Effect.andThen(hub.fail(Cause.fail(error))));

  /**
   * Sends one command and returns its reply with `since`, the last event the
   * provider had applied when it was sent. It fails with the refusal the reply
   * is, or with a `command_error` naming the command that arrived before it.
   */
  const dispatch = (
    operation: Command,
    data: Schema.JsonObject,
    uploads?: ReadonlyMap<string, UploadReference>,
    replyTimeout: Duration.Duration = limits.reply,
  ) =>
    lock.withPermit(
      Effect.gen(function* () {
        const since = (yield* SubscriptionRef.get(internal))?.applied ?? observation.revision;
        const source = yield* session.command(operation, data, {
          replyTimeout,
          ...(uploads === undefined ? {} : { uploads }),
        });
        const known = yield* applied(operation, source);
        if (source.kind === "message" && source.type === "command_error") {
          const own = Schema.decodeUnknownResult(CommandError)(source.data ?? {});
          return yield* Result.isSuccess(own)
            ? refused(operation, source, own.success)
            : unexpected(operation, source, "Vidu S2-Avatar sent a malformed refusal");
        }
        const refusal = known.timeline.find(
          (entry) =>
            entry._tag === "CommandError" &&
            entry.sequence > since &&
            entry.sequence < source.sequence &&
            entry.error.command === operation,
        );
        if (refusal?._tag === "CommandError")
          return yield* refused(operation, source, refusal.error);
        return { source, since };
      }),
    );

  /** Waits until the observer has applied `source`, so every event before it is known. */
  const applied = (operation: Command, source: CommandReply) =>
    SubscriptionRef.changes(internal).pipe(
      Stream.filter(
        (current): current is Internal =>
          current !== undefined &&
          (current.applied >= source.sequence || current.ended !== undefined),
      ),
      Stream.runHead,
      Effect.flatMap((found) =>
        Option.isSome(found) && found.value.ended === undefined
          ? Effect.succeed(found.value)
          : Effect.fail(
              CommandFailure.from(
                Option.isSome(found) && found.value.ended !== undefined
                  ? found.value.ended
                  : ReactorError.fromCode("Closed", "Vidu S2-Avatar provider closed"),
                { ...evidence(operation, source), outcome: "replied" },
              ),
            ),
      ),
    );

  /** A command whose answer is the message `type`, decoded. */
  const answered = <A>(
    operation: Command,
    type: string,
    schema: Schema.Decoder<A>,
    data: Schema.JsonObject = {},
    replyTimeout: Duration.Duration = limits.reply,
  ): Effect.Effect<A, CommandFailure> =>
    dispatch(operation, data, undefined, replyTimeout).pipe(
      Effect.flatMap(({ source }) =>
        source.kind === "message" && source.type === type
          ? Schema.decodeEffect(schema)(source.data ?? {}).pipe(
              Effect.mapError(() =>
                unexpected(operation, source, `Vidu S2-Avatar sent a malformed ${type}`),
              ),
            )
          : Effect.fail(
              unexpected(operation, source, `Vidu S2-Avatar ${operation} did not return ${type}`),
            ),
      ),
    );

  /**
   * Waits, within `limit`, for what the model reported once the command was
   * sent to decide it: a `command_error` naming it, or snapshots `decide` reads.
   */
  const settle = <A>(
    operation: Command,
    dispatched: { readonly source: CommandReply; readonly since: bigint },
    limit: Duration.Duration,
    decide: (entries: ReadonlyArray<Entry>) => Result.Result<A, CommandFailure> | undefined,
  ): Effect.Effect<A, CommandFailure> => {
    const { source, since } = dispatched;
    const failed = (error: ReactorError) =>
      CommandFailure.from(error, { ...evidence(operation, source), outcome: "replied" });
    const decision = (current: Internal | undefined) => {
      if (current === undefined) return undefined;
      if (current.ended !== undefined) return current.ended.pipe(failed, Result.fail);
      const after = current.timeline.filter((entry) => entry.sequence > since);
      const refusal = after.find(
        (entry) => entry._tag === "CommandError" && entry.error.command === operation,
      );
      return refusal?._tag === "CommandError"
        ? Result.fail(refused(operation, source, refusal.error))
        : decide(after);
    };
    return SubscriptionRef.changes(internal).pipe(
      Stream.filterMap((current) => {
        const decided = decision(current);
        return decided === undefined ? Result.failVoid : Result.succeed(decided);
      }),
      Stream.runHead,
      Effect.flatMap((found) =>
        Option.isSome(found)
          ? Effect.fromResult(found.value)
          : ReactorError.fromCode("Closed", "Vidu S2-Avatar provider closed").pipe(
              failed,
              Effect.fail,
            ),
      ),
      Effect.timeoutOrElse({
        duration: limit,
        orElse: () =>
          ReactorError.fromCode(
            "Timeout",
            `Vidu S2-Avatar ${operation} did not settle within ${Duration.format(limit)}`,
          ).pipe(failed, Effect.fail),
      }),
    );
  };

  const states = (entries: ReadonlyArray<Entry>) =>
    entries.flatMap((entry) => (entry._tag === "State" ? [entry.state] : []));

  /** A failed avatar or call, from the snapshot that reported it. */
  const failedBy = (
    operation: Command,
    dispatched: { readonly source: CommandReply },
    state: State,
    why: string,
  ) =>
    Result.fail(
      state.last_error === null
        ? CommandFailure.from(ReactorError.fromCode("InvalidState", why), {
            ...evidence(operation, dispatched.source),
            outcome: "replied",
          })
        : refused(operation, dispatched.source, state.last_error),
    );

  const createAvatar = Effect.fn("ViduS2Avatar.createAvatar")(function* (input: AvatarImage) {
    const image = yield* Schema.decodeEffect(AvatarImage)(input).pipe(
      Effect.mapError((cause) => invalid("create_avatar", "the image", cause)),
    );
    const name = image.name === undefined ? {} : { name: image.name };
    const dispatched =
      "url" in image
        ? yield* dispatch("create_avatar", { ...name, image_url: image.url })
        : yield* session
            .upload(`avatar.${extensions[image.type] ?? "img"}`, image.type, image.bytes, {
              uploadTimeout: limits.upload,
            })
            .pipe(
              Effect.mapError((error) => local("create_avatar", error)),
              Effect.flatMap((uploaded) =>
                dispatch("create_avatar", name, new Map([["image", uploaded.file]])),
              ),
            );
    // The avatar being made is reported preparing, then ready; back to idle, it failed.
    return yield* settle("create_avatar", dispatched, limits.avatar, (entries) => {
      const seen = states(entries);
      const preparing = seen.findIndex((state) => state.phase === "preparing_avatar");
      if (preparing < 0) return undefined;
      const outcome = seen.slice(preparing + 1).find((state) => state.phase !== "preparing_avatar");
      if (outcome === undefined) return undefined;
      return outcome.phase === "avatar_ready" && outcome.avatar_id !== null
        ? Result.succeed(outcome)
        : failedBy("create_avatar", dispatched, outcome, "the image did not become an avatar");
    });
  });

  const attachAvatar = Effect.fn("ViduS2Avatar.attachAvatar")(function* (avatarId: string) {
    if (avatarId.length < 1 || avatarId.length > 128)
      return yield* invalid("attach_avatar", "the avatar id", avatarId);
    const dispatched = yield* dispatch("attach_avatar", { avatar_id: avatarId });
    return yield* settle("attach_avatar", dispatched, limits.avatar, (entries) => {
      const ready = states(entries).find(
        (state) => state.phase === "avatar_ready" && state.avatar_id === avatarId,
      );
      return ready === undefined ? undefined : Result.succeed(ready);
    });
  });

  const startCall = Effect.fn("ViduS2Avatar.startCall")(function* (call: Call) {
    const data = yield* Schema.encodeUnknownEffect(Call)(call).pipe(
      Effect.mapError((cause) => invalid("start_call", "the call", cause)),
    );
    const dispatched = yield* dispatch("start_call", data);
    // A call is live, or failed, after the command that started it.
    return yield* settle("start_call", dispatched, limits.call, (entries) => {
      const outcome = states(entries).find(
        (state) => state.phase === "live" || state.phase === "failed",
      );
      if (outcome === undefined) return undefined;
      return outcome.phase === "live"
        ? Result.succeed(outcome)
        : failedBy("start_call", dispatched, outcome, "the call failed");
    });
  });

  const acknowledged = (operation: Command, data: Schema.JsonObject = {}) =>
    Effect.asVoid(dispatch(operation, data));

  const say = Effect.fn("ViduS2Avatar.say")(function* (text: string) {
    if (text.length < 1 || text.length > 2_000)
      return yield* invalid("say", "the text, 1 to 2,000 characters,", text.length);
    return yield* acknowledged("say", { text });
  });

  const updateCall = Effect.fn("ViduS2Avatar.updateCall")(function* (update: CallUpdate) {
    const data = yield* Schema.encodeUnknownEffect(CallUpdate)(update).pipe(
      Effect.mapError((cause) => invalid("update_call", "the update", cause)),
    );
    const { applied } = yield* answered("update_call", "call_updated", CallUpdated, data);
    return applied;
  });

  const setReferenceImages = Effect.fn("ViduS2Avatar.setReferenceImages")(function* (
    images: ReadonlyArray<ReferenceImage>,
  ) {
    const encoded = yield* Schema.encodeUnknownEffect(ReferenceImages)(images).pipe(
      Effect.mapError((cause) => invalid("set_reference_images", "the images", cause)),
    );
    const { image_ids } = yield* answered(
      "set_reference_images",
      "reference_images_applied",
      ReferenceImagesApplied,
      { images: encoded },
    );
    return image_ids;
  });

  const clearReferenceImages = Effect.fn("ViduS2Avatar.clearReferenceImages")(function* (
    imageIds?: ReadonlyArray<string>,
  ) {
    if (imageIds !== undefined && imageIds.length > 3)
      return yield* invalid("clear_reference_images", "more than three ids", imageIds.length);
    const { image_ids } = yield* answered(
      "clear_reference_images",
      "reference_images_applied",
      ReferenceImagesApplied,
      imageIds === undefined ? {} : { image_ids: [...imageIds] },
    );
    return image_ids;
  });

  const getState = answered("get_state", "session_state", State);

  // The model sends its snapshot as a connection opens, which may have passed:
  // apply events from the observation's start, and ask for it.
  yield* SubscriptionRef.set(internal, {
    state: undefined,
    applied: observation.revision,
    timeline: [],
    ended: undefined,
    resumed: undefined,
  });
  yield* observation.events.pipe(
    Stream.runForEach(apply),
    Effect.matchEffect({
      onFailure: end,
      onSuccess: () => end(ReactorError.fromCode("Closed", "the session's events ended")),
    }),
    Effect.forkScoped,
  );
  const first = yield* getState;

  return {
    sessionId: session.id,
    state: Effect.map(SubscriptionRef.get(internal), (current) => current?.state ?? first),
    changes: SubscriptionRef.changes(internal).pipe(
      Stream.map((current) => current?.state ?? first),
      Stream.changes,
    ),
    events: (options?: ObservationOptions) => Stream.unwrap(hub.subscribe(options?.capacity ?? 64)),
    createAvatar,
    attachAvatar,
    listVoices: answered("list_voices", "voices", Voices),
    startCall,
    say,
    interrupt: acknowledged("interrupt"),
    updateCall,
    setReferenceImages,
    clearReferenceImages,
    // The model answers once the call is released, which took longer than a reply deadline.
    endCall: answered("end_call", "call_ended", CallEnded, {}, limits.call),
    getState,
  } satisfies Provider;
});

export const make = build;
