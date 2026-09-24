import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import {
  CommandFailure,
  parsed,
  PolicyFailure,
  positiveLimit,
  ReactorError,
  Remote,
} from "../../errors.js";
import { duration } from "../../duration.js";
import { Observations } from "../../observation.js";
import type { CommandReply, Session, SessionEvent } from "../../session/index.js";
import * as Submission from "../../Submission.js";
import type { UploadReference } from "../../wire.generated.js";
import { decodeMessage } from "../messages.js";
import { Operations } from "./operations.js";
import type { Clip, DecodedMessage, Payload } from "../messages.js";
import { canvases } from "../profile.js";
import type {
  Acceptance,
  ControlResult,
  Options,
  PrepareHooks,
  Provider,
  ProviderEvent,
  Reply,
  Request,
  ValidatedAudioReference,
  ValidatedReference,
} from "../types.js";
import { Commands } from "./contracts.js";
import type {
  CommandArgs,
  CommandName,
  ControlCommand,
  ReplyCommand,
  ReplyType,
} from "./contracts.js";
import { validateDeployment } from "./deployment.js";
import { acceptanceFor, encodeMetadata, submissionFromMetadata } from "./evidence.js";
import type { AcceptanceIdentity } from "./evidence.js";
import { checkedUpload, referenceMaterial } from "./references.js";
import { captureRequest, clipId, enqueueArguments, nonnegative, seconds } from "./request.js";
import type { CapturedRequest } from "./request.js";
import { sizeOf } from "./retained.js";
import { ProviderState } from "./state.js";

const pure = <A>(evaluate: () => A): Effect.Effect<A, ReactorError> => parsed(evaluate);
const localFailure = (operation: string, cause: ReactorError | CommandFailure): CommandFailure =>
  CommandFailure.from(cause, { ...cause.context, operation, outcome: "not-submitted" });
/** A caller's local refusal already proves no dispatch; anything else becomes one. */
const preparationFailure = <E extends PolicyFailure>(
  operation: string,
  cause: ReactorError | CommandFailure | E,
): CommandFailure | E =>
  ReactorError.is(cause) || CommandFailure.is(cause) ? localFailure(operation, cause) : cause;
const uncertain = (
  operation: string,
  source: CommandReply,
  message: string,
  cause?: ReactorError,
): CommandFailure =>
  CommandFailure.from(cause ?? ReactorError.fromCode("UnexpectedReply", message), {
    ...(cause?.context ?? {}),
    operation,
    outcome: "unknown",
    requestId: source.requestId,
    generation: source.generation,
  });
/** The refusal reason is provider free text with no stable codes: kept for diagnosis only. */
const rejected = (operation: string, source: CommandReply, reason: string): CommandFailure =>
  new CommandFailure({
    reason: new Remote({ _tag: "Remote", message: `H3 ${operation} was refused`, body: reason }),
    context: {
      operation,
      outcome: "replied",
      requestId: source.requestId,
      generation: source.generation,
    },
  });
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

interface PendingAcceptance extends AcceptanceIdentity {
  readonly deferred: Deferred.Deferred<Acceptance, ReactorError>;
  /** The enqueue still awaits its reply, the one evidence that correlates. */
  awaiting: boolean;
  /**
   * Evidence by metadata that came while the reply was awaited: hosted H3
   * broadcasts the queue that lists a new clip before it replies. The reply
   * decides whether it counts.
   */
  held: Acceptance | undefined;
}
type ObservationResult = DecodedMessage | undefined;

/**
 * Whether a snapshot at `revision` already reflects `event`: the reducer
 * handles session events in sequence order. An acceptance is local evidence
 * that no snapshot holds, and a diagnostic without a source has no sequence,
 * so neither is ever covered.
 */
const covered = (event: ProviderEvent, revision: bigint): boolean => {
  switch (event._tag) {
    case "Acceptance":
      return false;
    case "Diagnostic":
      return event.source !== undefined && event.source.sequence <= revision;
    default:
      return event.source.sequence <= revision;
  }
};

const build = (
  session: Session,
  options: Options,
): Effect.Effect<Provider, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const crypto = yield* Crypto.Crypto;
    const limits = yield* pure(() => ({
      command: duration(options.replyTimeout ?? "15 seconds", "H3 reply timeout", {
        maximum: "10 minutes",
      }),
      upload: duration(options.uploadTimeout ?? "60 seconds", "H3 upload timeout", {
        maximum: "10 minutes",
      }),
      setup: duration(options.setupTimeout ?? "60 seconds", "H3 setup timeout", {
        maximum: "10 minutes",
      }),
      reconcile: duration(options.reconcileWindow ?? "5 seconds", "H3 reconciliation window", {
        maximum: "1 minute",
      }),
      hook: duration(options.resultHookTimeout ?? "1 second", "H3 result hook timeout", {
        maximum: "1 minute",
      }),
      pending: positiveLimit(options.maxPending ?? 128, "H3 pending acceptance bound", 4096),
      clips: positiveLimit(options.maxTrackedClips ?? 4096, "H3 clip observation bound", 16384),
      acceptances: positiveLimit(
        options.maxAcceptances ?? 1024,
        "H3 local acceptance bound",
        16384,
      ),
      cache: positiveLimit(options.maxCachedUploads ?? 256, "H3 upload cache bound", 4096),
      operations: positiveLimit(options.maxOperations ?? 1024, "H3 clip operation bound", 16384),
      prompt: positiveLimit(options.maxPromptBytes ?? 1048576, "H3 prompt byte bound", 4194304),
      retained: positiveLimit(
        options.maxRetainedBytes ?? 16777216,
        "H3 retained byte bound",
        67108864,
      ),
    }));
    // A provider attaches to an already connected session. It neither allocates
    // nor connects it, and it never takes ownership of its remote lifetime.
    yield* session.ready;
    const observation = yield* session.observe(options.observation);
    const state = new ProviderState(
      session.id,
      observation.initial.generation,
      observation.revision,
      limits.clips,
    );
    const namespace = hex(
      yield* crypto.randomBytes(16).pipe(
        Effect.mapError(() =>
          ReactorError.fromCode("InvalidState", "Could not allocate H3 acceptance namespace", {
            outcome: "not-submitted",
          }),
        ),
      ),
    );
    let counter = 0n,
      closed = false,
      fatalError: ReactorError | undefined;
    const fatal = yield* Deferred.make<ReactorError>();
    const events = new Observations<ProviderEvent>();
    const pending = new Map<string, PendingAcceptance>();
    const acceptances = new Map<string, Acceptance>();
    /** Acceptances count toward the reducer's retained bytes while the map holds them. */
    const retain = (acceptance: Acceptance, direction: 1 | -1): void => {
      state.retained.add(direction * (64 + acceptance.submissionId.length * 3));
      if (direction === 1) {
        state.retained.hold(acceptance.clip);
        state.retained.hold(acceptance.evidence.source);
      } else {
        state.retained.drop(acceptance.clip);
        state.retained.drop(acceptance.evidence.source);
      }
    };
    const operations = new Operations(limits.operations);
    const decoded = new WeakMap<CommandReply, Result.Result<ObservationResult, ReactorError>>();
    const observedWaiters = new Map<
      CommandReply,
      Deferred.Deferred<ObservationResult, ReactorError>
    >();
    const uploads = new Map<string, UploadReference>();
    const uploadGate = yield* Semaphore.make(1);
    // Callers waiting for the provider to synchronize; every applied event and
    // any failure wakes them to look again. A woken caller may wait again at
    // once, so the set is emptied before any waiter is released.
    const synchronizing = new Set<Deferred.Deferred<void>>();
    const wake = (): void => {
      const waiters = [...synchronizing];
      synchronizing.clear();
      for (const waiter of waiters) Deferred.doneUnsafe(waiter, Effect.void);
    };

    const emit = (event: ProviderEvent): void => events.emit(event, sizeOf(event));
    /** Decide a submission's acceptance: record it, resolve its waiters and announce it. */
    const record = (entry: PendingAcceptance, acceptance: Acceptance): void => {
      entry.held = undefined;
      if (acceptances.size >= limits.acceptances) {
        const first = acceptances.entries().next();
        if (!first.done) {
          acceptances.delete(first.value[0]);
          retain(first.value[1], -1);
        }
      }
      acceptances.set(entry.id, acceptance);
      retain(acceptance, 1);
      operations.accept(acceptance);
      Deferred.doneUnsafe(entry.deferred, Effect.succeed(acceptance));
      emit({ _tag: "Acceptance", acceptance });
    };
    /**
     * Held evidence decides the acceptance once the reply cannot: it came
     * without correlating the clip, or the wait for it ended short of a
     * definite failure, which discards the evidence instead.
     */
    const decideHeld = (entry: PendingAcceptance): void => {
      if (entry.held !== undefined) record(entry, entry.held);
    };
    const fail = (error: ReactorError): void => {
      if (fatalError !== undefined) return;
      fatalError = error;
      state.unavailable(error);
      // Recording held evidence resumes its waiters at once: by now the provider
      // refuses their new work, and they can read the acceptance before anyone
      // learns of the failure.
      for (const entry of pending.values()) decideHeld(entry);
      Deferred.doneUnsafe(fatal, Effect.succeed(error));
      for (const entry of pending.values()) Deferred.doneUnsafe(entry.deferred, Effect.fail(error));
      for (const waiter of observedWaiters.values())
        Deferred.doneUnsafe(waiter, Effect.fail(error));
      emit({ _tag: "Diagnostic", error });
      wake();
    };
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        closed = true;
        fail(
          ReactorError.fromCode("Closed", "H3 provider scope closed", {
            operation: "H3 observation",
          }),
        );
        operations.retire();
        events.end();
        pending.clear();
        observedWaiters.clear();
        uploads.clear();
      }),
    );

    const accept = (clip: Clip, source: CommandReply): void => {
      const id = submissionFromMetadata(namespace, clip.metadata);
      if (id === undefined) return;
      const entry = pending.get(id);
      const acceptance = entry === undefined ? undefined : acceptanceFor(entry, clip, source);
      if (entry === undefined || acceptance === undefined) {
        // After the reconcile window, or in a later transport generation, the
        // evidence can still resolve the operation, never the acceptances.
        operations.lateEvidence(id, clip, source);
        return;
      }
      const previous = acceptances.get(entry.id) ?? entry.held;
      if (previous !== undefined && previous.clip.clip_id !== clip.clip_id) {
        fail(
          ReactorError.fromCode("Protocol", "H3 acceptance identity named two clips", {
            operation: "enqueue",
          }),
        );
        return;
      }
      if (acceptances.has(entry.id)) return;
      // Until the reply is observed, it may still correlate the acceptance; the
      // clip's facts are recorded meanwhile.
      if (acceptance.evidence.kind === "metadata" && entry.awaiting) {
        if (entry.held === undefined) {
          entry.held = acceptance;
          operations.identify(acceptance);
        }
        return;
      }
      record(entry, acceptance);
    };

    const reduce = (source: SessionEvent): void => {
      if (closed || fatalError !== undefined) return;
      try {
        const before = state.transportGeneration;
        let disposition = state.admit(source);
        if (state.transportGeneration !== before) {
          uploads.clear();
          for (const entry of pending.values())
            if (entry.generation !== source.generation) {
              // No reply to an earlier generation's enqueue can correlate now.
              decideHeld(entry);
              Deferred.doneUnsafe(
                entry.deferred,
                Effect.fail(
                  ReactorError.fromCode(
                    "Disconnected",
                    "H3 acceptance crossed a transport generation",
                    { operation: "enqueue" },
                  ),
                ),
              );
            }
        }
        if (source._tag !== "Model") {
          emit({ _tag: "Session", source });
          return;
        }
        let result: Result.Result<ObservationResult, ReactorError>;
        if (source.kind === "ack") {
          result =
            disposition === "stale"
              ? Result.fail(
                  ReactorError.fromCode(
                    "Disconnected",
                    "H3 received a stale command acknowledgement",
                  ),
                )
              : Result.succeed(undefined);
          emit({ _tag: "Acknowledged", source });
        } else {
          const message = decodeMessage(source.type, source.data);
          if (disposition === "applied") disposition = state.apply(message, source);
          result =
            disposition === "stale"
              ? Result.fail(
                  ReactorError.fromCode("Disconnected", "H3 received a stale command message"),
                )
              : Result.succeed(message);
          if (disposition !== "stale" && message.type !== "unknown") {
            if (message.type === "queue_update") {
              for (const clip of [
                ...message.data.generation,
                ...message.data.playout,
                ...message.data.history,
              ])
                accept(clip, source);
            } else if ("clip" in message.data) accept(message.data.clip, source);
            operations.observe(message, source);
          }
          emit({ _tag: "Message", message, source, disposition });
        }
        decoded.set(source, result);
        const waiter = observedWaiters.get(source);
        if (waiter !== undefined)
          Deferred.doneUnsafe(
            waiter,
            Result.isSuccess(result) ? Effect.succeed(result.success) : Effect.fail(result.failure),
          );
        if (state.retained.bytes > limits.retained)
          fail(
            ReactorError.fromCode("Overflow", "H3 retained observation byte bound exceeded", {
              operation: "H3 observation",
            }),
          );
      } catch (cause) {
        // The reducer rejects provider data with a ReactorError; anything else
        // it throws is a bug, which stays a defect of the observation fiber.
        if (!ReactorError.is(cause)) throw cause;
        const error = cause;
        if (source._tag === "Model") {
          decoded.set(source, Result.fail(error));
          const waiter = observedWaiters.get(source);
          if (waiter !== undefined) Deferred.doneUnsafe(waiter, Effect.fail(error));
        }
        fail(error);
      }
    };
    yield* observation.events.pipe(
      Stream.runForEach((source) =>
        Effect.sync(() => {
          reduce(source);
          wake();
        }),
      ),
      Effect.catch((error) => Effect.sync(() => fail(error))),
      Effect.andThen(
        Effect.sync(() => {
          if (!closed) fail(ReactorError.fromCode("Closed", "H3 session observation ended"));
        }),
      ),
      Effect.forkScoped,
    );
    // Enqueues execute in a scope of their own. Finalizers run last-added first,
    // so a closing provider refuses new work before that scope interrupts them.
    const executions = yield* Scope.fork(scope);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        closed = true;
      }),
    );

    const refusal = (needsFacts: boolean): ReactorError | undefined =>
      closed
        ? ReactorError.fromCode("Closed", "H3 provider scope is closed")
        : (fatalError ??
          (needsFacts && state.availability !== "Ready"
            ? ReactorError.fromCode(
                "InvalidState",
                "H3 provider needs current state and queue observations",
              )
            : undefined));
    /**
     * H3 documents that a command replies before it broadcasts the state and
     * queue it changed (hosted H3 broadcasts an enqueue's queue first), so the
     * provider may be briefly synchronizing after a reply. A command that needs
     * current facts waits, within its own reply deadline, for those
     * broadcasts; it is refused, not submitted, only if they do not come or
     * the provider fails.
     */
    const synchronized = Effect.gen(function* () {
      while (!closed && fatalError === undefined && state.availability === "Synchronizing") {
        const waiter = Deferred.makeUnsafe<void>();
        synchronizing.add(waiter);
        yield* Deferred.await(waiter).pipe(
          Effect.ensuring(Effect.sync(() => synchronizing.delete(waiter))),
        );
      }
    });
    /** Wait, within one reply deadline, while the provider is only synchronizing. */
    const settled = Effect.suspend(() =>
      refusal(false) === undefined && state.availability === "Synchronizing"
        ? synchronized.pipe(Effect.timeout(limits.command), Effect.ignore)
        : Effect.void,
    );
    const active = (operation: string, needsFacts: boolean): Effect.Effect<void, CommandFailure> =>
      Effect.gen(function* () {
        if (needsFacts) yield* settled;
        const error = refusal(needsFacts);
        if (error !== undefined) return yield* localFailure(operation, error);
      });
    const awaitObservation = (
      source: CommandReply,
    ): Effect.Effect<ObservationResult, ReactorError> =>
      Effect.suspend(() => {
        const previous = decoded.get(source);
        if (previous !== undefined)
          return Result.isSuccess(previous)
            ? Effect.succeed(previous.success)
            : Effect.fail(previous.failure);
        if (fatalError !== undefined) return Effect.fail(fatalError);
        if (observedWaiters.size >= limits.pending)
          return Effect.fail(ReactorError.fromCode("Overflow", "H3 reply observer bound exceeded"));
        const waiter = Deferred.makeUnsafe<ObservationResult, ReactorError>();
        observedWaiters.set(source, waiter);
        return Deferred.await(waiter).pipe(
          Effect.timeoutOrElse({
            duration: limits.command,
            orElse: () =>
              Effect.fail(
                ReactorError.fromCode(
                  "Timeout",
                  "H3 did not observe the command's exact returned envelope",
                ),
              ),
          }),
          Effect.ensuring(
            Effect.sync(() => {
              observedWaiters.delete(source);
            }),
          ),
        );
      });
    const call = <K extends CommandName>(operation: K, args: CommandArgs<K>, needsFacts = true) =>
      Effect.gen(function* () {
        yield* active(operation, needsFacts);
        const source = yield* session.command(operation, args, { replyTimeout: limits.command });
        const message = yield* awaitObservation(source).pipe(
          Effect.mapError((error) =>
            uncertain(operation, source, "H3 reply observation failed", error),
          ),
        );
        if (message?.type === "command_error" && message.data.command === operation)
          return yield* rejected(operation, source, message.data.reason);
        return { source, message };
      });
    const named = <K extends ReplyCommand>(
      operation: K,
      args: CommandArgs<K>,
      needsFacts = true,
    ): Effect.Effect<Reply<ReplyType<K>>, CommandFailure> => {
      const expected = Commands[operation].reply;
      return call(operation, args, needsFacts).pipe(
        Effect.flatMap(({ source, message }) =>
          message?.type === expected
            ? // The broadcasts a reply implies follow it: the command settles once
              // they are observed, so its caller reads its own effects. The state
              // and queue reads are themselves that barrier and do not wait.
              (needsFacts ? settled : Effect.void).pipe(
                Effect.as(Object.freeze({ value: message.data as Payload<ReplyType<K>>, source })),
              )
            : Effect.fail(
                uncertain(operation, source, `H3 ${operation} did not return ${expected}`),
              ),
        ),
      );
    };
    const checked = <A>(operation: string, value: () => A) =>
      pure(value).pipe(Effect.mapError((error) => localFailure(operation, error)));
    const getState = named("get_state", {}, false);
    const getQueue = named("get_queue", {}, false);
    const refresh = getState.pipe(
      Effect.andThen(getQueue),
      Effect.flatMap((reply) =>
        state.snapshot()._tag === "Ready"
          ? Effect.void
          : Effect.fail(
              CommandFailure.from(
                ReactorError.fromCode("InvalidState", "H3 full snapshots changed during refresh"),
                {
                  operation: "H3 refresh",
                  outcome: "replied",
                  requestId: reply.source.requestId,
                  generation: reply.source.generation,
                },
              ),
            ),
      ),
    );
    const control = <K extends ControlCommand>(
      operation: K,
      args: CommandArgs<K>,
    ): Effect.Effect<ControlResult, CommandFailure> =>
      call(operation, args).pipe(
        Effect.flatMap(({ source, message }) =>
          source.kind === "ack"
            ? Effect.succeed<ControlResult>(Object.freeze({ _tag: "Acknowledged", source }))
            : message !== undefined && message.type !== "unknown"
              ? settled.pipe(
                  Effect.as<ControlResult>(Object.freeze({ _tag: "Reply", message, source })),
                )
              : Effect.fail(uncertain(operation, source, "Unexpected H3 control response")),
        ),
      );

    const stage = (request: CapturedRequest, metadata: string) =>
      Effect.gen(function* () {
        yield* active("enqueue", true);
        const before = state.snapshot();
        if (before._tag !== "Ready")
          return yield* localFailure(
            "enqueue",
            ReactorError.fromCode("InvalidState", "H3 is not ready"),
          );
        if (request.seconds !== undefined)
          yield* checked("enqueue", () =>
            seconds(request.seconds, {
              min: before.state.clip_seconds_min,
              max: before.state.clip_seconds_max,
            }),
          );
        if (request.audio.length > 0 && !contract.referenceAudio)
          return yield* localFailure(
            "enqueue",
            ReactorError.fromCode(
              "UnsupportedCapability",
              "The H3 deployment does not declare reference audio",
            ),
          );
        const files: UploadReference[] = [];
        for (const reference of request.references)
          files.push(yield* upload(reference, "image", before.transportGeneration));
        const audio: UploadReference[] = [];
        for (const reference of request.audio)
          audio.push(yield* upload(reference, "audio", before.transportGeneration));
        const args = enqueueArguments(request, files, audio, metadata);
        return { args, generation: before.transportGeneration };
      });

    /**
     * One reference's upload, content-addressed within a transport generation,
     * so identical bytes upload once however many requests carry them.
     */
    const upload = (
      reference: ValidatedReference | ValidatedAudioReference,
      kind: "image" | "audio",
      generation: bigint,
    ) =>
      Effect.gen(function* () {
        const material = referenceMaterial(reference);
        if (material._tag === "Uploaded") return material.file;
        const digest = yield* crypto
          .digest("SHA-256", material.bytes)
          .pipe(
            Effect.mapError(() =>
              localFailure(
                "enqueue",
                ReactorError.fromCode("InvalidState", "Could not hash H3 reference"),
              ),
            ),
          );
        const key = `${generation}:${hex(digest)}`;
        return yield* uploadGate.withPermit(
          Effect.gen(function* () {
            const cached = uploads.get(key);
            if (cached !== undefined) return cached;
            const uploaded = yield* session
              .upload(`h3-${hex(digest)}`, reference.mimeType, material.bytes, {
                uploadTimeout: limits.upload,
              })
              .pipe(Effect.mapError((error) => localFailure("enqueue", error)));
            const file = yield* checked("enqueue", () => checkedUpload(uploaded.file, kind));
            if (file.size !== BigInt(reference.size) || file.mime_type !== reference.mimeType)
              return yield* localFailure(
                "enqueue",
                ReactorError.fromCode("Protocol", "H3 upload returned different file facts"),
              );
            if (state.snapshot().transportGeneration !== generation)
              return yield* localFailure(
                "enqueue",
                ReactorError.fromCode("Disconnected", "H3 reference upload crossed a generation"),
              );
            if (uploads.size >= limits.cache) {
              const first = uploads.keys().next();
              if (!first.done) uploads.delete(first.value);
            }
            uploads.set(key, file);
            return file;
          }),
        );
      });

    const nextId = () =>
      pure(() => {
        if (counter === 0xffffffffffffffffn)
          throw ReactorError.fromCode("Overflow", "H3 submission identity space exhausted", {
            outcome: "not-submitted",
          });
        return `${namespace}:${++counter}`;
      });
    const prepared = <E extends PolicyFailure>(
      id: string,
      input: Effect.Effect<
        { readonly request: CapturedRequest; readonly metadata: string },
        ReactorError | CommandFailure | E,
        Scope.Scope
      >,
      hooks: PrepareHooks<E>,
    ): Effect.Effect<
      Submission.Submission<Acceptance, CommandFailure | E>,
      ReactorError | CommandFailure
    > =>
      Submission.make({
        id,
        prepare: Effect.gen(function* () {
          const { request, metadata } = yield* input.pipe(
            Effect.mapError((error) => preparationFailure("enqueue", error)),
          );
          const staged = yield* stage(request, metadata);
          const entry: PendingAcceptance = {
            id,
            metadata,
            prompt: request.prompt,
            generation: staged.generation,
            deferred: Deferred.makeUnsafe<Acceptance, ReactorError>(),
            awaiting: true,
            held: undefined,
          };
          return { ...staged, entry };
        }),
        commit: ({ entry }) =>
          Effect.gen(function* () {
            yield* active("enqueue", true);
            const live = yield* session.ready.pipe(
              Effect.mapError((error) => localFailure("enqueue", error)),
            );
            if (
              entry.generation !== state.snapshot().transportGeneration ||
              live.generation !== entry.generation
            )
              return yield* localFailure(
                "enqueue",
                ReactorError.fromCode(
                  "Disconnected",
                  "H3 prepared input crossed a transport generation",
                ),
              );
            if (pending.size >= limits.pending)
              return yield* localFailure(
                "enqueue",
                ReactorError.fromCode("Overflow", "H3 pending acceptance bound reached"),
              );
            // The operation's slot is taken before anything is sent.
            yield* pure(() => operations.reserve(entry)).pipe(
              Effect.mapError((error) => localFailure("enqueue", error)),
            );
            // Acceptance registration is inside the same commit boundary as the
            // orchestration hook and precedes Session.command's wire correlation.
            pending.set(id, entry);
            if (hooks.commit !== undefined)
              yield* hooks.commit(id).pipe(
                Effect.onExit((exit) =>
                  Exit.isFailure(exit)
                    ? Effect.sync(() => {
                        pending.delete(id);
                        operations.abandon(id);
                      })
                    : Effect.void,
                ),
              );
          }),
        execute: ({ args, entry }) => {
          const execution = Effect.gen(function* () {
            const sent = yield* Effect.result(
              session.command("enqueue", args, { replyTimeout: limits.command }),
            );
            let original: CommandFailure;
            if (Result.isFailure(sent)) {
              if (sent.failure.context.outcome !== "unknown") return yield* sent.failure;
              original = sent.failure;
            } else {
              const source = sent.success;
              const observed = yield* Effect.result(awaitObservation(source));
              if (
                Result.isSuccess(observed) &&
                observed.success?.type === "command_error" &&
                observed.success.data.command === "enqueue"
              )
                return yield* rejected("enqueue", source, observed.success.data.reason);
              original = uncertain(
                "enqueue",
                source,
                "H3 enqueue has no proven clip acceptance",
                Result.isFailure(observed) ? observed.failure : undefined,
              );
            }
            // The reply was observed or is lost: evidence by metadata that came
            // while it was awaited now decides, and later evidence at once.
            entry.awaiting = false;
            decideHeld(entry);
            const acceptance = yield* Deferred.await(entry.deferred).pipe(
              Effect.timeoutOrElse({
                duration: limits.reconcile,
                orElse: () => Effect.fail(original),
              }),
              Effect.mapError(() => original),
              Effect.withSpan("reactor.h3.reconcile", {}, { captureStackTrace: false }),
            );
            // Like every command, an enqueue settles once the snapshots its reply
            // implies are observed, so its caller finds the clip in the queue.
            yield* settled;
            return acceptance;
          }).pipe(
            Effect.onExit((exit) =>
              Effect.sync(() => {
                // However the enqueue ends, interrupted as the provider closes or
                // otherwise, the evidence it saw still decides; only a definite
                // failure discards it.
                const error = Exit.findError(exit);
                if (!(error._tag === "Success" && error.success.context.outcome !== "unknown"))
                  decideHeld(entry);
                pending.delete(id);
              }),
            ),
            Effect.onExit((exit) =>
              Effect.sync(() => {
                operations.settle(id, exit);
              }),
            ),
            // The submission's own execution fiber carries the span, so it ends with
            // the enqueue's outcome even when the caller stopped waiting.
            Effect.onExit((exit) => {
              const error = Exit.findError(exit);
              return Effect.annotateCurrentSpan(
                Exit.isSuccess(exit)
                  ? { "reactor.command.outcome": "replied" }
                  : error._tag === "Success"
                    ? {
                        "reactor.command.outcome": error.success.context.outcome,
                        "error.type": error.success.reason._tag,
                      }
                    : {},
              );
            }),
            Effect.withSpan(
              "reactor.h3.enqueue",
              { kind: "client", attributes: { "reactor.h3.submission.id": id } },
              { captureStackTrace: false },
            ),
          );
          return hooks.result === undefined
            ? execution
            : Effect.result(execution).pipe(
                Effect.flatMap((result) =>
                  Effect.gen(function* () {
                    const hook = yield* Effect.exit(
                      Effect.suspend(() => hooks.result!(id, result)).pipe(
                        Effect.interruptible,
                        Effect.timeoutOrElse({
                          duration: limits.hook,
                          orElse: () =>
                            Effect.fail(
                              ReactorError.fromCode(
                                "Timeout",
                                "H3 result hook exceeded its deadline",
                              ),
                            ),
                        }),
                      ),
                    );
                    // Preserve the entire secondary Cause for deliberate inspection,
                    // including defects; never substitute it for primary remote evidence.
                    if (Exit.isFailure(hook))
                      emit({
                        _tag: "Diagnostic",
                        error: ReactorError.fromCode("InvalidState", "H3 result hook failed", {
                          operation: "H3 result hook",
                          detail: hook.cause,
                        }),
                      });
                    return yield* Result.isSuccess(result)
                      ? Effect.succeed(result.success)
                      : Effect.fail(result.failure);
                  }),
                ),
              );
        },
      }).pipe(Scope.provide(executions));
    const prepare: Provider["prepare"] = <E extends PolicyFailure = never>(
      input: Request,
      hooks: PrepareHooks<E> = {},
    ) =>
      Effect.gen(function* () {
        yield* active("enqueue", true);
        const request = yield* pure(() => captureRequest(input, limits.prompt));
        const id = yield* nextId();
        const metadata = yield* pure(() => encodeMetadata(namespace, id, request.metadata));
        return yield* prepared(id, Effect.succeed({ request, metadata }), hooks);
      });
    const prepareFrom: Provider["prepareFrom"] = <E extends PolicyFailure = never>(
      preparation: Effect.Effect<Request, ReactorError | CommandFailure | E, Scope.Scope>,
      hooks: PrepareHooks<E> = {},
    ) =>
      Effect.gen(function* () {
        yield* active("enqueue", true);
        if (!Effect.isEffect(preparation))
          return yield* ReactorError.fromCode("InvalidInput", "H3 preparation must be an Effect", {
            outcome: "not-submitted",
          });
        const id = yield* nextId();
        return yield* prepared(
          id,
          preparation.pipe(
            Effect.flatMap((input) =>
              pure(() => {
                const request = captureRequest(input, limits.prompt);
                return { request, metadata: encodeMetadata(namespace, id, request.metadata) };
              }),
            ),
          ),
          hooks,
        );
      });

    const contract = yield* session.schema.pipe(
      Effect.flatMap(({ openapi }) => pure(() => validateDeployment(openapi))),
      Effect.timeoutOrElse({
        duration: limits.setup,
        orElse: () => Effect.fail(ReactorError.fromCode("Timeout", "H3 schema read timed out")),
      }),
    );
    yield* refresh.pipe(
      Effect.timeoutOrElse({
        duration: limits.setup,
        orElse: () =>
          Effect.fail(
            ReactorError.fromCode("Timeout", "H3 initial state and queue reads timed out"),
          ),
      }),
    );
    if (state.snapshot()._tag !== "Ready")
      return yield* (
        fatalError ??
          ReactorError.fromCode("InvalidState", "H3 initial observations are unavailable")
      );

    const provider: Provider = {
      sessionId: session.id,
      contract,
      current: Effect.sync(() => state.snapshot()),
      observe: (bounds) =>
        Effect.gen(function* () {
          const stream = yield* events.subscribe(bounds);
          const initial = state.snapshot();
          // The reducer may apply a queued event between the subscription and
          // this read; the snapshot then covers it, so it is not repeated.
          return {
            initial,
            revision: initial.revision,
            events: Stream.filter(stream, (event) => !covered(event, initial.revision)),
          };
        }),
      events: (bounds) => events.stream(bounds),
      failure: Deferred.await(fatal),
      acceptances: Effect.sync(() => Object.freeze([...acceptances.values()])),
      acceptance: (id) => Effect.sync(() => acceptances.get(id)),
      operation: (submission) => operations.attach(submission.id),
      prepare,
      prepareFrom,
      enqueue: (request) =>
        prepare(request).pipe(
          Effect.mapError((error) =>
            ReactorError.is(error) ? localFailure("enqueue", error) : error,
          ),
          Effect.flatMap((submission) => submission.submit),
        ),
      getState,
      getQueue,
      refresh,
      pop: (id) =>
        checked("pop", () => clipId(id)).pipe(
          Effect.flatMap((id) => named("pop", { clip_id: id })),
          Effect.filterOrFail(
            (reply) => reply.value.clip.clip_id === id,
            (reply) => uncertain("pop", reply.source, "H3 pop returned a different clip"),
          ),
        ),
      move: (id, position) =>
        checked("move", () => ({
          clip_id: clipId(id),
          position: nonnegative(position, "position"),
        })).pipe(
          Effect.flatMap((args) => named("move", args)),
          Effect.filterOrFail(
            (reply) => reply.value.clip.clip_id === id,
            (reply) => uncertain("move", reply.source, "H3 move returned a different clip"),
          ),
        ),
      play: (id) =>
        checked("play", () => ({ clip_id: id === undefined ? "" : clipId(id) })).pipe(
          Effect.flatMap((args) => control("play", args)),
        ),
      stop: control("stop", {}),
      setSeed: (value) =>
        checked("set_seed", () => nonnegative(value, "seed")).pipe(
          Effect.flatMap((seed) => named("set_seed", { seed })),
        ),
      setClipSeconds: (value) =>
        checked("set_clip_seconds", () => seconds(value)).pipe(
          Effect.flatMap((seconds) => named("set_clip_seconds", { seconds })),
        ),
      setCanvas: (aspect) =>
        checked("set_canvas", () => {
          if (!Object.hasOwn(canvases, aspect))
            throw ReactorError.fromCode("InvalidInput", "Unsupported H3 canvas aspect");
          return { aspect };
        }).pipe(Effect.flatMap((args) => named("set_canvas", args))),
      setAutoplay: (enabled) =>
        checked("set_autoplay", () => {
          if (typeof enabled !== "boolean")
            throw ReactorError.fromCode("InvalidInput", "Autoplay must be boolean");
          return { enabled };
        }).pipe(Effect.flatMap((args) => named("set_autoplay", args))),
      setFlushOnClipEnd: (enabled) =>
        checked("set_flush_on_clip_end", () => {
          if (typeof enabled !== "boolean")
            throw ReactorError.fromCode("InvalidInput", "Flush setting must be boolean");
          return { enabled };
        }).pipe(Effect.flatMap((args) => named("set_flush_on_clip_end", args))),
      reset: named("reset", {}),
    };
    return Object.freeze(provider);
  });

/** Failed acquisition closes its local observation scope immediately. */
export const make = (
  session: Session,
  options: Options = {},
): Effect.Effect<Provider, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope> =>
  Effect.gen(function* () {
    const child = yield* Scope.fork(yield* Effect.scope);
    return yield* build(session, options).pipe(
      Scope.provide(child),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(child, exit) : Effect.void)),
    );
  });
