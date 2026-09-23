import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { errorOf, positiveLimit, ReactorError } from "../../errors.js";
import { Observations } from "../../observation.js";
import { CommandFailure } from "../../session/commands.js";
import type { CommandReply, Session, SessionEvent } from "../../session/index.js";
import * as Submission from "../../Submission.js";
import type { UploadReference } from "../../wire.generated.js";
import { decodeMessage } from "../messages.js";
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
import { ProviderState } from "./state.js";

const pure = <A>(evaluate: () => A): Effect.Effect<A, ReactorError> =>
  Effect.try({ try: evaluate, catch: errorOf });
const localFailure = (operation: string, cause: ReactorError): CommandFailure =>
  CommandFailure.from(cause, { ...cause.context, operation, outcome: "not-submitted" });
const uncertain = (
  operation: string,
  source: CommandReply,
  message: string,
  cause?: ReactorError,
): CommandFailure =>
  CommandFailure.from(cause ?? new ReactorError({ code: "UnexpectedReply", message }), {
    ...(cause?.context ?? {}),
    operation,
    outcome: "unknown",
    requestId: source.requestId,
    generation: source.generation,
  });
const rejected = (operation: string, source: CommandReply): CommandFailure =>
  CommandFailure.from(
    new ReactorError({ code: "Remote", message: `H3 ${operation} was refused` }),
    {
      operation,
      outcome: "replied",
      requestId: source.requestId,
      generation: source.generation,
    },
  );
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

/** Conservative retained-size accounting includes shared objects only once. */
const sizeOf = (input: unknown): number => {
  const pending: unknown[] = [input],
    seen = new Set<object>();
  let bytes = 0,
    nodes = 0;
  while (pending.length > 0) {
    if (++nodes > 1_000_000) return Number.MAX_SAFE_INTEGER;
    const value = pending.pop();
    if (typeof value === "string") bytes += value.length * 3;
    else if (value !== null && typeof value === "object") {
      if (seen.has(value)) continue;
      seen.add(value);
      if (ArrayBuffer.isView(value)) bytes += value.byteLength;
      else if (value instanceof ArrayBuffer) bytes += value.byteLength;
      else if (value instanceof Map) {
        bytes += value.size * 32;
        for (const [key, child] of value) {
          pending.push(key);
          pending.push(child);
        }
      } else if (value instanceof Set) {
        bytes += value.size * 16;
        for (const child of value) pending.push(child);
      } else {
        bytes += 32;
        for (const child of Object.values(value)) pending.push(child);
      }
    } else bytes += 8;
  }
  return bytes;
};

interface PendingAcceptance extends AcceptanceIdentity {
  readonly deferred: Deferred.Deferred<Acceptance, ReactorError>;
}
type ObservationResult = DecodedMessage | undefined;

const build = (
  session: Session,
  options: Options,
): Effect.Effect<Provider, ReactorError, Crypto.Crypto | Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const crypto = yield* Crypto.Crypto;
    const limits = yield* pure(() => ({
      command: positiveLimit(options.commandTimeoutMs ?? 15000, "H3 command deadline", 600000),
      setup: positiveLimit(options.setupTimeoutMs ?? 60000, "H3 setup deadline", 600000),
      reconcile: positiveLimit(
        options.reconcileWindowMs ?? 5000,
        "H3 reconciliation deadline",
        60000,
      ),
      hook: positiveLimit(options.resultHookTimeoutMs ?? 1000, "H3 result hook deadline", 60000),
      pending: positiveLimit(options.maxPending ?? 128, "H3 pending acceptance bound", 4096),
      clips: positiveLimit(options.maxTrackedClips ?? 4096, "H3 clip observation bound", 16384),
      acceptances: positiveLimit(
        options.maxAcceptances ?? 1024,
        "H3 local acceptance bound",
        16384,
      ),
      cache: positiveLimit(options.maxCachedUploads ?? 256, "H3 upload cache bound", 4096),
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
        Effect.mapError(
          () =>
            new ReactorError({
              code: "InvalidState",
              message: "Could not allocate H3 acceptance namespace",
              context: { outcome: "not-submitted" },
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
    const decoded = new WeakMap<CommandReply, Result.Result<ObservationResult, ReactorError>>();
    const observedWaiters = new Map<
      CommandReply,
      Deferred.Deferred<ObservationResult, ReactorError>
    >();
    const uploads = new Map<string, UploadReference>();
    const uploadGate = yield* Semaphore.make(1);

    const emit = (event: ProviderEvent): void => events.emit(event, sizeOf(event));
    const fail = (error: ReactorError): void => {
      if (fatalError !== undefined) return;
      fatalError = error;
      state.unavailable(error);
      Deferred.doneUnsafe(fatal, Effect.succeed(error));
      for (const entry of pending.values()) Deferred.doneUnsafe(entry.deferred, Effect.fail(error));
      for (const waiter of observedWaiters.values())
        Deferred.doneUnsafe(waiter, Effect.fail(error));
      emit({ _tag: "Diagnostic", error });
    };
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        closed = true;
        fail(
          new ReactorError({
            code: "Closed",
            message: "H3 provider scope closed",
            context: { operation: "H3 observation" },
          }),
        );
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
      if (entry === undefined) return;
      const acceptance = acceptanceFor(entry, clip, source);
      if (acceptance === undefined) return;
      const previous = acceptances.get(entry.id);
      if (previous !== undefined) {
        if (previous.clip.clip_id !== clip.clip_id)
          fail(
            new ReactorError({
              code: "Protocol",
              message: "H3 acceptance identity named two clips",
              context: { operation: "enqueue" },
            }),
          );
        return;
      }
      if (acceptances.size >= limits.acceptances) {
        const first = acceptances.keys().next();
        if (!first.done) acceptances.delete(first.value);
      }
      acceptances.set(entry.id, acceptance);
      Deferred.doneUnsafe(entry.deferred, Effect.succeed(acceptance));
      emit({ _tag: "Acceptance", acceptance });
    };

    const reduce = (source: SessionEvent): void => {
      if (closed || fatalError !== undefined) return;
      try {
        const before = state.snapshot().transportGeneration;
        let disposition = state.admit(source);
        if (state.snapshot().transportGeneration !== before) {
          uploads.clear();
          for (const entry of pending.values())
            if (entry.generation !== source.generation)
              Deferred.doneUnsafe(
                entry.deferred,
                Effect.fail(
                  new ReactorError({
                    code: "Disconnected",
                    message: "H3 acceptance crossed a transport generation",
                    context: { operation: "enqueue" },
                  }),
                ),
              );
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
                  new ReactorError({
                    code: "Disconnected",
                    message: "H3 received a stale command acknowledgement",
                  }),
                )
              : Result.succeed(undefined);
          emit({ _tag: "Acknowledged", source });
        } else {
          const message = decodeMessage(source.type, source.data);
          if (disposition === "applied") disposition = state.apply(message, source);
          result =
            disposition === "stale"
              ? Result.fail(
                  new ReactorError({
                    code: "Disconnected",
                    message: "H3 received a stale command message",
                  }),
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
        if (
          sizeOf({ snapshot: state.snapshot(), acceptances: [...acceptances.values()] }) >
          limits.retained
        )
          fail(
            new ReactorError({
              code: "Overflow",
              message: "H3 retained observation byte bound exceeded",
              context: { operation: "H3 observation" },
            }),
          );
      } catch (cause) {
        const error = errorOf(cause);
        if (source._tag === "Model") {
          decoded.set(source, Result.fail(error));
          const waiter = observedWaiters.get(source);
          if (waiter !== undefined) Deferred.doneUnsafe(waiter, Effect.fail(error));
        }
        fail(error);
      }
    };
    yield* observation.events.pipe(
      Stream.runForEach((source) => Effect.sync(() => reduce(source))),
      Effect.catch((error) => Effect.sync(() => fail(error))),
      Effect.andThen(
        Effect.sync(() => {
          if (!closed)
            fail(new ReactorError({ code: "Closed", message: "H3 session observation ended" }));
        }),
      ),
      Effect.forkScoped,
    );

    const active = (operation: string, needsFacts: boolean): Effect.Effect<void, CommandFailure> =>
      Effect.suspend(() => {
        const current = state.snapshot();
        const error = closed
          ? new ReactorError({ code: "Closed", message: "H3 provider scope is closed" })
          : (fatalError ??
            (needsFacts && current._tag !== "Ready"
              ? new ReactorError({
                  code: "InvalidState",
                  message: "H3 provider needs current state and queue observations",
                })
              : undefined));
        return error === undefined ? Effect.void : Effect.fail(localFailure(operation, error));
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
          return Effect.fail(
            new ReactorError({ code: "Overflow", message: "H3 reply observer bound exceeded" }),
          );
        const waiter = Deferred.makeUnsafe<ObservationResult, ReactorError>();
        observedWaiters.set(source, waiter);
        return Deferred.await(waiter).pipe(
          Effect.timeoutOrElse({
            duration: limits.command,
            orElse: () =>
              Effect.fail(
                new ReactorError({
                  code: "Timeout",
                  message: "H3 did not observe the command's exact returned envelope",
                }),
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
        const source = yield* session.command(operation, args, undefined, limits.command);
        const message = yield* awaitObservation(source).pipe(
          Effect.mapError((error) =>
            uncertain(operation, source, "H3 reply observation failed", error),
          ),
        );
        if (message?.type === "command_error" && message.data.command === operation)
          return yield* rejected(operation, source);
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
            ? Effect.succeed(
                Object.freeze({ value: message.data as Payload<ReplyType<K>>, source }),
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
                new ReactorError({
                  code: "InvalidState",
                  message: "H3 full snapshots changed during refresh",
                }),
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
              ? Effect.succeed<ControlResult>(Object.freeze({ _tag: "Reply", message, source }))
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
            new ReactorError({ code: "InvalidState", message: "H3 is not ready" }),
          );
        if (request.seconds !== undefined)
          yield* checked("enqueue", () =>
            seconds(request.seconds, {
              min: before.state.clip_seconds_min,
              max: before.state.clip_seconds_max,
            }),
          );
        const files: UploadReference[] = [];
        for (const reference of request.references) {
          const material = referenceMaterial(reference);
          if (material._tag === "Uploaded") {
            files.push(material.file);
            continue;
          }
          const digest = yield* crypto.digest("SHA-256", material.bytes).pipe(
            Effect.mapError(() =>
              localFailure(
                "enqueue",
                new ReactorError({
                  code: "InvalidState",
                  message: "Could not hash H3 reference",
                }),
              ),
            ),
          );
          const key = `${before.transportGeneration}:${hex(digest)}`;
          const file = yield* uploadGate.withPermit(
            Effect.gen(function* () {
              const cached = uploads.get(key);
              if (cached !== undefined) return cached;
              const uploaded = yield* session
                .upload(`h3-${hex(digest)}`, reference.mimeType, material.bytes, limits.command)
                .pipe(Effect.mapError((error) => localFailure("enqueue", error)));
              const file = yield* checked("enqueue", () => checkedUpload(uploaded.file));
              if (file.size !== BigInt(reference.size) || file.mime_type !== reference.mimeType)
                return yield* localFailure(
                  "enqueue",
                  new ReactorError({
                    code: "Protocol",
                    message: "H3 upload returned different file facts",
                  }),
                );
              if (state.snapshot().transportGeneration !== before.transportGeneration)
                return yield* localFailure(
                  "enqueue",
                  new ReactorError({
                    code: "Disconnected",
                    message: "H3 reference upload crossed a generation",
                  }),
                );
              if (uploads.size >= limits.cache) {
                const first = uploads.keys().next();
                if (!first.done) uploads.delete(first.value);
              }
              uploads.set(key, file);
              return file;
            }),
          );
          files.push(file);
        }
        const args = enqueueArguments(request, files, metadata);
        return { args, generation: before.transportGeneration };
      });

    const nextId = () =>
      pure(() => {
        if (counter === 0xffffffffffffffffn)
          throw new ReactorError({
            code: "Overflow",
            message: "H3 submission identity space exhausted",
            context: { outcome: "not-submitted" },
          });
        return `${namespace}:${++counter}`;
      });
    const prepared = (
      id: string,
      input: Effect.Effect<
        { readonly request: CapturedRequest; readonly metadata: string },
        ReactorError,
        Scope.Scope
      >,
      hooks: PrepareHooks,
    ): ReturnType<Provider["prepare"]> =>
      Submission.make({
        id,
        prepare: Effect.gen(function* () {
          const { request, metadata } = yield* input.pipe(
            Effect.mapError((error) => localFailure("enqueue", error)),
          );
          const staged = yield* stage(request, metadata);
          return {
            ...staged,
            entry: {
              id,
              metadata,
              prompt: request.prompt,
              generation: staged.generation,
              deferred: Deferred.makeUnsafe<Acceptance, ReactorError>(),
            } satisfies PendingAcceptance,
          };
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
                new ReactorError({
                  code: "Disconnected",
                  message: "H3 prepared input crossed a transport generation",
                }),
              );
            if (pending.size >= limits.pending)
              return yield* localFailure(
                "enqueue",
                new ReactorError({
                  code: "Overflow",
                  message: "H3 pending acceptance bound reached",
                }),
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
                      })
                    : Effect.void,
                ),
              );
          }),
        execute: ({ args, entry }) => {
          const execution = Effect.gen(function* () {
            const sent = yield* Effect.result(
              session.command("enqueue", args, undefined, limits.command),
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
                return yield* rejected("enqueue", source);
              original = uncertain(
                "enqueue",
                source,
                "H3 enqueue has no proven clip acceptance",
                Result.isFailure(observed) ? observed.failure : undefined,
              );
            }
            return yield* Deferred.await(entry.deferred).pipe(
              Effect.timeoutOrElse({
                duration: limits.reconcile,
                orElse: () => Effect.fail(original),
              }),
              Effect.mapError(() => original),
            );
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                pending.delete(id);
              }),
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
                              new ReactorError({
                                code: "Timeout",
                                message: "H3 result hook exceeded its deadline",
                              }),
                            ),
                        }),
                      ),
                    );
                    // Preserve the entire secondary Cause for deliberate inspection,
                    // including defects; never substitute it for primary remote evidence.
                    if (Exit.isFailure(hook))
                      emit({
                        _tag: "Diagnostic",
                        error: new ReactorError({
                          code: "InvalidState",
                          message: "H3 result hook failed",
                          context: { operation: "H3 result hook", detail: hook.cause },
                        }),
                      });
                    return yield* Result.isSuccess(result)
                      ? Effect.succeed(result.success)
                      : Effect.fail(result.failure);
                  }),
                ),
              );
        },
      }).pipe(Scope.provide(scope));
    const prepare: Provider["prepare"] = (input, hooks = {}) =>
      Effect.gen(function* () {
        yield* active("enqueue", true);
        const request = yield* pure(() => captureRequest(input, limits.prompt));
        const id = yield* nextId();
        const metadata = yield* pure(() => encodeMetadata(namespace, id, request.metadata));
        return yield* prepared(id, Effect.succeed({ request, metadata }), hooks);
      });
    const prepareFrom: Provider["prepareFrom"] = (preparation, hooks = {}) =>
      Effect.gen(function* () {
        yield* active("enqueue", true);
        if (!Effect.isEffect(preparation))
          return yield* new ReactorError({
            code: "InvalidInput",
            message: "H3 preparation must be an Effect",
            context: { outcome: "not-submitted" },
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
        orElse: () =>
          Effect.fail(new ReactorError({ code: "Timeout", message: "H3 schema read timed out" })),
      }),
    );
    yield* refresh.pipe(
      Effect.timeoutOrElse({
        duration: limits.setup,
        orElse: () =>
          Effect.fail(
            new ReactorError({
              code: "Timeout",
              message: "H3 initial state and queue reads timed out",
            }),
          ),
      }),
    );
    if (state.snapshot()._tag !== "Ready")
      return yield* (
        fatalError ??
          new ReactorError({
            code: "InvalidState",
            message: "H3 initial observations are unavailable",
          })
      );

    const provider: Provider = {
      sessionId: session.id,
      contract,
      current: Effect.sync(() => state.snapshot()),
      observe: (bounds) =>
        Effect.gen(function* () {
          const stream = yield* events.subscribe(bounds);
          const initial = state.snapshot();
          return { initial, revision: initial.revision, events: stream };
        }),
      events: (bounds) => events.stream(bounds),
      failure: Deferred.await(fatal),
      acceptances: Effect.sync(() => Object.freeze([...acceptances.values()])),
      acceptance: (id) => Effect.sync(() => acceptances.get(id)),
      prepare,
      prepareFrom,
      enqueue: (request) =>
        prepare(request).pipe(
          Effect.mapError((error) => localFailure("enqueue", error)),
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
            throw new ReactorError({
              code: "InvalidInput",
              message: "Unsupported H3 canvas aspect",
            });
          return { aspect };
        }).pipe(Effect.flatMap((args) => named("set_canvas", args))),
      setAutoplay: (enabled) =>
        checked("set_autoplay", () => {
          if (typeof enabled !== "boolean")
            throw new ReactorError({ code: "InvalidInput", message: "Autoplay must be boolean" });
          return { enabled };
        }).pipe(Effect.flatMap((args) => named("set_autoplay", args))),
      setFlushOnClipEnd: (enabled) =>
        checked("set_flush_on_clip_end", () => {
          if (typeof enabled !== "boolean")
            throw new ReactorError({
              code: "InvalidInput",
              message: "Flush setting must be boolean",
            });
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
): Effect.Effect<Provider, ReactorError, Crypto.Crypto | Scope.Scope> =>
  Effect.gen(function* () {
    const child = yield* Scope.fork(yield* Effect.scope);
    return yield* build(session, options).pipe(
      Scope.provide(child),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(child, exit) : Effect.void)),
    );
  });
