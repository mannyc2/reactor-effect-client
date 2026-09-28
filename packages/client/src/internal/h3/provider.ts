/**
 * The H3 provider over a connected session. One fiber applies the session's
 * events to the reducer; commands wait on the state it publishes rather than
 * on waiter registries, and every transition returns the waiters it settles.
 */
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type {
  ControlResult,
  Options,
  PrepareHooks,
  Provider,
  ProviderEvent,
  Reply,
} from "../../H3.js";
import { CommandFailure, ReactorError, Remote } from "../../ReactorError.js";
import type { CommandReply, Session, SessionEvent, UploadReference } from "../../Session.js";
import * as Submission from "./submission.js";
import * as Hub from "../hub.js";
import { Commands, deploymentContract } from "./commands.js";
import type {
  CommandArgs,
  CommandName,
  ControlCommand,
  ReplyCommand,
  ReplyType,
} from "./commands.js";
import { decodeMessage } from "./messages.js";
import type { Clip, Payload } from "./messages.js";
import * as Operations from "./operations.js";
import { canvases, requestSeconds } from "./profile.js";
import type { CanvasAspect } from "./profile.js";
import { materialOf, validateAudioReference, validateReference } from "./references.js";
import type { ValidatedAudioReference, ValidatedReference } from "./references.js";
import { enqueueArguments, Request } from "./request.js";
import type { Captured } from "./request.js";
import * as State from "./state.js";
import type { Acceptance, Identity } from "./state.js";

/** Bounds on what the provider retains; none is an application setting. */
const bounds = {
  pending: 128,
  clips: 4096,
  acceptances: 1024,
  operations: 1024,
  uploads: 256,
  observation: 1024,
};

interface Pending extends Identity {
  readonly deferred: Deferred.Deferred<Acceptance, ReactorError>;
  /** The enqueue still awaits its reply, the one evidence that correlates. */
  readonly awaiting: boolean;
  /**
   * Metadata evidence that came while the reply was awaited: hosted H3
   * broadcasts the queue that lists a new clip before it replies. The reply
   * decides whether it counts.
   */
  readonly held: Acceptance | undefined;
}

interface Internal {
  readonly model: State.Model;
  readonly closed: boolean;
  readonly fatal: ReactorError | undefined;
  readonly pending: ReadonlyMap<string, Pending>;
  readonly acceptances: ReadonlyMap<string, Acceptance>;
  readonly operations: Operations.Table;
}

type Transition = readonly [Internal, ReadonlyArray<Effect.Effect<void>>];

const localFailure = (operation: string, cause: ReactorError | CommandFailure): CommandFailure =>
  CommandFailure.from(cause, { ...cause.context, operation, outcome: "not-submitted" });

/** A caller's own refusal already proves no dispatch; any other failure becomes one. */
const preparationFailure = <E>(cause: ReactorError | CommandFailure | E): CommandFailure | E =>
  ReactorError.is(cause) || CommandFailure.is(cause) ? localFailure("enqueue", cause) : cause;

const uncertain = (
  operation: string,
  source: CommandReply,
  message: string,
  cause?: ReactorError,
): CommandFailure =>
  CommandFailure.from(cause ?? ReactorError.fromCode("UnexpectedReply", message), {
    ...cause?.context,
    operation,
    outcome: "unknown",
    requestId: source.requestId,
    generation: source.generation,
  });

/** H3's refusal reason is free text with no stable codes: kept for diagnosis only. */
const rejected = (operation: string, source: CommandReply, reason: string): CommandFailure =>
  CommandFailure.make({
    reason: Remote.make({
      _tag: "Remote",
      message: `H3 ${operation} was refused`,
      body: Redacted.make(reason),
    }),
    context: {
      operation,
      outcome: "replied",
      requestId: source.requestId,
      generation: source.generation,
    },
  });

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

/** Whether a snapshot at `revision` already reflects `event`. */
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

const durationOf = (input: Duration.Input | undefined, fallback: Duration.Input) =>
  Duration.fromInput(input ?? fallback);

const build = Effect.fnUntraced(function* (session: Session, options: Options) {
  const scope = yield* Effect.scope;
  const crypto = yield* Crypto.Crypto;
  const invalid = (name: string) =>
    ReactorError.fromCode("InvalidInput", `H3 ${name} must be a duration`, {
      outcome: "not-submitted",
    });
  const limits = {
    command: yield* Effect.fromOption(durationOf(options.replyTimeout, "15 seconds")).pipe(
      Effect.mapError(() => invalid("replyTimeout")),
    ),
    upload: yield* Effect.fromOption(durationOf(options.uploadTimeout, "60 seconds")).pipe(
      Effect.mapError(() => invalid("uploadTimeout")),
    ),
    setup: yield* Effect.fromOption(durationOf(options.setupTimeout, "60 seconds")).pipe(
      Effect.mapError(() => invalid("setupTimeout")),
    ),
    reconcile: yield* Effect.fromOption(durationOf(options.reconcileWindow, "5 seconds")).pipe(
      Effect.mapError(() => invalid("reconcileWindow")),
    ),
  };
  // A provider attaches to an already connected session. It neither allocates
  // nor connects it, and never takes ownership of its remote lifetime.
  yield* session.ready;
  const observation = yield* session.observe({ capacity: bounds.observation });
  const namespace = hex(
    yield* crypto.randomBytes(16).pipe(
      Effect.mapError(() =>
        ReactorError.fromCode("InvalidState", "Could not allocate an H3 acceptance namespace", {
          outcome: "not-submitted",
        }),
      ),
    ),
  );
  const hub = yield* Hub.make<ProviderEvent>();
  const fatal = yield* Deferred.make<ReactorError>();
  const counter = yield* Ref.make(0n);
  const uploads = yield* Ref.make<ReadonlyMap<string, UploadReference>>(new Map());
  const uploadGate = yield* Semaphore.make(1);
  const state = yield* SubscriptionRef.make<Internal>({
    model: State.initial({
      sessionId: session.id,
      generation: observation.initial.generation,
      revision: observation.revision,
      maxClips: bounds.clips,
    }),
    closed: false,
    fatal: undefined,
    pending: new Map(),
    acceptances: new Map(),
    operations: Operations.empty(bounds.operations),
  });

  /** Applies a transition atomically, then settles the waiters it names, in order. */
  const step = (transition: (internal: Internal) => Transition): Effect.Effect<void> =>
    SubscriptionRef.modify(state, (internal) => {
      const [next, effects] = transition(internal);
      return [effects, next] as const;
    }).pipe(
      Effect.flatMap((effects) => Effect.forEach(effects, (effect) => effect, { discard: true })),
    );

  const publish = (event: ProviderEvent): Effect.Effect<void> => hub.publish(event);

  const withPending = (internal: Internal, id: string, pending: Pending | undefined): Internal => {
    const next = new Map(internal.pending);
    if (pending === undefined) next.delete(id);
    else next.set(id, pending);
    return { ...internal, pending: next };
  };

  /** Records a submission's acceptance, resolves its waiter and announces it. */
  const record = (internal: Internal, entry: Pending, acceptance: Acceptance): Transition => {
    const acceptances = new Map(internal.acceptances);
    if (acceptances.size >= bounds.acceptances)
      acceptances.delete(acceptances.keys().next().value ?? "");
    acceptances.set(entry.id, acceptance);
    const [operations, settled] = Operations.accept(internal.operations, acceptance);
    return [
      {
        ...withPending(internal, entry.id, { ...entry, held: undefined }),
        acceptances,
        operations,
      },
      [
        Effect.asVoid(Deferred.succeed(entry.deferred, acceptance)),
        ...settled,
        publish({ _tag: "Acceptance", acceptance }),
      ],
    ];
  };

  /** Held evidence decides once the reply cannot: it was lost, or it came without correlating. */
  const decideHeld = (internal: Internal, id: string): Transition => {
    const entry = internal.pending.get(id);
    return entry?.held === undefined ? [internal, []] : record(internal, entry, entry.held);
  };

  const sequence = (
    internal: Internal,
    transitions: ReadonlyArray<(internal: Internal) => Transition>,
  ): Transition => {
    let current = internal;
    const effects: Array<Effect.Effect<void>> = [];
    for (const transition of transitions) {
      const [next, more] = transition(current);
      current = next;
      effects.push(...more);
    }
    return [current, effects];
  };

  /**
   * The provider fails for good. Held evidence is recorded first, so a waiter
   * it resumes already finds the provider refusing new work.
   */
  const failProvider =
    (error: ReactorError) =>
    (internal: Internal): Transition => {
      if (internal.fatal !== undefined) return [internal, []];
      const failed: Internal = {
        ...internal,
        fatal: error,
        model: State.unavailable(internal.model, error),
      };
      const [decided, recorded] = sequence(
        failed,
        [...failed.pending.keys()].map((id) => (current: Internal) => decideHeld(current, id)),
      );
      return [
        decided,
        [
          ...recorded,
          Effect.asVoid(Deferred.succeed(fatal, error)),
          ...[...decided.pending.values()].map((entry) =>
            Effect.asVoid(Deferred.fail(entry.deferred, error)),
          ),
          publish({ _tag: "Diagnostic", error }),
        ],
      ];
    };

  const accept =
    (clip: Clip, source: CommandReply) =>
    (internal: Internal): Transition => {
      const id = State.submissionFromMetadata(namespace, clip.metadata);
      if (id === undefined) return [internal, []];
      const entry = internal.pending.get(id);
      const acceptance = entry === undefined ? undefined : State.acceptanceFor(entry, clip, source);
      if (entry === undefined || acceptance === undefined) {
        // After the reconcile window, or in a later generation, the evidence
        // can still resolve the operation, never the acceptances.
        const [operations, settled] = Operations.lateEvidence(
          internal.operations,
          id,
          clip,
          source,
        );
        return [{ ...internal, operations }, settled];
      }
      const previous = internal.acceptances.get(id) ?? entry.held;
      if (previous !== undefined && previous.clip.clip_id !== clip.clip_id)
        return failProvider(
          ReactorError.fromCode("Protocol", "H3 acceptance identity named two clips", {
            operation: "enqueue",
          }),
        )(internal);
      if (internal.acceptances.has(id)) return [internal, []];
      // Until the reply is observed it may still correlate the acceptance; the
      // clip's facts accrue meanwhile.
      if (acceptance.evidence.kind === "metadata" && entry.awaiting)
        return entry.held === undefined
          ? [
              {
                ...withPending(internal, id, { ...entry, held: acceptance }),
                operations: Operations.identify(internal.operations, acceptance),
              },
              [],
            ]
          : [internal, []];
      return record(internal, entry, acceptance);
    };

  const disconnected = ReactorError.fromCode(
    "Disconnected",
    "H3 acceptance crossed a transport generation",
    { operation: "enqueue" },
  );

  const reduce =
    (source: SessionEvent) =>
    (internal: Internal): Transition => {
      if (internal.closed || internal.fatal !== undefined) return [internal, []];
      const [disposition, admitted] = State.admit(internal.model, source);
      let current: Internal = { ...internal, model: admitted };
      const effects: Array<Effect.Effect<void>> = [];
      if (admitted.generation !== internal.model.generation) {
        effects.push(Ref.set(uploads, new Map()));
        // No reply to an earlier generation's enqueue can correlate now.
        for (const entry of current.pending.values())
          if (entry.generation !== source.generation) {
            const [decided, recorded] = decideHeld(current, entry.id);
            current = decided;
            effects.push(...recorded, Effect.asVoid(Deferred.fail(entry.deferred, disconnected)));
          }
      }
      if (source._tag !== "Model")
        return [current, [...effects, publish({ _tag: "Session", source })]];
      if (source.kind === "ack")
        return [current, [...effects, publish({ _tag: "Acknowledged", source })]];
      const decoded = decodeMessage(source);
      if (Result.isFailure(decoded)) {
        const [failed, failure] = failProvider(decoded.failure)(current);
        return [failed, [...effects, ...failure]];
      }
      const message = decoded.success;
      let applied = disposition;
      if (disposition === "applied") {
        const result = State.apply(current.model, message, source);
        if (Result.isFailure(result)) {
          const [failed, failure] = failProvider(result.failure)(current);
          return [failed, [...effects, ...failure]];
        }
        applied = result.success[0];
        current = { ...current, model: result.success[1] };
      }
      if (applied !== "stale" && message.type !== "unknown") {
        const clips =
          message.type === "queue_update"
            ? [...message.data.generation, ...message.data.playout, ...message.data.history]
            : "clip" in message.data
              ? [message.data.clip]
              : [];
        const [accepted, acceptedEffects] = sequence(
          current,
          clips.map((clip) => accept(clip, source)),
        );
        const [operations, observed] = Operations.observe(accepted.operations, message, source);
        current = { ...accepted, operations };
        effects.push(...acceptedEffects, ...observed);
      }
      return [
        current,
        [...effects, publish({ _tag: "Message", message, source, disposition: applied })],
      ];
    };

  yield* Effect.addFinalizer(() =>
    step((internal) => {
      const [failed, failure] = failProvider(
        ReactorError.fromCode("Closed", "H3 provider scope closed", {
          operation: "H3 observation",
        }),
      )(internal);
      const [operations, retired] = Operations.retire(failed.operations);
      return [
        { ...failed, closed: true, pending: new Map(), operations },
        [...failure, ...retired, hub.end],
      ];
    }),
  );
  yield* observation.events.pipe(
    Stream.runForEach((source) => step(reduce(source))),
    Effect.catch((error) => error.pipe(failProvider, step)),
    Effect.andThen(
      step((internal) =>
        internal.closed
          ? [internal, []]
          : failProvider(ReactorError.fromCode("Closed", "H3 session observation ended"))(internal),
      ),
    ),
    Effect.forkScoped,
  );
  // Enqueues run in a scope of their own. Finalizers run last-added first, so a
  // closing provider refuses new work before that scope interrupts them.
  const executions = yield* Scope.fork(scope);
  yield* Effect.addFinalizer(() =>
    SubscriptionRef.update(state, (internal) => ({ ...internal, closed: true })),
  );

  const refusal = (internal: Internal, needsFacts: boolean): ReactorError | undefined =>
    internal.closed
      ? ReactorError.fromCode("Closed", "H3 provider scope is closed")
      : (internal.fatal ??
        (needsFacts && State.availability(internal.model) !== "Ready"
          ? ReactorError.fromCode(
              "InvalidState",
              "H3 provider needs current state and queue observations",
            )
          : undefined));

  /** Waits, within one reply deadline, while the provider is only synchronizing. */
  const settled: Effect.Effect<void> = SubscriptionRef.changes(state).pipe(
    Stream.filter(
      (internal) =>
        refusal(internal, false) !== undefined ||
        State.availability(internal.model) !== "Synchronizing",
    ),
    Stream.runHead,
    Effect.timeoutOption(limits.command),
    Effect.asVoid,
  );

  const active = Effect.fnUntraced(function* (operation: string, needsFacts: boolean) {
    if (needsFacts) yield* settled;
    const error = refusal(yield* SubscriptionRef.get(state), needsFacts);
    if (error !== undefined) return yield* localFailure(operation, error);
  });

  /**
   * The reducer's own reading of a reply: it waits until the reducer applied
   * the reply's event, so the caller reads the effects its command caused.
   */
  const awaitReply = Effect.fnUntraced(function* (source: CommandReply) {
    const reached = yield* SubscriptionRef.changes(state).pipe(
      Stream.filter(
        (internal) =>
          internal.model.revision >= source.sequence ||
          internal.model.generation > source.generation ||
          refusal(internal, false) !== undefined,
      ),
      Stream.runHead,
      Effect.timeoutOption(limits.command),
    );
    const internal =
      reached._tag === "Some" && reached.value._tag === "Some" ? reached.value.value : undefined;
    if (internal === undefined)
      return yield* ReactorError.fromCode(
        "Timeout",
        "H3 did not observe the command's exact returned envelope",
      );
    if (internal.model.revision < source.sequence) {
      const error = refusal(internal, false);
      if (error !== undefined) return yield* error;
    }
    if (
      source.correlation === "stale-generation" ||
      (internal.model.revision < source.sequence && internal.model.generation > source.generation)
    )
      return yield* ReactorError.fromCode("Disconnected", "H3 received a stale command reply");
    if (source.kind === "ack") return undefined;
    return yield* Effect.fromResult(decodeMessage(source));
  });

  const call = Effect.fnUntraced(function* <K extends CommandName>(
    operation: K,
    args: CommandArgs<K>,
    needsFacts: boolean,
  ) {
    yield* active(operation, needsFacts);
    const source = yield* session.command(operation, args, { replyTimeout: limits.command });
    const message = yield* awaitReply(source).pipe(
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
          ? // The broadcasts a reply implies follow it: the command settles once they
            // are observed. The state and queue reads are that barrier themselves.
            (needsFacts ? settled : Effect.void).pipe(
              // TypeScript cannot correlate the reply type with its command.
              Effect.as({ value: message.data as Payload<ReplyType<K>>, source }),
            )
          : Effect.fail(uncertain(operation, source, `H3 ${operation} did not return ${expected}`)),
      ),
    );
  };

  const control = <K extends ControlCommand>(
    operation: K,
    args: CommandArgs<K>,
  ): Effect.Effect<ControlResult, CommandFailure> =>
    call(operation, args, true).pipe(
      Effect.flatMap(({ source, message }): Effect.Effect<ControlResult, CommandFailure> => {
        if (source.kind === "ack") return Effect.succeed({ _tag: "Acknowledged", source });
        if (message === undefined || message.type === "unknown")
          return Effect.fail(uncertain(operation, source, "Unexpected H3 control response"));
        return Effect.as(settled, { _tag: "Reply", message, source });
      }),
    );

  const getState = named("get_state", {}, false);
  const getQueue = named("get_queue", {}, false);
  const refresh: Effect.Effect<void, CommandFailure> = getState.pipe(
    Effect.andThen(getQueue),
    Effect.flatMap((reply) =>
      Effect.flatMap(SubscriptionRef.get(state), (internal) =>
        State.availability(internal.model) === "Ready"
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
    ),
  );

  /** One reference's upload, content-addressed within a transport generation. */
  const upload = Effect.fnUntraced(function* (
    reference: ValidatedReference | ValidatedAudioReference,
    generation: bigint,
  ) {
    const material = materialOf(reference);
    if (material === undefined)
      return yield* localFailure(
        "enqueue",
        ReactorError.fromCode("InvalidInput", "Reference was not validated by H3"),
      );
    if (material._tag === "Uploaded") return material.file;
    const digest = yield* crypto
      .digest("SHA-256", material.bytes)
      .pipe(
        Effect.mapError(() =>
          localFailure(
            "enqueue",
            ReactorError.fromCode("InvalidState", "Could not hash an H3 reference"),
          ),
        ),
      );
    const key = `${generation}:${hex(digest)}`;
    return yield* uploadGate.withPermit(
      Effect.gen(function* () {
        const cached = (yield* Ref.get(uploads)).get(key);
        if (cached !== undefined) return cached;
        const uploaded = yield* session
          .upload(`h3-${hex(digest)}`, reference.mimeType, material.bytes, {
            uploadTimeout: limits.upload,
          })
          .pipe(Effect.mapError((error) => localFailure("enqueue", error)));
        if (
          uploaded.file.size !== BigInt(reference.size) ||
          uploaded.file.mime_type !== reference.mimeType
        )
          return yield* localFailure(
            "enqueue",
            ReactorError.fromCode("Protocol", "H3 upload returned different file facts"),
          );
        if ((yield* SubscriptionRef.get(state)).model.generation !== generation)
          return yield* localFailure(
            "enqueue",
            ReactorError.fromCode("Disconnected", "H3 reference upload crossed a generation"),
          );
        yield* Ref.update(uploads, (all) => {
          const next = new Map(all);
          if (next.size >= bounds.uploads) next.delete(next.keys().next().value ?? "");
          return next.set(key, uploaded.file);
        });
        return uploaded.file;
      }),
    );
  });

  /** The request as the caller gave it, decoded and with its references validated and copied. */
  const capture = Effect.fnUntraced(function* (input: Request) {
    const request = yield* Schema.decodeEffect(Request)(input).pipe(
      Effect.mapError((cause) =>
        ReactorError.fromCode("InvalidInput", "Invalid H3 request", {
          operation: "enqueue",
          outcome: "not-submitted",
          detail: cause,
        }),
      ),
    );
    const references = yield* Effect.forEach(request.references ?? [], (reference) =>
      Effect.fromResult(validateReference(reference)),
    );
    const audio = yield* Effect.forEach(request.audio ?? [], (reference) =>
      Effect.fromResult(validateAudioReference(reference)),
    );
    const captured: Captured = { ...request, references, audio };
    return captured;
  });

  const stage = Effect.fnUntraced(function* (request: Captured, metadata: string) {
    yield* active("enqueue", true);
    const internal = yield* SubscriptionRef.get(state);
    const snapshot = State.snapshot(internal.model);
    if (snapshot._tag !== "Ready")
      return yield* localFailure(
        "enqueue",
        ReactorError.fromCode("InvalidState", "H3 is not ready"),
      );
    if (
      request.seconds !== undefined &&
      (request.seconds < snapshot.state.clip_seconds_min ||
        request.seconds > snapshot.state.clip_seconds_max)
    )
      return yield* localFailure(
        "enqueue",
        ReactorError.fromCode("InvalidInput", "Clip duration is outside H3's accepted range"),
      );
    if (request.audio.length > 0 && !contract.referenceAudio)
      return yield* localFailure(
        "enqueue",
        ReactorError.fromCode(
          "UnsupportedCapability",
          "The H3 deployment does not declare reference audio",
        ),
      );
    const generation = snapshot.transportGeneration;
    const images = yield* Effect.forEach(request.references, (reference) =>
      upload(reference, generation),
    );
    const audio = yield* Effect.forEach(request.audio, (reference) =>
      upload(reference, generation),
    );
    return { args: enqueueArguments({ request, images, audio, metadata }), generation };
  });

  const nextId = Ref.modify(counter, (n) => [`${namespace}:${n + 1n}`, n + 1n] as const);

  /** The enqueue itself, run once in the submission's own execution fiber. */
  const execute = (id: string, args: CommandArgs<"enqueue">, entry: Pending) =>
    Effect.gen(function* () {
      const sent = yield* Effect.result(
        session.command("enqueue", args, { replyTimeout: limits.command }),
      );
      let original: CommandFailure;
      if (Result.isFailure(sent)) {
        if (sent.failure.context.outcome !== "unknown") return yield* sent.failure;
        original = sent.failure;
      } else {
        const source = sent.success;
        const observed = yield* Effect.result(awaitReply(source));
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
      // The reply was observed or is lost: evidence held meanwhile decides now.
      yield* step((internal) => {
        const current = internal.pending.get(id);
        return current === undefined
          ? [internal, []]
          : decideHeld(withPending(internal, id, { ...current, awaiting: false }), id);
      });
      const acceptance = yield* Deferred.await(entry.deferred).pipe(
        Effect.timeoutOrElse({ duration: limits.reconcile, orElse: () => Effect.fail(original) }),
        Effect.mapError(() => original),
        Effect.withSpan("reactor.h3.reconcile", {}, { captureStackTrace: false }),
      );
      // An enqueue settles once the snapshots its reply implies are observed.
      yield* settled;
      return acceptance;
    }).pipe(
      Effect.onExit((exit) =>
        step((internal) => {
          // However the enqueue ends, the evidence it saw still decides; only a
          // definite failure discards it.
          const error = Exit.findError(exit);
          const definite = error._tag === "Success" && error.success.context.outcome !== "unknown";
          const [decided, recorded] = definite ? [internal, []] : decideHeld(internal, id);
          const [operations, settledOperation] = Operations.settle(decided.operations, id, exit);
          return [
            { ...withPending(decided, id, undefined), operations },
            [...recorded, ...settledOperation],
          ];
        }),
      ),
      // The span belongs to the execution fiber, so it ends with the enqueue's
      // outcome even when the caller stopped waiting.
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

  const observeResult = <E>(
    id: string,
    hooks: PrepareHooks<E>,
    result: Result.Result<Acceptance, CommandFailure | E>,
  ): Effect.Effect<void> =>
    hooks.result === undefined
      ? Effect.void
      : Effect.suspend(() => hooks.result?.(id, result) ?? Effect.void).pipe(
          Effect.interruptible,
          Effect.timeoutOrElse({
            duration: "1 second",
            orElse: () =>
              Effect.fail(ReactorError.fromCode("Timeout", "H3 result hook exceeded its deadline")),
          }),
          Effect.exit,
          Effect.flatMap((exit) =>
            Exit.isFailure(exit)
              ? publish({
                  _tag: "Diagnostic",
                  error: ReactorError.fromCode("InvalidState", "H3 result hook failed", {
                    operation: "H3 result hook",
                    detail: exit.cause,
                  }),
                })
              : Effect.void,
          ),
        );

  const prepared = <E>(
    id: string,
    input: Effect.Effect<
      { readonly request: Captured; readonly metadata: string },
      ReactorError | CommandFailure | E,
      Scope.Scope
    >,
    hooks: PrepareHooks<E>,
  ) =>
    Submission.make({
      id,
      prepare: Effect.gen(function* () {
        const { request, metadata } = yield* input.pipe(Effect.mapError(preparationFailure));
        const staged = yield* stage(request, metadata);
        const entry: Pending = {
          id,
          metadata,
          prompt: request.prompt,
          generation: staged.generation,
          deferred: yield* Deferred.make<Acceptance, ReactorError>(),
          awaiting: true,
          held: undefined,
        };
        return { args: staged.args, entry, waiters: yield* Operations.makeWaiters };
      }),
      commit: ({ entry, waiters }) =>
        Effect.gen(function* () {
          yield* active("enqueue", true);
          const live = yield* session.ready.pipe(
            Effect.mapError((error) => localFailure("enqueue", error)),
          );
          // The operation's slot and the pending acceptance are taken together,
          // before anything is sent.
          const refused = yield* SubscriptionRef.modify(
            state,
            (internal): readonly [ReactorError | undefined, Internal] => {
              if (
                entry.generation !== internal.model.generation ||
                live.generation !== entry.generation
              )
                return [
                  ReactorError.fromCode(
                    "Disconnected",
                    "H3 prepared input crossed a transport generation",
                  ),
                  internal,
                ];
              if (internal.pending.size >= bounds.pending)
                return [
                  ReactorError.fromCode("Overflow", "H3 pending acceptance bound reached"),
                  internal,
                ];
              const reserved = Operations.reserve(internal.operations, entry, waiters);
              return Result.isFailure(reserved)
                ? [reserved.failure, internal]
                : [
                    undefined,
                    { ...withPending(internal, id, entry), operations: reserved.success },
                  ];
            },
          );
          if (refused !== undefined) return yield* localFailure("enqueue", refused);
          if (hooks.commit !== undefined)
            yield* hooks.commit(id).pipe(
              Effect.onExitIf(Exit.isFailure, () =>
                SubscriptionRef.update(state, (internal) => ({
                  ...withPending(internal, id, undefined),
                  operations: Operations.abandon(internal.operations, id),
                })),
              ),
            );
        }),
      execute: ({ args, entry }) =>
        Effect.result(execute(id, args, entry)).pipe(
          Effect.tap((result) => observeResult(id, hooks, result)),
          Effect.flatMap(Effect.fromResult),
        ),
    }).pipe(Scope.provide(executions));

  const withMetadata = (id: string) => (input: Request) =>
    capture(input).pipe(
      Effect.flatMap((request) =>
        Effect.fromResult(
          State.encodeMetadata({ namespace, submission: id, caller: request.metadata }),
        ).pipe(Effect.map((metadata) => ({ request, metadata }))),
      ),
    );

  const prepare: Provider["prepare"] = <E = never>(input: Request, hooks: PrepareHooks<E> = {}) =>
    Effect.gen(function* () {
      yield* active("enqueue", true);
      const id = yield* nextId;
      const captured = yield* withMetadata(id)(input);
      return yield* prepared(id, Effect.succeed(captured), hooks);
    });

  const prepareFrom: Provider["prepareFrom"] = <E = never>(
    preparation: Effect.Effect<Request, ReactorError | CommandFailure | E, Scope.Scope>,
    hooks: PrepareHooks<E> = {},
  ) =>
    Effect.gen(function* () {
      yield* active("enqueue", true);
      const id = yield* nextId;
      return yield* prepared(id, Effect.flatMap(preparation, withMetadata(id)), hooks);
    });

  const clipId = (operation: string, id: string) =>
    Schema.decodeEffect(Schema.String.check(Schema.isUUID()))(id).pipe(
      Effect.mapError(() =>
        localFailure(operation, ReactorError.fromCode("InvalidInput", "clipId must be a UUID")),
      ),
    );
  const natural = (
    operation: string,
    name: string,
    value: number,
  ): Effect.Effect<number, CommandFailure> =>
    Number.isSafeInteger(value) && value >= 0
      ? Effect.succeed(value)
      : Effect.fail(
          localFailure(
            operation,
            ReactorError.fromCode("InvalidInput", `${name} must be a nonnegative safe integer`),
          ),
        );

  const contract = yield* session.schema.pipe(
    Effect.flatMap(({ openapi }) => Effect.fromResult(deploymentContract(openapi))),
    Effect.timeoutOrElse({
      duration: limits.setup,
      orElse: () => Effect.fail(ReactorError.fromCode("Timeout", "H3 schema read timed out")),
    }),
  );
  yield* refresh.pipe(
    Effect.timeoutOrElse({
      duration: limits.setup,
      orElse: () =>
        Effect.fail(ReactorError.fromCode("Timeout", "H3 initial state and queue reads timed out")),
    }),
  );
  const ready = yield* SubscriptionRef.get(state);
  if (State.availability(ready.model) !== "Ready")
    return yield* (
      ready.fatal ??
        ReactorError.fromCode("InvalidState", "H3 initial observations are unavailable")
    );

  const snapshot = Effect.map(SubscriptionRef.get(state), (internal) =>
    State.snapshot(internal.model),
  );

  const provider: Provider = {
    sessionId: session.id,
    contract,
    snapshot,
    changes: Stream.map(SubscriptionRef.changes(state), (internal) =>
      State.snapshot(internal.model),
    ),
    observe: (observeOptions) =>
      Effect.gen(function* () {
        const events = yield* hub.subscribe(observeOptions?.capacity);
        const initial = yield* snapshot;
        // The reducer may apply an event between the subscription and this read;
        // the snapshot then covers it, so it is not repeated.
        return {
          initial,
          revision: initial.revision,
          events: Stream.filter(events, (event) => !covered(event, initial.revision)),
        };
      }),
    events: (observeOptions) => Stream.unwrap(hub.subscribe(observeOptions?.capacity)),
    failure: Deferred.await(fatal),
    acceptances: Effect.map(SubscriptionRef.get(state), (internal) => [
      ...internal.acceptances.values(),
    ]),
    acceptance: (id) =>
      Effect.map(SubscriptionRef.get(state), (internal) => internal.acceptances.get(id)),
    operation: (submission) =>
      Effect.acquireRelease(
        SubscriptionRef.modify(
          state,
          (internal): readonly [Result.Result<Operations.Waiters, ReactorError>, Internal] => {
            const held = Operations.hold(internal.operations, submission.id);
            if (Result.isFailure(held)) return [Result.fail(held.failure), internal];
            const operation = held.success.operations.get(submission.id);
            return operation === undefined
              ? [
                  Result.fail(ReactorError.fromCode("InvalidState", "H3 clip operation vanished")),
                  internal,
                ]
              : [Result.succeed(operation.waiters), { ...internal, operations: held.success }];
          },
        ).pipe(Effect.flatMap(Effect.fromResult)),
        () =>
          SubscriptionRef.update(state, (internal) => ({
            ...internal,
            operations: Operations.release(internal.operations, submission.id),
          })),
      ).pipe(
        Effect.map((waiters) => ({
          submissionId: submission.id,
          accepted: Deferred.await(waiters.accepted),
          reached: (phase) =>
            Deferred.await(phase === "generated" ? waiters.generated : waiters.started),
          ended: Deferred.await(waiters.finished),
          facts: Effect.map(SubscriptionRef.get(state), (internal) =>
            Operations.factsOf(submission.id, internal.operations.operations.get(submission.id)),
          ),
        })),
      ),
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
      clipId("pop", id).pipe(
        Effect.flatMap((clip) => named("pop", { clip_id: clip })),
        Effect.filterOrFail(
          (reply) => reply.value.clip.clip_id === id,
          (reply) => uncertain("pop", reply.source, "H3 pop returned a different clip"),
        ),
      ),
    move: (id, position) =>
      Effect.all([clipId("move", id), natural("move", "position", position)]).pipe(
        Effect.flatMap(([clip, at]) => named("move", { clip_id: clip, position: at })),
        Effect.filterOrFail(
          (reply) => reply.value.clip.clip_id === id,
          (reply) => uncertain("move", reply.source, "H3 move returned a different clip"),
        ),
      ),
    play: (id) =>
      (id === undefined ? Effect.succeed("") : clipId("play", id)).pipe(
        Effect.flatMap((clip) => control("play", { clip_id: clip })),
      ),
    stop: control("stop", {}),
    setSeed: (value) =>
      natural("set_seed", "seed", value).pipe(
        Effect.flatMap((seed) => named("set_seed", { seed })),
      ),
    setClipSeconds: (value) =>
      Number.isFinite(value) && value >= requestSeconds.min && value <= requestSeconds.max
        ? named("set_clip_seconds", { seconds: value })
        : Effect.fail(
            localFailure(
              "set_clip_seconds",
              ReactorError.fromCode("InvalidInput", "Clip duration is outside the request bounds"),
            ),
          ),
    setCanvas: (aspect: CanvasAspect) =>
      Object.hasOwn(canvases, aspect)
        ? named("set_canvas", { aspect })
        : Effect.fail(
            localFailure(
              "set_canvas",
              ReactorError.fromCode("InvalidInput", "Unsupported H3 canvas aspect"),
            ),
          ),
    setAutoplay: (enabled) => named("set_autoplay", { enabled }),
    setFlushOnClipEnd: (enabled) => named("set_flush_on_clip_end", { enabled }),
    reset: named("reset", {}),
  };
  return provider;
});

/** A failed acquisition closes its own observation scope at once. */
export const make = Effect.fn("H3.make")(function* (session: Session, options: Options = {}) {
  const child = yield* Scope.fork(yield* Effect.scope);
  return yield* build(session, options).pipe(
    Scope.provide(child),
    Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(child, exit) : Effect.void)),
  );
});
