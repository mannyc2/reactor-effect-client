/**
 * The H3 provider over a connected session. One fiber applies the session's
 * events to the reducer; commands wait on the state it publishes rather than
 * on waiter registries, and every transition returns the waiters it settles.
 */
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as H3 from "../../H3.js";
import { CommandFailure, ReactorError, Remote } from "../../ReactorError.js";
import type { MessageCode } from "../../ReactorError.js";
import type { CommandReply, Session, SessionEvent, UploadReference } from "../../Session.js";
import * as Submission from "./submission.js";
import * as Deadline from "../deadline.js";
import * as Hub from "../hub.js";
import { Commands, deploymentContractFor } from "./commands.js";
import type {
  CommandArgs,
  CommandName,
  ControlCommand,
  ReplyCommand,
  ReplyType,
  Contract,
} from "./commands.js";
import type { Family, RequestFields } from "./family.js";
import { decodeMessageFor, payloadsFor } from "./messages.js";
import type { Clip, DecodedMessage, Message, MessageType, Payload } from "./messages.js";
import * as Operations from "./operations.js";
import type { CanvasAspect } from "./profile.js";
import { materialOf } from "./references.js";
import type { ValidatedAudioReference, ValidatedReference } from "./references.js";
import * as State from "./state.js";
import type { Acceptance, Identity } from "./state.js";

export type Reply<K extends MessageType, C extends Clip = Clip> = Omit<H3.Reply<K>, "value"> & {
  readonly value: Payload<K, C>;
};
export type ControlResult<C extends Clip = Clip> =
  | Exclude<H3.ControlResult, { readonly _tag: "Reply" }>
  | (Omit<Extract<H3.ControlResult, { readonly _tag: "Reply" }>, "message"> & {
      readonly message: Message<MessageType, C>;
    });
export type ProviderEvent<C extends Clip = Clip> =
  | Exclude<H3.ProviderEvent, { readonly _tag: "Message" | "Acceptance" }>
  | (Omit<Extract<H3.ProviderEvent, { readonly _tag: "Message" }>, "message"> & {
      readonly message: DecodedMessage<C>;
    })
  | { readonly _tag: "Acceptance"; readonly acceptance: Acceptance<C> };
export type ProviderObservation<C extends Clip = Clip> = Omit<
  H3.ProviderObservation,
  "initial" | "events"
> & {
  readonly initial: State.ProviderSnapshot<C>;
  readonly events: Stream.Stream<ProviderEvent<C>, ReactorError>;
};
export type PrepareHooks<E = never, C extends Clip = Clip> = Omit<H3.PrepareHooks<E>, "result"> & {
  readonly result?: (
    submissionId: string,
    result: Result.Result<Acceptance<C>, CommandFailure | E>,
  ) => Effect.Effect<void>;
};
interface ProviderMembers<Req, C extends Clip, Name extends string, Version extends string> {
  readonly contract: Contract<Name, Version>;
  readonly snapshot: Effect.Effect<State.ProviderSnapshot<C>>;
  readonly changes: Stream.Stream<State.ProviderSnapshot<C>>;
  readonly observe: (
    options?: H3.ObservationOptions,
  ) => Effect.Effect<ProviderObservation<C>, ReactorError, Scope.Scope>;
  readonly events: (
    options?: H3.ObservationOptions,
  ) => Stream.Stream<ProviderEvent<C>, ReactorError>;
  readonly acceptance: (submissionId: string) => Effect.Effect<Acceptance<C> | undefined>;
  readonly operation: (
    submission: Pick<Submission.Submission<Acceptance<C>, unknown>, "id">,
  ) => Effect.Effect<Operations.ClipOperation<C>, ReactorError, Scope.Scope>;
  readonly prepare: <E = never>(
    request: Req,
    hooks?: PrepareHooks<E, C>,
  ) => Effect.Effect<
    Submission.Submission<Acceptance<C>, CommandFailure | E>,
    ReactorError | CommandFailure
  >;
  readonly enqueue: (request: Req) => Effect.Effect<Acceptance<C>, CommandFailure>;
  readonly getQueue: Effect.Effect<Reply<"queue_update", C>, CommandFailure>;
  readonly pop: (clipId: string) => Effect.Effect<Reply<"clip_popped", C>, CommandFailure>;
  readonly move: (
    clipId: string,
    position: number,
  ) => Effect.Effect<Reply<"clip_moved", C>, CommandFailure>;
  readonly play: (clipId?: string) => Effect.Effect<ControlResult<C>, CommandFailure>;
  readonly stop: Effect.Effect<ControlResult<C>, CommandFailure>;
}
export type Provider<
  Req = H3.Request,
  C extends Clip = Clip,
  Name extends string = typeof H3.modelName,
  Version extends string = typeof H3.documentedVersion,
> = Omit<H3.Provider, keyof ProviderMembers<Req, C, Name, Version>> &
  ProviderMembers<Req, C, Name, Version>;

/** Bounds on what the provider retains; none is an application setting. */
const bounds = {
  pending: 128,
  clips: 4096,
  acceptances: 1024,
  operations: 1024,
  uploads: 256,
  observation: 1024,
};

interface Pending<C extends Clip = Clip> extends Identity {
  readonly deferred: Deferred.Deferred<Acceptance<C>, ReactorError>;
  /** The enqueue still awaits its reply, the one evidence that correlates. */
  readonly awaiting: boolean;
  /**
   * Metadata evidence that came while the reply was awaited: hosted H3
   * broadcasts the queue that lists a new clip before it replies. The reply
   * decides whether it counts, unless it stays away for the reconcile window:
   * then the evidence decides.
   */
  readonly held: Acceptance<C> | undefined;
}

interface Internal<C extends Clip = Clip> {
  readonly model: State.Model<C>;
  readonly closed: boolean;
  readonly fatal: ReactorError | undefined;
  readonly pending: ReadonlyMap<string, Pending<C>>;
  readonly acceptances: ReadonlyMap<string, Acceptance<C>>;
  readonly operations: Operations.Table<C>;
}

type Transition<C extends Clip = Clip> = readonly [Internal<C>, ReadonlyArray<Effect.Effect<void>>];

const localFailure = (operation: string, cause: ReactorError | CommandFailure): CommandFailure =>
  CommandFailure.from(cause, { ...cause.context, operation, outcome: "not-submitted" });

/** `operation` refused before anything was sent. */
const refused = (operation: string, code: MessageCode, message: string): CommandFailure =>
  localFailure(operation, ReactorError.fromCode(code, message));

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

/** The clips a message lists: a queue's three lists, or a lifecycle message's one clip. */
const clipsOf = <C extends Clip>(message: Message<MessageType, C>): ReadonlyArray<C> => {
  if (message.type === "queue_update")
    return [...message.data.generation, ...message.data.playout, ...message.data.history];
  return "clip" in message.data ? [message.data.clip] : [];
};

/** A command's outcome as its span records it; an interruption records none. */
const outcomeOf = (exit: Exit.Exit<unknown, CommandFailure>) => {
  if (Exit.isSuccess(exit)) return { "reactor.command.outcome": "replied" };
  const error = Exit.findError(exit);
  return error._tag === "Success"
    ? {
        "reactor.command.outcome": error.success.context.outcome,
        "error.type": error.success.reason._tag,
      }
    : {};
};

/**
 * Reactor's word that a session is over: it ended the session, or its content moderation did. A
 * 404 or a refused protocol stops the session's own reconnect too, but the application's
 * `session.reconnect` may still bring that session back.
 */
const endedByReactor = (error: ReactorError): boolean =>
  error.reason._tag === "TerminalSession" || error.reason._tag === "Moderated";

/** Whether a snapshot at `revision` already reflects `event`. */
const covered = <C extends Clip>(event: ProviderEvent<C>, revision: bigint): boolean => {
  switch (event._tag) {
    case "Acceptance":
      return false;
    case "Diagnostic":
      return event.source !== undefined && event.source.sequence <= revision;
    default:
      return event.source.sequence <= revision;
  }
};

const build = Effect.fnUntraced(function* <
  Req,
  Captured extends RequestFields,
  C extends Clip,
  Name extends string,
  Version extends string,
>(session: Session, family: Family<Req, Captured, C, Name, Version>, options: H3.Options) {
  const Payloads = payloadsFor(family.clip);
  const decodeMessage = decodeMessageFor<C>(Payloads);
  const scope = yield* Effect.scope;
  const crypto = yield* Crypto.Crypto;
  const limits = {
    command: yield* Deadline.decode("H3 replyTimeout")(options.replyTimeout ?? "15 seconds"),
    upload: yield* Deadline.decode("H3 uploadTimeout")(options.uploadTimeout ?? "60 seconds"),
    setup: yield* Deadline.decode("H3 setupTimeout")(options.setupTimeout ?? "60 seconds"),
    reconcile: yield* Deadline.decode("H3 reconcileWindow")(options.reconcileWindow ?? "5 seconds"),
  };
  // A provider attaches to an already connected session. It neither allocates
  // nor connects it, and never takes ownership of its remote lifetime.
  yield* session.ready;
  const observation = yield* session.observe({ capacity: bounds.observation });
  const namespace = hex(
    yield* crypto.randomBytes(State.namespaceBytes).pipe(
      Effect.mapError(() =>
        ReactorError.fromCode("InvalidState", "Could not allocate an H3 acceptance namespace", {
          outcome: "not-submitted",
        }),
      ),
    ),
  );
  const hub = yield* Hub.make<ProviderEvent<C>>();
  const fatal = yield* Deferred.make<ReactorError>();
  const counter = yield* Ref.make(0n);
  const uploads = yield* Ref.make<ReadonlyMap<string, UploadReference>>(new Map());
  const uploadGate = yield* Semaphore.make(1);
  const state = yield* SubscriptionRef.make<Internal<C>>({
    model: State.initial<C>({
      coherent: family.coherent,
      sessionId: session.id,
      generation: observation.initial.generation,
      revision: observation.revision,
      maxClips: bounds.clips,
    }),
    closed: false,
    fatal: undefined,
    pending: new Map(),
    acceptances: new Map(),
    operations: Operations.empty<C>(bounds.operations),
  });

  /** Applies a transition atomically, then settles the waiters it names, in order. */
  const step = (transition: (internal: Internal<C>) => Transition<C>): Effect.Effect<void> =>
    SubscriptionRef.modify(state, (internal) => {
      const [next, effects] = transition(internal);
      return [effects, next] as const;
    }).pipe(
      Effect.flatMap((effects) => Effect.forEach(effects, (effect) => effect, { discard: true })),
    );

  const withPending = (
    internal: Internal<C>,
    id: string,
    pending: Pending<C> | undefined,
  ): Internal<C> => {
    const next = new Map(internal.pending);
    if (pending === undefined) next.delete(id);
    else next.set(id, pending);
    return { ...internal, pending: next };
  };

  /** Records a submission's acceptance, resolves its waiter and announces it. */
  const record = (
    internal: Internal<C>,
    entry: Pending<C>,
    acceptance: Acceptance<C>,
  ): Transition<C> => {
    const acceptances = new Map(internal.acceptances);
    if (acceptances.size >= bounds.acceptances)
      acceptances.delete(acceptances.keys().next().value ?? "");
    acceptances.set(entry.id, acceptance);
    const [operations, settled] = Operations.accept({ table: internal.operations, acceptance });
    return [
      {
        ...withPending(internal, entry.id, { ...entry, held: undefined }),
        acceptances,
        operations,
      },
      [
        Effect.asVoid(Deferred.succeed(entry.deferred, acceptance)),
        ...settled,
        hub.publish({ _tag: "Acceptance", acceptance }),
      ],
    ];
  };

  /** Held evidence decides once the reply cannot: it was lost, or it came without correlating. */
  const decideHeld = (internal: Internal<C>, id: string): Transition<C> => {
    const entry = internal.pending.get(id);
    return entry?.held === undefined ? [internal, []] : record(internal, entry, entry.held);
  };

  const sequence = (
    internal: Internal<C>,
    transitions: ReadonlyArray<(internal: Internal<C>) => Transition<C>>,
  ): Transition<C> => {
    let current = internal;
    const effects: Array<Effect.Effect<void>> = [];
    for (const transition of transitions) {
      const [next, more] = transition(current);
      current = next;
      effects.push(...more);
    }
    return [current, effects];
  };

  /** Held evidence decides every pending acceptance it can. */
  const decideAll = (internal: Internal<C>): Transition<C> =>
    sequence(
      internal,
      [...internal.pending.keys()].map((id) => (current: Internal<C>) => decideHeld(current, id)),
    );

  /**
   * The provider fails for good and applies no more evidence. Held evidence is
   * recorded first, so a waiter it resumes already finds the provider refusing
   * new work; what evidence did not decide then fails with `error`.
   */
  const failProvider =
    (error: ReactorError) =>
    (internal: Internal<C>): Transition<C> => {
      if (internal.fatal !== undefined) return [internal, []];
      const [decided, recorded] = decideAll({
        ...internal,
        fatal: error,
        model: State.unavailable({ model: internal.model, cause: error }),
      });
      const [operations, stranded] = Operations.failUndecided({
        table: decided.operations,
        error,
      });
      return [
        { ...decided, operations },
        [
          ...recorded,
          Effect.asVoid(Deferred.succeed(fatal, error)),
          ...[...decided.pending.values()].map((entry) =>
            Effect.asVoid(Deferred.fail(entry.deferred, error)),
          ),
          ...stranded,
          hub.publish({ _tag: "Diagnostic", error }),
        ],
      ];
    };

  const accept =
    (clip: C, source: CommandReply) =>
    (internal: Internal<C>): Transition<C> => {
      const id = State.submissionFromMetadata({ namespace, metadata: clip.metadata });
      if (id === undefined) return [internal, []];
      const entry = internal.pending.get(id);
      const acceptance =
        entry === undefined ? undefined : State.acceptanceFor({ identity: entry, clip, source });
      if (entry === undefined || acceptance === undefined) {
        // After the reconcile window, or in a later generation, the evidence
        // can still resolve the operation, never the acceptances.
        const [operations, settled] = Operations.lateEvidence({
          table: internal.operations,
          id,
          clip,
          source,
        });
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
                operations: Operations.identify({ table: internal.operations, acceptance }),
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
    (internal: Internal<C>): Transition<C> => {
      if (internal.closed || internal.fatal !== undefined) return [internal, []];
      const [disposition, admitted] = State.admit({ model: internal.model, source });
      let current: Internal<C> = { ...internal, model: admitted };
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
      if (source._tag !== "Model") {
        effects.push(hub.publish({ _tag: "Session", source }));
        // A dropped session says so once Reactor or its content moderation has ended it, and no
        // evidence comes after that.
        if (
          source._tag === "Diagnostic" &&
          admitted.cause !== undefined &&
          endedByReactor(source.error)
        ) {
          const [failed, failure] = failProvider(source.error)(current);
          return [failed, [...effects, ...failure]];
        }
        return [current, effects];
      }
      if (source.kind === "ack")
        return [current, [...effects, hub.publish({ _tag: "Acknowledged", source })]];
      const decoded = decodeMessage(source);
      if (Result.isFailure(decoded)) {
        const [failed, failure] = failProvider(decoded.failure)(current);
        return [failed, [...effects, ...failure]];
      }
      const message = decoded.success;
      let applied = disposition;
      if (disposition === "applied") {
        const result = State.apply({ model: current.model, message, source });
        if (Result.isFailure(result)) {
          const [failed, failure] = failProvider(result.failure)(current);
          return [failed, [...effects, ...failure]];
        }
        applied = result.success[0];
        current = { ...current, model: result.success[1] };
      }
      if (applied !== "stale" && message.type !== "unknown") {
        const clips = clipsOf(message);
        const [accepted, acceptedEffects] = sequence(
          current,
          clips.map((clip) => accept(clip, source)),
        );
        const [operations, observed] = Operations.observe({
          table: accepted.operations,
          message,
          source,
        });
        current = { ...accepted, operations };
        effects.push(...acceptedEffects, ...observed);
      }
      return [
        current,
        [...effects, hub.publish({ _tag: "Message", message, source, disposition: applied })],
      ];
    };

  // A closing provider retires what evidence did not decide as `Indeterminate`, before its own
  // failure could fail it.
  yield* Effect.addFinalizer(() =>
    step((internal) => {
      const [decided, recorded] = decideAll(internal);
      const [operations, retired] = Operations.retire(decided.operations);
      const [failed, failure] = failProvider(
        ReactorError.fromCode("Closed", "H3 provider scope closed", {
          operation: "H3 observation",
        }),
      )({ ...decided, operations });
      return [
        { ...failed, closed: true, pending: new Map() },
        [...recorded, ...retired, ...failure, hub.end],
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

  const refusal = (internal: Internal<C>, needsFacts: boolean): ReactorError | undefined =>
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
    if (!contract.commands.has(operation))
      return yield* refused(
        operation,
        "UnsupportedCapability",
        `The deployment does not offer H3 ${operation}`,
      );
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
  ): Effect.Effect<Reply<ReplyType<K>, C>, CommandFailure> => {
    const expected = Commands[operation].reply;
    return call(operation, args, needsFacts).pipe(
      Effect.flatMap(({ source, message }) =>
        message?.type === expected
          ? // The payload is read back as its command's reply type, which TypeScript
            // cannot correlate with the command. The broadcasts a reply implies follow
            // it: the command settles once they are observed. The state and queue
            // reads are that barrier themselves.
            Schema.decodeEffect(Payloads[expected])(message.data).pipe(
              Effect.mapError(() =>
                uncertain(operation, source, `H3 ${operation} reply is malformed`),
              ),
              Effect.flatMap((value) =>
                Effect.as(needsFacts ? settled : Effect.void, { value, source }),
              ),
            )
          : Effect.fail(uncertain(operation, source, `H3 ${operation} did not return ${expected}`)),
      ),
    );
  };

  const control = <K extends ControlCommand>(
    operation: K,
    args: CommandArgs<K>,
  ): Effect.Effect<ControlResult<C>, CommandFailure> =>
    call(operation, args, true).pipe(
      Effect.flatMap(({ source, message }): Effect.Effect<ControlResult<C>, CommandFailure> => {
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
      return yield* refused("enqueue", "InvalidInput", "Reference was not validated by H3");
    if (material._tag === "Uploaded") return material.file;
    const digest = yield* crypto
      .digest("SHA-256", material.bytes)
      .pipe(
        Effect.mapError(() => refused("enqueue", "InvalidState", "Could not hash an H3 reference")),
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
          uploaded.file.mimeType !== reference.mimeType
        )
          return yield* refused("enqueue", "Protocol", "H3 upload returned different file facts");
        if ((yield* SubscriptionRef.get(state)).model.generation !== generation)
          return yield* refused(
            "enqueue",
            "Disconnected",
            "H3 reference upload crossed a generation",
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

  const stage = Effect.fnUntraced(function* (request: Captured, metadata: string) {
    yield* active("enqueue", true);
    const internal = yield* SubscriptionRef.get(state);
    const snapshot = State.snapshot(internal.model);
    if (snapshot._tag !== "Ready")
      return yield* refused("enqueue", "InvalidState", "H3 is not ready");
    if (
      request.seconds !== undefined &&
      (request.seconds < snapshot.state.clip_seconds_min ||
        request.seconds > snapshot.state.clip_seconds_max)
    )
      return yield* refused(
        "enqueue",
        "InvalidInput",
        "Clip duration is outside H3's accepted range",
      );
    const refusal = family.admit(request, contract);
    if (refusal !== undefined) return yield* refusal;
    const generation = snapshot.transportGeneration;
    const uploaded = yield* Effect.forEach(family.uploads(request), (reference) =>
      upload(reference, generation),
    );
    return { args: family.encode({ request, uploaded, metadata }), generation };
  });

  const nextId = Ref.modify(counter, (n) => [`${namespace}:${n + 1n}`, n + 1n] as const);

  /** Evidence held for `id` that has waited the reconcile window without its reply. */
  const outwaited = (id: string) =>
    SubscriptionRef.changes(state).pipe(
      Stream.filter((internal) => internal.pending.get(id)?.held !== undefined),
      Stream.runHead,
      Effect.andThen(Effect.sleep(limits.reconcile)),
    );

  /** The enqueue itself, run once in the submission's own execution fiber. */
  const execute = Effect.fnUntraced(
    function* (id: string, args: Schema.JsonObject, entry: Pending<C>) {
      // The command waits for its reply in a fiber of its own, so held evidence can decide
      // first. The request stays attributable after that, and nothing sends it again. That
      // fiber starts at once, so the enqueue is handed to the transport before this execution
      // goes on, as when the execution sent it itself: a command sent behind it stays behind.
      const sending = yield* session
        .command("enqueue", args, { replyTimeout: limits.command })
        .pipe(Effect.forkIn(executions, { startImmediately: true }));
      /**
       * The reply's reading: a refusal fails the enqueue, and anything else is
       * what it fails with when no evidence proves its clip.
       */
      const replied: Effect.Effect<CommandFailure, CommandFailure> = Effect.gen(function* () {
        const sent = yield* sending.pipe(Fiber.join, Effect.result);
        if (Result.isFailure(sent)) {
          if (sent.failure.context.outcome !== "unknown") return yield* sent.failure;
          return sent.failure;
        }
        const source = sent.success;
        const observed = yield* Effect.result(awaitReply(source));
        if (
          Result.isSuccess(observed) &&
          observed.success?.type === "command_error" &&
          observed.success.data.command === "enqueue"
        )
          return yield* rejected("enqueue", source, observed.success.data.reason);
        return uncertain(
          "enqueue",
          source,
          "H3 enqueue has no proven clip acceptance",
          Result.isFailure(observed) ? observed.failure : undefined,
        );
      });
      const answered = yield* Effect.raceFirst(
        Effect.asSome(replied),
        Effect.as(outwaited(id), Option.none<CommandFailure>()),
      );
      // The reply was observed, is lost, or stayed away past the window: evidence held
      // meanwhile decides now.
      yield* step((internal) => {
        const current = internal.pending.get(id);
        return current === undefined
          ? [internal, []]
          : decideHeld(withPending(internal, id, { ...current, awaiting: false }), id);
      });
      // What the enqueue fails with if nothing settles it: the reply's reading, which is
      // still to come when held evidence decided first.
      const unproven: Effect.Effect<never, CommandFailure> = Option.isSome(answered)
        ? Effect.fail(answered.value)
        : Effect.flatMap(replied, (failure) => Effect.fail(failure));
      const acceptance = yield* Deferred.await(entry.deferred).pipe(
        Effect.timeoutOption(limits.reconcile),
        Effect.orElseSucceed(() => Option.none<Acceptance<C>>()),
        Effect.flatMap((decided) =>
          Option.isSome(decided) ? Effect.succeed(decided.value) : unproven,
        ),
        Effect.withSpan("H3.reconcile", {}, { captureStackTrace: false }),
      );
      // An enqueue settles once the snapshots its reply implies are observed.
      yield* settled;
      return acceptance;
    },
    (effect, id) =>
      Effect.onExit(effect, (exit) =>
        step((internal) => {
          // However the enqueue ends, the evidence it saw still decides; only a
          // definite failure discards it.
          const error = Exit.findError(exit);
          const definite = error._tag === "Success" && error.success.context.outcome !== "unknown";
          const [decided, recorded] = definite ? [internal, []] : decideHeld(internal, id);
          const [operations, settledOperation] = Operations.settle({
            table: decided.operations,
            id,
            exit,
          });
          return [
            { ...withPending(decided, id, undefined), operations },
            [...recorded, ...settledOperation],
          ];
        }),
      ),
    // The span belongs to the execution fiber, so it ends with the enqueue's
    // outcome even when the caller stopped waiting.
    Effect.onExit((exit) => exit.pipe(outcomeOf, Effect.annotateCurrentSpan)),
    Effect.withSpan(
      "H3.enqueue",
      (id) => ({ kind: "client", attributes: { "reactor.h3.submission.id": id } }),
      { captureStackTrace: false },
    ),
  );

  const observeResult = <E>(
    id: string,
    hooks: PrepareHooks<E, C>,
    result: Result.Result<Acceptance<C>, CommandFailure | E>,
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
              ? hub.publish({
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
    request: Captured,
    metadata: string,
    hooks: PrepareHooks<E, C>,
  ) =>
    Submission.make({
      id,
      prepare: Effect.gen(function* () {
        const staged = yield* stage(request, metadata);
        const entry: Pending<C> = {
          id,
          metadata,
          prompt: request.prompt,
          generation: staged.generation,
          deferred: yield* Deferred.make<Acceptance<C>, ReactorError>(),
          awaiting: true,
          held: undefined,
        };
        return { args: staged.args, entry, waiters: yield* Operations.makeWaiters<C>() };
      }),
      commit: ({ entry, waiters }) =>
        Effect.gen(function* () {
          yield* active("enqueue", true);
          const live = yield* session.ready.pipe(
            Effect.mapError((error) => localFailure("enqueue", error)),
          );
          // The operation's slot and the pending acceptance are taken together,
          // before anything is sent.
          const conflict = yield* SubscriptionRef.modify(
            state,
            (internal): readonly [ReactorError | undefined, Internal<C>] => {
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
              const reserved = Operations.reserve({
                table: internal.operations,
                identity: entry,
                waiters,
              });
              return Result.isFailure(reserved)
                ? [reserved.failure, internal]
                : [
                    undefined,
                    { ...withPending(internal, id, entry), operations: reserved.success },
                  ];
            },
          );
          if (conflict !== undefined) return yield* localFailure("enqueue", conflict);
          if (hooks.commit !== undefined)
            yield* hooks.commit(id).pipe(
              Effect.onExitIf(Exit.isFailure, () =>
                SubscriptionRef.update(state, (internal) => ({
                  ...withPending(internal, id, undefined),
                  operations: Operations.abandon({ table: internal.operations, id }),
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

  const prepare: Provider<Req, C, Name, Version>["prepare"] = <E = never>(
    input: Req,
    hooks: PrepareHooks<E, C> = {},
  ) =>
    Effect.gen(function* () {
      yield* active("enqueue", true);
      const id = yield* nextId;
      const request = yield* family.capture(input);
      const metadata = yield* Effect.fromResult(
        State.encodeMetadata({ namespace, submission: id, caller: request.metadata }),
      );
      return yield* prepared(id, request, metadata, hooks);
    });

  const clipId = (operation: string, id: string) =>
    Schema.decodeEffect(Schema.String.check(Schema.isUUID()))(id).pipe(
      Effect.mapError(() => refused(operation, "InvalidInput", "clipId must be a UUID")),
    );
  const natural = (
    operation: string,
    name: string,
    value: number,
  ): Effect.Effect<number, CommandFailure> =>
    Number.isSafeInteger(value) && value >= 0
      ? Effect.succeed(value)
      : Effect.fail(
          refused(operation, "InvalidInput", `${name} must be a nonnegative safe integer`),
        );

  const contract = yield* session.schema.pipe(
    Effect.flatMap(({ openapi }) => Effect.fromResult(deploymentContractFor(family)(openapi))),
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

  // H3 interprets a delivered reply and waits for its resulting state after the transport settles.
  const traced = <A>(name: string, effect: Effect.Effect<A, CommandFailure>) =>
    effect.pipe(
      Effect.onExit((exit) => exit.pipe(outcomeOf, Effect.annotateCurrentSpan)),
      Effect.withSpan(
        `H3.${name}`,
        { kind: "client", attributes: { "reactor.session.id": session.id } },
        { captureStackTrace: false },
      ),
    );

  const provider: Provider<Req, C, Name, Version> = {
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
    acceptance: (id) =>
      Effect.map(SubscriptionRef.get(state), (internal) => internal.acceptances.get(id)),
    operation: (submission) =>
      Effect.acquireRelease(
        SubscriptionRef.modify(
          state,
          (
            internal,
          ): readonly [Result.Result<Operations.Waiters<C>, ReactorError>, Internal<C>] => {
            const held = Operations.hold({ table: internal.operations, id: submission.id });
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
            operations: Operations.release({ table: internal.operations, id: submission.id }),
          })),
      ).pipe(
        Effect.map((waiters) => ({
          submissionId: submission.id,
          accepted: Deferred.await(waiters.accepted),
          reached: (phase) =>
            Deferred.await(phase === "generated" ? waiters.generated : waiters.started),
          ended: Deferred.await(waiters.finished),
          facts: Effect.map(SubscriptionRef.get(state), (internal) =>
            Operations.factsOf({
              id: submission.id,
              operation: internal.operations.operations.get(submission.id),
            }),
          ),
        })),
      ),
    prepare,
    enqueue: (request) =>
      prepare(request).pipe(
        Effect.mapError((error) =>
          ReactorError.is(error) ? localFailure("enqueue", error) : error,
        ),
        Effect.flatMap((submission) => submission.submit),
      ),
    getState: traced("getState", getState),
    getQueue: traced("getQueue", getQueue),
    refresh: traced("refresh", refresh),
    pop: (id) =>
      traced(
        "pop",
        clipId("pop", id).pipe(
          Effect.flatMap((clip) => named("pop", { clip_id: clip })),
          Effect.filterOrFail(
            (reply) => reply.value.clip.clip_id === id,
            (reply) => uncertain("pop", reply.source, "H3 pop returned a different clip"),
          ),
        ),
      ),
    move: (id, position) =>
      traced(
        "move",
        Effect.all([clipId("move", id), natural("move", "position", position)]).pipe(
          Effect.flatMap(([clip, at]) => named("move", { clip_id: clip, position: at })),
          Effect.filterOrFail(
            (reply) => reply.value.clip.clip_id === id,
            (reply) => uncertain("move", reply.source, "H3 move returned a different clip"),
          ),
        ),
      ),
    play: (id) =>
      traced(
        "play",
        (id === undefined ? Effect.succeed("") : clipId("play", id)).pipe(
          Effect.flatMap((clip) => control("play", { clip_id: clip })),
        ),
      ),
    stop: traced("stop", control("stop", {})),
    setCanvas: (aspect: CanvasAspect) =>
      traced(
        "setCanvas",
        Object.hasOwn(family.canvases, aspect)
          ? named("set_canvas", { aspect })
          : Effect.fail(refused("set_canvas", "InvalidInput", "Unsupported H3 canvas aspect")),
      ),
    setAutoplay: (enabled) => traced("setAutoplay", named("set_autoplay", { enabled })),
    setFlushOnClipEnd: (enabled) =>
      traced("setFlushOnClipEnd", named("set_flush_on_clip_end", { enabled })),
    reset: traced("reset", named("reset", {})),
  };
  return provider;
});

/** A failed acquisition closes its own observation scope at once. */
export const make = (spanName: string) =>
  Effect.fn(spanName)(function* <
    Req,
    Captured extends RequestFields,
    C extends Clip,
    Name extends string,
    Version extends string,
  >(session: Session, family: Family<Req, Captured, C, Name, Version>, options: H3.Options = {}) {
    const child = yield* Scope.fork(yield* Effect.scope);
    return yield* build(session, family, options).pipe(
      Scope.provide(child),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(child, exit) : Effect.void)),
    );
  });
