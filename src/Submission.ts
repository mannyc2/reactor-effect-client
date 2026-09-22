import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

/** Prepared is inert. Committed means execution may have reached the peer. */
export type State<A, E> =
  | { readonly _tag: "Prepared" }
  | { readonly _tag: "Committed" }
  | { readonly _tag: "Completed"; readonly exit: Exit.Exit<A, E> };

export interface Submission<A, E> {
  readonly id: string;
  /** Each explicit call may retry interrupted prework; committed execution is never replayed. */
  readonly submit: Effect.Effect<A, E>;
  readonly state: Effect.Effect<State<A, E>>;
}

export interface Options<P, A, E> {
  readonly id: string;
  /** All interruptible uploads, validation and admission waits belong here. */
  readonly prepare: Effect.Effect<P, E, Scope.Scope>;
  /**
   * Optional local commit registration. It runs exactly once after successful
   * interruptible preparation and inside the uninterruptible commit boundary,
   * immediately before the state becomes Committed and execution is forked.
   * Keep it bounded and non-blocking. A failure sends nothing, closes the
   * provisional preparation scope and leaves the Submission retryable/Prepared.
   */
  readonly commit?: (prepared: P) => Effect.Effect<void, E>;
  /** Register correlation before dispatch. This effect must bound its response/reconciliation wait. */
  readonly execute: (prepared: P) => Effect.Effect<A, E>;
}

/**
 * Bind an input to one dispatch without starting it. The caller owns preparation;
 * the supplied scope owns committed execution. No registry entry exists merely
 * because a caller holds a prepared Submission.
 */
export const make = <P, A, E>(options: Options<P, A, E>): Effect.Effect<Submission<A, E>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const gate = yield* Semaphore.make(1);
    let committed: Fiber.Fiber<A, E> | undefined;
    let state: State<A, E> = { _tag: "Prepared" };

    const acquire = gate.withPermit(Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      if (committed !== undefined) return committed;
      // A provisional scope owns locks/staging acquired during preparation. It
      // is closed on cancellation or transferred with the committed execution.
      const provisional = yield* Scope.fork(scope);
      let transferred = false;
      return yield* Effect.gen(function* () {
        const prepared = yield* restore(options.prepare.pipe(Scope.provide(provisional)));
        if (options.commit !== undefined) yield* options.commit(prepared);
        // The fork and ownership transfer form one small commit. The execution
        // itself is interruptible by the session scope, never by a waiting caller.
        state = { _tag: "Committed" };
        const fiber = yield* Effect.suspend(() => options.execute(prepared)).pipe(
          Effect.interruptible,
          Effect.onExit((exit) => Scope.close(provisional, exit)),
          Effect.onExit((exit) => Effect.sync(() => { state = { _tag: "Completed", exit }; })),
          Effect.forkIn(scope, { startImmediately: true }),
        );
        committed = fiber;
        transferred = true;
        return fiber;
      }).pipe(Effect.onExit((exit) => transferred ? Effect.void : Scope.close(provisional, exit)));
    })));

    return {
      id: options.id,
      submit: acquire.pipe(Effect.flatMap(Fiber.join)),
      state: Effect.sync(() => state),
    };
  });
