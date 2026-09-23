import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { ReactorError } from "./errors.js";
/** Catch acquisition failures in the typed channel and own both cancel AND releaseLock.
 * In pinned Effect, fromReadableStream's default finalizer cancels without releasing;
 * releaseLockOnEnd instead releases without cancellation. Media needs both operations. */
export const fromOwnedReadableStream = <A>(options: {
  readonly evaluate: () => ReadableStream<A>;
  readonly onError: (error: unknown) => ReactorError;
}): Stream.Stream<A, ReactorError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const reader = yield* Effect.acquireRelease(
        Effect.try({ try: () => options.evaluate().getReader(), catch: options.onError }),
        (reader) =>
          Effect.promise(async () => {
            try {
              await reader.cancel();
            } catch {
              /* Underlying media finalizer still runs on stream error. */
            } finally {
              reader.releaseLock();
            }
          }),
      );
      return Stream.unfold(undefined, () =>
        Effect.tryPromise({ try: () => reader.read(), catch: options.onError }).pipe(
          Effect.map((next) => (next.done ? undefined : ([next.value, undefined] as const))),
        ),
      );
    }),
  );
