import * as Effect from "effect/Effect";
import * as Orchestration from "reactor-effect-client/orchestration";

/** Applications explicitly choose how each physical source is acquired and funded. */
export const submitWithRenewal = <R,>(
  open: Orchestration.Options<R>["open"],
  request: Orchestration.ClipRequest,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* Orchestration.make({ open });
      const prepared = yield* handle.engine.prepare(request);
      const clipId = yield* prepared.submit;
      const cleanup = yield* handle.close;
      return { clipId, cleanup };
    }),
  );
