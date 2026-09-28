/**
 * The run in progress: its evidence, saved after every milestone from before
 * the first token exists, the library's spans, and the secrets its evidence
 * must never hold. A save that fails stops the run.
 */
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as FileSystem from "effect/FileSystem";
import type * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Tracer from "effect/Tracer";
import { isReactorFailure } from "reactor-effect-client/ReactorError";
import type { Evidence, Outcome, Span } from "./Evidence.js";
import type { SaveFailed } from "./Ledger.js";
import { writer } from "./Ledger.js";

export class Run extends Context.Service<
  Run,
  {
    readonly runId: string;
    /** When the run started, in epoch milliseconds on Effect's clock. */
    readonly origin: number;
    /** Milliseconds since the run started, on Effect's clock. */
    readonly now: Effect.Effect<number>;
    readonly evidence: Effect.Effect<Evidence>;
    /** Changes the evidence in memory; the next save writes it. */
    readonly update: (change: (evidence: Evidence) => Evidence) => Effect.Effect<void>;
    readonly save: Effect.Effect<void, SaveFailed>;
    /** Records a step and saves. */
    readonly mark: (step: string, detail?: string) => Effect.Effect<void, SaveFailed>;
    readonly judge: (name: string, failure: string | undefined) => Effect.Effect<void>;
    /** A value the evidence must never contain. */
    readonly secret: (value: Redacted.Redacted<string>) => Effect.Effect<void>;
    /** Records the library's `reactor.*` spans. */
    readonly tracer: Tracer.Tracer;
  }
>()("reactor-effect-integration/hosted/Run") {}

const round = (value: number) => Math.round(value * 10) / 10;

export const make = Effect.fnUntraced(function* (initial: Evidence, file: string) {
  const origin = yield* Clock.currentTimeMillis;
  const state = yield* Ref.make(initial);
  const secrets = yield* Ref.make<ReadonlyArray<Redacted.Redacted<string>>>([]);
  const write = yield* writer(file);
  const spans: Array<Tracer.NativeSpan> = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      if (options.name.startsWith("reactor.") && spans.length < 4096) spans.push(span);
      return span;
    },
  });
  const ms = (nanos: bigint) => round(Number(nanos / 1000n) / 1000 - origin);
  const spanRecords = (): ReadonlyArray<Span> =>
    spans.map((span) => {
      const attributes: Record<string, string | number | boolean> = {};
      for (const [key, value] of span.attributes)
        if (key.startsWith("reactor.") || key === "error.type")
          if (typeof value === "string" || typeof value === "boolean") attributes[key] = value;
          else if (typeof value === "number" && Number.isFinite(value)) attributes[key] = value;
          else if (typeof value === "bigint") attributes[key] = String(value);
      const status = span.status;
      return {
        name: span.name,
        startMs: ms(status.startTime),
        ...(status._tag === "Ended"
          ? { durationMs: round(Number(status.endTime - status.startTime) / 1e6) }
          : {}),
        status: status._tag === "Started" ? "open" : Exit.isSuccess(status.exit) ? "ok" : "error",
        attributes,
      };
    });
  const now = Effect.map(Clock.currentTimeMillis, (at) => round(at - origin));
  const update = (change: (evidence: Evidence) => Evidence) => Ref.update(state, change);
  const save = Effect.gen(function* () {
    const evidence = { ...(yield* Ref.get(state)), spans: spanRecords() };
    yield* write(evidence, yield* Ref.get(secrets));
  });
  return Run.of({
    runId: initial.runId,
    origin,
    now,
    evidence: Effect.map(Ref.get(state), (evidence) => ({ ...evidence, spans: spanRecords() })),
    update,
    save,
    mark: (step, detail) =>
      Effect.flatMap(now, (atMs) =>
        update((evidence) => ({
          ...evidence,
          milestones: [
            ...evidence.milestones,
            { atMs, step, ...(detail === undefined ? {} : { detail }) },
          ],
        })),
      ).pipe(Effect.andThen(save)),
    judge: (name, failure) =>
      update((evidence) => ({
        ...evidence,
        criteria: [
          ...evidence.criteria,
          { name, passed: failure === undefined, ...(failure === undefined ? {} : { detail: failure }) },
        ],
      })),
    secret: (value) => Ref.update(secrets, (all) => [...all, value]),
    tracer,
  });
});

/** Records a failed command's remote outcome, which the stop rules read. */
export const recorded = <A, E, R>(
  command: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Run> =>
  Effect.tapError(command, (error) =>
    Effect.gen(function* () {
      const outcome: Outcome | undefined = isReactorFailure(error) ? error.context.outcome : undefined;
      if (outcome === undefined) return;
      const run = yield* Run;
      yield* run.update((evidence) => ({ ...evidence, outcomes: [...evidence.outcomes, outcome] }));
    }),
  );
