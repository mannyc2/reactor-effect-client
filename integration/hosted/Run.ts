/**
 * The run in progress: its evidence, saved after every milestone from before
 * the first token exists, the library's spans, and the secrets its evidence
 * must never hold. A save that fails stops the run. A rehearsal checks each
 * save and writes only the last: its clock runs ahead while real I/O waits,
 * so a write mid-check would move the simulated session's time.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import { isReactorFailure, type ReactorFailure } from "reactor-effect-client/ReactorError";
import type { Evidence, Outcome, Span } from "./Evidence.js";
import { ConnectPhase } from "./Evidence.js";
import { SaveFailed, writer } from "./Ledger.js";
import { Refused, type Model } from "./Spend.js";

export class Run extends Context.Service<
  Run,
  {
    readonly runId: string;
    readonly check: Evidence["check"];
    readonly model: Model;
    /** Where the evidence is saved; files a check makes go beside it, never into it. */
    readonly file: string;
    /** When the run started, in epoch milliseconds on Effect's clock. */
    readonly origin: number;
    /** Milliseconds since the run started, on Effect's clock. */
    readonly now: Effect.Effect<number>;
    readonly evidence: Effect.Effect<Evidence>;
    /** Changes the evidence in memory; the next save writes it. */
    readonly update: (change: (evidence: Evidence) => Evidence) => Effect.Effect<void>;
    readonly save: Effect.Effect<void, SaveFailed>;
    /** The last save: written in a rehearsal too. */
    readonly flush: Effect.Effect<void, SaveFailed>;
    /** Records a step and saves. */
    readonly mark: (step: string, detail?: string) => Effect.Effect<void, SaveFailed>;
    readonly judge: (name: string, failure: string | undefined) => Effect.Effect<void>;
    /** A value the evidence must never contain. */
    readonly secret: (value: Redacted.Redacted<string>) => Effect.Effect<void>;
    /** Records the library's spans the evidence keeps. */
    readonly tracer: Tracer.Tracer;
  }
>()("reactor-effect-integration/hosted/Run") {}

const round = (value: number) => Math.round(value * 10) / 10;

/**
 * The client's spans the evidence keeps: its operations, a command's execution, a source's, and
 * the Vidu S2-Avatar provider's.
 */
const clientSpans =
  /^(?:Reactor\.|Session\.|CoordinatorClient\.|H3\.(?:enqueue|reconcile)$|(?:H3Source|FastH3Source)\.(?:open|resume)$|ViduS2Avatar\.)/;

export const make = Effect.fnUntraced(function* (initial: Evidence, file: string, model: Model) {
  const origin = yield* Clock.currentTimeMillis;
  const state = yield* Ref.make(initial);
  const secrets = yield* Ref.make<ReadonlyArray<Redacted.Redacted<string>>>([]);
  const write_ = yield* writer(file);
  const spans: Array<Tracer.NativeSpan> = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      if (clientSpans.test(options.name) && spans.length < 4096) spans.push(span);
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
        events: span.events.flatMap(([name, time]) =>
          Schema.is(ConnectPhase)(name) ? [{ name, atMs: ms(time) }] : [],
        ),
      };
    });
  const now = Effect.map(Clock.currentTimeMillis, (at) => round(at - origin));
  const update = (change: (evidence: Evidence) => Evidence) => Ref.update(state, change);
  const saving = (write: boolean) =>
    Effect.gen(function* () {
      const evidence = { ...(yield* Ref.get(state)), spans: spanRecords() };
      yield* write_(evidence, yield* Ref.get(secrets), { write });
    });
  const save = saving(initial.mode === "paid");
  return Run.of({
    runId: initial.runId,
    check: initial.check,
    model,
    file,
    origin,
    now,
    evidence: Effect.map(Ref.get(state), (evidence) => ({ ...evidence, spans: spanRecords() })),
    update,
    save,
    flush: saving(true),
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
          {
            name,
            passed: failure === undefined,
            ...(failure === undefined ? {} : { detail: failure }),
          },
        ],
      })),
    secret: (value) => Ref.update(secrets, (all) => [...all, value]),
    tracer,
  });
});

/** A stalled runner delays its own timer even when no session can supply statistics. */
export const liveness = Effect.gen(function* () {
  const run = yield* Run;
  return yield* Effect.gen(function* () {
    const before = yield* Clock.monotonicTimeNanos;
    yield* Effect.sleep("1 second");
    const after = yield* Clock.monotonicTimeNanos;
    const lateMs = round(Number(after - before) / 1e6 - 1000);
    const atMs = yield* run.now;
    yield* run.update((evidence) => {
      const previous = evidence.liveness;
      const samples = previous?.samples ?? [];
      return {
        ...evidence,
        liveness: {
          samples: samples.length < 900 ? [...samples, { atMs, lateMs }] : samples,
          maxLateMs: Math.max(previous?.maxLateMs ?? 0, lateMs),
        },
      };
    });
  }).pipe(Effect.forever);
});

/** Records a failed command's remote outcome, which the stop rules read. */
export const recorded = <A, E, R>(command: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Run> =>
  Effect.tapError(command, (error) =>
    Effect.gen(function* () {
      const outcome: Outcome | undefined = isReactorFailure(error)
        ? error.context.outcome
        : undefined;
      if (outcome === undefined) return;
      const run = yield* Run;
      yield* run.update((evidence) => ({ ...evidence, outcomes: [...evidence.outcomes, outcome] }));
    }),
  );

/**
 * What the native addon itself says when it fails a connection as `Protocol` or `Overflow`: fixed
 * sentences naming at most a bridge channel, with no SDP, signaling or model text. Only these are
 * written down; any other backend text stays redacted.
 */
const addonTexts = [
  /^(control|data) data channel delivered a nonbinary message$/,
  /^(control|data) data channel message exceeds local bound$/,
  /^received native track without a declared receive mapping$/,
  /^native transport event queue overflowed; connection retired$/,
  /^native data channel message exceeds local bound$/,
  /^native data channel buffered amount bound exceeded$/,
  /^native call admission bound exceeded$/,
];

/** The addon's own sentence behind a `Protocol` or `Overflow` failure, if it is one of its own. */
const addonText = (error: ReactorFailure): string | undefined => {
  if (error.reason._tag !== "Protocol" && error.reason._tag !== "Overflow") return undefined;
  const detail =
    error.context.detail === undefined ? undefined : Redacted.value(error.context.detail);
  if (!Predicate.hasProperty(detail, "backendMessage")) return undefined;
  const backend = detail.backendMessage;
  const text = Redacted.isRedacted(backend) ? Redacted.value(backend) : undefined;
  return typeof text === "string" && addonTexts.some((own) => own.test(text)) ? text : undefined;
};

/** A failure as the evidence states it: the library's own message, never provider text. */
export const describe = (cause: Cause.Cause<unknown>): string => {
  if (Cause.hasInterruptsOnly(cause)) return "the run was interrupted";
  const error = Cause.squash(cause);
  if (Schema.is(Refused)(error)) return error.message;
  if (Cause.isTimeoutError(error)) return "a step ran past its deadline";
  if (isReactorFailure(error)) {
    const own = addonText(error);
    return `${error.reason._tag}: ${error.message}${own === undefined ? "" : `: ${own}`}${error.context.outcome === undefined ? "" : ` (outcome ${error.context.outcome})`}`;
  }
  if (Schema.is(SaveFailed)(error)) return error.message;
  return `unexpected: ${String(error).slice(0, 300)}`;
};
