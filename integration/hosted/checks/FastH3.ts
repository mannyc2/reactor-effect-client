/**
 * A question for Reactor, not a qualification: one raw FastH3 session records
 * Q1 state and queue counts, Q2 the deployment document, Q3 undeclared reference
 * images, Q4 state fields, Q5 lengths, Q6 history, Q7 deployment identity,
 * Q8 continuations, Q9 stop, and Q10 build and seam timing. Answers can differ
 * from H3's rehearsal without failing the probe.
 *
 * From allocation A: contract and first reads about A+3 s; lengths and settings
 * by A+6; plain, reference images, second, ahead and unknown source by A+9;
 * wait for plain to finish, then history and its continuation, optional stop,
 * and one final queue read. Work ends at A+40 s, leaving 10 s for cleanup.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Reactor from "reactor-effect-client/Reactor";
import { isReactorFailure, ReactorError } from "reactor-effect-client/ReactorError";
import type { CommandFailure } from "reactor-effect-client/ReactorError";
import type * as Session from "reactor-effect-client/Session";
import type { Pieces } from "../Checks.js";
import type { FastH3Record, StatsSample } from "../Evidence.js";
import { SaveFailed } from "../Ledger.js";
import * as Media from "../Media.js";
import * as Probes from "../Probes.js";
import { describe, Run } from "../Run.js";
import { modelFor } from "../Spend.js";
import { Target } from "../Target.js";

const prompt = "A slow aerial shot over a calm green valley at dawn.";
const maxHeard = 4096;
const stateKeys = [
  "clip_seconds",
  "clip_seconds_min",
  "clip_seconds_max",
  "seed",
  "autoplay",
  "flush_on_clip_end",
  "aspect",
  "width",
  "height",
  "playing",
  "playing_clip_id",
  "generation_queued",
  "generation_capacity",
  "playout_queued",
  "playout_capacity",
  "clips_played",
  "seconds_sent",
  "valid_commands",
];
const messageNames = new Set([
  "clip_queued",
  "clip_popped",
  "clip_generated",
  "clip_started",
  "clip_moved",
  "clip_failed",
  "clip_finished",
  "clip_stopped",
  "queue_update",
  "state_update",
  "command_error",
  "seed_accepted",
  "clip_length_accepted",
  "canvas_accepted",
  "autoplay_accepted",
  "flush_accepted",
  "reset_completed",
]);
const Aspect = Schema.Literals(["16:9", "1:1", "9:16", "4:3"]);
const StateCounts = Schema.Struct({ generation_queued: Schema.Int, playout_queued: Schema.Int });
const QueueLists = Schema.Struct({
  generation: Schema.Array(Schema.JsonObject),
  playout: Schema.Array(Schema.JsonObject),
  history: Schema.Array(Schema.JsonObject),
});
const ClipMessage = Schema.Struct({ clip: Schema.JsonObject });
const ClipIdentity = Schema.Struct({ clip_id: Schema.String });
const Length = Schema.Struct({ clip_seconds: Schema.Finite, frames: Schema.Int });
const CommandError = Schema.Struct({ command: Schema.String, reason: Schema.String });
const Document = Schema.Struct({
  openapi: Schema.String.check(Schema.isPattern(/^3\.[01]\./)),
  info: Schema.optionalKey(Schema.JsonObject),
  paths: Schema.Record(Schema.String, Schema.JsonObject),
  components: Schema.optionalKey(
    Schema.Struct({ schemas: Schema.optionalKey(Schema.Record(Schema.String, Schema.JsonObject)) }),
  ),
});
// A dotted version is useful, but arbitrary provider text and four-part addresses are private.
const Version = Schema.String.check(
  Schema.isPattern(/^v?\d{1,4}\.\d{1,4}\.\d{1,4}(?:[-+][\w-]{1,16})?$/),
);

interface Heard {
  readonly atMs: number;
  readonly event: Session.SessionEvent;
}
interface Sent {
  readonly from: number;
  readonly sentMs: number;
  readonly answeredMs: number;
  readonly result: Result.Result<Session.CommandReply, CommandFailure>;
}
interface Pair {
  readonly number: number;
  state?: Sent;
  queue?: Sent;
  comparable: boolean;
}

const field = (value: unknown, key: string): unknown =>
  Predicate.isObject(value) ? value[key] : undefined;
const parsed = <S extends Schema.ConstraintDecoder<unknown>>(schema: S, value: unknown) =>
  Option.getOrUndefined(Schema.decodeUnknownOption(schema)(value));
const keysOf = (value: object) => Object.keys(value).map(Probes.keptText);
const clipOf = (data: unknown) => parsed(ClipMessage, data)?.clip;
const idOf = (data: unknown) => parsed(ClipIdentity, clipOf(data))?.clip_id;
const replyOf = (sent: Sent) => (Result.isSuccess(sent.result) ? sent.result.success : undefined);
const dataOf = (sent: Sent) => {
  const reply = replyOf(sent);
  return reply?.kind === "message" ? reply.data : undefined;
};
const requestOf = (sent: Sent) =>
  Result.isSuccess(sent.result)
    ? sent.result.success.requestId
    : sent.result.failure.context.requestId;
const answered = (sent: Sent) =>
  Result.isSuccess(sent.result) || sent.result.failure.context.outcome === "replied";
const answerOf = (sent: Sent) => {
  if (Result.isFailure(sent.result)) return sent.result.failure.reason._tag;
  return sent.result.success.kind === "message" ? Probes.keptText(sent.result.success.type) : "ack";
};
const outcomeOf = (sent: Sent) =>
  Result.isSuccess(sent.result) ? "replied" : sent.result.failure.context.outcome;
const nativeFailure = (error: unknown) =>
  isReactorFailure(error) &&
  error.reason._tag === "Protocol" &&
  error.message === "native peer failed";
const bump = (counts: Record<string, number>, key: string) => {
  counts[key] = (counts[key] ?? 0) + 1;
};

/** Whether the event prefix contains a queued clip that has not generated, failed or been popped. */
const unbuilt = (all: ReadonlyArray<Heard>, sequence: bigint) => {
  const pending = new Set<string>();
  for (const { event } of all) {
    if (event.sequence > sequence) break;
    if (event._tag !== "Model" || event.kind !== "message") continue;
    const id = idOf(event.data);
    if (id === undefined) continue;
    if (event.type === "clip_queued") pending.add(id);
    if (["clip_generated", "clip_failed", "clip_popped"].includes(event.type)) pending.delete(id);
  }
  return pending.size > 0;
};

export const fastH3 = Effect.fnUntraced(function* (pieces: Pieces) {
  const run = yield* Run;
  const target = yield* Target;
  const model = modelFor("fasth3", target.mode).name;
  const initial: FastH3Record = {
    model,
    counts: [],
    readPairs: [],
    lengths: [],
    enqueues: [],
    clips: [],
    generatedOrder: [],
    history: [],
    seams: [],
    messages: {},
    unknown: {},
    observerLost: false,
  };
  yield* run.update((evidence) => ({ ...evidence, fasth3: initial }));
  yield* run.secret(Redacted.make(prompt));
  const record = (change: (value: FastH3Record) => FastH3Record) =>
    run.update((evidence) => ({ ...evidence, fasth3: change(evidence.fasth3 ?? initial) }));
  const keyed = CoordinatorClient.make({ apiUrl: target.apiUrl, apiKey: target.apiKey });
  const grant = yield* pieces.mint("fasth3");
  const reactor = yield* Reactor.make();

  yield* pieces.withSessions(
    (grants) =>
      Effect.gen(function* () {
        const outer = yield* Effect.scope;
        const heard = yield* SubscriptionRef.make<ReadonlyArray<Heard>>([]);
        const video = Media.videoLog();
        const samples: Array<StatsSample> = [];
        const pairs: Array<Pair> = [];
        const labels = new Map<string, string>();
        let deadline = Number.POSITIVE_INFINITY;
        let stepDeadline = deadline;
        const control = yield* Ref.make({ terminal: false, observerLost: false });

        /** A caller timeout also bounds the session-owned command's reply timer. Nothing is resent. */
        const step = <A, E, R>(name: string, seconds: number, body: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            if ((yield* Ref.get(control)).terminal) return Option.none<A>();
            stepDeadline = Math.min(deadline, (yield* Clock.currentTimeMillis) + seconds * 1000);
            const exit = yield* body.pipe(
              Effect.timeout(yield* pieces.until(stepDeadline)),
              Effect.exit,
            );
            if (Exit.isSuccess(exit)) return Option.some(exit.value);
            if (Cause.hasInterruptsOnly(exit.cause)) return yield* Effect.interrupt;
            const error = Cause.squash(exit.cause);
            if (Schema.is(SaveFailed)(error)) return yield* error;
            if (nativeFailure(error))
              yield* Ref.update(control, (value) => ({ ...value, terminal: true }));
            // Foreign defects may carry provider text; only the library's typed failures are described.
            const detail =
              isReactorFailure(error) || Cause.isTimeoutError(error)
                ? describe(exit.cause)
                : "the probe step failed unexpectedly";
            yield* run.judge(name, detail);
            yield* run.mark(`${name} failed`);
            return Option.none<A>();
          });

        const created = yield* step(
          "session",
          30,
          reactor.create({
            model,
            tokens: CoordinatorClient.fixedTokens(grant),
            onAllocated: (allocation) =>
              Effect.gen(function* () {
                deadline = yield* pieces.allocated(allocation.id, grant);
                stepDeadline = Math.min(stepDeadline, deadline);
                grants.set(allocation.id, grant);
                yield* pieces
                  .recordSessionEvents(allocation)
                  .pipe(
                    Effect.provideService(Scope.Scope, outer),
                    Effect.forkIn(outer, { startImmediately: true }),
                  );
                // Both the subscription and the reader outlive the step that connects the session.
                const observation = yield* allocation
                  .observe({ capacity: maxHeard })
                  .pipe(Effect.provideService(Scope.Scope, outer));
                yield* observation.events.pipe(
                  Stream.runForEach((event) =>
                    Effect.gen(function* () {
                      if (event._tag === "Model" && event.kind === "message") {
                        const reason = field(event.data, "reason");
                        if (Predicate.isString(reason)) yield* run.secret(Redacted.make(reason));
                      }
                      if (event._tag === "Diagnostic" && nativeFailure(event.error))
                        yield* Ref.update(control, (value) => ({ ...value, terminal: true })).pipe(
                          Effect.andThen(
                            run.judge("session events", event.error.pipe(Cause.fail, describe)),
                          ),
                        );
                      const atMs = yield* run.now;
                      const lost = yield* SubscriptionRef.modify(heard, (all) =>
                        all.length >= maxHeard ? [true, all] : [false, [...all, { atMs, event }]],
                      );
                      if (lost) yield* Ref.set(control, { terminal: true, observerLost: true });
                    }),
                  ),
                  Effect.catch((error) =>
                    Effect.gen(function* () {
                      yield* Ref.set(control, { terminal: true, observerLost: true });
                      yield* record((value) => ({ ...value, observerLost: true }));
                      yield* run.judge("events were observed without loss", error.reason._tag);
                    }),
                  ),
                  Effect.forkIn(outer, { startImmediately: true }),
                );
              }),
          }),
        );
        if (Option.isNone(created)) return;
        const session = created.value;
        yield* run.mark("connected");
        const decoded = yield* session.decoded;
        yield* pieces.readInto(decoded.video("main_video"), video).pipe(Effect.forkIn(outer));
        yield* pieces.sampleStats(session, samples).pipe(Effect.forkIn(outer));

        const send = Effect.fnUntraced(function* (name: string, data: Schema.JsonObject = {}) {
          if ((yield* Ref.get(control)).terminal)
            return yield* ReactorError.fromCode(
              "Protocol",
              "the probe stopped after its observation failed",
            );
          const sentMs = yield* run.now;
          const from = (yield* SubscriptionRef.get(heard)).length;
          const result = yield* Effect.result(
            session.command(name, data, {
              replyTimeout: yield* pieces.until(Math.min(stepDeadline, deadline)),
            }),
          );
          const sent: Sent = { from, sentMs, answeredMs: yield* run.now, result };
          const request = requestOf(sent);
          if (request !== undefined && answered(sent))
            yield* pieces
              .waitFor(
                heard,
                (all) =>
                  all.find(
                    ({ event }) =>
                      (event._tag === "Model" || event._tag === "CommandError") &&
                      event.requestId === request,
                  ),
                Math.min(stepDeadline, deadline),
              )
              .pipe(Effect.option);
          return sent;
        });
        const checkNative = (sent: Sent) =>
          Result.isFailure(sent.result) && nativeFailure(sent.result.failure)
            ? Effect.fail(sent.result.failure)
            : Effect.void;

        const readPair = Effect.fnUntraced(function* () {
          const pair: Pair = { number: pairs.length + 1, comparable: false };
          pairs.push(pair);
          const state = yield* send("get_state");
          pair.state = state;
          yield* checkNative(state);
          const queue = yield* send("get_queue");
          pair.queue = queue;
          yield* checkNative(queue);
          const s = replyOf(state);
          const q = replyOf(queue);
          const comparable =
            !(yield* Ref.get(control)).observerLost &&
            s?.kind === "message" &&
            s.type === "state_update" &&
            q?.kind === "message" &&
            q.type === "queue_update" &&
            s.generation === q.generation &&
            q.sequence === s.sequence + 1n &&
            parsed(StateCounts, s.data) !== undefined &&
            parsed(QueueLists, q.data) !== undefined;
          pair.comparable = comparable;
          return { state, queue };
        });

        const enqueue = Effect.fnUntraced(function* (label: string, extra: Schema.JsonObject = {}) {
          const sentMs = yield* run.now;
          yield* record((value) => ({
            ...value,
            enqueues: [
              ...value.enqueues,
              { label, sentMs, answer: "unknown", outcome: "unknown", commandErrors: [] },
            ],
          }));
          const sent = yield* send(
            "enqueue",
            Object.assign(
              {
                prompt,
                seconds: 5.5,
                metadata: `fasth3:${label}`,
              },
              extra,
            ),
          );
          const request = requestOf(sent);
          const reply = replyOf(sent);
          const identifies = ({ event }: Heard) =>
            event._tag === "Model" &&
            event.kind === "message" &&
            event.type === "clip_queued" &&
            reply !== undefined &&
            event.generation === reply.generation &&
            (event.requestId === request ||
              (field(clipOf(event.data), "metadata") === `fasth3:${label}` &&
                field(clipOf(event.data), "prompt") === prompt));
          // A bodyless ack has no clip identity. A separate echo can still identify its clip.
          if (replyOf(sent)?.kind === "ack")
            yield* pieces
              .waitFor(
                heard,
                (all) =>
                  all
                    .slice(sent.from)
                    .find(
                      (each) =>
                        identifies(each) ||
                        (each.event._tag === "Model" &&
                          each.event.kind === "message" &&
                          each.event.type === "command_error" &&
                          parsed(CommandError, each.event.data)?.command === "enqueue"),
                    ),
                Math.min(stepDeadline, deadline, run.origin + sent.answeredMs + 1000),
              )
              .pipe(Effect.option);
          const observations = (yield* SubscriptionRef.get(heard)).slice(sent.from);
          const queued = observations.find(identifies);
          const data = dataOf(sent);
          const clip =
            clipOf(data) ??
            (queued?.event._tag === "Model" && queued.event.kind === "message"
              ? clipOf(queued.event.data)
              : undefined);
          const identity = parsed(ClipIdentity, clip);
          if (identity !== undefined) labels.set(identity.clip_id, label);
          const length = parsed(Length, {
            clip_seconds: field(clip, "seconds"),
            frames: field(clip, "frames"),
          });
          const error = parsed(CommandError, data);
          const correlated = observations.find(
            ({ event }) =>
              event._tag === "CommandError" &&
              event.requestId === request &&
              event.correlation === "matched",
          );
          const commandErrors: Array<FastH3Record["enqueues"][number]["commandErrors"][number]> =
            [];
          for (const { atMs, event } of observations) {
            if (
              event._tag !== "Model" ||
              event.kind !== "message" ||
              event.type !== "command_error"
            )
              continue;
            const error = parsed(CommandError, event.data);
            if (error?.command !== "enqueue") continue;
            commandErrors.push({
              atMs,
              command: Probes.keptText(error.command),
              reasonLength: error.reason.length,
              attribution:
                event.requestId === request && event.correlation === "matched"
                  ? "exact"
                  : "temporal",
              beforeAnswer:
                reply === undefined ? atMs < sent.answeredMs : event.sequence < reply.sequence,
            });
          }
          const outcome = outcomeOf(sent);
          let clipAttribution: FastH3Record["enqueues"][number]["clipAttribution"];
          if (clipOf(data) !== undefined) clipAttribution = "reply";
          else if (queued?.event._tag === "Model" && queued.event.requestId === request)
            clipAttribution = "request";
          else if (clip !== undefined) clipAttribution = "metadata";
          yield* record((value) => ({
            ...value,
            enqueues: value.enqueues.map((entry) =>
              entry.label !== label
                ? entry
                : {
                    label,
                    sentMs: sent.sentMs,
                    answeredMs: sent.answeredMs,
                    answer: correlated === undefined ? answerOf(sent) : "command_error",
                    outcome,
                    commandErrors,
                    ...(length === undefined
                      ? {}
                      : { clipSeconds: length.clip_seconds, frames: length.frames }),
                    ...(clip === undefined ? {} : { clipKeys: keysOf(clip) }),
                    ...(clipAttribution === undefined ? {} : { clipAttribution }),
                    ...(error === undefined
                      ? {}
                      : {
                          commandError: {
                            command: Probes.keptText(error.command),
                            reasonLength: error.reason.length,
                          },
                        }),
                  },
            ),
          }));
          yield* run.mark(
            `${label} answered`,
            correlated === undefined ? answerOf(sent) : "command_error",
          );
          yield* checkNative(sent);
          return identity?.clip_id;
        });
        const pop = Effect.fnUntraced(function* (id: string | undefined) {
          if (id === undefined) return;
          const sent = yield* send("pop", { clip_id: id });
          yield* run.mark("pop answered", answerOf(sent));
          yield* checkNative(sent);
        });

        const recordAll = Effect.gen(function* () {
          const all = yield* SubscriptionRef.get(heard);
          const observerLost = (yield* Ref.get(control)).observerLost;
          const counts: Array<FastH3Record["counts"][number]> = [];
          const messages: Record<string, number> = {};
          const unknown: Record<string, number> = {};
          const clips = new Map<string, FastH3Record["clips"][number]>();
          const generatedOrder: Array<string> = [];
          const history: Array<FastH3Record["history"][number]> = [];
          const readRequests = new Map<string, { pair: number; comparable: boolean }>();
          for (const pair of pairs)
            for (const sent of [pair.state, pair.queue]) {
              if (sent === undefined) continue;
              const request = requestOf(sent);
              if (request !== undefined)
                readRequests.set(request, {
                  pair: pair.number,
                  comparable: pair.comparable && !observerLost,
                });
            }
          let first: FastH3Record["state"];
          for (const { atMs, event } of all) {
            if (event._tag !== "Model") continue;
            if (event.kind === "ack") {
              bump(messages, "ack");
              continue;
            }
            const name = Probes.keptText(event.type);
            bump(messages, name);
            if (!messageNames.has(event.type)) bump(unknown, name);
            const state =
              event.type === "state_update" ? parsed(StateCounts, event.data) : undefined;
            const queue =
              event.type === "queue_update" ? parsed(QueueLists, event.data) : undefined;
            const read = readRequests.get(event.requestId);
            if (state !== undefined || queue !== undefined) {
              counts.push({
                atMs,
                from: state === undefined ? "queue_update" : "state_update",
                read: read !== undefined,
                generation: state?.generation_queued ?? queue?.generation.length ?? 0,
                playout: state?.playout_queued ?? queue?.playout.length ?? 0,
                ...(queue === undefined ? {} : { history: queue.history.length }),
                building: unbuilt(all, event.sequence),
                ...(read ?? {}),
              });
            }
            if (event.type === "state_update" && event.data !== undefined && first === undefined) {
              const values: Record<string, string | number | boolean> = {};
              for (const [key, value] of Object.entries(event.data)) {
                if (!Probes.readable(key)) continue;
                if (Schema.is(Schema.Finite)(value) || Predicate.isBoolean(value))
                  values[key] = value;
                const aspect = key === "aspect" ? parsed(Aspect, value) : undefined;
                if (aspect !== undefined) values[key] = aspect;
              }
              first = {
                keys: keysOf(event.data),
                missing: stateKeys.filter((key) => !Object.hasOwn(event.data ?? {}, key)),
                extra: Object.keys(event.data)
                  .filter((key) => !stateKeys.includes(key))
                  .map(Probes.keptText),
                values,
              };
            }
            if (queue !== undefined && read !== undefined)
              history.push({
                atMs,
                length: queue.history.length,
                clipKeys: [...new Set(queue.history.flatMap(keysOf))],
              });
            const id = idOf(event.data);
            const label = id === undefined ? undefined : labels.get(id);
            if (label === undefined) continue;
            const existing = clips.get(label);
            if (event.type === "clip_queued") clips.set(label, { label, queuedMs: atMs });
            if (existing === undefined) continue;
            switch (event.type) {
              case "clip_generated":
                clips.set(label, { ...existing, generatedMs: atMs });
                generatedOrder.push(label);
                break;
              case "clip_started":
                clips.set(label, { ...existing, startedMs: atMs });
                break;
              case "clip_finished":
                clips.set(label, { ...existing, endedMs: atMs, ended: "finished" });
                break;
              case "clip_stopped":
                clips.set(label, { ...existing, endedMs: atMs, ended: "stopped" });
                break;
              case "clip_failed":
                clips.set(label, { ...existing, endedMs: atMs, ended: "failed" });
                break;
              case "clip_popped":
                clips.set(label, { ...existing, endedMs: atMs, ended: "popped" });
                break;
            }
          }
          const lifecycle = [...clips.values()];
          const seams: Array<FastH3Record["seams"][number]> = [];
          for (const clip of lifecycle) {
            if (clip.startedMs === undefined) continue;
            const start = clip.startedMs;
            const prior = lifecycle
              .filter(
                (other) =>
                  other.label !== clip.label &&
                  other.startedMs !== undefined &&
                  other.startedMs < start,
              )
              .sort((a, b) => (b.startedMs ?? 0) - (a.startedMs ?? 0))[0];
            if (
              prior?.endedMs === undefined ||
              prior.endedMs > start ||
              (prior.ended !== "finished" && prior.ended !== "stopped")
            )
              continue;
            const pause = video.pause(prior.endedMs - pieces.seamMs, start + pieces.seamMs);
            if (pause !== undefined) seams.push({ toLabel: clip.label, pauseMs: pause.durationMs });
          }
          const stop = (yield* run.evidence).fasth3?.stop;
          const stopped =
            stop === undefined
              ? undefined
              : all.find(
                  ({ atMs, event }) =>
                    atMs >= stop.sentMs &&
                    event._tag === "Model" &&
                    event.kind === "message" &&
                    event.type === "clip_stopped",
                );
          yield* record((value) => ({
            ...value,
            ...(first === undefined ? {} : { state: first }),
            counts,
            readPairs: pairs.map((pair) => ({
              pair: pair.number,
              stateAnswered: pair.state !== undefined && answered(pair.state),
              queueAnswered: pair.queue !== undefined && answered(pair.queue),
              comparable: pair.comparable && !observerLost,
            })),
            clips: lifecycle,
            generatedOrder,
            history,
            seams,
            messages,
            unknown,
            observerLost,
            video: video.summary(),
            ...(stop === undefined
              ? {}
              : {
                  stop: { ...stop, ...(stopped === undefined ? {} : { stoppedMs: stopped.atMs }) },
                }),
          }));
          const pair = samples.findLast(
            (sample) => sample.local !== undefined && (sample.receivedKbps ?? 0) > 0,
          )?.local;
          yield* run.update((evidence) => ({
            ...evidence,
            network: { samples: [...samples], ...(pair === undefined ? {} : { pair }) },
          }));
        });
        // Save the observations even on a failed step, and always confirm termination.
        yield* Effect.addFinalizer(() =>
          recordAll.pipe(Effect.ensuring(pieces.close(session).pipe(Effect.orDie)), Effect.orDie),
        );

        yield* step(
          "contract",
          10,
          Effect.gen(function* () {
            const schema = yield* session.schema;
            const document = parsed(Document, schema.openapi);
            if (document === undefined)
              return yield* ReactorError.fromCode(
                "Protocol",
                "the deployment document was not OpenAPI 3.0 or 3.1",
              );
            const commands = Object.keys(document.paths)
              .filter((path) => path.startsWith("/events/"))
              .map((path) => path.slice("/events/".length));
            const body = field(
              field(
                field(field(document.paths["/events/enqueue"], "post"), "requestBody"),
                "content",
              ),
              "application/json",
            );
            let shape = field(body, "schema");
            const visited = new Set<string>();
            // Only local component references are followed, with a finite bound for a malformed document.
            for (let n = 0; n < 16; n++) {
              const reference = field(shape, "$ref");
              if (
                !Predicate.isString(reference) ||
                !reference.startsWith("#/components/schemas/") ||
                visited.has(reference)
              )
                break;
              visited.add(reference);
              shape =
                document.components?.schemas?.[reference.slice("#/components/schemas/".length)];
            }
            const properties = field(shape, "properties");
            const title = field(document.info, "title");
            const version = parsed(Version, field(document.info, "version"));
            yield* record((value) => ({
              ...value,
              contract: {
                ...(Predicate.isString(title) ? { title: Probes.keptText(title) } : {}),
                ...(version === undefined ? {} : { version }),
                commands: commands.map(Probes.keptText),
                enqueue: Predicate.isObject(properties) ? keysOf(properties) : [],
              },
            }));
            yield* run.mark("contract read");
          }),
        );
        yield* step(
          "server",
          5,
          Effect.gen(function* () {
            const inspection = yield* pieces
              .withToken(grant.jwt)
              .pipe(Effect.flatMap((coordinator) => coordinator.inspect(session.id)));
            yield* run.update((evidence) => ({
              ...evidence,
              server: {
                cluster: inspection.cluster,
                zone: inspection.zone,
                serverVersion: inspection.serverVersion,
                transport:
                  inspection.selectedTransport === null
                    ? null
                    : `${inspection.selectedTransport.protocol}/${inspection.selectedTransport.version}`,
              },
            }));
          }),
        );
        const first = yield* step("first state", 5, readPair());
        yield* step(
          "lengths",
          6,
          Effect.gen(function* () {
            for (const requested of [15, 5.0, 5.167, 6, 10, 14.375, 5.5]) {
              yield* record((value) => ({
                ...value,
                lengths: [...value.lengths, { requested, answer: "unknown", outcome: "unknown" }],
              }));
              const sent = yield* send("set_clip_seconds", { seconds: requested });
              const length = parsed(Length, dataOf(sent));
              const outcome = outcomeOf(sent);
              yield* record((value) => ({
                ...value,
                lengths: value.lengths.map((entry) =>
                  entry.requested !== requested
                    ? entry
                    : {
                        requested,
                        answer: answerOf(sent),
                        outcome,
                        ...(length === undefined
                          ? {}
                          : { clipSeconds: length.clip_seconds, frames: length.frames }),
                      },
                ),
              }));
              yield* run.mark("length answered", answerOf(sent));
              yield* checkNative(sent);
            }
          }),
        );
        yield* step(
          "settings",
          3,
          Effect.gen(function* () {
            for (const [name, enabled] of [
              ["set_flush_on_clip_end", false],
              ["set_autoplay", true],
            ] as const) {
              const sent = yield* send(name, { enabled });
              yield* run.mark(`${name} answered`, answerOf(sent));
              yield* checkNative(sent);
            }
          }),
        );
        const plain = yield* step(
          "plain",
          5,
          Effect.gen(function* () {
            const id = yield* enqueue("plain");
            yield* readPair();
            return id;
          }),
        );
        yield* step(
          "reference_images",
          5,
          Effect.gen(function* () {
            yield* pop(yield* enqueue("reference_images", { reference_images: [] }));
          }),
        );
        const second = yield* step("second", 5, enqueue("second"));
        if (Option.isSome(second) && second.value !== undefined) {
          const anchor = second.value;
          yield* step(
            "ahead of its anchor",
            5,
            Effect.gen(function* () {
              const id = yield* enqueue("ahead of its anchor", {
                continue_from_clip_id: anchor,
                position: 1,
              });
              const pair = yield* readPair();
              const queue = parsed(QueueLists, dataOf(pair.queue));
              const order =
                queue?.generation.map((clip) => parsed(ClipIdentity, clip)?.clip_id) ?? [];
              const all = yield* SubscriptionRef.get(heard);
              const built = all.some(
                ({ event }) =>
                  event._tag === "Model" &&
                  event.kind === "message" &&
                  event.type === "clip_generated" &&
                  idOf(event.data) === anchor,
              );
              const aheadIndex = id === undefined ? -1 : order.indexOf(id);
              const anchorIndex = order.indexOf(anchor);
              yield* record((value) => ({
                ...value,
                ahead: {
                  checked: queue !== undefined,
                  observed:
                    queue !== undefined && !built && aheadIndex >= 0 && anchorIndex > aheadIndex,
                },
              }));
            }),
          );
        }
        yield* step(
          "unknown source",
          5,
          Effect.gen(function* () {
            yield* pop(
              yield* enqueue("unknown source", {
                continue_from_clip_id: "00000000-0000-4000-8000-000000000001",
              }),
            );
          }),
        );
        const plainId = Option.isSome(plain) ? plain.value : undefined;
        const played =
          plainId === undefined
            ? Option.none<Heard>()
            : yield* step(
                "played",
                20,
                pieces.waitFor(
                  heard,
                  (all) =>
                    all.find(
                      ({ event }) =>
                        event._tag === "Model" &&
                        event.kind === "message" &&
                        event.type === "clip_finished" &&
                        idOf(event.data) === plainId,
                    ),
                  deadline,
                ),
              );
        const history = yield* step("history", 5, readPair());
        if (plainId !== undefined && Option.isSome(played)) {
          yield* step(
            "from history",
            5,
            Effect.gen(function* () {
              yield* pop(yield* enqueue("from history", { continue_from_clip_id: plainId }));
            }),
          );
        }
        // Stop is optional. Its frozen tail is unmeasured: pause needs a later changing frame.
        const playing = (yield* SubscriptionRef.get(heard)).findLast(
          ({ event }) =>
            event._tag === "Model" &&
            event.kind === "message" &&
            event.type === "clip_started" &&
            ["second", "ahead of its anchor"].includes(labels.get(idOf(event.data) ?? "") ?? ""),
        );
        const ended =
          playing === undefined
            ? true
            : (yield* SubscriptionRef.get(heard)).some(
                ({ event }) =>
                  event.sequence > playing.event.sequence &&
                  event._tag === "Model" &&
                  event.kind === "message" &&
                  ["clip_finished", "clip_stopped", "clip_failed"].includes(event.type) &&
                  idOf(event.data) ===
                    idOf(
                      playing.event._tag === "Model" && playing.event.kind === "message"
                        ? playing.event.data
                        : undefined,
                    ),
              );
        if (
          !(yield* Ref.get(control)).terminal &&
          playing !== undefined &&
          !ended &&
          Duration.toMillis(yield* pieces.until(deadline)) > 1500
        ) {
          yield* step(
            "stop",
            5,
            Effect.gen(function* () {
              yield* pieces.sleepUntil(playing.atMs + 1500, Math.min(stepDeadline, deadline));
              const sentMs = yield* run.now;
              yield* record((value) => ({
                ...value,
                stop: {
                  sentMs,
                  answer: "unknown",
                  outcome: "unknown",
                  frozen: "unmeasured",
                },
              }));
              const sent = yield* send("stop");
              const outcome = outcomeOf(sent);
              yield* record((value) => ({
                ...value,
                stop: {
                  sentMs: sent.sentMs,
                  answer: answerOf(sent),
                  frozen: "unmeasured",
                  outcome,
                },
              }));
              yield* run.mark("stop answered", answerOf(sent));
              yield* checkNative(sent);
              if (answered(sent))
                yield* pieces
                  .waitFor(
                    heard,
                    (all) =>
                      all
                        .slice(sent.from)
                        .find(
                          ({ event }) =>
                            event._tag === "Model" &&
                            event.kind === "message" &&
                            event.type === "clip_stopped",
                        ),
                    Math.min(stepDeadline, deadline, run.origin + sent.answeredMs + 3000),
                  )
                  .pipe(Effect.option);
            }),
          );
        } else yield* run.mark("stop skipped: no clip playing before the deadline");
        yield* step("queue at the end", 5, readPair());
        yield* recordAll;
        const evidence = (yield* run.evidence).fasth3 ?? initial;
        const observerLost = (yield* Ref.get(control)).observerLost;
        const comparable = pairs.filter((pair) => pair.comparable);
        yield* pieces.judge("the deployment's document was read", [
          evidence.contract !== undefined,
          "no deployment document was recorded",
        ]);
        yield* pieces.judge("the first state and queue were read", [
          Option.isSome(first) &&
            parsed(StateCounts, dataOf(first.value.state)) !== undefined &&
            parsed(QueueLists, dataOf(first.value.queue)) !== undefined,
          "the first state or queue did not answer",
        ]);
        yield* pieces.judge(
          "state and queue counts were read in pairs at least three times, once with an unbuilt clip",
          [
            !observerLost &&
              comparable.length >= 3 &&
              evidence.counts.some((count) => count.comparable === true && count.building),
            "fewer than three uninterrupted read pairs, or none with an unbuilt clip",
          ],
        );
        yield* pieces.judge("every length was answered", [
          evidence.lengths.length === 7 &&
            evidence.lengths.every((length) => length.outcome === "replied"),
          "a length did not receive an answer",
        ]);
        yield* pieces.judge("every enqueue was answered", [
          evidence.enqueues.length > 0 &&
            evidence.enqueues.every((entry) => entry.outcome === "replied"),
          "an enqueue's remote outcome was unknown or not submitted",
        ]);
        yield* pieces.judge("after a clip finished, the queue was read", [
          Option.isSome(played) &&
            Option.isSome(history) &&
            parsed(QueueLists, dataOf(history.value.queue)) !== undefined,
          "no queue answer after plain finished",
        ]);
        if (evidence.stop !== undefined)
          yield* pieces.judge("stop was answered", [
            evidence.stop.outcome === "replied",
            "stop's outcome was unknown",
          ]);
        if (observerLost)
          yield* pieces.judge("events were observed without loss", [
            false,
            "the bounded observer lost events",
          ]);
        yield* run.mark("fasth3 recorded");
      }),
    pieces.endHeld(keyed),
  );
});
