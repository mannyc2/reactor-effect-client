/** Requested source workflows over the real client and simulated Reactor, written before FastH3Source. */
import { assert, layer } from "@effect/vitest";
import {
  Context,
  Deferred,
  Duration,
  Effect,
  Layer,
  Option,
  Stream,
  SubscriptionRef,
  Tracer,
} from "effect";
import * as FastH3 from "../src/FastH3.js";
import * as FastH3Source from "../src/FastH3Source.js";
import * as H3 from "../src/H3.js";
import * as H3Source from "../src/H3Source.js";
import * as Playout from "../src/Playout.js";
import { CoordinatorClient, ReactorTest } from "../src/index.js";
import { environment } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({
  buildSpeed: 2.4,
  http: "20 millis",
  channel: "10 millis",
});
const key = (value: string) => Playout.ItemKey.make(value);
const tag = (value: string): Playout.ClipTag => ({ _tag: "Item", key: key(value) });
const request = (prompt: string): FastH3.Request => ({ prompt, seconds: 5.167 });

class Channel extends Context.Service<Channel, Playout.Service<FastH3.Request>>()(
  "reactor-effect-client/test/FastH3Source.test/Channel",
) {}

const tokens = Effect.fnUntraced(function* (modelName: string, maxSessionDuration: Duration.Input) {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  return coordinator.tokens({
    apiKey: test.apiKey,
    modelName,
    maxSessionDuration,
    expiresAfter: Duration.sum(Duration.fromInputUnsafe(maxSessionDuration), Duration.minutes(5)),
  });
});

const channelLayer = (
  lifetime: Duration.Input,
  renewal?: Playout.Options<never, FastH3.Request>["renewal"],
) =>
  Layer.effect(
    Channel,
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const sessionTokens = yield* tokens(FastH3.modelName, lifetime);
      return yield* Playout.make({
        model: FastH3Source.model,
        open: FastH3Source.open({ tokens: sessionTokens }),
        lanes: [{ name: "line" }],
        ...(renewal === undefined ? {} : { renewal }),
      });
    }),
  ).pipe(Layer.provideMerge(environment({ timing })));

/** Records public events before any action can publish one. */
const record = Effect.fnUntraced(function* <A, E, R>(events: Stream.Stream<A, E, R>) {
  const recorded = yield* SubscriptionRef.make<ReadonlyArray<A>>([]);
  yield* events.pipe(
    Stream.runForEach((event) => SubscriptionRef.update(recorded, (all) => [...all, event])),
    Effect.forkScoped({ startImmediately: true }),
  );
  return recorded;
});

const until = <A>(
  recorded: SubscriptionRef.SubscriptionRef<A>,
  done: (value: A) => boolean,
  what: string,
) =>
  SubscriptionRef.changes(recorded).pipe(
    Stream.filter(done),
    Stream.runHead,
    Effect.timeoutOrElse({ duration: "2 minutes", orElse: () => Effect.die(`${what} never came`) }),
    Effect.flatMap(
      Option.match({ onNone: () => Effect.die(`${what} ended unseen`), onSome: Effect.succeed }),
    ),
  );

layer(channelLayer("5 minutes"))("a FastH3 channel", (it) => {
  it.effect("airs three items in order and the omitted length is 124 frames", () =>
    Effect.gen(function* () {
      const channel = yield* Channel;
      const events = yield* record(channel.events);
      const names = ["defaulted", "second", "third"];
      const handles = yield* Effect.forEach(names, (name, index) =>
        channel.submit({
          key: key(name),
          lane: "line",
          request: index === 0 ? { prompt: name } : request(name),
        }),
      );
      for (const handle of handles) {
        const outcome = yield* handle.outcome;
        assert.strictEqual(outcome._tag, "Ended");
        if (outcome._tag === "Ended") assert.strictEqual(outcome.termination, "finished");
      }
      const started = (yield* SubscriptionRef.get(events)).flatMap((event) =>
        event._tag === "AsRun" && event.event.status._tag === "Started" ? [event.event] : [],
      );
      assert.deepStrictEqual(
        started.map((event) => event.key),
        names,
      );
      const first = started[0];
      assert.isDefined(first);
      if (first.status._tag === "Started") assert.closeTo(first.status.seconds, 124 / 24, 1e-9);
    }),
  );
});

layer(channelLayer("40 seconds", { lead: "15 seconds" }))("a capped FastH3 channel", (it) => {
  it.effect("renews and airs every item fully in submission order", () =>
    Effect.gen(function* () {
      const channel = yield* Channel;
      const events = yield* record(channel.events);
      const names = Array.from({ length: 12 }, (_, index) => `line ${String(index)}`);
      const handles = yield* Effect.forEach(names, (name) =>
        channel.submit({ key: key(name), lane: "line", request: request(name) }),
      );
      for (const handle of handles) {
        const started = yield* handle.started;
        const outcome = yield* handle.outcome;
        assert.strictEqual(started._tag, "Started");
        assert.strictEqual(outcome._tag, "Ended");
        if (started._tag === "Started" && outcome._tag === "Ended") {
          assert.strictEqual(outcome.termination, "finished");
          assert.closeTo(outcome.airedSeconds, started.seconds, 1e-9);
        }
      }
      const all = yield* until(
        events,
        (entries) =>
          entries.some((event) => event._tag === "Session" && event.event._tag === "Switched"),
        "the planned switch",
      );
      assert.deepStrictEqual(
        all.flatMap((event) =>
          event._tag === "AsRun" && event.event.status._tag === "Started" ? [event.event.key] : [],
        ),
        names,
      );
      const switches = all.flatMap((event) =>
        event._tag === "Session" && event.event._tag === "Switched" ? [event.event] : [],
      );
      assert.isAbove(switches.length, 0);
      for (const switched of switches) assert.notStrictEqual(switched.from, switched.to);
    }),
  );
});

layer(environment({ timing }))("a continued FastH3 item", (it) => {
  it.effect("uses the previous built clip and never advertises an unbuilt source", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const events = yield* SubscriptionRef.make<ReadonlyArray<Playout.SourceEvent>>([]);
      const sessionTokens = yield* tokens(FastH3.modelName, "5 minutes");
      const context = yield* Layer.build(
        Layer.effect(
          Channel,
          Playout.make({
            model: FastH3Source.model,
            open: FastH3Source.open({ tokens: sessionTokens }).pipe(
              Effect.tap((source) =>
                source.events.pipe(
                  Stream.runForEach((event) =>
                    SubscriptionRef.update(events, (all) => [...all, event]),
                  ),
                  Effect.forkScoped({ startImmediately: true }),
                  Effect.asVoid,
                ),
              ),
            ),
            lanes: [{ name: "line" }],
          }),
        ),
      );
      const channel = Context.get(context, Channel);
      const first = yield* channel.submit({
        key: key("first"),
        lane: "line",
        request: request("first"),
      });
      const second = yield* channel.submit({
        key: key("second"),
        lane: "line",
        request: request("second"),
        continuity: "previous",
      });
      assert.strictEqual((yield* first.outcome)._tag, "Ended");
      assert.strictEqual((yield* second.outcome)._tag, "Ended");
      const continuationEvents = yield* SubscriptionRef.get(events);
      const starts = continuationEvents.flatMap((event) =>
        event._tag === "Started" ? [event.clip] : [],
      );
      const firstClip = starts.find(
        (clip) => clip.tag?._tag === "Item" && clip.tag.key === "first",
      );
      const secondClip = starts.find(
        (clip) => clip.tag?._tag === "Item" && clip.tag.key === "second",
      );
      assert.isDefined(firstClip);
      assert.isDefined(secondClip);
      const states = continuationEvents.flatMap((event) =>
        event._tag === "State" ? [event.state] : [],
      );
      assert.isTrue(
        states.some((state) => state.building.length > 0),
        "no unbuilt clip was observed",
      );
      for (const state of states)
        for (const clip of state.building) assert.notInclude(state.continuable, clip.clipId);
      assert.isTrue(states.some((state) => state.continuable.includes(firstClip.clipId)));
      const build = (yield* (yield* ReactorTest.ReactorTest).log).find(
        (entry) => entry.kind === "build" && entry.clipId === secondClip.clipId,
      );
      assert.isDefined(build);
      assert.strictEqual(build.name, "continued");
      assert.strictEqual(build.continuedFrom, firstClip.clipId);
    }),
  );
});

layer(environment({ timing, fastH3History: 1 }))("a FastH3 source's continuation hint", (it) => {
  it.effect("omits an evicted hint once and preserves every post-send failure", () => {
    const spans: Array<Tracer.Span> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    return Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const source = yield* FastH3Source.open({
        tokens: yield* tokens(FastH3.modelName, "5 minutes"),
      });
      const events = yield* record(source.events);
      yield* source.setAutoplay(true);
      const a = yield* source.enqueue(request("a"), tag("a"));
      yield* source.enqueue(request("b"), tag("b"));
      const c = yield* source.enqueue(request("c"), tag("c"));
      yield* until(
        events,
        (all) =>
          all.some((event) => {
            if (event._tag !== "State") return false;
            const state = event.state;
            const cBuilt =
              state.ready.some((clip) => clip.clipId === c) ||
              state.playing?.clipId === c ||
              state.continuable.includes(c);
            return (
              cBuilt &&
              !state.building.some((clip) => clip.clipId === a) &&
              !state.ready.some((clip) => clip.clipId === a) &&
              state.playing?.clipId !== a &&
              !state.continuable.includes(a)
            );
          }),
        "a's eviction after c was built",
      );
      const before = (yield* test.log).length;
      const d = yield* source
        .enqueue(request("d"), tag("d"), a)
        .pipe(Effect.withSpan("caller.evicted", { root: true }, { captureStackTrace: false }));
      yield* until(
        events,
        (all) =>
          all.some(
            (event) =>
              event._tag === "State" &&
              event.state.continuable.includes(d) &&
              !event.state.building.some((clip) => clip.clipId === d),
          ),
        "d's independent build",
      );
      const independent = (yield* test.log).slice(before);
      assert.deepStrictEqual(
        independent.filter((entry) => entry.kind === "command").map((entry) => entry.name),
        ["enqueue"],
      );
      assert.isFalse(
        independent.some((entry) => entry.kind === "message" && entry.name === "command_error"),
      );
      const build = independent.find((entry) => entry.kind === "build" && entry.clipId === d);
      assert.strictEqual(
        spans
          .find((span) => span.name === "caller.evicted")
          ?.attributes.get("reactor.continuation"),
        "dropped",
      );

      const beforeRefusal = (yield* test.log).length;
      const refused = yield* Effect.flip(
        source.enqueue(
          { ...request("explicit old source"), start: { continueFrom: a } },
          tag("refused"),
          d,
        ),
      ).pipe(Effect.withSpan("caller.refused", { root: true }, { captureStackTrace: false }));
      assert.deepStrictEqual(
        [refused._tag, refused.reason._tag, refused.context.outcome],
        ["CommandFailure", "Remote", "replied"],
      );
      assert.strictEqual(refused.context.operation, "enqueue");
      assert.isString(refused.context.requestId);
      assert.strictEqual(refused.context.generation, 1n);
      assert.deepStrictEqual(
        (yield* test.log)
          .slice(beforeRefusal)
          .filter((entry) => entry.kind === "command")
          .map((entry) => entry.name),
        ["enqueue"],
      );
      assert.strictEqual(
        spans
          .find((span) => span.name === "caller.refused")
          ?.attributes.get("reactor.continuation"),
        undefined,
      );

      yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 1, applied: false });
      const beforeUnknown = (yield* test.log).length;
      const unknown = yield* Effect.flip(source.enqueue(request("lost"), tag("lost"), d));
      assert.deepStrictEqual(
        [unknown._tag, unknown.reason._tag, unknown.context.outcome],
        ["CommandFailure", "Timeout", "unknown"],
      );
      assert.strictEqual(unknown.context.operation, "enqueue");
      assert.isString(unknown.context.requestId);
      assert.strictEqual(unknown.context.generation, 1n);
      // This interval proves no delayed resend after the command and reconcile deadlines.
      yield* Effect.sleep("30 seconds");
      const afterUnknown = (yield* test.log).slice(beforeUnknown);
      assert.deepStrictEqual(
        afterUnknown
          .filter((entry) => entry.kind === "command")
          .map((entry) => [entry.name, entry.dropped]),
        [["enqueue", "command"]],
      );
      assert.deepStrictEqual(
        (yield* test.log)
          .slice(beforeRefusal)
          .filter((entry) => entry.kind === "command")
          .map((entry) => entry.name),
        ["enqueue", "enqueue"],
      );
      assert.isDefined(build);
      assert.strictEqual(build.name, "independent");
      assert.strictEqual(build.continuedFrom, undefined);
    }).pipe(Effect.withTracer(tracer));
  });
});

layer(environment({ timing }))("a FastH3 source's model", (it) => {
  it.effect("refuses reciprocal wrong-model resumes and an H3-default playout", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const h3Tokens = yield* tokens(H3.modelName, "5 minutes");
      const fastTokens = yield* tokens(FastH3.modelName, "5 minutes");
      const h3Record = yield* Deferred.make<H3Source.Allocation>();
      const fastRecord = yield* Deferred.make<FastH3Source.Allocation>();
      yield* H3Source.open({
        tokens: h3Tokens,
        onAllocated: ({ allocation }) => Deferred.succeed(h3Record, allocation),
      });
      const fast = yield* FastH3Source.open({
        tokens: fastTokens,
        onAllocated: ({ allocation }) => Deferred.succeed(fastRecord, allocation),
      });
      const before = (yield* test.log).length;
      const fastRefusal = yield* Effect.flip(
        FastH3Source.resume({ allocation: yield* Deferred.await(h3Record), tokens: fastTokens }),
      );
      const h3Refusal = yield* Effect.flip(
        H3Source.resume({ allocation: yield* Deferred.await(fastRecord), tokens: h3Tokens }),
      );
      for (const refusal of [fastRefusal, h3Refusal])
        assert.deepStrictEqual(
          [refusal._tag, refusal.reason._tag, refusal.context.outcome],
          ["AcquisitionFailure", "InvalidInput", "not-submitted"],
        );
      assert.deepStrictEqual(
        (yield* test.log).slice(before).filter((entry) => entry.kind === "request"),
        [],
      );
      const wrongModel = yield* Playout.make({
        open: Effect.succeed(fast),
        lanes: [{ name: "line" }],
      });
      const failure = yield* wrongModel.failure.pipe(Effect.timeout("1 minute"));
      assert.strictEqual(failure._tag, "ReactorError");
      if (failure._tag === "ReactorError") assert.strictEqual(failure.reason._tag, "InvalidState");
      assert.deepStrictEqual(
        (yield* test.log)
          .slice(before)
          .filter((entry) => entry.kind === "command" && entry.name === "enqueue"),
        [],
      );
    }),
  );
});
