/** A local renderer as a playout source: its ids, the order of its evidence, its hooks and each clip's scope. */
import { assert, describe, it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Redacted,
  Result,
  Scheduler,
  Schema,
  Scope,
  Stream,
  SubscriptionRef,
  Tracer,
  type Types,
} from "effect";
import { LocalSource, Playout, type ReactorError, ReactorTest } from "../src/index.js";

const request = (prompt: string, seconds = 5) => ({ prompt, seconds });
const tag = (key: string): Playout.ClipTag => ({ _tag: "Item", key: Playout.ItemKey.make(key) });

/** Waits until `ref` holds a value `done` accepts, on the virtual clock; `what` names it if it never does. */
const until = <A>(
  ref: SubscriptionRef.SubscriptionRef<A>,
  what: string,
  done: (value: A) => boolean,
) =>
  SubscriptionRef.changes(ref).pipe(
    Stream.filter(done),
    Stream.runHead,
    Effect.timeoutOrElse({ duration: "5 minutes", orElse: () => Effect.die(`${what} never came`) }),
    Effect.asVoid,
  );

/** Every event `source` reports from now on, in order. */
const record = Effect.fnUntraced(function* (source: Playout.Source) {
  const events = yield* SubscriptionRef.make<ReadonlyArray<Playout.SourceEvent>>([]);
  yield* source.events.pipe(
    Stream.runForEach((event) => SubscriptionRef.update(events, (all) => [...all, event])),
    Effect.forkScoped({ startImmediately: true }),
  );
  return events;
});

const ended = (clipId: string) => (events: ReadonlyArray<Playout.SourceEvent>) =>
  events.some((event) => event._tag === "Ended" && event.clip.clipId === clipId);

/**
 * Where `events` break the order the playout relies on: once a State lists a
 * clip, every State lists it until its `Ended` or `Failed`, and none after.
 */
const breaches = (events: ReadonlyArray<Playout.SourceEvent>): ReadonlyArray<string> => {
  const found: Array<string> = [];
  const over = new Set<string>();
  let listed = new Set<string>();
  for (const event of events) {
    if (event._tag === "Ended" || event._tag === "Failed") over.add(event.clip.clipId);
    if (event._tag !== "State") continue;
    const { building, ready, playing } = event.state;
    const now = new Set(
      [...building, ...ready, ...(playing === undefined ? [] : [playing])].map(
        (clip) => clip.clipId,
      ),
    );
    for (const clipId of listed)
      if (!now.has(clipId) && !over.has(clipId)) found.push(`${clipId} left before its end`);
    for (const clipId of now) if (over.has(clipId)) found.push(`${clipId} is listed after its end`);
    listed = now;
  }
  return found;
};

/** A renderer's voice, which its builds need. */
class Voice extends Context.Service<
  Voice,
  { readonly speak: (line: string) => Effect.Effect<string, Unvoiced> }
>()("reactor-effect-client/test/LocalSource.test/Voice") {}
/** A line the voice has no voice for. */
class Unvoiced extends Schema.TaggedError<Unvoiced>()("Unvoiced", {}) {}

/** Where a renderer's presentations show a line. */
class Stage extends Context.Service<
  Stage,
  { readonly show: (line: string) => Effect.Effect<void, Unstaged> }
>()("reactor-effect-client/test/LocalSource.test/Stage") {}
/** A line the stage cannot show. */
class Unstaged extends Schema.TaggedError<Unstaged>()("Unstaged", {}) {}

describe("LocalSource", () => {
  it.effect("gives each session it opens an id of its own", () =>
    Effect.gen(function* () {
      const first = yield* LocalSource.open();
      const second = yield* LocalSource.open();
      assert.notStrictEqual(first.sessionId, second.sessionId);
    }),
  );

  it.effect("keeps each queued clip's trace and sampling through build and presentation", () => {
    const spans: Array<Tracer.Span> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    return Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const source = yield* LocalSource.open({
        build: (clip) =>
          Effect.as(Deferred.await(released), { value: clip.request.prompt }).pipe(
            Effect.withSpan("renderer.build", {}, { captureStackTrace: false }),
          ),
        present: () =>
          Effect.void.pipe(Effect.withSpan("renderer.present", {}, { captureStackTrace: false })),
      }).pipe(Effect.withSpan("acquisition", { root: true }, { captureStackTrace: false }));
      const events = yield* record(source);
      yield* source.setAutoplay(true);
      yield* source
        .enqueue(request("first"), tag("first"))
        .pipe(Effect.withSpan("caller.first", { root: true }, { captureStackTrace: false }));
      const last = yield* source
        .enqueue(request("second"), tag("second"))
        .pipe(
          Effect.withSpan(
            "caller.second",
            { root: true, sampled: false },
            { captureStackTrace: false },
          ),
        );
      yield* Deferred.succeed(released, undefined);
      yield* until(events, "the last clip's end", ended(last));

      const callers = ["caller.first", "caller.second"].map((name) =>
        spans.find((span) => span.name === name),
      );
      for (const caller of callers) assert.isDefined(caller);
      for (const name of ["renderer.build", "renderer.present"]) {
        const hooks = spans.filter((span) => span.name === name);
        assert.deepStrictEqual(
          hooks.map((span) => span.traceId),
          callers.map((span) => span?.traceId),
        );
        assert.deepStrictEqual(
          hooks.map((span) => span.sampled),
          [true, false],
        );
      }
    }).pipe(Effect.withTracer(tracer));
  });

  it.effect(
    "lists a clip from its enqueue until it reports its end, however its fibers interleave",
    () =>
      Effect.gen(function* () {
        const prompts = ["a", "bad", "c", "d", "bad", "f"];
        // Short scheduler budgets make the source's fibers yield mid-change, where a State
        // computed before an enqueue could be published after it unless each change goes
        // out whole.
        const found = yield* Effect.forEach([2048, 48, 32, 16, 8], (budget) =>
          Effect.gen(function* () {
            yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
            const source = yield* LocalSource.open({
              build: (clip) =>
                clip.request.prompt === "bad"
                  ? Effect.fail("unrenderable")
                  : Effect.succeed({ value: clip.request.prompt }),
            });
            const events = yield* record(source);
            yield* source.setAutoplay(true);
            const ids = yield* Effect.forEach(prompts, (prompt, index) =>
              source.enqueue(request(prompt, 1), tag(`${prompt} ${String(index)}`)),
            );
            yield* until(events, "the last clip's end", ended(ids.at(-1) ?? ""));
            const all = yield* SubscriptionRef.get(events);
            return breaches(all).map((breach) => `budget ${String(budget)}: ${breach}`);
          }).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, budget), Effect.scoped),
        );
        assert.deepStrictEqual(found.flat(), []);
      }),
  );

  it.effect("makes its events die with a hook's or a release's defect, for every reader", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const thrown = new Error("the renderer's own bug");
      /** How `events` ends for a reader from before clip "a" goes wrong, and for one from after. */
      const ends = <E>(
        open: Effect.Effect<Playout.Source, E, Scope.Scope>,
        after?: (
          source: Playout.Source,
          clipId: string,
        ) => Effect.Effect<void, ReactorError.CommandFailure>,
      ) =>
        Effect.gen(function* () {
          const source = yield* open;
          const drain = source.events.pipe(Stream.runDrain, Effect.exit);
          const before = yield* drain.pipe(Effect.forkScoped({ startImmediately: true }));
          yield* source.setAutoplay(true);
          const clipId = yield* source.enqueue(request("a"), tag("a"));
          if (after !== undefined) yield* after(source, clipId);
          const early = yield* Fiber.join(before).pipe(Effect.timeoutOption("1 minute"));
          const late = yield* drain.pipe(Effect.timeoutOption("1 minute"));
          return [early, late];
        });
      /** The defect `events` died with. */
      const defect = (
        ending: Option.Option<Exit.Exit<void, ReactorError.ReactorError>>,
      ): unknown => {
        if (Option.isNone(ending)) return assert.fail("the events never ended");
        if (Exit.isSuccess(ending.value)) return assert.fail("the events ended without a failure");
        const found = Cause.findDefect(ending.value.cause);
        return Result.isSuccess(found)
          ? found.success
          : assert.fail(`the events ended with ${Cause.pretty(ending.value.cause)}`);
      };
      const presenting = yield* Deferred.make<void>();
      const hooks = [
        ...(yield* ends(
          LocalSource.open({
            build: () => {
              throw thrown;
            },
          }),
        )),
        ...(yield* ends(
          LocalSource.open({
            build: () => Effect.succeed({ value: undefined }),
            present: () => {
              throw thrown;
            },
          }),
        )),
        // A presentation that dies as it is stopped.
        ...(yield* ends(
          LocalSource.open({
            build: () => Effect.succeed({ value: undefined }),
            present: () =>
              Deferred.succeed(presenting, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() => Effect.die(thrown)),
              ),
          }),
          (source, clipId) => Effect.andThen(Deferred.await(presenting), source.stop(clipId)),
        )),
      ];
      for (const ending of hooks) assert.strictEqual(defect(ending), thrown);
      // A release that fails without a defect, joining a fiber that something else interrupted,
      // loses the session too, rather than end playback unseen.
      const releases = yield* ends(
        LocalSource.open({
          build: () =>
            Effect.gen(function* () {
              const helper = yield* Effect.forkDetach(Effect.never);
              yield* Fiber.interrupt(helper);
              yield* Effect.addFinalizer(() => Fiber.join(helper));
              return { value: undefined };
            }),
        }),
      );
      for (const ending of releases) defect(ending);
    }),
  );

  it.effect("stops building and playing once a hook throws", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const thrown = new Error("the renderer's own bug");
      /** Work that runs until it is interrupted: `started` once it runs, `stopped` once interrupted. */
      const endless = (started: Deferred.Deferred<void>, stopped: Deferred.Deferred<void>) =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Effect.asVoid(Deferred.succeed(stopped, undefined))),
        );
      const waited = Effect.timeoutOption("1 minute");

      // A build throws while a clip plays: the presentation stops.
      const presenting = yield* Deferred.make<void>();
      const presentationStopped = yield* Deferred.make<void>();
      const playing = yield* LocalSource.open({
        build: (clip) => {
          if (clip.request.prompt === "b") throw thrown;
          return Effect.succeed({ value: undefined });
        },
        present: () => endless(presenting, presentationStopped),
      });
      yield* playing.setAutoplay(true);
      yield* playing.enqueue(request("a"), tag("a"));
      yield* Deferred.await(presenting);
      yield* playing.enqueue(request("b"), tag("b"));
      const presentation = yield* waited(Deferred.await(presentationStopped));
      assert.isTrue(Option.isSome(presentation), "the clip kept playing");

      // A presentation throws while a clip builds: the build stops.
      const building = yield* Deferred.make<void>();
      const buildStopped = yield* Deferred.make<void>();
      const built = yield* LocalSource.open({
        build: (clip) =>
          clip.request.prompt === "b"
            ? endless(building, buildStopped)
            : Effect.succeed({ value: undefined }),
        present: () => {
          throw thrown;
        },
      });
      yield* built.enqueue(request("a"), tag("a"));
      yield* built.enqueue(request("b"), tag("b"));
      yield* Deferred.await(building);
      yield* built.setAutoplay(true);
      const build = yield* waited(Deferred.await(buildStopped));
      assert.isTrue(Option.isSome(build), "the clip kept building");
    }),
  );

  it.effect(
    "hands present the value its build made and the length it built, and fails a length that cannot play",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
        const presented = yield* Deferred.make<{
          readonly seconds: number;
          readonly value: string;
        }>();
        const source = yield* LocalSource.open({
          build: (clip) =>
            Effect.succeed({
              value: `voiced ${clip.request.prompt}`,
              seconds: clip.request.prompt === "unmeasured" ? Number.NaN : clip.seconds + 2.25,
            }),
          present: (clip, value) =>
            Effect.andThen(
              Deferred.succeed(presented, { seconds: clip.seconds, value }),
              Effect.sleep(Duration.seconds(clip.seconds)),
            ),
        });
        const events = yield* record(source);
        yield* source.setAutoplay(true);
        const unmeasured = yield* source.enqueue(request("unmeasured", 5), tag("unmeasured"));
        yield* source.enqueue(request("a", 5), tag("a"));
        assert.deepStrictEqual(yield* Deferred.await(presented), {
          seconds: 7.25,
          value: "voiced a",
        });
        yield* until(events, "the unmeasured clip's failure", (all) =>
          all.some((event) => event._tag === "Failed" && event.clip.clipId === unmeasured),
        );
      }),
  );

  it.effect(
    "infers each hook's own error and services, and fails a clip with its hook's error",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
        const opened = LocalSource.open({
          build: (clip) =>
            Effect.map(
              Voice.use((voice) => voice.speak(clip.request.prompt)),
              (line) => ({ value: line }),
            ),
          present: (_clip, line) => Stage.use((stage) => stage.show(line)),
        });
        // Without type arguments, the source needs what either hook needs.
        const needs: Types.Equals<
          Effect.Services<typeof opened>,
          Voice | Stage | Scope.Scope
        > = true;
        assert.isTrue(needs);
        const shown = yield* SubscriptionRef.make<ReadonlyArray<string>>([]);
        const source = yield* opened.pipe(
          Effect.provideService(
            Voice,
            Voice.of({
              speak: (line) =>
                line === "mute" ? Unvoiced.make({}) : Effect.succeed(`voiced ${line}`),
            }),
          ),
          Effect.provideService(
            Stage,
            Stage.of({
              show: (line) =>
                line === "voiced offstage"
                  ? Unstaged.make({})
                  : SubscriptionRef.update(shown, (all) => [...all, line]),
            }),
          ),
        );
        const events = yield* record(source);
        yield* source.setAutoplay(true);
        const [mute, offstage, line] = yield* Effect.forEach(
          ["mute", "offstage", "line"],
          (prompt) => source.enqueue(request(prompt, 1), tag(prompt)),
        );
        yield* until(events, "the last line's end", ended(line ?? ""));
        const failed = (yield* SubscriptionRef.get(events)).filter(
          (event) => event._tag === "Failed",
        );
        // Each clip failed with the error of its own hook, which its provider text names.
        assert.deepStrictEqual(
          failed.map((event) => [
            event.clip.clipId,
            event.message,
            ["Unvoiced", "Unstaged"].filter((name) =>
              Redacted.value(event.provider).includes(name),
            ),
          ]),
          [
            [mute, "the local build failed", ["Unvoiced"]],
            [offstage, "the local presentation failed", ["Unstaged"]],
          ],
        );
        assert.deepStrictEqual(yield* SubscriptionRef.get(shown), ["voiced line"]);
      }),
  );

  it.effect("closes the scope a clip was built and played in once the clip leaves the source", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const released = yield* SubscriptionRef.make<ReadonlyArray<string>>([]);
      const openWhilePlaying = yield* Deferred.make<boolean>();
      const scope = yield* Scope.fork(yield* Effect.scope);
      const source = yield* LocalSource.open({
        build: (clip) =>
          Effect.gen(function* () {
            const prompt = clip.request.prompt;
            yield* Effect.addFinalizer(() =>
              SubscriptionRef.update(released, (all) => [...all, prompt]),
            );
            if (prompt === "bad") return yield* Effect.fail("unrenderable");
            return { value: prompt };
          }),
        present: (clip, value) =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              SubscriptionRef.update(released, (all) => [...all, `${value} played`]),
            );
            const open = !(yield* SubscriptionRef.get(released)).includes(value);
            yield* Deferred.succeed(openWhilePlaying, open);
            yield* Effect.sleep(Duration.seconds(clip.seconds));
          }),
      }).pipe(Scope.provide(scope));
      const events = yield* record(source);
      const [a, , r] = yield* Effect.forEach(["a", "bad", "r", "s"], (prompt) =>
        source.enqueue(request(prompt), tag(prompt)),
      );
      yield* until(events, "three Ready clips", (all) =>
        all.some((event) => event._tag === "State" && event.state.ready.length === 3),
      );
      yield* until(released, "the failed build's release", (all) => all.includes("bad"));
      assert.deepStrictEqual(yield* SubscriptionRef.get(released), ["bad"]);
      yield* source.remove(r ?? "");
      assert.deepStrictEqual(yield* SubscriptionRef.get(released), ["bad", "r"]);
      yield* source.play(a ?? "");
      yield* until(released, "the played clip's release", (all) => all.includes("a"));
      assert.isTrue(yield* Deferred.await(openWhilePlaying), "its scope closed before it played");
      assert.deepStrictEqual(yield* SubscriptionRef.get(released), ["bad", "r", "a played", "a"]);
      yield* Scope.close(scope, Exit.void);
      assert.deepStrictEqual(yield* SubscriptionRef.get(released), [
        "bad",
        "r",
        "a played",
        "a",
        "s",
      ]);
    }),
  );

  it.effect(
    "closes in order in any scope: playback stops, each clip is released in full, and a stop in flight returns",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
        const log = yield* SubscriptionRef.make<ReadonlyArray<string>>([]);
        const note = (entry: string) => SubscriptionRef.update(log, (all) => [...all, entry]);

        // Closed while a stop waits on a presentation that takes a second to stop, in a scope
        // that runs its finalizers all at once, as a ManagedRuntime's does.
        const playing = yield* Deferred.make<void>();
        const parallel = yield* Scope.make("parallel");
        const stopping = yield* LocalSource.open({
          build: (clip) =>
            Effect.as(
              Effect.addFinalizer(() => note(`released ${clip.request.prompt}`)),
              {
                value: clip.request.prompt,
              },
            ),
          present: (_clip, prompt) =>
            Deferred.succeed(playing, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Effect.andThen(Effect.sleep("1 second"), note(`stopped ${prompt}`)),
              ),
            ),
        }).pipe(Scope.provide(parallel));
        yield* stopping.setAutoplay(true);
        const a = yield* stopping.enqueue(request("a"), tag("a"));
        yield* Deferred.await(playing);
        const stop = yield* stopping.stop(a).pipe(Effect.forkScoped({ startImmediately: true }));
        yield* Scope.close(parallel, Exit.void);
        const stopped = yield* Fiber.await(stop).pipe(Effect.timeoutOption("1 minute"));
        assert.isTrue(Option.isSome(stopped), "the stop never returned");
        assert.deepStrictEqual(yield* SubscriptionRef.get(log), ["stopped a", "released a"]);

        // Closed while a clip that ended is being released, which takes a second.
        const scope = yield* Scope.make();
        const releasing = yield* LocalSource.open({
          build: (clip) =>
            Effect.gen(function* () {
              const prompt = clip.request.prompt;
              yield* Effect.addFinalizer(() => note(`released ${prompt}`));
              yield* Effect.addFinalizer(() =>
                note(`flushing ${prompt}`).pipe(
                  Effect.andThen(Effect.sleep("1 second")),
                  Effect.andThen(note(`flushed ${prompt}`)),
                ),
              );
              return { value: prompt };
            }),
        }).pipe(Scope.provide(scope));
        yield* releasing.setAutoplay(true);
        yield* releasing.enqueue(request("b", 1), tag("b"));
        yield* until(log, "b's release", (all) => all.includes("flushing b"));
        yield* Scope.close(scope, Exit.void);
        assert.deepStrictEqual(yield* SubscriptionRef.get(log), [
          "stopped a",
          "released a",
          "flushing b",
          "flushed b",
          "released b",
        ]);
      }),
  );
});
