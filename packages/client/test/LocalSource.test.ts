/** A local renderer as a playout source: its ids, the order of its evidence, its hooks and each clip's scope. */
import { assert, describe, it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Result,
  Scheduler,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import { LocalSource, Playout, ReactorTest } from "../src/index.js";

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

describe("LocalSource", () => {
  it.effect("gives each session it opens an id of its own", () =>
    Effect.gen(function* () {
      const first = yield* LocalSource.open();
      const second = yield* LocalSource.open();
      assert.notStrictEqual(first.sessionId, second.sessionId);
    }),
  );

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

  it.effect("makes its events die with what a hook threw, for every reader", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const thrown = new Error("the renderer's own bug");
      /** How `events` ends for a reader from before a hook throws, and for one from after. */
      const ends = <E>(open: Effect.Effect<Playout.Source, E, Scope.Scope>) =>
        Effect.gen(function* () {
          const source = yield* open;
          const drain = source.events.pipe(Stream.runDrain, Effect.exit);
          const before = yield* drain.pipe(Effect.forkScoped({ startImmediately: true }));
          yield* source.setAutoplay(true);
          yield* source.enqueue(request("a"), tag("a"));
          const early = yield* Fiber.join(before).pipe(Effect.timeoutOption("1 minute"));
          const late = yield* drain.pipe(Effect.timeoutOption("1 minute"));
          return [early, late];
        });
      const endings = [
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
      ];
      for (const ending of endings) {
        assert.isTrue(Option.isSome(ending), "the events never ended");
        if (Option.isNone(ending)) continue;
        assert.isTrue(Exit.isFailure(ending.value), "the events ended without a failure");
        if (Exit.isSuccess(ending.value)) continue;
        const defect = Cause.findDefect(ending.value.cause);
        assert.isTrue(
          Result.isSuccess(defect) && defect.success === thrown,
          `the events ended with ${Cause.pretty(ending.value.cause)}`,
        );
      }
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

  it.effect("hands present the value its build made and the length it built", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const presented = yield* Deferred.make<{
        readonly seconds: number;
        readonly value: string;
      }>();
      const source = yield* LocalSource.open({
        build: (clip) =>
          Effect.succeed({ value: `voiced ${clip.request.prompt}`, seconds: clip.seconds + 2.25 }),
        present: (clip, value) =>
          Effect.andThen(
            Deferred.succeed(presented, { seconds: clip.seconds, value }),
            Effect.sleep(Duration.seconds(clip.seconds)),
          ),
      });
      yield* source.setAutoplay(true);
      yield* source.enqueue(request("a", 5), tag("a"));
      assert.deepStrictEqual(yield* Deferred.await(presented), {
        seconds: 7.25,
        value: "voiced a",
      });
    }),
  );

  it.effect("closes the scope a clip was built in once the clip leaves the source", () =>
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
      yield* Scope.close(scope, Exit.void);
      assert.deepStrictEqual(yield* SubscriptionRef.get(released), ["bad", "r", "a", "s"]);
    }),
  );
});
