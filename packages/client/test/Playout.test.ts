/** The playout on the simulated Reactor, from submission to as-run, with the timing each case relies on. */
import { assert, layer } from "@effect/vitest";
import { Deferred, Duration, Effect, Exit, Option, Ref, Scope, Stream } from "effect";
import * as Coordinator from "../src/Coordinator.js";
import * as H3 from "../src/H3.js";
import { H3Source, LocalSource, Playout, ReactorError, ReactorTest } from "../src/index.js";
import type { Options } from "../src/Playout.js";
import { environment } from "./fixtures/Simulated.js";

const key = (value: string) => Playout.ItemKey.make(value);
const clip = (prompt: string, seconds = 5): H3.Request => ({ prompt, seconds });

const tokens = (maxSessionDuration: Duration.Input) =>
  Effect.gen(function* () {
    const test = yield* ReactorTest.ReactorTest;
    const coordinator = yield* Coordinator.Coordinator;
    return coordinator.tokens({
      apiKey: test.apiKey,
      modelName: H3.modelName,
      maxSessionDuration,
      expiresAfter: Duration.sum(Duration.fromInputUnsafe(maxSessionDuration), Duration.minutes(5)),
    });
  });

/** A playout on paid-shaped H3 sessions, with its events recorded from the start. */
const start = (options: Partial<Options<never>> & { readonly lifetime?: Duration.Input } = {}) =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
    const playout = yield* Playout.make({
      open: H3Source.open({ tokens: yield* tokens(options.lifetime ?? "10 minutes") }),
      lanes: [{ name: "urgent", cut: true }, { name: "line" }, { name: "quiet", conflict: "skip" }],
      ...options,
    });
    const events = yield* Ref.make<ReadonlyArray<Playout.Event>>([]);
    // Started at once, so it subscribes before anything is submitted. The playout publishes an
    // event before it resolves a handle, so a handle's caller finds the event recorded.
    yield* playout.events.pipe(
      Stream.runForEach((event) => Ref.update(events, (all) => [...all, event])),
      Effect.forkScoped({ startImmediately: true }),
    );
    const recorded = Ref.get(events);
    const starts = Effect.map(recorded, (all) =>
      all.flatMap((event) =>
        event._tag === "AsRun" && event.event.status._tag === "Started"
          ? [event.event.key as string]
          : [],
      ),
    );
    const statuses = (item: string) =>
      Effect.map(recorded, (all) =>
        all.flatMap((event) =>
          event._tag === "AsRun" && event.event.key === item ? [event.event.status._tag] : [],
        ),
      );
    return { playout, events: recorded, starts, statuses };
  });

/** Checks `effect` until `done` holds, on the virtual clock, failing after `within`. */
const eventually = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  done: (value: A) => boolean,
  within: Duration.Input = "1 minute",
) =>
  Effect.gen(function* () {
    const deadline = Duration.toMillis(Duration.fromInputUnsafe(within));
    for (let waited = 0; waited <= deadline; waited += 100) {
      const value = yield* effect;
      if (done(value)) return value;
      yield* Effect.sleep("100 millis");
    }
    return yield* Effect.die(`not within ${String(deadline)} ms`);
  });

/**
 * What went wrong with the stops the simulated H3 received: a stop that found
 * its clip already stopped once, or arrived while another stop was landing and
 * so would stop whatever started next.
 */
const stopProblems = (log: ReadonlyArray<ReactorTest.Entry>): ReadonlyArray<string> => {
  const problems: Array<string> = [];
  const sessions = new Map<
    string,
    { playing?: string | undefined; landing?: string | undefined; stopped: Set<string> }
  >();
  for (const entry of log) {
    const session = sessions.get(entry.sessionId) ?? { stopped: new Set<string>() };
    sessions.set(entry.sessionId, session);
    if (entry.kind === "message" && entry.name === "clip_started") session.playing = entry.clipId;
    if (
      entry.kind === "message" &&
      (entry.name === "clip_stopped" || entry.name === "clip_finished")
    ) {
      if (session.playing === entry.clipId) session.playing = undefined;
      if (session.landing === entry.clipId || entry.name === "clip_stopped")
        session.landing = undefined;
    }
    if (entry.kind === "command" && entry.name === "stop") {
      if (session.landing !== undefined)
        problems.push(
          `a stop at ${entry.at} ms arrived while ${session.landing} was being stopped`,
        );
      const playing = session.playing;
      if (playing === undefined) continue;
      if (session.stopped.has(playing)) problems.push(`${playing} was stopped twice`);
      session.stopped.add(playing);
      session.landing = playing;
    }
  }
  return problems;
};

const hosted = environment({
  timing: ReactorTest.Timing.fixed({
    buildSpeed: 2.4,
    seam: "70 millis",
    http: "40 millis",
    channel: "20 millis",
  }),
});

layer(hosted)("order", (it) => {
  it.effect("airs a lane first in, first out, and each item's as-run moves forward", () =>
    Effect.gen(function* () {
      const { playout, starts, statuses } = yield* start();
      const handles = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      for (const handle of handles) assert.strictEqual((yield* handle.outcome)._tag, "Ended");
      assert.deepStrictEqual(yield* starts, ["a", "b", "c"]);
      assert.deepStrictEqual(yield* statuses("b"), [
        "Accepted",
        "Building",
        "Ready",
        "Started",
        "Ended",
      ]);
    }),
  );

  it.effect(
    "plays a group back to back, an insert beside its anchor, and a higher lane between parts",
    () =>
      Effect.gen(function* () {
        const { playout, starts } = yield* start();
        const group = yield* playout.submitGroup({
          key: key("line"),
          lane: "line",
          parts: [
            { key: key("p1"), request: clip("one") },
            { key: key("p2"), request: clip("two") },
          ],
        });
        const after = yield* playout.submit({
          key: key("after"),
          lane: "line",
          request: clip("after"),
        });
        yield* playout.insert({
          key: key("inserted"),
          request: clip("inserted"),
          after: key("p2"),
        });
        yield* after.outcome;
        assert.deepStrictEqual(yield* starts, ["p1", "p2", "inserted", "after"]);
        const [, second] = group.parts;
        assert.strictEqual((yield* second?.outcome ?? Effect.die("no second part"))._tag, "Ended");
      }),
  );
});

layer(hosted)("edits", (it) => {
  it.effect("replaces an item make-before-break and withdraws one that waits", () =>
    Effect.gen(function* () {
      const { playout, starts, statuses } = yield* start();
      const first = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      yield* playout.submit({ key: key("c"), lane: "line", request: clip("c") });
      yield* first.started;
      const replacement = yield* playout.replace(key("b"), { key: key("b2"), request: clip("b2") });
      assert.strictEqual(yield* playout.withdraw(key("c")), "withdrawn");
      yield* replacement.outcome;
      assert.deepStrictEqual(yield* starts, ["a", "b2"]);
      assert.deepStrictEqual((yield* statuses("b")).at(-1), "Dropped");
    }),
  );

  it.effect("airs a clip inserted after the playing one at the next boundary", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const first = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      const queued = yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      yield* first.started;
      const refused = yield* Effect.flip(
        playout.insert({ key: key("early"), request: clip("early"), before: key("a") }),
      );
      assert.strictEqual(refused._tag, "InvalidItem");
      yield* playout.insert({ key: key("next"), request: clip("next"), after: key("a") });
      yield* queued.outcome;
      assert.deepStrictEqual(yield* starts, ["a", "next", "b"]);
    }),
  );

  it.effect("applies a batch together once everything it adds is Ready", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const first = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      yield* playout.submit({ key: key("old"), lane: "line", request: clip("old") });
      yield* first.started;
      const batch = yield* playout.edit([
        { _tag: "Withdraw", key: key("old") },
        { _tag: "Submit", item: { key: key("new"), lane: "line", request: clip("new") } },
      ]);
      yield* batch.committed;
      const added = batch.results[1];
      assert.strictEqual(added?._tag, "Added");
      if (added?._tag === "Added") yield* added.handle.outcome;
      assert.deepStrictEqual(yield* starts, ["a", "new"]);
    }),
  );

  it.effect("keeps keys idempotent and refuses what cannot be admitted", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const once = yield* playout.submit({ key: key("a"), lane: "quiet", request: clip("a") });
      const again = yield* playout.submit({ key: key("a"), lane: "quiet", request: clip("a") });
      assert.strictEqual(once.key, again.key);
      const mismatch = yield* Effect.flip(
        playout.submit({ key: key("a"), lane: "quiet", request: clip("changed") }),
      );
      assert.strictEqual(mismatch._tag, "KeyMismatch");
      const busy = yield* Effect.flip(
        playout.submit({ key: key("b"), lane: "quiet", request: clip("b") }),
      );
      assert.strictEqual(busy._tag, "LaneBusy");
      const unknown = yield* Effect.flip(
        playout.submit({ key: key("c"), lane: "nowhere", request: clip("c") }),
      );
      assert.strictEqual(unknown._tag, "InvalidItem");
    }),
  );
});

layer(hosted)("cuts", (it) => {
  it.effect("an urgent item stops a long lower-lane clip, and only that clip", () =>
    Effect.gen(function* () {
      const { playout, statuses } = yield* start();
      const long = yield* playout.submit({
        key: key("long"),
        lane: "line",
        request: clip("long", 15),
      });
      yield* long.started;
      yield* Effect.sleep("2 seconds");
      const urgent = yield* playout.submit({
        key: key("urgent"),
        lane: "urgent",
        request: clip("urgent"),
      });
      const aired = yield* urgent.outcome;
      assert.strictEqual(aired._tag, "Ended");
      if (aired._tag === "Ended") assert.strictEqual(aired.termination, "finished");
      const ended = yield* long.outcome;
      assert.strictEqual(ended._tag, "Ended");
      if (ended._tag === "Ended") assert.strictEqual(ended.termination, "stopped");
      assert.deepStrictEqual((yield* statuses("urgent")).at(-1), "Ended");
    }),
  );
});

// The 0.7.0 scheduler-cut paid run: H3 answered a stop about 20 ms before it reported the clip
// stopped. Here it lands 100 ms after its acknowledgement.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      seam: "70 millis",
      http: "40 millis",
      channel: "20 millis",
      stop: "100 millis",
    }),
  }),
)("cuts, when H3 answers a stop before the clip ends", (it) => {
  it.effect("stop the playing clip once, and play the cutter once it has ended", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const { playout, starts } = yield* start();
      const long = yield* playout.submit({
        key: key("long"),
        lane: "line",
        request: clip("long", 15),
      });
      yield* long.started;
      yield* Effect.sleep("2 seconds");
      const urgent = yield* playout.submit({
        key: key("urgent"),
        lane: "urgent",
        request: clip("urgent"),
      });
      const aired = yield* urgent.outcome;
      assert.deepStrictEqual(aired._tag === "Ended" ? aired.termination : aired._tag, "finished");
      const cut = yield* long.outcome;
      assert.deepStrictEqual(cut._tag === "Ended" ? cut.termination : cut._tag, "stopped");
      assert.deepStrictEqual(yield* starts, ["long", "urgent"]);
      const log = yield* test.log;
      const named = (kind: ReactorTest.Entry["kind"], name: string) =>
        log.filter((entry) => entry.kind === kind && entry.name === name);
      assert.strictEqual(named("command", "stop").length, 1);
      // The cutter's play waited for the stop to land, so H3 refused nothing.
      assert.deepStrictEqual(named("message", "command_error"), []);
      assert.deepStrictEqual(stopProblems(log), []);
    }),
  );
});

// The 0.7.0 scheduler-edits paid run: a continued 5 s clip took 5.45 s to build against about
// 2.2 s for an independent one. Here a continued build runs at 0.9 times real time.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      continuedBuildSpeed: 0.9,
      seam: "70 millis",
      http: "40 millis",
      channel: "20 millis",
    }),
  }),
)("continuity", (it) => {
  it.effect(
    "a continued insert that would miss its place continues from, and airs after, the clip before it then",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        const { playout, starts } = yield* start();
        const group = yield* playout.submitGroup({
          key: key("line"),
          lane: "line",
          parts: [
            { key: key("p1"), request: clip("p1") },
            { key: key("p2"), request: clip("p2") },
            { key: key("p3"), request: clip("p3") },
          ],
        });
        const [first, , last] = group.parts;
        yield* first.started;
        // p2 is building, so xc waits for the build slot, then builds far slower than p1 plays.
        yield* Effect.sleep("200 millis");
        yield* playout.insert({
          key: key("xc"),
          request: clip("xc"),
          before: key("p2"),
          continuity: "previous",
        });
        yield* (last ?? first).outcome;
        assert.deepStrictEqual(yield* starts, ["p1", "p2", "xc", "p3"]);
        const log = yield* test.log;
        const started = log.flatMap((entry) =>
          entry.kind === "message" && entry.name === "clip_started" ? [entry.clipId] : [],
        );
        const continued = log.filter((entry) => entry.kind === "build");
        assert.deepStrictEqual(
          continued.map((entry) => [entry.name, entry.clipId, entry.continuedFrom]),
          [["continued", started[2], started[1]]],
        );
      }),
  );
});

layer(hosted)("the cut's fence", (it) => {
  it.effect("puts back the autoplay the playout asked for, not always on", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const source = yield* H3Source.open({ tokens: yield* tokens("10 minutes") });
      const started = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* source.events.pipe(
        Stream.runForEach((event) =>
          event._tag === "Started"
            ? Ref.update(started, (all) => [...all, event.clip.clipId])
            : Effect.void,
        ),
        Effect.forkScoped,
      );
      yield* source.setAutoplay(false);
      const first = yield* source.enqueue(clip("first"), { _tag: "Filler", index: 0 });
      yield* source.enqueue(clip("second"), { _tag: "Filler", index: 1 });
      yield* Effect.sleep("6 seconds");
      // Nothing plays, so nothing is stopped; the play starts the first clip.
      yield* source.cut("no-such-clip", first);
      yield* Effect.sleep("12 seconds");
      // Autoplay stayed off: the second clip, Ready all along, waits for a play.
      assert.deepStrictEqual(yield* Ref.get(started), [first]);
    }),
  );
});

layer(hosted)("resume", (it) => {
  // H3 keeps no history: the process that adopts a session learns the playing clip's id
  // from the state alone. The rehearsed `resume` check found that id dropped.
  it.effect("names the clip already playing, and adopts the session by reading it", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const sessionTokens = yield* tokens("10 minutes");
      const recorded = yield* Deferred.make<H3Source.Allocation>();
      // The owner's scope stays open, as a crashed process's would.
      const owned = yield* Scope.make();
      const owner = yield* H3Source.open({
        tokens: sessionTokens,
        onAllocated: ({ allocation }) => Deferred.succeed(recorded, allocation),
      }).pipe(Scope.provide(owned));
      yield* owner.setAutoplay(true);
      const playing = yield* owner.enqueue(clip("playing", 15), {
        _tag: "Item",
        key: key("playing"),
      });
      yield* owner.events.pipe(
        Stream.filter((event) => event._tag === "State" && event.state.playing?.clipId === playing),
        Stream.take(1),
        Stream.runDrain,
      );
      const before = (yield* test.log).length;
      const source = yield* H3Source.resume({
        allocation: yield* Deferred.await(recorded),
        tokens: sessionTokens,
      });
      const first = yield* source.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));
      assert.deepStrictEqual(first._tag === "State" ? first.state.playing : undefined, {
        clipId: playing,
        tag: undefined,
        seconds: undefined,
      });
      const sent = (yield* test.log)
        .slice(before)
        .flatMap((entry) => (entry.kind === "command" ? [entry.name] : []));
      assert.deepStrictEqual(
        sent.filter((name) => name !== "get_state" && name !== "get_queue"),
        [],
      );
      yield* Scope.close(owned, Exit.void);
    }),
  );
});

layer(hosted)("time", (it) => {
  it.effect("refuses a firm item that cannot start before its deadline", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const long = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 15) });
      yield* long.started;
      const refused = yield* Effect.flip(
        playout.submit({
          key: key("late"),
          lane: "line",
          request: clip("late"),
          window: { startBy: "3 seconds", firm: true },
        }),
      );
      assert.strictEqual(refused._tag, "WouldMissDeadline");
    }),
  );

  it.effect("holds a Manual item until released and fires cues from its start", () =>
    Effect.gen(function* () {
      const { playout, events, starts } = yield* start();
      const held = yield* playout.submit({
        key: key("held"),
        lane: "line",
        request: clip("held"),
        start: { _tag: "Manual" },
        cues: [{ name: "caption", at: { from: "start", offset: "1 second" } }],
      });
      yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      yield* Effect.sleep("12 seconds");
      assert.deepStrictEqual(yield* starts, ["a"]);
      yield* playout.release(key("held"));
      yield* held.outcome;
      assert.deepStrictEqual(yield* starts, ["a", "held"]);
      const cues = (yield* events).filter((event) => event._tag === "Cue");
      assert.strictEqual(cues.length, 1);
    }),
  );
});

layer(hosted)("uncertainty", (it) => {
  it.effect("never sends an enqueue again after its reply was lost", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const { playout } = yield* start();
      yield* Effect.sleep("3 seconds");
      yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 1 });
      const lost = yield* playout.submit({ key: key("lost"), lane: "line", request: clip("lost") });
      const next = yield* playout.submit({ key: key("next"), lane: "line", request: clip("next") });
      yield* next.outcome;
      const enqueues = (yield* test.log).filter(
        (entry) => entry.kind === "command" && entry.name === "enqueue",
      );
      // The lost enqueue went once, whatever became of it.
      assert.strictEqual(enqueues.length, 2);
      const status = yield* lost.outcome.pipe(Effect.timeoutOption("30 seconds"));
      if (status._tag === "Some") assert.notStrictEqual(status.value._tag, "Accepted");
    }),
  );
});

layer(hosted)("renewal", (it) => {
  it.effect(
    "opens a replacement before the lifetime ends and switches at a boundary, in order",
    () =>
      Effect.gen(function* () {
        const { playout, starts, events } = yield* start({
          lifetime: "90 seconds",
          renewal: { lead: "40 seconds" },
        });
        const handles = yield* Effect.forEach(
          Array.from({ length: 16 }, (_, index) => `n${index}`),
          (name) => playout.submit({ key: key(name), lane: "line", request: clip(name) }),
        );
        for (const handle of handles) yield* handle.outcome;
        assert.deepStrictEqual(
          yield* starts,
          Array.from({ length: 16 }, (_, index) => `n${index}`),
        );
        // The switch waits for the retiring session's last clip to end and the grace to pass.
        const sessions = yield* eventually(
          Effect.map(events, (all) =>
            all.flatMap((event) => (event._tag === "Session" ? [event.event._tag] : [])),
          ),
          (all) => all.includes("Switched"),
        );
        assert.deepStrictEqual(sessions.slice(0, 3), ["Opened", "Opened", "Switched"]);
        // The retired session closes after the switch, confirmed by its own read.
        yield* eventually(playout.cleanup, (cleanup) => cleanup.sessions >= 1);
      }),
    { timeout: 60_000 },
  );

  // The critique's probe: with a 60 s cap, n10 was cut mid-clip at the cap and failed as lost,
  // and an item submitted later aired ahead of n11 to n13.
  it.effect(
    "a backlog longer than the cap airs whole and in order across the replacement",
    () =>
      Effect.gen(function* () {
        const { playout, starts } = yield* start({
          lifetime: "60 seconds",
          renewal: { lead: "10 seconds" },
        });
        const names = Array.from({ length: 14 }, (_, index) => `n${index}`);
        const handles = yield* Effect.forEach(names, (name) =>
          playout.submit({ key: key(name), lane: "line", request: clip(name) }),
        );
        yield* Effect.sleep("20 seconds");
        const late = yield* playout.submit({
          key: key("late"),
          lane: "line",
          request: clip("late"),
        });
        const outcomes = yield* Effect.forEach([...handles, late], (handle) => handle.outcome);
        assert.deepStrictEqual(
          outcomes.map((status) => status._tag),
          [...names, "late"].map(() => "Ended"),
        );
        assert.deepStrictEqual(yield* starts, [...names, "late"]);
      }),
    { timeout: 60_000 },
  );

  it.effect(
    "rebuilds a lost session's unaired clips on the replacement",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        // Every session ends 12 s after it becomes active, well before its grant.
        yield* test.inject({ _tag: "Expire", after: Duration.seconds(12) });
        const { playout, events } = yield* start();
        const first = yield* playout.submit({
          key: key("a"),
          lane: "line",
          request: clip("a", 10),
        });
        const second = yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
        yield* first.started;
        const status = yield* second.outcome;
        assert.strictEqual(status._tag, "Ended");
        const carried = (yield* events).some(
          (event) =>
            event._tag === "AsRun" &&
            event.event.key === "b" &&
            event.event.status._tag === "Accepted" &&
            event.event.status.carried !== undefined,
        );
        assert.isTrue(carried);
      }),
    { timeout: 60_000 },
  );
});

layer(hosted)("drain", (it) => {
  it.effect("finishes what was accepted, then admits nothing", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      yield* playout.drain({ finish: "accepted" });
      assert.deepStrictEqual(yield* starts, ["a", "b"]);
      const closed = yield* Effect.flip(
        playout.submit({ key: key("c"), lane: "line", request: clip("c") }),
      );
      assert.strictEqual(closed._tag, "PlayoutClosed");
    }),
  );
});

layer(hosted)("filler", (it) => {
  it.effect(
    "keeps the air covered with filler between items",
    () =>
      Effect.gen(function* () {
        const { playout, events } = yield* start({
          filler: {
            runway: { floor: "4 seconds", target: "8 seconds" },
            clip: ({ index }) => clip(`idle ${index}`),
          },
        });
        yield* Effect.sleep("30 seconds");
        const item = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
        assert.strictEqual((yield* item.outcome)._tag, "Ended");
        const state = yield* playout.state;
        assert.isAbove(state.runwaySeconds, 0);
        assert.strictEqual((yield* events).filter((event) => event._tag === "Starved").length, 0);
      }),
    { timeout: 60_000 },
  );
});

layer(hosted)("local renderer", (it) => {
  it.effect("runs the same plan on a local renderer", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const presented = yield* Ref.make<ReadonlyArray<string>>([]);
      const playout = yield* Playout.make({
        open: LocalSource.open({
          present: (local) =>
            Effect.andThen(
              Ref.update(presented, (all) => [...all, local.request.prompt]),
              Effect.sleep(Duration.seconds(local.seconds)),
            ),
        }),
        lanes: [{ name: "speech" }],
      });
      const items = yield* Effect.forEach(["hello", "world"], (prompt) =>
        playout.submit({ key: key(prompt), lane: "speech", request: clip(prompt) }),
      );
      for (const item of items) yield* item.outcome;
      assert.deepStrictEqual(yield* Ref.get(presented), ["hello", "world"]);
    }),
  );

  it.effect("reports what a local renderer failed, discarded or cut, and moves on", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const discarded = yield* Ref.make<ReadonlyArray<string>>([]);
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: (local) =>
            local.request.prompt === "unbuildable"
              ? Effect.fail(
                  ReactorError.ReactorError.fromCode("InvalidState", "the renderer refused"),
                )
              : Effect.sleep("500 millis"),
          present: (local) =>
            local.request.prompt === "broken"
              ? Effect.andThen(
                  Effect.sleep("1 second"),
                  Effect.fail(
                    ReactorError.ReactorError.fromCode("InvalidState", "the speaker failed"),
                  ),
                )
              : Effect.sleep(Duration.seconds(local.seconds)),
          discard: (local) => Ref.update(discarded, (all) => [...all, local.request.prompt]),
        }),
        lanes: [{ name: "urgent", cut: true }, { name: "speech" }],
      });
      const submit = (name: string, lane = "speech", seconds = 5) =>
        playout.submit({ key: key(name), lane, request: clip(name, seconds) });
      const unbuildable = yield* submit("unbuildable");
      const broken = yield* submit("broken");
      const long = yield* submit("long", "speech", 15);
      const spare = yield* submit("spare");
      assert.strictEqual((yield* unbuildable.outcome)._tag, "Failed");
      const cut = yield* broken.outcome;
      assert.deepStrictEqual(cut._tag === "Ended" ? cut.termination : cut._tag, "stopped");
      yield* long.started;
      // The spare is Ready behind the long clip; withdrawing it hands it back to the renderer.
      yield* Effect.sleep("2 seconds");
      yield* playout.withdraw(key("spare"));
      assert.strictEqual((yield* spare.outcome)._tag, "Dropped");
      assert.deepStrictEqual(yield* Ref.get(discarded), ["spare"]);
      const urgent = yield* submit("urgent", "urgent");
      yield* urgent.started;
      const stopped = yield* long.outcome;
      assert.deepStrictEqual(
        stopped._tag === "Ended" ? stopped.termination : stopped._tag,
        "stopped",
      );
    }),
  );
});

// The invariants hold across seeded random timing: builds from a quarter of real time to ten
// times it, requests and messages up to two seconds, seams up to half a second.
for (const seed of [1, 2, 3, 4, 5, 6])
  layer(environment({ timing: ReactorTest.Timing.random({ seed }) }))(
    `simulation, seed ${seed}`,
    (it) => {
      it.effect(
        "every item settles once, in lane order, and nothing is sent twice",
        () =>
          Effect.gen(function* () {
            const test = yield* ReactorTest.ReactorTest;
            const { playout, events } = yield* start({ lifetime: "3 minutes" });
            const names = Array.from({ length: 8 }, (_, index) => `s${index}`);
            const handles = yield* Effect.forEach(names, (name, index) =>
              playout.submit({
                key: key(name),
                lane: index % 3 === 0 ? "urgent" : "line",
                request: clip(name, 5 + (index % 3) * 3),
              }),
            );
            const outcomes = yield* Effect.forEach(handles, (handle) => handle.outcome);
            const all = yield* events;
            const byKey = new Map<string, ReadonlyArray<string>>();
            for (const event of all)
              if (event._tag === "AsRun")
                byKey.set(event.event.key, [
                  ...(byKey.get(event.event.key) ?? []),
                  event.event.status._tag,
                ]);
            for (const [item, history] of byKey) {
              // Exactly one terminal status, and it is the last.
              const terminal = history.filter((status) =>
                ["Ended", "Dropped", "Failed", "Unobserved"].includes(status),
              );
              assert.strictEqual(terminal.length, 1, `seed ${seed} ${item}: ${history.join(",")}`);
              assert.strictEqual(history.at(-1), terminal[0]);
            }
            const starts = all.flatMap((event) =>
              event._tag === "AsRun" && event.event.status._tag === "Started"
                ? [event.event.key as string]
                : [],
            );
            const line = starts.filter((name) => Number(name.slice(1)) % 3 !== 0);
            assert.deepStrictEqual(
              line,
              [...line].sort((a, b) => Number(a.slice(1)) - Number(b.slice(1))),
              `seed ${seed}`,
            );
            // A cut never stops the cutter or a clip of its own lane.
            for (const [index, outcome] of outcomes.entries())
              if (outcome._tag === "Ended" && outcome.termination === "stopped")
                assert.notStrictEqual(index % 3, 0, `seed ${seed}: an urgent clip was cut`);
            const enqueues = (yield* test.log).filter(
              (entry) => entry.kind === "command" && entry.name === "enqueue",
            );
            assert.isAtMost(enqueues.length, names.length, `seed ${seed}`);
          }),
        { timeout: 120_000 },
      );
    },
  );

// Cuts under the same wide timing, stops landing up to a second after their acknowledgement:
// urgent items arrive while long lower-lane clips play.
for (const seed of [1, 2, 3, 4, 5, 6])
  layer(environment({ timing: ReactorTest.Timing.random({ seed }) }))(
    `cuts, seed ${seed}`,
    (it) => {
      it.effect(
        "a cut stops one lower-lane clip, once, and its cutter airs next",
        () =>
          Effect.gen(function* () {
            const test = yield* ReactorTest.ReactorTest;
            const { playout, starts } = yield* start();
            const lines = yield* Effect.forEach(["l0", "l1", "l2", "l3"], (name) =>
              playout.submit({ key: key(name), lane: "line", request: clip(name, 14) }),
            );
            yield* lines[0]!.started;
            const urgent = yield* Effect.forEach(["u0", "u1", "u2"], (name) =>
              Effect.andThen(
                Effect.sleep("6 seconds"),
                playout.submit({ key: key(name), lane: "urgent", request: clip(name) }),
              ),
            );
            const outcomes = yield* Effect.forEach([...lines, ...urgent], (handle) =>
              Effect.map(handle.outcome, (status) => [handle.key as string, status] as const),
            );
            const aired = yield* starts;
            const cut = outcomes.flatMap(([name, status]) =>
              status._tag === "Ended" && status.termination === "stopped" ? [name] : [],
            );
            for (const name of cut) {
              assert.isTrue(name.startsWith("l"), `seed ${seed}: ${name} was cut`);
              const next = aired[aired.indexOf(name) + 1];
              assert.isTrue(next?.startsWith("u"), `seed ${seed}: ${name} was cut for ${next}`);
            }
            assert.deepStrictEqual(stopProblems(yield* test.log), [], `seed ${seed}`);
          }),
        { timeout: 120_000 },
      );
    },
  );

// Reactor's docs: "When submitted content violates the policy the session is terminated", and the
// SDK "observes the session leaving the ready state"; a verdict may or may not come first.
layer(hosted)("moderation with a verdict", (it) => {
  it.effect("fails the flagged item at once and airs the rest on the next session", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "Moderate", nth: 1 });
      const { playout, events } = yield* start();
      const flagged = yield* playout.submit({
        key: key("flagged"),
        lane: "line",
        request: clip("x"),
      });
      const fine = yield* playout.submit({ key: key("fine"), lane: "line", request: clip("y") });
      const outcome = yield* flagged.outcome;
      assert.deepStrictEqual(
        outcome._tag === "Failed" ? [outcome._tag, outcome.moderated] : [outcome._tag],
        ["Failed", true],
      );
      assert.strictEqual((yield* fine.outcome)._tag, "Ended");
      const moderated = (yield* events).flatMap((event) =>
        event._tag === "Session" && event.event._tag === "Moderated" ? [event.event.key] : [],
      );
      assert.deepStrictEqual(moderated, [key("flagged")]);
      assert.strictEqual((yield* test.sessions).length, 2);
    }),
  );
});

layer(hosted)("moderation without a verdict", (it) => {
  // Screening ends the session as the flagged clip's build does, and says nothing.
  it.effect(
    "fails a clip lost unbuilt twice in a row, and rebuilds a Ready one until it airs",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        yield* test.inject({ _tag: "Moderate", prompt: "flagged", verdict: false });
        const { playout } = yield* start();
        const submit = (name: string, seconds: number) =>
          playout.submit({ key: key(name), lane: "line", request: clip(name, seconds) });
        // The opener plays while the innocent clip waits Ready and the flagged one builds.
        yield* submit("opener", 15);
        const innocent = yield* submit("innocent", 5);
        const flagged = yield* submit("flagged", 15);
        const failed = yield* flagged.outcome;
        assert.deepStrictEqual(
          failed._tag === "Failed"
            ? [
                failed._tag,
                failed.lost !== undefined,
                failed.reason.includes("before it was built"),
              ]
            : [failed._tag],
          ["Failed", true, true],
        );
        assert.strictEqual((yield* innocent.outcome)._tag, "Ended");
        yield* Effect.sleep("2 minutes");
        // Two sessions ended over the flagged prompt; the third waits for work, and the playout goes on.
        assert.strictEqual((yield* test.sessions).length, 3);
        assert.isTrue(Option.isNone(yield* Effect.timeoutOption(playout.failure, "1 second")));
      }),
  );
});

layer(hosted)("a crash loop", (it) => {
  it.effect("ends the playout when sessions keep dying before anything plays", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "Moderate", verdict: false });
      const { playout } = yield* start({
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index }) => clip(`filler ${String(index)}`),
        },
      });
      const failure = yield* playout.failure;
      assert.strictEqual(failure._tag, "ReactorError");
      assert.strictEqual((yield* test.sessions).length, 3);
    }),
  );
});

layer(hosted)("resume after the owner's token expired", (it) => {
  it.effect("adopts the session with a token bound to it; the owner's stale one is refused", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const coordinator = yield* Coordinator.Coordinator;
      const sessionTokens = coordinator.tokens({
        apiKey: test.apiKey,
        modelName: H3.modelName,
        maxSessionDuration: "10 minutes",
        expiresAfter: "1 minute",
      });
      const grant = yield* sessionTokens.create;
      const recorded = yield* Deferred.make<H3Source.Allocation>();
      // The owner's scope stays open, as a crashed process's would.
      const owned = yield* Scope.make();
      yield* H3Source.open({
        tokens: { create: Effect.succeed(grant), bind: sessionTokens.bind },
        onAllocated: ({ allocation }) => Deferred.succeed(recorded, allocation),
      }).pipe(Scope.provide(owned));
      const allocation = yield* Deferred.await(recorded);
      yield* Effect.sleep("2 minutes");
      const stale = yield* Effect.flip(
        H3Source.resume({ allocation, tokens: { bind: () => Effect.succeed(grant) } }),
      );
      assert.deepStrictEqual(
        [stale.reason._tag, stale.reason._tag === "Http" ? stale.reason.status : undefined],
        ["Http", 401],
      );
      const source = yield* H3Source.resume({ allocation, tokens: sessionTokens });
      assert.strictEqual(source.sessionId, allocation.sessionId);
    }),
  );
});

// What 0.7.0 guaranteed when a scheduler closed or failed, found missing by an independent critique.
layer(hosted)("closing and failing", (it) => {
  // 0.7.0 SchedulerEdits:220.
  it.effect("a batch pending when the playout closes fails its commit as closed", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const scope = yield* Scope.make();
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: yield* tokens("10 minutes") }),
        lanes: [{ name: "line" }],
      }).pipe(Scope.provide(scope));
      const batch = yield* playout.edit([
        {
          _tag: "Submit",
          item: {
            key: key("later"),
            lane: "line",
            request: clip("later"),
            window: { notBefore: "1 hour", firm: false },
          },
        },
      ]);
      yield* Scope.close(scope, Exit.void);
      const committed = yield* batch.committed.pipe(
        Effect.flip,
        Effect.timeoutOption("10 seconds"),
      );
      assert.deepStrictEqual(
        Option.map(committed, (error) => error._tag),
        Option.some("PlayoutClosed"),
      );
    }),
  );

  // 0.7.0 SchedulerFates:182.
  it.effect("a key forgotten past maxHistory and submitted again gets a handle of its own", () =>
    Effect.gen(function* () {
      const { playout } = yield* start({ maxHistory: 1 });
      const first = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      const ended = yield* first.outcome;
      const other = yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      yield* other.outcome;
      const again = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      const outcome = yield* again.outcome;
      assert.isTrue(
        outcome._tag === "Ended" && ended._tag === "Ended" && outcome.at > ended.at,
        `${outcome._tag} again, first ${ended._tag}`,
      );
    }),
  );

  it.effect("a defect in the plan fails the playout and closes its sessions, never hanging", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const { playout } = yield* start({
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index }) => {
            if (index > 0) throw new Error("the filler generator broke");
            return clip("filler 0");
          },
        },
      });
      const failure = yield* playout.failure.pipe(Effect.timeoutOption("2 minutes"));
      assert.isTrue(Option.isSome(failure));
      const refused = yield* playout
        .submit({ key: key("after"), lane: "line", request: clip("after") })
        .pipe(Effect.flip, Effect.timeoutOption("10 seconds"));
      assert.deepStrictEqual(
        Option.map(refused, (error) => error._tag),
        Option.some("PlayoutClosed"),
      );
      yield* eventually(test.sessions, (all) => all.every((value) => value.state === "CLOSED"));
    }),
  );

  it.effect("a withdrawal after the playout closed answers what became of the item", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const scope = yield* Scope.make();
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: yield* tokens("10 minutes") }),
        lanes: [{ name: "line" }],
      }).pipe(Scope.provide(scope));
      const aired = yield* playout.submit({ key: key("aired"), lane: "line", request: clip("a") });
      yield* aired.outcome;
      yield* Scope.close(scope, Exit.void);
      const answers = yield* Effect.forEach(["aired", "never"], (name) =>
        playout.withdraw(key(name)),
      ).pipe(Effect.timeoutOption("10 seconds"));
      assert.deepStrictEqual(answers, Option.some(["already-started", "not-found"]));
    }),
  );
});

// Its faults stay armed for the rest of a block, so it has one of its own.
layer(hosted)("failing after a recovered open", (it) => {
  it.effect("reports why it failed, not an open failure it had recovered from", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "RefuseAllocation", nth: 1 });
      yield* test.inject({ _tag: "Expire", after: Duration.seconds(1) });
      const { playout } = yield* start({
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index }) => clip(`filler ${String(index)}`),
        },
      });
      const failure = yield* playout.failure.pipe(Effect.timeoutOption("5 minutes"));
      assert.isTrue(Option.isSome(failure));
      if (Option.isSome(failure)) assert.notStrictEqual(failure.value._tag, "AcquisitionFailure");
    }),
  );
});
layer(hosted)("media", (it) => {
  it.effect("video goes on after a reader falls behind its bound", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const source = yield* H3Source.open({ tokens: yield* tokens("10 minutes") });
      yield* source.setAutoplay(true);
      for (let index = 0; index < 8; index++)
        yield* source.enqueue(clip(`long ${String(index)}`, 15), { _tag: "Filler", index });
      const frames = yield* Ref.make(0);
      const stalled = yield* Deferred.make<void>();
      // The first frame stalls the reader past the simulated host's 512-frame bound.
      yield* source.video.pipe(
        Stream.runForEach(() =>
          Ref.getAndUpdate(frames, (count) => count + 1).pipe(
            Effect.flatMap((count) =>
              count === 0
                ? Effect.andThen(Effect.sleep("40 seconds"), Deferred.succeed(stalled, undefined))
                : Effect.void,
            ),
          ),
        ),
        Effect.forkScoped,
      );
      yield* Deferred.await(stalled);
      // What the bound held drains first; frames must go on arriving after it.
      yield* Effect.sleep("20 seconds");
      const before = yield* Ref.get(frames);
      yield* Effect.sleep("5 seconds");
      assert.isAbove((yield* Ref.get(frames)) - before, 24);
    }),
  );
});
