/** The playout on the simulated Reactor, from submission to as-run, with the timing each case relies on. */
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, Ref, Stream } from "effect";
import * as Coordinator from "../src/Coordinator.js";
import * as H3 from "../src/H3.js";
import { H3Source, LocalSource, Playout, ReactorError, ReactorTest } from "../src/index.js";
import type { Options } from "../src/Playout.js";
import { environment } from "./fixtures/Simulated.js";

const key = (value: string) => Playout.ItemKey.make(value);
const clip = (prompt: string, seconds = 5): H3.Request => ({ prompt, seconds });

const mint = (maxSessionDuration: Duration.Input) =>
  Effect.gen(function* () {
    const test = yield* ReactorTest.ReactorTest;
    const coordinator = yield* Coordinator.Coordinator;
    return yield* coordinator.mintToken({
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
      open: H3Source.open({ mint: mint(options.lifetime ?? "10 minutes") }),
      lanes: [{ name: "urgent", cut: true }, { name: "line" }, { name: "quiet", conflict: "skip" }],
      ...options,
    });
    const events = yield* Ref.make<ReadonlyArray<Playout.Event>>([]);
    yield* playout.events.pipe(
      Stream.runForEach((event) => Ref.update(events, (all) => [...all, event])),
      Effect.forkScoped,
    );
    // Let the recorder subscribe before anything is submitted.
    yield* Effect.sleep("20 millis");
    // Recorded events trail a resolved handle by a turn of the recorder.
    const recorded = Effect.andThen(Effect.sleep("200 millis"), Ref.get(events));
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

layer(hosted)("the cut's fence", (it) => {
  it.effect("puts back the autoplay the playout asked for, not always on", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const source = yield* H3Source.open({ mint: mint("10 minutes") });
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
        const sessions = (yield* events).flatMap((event) =>
          event._tag === "Session" ? [event.event._tag] : [],
        );
        assert.deepStrictEqual(sessions.slice(0, 3), ["Opened", "Opened", "Switched"]);
        const cleanup = yield* playout.cleanup;
        assert.isTrue(cleanup.sessions >= 1);
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
