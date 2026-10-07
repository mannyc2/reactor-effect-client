/** The playout on the simulated Reactor, from submission to as-run, with the timing each case relies on. */
import { assert, layer } from "@effect/vitest";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  ErrorReporter,
  Exit,
  Fiber,
  Option,
  Random,
  Redacted,
  Ref,
  Result,
  Scope,
  Stream,
  Tracer,
} from "effect";
import * as CoordinatorClient from "../src/CoordinatorClient.js";
import * as H3 from "../src/H3.js";
import {
  FastH3Source,
  H3Source,
  LocalSource,
  Playout,
  Reactor,
  ReactorError,
  ReactorTest,
} from "../src/index.js";
import type { Options } from "../src/Playout.js";
import { commands, environment } from "./fixtures/Simulated.js";

const key = (value: string) => Playout.ItemKey.make(value);
const clip = (prompt: string, seconds = 5): H3.Request => ({ prompt, seconds });

const tokens = (maxSessionDuration: Duration.Input) =>
  Effect.gen(function* () {
    const test = yield* ReactorTest.ReactorTest;
    const coordinator = yield* CoordinatorClient.CoordinatorClient;
    return coordinator.tokens({
      apiKey: test.apiKey,
      modelName: H3.modelName,
      maxSessionDuration,
      expiresAfter: Duration.sum(Duration.fromInputUnsafe(maxSessionDuration), Duration.minutes(5)),
    });
  });

/** A playout on paid-shaped H3 sessions, with its events recorded from the start. */
const start = (
  options: Partial<Options<never>> & { readonly lifetime?: Duration.Input } = {},
  step: Duration.Input = "20 millis",
) =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow(step));
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

layer(hosted)("tracing queued items", (it) => {
  it.effect(
    "keeps each admission's trace through waiting, duplicate submission and renewal",
    () => {
      const spans: Array<Tracer.Span> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      return Effect.gen(function* () {
        const { playout, starts, events } = yield* start({
          lifetime: "18 seconds",
          renewal: { lead: "6 seconds" },
        }).pipe(
          Effect.withSpan(
            "acquisition",
            { root: true, sampled: false },
            { captureStackTrace: false },
          ),
        );
        const first = yield* playout
          .submit({ key: key("first"), lane: "line", request: clip("first") })
          .pipe(Effect.withSpan("caller.first", { root: true }, { captureStackTrace: false }));
        const spec = { key: key("second"), lane: "line", request: clip("second", 10) };
        const second = yield* playout
          .submit(spec)
          .pipe(
            Effect.withSpan(
              "caller.second",
              { root: true, sampled: false },
              { captureStackTrace: false },
            ),
          );
        const duplicate = yield* playout
          .submit(spec)
          .pipe(Effect.withSpan("caller.duplicate", { root: true }, { captureStackTrace: false }));

        assert.strictEqual((yield* first.outcome)._tag, "Ended");
        assert.strictEqual((yield* second.outcome)._tag, "Ended");
        assert.deepStrictEqual(yield* duplicate.outcome, yield* second.outcome);
        assert.deepStrictEqual(yield* starts, ["first", "second"]);
        assert.isAtLeast(
          (yield* events).filter(
            (event) => event._tag === "Session" && event.event._tag === "Opened",
          ).length,
          2,
        );
        const callers = ["caller.first", "caller.second"].map((name) =>
          spans.find((span) => span.name === name),
        );
        for (const caller of callers) assert.isDefined(caller);
        const enqueues = spans.filter((span) => span.name === "H3.enqueue");
        assert.deepStrictEqual(
          enqueues.map((span) => span.traceId),
          callers.map((span) => span?.traceId),
        );
        assert.deepStrictEqual(
          enqueues.map((span) => span.sampled),
          [true, false],
        );
        assert.deepStrictEqual(
          spans
            .filter(
              (span) =>
                span.name === "Session.command" &&
                span.attributes.get("reactor.operation") === "enqueue",
            )
            .map((span) => span.traceId),
          callers.map((span) => span?.traceId),
        );
        // The playout's own work, its opens and autonomous commands, keeps the acquisition's
        // decision not to sample: only the traces of sampled callers are exported.
        const exported = new Set(
          spans
            .filter((span) => span.name.startsWith("caller.") && span.sampled)
            .map((span) => span.traceId),
        );
        assert.deepStrictEqual(
          spans
            .filter((span) => span.sampled && !exported.has(span.traceId))
            .map((span) => span.name),
          [],
        );
      }).pipe(Effect.withTracer(tracer));
    },
  );
});

layer(hosted)("what airs", (it) => {
  it.effect("names the playing item, when its start was seen and its length", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const item = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      const started = yield* item.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag === "Started")
        assert.deepStrictEqual((yield* playout.state).playing, {
          key: key("a"),
          startedAt: started.at,
          seconds: started.seconds,
        });
      yield* item.outcome;
      assert.isNull((yield* eventually(playout.state, (state) => state.playing === null)).playing);
    }),
  );

  it.effect("reports each filler clip's start and end", () =>
    Effect.gen(function* () {
      const { playout, events } = yield* start({
        filler: {
          runway: { floor: "4 seconds", target: "8 seconds" },
          clip: ({ index }) => clip(`idle ${index}`),
        },
      });
      const fillers = Effect.map(events, (all) =>
        all.flatMap((event) => (event._tag === "Filler" ? [event] : [])),
      );
      const seen = yield* eventually(fillers, (all) =>
        all.some((event) => event.index === 1 && event.phase === "Ended"),
      );
      // Once each, though H3 answers every start and end with the facts it held before it.
      assert.deepStrictEqual(
        seen
          .filter((event) => event.index <= 1)
          .map((event) => `${event.phase} ${String(event.index)}`),
        ["Started 0", "Ended 0", "Started 1", "Ended 1"],
      );
      const [first, second] = seen.filter((event) => event.index === 0);
      assert.strictEqual(first?.phase, "Started");
      assert.strictEqual(second?.phase, "Ended");
      assert.isAbove(first?.seconds ?? 0, 5);
      assert.isAbove((second?.at ?? 0) - (first?.at ?? 0), 5_000);
      const playing = yield* eventually(playout.state, (state) => state.playing?.key === "filler");
      assert.strictEqual(
        playing.playing?.startedAt,
        (yield* fillers).findLast((event) => event.phase === "Started")?.at,
      );
    }),
  );

  // H3 builds a request that names no length at its session's, 15 s by default.
  it.effect("asks for the length it plans with when a request names none", () =>
    Effect.gen(function* () {
      const { playout, events } = yield* start({
        filler: {
          runway: { floor: "4 seconds", target: "8 seconds" },
          clip: ({ index }) => ({ prompt: `idle ${index}` }),
        },
      });
      const item = yield* playout.submit({ key: key("a"), lane: "line", request: { prompt: "a" } });
      const started = yield* item.started;
      const filler = yield* eventually(events, (all) =>
        all.some((event) => event._tag === "Filler" && event.phase === "Started"),
      );
      // An item's 5 s and the filler's shortest, 5 s, on H3's grid: 124 frames at 24 fps.
      assert.deepStrictEqual(
        [
          started._tag === "Started" ? started.seconds : started._tag,
          filler.find((event) => event._tag === "Filler")?.seconds,
        ],
        [124 / 24, 124 / 24],
      );
    }),
  );
});

// Its fault stays armed for the rest of a block, so it has one of its own.
layer(hosted)("a dropped connection", (it) => {
  it.effect("reports the reconnect and how long it took, and airs on", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "Disconnect", nth: 1, after: Duration.seconds(8) });
      const { playout, events } = yield* start();
      const handles = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      for (const handle of handles) yield* handle.outcome;
      const sessions = (yield* events).flatMap((event) =>
        event._tag === "Session" ? [event.event] : [],
      );
      assert.deepStrictEqual(
        sessions.map((event) => event._tag),
        ["Opened", "Reconnecting", "Reconnected"],
      );
      const [opened, reconnecting, reconnected] = sessions;
      assert.deepStrictEqual(reconnecting, {
        _tag: "Reconnecting",
        sessionId: opened?._tag === "Opened" ? opened.sessionId : "",
      });
      assert.isAbove(reconnected?._tag === "Reconnected" ? reconnected.afterMillis : 0, 0);
      assert.strictEqual((yield* handles[2]?.outcome ?? Effect.die("no third item"))._tag, "Ended");
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

  it.effect("an insert after an At item takes its time, but not its staleness", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const first = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      yield* first.started;
      // Due now, but skipped once a second late: it can only start when a ends, 10 s on.
      const stale = yield* playout.submit({
        key: key("line"),
        lane: "line",
        request: clip("line"),
        start: {
          _tag: "At",
          time: yield* Clock.currentTimeMillis,
          late: { _tag: "skipIfLaterThan", by: "1 second" },
        },
      });
      const after = yield* playout.insert({
        key: key("after"),
        request: clip("after"),
        after: key("line"),
      });
      assert.deepStrictEqual(yield* stale.outcome, { _tag: "Dropped", reason: "late" });
      assert.strictEqual((yield* after.outcome)._tag, "Ended");
      assert.deepStrictEqual(yield* starts, ["a", "after"]);
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

  it.effect("refuses a request outside H3's limits, and sends nothing for it", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const sent = (yield* commands("enqueue")).length;
      const refusals = yield* Effect.forEach(
        [
          playout.submit({ key: key("long"), lane: "line", request: clip("long", 20) }),
          playout.submit({
            key: key("picture"),
            lane: "line",
            request: { prompt: "a", references: [{ _tag: "Bytes", bytes: new Uint8Array(3) }] },
          }),
          Effect.flatMap(
            playout.edit([
              { _tag: "Submit", item: { key: key("fine"), lane: "line", request: clip("fine") } },
              {
                _tag: "Submit",
                item: { key: key("blank"), lane: "line", request: { prompt: " " } },
              },
            ]),
            (batch) => batch.committed,
          ),
        ],
        (refused) => Effect.map(Effect.flip(refused), (error) => error._tag),
      );
      assert.deepStrictEqual(refusals, ["InvalidItem", "InvalidItem", "InvalidItem"]);
      yield* Effect.sleep("10 seconds");
      assert.strictEqual((yield* commands("enqueue")).length, sent);
    }),
  );

  // Metadata within H3's bound on its own can pass it once the playout's key and H3's own
  // identity wrap it, each escaping its quotes again: the enqueue would refuse it unsent.
  it.effect("refuses metadata that fits H3's bound only before it is wrapped", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const sent = (yield* commands("enqueue")).length;
      const refused = yield* Effect.flip(
        playout.submit({
          key: key("quoted"),
          lane: "line",
          request: { ...clip("quoted"), metadata: '"'.repeat(600) },
        }),
      );
      assert.strictEqual(refused._tag, "InvalidItem");
      const fits = yield* playout.submit({
        key: key("plain"),
        lane: "line",
        request: { ...clip("plain"), metadata: "x".repeat(1_700) },
      });
      assert.strictEqual((yield* fits.outcome)._tag, "Ended");
      assert.strictEqual((yield* commands("enqueue")).length, sent + 1);
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

  // A caller retrying an insert whose first attempt landed gets that insert, not a refusal.
  it.effect("answers an insert made again once its anchor has aired with its own handle", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      const insert = { key: key("u"), request: clip("u"), after: key("a") };
      const once = yield* playout.insert(insert);
      yield* once.started;
      const again = yield* playout.insert(insert);
      assert.strictEqual(again.key, once.key);
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

  // Adopted, the clip on air has no known length, so no follower's fence can go up before it ends.
  it.effect("builds on while a follower waits out a clip of unknown length", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const sessionTokens = yield* tokens("10 minutes");
      const recorded = yield* Deferred.make<H3Source.Allocation>();
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
      const allocation = yield* Deferred.await(recorded);
      const playout = yield* Playout.make({
        open: H3Source.resume({ allocation, tokens: sessionTokens }),
        lanes: [{ name: "line" }],
      });
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* playout.events.pipe(
        Stream.runForEach((event) =>
          event._tag === "AsRun"
            ? Ref.update(events, (all) => [...all, `${event.event.key} ${event.event.status._tag}`])
            : Effect.void,
        ),
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* playout.submit({ key: key("x"), lane: "line", request: clip("x") });
      const u = yield* playout.insert({
        key: key("u"),
        request: clip("u"),
        after: key("x"),
        follows: { _tag: "Item", key: key("x") },
      });
      const y = yield* playout.submit({ key: key("y"), lane: "line", request: clip("y") });
      assert.strictEqual((yield* y.outcome)._tag, "Ended");
      assert.strictEqual((yield* u.outcome)._tag, "Ended");
      const all = yield* Ref.get(events);
      assert.isBelow(all.indexOf("y Ready"), all.indexOf("x Started"));
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

  // A projected time is seldom a whole nanosecond, as the virtual clock counts: the playout waits
  // until the clock reaches it, rather than just short of it and on until the clock steps again.
  it.effect("drops a firm item as its projection reaches its startBy, not at a later step", () =>
    Effect.gen(function* () {
      const { playout, events } = yield* start();
      // Three builds measured, so the plan projects each item's build from the median.
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      for (const handle of measured) yield* handle.outcome;
      const median = (yield* playout.state).estimates.build?.median ?? 0;
      assert.isAbove(median, 0);
      // The plan counts a build per second of the length H3 builds, and each build measured asked
      // for 5 s.
      const perBuilt = (median * 5) / H3Source.model.builtSeconds(5);
      // Nothing airs, and none may build before 5 s: each would miss its startBy from 8 s less
      // its own build on, a time for each length. Each goes alone, on the lane that cuts, which
      // airs a clip once it is Ready, so only that projection decides it.
      const lengths = [8, 9, 10, 11, 12, 13];
      for (const seconds of lengths) {
        const firm = yield* playout.submit({
          key: key(`firm ${String(seconds)}`),
          lane: "urgent",
          request: clip(`firm ${String(seconds)}`, seconds),
          window: { notBefore: "5 seconds", startBy: "8 seconds", firm: true },
        });
        assert.deepStrictEqual(yield* firm.outcome, { _tag: "Dropped", reason: "late" });
      }
      const all = yield* events;
      const at = (name: string, status: string) =>
        all.flatMap((event) =>
          event._tag === "AsRun" && event.event.key === name && event.event.status._tag === status
            ? [event.event.at]
            : [],
        )[0] ?? Number.NaN;
      const late = lengths.map((seconds) => {
        const name = `firm ${String(seconds)}`;
        const build = H3Source.model.builtSeconds(seconds) * perBuilt * 1_000;
        return at(name, "Dropped") - at(name, "Accepted") - (8_000 - build);
      });
      // The virtual clock's wall and monotonic readings part by less than a microsecond here.
      assert.deepStrictEqual(
        late.map((ms) => Math.abs(ms) < 0.01),
        lengths.map(() => true),
        `dropped late by ${late.map((ms) => ms.toFixed(6)).join(", ")} ms`,
      );
    }),
  );

  it.effect("keeps a firm item due within a clip's length of that clip's end", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      // Three builds measured, so the plan projects each item's build from the median.
      const [, , last] = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      yield* last?.started ?? Effect.die("no third item");
      // The last clip ends 5.2 s from now, with nothing behind it. The firm item may build from
      // 5.5 s, is Ready about 2.2 s later and airs at once, well before its 9 s deadline, which
      // falls within that clip's length of its end.
      const firm = yield* playout.submit({
        key: key("firm"),
        lane: "line",
        request: clip("firm"),
        window: { notBefore: "5500 millis", startBy: "9 seconds", firm: true },
      });
      const started = yield* firm.started;
      assert.deepStrictEqual(
        started._tag === "Started" ? started.lateByMillis : started,
        undefined,
      );
    }),
  );

  it.effect(
    "counts the clip H3 holds armed in its seam once, and keeps a firm item due after it",
    () =>
      Effect.gen(function* () {
        const { playout } = yield* start({
          ...Playout.lineup({
            runway: { floor: "15 seconds", target: "15 seconds" },
            clip: ({ index }) => clip(`idle ${index}`),
          }),
        });
        yield* Effect.sleep("30 seconds");
        const a = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
        const started = yield* a.started;
        if (started._tag !== "Started") return yield* Effect.die("a never started");
        yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
        const end = started.at + started.seconds * 1000;
        yield* Effect.sleep(Duration.millis(end - 1000 - (yield* Clock.currentTimeMillis)));
        // Built once b airs, it starts about 5 s before its deadline.
        const now = yield* Clock.currentTimeMillis;
        const firm = yield* playout.submit({
          key: key("firm"),
          lane: "line",
          request: clip("firm"),
          window: {
            notBefore: Duration.millis(end + 300 - now),
            startBy: Duration.millis(end + 7000 - now),
            firm: true,
          },
        });
        // x's enqueue lands in b's seam, and the state read after it names b playing while H3
        // still lists it Ready.
        yield* Effect.sleep(Duration.millis(end - 20 - (yield* Clock.currentTimeMillis)));
        yield* playout.submit({ key: key("x"), lane: "line", request: clip("x") });
        assert.strictEqual((yield* firm.started)._tag, "Started");
      }),
  );
});

/** Hosted timing at the hosted median seam, which a Ready clip on a dark air starts after. */
const seamed = environment({
  timing: ReactorTest.Timing.fixed({
    buildSpeed: 2.4,
    seam: "40 millis",
    http: "40 millis",
    channel: "20 millis",
  }),
});

layer(seamed)("admission", (it) => {
  // Viewers' prompts reaching a channel at once. Each clip ahead takes its own length of air, and
  // the first, not Ready a margin before the clip on air ends, leaves that boundary to the filler:
  // the fourth would start over 2 s past its startBy.
  it.effect("airs every firm item it admits, refusing those that would start too late", () =>
    Effect.gen(function* () {
      const { playout } = yield* start({
        filler: {
          runway: { floor: "8 seconds", target: "16 seconds" },
          clip: ({ index }) => clip(`idle ${index}`, 8),
        },
      });
      // Filler on air, its builds measured.
      yield* Effect.sleep("40 seconds");
      const submitted = yield* Effect.forEach(
        Array.from({ length: 8 }, (_, index) => index),
        (index) =>
          Effect.result(
            playout.submit({
              key: key(`viewer ${index}`),
              lane: "line",
              request: clip(`viewer ${index}`, 8),
              window: { startBy: "34 seconds", firm: true },
            }),
          ),
      );
      const outcomes = yield* Effect.forEach(submitted, (result) =>
        Result.isSuccess(result)
          ? Effect.map(result.success.outcome, (outcome) => outcome._tag)
          : Effect.succeed(result.failure._tag),
      );
      assert.deepStrictEqual(outcomes, [
        ...Array.from({ length: 3 }, () => "Ended"),
        ...Array.from({ length: 5 }, () => "WouldMissDeadline"),
      ]);
    }),
  );
});

layer(seamed)("follows", (it) => {
  it.effect("drops an item as displaced once it can't be Ready by the end of what it follows", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      // Three builds measured, so the plan projects a build from the median.
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      yield* measured[2]!.started;
      // About a second of c is left, and a 5 s clip takes over 2 s to build.
      yield* Effect.sleep("4 seconds");
      const late = yield* playout.insert({
        key: key("late"),
        request: clip("late"),
        after: key("c"),
        follows: { _tag: "Item", key: key("c") },
      });
      const next = yield* playout.submit({ key: key("d"), lane: "line", request: clip("d") });
      assert.deepStrictEqual(yield* late.outcome, { _tag: "Dropped", reason: "displaced" });
      yield* next.started;
      assert.deepStrictEqual(yield* starts, ["a", "b", "c", "d"]);
    }),
  );

  // Ready with nothing on air, it would start after one seam, before its removal could land.
  it.effect("keeps an item that follows a clip unbuilt on a dark air, until that clip airs", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      yield* measured[2]!.outcome;
      yield* playout.submit({
        key: key("x"),
        lane: "line",
        request: clip("x"),
        start: { _tag: "Manual" },
      });
      const after = yield* playout.submit({
        key: key("after"),
        lane: "line",
        request: clip("after"),
        start: { _tag: "Asap" },
        follows: { _tag: "Item", key: key("x") },
      });
      yield* Effect.sleep("6 seconds");
      yield* playout.release(key("x"));
      yield* after.outcome;
      assert.deepStrictEqual(yield* starts, ["a", "b", "c", "x", "after"]);
    }),
  );

  it.effect("never advises a submission that a lane would refuse", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const playing = yield* playout.submit({
        key: key("c"),
        lane: "line",
        request: clip("c", 15),
      });
      yield* playing.started;
      // x cuts in once Ready: an insert after it would take its cutting lane.
      yield* playout.submit({ key: key("x"), lane: "urgent", request: clip("x") });
      const placement = yield* playout.place({ key: key("u") });
      if (placement === null) return;
      const spec = { key: key("u"), request: clip("u"), follows: placement.after };
      yield* placement.anchor === "next"
        ? playout.submit({ ...spec, lane: "quiet", start: { _tag: "Asap" } })
        : playout.insert({ ...spec, after: placement.anchor });
    }),
  );

  it.effect("answers no boundary its clip could not be Ready a margin before", () =>
    Effect.gen(function* () {
      const { playout } = yield* start();
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      yield* measured[2]!.outcome;
      const line = yield* playout.submit({
        key: key("line"),
        lane: "line",
        request: clip("line", 10),
      });
      yield* line.started;
      // 2 s of it is left with nothing queued, and a 5 s clip takes over 2 s to build.
      yield* Effect.sleep("8 seconds");
      assert.isNull(yield* playout.place({ key: key("u") }));
    }),
  );

  it.effect("before builds are measured, builds an item once the clip it follows is Ready", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const now = yield* Clock.currentTimeMillis;
      yield* playout.submit({ key: key("p"), lane: "line", request: clip("p") });
      yield* playout.submit({
        key: key("x"),
        lane: "line",
        request: clip("x", 15),
        start: { _tag: "At", time: now + 40_000, late: { _tag: "nextBoundary" } },
      });
      yield* Effect.sleep("3 seconds");
      // Its build could outlast the 2 s of p left.
      const after = yield* playout.submit({
        key: key("after"),
        lane: "line",
        request: clip("after", 15),
        follows: { _tag: "Item", key: key("x") },
      });
      yield* after.outcome;
      assert.deepStrictEqual(yield* starts, ["p", "x", "after"]);
    }),
  );

  it.effect("holds an insert after the item it follows while that item's enqueue is unknown", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const test = yield* ReactorTest.ReactorTest;
      const a = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      yield* a.started;
      // The reply to x's enqueue is lost, and x never made.
      yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 1, applied: false });
      yield* playout.submit({ key: key("x"), lane: "line", request: clip("x") });
      yield* Effect.sleep("500 millis");
      const after = yield* playout.insert({
        key: key("after"),
        request: clip("after"),
        after: key("x"),
        follows: { _tag: "Item", key: key("x") },
      });
      assert.deepStrictEqual(yield* after.outcome, { _tag: "Dropped", reason: "displaced" });
      assert.deepStrictEqual(yield* starts, ["a"]);
    }),
  );
});

// H3 holds the next clip armed through its seam and reports it playing; autoplay turned off then
// would leave it unstarted.
layer(hosted)("follows inside a boundary", (it) => {
  it.effect("raises its fence only while a clip plays, so the armed clip still starts", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start();
      const a = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      yield* a.started;
      yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      const c = yield* playout.submit({ key: key("c"), lane: "line", request: clip("c") });
      yield* a.outcome;
      // Its build is picked as a's end is seen, while b waits out its seam.
      yield* playout.insert({
        key: key("v"),
        request: clip("v"),
        after: key("b"),
        follows: { _tag: "Item", key: key("b") },
      });
      const ended = yield* c.outcome.pipe(Effect.timeoutOption("30 seconds"));
      assert.strictEqual(Option.getOrUndefined(ended)?._tag, "Ended");
      assert.deepStrictEqual(yield* starts, ["a", "b", "v", "c"]);
      // Autoplay started every clip: none was held armed, nor played while armed at v's boundary.
      assert.strictEqual((yield* commands("play")).length, 0);
    }),
  );
});

// Its own block: the scene's timing within a seam would move with the clock a block's tests share.
layer(hosted)("follows inside a boundary, read mid-seam", (it) => {
  it.effect("never leaves unstarted a clip a state read names playing in its seam", () =>
    Effect.gen(function* () {
      const { playout, starts } = yield* start({}, "10 millis");
      const a = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      const started = yield* a.started;
      if (started._tag !== "Started") return yield* Effect.die("a never started");
      yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      const c = yield* playout.submit({ key: key("c"), lane: "line", request: clip("c") });
      yield* playout.submit({ key: key("z"), lane: "line", request: clip("z") });
      // z's removal lands in b's seam, and the state read after it names b playing with all of
      // its length left while H3 still holds b armed.
      const end = started.at + started.seconds * 1000;
      yield* Effect.sleep(Duration.millis(end - 40 - (yield* Clock.currentTimeMillis)));
      yield* Effect.forkScoped(playout.withdraw(key("z")));
      yield* a.outcome;
      yield* playout.insert({
        key: key("v"),
        request: clip("v"),
        after: key("b"),
        follows: { _tag: "Item", key: key("b") },
      });
      const ended = yield* c.outcome.pipe(Effect.timeoutOption("30 seconds"));
      assert.strictEqual(Option.getOrUndefined(ended)?._tag, "Ended");
      assert.deepStrictEqual(yield* starts, ["a", "b", "v", "c"]);
    }),
  );
});

// Hosted runs measured seams up to 169 ms. Its own block, for the same reason as the one above.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      seam: "150 millis",
      http: "40 millis",
      channel: "20 millis",
    }),
  }),
)("follows inside a long seam", (it) => {
  it.effect("raises no fence on a clip a state read names playing before its start is seen", () =>
    Effect.gen(function* () {
      const { playout } = yield* start({}, "10 millis");
      const a = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a", 10) });
      const started = yield* a.started;
      if (started._tag !== "Started") return yield* Effect.die("a never started");
      yield* playout.submit({ key: key("b"), lane: "line", request: clip("b") });
      const c = yield* playout.submit({ key: key("c"), lane: "line", request: clip("c") });
      yield* playout.submit({ key: key("z"), lane: "line", request: clip("z") });
      // z's removal and v's enqueue both land in b's seam, and the state reads after them name b
      // playing; H3 says nothing more until v is Ready.
      const end = started.at + started.seconds * 1000;
      yield* Effect.sleep(Duration.millis(end - 20 - (yield* Clock.currentTimeMillis)));
      yield* Effect.forkScoped(playout.withdraw(key("z")));
      yield* a.outcome;
      yield* playout.insert({
        key: key("v"),
        request: clip("v"),
        after: key("b"),
        follows: { _tag: "Item", key: key("b") },
      });
      yield* c.outcome;
      const log = yield* (yield* ReactorTest.ReactorTest).log;
      const messages = (name: string) =>
        log.filter((entry) => entry.kind === "message" && entry.name === name);
      const ended = messages("clip_finished")[0]?.at ?? 0;
      const next = messages("clip_started")[1]?.at ?? Infinity;
      assert.isBelow(next - ended, 1000, `b started ${next - ended} ms after a ended`);
    }),
  );
});

// Its own block: the session's early end stays armed for the rest of a block.
layer(hosted)("follows across a lost session", (it) => {
  it.effect("drops a follower whose clip is lost with its session, rather than air it later", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      // The session ends 16 s after it becomes active, while x plays.
      yield* test.inject({ _tag: "Expire", after: Duration.seconds(16) });
      const { playout, starts, statuses } = yield* start();
      const p = yield* playout.submit({ key: key("p"), lane: "line", request: clip("p", 10) });
      yield* p.started;
      const x = yield* playout.submit({ key: key("x"), lane: "line", request: clip("x") });
      const u = yield* playout.insert({
        key: key("u"),
        request: clip("u", 15),
        after: key("x"),
        follows: { _tag: "Item", key: key("x") },
      });
      const y = yield* playout.submit({ key: key("y"), lane: "line", request: clip("y") });
      assert.strictEqual((yield* x.outcome)._tag, "Failed");
      assert.strictEqual((yield* y.outcome)._tag, "Ended");
      assert.deepStrictEqual(yield* u.outcome, { _tag: "Dropped", reason: "displaced" });
      // Ready on the lost session, it is dropped with x rather than carried and built again.
      assert.deepStrictEqual(yield* statuses("u"), ["Accepted", "Building", "Ready", "Dropped"]);
      assert.deepStrictEqual(yield* starts, ["p", "x", "y"]);
    }),
  );
});

/** A length on H3's grid: 124 frames at 24 fps, then steps of 17 frames. */
const grid = (steps: number) => (124 + 17 * steps) / 24;

type Aired =
  | { readonly _tag: "Item"; readonly key: string }
  | { readonly _tag: "Filler"; readonly index: number };

const sameClip = (tag: Playout.ClipTag | null, aired: Aired | undefined): boolean => {
  if (tag === null || aired === undefined) return tag === null && aired === undefined;
  return tag._tag === "Item"
    ? aired._tag === "Item" && aired.key === tag.key
    : aired._tag === "Filler" && aired.index === tag.index;
};

/**
 * Lines of one to three beats on H3's grid, each `At` its frame and the first skipped once 20 s
 * late, over `floor` of filler; then `place` is asked once and its clip submitted as it says. Nothing
 * else is submitted after the call, so the plan knows the whole future: the clip must start right
 * after `after`, be followed by `before` (any filler clip for a filler one once a replacement
 * opens), and start within `toleranceMs` of `startsAt`. Returns what went wrong.
 */
const forecast = (
  seed: number,
  lifetime: Duration.Input,
  toleranceMs: number,
  floor: Duration.Input = "15 seconds",
  step: Duration.Input = "10 millis",
) =>
  Effect.gen(function* () {
    const draws = yield* Effect.replicateEffect(Random.next, 64).pipe(Random.withSeed(seed));
    const draw = () => draws.pop() ?? 0;
    yield* Effect.forkScoped(ReactorTest.flow(step));
    const test = yield* ReactorTest.ReactorTest;
    yield* test.inject({ _tag: "Video", video: "absent" });
    yield* test.inject({ _tag: "NoAudio" });
    const playout = yield* Playout.make({
      open: H3Source.open({ tokens: yield* tokens(lifetime) }),
      ...Playout.lineup({
        runway: { floor, target: floor },
        clip: ({ index }) => clip(`idle ${index}`, grid(0)),
        lengths: { min: grid(0), max: grid(0) },
      }),
      renewal: { lead: "40 seconds", grace: "100 millis" },
    });
    const aired = yield* Ref.make<ReadonlyArray<Aired>>([]);
    const renewals = yield* Ref.make(0);
    yield* playout.events.pipe(
      Stream.runForEach((event) => {
        if (
          event._tag === "Session" &&
          (event.event._tag === "Opened" || event.event._tag === "Switched")
        )
          return Ref.update(renewals, (count) => count + 1);
        if (event._tag === "Filler" && event.phase === "Started")
          return Ref.update(aired, (all): ReadonlyArray<Aired> => [
            ...all,
            { _tag: "Filler", index: event.index },
          ]);
        if (event._tag === "AsRun" && event.event.status._tag === "Started") {
          const started = event.event.key;
          return Ref.update(aired, (all): ReadonlyArray<Aired> => [
            ...all,
            { _tag: "Item", key: started },
          ]);
        }
        return Effect.void;
      }),
      Effect.forkScoped({ startImmediately: true }),
    );
    let anchor = 0;
    const lines = 3 + Math.floor(draw() * 4);
    for (let line = 0; line < lines; line++) {
      const now = yield* Clock.currentTimeMillis;
      const frame = now - 1000 - Math.floor(draw() * 7000);
      anchor = Math.max(frame, anchor);
      const beats = 1 + Math.floor(draw() * 3);
      for (let beat = 0; beat < beats; beat++)
        yield* playout.submit({
          key: key(`L${line}#${beat}`),
          lane: "line",
          request: clip(`L${line}#${beat}`, grid(Math.floor(draw() * 9))),
          start: {
            _tag: "At",
            time: anchor,
            late:
              beat === 0
                ? { _tag: "skipIfLaterThan", by: Duration.millis(20_000 - (anchor - frame)) }
                : { _tag: "nextBoundary" },
          },
        });
      // Now and then a pause long enough for filler to take the air.
      const pause = (draw() < 0.2 ? 15_000 : 2000) + Math.floor(draw() * 8000);
      yield* Effect.sleep(Duration.millis(pause));
    }
    const writing = Duration.millis(Math.floor(draw() * 8000));
    const renewed = yield* Ref.get(renewals);
    const placement = yield* playout.place({ key: key("u"), submitIn: writing });
    if (placement === null) return [`seed ${seed}: no placement`];
    yield* Effect.sleep(writing);
    const spec = { key: key("u"), request: clip("u"), follows: placement.after };
    const u =
      placement.anchor === "next"
        ? yield* playout.submit({ ...spec, lane: "line", start: { _tag: "Asap" } })
        : yield* playout.insert({ ...spec, after: placement.anchor });
    const started = yield* u.started;
    if (started._tag !== "Started") {
      const outcome = yield* u.outcome;
      return outcome._tag === "Dropped" && outcome.reason === "displaced"
        ? [`seed ${seed}: displaced`]
        : [`seed ${seed}: ${outcome._tag}`];
    }
    // Long enough for the clip after it to start.
    yield* Effect.sleep("8 seconds");
    const all = yield* Ref.get(aired);
    const index = all.findIndex((entry) => entry._tag === "Item" && entry.key === "u");
    const errorMs = started.at - placement.startsAt;
    // Once a replacement opens, a filler index goes to whichever session asks for it first, so the
    // filler clip after the placed one may carry another.
    const before = placement.before;
    const next = all[index + 1];
    const renewing = (yield* Ref.get(renewals)) > renewed;
    return [
      ...(sameClip(placement.after, all[index - 1]) ? [] : [`seed ${seed}: after another clip`]),
      ...(before === null ||
      sameClip(before, next) ||
      (renewing && before._tag === "Filler" && next?._tag === "Filler")
        ? []
        : [`seed ${seed}: before another clip`]),
      ...(Math.abs(errorMs) < toleranceMs
        ? []
        : [`seed ${seed}: started ${errorMs.toFixed(0)} ms off`]),
    ];
  }).pipe(Effect.scoped);

// The fixed seam is the walk's own; the error includes event latency and a guarded start's
// provider command round trip.
layer(seamed, { timeout: "10 minutes" })("place", (it) => {
  it.effect(
    "with nothing submitted after it, the clip airs where and when it said",
    () =>
      Effect.gen(function* () {
        const problems: Array<string> = [];
        // Seeds 21 and 169 place a clip whose slack a command queued ahead of it, or the fence
        // before its build, would take.
        for (const seed of [1, 2, 3, 4, 5, 6, 7, 8, 21, 169])
          problems.push(...(yield* forecast(seed, "30 minutes", 100)));
        // With a 5 s floor, the filler clip it follows may not be built yet.
        for (let seed = 1; seed <= 16; seed++)
          problems.push(...(yield* forecast(seed, "30 minutes", 100, "5 seconds")));
        assert.deepStrictEqual(problems, []);
      }),
    { timeout: 60_000 },
  );

  // Every call falls near a renewal. The replacement's opening and its first start are only
  // projected: these seeds start within 30 ms of their forecast, and 399 of seeds 1-400 within
  // 0.1 s, so the tolerance still sees the walk forget the switch's grace.
  it.effect(
    "across a renewal, the clip airs where it said",
    () =>
      Effect.gen(function* () {
        const problems: Array<string> = [];
        // Seed 13 places a clip whose slack the fence before its build would take, seed 49 one
        // after a clip whose guarded start is under way as its deadline passes, and seed 343 one
        // that the replacement could not air before its cap at the first boundary it could make.
        for (const seed of [1, 2, 3, 4, 5, 6, 7, 8, 13, 49, 343])
          problems.push(...(yield* forecast(seed, "75 seconds", 100, "15 seconds", "20 millis")));
        assert.deepStrictEqual(problems, []);
      }),
    { timeout: 60_000 },
  );
});

// Sessions of 50 s renewed 40 s before their cap: a replacement has little of its cap left by
// the time it takes the air.
layer(seamed)("place across short sessions", (it) => {
  it.effect("answers no boundary the replacement could not air before its cap", () =>
    Effect.gen(function* () {
      const { playout } = yield* start(
        {
          lifetime: "50 seconds",
          renewal: { lead: "40 seconds", grace: "100 millis" },
          ...Playout.lineup({
            runway: { floor: "15 seconds", target: "15 seconds" },
            clip: ({ index }) => clip(`idle ${index}`, grid(0)),
            lengths: { min: grid(0), max: grid(0) },
          }),
        },
        "10 millis",
      );
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      yield* measured[2]!.outcome;
      const line = yield* playout.submit({
        key: key("line"),
        lane: "line",
        request: clip("line", 10),
      });
      yield* playout.submit({ key: key("next"), lane: "line", request: clip("next") });
      yield* line.started;
      // Built now on the replacement, the clip would air past that replacement's cap, and place
      // projects no renewal after it.
      yield* Effect.sleep("6 seconds");
      assert.isNull(yield* playout.place({ key: key("u") }));
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

  it.effect("a lost enqueue reply holds up neither the items behind it nor their session", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 3 });
      const { playout, events } = yield* start();
      const names = Array.from({ length: 8 }, (_, index) => `n${index}`);
      const handles = yield* Effect.forEach(names, (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      // n2's enqueue is the lost one: the rest air behind it on the same session.
      const rest = [...handles.slice(0, 2), ...handles.slice(3)];
      for (const handle of rest) assert.strictEqual((yield* handle.outcome)._tag, "Ended");
      const times = new Map<string, number>();
      for (const event of yield* events)
        if (event._tag === "AsRun" && event.event.status._tag !== "Accepted")
          times.set(`${event.event.key}:${event.event.status._tag}`, event.event.at);
      // The wait is the provider's own: its reply timeout, then its reconcile window.
      const gap = times.get("n3:Started")! - times.get("n1:Ended")!;
      assert.isBelow(gap, 20_000, `n3 aired ${gap} ms after n1`);
      const opened = (yield* events).filter(
        (event) => event._tag === "Session" && event.event._tag === "Opened",
      );
      assert.strictEqual(opened.length, 1);
    }),
  );

  // Hosted H3 lists a new clip before it replies, so when the reply is lost that listing
  // decides the enqueue, and the next one goes out without waiting out the reply.
  it.effect("an enqueue that landed but lost its reply airs in turn, with no gap after it", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 3, applied: true });
      const { playout, events } = yield* start();
      const names = Array.from({ length: 6 }, (_, index) => `n${index}`);
      const handles = yield* Effect.forEach(names, (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      for (const handle of handles) assert.strictEqual((yield* handle.outcome)._tag, "Ended");
      const times = new Map<string, number>();
      for (const event of yield* events)
        if (event._tag === "AsRun")
          times.set(`${event.event.key}:${event.event.status._tag}`, event.event.at);
      const gaps = names
        .slice(1)
        .map(
          (name, index) =>
            (times.get(`${name}:Started`) ?? Infinity) -
            (times.get(`${names[index]}:Ended`) ?? -Infinity),
        );
      assert.isBelow(Math.max(...gaps), 1_000, `gaps between clips: ${gaps.join(", ")} ms`);
    }),
  );
});

// Its fault stays armed for the rest of a block, so it has one of its own.
layer(hosted)("command lanes", (it) => {
  it.effect("an enqueue whose command was lost holds up no other session's commands", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      // The first enqueue's command is lost: it holds its session's commands for the provider's
      // reply timeout and then its reconcile window, about 20 s.
      yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 1 });
      const { playout, events, statuses } = yield* start({
        lifetime: "60 seconds",
        renewal: { lead: "55 seconds" },
      });
      yield* playout.submit({ key: key("lost"), lane: "line", request: clip("lost") });
      // The replacement opens at the lead, about 5 s in, and new work goes to it.
      yield* eventually(
        events,
        (all) =>
          all.filter((event) => event._tag === "Session" && event.event._tag === "Opened")
            .length === 2,
      );
      yield* playout.submit({ key: key("next"), lane: "line", request: clip("next") });
      yield* eventually(statuses("next"), (all) => all.includes("Ready"));
      const times = new Map<string, number>();
      for (const event of yield* events)
        if (event._tag === "AsRun" && event.event.key === "next")
          times.set(event.event.status._tag, event.event.at);
      const waited = times.get("Ready")! - times.get("Accepted")!;
      assert.isBelow(waited, 5_000, `next was Ready ${waited} ms after it was submitted`);
    }),
  );

  // A filler enqueue whose command is lost holds its own session's filler for about 20 s, and no
  // other session's.
  it.effect(
    "a filler enqueue whose command was lost holds up no other session's filler",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        const { events } = yield* start({
          lifetime: "60 seconds",
          renewal: { lead: "40 seconds" },
          filler: {
            runway: { floor: "20 seconds", target: "30 seconds" },
            clip: ({ index, seconds }) => clip(`filler ${index}`, seconds),
          },
        });
        yield* Effect.sleep("19 seconds");
        // s1's next enqueue, a filler clip's, is lost, as the replacement opens at the lead.
        yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 1 });
        yield* Effect.sleep("30 seconds");
        const log = yield* test.log;
        const [first, second] = (yield* events).flatMap((event) =>
          event._tag === "Session" && event.event._tag === "Opened" ? [event.event.sessionId] : [],
        );
        // The layer's log holds the tests before this one too.
        assert.isTrue(
          log.some(
            (entry) =>
              entry.sessionId === first && entry.kind === "command" && entry.dropped !== undefined,
          ),
        );
        const created = log.find((entry) => entry.sessionId === second)?.at ?? NaN;
        const filled = log.find(
          (entry) =>
            entry.sessionId === second && entry.kind === "command" && entry.name === "enqueue",
        )?.at;
        assert.isBelow((filled ?? Infinity) - created, 5_000, `s2 opened at ${created} ms`);
      }),
    { timeout: 60_000 },
  );
});

layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "40 millis", channel: "20 millis" }),
    sessionsPerMinute: 1,
  }),
)("refusals", (it) => {
  it.effect("asks for a session again only after the wait a refusal names", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reactor = yield* Reactor.Reactor;
      const grant = yield* tokens("10 minutes");
      // Three sessions back to back use the account's burst: the next is refused for a minute.
      for (let index = 0; index < 3; index++)
        yield* reactor.create({ model: H3.modelName, tokens: grant });
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: grant }),
        lanes: [{ name: "line" }],
      });
      const handle = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      const outcome = yield* Effect.raceFirst(
        handle.outcome,
        Effect.flatMap(playout.failure, (failure) => Effect.die(failure)),
      );
      assert.strictEqual(outcome._tag, "Ended");
    }),
  );
});

layer(hosted)("renewal", (it) => {
  it.effect(
    "opens a replacement before the lifetime ends and switches at a boundary, in order",
    () =>
      Effect.gen(function* () {
        const { playout, starts, events } = yield* start(
          {
            lifetime: "90 seconds",
            renewal: { lead: "40 seconds" },
          },
          "50 millis",
        );
        const handles = yield* Effect.forEach(
          Array.from({ length: 16 }, (_, index) => `n${index}`),
          (name) => playout.submit({ key: key(name), lane: "line", request: clip(name) }),
        );
        const outcomes = yield* Effect.forEach(handles, (handle) => handle.outcome);
        assert.deepStrictEqual(
          outcomes.map((status) => status._tag),
          handles.map(() => "Ended"),
        );
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

  it.effect(
    "opens the replacement at the lead however long the backlog, so it bills no idle wait",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        const { playout, events } = yield* start(
          {
            lifetime: "90 seconds",
            renewal: { lead: "30 seconds" },
            filler: {
              runway: { floor: "5 seconds", target: "10 seconds" },
              clip: ({ index, seconds }) => clip(`filler ${index}`, seconds),
            },
          },
          "50 millis",
        );
        const handles = yield* Effect.forEach(
          Array.from({ length: 30 }, (_, index) => `n${index}`),
          (name) => playout.submit({ key: key(name), lane: "line", request: clip(name) }),
        );
        for (const handle of handles) yield* handle.outcome;
        const opened = (yield* events).flatMap((event) =>
          event._tag === "Session" && event.event._tag === "Opened" ? [event.event.sessionId] : [],
        );
        const created = (sessionId: string | undefined) =>
          Effect.map(test.log, (log) => log.find((entry) => entry.sessionId === sessionId)?.at);
        const first = yield* created(opened[0]);
        const second = yield* created(opened[1]);
        // What will not fit before the cap waits for the replacement; it doesn't open it early.
        assert.isAtLeast(second! - first!, 60_000);
      }),
    { timeout: 60_000 },
  );

  it.effect(
    "replacements refused with a server error leave the session on air airing until its cap, then open once more",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        // The first allocation succeeds; the three renewal attempts are refused with 503, after
        // which nobody can tell whether a session was allocated, so each may bill.
        for (const nth of [2, 3, 4])
          yield* test.inject({ _tag: "RefuseAllocation", nth, status: 503 });
        const { playout, events } = yield* start({
          lifetime: "120 seconds",
          renewal: { lead: "60 seconds" },
        });
        yield* Effect.sleep("70 seconds");
        const late = yield* playout.submit({
          key: key("late"),
          lane: "line",
          request: clip("late"),
        });
        assert.strictEqual((yield* late.outcome)._tag, "Ended");
        assert.isTrue(
          Option.isNone(yield* playout.failure.pipe(Effect.timeoutOption("90 seconds"))),
        );
        const sessions = (yield* events).flatMap((event) =>
          event._tag === "Session" ? [event.event] : [],
        );
        assert.deepStrictEqual(
          sessions.flatMap((event) =>
            event._tag === "Opened"
              ? ["Opened"]
              : event._tag === "SetupFailed"
                ? [`SetupFailed ${event.consecutive}`]
                : [],
          ),
          ["Opened", "SetupFailed 1", "SetupFailed 2", "SetupFailed 3", "Opened"],
        );
      }),
    { timeout: 60_000 },
  );

  // The critique's probe: with a 60 s cap, n10 was cut mid-clip at the cap and failed as lost,
  // and an item submitted later aired ahead of n11 to n13.
  it.effect(
    "a backlog longer than the cap airs whole and in order across the replacement",
    () =>
      Effect.gen(function* () {
        const { playout, starts } = yield* start(
          {
            lifetime: "60 seconds",
            renewal: { lead: "10 seconds" },
          },
          "50 millis",
        );
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

// A refusal with a 4xx status allocated nothing, so it bills nothing. Its faults stay armed for
// the rest of a block, so it has one of its own.
layer(hosted)("renewal refused with nothing allocated", (it) => {
  it.effect(
    "keeps asking for the replacement while the session on air holds it, and no air is lost",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        // The first allocation succeeds, the next three are refused over quota, and the fifth
        // succeeds.
        for (const nth of [2, 3, 4])
          yield* test.inject({ _tag: "RefuseAllocation", nth, status: 429 });
        const { playout, events } = yield* start({
          lifetime: "120 seconds",
          renewal: { lead: "60 seconds" },
        });
        yield* Effect.sleep("70 seconds");
        const names = Array.from({ length: 12 }, (_, index) => `n${index}`);
        const handles = yield* Effect.forEach(names, (name) =>
          playout.submit({ key: key(name), lane: "line", request: clip(name) }),
        );
        for (const handle of handles) assert.strictEqual((yield* handle.outcome)._tag, "Ended");
        const times = new Map<string, number>();
        for (const event of yield* events)
          if (event._tag === "AsRun")
            times.set(`${event.event.key}:${event.event.status._tag}`, event.event.at);
        const gaps = names
          .slice(1)
          .map(
            (name, index) =>
              (times.get(`${name}:Started`) ?? Infinity) -
              (times.get(`${names[index]}:Ended`) ?? -Infinity),
          );
        assert.isBelow(Math.max(...gaps), 1_000, `gaps between clips: ${gaps.join(", ")} ms`);
      }),
    { timeout: 60_000 },
  );

  it.effect(
    "counts none refused while the session on air held it once that session is gone",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        // Filler airs. Two renewal opens are refused over quota while the first session holds
        // the air, a third just after its cap, and the fourth succeeds.
        for (const nth of [2, 3, 4])
          yield* test.inject({ _tag: "RefuseAllocation", nth, status: 429 });
        const { playout, events } = yield* start({
          lifetime: "60 seconds",
          renewal: { lead: "3 seconds" },
          filler: {
            runway: { floor: "5 seconds", target: "10 seconds" },
            clip: ({ index, seconds }) => clip(`filler ${index}`, seconds),
          },
        });
        const failure = yield* playout.failure.pipe(Effect.timeoutOption("90 seconds"));
        assert.deepStrictEqual(
          Option.map(failure, (error) => error.message),
          Option.none(),
        );
        const sessions = (yield* events).flatMap((event) =>
          event._tag === "Session" ? [event.event] : [],
        );
        assert.deepStrictEqual(
          sessions.flatMap((event) =>
            event._tag === "Opened"
              ? ["Opened"]
              : event._tag === "SetupFailed"
                ? [`SetupFailed ${event.consecutive}`]
                : [],
          ),
          ["Opened", "SetupFailed 1", "SetupFailed 2", "SetupFailed 3", "Opened"],
        );
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

  // Filler is all a quiet channel builds, and the floor covers a p95 build only once one is known.
  it.effect(
    "learns its build rate from filler alone",
    () =>
      Effect.gen(function* () {
        const { playout } = yield* start({
          filler: {
            runway: { floor: "4 seconds", target: "8 seconds" },
            clip: ({ index }) => clip(`idle ${index}`),
          },
        });
        const { estimates } = yield* eventually(
          playout.state,
          (state) => state.estimates.build !== undefined,
        );
        assert.isAbove(estimates.build?.median ?? 0, 0);
      }),
    { timeout: 60_000 },
  );

  // The hosted `show` rehearsal: with 5 s clips measured at H3's 5.167 s, a 6 s gap was tiled
  // with a 5.806 s request, H3 aligned it to 5.875 s, and the 125 ms left cost a whole clip.
  it.effect(
    "tiles the gap before an At item without falling short on H3's frame grid",
    () =>
      Effect.gen(function* () {
        const { playout } = yield* start({
          filler: {
            runway: { floor: "5 seconds", target: "8 seconds" },
            clip: ({ index, seconds }) => clip(`idle ${index}`, seconds),
          },
        });
        const measured = yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
        yield* measured.outcome;
        const secured = yield* eventually(playout.state, (state) => state.runwaySeconds >= 8);
        const due = (yield* Clock.currentTimeMillis) + secured.runwaySeconds * 1000 + 6_000;
        const timed = yield* playout.submit({
          key: key("timed"),
          lane: "line",
          request: clip("timed"),
          start: { _tag: "At", time: due, late: { _tag: "nextBoundary" } },
        });
        const started = yield* timed.started;
        assert.strictEqual(started._tag, "Started");
        // One step of H3's grid is 17 frames, 708 ms: a tile may run over by less than that.
        if (started._tag === "Started") assert.isBelow(started.lateByMillis ?? 0, 1_000);
      }),
    { timeout: 60_000 },
  );
});

// Filler airs alone for 40 s, then a 15 s item continuing the clip before it arrives with about
// 10 s of air secured. A continued build ran at 0.92 times real time on hosted H3, so it takes
// about 16 s; building it at once left 6.4 s of dead air.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      continuedBuildSpeed: 0.92,
      seam: "70 millis",
      http: "40 millis",
      channel: "20 millis",
    }),
  }),
)("air before queue order", (it) => {
  for (const protect of ["air", "order"] as const)
    it.effect(
      `a long continued item submitted on a thin runway, with filler protecting the ${protect}`,
      () =>
        Effect.gen(function* () {
          const { playout, events } = yield* start({
            filler: {
              runway: { floor: "5 seconds", target: "10 seconds" },
              clip: ({ index, seconds }) => clip(`filler ${index}`, seconds),
              protect,
            },
          });
          yield* Effect.sleep("40 seconds");
          const secured = (yield* playout.state).runwaySeconds;
          const long = yield* playout.submit({
            key: key("long"),
            lane: "line",
            request: clip("long", 15),
            continuity: "previous",
          });
          const started = yield* long.started;
          assert.strictEqual((yield* long.outcome)._tag, "Ended");
          const starved = (yield* events).flatMap((event) =>
            event._tag === "Starved" ? [event.at] : [],
          );
          const dark = starved.map((at) => (started._tag === "Started" ? started.at : at) - at);
          // Filler covering its build goes first; building it at once leaves the air dark.
          assert.strictEqual(
            starved.length,
            protect === "air" ? 0 : 1,
            `${secured.toFixed(1)} s secured at submission, dark for ${dark.join(", ")} ms`,
          );
        }),
      { timeout: 60_000 },
    );
});

layer(hosted)("local renderer", (it) => {
  it.effect("airs a local renderer's clip shorter than H3's shortest, under its own model", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const speech: Playout.ClipModel<H3.Request> = {
        name: "speech",
        lengths: { min: 0.5, max: 30 },
        defaultSeconds: 2,
        builtSeconds: (seconds) => seconds,
        check: () => [],
      };
      const presented = yield* Ref.make<ReadonlyArray<number>>([]);
      const playout = yield* Playout.make({
        model: speech,
        open: LocalSource.open({
          model: speech,
          build: (local) => Effect.succeed({ value: local.request.prompt }),
          present: (local) =>
            Effect.andThen(
              Ref.update(presented, (all) => [...all, local.seconds]),
              Effect.sleep(Duration.seconds(local.seconds)),
            ),
        }),
        lanes: [{ name: "speech" }],
      });
      const short = yield* playout.submit({
        key: key("short"),
        lane: "speech",
        request: { prompt: "line", seconds: 2 },
      });
      assert.strictEqual((yield* short.outcome)._tag, "Ended");
      const defaulted = yield* playout.submit({
        key: key("defaulted"),
        lane: "speech",
        request: { prompt: "another line" },
      });
      assert.strictEqual((yield* defaulted.outcome)._tag, "Ended");
      assert.deepStrictEqual(yield* Ref.get(presented), [2, 2]);
    }),
  );

  // The plan's longest clip to fit before a cap bisects on this.
  it.effect("every shipped model builds a longer request at least as long", () =>
    Effect.sync(() => {
      const shorter: Array<string> = [];
      for (const model of [H3Source.model, FastH3Source.model]) {
        const { min, max } = model.lengths;
        const steps = Math.round((max - min) * 10_000);
        let previous = model.builtSeconds(min);
        for (let step = 1; step <= steps; step++) {
          const seconds = min + ((max - min) * step) / steps;
          const built = model.builtSeconds(seconds);
          if (built < previous) shorter.push(`${model.name} at ${seconds.toFixed(4)} s`);
          previous = built;
        }
      }
      assert.deepStrictEqual(shorter, []);
    }),
  );

  // Silent filler builds at once and items take 0.8 s a requested second: the median of every
  // build is a filler's, which would project an item's build at nothing.
  it.effect("refuses a firm item its lane's builds cannot make, though filler builds at once", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: (local) =>
            local.request.prompt.startsWith("idle")
              ? Effect.succeed({ value: undefined })
              : Effect.as(Effect.sleep(Duration.seconds(0.8 * local.seconds)), {
                  value: undefined,
                }),
        }),
        ...Playout.lineup({
          runway: { floor: "4 seconds", target: "8 seconds" },
          clip: ({ index }) => clip(`idle ${index}`),
        }),
      });
      yield* Effect.sleep("30 seconds");
      const items = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "line", request: clip(name) }),
      );
      for (const item of items) yield* item.outcome;
      const mixed = (yield* playout.state).estimates.build?.median;
      assert.isBelow(mixed ?? Infinity, 0.1, "the median of every build is a filler's");
      // About 2.2 s of a filler clip is left: the next boundary is before the deadline, but a 4 s
      // build is not.
      yield* playout.events.pipe(
        Stream.filter((event) => event._tag === "Filler" && event.phase === "Started"),
        Stream.runHead,
      );
      yield* Effect.sleep("3 seconds");
      const firm = yield* Effect.result(
        playout.submit({
          key: key("firm"),
          lane: "line",
          request: clip("firm"),
          window: { startBy: "3 seconds", firm: true },
        }),
      );
      assert.strictEqual(
        Result.isFailure(firm) ? firm.failure._tag : "admitted",
        "WouldMissDeadline",
      );
      const { estimates } = yield* playout.state;
      const line = estimates.lanes.find((lane) => lane.name === "line");
      assert.approximately(line?.build?.median ?? Infinity, 0.8, 0.05);
      assert.isBelow(estimates.filler.build?.median ?? Infinity, 0.1);
    }),
  );

  it.effect("refuses a source that runs another model, before it airs anything", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const speech: Playout.ClipModel<H3.Request> = {
        name: "speech",
        lengths: { min: 0.5, max: 30 },
        defaultSeconds: 2,
        builtSeconds: (seconds) => seconds,
        check: () => [],
      };
      const opens = yield* Ref.make(0);
      const playout = yield* Playout.make({
        model: speech,
        open: Effect.andThen(
          Ref.update(opens, (count) => count + 1),
          LocalSource.open({ buildRatio: 0 }),
        ),
        lanes: [{ name: "speech" }],
      });
      const failure = yield* playout.failure.pipe(Effect.timeout("1 second"));
      assert.strictEqual(failure._tag, "ReactorError");
      if (failure._tag === "ReactorError") {
        assert.strictEqual(failure.reason._tag, "InvalidState");
        assert.include(failure.message, "H3");
        assert.include(failure.message, "speech");
      }
      assert.strictEqual(yield* Ref.get(opens), 1);
    }),
  );

  it.effect("runs the same plan on a local renderer", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const presented = yield* Ref.make<ReadonlyArray<string>>([]);
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: (local) => Effect.succeed({ value: local.request.prompt }),
          present: (local, prompt) =>
            Effect.andThen(
              Ref.update(presented, (all) => [...all, prompt]),
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

  it.effect("reports what a local renderer failed, released or cut, and moves on", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const released = yield* Ref.make<ReadonlyArray<string>>([]);
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: (local) =>
            Effect.gen(function* () {
              const prompt = local.request.prompt;
              yield* Effect.addFinalizer(() => Ref.update(released, (all) => [...all, prompt]));
              if (prompt === "unbuildable") return yield* Effect.fail("the renderer refused");
              yield* Effect.sleep("500 millis");
              return { value: prompt };
            }),
          present: (local) =>
            local.request.prompt === "broken"
              ? Effect.andThen(Effect.sleep("1 second"), Effect.fail("the speaker failed"))
              : Effect.sleep(Duration.seconds(local.seconds)),
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
      // A presentation that fails fails its clip, with the renderer's words kept out of the message.
      const failed = yield* broken.outcome;
      const reason = failed._tag === "Failed" ? failed.reason : undefined;
      assert.strictEqual(reason?._tag, "Clip");
      if (reason?._tag === "Clip") {
        assert.include(Redacted.value(reason.provider), "the speaker failed");
        assert.notInclude(reason.message, "the speaker failed");
      }
      yield* long.started;
      // The spare is Ready behind the long clip; withdrawing it hands it back to the renderer.
      yield* Effect.sleep("2 seconds");
      yield* playout.withdraw(key("spare"));
      assert.strictEqual((yield* spare.outcome)._tag, "Dropped");
      assert.deepStrictEqual(yield* Ref.get(released), ["unbuildable", "broken", "spare"]);
      const urgent = yield* submit("urgent", "urgent");
      yield* urgent.started;
      const stopped = yield* long.outcome;
      assert.deepStrictEqual(
        stopped._tag === "Ended" ? stopped.termination : stopped._tag,
        "stopped",
      );
    }),
  );

  it.effect("drops a follower not Ready when the clip it follows fails on air", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: Effect.fnUntraced(function* (local) {
            yield* Effect.sleep(local.request.prompt === "u" ? "3 seconds" : "500 millis");
            return { value: undefined };
          }),
          present: (local) =>
            local.request.prompt === "x"
              ? Effect.andThen(Effect.sleep("1 second"), Effect.fail("the speaker failed"))
              : Effect.sleep(Duration.seconds(local.seconds)),
        }),
        lanes: [{ name: "speech" }],
      });
      const submit = (name: string, seconds = 5) =>
        playout.submit({ key: key(name), lane: "speech", request: clip(name, seconds) });
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) => submit(name));
      for (const handle of measured) yield* handle.outcome;
      yield* (yield* submit("p", 8)).started;
      const x = yield* submit("x");
      yield* x.started;
      // u takes 3 s to build, and x fails a second in.
      const u = yield* playout.insert({
        key: key("u"),
        request: clip("u"),
        after: key("x"),
        follows: { _tag: "Item", key: key("x") },
      });
      assert.strictEqual((yield* x.outcome)._tag, "Failed");
      assert.deepStrictEqual(yield* u.outcome, { _tag: "Dropped", reason: "displaced" });
    }),
  );

  it.effect("renews a local renderer's sessions, and every item airs to its end", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const playout = yield* Playout.make({
        open: LocalSource.open({ buildRatio: 0.4, lifetime: "40 seconds" }),
        lanes: [{ name: "speech" }],
        renewal: { lead: "15 seconds" },
      });
      const switches = yield* Ref.make<ReadonlyArray<readonly [string, string]>>([]);
      yield* playout.events.pipe(
        Stream.runForEach((event) => {
          const session = event._tag === "Session" ? event.event : undefined;
          return session?._tag === "Switched"
            ? Ref.update(switches, (all) => [...all, [session.from, session.to] as const])
            : Effect.void;
        }),
        Effect.forkScoped({ startImmediately: true }),
      );
      const items = yield* Effect.forEach(
        Array.from({ length: 16 }, (_, index) => `line ${String(index)}`),
        (prompt) => playout.submit({ key: key(prompt), lane: "speech", request: clip(prompt) }),
      );
      const outcomes = yield* Effect.forEach(items, (item) =>
        Effect.map(item.outcome, (status) => status._tag),
      );
      assert.deepStrictEqual(
        outcomes,
        items.map(() => "Ended"),
      );
      const switched = yield* Ref.get(switches);
      assert.isAbove(switched.length, 0, "no session was renewed");
      for (const [from, to] of switched) assert.notStrictEqual(from, to);
    }),
  );

  it.effect(
    "hands over the grace after a clip fails on air, not at the retiring session's cap",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
        const playout = yield* Playout.make({
          open: LocalSource.open({
            build: (local) => Effect.succeed({ value: local.request.prompt }),
            present: (local, prompt) =>
              prompt === "failing"
                ? Effect.andThen(Effect.sleep("13 seconds"), Effect.fail("the speaker failed"))
                : Effect.sleep(Duration.seconds(local.seconds)),
            lifetime: "20 seconds",
          }),
          lanes: [{ name: "speech" }],
          renewal: { lead: "10 seconds" },
        });
        // The first clip is the session's only one: the next doesn't fit before its cap, and waits
        // for the replacement opened 10 seconds in.
        const failing = yield* playout.submit({
          key: key("failing"),
          lane: "speech",
          request: clip("failing", 15),
        });
        const next = yield* playout.submit({
          key: key("next"),
          lane: "speech",
          request: clip("next", 15),
        });
        assert.strictEqual((yield* failing.outcome)._tag, "Failed");
        const failedAt = yield* Clock.currentTimeMillis;
        yield* next.started;
        const gap = (yield* Clock.currentTimeMillis) - failedAt;
        assert.isBelow(gap, 1000, `the next item started ${String(gap)} ms after the failure`);
      }),
  );

  it.effect("reports a filler that fails on air as ended", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: (local) => Effect.succeed({ value: local.request.prompt }),
          present: (local, prompt) =>
            prompt === "idle 0"
              ? Effect.andThen(Effect.sleep("1 second"), Effect.fail("the speaker failed"))
              : Effect.sleep(Duration.seconds(local.seconds)),
        }),
        lanes: [{ name: "speech" }],
        filler: {
          runway: { floor: "4 seconds", target: "8 seconds" },
          clip: ({ index }) => clip(`idle ${index}`),
        },
      });
      const phases = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* playout.events.pipe(
        Stream.runForEach((event) =>
          event._tag === "Filler"
            ? Ref.update(phases, (all) => [...all, `${event.phase} ${String(event.index)}`])
            : Effect.void,
        ),
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Effect.sleep("12 seconds");
      assert.deepStrictEqual((yield* Ref.get(phases)).slice(0, 4), [
        "Started 0",
        "Ended 0",
        "Started 1",
        "Ended 1",
      ]);
    }),
  );

  // A source method that throws when called, rather than returning an effect, is reported, and its
  // lane's worker goes on to the session's next command.
  it.effect("reports a source method that throws when called, and its lane carries on", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reported: Array<unknown> = [];
      const reporter = ErrorReporter.make(({ cause }) => {
        reported.push(Cause.findDefect(cause).pipe(Result.getOrUndefined));
      });
      const bug = new Error("a source bug");
      let enqueues = 0;
      const playout = yield* Playout.make({
        open: Effect.map(LocalSource.open({ buildRatio: 0.2 }), (source) => ({
          ...source,
          enqueue: (request, tag, continueFrom) => {
            if (++enqueues === 2) throw bug;
            return source.enqueue(request, tag, continueFrom);
          },
        })),
        lanes: [{ name: "speech" }],
      }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
      const items = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "speech", request: clip(name) }),
      );
      // b's enqueue may have gone out, as far as the plan can tell; c goes out after it.
      const outcome = yield* items[2]!.outcome.pipe(Effect.timeoutOption("2 minutes"));
      assert.deepStrictEqual(
        Option.map(outcome, (value) => value._tag),
        Option.some("Ended"),
      );
      assert.deepStrictEqual(reported, [bug]);
    }),
  );

  // b's enqueue fails, and a finalizer dies as it does, so its cause holds both. The failure alone
  // decides b; the defect is reported once, and the failure, which the plan handles, is not.
  it.effect("reports a defect beside a source method's failure, which alone decides it", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reported: Array<string> = [];
      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });
      const refusal = ReactorError.CommandFailure.from(
        ReactorError.ReactorError.fromCode("InvalidInput", "the renderer refused it"),
        { operation: "enqueue", outcome: "not-submitted" },
      );
      const playout = yield* Playout.make({
        open: Effect.map(LocalSource.open({ buildRatio: 0.2 }), (source) => ({
          ...source,
          enqueue: (request, tag, continueFrom) =>
            request.prompt === "b"
              ? Effect.fail(refusal).pipe(Effect.ensuring(Effect.die(new Error("a source bug"))))
              : source.enqueue(request, tag, continueFrom),
        })),
        lanes: [{ name: "speech" }],
      }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
      const items = yield* Effect.forEach(["a", "b", "c"], (name) =>
        playout.submit({ key: key(name), lane: "speech", request: clip(name) }),
      );
      const outcome = yield* items[1]!.outcome;
      const reason = outcome._tag === "Failed" ? outcome.reason : undefined;
      assert.deepStrictEqual(
        reason?._tag === "Command" ? [reason._tag, reason.cause.message] : reason,
        ["Command", "the renderer refused it"],
      );
      assert.strictEqual((yield* items[2]!.outcome)._tag, "Ended");
      assert.deepStrictEqual(reported, ["a source bug"]);
    }),
  );

  // The first open fails, and the second session's events 2 s in, each as a finalizer dies. The
  // failures alone decide: the open is asked again, and the second session is lost and replaced.
  it.effect("reports a defect beside an open's failure, or its source's events'", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reported: Array<string> = [];
      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });
      const failing = (what: string) =>
        Effect.fail(ReactorError.ReactorError.fromCode("Timeout", `the ${what} failed`)).pipe(
          Effect.ensuring(Effect.die(new Error(`an ${what} bug`))),
        );
      let opens = 0;
      const playout = yield* Playout.make({
        open: Effect.suspend(() => {
          const nth = ++opens;
          if (nth === 1) return failing("open");
          return Effect.map(LocalSource.open({ buildRatio: 0.2 }), (source) =>
            nth === 2
              ? {
                  ...source,
                  events: Stream.merge(
                    source.events,
                    Stream.fromEffectDrain(
                      Effect.andThen(Effect.sleep("2 seconds"), failing("events")),
                    ),
                  ),
                }
              : source,
          );
        }),
        lanes: [{ name: "speech" }],
      }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
      yield* Effect.sleep("5 seconds");
      const item = yield* playout.submit({ key: key("a"), lane: "speech", request: clip("a") });
      assert.strictEqual((yield* item.outcome)._tag, "Ended");
      assert.strictEqual(opens, 3);
      assert.deepStrictEqual(reported, ["an open bug", "an events bug"]);
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

// Edits, faults and a renewal under the same wide timing: a lost reply, a failed build, a
// replacement, a withdrawal and an insert while a capped session hands over to the next.
for (const seed of [1, 2, 3, 4])
  layer(environment({ timing: ReactorTest.Timing.random({ seed }) }))(
    `edits, faults and renewal, seed ${seed}`,
    (it) => {
      it.effect(
        "every item settles once, edits keep their word, and every session ends",
        () =>
          Effect.gen(function* () {
            const test = yield* ReactorTest.ReactorTest;
            yield* test.inject({ _tag: "DropReply", command: "enqueue", nth: 3 });
            yield* test.inject({ _tag: "FailBuild", nth: 5 });
            const scope = yield* Scope.make();
            const { playout, events } = yield* start({
              lifetime: "90 seconds",
              renewal: { lead: "30 seconds" },
              unknownTimeout: "20 seconds",
            }).pipe(Scope.provide(scope));
            const submit = (name: string, lane = "line", seconds = 5) =>
              playout.submit({ key: key(name), lane, request: clip(name, seconds) });
            const handles = yield* Effect.forEach(
              Array.from({ length: 10 }, (_, index) => `e${index}`),
              (name, index) =>
                submit(name, index % 4 === 0 ? "urgent" : "line", 5 + (index % 3) * 3),
            );
            yield* handles[1]!.started;
            const replacement = yield* playout
              .replace(key("e6"), { key: key("e6b"), request: clip("e6b") })
              .pipe(Effect.option);
            const withdrawn = yield* playout.withdraw(key("e8"));
            const inserted = yield* playout
              .insert({ key: key("ins"), request: clip("ins"), after: key("e7") })
              .pipe(Effect.option);
            const all = [...handles, ...Option.toArray(replacement), ...Option.toArray(inserted)];
            const outcomes = yield* Effect.forEach(all, (handle) =>
              Effect.map(handle.outcome, (status) => [handle.key as string, status] as const),
            ).pipe(Effect.timeoutOption("10 minutes"));
            assert.isTrue(Option.isSome(outcomes), `seed ${seed}: an outcome never came`);
            const recorded = yield* events;
            const history = new Map<string, ReadonlyArray<string>>();
            for (const event of recorded)
              if (event._tag === "AsRun")
                history.set(event.event.key, [
                  ...(history.get(event.event.key) ?? []),
                  event.event.status._tag,
                ]);
            for (const [name, statuses] of history) {
              const terminal = statuses.filter((status) =>
                ["Ended", "Dropped", "Failed", "Unobserved"].includes(status),
              );
              assert.isAtMost(terminal.length, 1, `seed ${seed} ${name}: ${statuses.join(",")}`);
              if (terminal.length === 1) assert.strictEqual(statuses.at(-1), terminal[0]);
            }
            // A withdrawal answers what happened; a replaced item never airs after its replacement.
            const e8 = history.get("e8") ?? [];
            if (withdrawn === "withdrawn") assert.notInclude(e8, "Started", `seed ${seed}`);
            if (withdrawn === "already-started") assert.include(e8, "Started", `seed ${seed}`);
            const starts = recorded.flatMap((event) =>
              event._tag === "AsRun" && event.event.status._tag === "Started"
                ? [event.event.key as string]
                : [],
            );
            if (starts.includes("e6b")) assert.notInclude(starts, "e6", `seed ${seed}`);
            assert.deepStrictEqual(stopProblems(yield* test.log), [], `seed ${seed}`);
            // Closing retires every session, lost and replaced ones included.
            yield* Scope.close(scope, Exit.void);
            yield* eventually(
              test.sessions,
              (sessions) => sessions.every((value) => value.state === "CLOSED"),
              "5 minutes",
            );
          }),
        { timeout: 120_000 },
      );
    },
  );

// Provider text stays Redacted: an H3 `clip_failed` reason is the provider's own words.
layer(hosted)("failure reasons", (it) => {
  it.effect("says why an item failed: its clip, its command, or the playout's close", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "FailBuild", nth: 1, reason: "the provider's own words" });
      yield* test.inject({ _tag: "InvalidImage", nth: 1 });
      const scope = yield* Scope.make();
      const { playout } = yield* start().pipe(Scope.provide(scope));
      const built = yield* playout.submit({ key: key("built"), lane: "line", request: clip("a") });
      const clipFailure = yield* built.outcome;
      const clipReason = clipFailure._tag === "Failed" ? clipFailure.reason : undefined;
      assert.strictEqual(clipReason?._tag, "Clip");
      if (clipReason?._tag === "Clip") {
        assert.strictEqual(Redacted.value(clipReason.provider), "the provider's own words");
        assert.notInclude(clipReason.message, "own words");
      }
      const pictured = yield* playout.submit({
        key: key("pictured"),
        lane: "line",
        request: {
          prompt: "b",
          references: [{ _tag: "Bytes", bytes: ReactorTest.pngBytes({ width: 64, height: 64 }) }],
        },
      });
      const commandFailure = yield* pictured.outcome;
      const commandReason = commandFailure._tag === "Failed" ? commandFailure.reason : undefined;
      assert.deepStrictEqual(
        commandReason?._tag === "Command"
          ? [commandReason._tag, commandReason.cause.context.outcome]
          : commandReason,
        ["Command", "replied"],
      );
      const waiting = yield* playout.submit({
        key: key("waiting"),
        lane: "line",
        request: clip("c"),
        window: { notBefore: "1 hour", firm: false },
      });
      yield* Scope.close(scope, Exit.void);
      const closed = yield* waiting.outcome;
      assert.deepStrictEqual(closed._tag === "Failed" ? closed.reason : closed._tag, {
        _tag: "Closed",
      });
    }),
  );
});

layer(hosted)("sources", (it) => {
  it.effect("refuses a source whose session id a live source already holds", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const grant = yield* tokens("60 seconds");
      // A source that names every session alike, as a custom one might by mistake.
      const playout = yield* Playout.make({
        open: Effect.map(H3Source.open({ tokens: grant }), (source) => ({
          ...source,
          sessionId: "shared",
        })),
        lanes: [{ name: "line" }],
        renewal: { lead: "30 seconds" },
      });
      yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      const failure = yield* playout.failure.pipe(Effect.timeoutOption("2 minutes"));
      assert.isTrue(Option.isSome(failure));
      if (Option.isSome(failure)) assert.include(failure.value.message, "session id");
    }),
  );

  // The first open fails a second in, leaving in its scope a finalizer that dies as the scope
  // closes. The open is a failed setup as any is, so it is asked again; the defect is reported.
  it.effect("fails an open whose scope's finalizer dies, and asks again", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reported: Array<string> = [];
      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });
      let opens = 0;
      const playout = yield* Playout.make({
        open: Effect.suspend(() =>
          ++opens === 1
            ? Effect.addFinalizer(() => Effect.die(new Error("a release bug"))).pipe(
                Effect.andThen(Effect.sleep("1 second")),
                Effect.andThen(
                  Effect.fail(ReactorError.ReactorError.fromCode("Timeout", "the open failed")),
                ),
              )
            : LocalSource.open({ buildRatio: 0.2 }),
        ),
        lanes: [{ name: "speech" }],
      }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
      const sessions = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* playout.events.pipe(
        Stream.runForEach((event) =>
          event._tag !== "Session"
            ? Effect.void
            : Ref.update(sessions, (all) => [
                ...all,
                event.event._tag === "SetupFailed"
                  ? `SetupFailed ${event.event.consecutive}`
                  : event.event._tag,
              ]),
        ),
        Effect.forkScoped({ startImmediately: true }),
      );
      const item = yield* playout.submit({ key: key("a"), lane: "speech", request: clip("a") });
      const outcome = yield* item.outcome.pipe(Effect.timeoutOption("1 minute"));
      assert.deepStrictEqual(yield* Ref.get(sessions), ["SetupFailed 1", "Opened"]);
      assert.deepStrictEqual(
        Option.map(outcome, (value) => value._tag),
        Option.some("Ended"),
      );
      assert.deepStrictEqual(reported, ["a release bug"]);
    }),
  );

  // The renewal opens a second session under the first's id, and closing it unused dies. The
  // playout still fails as that refusal does, and the defect is reported.
  it.effect("refuses a source whose session id is held, though its close dies", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reported: Array<string> = [];
      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });
      let opens = 0;
      const playout = yield* Playout.make({
        open: Effect.map(
          LocalSource.open({ buildRatio: 0.2, lifetime: "20 seconds" }),
          (source) => ({
            ...source,
            sessionId: "shared",
            close:
              ++opens === 2
                ? Effect.andThen(source.close, Effect.die(new Error("a close bug")))
                : source.close,
          }),
        ),
        lanes: [{ name: "speech" }],
        renewal: { lead: "10 seconds" },
      }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
      const failure = yield* playout.failure.pipe(Effect.timeoutOption("1 minute"));
      assert.deepStrictEqual(
        Option.map(failure, (error) => error.message),
        Option.some("an opened source's session id is already in use"),
      );
      assert.deepStrictEqual(reported, ["a close bug"]);
    }),
  );

  // The replacement opens 10 s in and takes the air, and closing the retired session dies. The
  // defect is reported, and the retired session's scope, which holds what its open made, closes.
  it.effect("closes a retired session's scope though its close dies", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const reported: Array<string> = [];
      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });
      const released: Array<number> = [];
      let opens = 0;
      yield* Playout.make({
        open: Effect.suspend(() => {
          const nth = ++opens;
          return Effect.addFinalizer(() => Effect.sync(() => released.push(nth))).pipe(
            Effect.andThen(LocalSource.open({ buildRatio: 0.2, lifetime: "20 seconds" })),
            Effect.map((source) =>
              nth === 1
                ? {
                    ...source,
                    close: Effect.andThen(source.close, Effect.die(new Error("a close bug"))),
                  }
                : source,
            ),
          );
        }),
        lanes: [{ name: "speech" }],
        renewal: { lead: "10 seconds" },
      }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
      yield* Effect.sleep("18 seconds");
      assert.strictEqual(opens, 2);
      assert.deepStrictEqual(released, [1]);
      assert.deepStrictEqual(reported, ["a close bug"]);
    }),
  );
});

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
      // The verdict H3 sent in a paid run named no category.
      assert.deepStrictEqual(outcome._tag === "Failed" ? outcome.reason : outcome._tag, {
        _tag: "Moderated",
        categories: [],
      });
      assert.strictEqual((yield* fine.outcome)._tag, "Ended");
      const moderated = (yield* events).flatMap((event) =>
        event._tag === "Session" && event.event._tag === "Moderated" ? [event.event.key] : [],
      );
      assert.deepStrictEqual(moderated, [key("flagged")]);
      assert.strictEqual((yield* test.sessions).length, 2);
    }),
  );
});

// Screening ends the session a while after the flagged enqueue, and says nothing. At builds of 1.2
// times real time, the 8 s it takes lets the innocent clip air on the second session while the
// flagged one is still building (any delay from 6 to 10 s does).
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 1.2,
      seam: "70 millis",
      http: "40 millis",
      channel: "20 millis",
      moderation: "8 seconds",
    }),
  }),
)("moderation without a verdict", (it) => {
  it.effect(
    "fails a clip lost unbuilt twice in a row, and rebuilds a Ready one until it airs",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        yield* test.inject({ _tag: "Moderate", prompt: "flagged", verdict: false });
        const { playout, statuses } = yield* start();
        const submit = (name: string, seconds: number) =>
          playout.submit({ key: key(name), lane: "line", request: clip(name, seconds) });
        // The opener plays while the innocent clip waits Ready, and then the flagged one is sent.
        yield* submit("opener", 15);
        const innocent = yield* submit("innocent", 5);
        yield* eventually(statuses("innocent"), (all) => all.includes("Ready"));
        const flagged = yield* submit("flagged", 15);
        const failed = yield* flagged.outcome;
        assert.deepStrictEqual(failed._tag === "Failed" ? failed.reason._tag : failed._tag, "Lost");
        assert.notInclude(yield* statuses("flagged"), "Started");
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
      const coordinator = yield* CoordinatorClient.CoordinatorClient;
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

  it.effect(
    "a defect in the plan kills the playout with it, reported, and closes its sessions",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        const reported: Array<unknown> = [];
        const reporter = ErrorReporter.make(({ cause }) => {
          reported.push(Cause.findDefect(cause).pipe(Result.getOrUndefined));
        });
        const broke = new Error("the filler generator broke on a prompt for user alice");
        const { playout } = yield* start({
          filler: {
            runway: { floor: "5 seconds", target: "10 seconds" },
            clip: ({ index }) => {
              if (index > 0) throw broke;
              return clip("filler 0");
            },
          },
        }).pipe(Effect.provideService(ErrorReporter.CurrentErrorReporters, new Set([reporter])));
        const exit = yield* Effect.exit(playout.failure).pipe(Effect.timeoutOption("2 minutes"));
        // A bug stays a defect: its text never becomes a library message.
        assert.deepStrictEqual(
          Option.map(exit, (value) =>
            Exit.isFailure(value)
              ? Cause.findDefect(value.cause).pipe(Result.getOrUndefined)
              : value,
          ),
          Option.some<unknown>(broke),
        );
        assert.deepStrictEqual(reported, [broke]);
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

  it.effect("a group's withdrawal after the playout closed answers from its parts' fates", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const scope = yield* Scope.make();
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: yield* tokens("10 minutes") }),
        lanes: [{ name: "line" }],
      }).pipe(Scope.provide(scope));
      const group = (name: string) =>
        playout.submitGroup({
          key: key(name),
          lane: "line",
          parts: [
            { key: key(`${name}1`), request: clip(`${name} one`) },
            { key: key(`${name}2`), request: clip(`${name} two`) },
          ],
        });
      const cut = yield* group("cut");
      assert.strictEqual(yield* playout.withdraw(key("cut2")), "withdrawn");
      const whole = yield* group("whole");
      for (const part of [...cut.parts, ...whole.parts]) yield* part.outcome;
      yield* Scope.close(scope, Exit.void);
      const answers = yield* Effect.forEach(["cut", "whole", "never"], (name) =>
        playout.withdraw(key(name)),
      ).pipe(Effect.timeoutOption("10 seconds"));
      assert.deepStrictEqual(answers, Option.some(["withdrawn", "already-started", "not-found"]));
    }),
  );

  it.effect("a filler request outside H3's limits fails the playout, naming its index", () =>
    Effect.gen(function* () {
      const sent = (yield* commands("enqueue")).length;
      const { playout } = yield* start({
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index, seconds }) => clip(`filler ${String(index)}`, index === 1 ? 99 : seconds),
        },
      });
      const failure = yield* playout.failure.pipe(Effect.timeoutOption("2 minutes"));
      const index = Option.map(failure, (error) =>
        error._tag === "InvalidFiller" ? error.index : error._tag,
      );
      assert.deepStrictEqual(index, Option.some<number | string>(1));
      assert.strictEqual((yield* commands("enqueue")).length, sent + 1);
    }),
  );
});

// Its faults stay armed for the rest of a block, so it has one of its own.
layer(hosted)("cleanup evidence", (it) => {
  it.effect(
    "keeps the report of an open that failed after allocating, while it may still bill",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        // The first setup command's reply is lost, so the first open fails after allocating;
        // DELETE is ignored, so its termination stays unconfirmed.
        yield* test.inject({ _tag: "DropReply", command: "set_flush_on_clip_end", nth: 1 });
        yield* test.inject({ _tag: "IgnoreDelete" });
        const { playout, events } = yield* start();
        yield* eventually(events, (all) =>
          all.some((event) => event._tag === "Session" && event.event._tag === "Opened"),
        );
        const [failedOpen] = yield* test.sessions;
        const cleanup = yield* playout.cleanup;
        assert.deepStrictEqual(
          cleanup.retained.map((report) => [report.sessionId, report.remote.confirmed]),
          [[failedOpen?.id, false]],
        );
      }),
  );

  it.effect("keeps the report of an open whose allocation is unknown", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const unknown = {
        ...Reactor.noAcquisition,
        localClosed: true,
        allocation: "unknown" as const,
      };
      const playout = yield* Playout.make({
        open: Effect.fail(
          ReactorError.AcquisitionFailure.from(
            ReactorError.ReactorError.fromCode("Timeout", "create session timed out", {
              operation: "create session",
              outcome: "unknown",
            }),
            unknown,
          ),
        ),
        lanes: [{ name: "line" }],
      });
      yield* playout.submit({ key: key("a"), lane: "line", request: clip("a") });
      yield* playout.failure;
      // Nobody learned its id, so nothing confirms it ended: it may bill until its cap.
      const cleanup = yield* playout.cleanup;
      assert.strictEqual(
        cleanup.retained.filter((report) => report.allocation === "unknown").length,
        3,
      );
    }),
  );
});

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
  it.effect("the picture and the sound end when the playout stops", () =>
    Effect.gen(function* () {
      // The clock moves on after the playout's own scope, and its flow, have closed.
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const scope = yield* Scope.make();
      const { playout } = yield* start({
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index }) => clip(`filler ${String(index)}`),
        },
      }).pipe(Scope.provide(scope));
      const video = yield* playout.video.pipe(Stream.runDrain, Effect.forkScoped);
      const audio = yield* playout.audio.pipe(Stream.runDrain, Effect.forkScoped);
      yield* Effect.sleep("20 seconds");
      yield* Scope.close(scope, Exit.void);
      const ended = yield* Effect.all([Fiber.await(video), Fiber.await(audio)]).pipe(
        Effect.timeoutOption("10 seconds"),
      );
      assert.isTrue(Option.isSome(ended), "a reader was still waiting after the playout closed");
      if (Option.isSome(ended)) for (const exit of ended.value) assert.isTrue(Exit.isSuccess(exit));
    }),
  );

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

  it.effect("says which session's track a reader fell behind on", () =>
    Effect.gen(function* () {
      const { playout, events } = yield* start({
        filler: {
          runway: { floor: "10 seconds", target: "20 seconds" },
          clip: ({ index }) => clip(`long ${String(index)}`, 15),
        },
      });
      const frames = yield* Ref.make(0);
      const stalled = yield* Deferred.make<void>();
      // The first frame stalls the reader past the simulated host's 512-frame bound.
      yield* playout.video.pipe(
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
      const overflowed = yield* eventually(
        Effect.map(events, (all) =>
          all.flatMap((event) => (event._tag === "ReaderOverflow" ? [event] : [])),
        ),
        (all) => all.length > 0,
      );
      const opened = (yield* events).flatMap((event) =>
        event._tag === "Session" && event.event._tag === "Opened" ? [event.event.sessionId] : [],
      );
      assert.strictEqual(overflowed[0]?.track, "video");
      assert.strictEqual(overflowed[0]?.sessionId, opened[0]);
      assert.isAbove(Number(overflowed[0]?.pressure.readerOverflows ?? 0n), 0);
    }),
  );
});
