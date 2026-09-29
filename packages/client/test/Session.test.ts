/** One session through its public contract, on a simulated Reactor with the timing each case states. */
import { assert, layer } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  ErrorReporter,
  Exit,
  Fiber,
  FileSystem,
  Inspectable,
  Layer,
  Option,
  Path,
  Random,
  Redacted,
  Ref,
  Stream,
  Tracer,
} from "effect";
import * as H3 from "../src/H3.js";
import { Coordinator, Reactor, ReactorTest } from "../src/index.js";
import * as Wire from "../src/internal/wire.js";
import { PeerFactory } from "../src/Peer.js";
import { type CommandFailure, ReactorError } from "../src/ReactorError.js";
import type { CommandReply, Session, Snapshot } from "../src/Session.js";
import { connect, environment, tokens } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({ buildSpeed: 2.4, channel: "10 millis" });

layer(environment({ timing }))("replies", (it) => {
  // The simulated model refuses an unknown command with the code `unknown_command` and its name.
  it.effect(
    "a provider's error reply fails the command as replied, its code and text out of the message",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const session = yield* connect;
        const failure = yield* Effect.flip(session.command("no_such_command", {}));
        assert.deepStrictEqual(
          [failure.context.outcome, failure.reason._tag, failure.context.operation],
          ["replied", "Remote", "no_such_command"],
        );
        const provider =
          failure.reason._tag === "Remote"
            ? [failure.reason.remoteCode, failure.reason.body].map((field) =>
                field === undefined ? undefined : Redacted.value(field),
              )
            : [];
        assert.deepStrictEqual(provider, ["unknown_command", "no_such_command"]);
        assert.notInclude(failure.message, "unknown_command");
        assert.notInclude(failure.message, "no_such_command");
        // As a log or span prints it.
        assert.notInclude(Inspectable.toStringUnknown(failure), "unknown_command");
      }),
  );

  it.effect(
    "a recording request on a deployment without a recorder fails as RecorderDisabled",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const session = yield* connect;
        const observed = yield* session.events().pipe(
          Stream.filter((event) => event._tag === "Control"),
          Stream.runHead,
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        const failure = yield* Effect.flip(session.requestRecordingClip(5));
        assert.strictEqual(failure.reason._tag, "RecorderDisabled");
        // Observers see the reply too, its provider text kept out of what logs print.
        const control = Option.getOrThrow(yield* Fiber.join(observed));
        assert.strictEqual(control.message._tag, "ClipFailed");
        if (control.message._tag !== "ClipFailed") return;
        assert.strictEqual(Redacted.value(control.message.reason), "recorder disabled");
        assert.notInclude(Inspectable.toStringUnknown(control), "recorder disabled");
      }),
  );
});

/** A number whose text `Duration` cannot parse: `${huge} seconds` reads "Infinity seconds". */
const huge: number = 10 ** 999;
/** Deadlines `Duration` misreads: one it cannot parse, a NaN it reads as zero, a negative. */
const badDeadlines: ReadonlyArray<Duration.Input> = [`${huge} seconds`, Number.NaN, -5];

layer(environment({ timing }))("a per-call deadline", (it) => {
  it.effect("that is not a finite, non-negative duration is refused before anything is sent", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const session = yield* connect;
      const png = ReactorTest.pngBytes({ width: 16, height: 16 });
      for (const bad of badDeadlines) {
        const command = yield* Effect.flip(session.command("get_state", {}, { replyTimeout: bad }));
        const upload = yield* Effect.flip(
          session.upload("still.png", "image/png", png, { uploadTimeout: bad }),
        );
        assert.deepStrictEqual(
          [command, upload].map((error) => [error.reason._tag, error.context.outcome]),
          [
            ["InvalidInput", "not-submitted"],
            ["InvalidInput", "not-submitted"],
          ],
          Inspectable.toStringUnknown(bad),
        );
      }
      assert.deepStrictEqual((yield* session.snapshot).pending, { data: 0, control: 0 });
    }),
  );
});

layer(environment({ timing }))("tracing", (it) => {
  it.effect("names each operation's span after its module and operation", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const names = new Set<string>();
      const tracer = Tracer.make({
        span: (options) => {
          names.add(options.name);
          return new Tracer.NativeSpan(options);
        },
      });
      yield* Effect.gen(function* () {
        const session = yield* connect;
        yield* session.command("get_state", {});
        yield* session.close;
      }).pipe(Effect.withTracer(tracer));
      assert.includeMembers(
        [...names],
        [
          "Coordinator.mintToken",
          "Reactor.create",
          "Session.connect",
          "Session.command",
          "Session.close",
          "Coordinator.terminate",
        ],
      );
    }),
  );
});

/** The next connection to open drops a second after its channels do; its session goes on. */
const drop: ReactorTest.Fault = { _tag: "Disconnect", nth: 1, after: Duration.seconds(1) };

/** What the simulated Reactor says of the session `id`. */
const remoteState = (id: string) =>
  Effect.map(
    ReactorTest.ReactorTest.pipe(Effect.flatMap((test) => test.sessions)),
    (all) => all.find((info) => info.id === id)?.state,
  );

/** The statuses `session` reports from now on, each with its generation. */
const statuses = (session: Session) =>
  Effect.map(session.observe(), (observed) =>
    observed.events.pipe(
      Stream.filter((event) => event._tag === "Status"),
      Stream.map((event) => [event.status, event.generation] as const),
    ),
  );

// Reactor's docs: a session whose last connection drops reads INACTIVE, still billed, and ends 30 s
// later unless a connection returns.
layer(environment({ timing }))("a dropped connection", (it) => {
  it.effect("is reconnected by the session itself, within Reactor's 30 s, allocating nothing", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      // Longer than Reactor keeps a session whose last connection dropped.
      yield* Effect.sleep("40 seconds");
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual(
        [snapshot.status, snapshot.generation, yield* remoteState(session.id)],
        ["ready", 2n, "ACTIVE"],
      );
      assert.strictEqual((yield* test.sessions).length, 1);
      const reply = yield* session.command("get_state", {});
      assert.strictEqual(reply.generation, 2n);
    }),
  );
});

layer(environment({ timing }))("a session's own reconnect", (it) => {
  it.effect("shows in its status: the drop, then a new generation connecting to ready", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      const seen = yield* (yield* statuses(session)).pipe(
        Stream.takeUntil(([status, generation]) => status === "ready" && generation === 2n),
        Stream.runCollect,
        Effect.timeoutOption("30 seconds"),
      );
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["disconnected", 1n],
          ["connecting", 2n],
          ["waiting", 2n],
          ["ready", 2n],
        ]),
      );
      assert.isFalse((yield* session.snapshot).reconnecting);
    }),
  );
});

layer(environment({ timing }))("a session's own reconnect, read on `changes`", (it) => {
  it.effect("shows each change as it was, to a reader that takes its time", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      const seen = yield* session.changes.pipe(
        // A reader that takes 100 ms over each snapshot, so the session runs ahead of it.
        Stream.mapEffect((snapshot) =>
          Effect.as(
            Effect.sleep("100 millis"),
            `${snapshot.status} ${snapshot.generation} ${snapshot.reconnecting}`,
          ),
        ),
        Stream.changes,
        Stream.takeUntil((step) => step === "ready 2 false"),
        Stream.runCollect,
        Effect.timeoutOption("1 minute"),
      );
      assert.deepStrictEqual(
        seen,
        Option.some([
          "ready 1 false",
          "disconnected 1 true",
          "connecting 2 true",
          "waiting 2 true",
          "ready 2 false",
        ]),
      );
    }),
  );
});

layer(environment({ timing }))("an attached session's dropped connection", (it) => {
  it.effect("is reconnected by that session too, which never owned it", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const owner = yield* connect;
      // The viewer's connection is the next to open.
      yield* test.inject(drop);
      const reactor = yield* Reactor.Reactor;
      const viewer = yield* reactor.attach({ sessionId: owner.id, tokens: yield* tokens });
      yield* viewer.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "ready" && snapshot.generation > 1n),
        Stream.runHead,
        Effect.timeoutOption("30 seconds"),
      );
      const [kept, back] = [yield* owner.snapshot, yield* viewer.snapshot];
      assert.deepStrictEqual(
        [viewer.ownership, [kept.status, kept.generation], [back.status, back.generation]],
        ["attached", ["ready", 1n], ["ready", 2n]],
      );
    }),
  );
});

// A token that is never refreshed expires at 90 s; the connection drops at two minutes, so every
// attempt to reconnect is refused.
layer(environment({ timing }))("a reconnect that fails for good", (it) => {
  it.effect("gives up once Reactor's 30 s have passed, and says why", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const coordinator = yield* Coordinator.Coordinator;
      const grant = yield* coordinator.tokens({
        apiKey: test.apiKey,
        modelName: H3.modelName,
        maxSessionDuration: "10 minutes",
        expiresAfter: "90 seconds",
      }).create;
      yield* test.inject({ _tag: "Disconnect", nth: 1, after: Duration.minutes(2) });
      const reactor = yield* Reactor.Reactor;
      const session = yield* reactor.create({
        model: H3.modelName,
        tokens: Coordinator.fixedTokens(grant),
      });
      const reads = Effect.map(
        test.log,
        (log) =>
          log.filter(
            (entry) => entry.kind === "request" && entry.name.endsWith(`/sessions/${session.id}`),
          ).length,
      );
      const first = (accept: (snapshot: Snapshot) => boolean) =>
        session.changes.pipe(Stream.filter(accept), Stream.runHead, Effect.map(Option.getOrThrow));
      const dropped = yield* first((snapshot) => snapshot.status === "disconnected");
      const [droppedAt, before] = [yield* Clock.currentTimeMillis, yield* reads];
      const stopped = yield* first(
        (snapshot) => snapshot.status === "disconnected" && !snapshot.reconnecting,
      );
      assert.deepStrictEqual(
        [dropped.reconnecting, stopped.lastError?.reason._tag],
        [true, "Timeout"],
      );
      assert.approximately((yield* Clock.currentTimeMillis) - droppedAt, 30_000, 500);
      const attempts = (yield* reads) - before;
      assert.isAbove(attempts, 1);
      // A minute on, nothing has tried again.
      yield* Effect.sleep("1 minute");
      assert.strictEqual((yield* reads) - before, attempts);
    }),
  );
});

// Paid run cut c1796473: after a `terminate` verdict the session closed.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, moderation: "1 second" }),
    faults: [{ _tag: "Moderate", prompt: "flagged" }],
  }),
)("a session moderation ended", (it) => {
  it.effect("is never reconnected, and says why", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const session = yield* connect;
      const reported = yield* statuses(session);
      yield* session.command("enqueue", { prompt: "flagged", seconds: 5 });
      // Past Reactor's 30 s, in which a reconnect would have come.
      const seen = yield* reported.pipe(
        Stream.interruptWhen(Effect.sleep("40 seconds")),
        Stream.runCollect,
      );
      assert.deepStrictEqual(seen, [["disconnected", 1n]]);
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual(
        [
          snapshot.status,
          snapshot.reconnecting,
          snapshot.lastError?.reason._tag,
          yield* remoteState(session.id),
        ],
        ["disconnected", false, "Moderated", "CLOSED"],
      );
    }),
  );
});

// Each connection's answer takes 5 s, so the session's reconnect is still negotiating when it closes.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      channel: "10 millis",
      negotiation: "5 seconds",
    }),
  }),
)("a session closed while it reconnects", (it) => {
  it.effect("stops reconnecting, and closes as any session does", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.generation > 1n),
        Stream.runHead,
        Effect.timeoutOption("30 seconds"),
      );
      const report = yield* session.close;
      const seen = (yield* test.log).length;
      // Past Reactor's 30 s, in which the reconnect could have tried again.
      yield* Effect.sleep("40 seconds");
      assert.deepStrictEqual(
        [report.localClosed, report.localErrors, report.remote.confirmed],
        [true, [], true],
      );
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual([snapshot.status, snapshot.generation], ["closed", 2n]);
      const later = (yield* test.log)
        .slice(seen)
        .filter((entry) => entry.sessionId === session.id && entry.kind === "request");
      assert.deepStrictEqual(later, []);
    }),
  );
});

/** ReactorTest's peers, but each after the first takes `slow` to make, as a host that spawns one might. */
const slowPeers = (slow: Duration.Input) =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const peers = yield* PeerFactory;
      const made = yield* Ref.make(0);
      return PeerFactory.of({
        check: peers.check,
        make: Ref.getAndUpdate(made, (count) => count + 1).pipe(
          Effect.flatMap((count) =>
            count === 0 ? peers.make : Effect.andThen(Effect.sleep(slow), peers.make),
          ),
        ),
      });
    }),
  );

// The reconnect's new peer takes 40 s to make, past the 30 s the reconnect has.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(slowPeers("40 seconds")),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a reconnect whose new peer outlasts its deadline", (it) => {
  it.effect("fails the generation it began, and leaves the session disconnected, saying why", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected" && !snapshot.reconnecting),
        Stream.runHead,
        Effect.timeoutOption("2 minutes"),
      );
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual(
        [
          snapshot.status,
          snapshot.generation,
          snapshot.reconnecting,
          snapshot.lastError?.reason._tag,
        ],
        ["disconnected", 2n, false, "Timeout"],
      );
    }),
  );
});

/**
 * ReactorTest's peers, but shutting down peer `which`, counted from 0, dies with each of
 * `defects`, as a host's shutdown can.
 */
const dyingShutdown = (defects: ReadonlyArray<unknown>, which = 0) =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const peers = yield* PeerFactory;
      const made = yield* Ref.make(0);
      return PeerFactory.of({
        check: peers.check,
        make: Effect.gen(function* () {
          const peer = yield* peers.make;
          if ((yield* Ref.getAndUpdate(made, (count) => count + 1)) === which)
            for (const defect of defects) yield* Effect.addFinalizer(() => Effect.die(defect));
          return peer;
        }),
      });
    }),
  );

/** Each status `session` reports from now on, and each diagnostic's reason, with its generation. */
const notices = (session: Session) =>
  Effect.map(session.observe(), (observed) =>
    observed.events.pipe(
      Stream.filter((event) => event._tag === "Status" || event._tag === "Diagnostic"),
      Stream.map((event) =>
        event._tag === "Status"
          ? ([event.status, event.generation] as const)
          : ([event.error.reason._tag, event.generation] as const),
      ),
    ),
  );

/** Reporters that keep each bug reported to them, and the bugs they kept. */
const keepingBugs = () => {
  const bugs: Array<unknown> = [];
  const reporter = ErrorReporter.make(({ cause }) => {
    bugs.push(Cause.squash(cause));
  });
  return { bugs, reporters: new Set([reporter]) };
};

/** Each reason `exit` failed for: a failure's own reason, or `Die` or `Interrupt`. */
const reasonsOf = (exit: Exit.Exit<unknown, { readonly reason: { readonly _tag: string } }>) =>
  Exit.isSuccess(exit)
    ? []
    : exit.cause.reasons.map((reason) =>
        Cause.isFailReason(reason) ? reason.error.reason._tag : reason._tag,
      );

/**
 * A session whose connection drops, until it is ready again or 30 s have passed: where it ended,
 * what it reported meanwhile, and what was reported of it as a bug.
 */
const afterDrop = Effect.gen(function* () {
  yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
  const test = yield* ReactorTest.ReactorTest;
  yield* test.inject(drop);
  const { bugs, reporters } = keepingBugs();
  const session = yield* connect.pipe(
    Effect.provideService(ErrorReporter.CurrentErrorReporters, reporters),
  );
  const seen = yield* (yield* notices(session)).pipe(
    Stream.takeUntil(([what, generation]) => what === "ready" && generation === 2n),
    Stream.runCollect,
    Effect.timeoutOption("30 seconds"),
  );
  const snapshot = yield* session.snapshot;
  return { status: [snapshot.status, snapshot.generation], seen, bugs };
});

/** What the native peer's close dies with once its owner join outlives `shutdownTimeout`. */
const joinOutlived = ReactorError.fromCode(
  "Shutdown",
  "native owner join exceeded its deadline; handle retained",
);

// The dropped connection's peer fails to shut down as the reconnect retires it.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingShutdown([joinOutlived])),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a reconnect whose retired peer fails to shut down", (it) => {
  it.effect("says so on the retired generation, and goes on to ready", () =>
    Effect.gen(function* () {
      const { status, seen, bugs } = yield* afterDrop;
      assert.deepStrictEqual(status, ["ready", 2n]);
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["disconnected", 1n],
          ["Disconnected", 1n],
          ["connecting", 2n],
          ["Shutdown", 1n],
          ["waiting", 2n],
          ["ready", 2n],
        ]),
      );
      assert.deepStrictEqual(bugs, []);
    }),
  );
});

const shutdownBug = new Error("a host's shutdown bug");

// The dropped connection's peer dies of a bug as the reconnect retires it.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingShutdown([shutdownBug])),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a reconnect whose retired peer dies of a bug as it shuts down", (it) => {
  it.effect("reports the defect, and goes on to ready", () =>
    Effect.gen(function* () {
      const { status, seen, bugs } = yield* afterDrop;
      assert.deepStrictEqual(status, ["ready", 2n]);
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["disconnected", 1n],
          ["Disconnected", 1n],
          ["connecting", 2n],
          ["waiting", 2n],
          ["ready", 2n],
        ]),
      );
      assert.deepStrictEqual(bugs, [shutdownBug]);
    }),
  );
});

/** ReactorTest's peers, but fencing the first one dies with `defect` once it is fenced. */
const dyingFence = (defect: unknown) =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const peers = yield* PeerFactory;
      const made = yield* Ref.make(0);
      return PeerFactory.of({
        check: peers.check,
        make: Effect.gen(function* () {
          const peer = yield* peers.make;
          if ((yield* Ref.getAndUpdate(made, (count) => count + 1)) > 0) return peer;
          return { ...peer, close: Effect.andThen(peer.close, Effect.die(defect)) };
        }),
      });
    }),
  );

// The dropped connection's peer fails as its session fences it.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingFence(ReactorError.fromCode("Shutdown", "peer could not be fenced"))),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a dropped connection whose peer fails as it is fenced", (it) => {
  it.effect("still leaves the session disconnected, says so after the drop, and reconnects", () =>
    Effect.gen(function* () {
      const { status, seen, bugs } = yield* afterDrop;
      assert.deepStrictEqual(status, ["ready", 2n]);
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["disconnected", 1n],
          ["Disconnected", 1n],
          ["Shutdown", 1n],
          ["connecting", 2n],
          ["waiting", 2n],
          ["ready", 2n],
        ]),
      );
      assert.deepStrictEqual(bugs, []);
    }),
  );
});

const fenceBug = new Error("a host's fence bug");

// The dropped connection's peer dies of a bug as its session fences it.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingFence(fenceBug)),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a dropped connection whose peer dies of a bug as it is fenced", (it) => {
  it.effect("still leaves the session disconnected, reports the defect, and reconnects", () =>
    Effect.gen(function* () {
      const { status, seen, bugs } = yield* afterDrop;
      assert.deepStrictEqual(status, ["ready", 2n]);
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["disconnected", 1n],
          ["Disconnected", 1n],
          ["connecting", 2n],
          ["waiting", 2n],
          ["ready", 2n],
        ]),
      );
      assert.deepStrictEqual(bugs, [fenceBug]);
    }),
  );
});

// A reconnect is refused, and its own peer's host fails to shut down, with a bug besides.
layer(
  Reactor.layer({ reconnect: false }).pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingShutdown([joinOutlived, shutdownBug], 1)),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a refused reconnect whose peer dies as it shuts down", (it) => {
  it.effect("fails with the refusal alone, and reports the host after it", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "RefuseReconnect", nth: 1 });
      const session = yield* connect;
      const observed = yield* notices(session);
      const { bugs, reporters } = keepingBugs();
      const refused = yield* Effect.exit(session.reconnect).pipe(
        Effect.provideService(ErrorReporter.CurrentErrorReporters, reporters),
      );
      assert.deepStrictEqual(reasonsOf(refused), ["Http"]);
      assert.deepStrictEqual(bugs, [shutdownBug]);
      const seen = yield* observed.pipe(
        Stream.takeUntil(([what]) => what === "Shutdown"),
        Stream.runCollect,
        Effect.timeoutOption("1 second"),
      );
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["connecting", 2n],
          ["waiting", 2n],
          ["disconnected", 2n],
          ["Http", 2n],
          ["Shutdown", 2n],
        ]),
      );
    }),
  );
});

// A session's first connection is refused, and its peer dies of a bug as it shuts down.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingShutdown([shutdownBug])),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a refused connection whose peer dies of a bug as it shuts down", (it) => {
  it.effect("fails the acquisition with the refusal, and reports the defect", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "RefuseConnect", nth: 1 });
      const { bugs, reporters } = keepingBugs();
      const refused = yield* Effect.exit(connect).pipe(
        Effect.provideService(ErrorReporter.CurrentErrorReporters, reporters),
      );
      assert.deepStrictEqual(reasonsOf(refused), ["Http"]);
      assert.deepStrictEqual(bugs, [shutdownBug]);
    }),
  );
});

/**
 * ReactorTest's peers, but making peer `which`, counted from 0, fails with `error` once it has
 * registered a shutdown that dies with `defect`.
 */
const halfMade = (which: number, error: ReactorError, defect: unknown) =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const peers = yield* PeerFactory;
      const made = yield* Ref.make(0);
      return PeerFactory.of({
        check: peers.check,
        make: Effect.gen(function* () {
          if ((yield* Ref.getAndUpdate(made, (count) => count + 1)) !== which)
            return yield* peers.make;
          yield* Effect.addFinalizer(() => Effect.die(defect));
          return yield* error;
        }),
      });
    }),
  );

// A reconnect's host fails to make its peer, and dies of a bug as what it made shuts down.
layer(
  Reactor.layer({ reconnect: false }).pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(
      halfMade(1, ReactorError.fromCode("Native", "peer allocation failed"), shutdownBug),
    ),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a reconnect whose host fails to make its peer, and dies of a bug as it shuts it down", (it) => {
  it.effect("fails with the host's failure alone, and reports the defect", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const session = yield* connect;
      const { bugs, reporters } = keepingBugs();
      const failed = yield* Effect.exit(session.reconnect).pipe(
        Effect.provideService(ErrorReporter.CurrentErrorReporters, reporters),
      );
      assert.deepStrictEqual(reasonsOf(failed), ["Native"]);
      assert.deepStrictEqual(bugs, [shutdownBug]);
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual([snapshot.status, snapshot.generation], ["ready", 1n]);
    }),
  );
});

// Two reconnects begin half a second apart, each with a peer that takes a second to make: the
// later one finds the session taken over, and its unused peer dies of a bug as it shuts down.
layer(
  Reactor.layer({ reconnect: false }).pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingShutdown([shutdownBug], 2)),
    Layer.provideMerge(slowPeers("1 second")),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a reconnect overtaken by another, whose unused peer dies of a bug as it shuts down", (it) => {
  it.effect("fails as overtaken, and reports the defect", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const session = yield* connect;
      const { bugs, reporters } = keepingBugs();
      const reconnect = Effect.exit(session.reconnect).pipe(
        Effect.provideService(ErrorReporter.CurrentErrorReporters, reporters),
      );
      const first = yield* Effect.forkChild(reconnect);
      yield* Effect.sleep("500 millis");
      const second = yield* reconnect;
      assert.deepStrictEqual(
        [reasonsOf(yield* Fiber.join(first)), reasonsOf(second)],
        [[], ["InvalidState"]],
      );
      assert.deepStrictEqual(bugs, [shutdownBug]);
    }),
  );
});

/** A Random that always draws `value`: 0 is the bottom of its range, and 0.5 its middle. */
const drawing = (value: number): Random.Random => ({
  nextIntUnsafe: () => 0,
  nextDoubleUnsafe: () => value,
});

/**
 * How long after each refused reconnect of the session `id` the next attempt sent its first
 * request, on the TestClock: after every reconnect's offer but the last, which was accepted.
 */
const waitsAfterRefusals = (id: string) =>
  Effect.map(ReactorTest.ReactorTest.pipe(Effect.flatMap((test) => test.log)), (log) => {
    const requests = log.filter((entry) => entry.sessionId === id && entry.kind === "request");
    const waits = requests.flatMap((entry, index) => {
      const next = requests[index + 1];
      const reoffer = entry.name.startsWith("PUT ") && entry.name.endsWith("/sdp_params");
      return reoffer && next !== undefined ? [next.at - entry.at] : [];
    });
    return waits.slice(0, -1);
  });

// Reactor refuses the session's first six reconnects; each request takes 20 ms.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
  }),
)("a session's own reconnect, refused", (it) => {
  /** Each wait after a refusal before the next attempt, when every jitter draws `draw`. */
  const waits = (draw: number) =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      yield* Effect.forEach([1, 2, 3, 4, 5, 6], (nth) =>
        test.inject({ _tag: "RefuseReconnect", nth }),
      );
      const session = yield* connect;
      const back = yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "ready" && snapshot.generation > 1n),
        Stream.runHead,
        Effect.timeoutOption("30 seconds"),
        Effect.map(Option.flatten),
      );
      // Six generations refused, and the seventh ready.
      assert.strictEqual(Option.getOrUndefined(back)?.generation, 8n);
      // The next attempt's first request takes 20 ms.
      return (yield* waitsAfterRefusals(session.id)).map((wait) => wait - 20);
    }).pipe(Effect.provideService(Random.Random, drawing(draw)));

  it.effect(
    "tries again 250 ms after the first refusal, then twice as long each time, to 4 s",
    () =>
      Effect.map(waits(0.5), (all) => {
        assert.deepStrictEqual(all, [250, 500, 1_000, 2_000, 4_000, 4_000]);
      }),
  );

  it.effect("jitters each wait: at the bottom of its range, a fifth shorter", () =>
    Effect.map(waits(0), (all) => {
      assert.deepStrictEqual(all, [200, 400, 800, 1_600, 3_200, 3_200]);
    }),
  );
});

// Reactor refuses the session's first three reconnects.
layer(environment({ timing }))("a session's own reconnect, refused three times", (it) => {
  it.effect("recovers on the fourth attempt, each on a new generation of the same session", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      yield* Effect.forEach([1, 2, 3], (nth) => test.inject({ _tag: "RefuseReconnect", nth }));
      const session = yield* connect;
      const seen = yield* (yield* statuses(session)).pipe(
        Stream.takeUntil(([status]) => status === "ready"),
        Stream.runCollect,
        Effect.timeoutOption("30 seconds"),
      );
      assert.deepStrictEqual(
        seen,
        Option.some([
          ["disconnected", 1n],
          ["connecting", 2n],
          ["waiting", 2n],
          ["disconnected", 2n],
          ["connecting", 3n],
          ["waiting", 3n],
          ["disconnected", 3n],
          ["connecting", 4n],
          ["waiting", 4n],
          ["disconnected", 4n],
          ["connecting", 5n],
          ["waiting", 5n],
          ["ready", 5n],
        ]),
      );
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual(
        [snapshot.reconnecting, yield* remoteState(session.id), (yield* test.sessions).length],
        [false, "ACTIVE", 1],
      );
    }),
  );
});

// Reactor answers the session's reconnect as if it knew no such session.
layer(environment({ timing }))("a session's own reconnect answered 404", (it) => {
  it.effect("stops at once, saying why", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      yield* test.inject({ _tag: "RefuseReconnect", nth: 1, status: 404 });
      const session = yield* connect;
      const stopped = yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected" && !snapshot.reconnecting),
        Stream.runHead,
        Effect.timeoutOption("30 seconds"),
        Effect.map(Option.flatten),
      );
      const last = Option.getOrUndefined(stopped)?.lastError;
      assert.deepStrictEqual(
        [last?.reason._tag, last?.reason._tag === "Http" ? last.reason.status : undefined],
        ["Http", 404],
      );
      // One attempt, on the generation after the dropped one.
      assert.strictEqual((yield* session.snapshot).generation, 2n);
    }),
  );
});

// Every connection drops 100 ms after its channels open, as one to a host that crashes on the
// stream would; each request takes 20 ms.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
    faults: [{ _tag: "Disconnect", after: Duration.millis(100) }],
  }),
)("a session whose connection drops each time soon after it is ready", (it) => {
  it.effect("reconnects on its schedule across the drops, and stops 30 s after the first", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const session = yield* connect;
      const stopped = yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected" && !snapshot.reconnecting),
        Stream.runHead,
        Effect.timeoutOption("2 minutes"),
        Effect.map(Option.flatten),
      );
      const stoppedAt = yield* Clock.currentTimeMillis;
      const log = (yield* test.log).filter((entry) => entry.sessionId === session.id);
      const drops = log.filter((entry) => entry.name === "disconnected").map((entry) => entry.at);
      const requests = log.filter((entry) => entry.kind === "request");
      // After each drop, how long until the next attempt's first request, which takes 20 ms.
      const waits = drops.flatMap((at) => {
        const next = requests.find((request) => request.at > at);
        return next === undefined ? [] : [next.at - at - 20];
      });
      assert.deepStrictEqual(
        [Option.getOrUndefined(stopped)?.lastError?.reason._tag, waits],
        ["Timeout", [0, 250, 500, 1_000, 2_000, 4_000, 4_000, 4_000, 4_000, 4_000, 4_000]],
      );
      assert.approximately(stoppedAt - (drops[0] ?? 0), 30_000, 100);
      // Reactor keeps a session 30 s after its last connection drops.
      yield* Effect.sleep("30 seconds");
      assert.strictEqual(yield* remoteState(session.id), "CLOSED");
    }).pipe(Effect.provideService(Random.Random, drawing(0.5))),
  );
});

// Every connection drops 6 s after its channels open, so one is up when the reconnect's time runs
// out; each request takes 20 ms.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
    faults: [{ _tag: "Disconnect", after: Duration.seconds(6) }],
  }),
)("a session whose connection drops each time 6 s after it is ready", (it) => {
  it.effect("stops where the connection up as its reconnect's time ran out drops", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const session = yield* connect;
      const stopped = yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected" && !snapshot.reconnecting),
        Stream.runHead,
        Effect.timeoutOption("2 minutes"),
        Effect.map(Option.flatten),
      );
      const stoppedAt = yield* Clock.currentTimeMillis;
      const drops = (yield* test.log)
        .filter((entry) => entry.sessionId === session.id && entry.name === "disconnected")
        .map((entry) => entry.at);
      const [first, last] = [drops[0] ?? 0, drops.at(-1) ?? 0];
      assert.strictEqual(Option.getOrUndefined(stopped)?.lastError?.reason._tag, "Timeout");
      // Its time ran out 30 s after the first drop, while the connection that dropped last was up.
      assert.isAbove(last - first, 30_000);
      assert.approximately(stoppedAt, last, 50);
      // Reactor keeps a session 30 s after its last connection drops.
      yield* Effect.sleep("30 seconds");
      assert.strictEqual(yield* remoteState(session.id), "CLOSED");
    }).pipe(Effect.provideService(Random.Random, drawing(0.5))),
  );
});

layer(environment({ timing, reconnect: false }))("a dropped connection, reconnect off", (it) => {
  it.effect("stays down, and Reactor ends the session 30 s later", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      // Past Reactor's 30 s, in which a connection could have come back.
      yield* Effect.sleep("40 seconds");
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual(
        [snapshot.status, snapshot.generation, yield* remoteState(session.id)],
        ["disconnected", 1n, "CLOSED"],
      );
    }),
  );

  it.effect("reconnects when asked, on a new generation, and commands work again", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject(drop);
      const session = yield* connect;
      yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected"),
        Stream.runHead,
      );
      assert.strictEqual((yield* session.snapshot).status, "disconnected");
      yield* session.reconnect;
      const ready = yield* session.snapshot;
      assert.deepStrictEqual([ready.status, ready.generation], ["ready", 2n]);
      const reply = yield* session.command("get_state", {});
      assert.strictEqual(reply.generation, 2n);
    }),
  );
});

// Reactor's docs: a session-scoped token lives at most six hours and acts only on the sessions it
// created or was bound to, while every later call of a session needs a live token.
layer(environment({ timing }))("tokens", (it) => {
  const short = Effect.gen(function* () {
    const test = yield* ReactorTest.ReactorTest;
    const coordinator = yield* Coordinator.Coordinator;
    return coordinator.tokens({
      apiKey: test.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: "10 minutes",
      expiresAfter: "90 seconds",
    });
  });
  const png = ReactorTest.pngBytes({ width: 16, height: 16 });

  it.effect("a session outlives its first token: it goes on with tokens bound to it", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const reactor = yield* Reactor.Reactor;
      const session = yield* reactor.create({ model: H3.modelName, tokens: yield* short });
      yield* Effect.sleep("3 minutes");
      const uploaded = yield* session.upload("still.png", "image/png", png);
      assert.strictEqual(uploaded.notification, "submitted");
      yield* session.reconnect;
      assert.strictEqual((yield* session.snapshot).status, "ready");
      const report = yield* session.close;
      assert.isTrue(report.remote.confirmed);
    }),
  );

  it.effect("a short token is refreshed once near its end, not on every call", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const coordinator = yield* Coordinator.Coordinator;
      const tokens = coordinator.tokens({
        apiKey: test.apiKey,
        modelName: H3.modelName,
        maxSessionDuration: "10 minutes",
        expiresAfter: "20 seconds",
      });
      let mints = 0;
      const counted = {
        create: Effect.tap(tokens.create, () => Effect.sync(() => mints++)),
        bind: (sessionId: string) =>
          Effect.tap(tokens.bind(sessionId), () => Effect.sync(() => mints++)),
      };
      const reactor = yield* Reactor.Reactor;
      const session = yield* reactor.create({ model: H3.modelName, tokens: counted });
      const connected = mints;
      // Every upload calls the coordinator twice; none of these is near the token's end.
      for (let upload = 0; upload < 3; upload++)
        yield* session.upload("still.png", "image/png", png);
      assert.strictEqual(mints, connected);
      yield* Effect.sleep("16 seconds");
      yield* session.upload("still.png", "image/png", png);
      yield* session.upload("still.png", "image/png", png);
      assert.strictEqual(mints, connected + 1);
    }),
  );

  it.effect("a token that is never refreshed stops working when it expires", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const reactor = yield* Reactor.Reactor;
      const grant = yield* (yield* short).create;
      const session = yield* reactor.create({
        model: H3.modelName,
        tokens: Coordinator.fixedTokens(grant),
      });
      yield* Effect.sleep("3 minutes");
      const refused = yield* Effect.flip(session.reconnect);
      assert.deepStrictEqual(
        [refused.reason._tag, refused.reason._tag === "Http" ? refused.reason.status : undefined],
        ["Http", 401],
      );
    }),
  );

  it.effect("an attach whose token is not bound to the session is refused", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const reactor = yield* Reactor.Reactor;
      const tokens = yield* short;
      const owner = yield* reactor.create({ model: H3.modelName, tokens });
      const refused = yield* Effect.flip(
        reactor.attach({ sessionId: owner.id, tokens: { bind: () => tokens.create } }),
      );
      assert.deepStrictEqual(
        [refused.reason._tag, refused.reason._tag === "Http" ? refused.reason.status : undefined],
        ["Http", 403],
      );
      const viewer = yield* reactor.attach({ sessionId: owner.id, tokens });
      assert.strictEqual((yield* viewer.snapshot).status, "ready");
    }),
  );

  it.effect("a viewer can start with its tracks paused", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const reactor = yield* Reactor.Reactor;
      const session = yield* reactor.create({
        model: H3.modelName,
        tokens: yield* short,
        resumeTracks: false,
      });
      yield* Effect.sleep("1 second");
      const tracks = (yield* ReactorTest.ReactorTest.pipe(
        Effect.flatMap((test) => test.log),
      )).filter((entry) => entry.sessionId === session.id && entry.kind === "track");
      assert.deepStrictEqual(tracks, []);
    }),
  );
});

/** The reply the model sends `session` for the request `sent` names, whenever it comes. */
const replyTo = (session: Session, sent: Deferred.Deferred<string>) =>
  Effect.gen(function* () {
    const observed = yield* session.observe();
    return yield* observed.events.pipe(
      Stream.filter((event): event is CommandReply => event._tag === "Model"),
      Stream.filterEffect((reply) =>
        Effect.map(Deferred.await(sent), (requestId) => reply.requestId === requestId),
      ),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
      Effect.forkScoped,
    );
  });

/** The request a failed command was sent as. */
const requestOf = (failure: CommandFailure) =>
  failure.context.outcome === "not-submitted" ? "" : failure.context.requestId;

// A model command is paid and cannot be cancelled, so a reply that comes after its caller stopped
// waiting, or on a later connection, is kept and labelled rather than dropped or mistaken.
layer(environment({ timing }))("a reply's correlation", (it) => {
  it.effect("is late once the caller's deadline passed", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const session = yield* connect;
      yield* test.inject({
        _tag: "LateReply",
        nth: 1,
        command: "get_state",
        after: Duration.seconds(5),
      });
      const sent = yield* Deferred.make<string>();
      const late = yield* replyTo(session, sent);
      const failure = yield* Effect.flip(
        session.command("get_state", {}, { replyTimeout: "1 second" }),
      );
      yield* Deferred.succeed(sent, requestOf(failure));
      assert.deepStrictEqual(
        [failure.reason._tag, failure.context.outcome],
        ["Timeout", "unknown"],
      );
      const reply = yield* Fiber.join(late);
      assert.deepStrictEqual([reply.generation, reply.correlation], [1n, "late"]);
    }),
  );

  it.effect("is stale when it arrives on a later generation", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const session = yield* connect;
      // The model takes `set_seed` at once, which its state broadcast shows, and answers later.
      yield* test.inject({
        _tag: "LateReply",
        nth: 1,
        command: "set_seed",
        after: Duration.seconds(5),
      });
      const observed = yield* session.observe();
      const taken = yield* observed.events.pipe(
        Stream.filter(
          (event) =>
            event._tag === "Model" &&
            event.kind === "message" &&
            event.type === "state_update" &&
            event.data?.seed === 7,
        ),
        Stream.runHead,
        Effect.forkScoped,
      );
      const sent = yield* Deferred.make<string>();
      const stale = yield* replyTo(session, sent);
      const command = yield* Effect.forkChild(
        Effect.flip(session.command("set_seed", { seed: 7 })),
      );
      yield* Fiber.join(taken);
      yield* session.reconnect;
      const failure = yield* Fiber.join(command);
      yield* Deferred.succeed(sent, requestOf(failure));
      assert.deepStrictEqual(
        [failure.reason._tag, failure.context.outcome],
        ["Disconnected", "unknown"],
      );
      const reply = yield* Fiber.join(stale);
      assert.deepStrictEqual([reply.generation, reply.correlation], [2n, "stale-generation"]);
    }),
  );
});

// An observer that falls behind fails with Overflow rather than silently missing events.
layer(environment({ timing }))("a slow observer", (it) => {
  it.effect("fails with Overflow once it holds its capacity, and is counted", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const session = yield* connect;
      const observed = yield* session.observe({ capacity: 2 });
      for (let command = 0; command < 3; command++) yield* session.command("get_state", {});
      assert.strictEqual((yield* session.snapshot).observationOverflows, 1n);
      const failure = yield* Effect.flip(Stream.runCollect(observed.events));
      assert.strictEqual(failure.reason._tag, "Overflow");
      // The session goes on for everyone else.
      yield* session.command("get_state", {});
    }),
  );
});

/** Completes when a host's heartbeat ping has died. */
class PingDied extends Context.Service<PingDied, Deferred.Deferred<void>>()(
  "reactor-effect-client/test/Session.test/PingDied",
) {}

/** ReactorTest's peers, but sending a heartbeat ping dies, as a host with a bug would. */
const dyingPings = Layer.effectContext(
  Effect.gen(function* () {
    const peers = yield* PeerFactory;
    const died = yield* Deferred.make<void>();
    const isPing = (bytes: Uint8Array) =>
      Wire.decode(Wire.ControlClientMessageSchema)(bytes).pipe(
        Effect.map((message) => message.payload.case === "ping"),
        Effect.orElseSucceed(() => false),
      );
    return Context.make(PingDied, died).pipe(
      Context.add(
        PeerFactory,
        PeerFactory.of({
          check: peers.check,
          make: Effect.map(peers.make, (peer) => ({
            ...peer,
            send: (channel: "control" | "data", bytes: Uint8Array<ArrayBuffer>) =>
              Effect.gen(function* () {
                if (channel === "control" && (yield* isPing(bytes))) {
                  yield* Deferred.succeed(died, undefined);
                  return yield* Effect.die("a host's heartbeat bug");
                }
                return yield* peer.send(channel, bytes);
              }),
          })),
        }),
      ),
    );
  }),
);

// A bug stays a defect: it is not a Protocol failure that disconnects the session for a reconnect.
layer(
  Reactor.layer({ heartbeatInterval: "1 second" }).pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingPings),
    Layer.provideMerge(ReactorTest.layer({ timing })),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a host's defect in the heartbeat", (it) => {
  it.effect("does not fail the connection as a protocol error", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const session = yield* connect;
      yield* Deferred.await(yield* PingDied);
      yield* session.command("get_state", {});
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual([snapshot.status, snapshot.lastError], ["ready", undefined]);
    }),
  );
});
