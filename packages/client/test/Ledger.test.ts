/** The ledger on the simulated Reactor: what it records, resumes and ends, and when it allocates. */
import { assert, layer } from "@effect/vitest";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import {
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Path,
  Schedule,
} from "effect";
import type { Scope } from "effect";
import * as H3 from "../src/H3.js";
import { CoordinatorClient, H3Source, Ledger, ReactorTest } from "../src/index.js";
import { environment, tokens } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({ buildSpeed: 2.4 });

/** A CoordinatorClient with the simulated key, which ends any session of the account. */
const keyed = Layer.effect(
  CoordinatorClient.CoordinatorClient,
  Effect.flatMap(ReactorTest.ReactorTest, (test) =>
    CoordinatorClient.make({ apiKey: test.apiKey }),
  ),
);

/** The simulated Reactor and a ledger over `coordinator`, its own, so nothing carries over. */
const simulated = (
  ledger: Layer.Layer<Ledger.Ledger | Ledger.Store, never, CoordinatorClient.CoordinatorClient>,
  coordinator: typeof keyed,
) => ledger.pipe(Layer.provide(coordinator), Layer.provideMerge(environment({ timing })));

/** One test on a simulated Reactor of its own, its ledger in memory over the key by default. */
const alone = <E>(
  name: string,
  body: () => Effect.Effect<void, E, Layer.Success<ReturnType<typeof simulated>> | Scope.Scope>,
  ledger: Layer.Layer<
    Ledger.Ledger | Ledger.Store,
    never,
    CoordinatorClient.CoordinatorClient
  > = Ledger.layerMemory(),
  coordinator: typeof keyed = keyed,
) => layer(simulated(ledger, coordinator))(name, (it) => it.effect(name, body));

/** H3 through the ledger, on the fixture's tokens. */
const opener = Effect.map(tokens, (sessionTokens) => H3Source.opener({ tokens: sessionTokens }));

/**
 * A session a process that died left recorded: opened in the test's scope, which keeps it, and its
 * entry put in the store as `overrides` say.
 */
const leftover = Effect.fnUntraced(function* (
  overrides: Partial<Ledger.Entry> = {},
  sessionTokens?: CoordinatorClient.Tokens,
) {
  const allocated = yield* Deferred.make<H3Source.Allocation>();
  yield* H3Source.open({
    tokens: sessionTokens ?? (yield* tokens),
    onAllocated: ({ allocation }) => Deferred.succeed(allocated, allocation),
  });
  const { sessionId, model, endsAt } = yield* Deferred.await(allocated);
  const entry: Ledger.Entry = {
    sessionId,
    model,
    apiUrl: (yield* CoordinatorClient.CoordinatorClient).apiUrl,
    ...(endsAt === undefined ? {} : { endsAt }),
    state: "live",
    ...overrides,
  };
  yield* (yield* Ledger.Store).put(entry);
  return entry;
});

/** Each simulated session's state, by id. */
const states = Effect.map(
  Effect.flatMap(ReactorTest.ReactorTest, (test) => test.sessions),
  (sessions) => new Map(sessions.map((info) => [info.id, info.state])),
);

/** What the ledger holds, as session id and state. */
const held = Effect.map(
  Effect.flatMap(Ledger.Ledger, (ledger) => ledger.entries),
  (entries) => entries.map((entry) => [entry.sessionId, entry.state]),
);

alone("resumes the newest recorded session that can still air, and ends the rest", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const now = (yield* Clock.currentTimeMillis) / 1000;
    // Recorded first and ending last; then one ending sooner; then one with under a minute left.
    const newest = yield* leftover({ endsAt: now + 280 });
    const sooner = yield* leftover({ endsAt: now + 200 });
    const spent = yield* leftover({ endsAt: now + 30 });
    const ledger = yield* Ledger.Ledger;
    const source = yield* ledger.source(yield* opener);
    assert.strictEqual(source.sessionId, newest.sessionId);
    const state = yield* states;
    assert.deepStrictEqual(
      [state.get(sooner.sessionId), state.get(spent.sessionId)],
      ["CLOSED", "CLOSED"],
    );
    assert.deepStrictEqual(yield* ledger.entries, [newest]);
  }),
);

alone("never resumes a session whose end was asked", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const asked = yield* leftover({ state: "ending" });
    const ledger = yield* Ledger.Ledger;
    const source = yield* ledger.source(yield* opener);
    assert.notStrictEqual(source.sessionId, asked.sessionId);
    assert.strictEqual((yield* states).get(asked.sessionId), "CLOSED");
    assert.deepStrictEqual(yield* held, [[source.sessionId, "live"]]);
  }),
);

// Every DELETE is accepted and ignored, so the leftover runs on until its 90 s cap ends it.
alone("waits, allocating nothing, while a recorded session's end is unconfirmed", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
    const test = yield* ReactorTest.ReactorTest;
    const coordinator = yield* CoordinatorClient.CoordinatorClient;
    const capped = coordinator.tokens({
      apiKey: test.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: "90 seconds",
      expiresAfter: "10 minutes",
    });
    const unended = yield* leftover({}, capped);
    yield* test.inject({ _tag: "IgnoreDelete" });
    const ledger = yield* Ledger.Ledger;
    const fresh = yield* opener;
    const first = yield* Effect.forkScoped(ledger.source(fresh, { resume: false }));
    // While the leftover runs: nothing allocated, its entry `ending`, and a second source waiting.
    yield* Effect.sleep("30 seconds");
    const second = yield* Effect.forkScoped(ledger.source(fresh, { resume: false }));
    yield* Effect.sleep("30 seconds");
    assert.lengthOf(yield* test.sessions, 1);
    assert.deepStrictEqual(yield* held, [[unended.sessionId, "ending"]]);
    const opened = [yield* Fiber.join(first), yield* Fiber.join(second)];
    assert.strictEqual((yield* states).get(unended.sessionId), "CLOSED");
    assert.deepStrictEqual(
      yield* held,
      opened.map((source) => [source.sessionId, "live"]),
    );
    // Each fresh session was allocated only once the leftover had ended.
    const log = yield* test.log;
    const at = (sessionId: string, name: string) =>
      log.find((entry) => entry.sessionId === sessionId && entry.name === name)?.at;
    const ended = at(unended.sessionId, "expired") ?? Number.POSITIVE_INFINITY;
    for (const source of opened)
      assert.isAtLeast(at(source.sessionId, "created") ?? Number.NEGATIVE_INFINITY, ended);
  }),
);

// Every DELETE is accepted and ignored, and the ledger asks only once more before it gives up.
alone(
  "allocates nothing once a recorded session's end stops unconfirmed",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const unended = yield* leftover();
      yield* test.inject({ _tag: "IgnoreDelete" });
      const ledger = yield* Ledger.Ledger;
      const refused = yield* Effect.flip(ledger.source(yield* opener, { resume: false }));
      assert.deepStrictEqual(
        [refused.reason._tag, refused.cleanup.allocation],
        ["Indeterminate", "none"],
      );
      assert.lengthOf(yield* test.sessions, 1);
      assert.deepStrictEqual(yield* held, [[unended.sessionId, "ending"]]);
    }),
  Ledger.layerMemory({ ending: Schedule.recurs(1) }),
);

// The session reads STOPPING for 20 s after each DELETE.
alone("ends again until confirmed a session whose close Reactor did not confirm", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
    const test = yield* ReactorTest.ReactorTest;
    const ledger = yield* Ledger.Ledger;
    const source = yield* ledger.source(yield* opener);
    yield* test.inject({ _tag: "SlowDelete", for: Duration.seconds(20) });
    const report = yield* source.close;
    assert.isFalse(report.remote.confirmed);
    assert.deepStrictEqual(yield* held, [[source.sessionId, "ending"]]);
    // Long enough for the session to close and for the ledger to ask again after it has.
    yield* Effect.sleep("1 minute");
    assert.strictEqual((yield* states).get(source.sessionId), "CLOSED");
    assert.deepStrictEqual(yield* held, []);
  }),
);

alone("ends a session whose connection failed after it was recorded", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
    const test = yield* ReactorTest.ReactorTest;
    const ledger = yield* Ledger.Ledger;
    const ledgerOpener = yield* opener;
    yield* test.inject({ _tag: "RefuseConnect", nth: 1 });
    const refused = yield* Effect.flip(ledger.source(ledgerOpener));
    assert.deepStrictEqual([refused._tag, refused.reason._tag], ["AcquisitionFailure", "Http"]);
    assert.strictEqual((yield* states).get(refused.cleanup.sessionId ?? ""), "CLOSED");
    assert.deepStrictEqual(yield* held, []);
    // An end Reactor takes 20 s to honour leaves the entry `ending` until it is confirmed.
    yield* test.inject({ _tag: "SlowDelete", for: Duration.seconds(20) });
    yield* test.inject({ _tag: "RefuseConnect", nth: 1 });
    const slow = yield* Effect.flip(ledger.source(ledgerOpener));
    const sessionId = slow.cleanup.sessionId ?? "";
    assert.deepStrictEqual(yield* held, [[sessionId, "ending"]]);
    yield* Effect.sleep("1 minute");
    assert.strictEqual((yield* states).get(sessionId), "CLOSED");
    assert.deepStrictEqual(yield* held, []);
  }),
);

/** A store that records nothing: every `put` fails. */
const unwritable = Layer.succeed(
  Ledger.Store,
  Ledger.Store.of({
    entries: Effect.succeed([]),
    put: () =>
      Effect.fail(
        Ledger.LedgerError.make({ operation: "put", message: "the disk is full", cause: "full" }),
      ),
    remove: () => Effect.void,
  }),
);

alone(
  "ends before it connects a session it cannot record",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const ledger = yield* Ledger.Ledger;
      const failure = yield* Effect.flip(ledger.source(yield* opener));
      assert.strictEqual(failure._tag, "AcquisitionFailure");
      const sessions = yield* test.sessions;
      assert.deepStrictEqual(
        sessions.map((info) => info.state),
        ["CLOSED"],
      );
      const connected = (yield* test.log).filter(
        (entry) => entry.kind === "session" && entry.name === "connected",
      );
      assert.lengthOf(connected, 0);
      assert.deepStrictEqual(yield* ledger.entries, []);
    }),
  Ledger.layer().pipe(Layer.provideMerge(unwritable)),
);

alone("settles only its own coordinator's sessions", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const elsewhere = yield* leftover({ apiUrl: "https://elsewhere.reactor.test" });
    const ledger = yield* Ledger.Ledger;
    const source = yield* ledger.source(yield* opener);
    assert.notStrictEqual(source.sessionId, elsewhere.sessionId);
    assert.deepStrictEqual(yield* ledger.release, { ended: [], ending: [] });
    const other = (yield* test.sessions).find((info) => info.id === elsewhere.sessionId);
    assert.deepStrictEqual([other?.state, other?.deletes], ["ACTIVE", 0]);
    assert.deepStrictEqual((yield* ledger.entries)[0], elsewhere);
  }),
);

alone("release ends what this ledger does not hold", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const ledger = yield* Ledger.Ledger;
    const airing = yield* ledger.source(yield* opener);
    const stray = yield* leftover();
    assert.deepStrictEqual(yield* ledger.release, { ended: [stray.sessionId], ending: [] });
    const sessions = yield* test.sessions;
    const kept = sessions.find((info) => info.id === airing.sessionId);
    assert.deepStrictEqual([kept?.state, kept?.connected], ["ACTIVE", true]);
    assert.strictEqual(sessions.find((info) => info.id === stray.sessionId)?.state, "CLOSED");
    assert.deepStrictEqual(yield* held, [[airing.sessionId, "live"]]);
  }),
);

/** A CoordinatorClient with neither the key nor a token: Reactor refuses its every end. */
const keyless = Layer.effect(CoordinatorClient.CoordinatorClient, CoordinatorClient.make({}));

alone(
  "fails at once when its coordinator cannot end sessions",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* leftover({ state: "ending" });
      const ledger = yield* Ledger.Ledger;
      const refused = yield* Effect.flip(ledger.source(yield* opener));
      assert.deepStrictEqual(
        [
          refused.reason._tag,
          refused.reason._tag === "Http" ? refused.reason.status : undefined,
          refused.cleanup.allocation,
        ],
        ["Http", 401, "none"],
      );
      assert.lengthOf(yield* test.sessions, 1);
    }),
  Ledger.layerMemory(),
  keyless,
);

layer(
  Layer.mergeAll(
    NodeFileSystem.layer,
    NodePath.layer,
    CoordinatorClient.layer().pipe(Layer.provide(ReactorTest.layerCoordinator({ timing }))),
  ),
)("a file ledger", (it) => {
  it.effect("reads back what it wrote, and fails to read what does not decode", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const file = path.join(yield* fs.makeTempDirectoryScoped(), "channel", "ledger.json");
      const storeAt = Effect.map(Layer.build(Ledger.layerFile(file)), Context.get(Ledger.Store));
      const first = yield* storeAt;
      assert.deepStrictEqual(yield* first.entries, []);
      const apiUrl = "https://api.reactor.inc";
      const capped: Ledger.Entry = {
        sessionId: "sess_capped",
        model: H3.modelName,
        apiUrl,
        endsAt: 1_790_815_443.619,
        state: "live",
      };
      const uncapped: Ledger.Entry = {
        sessionId: "sess_uncapped",
        model: H3.modelName,
        apiUrl,
        state: "live",
      };
      yield* first.put(capped);
      yield* first.put(uncapped);
      yield* first.put({ ...capped, state: "ending" });
      const second = yield* storeAt;
      assert.deepStrictEqual(yield* second.entries, [{ ...capped, state: "ending" }, uncapped]);
      yield* fs.writeFileString(file, "not a ledger");
      const unreadable = yield* Effect.flip(second.entries);
      assert.deepStrictEqual([unreadable._tag, unreadable.operation], ["LedgerError", "entries"]);
    }),
  );
});
