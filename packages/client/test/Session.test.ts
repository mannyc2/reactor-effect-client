/** One session through its public contract, on a simulated Reactor with the timing each case states. */
import { assert, layer } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  Context,
  Deferred,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Inspectable,
  Layer,
  Option,
  Path,
  Redacted,
  Stream,
} from "effect";
import * as H3 from "../src/H3.js";
import { Coordinator, Reactor, ReactorTest } from "../src/index.js";
import * as Wire from "../src/internal/wire.js";
import { PeerFactory } from "../src/Peer.js";
import type { CommandFailure } from "../src/ReactorError.js";
import type { CommandReply, Session } from "../src/Session.js";
import { connect, environment } from "./fixtures/Simulated.js";

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
        assert.isTrue(control._tag === "Control" && control.message._tag === "ClipFailed");
        if (control._tag !== "Control" || control.message._tag !== "ClipFailed") return;
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

layer(environment({ timing }))("reconnection", (it) => {
  it.effect("a dropped connection reconnects on a new generation, and commands work again", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "Disconnect", nth: 1, after: Duration.seconds(1) });
      const session = yield* connect;
      // The fault drops the connection a second after its channels open.
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
      Wire.decode(Wire.ControlClientMessageSchema, bytes).pipe(
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
