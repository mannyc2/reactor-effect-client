/** One session through its public contract, on a simulated Reactor with the timing each case states. */
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, Redacted } from "effect";
import * as H3 from "../src/H3.js";
import { Coordinator, Reactor, ReactorTest } from "../src/index.js";
import { connect, environment } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({ buildSpeed: 2.4, channel: "10 millis" });

layer(environment({ timing }))("replies", (it) => {
  it.effect(
    "a provider's error reply fails the command as replied, its text out of the message",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const session = yield* connect;
        const failure = yield* Effect.flip(session.command("no_such_command", {}));
        assert.deepStrictEqual(
          [failure.context.outcome, failure.reason._tag, failure.context.operation],
          ["replied", "Remote", "no_such_command"],
        );
        const text = failure.reason._tag === "Remote" ? failure.reason.body : undefined;
        assert.strictEqual(
          text === undefined ? undefined : Redacted.value(text),
          "no_such_command",
        );
        assert.notInclude(failure.message, "no_such_command");
      }),
  );

  it.effect(
    "a recording request on a deployment without a recorder fails as RecorderDisabled",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const session = yield* connect;
        const failure = yield* Effect.flip(session.requestRecordingClip(5));
        assert.strictEqual(failure.reason._tag, "RecorderDisabled");
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
      yield* Effect.sleep("2 seconds");
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
