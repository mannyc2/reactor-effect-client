/** One session through its public contract, on a simulated Reactor with the timing each case states. */
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, Redacted } from "effect";
import { ReactorTest } from "../src/index.js";
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
