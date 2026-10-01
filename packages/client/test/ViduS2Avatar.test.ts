/** Vidu S2-Avatar through its provider, on a simulated Reactor at the timing each case states. */
import { assert, layer } from "@effect/vitest";
import { Effect, Fiber, Stream } from "effect";
import { CoordinatorClient, Reactor, ReactorTest, ViduS2Avatar } from "../src/index.js";
import type { CommandFailure } from "../src/ReactorError.js";
import { environment } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({
  buildSpeed: 1,
  channel: "10 millis",
  avatar: "2 seconds",
  call: "4 seconds",
  answer: "1 second",
  speech: "2 seconds",
  hangup: "1 second",
});

const photo = {
  bytes: ReactorTest.pngBytes({ width: 64, height: 64 }),
  type: "image/png",
} as const;
const persona = "You are Tina, a guide at a sea-life museum. Answer in one short sentence.";

/** A connected Vidu S2-Avatar session and its provider. */
const connect = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: ViduS2Avatar.modelName,
    tokens: coordinator.tokens({
      apiKey: test.apiKey,
      modelName: ViduS2Avatar.modelName,
      maxSessionDuration: "5 minutes",
      expiresAfter: "10 minutes",
    }),
  });
  return { session, provider: yield* ViduS2Avatar.make(session) };
});

const refusal = (failure: CommandFailure) =>
  failure.reason._tag === "Refused"
    ? [failure.reason.code, failure.reason.origin, failure.context.outcome]
    : [failure.reason._tag, failure.context.outcome];

layer(environment({ timing }))("a call", (it) => {
  it.effect("goes from a photo to a live call, a spoken answer and its end", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { provider } = yield* connect;
      assert.strictEqual((yield* provider.state).phase, "idle");
      const avatar = yield* provider.createAvatar({ ...photo, name: "Tina" });
      assert.strictEqual(avatar.phase, "avatar_ready");
      assert.strictEqual(avatar.avatar_name, "Tina");
      assert.isNotNull(avatar.avatar_id);

      const transcripts = yield* provider.events().pipe(
        Stream.filter((event) => event._tag === "Transcript"),
        Stream.map((event) => event.transcript.speaker),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const live = yield* provider.startCall({ persona, greeting: "Say hello." });
      assert.deepStrictEqual([live.phase, live.control_ready], ["live", true]);
      yield* provider.say("What lives in the deepest tank?");
      // The caller's line is heard at once; the character speaks its greeting, then its answer.
      assert.deepStrictEqual(yield* Fiber.join(transcripts), ["user", "character", "character"]);

      const ended = yield* provider.endCall;
      assert.strictEqual(ended.end_reason, "ended_by_client");
      assert.isAbove(ended.duration_seconds, 0);
      assert.strictEqual((yield* provider.getState).phase, "ended");
    }),
  );
});

layer(environment({ timing }))("refusals", (it) => {
  it.effect("fail with the model's code, and leave the next command its own answer", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { provider } = yield* connect;
      const early = yield* Effect.flip(provider.say("Hello?"));
      assert.deepStrictEqual(refusal(early), ["NOT_LIVE", "state", "replied"]);
      const noAvatar = yield* Effect.flip(provider.startCall({ persona }));
      assert.deepStrictEqual(refusal(noAvatar), ["NO_AVATAR", "state", "replied"]);
      assert.strictEqual((yield* provider.getState).phase, "idle");
      assert.strictEqual((yield* provider.listVoices).system.length > 0, true);
    }),
  );

  it.effect("refuse input outside the documented ranges before anything is sent", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const { session, provider } = yield* connect;
      const failures = yield* Effect.forEach(
        [
          provider.startCall({ persona, greeting: "x".repeat(201) }),
          provider.startCall({ persona, llm: { temperature: 2 } }),
          provider.updateCall({}),
          provider.say(""),
        ],
        Effect.flip,
      );
      for (const failure of failures)
        assert.deepStrictEqual(refusal(failure), ["InvalidInput", "not-submitted"]);
      const sent = (yield* test.log).filter(
        (entry) =>
          entry.sessionId === session.id && entry.kind === "command" && entry.name !== "get_state",
      );
      assert.deepStrictEqual(sent, []);
    }),
  );
});

layer(environment({ timing }))("avatars", (it) => {
  it.effect("a second avatar in a session is the new one", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { provider } = yield* connect;
      const first = yield* provider.createAvatar(photo);
      const second = yield* provider.createAvatar({ url: "https://example.test/tina.jpg" });
      assert.notStrictEqual(second.avatar_id, first.avatar_id);
    }),
  );

  it.effect("a later session attaches a saved avatar, and an unknown id is not found", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const first = yield* connect;
      const made = yield* first.provider.createAvatar(photo);
      yield* first.session.close;
      const { provider } = yield* connect;
      const attached = yield* provider.attachAvatar(made.avatar_id ?? "");
      assert.deepStrictEqual(
        [attached.phase, attached.avatar_id],
        ["avatar_ready", made.avatar_id],
      );
      const missing = yield* Effect.flip(provider.attachAvatar("avatar_unknown"));
      assert.deepStrictEqual(refusal(missing), ["AVATAR_NOT_FOUND", "request", "replied"]);
    }),
  );
});
