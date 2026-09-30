import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Config, Console, Effect, Layer, Option, Redacted, Stream } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { CoordinatorClient, H3, Reactor, ReactorTest } from "reactor-effect-client";
import { NativePeer } from "reactor-effect-native";

/** One H3 clip from prompt to its end, with its frames decoded in this process. */
const firstClip = Effect.gen(function* () {
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: H3.modelName,
    // The session runs on a token of its own, capped so it never bills past two minutes.
    tokens: coordinator.tokens({ modelName: H3.modelName, maxSessionDuration: "2 minutes" }),
  });
  yield* Console.log(`session ${session.id} connected`);

  const h3 = yield* H3.make(session);
  // H3 plays nothing on its own: play each clip as soon as it is ready.
  yield* h3.setAutoplay(true);
  const media = yield* session.decoded;
  yield* media.video("main_video").pipe(
    Stream.filter((frame) => frame.sequence % 24n === 0n),
    Stream.runForEach((frame) =>
      Console.log(`frame ${frame.sequence}: ${frame.width}x${frame.height} ${frame.format}`),
    ),
    Effect.forkScoped,
  );

  const submission = yield* h3.prepare({
    prompt: "A paper boat drifting down a rain-soaked street at dusk, neon reflections",
    seconds: 5,
  });
  const accepted = yield* submission.submit;
  yield* Console.log(`clip ${accepted.clip.clip_id} accepted`);
  const clip = yield* h3.operation(submission);
  yield* clip.reached("started");
  yield* Console.log("playing");
  yield* clip.ended;
  yield* Console.log("ended");

  const report = yield* session.close;
  yield* Console.log(`session closed, termination confirmed: ${report.remote.confirmed}`);
}).pipe(Effect.scoped);

/** Reactor simulated in memory, at the timing paid runs measured: no key, nothing billed. */
const Simulated = Reactor.layer().pipe(
  Layer.provideMerge(CoordinatorClient.layer({ apiKey: Redacted.make("demo") })),
  Layer.provideMerge(
    ReactorTest.layer({
      timing: ReactorTest.Timing.hosted,
      apiKey: "demo",
      width: 320,
      height: 180,
    }),
  ),
);

/** Hosted Reactor with the key in REACTOR_API_KEY, frames decoded by libwebrtc in this process. */
const Hosted = Reactor.layer().pipe(
  Layer.provideMerge(Layer.mergeAll(CoordinatorClient.layerConfig, NativePeer.layer())),
  Layer.provide(FetchHttpClient.layer),
);

/** The program is the same either way; only this choice differs. */
const Reactors = Layer.unwrap(
  Config.option(Config.Redacted("REACTOR_API_KEY")).pipe(
    Effect.map((key) => (Option.isSome(key) ? Hosted : Simulated)),
  ),
);

firstClip.pipe(
  // The program's entry point, the one place a layer is provided.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Layer.mergeAll(Reactors, NodeServices.layer)),
  NodeRuntime.runMain,
);
