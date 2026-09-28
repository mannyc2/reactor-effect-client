/** A connected H3 session as a playout source, on the simulated Reactor. */
import { assert, layer } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { Effect, Exit, FileSystem, Layer, Option, Path, Stream } from "effect";
import { Coordinator, H3Source, Reactor, ReactorTest } from "../src/index.js";
import { PeerFactory } from "../src/Peer.js";
import { tokens } from "./fixtures/Simulated.js";

/** ReactorTest's peers, but their decoded video dies, as a host with a bug would. */
const dyingVideo = Layer.effect(
  PeerFactory,
  Effect.gen(function* () {
    const peers = yield* PeerFactory;
    return PeerFactory.of({
      check: peers.check,
      make: Effect.map(peers.make, (peer) =>
        peer.media._tag === "Decoded"
          ? { ...peer, media: { ...peer.media, video: () => Stream.die("a host's frame bug") } }
          : peer,
      ),
    });
  }),
);

// A retired generation's failure ends its frames until the next one is ready; a bug stays a defect.
layer(
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(dyingVideo),
    Layer.provideMerge(
      ReactorTest.layer({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }),
    ),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  ),
)("a source's video", (it) => {
  it.effect("carries a host's defect in its frames, rather than going quiet", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const source = yield* H3Source.open({ tokens: yield* tokens });
      const ended = yield* source.video.pipe(
        Stream.runDrain,
        Effect.exit,
        Effect.timeoutOption("1 minute"),
      );
      assert.isTrue(Option.isSome(ended) && Exit.hasDies(ended.value));
    }),
  );
});
