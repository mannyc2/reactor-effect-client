/** A connected H3 session as a playout source, on the simulated Reactor. */
import { assert, layer } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Path,
  Stream,
} from "effect";
import { Coordinator, H3Source, Reactor, ReactorTest } from "../src/index.js";
import { PeerFactory } from "../src/Peer.js";
import { ReactorError } from "../src/ReactorError.js";
import type { Session } from "../src/Session.js";
import { environment, tokens } from "./fixtures/Simulated.js";

/** A source on the simulated key, and the session it opened, from its owner record. */
const opened = Effect.gen(function* () {
  const allocated = yield* Deferred.make<Session>();
  const source = yield* H3Source.open({
    tokens: yield* tokens,
    onAllocated: ({ session }) => Deferred.succeed(allocated, session),
  });
  return { source, session: yield* Deferred.await(allocated) };
});

// Allocating and connecting take over 3 s here, of a session capped at five minutes.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      http: "100 millis",
      allocation: "2 seconds",
      negotiation: "500 millis",
      connect: "500 millis",
    }),
  }),
)("a source's lifetime", (it) => {
  it.effect("is what remains of its cap when open returns, counted from the request", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const started = yield* Clock.currentTimeMillis;
      const source = yield* H3Source.open({ tokens: yield* tokens });
      const elapsed = (yield* Clock.currentTimeMillis) - started;
      const lifetime = Duration.toMillis(source.lifetime);
      assert.isBelow(lifetime, 300_000 - 3_000);
      assert.isAtLeast(lifetime, 300_000 - elapsed);
    }),
  );
});

/** Each block's first connection drops 5 s after it opens, and its session goes on. */
const dropped = environment({
  timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
  faults: [{ _tag: "Disconnect", nth: 1, after: Duration.seconds(5) }],
});

/** The session ready again on a later connection generation, if that happens within a minute. */
const reconnected = (session: Session) =>
  session.changes.pipe(
    Stream.filter((snapshot) => snapshot.status === "ready" && snapshot.generation > 1n),
    Stream.runHead,
    Effect.timeoutOption("1 minute"),
    Effect.map(Option.flatten),
  );

layer(dropped)("a dropped connection nobody reads the source's events for", (it) => {
  it.effect("is recovered by the source itself", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { session } = yield* opened;
      assert.isTrue(Option.isSome(yield* reconnected(session)));
    }),
  );
});

layer(dropped)("a dropped connection two readers of the source's events see", (it) => {
  it.effect("is recovered once, and reported to each", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { source, session } = yield* opened;
      const reader = source.events.pipe(
        Stream.filter((event) => event._tag === "Reconnecting" || event._tag === "Reconnected"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped({ startImmediately: true }),
      );
      const readers = [yield* reader, yield* reader];
      assert.isTrue(Option.isSome(yield* reconnected(session)));
      const [first, second] = yield* Fiber.joinAll(readers);
      assert.deepStrictEqual(
        first?.map((event) => event._tag),
        ["Reconnecting", "Reconnected"],
      );
      assert.deepStrictEqual(second, first);
      assert.strictEqual((yield* session.snapshot).generation, 2n);
    }),
  );
});

/** The simulated Reactor, its peers' decoded video replaced by `video`, as a host's might be. */
const hostVideo = (video: Stream.Stream<never, ReactorError>) =>
  Reactor.layer().pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(
      Layer.effect(
        PeerFactory,
        Effect.gen(function* () {
          const peers = yield* PeerFactory;
          return PeerFactory.of({
            check: peers.check,
            make: Effect.map(peers.make, (peer) =>
              peer.media._tag === "Decoded"
                ? { ...peer, media: { ...peer.media, video: () => video } }
                : peer,
            ),
          });
        }),
      ),
    ),
    Layer.provideMerge(
      ReactorTest.layer({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }),
    ),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  );

/** How a source's video ends, if it ends within a minute. */
const videoEnd = Effect.gen(function* () {
  yield* Effect.forkScoped(ReactorTest.flow());
  const source = yield* H3Source.open({ tokens: yield* tokens });
  return yield* source.video.pipe(Stream.runDrain, Effect.exit, Effect.timeoutOption("1 minute"));
});

// A retired generation's failure ends its frames until the next one is ready; a bug stays a defect.
layer(hostVideo(Stream.die("a host's frame bug")))("a source's video", (it) => {
  it.effect("carries a host's defect in its frames, rather than going quiet", () =>
    Effect.gen(function* () {
      const ended = yield* videoEnd;
      assert.isTrue(Option.isSome(ended) && Exit.hasDies(ended.value));
    }),
  );
});

// A host refuses a track it cannot decode with UnsupportedCapability, as a browser host, with
// platform tracks and no decoded media, refuses them all.
layer(
  hostVideo(Stream.fail(ReactorError.fromCode("UnsupportedCapability", "no decoded video here"))),
)("a source's video on a host that cannot decode it", (it) => {
  it.effect("fails with the host's refusal, rather than going quiet", () =>
    Effect.gen(function* () {
      const ended = yield* videoEnd;
      const failure = Option.flatMap(ended, Exit.findErrorOption);
      assert.strictEqual(Option.getOrUndefined(failure)?.reason._tag, "UnsupportedCapability");
    }),
  );
});
