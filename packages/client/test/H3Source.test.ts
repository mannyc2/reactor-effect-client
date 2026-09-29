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
  Tracer,
} from "effect";
import { Coordinator, H3Source, Reactor, ReactorTest } from "../src/index.js";
import { PeerFactory } from "../src/Peer.js";
import type { Source, SourceEvent } from "../src/Playout.js";
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

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }))("tracing", (it) => {
  it.effect("names the spans it opens and resumes sessions in after its module", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const names = new Set<string>();
      const tracer = Tracer.make({
        span: (options) => {
          names.add(options.name);
          return new Tracer.NativeSpan(options);
        },
      });
      const sessionTokens = yield* tokens;
      const recorded = yield* Deferred.make<H3Source.Allocation>();
      yield* Effect.gen(function* () {
        yield* H3Source.open({
          tokens: sessionTokens,
          onAllocated: ({ allocation }) => Deferred.succeed(recorded, allocation),
        });
        yield* H3Source.resume({
          allocation: yield* Deferred.await(recorded),
          tokens: sessionTokens,
        });
      }).pipe(Effect.withTracer(tracer));
      assert.includeMembers([...names], ["H3Source.open", "H3Source.resume"]);
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

// Each connection's answer takes 5 s, so a reader can come while the session reconnects.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      http: "20 millis",
      channel: "10 millis",
      negotiation: "5 seconds",
    }),
    faults: [{ _tag: "Disconnect", nth: 1, after: Duration.seconds(5) }],
  }),
)("a reader of the source's events that comes while its session reconnects", (it) => {
  it.effect("is told of the recovery under way, and of its end", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { source, session } = yield* opened;
      yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "connecting"),
        Stream.runHead,
      );
      const read = yield* source.events.pipe(
        Stream.filter((event) => event._tag === "Reconnecting" || event._tag === "Reconnected"),
        Stream.take(2),
        Stream.runCollect,
        Effect.timeoutOption("1 minute"),
      );
      assert.deepStrictEqual(
        Option.map(read, (events) => events.map((event) => event._tag)),
        Option.some(["Reconnecting", "Reconnected"]),
      );
    }),
  );
});

/**
 * A source's events until they end, if they end within `wait`: the recovery it reported, why it
 * ended, and how long after reporting the drop.
 */
const readUntilEnd = (source: Source, wait: Duration.Input) =>
  Effect.gen(function* () {
    const read: Array<readonly [SourceEvent["_tag"], number]> = [];
    const ended = yield* source.events.pipe(
      Stream.runForEach((event) =>
        Effect.map(Clock.currentTimeMillis, (at) => {
          read.push([event._tag, at]);
        }),
      ),
      Effect.exit,
      Effect.timeoutOption(wait),
    );
    const endedAt = yield* Clock.currentTimeMillis;
    const dropped = read.find(([tag]) => tag === "Reconnecting")?.[1];
    return {
      endedAt,
      recovery: read
        .map(([tag]) => tag)
        .filter((tag) => tag === "Reconnecting" || tag === "Reconnected"),
      failure: Option.getOrUndefined(Option.flatMap(ended, Exit.findErrorOption))?.reason._tag,
      afterDrop: dropped === undefined ? undefined : (yield* Clock.currentTimeMillis) - dropped,
    };
  });

layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
    faults: [{ _tag: "Disconnect", nth: 1, after: Duration.seconds(5) }],
    reconnect: false,
  }),
)("a dropped connection its session does not reconnect", (it) => {
  it.effect("loses the source where its readers see the drop, at once, reconnecting nothing", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { source, session } = yield* opened;
      const drop = yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected"),
        Stream.runHead,
        Effect.andThen(Clock.currentTimeMillis),
        Effect.forkScoped,
      );
      const read = yield* readUntilEnd(source, "1 minute");
      assert.deepStrictEqual(
        [read.recovery, read.failure, read.endedAt - (yield* Fiber.join(drop))],
        [[], "Disconnected", 0],
      );
    }),
  );
});

// Each connection's answer takes 5 s, so reconnecting takes longer than the source waits for it.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({
      buildSpeed: 2.4,
      http: "20 millis",
      channel: "10 millis",
      negotiation: "5 seconds",
    }),
    faults: [{ _tag: "Disconnect", nth: 1, after: Duration.seconds(5) }],
  }),
)("a dropped connection its session reconnects after the source's recovery", (it) => {
  it.effect("loses the source at `recovery`, while the session reconnects on its own", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const allocated = yield* Deferred.make<Session>();
      const source = yield* H3Source.open({
        tokens: yield* tokens,
        recovery: "2 seconds",
        onAllocated: ({ session }) => Deferred.succeed(allocated, session),
      });
      const session = yield* Deferred.await(allocated);
      const read = yield* readUntilEnd(source, "1 minute");
      assert.deepStrictEqual([read.recovery, read.failure], [["Reconnecting"], "Timeout"]);
      assert.approximately(read.afterDrop ?? 0, 2_000, 50);
      assert.isTrue(Option.isSome(yield* reconnected(session)));
    }),
  );
});

// The first connection drops 5 s after its channels open, and the second 5 ms after its own do,
// while the source reads H3 afresh on it.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
    faults: [
      { _tag: "Disconnect", nth: 1, after: Duration.seconds(5) },
      { _tag: "Disconnect", nth: 2, after: Duration.millis(5) },
    ],
  }),
)("a reconnected connection that drops while the source reads H3 on it", (it) => {
  it.effect("is not the source's loss: it recovers on the next connection", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { source, session } = yield* opened;
      const read = yield* readUntilEnd(source, "30 seconds");
      const snapshot = yield* session.snapshot;
      assert.deepStrictEqual(
        [read.recovery, read.failure, snapshot.status, snapshot.generation],
        [["Reconnecting", "Reconnected"], undefined, "ready", 3n],
      );
    }),
  );
});

// The first connection drops 5 s after its channels open, and the model never answers the read
// of H3 on the next.
layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, http: "20 millis", channel: "10 millis" }),
    faults: [
      { _tag: "Disconnect", nth: 1, after: Duration.seconds(5) },
      { _tag: "DropReply", nth: 2, command: "get_state" },
    ],
  }),
)("a read of H3 that fails on a reconnected connection still up", (it) => {
  it.effect("loses the source when it fails, without waiting for another connection", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { source } = yield* opened;
      const read = yield* readUntilEnd(source, "1 minute");
      assert.deepStrictEqual([read.recovery, read.failure], [["Reconnecting"], "Timeout"]);
      // At H3's 15 s reply deadline, after the 80 ms the reconnect took: before `recovery` ends.
      assert.approximately(read.afterDrop ?? 0, 15_080, 50);
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
