import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import { TestClock } from "effect/testing";
import { StatsSampler } from "../src/stats.js";
import { assert, equal, run, test, throws } from "./harness.js";
import { makeSession, withFixture } from "./fixtures.js";
const pair = (sent: number | bigint, received: number | bigint) => ({
  id: "pair",
  type: "candidate-pair",
  nominated: true,
  state: "succeeded",
  bytesSent: sent,
  bytesReceived: received,
});
test("stats policy: real counter rates use elapsed time and a minimum observation interval", () => {
  const s = new StatsSampler();
  equal(s.sample([pair(0, 0)], 1n, 0).rates, undefined);
  equal(s.sample([pair(10, 20)], 1n, 100).rates, undefined);
  equal(s.sample([pair(1000, 2000)], 1n, 1000).rates, {
    sentBitsPerSecond: 8000,
    receivedBitsPerSecond: 16000,
    intervalMs: 1000,
  });
});
test("stats policy: generation changes and decreasing counters reset the rate baseline", () => {
  const s = new StatsSampler();
  s.sample([pair(100, 100)], 1n, 0);
  equal(s.sample([pair(200, 200)], 2n, 1000).rates, undefined);
  equal(s.sample([pair(0, 0)], 2n, 2000).rates, undefined);
  s.reset();
  equal(s.sample([pair(1, 1)], 2n, 3000).rates, undefined);
});
test("stats policy: unsafe numeric counters are flagged, never reconstructed as precise bigint", () => {
  const sample = new StatsSampler().sample([pair(Number.MAX_SAFE_INTEGER + 1, 0)], 1n, 0);
  equal(sample.pair?.bytesSent, undefined);
  assert(sample.warnings.length > 0);
});
test("stats policy: signed packet-loss correction is preserved without a negative loss ratio", () => {
  const sample = new StatsSampler().sample(
    [
      pair(0, 0),
      {
        type: "inbound-rtp",
        kind: "video",
        packetsLost: -3,
        packetsReceived: 40,
        jitter: 0.01,
        framesPerSecond: 20,
      },
    ],
    1n,
    0,
  );
  equal(sample.totalPacketsLost, -3n);
  equal(sample.lossRatio, 0);
  equal(sample.jitterSeconds, 0.01);
  equal(sample.framesPerSecond, 20);
});
test("stats policy: missing nominated succeeded pair is explicit and bounded input is validated", () => {
  const sampler = new StatsSampler();
  assert(sampler.sample([{ ...pair(1, 1), nominated: false }], 1n, 0).warnings.length > 0);
  throws(
    () =>
      sampler.sample(
        Array.from({ length: 4097 }, () => ({})),
        1n,
        0,
      ),
    "Protocol",
  );
  throws(() => sampler.sample([], 1n, NaN), "Protocol");
});

test("session stats use the injected monotonic clock across wall jumps and reconnect reset", () =>
  withFixture(async (fixture) => {
    const { session, peers } = makeSession(fixture);
    try {
      await run(session.start());
      const firstPeer = peers[0];
      assert(firstPeer !== undefined, "first peer missing");
      firstPeer.statsEntries = [pair(0, 0)];

      const observations = Effect.gen(function* () {
        const first = yield* session.stats();

        yield* TestClock.adjust("1 second");
        firstPeer.statsEntries = [pair(1000, 2000)];
        const afterSecond = yield* session.stats();

        const wallBeforeJump = yield* Clock.currentTimeMillis;
        const monotonicBeforeJump = yield* Clock.monotonicTimeNanos;
        yield* TestClock.setTime(-50_000);
        const wallAfterJump = yield* Clock.currentTimeMillis;
        const monotonicAfterJump = yield* Clock.monotonicTimeNanos;

        firstPeer.statsEntries = [pair(1500, 3000)];
        const afterWallJump = yield* session.stats();
        yield* TestClock.adjust("1 second");
        firstPeer.statsEntries = [pair(2000, 4000)];
        const afterElapsedSecond = yield* session.stats();

        yield* session.reconnect();
        const secondPeer = peers[1];
        if (secondPeer === undefined) {
          return yield* Effect.die(new Error("reconnect peer missing"));
        }
        secondPeer.statsEntries = [pair(5000, 9000)];
        const afterReconnect = yield* session.stats();
        yield* TestClock.adjust("1 second");
        secondPeer.statsEntries = [pair(6000, 11_000)];
        const afterReconnectSecond = yield* session.stats();

        return {
          first,
          afterSecond,
          wallBeforeJump,
          monotonicBeforeJump,
          wallAfterJump,
          monotonicAfterJump,
          afterWallJump,
          afterElapsedSecond,
          afterReconnect,
          afterReconnectSecond,
        };
      });
      const observed = await Effect.runPromise(
        observations.pipe(Effect.provide(TestClock.layer())),
      );
      const expectedRates = {
        sentBitsPerSecond: 8000,
        receivedBitsPerSecond: 16000,
        intervalMs: 1000,
      };

      equal(observed.first.generation, 1n);
      equal(observed.first.sampledAtMs, 0);
      equal(observed.first.rates, undefined);
      equal(observed.afterSecond.sampledAtMs, 1000);
      equal(observed.afterSecond.rates, expectedRates);
      equal(observed.wallBeforeJump, 1000);
      equal(observed.wallAfterJump, -50_000);
      equal(observed.monotonicAfterJump, observed.monotonicBeforeJump);
      equal(observed.afterWallJump.sampledAtMs, 1000);
      equal(observed.afterWallJump.rates, undefined);
      equal(observed.afterElapsedSecond.sampledAtMs, 2000);
      equal(observed.afterElapsedSecond.rates, expectedRates);
      equal(observed.afterReconnect.generation, 2n);
      equal(observed.afterReconnect.sampledAtMs, 2000);
      equal(observed.afterReconnect.rates, undefined);
      equal(observed.afterReconnectSecond.sampledAtMs, 3000);
      equal(observed.afterReconnectSecond.rates, expectedRates);
    } finally {
      await run(session.close());
    }
  }));
