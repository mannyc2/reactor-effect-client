import { assert, it } from "@effect/vitest";
import { initialSampler, sample } from "../src/internal/stats.js";
import type { SamplerState } from "../src/internal/stats.js";

/** Consecutive samples of one session, as the session keeps them. */
const sampler = () => {
  let state: SamplerState = initialSampler;
  return {
    sample: (raw: ReadonlyArray<unknown>, generation: bigint, atMs: number) => {
      const [statistics, next] = sample({ state, raw, generation, atMs });
      state = next;
      return statistics;
    },
    reset: () => {
      state = initialSampler;
    },
  };
};

const pair = (sent: number | bigint, received: number | bigint) => ({
  id: "pair",
  type: "candidate-pair",
  nominated: true,
  state: "succeeded",
  bytesSent: sent,
  bytesReceived: received,
});
it("stats policy: real counter rates use elapsed time and a minimum observation interval", () => {
  const s = sampler();
  assert.isUndefined(s.sample([pair(0, 0)], 1n, 0).rates);
  assert.isUndefined(s.sample([pair(10, 20)], 1n, 100).rates);
  assert.deepStrictEqual(s.sample([pair(1000, 2000)], 1n, 1000).rates, {
    sentBitsPerSecond: 8000,
    receivedBitsPerSecond: 16000,
    intervalMs: 1000,
  });
});
it("stats policy: generation changes and decreasing counters reset the rate baseline", () => {
  const s = sampler();
  s.sample([pair(100, 100)], 1n, 0);
  assert.isUndefined(s.sample([pair(200, 200)], 2n, 1000).rates);
  const fallen = s.sample([pair(0, 0)], 2n, 2000);
  assert.isUndefined(fallen.rates);
  // A falling counter restarts the pair's baseline; it is still the pair reported.
  assert.deepStrictEqual(fallen.pair?.id, "pair");
  s.reset();
  assert.isUndefined(s.sample([pair(1, 1)], 2n, 3000).rates);
});
it("stats policy: unsafe numeric counters are flagged, never reconstructed as precise bigint", () => {
  const sample = sampler().sample([pair(Number.MAX_SAFE_INTEGER + 1, 0)], 1n, 0);
  assert.isUndefined(sample.pair?.bytesSent);
  assert.isTrue(sample.warnings.length > 0);
});
it("stats policy: signed packet-loss correction is preserved without a negative loss ratio", () => {
  const sample = sampler().sample(
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
  assert.deepStrictEqual(sample.totalPacketsLost, -3n);
  assert.deepStrictEqual(sample.lossRatio, 0);
  assert.deepStrictEqual(sample.jitterSeconds, 0.01);
  assert.deepStrictEqual(sample.framesPerSecond, 20);
});
it("stats policy: a missing nominated, succeeded pair is reported as a warning", () => {
  const samples = sampler();
  assert.isTrue(samples.sample([{ ...pair(1, 1), nominated: false }], 1n, 0).warnings.length > 0);
});
const local = (id: string, candidateType: string) => ({
  id,
  type: "local-candidate",
  candidateType,
});
const nominated = (id: string, localCandidateId: string, received: number) => ({
  id,
  type: "candidate-pair",
  nominated: true,
  state: "succeeded",
  bytesSent: 0,
  bytesReceived: received,
  localCandidateId,
});
it("stats policy: the pair carrying traffic is reported, not a nominated pair ICE moved off", () => {
  const s = sampler();
  // As a hosted session showed: ICE nominated the relay pair first and left it
  // nominated after moving to the direct pair, which then carried the media.
  const report = (stale: number, live: number) => [
    local("relay", "relay"),
    local("prflx", "prflx"),
    nominated("stale", "relay", stale),
    nominated("live", "prflx", live),
  ];
  assert.deepStrictEqual(s.sample(report(4_600, 900), 1n, 0).pair?.localCandidateType, "relay");
  assert.deepStrictEqual(
    s.sample(report(4_600, 1_250_000), 1n, 1000).pair?.localCandidateType,
    "prflx",
  );
  const settled = s.sample(report(4_600, 2_500_000), 1n, 2000);
  assert.deepStrictEqual(settled.pair?.id, "live");
  assert.deepStrictEqual(settled.rates?.receivedBitsPerSecond, 10_000_000);
  // With nothing received since the last sample, the pair reported before stands.
  assert.deepStrictEqual(s.sample(report(4_600, 2_500_000), 1n, 3000).pair?.id, "live");
  // A new generation compares nothing with the last one's counters.
  assert.deepStrictEqual(s.sample(report(9_000, 20), 2n, 4000).pair?.id, "stale");
  s.reset();
  assert.deepStrictEqual(s.sample(report(4_600, 4_600), 2n, 5000).pair?.id, "stale");
});
it("stats policy: a pair keeps its identity when the native host renumbers its pairs", () => {
  const s = sampler();
  // The native host names pairs by position; its priority says which pair it is.
  const native = (
    pairs: readonly { type: string; priority: bigint; received: bigint; nominated?: boolean }[],
  ) =>
    pairs.flatMap(({ type, priority, received, nominated = true }, index) => [
      local(`local-candidate-${index}`, type),
      {
        id: `candidate-pair-${index}`,
        type: "candidate-pair",
        state: nominated ? "succeeded" : "waiting",
        nominated,
        priority,
        bytesSent: 0n,
        bytesReceived: received,
        localCandidateId: `local-candidate-${index}`,
      },
    ]);
  // The relay pair carried the session's first minutes before ICE moved off it.
  const stale = { type: "relay", priority: 100n, received: 50_000_000n };
  const live = (received: bigint) => ({ type: "prflx", priority: 200n, received });
  s.sample(native([stale, live(1_000_000n)]), 1n, 0);
  assert.deepStrictEqual(
    s.sample(native([stale, live(2_000_000n)]), 1n, 1000).pair?.localCandidateType,
    "prflx",
  );
  // A new pair sorts first, and every pair after it moves up a position.
  const added = { type: "host", priority: 300n, received: 0n, nominated: false };
  const shifted = s.sample(native([added, stale, live(3_000_000n)]), 1n, 2000);
  assert.deepStrictEqual(shifted.pair?.localCandidateType, "prflx");
  assert.deepStrictEqual(shifted.rates?.receivedBitsPerSecond, 8_000_000);
});
it("stats policy: a transport's selected pair is reported where the host names one", () => {
  const sample = sampler().sample(
    [
      local("host", "host"),
      local("relay", "relay"),
      { id: "remote", type: "remote-candidate", candidateType: "srflx" },
      nominated("first", "relay", 9_000),
      { ...nominated("second", "host", 10), remoteCandidateId: "remote" },
      { id: "T01", type: "transport", selectedCandidatePairId: "second" },
    ],
    1n,
    0,
  );
  assert.deepStrictEqual(sample.pair?.id, "second");
  assert.deepStrictEqual(sample.pair?.localCandidateType, "host");
  assert.deepStrictEqual(sample.pair?.remoteCandidateType, "srflx");
});
