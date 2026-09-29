import * as Predicate from "effect/Predicate";
export interface Statistics {
  /** Injected monotonic clock in milliseconds, not wall time or an RTC sample timestamp. */
  readonly sampledAtMs: number;
  readonly generation: bigint;
  /**
   * The candidate pair carrying the connection: the one a transport names as
   * selected or else, of the pairs nominated that succeeded, the one that
   * received the most since the previous sample. A pair stays nominated after
   * ICE moves off it, and the native host names no selected pair.
   */
  readonly pair?: {
    readonly id: string;
    readonly bytesSent?: bigint;
    readonly bytesReceived?: bigint;
    readonly localCandidateType?: string;
    readonly remoteCandidateType?: string;
    readonly availableOutgoingBitrate?: number;
    readonly availableIncomingBitrate?: number;
  };
  /**
   * Over the interval since the session's previous sample, whoever took it,
   * and absent from a sample taken within 200 ms of that one. Every caller of
   * `stats` on one session shares the baseline: of two monitors polling it
   * 100 ms apart, one never gets rates.
   */
  readonly rates?: {
    readonly sentBitsPerSecond: number;
    readonly receivedBitsPerSecond: number;
    readonly intervalMs: number;
  };
  readonly jitterSeconds?: number;
  readonly lossRatio?: number;
  readonly totalPacketsLost?: bigint;
  readonly framesPerSecond?: number;
  readonly roundTripTimeSeconds?: number;
  readonly targetBitrate?: number;
  readonly warnings: readonly string[];
}
export interface Baseline {
  readonly key: string;
  readonly at: number;
  readonly sent: bigint;
  readonly received: bigint;
}
const number = (u: unknown): number | undefined =>
  Predicate.isNumber(u) && Number.isFinite(u) ? u : undefined;
const positive = (u: unknown): number | undefined => {
  const n = number(u);
  return n !== undefined && n > 0 ? n : undefined;
};
const text = (u: unknown): string | undefined => (Predicate.isString(u) ? u : undefined);
/** A whole number as a bigint, when it is one exactly. */
const whole = (u: unknown): bigint | undefined => {
  if (Predicate.isBigInt(u)) return u;
  return Predicate.isNumber(u) && Number.isSafeInteger(u) ? BigInt(u) : undefined;
};
/** A byte counter as a bigint, when it is one exactly; only for choosing a pair. */
const bytes = (u: unknown): bigint | undefined => {
  if (Predicate.isBigInt(u)) return u;
  const n = whole(u);
  return n !== undefined && n >= 0n ? n : undefined;
};
/** A counter's growth since `prior`; one below it belongs to a reset pair, which grew by all of it. */
const growth = (count: bigint | undefined, prior: bigint | undefined): bigint => {
  if (count === undefined) return 0n;
  return prior === undefined || count < prior ? count : count - prior;
};
type Entry = { readonly [x: PropertyKey]: unknown };
type Pair = Entry & { readonly id: string };
const isPair = (e: Entry): e is Pair => e.type === "candidate-pair" && Predicate.isString(e.id);
/**
 * One pair across the samples of a generation. The native host names pairs by
 * their position in its report, which shifts when a pair is added or dropped.
 * A pair's priority, fixed by its two candidates, and its local candidate
 * identify it instead; where a host reports no priority, its id does.
 */
const pairKey = (generation: bigint, pair: Pair, byId: ReadonlyMap<string, Entry>): string => {
  const priority = pair.priority;
  if (!Predicate.isBigInt(priority) && !Predicate.isNumber(priority))
    return `${generation}:id:${pair.id}`;
  const local = byId.get(text(pair.localCandidateId) ?? "");
  return `${generation}:priority:${priority}:${text(local?.candidateType) ?? ""}:${text(local?.relayProtocol) ?? ""}`;
};
/** What one generation's previous sample leaves for the next. */
export interface SamplerState {
  readonly baseline: Baseline | undefined;
  /** Each candidate pair's received bytes at the previous sample, by `pairKey`. */
  readonly received: ReadonlyMap<string, bigint>;
  /** The pair the previous sample reported, by the same key. */
  readonly chosen: string | undefined;
}

export const initialSampler: SamplerState = {
  baseline: undefined,
  received: new Map(),
  chosen: undefined,
};

/**
 * The pair carrying the connection. A transport's `selectedCandidatePairId`
 * names it where the host reports one, as browsers do. Otherwise it is the
 * nominated, succeeded pair that received the most since the previous sample:
 * ICE leaves a pair nominated after moving off it. A tie keeps the pair
 * reported before, and otherwise goes to the first.
 */
const selectedPair = (
  state: SamplerState,
  entries: readonly Entry[],
  byId: ReadonlyMap<string, Entry>,
  generation: bigint,
): {
  readonly pair: Pair | undefined;
  readonly received: ReadonlyMap<string, bigint>;
  readonly chosen: string | undefined;
} => {
  const pairs = entries.filter(isPair);
  const key = (pair: Pair) => pairKey(generation, pair, byId);
  const received = new Map(
    pairs.flatMap((pair) => {
      const count = bytes(pair.bytesReceived);
      return count === undefined ? [] : [[key(pair), count] as const];
    }),
  );
  let chosen: Pair | undefined;
  for (const transport of entries) {
    if (transport.type !== "transport") continue;
    const named = byId.get(text(transport.selectedCandidatePairId) ?? "");
    if (named !== undefined && isPair(named)) {
      chosen = named;
      break;
    }
  }
  let most = -1n;
  if (chosen === undefined)
    for (const pair of pairs) {
      if (pair.nominated !== true || pair.state !== "succeeded") continue;
      const grown = growth(bytes(pair.bytesReceived), state.received.get(key(pair)));
      if (grown > most || (grown === most && key(pair) === state.chosen)) {
        chosen = pair;
        most = grown;
      }
    }
  return { pair: chosen, received, chosen: chosen === undefined ? undefined : key(chosen) };
};

/**
 * One statistics sample and the state the next one compares against. Browser
 * counters are JS numbers: an unsafe integer is flagged, never falsely
 * converted back to full precision.
 */
export const sample = (input: {
  readonly state: SamplerState;
  readonly raw: ReadonlyArray<unknown>;
  readonly generation: bigint;
  /** Monotonic milliseconds. */
  readonly atMs: number;
}): readonly [Statistics, SamplerState] => {
  const { state, raw, generation, atMs } = input;
  const warnings: string[] = [],
    entries = raw.filter(Predicate.isObject),
    byId = new Map(entries.flatMap((e) => (Predicate.isString(e.id) ? [[e.id, e] as const] : [])));
  const counter = (u: unknown, field: string, signed = false): bigint | undefined => {
    if (u === undefined) return undefined;
    const n = whole(u);
    if (
      n === undefined ||
      (signed ? n < -(1n << 63n) || n >= 1n << 63n : n < 0n || n > 0xffffffffffffffffn)
    ) {
      warnings.push(`${field}: counter precision/range unavailable`);
      return undefined;
    }
    return n;
  };
  const selection = selectedPair(state, entries, byId, generation);
  const pair = selection.pair;
  let baseline = state.baseline;
  let pairResult: Statistics["pair"], rates: Statistics["rates"];
  if (pair !== undefined) {
    const sent = counter(pair.bytesSent, "pair.bytesSent"),
      received = counter(pair.bytesReceived, "pair.bytesReceived");
    const local = byId.get(text(pair.localCandidateId) ?? ""),
      remote = byId.get(text(pair.remoteCandidateId) ?? "");
    const localType = text(local?.candidateType),
      remoteType = text(remote?.candidateType);
    const out = positive(pair.availableOutgoingBitrate),
      incoming = positive(pair.availableIncomingBitrate);
    pairResult = {
      id: pair.id,
      ...(sent === undefined ? {} : { bytesSent: sent }),
      ...(received === undefined ? {} : { bytesReceived: received }),
      ...(localType === undefined ? {} : { localCandidateType: localType }),
      ...(remoteType === undefined ? {} : { remoteCandidateType: remoteType }),
      ...(out === undefined ? {} : { availableOutgoingBitrate: out }),
      ...(incoming === undefined ? {} : { availableIncomingBitrate: incoming }),
    };
    if (sent !== undefined && received !== undefined) {
      const key = pairKey(generation, pair, byId),
        previous = state.baseline;
      if (
        previous?.key === key &&
        atMs >= previous.at &&
        sent >= previous.sent &&
        received >= previous.received
      ) {
        const dt = atMs - previous.at;
        if (dt >= 200) {
          rates = {
            sentBitsPerSecond: (Number(sent - previous.sent) * 8000) / dt,
            receivedBitsPerSecond: (Number(received - previous.received) * 8000) / dt,
            intervalMs: dt,
          };
          baseline = { key, at: atMs, sent, received };
        }
        // Sub-200ms samples intentionally keep the previous baseline.
      } else baseline = { key, at: atMs, sent, received };
    } else baseline = undefined;
  } else {
    baseline = undefined;
    warnings.push("no nominated AND succeeded candidate pair");
  }
  const inbound = entries.filter((e) => e.type === "inbound-rtp"),
    firstVideo = inbound.find((e) => e.kind === "video" || e.mediaType === "video");
  const measured = firstVideo === undefined ? inbound : [firstVideo];
  let ratioLost = 0n,
    ratioReceived = 0n,
    ratioValid = measured.length > 0;
  let totalLost = 0n,
    totalValid = inbound.length > 0;
  for (const entry of inbound) {
    const lost = counter(entry.packetsLost, "packetsLost", true);
    if (lost === undefined) totalValid = false;
    else totalLost += lost;
  }
  for (const entry of measured) {
    const lost = counter(entry.packetsLost, "loss ratio packetsLost", true),
      received = counter(entry.packetsReceived, "loss ratio packetsReceived");
    if (lost === undefined || received === undefined) ratioValid = false;
    else {
      ratioLost += lost > 0n ? lost : 0n;
      ratioReceived += received;
    }
  }
  const jitterSamples = measured
    .map((e) => number(e.jitter))
    .filter((n): n is number => n !== undefined && n >= 0);
  const jitter = jitterSamples.length > 0 ? Math.max(...jitterSamples) : undefined;
  const counted = ratioLost + ratioReceived;
  const lostShare = counted === 0n ? 0 : Number(ratioLost) / Number(counted);
  const ratio = ratioValid ? lostShare : undefined;
  const fps = firstVideo === undefined ? undefined : positive(firstVideo.framesPerSecond);
  const fallbackRtts = entries
    .map((e) =>
      e.type === "remote-inbound-rtp" || e.type === "outbound-rtp"
        ? positive(e.roundTripTime)
        : undefined,
    )
    .filter((n): n is number => n !== undefined);
  const rtt =
    positive(pair?.currentRoundTripTime) ??
    (fallbackRtts.length > 0 ? Math.max(...fallbackRtts) : undefined);
  const targets = entries
    .filter((e) => e.type === "outbound-rtp")
    .map((e) => positive(e.targetBitrate))
    .filter((n): n is number => n !== undefined);
  const statistics: Statistics = {
    sampledAtMs: atMs,
    generation,
    ...(pairResult === undefined ? {} : { pair: pairResult }),
    ...(rates === undefined ? {} : { rates }),
    ...(jitter === undefined ? {} : { jitterSeconds: jitter }),
    ...(ratio === undefined ? {} : { lossRatio: ratio }),
    ...(totalValid ? { totalPacketsLost: totalLost } : {}),
    ...(fps === undefined ? {} : { framesPerSecond: fps }),
    ...(rtt === undefined ? {} : { roundTripTimeSeconds: rtt }),
    ...(targets.length > 0 ? { targetBitrate: targets.reduce((a, b) => a + b, 0) } : {}),
    warnings,
  };
  return [statistics, { baseline, received: selection.received, chosen: selection.chosen }];
};
