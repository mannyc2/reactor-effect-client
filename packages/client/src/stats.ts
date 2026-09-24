import * as Predicate from "effect/Predicate";
import { ReactorError } from "./errors.js";
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
interface Baseline {
  readonly key: string;
  readonly at: number;
  readonly sent: bigint;
  readonly received: bigint;
}
const number = (u: unknown): number | undefined =>
  typeof u === "number" && Number.isFinite(u) ? u : undefined;
const positive = (u: unknown): number | undefined => {
  const n = number(u);
  return n !== undefined && n > 0 ? n : undefined;
};
const text = (u: unknown): string | undefined => (typeof u === "string" ? u : undefined);
/** A byte counter as a bigint, when it is one exactly; only for choosing a pair. */
const bytes = (u: unknown): bigint | undefined =>
  typeof u === "bigint"
    ? u
    : typeof u === "number" && Number.isSafeInteger(u) && u >= 0
      ? BigInt(u)
      : undefined;
type Entry = { readonly [x: PropertyKey]: unknown };
type Pair = Entry & { readonly id: string };
const isPair = (e: Entry): e is Pair => e.type === "candidate-pair" && typeof e.id === "string";
/**
 * One pair across the samples of a generation. The native host names pairs by
 * their position in its report, which shifts when a pair is added or dropped.
 * A pair's priority, fixed by its two candidates, and its local candidate
 * identify it instead; where a host reports no priority, its id does.
 */
const pairKey = (generation: bigint, pair: Pair, byId: ReadonlyMap<string, Entry>): string => {
  const priority = pair.priority;
  if (typeof priority !== "bigint" && typeof priority !== "number")
    return `${generation}:id:${pair.id}`;
  const local = byId.get(text(pair.localCandidateId) ?? "");
  return `${generation}:priority:${priority}:${text(local?.candidateType) ?? ""}:${text(local?.relayProtocol) ?? ""}`;
};
/** Browser counters are JS numbers. Unsafe integers are flagged, never falsely converted back to full precision. */
export class StatsSampler {
  private baseline: Baseline | undefined;
  /** Each candidate pair's received bytes at the previous sample, by `pairKey`. */
  private received = new Map<string, bigint>();
  /** The pair the previous sample reported, by the same key. */
  private chosen: string | undefined;
  reset(): void {
    this.baseline = undefined;
    this.received = new Map();
    this.chosen = undefined;
  }
  /**
   * The pair carrying the connection. A transport's `selectedCandidatePairId`
   * names it where the host reports one, as browsers do. Otherwise it is the
   * nominated, succeeded pair that received the most since the previous
   * sample: ICE leaves a pair nominated after moving off it, and only the pair
   * in use carries media and the channels. A tie keeps the pair reported
   * before, and otherwise goes to the first.
   */
  private selectedPair(
    entries: readonly Entry[],
    byId: ReadonlyMap<string, Entry>,
    generation: bigint,
  ): Pair | undefined {
    const pairs = entries.filter(isPair);
    const key = (pair: Pair) => pairKey(generation, pair, byId);
    const previous = this.received,
      before = this.chosen;
    this.received = new Map(
      pairs.flatMap((pair) => {
        const received = bytes(pair.bytesReceived);
        return received === undefined ? [] : [[key(pair), received] as const];
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
        const received = bytes(pair.bytesReceived),
          prior = previous.get(key(pair));
        // A count below the last one belongs to a reset pair: it grew by all of it.
        const growth =
          received === undefined
            ? 0n
            : prior === undefined || received < prior
              ? received
              : received - prior;
        if (growth > most || (growth === most && key(pair) === before)) {
          chosen = pair;
          most = growth;
        }
      }
    this.chosen = chosen === undefined ? undefined : key(chosen);
    return chosen;
  }
  sample(raw: readonly unknown[], generation: bigint, atMs: number): Statistics {
    if (!Number.isFinite(atMs) || raw.length > 4096)
      throw ReactorError.fromCode("Protocol", "invalid statistics sample/bound");
    const warnings: string[] = [],
      entries = raw.filter(Predicate.isObject),
      byId = new Map(
        entries.flatMap((e) => (typeof e.id === "string" ? [[e.id, e] as const] : [])),
      );
    const counter = (u: unknown, field: string, signed = false): bigint | undefined => {
      if (u === undefined) return undefined;
      const n =
        typeof u === "bigint"
          ? u
          : typeof u === "number" && Number.isSafeInteger(u)
            ? BigInt(u)
            : undefined;
      if (
        n === undefined ||
        (signed ? n < -(1n << 63n) || n >= 1n << 63n : n < 0n || n > 0xffffffffffffffffn)
      ) {
        warnings.push(`${field}: counter precision/range unavailable`);
        return undefined;
      }
      return n;
    };
    const pair = this.selectedPair(entries, byId, generation);
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
          previous = this.baseline;
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
            this.baseline = { key, at: atMs, sent, received };
          }
          // Sub-200ms samples intentionally keep the previous baseline.
        } else this.baseline = { key, at: atMs, sent, received };
      } else this.baseline = undefined;
    } else {
      this.baseline = undefined;
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
    const jitter = jitterSamples.length ? Math.max(...jitterSamples) : undefined;
    const ratio = ratioValid
      ? ratioLost + ratioReceived === 0n
        ? 0
        : Number(ratioLost) / Number(ratioLost + ratioReceived)
      : undefined;
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
      (fallbackRtts.length ? Math.max(...fallbackRtts) : undefined);
    const targets = entries
      .filter((e) => e.type === "outbound-rtp")
      .map((e) => positive(e.targetBitrate))
      .filter((n): n is number => n !== undefined);
    return Object.freeze({
      sampledAtMs: atMs,
      generation,
      ...(pairResult === undefined ? {} : { pair: Object.freeze(pairResult) }),
      ...(rates === undefined ? {} : { rates: Object.freeze(rates) }),
      ...(jitter === undefined ? {} : { jitterSeconds: jitter }),
      ...(ratio === undefined ? {} : { lossRatio: ratio }),
      ...(totalValid ? { totalPacketsLost: totalLost } : {}),
      ...(fps === undefined ? {} : { framesPerSecond: fps }),
      ...(rtt === undefined ? {} : { roundTripTimeSeconds: rtt }),
      ...(targets.length ? { targetBitrate: targets.reduce((a, b) => a + b, 0) } : {}),
      warnings: Object.freeze(warnings),
    });
  }
}
