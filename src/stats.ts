import { ReactorError } from "./errors.js";
import { isRecord } from "./json.js";
export interface Statistics {
  /** Injected monotonic clock in milliseconds, not wall time or an RTC sample timestamp. */
  readonly sampledAtMs: number;
  readonly generation: bigint;
  readonly pair?: { readonly id: string; readonly bytesSent?: bigint; readonly bytesReceived?: bigint; readonly localCandidateType?: string; readonly remoteCandidateType?: string; readonly availableOutgoingBitrate?: number; readonly availableIncomingBitrate?: number };
  readonly rates?: { readonly sentBitsPerSecond: number; readonly receivedBitsPerSecond: number; readonly intervalMs: number };
  readonly jitterSeconds?: number;
  readonly lossRatio?: number;
  readonly totalPacketsLost?: bigint;
  readonly framesPerSecond?: number;
  readonly roundTripTimeSeconds?: number;
  readonly targetBitrate?: number;
  readonly warnings: readonly string[];
}
interface Baseline { readonly key: string; readonly at: number; readonly sent: bigint; readonly received: bigint }
const number = (u: unknown): number | undefined => typeof u === "number" && Number.isFinite(u) ? u : undefined;
const positive = (u: unknown): number | undefined => { const n = number(u); return n !== undefined && n > 0 ? n : undefined; };
const text = (u: unknown): string | undefined => typeof u === "string" ? u : undefined;
/** Browser counters are JS numbers. Unsafe integers are flagged, never falsely converted back to full precision. */
export class StatsSampler {
  private baseline: Baseline | undefined;
  reset(): void { this.baseline = undefined; }
  sample(raw: readonly unknown[], generation: bigint, atMs: number): Statistics {
    if (!Number.isFinite(atMs) || raw.length > 4096) throw new ReactorError("Protocol", "invalid statistics sample/bound");
    const warnings: string[] = [], entries = raw.filter(isRecord), byId = new Map(entries.flatMap((e) => typeof e.id === "string" ? [[e.id, e] as const] : []));
    const counter = (u: unknown, field: string, signed = false): bigint | undefined => {
      if (u === undefined) return undefined;
      const n = typeof u === "bigint" ? u : typeof u === "number" && Number.isSafeInteger(u) ? BigInt(u) : undefined;
      if (n === undefined || (signed ? n < -(1n << 63n) || n >= 1n << 63n : n < 0n || n > 0xffffffffffffffffn)) { warnings.push(`${field}: counter precision/range unavailable`); return undefined; }
      return n;
    };
    const pair = entries.find((e) => e.type === "candidate-pair" && e.nominated === true && e.state === "succeeded");
    let pairResult: Statistics["pair"], rates: Statistics["rates"];
    if (pair !== undefined && typeof pair.id === "string") {
      const sent = counter(pair.bytesSent, "pair.bytesSent"), received = counter(pair.bytesReceived, "pair.bytesReceived");
      const local = byId.get(text(pair.localCandidateId) ?? ""), remote = byId.get(text(pair.remoteCandidateId) ?? "");
      const localType = text(local?.candidateType), remoteType = text(remote?.candidateType);
      const out = positive(pair.availableOutgoingBitrate), incoming = positive(pair.availableIncomingBitrate);
      pairResult = { id: pair.id, ...(sent === undefined ? {} : { bytesSent: sent }), ...(received === undefined ? {} : { bytesReceived: received }),
        ...(localType === undefined ? {} : { localCandidateType: localType }), ...(remoteType === undefined ? {} : { remoteCandidateType: remoteType }),
        ...(out === undefined ? {} : { availableOutgoingBitrate: out }), ...(incoming === undefined ? {} : { availableIncomingBitrate: incoming }) };
      if (sent !== undefined && received !== undefined) {
        const key = `${generation}:${pair.id}`, previous = this.baseline;
        if (previous !== undefined && previous.key === key && atMs >= previous.at && sent >= previous.sent && received >= previous.received) {
          const dt = atMs - previous.at;
          if (dt >= 200) { rates = { sentBitsPerSecond: Number(sent - previous.sent) * 8000 / dt, receivedBitsPerSecond: Number(received - previous.received) * 8000 / dt, intervalMs: dt }; this.baseline = { key, at: atMs, sent, received }; }
          // Sub-200ms samples intentionally keep the previous baseline.
        } else this.baseline = { key, at: atMs, sent, received };
      } else this.baseline = undefined;
    } else { this.baseline = undefined; warnings.push("no nominated AND succeeded candidate pair"); }
    const inbound = entries.filter((e) => e.type === "inbound-rtp"), firstVideo = inbound.find((e) => e.kind === "video" || e.mediaType === "video");
    const measured = firstVideo === undefined ? inbound : [firstVideo];
    let ratioLost = 0n, ratioReceived = 0n, ratioValid = measured.length > 0;
    let totalLost = 0n, totalValid = inbound.length > 0;
    for (const entry of inbound) { const lost = counter(entry.packetsLost, "packetsLost", true); if (lost === undefined) totalValid = false; else totalLost += lost; }
    for (const entry of measured) {
      const lost = counter(entry.packetsLost, "loss ratio packetsLost", true), received = counter(entry.packetsReceived, "loss ratio packetsReceived");
      if (lost === undefined || received === undefined) ratioValid = false; else { ratioLost += lost > 0n ? lost : 0n; ratioReceived += received; }
    }
    const jitterSamples = measured.map((e) => number(e.jitter)).filter((n): n is number => n !== undefined && n >= 0);
    const jitter = jitterSamples.length ? Math.max(...jitterSamples) : undefined;
    const ratio = ratioValid ? ratioLost + ratioReceived === 0n ? 0 : Number(ratioLost) / Number(ratioLost + ratioReceived) : undefined;
    const fps = firstVideo === undefined ? undefined : positive(firstVideo.framesPerSecond);
    const fallbackRtts = entries.map((e) => (e.type === "remote-inbound-rtp" || e.type === "outbound-rtp") ? positive(e.roundTripTime) : undefined).filter((n): n is number => n !== undefined);
    const rtt = positive(pair?.currentRoundTripTime) ?? (fallbackRtts.length ? Math.max(...fallbackRtts) : undefined);
    const targets = entries.filter((e) => e.type === "outbound-rtp").map((e) => positive(e.targetBitrate)).filter((n): n is number => n !== undefined);
    return Object.freeze({ sampledAtMs: atMs, generation,
      ...(pairResult === undefined ? {} : { pair: Object.freeze(pairResult) }), ...(rates === undefined ? {} : { rates: Object.freeze(rates) }),
      ...(jitter === undefined ? {} : { jitterSeconds: jitter }), ...(ratio === undefined ? {} : { lossRatio: ratio }),
      ...(totalValid ? { totalPacketsLost: totalLost } : {}), ...(fps === undefined ? {} : { framesPerSecond: fps }),
      ...(rtt === undefined ? {} : { roundTripTimeSeconds: rtt }), ...(targets.length ? { targetBitrate: targets.reduce((a, b) => a + b, 0) } : {}), warnings: Object.freeze(warnings) });
  }
}
