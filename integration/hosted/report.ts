/**
 * The evidence as a person reads it: one table of runs, then what each run
 * saw. It is what a pull request, the changelog or an upstream report quotes.
 */
import { confirmedOwnedCleanup, rejudged } from "./evidence.js";
import type { Evidence, SchedulerRenewal, SpanRecord } from "./evidence.js";

const seconds = (ms: number | undefined): string =>
  ms === undefined ? "?" : `${(ms / 1000).toFixed(2)} s`;
const usd = (amount: number | undefined): string =>
  amount === undefined ? "?" : `$${amount.toFixed(3)}`;
const counts = (record: Readonly<Record<string, number>>): string => {
  const entries = Object.entries(record);
  return entries.length === 0 ? "none" : entries.map(([key, n]) => `${key} ${n}`).join(", ");
};
const median = (values: readonly number[]): number | undefined => {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

/**
 * The canonical owned-termination proof the renewal verdict requires, over the
 * source's close report or, after a failed open, its lease's own report.
 */
const ownedTermination = (slot: SchedulerRenewal["allocations"][number]): boolean => {
  const cleanup =
    slot.cleanup ??
    (slot.leaseCleanup === undefined ? undefined : { lease: slot.leaseCleanup, policy: [] });
  return (
    slot.sessionId !== undefined &&
    cleanup !== undefined &&
    confirmedOwnedCleanup(cleanup, slot.sessionId)
  );
};

/** The connect span's phases, each as the time since the one before. */
const phases = (spans: readonly SpanRecord[]): string | undefined => {
  const connect = spans.find((span) => span.name === "reactor.session.connect");
  if (connect === undefined || connect.events.length === 0) return undefined;
  let previous = connect.startMs;
  return connect.events
    .map((event) => {
      const step = `${event.name.replace("reactor.connect.", "")} +${seconds(event.atMs - previous)}`;
      previous = event.atMs;
      return step;
    })
    .join(", ");
};

const section = (evidence: Evidence): string => {
  const lines: string[] = [];
  const add = (label: string, value: string | undefined) => {
    if (value !== undefined) lines.push(`- **${label}:** ${value}`);
  };
  const env = evidence.environment;
  const native = env.native;
  add(
    "Environment",
    [
      ...Object.entries(env.packages).map(([name, version]) => `${name} ${version}`),
      native === undefined
        ? undefined
        : `native ${String(native.webrtcPrebuilt)} ABI ${String(native.abiVersion)}`,
      env.runtime,
      env.os,
      env.commit === undefined
        ? undefined
        : `commit ${env.commit.slice(0, 12)}${env.dirty === true ? " (dirty)" : ""}`,
    ]
      .filter((part) => part !== undefined)
      .join(", "),
  );
  add("Network", env.network);
  const server = evidence.server;
  if (server !== undefined)
    add(
      "Server",
      `cluster ${server.cluster ?? "?"}, zone ${server.zone ?? "?"}, version ${server.serverVersion ?? "?"}, transport ${server.transport ?? "?"}`,
    );
  add(
    "Cost",
    `worst case ${usd(evidence.budget.worstCaseUsd)}, estimated ${usd(evidence.budget.estimatedUsd)}${evidence.budget.rate === undefined ? "" : ` at ${evidence.budget.rate.creditsPerSecond} credits/s and ${evidence.budget.rate.creditsPerDollar} credits/$`}`,
  );
  add(
    "Timeline",
    evidence.milestones
      .map((milestone) => `${milestone.step} ${seconds(milestone.atMs)}`)
      .join(" · "),
  );
  add("Connect phases", phases(evidence.spans));
  const acceptance = evidence.acceptance;
  const lifecycle = evidence.lifecycle;
  if (acceptance !== undefined)
    add(
      "Clip",
      [
        `accepted ${seconds(acceptance.acceptedMs - acceptance.submitMs)} after submit (${acceptance.evidence})`,
        lifecycle?.generated === undefined
          ? undefined
          : `generated +${seconds(lifecycle.generated.atMs - acceptance.acceptedMs)}`,
        lifecycle?.started === undefined
          ? undefined
          : `started +${seconds(lifecycle.started.atMs - acceptance.acceptedMs)}`,
        evidence.media?.firstClipFrameMs === undefined || lifecycle?.started === undefined
          ? undefined
          : `first clip frame +${seconds(evidence.media.firstClipFrameMs - lifecycle.started.atMs)} after start`,
        lifecycle?.ended === undefined
          ? undefined
          : `ended +${seconds(lifecycle.ended.atMs - acceptance.acceptedMs)}`,
        `metadata echoed by ${counts(acceptance.metadataEchoes)}`,
        acceptance.references === undefined
          ? undefined
          : `sent ${acceptance.references.images} image(s) and ${acceptance.references.audio} audio; the clip reports ${acceptance.references.reportedImages ?? "?"} image(s), ${acceptance.references.reportedAudio ?? "?"} audio (has_reference_audio ${acceptance.references.hasReferenceAudio ?? "?"})`,
      ]
        .filter((part) => part !== undefined)
        .join("; "),
    );
  const video = evidence.media?.video ?? evidence.takeover?.video;
  if (video !== undefined)
    add(
      "Video",
      `${video.frames} frames, ${video.sizes.join("/")} ${video.formats.join("/")}, ${video.fps ?? "?"} fps, interval p50 ${video.interval?.p50 ?? "?"} / p95 ${video.interval?.p95 ?? "?"} / max ${video.interval?.max ?? "?"} ms, ${video.lost} lost in ${video.gaps} gaps, ${video.lit} lit, ${video.distinct} distinct, metadata on ${video.withMetadata}`,
    );
  const audio = evidence.media?.audio;
  if (audio !== undefined)
    add(
      "Audio",
      `${audio.blocks} blocks, ${audio.sampleRates.join("/")} Hz, ${audio.channels.join("/")} channel(s), ${audio.samplesPerBlock.join("/")} samples a block, peak RMS ${audio.peakRms}, ${audio.lost} lost`,
    );
  else if (evidence.media !== undefined && !evidence.media.audioOffered)
    add("Audio", "not offered");
  const pressure = evidence.media?.pressure;
  if (pressure !== undefined)
    add(
      "Pressure",
      `delivered ${pressure.deliveredVideo} video / ${pressure.deliveredAudio} audio, dropped ${pressure.droppedVideo} / ${pressure.droppedAudio}, reader overflows ${pressure.readerOverflows}`,
    );
  const network = evidence.network;
  if (network !== undefined) {
    const rtt = median(
      network.samples.flatMap((sample) => (sample.rttMs === undefined ? [] : [sample.rttMs])),
    );
    const kbps = median(
      network.samples.flatMap((sample) =>
        sample.receivedKbps === undefined ? [] : [sample.receivedKbps],
      ),
    );
    // The native host names only the local candidate of a pair.
    const pair =
      network.pair === undefined
        ? "no pair"
        : network.pair.remote === null
          ? `local ${network.pair.local ?? "?"}`
          : `${network.pair.local ?? "?"} to ${network.pair.remote}`;
    add(
      "Network path",
      `${pair}, RTT median ${rtt ?? "?"} ms, received median ${kbps ?? "?"} kbps over ${network.samples.length} samples`,
    );
  }
  const contract = evidence.contract;
  if (contract !== undefined)
    add(
      "Contract",
      `${contract.deploymentTitle ?? "untitled"} ${contract.deploymentVersion ?? "?"} (documented ${contract.documentedVersion})${contract.referenceAudio === undefined ? "" : `; reference_audios ${contract.referenceAudio ? "declared" : "not declared"}`}; messages ${counts(contract.messages)}; unknown ${counts(contract.unknown)}; ${contract.duplicates} duplicate, ${contract.stale} stale; diagnostics ${counts(contract.diagnostics)}`,
    );
  const takeover = evidence.takeover;
  if (takeover !== undefined)
    add(
      "Takeover",
      `attached ${seconds(takeover.attachMs)} after the kill; clip ${takeover.clipIdentified === true ? "identified" : "not identified"}; metadata ${takeover.metadataPreserved === true ? "preserved" : "not preserved"}; ${takeover.enqueuesAfterAttach ?? "?"} enqueue(s) after attach; first fresh frame ${takeover.firstFreshFrameMs === undefined ? "none" : `+${seconds(takeover.firstFreshFrameMs - takeover.killedMs - (takeover.attachMs ?? 0))} after attach`}`,
    );
  const renewal = evidence.schedulerRenewal;
  if (renewal !== undefined) {
    add(
      "Public scheduler renewal",
      `${renewal.configuration.constructor} constructor; ${renewal.openAttempts} open attempts; monotonic clock; ${renewal.fillerRequests} filler requests and ${renewal.fillerEvents.length} filler observations`,
    );
    add(
      "Keyed playback",
      renewal.items
        .map(
          (item) =>
            `${item.key} on ${item.sessionId ?? "unknown source"}: ${item.statuses.map((status) => `${status._tag}@${seconds(status.atMs)}`).join(" → ")}`,
        )
        .join("; "),
    );
    for (const switched of renewal.switches) {
      const handoff = switched.handoff;
      add(
        "Planned switch",
        `${switched.retiringSessionId} → ${handoff?.replacementSessionId ?? "unknown"}: ${handoff?.decision ?? "missing evidence"}; ${handoff?.finalClip._tag === "Observed" ? `${handoff.finalClip.receivedVideoFrames}/${handoff.finalClip.expectedVideoFrames} local final-clip frames` : "no observed start"}; ${handoff?.grace._tag === "Observed" ? `${handoff.grace.origin} grace ${handoff.grace.elapsedMs}/${handoff.grace.limitMs} ms` : "grace not observed"}`,
      );
    }
    const boundary = renewal.media.decodedBoundary;
    add(
      "Logical decoded media",
      `${renewal.media.video.frames} video frames; attribution ${renewal.media.attributionComplete ? "complete" : "INCOMPLETE"}; ${boundary === undefined ? "no decoded boundary" : `boundary gap ${boundary.gapMs} ms`}; audio completeness unverified; no encoded or viewer-output claim`,
    );
    add(
      "Accepted drain",
      `${renewal.drain?.outcome ?? "not requested"}; allocated sources ${renewal.drain?.allocationsWhenRequested ?? "?"} → ${renewal.drain?.allocationsWhenCompleted ?? "?"}`,
    );
    const cleanup = renewal.cleanup;
    const summary = cleanup?._tag === "Continuous" ? cleanup.summary : undefined;
    // Complete only when nothing is left open: the observation's own list, a
    // close that never returned, any allocated lease without canonical
    // confirmation, any unknown allocation, and incomplete or exhausted
    // retained cleanup.
    const open =
      cleanup === undefined
        ? ["not recorded"]
        : [
            ...cleanup.incomplete,
            ...(cleanup.completedMs === undefined ? ["close did not return"] : []),
            ...renewal.allocations
              .filter((slot) => slot.sessionId !== undefined && !ownedTermination(slot))
              .map((slot) => `source ${slot.slot} lease unconfirmed`),
            ...renewal.allocations
              .filter((slot) => slot.allocation === "unknown")
              .map((slot) => `source ${slot.slot} allocation unknown`),
            ...(summary?.retained.some((row) => row.disposition === "incomplete") === true
              ? ["incomplete retained cleanup"]
              : []),
            ...(summary?.exhausted === true ? ["retention exhausted"] : []),
          ];
    add(
      "Renewal cleanup",
      `${cleanup?._tag ?? "missing"}; ${renewal.allocations.filter((slot) => slot.cleanup !== undefined).length} canonical source reports; ${open.length === 0 ? "complete" : open.join(", ")}`,
    );
    if (renewal.configuration.constructor === "continuous")
      add(
        "Continuous retention",
        `keep ${renewal.configuration.retainedSuccessfulCleanups} successes; unresolved limit ${renewal.configuration.maxUnresolvedCleanups}`,
      );
    if (summary !== undefined)
      add(
        "Cleanup summary",
        `${summary.totalRetirements} retirements; ${summary.retained.length} retained (${summary.retained.filter((row) => row.disposition === "incomplete").length} incomplete); ${summary.omittedComplete.ownedTerminated} omitted complete owned terminations; exhausted ${summary.exhausted}`,
      );
    for (const slot of renewal.allocations)
      add(
        `Source ${slot.slot}`,
        `${slot.sessionId ?? (slot.allocation === "unknown" ? "allocation outcome unknown" : "not allocated")}; canonical owned termination ${ownedTermination(slot) ? "confirmed" : "UNCONFIRMED"}; cap expiry ${slot.capEndsAt ?? "not recorded"}`,
      );
  }
  const termination = evidence.termination;
  if (termination !== undefined)
    add(
      "Termination",
      `${termination.confirmed ? "confirmed" : "NOT confirmed"}; coordinator terminal ${termination.terminalMs === undefined ? "never seen" : `+${seconds(termination.terminalMs - termination.requestedMs)}`} after the request; trail ${termination.trail.map((entry) => `${entry.state}@+${seconds(entry.atMs - termination.requestedMs)}`).join(" > ") || "empty"}`,
    );
  add(
    "Criteria",
    evidence.criteria
      .map((criterion) => `${criterion.passed ? "✓" : "✗"} ${criterion.name}`)
      .join(" · "),
  );
  for (const reason of evidence.reasons) lines.push(`  - ${reason}`);
  if (evidence.cleanup !== undefined) add("Cleanup", evidence.cleanup);
  return [
    `### ${evidence.check}: ${evidence.verdict ?? "unfinished"} (${evidence.mode}, run ${evidence.runId.slice(0, 8)}, ${evidence.startedAt})`,
    "",
    ...lines,
  ].join("\n");
};

export const summarize = (evidence: readonly Evidence[]): string => {
  // A stored renewal pass renders only while its own evidence still supports it.
  const runs = evidence.map(rejudged);
  const table = [
    "| Run | Check | Mode | Verdict | Started | Worst case | Estimated |",
    "|---|---|---|---|---|---|---|",
    ...runs.map(
      (run) =>
        `| ${run.runId.slice(0, 8)} | ${run.check} | ${run.mode} | ${run.verdict ?? "unfinished"} | ${run.startedAt} | ${usd(run.budget.worstCaseUsd)} | ${usd(run.budget.estimatedUsd)} |`,
    ),
  ];
  return [table.join("\n"), ...runs.map(section)].join("\n\n").replaceAll("\n\n\n", "\n\n");
};
