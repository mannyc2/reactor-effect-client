/** A ledger's runs as Markdown, for `summary.md` and the release notes. */
import type { Evidence } from "./Evidence.js";
import { cleanupInstructions } from "./Evidence.js";

const seconds = (ms: number) => `${(ms / 1000).toFixed(2)} s`;
const usd = (value: number | undefined) => (value === undefined ? "–" : `$${value.toFixed(3)}`);

const measurements = (evidence: Evidence): ReadonlyArray<string> => {
  const lines: Array<string> = [];
  const clip = evidence.clip;
  if (clip !== undefined)
    lines.push(
      `**Clip:** accepted ${clip.acceptance} in ${seconds(clip.acceptedMs - clip.submitMs)}${clip.generatedMs === undefined ? "" : `, generated at ${seconds(clip.generatedMs)}`}${clip.startedMs === undefined ? "" : `, started at ${seconds(clip.startedMs)}`}`,
    );
  const media = evidence.media;
  if (media !== undefined)
    lines.push(
      `**Video:** ${media.video.frames} frames${media.video.fps === undefined ? "" : ` at ${media.video.fps} fps`}, ${media.video.lit} lit, ${media.video.distinct} distinct, ${media.video.lost} lost${media.audio === undefined ? "" : `; audio ${media.audio.blocks} blocks, peak RMS ${media.audio.peakRms}`}`,
    );
  if (evidence.network?.pair !== undefined) lines.push(`**Pair:** ${evidence.network.pair}`);
  const takeover = evidence.takeover;
  if (takeover?.attachMs !== undefined)
    lines.push(
      `**Takeover:** attached ${seconds(takeover.attachMs)} after the kill; playing clip ${takeover.clipIdentified === true ? "identified" : "not identified"}; commands after the kill: ${Object.entries(
        takeover.commands ?? {},
      )
        .map(([name, count]) => `${name} ${count}`)
        .join(", ")}`,
    );
  const queue = evidence.queue;
  if (queue !== undefined) {
    lines.push(`**Builds:** ${queue.builds.map((ms) => seconds(ms)).join(", ")}`);
    for (const [index, boundary] of queue.boundaries.entries())
      lines.push(
        `**Boundary ${index + 1}:** ${boundary.edit} ${boundary.aimMs} ms before the end${boundary.refused === true ? " (refused)" : ""}; next ${boundary.nextClipId === boundary.expectedClipId || boundary.expectedClipId === undefined ? "as expected" : "unexpected"}${boundary.pause === undefined ? "" : `; pause ${boundary.pause.durationMs} ms (${boundary.pause.frames} frames, ${boundary.pause.dark} dark)`}`,
      );
  }
  const playout = evidence.playout;
  if (playout !== undefined) {
    lines.push(`**Order:** started ${playout.startOrder.join(", ")}`);
    for (const seam of playout.seams)
      lines.push(
        `**Seam ${seam.ending} to ${seam.next}${seam.continued ? " (continued)" : ""}:** ${seam.pause === undefined ? "no pause measured" : `pause ${seam.pause.durationMs} ms (${seam.pause.frames} frames, ${seam.pause.dark} dark)`}; ${seam.darkFrames ?? 0} dark frames${seam.jump === undefined ? "" : `; join change ${seam.jump.change} against ${seam.jump.typical} (×${seam.jump.ratio})`}`,
      );
    if (playout.batch !== undefined)
      lines.push(
        `**Batch:** ${playout.batch.committedMs === undefined ? "never took effect" : `took effect ${seconds(playout.batch.committedMs - playout.batch.submittedMs)} after it was sent${playout.batch.boundaryMs === undefined ? "" : `, ${seconds(playout.batch.boundaryMs - playout.batch.committedMs)} before its boundary`}`}`,
      );
    for (const change of playout.switches ?? [])
      lines.push(`**Switch:** at ${seconds(change.atMs)}, ${change.decision}`);
    if (playout.stops !== undefined) lines.push(`**Stops:** ${playout.stops}`);
  }
  return lines;
};

/** One run as a section. */
const section = (evidence: Evidence): string => {
  const environment = evidence.environment;
  const packages = Object.entries(environment.packages)
    .map(([name, version]) => `${name} ${version}`)
    .join(", ");
  return [
    `### ${evidence.check}: ${evidence.verdict ?? "unfinished"} (${evidence.mode}, run ${evidence.runId}, ${evidence.startedAt})`,
    "",
    `- **Environment:** ${packages}, ${environment.runtime}, ${environment.os}${environment.commit === undefined ? "" : `, commit ${environment.commit.slice(0, 8)}${environment.dirty === true ? " (dirty)" : ""}`}`,
    ...(environment.native === undefined
      ? []
      : [
          `- **Native addon:** ${environment.native.platform ?? "?"} sha256 ${(environment.native.sha256 ?? "").slice(0, 8)}, source ${(environment.native.sourceSha256 ?? "").slice(0, 8)}, ${environment.native.webrtcPrebuilt ?? "?"}`,
        ]),
    `- **Network:** ${environment.network}`,
    `- **Cost:** worst case ${usd(evidence.budget.worstCaseUsd)}, estimated ${usd(evidence.budget.estimatedUsd)}${evidence.budget.rate === undefined ? "" : ` at ${evidence.budget.rate.creditsPerSecond} credits/s, billed per ${evidence.budget.rate.per}, and ${evidence.budget.rate.creditsPerDollar} credits/$`}`,
    `- **Timeline:** ${evidence.milestones.map((milestone) => `${milestone.step} ${seconds(milestone.atMs)}`).join(" · ")}`,
    ...measurements(evidence).map((line) => `- ${line}`),
    `- **Termination:** ${evidence.sessions.map((session) => `${session.id} ${session.close?.confirmed === true ? "confirmed" : "unconfirmed"}${session.trail.length === 0 ? "" : ` (trail ${session.trail.map((entry) => entry.state).join(" > ")})`}`).join("; ")}`,
    `- **Criteria:** ${evidence.criteria.map((criterion) => `${criterion.passed ? "✓" : "✗"} ${criterion.name}`).join(" · ")}`,
    ...evidence.reasons.map((reason) => `  - ${reason}`),
    ...cleanupInstructions(evidence).map((line) => `- **Cleanup:** ${line}`),
  ].join("\n");
};

/** The runs as a table, then a section each. */
export const summarize = (runs: ReadonlyArray<Evidence>): string =>
  [
    "| Run | Check | Mode | Verdict | Started | Worst case | Estimated |",
    "| --- | ----- | ---- | ------- | ------- | ---------- | --------- |",
    ...runs.map(
      (run) =>
        `| ${run.runId} | ${run.check} | ${run.mode} | ${run.verdict ?? "unfinished"} | ${run.startedAt} | ${usd(run.budget.worstCaseUsd)} | ${usd(run.budget.estimatedUsd)} |`,
    ),
    "",
    ...runs.map(section),
    "",
  ].join("\n");
