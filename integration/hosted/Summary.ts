/** A ledger's runs as Markdown, for `summary.md` and the release notes. */
import type { Evidence } from "./Evidence.js";
import { cleanupInstructions } from "./Evidence.js";

const seconds = (ms: number) => `${(ms / 1000).toFixed(2)} s`;
const usd = (value: number | undefined) => (value === undefined ? "–" : `$${value.toFixed(3)}`);

const listed = (names: ReadonlyArray<string>) => names.join(", ") || "none";

/** `tour`'s phases, a line each. */
const tourLines = (tour: NonNullable<Evidence["tour"]>): ReadonlyArray<string> => {
  const lines: Array<string> = [];
  const expires = tour.createExpiresMs;
  lines.push(
    `**Tokens:** ${tour.mints.map((mint) => `${mint.kind}${mint.ownSession ? " (its own id)" : ""} at ${seconds(mint.atMs)} living ${mint.lifetimeSeconds} s${mint.refused === undefined ? "" : `, refused: ${mint.refused}`}`).join("; ")}${expires === undefined ? "" : `; the creating token expired at ${seconds(expires)}, refresh due at ${seconds(tour.refreshDueMs ?? expires)}`}`,
  );
  const call = (label: string, value: NonNullable<typeof tour.refreshCall>) =>
    `${label} ${value.what} at ${seconds(value.startedMs)}, ${value.ok ? "succeeded" : `failed with ${value.failure ?? "?"}`}`;
  if (tour.refreshCall !== undefined || tour.afterExpiryCall !== undefined)
    lines.push(
      `**Calls:** ${[
        ...(tour.refreshCall === undefined
          ? []
          : [call("past the refresh point,", tour.refreshCall)]),
        ...(tour.afterExpiryCall === undefined
          ? []
          : [call("after the expiry,", tour.afterExpiryCall)]),
      ].join("; ")}`,
    );
  if (tour.canvas !== undefined)
    lines.push(
      `**Canvas:** asked ${tour.canvas.requested} (${tour.canvas.listed ? "listed" : "not listed"} in valid_commands); accepted ${tour.canvas.reply === undefined ? "nothing" : `${tour.canvas.reply.aspect} at ${tour.canvas.reply.width}x${tour.canvas.reply.height}`}; state ${tour.canvas.state?.aspect ?? "unread"}`,
    );
  const clip = (value: NonNullable<typeof tour.clip2>) =>
    `accepted ${value.acceptance} in ${seconds(value.acceptedMs - value.submitMs)}${value.generatedMs === undefined ? "" : `, generated at ${seconds(value.generatedMs)}`}${value.startedMs === undefined ? "" : `, started at ${seconds(value.startedMs)}`}${value.ended === undefined ? "" : `, ended by ${value.ended}`}; ${value.references.uploads} upload(s); reports ${String(value.references.reportedImages ?? "no")} image and ${String(value.references.reportedAudio ?? "no")} audio reference(s), has_reference_audio ${String(value.references.hasReferenceAudio)}`;
  if (tour.clip1 !== undefined)
    lines.push(
      `**Clip 1:** ${clip(tour.clip1)}; seed ${tour.clip1.seed.sent} sent, ${String(tour.clip1.seed.echoed)} echoed, default ${String(tour.clip1.seed.defaultBefore)} before and ${String(tour.clip1.seed.defaultAfter)} after; ${tour.clip1.video?.frames ?? 0} frames while it played`,
    );
  if (tour.clip2 !== undefined) lines.push(`**Clip 2:** ${clip(tour.clip2)}`);
  const queue = tour.queue;
  if (queue !== undefined)
    lines.push(
      `**Queue:** generation ${listed(queue.enqueued)}${queue.move === undefined ? "" : `; moved to ${queue.move.queue} ${queue.move.position}, then ${listed(queue.move.generation)}`}; pops ${queue.pops.map((pop) => `${pop.name}${pop.headOfGeneration ? " (heading the generation queue)" : ""}${pop.generatedAfter === true ? " generated after" : ""}${pop.startedAfter === true ? " started after" : ""}`).join(", ") || "none"}${queue.clip2GeneratedAfterPopMs === undefined ? "" : `; clip 2 generated ${seconds(queue.clip2GeneratedAfterPopMs)} after the moved clip's pop`}${queue.after === undefined ? "" : `; refresh ${queue.after.refresh}`}; round trips ${Object.entries(
        queue.replies,
      )
        .map(([name, ms]) => `${name} ${ms} ms`)
        .join(", ")}`,
    );
  const stopPlay = tour.stopPlay;
  if (stopPlay !== undefined)
    lines.push(
      `**Stop and play:** stop (${stopPlay.stop}) cut ${stopPlay.stopped}${stopPlay.stoppedMs === undefined ? "" : `, clip_stopped ${Math.round(stopPlay.stoppedMs - stopPlay.stopSentMs)} ms after it was sent`}; started before the play: ${listed(stopPlay.startedBetween)}; play (${stopPlay.play ?? "unsent"}) of ${stopPlay.played ?? "nothing"}${stopPlay.playStartedMs === undefined || stopPlay.playSentMs === undefined ? "" : ` started ${Math.round(stopPlay.playStartedMs - stopPlay.playSentMs)} ms after it was sent`}`,
    );
  const failed = tour.failedBuild;
  if (failed !== undefined)
    lines.push(
      `**Build past the text budget:** ${failed.promptChars} characters, ended by ${failed.ended ?? "nothing"}${failed.endedMs === undefined || failed.acceptedMs === undefined ? "" : ` ${seconds(failed.endedMs - failed.acceptedMs)} after its acceptance`}; waiting for it to generate failed with ${failed.generatedFailure ?? "nothing"}${failed.clipEnded === undefined ? "" : ` (${failed.clipEnded.lifecycle}, ${failed.clipEnded.sameClip ? "its own clip" : "another clip"})`}; reason ${failed.reasonChars ?? "?"} characters; ${failed.started ? "started" : "never started"}`,
    );
  for (const recording of tour.recordings ?? [])
    lines.push(
      `**Recording, ${recording.request}:** ${recording.outcome}${recording.kind === undefined ? "" : ` (${recording.kind}, markers ${String(recording.markers)}, ready in ${String(recording.readyInMs)} ms)`}${recording.download === undefined ? "" : `; download ${recording.download.outcome} in ${seconds(recording.download.ms)}${recording.download.bytes === undefined ? "" : `, ${recording.download.bytes} bytes in ${String(recording.download.segments)} segment(s)${recording.download.init === true ? " with an init" : ""}`}`}`,
    );
  const reconnect = tour.reconnect;
  if (reconnect !== undefined)
    lines.push(
      `**Reconnect:** generation ${reconnect.generationBefore} to ${reconnect.generationAfter ?? "none"}${reconnect.readyMs === undefined ? "" : ` in ${seconds(reconnect.readyMs - reconnect.startedMs)}`}${expires === undefined ? "" : `, from ${seconds(reconnect.startedMs - expires)} after the creating token expired`}; the long clip ${reconnect.keptClip === true ? "still ready" : "not ready"}; ${reconnect.video === undefined ? "no frames read" : `${reconnect.video.frames} frames${reconnect.firstFreshFrameMs === undefined || reconnect.playStartedMs === undefined ? "" : `, the first ${Math.round(reconnect.firstFreshFrameMs - reconnect.playStartedMs)} ms after the start`}`}; get_state ${reconnect.stateRead === true ? "answered" : "failed"}`,
    );
  const reset = tour.reset;
  if (reset !== undefined) {
    const settings = (value: typeof reset.before) =>
      `${value.aspect}, seed ${value.seed}, autoplay ${value.autoplay ? "on" : "off"}, ${value.queued} queued, ${value.playingClip ?? "nothing"} playing`;
    lines.push(
      `**Reset:** was_playing ${String(reset.wasPlaying)}, ${String(reset.clearedClips)} cleared${reset.stoppedMs === undefined ? "" : `, clip_stopped ${Math.round(reset.stoppedMs - reset.sentMs)} ms after it was sent`}; before ${settings(reset.before)}; after ${reset.after === undefined ? "unread" : settings(reset.after)}`,
    );
  }
  const termination = (value: NonNullable<typeof tour.apiKeyTermination>) =>
    `DELETE ${String(value.deleteStatus ?? "unanswered")}, ${value.confirmed ? `confirmed ${value.state ?? value.evidence ?? ""}` : "unconfirmed"}`;
  if (tour.apiKeyTermination !== undefined)
    lines.push(
      `**Ended:** the API key: ${termination(tour.apiKeyTermination)}; the session's own close: ${tour.ownedClose === undefined ? "none" : termination(tour.ownedClose.remote)}`,
    );
  const afterEnd = tour.afterEnd;
  if (afterEnd !== undefined)
    lines.push(
      `**After the end:** attaching ${afterEnd.attach}${afterEnd.attachStatus === undefined ? "" : ` ${afterEnd.attachStatus}`}; the key reading an unknown session ${afterEnd.inspectUnknown}${afterEnd.inspectStatus === undefined ? "" : ` ${afterEnd.inspectStatus}`}, ending it ${afterEnd.terminateUnknown === undefined ? "unsent" : termination(afterEnd.terminateUnknown)}`,
    );
  if (tour.freeMints.length > 0)
    lines.push(
      `**Free mints:** ${tour.freeMints.map((mint) => `${mint.name}: ${mint.outcome}${mint.maxSessions === undefined ? "" : `, max_sessions ${String(mint.maxSessions)}`}${mint.maxSessionSeconds === undefined ? "" : `, cap ${String(mint.maxSessionSeconds)}`}`).join("; ")}`,
    );
  return lines;
};

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
  const reads = evidence.adopterReads;
  if (reads !== undefined && reads.length > 0)
    lines.push(
      `**Adopter's session reads:** ${reads.map((read) => `+${seconds(read.sinceKillMs)} ${read.status} ${read.state ?? "no state"} (${read.keys.join(", ")})`).join("; ")}`,
    );
  const takeover = evidence.takeover;
  if (takeover?.attachMs !== undefined)
    lines.push(
      `**Takeover:** attached ${seconds(takeover.attachMs)} after it began; playing clip ${takeover.clipIdentified === true ? "identified" : "not identified"}; commands after the kill: ${Object.entries(
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
  const tokens = evidence.tokens;
  if (tokens !== undefined) {
    for (const probe of tokens.probes)
      lines.push(
        `**Probe, ${probe.name}:** ${probe.status}${probe.code === undefined ? "" : ` ${probe.code}`}${probe.lifetimeSeconds === undefined ? "" : `; lives ${probe.lifetimeSeconds} s of ${probe.requestedSeconds ?? "?"} asked`}${
          probe.echo === undefined
            ? ""
            : `; echo ${Object.entries(probe.echo)
                .map(([key, value]) => `${key} ${String(value)}`)
                .join(", ")}`
        }`,
      );
    lines.push(
      `**Tokens:** ${tokens.mints.map((mint) => `${mint.kind} at ${seconds(mint.atMs)} living ${mint.lifetimeSeconds} s`).join("; ")}`,
    );
    if (tokens.resumeStartedMs !== undefined && tokens.createExpiresMs !== undefined)
      lines.push(
        `**Adoption:** started ${seconds(tokens.resumeStartedMs - tokens.createExpiresMs)} after the creating token expired, ${tokens.ownerKilledMs === undefined ? "" : `${seconds(tokens.resumeStartedMs - tokens.ownerKilledMs)} after the owner died, `}attached ${tokens.attachedMs === undefined ? "never" : seconds(tokens.attachedMs - tokens.resumeStartedMs)} later; playing clip ${tokens.clipIdentified === true ? "identified" : "not identified"}`,
      );
    if (tokens.upload !== undefined)
      lines.push(
        `**Refreshed call:** ${tokens.refreshedMs === undefined ? "no refresh" : `refreshed at ${seconds(tokens.refreshedMs)}`}; clip accepted ${tokens.upload.acceptedMs === undefined ? "never" : `in ${seconds(tokens.upload.acceptedMs - tokens.upload.startedMs)}`}, has_reference_audio ${String(tokens.upload.hasReferenceAudio)}`,
      );
    lines.push(
      `**Refusals:** expired token ${tokens.expiredTokenStatus ?? "–"}, unbound token ${tokens.unboundTokenStatus ?? "–"}; API key termination ${tokens.apiKeyTermination === undefined ? "–" : `${tokens.apiKeyTermination.confirmed ? "confirmed" : "unconfirmed"} (DELETE ${String(tokens.apiKeyTermination.deleteStatus)})`}`,
    );
  }
  const show = evidence.show;
  if (show !== undefined) {
    lines.push(
      `**Filler:** ${show.fills.length} clips asked for (${show.fills.map((fill) => `${fill.seconds} s at ${fill.runwaySeconds} s secured`).join(", ")}); ${show.starved.length === 0 ? "never starved" : `starved at ${show.starved.map((atMs) => seconds(atMs)).join(", ")}`}`,
    );
    const longest = [...show.gaps]
      .sort((a, b) => b.toMs - b.fromMs - (a.toMs - a.fromMs))
      .slice(0, 3);
    lines.push(
      `**On air:** ${show.filler.filter((clip) => clip.phase === "Started").length} filler clips started and ${show.filler.filter((clip) => clip.phase === "Ended").length} ended; ${show.gaps.length} gaps between clips${longest.length === 0 ? "" : `, the longest ${longest.map((gap) => `${Math.round(gap.toMs - gap.fromMs)} ms from ${gap.ending} to ${gap.next} at ${seconds(gap.fromMs)}`).join(", ")}`}`,
    );
    lines.push(
      `**State on air:** named ${show.playing.named} clips as they started, ${show.playing.later} read after a later start${show.playing.mismatched.length === 0 ? "" : `; otherwise ${show.playing.mismatched.map((reading) => `${reading.key} at ${seconds(reading.startedMs)} as ${reading.stateKey === undefined ? "none" : `${reading.stateKey} from ${reading.stateStartedMs === undefined ? "?" : seconds(reading.stateStartedMs)}`}`).join(", ")}`}`,
    );
    lines.push(`**Sessions:** ${show.sessions.map((logged) => logged.event).join(" > ")}`);
    lines.push(
      `**Reconnects:** ${show.reconnects.map((reconnect) => `${reconnect.sessionId} at ${seconds(reconnect.reconnectingMs)}, ${reconnect.reconnectedMs === undefined ? "never back" : `back ${seconds(reconnect.reconnectedMs - reconnect.reconnectingMs)} later, measured ${reconnect.afterMillis ?? "?"} ms`}`).join("; ") || "none"}`,
    );
    lines.push(
      `**Readers:** ${show.readerOverflows.length === 0 ? "never fell behind" : show.readerOverflows.map((overflow) => `${overflow.track} of ${overflow.sessionId} at ${seconds(overflow.atMs)} (${overflow.readerOverflows} so far)`).join(", ")}`,
    );
    if (show.failures.length > 0)
      lines.push(
        `**Failed:** ${show.failures.map((failure) => `${failure.key} ${failure.reason}${failure.sessionId === undefined ? "" : ` with ${failure.sessionId}`} at ${seconds(failure.atMs)}`).join(", ")}`,
      );
    const recovery = show.recovery;
    if (recovery !== undefined)
      lines.push(
        `**Recovery:** ${recovery.dropped} connection dropped at ${seconds(recovery.droppedMs)} with ${recovery.runwaySeconds} s secured; ${recovery.statuses.map((status) => status.status).join(" > ")}; ready ${recovery.readyMs === undefined ? "never" : `${seconds(recovery.readyMs - recovery.droppedMs)} later`}, first frame ${recovery.firstFrameMs === undefined ? "never" : `${seconds(recovery.firstFrameMs - recovery.droppedMs)} after the drop`}`,
      );
    if (show.at !== undefined)
      lines.push(
        `**At:** due at ${seconds(show.at.dueMs)}, ${show.at.lateByMs === undefined ? "never started" : `started ${show.at.lateByMs} ms after it`}`,
      );
    if (show.cues.length > 0)
      lines.push(
        `**Cues:** ${show.cues.map((cue) => `${cue.key} ${cue.name} ${cue.lateByMs === undefined ? "at an unknown offset" : `${cue.lateByMs} ms from due`}`).join(", ")}`,
      );
    const loss = show.loss;
    if (loss !== undefined)
      lines.push(
        `**Loss:** ${loss.sessionId} ended by ${loss.by === "key" ? "the API key" : "moderation"} from ${seconds(loss.requestedMs)}${loss.termination === undefined ? "" : ` (DELETE ${String(loss.termination.deleteStatus)}, ${loss.termination.confirmed ? "confirmed" : "unconfirmed"})`}; replaced ${loss.replacedMs === undefined ? "never" : `${seconds(loss.replacedMs - loss.requestedMs)} later`}; ${loss.nextSessionId ?? "no session"} opened${loss.nextOpenedMs === undefined ? "" : ` at ${seconds(loss.nextOpenedMs)}`}`,
      );
  }
  const moderation = evidence.moderation;
  if (moderation !== undefined) {
    const verdict = moderation.verdict;
    lines.push(
      `**Moderation:** ${moderation.flagged ? "flagged" : "not flagged"}${moderation.aired ? ", aired" : ""}; enqueue ${moderation.enqueue === undefined ? "never sent" : `${moderation.enqueue.status}${moderation.enqueue.durationMs === undefined ? "" : ` in ${moderation.enqueue.durationMs} ms`}`}; item ${moderation.statuses.map((status) => status.status).join(" > ")}`,
    );
    lines.push(
      `**Verdict:** ${verdict === undefined ? "none arrived" : `${verdict.action} at ${seconds(verdict.atMs - moderation.submittedMs)} after the submission, categories ${verdict.categories.join(", ") || "none"}, input ${verdict.inputKind ?? "unnamed"}, command ${verdict.command ?? "unnamed"}, ${verdict.namesEnqueue ? "names the enqueue" : "names no request of ours"}`}`,
    );
    lines.push(
      `**Afterwards:** session ${moderation.session.map((entry) => entry.event).join(" > ") || "quiet"}; playout ${moderation.playout.map((entry) => entry.event).join(" > ") || "quiet"}${moderation.read === undefined ? "" : `; read ${moderation.read.status} ${moderation.read.state ?? ""} (keys ${moderation.read.keys.join(", ")})`}`,
    );
  }
  const tour = evidence.tour;
  if (tour !== undefined) lines.push(...tourLines(tour));
  const adoption = evidence.adoption;
  if (adoption !== undefined) {
    const commands = (counts: Readonly<Record<string, number>> | undefined) =>
      Object.entries(counts ?? {})
        .map(([name, count]) => `${name} ${count}`)
        .join(", ") || "none";
    const which = (clipId: string | null | undefined) => {
      if (clipId === undefined) return "not read";
      if (clipId === adoption.ownerClipIds?.playing) return "the owner's playing clip";
      if (clipId === adoption.ownerClipIds?.queued) return "the owner's queued clip";
      return clipId === null ? "no clip" : "neither of the owner's clips";
    };
    lines.push(
      `**Owner:** ${adoption.ownerHost ?? "host not reported"}${adoption.ownerStreamingMs === undefined ? "" : `; streaming at ${seconds(adoption.ownerStreamingMs)}`}${adoption.killedMs === undefined ? "" : `, killed at ${seconds(adoption.killedMs)}`}${adoption.createExpiresMs === undefined ? "" : `; its creating token expired at ${seconds(adoption.createExpiresMs)}`}`,
    );
    const attach = adoption.attach;
    if (attach !== undefined)
      lines.push(
        `**Raw attach:** ready ${seconds(attach.attachedMs - (adoption.killedMs ?? attach.startedMs))} after the kill; playing ${which(attach.playingClipId)}; the queued clip's metadata ${attach.queuedMetadata === true ? "read" : "not read"}; commands ${commands(attach.commands)}; ${attach.close === undefined ? "never closed" : attach.close.report.remote.attempted ? "its close attempted a termination" : "closed without ending the session"}`,
      );
    if (adoption.gap.length > 0)
      lines.push(
        `**With nothing connected:** ${adoption.gap.map((read) => `+${seconds(read.sinceConnectionMs)} ${read.status} ${read.state ?? "no state"}`).join("; ")}`,
      );
    const resume = adoption.resume;
    if (resume !== undefined)
      lines.push(
        `**Resume:** ${adoption.createExpiresMs === undefined ? "started" : `started ${seconds(resume.startedMs - adoption.createExpiresMs)} after the creating token expired`}, ready ${seconds(resume.attachedMs - resume.startedMs)} later, ${resume.ownership}; playing ${which(resume.playingClipId)}; ${resume.refreshedMs === undefined ? "no refresh" : `refreshed at ${seconds(resume.refreshedMs)}`}; ${resume.upload === undefined ? "no clip enqueued" : `clip accepted in ${seconds(resume.upload.acceptedMs - resume.upload.startedMs)}, has_reference_audio ${String(resume.upload.hasReferenceAudio)}`}; commands ${commands(resume.commands)}`,
      );
    lines.push(
      `**Tokens:** ${adoption.mints.map((mint) => `${mint.kind} at ${seconds(mint.atMs)} living ${mint.lifetimeSeconds} s`).join("; ")}`,
    );
    lines.push(
      `**Refusals:** expired token ${adoption.expiredTokenStatus ?? "–"}, unbound token ${adoption.unboundTokenStatus ?? "–"}`,
    );
  }
  const unconnected = evidence.unconnected;
  if (unconnected !== undefined) lines.push(...unconnectedLines(evidence, unconnected));
  return lines;
};

/** `unconnected`'s window, its states and the spent token's answer, a line each. */
const unconnectedLines = (
  evidence: Evidence,
  probe: NonNullable<Evidence["unconnected"]>,
): ReadonlyArray<string> => {
  const allocatedMs = evidence.sessions.find(
    (session) => session.id === probe.sessionId,
  )?.allocatedMs;
  const since = (atMs: number) =>
    allocatedMs === undefined ? seconds(atMs) : `${seconds(atMs - allocatedMs)} after allocation`;
  const codes = (value: Readonly<Record<string, string>> | undefined) =>
    Object.entries(value ?? {})
      .map(([key, code]) => `${key} ${code}`)
      .join(", ") || "none";
  const ended = probe.ended;
  const spent = probe.spentToken;
  const read = probe.read;
  return [
    `**Unconnected:** ${probe.sessionId} requested ${probe.requestedAt}; ${probe.connectableMs === undefined ? "never read connectable" : `connectable ${since(probe.connectableMs)}`}; ${ended === undefined ? "its end unconfirmed" : `ended by ${ended.by === "reactor" ? "Reactor" : "the API key"} ${since(ended.atMs)}, ${ended.at}`}`,
    `**Reads:** ${probe.states.map((entry) => `${entry.state} from ${since(entry.firstMs)} to ${since(entry.lastMs)} (${entry.reads} ${entry.reads === 1 ? "read" : "reads"})`).join(" > ") || "none"}`,
    ...(spent === undefined
      ? []
      : [
          `**Spent token:** a second create ${spent.answer === "allocated" ? `allocated ${spent.sessionId ?? "a session"} in ${seconds(spent.answeredMs - spent.sentMs)}` : spent.answer === "the same session" ? `answered with ${probe.sessionId}, the session it made, in ${seconds(spent.answeredMs - spent.sentMs)}` : `failed in ${seconds(spent.answeredMs - spent.sentMs)} with ${spent.answer}${spent.status === undefined ? "" : ` ${spent.status}`}, outcome ${spent.outcome ?? "unknown"}`}; keys ${spent.keys.join(", ") || "none"}; codes ${codes(spent.codes)}`,
        ]),
    ...(read === undefined
      ? []
      : [
          `**Read at the end:** ${read.status} ${read.state ?? "no state"}; keys ${read.keys.join(", ") || "none"}; codes ${codes(read.codes)}`,
        ]),
  ];
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
