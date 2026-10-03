/** A ledger's runs as Markdown, for `summary.md` and the release notes. */
import { isTerminal } from "reactor-effect-client/CoordinatorClient";
import type { AvatarWire, Evidence } from "./Evidence.js";
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
        `**Boundary ${index + 1}:** ${boundary.edit} ${boundary.aimMs} ms before the end${boundary.refused === true ? " (refused)" : ""}; next ${boundary.nextClipId === boundary.expectedClipId || boundary.expectedClipId === undefined ? "as expected" : "unexpected"}${boundary.pause === undefined ? "" : `; pause ${Math.round(boundary.pause.durationMs)} ms (${boundary.pause.frames} frames, ${boundary.pause.dark} dark)`}`,
      );
  }
  const playout = evidence.playout;
  if (playout !== undefined) {
    lines.push(`**Order:** started ${playout.startOrder.join(", ")}`);
    for (const seam of playout.seams)
      lines.push(
        `**Seam ${seam.ending} to ${seam.next}${seam.continued ? " (continued)" : ""}:** ${seam.pause === undefined ? "no pause measured" : `pause ${Math.round(seam.pause.durationMs)} ms (${seam.pause.frames} frames, ${seam.pause.dark} dark)`}; ${seam.darkFrames ?? 0} dark frames${seam.jump === undefined ? "" : `; join change ${seam.jump.change} against ${seam.jump.typical} (×${seam.jump.ratio})`}`,
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
  const showreel = evidence.showreel;
  if (showreel !== undefined) lines.push(...showreelLines(showreel));
  const avatar = evidence.avatar;
  if (avatar !== undefined) lines.push(...avatarLines(avatar));
  const character = evidence.character;
  if (character !== undefined) lines.push(...characterLines(character));
  const fasth3 = evidence.fasth3;
  if (fasth3 !== undefined) {
    if (evidence.mode === "rehearsal")
      lines.push(
        `**FastH3 rehearsal:** Rehearsed on ReactorTest (${fasth3.model}): proves the program, not hosted FastH3`,
      );
    lines.push(...fastH3Lines(fasth3));
  }
  return lines;
};

type AvatarRecord = NonNullable<Evidence["avatar"]>;

/** A time in `avatar`'s record, which counts from the allocation. */
const fromAllocation = (ms: number) => `A+${seconds(ms)}`;

/** What a command met on the wire. */
const wired = (wire: AvatarWire): string => {
  switch (wire.wire) {
    case "ack":
      return "ack";
    case "message":
      return `message ${wire.type ?? "of no type"}`;
    case "error":
      return `error frame ${wire.code ?? "without a code"}`;
    case "timeout":
      return `no answer by its deadline (outcome ${wire.outcome ?? "unknown"})`;
    case "failed":
      return `failed with ${wire.reason ?? "?"} (outcome ${wire.outcome ?? "?"})`;
  }
};

/** What a command met on the wire, and how long after its send. */
const answered = (wire: AvatarWire) =>
  `${wired(wire)} in ${seconds(wire.answeredMs - wire.sentMs)}`;

const phased = (phases: ReadonlyArray<{ readonly phase: string; readonly afterMs: number }>) =>
  phases.map((entry) => `${entry.phase} +${seconds(entry.afterMs)}`).join(", ") || "no phase";

/** The live snapshot's fields Reactor's docs name for a call that is ready. */
const atLiveFields = [
  "warmup_attempts",
  "call_max_seconds",
  "control_ready",
  "video_receiving",
  "audio_receiving",
  "mic_forwarding",
] as const;

/** The one reconnect a call made for its picture, if it made one. */
const reconnected = (reconnect: AvatarRecord["calls"][number]["reconnect"]) => {
  if (reconnect === undefined) return "no reconnect";
  if (reconnect.failure !== undefined)
    return `no frame within 5 s, and the one reconnect failed: ${reconnect.failure}`;
  return `no frame within 5 s, so one reconnect at ${fromAllocation(reconnect.startedMs)}${reconnect.readyMs === undefined ? "" : `, ready in ${seconds(reconnect.readyMs - reconnect.startedMs)}`}; ${reconnect.firstFrameMs === undefined ? "no frame within 5 s of its ready" : `the first frame ${seconds(reconnect.firstFrameMs)} after its ready`}`;
};

type AvatarCall = AvatarRecord["calls"][number];

const pictureActions = {
  wait: "nothing done",
  resume: "both tracks resumed",
  cycle: "main_video paused and resumed",
} as const;

/** What was done for a call's picture, in turn, and what came after each. */
const pictured = (call: AvatarCall) => {
  if (call.picture === undefined) return reconnected(call.reconnect);
  return (
    call.picture
      .map(
        (stage) =>
          `${pictureActions[stage.action]} at ${fromAllocation(stage.atMs)}${stage.failure === undefined ? "" : ` (failed: ${stage.failure})`}: ${stage.firstFrameMs === undefined ? "no frame within 5 s" : `the first frame ${seconds(stage.firstFrameMs)} after`}${stage.blocks === undefined ? "" : `, ${stage.blocks} blocks`}`,
      )
      .join("; ") || "nothing done for the picture"
  );
};

/** A call's start, and its picture and sound once it was live. */
const callOpening = (call: AvatarCall, label: string): ReadonlyArray<string> => {
  const lines = [
    `**${label}:** start_call ${answered(call.start)}; ${phased(call.phases)}${call.liveMs === undefined ? "; never live" : ` (live at ${fromAllocation(call.liveMs)}); at live ${atLiveFields.map((key) => `${key} ${String(call.atLive?.[key] ?? "none")}`).join(", ")}`}`,
  ];
  if (call.liveMs !== undefined)
    lines.push(
      `**${label}, picture and sound:** first frame ${call.firstFrameMs === undefined ? "never" : `${seconds(call.firstFrameMs)} after live`}, first block ${call.firstBlockMs === undefined ? "never" : `${seconds(call.firstBlockMs)} after live`}; ${pictured(call)}; ${call.video === undefined ? "no picture read" : `${call.video.frames} frames of ${listed(call.video.sizes)}${call.video.fps === undefined ? "" : ` at ${call.video.fps} fps`} (${call.video.lit} lit, ${call.video.distinct} distinct, ${call.video.lost} lost)`}; ${call.audio === undefined ? "no sound read" : `${call.audio.blocks} blocks at ${listed(call.audio.sampleRates.map(String))} Hz, ${listed(call.audio.channels.map(String))} channel(s), peak RMS ${call.audio.peakRms}`}; speech ${call.speech.map((stretch) => `${fromAllocation(stretch.fromMs)}–${seconds(stretch.toMs)}`).join(", ") || "none"}`,
    );
  return lines;
};

/** A `say` in a call: its answer on the wire, the user's transcript and the answer's sound. */
const callSay = (call: AvatarCall, label: string): ReadonlyArray<string> => {
  const say = call.say;
  return say === undefined
    ? []
    : [
        `**${label}, say:** ${answered(say)}; the user's transcript ${say.userTranscriptMs === undefined ? "never came" : `${seconds(say.userTranscriptMs)} after the send`}; the answer's sound ${say.onsetMs === undefined ? "never came" : `${seconds(say.onsetMs)} after the send`}`,
      ];
};

/** A call's `end_call`, its phases, and what still arrived after `ended`. */
const callEnd = (call: AvatarCall, label: string): ReadonlyArray<string> => {
  const end = call.end;
  return end === undefined
    ? []
    : [
        `**${label}, end:** end_call ${answered(end)}; end_reason ${end.endReason ?? "unread"}, duration_seconds ${String(end.durationSeconds ?? "unread")}; ${phased(end.phases)}; ${end.afterEnded === undefined ? "ended never reported" : `${end.afterEnded.frames} frames and ${end.afterEnded.blocks} blocks in the ${seconds(end.afterEnded.forMs)} after ended`}`,
      ];
};

/** `avatar`'s steps, in order, a line each. */
const avatarLines = (avatar: AvatarRecord): ReadonlyArray<string> => {
  const lines = [`**Photo:** ${avatar.photo.type}, ${avatar.photo.bytes} bytes`];
  const contract = avatar.contract;
  if (contract !== undefined)
    lines.push(
      `**Contract:** title ${contract.title ?? "none"}, version ${contract.version ?? "none"}; declares ${listed(contract.commands)}; clone_voice ${contract.cloneVoice ? "declared" : "not declared"}`,
    );
  const first = avatar.first;
  if (first !== undefined)
    lines.push(
      `**First snapshot:** ${fromAllocation(first.atMs)}, phase ${String(first.values.phase ?? "unset")}; set ${listed(first.present)}; null ${listed(first.nulls)}; left out ${listed(first.absent)}; undocumented ${listed(first.undocumented)}`,
    );
  if (avatar.getState !== undefined) lines.push(`**get_state:** ${answered(avatar.getState)}`);
  const voices = avatar.voices;
  if (voices !== undefined)
    lines.push(
      `**list_voices:** ${answered(voices)}; ${voices.system} system voice(s)${voices.ids.length === 0 ? "" : `: ${voices.ids.join(", ")}`}; cloned ${voices.cloned ? "present" : "absent"}, default_voice ${voices.defaultVoice ? "present" : "absent"}`,
    );
  const made = avatar.avatar;
  if (made !== undefined)
    lines.push(
      `**Avatar:** upload ${made.upload.outcome} in ${seconds(made.upload.endedMs - made.upload.startedMs)}; create_avatar ${made.create === undefined ? "unsent" : answered(made.create)}; ${phased(made.phases)}; avatar_status ${made.status ?? "unread"}; ${made.idLength === undefined ? "no avatar_id" : `an avatar_id of ${made.idLength} characters`}`,
    );
  const [call1, call2] = avatar.calls;
  if (call1 !== undefined) lines.push(...callOpening(call1, "Call 1"));
  const greeting = avatar.greeting;
  if (greeting !== undefined)
    lines.push(
      `**Greeting:** sound ${greeting.onsetMs === undefined ? "never came" : `${seconds(greeting.onsetMs)} after live`}; the first character transcript ${greeting.transcriptMs === undefined ? "never came" : `${seconds(greeting.transcriptMs)} after live`}; ${greeting.transcripts} character transcript(s), final ${greeting.finals.map(String).join(", ") || "–"}`,
    );
  if (call1 !== undefined) lines.push(...callSay(call1, "Call 1"));
  const interrupt = avatar.interrupt;
  if (interrupt !== undefined)
    lines.push(
      `**Interrupt:** ${answered(interrupt)}, sent ${interrupt.afterOnsetMs === undefined ? "with no answer heard" : `${seconds(interrupt.afterOnsetMs)} into the answer's sound`}; silent ${interrupt.silenceMs === undefined ? "never" : `${seconds(interrupt.silenceMs)} after it`}; the cut answer's character transcript ${interrupt.cut === undefined ? "never came" : `came ${seconds(Math.abs(interrupt.cut.afterMs))} ${interrupt.cut.afterMs < 0 ? "before" : "after"} it, final ${String(interrupt.cut.final)}, ${interrupt.cut.length} characters`}`,
    );
  const change = avatar.voiceChange;
  if (change !== undefined)
    lines.push(
      `**Voice change:** update_call ${answered(change)}${change.voice === undefined ? "" : ` to ${change.voice}`}; applied ${listed(change.applied)}; the state's voice ${change.changed ? "changed" : "did not change"}`,
    );
  if (call1 !== undefined) lines.push(...callEnd(call1, "Call 1"));
  const attach = avatar.attach;
  if (attach !== undefined)
    lines.push(`**Attach:** attach_avatar ${answered(attach)}; ${phased(attach.phases)}`);
  if (call2 !== undefined)
    lines.push(
      ...callOpening(call2, "Call 2"),
      ...callSay(call2, "Call 2"),
      ...callEnd(call2, "Call 2"),
    );
  const last = avatar.lastState;
  if (last !== undefined)
    lines.push(`**Last state:** get_state ${answered(last)}, phase ${last.phase ?? "unread"}`);
  lines.push(
    `**Phases, with the frames and blocks that arrived in each:** ${avatar.windows.map((window) => `${window.phase} ${fromAllocation(window.fromMs)} (${window.frames}, ${window.blocks})`).join(" › ") || "none reported"}`,
  );
  const spoken = (speaker: string) =>
    avatar.transcripts.filter((entry) => entry.speaker === speaker).length;
  const finals = (final: boolean | null) =>
    avatar.transcripts.filter((entry) => entry.final === final).length;
  lines.push(
    `**Transcripts:** ${avatar.transcripts.length}: ${spoken("user")} user, ${spoken("character")} character; ${finals(true)} final, ${finals(false)} not final, ${finals(null)} that did not say`,
  );
  lines.push(
    `**Messages:** ${
      Object.entries(avatar.messages)
        .map(([type, count]) => `${type} ${count}`)
        .join(", ") || "none"
    }; ${avatar.events.length} session events`,
  );
  return lines;
};

/** The `last_error` of the state after a refused command. */
const lastErrorOf = (state: AvatarRecord["refusals"][number]["state"]) => {
  if (state === undefined) return "no state";
  return state.lastError ? (state.code ?? "set, with no code") : "null";
};

/** `avatar`'s refused commands as a table, and what the run answers of the docs' open questions. */
const avatarAnswers = (avatar: AvatarRecord): ReadonlyArray<string> => {
  const yesNo = (value: boolean | undefined, yes: string, no: string) =>
    value === undefined ? "–" : value ? yes : no;
  const calls = avatar.calls;
  const timed = (label: string, wires: ReadonlyArray<AvatarWire | undefined>) => {
    const sent = wires.filter((wire) => wire !== undefined);
    return sent.length === 0
      ? []
      : [`${label} ${sent.map((wire) => seconds(wire.answeredMs - wire.sentMs)).join(" and ")}`];
  };
  const clone = avatar.refusals.find((refusal) => refusal.command === "clone_voice");
  const nulled = avatar.refusals.find((refusal) => refusal.changed !== undefined);
  const picture = calls.map((call, index) => {
    if (call.liveMs === undefined) return `call ${index + 1} never went live`;
    if (call.picture !== undefined) {
      const brought = call.picture.find((stage) => stage.firstFrameMs !== undefined);
      return brought === undefined
        ? `call ${index + 1} had no frame after ${call.picture.map((stage) => pictureActions[stage.action]).join(", then ")}`
        : `call ${index + 1}'s picture came ${seconds(brought.firstFrameMs ?? 0)} after ${pictureActions[brought.action]}`;
    }
    if (call.reconnect === undefined)
      return `call ${index + 1}'s first frame came ${seconds(call.firstFrameMs ?? 0)} after live, with no reconnect`;
    return `call ${index + 1} had no frame within 5 s of live; ${call.reconnect.firstFrameMs === undefined ? "none came within 5 s of the one reconnect either" : `after one reconnect the first came ${seconds(call.reconnect.firstFrameMs)} after its ready`}`;
  });
  const finals = avatar.transcripts.map((entry) => entry.final);
  return [
    "",
    `**Refused commands** (each watched until its command_error and next session_state came, or 2 s after its answer):`,
    "",
    "| Probe | On the wire | Answered in | command_error | Before the answer | trace_id | Next state's last_error | Changed after it |",
    "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ...avatar.refusals.map((refusal) => {
      const error = refusal.commandError;
      return `| ${refusal.probe} | ${wired(refusal)} | ${seconds(refusal.answeredMs - refusal.sentMs)} | ${error === undefined ? "none" : `${error.code ?? "no code"} (${error.origin ?? "no origin"}, ${error.command ?? "no command"}, ${yesNo(error.retryable, "retryable", "not retryable")})`} | ${yesNo(error?.beforeAnswer, "yes", "no")} | ${error === undefined ? "–" : yesNo(error.traceId, "named", "none")} | ${lastErrorOf(refusal.state)} | ${refusal.changed === undefined ? "–" : listed(refusal.changed)} |`;
    }),
    "",
    "**What this run answers:**",
    "",
    `- A refused command, on the wire: ${avatar.refusals.map((refusal) => `${refusal.probe}, ${wired(refusal)}`).join("; ") || "nothing was refused"}.`,
    `- command_error against its command's answer: ${
      avatar.refusals
        .flatMap((refusal) =>
          refusal.commandError === undefined
            ? []
            : [
                `${refusal.probe}, ${yesNo(refusal.commandError.beforeAnswer, "before it", "after it")}`,
              ],
        )
        .join("; ") || "no command_error was broadcast"
    }.`,
    `- A command carrying an explicit null: ${nulled === undefined ? "not sent" : `${wired(nulled)}; ${nulled.changed?.length === 0 ? "nothing read otherwise after it" : `${listed(nulled.changed ?? [])} read otherwise after it`}`}.`,
    `- Answer times, against the SDK's 10 s default: ${
      [
        ...timed("create_avatar", [avatar.avatar?.create]),
        ...timed(
          "start_call",
          calls.map((call) => call.start),
        ),
        ...timed(
          "end_call",
          calls.map((call) => call.end),
        ),
        ...timed("clone_voice", [clone]),
        ...timed("attach_avatar", [avatar.attach]),
      ].join("; ") || "none answered"
    }.`,
    `- The picture after live: ${picture.join("; ") || "no call was started"}.`,
    `- The call's limit at live: ${calls.map((call, index) => `call ${index + 1} call_max_seconds ${String(call.atLive?.call_max_seconds ?? "none")}`).join(", ") || "no call went live"}.`,
    `- The picture's size: ${listed([...new Set(calls.flatMap((call) => call.video?.sizes ?? []))])}.`,
    `- Voice cloning: clone_voice ${avatar.contract?.cloneVoice === true ? "declared" : "not declared"}, ${clone === undefined ? "not sent" : `answered ${wired(clone)}`}; voices.cloned ${avatar.voices?.cloned === true ? "present" : "absent"}.`,
    `- Partial transcripts: ${finals.filter((final) => final === false).length} of ${finals.length} not final${finals.includes(null) ? `, ${finals.filter((final) => final === null).length} with no final` : ""}.`,
    `- The deployment's version: ${avatar.contract?.version ?? "unread"}.`,
  ];
};

/** `showreel`'s scenes, its seams on air, and its reel and the files made from it. */
/** `character`'s call through the provider: its operations, phases, picture, sound and transcripts. */
const characterLines = (character: NonNullable<Evidence["character"]>): ReadonlyArray<string> => {
  const lines = [
    `**Photo:** ${character.photo.type}, ${character.photo.bytes} bytes`,
    // The session's own step starts before its allocation.
    `**Operations:** ${character.steps.map((step) => `${step.name} ${step.outcome} in ${seconds(step.endedMs - step.startedMs)} at ${step.startedMs < 0 ? `A-${seconds(-step.startedMs)}` : fromAllocation(step.startedMs)}`).join("; ") || "none"}`,
    `**Phases:** ${character.phases.map((entry) => `${entry.phase} ${fromAllocation(entry.atMs)}`).join(" › ") || "none seen"}`,
  ];
  const call = character.call;
  if (call !== undefined)
    lines.push(
      `**Call:** live at ${fromAllocation(call.liveMs)}${call.callMaxSeconds === undefined ? "" : `, call_max_seconds ${call.callMaxSeconds}`}; the first frame ${call.firstFrameMs === undefined ? "never came" : `${seconds(call.firstFrameMs)} after live`}; the greeting's sound ${call.greetingOnsetMs === undefined ? "never came" : `${seconds(call.greetingOnsetMs)} after live`}${call.greetingSilenceMs === undefined ? "" : `, silent ${seconds(call.greetingSilenceMs)} after live`}; ${call.video.frames} frames of ${listed(call.video.sizes)}${call.video.fps === undefined ? "" : ` at ${call.video.fps} fps`} (${call.video.lit} lit, ${call.video.distinct} distinct, ${call.video.lost} lost); ${call.audio.blocks} blocks at ${listed(call.audio.sampleRates.map(String))} Hz, peak RMS ${call.audio.peakRms}`,
    );
  const say = character.say;
  if (say !== undefined)
    lines.push(
      `**Say:** sent at ${fromAllocation(say.sentMs)}; the user's transcript ${say.userTranscriptMs === undefined ? "never came" : `${seconds(say.userTranscriptMs)} after`}; the answer's sound ${say.onsetMs === undefined ? "never came" : `${seconds(say.onsetMs)} after`}; the character's transcript ${say.characterTranscriptMs === undefined ? "never came" : `${seconds(say.characterTranscriptMs)} after`}`,
    );
  const end = character.end;
  if (end !== undefined)
    lines.push(`**End:** ${end.endReason}, the call live ${end.durationSeconds} s`);
  const finals = character.transcripts.filter((entry) => entry.final);
  lines.push(
    `**Transcripts:** ${character.transcripts.length}: ${finals.filter((entry) => entry.speaker === "user").length} final from the user, ${finals.filter((entry) => entry.speaker === "character").length} from the character`,
    `**Provider events:** command errors ${character.commandErrors.map((entry) => `${entry.command} ${entry.code} at ${fromAllocation(entry.atMs)}`).join(", ") || "none"}; diagnostics ${character.diagnostics.map((entry) => `${entry.reason} at ${fromAllocation(entry.atMs)}`).join(", ") || "none"}`,
  );
  return lines;
};

const showreelLines = (showreel: NonNullable<Evidence["showreel"]>): ReadonlyArray<string> => {
  const lines = [
    `**Scenes:** ${showreel.scenes.map((scene) => `${scene.key} ${scene.seconds} s`).join(", ")}; gaps on air ${showreel.gaps.map((gap) => `${Math.round(gap.toMs - gap.fromMs)} ms`).join(", ") || "none measured"}`,
    `**Readers:** ${showreel.readerOverflows.length === 0 ? "never fell behind" : showreel.readerOverflows.map((overflow) => `${overflow.track} at ${seconds(overflow.atMs)}`).join(", ")}`,
  ];
  if (showreel.notRecorded !== undefined)
    lines.push(`**Reel:** not recorded: ${showreel.notRecorded}`);
  const recording = showreel.recording;
  if (recording !== undefined)
    lines.push(
      `**Reel:** ${recording.frames} frames of ${recording.width}x${recording.height} at 24 fps (${seconds((recording.frames * 1000) / 24)}), ${recording.repeated} repeated, ${recording.superseded} superseded, ${recording.mismatched} of another size dropped; ${recording.audio === undefined ? "no sound" : `sound ${recording.audio.blocks} blocks at ${recording.audio.sampleRate} Hz, ${recording.audio.silenceMs} ms of silence added`}; ${recording.failure ?? `ffmpeg exited ${String(recording.exitCode)}`}`,
    );
  if (showreel.files.length > 0)
    lines.push(
      `**Files beside the evidence:** ${showreel.files.map((file) => `${file.name} ${(file.bytes / 1024).toFixed(1)} KiB`).join(", ")}${showreel.poster === undefined ? "" : `; poster at ${showreel.poster.atSeconds} s`}${showreel.loop === undefined ? "" : `; loop from ${showreel.loop.fromSeconds} s for ${showreel.loop.seconds} s`}`,
    );
  return lines;
};

/** `unconnected`'s session, its window, its states and the spent token's answers, a line each. */
const unconnectedLines = (
  evidence: Evidence,
  probe: NonNullable<Evidence["unconnected"]>,
): ReadonlyArray<string> => {
  const codes = (value: Readonly<Record<string, string>> | undefined) =>
    Object.entries(value ?? {})
      .map(([key, code]) => `${key} ${code}`)
      .join(", ") || "none";
  const failure = (answer: {
    readonly answer: string;
    readonly status?: number;
    readonly outcome?: string;
  }) =>
    `${answer.answer}${answer.status === undefined ? "" : ` ${answer.status}`}, outcome ${answer.outcome ?? "unknown"}`;
  const create = probe.create;
  if (probe.sessionId === undefined)
    return [
      create === undefined
        ? `**Unconnected:** requested ${probe.requestedAt}; no answer to its create was recorded`
        : `**Unconnected:** no session named; its create failed with ${failure(create)}; keys ${create.keys.join(", ") || "none"}; codes ${codes(create.codes)}`,
    ];
  // A time from a session's allocation, where the evidence has it.
  const afterAllocation = (sessionId: string) => {
    const allocatedMs = evidence.sessions.find((session) => session.id === sessionId)?.allocatedMs;
    return (atMs: number) =>
      allocatedMs === undefined ? seconds(atMs) : `${seconds(atMs - allocatedMs)} after allocation`;
  };
  const allocatedMs = evidence.sessions.find(
    (session) => session.id === probe.sessionId,
  )?.allocatedMs;
  const since = afterAllocation(probe.sessionId);
  const readsOf = (states: typeof probe.states, from: (atMs: number) => string) =>
    states
      .map(
        (entry) =>
          `${entry.state} from ${from(entry.firstMs)} to ${from(entry.lastMs)} (${entry.reads} ${entry.reads === 1 ? "read" : "reads"})`,
      )
      .join(" > ") || "none";
  const ended = probe.ended;
  const spent = probe.spentToken;
  const read = probe.read;
  // The spent token's first create, then its second, with what the second's reply held.
  const spentLine = (token: NonNullable<typeof spent>) => {
    const first = token.create;
    const opening =
      token.sessionId !== undefined
        ? `its first create allocated ${token.sessionId}`
        : first === undefined
          ? "no answer to its first create was recorded"
          : `its first create failed with ${failure(first)}; keys ${first.keys.join(", ") || "none"}; codes ${codes(first.codes)}`;
    const second = token.second;
    if (second === undefined)
      return `**Spent token:** ${opening}${token.sessionId === undefined ? "" : "; no answer to a second create was recorded"}`;
    const took = seconds(second.answeredMs - second.sentMs);
    const answered =
      second.answer === "allocated"
        ? `allocated ${second.sessionId ?? "a session"} in ${took}`
        : second.answer === "the same session"
          ? `answered with ${token.sessionId ?? "?"}, the session it made, in ${took}`
          : `failed in ${took} with ${failure(second)}`;
    return `**Spent token:** ${opening}; a second create ${answered}; keys ${second.keys.join(", ") || "none"}; codes ${codes(second.codes)}`;
  };
  const windowEndsMs = probe.windowEndsMs;
  // How far the window ran past the cap and the 30 s after it, counted from `from`.
  const capMs = (evidence.grants[0]?.maxSessionSeconds ?? 0) * 1000;
  const past = (startMs: number | undefined, from: string) => {
    if (startMs === undefined || windowEndsMs === undefined) return `${from} never read`;
    const spare = windowEndsMs - startMs - capMs - 30_000;
    return spare < 0 ? `${seconds(-spare)} short from ${from}` : `${seconds(spare)} from ${from}`;
  };
  return [
    `**Unconnected:** ${probe.sessionId} requested ${probe.requestedAt}; ${probe.connectableMs === undefined ? "never read connectable" : `connectable ${since(probe.connectableMs)}`}; ${ended === undefined ? "its end unconfirmed" : `ended by ${ended.by === "reactor" ? "Reactor" : "the API key"} ${since(ended.atMs)}, ${ended.at}`}`,
    ...(probe.unanswered === undefined
      ? []
      : [`**Q1:** unanswered by this run. ${probe.unanswered}`]),
    ...(windowEndsMs === undefined
      ? []
      : [
          `**Window:** reads until ${seconds(windowEndsMs - probe.requestedMs)} after the request, past the cap and 30 s by ${past(allocatedMs, "allocation")}, ${past(probe.states.find((entry) => entry.state === "ACTIVE")?.firstMs, "ACTIVE")} and ${past(probe.connectableMs, "ready")}`,
        ]),
    `**Reads:** ${readsOf(probe.states, since)}`,
    ...(spent === undefined ? [] : [spentLine(spent)]),
    // The spent token's sessions, read from the second create's answer until ready.
    ...(spent?.held ?? []).map((held) => {
      const after = afterAllocation(held.sessionId);
      return `**Held:** ${held.sessionId} ${held.connectableMs === undefined ? "never read connectable" : `connectable ${after(held.connectableMs)}`}; ${readsOf(held.states, after)}`;
    }),
    ...(read === undefined
      ? []
      : [
          `**Read at the end:** ${read.status} ${read.state ?? "no state"}; keys ${read.keys.join(", ") || "none"}; codes ${codes(read.codes)}`,
        ]),
  ];
};

/**
 * `unconnected`'s sessions as a table for the Reactor dashboard: each one's
 * times, from the watched session's request, beside the duration and charge
 * the maintainer reads there. Ready is the first read that found a session
 * connectable, and each spent token's session's hold is planned from it, or
 * from the end of a wait without it. Each end lies between two times: for
 * Reactor's, the last read that found the session running and the first that
 * found it ended; for the key's, its DELETE and the read that confirmed it;
 * and before a DELETE that found no session, its allocation and that DELETE.
 */
const billing = (
  evidence: Evidence,
  probe: NonNullable<Evidence["unconnected"]>,
): ReadonlyArray<string> => {
  if (evidence.sessions.length === 0) return [];
  const since = (atMs: number | undefined) =>
    atMs === undefined ? "–" : seconds(atMs - probe.requestedMs);
  // No end comes before the allocation, though the DELETE after it may share its instant.
  const between = (fromMs: number, toMs: number, startMs: number) =>
    `${(Math.max(0, fromMs - startMs) / 1000).toFixed(2)}–${seconds(toMs - startMs)}`;
  const spent = probe.spentToken;
  const made = [
    {
      sessionId: probe.sessionId,
      by: "the watched token's create",
      requestedMs: probe.requestedMs,
    },
    {
      sessionId: spent?.sessionId,
      by: "the spent token's first create",
      requestedMs: spent?.requestedMs,
    },
    {
      sessionId: spent?.second?.sessionId,
      by: "its second create",
      requestedMs: spent?.second?.sentMs,
    },
  ];
  // A failed read, or one that found it gone, says nothing of whether the session ran.
  const running = (state: string) =>
    !isTerminal(state) && state !== "gone" && !/^(?:http|error):/.test(state);
  const heldOf = (session: Evidence["sessions"][number]) =>
    spent?.held?.find((held) => held.sessionId === session.id);
  // The reads of a session, the watch's or the ones after the second create.
  const statesOf = (session: Evidence["sessions"][number]) =>
    session.id === probe.sessionId ? probe.states : (heldOf(session)?.states ?? []);
  const readyOf = (session: Evidence["sessions"][number]) =>
    session.id === probe.sessionId ? probe.connectableMs : heldOf(session)?.connectableMs;
  const heldText = (session: Evidence["sessions"][number]) => {
    const held = heldOf(session);
    if (held === undefined) return "–";
    if (held.heldFromMs === undefined || held.endsMs === undefined) return "none: found ended";
    return `${seconds(held.endsMs - held.heldFromMs)} past ${held.connectableMs === undefined ? "the wait" : "ready"}`;
  };
  const endOf = (session: Evidence["sessions"][number]) => {
    const ended = probe.ended;
    const states = statesOf(session);
    // Reactor's end, for the watched session as the watch found it, and for a spent token's as
    // the first read that found it closed.
    const endedMs =
      session.id === probe.sessionId
        ? ended?.by === "reactor"
          ? ended.atMs
          : undefined
        : states.find((entry) => isTerminal(entry.state))?.firstMs;
    if (endedMs !== undefined) {
      const runningMs = states
        .filter((entry) => entry.lastMs < endedMs && running(entry.state))
        .reduce((latest, entry) => Math.max(latest, entry.lastMs), session.allocatedMs);
      return { by: "Reactor", fromMs: runningMs, toMs: endedMs };
    }
    const close = session.close;
    if (close?.confirmed !== true) return undefined;
    // A DELETE that found no session came after the session had ended, whatever ended it.
    return close.termination?.deleteStatus === 404
      ? { by: "before the key's DELETE", fromMs: session.allocatedMs, toMs: close.requestedMs }
      : { by: "the key", fromMs: close.requestedMs, toMs: close.reportedMs };
  };
  return [
    "",
    `**Billing, for the dashboard:** seconds from the watched session's request, ${probe.requestedAt}. Ready is the first read that found the session connectable; the spent token's sessions are read from its second create's answer, and each is held the time given past its ready, or past a 5 s wait it never became ready in. Each end lies between the two times given: for Reactor's, the last read that found the session running and the first that found it ended; for the key's, its DELETE and the read that confirmed it; and before a DELETE that found no session, its allocation and that DELETE. Fill in the last two columns from the Reactor dashboard.`,
    "",
    "| Session | Made by | Requested | Allocated | First ACTIVE read | Ready | Held | Ended by | Ended | Allocated to ended | Ready to ended | Dashboard duration | Dashboard charge |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ...evidence.sessions.map((session) => {
      const role = made.find((entry) => entry.sessionId === session.id);
      const active = statesOf(session).find((entry) => entry.state === "ACTIVE")?.firstMs;
      const ready = readyOf(session);
      const end = endOf(session);
      const span = (startMs: number | undefined) =>
        end === undefined || startMs === undefined ? "–" : between(end.fromMs, end.toMs, startMs);
      return `| ${session.id} | ${role?.by ?? "–"} | ${since(role?.requestedMs)} | ${since(session.allocatedMs)} | ${since(active)} | ${since(ready)} | ${heldText(session)} | ${end?.by ?? "unconfirmed"} | ${span(probe.requestedMs)} | ${span(session.allocatedMs)} | ${span(ready)} |  |  |`;
    }),
  ];
};

/** One run as a section. */
const section = (evidence: Evidence): string => {
  const environment = evidence.environment;
  const firstLate = evidence.liveness?.samples.find((sample) => sample.lateMs > 2000);
  const firstLateText =
    firstLate === undefined ? "" : `, first over 2 s at ${seconds(firstLate.atMs)}`;
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
    ...(evidence.liveness === undefined
      ? []
      : [
          `- **Liveness:** the runner's 1 s timer fired at most ${seconds(evidence.liveness.maxLateMs)} late${firstLateText}; ${evidence.sessionEvents?.length ?? 0} session events kept`,
        ]),
    ...measurements(evidence).map((line) => `- ${line}`),
    `- **Termination:** ${evidence.sessions.map((session) => `${session.id} ${session.close?.confirmed === true ? "confirmed" : "unconfirmed"}${session.trail.length === 0 ? "" : ` (trail ${session.trail.map((entry) => entry.state).join(" > ")})`}`).join("; ")}`,
    `- **Criteria:** ${evidence.criteria.map((criterion) => `${criterion.passed ? "✓" : "✗"} ${criterion.name}`).join(" · ")}`,
    ...evidence.reasons.map((reason) => `  - ${reason}`),
    ...cleanupInstructions(evidence).map((line) => `- **Cleanup:** ${line}`),
    ...(evidence.unconnected === undefined ? [] : billing(evidence, evidence.unconnected)),
    ...(evidence.avatar === undefined ? [] : avatarAnswers(evidence.avatar)),
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

/** FastH3's answers, with uncertainty kept beside each measurement. */
const fastH3Lines = (probe: NonNullable<Evidence["fasth3"]>): ReadonlyArray<string> => {
  const comparisons: Array<{ difference: number; unbuilt: boolean }> = [];
  for (const count of probe.counts) {
    if (
      !count.read ||
      count.from !== "state_update" ||
      count.comparable !== true ||
      count.pair === undefined
    )
      continue;
    const queue = probe.counts.find(
      (other) =>
        other.read &&
        other.from === "queue_update" &&
        other.comparable === true &&
        other.pair === count.pair,
    );
    if (queue !== undefined)
      comparisons.push({
        difference: count.generation - queue.generation,
        unbuilt: count.building || queue.building,
      });
  }
  const lines = [
    `**Q1, counts:** at ${comparisons.length} comparable explicit read pairs of ${probe.readPairs.length} attempts, generation_queued minus the generation list was ${comparisons.map((pair) => String(pair.difference)).join(", ") || "unmeasured"}; while a clip was unbuilt: ${
      comparisons
        .filter((pair) => pair.unbuilt)
        .map((pair) => String(pair.difference))
        .join(", ") || "unmeasured"
    }. Other broadcasts and interrupted pairs are not compared${probe.observerLost ? "; observation was lost" : ""}.`,
  ];
  const contract = probe.contract;
  lines.push(
    contract === undefined
      ? "**Q2 and Q7, document:** unread"
      : `**Q2 and Q7, document:** model ${probe.model}; title ${contract.title ?? "unmeasured"}, version ${contract.version ?? "unmeasured"}; commands ${listed(contract.commands)}; enqueue properties ${listed(contract.enqueue)}`,
  );
  const enqueue = (label: string) => {
    const entry = probe.enqueues.find((each) => each.label === label);
    if (entry === undefined) return "not attempted";
    const errors = entry.commandErrors.map(
      (error) =>
        `command_error ${error.command}, ${error.reasonLength} reason characters (${error.attribution}, ${error.beforeAnswer ? "before" : "after or with"} the answer)`,
    );
    const own = entry.commandError;
    if (own !== undefined && errors.length === 0)
      errors.push(
        `command_error ${own.command}, ${own.reasonLength} reason characters (own reply)`,
      );
    return `${entry.answer}${entry.outcome === undefined ? "" : ` (${entry.outcome})`}${entry.clipAttribution === undefined ? "" : `; clip identified by ${entry.clipAttribution}`}${errors.length === 0 ? "" : `; observed ${errors.join("; ")}`}`;
  };
  lines.push(
    `**Q3, undeclared reference_images:** ${enqueue("reference_images")}. A temporal error does not establish this command was refused.`,
  );
  const state = probe.state;
  lines.push(
    state === undefined
      ? "**Q4, state:** unread"
      : `**Q4, state:** ${state.missing.length === 0 ? "none of H3's 18 missing" : `missing ${listed(state.missing)}`}; extra keys ${listed(state.extra)}; clip_seconds_min ${String(state.values.clip_seconds_min ?? "unread")}, clip_seconds_max ${String(state.values.clip_seconds_max ?? "unread")}`,
  );
  lines.push(
    `**Q5, lengths:** ${probe.lengths.map((length) => `${length.requested} → ${length.clipSeconds === undefined ? length.answer : `${length.clipSeconds} (${String(length.frames)} frames)`}`).join("; ") || "unmeasured"}`,
  );
  lines.push(
    `**Q6, history:** lengths seen ${probe.history.map((read) => String(read.length)).join(", ") || "unmeasured"}; observed maximum ${probe.history.length === 0 ? "unmeasured" : Math.max(...probe.history.map((read) => read.length))}, a lower bound on capacity; clip keys ${listed([...new Set(probe.history.flatMap((read) => read.clipKeys))])}`,
  );
  const ahead = probe.ahead;
  let precondition = "not checked";
  if (ahead?.checked === true)
    precondition = ahead.observed
      ? "observed ahead of its unbuilt anchor"
      : "not established by the returned queue and events";
  lines.push(
    `**Q8, continuations:** Q8a precondition ${precondition}; generated order ${listed(probe.generatedOrder)}; unknown source ${enqueue("unknown source")}; from history ${enqueue("from history")}`,
  );
  const stop = probe.stop;
  lines.push(
    stop === undefined
      ? "**Q9, stop:** skipped; frozen-tail timing unmeasured"
      : `**Q9, stop:** ${stop.answer}; stop → clip_stopped ${stop.stoppedMs === undefined ? "unmeasured" : seconds(stop.stoppedMs - stop.sentMs)}; frozen-tail timing unmeasured`,
  );
  lines.push(
    `**Q10, builds and seams:** ${
      probe.clips
        .map((clip) => {
          const generated =
            clip.generatedMs === undefined
              ? "unmeasured"
              : seconds(clip.generatedMs - clip.queuedMs);
          const started =
            clip.startedMs === undefined || clip.generatedMs === undefined
              ? "unmeasured"
              : seconds(clip.startedMs - clip.generatedMs);
          const ended =
            clip.endedMs === undefined || clip.startedMs === undefined
              ? "unmeasured"
              : `${seconds(clip.endedMs - clip.startedMs)} (${clip.ended ?? "unknown"})`;
          return `${clip.label}: queued → generated ${generated}, generated → started ${started}, started → end ${ended}`;
        })
        .join("; ") || "unmeasured"
    }; seams ${probe.seams.map((seam) => `to ${seam.toLabel} ${seconds(seam.pauseMs)}`).join(", ") || "unmeasured"}`,
  );
  return lines;
};
