/**
 * The evidence of one hosted qualification run: its schema, the fields each
 * check must fill before it can pass, and the ledger that every paid run's
 * evidence forms. A run saves its evidence at every milestone, so a crash or a
 * kill still leaves what it saw, the session to clean up, and its spend.
 */
import { existsSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import * as Schema from "effect/Schema";
import type { Mutable } from "effect/Types";
import * as Reactor from "reactor-effect-client";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Result from "effect/Result";
import {
  Refused,
  checks,
  ceilingFor,
  reservationUsd,
  sessionSeconds,
  sessionsFor,
  stopFor,
  worstCaseUsd,
} from "./gates.js";

export const format = "reactor-hosted-qualification/v1";

/** Historical wall-clock offsets. The schedulerRenewal subtree uses one monotonic origin. */
const Ms = Schema.Finite;
const Usd = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
/** A 64-bit counter, as decimal text. */
const Counter = Schema.String.check(Schema.isPattern(/^\d+$/));
const Scalar = Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]);
const Counts = Schema.Record(Schema.String, Schema.Natural);

export const SpanRecord = Schema.Struct({
  name: Schema.String,
  startMs: Ms,
  durationMs: Schema.optionalKey(Ms),
  status: Schema.Literals(["ok", "error", "open"]),
  attributes: Schema.Record(Schema.String, Scalar),
  events: Schema.Array(Schema.Struct({ name: Schema.String, atMs: Ms })),
});
export type SpanRecord = typeof SpanRecord.Type;

export const Milestone = Schema.Struct({
  atMs: Ms,
  step: Schema.String,
  detail: Schema.optionalKey(Schema.String),
});

const Spread = Schema.Struct({ p50: Ms, p95: Ms, max: Ms });

export const VideoSummary = Schema.Struct({
  frames: Schema.Natural,
  /** Individual decoded arrivals, relative to run start; absent in older evidence. */
  arrivalsMs: Schema.optionalKey(Schema.Array(Ms)),
  formats: Schema.Array(Schema.String),
  sizes: Schema.Array(Schema.String),
  firstFrameMs: Schema.optionalKey(Ms),
  fps: Schema.optionalKey(Schema.Finite),
  interval: Schema.optionalKey(Spread),
  lit: Schema.Natural,
  distinct: Schema.Natural,
  meanLuma: Schema.optionalKey(Schema.Finite),
  /** Frames the host dropped before admission, from the recorder's gaps. */
  lost: Schema.Natural,
  gaps: Schema.Natural,
  withMetadata: Schema.Natural,
  withFrameId: Schema.Natural,
  withTimestamp: Schema.Natural,
});
export type VideoSummary = typeof VideoSummary.Type;

export const AudioSummary = Schema.Struct({
  blocks: Schema.Natural,
  arrivalsMs: Schema.optionalKey(Schema.Array(Ms)),
  sampleRates: Schema.Array(Schema.Natural),
  channels: Schema.Array(Schema.Natural),
  samplesPerBlock: Schema.Array(Schema.Natural),
  firstBlockMs: Schema.optionalKey(Ms),
  /** The loudest block's RMS, from 0 to 1. */
  peakRms: Schema.Finite,
  lost: Schema.Natural,
});
export type AudioSummary = typeof AudioSummary.Type;

/**
 * The longest stretch around a clip boundary with no new decoded picture: from
 * the last frame that differed from the one before it to the next that did.
 * A held frame, black frames and no frames at all each count.
 */
export const SeamPause = Schema.Struct({
  lastNewFrameMs: Ms,
  firstNewFrameMs: Ms,
  durationMs: Ms,
  /** The frames that arrived inside the pause, and how many of them were dark. */
  frames: Schema.Natural,
  dark: Schema.Natural,
});
export type SeamPause = typeof SeamPause.Type;

/**
 * The scheduler check's edit before each clip boundary, in playing order, and
 * how long before the playing clip's expected end it is sent.
 */
export const schedulerBoundaries = [
  { edit: "none", aimMs: 0 },
  { edit: "move", aimMs: 2500 },
  { edit: "move", aimMs: 250 },
  { edit: "pop", aimMs: 1000 },
  { edit: "pop", aimMs: 250 },
] as const;

/** One clip boundary in the scheduler check: its edit, the clips on either side, and its seam. */
export const SchedulerBoundary = Schema.Struct({
  edit: Schema.Literals(["none", "move", "pop"]),
  /** How long before the ending clip's expected end the edit was sent. */
  aimMs: Ms,
  ending: Schema.Struct({
    clipId: Schema.String,
    seconds: Schema.Finite,
    startedMs: Ms,
    finishedMs: Schema.optionalKey(Ms),
  }),
  /** The clip moved to position zero, or popped. */
  editedClipId: Schema.optionalKey(Schema.String),
  /** The clip the edit should start next. */
  expectedClipId: Schema.optionalKey(Schema.String),
  command: Schema.optionalKey(Schema.Struct({ sentMs: Ms, replyMs: Ms, refused: Schema.Boolean })),
  next: Schema.optionalKey(Schema.Struct({ clipId: Schema.String, startedMs: Ms })),
  pause: Schema.optionalKey(SeamPause),
});
export type SchedulerBoundary = typeof SchedulerBoundary.Type;

/**
 * The largest change between consecutive decoded frames around a boundary,
 * against the change between the frames of the clip that ends just before it.
 */
export const SeamJump = Schema.Struct({
  atMs: Ms,
  /** Mean absolute luma difference, 0 to 255, between the frames either side. */
  change: Schema.Finite,
  /** The median change between consecutive frames of the ending clip. */
  typical: Schema.Finite,
  /** `change` over `typical`: near 1 when the picture carries across the boundary. */
  ratio: Schema.Finite,
});
export type SeamJump = typeof SeamJump.Type;

/** One boundary between two scheduled items: when it fell and how it looked. */
export const ItemSeam = Schema.Struct({
  ending: Schema.String,
  next: Schema.String,
  /** Whether the next clip was built continuing from the ending one. */
  continued: Schema.Boolean,
  endedMs: Schema.optionalKey(Ms),
  startedMs: Schema.optionalKey(Ms),
  pause: Schema.optionalKey(SeamPause),
  /** Dark frames anywhere around the boundary, including single ones that are no pause. */
  darkFrames: Schema.optionalKey(Schema.Int),
  jump: Schema.optionalKey(SeamJump),
  /** Names of the frames either side, written beside the run, never into this file. */
  frames: Schema.optionalKey(Schema.Array(Schema.String)),
});
export type ItemSeam = typeof ItemSeam.Type;

/** A scheduled item as its as-run told it, in run-relative milliseconds. */
export const RunItem = Schema.Struct({
  key: Schema.String,
  submittedMs: Ms,
  readyMs: Schema.optionalKey(Ms),
  startedMs: Schema.optionalKey(Ms),
  endedMs: Schema.optionalKey(Ms),
  seconds: Schema.optionalKey(Schema.Finite),
  airedSeconds: Schema.optionalKey(Schema.Finite),
  termination: Schema.optionalKey(Schema.Literals(["finished", "stopped"])),
  /** The last as-run status. */
  last: Schema.String,
  dropped: Schema.optionalKey(Schema.String),
});
export type RunItem = typeof RunItem.Type;

export const Pressure = Schema.Struct({
  deliveredVideo: Counter,
  deliveredAudio: Counter,
  droppedVideo: Counter,
  droppedAudio: Counter,
  readerOverflows: Counter,
});
export type Pressure = typeof Pressure.Type;

export const StatsSample = Schema.Struct({
  atMs: Ms,
  local: Schema.optionalKey(Schema.String),
  remote: Schema.optionalKey(Schema.String),
  rttMs: Schema.optionalKey(Schema.Finite),
  receivedKbps: Schema.optionalKey(Schema.Finite),
  availableIncomingKbps: Schema.optionalKey(Schema.Finite),
  fps: Schema.optionalKey(Schema.Finite),
  jitterMs: Schema.optionalKey(Schema.Finite),
  lossRatio: Schema.optionalKey(Schema.Finite),
});
export type StatsSample = typeof StatsSample.Type;

/** What the coordinator said about the session after termination was requested. */
export const TrailEntry = Schema.Struct({ atMs: Ms, state: Schema.String });

export const Criterion = Schema.Struct({
  name: Schema.String,
  passed: Schema.Boolean,
  detail: Schema.optionalKey(Schema.String),
});
export type Criterion = typeof Criterion.Type;

const Lifecycle = Schema.Struct({ atMs: Ms, message: Schema.String, transportGeneration: Counter });

const ElapsedMs = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const Grant = Schema.Struct({
  maxSessions: Schema.Natural,
  maxSessionSeconds: Schema.Natural,
  expiresAt: Schema.Finite,
});
const Handoff = Schema.Struct({
  replacementSessionId: Schema.String,
  decision: Schema.Literals(["no-observed-start", "count-complete", "grace-elapsed"]),
  finalClip: Schema.Union([
    Schema.TaggedStruct("NoObservedStart", {}),
    Schema.TaggedStruct("Observed", {
      clipId: Schema.String,
      expectedVideoFrames: Schema.Natural,
      receivedVideoFrames: Schema.Natural,
      videoStatus: Schema.Literals(["count-complete", "incomplete"]),
    }),
  ]),
  grace: Schema.Union([
    Schema.TaggedStruct("NotObserved", {}),
    Schema.TaggedStruct("Observed", {
      origin: Schema.Literals(["Ended", "Idle"]),
      elapsedMs: ElapsedMs,
      limitMs: ElapsedMs,
    }),
  ]),
}).check(
  Schema.makeFilter(
    (value) => {
      const clip = value.finalClip,
        grace = value.grace;
      if (value.decision === "no-observed-start") return clip._tag === "NoObservedStart";
      if (clip._tag !== "Observed") return false;
      const complete = clip.receivedVideoFrames >= clip.expectedVideoFrames;
      if ((clip.videoStatus === "count-complete") !== complete) return false;
      return value.decision === "count-complete"
        ? complete
        : !complete && grace._tag === "Observed" && grace.elapsedMs >= grace.limitMs;
    },
    { message: "handoff decision must agree with the observed counts and grace" },
  ),
);

const ItemObservation = Schema.Struct({
  _tag: Schema.Literals([
    "Accepted",
    "Building",
    "Ready",
    "Started",
    "Ended",
    "Dropped",
    "Failed",
    "Unknown",
    "Unobserved",
  ]),
  atMs: ElapsedMs,
  sessionId: Schema.optionalKey(Schema.String),
  terminal: Schema.optionalKey(Schema.Boolean),
  termination: Schema.optionalKey(Schema.Literals(["finished", "stopped"])),
  failureKind: Schema.optionalKey(Schema.String),
});

/** Partial checkpoints are valid; complete-pass requirements live in renewalProblems. */
export const SchedulerRenewal = Schema.Struct({
  version: Schema.Literal(1),
  clock: Schema.Literal("effect-monotonic"),
  scenario: Schema.Literal("two-source-accepted-drain"),
  configuration: Schema.Struct({
    constructor: Schema.Literals(["legacy", "continuous"]),
    retainedSuccessfulCleanups: Schema.optionalKey(Schema.Literal(1)),
    maxUnresolvedCleanups: Schema.optionalKey(Schema.Literal(2)),
    setupLimitMs: Schema.Literal(20000),
    workLimitMs: Schema.Literal(40000),
    cleanupLimitMs: Schema.Literal(20000),
    leadMs: Schema.Literal(40000),
    graceMs: Schema.Literal(250),
    clipSeconds: Schema.Literal(5),
    maxSessions: Schema.Literal(2),
    maxOpenAttempts: Schema.Literal(2),
  }).check(
    Schema.makeFilter(
      (configuration) =>
        configuration.constructor === "continuous"
          ? configuration.retainedSuccessfulCleanups === 1 &&
            configuration.maxUnresolvedCleanups === 2
          : configuration.retainedSuccessfulCleanups === undefined &&
            configuration.maxUnresolvedCleanups === undefined,
      { message: "retention settings must match the selected constructor" },
    ),
  ),
  openAttempts: Schema.Natural,
  allocations: Schema.Array(
    Schema.Struct({
      slot: Schema.Literals([1, 2]),
      grant: Schema.optionalKey(Grant),
      /** An open whose allocation the SDK could not settle: a session may exist under the grant. */
      allocation: Schema.optionalKey(Schema.Literal("unknown")),
      sessionId: Schema.optionalKey(Schema.String),
      allocatedAt: Schema.optionalKey(Schema.String),
      capEndsAt: Schema.optionalKey(Schema.String),
      allocatedMs: Schema.optionalKey(ElapsedMs),
      closeRequestedMs: Schema.optionalKey(ElapsedMs),
      closedMs: Schema.optionalKey(ElapsedMs),
      cleanup: Schema.optionalKey(Schema.toCodecJson(Orchestration.SourceCleanup)),
      leaseCleanup: Schema.optionalKey(Schema.toCodecJson(Reactor.CloseReport)),
    }),
  ).check(Schema.isMaxLength(2)),
  items: Schema.Array(
    Schema.Struct({
      key: Schema.Literals(["qualification-A", "qualification-B"]),
      requestedSeconds: Schema.Literal(5),
      clipId: Schema.optionalKey(Schema.String),
      sessionId: Schema.optionalKey(Schema.String),
      statuses: Schema.Array(ItemObservation).check(Schema.isMaxLength(128)),
    }),
  ).check(Schema.isMaxLength(2)),
  prepared: Schema.optionalKey(
    Schema.Struct({ atMs: ElapsedMs, preferredSessionId: Schema.String }),
  ),
  switches: Schema.Array(
    Schema.Struct({
      atMs: ElapsedMs,
      retiringSessionId: Schema.String,
      handoff: Schema.optionalKey(Handoff),
      tail: Schema.Struct({
        video: Schema.Struct({
          framesPerSecond: Schema.Finite,
          expectedFrames: Schema.Natural,
          receivedFrames: Schema.Natural,
          status: Schema.Literals(["not-started", "count-complete", "incomplete"]),
        }),
        audio: Schema.Struct({
          receivedSamples: Schema.Natural,
          status: Schema.Literal("unverified"),
        }),
        sourceDrops: Schema.Struct({
          video: Schema.NullOr(Counter),
          audio: Schema.NullOr(Counter),
        }),
        forwarded: Schema.Struct({
          queuedVideoFrames: Schema.Natural,
          queuedAudioSamples: Schema.Natural,
        }),
      }),
    }),
  ).check(Schema.isMaxLength(4)),
  media: Schema.Struct({
    video: VideoSummary,
    audio: AudioSummary,
    audioCompleteness: Schema.Literal("unverified"),
    attributionComplete: Schema.Boolean,
    sources: Schema.Array(
      Schema.Struct({
        sessionId: Schema.String,
        generation: Counter,
        video: VideoSummary,
        audio: AudioSummary,
      }),
    ).check(Schema.isMaxLength(16)),
    decodedBoundary: Schema.optionalKey(
      Schema.Struct({
        retiringSessionId: Schema.String,
        replacementSessionId: Schema.String,
        lastRetiringFrameMs: ElapsedMs,
        firstReplacementFrameMs: ElapsedMs,
        gapMs: Schema.Finite,
      }),
    ),
    pressure: Schema.optionalKey(Pressure),
  }),
  fillerRequests: Schema.Natural,
  fillerEvents: Schema.Array(Schema.Struct({ key: Schema.String, status: ItemObservation })).check(
    Schema.isMaxLength(128),
  ),
  drain: Schema.optionalKey(
    Schema.Struct({
      requestedMs: ElapsedMs,
      completedMs: Schema.optionalKey(ElapsedMs),
      acceptedKeys: Schema.Array(Schema.String),
      outcome: Schema.Literals(["pending", "completed", "failed"]),
      allocationsWhenRequested: Schema.Natural,
      allocationsWhenCompleted: Schema.optionalKey(Schema.Natural),
    }),
  ),
  cleanup: Schema.optionalKey(
    Schema.Union([
      Schema.TaggedStruct("Legacy", {
        report: Schema.optionalKey(Schema.toCodecJson(Orchestration.CleanupReport)),
        requestedMs: ElapsedMs,
        completedMs: Schema.optionalKey(ElapsedMs),
        incomplete: Schema.Array(Schema.String),
      }),
      Schema.TaggedStruct("Continuous", {
        summary: Schema.optionalKey(Schema.toCodecJson(Orchestration.CleanupSummary)),
        requestedMs: ElapsedMs,
        completedMs: Schema.optionalKey(ElapsedMs),
        incomplete: Schema.Array(Schema.String),
      }),
    ]),
  ),
});
export type SchedulerRenewal = typeof SchedulerRenewal.Type;

export const Evidence = Schema.Struct({
  format: Schema.Literal(format),
  runId: Schema.String,
  check: Schema.Literals(checks),
  mode: Schema.Literals(["paid", "rehearsal"]),
  /** Wall time of run start; schedulerRenewal timings use a separate monotonic origin. */
  startedAt: Schema.String,
  finishedAt: Schema.optionalKey(Schema.String),
  environment: Schema.Struct({
    runtime: Schema.String,
    os: Schema.String,
    commit: Schema.optionalKey(Schema.String),
    dirty: Schema.optionalKey(Schema.Boolean),
    /** Each package as it was loaded: name to version. */
    packages: Schema.Record(Schema.String, Schema.String),
    /** The loaded native library's identity, as staged next to it. */
    native: Schema.optionalKey(Schema.Record(Schema.String, Scalar)),
    network: Schema.String,
    apiOrigin: Schema.String,
  }),
  budget: Schema.Struct({
    checkUsd: Usd,
    totalUsd: Usd,
    /** What earlier paid runs in the ledger had reserved when this one was admitted. */
    reservedBeforeUsd: Usd,
    sessionSeconds: Schema.Natural,
    rate: Schema.optionalKey(
      Schema.Struct({ creditsPerSecond: Schema.Finite, creditsPerDollar: Schema.Finite }),
    ),
    /** Set once admitted, before any token exists: what the ledger reserves for this run. */
    worstCaseUsd: Schema.optionalKey(Usd),
    /** Allocation to the confirmed end of the session, at the published rate. */
    estimatedUsd: Schema.optionalKey(Usd),
  }),
  grant: Schema.optionalKey(
    Schema.Struct({
      maxSessions: Schema.Natural,
      maxSessionSeconds: Schema.Natural,
      expiresAt: Schema.Finite,
    }),
  ),
  session: Schema.optionalKey(
    Schema.Struct({
      id: Schema.String,
      allocatedMs: Ms,
      endedMs: Schema.optionalKey(Ms),
      tracks: Schema.Array(
        Schema.Struct({ name: Schema.String, kind: Schema.String, direction: Schema.String }),
      ),
    }),
  ),
  /** What the coordinator reports about the session once it is connected. */
  server: Schema.optionalKey(
    Schema.Struct({
      cluster: Schema.NullOr(Schema.String),
      zone: Schema.NullOr(Schema.String),
      serverVersion: Schema.NullOr(Schema.String),
      transport: Schema.NullOr(Schema.String),
      additional: Schema.Record(
        Schema.String,
        Schema.Union([Schema.String, Schema.Finite, Schema.Boolean, Schema.Null]),
      ),
    }),
  ),
  milestones: Schema.Array(Milestone),
  spans: Schema.Array(SpanRecord),
  contract: Schema.optionalKey(
    Schema.Struct({
      modelName: Schema.String,
      documentedVersion: Schema.String,
      deploymentTitle: Schema.NullOr(Schema.String),
      deploymentVersion: Schema.NullOr(Schema.String),
      messages: Counts,
      unknown: Counts,
      duplicates: Schema.Natural,
      stale: Schema.Natural,
      diagnostics: Counts,
      /** Whether the deployment's `enqueue` declares `reference_audios`. */
      referenceAudio: Schema.optionalKey(Schema.Boolean),
      /** The deployment's last `state_update`: canvas, capacities and clip bounds. */
      lastState: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  ),
  acceptance: Schema.optionalKey(
    Schema.Struct({
      clipId: Schema.String,
      evidence: Schema.Literals(["correlated", "metadata"]),
      transportGeneration: Counter,
      submitMs: Ms,
      acceptedMs: Ms,
      /** Later provider messages that listed the clip with the submission's metadata intact. */
      metadataEchoes: Counts,
      /**
       * The references the submission carried, and what the accepted clip
       * reports of them (null when the deployment leaves a field out).
       */
      references: Schema.optionalKey(
        Schema.Struct({
          images: Schema.Natural,
          audio: Schema.Natural,
          reportedImages: Schema.NullOr(Schema.Natural),
          reportedAudio: Schema.NullOr(Schema.Natural),
          hasReferenceAudio: Schema.NullOr(Schema.Boolean),
        }),
      ),
    }),
  ),
  lifecycle: Schema.optionalKey(
    Schema.Struct({
      generated: Schema.optionalKey(Lifecycle),
      started: Schema.optionalKey(Lifecycle),
      ended: Schema.optionalKey(Lifecycle),
    }),
  ),
  media: Schema.optionalKey(
    Schema.Struct({
      audioOffered: Schema.Boolean,
      /** The first frame after the clip was reported started. */
      firstClipFrameMs: Schema.optionalKey(Ms),
      video: VideoSummary,
      audio: Schema.optionalKey(AudioSummary),
      pressure: Schema.optionalKey(Pressure),
    }),
  ),
  network: Schema.optionalKey(
    Schema.Struct({
      pair: Schema.optionalKey(
        Schema.Struct({
          local: Schema.NullOr(Schema.String),
          remote: Schema.NullOr(Schema.String),
        }),
      ),
      samples: Schema.Array(StatsSample),
    }),
  ),
  takeover: Schema.optionalKey(
    Schema.Struct({
      ownerStreamingMs: Ms,
      killedMs: Ms,
      attachMs: Schema.optionalKey(Ms),
      clipIdentified: Schema.optionalKey(Schema.Boolean),
      metadataPreserved: Schema.optionalKey(Schema.Boolean),
      enqueuesAfterAttach: Schema.optionalKey(Schema.Natural),
      firstFreshFrameMs: Schema.optionalKey(Ms),
      video: Schema.optionalKey(VideoSummary),
    }),
  ),
  termination: Schema.optionalKey(
    Schema.Struct({
      requestedMs: Ms,
      /** When the library's report returned. */
      reportedMs: Ms,
      /** Whether the library confirmed the remote end, as its report says. */
      confirmed: Schema.Boolean,
      /** The session's close report, stored through the library's own codec. */
      close: Schema.optionalKey(Schema.toCodecJson(Reactor.CloseReport)),
      /** The coordinator's report when the durable owner record terminated the session. */
      remote: Schema.optionalKey(Schema.toCodecJson(Reactor.Termination)),
      /** Inspections after the request, until the session was terminal or gone. */
      trail: Schema.Array(TrailEntry),
      terminalMs: Schema.optionalKey(Ms),
    }),
  ),
  /** One capped session's queue edits with autoplay on, and the clip boundaries
   * they meet. These are protocol and decoded-media observations, not billing
   * or output proof. */
  scheduler: Schema.optionalKey(
    Schema.Struct({
      builds: Schema.Array(
        Schema.Struct({
          clipId: Schema.String,
          requestedSeconds: Schema.Finite,
          readySeconds: Schema.optionalKey(Schema.Finite),
          submittedMs: Ms,
          readyMs: Schema.optionalKey(Ms),
          /** Every clip is queued at once, so this includes waiting behind the builds ahead of it. */
          submitToReadyMs: Schema.optionalKey(Ms),
        }),
      ),
      positionZero: Schema.optionalKey(
        Schema.Struct({
          buildingClipId: Schema.String,
          requestedClipId: Schema.String,
          generationOrder: Schema.Array(Schema.String),
        }),
      ),
      poppedBuild: Schema.optionalKey(
        Schema.Struct({
          clipId: Schema.String,
          wasGeneration: Schema.Boolean,
          poppedMs: Ms,
          observedUntilMs: Ms,
          generatedAfterPop: Schema.Boolean,
          startedAfterPop: Schema.Boolean,
        }),
      ),
      /** One entry per clip boundary, in playing order, as `schedulerBoundaries` plans them. */
      boundaries: Schema.Array(SchedulerBoundary),
      metadata: Schema.Struct({ observed: Counts, mismatched: Counts }),
      media: Schema.optionalKey(Schema.Struct({ video: VideoSummary, audio: AudioSummary })),
    }),
  ),
  schedulerRenewal: Schema.optionalKey(SchedulerRenewal),
  /**
   * One capped session through the public scheduler: inserts with and without
   * continuity inside a line, and an edit batch timed to take effect just
   * before a boundary. Protocol, as-run and decoded-media observations only.
   */
  schedulerEdits: Schema.optionalKey(
    Schema.Struct({
      items: Schema.Array(RunItem),
      plannedOrder: Schema.Array(Schema.String),
      startOrder: Schema.Array(Schema.String),
      seams: Schema.Array(ItemSeam),
      batch: Schema.optionalKey(
        Schema.Struct({
          submittedMs: Ms,
          committedMs: Schema.optionalKey(Ms),
          /** When the clip playing at the commit ended. */
          boundaryMs: Schema.optionalKey(Ms),
          withdrawn: Schema.Array(Schema.String),
          inserted: Schema.String,
          withdrawnStarted: Schema.Boolean,
        }),
      ),
      estimates: Schema.optionalKey(
        Schema.Struct({
          buildMedian: Schema.optionalKey(Schema.Finite),
          buildP95: Schema.optionalKey(Schema.Finite),
          length: Schema.Finite,
        }),
      ),
      media: Schema.optionalKey(Schema.Struct({ video: VideoSummary, audio: AudioSummary })),
    }),
  ),
  /**
   * One capped session: raw H3 probes of position zero behind a running build,
   * a popped build's hold on the build slot and whether a queue read sent
   * right after an enqueue reflects it, then a cut lane cutting a long clip.
   */
  schedulerCut: Schema.optionalKey(
    Schema.Struct({
      positionZero: Schema.optionalKey(
        Schema.Struct({
          buildingClipId: Schema.String,
          requestedClipId: Schema.String,
          /** Enqueued after the position-zero clip, at the end of the queue. */
          tailClipId: Schema.String,
          generationOrder: Schema.Array(Schema.String),
        }),
      ),
      poppedBuild: Schema.optionalKey(
        Schema.Struct({
          poppedMs: Ms,
          nextSubmittedMs: Ms,
          nextReadyMs: Schema.optionalKey(Ms),
          /** How long the clip queued behind the popped one took to be Ready. */
          nextBuildMs: Schema.optionalKey(Ms),
          /** A lone build's time in the same session, for comparison. */
          loneBuildMs: Schema.optionalKey(Ms),
        }),
      ),
      /** An enqueue then a queue read sent right behind it, without waiting for the enqueue's reply. */
      ordering: Schema.Array(
        Schema.Struct({
          sentMs: Ms,
          listed: Schema.Boolean,
          clipKnown: Schema.Boolean,
          /** Whether the read's reply arrived before the enqueue's. */
          repliedFirst: Schema.optionalKey(Schema.Boolean),
        }),
      ),
      items: Schema.Array(RunItem),
      cut: Schema.optionalKey(
        Schema.Struct({
          longKey: Schema.String,
          cutterKey: Schema.String,
          cutterSubmittedMs: Ms,
          seam: Schema.optionalKey(ItemSeam),
        }),
      ),
      media: Schema.optionalKey(Schema.Struct({ video: VideoSummary, audio: AudioSummary })),
    }),
  ),
  outcomes: Schema.Array(Schema.Literals(["not-submitted", "unknown", "replied"])),
  criteria: Schema.Array(Criterion),
  missing: Schema.Array(Schema.String),
  verdict: Schema.optionalKey(Schema.Literals(["pass", "fail"])),
  reasons: Schema.Array(Schema.String),
  /** What a person must do when the check could not confirm the session ended. */
  cleanup: Schema.optionalKey(Schema.String),
});
export type Evidence = typeof Evidence.Type;
/** The evidence as a run fills it in: its fields, budget and lists can change. */
export type Draft = Omit<Mutable<Evidence>, "budget" | "milestones" | "criteria" | "outcomes"> & {
  budget: Mutable<Evidence["budget"]>;
  milestones: Evidence["milestones"][number][];
  criteria: Criterion[];
  outcomes: Evidence["outcomes"][number][];
};

/**
 * The fields a check must have filled before it can pass. A field left empty
 * fails the run as incomplete: a paid session that returns without its data
 * is a failed qualification, whatever else it saw.
 */
export const required = (evidence: Evidence): readonly string[] => {
  if (evidence.check === "scheduler-renewal")
    return [
      "budget.worstCaseUsd",
      "budget.estimatedUsd",
      "budget.rate",
      "schedulerRenewal",
      "schedulerRenewal.prepared",
      "schedulerRenewal.allocations.0.cleanup",
      "schedulerRenewal.allocations.1.cleanup",
      "schedulerRenewal.switches.0.handoff",
      "schedulerRenewal.media.decodedBoundary",
      "schedulerRenewal.drain.completedMs",
      evidence.schedulerRenewal?.configuration.constructor === "continuous"
        ? "schedulerRenewal.cleanup.summary"
        : "schedulerRenewal.cleanup.report",
    ];
  const common = [
    "budget.worstCaseUsd",
    "budget.estimatedUsd",
    "grant",
    "session",
    "session.endedMs",
    "termination",
  ];
  if (evidence.check === "scheduler")
    return [
      ...common,
      "scheduler.builds.0.readyMs",
      "scheduler.builds.0.readySeconds",
      "scheduler.positionZero",
      "scheduler.poppedBuild",
      // An edit that was not staged leaves its command empty: its boundary answers nothing.
      ...schedulerBoundaries.flatMap(({ edit }, index) => [
        `scheduler.boundaries.${index}.ending.finishedMs`,
        `scheduler.boundaries.${index}.next`,
        `scheduler.boundaries.${index}.pause`,
        ...(edit === "none" ? [] : [`scheduler.boundaries.${index}.command`]),
      ]),
      "scheduler.media.video.arrivalsMs.0",
    ];
  if (evidence.check === "scheduler-edits")
    return [
      ...common,
      "schedulerEdits.items.0.startedMs",
      "schedulerEdits.seams.0.jump",
      "schedulerEdits.seams.0.pause",
      "schedulerEdits.batch.committedMs",
      "schedulerEdits.media.video.arrivalsMs.0",
    ];
  if (evidence.check === "scheduler-cut")
    return [
      ...common,
      "schedulerCut.positionZero",
      "schedulerCut.poppedBuild.nextReadyMs",
      "schedulerCut.ordering.0",
      "schedulerCut.cut.seam.pause",
      "schedulerCut.media.video.arrivalsMs.0",
    ];
  if (evidence.check === "takeover" || evidence.check === "resume")
    return [
      ...common,
      "takeover.attachMs",
      "takeover.clipIdentified",
      "takeover.metadataPreserved",
      "takeover.enqueuesAfterAttach",
      "takeover.firstFreshFrameMs",
      "takeover.video",
      // A resume is ended by the library's own close of the adopted session.
      ...(evidence.check === "resume" ? ["termination.close"] : []),
    ];
  return [
    ...common,
    "contract",
    "acceptance",
    ...(evidence.check === "audio" ? ["contract.referenceAudio", "acceptance.references"] : []),
    "lifecycle.generated",
    "lifecycle.started",
    "server",
    "media.firstClipFrameMs",
    "media.video.fps",
    "media.pressure",
    ...(evidence.media?.audioOffered === true ? ["media.audio"] : []),
    "network.pair",
    "network.samples.0",
  ];
};

const at = (value: unknown, path: string): unknown =>
  path
    .split(".")
    .reduce<unknown>(
      (node, key) =>
        node !== null && typeof node === "object"
          ? (node as Record<string, unknown>)[key]
          : undefined,
      value,
    );

export const missing = (evidence: Evidence): readonly string[] =>
  required(evidence).filter((path) => at(evidence, path) === undefined);

export const renewalCriteria = [
  "public renewal preparation",
  "keyed playback order",
  "planned switch",
  "attributed logical media",
  "accepted drain",
  "owned lease cleanup",
  "bounded allocation",
] as const;

/** Canonical owned-lease evidence, never an inference from a counter or a coordinator timestamp. */
export const confirmedOwnedCleanup = (
  cleanup: Orchestration.SourceCleanup,
  sessionId: string,
): boolean => {
  const lease = cleanup.lease;
  return (
    lease.sessionId === sessionId &&
    lease.ownership === "owned" &&
    lease.allocation === "known" &&
    lease.localClosed &&
    lease.remote.confirmed &&
    lease.remote.error === undefined &&
    lease.remote.responseReceived === (lease.remote.deleteStatus !== null) &&
    (!lease.remote.responseReceived || lease.remote.attempted) &&
    (lease.remote.evidence === "terminal" || lease.remote.evidence === "absent") &&
    lease.localErrors.length === 0 &&
    lease.unresolvedPublications.length === 0 &&
    cleanup.policy.every((row) => Result.isSuccess(row.result))
  );
};

/** Cross-field proof for the two-source scenario, also applied when a file claims passing criteria. */
export const renewalJudgments = (evidence: Evidence): readonly Criterion[] => {
  const run = evidence.schedulerRenewal;
  if (run === undefined)
    return renewalCriteria.map((name) => ({
      name,
      passed: false,
      detail: "missing schedulerRenewal evidence",
    }));
  const problems: { name: (typeof renewalCriteria)[number]; reason: string }[] = [];
  const requireEvidence = (
    condition: boolean | undefined,
    reason: string,
    name: (typeof renewalCriteria)[number],
  ) => {
    if (!condition) problems.push({ name, reason });
  };
  const budget = evidence.budget;
  const rate = budget.rate;
  requireEvidence(
    rate !== undefined &&
      rate.creditsPerSecond > 0 &&
      rate.creditsPerDollar > 0 &&
      budget.worstCaseUsd !== undefined &&
      // Admission's own rounding: both sides are on the ledger's four-decimal grid.
      reservationUsd(worstCaseUsd(rate) * sessionsFor(evidence.check)) <= budget.worstCaseUsd &&
      budget.worstCaseUsd <= budget.checkUsd &&
      budget.checkUsd <= 1.5 &&
      budget.totalUsd <= 3.75 &&
      budget.reservedBeforeUsd + budget.worstCaseUsd <= budget.totalUsd + 1e-9,
    "the two capped sessions must fit their reservation and ledger ceilings",
    "bounded allocation",
  );
  const allocations = run.allocations.filter((slot) => slot.sessionId !== undefined);
  const ids = allocations.map((slot) => slot.sessionId!);
  requireEvidence(
    allocations.length === 2 &&
      new Set(ids).size === 2 &&
      allocations[0]?.slot === 1 &&
      allocations[1]?.slot === 2 &&
      run.openAttempts === 2,
    "bounded allocation requires exactly two distinct sources and two attempts",
    "bounded allocation",
  );
  for (const slot of allocations) {
    requireEvidence(
      slot.grant?.maxSessions === 1 &&
        slot.grant.maxSessionSeconds === 50 &&
        slot.allocatedMs !== undefined &&
        slot.allocatedAt !== undefined &&
        slot.capEndsAt !== undefined,
      "each allocated source needs its granted cap and allocation timing",
      "bounded allocation",
    );
    requireEvidence(
      slot.cleanup !== undefined && confirmedOwnedCleanup(slot.cleanup, slot.sessionId!),
      "each allocated source needs confirmed canonical owned cleanup",
      "owned lease cleanup",
    );
    requireEvidence(
      slot.closedMs !== undefined &&
        slot.closeRequestedMs !== undefined &&
        slot.allocatedMs !== undefined &&
        slot.allocatedMs <= slot.closeRequestedMs &&
        slot.closeRequestedMs <= slot.closedMs,
      "source cleanup timestamps must follow allocation",
      "owned lease cleanup",
    );
  }
  const a = run.items.find((item) => item.key === "qualification-A");
  const b = run.items.find((item) => item.key === "qualification-B");
  requireEvidence(
    run.items.length === 2 && a !== undefined && b !== undefined,
    "both keyed items must be present",
    "keyed playback order",
  );
  const startedA = a?.statuses.find((status) => status._tag === "Started");
  const startedB = b?.statuses.find((status) => status._tag === "Started");
  const endedA = a?.statuses.find((status) => status._tag === "Ended");
  const endedB = b?.statuses.find((status) => status._tag === "Ended");
  requireEvidence(
    startedA !== undefined &&
      startedB !== undefined &&
      endedA !== undefined &&
      endedB !== undefined &&
      startedA.atMs <= endedA.atMs &&
      endedA.atMs <= startedB.atMs &&
      startedB.atMs <= endedB.atMs &&
      endedA.termination === "finished" &&
      endedB.termination === "finished",
    "keyed playback must start and finish A then B",
    "keyed playback order",
  );
  for (const item of run.items) {
    requireEvidence(
      item.clipId !== undefined && item.sessionId !== undefined && ids.includes(item.sessionId),
      "item clip and allocated source identity are required",
      "keyed playback order",
    );
    requireEvidence(
      !item.statuses.some((status) =>
        ["Unknown", "Failed", "Dropped", "Unobserved"].includes(status._tag),
      ),
      "an unknown or failed item cannot qualify playback",
      "keyed playback order",
    );
    requireEvidence(
      item.statuses.filter((status) => status._tag === "Started").length === 1 &&
        item.statuses.filter((status) => status._tag === "Ended").length === 1,
      "each keyed item needs exactly one observed start and end",
      "keyed playback order",
    );
    requireEvidence(
      item.statuses.every(
        (status, index) => index === 0 || item.statuses[index - 1]!.atMs <= status.atMs,
      ),
      "item observations must be in monotonic order",
      "keyed playback order",
    );
  }
  requireEvidence(
    a?.sessionId === ids[0] &&
      b?.sessionId === ids[1] &&
      a?.clipId !== b?.clipId &&
      startedA?.sessionId === ids[0] &&
      startedB?.sessionId === ids[1],
    "keyed items must play on the retiring and replacement sources respectively",
    "keyed playback order",
  );
  requireEvidence(
    run.prepared !== undefined &&
      run.prepared.preferredSessionId === ids[1] &&
      b?.statuses.some(
        (status) =>
          status._tag === "Ready" &&
          status.sessionId === ids[1] &&
          status.atMs >= run.prepared!.atMs,
      ),
    "B must be routed to the observed prepared preferred source",
    "public renewal preparation",
  );
  const switched = run.switches[0];
  requireEvidence(
    run.switches.length === 1 &&
      switched !== undefined &&
      switched.retiringSessionId === ids[0] &&
      switched.handoff !== undefined &&
      switched.handoff.replacementSessionId === ids[1] &&
      switched.handoff.finalClip._tag === "Observed" &&
      switched.handoff.finalClip.clipId === a?.clipId,
    "exactly one matching planned switch with final-clip evidence is required",
    "planned switch",
  );
  // Orders the harness flow guarantees, each checked once both times exist; a
  // missing time fails its own requirement. The first source opens while the
  // owner is built, before A is submitted. Prepared follows the replacement's
  // allocation, and B is submitted only after Prepared and A's Ended are
  // recorded. A switch closes the retiring source only once B is Ready on the
  // replacement, and the SDK announces Switched after that close returns. A
  // completed drain follows that announcement, and the scenario then records
  // the switch before cleanup begins; a failure can start cleanup sooner. B's
  // start is not ordered against the switch: the SDK resumes autoplay before
  // the retiring close.
  const ordered = (earlier: number | undefined, later: number | undefined) =>
    earlier === undefined || later === undefined || earlier <= later;
  const retiring = allocations[0],
    replacement = allocations[1];
  requireEvidence(
    a?.statuses.every((status) => ordered(retiring?.allocatedMs, status.atMs)) !== false,
    "A cannot be observed before its source was allocated",
    "keyed playback order",
  );
  requireEvidence(
    ordered(replacement?.allocatedMs, run.prepared?.atMs),
    "Prepared cannot precede the replacement's allocation",
    "public renewal preparation",
  );
  // B after Prepared is then after the replacement's allocation as well.
  requireEvidence(
    b?.statuses.every((status) => ordered(run.prepared?.atMs, status.atMs)) !== false,
    "B cannot be observed before Prepared",
    "public renewal preparation",
  );
  requireEvidence(
    switched === undefined || ordered(endedA?.atMs, retiring?.closeRequestedMs),
    "a switch cannot close the retiring source before A ended",
    "planned switch",
  );
  requireEvidence(
    ordered(retiring?.closedMs, switched?.atMs),
    "the switch cannot precede the retiring close's return",
    "planned switch",
  );
  requireEvidence(
    ordered(run.prepared?.atMs, switched?.atMs),
    "the switch cannot precede Prepared",
    "planned switch",
  );
  requireEvidence(
    run.drain?.outcome !== "completed" || ordered(switched?.atMs, run.cleanup?.requestedMs),
    "after a completed drain, the switch must precede cleanup",
    "planned switch",
  );
  requireEvidence(
    switched?.handoff?.grace._tag !== "Observed" ||
      switched.handoff.grace.limitMs === run.configuration.graceMs,
    "an observed handoff grace must report the configured limit",
    "planned switch",
  );
  requireEvidence(
    run.media.sources.every(
      (source) =>
        ids.includes(source.sessionId) &&
        source.video.frames === source.video.arrivalsMs?.length &&
        source.audio.blocks === source.audio.arrivalsMs?.length,
    ) &&
      run.media.video.frames === run.media.video.arrivalsMs?.length &&
      run.media.audio.blocks === run.media.audio.arrivalsMs?.length &&
      new Set(run.media.sources.map((source) => `${source.sessionId}/${source.generation}`))
        .size === run.media.sources.length,
    "media counts and unique source generations must match their retained arrivals",
    "attributed logical media",
  );
  const arrivalsMatch = (
    logical: readonly number[] | undefined,
    parts: readonly (readonly number[] | undefined)[],
  ) => {
    if (logical === undefined) return false;
    const attributed = parts.flatMap((part) => part ?? []).sort((left, right) => left - right);
    return (
      logical.length === attributed.length &&
      logical.every((at, index) => at >= 0 && at === attributed[index])
    );
  };
  requireEvidence(
    arrivalsMatch(
      run.media.video.arrivalsMs,
      run.media.sources.map((source) => source.video.arrivalsMs),
    ) &&
      arrivalsMatch(
        run.media.audio.arrivalsMs,
        run.media.sources.map((source) => source.audio.arrivalsMs),
      ),
    "source-attributed arrivals must reconcile exactly with logical monotonic arrivals",
    "attributed logical media",
  );
  const boundary = run.media.decodedBoundary;
  const sourceVideo = (id: string | undefined) =>
    run.media.sources
      .filter((source) => source.sessionId === id)
      .flatMap((source) => source.video.arrivalsMs ?? []);
  const oldVideo = sourceVideo(ids[0]),
    newVideo = sourceVideo(ids[1]);
  requireEvidence(
    run.media.attributionComplete &&
      oldVideo.length > 0 &&
      newVideo.length > 0 &&
      run.media.video.frames ===
        run.media.sources.reduce((sum, source) => sum + source.video.frames, 0) &&
      run.media.audio.blocks ===
        run.media.sources.reduce((sum, source) => sum + source.audio.blocks, 0),
    "logical video from both sources and complete frame attribution are required",
    "attributed logical media",
  );
  requireEvidence(
    boundary !== undefined &&
      boundary.retiringSessionId === ids[0] &&
      boundary.replacementSessionId === ids[1] &&
      boundary.lastRetiringFrameMs === Math.max(...oldVideo) &&
      boundary.firstReplacementFrameMs === Math.min(...newVideo) &&
      boundary.gapMs === boundary.firstReplacementFrameMs - boundary.lastRetiringFrameMs &&
      boundary.gapMs >= 0,
    "decoded boundary must match the source-attributed logical arrivals",
    "attributed logical media",
  );
  const drain = run.drain;
  requireEvidence(
    drain?.outcome === "completed" &&
      drain.completedMs !== undefined &&
      drain.completedMs >= drain.requestedMs &&
      endedB !== undefined &&
      drain.completedMs >= endedB.atMs &&
      drain.acceptedKeys.length === 2 &&
      new Set(drain.acceptedKeys).size === 2 &&
      drain.acceptedKeys.includes("qualification-A") &&
      drain.acceptedKeys.includes("qualification-B") &&
      drain.allocationsWhenRequested === 2 &&
      drain.allocationsWhenCompleted === 2 &&
      allocations.every(
        (slot) => slot.allocatedMs !== undefined && slot.allocatedMs <= drain.requestedMs,
      ),
    "accepted drain must finish both keys without a later allocation",
    "accepted drain",
  );
  requireEvidence(
    drain !== undefined &&
      run.prepared !== undefined &&
      drain.requestedMs >= run.prepared.atMs &&
      b?.statuses.some(
        (status) =>
          (status._tag === "Building" || status._tag === "Ready") &&
          status.atMs <= drain.requestedMs,
      ),
    "drain must follow replacement preparation and B admission",
    "accepted drain",
  );
  // A missing allocation or an unfinished drain fails its own requirement above;
  // these name a deadline only when the recorded times show it elapsed.
  const first = allocations[0]?.allocatedMs;
  requireEvidence(
    first === undefined || first <= run.configuration.setupLimitMs,
    "the first allocation came after the shared setup deadline",
    "bounded allocation",
  );
  requireEvidence(
    first === undefined ||
      drain?.completedMs === undefined ||
      drain.completedMs <= first + run.configuration.workLimitMs,
    "the accepted drain completed after the shared work deadline",
    "bounded allocation",
  );
  const cleanup = run.cleanup;
  requireEvidence(
    cleanup?.completedMs !== undefined &&
      cleanup.incomplete.length === 0 &&
      cleanup.completedMs >= cleanup.requestedMs &&
      cleanup.completedMs <= cleanup.requestedMs + run.configuration.cleanupLimitMs,
    "canonical cleanup must complete within its observation budget",
    "owned lease cleanup",
  );
  const sameCleanup = (left: Orchestration.SourceCleanup, right: Orchestration.SourceCleanup) =>
    JSON.stringify(Schema.encodeSync(Schema.toCodecJson(Orchestration.SourceCleanup))(left)) ===
    JSON.stringify(Schema.encodeSync(Schema.toCodecJson(Orchestration.SourceCleanup))(right));
  if (run.configuration.constructor === "legacy") {
    const reports = cleanup?._tag === "Legacy" ? (cleanup.report?.sessions ?? []) : [];
    requireEvidence(
      reports.length === 2 &&
        allocations.every((slot) =>
          reports.some(
            (report) =>
              confirmedOwnedCleanup(report, slot.sessionId!) &&
              slot.cleanup !== undefined &&
              sameCleanup(report, slot.cleanup),
          ),
        ),
      "legacy cleanup must agree with both recorded source-close reports",
      "owned lease cleanup",
    );
    requireEvidence(
      evidence.mode !== "paid",
      "hosted qualification requires the continuous constructor",
      "owned lease cleanup",
    );
  } else {
    const summary = cleanup?._tag === "Continuous" ? cleanup.summary : undefined;
    const retained = summary?.retained[0];
    // The summary intentionally compacts A. Its omission count is meaningful only
    // alongside the independent, canonical reports retained for both actual leases.
    requireEvidence(
      summary?.totalRetirements === 2n &&
        summary.omittedComplete.ownedTerminated === 1n &&
        summary.omittedComplete.noAllocation === 0n &&
        summary.omittedComplete.attachedDetached === 0n &&
        !summary.exhausted &&
        summary.retained.length === 1 &&
        retained?.ordinal === 2n &&
        retained.source?.incarnation === 2n &&
        retained.source.sessionId === ids[1] &&
        retained.disposition === "complete" &&
        retained.conflictingCleanup === undefined &&
        retained.retirement.accounting === "settled" &&
        retained.retirement.scope === "closed" &&
        retained.retirement.affinity === "retired" &&
        retained.retirement.unknownSubmissions === 0n &&
        retained.retirement.errors.length === 0 &&
        retained.cleanup !== undefined &&
        allocations[1]?.cleanup !== undefined &&
        confirmedOwnedCleanup(retained.cleanup, ids[1]) &&
        sameCleanup(retained.cleanup, allocations[1].cleanup),
      "continuous cleanup must reconcile one omitted and one retained complete owned termination with both source reports",
      "owned lease cleanup",
    );
  }
  return renewalCriteria.map((name) => {
    const failures = problems.filter((problem) => problem.name === name);
    return {
      name,
      passed: failures.length === 0,
      ...(failures.length === 0
        ? {}
        : { detail: failures.map((failure) => failure.reason).join("; ") }),
    };
  });
};

export const renewalProblems = (evidence: Evidence): readonly string[] => [
  ...renewalJudgments(evidence)
    .filter((criterion) => !criterion.passed)
    .map((criterion) => criterion.detail ?? criterion.name),
  ...renewalCriteria
    .filter((name) => !evidence.criteria.some((criterion) => criterion.name === name))
    .map((name) => `missing required criterion: ${name}`),
];

/**
 * The run's verdict: it passes only when every criterion it evaluated passed,
 * nothing required is missing, no stop rule fired and the check itself
 * completed. `failure` is why the check did not complete, if it did not.
 */
const verdictOf = (evidence: Evidence, failure: string | undefined) => {
  const reasons: string[] = [];
  const stop = stopFor({
    outcomes: evidence.outcomes,
    terminationConfirmed:
      evidence.schedulerRenewal?.allocations.some(
        (slot) =>
          slot.cleanup?.lease.remote.confirmed === false ||
          // A failed open that allocated nothing has nothing to terminate.
          (slot.leaseCleanup !== undefined &&
            slot.leaseCleanup.allocation !== "none" &&
            !slot.leaseCleanup.remote.confirmed),
      ) === true || evidence.termination?.confirmed === false
        ? false
        : evidence.termination?.confirmed,
  });
  if (stop !== undefined) reasons.push(stop);
  if (failure !== undefined) reasons.push(failure);
  if (evidence.check === "scheduler-renewal") reasons.push(...renewalProblems(evidence));
  for (const criterion of evidence.criteria)
    if (!criterion.passed)
      reasons.push(
        `${criterion.name}${criterion.detail === undefined ? "" : `: ${criterion.detail}`}`,
      );
  const absent = missing(evidence);
  if (absent.length > 0) reasons.push(`incomplete evidence: ${absent.join(", ")}`);
  if (evidence.criteria.length === 0) reasons.push("no criterion was evaluated");
  return {
    missing: absent,
    reasons,
    verdict: reasons.length === 0 ? ("pass" as const) : ("fail" as const),
  };
};

/** Records the run's verdict, its reasons and what it is missing. */
export const conclude = (evidence: Draft, failure: string | undefined): void => {
  const judged = verdictOf(evidence, failure);
  evidence.missing = [...judged.missing];
  evidence.reasons = judged.reasons;
  evidence.verdict = judged.verdict;
};

/**
 * A ledger entry as it is judged today. A stored scheduler-renewal pass is
 * recomputed from its own evidence, criteria included, since the stored
 * verdict and criteria alone cannot qualify the check; every other stored
 * verdict is a historical record and stays as written.
 */
export const rejudged = (evidence: Evidence): Evidence => {
  if (evidence.check !== "scheduler-renewal" || evidence.verdict !== "pass") return evidence;
  const judged = verdictOf(evidence, undefined);
  return judged.verdict === "pass"
    ? evidence
    : {
        ...evidence,
        criteria: renewalJudgments(evidence),
        missing: judged.missing,
        reasons: ["the stored pass is not supported by its evidence", ...judged.reasons],
        verdict: "fail",
      };
};

/** An instant as ISO text, or a phrase when the recorded value cannot be one. */
const instant = (ms: number): string => {
  const date = new Date(ms);
  return Number.isNaN(date.getTime()) ? "an unrepresentable time" : date.toISOString();
};

/**
 * What a person must do when the run could not confirm that a session ended,
 * or could not tell whether one was allocated: the Reactor dashboard is then
 * the only remaining record.
 */
/** Every session identity the run holds, for reconciling billing when its file is incomplete. */
export const recordedSessions = (evidence: Evidence): readonly string[] => [
  ...(evidence.session === undefined ? [] : [evidence.session.id]),
  ...(evidence.schedulerRenewal?.allocations ?? []).flatMap((slot) =>
    slot.sessionId === undefined ? [] : [slot.sessionId],
  ),
];

export const cleanupInstructions = (evidence: Evidence): string | undefined => {
  const origin = Date.parse(evidence.startedAt);
  const lines: string[] = [];
  const session = evidence.session;
  if (session !== undefined && evidence.termination?.confirmed !== true)
    lines.push(
      `Session ${session.id} was not confirmed ended. Its token capped it at ${sessionSeconds} s, so the server ends it by ${instant(origin + session.allocatedMs + sessionSeconds * 1000)}; confirm in the Reactor dashboard that it ended, and what it cost.`,
    );
  for (const slot of evidence.schedulerRenewal?.allocations ?? [])
    if (
      slot.sessionId !== undefined &&
      (slot.cleanup?.lease ?? slot.leaseCleanup)?.remote.confirmed !== true
    )
      lines.push(
        `Session ${slot.sessionId} was not confirmed ended. Its recorded cap expiry is ${slot.capEndsAt ?? "unknown"}; confirm termination and cost in the Reactor dashboard.`,
      );
    else if (slot.allocation === "unknown")
      lines.push(
        `Source ${slot.slot}'s allocation outcome is unknown. A session its grant allocated runs at most ${sessionSeconds} s, and the grant expires at ${slot.grant === undefined ? "an unrecorded time" : instant(slot.grant.expiresAt * 1000)}; check the Reactor dashboard for a session under it, and confirm its termination and cost.`,
      );
  return lines.length === 0 ? undefined : lines.join("\n");
};

const encode = Schema.encodeSync(Evidence);
const decode = Schema.decodeUnknownSync(Evidence);

/**
 * Saves one run's evidence as it grows. The first save claims a new file, so
 * a run never overwrites another's; each later save replaces it atomically.
 * A save whose text contains any of `secrets` is refused and nothing is
 * written, so a credential cannot reach the evidence even through a bug.
 */
export class Writer {
  private claimed = false;
  constructor(
    readonly path: string,
    private readonly secrets: () => readonly string[],
  ) {}

  save(evidence: Draft): void {
    const text = `${JSON.stringify(encode(evidence), null, 2)}\n`;
    for (const secret of this.secrets())
      if (secret.length >= 8 && text.includes(secret))
        throw new Error("the evidence would contain a credential; it was not written");
    if (!this.claimed) {
      writeFileSync(this.path, text, { flag: "wx", mode: 0o600 });
      this.claimed = true;
      return;
    }
    const pending = `${this.path}.pending`;
    writeFileSync(pending, text, { mode: 0o600 });
    renameSync(pending, this.path);
  }
}

export interface LedgerEntry {
  readonly file: string;
  readonly evidence: Evidence;
}

/** Every run's evidence in the ledger directory; one that does not decode refuses. */
export const readLedger = (directory: string): readonly LedgerEntry[] => {
  if (!existsSync(directory)) return [];
  return readdirSync(directory)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .map((name) => {
      const file = join(directory, name);
      try {
        return { file, evidence: decode(JSON.parse(readFileSync(file, "utf8"))) };
      } catch {
        throw new Refused({
          message: `${file} is not readable evidence, so the spend it records is unknown`,
        });
      }
    });
};

/**
 * What a ledger entry reserves: a paid run's worst case once it was admitted,
 * and the whole per-check budget if it somehow holds a grant without one.
 */
export const reservedUsd = (evidence: Evidence): number =>
  evidence.mode !== "paid"
    ? 0
    : (evidence.budget.worstCaseUsd ??
      (evidence.grant === undefined &&
      !evidence.schedulerRenewal?.allocations.some((slot) => slot.grant !== undefined)
        ? 0
        : ceilingFor(evidence.check)));

/** The ledger's lock: one paid run at a time may reserve budget. */
export const lockLedger = (directory: string): (() => void) => {
  const lock = join(directory, ".lock");
  try {
    writeFileSync(lock, `${process.pid}\n`, { flag: "wx" });
  } catch {
    throw new Refused({
      message: `${lock} exists: another run holds the ledger, or one crashed; remove it once no run is active`,
    });
  }
  return () => rmSync(lock, { force: true });
};
