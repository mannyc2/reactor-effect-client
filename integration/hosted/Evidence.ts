/**
 * The evidence one run leaves: one Schema for every check, what each check must
 * fill, and the verdict it supports. A paid session that comes back without its
 * data fails as incomplete, never passes with gaps. It never holds a
 * credential, an address, SDP, a frame, audio or provider text.
 */
import * as Schema from "effect/Schema";
import { CloseReport } from "reactor-effect-client/Session";
import { Termination } from "reactor-effect-client/Coordinator";
import { Check } from "./Spend.js";

export const format = "reactor-hosted-qualification/v2";

const Ms = Schema.Finite;
const Usd = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const Counts = Schema.Record(Schema.String, Schema.Int);

export const Outcome = Schema.Literals(["not-submitted", "unknown", "replied"]);
export type Outcome = typeof Outcome.Type;

export const Criterion = Schema.Struct({
  name: Schema.String,
  passed: Schema.Boolean,
  detail: Schema.optionalKey(Schema.String),
});
export type Criterion = typeof Criterion.Type;

const Spread = Schema.Struct({ p50: Ms, p95: Ms, max: Ms });

/** What a video reader saw: arrivals and sampled digests, never a frame. */
export const VideoSummary = Schema.Struct({
  frames: Schema.Int,
  formats: Schema.Array(Schema.String),
  sizes: Schema.Array(Schema.String),
  firstFrameMs: Schema.optionalKey(Ms),
  fps: Schema.optionalKey(Schema.Finite),
  interval: Schema.optionalKey(Spread),
  /** Frames whose mean luma is above black. */
  lit: Schema.Int,
  /** Distinct sampled digests. */
  distinct: Schema.Int,
  lost: Schema.Int,
  gaps: Schema.Int,
});
export type VideoSummary = typeof VideoSummary.Type;

export const AudioSummary = Schema.Struct({
  blocks: Schema.Int,
  sampleRates: Schema.Array(Schema.Int),
  channels: Schema.Array(Schema.Int),
  peakRms: Schema.Finite,
  lost: Schema.Int,
});
export type AudioSummary = typeof AudioSummary.Type;

/** The longest stretch around a boundary with no new picture: a held frame, black or none. */
export const Pause = Schema.Struct({
  lastNewFrameMs: Ms,
  firstNewFrameMs: Ms,
  durationMs: Ms,
  frames: Schema.Int,
  dark: Schema.Int,
});
export type Pause = typeof Pause.Type;

/** The largest change between consecutive frames, against the ending clip's own motion. */
export const Jump = Schema.Struct({
  atMs: Ms,
  change: Schema.Finite,
  typical: Schema.Finite,
  ratio: Schema.Finite,
});
export type Jump = typeof Jump.Type;

/** One boundary between two clips as the decoded picture shows it. */
export const Seam = Schema.Struct({
  ending: Schema.String,
  next: Schema.String,
  continued: Schema.Boolean,
  endedMs: Schema.optionalKey(Ms),
  startedMs: Schema.optionalKey(Ms),
  pause: Schema.optionalKey(Pause),
  darkFrames: Schema.optionalKey(Schema.Int),
  jump: Schema.optionalKey(Jump),
  /** Review images written beside the run, never into it. */
  frames: Schema.String.pipe(Schema.Array, Schema.optionalKey),
});
export type Seam = typeof Seam.Type;

/** One playout item as the run saw it, from its as-run. */
export const Item = Schema.Struct({
  key: Schema.String,
  submittedMs: Ms,
  readyMs: Schema.optionalKey(Ms),
  startedMs: Schema.optionalKey(Ms),
  endedMs: Schema.optionalKey(Ms),
  seconds: Schema.optionalKey(Schema.Finite),
  airedSeconds: Schema.optionalKey(Schema.Finite),
  termination: Schema.optionalKey(Schema.Literals(["finished", "stopped"])),
  dropped: Schema.optionalKey(Schema.String),
  last: Schema.String,
  sessionId: Schema.optionalKey(Schema.String),
});
export type Item = typeof Item.Type;

export const StatsSample = Schema.Struct({
  atMs: Ms,
  local: Schema.optionalKey(Schema.String),
  remote: Schema.optionalKey(Schema.String),
  rttMs: Schema.optionalKey(Schema.Finite),
  receivedKbps: Schema.optionalKey(Schema.Finite),
  fps: Schema.optionalKey(Schema.Finite),
  jitterMs: Schema.optionalKey(Schema.Finite),
  lossRatio: Schema.optionalKey(Schema.Finite),
});
export type StatsSample = typeof StatsSample.Type;

/** A library span, with only its `reactor.*` attributes and `error.type`. */
export const Span = Schema.Struct({
  name: Schema.String,
  startMs: Ms,
  durationMs: Schema.optionalKey(Ms),
  status: Schema.Literals(["open", "ok", "error"]),
  attributes: Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]).pipe((value) =>
    Schema.Record(Schema.String, value),
  ),
});
export type Span = typeof Span.Type;

/** A session the run allocated, and how it ended. */
export const SessionRecord = Schema.Struct({
  id: Schema.String,
  allocatedMs: Ms,
  /** When the grant's cap ends it at the latest, ISO. */
  capEndsAt: Schema.String,
  close: Schema.optionalKey(
    Schema.Struct({
      requestedMs: Ms,
      reportedMs: Ms,
      confirmed: Schema.Boolean,
      /** The library's close report, or a termination through the owner record. */
      report: Schema.optionalKey(CloseReport),
      termination: Schema.optionalKey(Termination),
    }),
  ),
  /** The coordinator's answers after the close, each once, until terminal or gone. */
  trail: Schema.Array(Schema.Struct({ atMs: Ms, state: Schema.String })),
  terminalMs: Schema.optionalKey(Ms),
});
export type SessionRecord = typeof SessionRecord.Type;

const Nullable = Schema.NullOr(Schema.String);

/** One boundary of the `queue` check: the edit before it, the clips either side, and its seam. */
export const Boundary = Schema.Struct({
  edit: Schema.Literals(["none", "move", "pop"]),
  aimMs: Ms,
  endingClipId: Schema.String,
  editedClipId: Schema.optionalKey(Schema.String),
  expectedClipId: Schema.optionalKey(Schema.String),
  sentMs: Schema.optionalKey(Ms),
  refused: Schema.optionalKey(Schema.Boolean),
  finishedMs: Schema.optionalKey(Ms),
  nextClipId: Schema.optionalKey(Schema.String),
  nextStartedMs: Schema.optionalKey(Ms),
  pause: Schema.optionalKey(Pause),
});
export type Boundary = typeof Boundary.Type;

export const Evidence = Schema.Struct({
  format: Schema.Literal(format),
  runId: Schema.String,
  check: Check,
  mode: Schema.Literals(["paid", "rehearsal"]),
  startedAt: Schema.String,
  finishedAt: Schema.optionalKey(Schema.String),
  environment: Schema.Struct({
    runtime: Schema.String,
    os: Schema.String,
    commit: Schema.optionalKey(Schema.String),
    dirty: Schema.optionalKey(Schema.Boolean),
    packages: Schema.Record(Schema.String, Schema.String),
    /** The native addon's identity, from its platform package's native-identity.json. */
    native: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    network: Schema.String,
    apiOrigin: Schema.String,
  }),
  budget: Schema.Struct({
    checkUsd: Usd,
    totalUsd: Usd,
    /** What earlier paid runs in the ledger had reserved when this one was admitted. */
    reservedBeforeUsd: Usd,
    rate: Schema.optionalKey(
      Schema.Struct({ creditsPerSecond: Schema.Finite, creditsPerDollar: Schema.Finite }),
    ),
    /** Recorded before any token exists: what the ledger reserves for this run. */
    worstCaseUsd: Schema.optionalKey(Usd),
    /** Allocation to confirmed end, every started minute whole, at the published rate. */
    estimatedUsd: Schema.optionalKey(Usd),
  }),
  grants: Schema.Array(
    Schema.Struct({
      maxSessions: Schema.Int,
      maxSessionSeconds: Schema.Int,
      expiresAt: Schema.Finite,
    }),
  ),
  sessions: Schema.Array(SessionRecord),
  milestones: Schema.Array(
    Schema.Struct({ atMs: Ms, step: Schema.String, detail: Schema.optionalKey(Schema.String) }),
  ),
  outcomes: Schema.Array(Outcome),
  criteria: Schema.Array(Criterion),
  spans: Schema.Array(Span),
  /** H3's contract as the deployment and its messages showed it. */
  contract: Schema.optionalKey(
    Schema.Struct({
      deploymentTitle: Nullable,
      deploymentVersion: Nullable,
      documentedVersion: Schema.String,
      referenceAudio: Schema.Boolean,
      messages: Counts,
      unknown: Counts,
      diagnostics: Counts,
      duplicates: Schema.Int,
      stale: Schema.Int,
    }),
  ),
  server: Schema.optionalKey(
    Schema.Struct({
      cluster: Nullable,
      zone: Nullable,
      serverVersion: Nullable,
      transport: Nullable,
    }),
  ),
  /** The vertical's clip, from submission to its end. */
  clip: Schema.optionalKey(
    Schema.Struct({
      clipId: Schema.String,
      acceptance: Schema.Literals(["correlated", "metadata"]),
      submitMs: Ms,
      acceptedMs: Ms,
      generatedMs: Schema.optionalKey(Ms),
      startedMs: Schema.optionalKey(Ms),
      endedMs: Schema.optionalKey(Ms),
      /** Later messages that listed the clip with its metadata, by type. */
      echoes: Counts,
      references: Schema.optionalKey(
        Schema.Struct({
          images: Schema.Int,
          audio: Schema.Int,
          reportedAudio: Schema.NullOr(Schema.Int),
          hasReferenceAudio: Schema.NullOr(Schema.Boolean),
        }),
      ),
    }),
  ),
  media: Schema.optionalKey(
    Schema.Struct({
      audioOffered: Schema.Boolean,
      firstClipFrameMs: Schema.optionalKey(Ms),
      video: VideoSummary,
      audio: Schema.optionalKey(AudioSummary),
      pressure: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    }),
  ),
  network: Schema.optionalKey(
    Schema.Struct({
      samples: Schema.Array(StatsSample),
      /** The local candidate type of the pair that carried the media. */
      pair: Schema.optionalKey(Schema.String),
    }),
  ),
  takeover: Schema.optionalKey(
    Schema.Struct({
      ownerStreamingMs: Ms,
      killedMs: Ms,
      attachMs: Schema.optionalKey(Ms),
      playingClipId: Schema.optionalKey(Nullable),
      clipIdentified: Schema.optionalKey(Schema.Boolean),
      metadataPreserved: Schema.optionalKey(Schema.Boolean),
      /** Commands this process sent the model after the owner died, by name. */
      commands: Schema.optionalKey(Counts),
      firstFreshFrameMs: Schema.optionalKey(Ms),
      video: Schema.optionalKey(VideoSummary),
    }),
  ),
  /**
   * `queue`: H3's own queue, raw: queue reads right behind an enqueue, position
   * zero, a popped build, and a move or pop before each of five boundaries.
   */
  queue: Schema.optionalKey(
    Schema.Struct({
      positionZero: Schema.optionalKey(
        Schema.Struct({
          buildingClipId: Schema.String,
          requestedClipId: Schema.String,
          generationOrder: Schema.Array(Schema.String),
        }),
      ),
      poppedBuild: Schema.optionalKey(
        Schema.Struct({
          wasBuilding: Schema.Boolean,
          generatedAfterPop: Schema.Boolean,
          startedAfterPop: Schema.Boolean,
        }),
      ),
      boundaries: Schema.Array(Boundary),
      /** Queue reads sent right behind an enqueue: whether each listed the new clip. */
      ordering: Schema.Array(Schema.Boolean),
      /** Submission to `clip_generated` for each build, in milliseconds. */
      builds: Schema.Array(Ms),
      metadata: Schema.Struct({ observed: Counts, mismatched: Counts }),
    }),
  ),
  /** The playout checks: `renewal`, `edits` and `cut`. */
  playout: Schema.optionalKey(
    Schema.Struct({
      items: Schema.Array(Item),
      startOrder: Schema.Array(Schema.String),
      seams: Schema.Array(Seam),
      batch: Schema.optionalKey(
        Schema.Struct({
          submittedMs: Ms,
          committedMs: Schema.optionalKey(Ms),
          boundaryMs: Schema.optionalKey(Ms),
        }),
      ),
      /** Stops sent while the playout ran, from the library's spans. */
      stops: Schema.optionalKey(Schema.Int),
      switches: Schema.Struct({
        atMs: Ms,
        from: Schema.String,
        to: Schema.String,
        decision: Schema.String,
      }).pipe(Schema.Array, Schema.optionalKey),
      /** Frames the playout's picture carried while each item aired. */
      framesByItem: Schema.optionalKey(Counts),
      drainedMs: Schema.optionalKey(Ms),
      estimates: Schema.optionalKey(
        Schema.Struct({
          buildMedian: Schema.optionalKey(Schema.Finite),
          buildP95: Schema.optionalKey(Schema.Finite),
          length: Schema.Finite,
        }),
      ),
      video: Schema.optionalKey(VideoSummary),
      audio: Schema.optionalKey(AudioSummary),
    }),
  ),
  verdict: Schema.optionalKey(Schema.Literals(["pass", "fail"])),
  reasons: Schema.Array(Schema.String),
  missing: Schema.Array(Schema.String),
});
export type Evidence = typeof Evidence.Type;

type Section = keyof typeof Evidence.fields;

/** The sections each check must fill; a run without one fails as incomplete. */
const sections: Record<Check, ReadonlyArray<Section>> = {
  vertical: ["contract", "server", "clip", "media", "network"],
  turn: ["contract", "server", "clip", "media", "network"],
  audio: ["contract", "server", "clip", "media", "network"],
  takeover: ["takeover"],
  resume: ["takeover"],
  queue: ["queue"],
  renewal: ["playout"],
  edits: ["playout"],
  cut: ["playout"],
};

/** What the evidence lacks: a section its check needs, a session's close, or a paid run's reservation. */
export const missing = (evidence: Evidence): ReadonlyArray<string> => [
  ...(evidence.mode === "paid" && evidence.budget.worstCaseUsd === undefined
    ? ["budget.worstCaseUsd"]
    : []),
  ...(evidence.sessions.length === 0 ? ["sessions"] : []),
  ...evidence.sessions.flatMap((session) =>
    session.close === undefined ? [`sessions.${session.id}.close`] : [],
  ),
  ...sections[evidence.check].filter((section) => evidence[section] === undefined),
];

/**
 * The verdict the evidence supports: a pass needs every criterion passed,
 * nothing missing, no unknown outcome and every session's end confirmed.
 */
export const judged = (input: {
  readonly evidence: Evidence;
  readonly failure: string | undefined;
}): Evidence => {
  const { evidence, failure } = input;
  const absent = missing(evidence);
  const reasons = [
    ...(evidence.outcomes.includes("unknown")
      ? ["an outcome is unknown; the check stopped and is not repeated"]
      : []),
    ...(evidence.sessions.some((session) => session.close?.confirmed === false)
      ? ["remote termination is unconfirmed; a session may still be billing"]
      : []),
    ...(failure === undefined ? [] : [failure]),
    ...evidence.criteria.flatMap((criterion) =>
      criterion.passed
        ? []
        : [`${criterion.name}${criterion.detail === undefined ? "" : `: ${criterion.detail}`}`],
    ),
    ...(absent.length === 0 ? [] : [`incomplete evidence: ${absent.join(", ")}`]),
    ...(evidence.criteria.length === 0 ? ["no criterion was evaluated"] : []),
  ];
  return { ...evidence, missing: absent, reasons, verdict: reasons.length === 0 ? "pass" : "fail" };
};

/** What a person must confirm in the Reactor dashboard: sessions whose end the run could not confirm. */
export const cleanupInstructions = (evidence: Evidence): ReadonlyArray<string> =>
  evidence.sessions.flatMap((session) =>
    session.close?.confirmed === true
      ? []
      : [
          `Session ${session.id} was not confirmed ended; its cap ends it by ${session.capEndsAt}. Confirm in the Reactor dashboard that it ended, and what it cost.`,
        ],
  );

/** Evidence as a file holds it: indented, so a ledger reads well in review. */
export const EvidenceJson = Schema.fromJsonString(Evidence, { space: 2 });
