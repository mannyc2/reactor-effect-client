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
import { Refused, checks, maxCheckUsd, maxSchedulerUsd, stopFor } from "./gates.js";

export const format = "reactor-hosted-qualification/v1";

/** Milliseconds since the run started: every timing in the evidence is one. */
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

export const Evidence = Schema.Struct({
  format: Schema.Literal(format),
  runId: Schema.String,
  check: Schema.Literals(checks),
  mode: Schema.Literals(["paid", "rehearsal"]),
  /** Wall time of the run's start, which every `...Ms` field counts from. */
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
  /** Two capped sessions and observations relevant to scheduler policy. These
   * are protocol and decoded-media observations, not billing or output proof. */
  scheduler: Schema.optionalKey(
    Schema.Struct({
      replacement: Schema.Struct({
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
          }),
        ),
        termination: Schema.optionalKey(
          Schema.Struct({
            requestedMs: Ms,
            reportedMs: Ms,
            confirmed: Schema.Boolean,
          }),
        ),
        estimatedUsd: Schema.optionalKey(Usd),
      }),
      builds: Schema.Array(
        Schema.Struct({
          clipId: Schema.String,
          requestedSeconds: Schema.Finite,
          readySeconds: Schema.optionalKey(Schema.Finite),
          submittedMs: Ms,
          readyMs: Schema.optionalKey(Ms),
          /** Submission to Ready includes provider queue waiting. */
          submitToReadyMs: Schema.optionalKey(Ms),
        }),
      ),
      latencyByRequestedSeconds: Schema.Array(
        Schema.Struct({
          requestedSeconds: Schema.Finite,
          count: Schema.Natural,
          p50Ms: Ms,
          p95Ms: Ms,
        }),
      ),
      readyMove: Schema.optionalKey(
        Schema.Struct({
          clipId: Schema.String,
          replyMs: Ms,
          elapsedMs: Ms,
          queue: Schema.String,
          position: Schema.Natural,
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
      metadata: Schema.Struct({ observed: Counts, mismatched: Counts }),
      media: Schema.optionalKey(
        Schema.Struct({
          retiring: Schema.Struct({ video: VideoSummary, audio: AudioSummary }),
          replacement: Schema.Struct({ video: VideoSummary, audio: AudioSummary }),
        }),
      ),
      decodedHandoff: Schema.optionalKey(
        Schema.Struct({
          oldLastFrameMs: Ms,
          replacementFirstFrameMs: Ms,
          gapMs: Ms,
        }),
      ),
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
      "scheduler.replacement.grant",
      "scheduler.replacement.session",
      "scheduler.replacement.session.endedMs",
      "scheduler.replacement.termination",
      "scheduler.builds.0.readyMs",
      "scheduler.builds.0.readySeconds",
      "scheduler.builds.1.readyMs",
      "scheduler.builds.1.readySeconds",
      "scheduler.readyMove",
      "scheduler.positionZero",
      "scheduler.poppedBuild",
      "scheduler.decodedHandoff",
      "scheduler.media.retiring.video.arrivalsMs.0",
      "scheduler.media.replacement.video.arrivalsMs.0",
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

/**
 * The run's verdict: it passes only when every criterion it evaluated passed,
 * nothing required is missing, no stop rule fired and the check itself
 * completed. `failure` is why the check did not complete, if it did not.
 */
export const conclude = (evidence: Draft, failure: string | undefined): void => {
  const reasons: string[] = [];
  const stop = stopFor({
    outcomes: evidence.outcomes,
    terminationConfirmed:
      evidence.termination?.confirmed === false ||
      evidence.scheduler?.replacement.termination?.confirmed === false
        ? false
        : evidence.termination?.confirmed,
  });
  if (stop !== undefined) reasons.push(stop);
  if (failure !== undefined) reasons.push(failure);
  for (const criterion of evidence.criteria)
    if (!criterion.passed)
      reasons.push(
        `${criterion.name}${criterion.detail === undefined ? "" : `: ${criterion.detail}`}`,
      );
  evidence.missing = [...missing(evidence)];
  if (evidence.missing.length > 0)
    reasons.push(`incomplete evidence: ${evidence.missing.join(", ")}`);
  if (evidence.criteria.length === 0) reasons.push("no criterion was evaluated");
  evidence.reasons = reasons;
  evidence.verdict = reasons.length === 0 ? "pass" : "fail";
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
      (evidence.grant === undefined
        ? 0
        : evidence.check === "scheduler"
          ? maxSchedulerUsd
          : maxCheckUsd));

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
