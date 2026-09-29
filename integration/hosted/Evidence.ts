/**
 * The evidence one run leaves: one Schema for every check, what each check must
 * fill, and the verdict it supports. A paid session that comes back without its
 * data fails as incomplete, never passes with gaps. It never holds a
 * credential, an address, SDP, a frame, audio or provider text.
 */
import * as Schema from "effect/Schema";
import type { FailureReason } from "reactor-effect-client/Playout";
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
  /** Why it failed, in the library's words, and whether moderation or a lost session was why. */
  failed: Schema.optionalKey(
    Schema.Struct({ reason: Schema.String, moderated: Schema.Boolean, lost: Schema.Boolean }),
  ),
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

/** A failed item's record: why, in the library's words, never the provider's. */
export const failedOf = (reason: FailureReason): NonNullable<Item["failed"]> => {
  switch (reason._tag) {
    case "Clip":
      return { reason: reason.message, moderated: false, lost: false };
    case "Command":
      return { reason: reason.cause.message, moderated: false, lost: false };
    case "Lost":
      return { reason: "its session was lost", moderated: false, lost: true };
    case "Moderated":
      return { reason: "content moderation flagged it", moderated: true, lost: false };
    case "Closed":
      return { reason: "the playout closed", moderated: false, lost: false };
  }
};

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

/** One free question to the coordinator: its status, and only numbers, codes and key names. */
export const Probe = Schema.Struct({
  name: Schema.String,
  /** The HTTP status; 0 when no reply came. */
  status: Schema.Int,
  requestedSeconds: Schema.optionalKey(Schema.Int),
  /** `expires_at` less the time the request was sent. */
  lifetimeSeconds: Schema.optionalKey(Schema.Finite),
  code: Schema.optionalKey(Schema.String),
  /** What the reply echoed of the grant. */
  echo: Schema.optionalKey(
    Schema.Record(Schema.String, Schema.Union([Schema.Finite, Schema.Boolean, Schema.Null])),
  ),
  /** The reply's key paths and value types. */
  shape: Schema.String.pipe(Schema.Array, Schema.optionalKey),
});
export type Probe = typeof Probe.Type;

/**
 * `tokens`: a session outliving the token that created it. The owner creates
 * it on a short token and dies; once that token expired, this process adopts
 * the session with tokens bound to it, refreshes one, and ends the session
 * with the API key.
 */
export const TokensRecord = Schema.Struct({
  probes: Schema.Array(Probe),
  /** Every token the check minted for the session, in order. */
  mints: Schema.Array(
    Schema.Struct({
      atMs: Ms,
      kind: Schema.Literals(["create", "bind", "unbound"]),
      lifetimeSeconds: Schema.Finite,
      echoed: Schema.Boolean,
      bound: Schema.optionalKey(Schema.Int),
    }),
  ),
  /** When the creating token expired. */
  createExpiresMs: Schema.optionalKey(Ms),
  ownerKilledMs: Schema.optionalKey(Ms),
  resumeStartedMs: Schema.optionalKey(Ms),
  attachedMs: Schema.optionalKey(Ms),
  /**
   * The owner's clips: the one playing when it was killed, and the one queued
   * behind it. The adoption waits for the creating token to expire, so either
   * may be playing then (paid run tokens 7bc779d4 found the queued one).
   */
  ownerClipIds: Schema.optionalKey(
    Schema.Struct({ playing: Schema.String, queued: Schema.String }),
  ),
  playingClipId: Schema.optionalKey(Nullable),
  clipIdentified: Schema.optionalKey(Schema.Boolean),
  firstFreshFrameMs: Schema.optionalKey(Ms),
  video: Schema.optionalKey(VideoSummary),
  /** When the next bound token was minted for a call, the refresh. */
  refreshedMs: Schema.optionalKey(Ms),
  /** A clip enqueued on the refreshed token with a reference image and reference audio. */
  upload: Schema.optionalKey(
    Schema.Struct({
      startedMs: Ms,
      acceptedMs: Schema.optionalKey(Ms),
      images: Schema.Int,
      audio: Schema.Int,
      reportedAudio: Schema.NullOr(Schema.Int),
      hasReferenceAudio: Schema.NullOr(Schema.Boolean),
    }),
  ),
  /** Reading the session with the creating token after it expired: 401 is documented. */
  expiredTokenStatus: Schema.optionalKey(Schema.Int),
  /** Reading it with a fresh token not bound to it: 403 is documented. */
  unboundTokenStatus: Schema.optionalKey(Schema.Int),
  /** Ending it with the API key as the bearer. */
  apiKeyTermination: Schema.optionalKey(Termination),
  /** Commands the adopter sent the model, by name. */
  commands: Schema.optionalKey(Counts),
});
export type TokensRecord = typeof TokensRecord.Type;

/** A raw read of a session: its status, key names, state and identifier-like codes, never values. */
export const SessionRead = Schema.Struct({
  atMs: Ms,
  status: Schema.Int,
  keys: Schema.Array(Schema.String),
  state: Schema.optionalKey(Schema.String),
  codes: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});
export type SessionRead = typeof SessionRead.Type;

/**
 * `adoption`: one session taken over three ways in turn. An owner on the
 * isolated native host creates it on a short token and is killed; a raw
 * attach reads it and closes without ending it; nothing is connected until
 * that token has expired; then `H3Source.resume` adopts it with tokens bound
 * to it, refreshes one for a clip with references, and ends it by closing.
 */
export const AdoptionRecord = Schema.Struct({
  /** Every token the check minted for the session, in order. */
  mints: Schema.Array(
    Schema.Struct({
      atMs: Ms,
      kind: Schema.Literals(["create", "read", "attach", "resume", "unbound"]),
      lifetimeSeconds: Schema.Finite,
      echoed: Schema.Boolean,
      bound: Schema.optionalKey(Schema.Int),
    }),
  ),
  /** Where the owner ran, as it reported it: its runtime and native peer. */
  ownerHost: Schema.optionalKey(Schema.String),
  /** The owner's clips: the one it played, and the one queued behind it. */
  ownerClipIds: Schema.optionalKey(
    Schema.Struct({ playing: Schema.String, queued: Schema.String }),
  ),
  ownerStreamingMs: Schema.optionalKey(Ms),
  killedMs: Schema.optionalKey(Ms),
  /** When the creating token expired. */
  createExpiresMs: Schema.optionalKey(Ms),
  /** The raw attach, which does not adopt: what it read, and its close. */
  attach: Schema.optionalKey(
    Schema.Struct({
      startedMs: Ms,
      attachedMs: Ms,
      playingClipId: Schema.optionalKey(Nullable),
      /** The owner's queued clip was listed with its metadata. */
      queuedMetadata: Schema.optionalKey(Schema.Boolean),
      firstFreshFrameMs: Schema.optionalKey(Ms),
      video: Schema.optionalKey(VideoSummary),
      /** Commands it sent the model, by name. */
      commands: Schema.optionalKey(Counts),
      close: Schema.optionalKey(
        Schema.Struct({ requestedMs: Ms, reportedMs: Ms, report: CloseReport }),
      ),
    }),
  ),
  /**
   * Reads of the session while nothing was connected, each with the time since
   * the last connection closed.
   */
  gap: Schema.Array(Schema.Struct({ ...SessionRead.fields, sinceConnectionMs: Ms })),
  /** `H3Source.resume`: its adoption, what it read, and its clip on a refreshed token. */
  resume: Schema.optionalKey(
    Schema.Struct({
      startedMs: Ms,
      attachedMs: Ms,
      ownership: Schema.Literals(["owned", "attached"]),
      playingClipId: Schema.optionalKey(Nullable),
      firstFreshFrameMs: Schema.optionalKey(Ms),
      video: Schema.optionalKey(VideoSummary),
      /** When the next bound token was minted for a call, the refresh. */
      refreshedMs: Schema.optionalKey(Ms),
      /** A clip enqueued on the refreshed token with a reference image and reference audio. */
      upload: Schema.optionalKey(
        Schema.Struct({
          startedMs: Ms,
          acceptedMs: Ms,
          images: Schema.Int,
          audio: Schema.Int,
          reportedAudio: Schema.NullOr(Schema.Int),
          hasReferenceAudio: Schema.NullOr(Schema.Boolean),
        }),
      ),
      /** Commands it sent the model, by name. */
      commands: Schema.optionalKey(Counts),
    }),
  ),
  /** Reading the session with the creating token after it expired: 401 is documented. */
  expiredTokenStatus: Schema.optionalKey(Schema.Int),
  /** Reading it with a fresh token not bound to it: 403 is documented. */
  unboundTokenStatus: Schema.optionalKey(Schema.Int),
});
export type AdoptionRecord = typeof AdoptionRecord.Type;

/**
 * The moderation tail of `cut`: a held item whose prompt is meant to be
 * flagged, and what hosted Reactor, the session and the playout did.
 * Observations, not criteria; the prompt itself is never kept.
 */
export const ModerationRecord = Schema.Struct({
  promptLength: Schema.Int,
  submittedMs: Ms,
  /** The flagged item's as-run, in order. */
  statuses: Schema.Array(
    Schema.Struct({ atMs: Ms, status: Schema.String, detail: Schema.optionalKey(Schema.String) }),
  ),
  /** The item's enqueue command, from the library's span. */
  enqueue: Schema.optionalKey(
    Schema.Struct({
      startMs: Ms,
      durationMs: Schema.optionalKey(Ms),
      status: Schema.Literals(["open", "ok", "error"]),
      requestId: Schema.optionalKey(Schema.String),
    }),
  ),
  verdict: Schema.optionalKey(
    Schema.Struct({
      atMs: Ms,
      action: Schema.String,
      categories: Schema.Array(Schema.String),
      inputKind: Schema.optionalKey(Schema.String),
      command: Schema.optionalKey(Schema.String),
      requestId: Schema.optionalKey(Schema.String),
      /** Its request id is the enqueue's. */
      namesEnqueue: Schema.Boolean,
    }),
  ),
  /** The session's statuses, control messages and diagnostics from the submission on. */
  session: Schema.Array(Schema.Struct({ atMs: Ms, event: Schema.String })),
  /** The playout's session events and failure from the submission on. */
  playout: Schema.Array(Schema.Struct({ atMs: Ms, event: Schema.String })),
  /** The coordinator's read of the session afterwards: status, key names, state and codes. */
  read: Schema.optionalKey(SessionRead),
  /** A verdict arrived or the session ended while the item waited. */
  flagged: Schema.Boolean,
  /** The item started: moderation let it through. */
  aired: Schema.Boolean,
});
export type ModerationRecord = typeof ModerationRecord.Type;

/** A token the tour's session minted for itself. */
const TourMint = Schema.Struct({
  sentMs: Ms,
  atMs: Ms,
  kind: Schema.Literals(["create", "bind"]),
  lifetimeSeconds: Schema.Finite,
  echoed: Schema.Boolean,
  /** How many sessions it names, and whether it names exactly the session's own id. */
  bound: Schema.Int,
  ownSession: Schema.Boolean,
  /** Why proving or accepting it failed: a grant refused is never used. */
  refused: Schema.optionalKey(Schema.String),
});

/** A session call the tour made either side of the creating token's expiry. */
const TourCall = Schema.Struct({
  what: Schema.String,
  startedMs: Ms,
  endedMs: Schema.optionalKey(Ms),
  ok: Schema.Boolean,
  /** The reason it failed with. */
  failure: Schema.optionalKey(Schema.String),
});

const Canvas = Schema.Struct({ aspect: Schema.String, width: Schema.Int, height: Schema.Int });

/** A clip the tour enqueued, from its submission to what became of it. */
const TourClip = Schema.Struct({
  clipId: Schema.String,
  acceptance: Schema.Literals(["correlated", "metadata"]),
  submitMs: Ms,
  acceptedMs: Ms,
  generatedMs: Schema.optionalKey(Ms),
  startedMs: Schema.optionalKey(Ms),
  endedMs: Schema.optionalKey(Ms),
  /** The lifecycle message that ended it. */
  ended: Schema.optionalKey(Schema.String),
  /** The references it was sent, the uploads its preparation made, and what the clip reports. */
  references: Schema.Struct({
    images: Schema.Int,
    audio: Schema.Int,
    uploads: Schema.Int,
    reportedImages: Schema.NullOr(Schema.Int),
    reportedAudio: Schema.NullOr(Schema.Int),
    hasReferenceAudio: Schema.NullOr(Schema.Boolean),
  }),
});

/** What H3's state and queue said at one moment; clips go by the tour's names for them. */
const TourSettings = Schema.Struct({
  ...Canvas.fields,
  autoplay: Schema.Boolean,
  flushOnClipEnd: Schema.Boolean,
  seed: Schema.Int,
  clipSeconds: Schema.Finite,
  playing: Schema.Boolean,
  playingClip: Nullable,
  /** Clips in both queues. */
  queued: Schema.Int,
});

/** A recording request and, when a clip came back, its download: sizes and counts, never content. */
const TourRecording = Schema.Struct({
  request: Schema.Literals(["clip", "recording"]),
  /** `ClipReady`, or the reason the request failed with. */
  outcome: Schema.String,
  kind: Schema.optionalKey(Schema.String),
  /** Its end marker less its start marker, in the recorder's own unit. */
  markers: Schema.optionalKey(Schema.Finite),
  /** How long after the reply it was predicted to be ready. */
  readyInMs: Schema.optionalKey(Schema.Finite),
  download: Schema.optionalKey(
    Schema.Struct({
      /** `downloaded`, or the reason the download failed with. */
      outcome: Schema.String,
      ms: Ms,
      bytes: Schema.optionalKey(Schema.Int),
      segments: Schema.optionalKey(Schema.Int),
      init: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});

/** A token minted only to see what Reactor echoes of it and how the SDK reads that. */
const TourFreeMint = Schema.Struct({
  name: Schema.String,
  /** `granted`, or the reason the mint failed with. */
  outcome: Schema.String,
  lifetimeSeconds: Schema.optionalKey(Schema.Finite),
  echoed: Schema.optionalKey(Schema.Boolean),
  maxSessions: Schema.Int.pipe(Schema.NullOr, Schema.optionalKey),
  /** The cap the SDK read from the echo: seconds, `unlimited` for a null cap, `absent` for none. */
  maxSessionSeconds: Schema.optionalKey(
    Schema.Union([Schema.Int, Schema.Literals(["unlimited", "absent"])]),
  ),
  bound: Schema.optionalKey(Schema.Int),
});

/**
 * `tour`: one session through the raw API, a phase at a time. Clips in queue
 * reads go by the tour's names for them; provider text is never kept.
 */
export const TourRecord = Schema.Struct({
  /** Every token the session's `Tokens` minted, in order. */
  mints: Schema.Array(TourMint),
  /** When the creating token expired, and from when the session refreshes it. */
  createExpiresMs: Schema.optionalKey(Ms),
  refreshDueMs: Schema.optionalKey(Ms),
  /** The upload at the refresh point on the session's token, and the reconnect after its expiry. */
  refreshCall: Schema.optionalKey(TourCall),
  afterExpiryCall: Schema.optionalKey(TourCall),
  /** The commands the tour sends that the deployment does not offer. */
  missingCommands: Schema.String.pipe(Schema.Array, Schema.optionalKey),
  canvas: Schema.optionalKey(
    Schema.Struct({
      requested: Schema.String,
      /** `valid_commands` listed `set_canvas` while the session was empty. */
      listed: Schema.Boolean,
      reply: Schema.optionalKey(Canvas),
      state: Schema.optionalKey(Canvas),
    }),
  ),
  clip1: Schema.optionalKey(
    Schema.Struct({
      ...TourClip.fields,
      /** The seed sent, the one the clip reports, and the session's default before and after. */
      seed: Schema.Struct({
        sent: Schema.Int,
        echoed: Schema.NullOr(Schema.Int),
        defaultBefore: Schema.NullOr(Schema.Int),
        defaultAfter: Schema.NullOr(Schema.Int),
      }),
      /** Later messages that listed the clip with its metadata, by type. */
      echoes: Counts,
      /** What arrived while it played, until it ended or the window closed. */
      video: Schema.optionalKey(VideoSummary),
      audio: Schema.optionalKey(AudioSummary),
    }),
  ),
  clip2: Schema.optionalKey(TourClip),
  queue: Schema.optionalKey(
    Schema.Struct({
      /** The generation queue once a clip at position zero, then one more, were enqueued. */
      enqueued: Schema.Array(Schema.String),
      /** The move's reply, and the generation queue read after it. */
      move: Schema.optionalKey(
        Schema.Struct({
          queue: Schema.String,
          position: Schema.Int,
          generation: Schema.Array(Schema.String),
        }),
      ),
      pops: Schema.Array(
        Schema.Struct({
          name: Schema.String,
          /**
           * It headed the generation queue when popped. H3 lists the clip it is building
           * first, and a clip waiting to be built first too, so this is the build in flight
           * only by inference.
           */
          headOfGeneration: Schema.Boolean,
          sentMs: Ms,
          repliedMs: Ms,
          /** Whether it was built, or started, after the pop's reply; read before the end. */
          generatedAfter: Schema.optionalKey(Schema.Boolean),
          startedAfter: Schema.optionalKey(Schema.Boolean),
        }),
      ),
      /** When clip 2, built after the moved clip, was generated, from the moved clip's pop. */
      clip2GeneratedAfterPopMs: Schema.optionalKey(Ms),
      /** The reads after the edits, and `refreshed` or the reason H3's refresh failed with. */
      after: Schema.optionalKey(
        Schema.Struct({
          generation: Schema.Array(Schema.String),
          playout: Schema.Array(Schema.String),
          generationQueued: Schema.Int,
          playoutQueued: Schema.Int,
          refresh: Schema.String,
        }),
      ),
      /** Each queue command's round trip, in milliseconds. */
      replies: Schema.Record(Schema.String, Ms),
    }),
  ),
  stopPlay: Schema.optionalKey(
    Schema.Struct({
      stopped: Schema.String,
      stopSentMs: Ms,
      stop: Schema.Literals(["Acknowledged", "Reply"]),
      /** When the stopped clip's `clip_stopped` arrived. */
      stoppedMs: Schema.optionalKey(Ms),
      /** Clips that started between the stop and the play. */
      startedBetween: Schema.Array(Schema.String),
      played: Schema.optionalKey(Schema.String),
      playSentMs: Schema.optionalKey(Ms),
      play: Schema.optionalKey(Schema.Literals(["Acknowledged", "Reply"])),
      playStartedMs: Schema.optionalKey(Ms),
    }),
  ),
  failedBuild: Schema.optionalKey(
    Schema.Struct({
      promptChars: Schema.Int,
      submitMs: Ms,
      acceptedMs: Schema.optionalKey(Ms),
      acceptance: Schema.optionalKey(Schema.Literals(["correlated", "metadata"])),
      endedMs: Schema.optionalKey(Ms),
      /** The lifecycle message that ended it. */
      ended: Schema.optionalKey(Schema.String),
      /** How the operation's wait for `generated` failed: its reason, and `ClipEnded`'s fields. */
      generatedFailure: Schema.optionalKey(Schema.String),
      clipEnded: Schema.optionalKey(
        Schema.Struct({
          lifecycle: Schema.String,
          sameClip: Schema.Boolean,
          transportGeneration: Schema.String,
        }),
      ),
      /** How long `clip_failed`'s reason was; its text is never kept. */
      reasonChars: Schema.optionalKey(Schema.Int),
      started: Schema.Boolean,
    }),
  ),
  recordings: TourRecording.pipe(Schema.Array, Schema.optionalKey),
  reconnect: Schema.optionalKey(
    Schema.Struct({
      startedMs: Ms,
      readyMs: Schema.optionalKey(Ms),
      generationBefore: Schema.String,
      generationAfter: Schema.optionalKey(Schema.String),
      /** The long clip was still ready to play after the reconnect. */
      keptClip: Schema.optionalKey(Schema.Boolean),
      /** The clip played on the new generation, and the frames that came of it. */
      playSentMs: Schema.optionalKey(Ms),
      playStartedMs: Schema.optionalKey(Ms),
      firstFreshFrameMs: Schema.optionalKey(Ms),
      video: Schema.optionalKey(VideoSummary),
      stateRead: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  reset: Schema.optionalKey(
    Schema.Struct({
      before: TourSettings,
      sentMs: Ms,
      clearedClips: Schema.optionalKey(Schema.Int),
      wasPlaying: Schema.optionalKey(Schema.Boolean),
      /** When the playing clip's `clip_stopped` arrived. */
      stoppedMs: Schema.optionalKey(Ms),
      after: Schema.optionalKey(TourSettings),
    }),
  ),
  /** Ending the session with the API key as the bearer, then the owned session's own close. */
  apiKeyTermination: Schema.optionalKey(Termination),
  ownedClose: Schema.optionalKey(CloseReport),
  afterEnd: Schema.optionalKey(
    Schema.Struct({
      /** How attaching to the ended session failed, and the HTTP status if one came. */
      attach: Schema.String,
      attachStatus: Schema.optionalKey(Schema.Int),
      /** The API key reading an unknown session: `found`, or the reason it failed with. */
      inspectUnknown: Schema.String,
      inspectStatus: Schema.optionalKey(Schema.Int),
      terminateUnknown: Schema.optionalKey(Termination),
    }),
  ),
  freeMints: Schema.Array(TourFreeMint),
  /** Commands the tour sent the model, by name. */
  commands: Schema.optionalKey(Counts),
});
export type TourRecord = typeof TourRecord.Type;
/**
 * `show`: one playout across three sessions. What its filler asked for and
 * when the air starved, the playout's session events, the cues, the first
 * session's dropped connection, the second session ended mid-clip and the
 * item due at a wall-clock instant. The items themselves are in `playout`.
 */
export const ShowRecord = Schema.Struct({
  /** Each filler clip the playout asked for: its index, the runway then and the length it asked. */
  fills: Schema.Array(
    Schema.Struct({ index: Schema.Int, runwaySeconds: Schema.Finite, seconds: Schema.Finite }),
  ),
  /** When nothing was left to play while the playout still wanted air. */
  starved: Schema.Array(Ms),
  /** The playout's session events, in order. */
  sessions: Schema.Array(Schema.Struct({ atMs: Ms, event: Schema.String })),
  /** Each cue as it fired, and how long after it was due from its clip's observed start. */
  cues: Schema.Array(
    Schema.Struct({
      key: Schema.String,
      name: Schema.String,
      atMs: Ms,
      lateByMs: Schema.optionalKey(Schema.Finite),
    }),
  ),
  /** The first session's connection, dropped while filler held the air, and its return. */
  recovery: Schema.optionalKey(
    Schema.Struct({
      sessionId: Schema.String,
      droppedMs: Ms,
      /** Connections the drop cut. */
      dropped: Schema.Int,
      runwaySeconds: Schema.Finite,
      /** The session's statuses from the drop on. */
      statuses: Schema.Array(Schema.Struct({ atMs: Ms, status: Schema.String })),
      /** When it read ready again, and the first frame after that. */
      readyMs: Schema.optionalKey(Ms),
      firstFrameMs: Schema.optionalKey(Ms),
    }),
  ),
  /** The session ended mid-clip, what ended it, and the session that took over. */
  loss: Schema.optionalKey(
    Schema.Struct({
      sessionId: Schema.String,
      by: Schema.Literals(["moderation", "key"]),
      requestedMs: Ms,
      /** Ending it with the API key as the bearer. */
      termination: Schema.optionalKey(Termination),
      replacedMs: Schema.optionalKey(Ms),
      nextSessionId: Schema.optionalKey(Schema.String),
      nextOpenedMs: Schema.optionalKey(Ms),
    }),
  ),
  /** The item due `At` a wall-clock instant: when, and how late its boundary came. */
  at: Schema.optionalKey(Schema.Struct({ dueMs: Ms, lateByMs: Schema.optionalKey(Schema.Finite) })),
  /** Each filler clip's start and end on air, and its length, as the playout reported them. */
  filler: Schema.Array(
    Schema.Struct({
      index: Schema.Int,
      phase: Schema.Literals(["Started", "Ended"]),
      atMs: Ms,
      seconds: Schema.optionalKey(Schema.Finite),
    }),
  ),
  /**
   * Every gap on air between one clip's end and the next one's start, items
   * and filler alike, from their reported starts and ends.
   */
  gaps: Schema.Array(
    Schema.Struct({ fromMs: Ms, toMs: Ms, ending: Schema.String, next: Schema.String }),
  ),
  /**
   * Each dropped connection the playout reported: its session, when the
   * playout said it was reconnecting, when it said it was back, and how long
   * the source measured the reconnect.
   */
  reconnects: Schema.Array(
    Schema.Struct({
      sessionId: Schema.String,
      reconnectingMs: Ms,
      reconnectedMs: Schema.optionalKey(Ms),
      afterMillis: Schema.optionalKey(Schema.Finite),
    }),
  ),
  /** Each reader of the on-air picture or sound that fell behind, and its session's count then. */
  readerOverflows: Schema.Array(
    Schema.Struct({
      sessionId: Schema.String,
      track: Schema.Literals(["video", "audio"]),
      atMs: Ms,
      readerOverflows: Schema.Int,
    }),
  ),
  /** Each item that failed for good, the tag of its failure's reason, and the session it lost. */
  failures: Schema.Array(
    Schema.Struct({
      key: Schema.String,
      atMs: Ms,
      reason: Schema.Literals(["Clip", "Command", "Lost", "Moderated", "Closed"]),
      sessionId: Schema.optionalKey(Schema.String),
    }),
  ),
  /**
   * The playout's state read as each clip's start was reported, items and
   * filler alike: how often it named that clip with that start, how often a
   * later clip had started by then, and every other reading.
   */
  playing: Schema.Struct({
    named: Schema.Int,
    later: Schema.Int,
    mismatched: Schema.Array(
      Schema.Struct({
        key: Schema.String,
        startedMs: Ms,
        seconds: Schema.optionalKey(Schema.Finite),
        /** What the state named instead, and its start and length; nothing when it named none. */
        stateKey: Schema.optionalKey(Schema.String),
        stateStartedMs: Schema.optionalKey(Ms),
        stateSeconds: Schema.optionalKey(Schema.Finite),
      }),
    ),
  }),
});
export type ShowRecord = typeof ShowRecord.Type;

/**
 * A create's answer: `allocated`; `the same session` when the reply named the
 * session its token had made; or the reason the create failed with, the SDK's
 * outcome and the HTTP status. Of the reply's body, its key names and codes:
 * free text only by its length. A reply that named no session is kept so too.
 */
const Answer = {
  answer: Schema.String,
  /** Whether a failed create may have allocated, as the SDK classes its failure. */
  outcome: Schema.optionalKey(Outcome),
  status: Schema.optionalKey(Schema.Int),
  keys: Schema.Array(Schema.String),
  codes: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
};

/**
 * `unconnected`: a session allocated and never connected, read with the API
 * key until it ended or its window closed, and what a second create on its
 * spent single-session token answered. The instants let a person set the
 * dashboard's charge beside the window.
 */
export const UnconnectedRecord = Schema.Struct({
  /** The session the create allocated; absent when the create failed. */
  sessionId: Schema.optionalKey(Schema.String),
  /** When the create was sent; the session's `allocatedMs` is when its reply named it. */
  requestedMs: Ms,
  requestedAt: Schema.String,
  /** The create's failure, when it failed. */
  create: Schema.optionalKey(Schema.Struct(Answer)),
  /** A second create on the spent token, sent as soon as the first was answered. */
  spentToken: Schema.optionalKey(
    Schema.Struct({
      sentMs: Ms,
      answeredMs: Ms,
      ...Answer,
      /** The session its reply named: one it allocated, which the key then ended, or the first. */
      sessionId: Schema.optionalKey(Schema.String),
    }),
  ),
  /**
   * When the reads were to stop at the latest: 15 s after the request, for
   * allocation, `ACTIVE` and ready to come in, then the cap, the 30 s Reactor
   * gives a session after its last connection drops, and 15 s to spare. So it
   * ends past the cap and the 30 s after it, counted from each of the three.
   */
  windowEndsMs: Schema.optionalKey(Ms),
  /**
   * Each state the reads found, in order, with the first and last read that
   * found it. A read answered 404 is `gone`; one that failed otherwise is its
   * HTTP status or its reason's tag.
   */
  states: Schema.Array(
    Schema.Struct({ state: Schema.String, firstMs: Ms, lastMs: Ms, reads: Schema.Int }),
  ),
  /** The first read that found its capabilities and a transport, which the SDK connects on. */
  connectableMs: Schema.optionalKey(Ms),
  /** The coordinator's read once the reads stopped: status, key names, state and codes. */
  read: Schema.optionalKey(SessionRead),
  /**
   * Its end, once confirmed: by Reactor at the first read that found it `CLOSED`
   * or gone, the read at the end included; or by the key, when a termination
   * was confirmed, after any unconfirmed tries the timeline shows. The
   * session's `close.requestedMs` is the key's first DELETE.
   */
  ended: Schema.optionalKey(
    Schema.Struct({ by: Schema.Literals(["reactor", "key"]), atMs: Ms, at: Schema.String }),
  ),
  /**
   * Why this run cannot say whether Reactor ends a session nothing connects
   * to, when it cannot. The spent token's create may have ended it: Reactor
   * ended it before any read found it still running 10 s after that create
   * was answered. Or the reads contradict each other: the watch found it
   * ended and the read at the end found it running.
   */
  unanswered: Schema.optionalKey(Schema.String),
});
export type UnconnectedRecord = typeof UnconnectedRecord.Type;

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
      Schema.Struct({
        creditsPerSecond: Schema.Finite,
        creditsPerDollar: Schema.Finite,
        per: Schema.Literals(["second", "minute"]),
      }),
    ),
    /** Recorded before any token exists: what the ledger reserves for this run. */
    worstCaseUsd: Schema.optionalKey(Usd),
    /** Allocation to confirmed end, every started unit of the published rate whole. */
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
  /**
   * Every read the adopting process made of the session it took over (takeover,
   * resume, tokens), with the time since the owner was killed. Paid run tokens
   * 83d17eb7 read INACTIVE there.
   */
  adopterReads: Schema.Struct({ ...SessionRead.fields, sinceKillMs: Ms }).pipe(
    Schema.Array,
    Schema.optionalKey,
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
  tokens: Schema.optionalKey(TokensRecord),
  moderation: Schema.optionalKey(ModerationRecord),
  tour: Schema.optionalKey(TourRecord),
  adoption: Schema.optionalKey(AdoptionRecord),
  show: Schema.optionalKey(ShowRecord),
  unconnected: Schema.optionalKey(UnconnectedRecord),
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
  tokens: ["tokens"],
  tour: ["contract", "server", "media", "network", "tour"],
  adoption: ["adoption"],
  show: ["playout", "show"],
  unconnected: ["unconnected"],
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
      ? ["an outcome is unknown, so the run fails and is not repeated"]
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

/**
 * `unconnected`'s create whose outcome is unknown, which may have allocated a
 * session the run never learned of, with any codes its reply held. Its only
 * commands whose outcome is recorded are its two creates, and once the
 * check's record names a session the first is known to have allocated: the
 * unknown one is then the second, whether or not an interrupt kept its answer
 * from being recorded.
 */
const unknownCreates = (evidence: Evidence): ReadonlyArray<string> => {
  if (evidence.check !== "unconnected" || !evidence.outcomes.includes("unknown")) return [];
  const probe = evidence.unconnected;
  const [create, answer] =
    probe?.sessionId === undefined
      ? ["The create", probe?.create]
      : ["The spent token's second create", probe.spentToken];
  const codes = Object.entries(answer?.codes ?? {}).map(([key, code]) => `${key} ${code}`);
  return [
    `${create} has an unknown outcome, so it may have allocated a session the run never learned of, shortly after ${probe?.requestedAt ?? evidence.startedAt}. Look for one in the Reactor dashboard, end it, and note what it cost.${codes.length === 0 ? "" : ` Its reply's codes: ${codes.join(", ")}.`}`,
  ];
};

/**
 * What a person must confirm in the Reactor dashboard: sessions whose end the
 * run could not confirm. Nothing connected to `unconnected`'s sessions, and
 * whether a cap ends such a session is what that check asks, so its
 * instructions promise no end, and name any create that may have allocated
 * one unseen.
 */
export const cleanupInstructions = (evidence: Evidence): ReadonlyArray<string> => [
  ...evidence.sessions.flatMap((session) =>
    session.close?.confirmed === true
      ? []
      : [
          evidence.check === "unconnected"
            ? `Session ${session.id} was not confirmed ended, and nothing connected to it, so its cap at ${session.capEndsAt} may not end it. End it in the Reactor dashboard, and note what it cost.`
            : `Session ${session.id} was not confirmed ended; its cap ends it by ${session.capEndsAt}. Confirm in the Reactor dashboard that it ended, and what it cost.`,
        ],
  ),
  ...unknownCreates(evidence),
];

/** Evidence as a file holds it: indented, so a ledger reads well in review. */
export const EvidenceJson = Schema.fromJsonString(Evidence, { space: 2 });
