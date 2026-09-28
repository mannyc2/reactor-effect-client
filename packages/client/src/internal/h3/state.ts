/**
 * The provider's facts as a pure reducer over session events: which state and
 * queue H3 last reported, whether the two agree, and each clip's latest
 * lifecycle. Acceptance evidence is decided here too, from a clip's exact
 * captured prompt and namespaced metadata.
 */
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ReactorError } from "../../ReactorError.js";
import type { CommandReply, SessionEvent } from "../../Session.js";
import type { Clip, DecodedMessage, MessageType, Queue, State } from "./messages.js";
import { metadataMaxChars } from "./profile.js";

export interface ClipObservation {
  readonly clip: Clip;
  /** The last explicit lifecycle message; null means only a queue snapshot listed it. */
  readonly lifecycle: Exclude<MessageType, "queue_update" | "state_update"> | null;
  readonly source: CommandReply;
}

export interface Facts {
  readonly state: State;
  readonly queue: Queue;
}

interface SnapshotBase {
  readonly sessionId: string;
  readonly transportGeneration: bigint;
  readonly revision: bigint;
  readonly clips: ReadonlyArray<ClipObservation>;
}

export type ProviderSnapshot = SnapshotBase &
  (
    | { readonly _tag: "Synchronizing"; readonly lastFacts: Facts | null }
    | { readonly _tag: "Ready"; readonly state: State; readonly queue: Queue }
    | {
        readonly _tag: "Unavailable";
        readonly cause: ReactorError;
        readonly lastFacts: Facts | null;
      }
  );

export interface Model {
  readonly sessionId: string;
  readonly generation: bigint;
  readonly revision: bigint;
  readonly maxClips: number;
  readonly state: State | undefined;
  readonly queue: Queue | undefined;
  /** A message implied a change the last full state or queue does not show. */
  readonly stateDirty: boolean;
  readonly queueDirty: boolean;
  readonly lastFacts: Facts | null;
  readonly cause: ReactorError | undefined;
  readonly clips: ReadonlyMap<string, ClipObservation>;
  /** Request ids whose message body this generation applied, to drop a repeated body. */
  readonly bodies: ReadonlyMap<string, bigint>;
}

type Disposition = "applied" | "duplicate" | "stale";

export const initial = ({
  sessionId,
  generation,
  revision,
  maxClips,
}: {
  readonly sessionId: string;
  readonly generation: bigint;
  readonly revision: bigint;
  readonly maxClips: number;
}): Model => ({
  sessionId,
  generation,
  revision,
  maxClips,
  state: undefined,
  queue: undefined,
  stateDirty: true,
  queueDirty: true,
  lastFacts: null,
  cause: undefined,
  clips: new Map(),
  bodies: new Map(),
});

const rank = (type: MessageType | null): number => {
  switch (type) {
    case "clip_generated":
      return 1;
    case "clip_started":
      return 2;
    case "clip_finished":
    case "clip_stopped":
    case "clip_failed":
    case "clip_popped":
      return 3;
    default:
      return 0;
  }
};

/**
 * A state and queue that were each read in full, and that agree with each
 * other. H3 reports a clip armed to start after its seam as playing; whether
 * it has left the playout queue by then is undocumented, so a playing clip at
 * the front of that queue is taken as armed rather than as a disagreement.
 */
const coherent = (model: Model): model is Model & Facts =>
  model.state !== undefined &&
  model.queue !== undefined &&
  !model.stateDirty &&
  !model.queueDirty &&
  model.state.generation_queued === model.queue.generation.length &&
  model.state.playout_queued === model.queue.playout.length &&
  ![...model.queue.generation, ...model.queue.playout.slice(1)].some(
    (clip) => clip.clip_id === model.state?.playing_clip_id,
  );

export const availability = (model: Model): ProviderSnapshot["_tag"] =>
  model.cause !== undefined ? "Unavailable" : coherent(model) ? "Ready" : "Synchronizing";

export const snapshot = (model: Model): ProviderSnapshot => {
  const base = {
    sessionId: model.sessionId,
    transportGeneration: model.generation,
    revision: model.revision,
    clips: [...model.clips.values()],
  };
  if (model.cause !== undefined)
    return { ...base, _tag: "Unavailable", cause: model.cause, lastFacts: model.lastFacts };
  if (coherent(model)) return { ...base, _tag: "Ready", state: model.state, queue: model.queue };
  return { ...base, _tag: "Synchronizing", lastFacts: model.lastFacts };
};

const remember = (model: Model): Model =>
  coherent(model) &&
  (model.lastFacts?.state !== model.state || model.lastFacts.queue !== model.queue)
    ? { ...model, lastFacts: { state: model.state, queue: model.queue } }
    : model;

export const unavailable = ({
  model,
  cause,
}: {
  readonly model: Model;
  readonly cause: ReactorError;
}): Model => ({
  ...remember(model),
  cause,
});

const reread = (model: Model): Model => ({
  ...model,
  state: undefined,
  queue: undefined,
  stateDirty: true,
  queueDirty: true,
});

/** Orders an event against the facts: a later generation starts over from fresh reads. */
export const admit = ({
  model,
  source,
}: {
  readonly model: Model;
  readonly source: SessionEvent;
}): readonly [Disposition, Model] => {
  if (source.generation < model.generation) return ["stale", model];
  if (source.sequence <= model.revision) return ["duplicate", model];
  let next: Model = { ...model, revision: source.sequence };
  if (source.generation > model.generation)
    next = {
      ...reread(remember(next)),
      generation: source.generation,
      bodies: new Map(),
      cause: undefined,
    };
  if (source._tag === "Model" && source.correlation === "stale-generation") return ["stale", next];
  // The correlator labels a body that follows an ACK duplicate; an ACK is never a payload.
  if (source._tag === "Model" && source.kind === "message" && source.requestId !== "") {
    if (
      source.correlation === "duplicate" &&
      next.bodies.get(source.requestId) === source.generation
    )
      return ["duplicate", next];
    const bodies = new Map(next.bodies).set(source.requestId, source.generation);
    if (bodies.size > 256) bodies.delete(bodies.keys().next().value ?? "");
    next = { ...next, bodies };
  }
  if (source._tag === "Status") {
    if (
      source.status === "disconnected" ||
      source.status === "closing" ||
      source.status === "closed"
    )
      next = unavailable({
        model: next,
        cause: ReactorError.fromCode(
          source.status === "disconnected" ? "Disconnected" : "Closed",
          `H3 session is ${source.status}`,
          { operation: "H3 observation" },
        ),
      });
    else if (source.status === "ready" && next.cause !== undefined)
      next = { ...reread(next), cause: undefined };
  }
  return ["applied", next];
};

const overflow = (): ReactorError =>
  ReactorError.fromCode("Overflow", "H3 clip observation bound exceeded", {
    operation: "H3 observation",
  });

const invalidate = (model: Model, state: boolean, queue: boolean): Model => ({
  ...remember(model),
  stateDirty: model.stateDirty || state,
  queueDirty: model.queueDirty || queue,
});

/** Applies a decoded message; only full state and queue reads make the facts current. */
export const apply = ({
  model,
  message,
  source,
}: {
  readonly model: Model;
  readonly message: DecodedMessage;
  readonly source: CommandReply;
}): Result.Result<readonly [Disposition, Model], ReactorError> => {
  const done = (next: Model): Result.Result<readonly [Disposition, Model], ReactorError> =>
    Result.succeed(["applied", next.cause === undefined ? remember(next) : next]);
  if (message.type === "unknown") return done(model);
  if (message.type === "state_update")
    return done({ ...model, state: message.data, stateDirty: false });
  if (message.type === "queue_update") {
    const listed = [...message.data.generation, ...message.data.playout, ...message.data.history];
    const clips = new Map(model.clips);
    for (const clip of listed)
      clips.set(clip.clip_id, {
        clip,
        source,
        lifecycle: clips.get(clip.clip_id)?.lifecycle ?? null,
      });
    if (clips.size > model.maxClips) return Result.fail(overflow());
    return done({ ...model, queue: message.data, queueDirty: false, clips });
  }
  if (!("clip" in message.data)) return done(settingChanged(model, message));
  const clip = message.data.clip;
  const previous = model.clips.get(clip.clip_id);
  if (previous === undefined && model.clips.size >= model.maxClips) return Result.fail(overflow());
  const put = (lifecycle: ClipObservation["lifecycle"]): Model => ({
    ...model,
    clips: new Map(model.clips).set(clip.clip_id, { clip, source, lifecycle }),
  });
  if (message.type === "clip_moved") {
    const next = put(previous?.lifecycle ?? null);
    const moved =
      model.queue?.[message.data.queue][message.data.position]?.clip_id === clip.clip_id;
    return done(moved ? next : invalidate(next, false, true));
  }
  // An end is final, and a later phase is never reversed by an earlier one.
  if (
    previous !== undefined &&
    previous.lifecycle !== null &&
    (previous.lifecycle === message.type ||
      rank(previous.lifecycle) === 3 ||
      rank(previous.lifecycle) > rank(message.type))
  )
    return Result.succeed(["duplicate", model]);
  const next = put(message.type);
  const queued = model.queue?.generation.some((entry) => entry.clip_id === clip.clip_id) === true;
  const ready = model.queue?.playout.some((entry) => entry.clip_id === clip.clip_id) === true;
  const playing = model.state?.playing_clip_id === clip.clip_id;
  switch (message.type) {
    case "clip_queued":
      return done(!queued && !ready && !playing ? invalidate(next, true, true) : next);
    case "clip_generated":
      return done(!ready && !playing ? invalidate(next, true, true) : next);
    case "clip_started":
      return done(invalidate(next, !playing, queued || ready));
    case "clip_failed":
    case "clip_popped":
      return done(queued || ready ? invalidate(next, true, true) : next);
    case "clip_finished":
    case "clip_stopped":
      return done(playing || model.state === undefined ? invalidate(next, true, false) : next);
    default:
      // Only the clip lifecycle messages above carry a clip.
      return done(model);
  }
};

/** A setting's acknowledgement invalidates the state only when it differs from it. */
const settingChanged = (model: Model, message: DecodedMessage): Model => {
  const state = model.state;
  switch (message.type) {
    case "seed_accepted":
      return state?.seed === message.data.seed ? model : invalidate(model, true, false);
    case "clip_length_accepted":
      return state?.clip_seconds === message.data.clip_seconds
        ? model
        : invalidate(model, true, false);
    case "canvas_accepted":
      return state?.aspect === message.data.aspect &&
        state.width === message.data.width &&
        state.height === message.data.height
        ? model
        : invalidate(model, true, false);
    case "autoplay_accepted":
      return state?.autoplay === message.data.enabled ? model : invalidate(model, true, false);
    case "flush_accepted":
      return state?.flush_on_clip_end === message.data.enabled
        ? model
        : invalidate(model, true, false);
    case "session_reset":
      return state?.playing === false &&
        model.queue?.generation.length === 0 &&
        model.queue.playout.length === 0
        ? model
        : invalidate(model, true, true);
    default:
      return model;
  }
};

// ---------------------------------------------------------------------------
// Acceptance evidence
// ---------------------------------------------------------------------------

export interface Acceptance {
  readonly submissionId: string;
  readonly clip: Clip;
  readonly evidence: { readonly kind: "correlated" | "metadata"; readonly source: CommandReply };
}

/** What a submission sent, captured once, which only its own clip can match. */
export interface Identity {
  readonly id: string;
  readonly metadata: string;
  readonly prompt: string;
  readonly generation: bigint;
}

const Envelope = Schema.fromJsonString(
  Schema.Struct({
    reactor_effect_h3: Schema.Literal(1),
    namespace: Schema.String,
    submission: Schema.String,
    caller: Schema.String,
  }),
);

/** The caller's metadata wrapped with this provider's namespace and the submission id. */
export const encodeMetadata = ({
  namespace,
  submission,
  caller = "",
}: {
  readonly namespace: string;
  readonly submission: string;
  readonly caller?: string | undefined;
}): Result.Result<string, ReactorError> =>
  Result.flatMap(
    Result.mapError(
      Schema.encodeResult(Envelope)({ reactor_effect_h3: 1, namespace, submission, caller }),
      () => ReactorError.fromCode("InvalidInput", "Metadata could not be encoded"),
    ),
    (value) =>
      Array.from(value).length > metadataMaxChars
        ? Result.fail(
            ReactorError.fromCode(
              "InvalidInput",
              `Metadata including acceptance identity exceeds ${metadataMaxChars} characters`,
              { operation: "enqueue", outcome: "not-submitted" },
            ),
          )
        : Result.succeed(value),
  );

/** The local submission a clip's metadata names; foreign or extended metadata names none. */
export const submissionFromMetadata = ({
  namespace,
  metadata,
}: {
  readonly namespace: string;
  readonly metadata: string;
}): string | undefined => {
  const decoded = Schema.decodeResult(Envelope)(metadata, { onExcessProperty: "error" });
  return Result.isSuccess(decoded) && decoded.success.namespace === namespace
    ? decoded.success.submission
    : undefined;
};

/** Only the exact captured prompt and metadata, in the submission's generation, prove acceptance. */
export const acceptanceFor = ({
  identity,
  clip,
  source,
}: {
  readonly identity: Identity;
  readonly clip: Clip;
  readonly source: CommandReply;
}): Acceptance | undefined => {
  if (
    identity.generation !== source.generation ||
    identity.metadata !== clip.metadata ||
    identity.prompt !== clip.prompt
  )
    return undefined;
  const correlated =
    source.kind === "message" && source.type === "clip_queued" && source.correlation === "matched";
  return {
    submissionId: identity.id,
    clip,
    evidence: { kind: correlated ? "correlated" : "metadata", source },
  };
};
