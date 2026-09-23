import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { SequenceSnapshot } from "../Sequence.js";
import type { Missing } from "../errors.js";
import { PolicyFailure } from "./request.js";
import type { ClipId, ClipRequest } from "./request.js";
import type { ClipRecord, EngineState } from "./types.js";

export interface Candidate<Owner> {
  readonly owner: Owner;
  readonly state: EngineState;
  readonly accepted: ReadonlySet<ClipId>;
  readonly closed: boolean;
  readonly recovering: boolean;
}

export interface RoutingDecision<Owner> {
  readonly owner: Owner;
  readonly position: number | undefined;
}

export const generation = (state: EngineState): readonly ClipRecord[] => {
  const records = new Map(state.queued.map((clip) => [clip.clipId, clip]));
  if (Option.isSome(state.building))
    records.set(state.building.value.record.clipId, state.building.value.record);
  return state.generationOrder.map((id) => {
    const clip = records.get(id);
    if (clip === undefined) throw new Error("Source generation order refers to a missing clip");
    return clip;
  });
};

export const activeIds = (state: EngineState): readonly ClipId[] => [
  ...new Set([
    ...generation(state).map((clip) => clip.clipId),
    ...state.ready.map((clip) => clip.clipId),
    ...Option.match(state.playing, { onNone: () => [], onSome: (playing) => [playing.clipId] }),
  ]),
];

const owns = <Owner>(candidate: Candidate<Owner>, id: ClipId): boolean =>
  candidate.accepted.has(id) ||
  candidate.state.continuable.includes(id) ||
  activeIds(candidate.state).includes(id) ||
  candidate.state.failed.includes(id);

/** Resolve every ownership constraint once, before committing any member. */
export const resolve = <Owner>(
  request: ClipRequest,
  candidates: readonly Candidate<Owner>[],
  defaultOwner: Owner,
  sequence: SequenceSnapshot<Owner> | undefined,
): Effect.Effect<RoutingDecision<Owner>, PolicyFailure> =>
  Effect.try({
    try: () => {
      if (sequence !== undefined && (sequence.status !== "open" || sequence.sealRequested)) {
        throw PolicyFailure.sequence(
          sequence.id,
          sequence.status === "open" ? "sealing" : sequence.status,
        );
      }
      const constraints: Owner[] = sequence === undefined ? [] : [sequence.owner];
      const ownerFor = (id: ClipId, purpose: Missing["purpose"]): Candidate<Owner> => {
        const matches = candidates.filter((candidate) => owns(candidate, id));
        if (matches.length === 0) throw PolicyFailure.missing(purpose);
        if (matches.length !== 1)
          throw PolicyFailure.refuse(
            "OwnerConflict",
            "A clip identity was observed in multiple sessions",
          );
        return matches[0]!;
      };
      const anchor = request.before === undefined ? undefined : ownerFor(request.before, "anchor");
      const sessionAnchor =
        request.sameSessionAs === undefined
          ? undefined
          : ownerFor(request.sameSessionAs, "session_anchor");
      const continuation =
        request.continueFrom === undefined
          ? undefined
          : ownerFor(request.continueFrom, "continuation");
      if (anchor !== undefined) constraints.push(anchor.owner);
      if (sessionAnchor !== undefined) constraints.push(sessionAnchor.owner);
      if (continuation !== undefined) constraints.push(continuation.owner);
      const owner = constraints[0] ?? defaultOwner;
      if (constraints.some((value) => value !== owner)) {
        throw PolicyFailure.refuse(
          "OwnerConflict",
          "Sequence, source affinity, insertion anchor and continuation require different sessions",
        );
      }
      const candidate = candidates.find((value) => value.owner === owner);
      if (candidate === undefined || candidate.closed)
        throw PolicyFailure.refuse("SessionRetired", "The request's owning session has retired");
      if (candidate.recovering || candidate.state.availability !== "Ready") {
        throw PolicyFailure.refuse("SessionRecovering", "Waiting for a coherent provider snapshot");
      }
      const queue = generation(candidate.state);
      if (queue.length >= candidate.state.capacities.generation)
        throw PolicyFailure.refuse("QueueFull", "The generation queue is full");
      if (
        request.continueFrom !== undefined &&
        !candidate.state.continuable.includes(request.continueFrom)
      ) {
        throw PolicyFailure.refuse(
          "ContinuationUnavailable",
          "Continuation ownership is known, but the retained provider view no longer offers that clip",
        );
      }
      let position = request.position;
      if (request.before !== undefined) {
        const index = queue.findIndex((clip) => clip.clipId === request.before);
        if (index < 0)
          throw PolicyFailure.refuse(
            "AnchorUnavailable",
            "Insertion anchor is no longer in the generation queue",
          );
        if (position !== undefined && position !== index) {
          throw PolicyFailure.refuse(
            "PositionConflict",
            "Explicit position and insertion anchor disagree",
          );
        }
        position = index;
      }
      // An explicit index, including a past-end index, reaches the provider
      // unchanged. With no index or anchor, omission retains provider append.
      return Object.freeze({ owner, position });
    },
    // Only a refusal is a typed failure; any other throw, such as a generation
    // order naming a missing clip, is a broken invariant and stays a defect.
    catch: (cause) => {
      if (PolicyFailure.is(cause)) return cause;
      throw cause;
    },
  });
