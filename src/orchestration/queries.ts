import * as Option from "effect/Option";
import { h3ReferenceTurboRealtime } from "../h3/profile.js";
import type { EngineState } from "./types.js";

export const emptyState = (): EngineState => ({
  availability: "Synchronizing",
  queued: [],
  generationOrder: [],
  building: Option.none(),
  ready: [],
  playing: Option.none(),
  continuable: [],
  failed: [],
  started: false,
  canvas: Option.none(),
  capacities: { ...h3ReferenceTurboRealtime.expectedCapacities },
});

/** Incomplete provider observations cannot prove that a session is idle. */
export const isIdle = (state: EngineState): boolean =>
  state.availability === "Ready" &&
  state.queued.length === 0 &&
  Option.isNone(state.building) &&
  state.ready.length === 0 &&
  Option.isNone(state.playing);

/** Unknown initial playback timing contributes no promised duration. */
export const securedMs = (state: EngineState, now: number): number => {
  const playing =
    Option.isSome(state.playing) &&
    Option.isSome(state.playing.value.record) &&
    Option.isSome(state.playing.value.startedAt)
      ? Math.max(
          0,
          state.playing.value.record.value.durationSeconds * 1000 -
            (now - state.playing.value.startedAt.value),
        )
      : 0;
  return playing + state.ready.reduce((total, clip) => total + clip.durationSeconds * 1000, 0);
};

export const committedMs = (state: EngineState, now: number): number =>
  securedMs(state, now) +
  Option.match(state.building, {
    onNone: () => 0,
    onSome: (building) => building.record.durationSeconds * 1000,
  }) +
  state.queued.reduce((total, clip) => total + clip.durationSeconds * 1000, 0);

export const pendingCount = (state: EngineState): number =>
  state.queued.length + state.ready.length + (Option.isSome(state.building) ? 1 : 0);

export const isLive = (state: EngineState): boolean => state.started;
