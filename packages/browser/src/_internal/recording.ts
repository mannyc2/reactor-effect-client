import type * as Effect from "effect/Effect";
import { Coordinator } from "reactor-effect-client";
import type { ReactorError } from "reactor-effect-client";
import type { ClipReady } from "reactor-effect-client/wire";

export type Segment = Coordinator.Segment;
export type DownloadOptions = Coordinator.DownloadOptions;
export type DownloadedClip = Coordinator.DownloadedClip;

/** Parse a clip's HLS media playlist into the segments to concatenate. */
export const parsePlaylist = Coordinator.parsePlaylist;

/**
 * Bounded transport of a prepared recording, through the portable coordinator
 * client (`Coordinator.make()`); see `Coordinator.Client.downloadClip`.
 */
export const downloadClip = (
  coordinator: Coordinator.Client,
  clip: ClipReady,
  options?: DownloadOptions,
): Effect.Effect<DownloadedClip, ReactorError> => coordinator.downloadClip(clip, options);
