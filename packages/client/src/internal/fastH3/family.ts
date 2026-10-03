/** FastH3's request and message boundary for the shared provider. */
import * as Schema from "effect/Schema";
import type { Family } from "../h3/family.js";
import { Clip as H3Clip } from "../h3/messages.js";
import { canvases, documentedVersion, modelName, source } from "./profile.js";
import { capture, encode, uploads } from "./request.js";
import type { Captured, Request } from "./request.js";

const ClipId = Schema.String.check(Schema.isUUID());
export const Clip = Schema.Struct({
  ...H3Clip.fields,
  continue_from_clip_id: Schema.NullOr(ClipId),
  has_starting_frame: Schema.Boolean,
  ending_from_clip_id: ClipId.pipe(Schema.NullOr, Schema.optionalKey),
  has_ending_frame: Schema.optionalKey(Schema.Boolean),
});
export type Clip = typeof Clip.Type;
export const fastH3: Family<Request, Captured, Clip, typeof modelName, typeof documentedVersion> = {
  modelName,
  documentedVersion,
  source,
  clip: Clip,
  refuses: "reference_images",
  canvases,
  capture,
  uploads,
  admit: () => undefined,
  encode,
  // Comparable hosted state/queue reads on 2026-10-03 had equal generation counts.
  coherent: (state, queue) =>
    state.generation_queued === queue.generation.length &&
    state.playout_queued === queue.playout.length &&
    ![...queue.generation, ...queue.playout.slice(1)].some(
      (clip) => clip.clip_id === state.playing_clip_id,
    ),
};
