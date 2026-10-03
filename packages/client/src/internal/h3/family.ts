/** The model's wire and request rules, supplied to the shared session provider. */
import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import { CommandFailure, ReactorError } from "../../ReactorError.js";
import type { UploadReference } from "../../Session.js";
import type { Contract } from "./commands.js";
import { Clip } from "./messages.js";
import type { Queue, State } from "./messages.js";
import { canvases, documentedVersion, modelName, source } from "./profile.js";
import type { ValidatedAudioReference, ValidatedReference } from "./references.js";
import { capture, enqueueArguments } from "./request.js";
import type { Captured, Request } from "./request.js";

/** The request fields the engine uses for live bounds and acceptance identity. */
export interface RequestFields {
  readonly prompt: string;
  readonly seconds?: number | undefined;
  readonly metadata?: string | undefined;
}

export interface Family<
  Req,
  Captured extends RequestFields,
  C extends Clip,
  Name extends string,
  Version extends string,
> {
  readonly modelName: Name;
  readonly documentedVersion: Version;
  readonly source: string;
  readonly clip: Schema.Codec<C, unknown>;
  readonly refuses?: string | undefined;
  readonly canvases: Readonly<Record<string, { readonly width: number; readonly height: number }>>;
  readonly capture: (input: Req) => Effect.Effect<Captured, ReactorError>;
  readonly uploads: (
    request: Captured,
  ) => ReadonlyArray<ValidatedReference | ValidatedAudioReference>;
  readonly admit: (
    request: Captured,
    contract: Contract<Name, Version>,
  ) => CommandFailure | undefined;
  readonly encode: (input: {
    readonly request: Captured;
    readonly uploaded: ReadonlyArray<UploadReference>;
    readonly metadata: string;
  }) => Schema.JsonObject;
  readonly coherent: (state: State, queue: Queue<C>) => boolean;
}

export const h3: Family<Request, Captured, Clip, typeof modelName, typeof documentedVersion> = {
  modelName,
  documentedVersion,
  source,
  clip: Clip,
  canvases,
  capture,
  uploads: (request) => [...request.references, ...request.audio],
  admit: (request, contract) =>
    request.audio.length > 0 && !contract.referenceAudio
      ? CommandFailure.from(
          ReactorError.fromCode(
            "UnsupportedCapability",
            "The H3 deployment does not declare reference audio",
          ),
          { operation: "enqueue", outcome: "not-submitted" },
        )
      : undefined,
  encode: ({ request, uploaded, metadata }) =>
    enqueueArguments({
      request,
      images: uploaded.slice(0, request.references.length),
      audio: uploaded.slice(request.references.length),
      metadata,
    }),
  coherent: (state, queue) =>
    state.generation_queued === queue.generation.length &&
    state.playout_queued === queue.playout.length &&
    ![...queue.generation, ...queue.playout.slice(1)].some(
      (clip) => clip.clip_id === state.playing_clip_id,
    ),
};
