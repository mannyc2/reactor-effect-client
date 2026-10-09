/** The H3-family values a hosted check runs, over the public provider and source APIs. */
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Struct from "effect/Struct";
import * as FastH3 from "reactor-effect-client/FastH3";
import * as FastH3Source from "reactor-effect-client/FastH3Source";
import * as H3 from "reactor-effect-client/H3";
import * as H3Source from "reactor-effect-client/H3Source";
import type * as Playout from "reactor-effect-client/Playout";
import type * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { AcquisitionFailure, CommandFailure } from "reactor-effect-client/ReactorError";
import type * as Session from "reactor-effect-client/Session";
import * as Media from "./Media.js";
import type { Model, ModelKey } from "./Spend.js";

export const prompt = "A slow camera move across a sunlit table with a glass of water.";

export type RequestInput = Pick<
  H3.Request,
  "prompt" | "seconds" | "metadata" | "seed" | "position"
>;
export type Acceptance<C extends H3.Clip> = Omit<H3.Acceptance, "clip"> & {
  readonly clip: C;
};

/** Commands share their public contract; requests and accepted clips retain their family. */
export interface Provider<Req, C extends H3.Clip> extends Omit<
  H3.Provider,
  "contract" | "prepare" | "enqueue"
> {
  readonly contract: H3.Contract | FastH3.Contract;
  readonly prepare: <E = never>(
    request: Req,
    hooks?: H3.PrepareHooks<E>,
  ) => Effect.Effect<
    H3.Submission<Acceptance<C>, CommandFailure | E>,
    ReactorError | CommandFailure
  >;
  readonly enqueue: (request: Req) => Effect.Effect<Acceptance<C>, CommandFailure>;
}

export type UploadFacts =
  | {
      readonly _tag: "References";
      readonly reportedImages: number | null;
      readonly reportedAudio: number | null;
      readonly hasReferenceAudio: boolean | null;
    }
  | { readonly _tag: "Frame"; readonly hasStartingFrame: boolean; readonly continued: boolean };

export interface Family<Req extends RequestInput, C extends H3.Clip> {
  readonly key: ModelKey;
  readonly modelName: string;
  readonly documentedVersion: string;
  readonly tracks: { readonly video: string; readonly audio: string };
  readonly canvases: ReadonlyArray<{
    readonly aspect: H3.CanvasAspect;
    readonly width: number;
    readonly height: number;
  }>;
  readonly lengths: { readonly short: number; readonly long: number };
  readonly request: (input: RequestInput) => Req;
  readonly withUploads: (metadata: string) => Req;
  readonly withUploaded: (
    metadata: string,
    files: ReadonlyArray<Session.UploadReference>,
  ) => Effect.Effect<Req, ReactorError>;
  readonly uploadCounts: { readonly images: number; readonly audio: number };
  readonly uploadsOf: (clip: C) => UploadFacts;
  readonly clip: Schema.Codec<C>;
  readonly provider: (
    session: Session.Session,
  ) => Effect.Effect<Provider<Req, C>, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope>;
  readonly open: <E = never, R = never>(
    options: H3Source.OpenOptions<E, R>,
  ) => Effect.Effect<
    Playout.Source<Req>,
    AcquisitionFailure,
    R | Reactor.Reactor | Crypto.Crypto | Scope.Scope
  >;
  readonly resume: (
    options: H3Source.ResumeOptions,
  ) => Effect.Effect<
    Playout.Source<Req>,
    AcquisitionFailure,
    Reactor.Reactor | Crypto.Crypto | Scope.Scope
  >;
  readonly clipModel: Playout.ClipModel<Req>;
}

const referencePrompt = `Picture 1 is a plain gray backdrop. Audio 1 is a low, steady hum under the scene. ${prompt}`;

export const h3 = {
  key: "h3",
  modelName: H3.modelName,
  documentedVersion: H3.documentedVersion,
  tracks: H3.h3ReferenceTurboRealtime.tracks,
  canvases: H3.h3ReferenceTurboRealtime.canvases,
  lengths: { short: 5, long: 15 },
  request: (input: RequestInput): H3.Request => ({ ...input }),
  withUploads: (metadata: string): H3.Request => ({
    prompt: referencePrompt,
    seconds: 5,
    metadata,
    references: [{ _tag: "Bytes", bytes: Media.grayPng({ width: 256, height: 144 }) }],
    audio: [
      { _tag: "Bytes", bytes: Media.tone({ seconds: 3, sampleRate: 48_000, frequency: 220 }) },
    ],
  }),
  withUploaded: Effect.fnUntraced(function* (
    metadata: string,
    files: ReadonlyArray<Session.UploadReference>,
  ): Effect.fn.Return<H3.Request, ReactorError> {
    const image = files.find((file) => file.mimeType.startsWith("image/"));
    const sound = files.find((file) => file.mimeType.startsWith("audio/"));
    if (image === undefined || sound === undefined)
      return yield* ReactorError.fromCode(
        "InvalidState",
        "clip 1's uploads were not an image and a sound",
      );
    return {
      prompt: referencePrompt,
      seconds: 5,
      metadata,
      references: [{ _tag: "Uploaded", file: image }],
      audio: [{ _tag: "Uploaded", file: sound }],
    };
  }),
  uploadCounts: { images: 1, audio: 1 },
  uploadsOf: (clip: H3.Clip): UploadFacts => ({
    _tag: "References",
    reportedImages: clip.reference_image_count ?? null,
    reportedAudio: clip.reference_audio_count ?? null,
    hasReferenceAudio: clip.has_reference_audio ?? null,
  }),
  clip: H3.Clip,
  provider: H3.make,
  open: H3Source.open,
  resume: H3Source.resume,
  clipModel: H3Source.model,
} satisfies Family<H3.Request, H3.Clip>;

export const fastH3 = {
  key: "fast-h3",
  modelName: FastH3.modelName,
  documentedVersion: FastH3.documentedVersion,
  tracks: FastH3.tracks,
  canvases: Struct.keys(FastH3.canvases).map((aspect) => ({
    aspect,
    ...FastH3.canvases[aspect],
  })),
  lengths: { short: FastH3.requestSeconds.min, long: FastH3.requestSeconds.max },
  request: (input: RequestInput): FastH3.Request => ({ ...input }),
  withUploads: (metadata: string): FastH3.Request => ({
    prompt,
    seconds: FastH3.requestSeconds.min,
    metadata,
    start: { _tag: "Bytes", bytes: Media.grayPng({ width: 256, height: 144 }) },
  }),
  withUploaded: Effect.fnUntraced(function* (
    metadata: string,
    files: ReadonlyArray<Session.UploadReference>,
  ): Effect.fn.Return<FastH3.Request, ReactorError> {
    const image = files.find((file) => file.mimeType.startsWith("image/"));
    if (image === undefined)
      return yield* ReactorError.fromCode("InvalidState", "clip 1 left no uploaded image");
    return {
      prompt,
      seconds: FastH3.requestSeconds.min,
      metadata,
      start: { _tag: "Uploaded", file: image },
    };
  }),
  uploadCounts: { images: 1, audio: 0 },
  uploadsOf: (clip: FastH3.Clip): UploadFacts => ({
    _tag: "Frame",
    hasStartingFrame: clip.has_starting_frame,
    continued: clip.continue_from_clip_id !== null,
  }),
  clip: FastH3.Clip,
  provider: FastH3.make,
  open: FastH3Source.open,
  resume: FastH3Source.resume,
  clipModel: FastH3Source.model,
} satisfies Family<FastH3.Request, FastH3.Clip>;

/** Authorization selected the model before a check asks for its family. */
export const familyOf = (run: { readonly model: Pick<Model, "name"> }) => {
  switch (run.model.name) {
    case H3.modelName:
      return h3;
    case FastH3.modelName:
      return fastH3;
    default:
      throw new Error("the check's model is not an H3-family model");
  }
};

/** Each call keeps its request, accepted clip and source on the same concrete family. */
export const withFamily: {
  <A, E, R>(
    use: <Req extends RequestInput, C extends H3.Clip>(
      family: Family<Req, C>,
    ) => Effect.Effect<A, E, R>,
  ): (run: { readonly model: Pick<Model, "name"> }) => Effect.Effect<A, E, R>;
  <A, E, R>(
    run: { readonly model: Pick<Model, "name"> },
    use: <Req extends RequestInput, C extends H3.Clip>(
      family: Family<Req, C>,
    ) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R>;
} = dual(
  2,
  <A, E, R>(
    run: { readonly model: Pick<Model, "name"> },
    use: <Req extends RequestInput, C extends H3.Clip>(
      family: Family<Req, C>,
    ) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> => {
    const family = familyOf(run);
    return family.key === "h3" ? use(family) : use(family);
  },
);
