/** FastH3's opener and closer, captured once and projected onto its wire arguments. */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ReactorError } from "../../ReactorError.js";
import type { UploadReference } from "../../Session.js";
import { Request as H3Request } from "../h3/request.js";
import { Reference, ValidatedReferenceSchema, validateReference } from "../h3/references.js";
import type { ValidatedReference } from "../h3/references.js";
import { requestSeconds } from "./profile.js";

const ClipId = Schema.String.check(Schema.isUUID());
const Continue = Schema.Struct({ continueFrom: ClipId });
const EndFrom = Schema.Struct({ endFrom: ClipId });
export const Request = Schema.Struct({
  prompt: H3Request.fields.prompt,
  metadata: H3Request.fields.metadata,
  seconds: Schema.optionalKey(
    Schema.Finite.check(
      Schema.isBetween({ minimum: requestSeconds.min, maximum: requestSeconds.max }),
    ),
  ),
  seed: Schema.optionalKey(Schema.Natural),
  position: Schema.optionalKey(Schema.Natural),
  start: Schema.optionalKey(Schema.Union([Reference, ValidatedReferenceSchema, Continue])),
  end: Schema.optionalKey(Schema.Union([Reference, ValidatedReferenceSchema, EndFrom])),
});
export type Request = typeof Request.Type;
export interface Captured extends Omit<Request, "start" | "end"> {
  readonly start?: ValidatedReference | typeof Continue.Type | undefined;
  readonly end?: ValidatedReference | typeof EndFrom.Type | undefined;
}

export const capture = Effect.fnUntraced(function* (input: Request) {
  const request = yield* Schema.decodeEffect(Request)(input).pipe(
    Effect.mapError((cause) =>
      ReactorError.fromCode("InvalidInput", "Invalid FastH3 request", {
        operation: "enqueue",
        outcome: "not-submitted",
        detail: cause,
      }),
    ),
  );
  let start = request.start;
  if (start !== undefined && !("continueFrom" in start))
    start = yield* Effect.fromResult(validateReference(start));
  let end = request.end;
  if (end !== undefined && !("endFrom" in end))
    end = yield* Effect.fromResult(validateReference(end));
  const captured: Captured = { ...request, start, end };
  return captured;
});

export const uploads = (request: Captured): ReadonlyArray<ValidatedReference> => [
  ...(request.start === undefined || "continueFrom" in request.start ? [] : [request.start]),
  ...(request.end === undefined || "endFrom" in request.end ? [] : [request.end]),
];
const wireUpload = (file: UploadReference) => ({
  upload_id: file.uploadId,
  name: file.name,
  mime_type: file.mimeType,
  size: Number(file.size),
});
export const encode = ({
  request,
  uploaded,
  metadata,
}: {
  readonly request: Captured;
  readonly uploaded: ReadonlyArray<UploadReference>;
  readonly metadata: string;
}): Schema.JsonObject => {
  let index = 0;
  const nextFrame = () => {
    const file = uploaded[index++];
    // Capture and staging own the matching frame order; an absent upload is a defect.
    if (file === undefined) throw new Error("FastH3 frame upload is missing");
    return wireUpload(file);
  };
  const args: Record<string, Schema.Json> = {
    prompt: request.prompt,
    metadata,
  };
  if (request.seconds !== undefined) args.seconds = request.seconds;
  if (request.seed !== undefined) args.seed = request.seed;
  if (request.position !== undefined) args.position = request.position;
  if (request.start !== undefined) {
    if ("continueFrom" in request.start) args.continue_from_clip_id = request.start.continueFrom;
    else args.starting_frame = nextFrame();
  }
  if (request.end !== undefined) {
    if ("endFrom" in request.end) args.ending_from_clip_id = request.end.endFrom;
    else args.ending_frame = nextFrame();
  }
  return args;
};
