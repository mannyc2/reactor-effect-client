import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import type { ClipModel, ClipTag } from "../../Playout.js";
import type { ReactorError } from "../../ReactorError.js";
import * as Tag from "../playout/tag.js";
import {
  alignFrames,
  h3ReferenceTurboRealtime,
  metadataMaxChars,
  requestSeconds,
} from "./profile.js";
import { validateAudioReference, validateReference } from "./references.js";
import { Request } from "./request.js";

/**
 * Where a request for the clip `tag` names falls outside H3's documented
 * limits, field by field, or an empty array within them: its metadata counted as
 * sent, wrapped with the tag and H3's own identity. Each issue names its field
 * and the limit, never the value, which may be a prompt.
 */
const check = (request: Request, tag: ClipTag): ReadonlyArray<string> => {
  const decoded = Schema.decodeResult(Request)(request, { errors: "all" });
  const refused = (field: string, index: number) => (error: ReactorError) => [
    { path: [field, String(index)], message: error.message },
  ];
  const issues = Result.isFailure(decoded)
    ? SchemaIssue.makeFormatterStandardSchemaV1()(decoded.failure.issue).issues.map((issue) => ({
        path: (issue.path ?? []).map((segment) =>
          String(Predicate.isObject(segment) ? segment.key : segment),
        ),
        message: issue.message,
      }))
    : [
        ...(decoded.success.references ?? []).flatMap((reference, index) =>
          Result.match(validateReference(reference), {
            onFailure: refused("references", index),
            onSuccess: () => [],
          }),
        ),
        ...(decoded.success.audio ?? []).flatMap((reference, index) =>
          Result.match(validateAudioReference(reference), {
            onFailure: refused("audio", index),
            onSuccess: () => [],
          }),
        ),
        ...(Tag.fits({ tag, metadata: decoded.success.metadata })
          ? []
          : [
              {
                path: ["metadata"],
                message: `exceeds ${String(metadataMaxChars)} characters once wrapped as sent`,
              },
            ]),
      ];
  return issues.map(({ path, message }) =>
    path.length === 0 ? message : `${path.join(".")}: ${message}`,
  );
};

/**
 * H3 Reference Turbo Realtime as the playout plans for it: its documented request limits, and the
 * frame grid it aligns each length up to.
 */
export const clipModel: ClipModel<Request> = {
  name: "H3",
  lengths: requestSeconds,
  defaultSeconds: requestSeconds.min,
  builtSeconds: (seconds) =>
    alignFrames(h3ReferenceTurboRealtime, seconds) / h3ReferenceTurboRealtime.fps,
  check,
};
