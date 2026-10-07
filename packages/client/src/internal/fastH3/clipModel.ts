import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import type { ClipModel, ClipTag } from "../../Playout.js";
import type { ReactorError } from "../../ReactorError.js";
import * as Tag from "../playout/tag.js";
import { metadataMaxChars } from "../h3/profile.js";
import { validateReference } from "../h3/references.js";
import { builtSeconds, requestSeconds } from "./profile.js";
import { Request } from "./request.js";

/** Field limits and references, with metadata counted as sent; never report their values. */
const check = (request: Request, tag: ClipTag): ReadonlyArray<string> => {
  const decoded = Schema.decodeResult(Request)(request, { errors: "all" });
  const refused = (field: string) => (error: ReactorError) => [
    { path: [field], message: error.message },
  ];
  const issues = Result.isFailure(decoded)
    ? SchemaIssue.makeFormatterStandardSchemaV1()(decoded.failure.issue).issues.map((issue) => ({
        path: (issue.path ?? []).map((segment) =>
          String(Predicate.isObject(segment) ? segment.key : segment),
        ),
        message: issue.message,
      }))
    : [
        ...(decoded.success.start === undefined || "continueFrom" in decoded.success.start
          ? []
          : Result.match(validateReference(decoded.success.start), {
              onFailure: refused("start"),
              onSuccess: () => [],
            })),
        ...(decoded.success.end === undefined || "endFrom" in decoded.success.end
          ? []
          : Result.match(validateReference(decoded.success.end), {
              onFailure: refused("end"),
              onSuccess: () => [],
            })),
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
 * FastH3 as the playout plans for it: its documented request limits, and the frame grid it aligns
 * each length up to.
 */
export const clipModel: ClipModel<Request> = {
  name: "FastH3",
  lengths: requestSeconds,
  // FastH3's 14.375 s session default would outlast a playout item planned at the minimum.
  defaultSeconds: requestSeconds.min,
  builtSeconds,
  check,
};
