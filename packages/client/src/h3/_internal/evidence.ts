import { ReactorError } from "../../errors.js";
import type { CommandReply } from "../../session/index.js";
import type { Clip } from "../messages.js";
import { metadataMaxChars } from "../profile.js";
import type { Acceptance } from "../types.js";
import { Commands } from "./contracts.js";
import { plain } from "./references.js";

/** Captured once for a submission; pending registration and retirement belong to the client. */
export interface AcceptanceIdentity {
  readonly id: string;
  readonly metadata: string;
  readonly prompt: string;
  readonly generation: bigint;
}

/** Namespaced acceptance metadata is not a claim about the provider's own state. */
export const encodeMetadata = (namespace: string, submission: string, caller = ""): string => {
  const value = JSON.stringify({ reactor_effect_h3: 1, namespace, submission, caller });
  if (Array.from(value).length > metadataMaxChars)
    throw new ReactorError({
      code: "InvalidInput",
      message: `Metadata including acceptance identity exceeds ${metadataMaxChars} characters`,
      context: { operation: "enqueue", outcome: "not-submitted" },
    });
  return value;
};

/** Foreign, malformed and extended annotations cannot identify a local pending submission. */
export const submissionFromMetadata = (namespace: string, value: string): string | undefined => {
  try {
    const object: unknown = JSON.parse(value);
    const fields = plain(object, ["reactor_effect_h3", "namespace", "submission", "caller"]);
    if (
      fields.reactor_effect_h3 !== 1 ||
      typeof fields.namespace !== "string" ||
      fields.namespace !== namespace ||
      typeof fields.submission !== "string" ||
      typeof fields.caller !== "string"
    )
      return undefined;
    return fields.submission;
  } catch {
    return undefined;
  }
};

/** Only exact captured data in its generation proves acceptance; an ACK has no clip to supply. */
export const acceptanceFor = (
  identity: AcceptanceIdentity,
  clip: Clip,
  source: CommandReply,
): Acceptance | undefined => {
  if (
    identity.generation !== source.generation ||
    identity.metadata !== clip.metadata ||
    identity.prompt !== clip.prompt
  )
    return undefined;
  return Object.freeze({
    submissionId: identity.id,
    clip,
    evidence: Object.freeze({
      kind:
        source.kind === "message" &&
        source.type === Commands.enqueue.reply &&
        source.correlation === "matched"
          ? "correlated"
          : "metadata",
      source,
    }),
  });
};
