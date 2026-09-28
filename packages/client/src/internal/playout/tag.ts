/**
 * A playout clip's identity on H3: the item key or filler index, with the
 * application's own metadata, inside the caller part of H3's metadata
 * envelope, so a resumed session's clips are recognized whatever process sent
 * them.
 */
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { ClipTag } from "../../Playout.js";
import { callerFits } from "../h3/state.js";
import { ItemKey } from "./errors.js";

const Envelope = Schema.fromJsonString(
  Schema.Struct({ reactor_effect_h3: Schema.Literal(1), caller: Schema.String }),
);
const Tag = Schema.fromJsonString(
  Schema.Struct({
    playout: Schema.Literal(1),
    key: Schema.optionalKey(ItemKey),
    filler: Schema.optionalKey(Schema.Int),
    metadata: Schema.optionalKey(Schema.String),
  }),
);

/** The caller metadata a clip is enqueued with, or undefined when it cannot be encoded. */
export const encode = ({
  tag,
  metadata,
}: {
  readonly tag: ClipTag;
  readonly metadata: string | undefined;
}): string | undefined =>
  Result.getOrUndefined(
    Schema.encodeResult(Tag)({
      playout: 1,
      ...(tag._tag === "Item" ? { key: tag.key } : { filler: tag.index }),
      ...(metadata === undefined ? {} : { metadata }),
    }),
  );

/** The tag a clip's full H3 metadata carries; undefined for a clip this library did not tag. */
export const decode = (metadata: string): ClipTag | undefined => {
  const envelope = Schema.decodeResult(Envelope)(metadata);
  if (Result.isFailure(envelope)) return undefined;
  const tag = Schema.decodeResult(Tag)(envelope.success.caller);
  if (Result.isFailure(tag)) return undefined;
  if (tag.success.key !== undefined) return { _tag: "Item", key: tag.success.key };
  return tag.success.filler === undefined
    ? undefined
    : { _tag: "Filler", index: tag.success.filler };
};

/** Whether a clip's metadata still fits H3's bound once its tag and H3's own identity wrap it. */
export const fits = (clip: {
  readonly tag: ClipTag;
  readonly metadata: string | undefined;
}): boolean => {
  const caller = encode(clip);
  return caller !== undefined && callerFits(caller);
};
