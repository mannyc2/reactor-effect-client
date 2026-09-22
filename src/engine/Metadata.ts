import { Option, Schema } from "effect"
import type { ClipMetadata } from "../Clip.js"

/** Provider metadata is opaque; this envelope adds only the private admission token. */
export const EncodedMetadata = Schema.Struct({
  v: Schema.Literal(1),
  token: Schema.String,
  caller: Schema.optionalKey(Schema.JsonObject)
})
export type EncodedMetadata = typeof EncodedMetadata.Type

export const encodeMetadata = (metadata: ClipMetadata, token: string): string =>
  JSON.stringify(Object.keys(metadata).length === 0 ? { v: 1, token } : { v: 1, token, caller: metadata })

export const decodeMetadata = (raw: string | null | undefined): Option.Option<EncodedMetadata> => {
  if (raw === null || raw === undefined) return Option.none()
  try {
    const result = Schema.decodeUnknownResult(EncodedMetadata)(JSON.parse(raw))
    return result._tag === "Success" ? Option.some(result.success) : Option.none()
  } catch {
    return Option.none()
  }
}
