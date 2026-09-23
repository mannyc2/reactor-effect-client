/** The JSON and wire Struct boundaries, over generated JSON. */
import { expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { ReactorError } from "../src/errors.js";
import { json, objectFromStruct, structFromObject } from "../src/json.js";
import type { Json } from "../src/json.js";
import * as W from "../src/wire.generated.js";

/** Under the `u` flag a surrogate code point is exactly an unpaired surrogate. */
const unpaired = /\p{Cs}/u;
const wellFormedText = (text: string): boolean => !unpaired.test(text);
/** Every string, key or value, is well-formed UTF-16, as the wire requires. */
const wellFormed = (value: Json): boolean =>
  typeof value === "string"
    ? wellFormedText(value)
    : Array.isArray(value)
      ? value.every(wellFormed)
      : typeof value === "object" && value !== null
        ? Object.entries(value).every(([key, item]) => wellFormedText(key) && wellFormed(item))
        : true;

it.prop(
  "json() copies well-formed JSON exactly and refuses the rest as a protocol failure",
  { value: Schema.Json },
  ({ value }) => {
    if (wellFormed(value)) expect(json(value)).toEqual(value);
    else {
      let refused: unknown;
      try {
        json(value);
      } catch (cause) {
        refused = cause;
      }
      expect(ReactorError.is(refused) && refused.reason._tag).toBe("Protocol");
    }
  },
);

it.prop(
  "a JSON object survives the wire Struct and its encoding",
  { value: Schema.JsonObject },
  ({ value }) => {
    if (!wellFormed(value)) return;
    const struct = structFromObject(value);
    expect(objectFromStruct(struct)).toEqual(value);
    expect(objectFromStruct(W.Google_Struct.decode(W.Google_Struct.encode(struct)))).toEqual(value);
  },
);
