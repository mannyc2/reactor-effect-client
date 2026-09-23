import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import { ReactorError } from "./errors.js";
import { checkedString } from "./protobuf.js";
import type { Google_Struct, Google_Value } from "./wire.generated.js";
/** Immutable JSON, as Effect's `Schema.Json` defines it. */
export type Json = Schema.Json;
export type JsonObject = Schema.JsonObject;
const bad = (message: string): never => {
  throw ReactorError.fromCode("Protocol", message);
};
export const record = (u: unknown, context = "object"): Record<string, unknown> =>
  Predicate.isObject(u) ? u : bad(`expected ${context}`);
export const string = (u: unknown, context: string, max = 1_048_576): string =>
  typeof u === "string" ? checkedString(u, max) : bad(`expected string: ${context}`);
export const nonempty = (u: unknown, context: string): string => {
  const s = string(u, context);
  return s.length ? s : bad(`empty ${context}`);
};
export const finite = (u: unknown, context: string): number =>
  typeof u === "number" && Number.isFinite(u) ? u : bad(`expected finite number: ${context}`);
export const uint32 = (u: unknown, context: string): number => {
  const n = finite(u, context);
  return Number.isInteger(n) && n >= 0 && n <= 0xffffffff ? n : bad(`expected uint32: ${context}`);
};
export const array = (u: unknown, context: string): readonly unknown[] =>
  Array.isArray(u) ? u : bad(`expected array: ${context}`);

/**
 * Validates, copies, and freezes finite JSON; never coerces undefined, cycles, Dates, accessors
 * or array holes. Keys and string values share one text budget; array length is charged against
 * the node budget before any slot is read.
 *
 * An array's JSON value is its index slots, as for JSON.stringify and Schema.Json. Its other own
 * keys, string or symbol, are never read and are absent from the copy, so a RegExp match result
 * copies as its matches. An object's own keys are its value, so a symbol key there is rejected.
 */
export const json = (input: unknown, maximumNodes = 65_536): Json => {
  const ancestors = new Set<object>();
  let nodes = 0,
    chars = 0;
  const text = (s: string): string => {
    chars += s.length;
    return chars > 4_194_304 ? bad("JSON text limit") : checkedString(s, 4_194_304);
  };
  const visit = (u: unknown, depth: number): Json => {
    if (++nodes > maximumNodes || depth > 64) return bad("JSON node/depth limit");
    if (u === null || typeof u === "boolean") return u;
    if (typeof u === "number") return finite(u, "JSON number");
    if (typeof u === "string") return text(u);
    if (typeof u !== "object") return bad("not a JSON value");
    if (ancestors.has(u)) return bad("cyclic JSON");
    ancestors.add(u);
    try {
      if (Array.isArray(u)) {
        const length = u.length;
        if (length > maximumNodes - nodes) return bad("JSON node/depth limit");
        const items: Json[] = [];
        for (let index = 0; index < length; index++) {
          const descriptor = Object.getOwnPropertyDescriptor(u, index);
          if (descriptor === undefined) return bad("sparse arrays are not JSON");
          if (!("value" in descriptor)) return bad("JSON accessors are not supported");
          items.push(visit(descriptor.value, depth + 1));
        }
        return Object.freeze(items);
      }
      const proto: unknown = Object.getPrototypeOf(u);
      if (proto !== Object.prototype && proto !== null)
        return bad("JSON object must have a plain prototype");
      if (Object.getOwnPropertySymbols(u).length) return bad("symbol keys are not JSON");
      const result: Record<string, Json> = {};
      for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(u))) {
        if (!descriptor.enumerable) continue;
        if (!("value" in descriptor)) return bad("JSON accessors are not supported");
        text(key);
        const value: unknown = descriptor.value;
        Object.defineProperty(result, key, { value: visit(value, depth + 1), enumerable: true });
      }
      return Object.freeze(result);
    } finally {
      ancestors.delete(u);
    }
  };
  return visit(input, 0);
};
const isJsonObject = (value: Json): value is JsonObject =>
  typeof value === "object" && value !== null && !Array.isArray(value);
export const jsonObject = (input: unknown): JsonObject => {
  const value = json(input);
  return isJsonObject(value) ? value : bad("command data must be a top-level JSON object");
};
const valueFromJson = (value: Json): Google_Value => {
  if (value === null) return { kind: { case: "null_value", value: 0 } };
  if (typeof value === "boolean") return { kind: { case: "bool_value", value } };
  if (typeof value === "number") return { kind: { case: "number_value", value } };
  if (typeof value === "string") return { kind: { case: "string_value", value } };
  if (Array.isArray(value))
    return { kind: { case: "list_value", value: { values: value.map(valueFromJson) } } };
  return { kind: { case: "struct_value", value: structFromObject(value) } };
};
export const structFromObject = (input: unknown): Google_Struct => {
  const data = jsonObject(input),
    fields = new Map<string, Google_Value>();
  for (const [key, value] of Object.entries(data)) fields.set(key, valueFromJson(value));
  return { fields };
};
/** Upstream Struct conversion maps unset/unknown Value kinds and nonfinite doubles to null.
 * The raw generated codecs preserve those distinctions; use them when JSON is insufficient.
 */
const valueToJson = (value: Google_Value, depth: number): Json => {
  if (depth > 64) return bad("Struct depth limit");
  const kind = value.kind;
  if (kind === undefined) return null;
  switch (kind.case) {
    case "null_value":
      return null;
    case "number_value":
      return Number.isFinite(kind.value) ? kind.value : null;
    case "string_value":
    case "bool_value":
      return kind.value;
    case "struct_value":
      return objectFromStruct(kind.value, depth + 1);
    case "list_value":
      return Object.freeze(kind.value.values.map((v) => valueToJson(v, depth + 1)));
  }
};
export const objectFromStruct = (s: Google_Struct, depth = 0): JsonObject => {
  if (depth > 64) return bad("Struct depth limit");
  const result: Record<string, Json> = {};
  for (const [key, value] of s.fields)
    Object.defineProperty(result, key, { value: valueToJson(value, depth + 1), enumerable: true });
  return Object.freeze(result);
};
