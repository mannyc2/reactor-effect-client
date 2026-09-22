import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as FetchHttp from "../src/FetchHttp.js";
import * as NodeServices from "@effect/platform-node/NodeServices";
type Services = PlatformHttp.HttpClient | NodeServices.NodeServices;
const provided = <A, E>(effect: Effect.Effect<A, E, Services>) =>
  effect.pipe(
    Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
    Effect.provideService(FetchHttpClient.Fetch, globalThis.fetch),
  );
import { ReactorError } from "../src/errors.js";
import { isRecord } from "../src/json.js";
export { test } from "bun:test";
export function assert(condition: unknown, message = "assertion failed"): asserts condition {
  if (!condition) throw new Error(message);
}
export const normalize = (value: unknown): unknown => {
  if (typeof value === "bigint") return String(value);
  if (typeof value === "number") {
    if (Number.isNaN(value)) return { $number: "NaN" };
    if (!Number.isFinite(value)) return { $number: value > 0 ? "+Infinity" : "-Infinity" };
    if (Object.is(value, -0)) return { $number: "-0" };
  }
  if (value instanceof Map) return normalize(Object.fromEntries(value));
  if (value instanceof Uint8Array) return [...value];
  if (Array.isArray(value)) return value.map((x: unknown) => normalize(x));
  if (isRecord(value))
    return Object.fromEntries(
      Object.entries(value)
        .filter(([k, v]) => k !== "_unknown" && v !== undefined)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => [k, normalize(v)]),
    );
  return value;
};
export const equal = (actual: unknown, expected: unknown): void => {
  const a = JSON.stringify(normalize(actual)),
    e = JSON.stringify(normalize(expected));
  if (a !== e) throw new Error(`Expected ${e}, received ${a}`);
};
export const throws = (body: () => unknown, code?: string): void => {
  try {
    body();
  } catch (error) {
    if (code !== undefined)
      assert(
        error instanceof ReactorError && error.code === code,
        `expected ${code}, got ${String(error)}`,
      );
    return;
  }
  throw new Error("expected a throw");
};
export const run = <A>(effect: Effect.Effect<A, ReactorError, Services>): Promise<A> =>
  Effect.runPromise(provided(effect));
export const failure = async <A>(
  effect: Effect.Effect<A, ReactorError, Services>,
): Promise<ReactorError> => {
  const result = await Effect.runPromise(provided(Effect.result(effect)));
  assert(result._tag === "Failure", "expected Effect failure");
  return result.failure;
};
export const eventually = async (
  check: () => boolean,
  message = "condition deadline",
): Promise<void> => {
  const end = performance.now() + 2000;
  while (!check()) {
    if (performance.now() > end) throw new Error(message);
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
};
export const hex = (bytes: Uint8Array): string =>
  [...bytes].map((n) => n.toString(16).padStart(2, "0")).join("");
export const unhex = (text: string): Uint8Array<ArrayBuffer> => {
  assert(text.length % 2 === 0 && /^[0-9a-f]*$/i.test(text), "invalid hexadecimal fixture");
  return new Uint8Array(
    Array.from({ length: text.length / 2 }, (_, i) =>
      Number.parseInt(text.slice(i * 2, i * 2 + 2), 16),
    ),
  );
};

export const withGlobals = async (
  values: Readonly<Record<string, unknown>>,
  body: () => Promise<void>,
): Promise<void> => {
  const previous = Object.entries(values).map(([key, value]) => {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, key);
    Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
    return { key, descriptor };
  });
  try {
    await body();
  } finally {
    for (const { key, descriptor } of previous) {
      if (descriptor === undefined) Reflect.deleteProperty(globalThis, key);
      else Object.defineProperty(globalThis, key, descriptor);
    }
  }
};
