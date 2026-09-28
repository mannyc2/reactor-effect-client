/**
 * The Node-API addon, from this platform's package or an explicit path. A
 * loaded addon stays loaded for the life of the process, as does its one
 * libwebrtc factory, which every peer on it shares.
 */
import { createRequire } from "node:module";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type * as Binding from "./binding.js";

export type Addon = typeof Binding;

const require = createRequire(import.meta.url);

/** The platform packages `napi build --platform` names, by Node's platform and architecture. */
const platforms: Readonly<Record<string, string>> = {
  "darwin-arm64": "darwin-arm64",
  "linux-x64": "linux-x64-gnu",
};

/** What the binding reads of a package manifest. */
const Manifest = Schema.Struct({ version: Schema.String });

const refused = (code: "Native" | "UnsupportedHost", message: string, detail?: unknown) =>
  ReactorError.fromCode(code, message, {
    outcome: "not-submitted",
    ...(detail === undefined ? {} : { detail }),
  });

const unloadable = (cause: unknown) => refused("Native", "could not load the native addon", cause);

const missing = (cause: unknown): boolean =>
  Predicate.hasProperty(cause, "code") && cause.code === "MODULE_NOT_FOUND";

/** The module `id` names, whatever it exports. */
const loaded = (
  id: string,
  refusal: (cause: unknown) => ReactorError,
): Effect.Effect<unknown, ReactorError> =>
  Effect.try({ try: (): unknown => require(id), catch: refusal });

/** The version a manifest declares. */
const version = (manifest: unknown) =>
  Schema.decodeUnknownEffect(Manifest)(manifest).pipe(
    Effect.map(({ version }) => version),
    Effect.mapError((cause) =>
      refused("Native", "a native package manifest has no version", cause),
    ),
  );

/**
 * The addon `id` names. Node types every module it loads as `any`; the
 * addon's surface is the declaration `napi build` generates from binding.rs.
 */
const addon = (id: string): Effect.Effect<Addon, ReactorError> =>
  Effect.map(loaded(id, unloadable), (module) => module as Addon);

/**
 * This platform's addon, whose package must be this package's own version:
 * the binding and the addon it drives are released together.
 */
const installed: Effect.Effect<Addon, ReactorError> = Effect.gen(function* () {
  const host = `${process.platform}-${process.arch}`;
  const platform = platforms[host];
  if (platform === undefined)
    return yield* refused("UnsupportedHost", `no native addon is built for ${host}`);
  const name = `reactor-effect-native-${platform}`;
  const theirs = yield* loaded(`${name}/package.json`, (cause) =>
    missing(cause)
      ? refused("UnsupportedHost", `the native peer needs its platform package ${name}`, cause)
      : unloadable(cause),
  ).pipe(Effect.flatMap(version));
  const own = yield* loaded("../../package.json", unloadable).pipe(Effect.flatMap(version));
  if (theirs !== own) return yield* refused("Native", `${name} is version ${theirs}, not ${own}`);
  return yield* addon(name);
});

/** Load the addon at `path`, or this platform's own. */
export const load = (path: string | undefined): Effect.Effect<Addon, ReactorError> =>
  path === undefined ? installed : addon(path);

/**
 * Peers whose owner join outlived its deadline, per addon, until each join
 * completes. The join may have wedged the shared libwebrtc threads a new peer
 * would need, so while any is retained no new peer is made.
 */
const retained = new WeakMap<Addon, Set<object>>();

/** Retain `peer` on `addon`; the returned function releases it. */
export const retain =
  (addon: Addon) =>
  (peer: object): (() => void) => {
    const peers = retained.get(addon) ?? new Set();
    retained.set(addon, peers.add(peer));
    return () => peers.delete(peer);
  };

/** Refuse before a session is allocated for a peer that could not work. */
export const usable = (addon: Addon): Effect.Effect<void, ReactorError> =>
  Effect.suspend(() =>
    (retained.get(addon)?.size ?? 0) === 0
      ? Effect.void
      : Effect.fail(
          refused(
            "Native",
            "native WebRTC runtime is degraded: an earlier peer's owner join exceeded its shutdown deadline and is still retained",
          ),
        ),
  );
