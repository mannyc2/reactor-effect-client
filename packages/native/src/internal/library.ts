/**
 * The native library: Koffi, the loaded shared library, its symbols and the
 * staged artifact's identity. A loaded library stays loaded for the life of the
 * process, as does its one libwebrtc factory, so a library is loaded once per
 * path and shared by every peer that uses it.
 */
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the Node binding reads its own staged artifact before Koffi maps it
import { createHash } from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the Node binding reads its own staged artifact before Koffi maps it
import { readFile } from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Koffi takes a filesystem path; the sidecar sits beside it
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ReactorError } from "reactor-effect-client/ReactorError";

export const ABI_VERSION = 4;

export interface AsyncNativeFunction {
  readonly async: (
    ...args: readonly [...unknown[], (error: unknown, result: number) => void]
  ) => void;
}

interface KoffiLibrary {
  readonly func: (prototype: string) => unknown;
}

interface KoffiModule {
  readonly load: (path: string) => KoffiLibrary;
  readonly proto: (result: string, parameters: readonly string[]) => unknown;
  readonly pointer: (type: unknown) => unknown;
  readonly register: (callback: (ready: number) => void, type: unknown) => unknown;
  readonly unregister: (callback: unknown) => void;
}

export type OutLength = Array<number | bigint | null>;

/** The C ABI, as Koffi binds it. */
export interface NativeApi {
  readonly register: (callback: (ready: number) => void) => unknown;
  readonly unregister: (callback: unknown) => void;
  readonly create: (notify: unknown) => bigint | null;
  readonly call: AsyncNativeFunction;
  readonly send: AsyncNativeFunction;
  readonly takeEvent: (handle: bigint, out: Uint8Array, cap: number, length: OutLength) => number;
  readonly takeVideo: (
    handle: bigint,
    header: Uint8Array,
    bgra: Uint8Array,
    bgraCap: number,
    metadata: Uint8Array,
    metadataCap: number,
  ) => number;
  readonly takeAudio: (
    handle: bigint,
    header: Uint8Array,
    pcm: Int16Array,
    pcmCap: number,
  ) => number;
  readonly close: (handle: bigint) => void;
  readonly shutdown: AsyncNativeFunction;
  readonly destroy: (handle: bigint) => void;
}

/** A peer whose owner join outlived its deadline; the join still owns its handle. */
export interface Retained {
  readonly handle: bigint;
}

export interface Library {
  readonly path: string;
  readonly api: NativeApi;
  readonly binarySha256: string;
  readonly buildIdentity: string;
  /**
   * Owner joins that outlived their deadline. Every peer of a library shares
   * its libwebrtc factory, so while one is retained no new peer is made.
   */
  readonly retained: Set<Retained>;
}

const libraries = new Map<string, Library>();
// Koffi type names are process-global; one prototype serves every library.
let notifyType: unknown;

const refused = (message: string, detail?: unknown): ReactorError =>
  ReactorError.fromCode("Native", message, {
    outcome: "not-submitted",
    ...(detail === undefined ? {} : { detail }),
  });

export const libraryName = (): string => {
  switch (process.platform) {
    case "darwin":
      return "libreactor_effect_native.dylib";
    case "win32":
      return "reactor_effect_native.dll";
    default:
      return "libreactor_effect_native.so";
  }
};

/** The staged library beside the package's compiled and source trees alike. */
export const stagedPath = (): string =>
  fileURLToPath(
    new URL(`../../lib/${process.platform}-${process.arch}/${libraryName()}`, import.meta.url),
  );

const sha256 = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");

const koffi = Effect.tryPromise({
  // Koffi's module declarations describe its CommonJS default export loosely.
  try: () => import("koffi").then((module) => module.default as unknown as KoffiModule),
  catch: (cause) =>
    ReactorError.fromCode("UnsupportedHost", "the native peer needs the optional koffi package", {
      outcome: "not-submitted",
      detail: cause,
    }),
});

const bind = (koffiModule: KoffiModule, library: KoffiLibrary): NativeApi => {
  const symbol = (prototype: string): unknown => {
    try {
      return library.func(prototype);
    } catch (cause) {
      throw refused(
        `native WebRTC bridge is incompatible: missing ${prototype.split("(")[0] ?? prototype}`,
        cause,
      );
    }
  };
  const asynchronous = (prototype: string): AsyncNativeFunction => {
    const value = symbol(prototype);
    if (typeof value !== "function" || !("async" in value))
      throw refused("native library symbol does not support asynchronous calls");
    // Koffi exposes `async` on every bound function; the guard above checks it.
    return value as AsyncNativeFunction;
  };
  notifyType ??= koffiModule.pointer(koffiModule.proto("void", ["uint32_t"]));
  const notify = notifyType;
  // Each cast names the C prototype bound on the same line.
  return {
    register: (callback) => koffiModule.register(callback, notify),
    unregister: (callback) => koffiModule.unregister(callback),
    create: symbol("void *reactor_effect_peer_create(void *notify)") as NativeApi["create"],
    call: asynchronous(
      "int reactor_effect_peer_call(void *peer, uint32_t operation, const uint8_t *request, size_t request_len, _Out_ uint8_t *response, size_t response_cap, _Out_ size_t *response_len, _Out_ uint8_t *failure)",
    ),
    send: asynchronous(
      "int reactor_effect_peer_send(void *peer, uint32_t channel, const uint8_t *data, size_t data_len, _Out_ uint8_t *failure)",
    ),
    takeEvent: symbol(
      "int reactor_effect_peer_take_event(void *peer, _Out_ uint8_t *out, size_t out_cap, _Out_ size_t *out_len)",
    ) as NativeApi["takeEvent"],
    takeVideo: symbol(
      "int reactor_effect_peer_take_video(void *peer, _Out_ uint8_t *header, _Out_ uint8_t *bgra, size_t bgra_cap, _Out_ uint8_t *metadata, size_t metadata_cap)",
    ) as NativeApi["takeVideo"],
    takeAudio: symbol(
      "int reactor_effect_peer_take_audio(void *peer, _Out_ uint8_t *header, _Out_ int16_t *pcm, size_t pcm_cap)",
    ) as NativeApi["takeAudio"],
    close: symbol("void reactor_effect_peer_close(void *peer)") as NativeApi["close"],
    shutdown: asynchronous("int reactor_effect_peer_shutdown(void *peer, _Out_ uint8_t *failure)"),
    destroy: symbol("void reactor_effect_peer_destroy(void *peer)") as NativeApi["destroy"],
  };
};

/** Load, check the ABI and bind the library at `path`, once per process. */
export const load = Effect.fnUntraced(function* (path: string) {
  const cached = libraries.get(path);
  if (cached !== undefined) return cached;
  const koffiModule = yield* koffi;
  const binary = yield* Effect.tryPromise({
    try: () => readFile(path),
    catch: (cause) => refused(`could not read the native WebRTC bridge at ${path}`, cause),
  });
  const library = yield* Effect.try({
    try: () => {
      const loaded = koffiModule.load(path);
      // The ABI version and build identity are plain C functions of no arguments.
      const abi = (loaded.func("uint32_t reactor_effect_abi_version(void)") as () => number)();
      if (abi !== ABI_VERSION)
        throw refused(`native WebRTC ABI mismatch: expected ${ABI_VERSION}, received ${abi}`);
      const identity = (
        loaded.func("const char *reactor_effect_build_identity(void)") as () => string
      )();
      return {
        path,
        api: bind(koffiModule, loaded),
        binarySha256: sha256(binary),
        buildIdentity: identity,
        retained: new Set<Retained>(),
      };
    },
    catch: (cause) =>
      ReactorError.is(cause)
        ? cause
        : refused(`could not load native WebRTC bridge at ${path}`, cause),
  });
  libraries.set(path, library);
  return library;
});

const Json = Schema.fromJsonString(Schema.Json);

const Manifest = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  platform: Schema.String,
  library: Schema.String,
  sha256: Schema.String,
  build: Schema.Json,
});
export type Manifest = typeof Manifest.Type;

const Build = Schema.Struct({
  abiVersion: Schema.Literal(ABI_VERSION),
  profile: Schema.Literal("release"),
});

const identityPrefix = "reactor-effect-native:build-identity:";
const identitySuffix = ":end";

/**
 * Check the staged artifact at `path` against its identity sidecar, then load
 * it and check that what was mapped is what the sidecar names.
 */
export const verifyStaged = Effect.fnUntraced(
  function* (path: string) {
    const [sidecar, binary] = yield* Effect.tryPromise({
      try: () =>
        Promise.all([
          readFile(join(dirname(path), "native-identity.json"), "utf8"),
          readFile(path),
        ]),
      catch: (cause) => refused("native staged artifact is unreadable", cause),
    });
    const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(sidecar);
    if (
      manifest.platform !== `${process.platform}-${process.arch}` ||
      manifest.library !== libraryName() ||
      manifest.sha256 !== sha256(binary)
    )
      return yield* refused("native artifact does not match its staged identity");
    const library = yield* load(path);
    if (library.binarySha256 !== manifest.sha256)
      return yield* refused("staged native artifact changed after this process loaded it");
    const identity = library.buildIdentity;
    if (!identity.startsWith(identityPrefix) || !identity.endsWith(identitySuffix))
      return yield* refused("loaded native artifact omitted its source and build identity");
    const embedded = yield* Schema.decodeEffect(Json)(
      identity.slice(identityPrefix.length, -identitySuffix.length),
    );
    yield* Schema.decodeUnknownEffect(Build)(embedded);
    const [loadedBuild, stagedBuild] = yield* Effect.all([
      Schema.encodeEffect(Json)(embedded),
      Schema.encodeEffect(Json)(manifest.build),
    ]);
    if (loadedBuild !== stagedBuild)
      return yield* refused("loaded native build identity differs from its staged artifact");
    return manifest;
  },
  Effect.mapError((cause) =>
    ReactorError.is(cause) ? cause : refused("native staged artifact verification failed", cause),
  ),
);

/** An explicit library as it is; otherwise the staged library, verified. */
export const resolve = (path: string | undefined): Effect.Effect<Library, ReactorError> =>
  path === undefined
    ? Effect.flatMap(
        Effect.suspend(() => verifyStaged(stagedPath())).pipe(
          Effect.mapError((cause) =>
            ReactorError.fromCode(
              "Native",
              "native WebRTC bridge is not staged or its identity is invalid; run bun run native:build or give an explicit library path",
              { outcome: "not-submitted", detail: cause },
            ),
          ),
        ),
        () => load(stagedPath()),
      )
    : load(path);

/**
 * Refuse a new peer while an earlier owner join is retained: it may have
 * wedged the shared libwebrtc threads a new owner would need, and failing here
 * keeps a caller from allocating remote sessions for peers that cannot work.
 */
export const requireUsable = (library: Library): Effect.Effect<void, ReactorError> =>
  Effect.suspend(() =>
    library.retained.size === 0
      ? Effect.void
      : Effect.fail(
          refused(
            "native WebRTC runtime is degraded: an earlier peer's owner join exceeded its shutdown deadline and is still retained",
          ),
        ),
  );
