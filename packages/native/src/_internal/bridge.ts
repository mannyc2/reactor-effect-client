import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import * as Schema from "effect/Schema";
import { ReactorError, ErrorCode } from "reactor-effect-client";

const ABI_VERSION = 2;
const CALL_BUFFER_BYTES = 4 * 1024 * 1024;
const ERROR_BUFFER_BYTES = 4096;
const MAX_PACKET_BYTES = 96 * 1024 * 1024;
const MAX_IN_FLIGHT_CALLS = 128;
const MAX_IN_FLIGHT_REQUEST_BYTES = 16 * 1024 * 1024;
const MAX_REQUEST_BYTES = 1024 * 1024;
const MAX_MESSAGE_BYTES = 262_144;

const STATUS_OK = 0;
const STATUS_AGAIN = 1;
const STATUS_BUFFER_TOO_SMALL = 2;
const STATUS_CLOSED = 3;

export const NativeCall = Object.freeze({
  Prepare: 1,
  Answer: 2,
  Direction: 3,
  MaxBitrate: 4,
  Stats: 5,
  MediaSnapshot: 6,
} as const);

export type NativeCall = (typeof NativeCall)[keyof typeof NativeCall];

interface AsyncNativeFunction {
  readonly async: (
    ...args: readonly [...unknown[], (error: unknown, result: number) => void]
  ) => void;
}

interface NativeLibrary {
  readonly func: (prototype: string) => unknown;
}

interface KoffiModule {
  readonly load: (path: string) => NativeLibrary;
}

interface NativeApi {
  readonly library: NativeLibrary;
  readonly binarySha256: string;
  readonly buildIdentity: string;
  readonly create: () => bigint | null;
  readonly call: AsyncNativeFunction;
  readonly send: AsyncNativeFunction;
  readonly pollEvent: AsyncNativeFunction;
  readonly pollVideo: AsyncNativeFunction;
  readonly pollAudio: AsyncNativeFunction;
  readonly close: (handle: bigint) => void;
  readonly shutdown: AsyncNativeFunction;
  readonly destroy: (handle: bigint) => void;
}

const APIs = new Map<string, NativeApi>();

const libraryName = (): string => {
  switch (process.platform) {
    case "darwin":
      return "libreactor_effect_native.dylib";
    case "win32":
      return "reactor_effect_native.dll";
    default:
      return "libreactor_effect_native.so";
  }
};

const defaultPaths = (): readonly string[] => {
  const name = libraryName();
  const platform = `${process.platform}-${process.arch}`;
  // The staged library lives beside the package's compiled and source trees:
  // lib/<platform>-<arch>/ resolves identically from dist/ and from src/.
  return [fileURLToPath(new URL(`../../lib/${platform}/${name}`, import.meta.url))];
};

const asAsync = (value: unknown, symbol: string): AsyncNativeFunction => {
  if (typeof value !== "function" || !("async" in value)) {
    throw new ReactorError(
      "Native",
      `native library symbol ${symbol} does not support asynchronous calls`,
    );
  }
  return value as AsyncNativeFunction;
};

const importKoffi = async (): Promise<KoffiModule> => {
  try {
    const module = await import("koffi");
    return module.default as unknown as KoffiModule;
  } catch (cause) {
    throw new ReactorError(
      "UnsupportedHost",
      "the native export requires the optional koffi dependency",
      { detail: cause, outcome: "not-submitted" },
    );
  }
};

const loadAt = async (path: string): Promise<NativeApi> => {
  const cached = APIs.get(path);
  if (cached !== undefined) return cached;
  const koffi = await importKoffi();
  let library: NativeLibrary;
  let binarySha256: string;
  try {
    binarySha256 = createHash("sha256")
      .update(await readFile(path))
      .digest("hex");
    library = koffi.load(path);
  } catch (cause) {
    throw new ReactorError("Native", `could not load native WebRTC bridge at ${path}`, {
      detail: cause,
      outcome: "not-submitted",
    });
  }
  const symbol = (prototype: string): unknown => {
    try {
      return library.func(prototype);
    } catch (cause) {
      throw new ReactorError(
        "Native",
        `native WebRTC bridge is incompatible: missing ${prototype.split("(")[0] ?? prototype}`,
        { detail: cause, outcome: "not-submitted" },
      );
    }
  };
  const abi = symbol("uint32_t reactor_effect_abi_version(void)") as () => number;
  const actual = abi();
  if (actual !== ABI_VERSION)
    throw new ReactorError(
      "Native",
      `native WebRTC ABI mismatch: expected ${ABI_VERSION}, received ${actual}`,
      { outcome: "not-submitted" },
    );
  const api: NativeApi = {
    library,
    binarySha256,
    buildIdentity: (symbol("const char *reactor_effect_build_identity(void)") as () => string)(),
    create: symbol("void *reactor_effect_peer_create(void)") as () => bigint | null,
    call: asAsync(
      symbol(
        "int reactor_effect_peer_call(void *peer, uint32_t operation, const uint8_t *request, size_t request_len, _Out_ uint8_t *response, size_t response_cap, _Out_ size_t *response_len)",
      ),
      "reactor_effect_peer_call",
    ),
    send: asAsync(
      symbol(
        "int reactor_effect_peer_send(void *peer, uint32_t channel, const uint8_t *data, size_t data_len, _Out_ uint8_t *error, size_t error_cap, _Out_ size_t *error_len)",
      ),
      "reactor_effect_peer_send",
    ),
    pollEvent: asAsync(
      symbol(
        "int reactor_effect_peer_poll_event(void *peer, uint32_t timeout_ms, _Out_ uint8_t *out, size_t out_cap, _Out_ size_t *out_len)",
      ),
      "reactor_effect_peer_poll_event",
    ),
    pollVideo: asAsync(
      symbol(
        "int reactor_effect_peer_poll_video(void *peer, uint32_t timeout_ms, _Out_ uint8_t *out, size_t out_cap, _Out_ size_t *out_len)",
      ),
      "reactor_effect_peer_poll_video",
    ),
    pollAudio: asAsync(
      symbol(
        "int reactor_effect_peer_poll_audio(void *peer, uint32_t timeout_ms, _Out_ uint8_t *out, size_t out_cap, _Out_ size_t *out_len)",
      ),
      "reactor_effect_peer_poll_audio",
    ),
    close: symbol("void reactor_effect_peer_close(void *peer)") as (handle: bigint) => void,
    shutdown: asAsync(
      symbol(
        "int reactor_effect_peer_shutdown(void *peer, _Out_ uint8_t *error, size_t error_cap, _Out_ size_t *error_len)",
      ),
      "reactor_effect_peer_shutdown",
    ),
    destroy: symbol("void reactor_effect_peer_destroy(void *peer)") as (handle: bigint) => void,
  };
  APIs.set(path, api);
  return api;
};

export const resolveNativeBridge = async (path?: string): Promise<string> => {
  if (path !== undefined) {
    await loadAt(path);
    return path;
  }
  const failures: string[] = [];
  for (const candidate of defaultPaths()) {
    try {
      await verifyStagedNativeBridge(candidate);
      return candidate;
    } catch (cause) {
      failures.push(cause instanceof Error ? cause.message : String(cause));
    }
  }
  throw new ReactorError(
    "Native",
    "native WebRTC bridge is not staged or its identity is invalid; run bun run native:build or provide an explicit library path",
    { detail: failures, outcome: "not-submitted" },
  );
};

export const checkNativeBridge = async (path?: string): Promise<void> => {
  await resolveNativeBridge(path);
};

const checked = (path: string): NativeApi => {
  const api = APIs.get(path);
  if (api === undefined)
    throw new ReactorError(
      "InvalidState",
      "native WebRTC bridge was not preflighted; run PeerFactory.check before make",
      { outcome: "not-submitted" },
    );
  return api;
};

const toNumber = (value: unknown, name: string): number => {
  const number = typeof value === "bigint" ? Number(value) : value;
  if (typeof number !== "number" || !Number.isSafeInteger(number) || number < 0)
    throw new ReactorError("Protocol", `native ${name} is outside the safe integer range`);
  return number;
};

const jsonRecord = (bytes: Uint8Array, operation: string): Record<string, unknown> => {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(bytes));
  } catch (cause) {
    throw new ReactorError("Protocol", `native ${operation} returned invalid JSON`, {
      detail: cause,
    });
  }
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new ReactorError("Protocol", `native ${operation} returned a non-object response`);
  return value as Record<string, unknown>;
};

/** Validate the staged artifact used by both source and installed-package tests. */
export const verifyStagedNativeBridge = async (
  path: string,
): Promise<Readonly<Record<string, unknown>>> => {
  try {
    const [manifestBytes, binary] = await Promise.all([
      readFile(join(dirname(path), "native-identity.json")),
      readFile(path),
    ]);
    const manifest = jsonRecord(manifestBytes, "artifact identity");
    if (
      manifest.schemaVersion !== 1 ||
      manifest.platform !== `${process.platform}-${process.arch}` ||
      manifest.library !== libraryName() ||
      manifest.sha256 !== createHash("sha256").update(binary).digest("hex")
    ) {
      throw new ReactorError("Native", "native artifact does not match its staged identity");
    }
    const api = await loadAt(path);
    if (api.binarySha256 !== manifest.sha256) {
      throw new ReactorError(
        "Native",
        "staged native artifact changed after this process loaded it",
      );
    }
    const prefix = "reactor-effect-native:build-identity:",
      suffix = ":end";
    if (!api.buildIdentity.startsWith(prefix) || !api.buildIdentity.endsWith(suffix)) {
      throw new ReactorError("Native", "loaded native artifact omitted its source/build identity");
    }
    const build = jsonRecord(
      Buffer.from(api.buildIdentity.slice(prefix.length, -suffix.length)),
      "build identity",
    );
    if (
      build.abiVersion !== ABI_VERSION ||
      build.profile !== "release" ||
      JSON.stringify(build) !== JSON.stringify(manifest.build)
    ) {
      throw new ReactorError(
        "Native",
        "loaded native build identity differs from its staged artifact",
      );
    }
    return Object.freeze(manifest);
  } catch (cause) {
    throw new ReactorError("Native", "native staged artifact verification failed", {
      detail: cause,
      outcome: "not-submitted",
    });
  }
};

const isErrorCode = Schema.is(ErrorCode);

const operationError = (status: number, bytes: Uint8Array, operation: string): ReactorError => {
  const response = bytes.length === 0 ? {} : jsonRecord(bytes, operation);
  const rawCode = typeof response.code === "string" ? response.code : undefined;
  const code = isErrorCode(rawCode) ? rawCode : status === STATUS_CLOSED ? "Closed" : "Native";
  // A native status/code does not establish whether a side effect executed.
  // Keep backend messages for explicit inspection rather than diagnostics: a
  // libwebrtc error can contain peer SDP or caller-supplied signaling material.
  return new ReactorError(code, `native ${operation} failed (${code})`, {
    operation,
    outcome: "unknown",
    detail: response,
  });
};

const asyncStatus = (fn: AsyncNativeFunction, args: readonly unknown[]): Promise<number> =>
  new Promise((resolve, reject) => {
    fn.async(...args, (error, result) => (error == null ? resolve(result) : reject(error)));
  });

export interface NativePacket {
  readonly header: Readonly<Record<string, unknown>>;
  readonly payload: Uint8Array<ArrayBuffer>;
}

export type NativePoll =
  | { readonly _tag: "Packet"; readonly packet: NativePacket }
  | { readonly _tag: "Again" }
  | { readonly _tag: "Closed" };

const parsePacket = (bytes: Uint8Array): NativePacket => {
  if (bytes.length < 4)
    throw new ReactorError("Protocol", "native packet omitted its header length");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const headerLength = view.getUint32(0, true);
  if (headerLength > bytes.length - 4)
    throw new ReactorError("Protocol", "native packet header length exceeds packet size");
  const header = jsonRecord(bytes.subarray(4, 4 + headerLength), "packet");
  const payload = Uint8Array.from(bytes.subarray(4 + headerLength));
  return { header: Object.freeze(header), payload };
};

export class NativeBridge {
  private readonly api: NativeApi;
  private handle: bigint | undefined;
  private closed = false;
  private readonly active = new Set<Promise<void>>();
  private readonly readers = new Set<AsyncNativeFunction>();
  private requestBytes = 0;
  private shutdownTask: Promise<void> | undefined;
  constructor(path: string) {
    this.api = checked(path);
    const handle = this.api.create();
    if (handle === null)
      throw new ReactorError("Native", "native WebRTC peer allocation failed", {
        outcome: "not-submitted",
      });
    this.handle = handle;
  }

  private require(): bigint {
    if (this.closed || this.handle === undefined)
      throw new ReactorError("Closed", "native WebRTC peer is closed", {
        outcome: "not-submitted",
      });
    return this.handle;
  }

  private async withHandle<A>(bytes: number, body: (handle: bigint) => Promise<A>): Promise<A> {
    const handle = this.require();
    if (
      this.active.size >= MAX_IN_FLIGHT_CALLS ||
      this.requestBytes + bytes > MAX_IN_FLIGHT_REQUEST_BYTES
    ) {
      throw new ReactorError("Overflow", "native foreign-call admission bound exceeded", {
        outcome: "not-submitted",
      });
    }
    let release!: () => void;
    const lease = new Promise<void>((resolve) => {
      release = resolve;
    });
    // Register before dispatching to Koffi. Effect interruption may abandon the
    // waiter, but the lease belongs to actual native completion, including time
    // spent queued on Koffi's executor and both halves of a packet poll.
    this.active.add(lease);
    this.requestBytes += bytes;
    try {
      return await body(handle);
    } finally {
      this.requestBytes -= bytes;
      this.active.delete(lease);
      release();
    }
  }

  async call(operation: NativeCall, request: Uint8Array = new Uint8Array()): Promise<unknown> {
    if (request.byteLength > MAX_REQUEST_BYTES)
      throw new ReactorError("Overflow", "native request exceeds 1 MiB", {
        outcome: "not-submitted",
      });
    return this.withHandle(CALL_BUFFER_BYTES + request.byteLength, async (handle) => {
      const response = Buffer.allocUnsafe(CALL_BUFFER_BYTES),
        responseLength: Array<number | bigint | null> = [0];
      const input = Buffer.from(request);
      let status: number;
      try {
        status = await asyncStatus(this.api.call, [
          handle,
          operation,
          input,
          input.byteLength,
          response,
          response.byteLength,
          responseLength,
        ]);
      } catch (cause) {
        throw new ReactorError("Native", "native WebRTC call completion failed", {
          detail: cause,
          outcome: "unknown",
        });
      }
      const length = toNumber(responseLength[0], "call response length");
      if (length > response.byteLength)
        throw new ReactorError("Protocol", "native call response exceeded its declared buffer");
      const bytes = Uint8Array.from(response.subarray(0, length));
      if (status !== STATUS_OK) throw operationError(status, bytes, `call:${operation}`);
      try {
        return JSON.parse(new TextDecoder().decode(bytes));
      } catch (cause) {
        throw new ReactorError("Protocol", "native call returned invalid JSON", { detail: cause });
      }
    });
  }

  async send(channel: "control" | "data", bytes: Uint8Array): Promise<void> {
    if (bytes.byteLength > MAX_MESSAGE_BYTES)
      throw new ReactorError("Overflow", "native data channel message exceeds 262144 bytes", {
        outcome: "not-submitted",
      });
    return this.withHandle(ERROR_BUFFER_BYTES + bytes.byteLength, async (handle) => {
      const error = Buffer.allocUnsafe(ERROR_BUFFER_BYTES),
        errorLength: Array<number | bigint | null> = [0];
      const input = Buffer.from(bytes);
      let status: number;
      try {
        status = await asyncStatus(this.api.send, [
          handle,
          channel === "control" ? 0 : 1,
          input,
          input.byteLength,
          error,
          error.byteLength,
          errorLength,
        ]);
      } catch (cause) {
        throw new ReactorError("Native", `native ${channel} send completion failed`, {
          detail: cause,
          outcome: "unknown",
        });
      }
      const length = toNumber(errorLength[0], "send error length");
      if (length > error.byteLength)
        throw new ReactorError("Protocol", "native send error exceeded its declared buffer");
      if (status !== STATUS_OK)
        throw operationError(status, Uint8Array.from(error.subarray(0, length)), `send:${channel}`);
    });
  }

  private async poll(fn: AsyncNativeFunction, timeoutMs: number): Promise<NativePoll> {
    if (this.closed) return { _tag: "Closed" };
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 0xffffffff) {
      throw new ReactorError(
        "InvalidInput",
        "native poll timeout must be an unsigned 32-bit integer",
        { outcome: "not-submitted" },
      );
    }
    if (this.readers.has(fn))
      throw new ReactorError("AlreadyReading", "native packet queue already has a reader", {
        outcome: "not-submitted",
      });
    this.readers.add(fn);
    try {
      return await this.withHandle(0, async (handle) => {
        const needed: Array<number | bigint | null> = [0];
        let status: number;
        try {
          status = await asyncStatus(fn, [handle, timeoutMs, null, 0, needed]);
        } catch (cause) {
          throw new ReactorError("Native", "native event poll could not execute", {
            detail: cause,
          });
        }
        if (this.closed) return { _tag: "Closed" };
        if (status === STATUS_AGAIN) return { _tag: "Again" };
        if (status === STATUS_CLOSED) return { _tag: "Closed" };
        if (status !== STATUS_BUFFER_TOO_SMALL)
          throw new ReactorError("Native", `native event poll failed with status ${status}`);
        const length = toNumber(needed[0], "packet length");
        if (length < 4 || length > MAX_PACKET_BYTES)
          throw new ReactorError(
            "Overflow",
            `native packet size ${length} exceeds the local bound`,
          );
        const output = Buffer.allocUnsafe(length),
          actual: Array<number | bigint | null> = [0];
        try {
          status = await asyncStatus(fn, [handle, 0, output, output.byteLength, actual]);
        } catch (cause) {
          throw new ReactorError("Native", "native packet copy could not execute", {
            detail: cause,
          });
        }
        if (this.closed || status === STATUS_CLOSED) return { _tag: "Closed" };
        if (status !== STATUS_OK)
          throw new ReactorError("Native", `native packet copy failed with status ${status}`);
        const actualLength = toNumber(actual[0], "copied packet length");
        if (actualLength !== length)
          throw new ReactorError("Protocol", "native packet changed between size and copy polls");
        return { _tag: "Packet", packet: parsePacket(Uint8Array.from(output)) };
      });
    } finally {
      this.readers.delete(fn);
    }
  }

  pollEvent(timeoutMs = 100): Promise<NativePoll> {
    return this.poll(this.api.pollEvent, timeoutMs);
  }
  pollVideo(timeoutMs = 100): Promise<NativePoll> {
    return this.poll(this.api.pollVideo, timeoutMs);
  }
  pollAudio(timeoutMs = 100): Promise<NativePoll> {
    return this.poll(this.api.pollAudio, timeoutMs);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const handle = this.handle;
    if (handle !== undefined) this.api.close(handle);
  }

  shutdown(): Promise<void> {
    if (this.shutdownTask !== undefined) return this.shutdownTask;
    const handle = this.handle;
    if (handle === undefined) return Promise.resolve();
    this.close();
    this.shutdownTask = this.finishShutdown(handle);
    return this.shutdownTask;
  }

  private async finishShutdown(handle: bigint): Promise<void> {
    // close() fences new host admission and wakes native polls. Drain all host
    // foreign calls before joining and destroying the native owner. A native
    // owner join cannot see work that is still queued in Koffi.
    await Promise.all(this.active);
    const error = Buffer.allocUnsafe(ERROR_BUFFER_BYTES),
      errorLength: Array<number | bigint | null> = [0];
    let status: number;
    try {
      status = await asyncStatus(this.api.shutdown, [handle, error, error.byteLength, errorLength]);
    } catch (cause) {
      throw new ReactorError(
        "Shutdown",
        "native WebRTC owner join could not execute; handle retained",
        { detail: cause },
      );
    }
    const length = toNumber(errorLength[0], "shutdown error length");
    if (length > error.byteLength)
      throw new ReactorError("Shutdown", "native shutdown error exceeded its declared buffer");
    if (status !== STATUS_OK)
      throw new ReactorError(
        "Shutdown",
        operationError(status, Uint8Array.from(error.subarray(0, length)), "shutdown").message,
      );
    this.api.destroy(handle);
    this.handle = undefined;
  }
}

export const encodeNativeJson = (value: unknown): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(new TextEncoder().encode(JSON.stringify(value)));
export const encodeNativeText = (value: string): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(new TextEncoder().encode(value));
