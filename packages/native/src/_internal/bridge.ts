import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { ReactorError } from "reactor-effect-client";
import type { ErrorCode } from "reactor-effect-client";

const ABI_VERSION = 3;
const CALL_BUFFER_BYTES = 4 * 1024 * 1024;
const FAILURE_BYTES = 1024;
const VIDEO_HEADER_BYTES = 40;
const AUDIO_HEADER_BYTES = 16;
const EVENT_BUFFER_BYTES = 64 * 1024;
// The native queues' byte bounds: no single item can exceed them.
const MAX_EVENT_BYTES = 16 * 1024 * 1024;
const MAX_VIDEO_BYTES = 64 * 1024 * 1024;
const MAX_AUDIO_SAMPLES = 2 * 1024 * 1024;
const MAX_IN_FLIGHT_CALLS = 128;
const MAX_IN_FLIGHT_REQUEST_BYTES = 16 * 1024 * 1024;
const MAX_REQUEST_BYTES = 1024 * 1024;
const MAX_MESSAGE_BYTES = 262_144;

const STATUS_OK = 0;
const STATUS_AGAIN = 1;
const STATUS_BUFFER_TOO_SMALL = 2;
const STATUS_CLOSED = 3;

/** The C ABI's closed set of failure classes, keyed by status. */
const FAILURE_CODES: ReadonlyMap<number, ErrorCode> = new Map([
  [STATUS_CLOSED, "Closed"],
  [-1, "InvalidInput"],
  [-2, "Native"],
  [-3, "Overflow"],
  [-4, "Protocol"],
  [-5, "SdpRejected"],
  [-6, "ChannelClosed"],
]);

/** A status outside the ABI's classes is an unclassified native failure. */
export const failureCode = (status: number): ErrorCode => FAILURE_CODES.get(status) ?? "Native";

export const NativeCall = Object.freeze({
  Prepare: 1,
  Answer: 2,
  Direction: 3,
  MaxBitrate: 4,
  Stats: 5,
  MediaSnapshot: 6,
} as const);

export type NativeCall = (typeof NativeCall)[keyof typeof NativeCall];

/** Readiness bits the native notifier passes to the host callback. */
export const Ready = Object.freeze({ Events: 1, Video: 2, Audio: 4 } as const);

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
  readonly proto: (result: string, parameters: readonly string[]) => unknown;
  readonly pointer: (type: unknown) => unknown;
  readonly register: (callback: (ready: number) => void, type: unknown) => unknown;
  readonly unregister: (callback: unknown) => void;
}

type OutLength = Array<number | bigint | null>;

interface NativeApi {
  readonly library: NativeLibrary;
  readonly binarySha256: string;
  readonly buildIdentity: string;
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

const APIs = new Map<string, NativeApi>();
// Koffi type names are process-global; one anonymous prototype serves every library.
let notifyType: unknown;

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
    throw new ReactorError({
      code: "Native",
      message: `native library symbol ${symbol} does not support asynchronous calls`,
    });
  }
  return value as AsyncNativeFunction;
};

const importKoffi = async (): Promise<KoffiModule> => {
  try {
    const module = await import("koffi");
    return module.default as unknown as KoffiModule;
  } catch (cause) {
    throw new ReactorError({
      code: "UnsupportedHost",
      message: "the native export requires the optional koffi dependency",
      context: { detail: cause, outcome: "not-submitted" },
    });
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
    throw new ReactorError({
      code: "Native",
      message: `could not load native WebRTC bridge at ${path}`,
      context: { detail: cause, outcome: "not-submitted" },
    });
  }
  const symbol = (prototype: string): unknown => {
    try {
      return library.func(prototype);
    } catch (cause) {
      throw new ReactorError({
        code: "Native",
        message: `native WebRTC bridge is incompatible: missing ${prototype.split("(")[0] ?? prototype}`,
        context: { detail: cause, outcome: "not-submitted" },
      });
    }
  };
  const abi = symbol("uint32_t reactor_effect_abi_version(void)") as () => number;
  const actual = abi();
  if (actual !== ABI_VERSION)
    throw new ReactorError({
      code: "Native",
      message: `native WebRTC ABI mismatch: expected ${ABI_VERSION}, received ${actual}`,
      context: { outcome: "not-submitted" },
    });
  notifyType ??= koffi.pointer(koffi.proto("void", ["uint32_t"]));
  const notify = notifyType;
  const api: NativeApi = {
    library,
    binarySha256,
    buildIdentity: (symbol("const char *reactor_effect_build_identity(void)") as () => string)(),
    register: (callback) => koffi.register(callback, notify),
    unregister: (callback) => koffi.unregister(callback),
    create: symbol("void *reactor_effect_peer_create(void *notify)") as (
      notify: unknown,
    ) => bigint | null,
    call: asAsync(
      symbol(
        "int reactor_effect_peer_call(void *peer, uint32_t operation, const uint8_t *request, size_t request_len, _Out_ uint8_t *response, size_t response_cap, _Out_ size_t *response_len, _Out_ uint8_t *failure)",
      ),
      "reactor_effect_peer_call",
    ),
    send: asAsync(
      symbol(
        "int reactor_effect_peer_send(void *peer, uint32_t channel, const uint8_t *data, size_t data_len, _Out_ uint8_t *failure)",
      ),
      "reactor_effect_peer_send",
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
    close: symbol("void reactor_effect_peer_close(void *peer)") as (handle: bigint) => void,
    shutdown: asAsync(
      symbol("int reactor_effect_peer_shutdown(void *peer, _Out_ uint8_t *failure)"),
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
  throw new ReactorError({
    code: "Native",
    message:
      "native WebRTC bridge is not staged or its identity is invalid; run bun run native:build or provide an explicit library path",
    context: { detail: failures, outcome: "not-submitted" },
  });
};

export const checkNativeBridge = async (path?: string): Promise<void> => {
  await resolveNativeBridge(path);
};

const checked = (path: string): NativeApi => {
  const api = APIs.get(path);
  if (api === undefined)
    throw new ReactorError({
      code: "InvalidState",
      message: "native WebRTC bridge was not preflighted; run PeerFactory.check before make",
      context: { outcome: "not-submitted" },
    });
  return api;
};

const toNumber = (value: unknown, name: string): number => {
  const number = typeof value === "bigint" ? Number(value) : value;
  if (typeof number !== "number" || !Number.isSafeInteger(number) || number < 0)
    throw new ReactorError({
      code: "Protocol",
      message: `native ${name} is outside the safe integer range`,
    });
  return number;
};

const jsonRecord = (bytes: Uint8Array, operation: string): Record<string, unknown> => {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(bytes));
  } catch (cause) {
    throw new ReactorError({
      code: "Protocol",
      message: `native ${operation} returned invalid JSON`,
      context: { detail: cause },
    });
  }
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new ReactorError({
      code: "Protocol",
      message: `native ${operation} returned a non-object response`,
    });
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
      throw new ReactorError({
        code: "Native",
        message: "native artifact does not match its staged identity",
      });
    }
    const api = await loadAt(path);
    if (api.binarySha256 !== manifest.sha256) {
      throw new ReactorError({
        code: "Native",
        message: "staged native artifact changed after this process loaded it",
      });
    }
    const prefix = "reactor-effect-native:build-identity:",
      suffix = ":end";
    if (!api.buildIdentity.startsWith(prefix) || !api.buildIdentity.endsWith(suffix)) {
      throw new ReactorError({
        code: "Native",
        message: "loaded native artifact omitted its source/build identity",
      });
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
      throw new ReactorError({
        code: "Native",
        message: "loaded native build identity differs from its staged artifact",
      });
    }
    return Object.freeze(manifest);
  } catch (cause) {
    throw new ReactorError({
      code: "Native",
      message: "native staged artifact verification failed",
      context: { detail: cause, outcome: "not-submitted" },
    });
  }
};

const failureOf = (
  status: number,
  failure: Uint8Array,
  operation: string,
  detail: Readonly<Record<string, unknown>> = {},
): ReactorError => {
  const code = failureCode(status);
  const length = Math.min(
    new DataView(failure.buffer, failure.byteOffset, failure.byteLength).getUint32(0, true),
    failure.byteLength - 4,
  );
  // A native status does not establish whether a side effect executed. Keep
  // backend messages for explicit inspection rather than diagnostics: a
  // libwebrtc error can contain peer SDP or caller-supplied signaling material.
  return new ReactorError({
    code,
    message: `native ${operation} failed (${code})`,
    context: {
      operation,
      outcome: "unknown",
      detail: {
        status,
        message: new TextDecoder().decode(failure.subarray(4, 4 + length)),
        ...detail,
      },
    },
  });
};

const asyncStatus = (fn: AsyncNativeFunction, args: readonly unknown[]): Promise<number> =>
  new Promise((resolve, reject) => {
    fn.async(...args, (error, result) =>
      error == null
        ? resolve(result)
        : reject(
            error instanceof Error
              ? error
              : new Error("native async call failed", { cause: error }),
          ),
    );
  });

export interface NativePacket {
  readonly header: Readonly<Record<string, unknown>>;
  readonly payload: Uint8Array<ArrayBuffer>;
}

export interface NativeVideo {
  readonly track: number;
  readonly width: number;
  readonly height: number;
  readonly frameId: bigint;
  readonly timestampMicros: bigint;
  readonly data: Uint8Array<ArrayBuffer>;
  readonly metadata: Uint8Array<ArrayBuffer>;
}

export interface NativeAudio {
  readonly track: number;
  readonly sampleRate: number;
  readonly channels: number;
  readonly samples: Int16Array<ArrayBuffer>;
}

/** A take: the item, `undefined` when the queue is empty, `null` once it is closed. */
export type Take<A> = A | undefined | null;

const parsePacket = (bytes: Uint8Array): NativePacket => {
  if (bytes.length < 4)
    throw new ReactorError({
      code: "Protocol",
      message: "native packet omitted its header length",
    });
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const headerLength = view.getUint32(0, true);
  if (headerLength > bytes.length - 4)
    throw new ReactorError({
      code: "Protocol",
      message: "native packet header length exceeds packet size",
    });
  const header = jsonRecord(bytes.subarray(4, 4 + headerLength), "packet");
  return { header: Object.freeze(header), payload: bytes.slice(4 + headerLength) };
};

const overSized = (what: string, size: number): ReactorError =>
  new ReactorError({
    code: "Protocol",
    message: `native ${what} of ${size} exceeds the native queue bound`,
  });

/**
 * One native peer. Media and transport events never cross into JavaScript on
 * their own: a native notifier thread invokes `onReady` on the JavaScript
 * thread, and the host drains each named queue with synchronous takes, one
 * copy per item into memory the consumer then owns.
 */
export class NativeBridge {
  private readonly api: NativeApi;
  private readonly notify: unknown;
  private handle: bigint | undefined;
  private closed = false;
  private readonly active = new Set<Promise<void>>();
  private requestBytes = 0;
  private shutdownTask: Promise<void> | undefined;
  private readonly videoHeader = new Uint8Array(VIDEO_HEADER_BYTES);
  private readonly audioHeader = new Uint8Array(AUDIO_HEADER_BYTES);
  private readonly eventLength: OutLength = [0];
  private event = new Uint8Array(EVENT_BUFFER_BYTES);
  private metadata = new Uint8Array(256);
  // The buffer the next take copies into. Frames keep their size, so each is
  // allocated once at the previous frame's size and handed to its consumer.
  private nextVideo = new Uint8Array(0);
  private nextAudio = new Int16Array(0);

  constructor(path: string, onReady: (ready: number) => void) {
    this.api = checked(path);
    this.notify = this.api.register((ready) => {
      if (!this.closed) onReady(ready);
    });
    let handle: bigint | null = null;
    try {
      handle = this.api.create(this.notify);
    } finally {
      if (handle === null) this.api.unregister(this.notify);
    }
    if (handle === null)
      throw new ReactorError({
        code: "Native",
        message: "native WebRTC peer allocation failed",
        context: { outcome: "not-submitted" },
      });
    this.handle = handle;
  }

  private require(): bigint {
    if (this.closed || this.handle === undefined)
      throw new ReactorError({
        code: "Closed",
        message: "native WebRTC peer is closed",
        context: { outcome: "not-submitted" },
      });
    return this.handle;
  }

  private async withHandle<A>(bytes: number, body: (handle: bigint) => Promise<A>): Promise<A> {
    const handle = this.require();
    if (
      this.active.size >= MAX_IN_FLIGHT_CALLS ||
      this.requestBytes + bytes > MAX_IN_FLIGHT_REQUEST_BYTES
    ) {
      throw new ReactorError({
        code: "Overflow",
        message: "native foreign-call admission bound exceeded",
        context: { outcome: "not-submitted" },
      });
    }
    let release!: () => void;
    const lease = new Promise<void>((resolve) => {
      release = resolve;
    });
    // Register before dispatching to Koffi. Effect interruption may abandon the
    // waiter, but the lease belongs to actual native completion, including time
    // spent queued on Koffi's executor.
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
      throw new ReactorError({
        code: "Overflow",
        message: "native request exceeds 1 MiB",
        context: { outcome: "not-submitted" },
      });
    return this.withHandle(CALL_BUFFER_BYTES + request.byteLength, async (handle) => {
      const response = Buffer.allocUnsafe(CALL_BUFFER_BYTES),
        responseLength: OutLength = [0],
        failure = Buffer.alloc(FAILURE_BYTES);
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
          failure,
        ]);
      } catch (cause) {
        throw new ReactorError({
          code: "Native",
          message: "native WebRTC call completion failed",
          context: { detail: cause, outcome: "unknown" },
        });
      }
      if (status !== STATUS_OK) throw failureOf(status, failure, `call:${operation}`);
      const length = toNumber(responseLength[0], "call response length");
      if (length > response.byteLength)
        throw new ReactorError({
          code: "Protocol",
          message: "native call response exceeded its declared buffer",
        });
      try {
        const reply: unknown = JSON.parse(new TextDecoder().decode(response.subarray(0, length)));
        return reply;
      } catch (cause) {
        throw new ReactorError({
          code: "Protocol",
          message: "native call returned invalid JSON",
          context: { detail: cause },
        });
      }
    });
  }

  async send(channel: "control" | "data", bytes: Uint8Array): Promise<void> {
    if (bytes.byteLength > MAX_MESSAGE_BYTES)
      throw new ReactorError({
        code: "Overflow",
        message: "native data channel message exceeds 262144 bytes",
        context: { outcome: "not-submitted" },
      });
    return this.withHandle(FAILURE_BYTES + bytes.byteLength, async (handle) => {
      const failure = Buffer.alloc(FAILURE_BYTES);
      const input = Buffer.from(bytes);
      let status: number;
      try {
        status = await asyncStatus(this.api.send, [
          handle,
          channel === "control" ? 0 : 1,
          input,
          input.byteLength,
          failure,
        ]);
      } catch (cause) {
        throw new ReactorError({
          code: "Native",
          message: `native ${channel} send completion failed`,
          context: { detail: cause, outcome: "unknown" },
        });
      }
      if (status !== STATUS_OK) throw failureOf(status, failure, `send:${channel}`, { channel });
    });
  }

  /** Synchronous and nonblocking; call from the readiness callback until empty. */
  takeEvent(): Take<NativePacket> {
    const handle = this.handle;
    if (this.closed || handle === undefined) return null;
    for (;;) {
      const status = this.api.takeEvent(
        handle,
        this.event,
        this.event.byteLength,
        this.eventLength,
      );
      if (status === STATUS_AGAIN) return undefined;
      if (status === STATUS_CLOSED) return null;
      const length = toNumber(this.eventLength[0], "event length");
      if (status === STATUS_OK) {
        if (length > this.event.byteLength)
          throw new ReactorError({
            code: "Protocol",
            message: "native event exceeded its declared buffer",
          });
        return parsePacket(this.event.subarray(0, length));
      }
      if (status !== STATUS_BUFFER_TOO_SMALL)
        throw new ReactorError({
          code: "Native",
          message: `native event take failed with status ${status}`,
        });
      if (length <= this.event.byteLength || length > MAX_EVENT_BYTES)
        throw overSized("event", length);
      this.event = new Uint8Array(length);
    }
  }

  /** Synchronous and nonblocking; the returned bytes belong to the caller. */
  takeVideo(): Take<NativeVideo> {
    const handle = this.handle;
    if (this.closed || handle === undefined) return null;
    const header = new DataView(this.videoHeader.buffer);
    for (;;) {
      const status = this.api.takeVideo(
        handle,
        this.videoHeader,
        this.nextVideo,
        this.nextVideo.byteLength,
        this.metadata,
        this.metadata.byteLength,
      );
      if (status === STATUS_AGAIN) return undefined;
      if (status === STATUS_CLOSED) return null;
      const dataLength = header.getUint32(8, true),
        metadataLength = header.getUint32(12, true);
      if (status === STATUS_OK) {
        const data = this.nextVideo;
        this.nextVideo = new Uint8Array(dataLength);
        return {
          track: header.getUint32(32, true),
          width: header.getUint32(0, true),
          height: header.getUint32(4, true),
          frameId: header.getBigUint64(16, true),
          timestampMicros: header.getBigUint64(24, true),
          // A smaller frame than its predecessor leaves slack; never expose it.
          data: dataLength === data.byteLength ? data : data.slice(0, dataLength),
          metadata: this.metadata.slice(0, metadataLength),
        };
      }
      if (status !== STATUS_BUFFER_TOO_SMALL)
        throw new ReactorError({
          code: "Native",
          message: `native video take failed with status ${status}`,
        });
      if (dataLength + metadataLength > MAX_VIDEO_BYTES)
        throw overSized("video frame", dataLength + metadataLength);
      const growData = dataLength > this.nextVideo.byteLength,
        growMetadata = metadataLength > this.metadata.byteLength;
      if (!growData && !growMetadata)
        throw new ReactorError({
          code: "Protocol",
          message: "native video take refused a fitting frame",
        });
      if (growData) this.nextVideo = new Uint8Array(dataLength);
      if (growMetadata) this.metadata = new Uint8Array(metadataLength);
    }
  }

  /** Synchronous and nonblocking; the returned samples belong to the caller. */
  takeAudio(): Take<NativeAudio> {
    const handle = this.handle;
    if (this.closed || handle === undefined) return null;
    const header = new DataView(this.audioHeader.buffer);
    for (;;) {
      const status = this.api.takeAudio(
        handle,
        this.audioHeader,
        this.nextAudio,
        this.nextAudio.length,
      );
      if (status === STATUS_AGAIN) return undefined;
      if (status === STATUS_CLOSED) return null;
      const samples = header.getUint32(8, true);
      if (status === STATUS_OK) {
        const pcm = this.nextAudio;
        this.nextAudio = new Int16Array(samples);
        return {
          sampleRate: header.getUint32(0, true),
          channels: header.getUint32(4, true),
          track: header.getUint32(12, true),
          samples: samples === pcm.length ? pcm : pcm.slice(0, samples),
        };
      }
      if (status !== STATUS_BUFFER_TOO_SMALL)
        throw new ReactorError({
          code: "Native",
          message: `native audio take failed with status ${status}`,
        });
      if (samples > MAX_AUDIO_SAMPLES) throw overSized("audio block", samples);
      if (samples <= this.nextAudio.length)
        throw new ReactorError({
          code: "Protocol",
          message: "native audio take refused a fitting block",
        });
      this.nextAudio = new Int16Array(samples);
    }
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
    // close() fences new host admission. Drain all host foreign calls before
    // joining and destroying the native owner. A native owner join cannot see
    // work that is still queued in Koffi.
    await Promise.all(this.active);
    const failure = Buffer.alloc(FAILURE_BYTES);
    let status: number;
    try {
      // Asynchronous on purpose: the join waits for a notifier that may be
      // blocked until this thread runs its readiness callback.
      status = await asyncStatus(this.api.shutdown, [handle, failure]);
    } catch (cause) {
      throw new ReactorError({
        code: "Shutdown",
        message: "native WebRTC owner join could not execute; handle retained",
        context: { detail: cause },
      });
    }
    if (status !== STATUS_OK) {
      const error = failureOf(status, failure, "shutdown");
      throw new ReactorError({ code: "Shutdown", message: error.message, context: error.context });
    }
    this.api.destroy(handle);
    this.handle = undefined;
    // The notifier thread is joined: nothing can invoke the callback again.
    this.api.unregister(this.notify);
  }
}

export const encodeNativeJson = (value: unknown): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(new TextEncoder().encode(JSON.stringify(value)));
export const encodeNativeText = (value: string): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(new TextEncoder().encode(value));
