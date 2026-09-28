/**
 * One native peer's handle and the C ABI calls on it. Media and transport
 * events never cross into JavaScript on their own: a native notifier thread
 * invokes the readiness callback on the JavaScript thread, and the host drains
 * each queue with synchronous takes, one copy per item into memory the
 * consumer then owns.
 */
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { Native, ReactorError } from "reactor-effect-client/ReactorError";
import type { ErrorContext, FailureCode } from "reactor-effect-client/ReactorError";
import { requireUsable } from "./library.js";
import type { AsyncNativeFunction, Library, OutLength, Retained } from "./library.js";

const CALL_BUFFER_BYTES = 4 * 1024 * 1024;
const FAILURE_BYTES = 1024;
const VIDEO_HEADER_BYTES = 48;
const AUDIO_HEADER_BYTES = 24;
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
const FAILURE_CODES: ReadonlyMap<number, FailureCode | "Native"> = new Map<
  number,
  FailureCode | "Native"
>([
  [STATUS_CLOSED, "Closed"],
  [-1, "InvalidInput"],
  [-2, "Native"],
  [-3, "Overflow"],
  [-4, "Protocol"],
  [-5, "SdpRejected"],
  [-6, "ChannelClosed"],
]);

/** A status outside the ABI's classes is an unclassified native failure. */
export const failureCode = (status: number): FailureCode | "Native" =>
  FAILURE_CODES.get(status) ?? "Native";

/**
 * A failure the native ABI returned with `status`, classified by its failure
 * class. libwebrtc's own text can contain peer SDP or signaling material, so it
 * is kept Redacted: in the `Native` reason for an unclassified failure, and in
 * `context.detail` beside the status for a classified one.
 */
export const nativeFailure = (failure: {
  readonly status: number;
  readonly backendText: string;
  readonly message: (code: FailureCode | "Native") => string;
  readonly context: Omit<ErrorContext, "detail">;
  readonly detail?: Readonly<Record<string, unknown>>;
}): ReactorError => {
  const { status, message, context, detail = {} } = failure;
  const code = failureCode(status);
  const backendMessage = Redacted.make(failure.backendText);
  return code === "Native"
    ? ReactorError.make({
        reason: Native.make({ message: message(code), status, backendMessage }),
        context:
          Object.keys(detail).length === 0
            ? context
            : { ...context, detail: Redacted.make(detail) },
      })
    : ReactorError.fromCode(code, message(code), {
        ...context,
        detail: { status, backendMessage, ...detail },
      });
};

export const NativeCall = {
  Prepare: 1,
  Answer: 2,
  Direction: 3,
  MaxBitrate: 4,
  Stats: 5,
  MediaSnapshot: 6,
} as const;
export type NativeCall = (typeof NativeCall)[keyof typeof NativeCall];

/** Readiness bits the native notifier passes to the host callback. */
export const Ready = { Events: 1, Video: 2, Audio: 4 } as const;

/** An event: its JSON header text and the bytes after it. */
export interface NativePacket {
  readonly header: string;
  readonly payload: Uint8Array<ArrayBuffer>;
}

export interface NativeVideo {
  readonly track: number;
  readonly width: number;
  readonly height: number;
  readonly frameId: bigint;
  readonly timestampMicros: bigint;
  readonly sequence: bigint;
  readonly data: Uint8Array<ArrayBuffer>;
  readonly metadata: Uint8Array<ArrayBuffer>;
}

export interface NativeAudio {
  readonly track: number;
  readonly sampleRate: number;
  readonly channels: number;
  readonly sequence: bigint;
  readonly samples: Int16Array<ArrayBuffer>;
}

/** A take: the item, `undefined` when the queue is empty, `null` once it is closed. */
export type Take<A> = A | undefined | null;

const protocol = (message: string): ReactorError => ReactorError.fromCode("Protocol", message);

const safeLength = (value: number | bigint | null | undefined, name: string): number => {
  const length = typeof value === "bigint" ? Number(value) : value;
  if (length === null || length === undefined || !Number.isSafeInteger(length) || length < 0)
    throw protocol(`native ${name} is outside the safe integer range`);
  return length;
};

const decoder = new TextDecoder();
const encoder = new TextEncoder();

export const encodeText = (value: string): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(encoder.encode(value));

const decodeReply = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json));

/**
 * One native peer handle. The class owns the take buffers, which it grows and
 * hands over one exact-size allocation per frame; every asynchronous operation
 * is an Effect over Koffi's callbacks.
 */
export class NativeBridge {
  private readonly api: Library["api"];
  private readonly notify: unknown;
  private handle: bigint | undefined;
  private closed = false;
  private inFlight = 0;
  private requestBytes = 0;
  private joinRequested = false;
  private joinStarted = false;
  private joinSucceeded = false;
  private readonly retainedJoin: Retained;
  private readonly videoHeader = new Uint8Array(VIDEO_HEADER_BYTES);
  private readonly audioHeader = new Uint8Array(AUDIO_HEADER_BYTES);
  private readonly eventLength: OutLength = [0];
  private event = new Uint8Array(EVENT_BUFFER_BYTES);
  private metadata = new Uint8Array(256);
  // The buffer the next take copies into. Frames keep their size, so each is
  // allocated once at the previous frame's size and handed to its consumer.
  private nextVideo = new Uint8Array(0);
  private nextAudio = new Int16Array(0);

  /** Make a handle on `library`; `onReady` runs on the JavaScript thread with readiness bits. */
  static make(
    library: Library,
    onReady: (ready: number) => void,
  ): Effect.Effect<NativeBridge, ReactorError> {
    return Effect.gen(function* () {
      yield* requireUsable(library);
      const joined = yield* Deferred.make<void, ReactorError>();
      return yield* Effect.try({
        try: () => new NativeBridge(library, onReady, joined),
        catch: (cause) =>
          ReactorError.is(cause)
            ? cause
            : ReactorError.fromCode("Native", "native WebRTC peer allocation failed", {
                outcome: "not-submitted",
                detail: cause,
              }),
      });
    });
  }

  private constructor(
    private readonly library: Library,
    onReady: (ready: number) => void,
    private readonly joined: Deferred.Deferred<void, ReactorError>,
  ) {
    this.api = library.api;
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
      throw ReactorError.fromCode("Native", "native WebRTC peer allocation failed", {
        outcome: "not-submitted",
      });
    this.handle = handle;
    this.retainedJoin = { handle };
  }

  /**
   * One asynchronous ABI call. Admission is counted until native code
   * completes the call, not until its waiter stops waiting: a waiter that is
   * interrupted leaves the call counted, so shutdown still waits for it.
   */
  private invoke(
    fn: AsyncNativeFunction,
    args: (handle: bigint) => readonly unknown[],
    bytes: number,
    operation: string,
  ): Effect.Effect<number, ReactorError> {
    return Effect.callback<number, ReactorError>((resume) => {
      const handle = this.handle;
      if (this.closed || handle === undefined) {
        resume(
          Effect.fail(
            ReactorError.fromCode("Closed", "native WebRTC peer is closed", {
              outcome: "not-submitted",
            }),
          ),
        );
        return;
      }
      if (
        this.inFlight >= MAX_IN_FLIGHT_CALLS ||
        this.requestBytes + bytes > MAX_IN_FLIGHT_REQUEST_BYTES
      ) {
        resume(
          Effect.fail(
            ReactorError.fromCode("Overflow", "native foreign-call admission bound exceeded", {
              outcome: "not-submitted",
            }),
          ),
        );
        return;
      }
      this.inFlight++;
      this.requestBytes += bytes;
      fn.async(...args(handle), (error: unknown, status: number) => {
        this.inFlight--;
        this.requestBytes -= bytes;
        this.startJoinWhenIdle();
        resume(
          error === null || error === undefined
            ? Effect.succeed(status)
            : Effect.fail(
                ReactorError.fromCode("Native", `native ${operation} completion failed`, {
                  operation,
                  outcome: "unknown",
                  detail: error,
                }),
              ),
        );
      });
    });
  }

  private failure(
    status: number,
    failure: Uint8Array,
    operation: string,
    detail: Readonly<Record<string, unknown>> = {},
  ): ReactorError {
    const view = new DataView(failure.buffer, failure.byteOffset, failure.byteLength);
    const length = Math.min(view.getUint32(0, true), failure.byteLength - 4);
    // A native status does not establish whether a side effect executed.
    return nativeFailure({
      status,
      backendText: decoder.decode(failure.subarray(4, 4 + length)),
      message: (code) => `native ${operation} failed (${code})`,
      context: { operation, outcome: "unknown" },
      detail,
    });
  }

  /** A call and its JSON reply. */
  call(
    operation: NativeCall,
    request: Uint8Array = new Uint8Array(),
  ): Effect.Effect<unknown, ReactorError> {
    const name = `call:${operation}`;
    if (request.byteLength > MAX_REQUEST_BYTES)
      return Effect.fail(
        ReactorError.fromCode("Overflow", "native request exceeds 1 MiB", {
          outcome: "not-submitted",
        }),
      );
    const response = new Uint8Array(CALL_BUFFER_BYTES);
    const responseLength: OutLength = [0];
    const failure = new Uint8Array(FAILURE_BYTES);
    // Native code reads the request when the call runs, which may be after the
    // caller has reused its buffer: it gets a copy of its own.
    const input = request.slice();
    return this.invoke(
      this.api.call,
      (handle) => [
        handle,
        operation,
        input,
        input.byteLength,
        response,
        response.byteLength,
        responseLength,
        failure,
      ],
      CALL_BUFFER_BYTES + request.byteLength,
      name,
    ).pipe(
      Effect.flatMap((status) => {
        if (status !== STATUS_OK) return Effect.fail(this.failure(status, failure, name));
        const length = responseLength[0];
        if (typeof length !== "number" && typeof length !== "bigint")
          return Effect.fail(protocol("native call omitted its response length"));
        if (Number(length) > response.byteLength)
          return Effect.fail(protocol("native call response exceeded its declared buffer"));
        return decodeReply(decoder.decode(response.subarray(0, Number(length)))).pipe(
          Effect.mapError((cause) =>
            ReactorError.fromCode("Protocol", "native call returned invalid JSON", {
              detail: cause,
            }),
          ),
        );
      }),
    );
  }

  send(channel: "control" | "data", bytes: Uint8Array): Effect.Effect<void, ReactorError> {
    if (bytes.byteLength > MAX_MESSAGE_BYTES)
      return Effect.fail(
        ReactorError.fromCode("Overflow", "native data channel message exceeds 262144 bytes", {
          outcome: "not-submitted",
        }),
      );
    const failure = new Uint8Array(FAILURE_BYTES);
    const operation = `send:${channel}`;
    // Native code reads the bytes when the send runs: it gets a copy of its own.
    const input = bytes.slice();
    return this.invoke(
      this.api.send,
      (handle) => [handle, channel === "control" ? 0 : 1, input, input.byteLength, failure],
      FAILURE_BYTES + bytes.byteLength,
      operation,
    ).pipe(
      Effect.flatMap((status) =>
        status === STATUS_OK
          ? Effect.void
          : Effect.fail(this.failure(status, failure, operation, { channel })),
      ),
    );
  }

  /** Synchronous and nonblocking; call from the readiness pump until empty. */
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
      const length = safeLength(this.eventLength[0], "event length");
      if (status === STATUS_OK) {
        if (length > this.event.byteLength)
          throw protocol("native event exceeded its declared buffer");
        const packet = this.event.subarray(0, length);
        if (packet.length < 4) throw protocol("native packet omitted its header length");
        const view = new DataView(packet.buffer, packet.byteOffset, packet.byteLength);
        const headerLength = view.getUint32(0, true);
        if (headerLength > packet.length - 4)
          throw protocol("native packet header length exceeds packet size");
        return {
          header: decoder.decode(packet.subarray(4, 4 + headerLength)),
          payload: packet.slice(4 + headerLength),
        };
      }
      if (status !== STATUS_BUFFER_TOO_SMALL)
        throw ReactorError.fromCode("Native", `native event take failed with status ${status}`);
      if (length <= this.event.byteLength || length > MAX_EVENT_BYTES)
        throw protocol(`native event of ${length} bytes exceeds the native queue bound`);
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
          sequence: header.getBigUint64(40, true),
          // A smaller frame than its predecessor leaves slack; never expose it.
          data: dataLength === data.byteLength ? data : data.slice(0, dataLength),
          metadata: this.metadata.slice(0, metadataLength),
        };
      }
      if (status !== STATUS_BUFFER_TOO_SMALL)
        throw ReactorError.fromCode("Native", `native video take failed with status ${status}`);
      if (dataLength + metadataLength > MAX_VIDEO_BYTES)
        throw protocol(
          `native video frame of ${dataLength + metadataLength} bytes exceeds the native queue bound`,
        );
      const growData = dataLength > this.nextVideo.byteLength,
        growMetadata = metadataLength > this.metadata.byteLength;
      if (!growData && !growMetadata) throw protocol("native video take refused a fitting frame");
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
          sequence: header.getBigUint64(16, true),
          samples: samples === pcm.length ? pcm : pcm.slice(0, samples),
        };
      }
      if (status !== STATUS_BUFFER_TOO_SMALL)
        throw ReactorError.fromCode("Native", `native audio take failed with status ${status}`);
      if (samples > MAX_AUDIO_SAMPLES)
        throw protocol(`native audio block of ${samples} samples exceeds the native queue bound`);
      if (samples <= this.nextAudio.length)
        throw protocol("native audio take refused a fitting block");
      this.nextAudio = new Int16Array(samples);
    }
  }

  /** Fence the handle at once: nothing more is admitted or taken. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    const handle = this.handle;
    if (handle !== undefined) this.api.close(handle);
  }

  /**
   * Close, wait until every admitted call has completed natively, then join
   * the native owner and destroy the handle. The join runs to completion on
   * native callbacks whether or not anyone still waits for it, and every call
   * awaits the same outcome.
   */
  get shutdown(): Effect.Effect<void, ReactorError> {
    return Effect.suspend(() => {
      this.close();
      if (!this.joinRequested) {
        this.joinRequested = true;
        if (this.handle === undefined) Deferred.doneUnsafe(this.joined, Effect.void);
        else this.startJoinWhenIdle();
      }
      return Deferred.await(this.joined);
    });
  }

  /**
   * The host stopped waiting for this handle's join. The join still owns the
   * handle, so no other peer is made on this library until it completes.
   */
  get retain(): Effect.Effect<void> {
    return Effect.sync(() => {
      if (!this.joinSucceeded) this.library.retained.add(this.retainedJoin);
    });
  }

  // A native call's completion or shutdown itself starts the join once no call
  // is in flight: the join cannot see work still queued in Koffi.
  private startJoinWhenIdle(): void {
    const handle = this.handle;
    if (!this.joinRequested || this.joinStarted || this.inFlight > 0 || handle === undefined)
      return;
    this.joinStarted = true;
    const failure = new Uint8Array(FAILURE_BYTES);
    // Asynchronous on purpose: the join waits for a notifier that may be
    // blocked until this thread runs its readiness callback.
    this.api.shutdown.async(handle, failure, (error: unknown, status: number) => {
      if (error !== null && error !== undefined) {
        Deferred.doneUnsafe(
          this.joined,
          Effect.fail(
            ReactorError.fromCode(
              "Shutdown",
              "native WebRTC owner join could not execute; handle retained",
              { detail: error },
            ),
          ),
        );
        return;
      }
      if (status !== STATUS_OK) {
        const cause = this.failure(status, failure, "shutdown");
        Deferred.doneUnsafe(
          this.joined,
          Effect.fail(ReactorError.fromCode("Shutdown", cause.message, cause.context)),
        );
        return;
      }
      this.api.destroy(handle);
      this.handle = undefined;
      // The notifier thread is joined: nothing can invoke the callback again.
      this.api.unregister(this.notify);
      this.joinSucceeded = true;
      this.library.retained.delete(this.retainedJoin);
      Deferred.doneUnsafe(this.joined, Effect.void);
    });
  }
}
