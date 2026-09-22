import { Schema, type Effect, type Redacted, type Stream } from "effect"

import { ReactorError } from "./errors.js"
export { ReactorError, ProviderFailure, ProviderFailure as NativeFailure } from "./errors.js"

/** Native diagnostics exclude peer SDP and credentials. */
export const diagnosticLogFilter = "off,reactor_webrtc=warn"

export const UploadReference = Schema.Struct({
  upload_id: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  mime_type: Schema.NonEmptyString,
  size: Schema.Number.check(Schema.isFinite(), Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
})
export type UploadReference = typeof UploadReference.Type

export interface Options {
  readonly modelName: string
  readonly jwt: Redacted.Redacted<string>
  readonly apiUrl?: string
  /** Override the installed library for an explicitly managed native deployment. */
  readonly nativeLibraryPath?: string
}

export type ControlEvent =
  | { readonly _tag: "Status"; readonly status: string }
  | { readonly _tag: "Error"; readonly error: Schema.Json }
  | { readonly _tag: "Message"; readonly message: Schema.Json }
  | { readonly _tag: "RuntimeMessage"; readonly message: Schema.Json }
  | { readonly _tag: "Track"; readonly name: string; readonly mid: string | undefined }
  | { readonly _tag: "Capabilities"; readonly capabilities: Schema.Json }
  | { readonly _tag: "Session"; readonly sessionId: string | undefined }

export interface VideoFrame {
  readonly _tag: "VideoFrame"
  readonly track: string
  readonly width: number
  readonly height: number
  /** Zero means the sender supplied no frame identity. */
  readonly frameId: bigint
  /** Sender-clock microseconds; zero means absent. Never compare with our wall clock. */
  readonly timestampMicros: bigint
  /** Owned BGRA bytes. Keeping a frame does not retain native memory. */
  readonly data: Uint8Array
  readonly metadata: Uint8Array
}

export interface AudioFrame {
  readonly _tag: "AudioFrame"
  readonly track: string
  readonly sampleRate: number
  readonly channels: number
  /** Owned, interleaved signed 16-bit PCM samples. */
  readonly samples: Int16Array
}

export interface Snapshot {
  readonly closed: boolean
  readonly queuedControl: number
  readonly queuedVideo: number
  readonly queuedAudio: number
  readonly queuedBytes: number
  readonly droppedVideo: bigint
  readonly droppedAudio: bigint
  readonly pendingRequests: number
  readonly deliveredVideo: bigint
  readonly deliveredAudio: bigint
}

export interface Client {
  readonly connect: (options?: { readonly sessionId?: string }) => Effect.Effect<void, ReactorError>
  /** Returns the model's correlated reply, or undefined for an acknowledgement without a reply. */
  readonly send: (command: string, args?: Schema.JsonObject, uploads?: Readonly<Record<string, UploadReference>>) => Effect.Effect<Schema.Json | undefined, ReactorError>
  /** Uploads a local file to this session for use in a model command. */
  readonly uploadFile: (path: string) => Effect.Effect<UploadReference, ReactorError>
  readonly reconnect: Effect.Effect<void, ReactorError>
  readonly disconnect: Effect.Effect<void, ReactorError>
  readonly stats: Effect.Effect<Schema.Json | undefined, ReactorError>
  /**
   * The live model's own OpenAPI document. This is the only authority on the
   * contract a running session actually implements; a fetched schema snapshot
   * can be newer or older than the deployment answering us.
   */
  readonly requestSchema: Effect.Effect<Schema.Json | undefined, ReactorError>
  readonly events: Stream.Stream<ControlEvent, ReactorError>
  readonly video: Stream.Stream<VideoFrame, ReactorError>
  readonly audio: Stream.Stream<AudioFrame, ReactorError>
  readonly snapshot: Effect.Effect<Snapshot, ReactorError>
  /** Closes the owned session and joins local transport cleanup. */
  readonly close: Effect.Effect<void, ReactorError>
}

/** A transport capability, with no Reactor session or command authority. */
export interface RawMedia {
  readonly video: (name: string) => Stream.Stream<VideoFrame, ReactorError>
  readonly audio: (name: string) => Stream.Stream<AudioFrame, ReactorError>
  readonly snapshot: Effect.Effect<Snapshot, ReactorError>
}
