import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import * as Http from "effect/unstable/http/HttpClient";
import type { HttpOptions } from "../coordinator/_internal/client.js";
import { PeerFactory } from "../PeerFactory.js";
import type { ReactorError } from "../errors.js";
import type { Json, JsonObject } from "../json.js";
import type { Statistics } from "../stats.js";
import type { UploadReference, ClipReady } from "../wire.generated.js";
import type {
  SessionOptions,
  Snapshot,
  ReadyState,
  SessionEvent,
  CommandReply,
  CloseReport,
  Uploaded,
  Observe,
} from "../SessionTypes.js";
import type { CommandFailure } from "./commands.js";
import { makeFactory, type AcquisitionFailure } from "./_internal/acquire.js";

export interface Configuration extends Omit<HttpOptions, "apiUrl" | "credential"> {
  readonly apiUrl?: string;
  readonly credential?: Effect.Effect<Redacted.Redacted<string>, ReactorError>;
  readonly session?: Pick<
    SessionOptions,
    | "commandTimeoutMs"
    | "connectTimeoutMs"
    | "readyTimeoutMs"
    | "heartbeatMs"
    | "maxPending"
    | "maxUploadBytes"
  >;
}

export interface CreateOptions {
  readonly model: string;
  readonly version?: string;
  readonly jwt?: Redacted.Redacted<string>;
  readonly extraArgs?: Json;
}

export interface AttachOptions {
  readonly sessionId: string;
  readonly connectionId?: number;
  readonly jwt?: Redacted.Redacted<string>;
}

/** One lifecycle and command contract, independent of the selected host peer. */
export interface Session {
  readonly id: string;
  readonly ownership: "owned" | "attached";
  readonly connect: Effect.Effect<void, ReactorError>;
  readonly reconnect: Effect.Effect<void, ReactorError>;
  readonly current: Effect.Effect<Snapshot>;
  readonly ready: Effect.Effect<ReadyState, ReactorError>;
  readonly events: (options?: {
    readonly capacity?: number;
    readonly maxBytes?: number;
  }) => Stream.Stream<SessionEvent, ReactorError>;
  readonly observe: Observe;
  readonly command: (
    name: string,
    data: unknown,
    uploads?: ReadonlyMap<string, UploadReference>,
    timeoutMs?: number,
  ) => Effect.Effect<CommandReply, CommandFailure>;
  readonly schema: Effect.Effect<{ readonly openapi?: JsonObject }, ReactorError>;
  readonly upload: (
    name: string,
    mimeType: string,
    bytes: Uint8Array,
    timeoutMs?: number,
  ) => Effect.Effect<Uploaded, ReactorError>;
  readonly requestRecordingClip: (seconds: number) => Effect.Effect<ClipReady, ReactorError>;
  readonly recording: Effect.Effect<ClipReady, ReactorError>;
  readonly stats: Effect.Effect<Statistics, ReactorError>;
  readonly close: Effect.Effect<CloseReport>;
}

export interface Factory {
  /** Allocate without connecting so a supervisor can first register the owner. */
  readonly create: (
    options: CreateOptions,
  ) => Effect.Effect<Session, AcquisitionFailure, Scope.Scope>;
  /** Identify an existing session without acquiring its remote lifetime. */
  readonly attach: (
    options: AttachOptions,
  ) => Effect.Effect<Session, AcquisitionFailure, Scope.Scope>;
  /** Failure releases this partial acquisition before returning to the caller. */
  readonly createConnected: (
    options: CreateOptions,
  ) => Effect.Effect<Session, AcquisitionFailure, Scope.Scope>;
  readonly attachConnected: (
    options: AttachOptions,
  ) => Effect.Effect<Session, AcquisitionFailure, Scope.Scope>;
}

export class Client extends Context.Service<Client, Factory>()("reactor-effect-client/Client") {}

export const make = (
  configuration: Configuration = {},
): Effect.Effect<Factory, never, Http.HttpClient | PeerFactory | Crypto.Crypto> =>
  Effect.gen(function* () {
    return makeFactory(
      configuration,
      yield* Http.HttpClient,
      yield* PeerFactory,
      yield* Crypto.Crypto,
    );
  });

export const layer = (
  configuration: Configuration = {},
): Layer.Layer<Client, never, Http.HttpClient | PeerFactory | Crypto.Crypto> =>
  Layer.effect(Client, make(configuration));

export { CommandFailure } from "./commands.js";
export { AcquisitionFailure } from "./_internal/acquire.js";
export type { CommandContext } from "./commands.js";
export type {
  Snapshot,
  ReadyState,
  SessionEvent,
  CommandReply,
  CloseReport,
  Uploaded,
  UploadProgress,
  Observation,
} from "../SessionTypes.js";
export type { VideoFrame, AudioFrame, MediaPressure, MediaGeneration, RawMedia } from "./media.js";
