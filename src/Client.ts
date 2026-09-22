import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import * as Http from "effect/unstable/http/HttpClient";
import { HttpClient } from "./http.js";
import type { HttpOptions } from "./http.js";
import { Session as SessionImplementation } from "./session.js";
import type { SessionOptions, Snapshot, SessionEvent, CommandReply, CloseReport, Uploaded, Observe } from "./SessionTypes.js";
import { errorOf, ReactorError } from "./errors.js";
import { PeerFactory } from "./PeerFactory.js";
import type { UploadReference, ClipReady } from "./wire.generated.js";
import type { Json, JsonObject } from "./json.js";

export interface Configuration extends Omit<HttpOptions, "apiUrl" | "credential"> {
  readonly apiUrl?: string;
  readonly credential?: Effect.Effect<Redacted.Redacted<string>, ReactorError>;
  readonly session?: Pick<SessionOptions,
    "commandTimeoutMs" | "connectTimeoutMs" | "readyTimeoutMs" | "heartbeatMs" | "maxPending" | "maxUploadBytes"
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

/** Portable session operations. Browser tracks and native frames are separate capabilities. */
export interface Session {
  readonly id: string;
  readonly ownership: "owned" | "attached";
  readonly connect: Effect.Effect<void, ReactorError>;
  readonly reconnect: Effect.Effect<void, ReactorError>;
  readonly current: Effect.Effect<Snapshot>;
  readonly events: (options?: { readonly capacity?: number; readonly maxBytes?: number }) => Stream.Stream<SessionEvent, ReactorError>;
  readonly observe: Observe;
  readonly command: (name: string, data: unknown, uploads?: ReadonlyMap<string, UploadReference>, timeoutMs?: number) => Effect.Effect<CommandReply, ReactorError>;
  readonly schema: Effect.Effect<{ readonly openapi?: JsonObject }, ReactorError>;
  readonly upload: (name: string, mimeType: string, bytes: Uint8Array, timeoutMs?: number) => Effect.Effect<Uploaded, ReactorError>;
  /** Request recent recorded output; this does not submit a generation command. */
  readonly requestRecordingClip: (seconds: number) => Effect.Effect<ClipReady, ReactorError>;
  readonly recording: Effect.Effect<ClipReady, ReactorError>;
  readonly close: Effect.Effect<CloseReport>;
}

export interface Factory {
  readonly create: (options: CreateOptions) => Effect.Effect<Session, ReactorError, Scope.Scope>;
  readonly attach: (options: AttachOptions) => Effect.Effect<Session, ReactorError, Scope.Scope>;
}

export class Client extends Context.Service<Client, Factory>()("reactor-effect-client/Client") {}

/** The layer shares configuration. Each create/attach acquires an independent scope-owned session. */
export const make = (configuration: Configuration = {}): Effect.Effect<Factory, never, Http.HttpClient | PeerFactory | Crypto.Crypto> =>
  Effect.gen(function* () {
    const http = yield* Http.HttpClient;
    const peers = yield* PeerFactory;
    const crypto = yield* Crypto.Crypto;

    const acquire = (input: CreateOptions | AttachOptions): Effect.Effect<Session, ReactorError, Scope.Scope> =>
      Effect.gen(function* () {
        yield* peers.check;
        const bytes = yield* crypto.randomBytes(16).pipe(Effect.mapError((cause) =>
          new ReactorError("InvalidState", "could not allocate a request identity", { outcome: "not-submitted", detail: cause })));
        const options = yield* Effect.try({
          try: (): SessionOptions => {
            if (input === null || typeof input !== "object") throw new ReactorError("InvalidInput", "session options must be an object", { outcome: "not-submitted" });
            if (input.jwt !== undefined && !Redacted.isRedacted(input.jwt)) throw new ReactorError("InvalidInput", "jwt must be Redacted", { outcome: "not-submitted" });
            const credential = input.jwt === undefined ? configuration.credential : Effect.succeed(input.jwt);
            return {
              ...configuration,
              ...configuration.session,
              apiUrl: configuration.apiUrl ?? "https://api.reactor.inc",
              credential: credential === undefined ? Effect.succeed(undefined) : Effect.map(credential, Redacted.value),
              requestNamespace: Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(""),
              ...("sessionId" in input ? {
                attach: { sessionId: input.sessionId, ...(input.connectionId === undefined ? {} : { connectionId: input.connectionId }) },
                // Attaching does not change remote model playback settings.
                autoResumeTracks: false,
              } : {
                model: { name: input.model, ...(input.version === undefined ? {} : { version: input.version }) },
                ...(input.extraArgs === undefined ? {} : { extraArgs: input.extraArgs }),
              }),
            };
          },
          catch: errorOf,
        });
        const implementation = yield* Effect.acquireRelease(
          Effect.try({ try: () => new SessionImplementation(options, peers.make, new HttpClient(options, http)), catch: errorOf }),
          (session) => session.close(),
        );
        const id = yield* implementation.allocate();
        return {
          id,
          ownership: options.attach === undefined ? "owned" : "attached",
          connect: implementation.start(),
          reconnect: implementation.reconnect(),
          current: Effect.sync(() => implementation.snapshot),
          events: (bounds) => implementation.events(bounds),
          observe: (bounds) => implementation.observe(bounds),
          command: (name, data, uploads, timeoutMs) => implementation.command(name, data, uploads, timeoutMs),
          schema: implementation.schema(),
          upload: (name, mimeType, data, timeoutMs) => implementation.upload(name, mimeType, data, timeoutMs),
          requestRecordingClip: (seconds) => implementation.requestClip(seconds),
          recording: implementation.recording(),
          close: implementation.close(),
        };
      });

    return { create: acquire, attach: acquire };
  });

export const layer = (configuration: Configuration = {}) => Layer.effect(Client, make(configuration));
