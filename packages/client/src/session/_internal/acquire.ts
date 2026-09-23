import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import type * as Http from "effect/unstable/http/HttpClient";
import { CoordinatorClient } from "../../coordinator/_internal/client.js";
import { AcquisitionFailure, errorOf, ReactorError } from "../../errors.js";
import { json, nonempty, uint32 } from "../../json.js";
import type { PeerFactoryShape } from "../../PeerFactory.js";
import { Session as SessionImplementation } from "../../session.js";
import type { MediaGeneration, TrackGeneration } from "../media.js";
import type { AcquisitionIntent, SessionOptions, CloseReport } from "../../SessionTypes.js";
import type { AttachOptions, Configuration, CreateOptions, Factory, Session } from "../index.js";

type Input =
  | { readonly _tag: "Create"; readonly options: CreateOptions }
  | { readonly _tag: "Attach"; readonly options: AttachOptions };

export { AcquisitionFailure };

/** The report of an acquisition that allocated and attached nothing. */
export const noAcquisition: CloseReport = Object.freeze({
  localClosed: true,
  allocation: "none",
  remote: Object.freeze({
    attempted: false,
    responseReceived: false,
    confirmed: false,
    evidence: null,
    deleteStatus: null,
    state: null,
  }),
  unpublishSubmitted: Object.freeze([]),
  unresolvedPublications: Object.freeze([]),
  localErrors: Object.freeze([]),
});

const implementations = new WeakMap<Session, SessionImplementation>();

/** Host projections get capabilities without exposing private lifecycle methods. */
export const implementationOf = (
  session: Session,
): Effect.Effect<SessionImplementation, ReactorError> =>
  Effect.try({
    try: () => {
      const implementation = implementations.get(session);
      if (implementation === undefined)
        throw ReactorError.fromCode("InvalidInput", "session was not acquired by this client");
      return implementation;
    },
    catch: errorOf,
  });

/** A host's decoded-media projection of the current negotiated generation. */
export const mediaGeneration = (session: Session): Effect.Effect<MediaGeneration, ReactorError> =>
  implementationOf(session).pipe(Effect.flatMap((value) => value.mediaGeneration()));

/** A host's track-lease projection of the current negotiated generation. */
export const trackGeneration = (session: Session): Effect.Effect<TrackGeneration, ReactorError> =>
  implementationOf(session).pipe(Effect.flatMap((value) => value.trackGeneration()));

const validate = (
  input: Input,
): { readonly intent: AcquisitionIntent; readonly jwt?: Redacted.Redacted<string> } => {
  const options = input.options;
  if (options === null || typeof options !== "object") {
    throw ReactorError.fromCode("InvalidInput", "session options must be an object", {
      outcome: "not-submitted",
    });
  }
  if (options.jwt !== undefined && !Redacted.isRedacted(options.jwt)) {
    throw ReactorError.fromCode("InvalidInput", "jwt must be Redacted", {
      outcome: "not-submitted",
    });
  }
  const credential = options.jwt === undefined ? {} : { jwt: options.jwt };
  if (input._tag === "Attach") {
    return {
      ...credential,
      intent: {
        _tag: "Attach",
        sessionId: nonempty(input.options.sessionId, "attach sessionId"),
        ...(input.options.connectionId === undefined
          ? {}
          : { connectionId: uint32(input.options.connectionId, "attach connectionId") }),
      },
    };
  }
  return {
    ...credential,
    intent: {
      _tag: "Create",
      model: {
        name: nonempty(input.options.model, "model name"),
        ...(input.options.version === undefined
          ? {}
          : { version: nonempty(input.options.version, "model version") }),
      },
      ...(input.options.extraArgs === undefined
        ? {}
        : { extraArgs: json(input.options.extraArgs) }),
    },
  };
};

/** One acquisition owner for portable, browser, and native factories. */
export const makeFactory = (
  configuration: Configuration,
  http: Http.HttpClient,
  peers: PeerFactoryShape,
  crypto: Crypto.Crypto,
): Factory => {
  const acquire = (
    input: Input,
    connected: boolean,
  ): Effect.Effect<Session, AcquisitionFailure, Scope.Scope> =>
    Effect.uninterruptibleMask((restore) => {
      let acquired: SessionImplementation | undefined;
      return Effect.gen(function* () {
        const validated = yield* Effect.try({
          try: () => validate(input),
          catch: (cause) =>
            ReactorError.fromCode("InvalidInput", "invalid session acquisition input", {
              detail: cause,
              outcome: "not-submitted",
            }),
        });
        yield* restore(peers.check);
        const bytes = yield* restore(crypto.randomBytes(16)).pipe(
          Effect.mapError((cause) =>
            ReactorError.fromCode("InvalidState", "could not allocate a request identity", {
              outcome: "not-submitted",
              detail: cause,
            }),
          ),
        );
        const credential =
          validated.jwt === undefined ? configuration.credential : Effect.succeed(validated.jwt);
        const options: SessionOptions = {
          ...configuration,
          ...configuration.session,
          apiUrl: configuration.apiUrl ?? "https://api.reactor.inc",
          credential:
            credential === undefined ? Effect.undefined : Effect.map(credential, Redacted.value),
          requestNamespace: Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
            "",
          ),
          intent: validated.intent,
        };

        // A failed allocation or connection closes this child scope immediately,
        // even when its caller catches the failure in a much longer-lived scope.
        const scope = yield* Scope.fork(yield* Effect.scope);
        const acquisition = Effect.gen(function* () {
          const implementation = yield* Effect.acquireRelease(
            Effect.try({
              try: () => {
                acquired = new SessionImplementation(
                  options,
                  peers.make,
                  new CoordinatorClient(options, http),
                );
                return acquired;
              },
              catch: errorOf,
            }),
            (value) => value.close(),
          );
          const id = yield* implementation.allocate();
          const session: Session = {
            id,
            ownership: validated.intent._tag === "Create" ? "owned" : "attached",
            connect: implementation.start(),
            reconnect: implementation.reconnect(),
            current: Effect.sync(() => implementation.snapshot),
            ready: implementation.readyState(),
            events: (bounds) => implementation.events(bounds),
            observe: (bounds) => implementation.observe(bounds),
            command: (name, data, uploads, timeoutMs) =>
              implementation.command(name, data, uploads, timeoutMs),
            schema: implementation.schema(),
            upload: (name, mimeType, data, timeoutMs) =>
              implementation.upload(name, mimeType, data, timeoutMs),
            requestRecordingClip: (seconds) => implementation.requestClip(seconds),
            recording: implementation.recording(),
            stats: implementation.stats(),
            close: implementation.close(),
          };
          implementations.set(session, implementation);
          if (connected) yield* session.connect;
          return session;
        }).pipe(Scope.provide(scope));

        return yield* restore(acquisition).pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
        );
      }).pipe(
        Effect.catch((error) => {
          const cleanup = acquired === undefined ? Effect.succeed(noAcquisition) : acquired.close();
          return cleanup.pipe(
            Effect.flatMap((report) => Effect.fail(AcquisitionFailure.from(error, report))),
          );
        }),
      );
    });

  return {
    create: (options) => acquire({ _tag: "Create", options }, false),
    attach: (options) => acquire({ _tag: "Attach", options }, false),
    createConnected: (options) => acquire({ _tag: "Create", options }, true),
    attachConnected: (options) => acquire({ _tag: "Attach", options }, true),
  };
};
