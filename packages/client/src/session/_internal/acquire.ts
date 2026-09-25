import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import type * as Http from "effect/unstable/http/HttpClient";
import { CoordinatorClient } from "../../coordinator/_internal/client.js";
import { AcquisitionFailure, parsed, ReactorError } from "../../errors.js";
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
  parsed(() => {
    const implementation = implementations.get(session);
    if (implementation === undefined)
      throw ReactorError.fromCode("InvalidInput", "session was not acquired by this client");
    return implementation;
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
    const adopt: unknown = input.options.adopt;
    if (adopt !== undefined && adopt !== true)
      throw ReactorError.fromCode("InvalidInput", "attach adopt must be true when given", {
        outcome: "not-submitted",
      });
    return {
      ...credential,
      intent: {
        _tag: "Attach",
        sessionId: nonempty(input.options.sessionId, "attach sessionId"),
        ...(input.options.connectionId === undefined
          ? {}
          : { connectionId: uint32(input.options.connectionId, "attach connectionId") }),
        ...(adopt === true ? { adopt } : {}),
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
        // A rejected option is invalid input; a bug in the checks stays a defect.
        const validated = yield* parsed(() => validate(input)).pipe(
          Effect.mapError((cause) =>
            ReactorError.fromCode("InvalidInput", "invalid session acquisition input", {
              detail: cause,
              outcome: "not-submitted",
            }),
          ),
        );
        // Only validated input reaches the span.
        yield* Effect.annotateCurrentSpan(
          validated.intent._tag === "Create"
            ? { "reactor.model.name": validated.intent.model.name }
            : {
                "reactor.session.id": validated.intent.sessionId,
                "reactor.session.adopt": validated.intent.adopt === true,
              },
        );
        if (peers.check !== undefined) yield* restore(peers.check);
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
            parsed(() => {
              acquired = new SessionImplementation(
                options,
                peers.make,
                new CoordinatorClient(options, http),
              );
              return acquired;
            }),
            (value) => value.close(),
          );
          const id = yield* implementation.allocate();
          const session: Session = {
            id,
            ownership:
              validated.intent._tag === "Create" || validated.intent.adopt === true
                ? "owned"
                : "attached",
            connect: implementation.start(),
            reconnect: implementation.reconnect(),
            current: Effect.sync(() => implementation.snapshot),
            ready: implementation.readyState(),
            events: (bounds) => implementation.events(bounds),
            observe: (bounds) => implementation.observe(bounds),
            command: (name, data, options) => implementation.command(name, data, options),
            schema: implementation.schema(),
            upload: (name, mimeType, data, options) =>
              implementation.upload(name, mimeType, data, options),
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

  // A caller-boundary span: the caller can cancel an acquisition, and a
  // connected one parents the connect span. It names the model or the attached
  // session, never a credential.
  const traced = (
    input: Input,
    connected: boolean,
  ): Effect.Effect<Session, AcquisitionFailure, Scope.Scope> =>
    acquire(input, connected).pipe(
      Effect.tap((session) => Effect.annotateCurrentSpan("reactor.session.id", session.id)),
      Effect.withSpan(
        input._tag === "Create" ? "reactor.session.create" : "reactor.session.attach",
        { kind: "client", attributes: { "reactor.connect": connected } },
        { captureStackTrace: false },
      ),
    );

  return {
    create: (options) => traced({ _tag: "Create", options }, false),
    attach: (options) => traced({ _tag: "Attach", options }, false),
    createConnected: (options) => traced({ _tag: "Create", options }, true),
    attachConnected: (options) => traced({ _tag: "Attach", options }, true),
  };
};
