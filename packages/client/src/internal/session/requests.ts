/**
 * Requests over the data and control channels. Each is registered with its
 * correlator before it is sent and runs on its generation's scope, past its
 * caller's wait, until its own outcome; a failure carries the dispatch
 * evidence it established. A notification is sent with no reply to wait for.
 */
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type { CommandContext } from "../../ReactorError.js";
import { CommandFailure, ReactorError, Remote } from "../../ReactorError.js";
import type { ClipReady, CommandOptions, UploadReference } from "../../Session.js";
import type * as Correlator from "../correlator.js";
import * as Deadline from "../deadline.js";
import * as Wire from "../wire.js";
import type { Generation } from "./generation.js";
import type { Connection, ControlPayload, Core, Link } from "./model.js";
import { timedOut, withoutKey } from "./model.js";

const UploadReference = Schema.Struct({
  uploadId: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  mimeType: Schema.NonEmptyString,
  size: Schema.BigInt.check(
    Schema.isGreaterThanOrEqualToBigInt(0n),
    Schema.isLessThanBigInt(1n << 63n),
  ),
});
const CommandInput = Schema.Struct({
  name: Schema.NonEmptyString,
  data: Wire.StructJson,
  uploads: Schema.ReadonlyMap(Schema.NonEmptyString, UploadReference).check(
    Schema.makeFilter((uploads) => uploads.size <= 128 || "at most 128 upload references"),
  ),
});

export const unexpected = (message: string) =>
  ReactorError.fromCode("UnexpectedReply", message, { outcome: "replied" });

/** How a request's execution ended, as its span records it: identity and outcome only. */
const outcomeOf = (exit: Exit.Exit<unknown, ReactorError>, submitted: boolean) => {
  if (Exit.isSuccess(exit)) return { "reactor.command.outcome": "replied" };
  const error = Exit.findError(exit);
  if (error._tag === "Success")
    return {
      "reactor.command.outcome": error.success.context.outcome,
      "error.type": error.success.reason._tag,
    };
  return { "reactor.command.outcome": submitted ? "unknown" : "not-submitted" };
};

/** A command's failure with the dispatch evidence it established. */
const commandFailure = (name: string) => (error: ReactorError) => {
  const { outcome, requestId, generation } = error.context;
  if (
    (outcome === "unknown" || outcome === "replied") &&
    requestId !== undefined &&
    generation !== undefined
  ) {
    const context: CommandContext = {
      ...error.context,
      operation: name,
      outcome,
      requestId,
      generation,
    };
    return CommandFailure.from(error, context);
  }
  return CommandFailure.from(error, {
    ...error.context,
    operation: name,
    outcome: "not-submitted",
  });
};

export const make = ({
  core,
  generation,
  apiUrl,
}: {
  readonly core: Core;
  readonly generation: Generation;
  /** Where a clip's relative playlist URL resolves. */
  readonly apiUrl: string;
}) => {
  const { settings, data, control } = core;
  const { current, currentReady, guard } = generation;

  const request = <A>(spec: {
    readonly c: Connection;
    readonly correlator: Correlator.Correlator<A>;
    readonly operation: string;
    readonly encode: (id: string) => Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError>;
    readonly channel: "control" | "data";
    readonly wait: Duration.Duration;
    /** The track a publication claim is for, until its reply names it. */
    readonly publication?: string | undefined;
  }): Effect.Effect<A, ReactorError> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const { c, correlator, operation, channel } = spec;
        yield* current(c);
        const pending = yield* correlator.register(c.generation, operation);
        const forget = Effect.all([
          correlator.cancel(pending),
          Ref.update(c.link, (link): Link => ({
            ...link,
            claims: withoutKey(pending.id)(link.claims),
          })),
        ]);
        const publication = spec.publication;
        if (publication !== undefined)
          yield* Ref.update(c.link, (link): Link => ({
            ...link,
            claims: new Map(link.claims).set(pending.id, publication),
          }));
        const encoded = yield* Effect.result(spec.encode(pending.id));
        if (encoded._tag === "Failure") {
          yield* forget;
          return yield* ReactorError.make({
            reason: encoded.failure.reason,
            context: {
              ...encoded.failure.context,
              operation,
              requestId: pending.id,
              generation: c.generation,
              outcome: "not-submitted",
            },
          });
        }
        const attribute = (error: ReactorError) =>
          correlator.isSubmitted(pending).pipe(
            Effect.map((submitted) =>
              ReactorError.make({
                reason: error.reason,
                context: {
                  ...error.context,
                  operation,
                  requestId: pending.id,
                  generation: c.generation,
                  outcome: error.context.outcome ?? (submitted ? "unknown" : "not-submitted"),
                },
              }),
            ),
          );
        const sending = current(c).pipe(
          // The record exists before the peer could reply, however fast.
          Effect.andThen(correlator.submitted(pending)),
          Effect.andThen(c.peer.send(channel, encoded.success)),
          Effect.catch((error) =>
            Effect.gen(function* () {
              if (!(yield* correlator.isPending(pending))) return;
              if (error.context.outcome === "not-submitted") yield* forget;
              yield* Deferred.fail(pending.deferred, yield* attribute(error));
            }),
          ),
        );
        const execution = Effect.gen(function* () {
          // The reply can precede the transport's acknowledgement of the send.
          yield* Effect.forkIn(sending, yield* Effect.scope, { startImmediately: true });
          return yield* Deferred.await(pending.deferred);
        }).pipe(
          Effect.scoped,
          Effect.interruptible,
          Effect.timeoutOrElse({ duration: spec.wait, orElse: timedOut(operation) }),
          Effect.catch((error) => Effect.flatMap(attribute(error), Effect.fail)),
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              // A submitted request stays attributable until a late reply or its
              // generation retires; its slot is not released by the deadline.
              const submitted = yield* correlator.isSubmitted(pending);
              if (!submitted) yield* forget;
              else if (Exit.isFailure(exit)) yield* correlator.abandon(pending);
              // The span covers the owned execution, which ends with the request's
              // own outcome even after its caller stops waiting.
              yield* Effect.annotateCurrentSpan(outcomeOf(exit, submitted));
            }),
          ),
          Effect.withSpan(
            channel === "data" ? "reactor.session.command" : "reactor.session.control",
            {
              kind: "client",
              attributes: {
                "reactor.operation": operation,
                "reactor.request.id": pending.id,
                "reactor.connection.generation": c.generation,
              },
            },
            { captureStackTrace: false },
          ),
        );
        const owner = yield* Effect.forkIn(execution, c.scope, { startImmediately: true });
        return yield* restore(Fiber.join(owner));
      }),
    );

  const command = Effect.fnUntraced(
    function* (name: string, input: unknown, options: CommandOptions = {}) {
      const { c } = yield* currentReady;
      const payload = yield* Schema.decodeUnknownEffect(CommandInput)({
        name,
        data: input,
        uploads: options.uploads ?? new Map<string, UploadReference>(),
      }).pipe(
        Effect.mapError((cause) =>
          ReactorError.fromCode("InvalidInput", "invalid command", {
            operation: name,
            outcome: "not-submitted",
            detail: cause,
          }),
        ),
      );
      const wait = yield* Deadline.decode("replyTimeout")(
        options.replyTimeout ?? settings.replyTimeout,
      );
      return yield* request({
        c,
        correlator: data,
        operation: name,
        encode: (id) =>
          Wire.encode(Wire.DataClientMessageSchema)({
            requestId: id,
            kind: Wire.MessageKind.REQUEST,
            payload: {
              case: "command",
              value: {
                type: payload.name,
                data: payload.data,
                uploads: Object.fromEntries(payload.uploads),
              },
            },
          }),
        channel: "data",
        wait,
      });
    },
    (effect, name) => Effect.mapError(effect, commandFailure(name)),
  );

  /** A control request on `expected`, or on the ready generation. */
  const controlRequest = Effect.fnUntraced(function* (
    operation: string,
    payload: ControlPayload,
    expected?: Connection,
  ) {
    const c = expected ?? (yield* currentReady).c;
    yield* current(c);
    return yield* request({
      c,
      correlator: control,
      operation,
      encode: (id) =>
        Wire.encode(Wire.ControlClientMessageSchema)({
          requestId: id,
          kind: Wire.MessageKind.REQUEST,
          payload,
        }),
      channel: "control",
      wait: settings.replyTimeout,
      publication: payload.case === "publishTrack" ? payload.value.name : undefined,
    });
  });

  const notification = (c: Connection, payload: ControlPayload) =>
    current(c).pipe(
      Effect.andThen(
        Wire.encode(Wire.ControlClientMessageSchema)({
          kind: Wire.MessageKind.NOTIFICATION,
          payload,
        }),
      ),
      Effect.flatMap((bytes) => guard(c, c.peer.send("control", bytes))),
      Effect.timeoutOrElse({
        duration: settings.replyTimeout,
        orElse: timedOut("control notification"),
      }),
    );

  /** A recording request: its clip, with a playlist URL resolved against the API. */
  const clip = (
    operation: string,
    payload: ControlPayload,
  ): Effect.Effect<ClipReady, ReactorError> =>
    controlRequest(operation, payload).pipe(
      Effect.flatMap((reply) => {
        if (reply._tag === "ClipFailed")
          return Effect.fail(
            ReactorError.make({
              reason: Remote.make({
                // The one classification of provider text: a clip failure carries
                // only a reason string, and a disabled recorder must be told apart.
                _tag: /recorder disabled|encoder crashed/i.test(Redacted.value(reply.reason))
                  ? "RecorderDisabled"
                  : "Remote",
                message: "clip failed",
                body: reply.reason,
              }),
              context: { outcome: "replied" },
            }),
          );
        if (reply._tag !== "ClipReady")
          return Effect.fail(unexpected(`clip reply was ${reply._tag}`));
        const playlist = URL.parse(reply.clip.playlistUrl, `${apiUrl}/`);
        return playlist === null
          ? Effect.fail(
              ReactorError.fromCode("Protocol", "clip playlist URL is malformed", {
                outcome: "replied",
              }),
            )
          : Effect.succeed({ ...reply.clip, playlistUrl: playlist.href });
      }),
    );

  const schema = controlRequest("request_schema", { case: "requestSchema", value: {} }).pipe(
    Effect.flatMap((reply) =>
      reply._tag === "ModelSchema"
        ? Effect.succeed(reply.openapi === undefined ? {} : { openapi: reply.openapi })
        : Effect.fail(unexpected(`schema reply was ${reply._tag}`)),
    ),
  );

  const requestRecordingClip = (seconds: number) =>
    Number.isFinite(seconds) && seconds > 0
      ? clip("request_clip", { case: "requestClip", value: { durationSeconds: seconds } })
      : Effect.fail(
          ReactorError.fromCode("InvalidInput", "clip duration must be finite and positive", {
            outcome: "not-submitted",
          }),
        );

  return {
    command,
    controlRequest,
    notification,
    schema,
    requestRecordingClip,
    recording: clip("request_recording", { case: "requestRecording", value: {} }),
  };
};

export type Requests = ReturnType<typeof make>;
