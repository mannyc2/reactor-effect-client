/**
 * What a generation's peer reports, applied in order on the generation's
 * event fiber: its state and channels, gathered ICE, received tracks, and the
 * data and control messages that settle this client's requests or reach its
 * observers.
 */
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { PeerEvent } from "../../Peer.js";
import type { Correlation } from "../correlator.js";
import { ReactorError, Remote } from "../../ReactorError.js";
import type { CommandReply, ControlMessage } from "../../Session.js";
import { ClipReady } from "../../Session.js";
import * as Wire from "../wire.js";
import type { Generation } from "./generation.js";
import type { Ice } from "./ice.js";
import type { Connection, Core, Link, State } from "./model.js";
import { including, isClosing, withoutKey } from "./model.js";

type ControlPayload = Exclude<
  Wire.ControlServerMessage["payload"],
  { readonly case: undefined | "moderation" }
>;

/** A control message as the library reads it; provider text stays redacted. */
const controlMessage = (payload: ControlPayload): Effect.Effect<ControlMessage, ReactorError> => {
  switch (payload.case) {
    case "modelSchema": {
      const openapi = payload.value.openapi;
      return openapi === undefined
        ? Effect.succeed({ _tag: "ModelSchema" })
        : Effect.map(Wire.json(openapi), (json) => ({ _tag: "ModelSchema", openapi: json }));
    }
    case "clipReady":
      return Schema.decodeEffect(ClipReady)(payload.value).pipe(
        Effect.map((clip) => ({ _tag: "ClipReady", clip }) as const),
        Effect.mapError((cause) =>
          ReactorError.fromCode("Protocol", "invalid clip_ready", { detail: cause }),
        ),
      );
    case "clipFailed":
      return Effect.succeed({ _tag: "ClipFailed", reason: Redacted.make(payload.value.reason) });
    case "publishTrack":
      return Effect.succeed({ _tag: "TrackPublished", name: payload.value.name });
    case "error":
      return Effect.succeed({
        _tag: "Error",
        code: Redacted.make(payload.value.code),
        message: Redacted.make(payload.value.message),
      });
  }
};

const remoteError = (value: { readonly code: string; readonly message: string }) =>
  Remote.make({
    _tag: "Remote",
    message: "remote command error",
    remoteCode: Redacted.make(value.code),
    body: Redacted.make(value.message),
  });

export const make = ({
  core,
  generation,
  ice,
}: {
  readonly core: Core;
  readonly generation: Generation;
  readonly ice: Ice;
}) => {
  const { state, sequence, hub, data, control, publish } = core;
  const { fail, readyGate, failedConnection } = generation;

  const receiveData = Effect.fnUntraced(function* (c: Connection, bytes: Uint8Array) {
    const message = yield* Wire.decode(Wire.DataServerMessageSchema, bytes);
    const payload = message.payload;
    if (payload.case === "error") {
      const error = ReactorError.make({
        reason: remoteError(payload.value),
        context: {
          requestId: message.requestId,
          generation: c.generation,
          outcome: "replied",
          detail: Redacted.make(message),
        },
      });
      yield* data.settle(message.requestId, c.generation, (correlation) =>
        publish(
          { _tag: "CommandError", requestId: message.requestId, error, correlation },
          c.generation,
        ).pipe(Effect.andThen(Effect.fail(error))),
      );
      return;
    }
    const body =
      payload.case === undefined
        ? { kind: "ack" as const }
        : {
            kind: "message" as const,
            type: payload.value.type,
            ...(payload.value.data === undefined
              ? {}
              : { data: yield* Wire.json(payload.value.data) }),
          };
    yield* data.settle(
      message.requestId,
      c.generation,
      Effect.fnUntraced(function* (correlation: Correlation) {
        // The exact value that completes the request is the one observers see.
        const reply: CommandReply = {
          ...body,
          _tag: "Model",
          outcome: "replied",
          requestId: message.requestId,
          generation: c.generation,
          sequence: yield* Ref.updateAndGet(sequence, (value) => value + 1n),
          correlation,
        };
        yield* hub.publish(reply);
        return reply;
      }),
      body.kind === "ack" ? "acknowledged" : "replied",
    );
  });

  const receiveControl = Effect.fnUntraced(function* (c: Connection, bytes: Uint8Array) {
    const message = yield* Wire.decode(Wire.ControlServerMessageSchema, bytes);
    const payload = message.payload;
    // Unlike a data reply, a bodyless control message acknowledges nothing.
    if (payload.case === undefined)
      return yield* publish(
        {
          _tag: "Diagnostic",
          error: ReactorError.fromCode(
            "Protocol",
            "bodyless control response resolves no request",
            {
              requestId: message.requestId,
            },
          ),
        },
        c.generation,
      );
    // A moderation verdict answers no request. `terminate` means Reactor is ending the session.
    if (payload.case === "moderation") {
      const verdict = payload.value;
      if (verdict.action === "terminate")
        yield* SubscriptionRef.update(state, (session): State => ({ ...session, moderated: true }));
      return yield* publish(
        {
          _tag: "Moderation",
          action: verdict.action,
          categories: verdict.categories,
          ...(verdict.inputKind === "" ? {} : { inputKind: verdict.inputKind }),
          ...(verdict.command === "" ? {} : { command: verdict.command }),
          ...(message.requestId === "" ? {} : { requestId: message.requestId }),
        },
        c.generation,
      );
    }
    const claim = (yield* Ref.get(c.link)).claims.get(message.requestId);
    if (claim !== undefined) {
      if (payload.case === "publishTrack" && payload.value.name === claim)
        // Remote ownership is recorded even after the publisher stops waiting.
        yield* Ref.update(c.link, (link): Link => ({
          ...link,
          claimed: including(claim)(link.claimed),
          claims: withoutKey(message.requestId)(link.claims),
        }));
      else if (payload.case === "error")
        yield* Ref.update(c.link, (link): Link => ({
          ...link,
          claims: withoutKey(message.requestId)(link.claims),
        }));
      else
        return yield* fail(
          c,
          ReactorError.fromCode("UnexpectedReply", "publisher claim reply named another track", {
            operation: "publish_track",
            requestId: message.requestId,
            outcome: "unknown",
          }),
        );
    }
    const read = yield* Effect.exit(controlMessage(payload));
    const result =
      payload.case === "error"
        ? Effect.fail(
            ReactorError.make({
              reason: remoteError(payload.value),
              context: {
                requestId: message.requestId,
                outcome: "replied",
                detail: Redacted.make(message),
              },
            }),
          )
        : read;
    const correlation = yield* control.settle(message.requestId, c.generation, () => result);
    if (Exit.isFailure(read)) return yield* read;
    if (payload.case !== "error" || correlation !== "matched")
      yield* publish(
        { _tag: "Control", message: read.value, requestId: message.requestId, correlation },
        c.generation,
      );
  });

  /** Applies one of `c`'s events, unless `c` has failed or retired. */
  const apply = Effect.fnUntraced(function* (c: Connection, event: PeerEvent) {
    const link = yield* Ref.get(c.link);
    const session = yield* SubscriptionRef.get(state);
    if (link.failure !== undefined || session.connection !== c || isClosing(session.status)) return;
    switch (event.type) {
      case "state":
        if (event.state === "failed") return yield* fail(c, yield* failedConnection(c));
        if (event.state === "disconnected" || event.state === "closed")
          return yield* fail(
            c,
            ReactorError.fromCode("Disconnected", `peer state ${event.state}`, {
              generation: c.generation,
            }),
          );
        yield* Ref.update(c.link, (held): Link => ({
          ...held,
          peerConnected: event.state === "connected",
        }));
        return yield* readyGate(c);
      case "channel":
        if (!event.open)
          return yield* fail(
            c,
            ReactorError.fromCode("ChannelClosed", `${event.channel} channel closed`, {
              generation: c.generation,
              detail: { channel: event.channel },
            }),
          );
        yield* Ref.update(c.link, (held) =>
          event.channel === "control"
            ? { ...held, controlOpen: true }
            : { ...held, dataOpen: true },
        );
        return yield* readyGate(c);
      case "ice":
        return yield* ice
          .gathered(c, event.candidate)
          .pipe(Effect.catch((error) => fail(c, error)));
      case "decoded":
      case "track":
        yield* SubscriptionRef.update(state, (current): State => ({
          ...current,
          received: including(event.name)(current.received),
        }));
        return yield* publish(
          event.type === "decoded"
            ? { _tag: "Decoded", kind: event.kind, name: event.name, mid: event.mid }
            : { _tag: "Track", name: event.name, mid: event.mid },
          c.generation,
        );
      case "error":
        return yield* fail(c, event.error);
      case "message":
        return yield* (
          event.channel === "data" ? receiveData(c, event.bytes) : receiveControl(c, event.bytes)
        ).pipe(Effect.catch((error) => publish({ _tag: "Diagnostic", error }, c.generation)));
    }
  });

  return { apply };
};

export type Inbound = ReturnType<typeof make>;
