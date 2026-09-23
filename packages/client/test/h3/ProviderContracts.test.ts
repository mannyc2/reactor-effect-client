import { describe, expect, test } from "vitest";
import { Crypto, Effect, Result, Schema, Scope } from "effect";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { ReactorError } from "../../src/errors.js";
import * as H3 from "../../src/h3/index.js";
import { decodeMessage } from "../../src/h3/messages.js";
import { validateDeployment } from "../../src/h3/_internal/deployment.js";
import type { JsonObject } from "../../src/json.js";
import type { CommandFailure, CommandReply } from "../../src/session/index.js";
import { at, providerSchema } from "./ProviderSchema.js";
import { fixture, fixtureClip } from "./ProviderSession.js";
import type { Fixture, WireMessage } from "./ProviderSession.js";

const options: H3.Options = { replyTimeout: 100, setupTimeout: 1000, reconcileWindow: 20 };
const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Crypto.Crypto>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(NodeCrypto.layer))));

/** Independent provider examples; never generated from the adapter's contract table. */
const messages = (fake: Fixture) => {
  const clip = { ...fixtureClip() };
  return {
    clip_queued: { component: "ClipQueued", data: { clip } },
    clip_moved: { component: "ClipMoved", data: { clip, queue: "generation", position: 0 } },
    clip_popped: { component: "ClipPopped", data: { clip } },
    clip_generated: { component: "ClipGenerated", data: { clip } },
    clip_failed: { component: "ClipFailed", data: { clip, reason: "fixture failure" } },
    clip_started: { component: "ClipStarted", data: { clip } },
    clip_finished: { component: "ClipFinished", data: { clip, seconds_sent: 2.5 } },
    clip_stopped: { component: "ClipStopped", data: { clip, seconds_sent: 2.5 } },
    queue_update: { component: "QueueUpdate", data: fake.queue() },
    state_update: { component: "StateUpdate", data: fake.state() },
    command_error: { component: "CommandError", data: { command: "enqueue", reason: "full" } },
    seed_accepted: { component: "SeedAccepted", data: { seed: 7 } },
    clip_length_accepted: {
      component: "ClipLengthAccepted",
      data: { clip_seconds: 175 / 24, frames: 175 },
    },
    canvas_accepted: {
      component: "CanvasAccepted",
      data: { aspect: "1:1", width: 768, height: 768 },
    },
    autoplay_accepted: { component: "AutoplayAccepted", data: { enabled: false } },
    flush_accepted: { component: "FlushAccepted", data: { enabled: true } },
    session_reset: { component: "SessionReset", data: { cleared_clips: 0, was_playing: false } },
  } satisfies Record<H3.MessageType, { readonly component: string; readonly data: JsonObject }>;
};

/** The decode failure of a known message, with its retained SchemaError. */
const decodeFailure = (type: string, data: unknown): ReactorError => {
  try {
    decodeMessage(type, data);
  } catch (cause) {
    if (ReactorError.is(cause)) return cause;
    throw cause;
  }
  throw new Error(`${type} decoded`);
};

describe("H3 cross-field rules", () => {
  const fake = Effect.runSync(fixture());
  const clip = fixtureClip();
  for (const [name, changes, path] of [
    ["a nonpositive minimum", { clip_seconds_min: 0 }, '["clip_seconds_min"]'],
    ["a maximum below the minimum", { clip_seconds_max: 0.5 }, '["clip_seconds_max"]'],
    ["playback without a clip", { playing: true, playing_clip_id: null }, '["playing_clip_id"]'],
  ] as const)
    test(`state_update: ${name} fails at its path, naming no value`, () => {
      const error = decodeFailure("state_update", fake.state(changes));
      expect(error).toMatchObject({
        reason: { _tag: "Protocol" },
        message: "H3 state_update payload is malformed",
      });
      expect(Schema.isSchemaError(error.context.detail)).toBe(true);
      expect(String(error.context.detail)).toContain(path);
    });

  test("queue_update: a clip queued twice fails at the second position", () => {
    const error = decodeFailure("queue_update", {
      generation: [clip],
      playout: [clip],
      history: [],
    });
    expect(String(error.context.detail)).toContain('["playout"][0]["clip_id"]');
    expect(String(error.context.detail)).not.toContain(clip.clip_id);
    // A retained history entry may still describe a clip in playout.
    expect(
      decodeMessage("queue_update", { generation: [], playout: [clip], history: [clip] }),
    ).toMatchObject({ type: "queue_update" });
  });
});

describe("H3 deployment and decoder contracts", () => {
  const examples = messages(Effect.runSync(fixture()));
  for (const [name, example] of Object.entries(examples))
    test(`${name} requires the independent provider fields at both boundaries`, () => {
      const document = providerSchema();
      expect(validateDeployment(document).documentedVersion).toBe("0.5.5");
      const decoded = decodeMessage(name, example.data);
      expect<unknown>(decoded).toEqual({ type: name, data: example.data });
      expect(Object.isFrozen(decoded.data)).toBe(true);
      const required = at(document, "components", "schemas", example.component)
        .required as string[];
      for (const field of required) {
        const incomplete = providerSchema();
        at(incomplete, "components", "schemas", example.component).required = required.filter(
          (key) => key !== field,
        );
        expect(() => validateDeployment(incomplete)).toThrow(ReactorError);
        const body: Record<string, unknown> = { ...example.data };
        delete body[field];
        expect(() => decodeMessage(name, body)).toThrow(ReactorError);
      }
    });

  test("nested clip requirements stay shared while optional image and audio facts stay optional", () => {
    const document = providerSchema();
    const required = at(document, "components", "schemas", "ClipInfo").required as string[];
    const clip = { ...fixtureClip() };
    for (const field of required) {
      const incomplete = providerSchema();
      at(incomplete, "components", "schemas", "ClipInfo").required = required.filter(
        (key) => key !== field,
      );
      expect(() => validateDeployment(incomplete)).toThrow(ReactorError);
      const body: Record<string, unknown> = { ...clip };
      delete body[field];
      expect(() => decodeMessage("clip_queued", { clip: body })).toThrow(ReactorError);
    }
    const optional = [
      "has_reference_image",
      "reference_image_count",
      "has_reference_audio",
      "reference_audio_count",
    ];
    const minimal: Record<string, unknown> = { ...clip };
    for (const field of optional) {
      delete at(document, "components", "schemas", "ClipInfo", "properties")[field];
      delete minimal[field];
    }
    expect(validateDeployment(document).subset).toBe("prompt-and-images");
    expect<unknown>(decodeMessage("clip_queued", { clip: minimal })).toEqual({
      type: "clip_queued",
      data: { clip: minimal },
    });
    expect(() =>
      decodeMessage("clip_queued", { clip: { ...minimal, reference_audio_count: "wrong" } }),
    ).toThrow(ReactorError);
  });

  test("deployment structure does not claim that payload values already passed the decoder", () => {
    const document = providerSchema();
    // These loose declarations are compatible, while actual values must still satisfy H3 checks.
    at(document, "components", "schemas", "ClipInfo", "properties").seconds = { type: "integer" };
    expect(validateDeployment(document).documentedVersion).toBe("0.5.5");
    for (const clip of [
      { ...fixtureClip(), clip_id: "not-a-uuid" },
      { ...fixtureClip(), frames: 0 },
      { ...fixtureClip(), seed: -1 },
      { ...fixtureClip(), seconds: 0 },
    ])
      expect(() => decodeMessage("clip_queued", { clip })).toThrow(ReactorError);
    expect(() =>
      decodeMessage("clip_moved", { clip: { ...fixtureClip() }, queue: "history", position: 0 }),
    ).toThrow(ReactorError);
  });

  test("prompt-only and omitted optional arguments remain compatible with non-null declarations", () => {
    const document = providerSchema();
    const properties = at(
      document,
      "paths",
      "/events/enqueue",
      "post",
      "requestBody",
      "content",
      "application/json",
      "schema",
      "properties",
    );
    for (const field of ["reference_images", "seconds", "seed", "position"]) {
      const declaration = properties[field] as { anyOf: unknown[] };
      properties[field] = declaration.anyOf[0];
    }
    expect(validateDeployment(document).subset).toBe("prompt-and-images");
    properties.reference_images = {
      type: "array",
      maxItems: 8,
      items: { $ref: "#/components/schemas/ReactorUploadReference" },
    };
    expect(() => validateDeployment(document)).toThrow(ReactorError);
  });
});

interface NamedCase {
  readonly command: string;
  readonly args: JsonObject;
  readonly reply: WireMessage;
  readonly invoke: (
    provider: H3.Provider,
  ) => Effect.Effect<{ readonly value: unknown; readonly source: CommandReply }, CommandFailure>;
}
const namedCases = (fake: Fixture): readonly NamedCase[] => {
  const clip = { ...fixtureClip() },
    id = clip.clip_id;
  return [
    {
      command: "get_state",
      args: {},
      reply: { type: "state_update", data: fake.state() },
      invoke: (p) => p.getState,
    },
    {
      command: "get_queue",
      args: {},
      reply: { type: "queue_update", data: fake.queue() },
      invoke: (p) => p.getQueue,
    },
    {
      command: "pop",
      args: { clip_id: id },
      reply: { type: "clip_popped", data: { clip } },
      invoke: (p) => p.pop(id),
    },
    {
      command: "move",
      args: { clip_id: id, position: 2 },
      reply: { type: "clip_moved", data: { clip, queue: "generation", position: 2 } },
      invoke: (p) => p.move(id, 2),
    },
    {
      command: "set_seed",
      args: { seed: 7 },
      reply: { type: "seed_accepted", data: { seed: 7 } },
      invoke: (p) => p.setSeed(7),
    },
    {
      command: "set_clip_seconds",
      args: { seconds: 7 },
      reply: { type: "clip_length_accepted", data: { clip_seconds: 175 / 24, frames: 175 } },
      invoke: (p) => p.setClipSeconds(7),
    },
    {
      command: "set_canvas",
      args: { aspect: "1:1" },
      reply: { type: "canvas_accepted", data: { aspect: "1:1", width: 768, height: 768 } },
      invoke: (p) => p.setCanvas("1:1"),
    },
    {
      command: "set_autoplay",
      args: { enabled: true },
      reply: { type: "autoplay_accepted", data: { enabled: true } },
      invoke: (p) => p.setAutoplay(true),
    },
    {
      command: "set_flush_on_clip_end",
      args: { enabled: false },
      reply: { type: "flush_accepted", data: { enabled: false } },
      invoke: (p) => p.setFlushOnClipEnd(false),
    },
    {
      command: "reset",
      args: {},
      reply: { type: "session_reset", data: { cleared_clips: 0, was_playing: false } },
      invoke: (p) => p.reset,
    },
  ];
};

describe("H3 command consumers use the admitted reply identity", () => {
  for (const command of namedCases(Effect.runSync(fixture())))
    test(`${command.command} keeps its arguments, named result, ACK and malformed outcomes`, () =>
      run(
        Effect.gen(function* () {
          for (const mode of ["body", "ack", "other", "malformed"] as const)
            yield* Effect.scoped(
              Effect.gen(function* () {
                let acquired = false;
                const fake = yield* fixture({
                  command: {
                    [command.command]: ({ defaults }) =>
                      !acquired
                        ? defaults
                        : Effect.succeed(
                            mode === "body"
                              ? command.reply
                              : mode === "ack"
                                ? undefined
                                : mode === "malformed"
                                  ? { type: command.reply.type, data: {} }
                                  : {
                                      type:
                                        command.reply.type === "autoplay_accepted"
                                          ? "flush_accepted"
                                          : "autoplay_accepted",
                                      data: { enabled: false },
                                    },
                          ),
                  },
                });
                const provider = yield* H3.make(fake.session, options);
                acquired = true;
                const result = yield* Effect.result(command.invoke(provider));
                expect(fake.calls).toHaveLength(3);
                expect(fake.calls[2]).toMatchObject({
                  command: command.command,
                  args: command.args,
                });
                if (mode === "body") {
                  expect(Result.isSuccess(result)).toBe(true);
                  if (Result.isSuccess(result)) {
                    expect(result.success.value).toEqual(command.reply.data);
                    expect(result.success.source).toBe(fake.returns[2]!);
                  }
                } else {
                  expect(Result.isFailure(result)).toBe(true);
                  if (Result.isFailure(result)) {
                    expect(result.failure.reason._tag).toBe(
                      mode === "malformed" ? "Protocol" : "UnexpectedReply",
                    );
                    expect(result.failure.context).toMatchObject({
                      operation: command.command,
                      outcome: "unknown",
                      requestId: fake.calls[2]!.requestId,
                      generation: 1n,
                    });
                  }
                }
                expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
              }),
            );
        }),
      ));

  for (const command of ["play", "stop"] as const)
    test(`${command} accepts ACK or observed messages and preserves malformed/unknown uncertainty`, () =>
      run(
        Effect.gen(function* () {
          for (const mode of ["ack", "body", "unknown", "malformed"] as const) {
            const fake = yield* fixture({
              command: {
                [command]: () =>
                  Effect.succeed(
                    mode === "ack"
                      ? undefined
                      : mode === "body"
                        ? { type: "seed_accepted", data: { seed: 7 } }
                        : mode === "unknown"
                          ? { type: "future_message", data: {} }
                          : { type: "seed_accepted", data: {} },
                  ),
              },
            });
            const provider = yield* H3.make(fake.session, options);
            const result = yield* Effect.result(
              command === "play" ? provider.play() : provider.stop,
            );
            expect(fake.calls[2]!.args).toEqual(command === "play" ? { clip_id: "" } : {});
            expect(fake.calls).toHaveLength(3);
            if (mode === "ack" || mode === "body") {
              expect(Result.isSuccess(result)).toBe(true);
              if (Result.isSuccess(result)) {
                expect(result.success._tag).toBe(mode === "ack" ? "Acknowledged" : "Reply");
                expect(result.success.source).toBe(fake.returns[2]!);
              }
            } else {
              expect(Result.isFailure(result)).toBe(true);
              if (Result.isFailure(result)) {
                expect(result.failure.context.outcome).toBe("unknown");
                expect(result.failure.reason._tag).toBe(
                  mode === "malformed" ? "Protocol" : "UnexpectedReply",
                );
              }
            }
          }
        }),
      ));
});

test("the shared profile's image MIME types are accepted while audio stays excluded", () =>
  run(
    Effect.gen(function* () {
      for (const mime_type of H3.h3ReferenceTurboRealtime.references.mimeTypes) {
        const reference = yield* H3.validateReference({
          _tag: "Uploaded",
          file: { upload_id: fixtureClip().clip_id, name: "image", mime_type, size: 1n },
        });
        expect<string>(reference.mimeType).toBe(mime_type);
      }
      const audio = yield* Effect.result(
        H3.validateReference({
          _tag: "Uploaded",
          file: {
            upload_id: fixtureClip().clip_id,
            name: "audio",
            mime_type: "audio/wav",
            size: 1n,
          },
        }),
      );
      expect(Result.isFailure(audio)).toBe(true);
      if (Result.isFailure(audio)) expect(audio.failure.context.outcome).toBe("not-submitted");
    }),
  ));
