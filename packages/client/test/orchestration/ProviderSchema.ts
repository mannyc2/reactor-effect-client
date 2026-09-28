import type { JsonObject } from "../../src/json.js";

/**
 * Independent, hand-authored offline fixture. Payload fields follow the saved
 * September 22 H3 0.5.5 documentation; the OpenAPI layout follows pinned
 * reactor-runtime's ModelSchema.to_openapi. Nothing here is a live capture or
 * generated from the adapter's expected schema/decoder.
 */
export const providerSchema = (): JsonObject => {
  const str = { type: "string" },
    int = { type: "integer" },
    num = { type: "number" },
    bool = { type: "boolean" };
  const obj = (properties: JsonObject, required = Object.keys(properties)): JsonObject => ({
    type: "object",
    properties,
    required,
  });
  const list = (items: JsonObject): JsonObject => ({ type: "array", items });
  const ref = (name: string): JsonObject => ({ $ref: `#/components/schemas/${name}` });
  const optional = (schema: JsonObject): JsonObject => ({
    anyOf: [schema, { type: "null" }],
    default: null,
  });
  const clip = obj(
    {
      clip_id: { type: "string", format: "uuid" },
      prompt: str,
      metadata: str,
      frames: int,
      seconds: num,
      seed: int,
      ready: bool,
      has_reference_image: bool,
      reference_image_count: int,
      has_reference_audio: bool,
      reference_audio_count: int,
    },
    ["clip_id", "prompt", "metadata", "frames", "seconds", "seed", "ready"],
  );
  const components: JsonObject = {
    ReactorUploadReference: obj({
      upload_id: { type: "string", format: "uuid" },
      name: str,
      mime_type: str,
      size: int,
    }),
    ClipInfo: clip,
    ClipQueued: obj({ clip: ref("ClipInfo") }),
    ClipMoved: obj({ clip: ref("ClipInfo"), queue: str, position: int }),
    ClipPopped: obj({ clip: ref("ClipInfo") }),
    ClipGenerated: obj({ clip: ref("ClipInfo") }),
    ClipFailed: obj({ clip: ref("ClipInfo"), reason: str }),
    ClipStarted: obj({ clip: ref("ClipInfo") }),
    ClipFinished: obj({ clip: ref("ClipInfo"), seconds_sent: num }),
    ClipStopped: obj({ clip: ref("ClipInfo"), seconds_sent: num }),
    QueueUpdate: obj({
      generation: list(ref("ClipInfo")),
      playout: list(ref("ClipInfo")),
      history: list(ref("ClipInfo")),
    }),
    StateUpdate: obj({
      clip_seconds: num,
      clip_seconds_min: num,
      clip_seconds_max: num,
      seed: int,
      autoplay: bool,
      flush_on_clip_end: bool,
      aspect: str,
      width: int,
      height: int,
      playing: bool,
      playing_clip_id: { anyOf: [str, { type: "null" }] },
      generation_queued: int,
      generation_capacity: int,
      playout_queued: int,
      playout_capacity: int,
      clips_played: int,
      seconds_sent: num,
      valid_commands: list(str),
    }),
    CommandError: obj({ command: str, reason: str }),
    SeedAccepted: obj({ seed: int }),
    ClipLengthAccepted: obj({ clip_seconds: num, frames: int }),
    CanvasAccepted: obj({ aspect: str, width: int, height: int }),
    AutoplayAccepted: obj({ enabled: bool }),
    FlushAccepted: obj({ enabled: bool }),
    SessionReset: obj({ cleared_clips: int, was_playing: bool }),
  };
  const path = (name: string, properties: JsonObject, reply: string | null): JsonObject => ({
    post: {
      operationId: name,
      requestBody: {
        required: true,
        content: { "application/json": { schema: obj(properties, []) } },
      },
      responses:
        reply === null
          ? { "202": { description: "Command accepted" } }
          : {
              "200": {
                description: reply,
                content: { "application/json": { schema: ref(reply) } },
              },
            },
    },
  });
  const events = {
    clip_queued: "ClipQueued",
    clip_moved: "ClipMoved",
    clip_popped: "ClipPopped",
    clip_generated: "ClipGenerated",
    clip_failed: "ClipFailed",
    clip_started: "ClipStarted",
    clip_finished: "ClipFinished",
    clip_stopped: "ClipStopped",
    queue_update: "QueueUpdate",
    state_update: "StateUpdate",
    command_error: "CommandError",
    seed_accepted: "SeedAccepted",
    clip_length_accepted: "ClipLengthAccepted",
    canvas_accepted: "CanvasAccepted",
    autoplay_accepted: "AutoplayAccepted",
    flush_accepted: "FlushAccepted",
    session_reset: "SessionReset",
  };
  return {
    openapi: "3.1.0",
    info: { title: "H3 hand-authored offline fixture", version: "fixture-declaration" },
    "x-reactor": {
      tracks: [
        { name: "main_video", kind: "video", direction: "out" },
        { name: "main_audio", kind: "audio", direction: "out" },
      ],
    },
    paths: {
      "/events/enqueue": path(
        "enqueue",
        {
          prompt: { ...str, default: "" },
          reference_image: optional(ref("ReactorUploadReference")),
          reference_images: optional(list(ref("ReactorUploadReference"))),
          reference_audio: optional(ref("ReactorUploadReference")),
          reference_audios: optional(list(ref("ReactorUploadReference"))),
          seconds: optional(num),
          seed: optional(int),
          position: optional(int),
          metadata: { ...str, default: "" },
          continue_from_clip_id: { ...str, default: "" },
        },
        "ClipQueued",
      ),
      "/events/move": path("move", { clip_id: str, position: int }, "ClipMoved"),
      "/events/pop": path("pop", { clip_id: str }, "ClipPopped"),
      "/events/play": path("play", { clip_id: { ...str, default: "" } }, null),
      "/events/stop": path("stop", {}, null),
      "/events/set_seed": path("set_seed", { seed: int }, "SeedAccepted"),
      "/events/set_clip_seconds": path("set_clip_seconds", { seconds: num }, "ClipLengthAccepted"),
      "/events/set_canvas": path("set_canvas", { aspect: str }, "CanvasAccepted"),
      "/events/set_autoplay": path("set_autoplay", { enabled: bool }, "AutoplayAccepted"),
      "/events/set_flush_on_clip_end": path(
        "set_flush_on_clip_end",
        { enabled: bool },
        "FlushAccepted",
      ),
      "/events/get_state": path("get_state", {}, "StateUpdate"),
      "/events/get_queue": path("get_queue", {}, "QueueUpdate"),
      "/events/reset": path("reset", {}, "SessionReset"),
    },
    webhooks: Object.fromEntries(
      Object.entries(events).map(([name, component]) => [
        name,
        {
          post: {
            operationId: name,
            requestBody: {
              required: true,
              content: { "application/json": { schema: ref(component) } },
            },
          },
        },
      ]),
    ),
    components: { schemas: components },
  };
};

/** Tests mutate an independent document without trusting adapter-internal lookup helpers. */
export const at = (input: unknown, ...keys: readonly string[]): Record<string, unknown> => {
  let value = input;
  for (const key of keys) {
    if (value === null || typeof value !== "object" || Array.isArray(value))
      throw new Error(`Fixture path is not an object: ${key}`);
    value = (value as Record<string, unknown>)[key];
  }
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Fixture path is not an object");
  return value as Record<string, unknown>;
};
