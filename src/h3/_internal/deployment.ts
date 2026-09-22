import { ReactorError } from "../../errors.js";
import { isRecord, jsonObject } from "../../json.js";
import { documentedVersion, modelName, source } from "../profile.js";
import type { Contract } from "../types.js";

type Kind = "string" | "number" | "integer" | "boolean" | "object" | "array";
interface Shape {
  readonly type: Kind;
  readonly nullable?: boolean;
  readonly fields?: Readonly<Record<string, Shape>>;
  readonly required?: readonly string[];
  readonly items?: Shape;
}
const text: Shape = { type: "string" },
  integer: Shape = { type: "integer" },
  number: Shape = { type: "number" },
  boolean: Shape = { type: "boolean" };
const object = (
  fields: Readonly<Record<string, Shape>>,
  required: readonly string[] = Object.keys(fields),
): Shape => ({ type: "object", fields, required });
const array = (items: Shape): Shape => ({ type: "array", items });
const optional = (shape: Shape): Shape => ({ ...shape, nullable: true });
const upload = object({ upload_id: text, name: text, mime_type: text, size: integer });
const clip = object({
  clip_id: text,
  prompt: text,
  metadata: text,
  frames: integer,
  seconds: number,
  seed: integer,
  ready: boolean,
});
const queue = object({ generation: array(clip), playout: array(clip), history: array(clip) });
const state = object({
  clip_seconds: number,
  clip_seconds_min: number,
  clip_seconds_max: number,
  seed: integer,
  autoplay: boolean,
  flush_on_clip_end: boolean,
  aspect: text,
  width: integer,
  height: integer,
  playing: boolean,
  playing_clip_id: optional(text),
  generation_queued: integer,
  generation_capacity: integer,
  playout_queued: integer,
  playout_capacity: integer,
  clips_played: integer,
  seconds_sent: number,
  valid_commands: array(text),
});
const replies: Readonly<Record<string, Shape>> = {
  clip_queued: object({ clip }),
  clip_moved: object({ clip, queue: text, position: integer }),
  clip_popped: object({ clip }),
  clip_generated: object({ clip }),
  clip_failed: object({ clip, reason: text }),
  clip_started: object({ clip }),
  clip_finished: object({ clip, seconds_sent: number }),
  clip_stopped: object({ clip, seconds_sent: number }),
  queue_update: queue,
  state_update: state,
  command_error: object({ command: text, reason: text }),
  seed_accepted: object({ seed: integer }),
  clip_length_accepted: object({ clip_seconds: number, frames: integer }),
  canvas_accepted: object({ aspect: text, width: integer, height: integer }),
  autoplay_accepted: object({ enabled: boolean }),
  flush_accepted: object({ enabled: boolean }),
  session_reset: object({ cleared_clips: integer, was_playing: boolean }),
};
const commands: Readonly<
  Record<
    string,
    { readonly args: Shape; readonly supplied: readonly string[]; readonly reply: string | null }
  >
> = {
  enqueue: {
    args: object(
      {
        prompt: text,
        reference_images: optional(array(upload)),
        seconds: optional(number),
        seed: optional(integer),
        position: optional(integer),
        metadata: text,
        continue_from_clip_id: text,
      },
      [],
    ),
    supplied: ["prompt", "reference_images", "metadata"],
    reply: "clip_queued",
  },
  move: {
    args: object({ clip_id: text, position: integer }, []),
    supplied: ["clip_id", "position"],
    reply: "clip_moved",
  },
  pop: { args: object({ clip_id: text }, []), supplied: ["clip_id"], reply: "clip_popped" },
  play: { args: object({ clip_id: text }, []), supplied: ["clip_id"], reply: null },
  stop: { args: object({}, []), supplied: [], reply: null },
  set_seed: { args: object({ seed: integer }, []), supplied: ["seed"], reply: "seed_accepted" },
  set_clip_seconds: {
    args: object({ seconds: number }, []),
    supplied: ["seconds"],
    reply: "clip_length_accepted",
  },
  set_canvas: {
    args: object({ aspect: text }, []),
    supplied: ["aspect"],
    reply: "canvas_accepted",
  },
  set_autoplay: {
    args: object({ enabled: boolean }, []),
    supplied: ["enabled"],
    reply: "autoplay_accepted",
  },
  set_flush_on_clip_end: {
    args: object({ enabled: boolean }, []),
    supplied: ["enabled"],
    reply: "flush_accepted",
  },
  get_queue: { args: object({}, []), supplied: [], reply: "queue_update" },
  get_state: { args: object({}, []), supplied: [], reply: "state_update" },
  reset: { args: object({}, []), supplied: [], reply: "session_reset" },
};

const incompatible = (location: string): never => {
  throw new ReactorError(
    "UnsupportedCapability",
    `Deployment does not structurally support H3 ${documentedVersion}: ${location}`,
    { operation: "H3 schema", outcome: "not-submitted" },
  );
};
const record = (input: unknown, path: string): Record<string, unknown> =>
  isRecord(input) ? input : incompatible(path);

/**
 * Matches the pinned reactor-runtime ModelSchema.to_openapi representation:
 * https://github.com/reactor-team/reactor-runtime/blob/8e98536c6daedca298700dc45cbb5fd9e3676f16/src/reactor_runtime/interface/model/schema.py
 * No network references are followed, and prose/name mentions cannot establish compatibility.
 */
export const validateDeployment = (input: unknown): Contract => {
  const doc = jsonObject(input);
  if (typeof doc.openapi !== "string" || !/^3\.(0|1)\./.test(doc.openapi))
    return incompatible("OpenAPI version");
  let checks = 0;
  const resolve = (input: unknown, location: string): Record<string, unknown> => {
    let value = record(input, location);
    const seen = new Set<string>();
    while (typeof value.$ref === "string") {
      const ref = value.$ref;
      if (!ref.startsWith("#/components/schemas/") || seen.has(ref) || seen.size >= 32)
        return incompatible(`${location} reference`);
      seen.add(ref);
      let current: unknown = doc;
      for (const part of ref.slice(2).split("/"))
        current = record(current, location)[part.replace(/~1/g, "/").replace(/~0/g, "~")];
      value = record(current, location);
    }
    return value;
  };
  const check = (input: unknown, expected: Shape, location: string, depth = 0): void => {
    if (++checks > 10000 || depth > 32) return incompatible("schema complexity bound");
    let actual = resolve(input, location);
    const choices = actual.anyOf ?? actual.oneOf;
    let nullable = actual.nullable === true;
    if (Array.isArray(choices)) {
      const nonNull = choices.filter((choice) => resolve(choice, location).type !== "null");
      nullable = nonNull.length !== choices.length;
      if (nonNull.length !== 1) return incompatible(`${location} union`);
      actual = resolve(nonNull[0], location);
    }
    const types = Array.isArray(actual.type) ? actual.type : [actual.type];
    nullable ||= types.includes("null");
    const nonNullTypes = types.filter((type) => type !== "null");
    if (
      nonNullTypes.length !== 1 ||
      !(
        nonNullTypes[0] === expected.type ||
        (location.startsWith("message ") &&
          expected.type === "number" &&
          nonNullTypes[0] === "integer")
      )
    )
      return incompatible(`${location} type`);
    // Nullable outputs must declare the null alternative; nullable request
    // parameters may also be a narrower non-null type because this client omits null.
    if (location.startsWith("message ") && expected.nullable === true && !nullable)
      return incompatible(`${location} nullability`);
    if (expected.nullable !== true && nullable) return incompatible(`${location} nullability`);
    if (expected.type === "array") {
      if (
        location === "command enqueue.reference_images" &&
        ((typeof actual.minItems === "number" && actual.minItems > 0) ||
          (typeof actual.maxItems === "number" && actual.maxItems < 9))
      )
        return incompatible("command enqueue.reference_images count bounds");
      if (expected.items !== undefined)
        check(actual.items, expected.items, `${location} items`, depth + 1);
    } else if (expected.type === "object") {
      const properties =
        actual.properties === undefined ? {} : record(actual.properties, `${location} properties`);
      const required = actual.required ?? [];
      if (!Array.isArray(required) || required.some((key) => typeof key !== "string"))
        return incompatible(`${location} required fields`);
      for (const name of expected.required ?? [])
        if (!required.includes(name)) return incompatible(`${location}.${name} required`);
      for (const [name, shape] of Object.entries(expected.fields ?? {}))
        check(properties[name], shape, `${location}.${name}`, depth + 1);
    }
  };
  const bodySchema = (input: unknown, location: string): unknown => {
    const body = record(input, location),
      content = record(body.content, location);
    return record(content["application/json"], location).schema;
  };
  const paths = record(doc.paths, "command paths"),
    webhooks = record(doc.webhooks, "message webhooks");
  const messageSchemas = new Map<string, unknown>();
  for (const [name, shape] of Object.entries(replies)) {
    const post = record(record(webhooks[name], `message ${name}`).post, `message ${name}`);
    if (post.operationId !== name) return incompatible(`message ${name} operationId`);
    const schema = bodySchema(post.requestBody, `message ${name}`);
    check(schema, shape, `message ${name}`);
    messageSchemas.set(name, schema);
  }
  for (const [name, command] of Object.entries(commands)) {
    const post = record(
      record(paths[`/events/${name}`], `command ${name}`).post,
      `command ${name}`,
    );
    if (post.operationId !== name) return incompatible(`command ${name} operationId`);
    const args = bodySchema(post.requestBody, `command ${name}`);
    check(args, command.args, `command ${name}`);
    const required = resolve(args, `command ${name}`).required ?? [];
    if (!Array.isArray(required) || required.some((key) => !command.supplied.includes(String(key))))
      return incompatible(`command ${name} unsupported required argument`);
    const responses = record(post.responses, `command ${name} responses`);
    if (command.reply === null) {
      const accepted = record(responses["202"], `command ${name} acceptance`);
      if (accepted.content !== undefined)
        return incompatible(`command ${name} bodyless acceptance`);
    } else {
      const schema = bodySchema(responses["200"], `command ${name} response`);
      check(schema, replies[command.reply]!, `message ${command.reply}`);
      const expected = record(messageSchemas.get(command.reply), `message ${command.reply}`),
        response = record(schema, `command ${name}`);
      if (typeof expected.$ref === "string" && response.$ref !== expected.$ref)
        return incompatible(`command ${name} response identity`);
    }
  }
  const info = isRecord(doc.info) ? doc.info : {};
  return Object.freeze({
    modelName,
    documentedVersion,
    source,
    subset: "prompt-and-images",
    deployment: Object.freeze({
      title: typeof info.title === "string" ? info.title : null,
      version: typeof info.version === "string" ? info.version : null,
    }),
  });
};
