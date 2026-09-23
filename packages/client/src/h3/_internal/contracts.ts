import * as Schema from "effect/Schema";
import { isRecord } from "../../json.js";
import { Payloads } from "../messages.js";
import type { MessageType } from "../messages.js";

const NumberArgument = Schema.Number.check(Schema.isFinite());
const NoArguments = Schema.Record(Schema.String, Schema.Never);
const Upload = Schema.Struct({
  upload_id: Schema.String,
  name: Schema.String,
  mime_type: Schema.String,
  size: Schema.Int,
});

/**
 * One command-to-reply authority for deployment admission and client calls.
 * Required keys describe what this adapter always supplies, not what a server
 * must require. Nullable parameters describe the documented deployment shape;
 * the adapter's outgoing arguments omit null and retain its existing validators.
 */
export const Commands = {
  enqueue: {
    args: Schema.Struct({
      prompt: Schema.String,
      reference_images: Schema.NullOr(Schema.Array(Upload)),
      seconds: Schema.optionalKey(Schema.NullOr(NumberArgument)),
      seed: Schema.optionalKey(Schema.NullOr(Schema.Int)),
      position: Schema.optionalKey(Schema.NullOr(Schema.Int)),
      metadata: Schema.String,
      continue_from_clip_id: Schema.optionalKey(Schema.String),
    }),
    reply: "clip_queued",
  },
  move: {
    args: Schema.Struct({ clip_id: Schema.String, position: Schema.Int }),
    reply: "clip_moved",
  },
  pop: { args: Schema.Struct({ clip_id: Schema.String }), reply: "clip_popped" },
  play: { args: Schema.Struct({ clip_id: Schema.String }), reply: null },
  stop: { args: NoArguments, reply: null },
  set_seed: { args: Schema.Struct({ seed: Schema.Int }), reply: "seed_accepted" },
  set_clip_seconds: {
    args: Schema.Struct({ seconds: NumberArgument }),
    reply: "clip_length_accepted",
  },
  set_canvas: { args: Schema.Struct({ aspect: Schema.String }), reply: "canvas_accepted" },
  set_autoplay: { args: Schema.Struct({ enabled: Schema.Boolean }), reply: "autoplay_accepted" },
  set_flush_on_clip_end: {
    args: Schema.Struct({ enabled: Schema.Boolean }),
    reply: "flush_accepted",
  },
  get_queue: { args: NoArguments, reply: "queue_update" },
  get_state: { args: NoArguments, reply: "state_update" },
  reset: { args: NoArguments, reply: "session_reset" },
} as const satisfies Readonly<
  Record<string, { readonly args: Schema.Constraint; readonly reply: MessageType | null }>
>;

export type CommandName = keyof typeof Commands;
export type CommandArgs<K extends CommandName> = {
  readonly [F in keyof (typeof Commands)[K]["args"]["Type"]]: Exclude<
    (typeof Commands)[K]["args"]["Type"][F],
    null
  >;
};
export type ReplyCommand = {
  [K in CommandName]: (typeof Commands)[K]["reply"] extends MessageType ? K : never;
}[CommandName];
export type ReplyType<K extends ReplyCommand> = (typeof Commands)[K]["reply"];
export type ControlCommand = Exclude<CommandName, ReplyCommand>;

/** Only the structural subset already required by H3 deployment admission. */
export interface Shape {
  readonly type: "string" | "number" | "integer" | "boolean" | "object" | "array";
  readonly nullable?: boolean;
  readonly fields?: Readonly<Record<string, Shape>>;
  readonly required?: readonly string[];
  readonly items?: Shape;
}

const internalShapeError = (): never => {
  throw new Error("H3 contract schemas must have concrete inline JSON shapes");
};
const schemaObject = (value: unknown): Record<string, unknown> =>
  isRecord(value) ? value : internalShapeError();

/**
 * Project the pinned Effect schema's JSON representation once. This is not a
 * second payload decoder: numeric/string refinements still belong exclusively
 * to messages.ts. Optional observations do not become deployment requirements.
 */
const structuralShape = (schema: Schema.Constraint, includeOptional = false): Shape => {
  const document = Schema.toJsonSchemaDocument(schema);
  if (Object.keys(document.definitions).length !== 0) return internalShapeError();
  const visit = (input: unknown, allFields = false): Shape => {
    let node = schemaObject(input);
    let nullable = false;
    if (Array.isArray(node.anyOf)) {
      const nonNull = node.anyOf.filter((choice) => schemaObject(choice).type !== "null");
      if (nonNull.length !== 1 || nonNull.length === node.anyOf.length) return internalShapeError();
      nullable = true;
      node = schemaObject(nonNull[0]);
    }
    const type = node.type;
    if (
      type !== "string" &&
      type !== "number" &&
      type !== "integer" &&
      type !== "boolean" &&
      type !== "object" &&
      type !== "array"
    )
      return internalShapeError();
    const base: Shape = { type, ...(nullable ? { nullable } : {}) };
    if (type === "array") return { ...base, items: visit(node.items) };
    if (type !== "object") return base;
    const properties = schemaObject(node.properties ?? {});
    const required = node.required ?? [];
    if (!Array.isArray(required) || required.some((key) => typeof key !== "string"))
      return internalShapeError();
    return {
      ...base,
      required,
      fields: Object.fromEntries(
        Object.entries(properties)
          .filter(([name]) => allFields || required.includes(name))
          .map(([name, field]) => [name, visit(field)]),
      ),
    };
  };
  return visit(document.schema, includeOptional);
};

export const messageShapes: Readonly<Record<MessageType, Shape>> = Object.fromEntries(
  Object.entries(Payloads).map(([name, schema]) => [name, structuralShape(schema)]),
) as Readonly<Record<MessageType, Shape>>;

export const deploymentCommands = Object.entries(Commands).map(([name, command]) => {
  const shape = structuralShape(command.args, true);
  return {
    name,
    args: { ...shape, required: [] },
    supplied: shape.required ?? [],
    reply: command.reply,
  };
});
