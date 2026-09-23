import { ReactorError } from "../../errors.js";
import { isRecord, jsonObject } from "../../json.js";
import { documentedVersion, modelName, referenceLimits, source } from "../profile.js";
import type { Contract } from "../types.js";
import { deploymentCommands, messageShapes } from "./contracts.js";
import type { Shape } from "./contracts.js";

const incompatible = (location: string): never => {
  throw ReactorError.fromCode(
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
          (typeof actual.maxItems === "number" && actual.maxItems < referenceLimits.maxImages))
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
  for (const [name, shape] of Object.entries(messageShapes)) {
    const post = record(record(webhooks[name], `message ${name}`).post, `message ${name}`);
    if (post.operationId !== name) return incompatible(`message ${name} operationId`);
    const schema = bodySchema(post.requestBody, `message ${name}`);
    check(schema, shape, `message ${name}`);
    messageSchemas.set(name, schema);
  }
  for (const command of deploymentCommands) {
    const { name } = command;
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
      check(schema, messageShapes[command.reply], `message ${command.reply}`);
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
