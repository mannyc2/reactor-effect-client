/**
 * The Reactor wire protocol: the protobuf-es code `bun run generate:wire`
 * builds from `wire/proto`, and the bounds messages are held to at the edge.
 *
 * protobuf-es converts every `google.protobuf.Struct` field to and from a
 * plain JSON object, so no other module converts Struct.
 */
import * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import * as Schema from "effect/Schema";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { DescMessage, JsonObject, MessageInitShape, MessageShape } from "@bufbuild/protobuf";
import { ReactorError } from "../ReactorError.js";

export * from "./proto/reactor_wire/v1/common_pb.js";
export * from "./proto/reactor_wire/v1/control_pb.js";
export * from "./proto/reactor_wire/v1/data_pb.js";
export * from "./proto/reactor_wire/v1/model_pb.js";
export * from "./proto/reactor_wire/v1/platform_pb.js";
export * from "./proto/reactor_wire/v1/track_pb.js";

/** The largest channel message: the SCTP limit both peers hold. */
export const maxMessageBytes = 262_144;

const tooLarge = `a wire message exceeds ${maxMessageBytes} bytes`;

/**
 * A received message, decoded. Remote bytes are untrusted: a message larger
 * than a channel can carry, or one protobuf-es refuses, fails as `Protocol`.
 *
 * The hand-written reader this replaced also budgeted nesting depth (64) and
 * field count (65,536). Depth guarded the recursive decoder's stack;
 * protobuf-es stops at 100 nested messages instead. The field budget capped
 * the objects one message could allocate; the byte bound caps them now, since
 * every field costs at least two bytes.
 */
export const decode: {
  <Desc extends DescMessage>(
    bytes: Uint8Array,
  ): (schema: Desc) => Effect.Effect<MessageShape<Desc>, ReactorError>;
  <Desc extends DescMessage>(
    schema: Desc,
    bytes: Uint8Array,
  ): Effect.Effect<MessageShape<Desc>, ReactorError>;
} = dual(
  2,
  <Desc extends DescMessage>(
    schema: Desc,
    bytes: Uint8Array,
  ): Effect.Effect<MessageShape<Desc>, ReactorError> =>
    bytes.byteLength > maxMessageBytes
      ? Effect.fail(ReactorError.fromCode("Protocol", tooLarge))
      : Effect.try({
          try: () => fromBinary(schema, bytes),
          catch: (cause) =>
            ReactorError.fromCode("Protocol", `malformed ${schema.typeName}`, { detail: cause }),
        }),
);

/** A message built from its fields and encoded, or `InvalidInput` if a channel cannot carry it. */
export const encode: {
  <Desc extends DescMessage>(
    init: MessageInitShape<Desc>,
  ): (schema: Desc) => Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError>;
  <Desc extends DescMessage>(
    schema: Desc,
    init: MessageInitShape<Desc>,
  ): Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError>;
} = dual(
  2,
  <Desc extends DescMessage>(
    schema: Desc,
    init: MessageInitShape<Desc>,
  ): Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError> =>
    Effect.try({
      try: () => toBinary(schema, create(schema, init)),
      catch: (cause) =>
        ReactorError.fromCode("InvalidInput", `unencodable ${schema.typeName}`, { detail: cause }),
    }).pipe(
      Effect.filterOrFail(
        (bytes) => bytes.byteLength <= maxMessageBytes,
        () => ReactorError.fromCode("InvalidInput", tooLarge),
      ),
    ),
);

/** The JSON a `google.protobuf.Struct` field holds, typed as protobuf-es takes it. */
export const StructJson = Schema.Record(Schema.String, Schema.MutableJson);

const isStructJson = Schema.is(StructJson);

/** A received Struct as JSON: protobuf-es passes through the non-finite numbers JSON cannot hold. */
export const json = (object: JsonObject): Effect.Effect<typeof StructJson.Type, ReactorError> =>
  isStructJson(object)
    ? Effect.succeed(object)
    : Effect.fail(ReactorError.fromCode("Protocol", "a Struct holds a number JSON cannot"));
