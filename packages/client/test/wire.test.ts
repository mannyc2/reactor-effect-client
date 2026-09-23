import { test, equal, throws, hex, unhex, assert } from "./harness.js";
import * as W from "../src/wire.generated.js";
import {
  array,
  record,
  string,
  structFromObject,
  objectFromStruct,
  json,
  jsonObject,
} from "../src/json.js";
import { Reader, Writer } from "../src/protobuf.js";
export const oracleOutputs: { readonly name: string; readonly hex: string }[] = [];
const transcode = (
  type: string,
  bytes: Uint8Array,
): { readonly decoded: unknown; readonly encoded: Uint8Array } => {
  switch (type) {
    case "reactor_wire.v1.DataClientMessage": {
      const decoded = W.DataClientMessage.decode(bytes);
      return { decoded, encoded: W.DataClientMessage.encode(decoded) };
    }
    case "reactor_wire.v1.DataServerMessage": {
      const decoded = W.DataServerMessage.decode(bytes);
      return { decoded, encoded: W.DataServerMessage.encode(decoded) };
    }
    case "reactor_wire.v1.ControlClientMessage": {
      const decoded = W.ControlClientMessage.decode(bytes);
      return { decoded, encoded: W.ControlClientMessage.encode(decoded) };
    }
    case "reactor_wire.v1.ControlServerMessage": {
      const decoded = W.ControlServerMessage.decode(bytes);
      return { decoded, encoded: W.ControlServerMessage.encode(decoded) };
    }
    case "reactor_wire.v1.UploadReference": {
      const decoded = W.UploadReference.decode(bytes);
      return { decoded, encoded: W.UploadReference.encode(decoded) };
    }
    case "reactor_wire.v1.ModelMessage": {
      const decoded = W.ModelMessage.decode(bytes);
      return { decoded, encoded: W.ModelMessage.encode(decoded) };
    }
    case "reactor_wire.v1.Command": {
      const decoded = W.Command.decode(bytes);
      return { decoded, encoded: W.Command.encode(decoded) };
    }
    case "reactor_wire.v1.RequestClip": {
      const decoded = W.RequestClip.decode(bytes);
      return { decoded, encoded: W.RequestClip.encode(decoded) };
    }
    case "google.protobuf.Struct": {
      const decoded = W.Google_Struct.decode(bytes);
      return { decoded, encoded: W.Google_Struct.encode(decoded) };
    }
    default:
      throw new Error(`unhandled independent fixture type ${type}`);
  }
};
export const registerOracle = (input: unknown): void => {
  for (const value of array(record(input).vectors, "oracle vectors")) {
    const v = record(value),
      name = string(v.name, "name"),
      type = string(v.type, "type"),
      bytes = unhex(string(v.hex, "hex"));
    test(`oracle: ${name}`, () => {
      const result = transcode(type, bytes);
      equal(result.decoded, v.semantic);
      oracleOutputs.push({ name, hex: hex(result.encoded) });
    });
  }
};
for (const [name, bytes] of [
  ["zero tag", "00"],
  ["invalid wire", "0e"],
  ["truncated tag varint", "80"],
  ["uint64 overflow", "80808080808080808002"],
  ["length overflow", "0affffffff7f"],
  ["invalid UTF8", "0a02c0af"],
  ["known wrong wire type (intentional strictness)", "0801"],
  ["unexpected end group", "1c"],
  ["mismatched group", "a306ac06"],
  ["unterminated group", "a306"],
] as const)
  test(`malformed wire: ${name}`, () =>
    throws(() => W.DataServerMessage.decode(unhex(bytes)), "Protocol"));
test("wire bounds: message bytes, depth and field count", () => {
  throws(() => W.DataServerMessage.decode(new Uint8Array(262_145)), "Protocol");
  let nested: unknown = { value: 1 };
  for (let i = 0; i < 12; i++) nested = { nested };
  const encoded = W.Google_Struct.encode(structFromObject(nested));
  throws(
    () => W.Google_Struct.decode(encoded, { bytes: 262_144, depth: 4, fields: 65_536 }),
    "Protocol",
  );
  throws(
    () => W.DataServerMessage.decode(unhex("100210021002"), { bytes: 64, depth: 4, fields: 2 }),
    "Protocol",
  );
  throws(() => W.ModelMessage.encode({ type: "x".repeat(262_145) }), "Protocol");
});
test("wire UTF8: reject lone UTF16 surrogate instead of silently replacing it", () =>
  throws(() => W.ModelMessage.encode({ type: "\ud800" }), "Protocol"));
test("wire int64: bounds reject wrapping", () => {
  for (const size of [1n << 63n, -(1n << 63n) - 1n])
    throws(
      () => W.UploadReference.encode({ upload_id: "u", name: "n", mime_type: "m", size }),
      "Protocol",
    );
});
test("wire unsigned varint covers all 64 bits", () => {
  const w = new Writer();
  w.varint(0xffffffffffffffffn);
  equal(new Reader(w.finish()).varint(), 0xffffffffffffffffn);
});
test("wire canonical minimal bodyless ack bytes", () =>
  equal(
    hex(W.DataServerMessage.encode({ request_id: "data_1", kind: 2 })),
    "0a06646174615f311002",
  ));
test("JSON command validation: top object, finite numbers, cycles, prototypes and accessors", () => {
  for (const input of [null, 1, "x", [], { x: Number.NaN }, new Date()])
    throws(() => structFromObject(input), "Protocol");
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  throws(() => json(cycle), "Protocol");
  throws(
    () =>
      json({
        get secret() {
          throw new Error("must not execute getter");
        },
      }),
    "Protocol",
  );
});
test("JSON object boundary retains owned frozen output and original validation bounds", () => {
  const input = { nested: { text: "before", values: [1, null, { enabled: true }] } };
  const copy = jsonObject(input);
  const nested = record(copy.nested);
  const values = array(nested.values, "nested.values");
  assert(copy !== input && nested !== input.nested && values !== input.nested.values);
  for (const value of [copy, nested, values, values[2]]) {
    assert(Object.isFrozen(value), "validated output must stay recursively frozen");
  }
  input.nested.text = "after";
  input.nested.values.push(2);
  equal(copy, { nested: { text: "before", values: [1, null, { enabled: true }] } });

  throws(() => jsonObject({ [Symbol("non-JSON key")]: 1 }), "Protocol");
  throws(() => jsonObject({ values: Array.from({ length: 65_536 }, () => 0) }), "Protocol");
  throws(() => jsonObject({ text: "x".repeat(4_194_305) }), "Protocol");
  let deep: unknown = {};
  for (let i = 0; i < 65; i++) deep = { child: deep };
  throws(() => jsonObject(deep), "Protocol");
});
test("Struct conversion preserves object/null/absence differences and resists prototype pollution", () => {
  const input: unknown = JSON.parse('{"__proto__":{"polluted":true},"x":null,"array":[{},[]]}');
  const back = objectFromStruct(structFromObject(input));
  equal(back, input);
  assert(Object.prototype.hasOwnProperty.call(back, "__proto__"));
  const empty = W.ModelMessage.decode(unhex("1200"));
  assert(empty.data !== undefined);
  equal(objectFromStruct(empty.data), {});
  assert(W.ModelMessage.decode(new Uint8Array()).data === undefined);
});
test("Struct helper source semantics: unset Value/nonfinite numbers normalize to JSON null only at conversion", () => {
  equal(
    objectFromStruct({
      fields: new Map([
        ["unset", {}],
        ["inf", { kind: { case: "number_value", value: Infinity } }],
      ]),
    }),
    { unset: null, inf: null },
  );
});
