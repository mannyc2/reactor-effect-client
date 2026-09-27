/**
 * The Reactor wire protocol: the generated protobuf messages and the Struct
 * conversion around them. It is published for this repository's own real-host
 * and hosted checks, which speak the protocol directly; an application uses
 * `Session` and `H3` instead.
 */
export * from "./internal/wire.generated.js";
export { objectFromStruct, structFromObject } from "./json.js";
