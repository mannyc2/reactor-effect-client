/**
 * The Reactor wire protocol: the protobuf-es messages and the bounded codec
 * around them. It is published for this repository's own real-host and hosted
 * checks, which speak the protocol directly; an application uses `Session` and
 * `H3` instead.
 */
export * from "./internal/wire.js";
