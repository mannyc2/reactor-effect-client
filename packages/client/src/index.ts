/** Portable entry point: importing it does not select or load a native/browser peer. */
export { Client, make, layer, AcquisitionFailure, CommandFailure } from "./session/index.js";
export type {
  Factory,
  Configuration,
  CreateOptions,
  AttachOptions,
  Session,
} from "./session/index.js";
export * as Coordinator from "./coordinator/index.js";
export * as Peers from "./PeerFactory.js";
export * as FetchHttp from "./FetchHttp.js";
export {
  ReactorError,
  ErrorCode,
  FailureCode,
  Failure,
  Http,
  Remote,
  Native,
  IceFailed,
  TransportFailed,
  ClipEnded,
  ReactorErrorReason,
  isReactorFailure,
} from "./errors.js";
export type { ErrorContext, MessageCode, ReactorFailure, RemoteOutcome } from "./errors.js";
export { PeerFactory } from "./PeerFactory.js";
export { recorder } from "./session/recorder.js";
export type { Recorded } from "./session/recorder.js";
export type { PeerFactoryShape } from "./PeerFactory.js";
export type {
  SessionEvent,
  EventPayload,
  Snapshot,
  ReadyState,
  ReadyDescriptor,
  Status,
  Ownership,
  Observe,
  Observation,
  Attribution,
  CommandReply,
  CommandOptions,
  ReplyTimeoutOptions,
  SessionTimeouts,
  Uploaded,
  UploadProgress,
  UploadTimeoutOptions,
} from "./SessionTypes.js";
export { CloseReport } from "./SessionTypes.js";
export { Termination } from "./coordinator/_internal/client.js";
export type { HttpOptions } from "./coordinator/_internal/client.js";
export type { CommandContext } from "./session/commands.js";
export type { Correlation } from "./correlation.js";
export type { Statistics } from "./stats.js";
export type { Json, JsonObject } from "./json.js";
export { Capabilities, Mapping, SessionDescriptor, Track } from "./contract.js";
export type { Descriptor } from "./contract.js";
