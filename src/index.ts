/** Portable entry point: importing it does not select or load a native/browser peer. */
export { Client, make, layer } from "./Client.js";
export type { Factory, Configuration, CreateOptions, AttachOptions, Session } from "./Client.js";
export { ReactorError, ErrorCode, ErrorContext, ProviderFailure } from "./errors.js";
export type { RemoteOutcome } from "./errors.js";
export { PeerFactory } from "./PeerFactory.js";
export type { PeerFactoryShape } from "./PeerFactory.js";
export type { SessionOptions, SessionEvent, Snapshot, Status, CommandReply, CloseReport, Uploaded, UploadProgress } from "./SessionTypes.js";
export type { HttpOptions, Termination } from "./http.js";
export type { Statistics } from "./stats.js";
export type { Json, JsonObject } from "./json.js";
export type { Capabilities, Descriptor, Track, Mapping } from "./contract.js";
export { downloadClip, parsePlaylist } from "./recording.js";
export type { DownloadOptions, Segment } from "./recording.js";
