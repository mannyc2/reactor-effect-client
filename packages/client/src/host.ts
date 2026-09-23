/**
 * Host transport extension surface.
 *
 * reactor-effect-browser and reactor-effect-native implement the peer contract
 * and project a connected session's media generations through this module. It
 * is published for those first-party host packages, which pin this exact
 * version, and is not part of the application-facing contract: applications
 * compose hosts through their packages and read media through their `media`
 * functions. Nothing here allocates or owns a session.
 */
export { errorOf, positiveLimit } from "./errors.js";
export { finite, isRecord, record } from "./json.js";
export { fromOwnedReadableStream } from "./media-stream.js";
export { terminal } from "./contract.js";
export type { IceCandidate, IceServer } from "./contract.js";
export { Observations } from "./observation.js";
export type { ObservationOptions } from "./observation.js";
export { CoordinatorClient } from "./coordinator/_internal/client.js";
export { retryAfterMs } from "./coordinator/_internal/response.js";
export { mediaGeneration, trackGeneration } from "./session/_internal/acquire.js";
export type { Channel, MediaTrack, Peer, PeerEvent, PeerState, Prepared } from "./PeerTypes.js";
export type {
  AudioFrame,
  MediaGeneration,
  MediaPressure,
  RawMedia,
  TrackGeneration,
  VideoFrame,
} from "./session/media.js";
export { readFileBytes, uploadFile } from "./session/files.js";
export type { FileUploadOptions } from "./session/files.js";
