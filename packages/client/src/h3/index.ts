/** Portable H3 Reference Turbo Realtime provider over a canonical, already connected Session. */
export { make } from "./_internal/client.js";
export { validateReference } from "./_internal/references.js";
export {
  modelName,
  documentedVersion,
  source,
  requestSeconds,
  canvases,
  referenceLimits,
} from "./profile.js";
export {
  h3ReferenceTurboRealtime,
  minSeconds,
  maxSeconds,
  alignFrames,
  alignSecondsTo,
  clampSecondsTo,
  isRequestableSeconds,
  estimateTokens,
  canvasSize,
} from "./profile.js";
export type { ModelProfile, CanvasAspect } from "./profile.js";
export { Clip, Queue, State } from "./messages.js";
export type { Message, MessageType, Payload, DecodedMessage } from "./messages.js";
export type {
  Reference,
  ValidatedReference,
  Request,
  Options,
  Aspect,
  Provider,
  ProviderSnapshot,
  ProviderEvent,
  ProviderObservation,
  ObservationOptions,
  Acceptance,
  PrepareHooks,
  Reply,
  ControlResult,
  Contract,
  Facts,
  ClipObservation,
  ClipOperation,
  ClipFact,
  ClipPhase,
  OperationFacts,
} from "./types.js";
