/**
 * FastH3 over a connected Session: clips opened and closed by image frames or
 * retained clip IDs, with acceptance and lifecycle evidence from the model.
 * The provider observes a session it neither allocates nor closes. A lost
 * enqueue reply is never resent; later evidence can still establish its clip.
 */
import type * as Crypto from "effect/Crypto";
import type * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import * as Predicate from "effect/Predicate";
import type * as Scope from "effect/Scope";
import type { CommandFailure, ReactorError } from "./ReactorError.js";
import type { Session } from "./Session.js";
import type { Options } from "./H3.js";
import type * as Contract_ from "./internal/h3/commands.js";
import type * as Messages from "./internal/h3/messages.js";
import { payloadsFor } from "./internal/h3/messages.js";
import type * as Operations from "./internal/h3/operations.js";
import * as Provider_ from "./internal/h3/provider.js";
import type * as State_ from "./internal/h3/state.js";
import { Clip, fastH3 } from "./internal/fastH3/family.js";
import { documentedVersion, modelName } from "./internal/fastH3/profile.js";
import type { Request } from "./internal/fastH3/request.js";

export {
  canvases,
  documentedVersion,
  modelName,
  requestSeconds,
  source,
  tracks,
} from "./internal/fastH3/profile.js";
export type { CanvasAspect } from "./internal/fastH3/profile.js";
export { Request } from "./internal/fastH3/request.js";
export { Clip } from "./internal/fastH3/family.js";
export { State } from "./internal/h3/messages.js";
export type { Options } from "./H3.js";
export type { CommandName } from "./internal/h3/commands.js";
export type { ClipFact, ClipPhase } from "./internal/h3/operations.js";
export type { Submission, State as SubmissionState } from "./internal/h3/submission.js";

export const Queue = payloadsFor(Clip).queue_update;
export type Queue = Messages.Queue<Clip>;
export type MessageType = Messages.MessageType;
export type Payload<K extends MessageType> = Messages.Payload<K, Clip>;
export type Message<K extends MessageType = MessageType> = Messages.Message<K, Clip>;
export type DecodedMessage = Messages.DecodedMessage<Clip>;
export type Contract = Contract_.Contract<typeof modelName, typeof documentedVersion>;
export type Provider = Provider_.Provider<
  Request,
  Clip,
  typeof modelName,
  typeof documentedVersion
>;
export type Reply<K extends MessageType> = Provider_.Reply<K, Clip>;
export type ControlResult = Provider_.ControlResult<Clip>;
export type ProviderEvent = Provider_.ProviderEvent<Clip>;
export type ProviderObservation = Provider_.ProviderObservation<Clip>;
export type Acceptance = State_.Acceptance<Clip>;
export type ProviderSnapshot = State_.ProviderSnapshot<Clip>;
export type Facts = State_.Facts<Clip>;
export type ClipObservation = State_.ClipObservation<Clip>;
export type ClipOperation = Operations.ClipOperation<Clip>;
export type OperationFacts = Operations.OperationFacts<Clip>;
export type PrepareHooks<E = never> = Provider_.PrepareHooks<E, Clip>;

/** Observe an already connected session under FastH3's deployment contract. */
export const make: {
  (
    options?: Options,
  ): (
    session: Session,
  ) => Effect.Effect<Provider, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope>;
  (
    session: Session,
    options?: Options,
  ): Effect.Effect<Provider, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope>;
} = dual(
  (args) => Predicate.hasProperty(args[0], "command"),
  (session: Session, options?: Options) => Provider_.make("FastH3.make")(session, fastH3, options),
);
