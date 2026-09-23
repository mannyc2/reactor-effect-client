import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { Peer } from "./PeerTypes.js";
import type { ReactorError } from "./errors.js";

/**
 * A host's transport capability. A host resolves and validates what it needs
 * while its layer is built, so building the layer is the host preflight and
 * `make` never depends on another call having run first.
 */
export interface PeerFactoryShape {
  /** A fresh transport for each connection generation. No Reactor orchestration. */
  readonly make: () => Peer;
  /**
   * Fails when the host cannot create a peer right now, for example while a
   * native runtime still holds an owner that did not join. The session factory
   * runs it before every remote allocation, so a paid session is never
   * allocated for a peer that cannot be made. It must not change what `make`
   * does; a host with nothing to check omits it.
   */
  readonly check?: Effect.Effect<void, ReactorError> | undefined;
}

export class PeerFactory extends Context.Service<PeerFactory, PeerFactoryShape>()(
  "reactor-effect-client/PeerFactory",
) {}

export type { Peer, PeerEvent, Prepared, Channel } from "./PeerTypes.js";
