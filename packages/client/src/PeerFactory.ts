import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { Peer } from "./PeerTypes.js";
import type { ReactorError } from "./errors.js";

export interface PeerFactoryShape {
  /** Validate host support before allocating a remote session. */
  readonly check: Effect.Effect<void, ReactorError>;
  /** A fresh transport for each connection generation. No Reactor orchestration. */
  readonly make: () => Peer;
}

export class PeerFactory extends Context.Service<PeerFactory, PeerFactoryShape>()(
  "reactor-effect-client/PeerFactory",
) {}

export type { Peer, PeerEvent, Prepared, Channel } from "./PeerTypes.js";
