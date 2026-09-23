import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Crypto from "effect/Crypto";
import type * as Scope from "effect/Scope";
import type { AcquisitionFailure, ReactorError } from "../errors.js";
import { make as orchestrate } from "../orchestration/renewal.js";
import { handleContext } from "../orchestration/types.js";
import type { Engine, Handle, HandleShape, Media } from "../orchestration/types.js";
import { source } from "./_internal/source.js";
import type { SimOptions } from "./types.js";

export { source } from "./_internal/source.js";
export type { SimOptions, SimulatedFaults, SimulatedMediaSink } from "./types.js";

/** Unpaid production scheduling with the same explicit orchestration policy. */
export const make = (
  options: SimOptions = {},
): Effect.Effect<HandleShape, ReactorError | AcquisitionFailure, Scope.Scope | Crypto.Crypto> =>
  orchestrate({
    open: source(options).pipe(Effect.map((value) => ({ source: value, lifetime: "Infinity" }))),
  });

export const layerSim = (
  options: SimOptions = {},
): Layer.Layer<Engine | Media | Handle, ReactorError | AcquisitionFailure, Crypto.Crypto> =>
  Layer.effectContext(Effect.map(make(options), handleContext));
