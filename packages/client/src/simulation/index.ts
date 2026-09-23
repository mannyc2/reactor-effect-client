import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Crypto from "effect/Crypto";
import type * as Scope from "effect/Scope";
import type { ReactorError } from "../errors.js";
import { make as orchestrate } from "../orchestration/renewal.js";
import { Engine, Handle, Media } from "../orchestration/types.js";
import type { HandleShape } from "../orchestration/types.js";
import { source } from "./_internal/source.js";
import type { SimOptions } from "./types.js";

export { source } from "./_internal/source.js";
export type { SimOptions, SimulatedFaults, SimulatedMediaSink } from "./types.js";

/** Unpaid production scheduling with the same explicit orchestration policy. */
export const make = (
  options: SimOptions = {},
): Effect.Effect<HandleShape, ReactorError, Scope.Scope | Crypto.Crypto> =>
  orchestrate({
    open: source(options).pipe(Effect.map((value) => ({ source: value, maxSeconds: Infinity }))),
  });

export const layerSim = (
  options: SimOptions = {},
): Layer.Layer<Engine | Media | Handle, ReactorError, Crypto.Crypto> =>
  Layer.effectContext(
    make(options).pipe(
      Effect.map((handle) =>
        Context.make(Engine, handle.engine).pipe(
          Context.add(Media, handle.media),
          Context.add(Handle, handle),
        ),
      ),
    ),
  );
