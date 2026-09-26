import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { metadataMaxChars } from "../h3/profile.js";
import { ClipRequest, PolicyFailure, captureRequest } from "./request.js";

const keys = new WeakMap<ClipRequest, string>();
const ProviderEnvelope = Schema.Struct({
  reactor_effect_h3: Schema.Literal(1),
  namespace: Schema.NonEmptyString,
  submission: Schema.NonEmptyString,
  caller: Schema.String,
});
const SchedulerEnvelope = Schema.Struct({
  reactor_effect_scheduler: Schema.Literal(1),
  key: Schema.NonEmptyString,
  application: Schema.JsonObject,
});
const fillerPrefix = "\u0000reactor-effect-client:filler:";

export const isReservedSchedulerKey = (key: string): boolean => key.startsWith(fillerPrefix);
export const fillerKey = (index: number): string => `${fillerPrefix}${index}`;
export const fillerIndexFromKey = (key: string | undefined): number | undefined => {
  if (key === undefined || !isReservedSchedulerKey(key)) return undefined;
  const suffix = key.slice(fillerPrefix.length);
  if (!/^(0|[1-9][0-9]*)$/.test(suffix)) return undefined;
  const index = Number(suffix);
  return Number.isSafeInteger(index) ? index : undefined;
};

/** Give each scheduled item its own captured request, even when callers reuse one input. */
export const keyedRequest = (
  input: ClipRequest,
  key: string,
): Effect.Effect<ClipRequest, PolicyFailure> =>
  Effect.gen(function* () {
    if (typeof key !== "string" || key.length === 0)
      return yield* PolicyFailure.refuse("InvalidRequest", "Scheduler key must be nonempty");
    const captured = yield* captureRequest(input);
    const copy = yield* captureRequest(new ClipRequest({ ...captured }));
    keys.set(copy, key);
    return copy;
  });

export const schedulerKeyOf = (request: ClipRequest): string | undefined => keys.get(request);

/** The H3 adapter keeps its existing caller bytes for ordinary unscheduled clips. */
export const metadataForH3 = (request: ClipRequest): string => {
  const key = schedulerKeyOf(request);
  return key === undefined
    ? JSON.stringify(request.metadata)
    : JSON.stringify({ reactor_effect_scheduler: 1, key, application: request.metadata });
};

/** Recognize only this library's nested scheduler envelope on a provider clip. */
export const keyFromProviderMetadata = (metadata: string): string | undefined => {
  if (Array.from(metadata).length > metadataMaxChars) return undefined;
  try {
    const h3 = Schema.decodeUnknownOption(ProviderEnvelope, { onExcessProperty: "error" })(
      JSON.parse(metadata),
    );
    if (Option.isNone(h3)) return undefined;
    const caller = Schema.decodeUnknownOption(SchedulerEnvelope, { onExcessProperty: "error" })(
      JSON.parse(h3.value.caller),
    );
    return Option.isSome(caller) ? caller.value.key : undefined;
  } catch {
    return undefined;
  }
};
