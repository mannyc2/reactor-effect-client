import * as Effect from "effect/Effect";
import type { Session } from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";

/** The caller owns a connected session created with H3.modelName. */
export const enqueueClip = (session: Session, prompt: string) =>
  Effect.gen(function* () {
    const provider = yield* H3.make(session);
    // Prompt-only input is supported. No autoplay, flush, reset or reconnect is implicit.
    const acceptance = yield* provider.enqueue({ prompt, seconds: 5 });
    return { clip: acceptance.clip, evidence: acceptance.evidence };
  });
