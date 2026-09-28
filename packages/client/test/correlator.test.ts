/** Correlation of replies to the requests a session made. */
import { assert, it } from "@effect/vitest";
import { Effect } from "effect";
import * as Correlator from "../src/internal/correlator.js";

// A session fails a generation's requests before its next transport opens, so no session reaches
// this: the correlator alone keeps a reply on another generation from completing a live request.
it.effect("a reply on another generation never completes a pending request", () =>
  Effect.gen(function* () {
    const correlator = yield* Correlator.make<string>({ prefix: "data", limit: 4, namespace: "n" });
    const pending = yield* correlator.register(1n, "get_state");
    const correlation = yield* correlator.settle(pending.id, 2n, () => Effect.succeed("reply"));
    assert.strictEqual(correlation, "stale-generation");
    assert.isTrue(yield* correlator.isPending(pending));
  }),
);
