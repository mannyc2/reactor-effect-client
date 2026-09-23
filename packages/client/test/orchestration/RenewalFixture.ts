import { Effect } from "effect";
import { TestClock } from "effect/testing";
import * as Renewal from "../../src/orchestration/renewal.js";
import { sourceFixture } from "./SourceFixture.js";
import type { SourceFixture, SourceScript } from "./SourceFixture.js";
import { signals } from "./Signals.js";

export const renewalFixture = (
  script: (index: number) => SourceScript = () => ({}),
  options: Omit<Renewal.Options, "open" | "onRenewal"> = {},
) =>
  Effect.gen(function* () {
    const sources: SourceFixture[] = [];
    const recorded = signals<Renewal.Renewal>();
    const renewals = recorded.values;
    const handle = yield* Renewal.make({
      leadSeconds: 0.5,
      reconnectTimeoutMs: 50,
      ...options,
      open: Effect.gen(function* () {
        const entry = yield* sourceFixture(`source-${sources.length + 1}`, script(sources.length));
        sources.push(entry);
        return { source: entry.source, maxSeconds: 1 };
      }),
      onRenewal: (event) =>
        Effect.sync(() => {
          recorded.record(event);
        }),
    });
    const warm = TestClock.adjust(600).pipe(
      Effect.andThen(recorded.wait((event) => event._tag === "Prepared")),
    );
    return { handle, sources, renewals, warm, awaitRenewal: recorded.wait };
  });
