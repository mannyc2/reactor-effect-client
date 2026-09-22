import { Effect } from "effect";
import { TestClock } from "effect/testing";
import * as Renewal from "../../src/orchestration/renewal.js";
import { sourceFixture, until } from "./SourceFixture.js";
import type { SourceFixture, SourceScript } from "./SourceFixture.js";

export const renewalFixture = (
  script: (index: number) => SourceScript = () => ({}),
  options: Omit<Renewal.Options, "open" | "onRenewal"> = {},
) =>
  Effect.gen(function* () {
    const sources: SourceFixture[] = [],
      renewals: Renewal.Renewal[] = [];
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
          renewals.push(event);
        }),
    });
    const warm = TestClock.adjust(600).pipe(
      Effect.andThen(
        until(
          () => renewals.some((event) => event._tag === "Prepared"),
          TestClock.adjust(100),
          "replacement was never prepared",
        ),
      ),
    );
    return { handle, sources, renewals, warm };
  });
