import { assert, describe, it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import * as Spend from "../Spend.js";

const rate = { creditsPerSecond: 125, creditsPerDollar: 10_000 };
const refused = <A>(effect: Effect.Effect<A, Spend.Refused>) =>
  Effect.map(Effect.exit(effect), (exit) => {
    assert.isTrue(Exit.isFailure(exit));
  });

describe("the spending gates", () => {
  it.effect(
    "a budget above its check's ceiling, above the ledger's, or above the total refuses",
    () =>
      Effect.gen(function* () {
        yield* refused(Spend.authorize({ check: "vertical", budgetUsd: 0.76, totalUsd: 1.5 }));
        yield* refused(Spend.authorize({ check: "vertical", budgetUsd: 0.75, totalUsd: 3.76 }));
        yield* refused(Spend.authorize({ check: "renewal", budgetUsd: 1.5, totalUsd: 0.75 }));
        yield* refused(Spend.authorize({ check: "vertical", budgetUsd: Number.NaN, totalUsd: 1 }));
        const renewal = yield* Spend.authorize({
          check: "renewal",
          budgetUsd: 1.5,
          totalUsd: 3.75,
        });
        assert.strictEqual(renewal.budgetUsd, 1.5);
      }),
  );

  it.effect("a capped session reserves a whole billed minute, and five fit the total", () =>
    Effect.gen(function* () {
      const authorization = { check: "vertical", budgetUsd: 0.75, totalUsd: 3.75 } as const;
      assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 3 }), 0.75);
      yield* refused(Spend.admit({ rate, authorization, reservedUsd: 3.0001 }));
      // At a higher published rate the same budget no longer covers the minute.
      yield* refused(
        Spend.admit({
          rate: { ...rate, creditsPerSecond: 126 },
          authorization,
          reservedUsd: 0,
        }),
      );
      assert.strictEqual(
        yield* Spend.admit({
          rate,
          authorization: { check: "renewal", budgetUsd: 1.5, totalUsd: 1.5 },
          reservedUsd: 0,
        }),
        1.5,
      );
    }),
  );

  it("a reservation rounds up to four decimals, and a started minute bills whole", () => {
    assert.strictEqual(Spend.reservationUsd(0.7500000000000001), 0.75);
    assert.strictEqual(Spend.reservationUsd(0.75001), 0.7501);
    assert.strictEqual(Spend.billedUsd({ rate, seconds: 61 }), 1.5);
    assert.strictEqual(Spend.billedUsd({ rate, seconds: 0 }), 0);
  });

  it.effect(
    "a token granting more than one capped session refuses; renewal needs the full grant",
    () =>
      Effect.gen(function* () {
        yield* refused(
          Spend.acceptGrant({
            check: "vertical",
            granted: { maxSessions: 1, maxSessionSeconds: 51 },
          }),
        );
        yield* refused(
          Spend.acceptGrant({
            check: "renewal",
            granted: { maxSessions: 1, maxSessionSeconds: 40 },
          }),
        );
        yield* Spend.acceptGrant({
          check: "vertical",
          granted: { maxSessions: 1, maxSessionSeconds: 40 },
        });
      }),
  );

  it.effect("turn refuses once an earlier paid run selected a relay pair", () =>
    Effect.gen(function* () {
      yield* refused(Spend.admitRelay(["host", "relay"]));
      yield* Spend.admitRelay(["host", "srflx"]);
    }),
  );
});
