import { assert, describe, it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import * as Spend from "../Spend.js";

// H3's published rate: 125 credits a second, stated per second since September 2026.
const rate = { creditsPerSecond: 125, creditsPerDollar: 10_000, per: "second" } as const;
const perMinute = { ...rate, per: "minute" } as const;
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

  it.effect(
    "a capped session reserves its whole cap per second: three fit $2, a fourth does not",
    () =>
      Effect.gen(function* () {
        const authorization = { check: "vertical", budgetUsd: 0.75, totalUsd: 2 } as const;
        assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 0 }), 0.625);
        assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 1.25 }), 0.625);
        yield* refused(Spend.admit({ rate, authorization, reservedUsd: 1.875 }));
        assert.strictEqual(
          yield* Spend.admit({
            rate,
            authorization: { check: "renewal", budgetUsd: 1.5, totalUsd: 2 },
            reservedUsd: 0,
          }),
          1.25,
        );
      }),
  );

  it.effect("a rate stated per minute reserves a whole minute, and five fit the total", () =>
    Effect.gen(function* () {
      const authorization = { check: "vertical", budgetUsd: 0.75, totalUsd: 3.75 } as const;
      const minute = perMinute;
      assert.strictEqual(yield* Spend.admit({ rate: minute, authorization, reservedUsd: 3 }), 0.75);
      yield* refused(Spend.admit({ rate: minute, authorization, reservedUsd: 3.0001 }));
      // At a higher published rate the same budget no longer covers the minute.
      yield* refused(
        Spend.admit({
          rate: { ...minute, creditsPerSecond: 126 },
          authorization,
          reservedUsd: 0,
        }),
      );
    }),
  );

  it("a reservation rounds up to four decimals, and a started unit of the rate bills whole", () => {
    assert.strictEqual(Spend.reservationUsd(0.7500000000000001), 0.75);
    assert.strictEqual(Spend.reservationUsd(0.75001), 0.7501);
    assert.strictEqual(Spend.billedUsd({ rate: perMinute, seconds: 61 }), 1.5);
    assert.strictEqual(Spend.billedUsd({ rate, seconds: 61 }), 0.7625);
    assert.strictEqual(Spend.billedUsd({ rate, seconds: 0.2 }), 0.0125);
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

  it.effect("a grant is proven by Reactor's echo, else by the token's claims, or refused", () =>
    Effect.gen(function* () {
      const claims = yield* Schema.encodeEffect(
        Schema.StringFromBase64Url.pipe(Schema.decodeTo(Schema.fromJsonString(Schema.Unknown))),
      )({
        authorization_details: [
          { constraints: { max_sessions: 1, max_session_duration_seconds: 50 } },
        ],
      });
      const echoed = yield* Spend.provenGrant({
        jwt: "e30.e30.sig",
        granted: { maxSessions: 1, maxSessionSeconds: 50 },
      });
      const claimed = yield* Spend.provenGrant({ jwt: `e30.${claims}.sig` });
      assert.deepStrictEqual(
        [echoed, claimed],
        [
          { maxSessions: 1, maxSessionSeconds: 50 },
          { maxSessions: 1, maxSessionSeconds: 50 },
        ],
      );
      yield* refused(Spend.provenGrant({ jwt: "e30.e30.sig" }));
      yield* refused(
        Spend.provenGrant({
          jwt: "e30.e30.sig",
          granted: { maxSessions: 1, maxSessionSeconds: "unlimited" },
        }),
      );
    }),
  );

  it.effect("turn refuses once an earlier paid run selected a relay pair", () =>
    Effect.gen(function* () {
      yield* refused(Spend.admitRelay(["host", "relay"]));
      yield* Spend.admitRelay(["host", "srflx"]);
    }),
  );
});
