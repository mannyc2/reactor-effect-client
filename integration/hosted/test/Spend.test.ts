import { assert, describe, it } from "@effect/vitest";
import { Cause, Effect, Exit, Schema } from "effect";
import * as Spend from "../Spend.js";

// H3's published rate: 350 credits a second, stated per second, on September 30, 2026.
const rate = { creditsPerSecond: 350, creditsPerDollar: 10_000, per: "second" } as const;
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
        yield* refused(Spend.authorize({ check: "vertical", budgetUsd: 2.11, totalUsd: 3.5 }));
        yield* refused(Spend.authorize({ check: "vertical", budgetUsd: 2.1, totalUsd: 10.01 }));
        yield* refused(Spend.authorize({ check: "renewal", budgetUsd: 4.2, totalUsd: 2.1 }));
        yield* refused(Spend.authorize({ check: "vertical", budgetUsd: Number.NaN, totalUsd: 1 }));
        const renewal = yield* Spend.authorize({
          check: "renewal",
          budgetUsd: 4.2,
          totalUsd: 10,
        });
        assert.strictEqual(renewal.budgetUsd, 4.2);
      }),
  );

  it.effect(
    "a capped session reserves its whole cap per second: three fit $6, a fourth does not",
    () =>
      Effect.gen(function* () {
        const authorization = { check: "vertical", budgetUsd: 2.1, totalUsd: 6 } as const;
        assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 0 }), 1.75);
        assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 3.5 }), 1.75);
        yield* refused(Spend.admit({ rate, authorization, reservedUsd: 5.25 }));
        assert.strictEqual(
          yield* Spend.admit({
            rate,
            authorization: { check: "renewal", budgetUsd: 4.2, totalUsd: 6 },
            reservedUsd: 0,
          }),
          3.5,
        );
      }),
  );

  it.effect("a rate stated per minute reserves a whole minute, and four fit the total", () =>
    Effect.gen(function* () {
      const authorization = { check: "vertical", budgetUsd: 2.1, totalUsd: 8.4 } as const;
      const minute = perMinute;
      assert.strictEqual(
        yield* Spend.admit({ rate: minute, authorization, reservedUsd: 2.1 + 2.1 + 2.1 }),
        2.1,
      );
      yield* refused(Spend.admit({ rate: minute, authorization, reservedUsd: 6.3001 }));
      // At a higher published rate the same budget no longer covers the minute.
      yield* refused(
        Spend.admit({
          rate: { ...minute, creditsPerSecond: 351 },
          authorization,
          reservedUsd: 0,
        }),
      );
    }),
  );

  // unconnected watches its session past the cap, to 155 s at most from its request. On a second
  // token, the key holds a session past its ready and ends it within 59 s of its request, and one
  // its second create allocates within 56 s: each within a started minute. The $10 total admits it
  // only per second: in whole minutes it comes to its $10.50 ceiling.
  it.effect("a session held past its cap reserves the whole hold, per second and per minute", () =>
    Effect.gen(function* () {
      assert.strictEqual(Spend.ceilingFor("unconnected"), 10.5);
      assert.strictEqual(Spend.tokenSecondsFor("unconnected"), 215);
      const authorization = { check: "unconnected", budgetUsd: 10, totalUsd: 10 } as const;
      yield* Spend.authorize(authorization);
      assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 0 }), 9.45);
      yield* refused(Spend.admit({ rate: perMinute, authorization, reservedUsd: 0 }));
      yield* refused(
        Spend.admit({ rate, authorization: { ...authorization, budgetUsd: 9.44 }, reservedUsd: 0 }),
      );
      yield* refused(Spend.authorize({ check: "unconnected", budgetUsd: 10.5, totalUsd: 10.5 }));
      // The checks that end their sessions by the cap reserve as before.
      assert.deepStrictEqual(
        [Spend.ceilingFor("vertical"), Spend.tokenSecondsFor("vertical")],
        [2.1, 110],
      );
    }),
  );

  // Vidu S2-Avatar's published rate: 70 credits a second, stated per second, on October 1, 2026.
  // avatar's one session is capped at 120 s, two whole minutes, so either unit reserves $0.84.
  it.effect("avatar reserves its session at Vidu's rate, and H3's checks keep theirs", () =>
    Effect.gen(function* () {
      const vidu = { creditsPerSecond: 70, creditsPerDollar: 10_000, per: "second" } as const;
      assert.strictEqual(Spend.plans.avatar.model.name, "reactor/vidu-s2-avatar");
      assert.strictEqual(Spend.ceilingFor("avatar"), 1.26);
      const authorization = { check: "avatar", budgetUsd: 1.26, totalUsd: 2 } as const;
      yield* Spend.authorize(authorization);
      yield* refused(Spend.authorize({ ...authorization, budgetUsd: 1.27 }));
      assert.strictEqual(yield* Spend.admit({ rate: vidu, authorization, reservedUsd: 0 }), 1.26);
      assert.strictEqual(
        yield* Spend.admit({ rate: { ...vidu, per: "minute" }, authorization, reservedUsd: 0 }),
        1.26,
      );
      // Priced at H3's rate, the same session would cost five times its budget.
      yield* refused(Spend.admit({ rate, authorization, reservedUsd: 0 }));
      assert.deepStrictEqual(
        [Spend.ceilingFor("vertical"), Spend.ceilingFor("tour"), Spend.ceilingFor("showreel")],
        [2.1, 4.2, 4.2],
      );
    }),
  );

  // The requested spend seam uses the model ReactorTest now serves directly.
  it.effect("fasth3 reserves one FastH3 session and rehearses on FastH3", () =>
    Effect.sync(() => {
      assert.strictEqual(Spend.plans.fasth3.model.name, "reactor/fast-h3");
      assert.strictEqual(Spend.plans.fasth3.sessions, 1);
      assert.strictEqual(Spend.plans.fasth3.seconds, 50);
      assert.strictEqual(Spend.ceilingFor("fasth3"), 2.1);
      assert.strictEqual(Spend.modelFor("fasth3").name, "reactor/fast-h3");
    }),
  );

  // The requested model seam refuses a check on an unsupported model before admission.
  it.effect(
    "the model axis refuses FastH3 audio and admits its vertical at the reviewed ceiling",
    () =>
      Effect.gen(function* () {
        const audio = yield* Effect.exit(
          Spend.authorize({ check: "audio", model: "fast-h3", budgetUsd: 2.1, totalUsd: 10 }),
        );
        assert.isTrue(Exit.isFailure(audio));
        if (Exit.isFailure(audio)) {
          const error = Cause.squash(audio.cause);
          assert.isTrue(Schema.is(Spend.Refused)(error));
          if (Schema.is(Spend.Refused)(error))
            assert.strictEqual(error.message, "audio runs on h3 only");
        }
        const vertical = yield* Spend.authorize({
          check: "vertical",
          model: "fast-h3",
          budgetUsd: 2.1,
          totalUsd: 10,
        });
        assert.strictEqual(vertical.model, "fast-h3");
        assert.strictEqual(Spend.ceilingFor("vertical", "fast-h3"), 2.1);
      }),
  );

  // dropped's session has no cap; its reviewed hold reserves 80 s before cleanup retries.
  it.effect("an uncapped session reserves its hold: $2.80 at the per-second rate", () =>
    Effect.gen(function* () {
      assert.deepStrictEqual(
        [Spend.ceilingFor("dropped"), Spend.tokenSecondsFor("dropped")],
        [4.2, 140],
      );
      const authorization = { check: "dropped", budgetUsd: 2.8, totalUsd: 2.8 } as const;
      assert.strictEqual(yield* Spend.admit({ rate, authorization, reservedUsd: 0 }), 2.8);
      yield* refused(
        Spend.admit({ rate, authorization: { ...authorization, budgetUsd: 2.79 }, reservedUsd: 0 }),
      );
      yield* refused(Spend.admit({ rate, authorization, reservedUsd: 0.0125 }));
    }),
  );

  it("a reservation rounds up to four decimals, and a started unit of the rate bills whole", () => {
    assert.strictEqual(Spend.reservationUsd(0.7500000000000001), 0.75);
    assert.strictEqual(Spend.reservationUsd(0.75001), 0.7501);
    assert.strictEqual(Spend.billedUsd({ rate: perMinute, seconds: 61 }), 4.2);
    assert.strictEqual(Spend.billedUsd({ rate, seconds: 61 }), 2.135);
    assert.strictEqual(Spend.billedUsd({ rate, seconds: 0.2 }), 0.035);
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

  it.effect("only an uncapped check takes an uncapped grant, and it takes nothing else", () =>
    Effect.gen(function* () {
      const uncapped = { maxSessions: 1, maxSessionSeconds: "unlimited" } as const;
      yield* Spend.acceptGrant({ check: "dropped", granted: uncapped });
      yield* refused(Spend.acceptGrant({ check: "vertical", granted: uncapped }));
      yield* refused(
        Spend.acceptGrant({ check: "dropped", granted: { maxSessions: 1, maxSessionSeconds: 50 } }),
      );
      yield* refused(
        Spend.acceptGrant({ check: "dropped", granted: { ...uncapped, maxSessions: 2 } }),
      );
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
      // Hosted Reactor's echo of an uncapped token leaves its cap out, which says nothing: the
      // claims decide, and a cap they leave out is none.
      const uncappedClaims = yield* Schema.encodeEffect(
        Schema.StringFromBase64Url.pipe(Schema.decodeTo(Schema.fromJsonString(Schema.Unknown))),
      )({ authorization_details: [{ constraints: { max_sessions: 1 } }] });
      assert.deepStrictEqual(
        [
          yield* Spend.provenGrant({
            jwt: "e30.e30.sig",
            granted: { maxSessions: 1, maxSessionSeconds: "unlimited" },
          }),
          yield* Spend.provenGrant({
            jwt: `e30.${uncappedClaims}.sig`,
            granted: { maxSessions: 1, maxSessionSeconds: undefined },
          }),
        ],
        [
          { maxSessions: 1, maxSessionSeconds: "unlimited" },
          { maxSessions: 1, maxSessionSeconds: "unlimited" },
        ],
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
