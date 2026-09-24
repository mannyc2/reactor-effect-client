import { Config, Duration, Effect, Layer } from "effect";
import * as Reactor from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import * as Native from "reactor-effect-native";
import { Ledger } from "./Ledger.ts";
import { Settings } from "./Settings.ts";
import * as TestCard from "./TestCard.ts";

/**
 * Offline: every source is the SDK's simulation, drawn by the test card, and
 * lives one session length, so the orchestration renews it as it renews a
 * paid session: it opens the replacement `lead` before expiry, routes new
 * clips to it, and switches the output once the old source has played out.
 */
const simulated = (settings: Settings["Service"]) =>
  Orchestration.layer({
    open: Effect.suspend(() =>
      Simulation.source({ present: TestCard.make().present, buildRatio: 0.25 }),
    ).pipe(Effect.map((source) => ({ source, lifetime: settings.sessionLength }))),
    lead: settings.lead,
  });

/**
 * Paid: every source is an H3 session whose token is minted here, so the API
 * key never leaves this process. `openH3` records the owner through the
 * ledger before the session connects. Sessions are capped in length and in
 * number: once `CHANNEL_MAX_SESSIONS` have been opened, the orchestration
 * refuses the next renewal, fails after three attempts (about ten seconds
 * past the renewal point) and the channel goes off air; the application then
 * closes the last session rather than let it run to its expiry.
 */
const live = ({ sessionLength, lead }: Settings["Service"]) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const apiKey = yield* Config.Redacted("REACTOR_API_KEY");
      const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
        Config.withDefault("https://api.reactor.inc"),
      );
      const maxSessions = yield* Config.Int("CHANNEL_MAX_SESSIONS").pipe(Config.withDefault(3));
      const coordinator = yield* Reactor.Coordinator.make({ apiUrl });
      const rate = yield* Reactor.Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
      yield* Effect.logInfo("live channel", {
        creditsPerSecond: rate.creditsPerSecond,
        creditsPerDollar: rate.creditsPerDollar,
        sessionSeconds: Duration.toSeconds(sessionLength),
        maxSessions,
      });
      const ledger = yield* Ledger;
      return Orchestration.layer({
        open: Orchestration.openH3({
          mint: coordinator.mintToken({
            apiKey,
            modelName: H3.modelName,
            maxSessionDuration: sessionLength,
            // The token outlives the session, so cleanup can still terminate it.
            expiresAfter: Duration.sum(sessionLength, Duration.minutes(2)),
          }),
          onAllocated: ({ allocation }) => ledger.allocated(allocation),
        }),
        lead,
        maxSessions,
      }).pipe(
        Layer.provide(Reactor.layer({ apiUrl })),
        // Each connection's native peer runs in a child process of its own, so a
        // crash in libwebrtc ends that connection, not the server (Node only).
        Layer.provide(Native.Isolated.layer()),
      );
    }),
  );

/**
 * The orchestration's Engine, Media and Handle services, from one chain of
 * sources: simulated unless `CHANNEL_MODE=live` chooses paid sessions.
 */
export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const settings = yield* Settings;
    return settings.mode === "live" ? live(settings) : simulated(settings);
  }),
);
