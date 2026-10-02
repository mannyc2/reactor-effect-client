import { Config, Duration, Effect, Layer, Ref } from "effect";
import type { Crypto, Redacted } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import {
  CoordinatorClient,
  H3,
  H3Source,
  Playout,
  Reactor,
  ReactorTest,
} from "reactor-effect-client";
import { AcquisitionFailure, ReactorError } from "reactor-effect-client/ReactorError";
import { NativePeer } from "reactor-effect-native";
import { Ledger } from "./Ledger.ts";
import { Settings } from "./Settings.ts";

/** What plays when no viewer has asked for anything. */
const house = [
  "A slow dolly shot through a sunlit greenhouse full of ferns",
  "Waves rolling onto a black sand beach at dusk, seen from a low angle",
  "A paper lantern drifting along a quiet canal at night",
  "A time-lapse of clouds pouring over a mountain ridge",
  "Steam rising off a still mountain lake at dawn, a heron standing in the shallows",
  "Snow falling past the lit windows of a small railway station at night",
];

/** The house rotation's prompt for the filler clip at `index`. */
export const housePrompt = (index: number): string => house[index % house.length] ?? "";

/**
 * The channel's playout: viewers' prompts in one lane, the house rotation as
 * filler, and sessions renewed `lead` before they end. The same plan runs
 * offline on the simulated Reactor and live on paid H3; only the network edge
 * differs, and in both the session token is minted here, so the API key never
 * leaves this process.
 */
export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const settings = yield* Settings;
    const ledger = yield* Ledger;
    const maxSessions = yield* Config.Int("CHANNEL_MAX_SESSIONS").pipe(Config.withDefault(3));
    const opened = yield* Ref.make(0);
    const options = <R>(
      apiKey: Effect.Effect<Redacted.Redacted<string>, never, R>,
    ): Playout.Options<
      R | Reactor.Reactor | CoordinatorClient.CoordinatorClient | Crypto.Crypto
    > => ({
      // A live channel opens at most CHANNEL_MAX_SESSIONS sessions; past that
      // the playout airs its last session until its cap ends, then fails, and
      // the channel goes off air.
      open: Ref.getAndUpdate(opened, (count) => count + 1).pipe(
        Effect.flatMap((count) =>
          settings.mode === "live" && count >= maxSessions
            ? Effect.fail(
                AcquisitionFailure.from(
                  ReactorError.fromCode("InvalidState", "the channel's session cap was reached"),
                  Reactor.noAcquisition,
                ),
              )
            : Effect.gen(function* () {
                const coordinator = yield* CoordinatorClient.CoordinatorClient;
                // Each session starts on a token and carries on with tokens bound to it.
                return yield* H3Source.open({
                  tokens: coordinator.tokens({
                    apiKey: yield* apiKey,
                    modelName: H3.modelName,
                    maxSessionDuration: settings.sessionLength,
                  }),
                  onAllocated: ({ allocation }) => ledger.allocated(allocation),
                });
              }),
        ),
      ),
      lanes: [{ name: "viewer" }],
      filler: {
        runway: {
          floor: Duration.seconds(settings.clipSeconds),
          target: Duration.seconds(2 * settings.clipSeconds),
        },
        clip: ({ index }) => ({ prompt: housePrompt(index), seconds: settings.clipSeconds }),
      },
      renewal: { lead: settings.lead },
    });
    // The ledger records what the playout closed once it has closed it: a finalizer
    // registered before the playout is built runs after the playout's own.
    const recorded = <R>(input: Playout.Options<R>) =>
      Layer.effect(
        Playout.Playout,
        Effect.gen(function* () {
          const built = yield* Ref.make<Playout.Playout["Service"] | undefined>(undefined);
          yield* Effect.addFinalizer(() =>
            Ref.get(built).pipe(
              Effect.flatMap((playout) =>
                playout === undefined
                  ? Effect.void
                  : Effect.flatMap(playout.cleanup, ledger.closed),
              ),
            ),
          );
          const playout = yield* Playout.make(input);
          yield* Ref.set(built, playout);
          yield* playout.failure.pipe(
            Effect.flatMap((failure) =>
              Effect.logWarning("channel off air", { reason: failure.message }),
            ),
            Effect.forkScoped,
          );
          return playout;
        }),
      );
    if (settings.mode === "live") {
      const apiKey = yield* Config.Redacted("REACTOR_API_KEY");
      const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
        Config.withDefault("https://api.reactor.inc"),
      );
      return Effect.succeed(apiKey).pipe(
        options,
        recorded,
        Layer.provide(Reactor.layer()),
        Layer.provideMerge(CoordinatorClient.layer({ apiUrl })),
        // Each connection's native peer runs in a child process of its own, so a
        // crash in libwebrtc ends that connection, not the server (Node only).
        Layer.provide(NativePeer.layerIsolated()),
        Layer.provide(FetchHttpClient.layer),
      );
    }
    // Offline: the simulated Reactor plays the timing two paid runs measured.
    return Effect.gen(function* () {
      return (yield* ReactorTest.ReactorTest).apiKey;
    }).pipe(
      options,
      recorded,
      Layer.provide(Reactor.layer()),
      Layer.provideMerge(CoordinatorClient.layer()),
      Layer.provideMerge(
        ReactorTest.layer({ timing: ReactorTest.Timing.hosted, width: 320, height: 180 }),
      ),
    );
  }),
);
