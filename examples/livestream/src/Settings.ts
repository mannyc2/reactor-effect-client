import { Config, Context, Duration, Effect, Layer, Schema } from "effect";
import * as H3 from "reactor-effect-client/h3";

export class SettingsError extends Schema.TaggedError<SettingsError>()("SettingsError", {
  message: Schema.String,
}) {}

/**
 * The channel's settings, read once from the environment.
 *
 * - `CHANNEL_MODE`: `simulated` (the default, offline and unpaid) or `live`.
 * - `CHANNEL_SESSION_LENGTH`: how long each session lives before it is
 *   renewed; 10 minutes live, 2 minutes simulated so a renewal is soon seen.
 * - `CHANNEL_RENEWAL_LEAD`: how long before a session ends its replacement
 *   is opened; 45 seconds.
 * - `CHANNEL_CLIP_SECONDS`: the length of every clip, within H3's request
 *   range; 8.
 */
export class Settings extends Context.Service<
  Settings,
  {
    readonly mode: "simulated" | "live";
    readonly sessionLength: Duration.Duration;
    readonly lead: Duration.Duration;
    readonly clipSeconds: number;
  }
>()("reactor-effect-example-livestream/Settings") {
  static readonly layer = Layer.effect(
    Settings,
    Effect.gen(function* () {
      const mode = yield* Config.Literals(["simulated", "live"], "CHANNEL_MODE").pipe(
        Config.withDefault("simulated"),
      );
      const sessionLength = yield* Config.Duration("CHANNEL_SESSION_LENGTH").pipe(
        Config.withDefault(mode === "live" ? Duration.minutes(10) : Duration.minutes(2)),
      );
      const lead = yield* Config.Duration("CHANNEL_RENEWAL_LEAD").pipe(
        Config.withDefault(Duration.seconds(45)),
      );
      const clipSeconds = yield* Config.Finite("CHANNEL_CLIP_SECONDS").pipe(Config.withDefault(8));
      if (!H3.isRequestableSeconds(H3.h3ReferenceTurboRealtime, clipSeconds))
        return yield* new SettingsError({
          message: `CHANNEL_CLIP_SECONDS must be ${H3.requestSeconds.min} to ${H3.requestSeconds.max} seconds`,
        });
      // Admission keeps at most the lead, less one clip, committed, so the
      // lead must hold two clips and end before the session does.
      if (
        Duration.toSeconds(lead) < 2 * clipSeconds ||
        Duration.isGreaterThanOrEqualTo(lead, sessionLength)
      )
        return yield* new SettingsError({
          message: "CHANNEL_RENEWAL_LEAD must hold two clips and be shorter than a session",
        });
      return Settings.of({ mode, sessionLength, lead, clipSeconds });
    }),
  );
}
