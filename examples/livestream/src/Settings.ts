import { Config, Context, Duration, Effect, Layer, Option, Redacted, Schema } from "effect";
import * as H3 from "reactor-effect-client/H3";

export class SettingsError extends Schema.TaggedError<SettingsError>()("SettingsError", {
  message: Schema.String,
}) {}

/**
 * An RTMP ingest the program also goes to, such as Twitch's, YouTube's or X's.
 * Its URL holds the stream key, so it stays redacted, and logs show `host`.
 */
export interface Ingest {
  readonly url: Redacted.Redacted<string>;
  /** The scheme and host, such as `rtmp://live.twitch.tv`. */
  readonly host: string;
  /**
   * What a log line must not show: the URL's path and query, its last path
   * segment and its query alone, longest first. The stream key is one of them.
   */
  readonly hidden: Redacted.Redacted<ReadonlyArray<string>>;
}

// The scheme and host, with no user or password, then the path and query.
// The URL goes to ffmpeg's tee muxer, which reads whitespace, `|`, `[`, `]`,
// `\` and `'` as its own syntax.
const ingestUrl = /^(rtmps?:\/\/[^\s/?#@|[\]\\']+)([/?][^\s|[\]\\']*)?$/;

const ingestOf = (url: Redacted.Redacted<string>): Effect.Effect<Ingest, SettingsError> => {
  const [, host, rest = ""] = ingestUrl.exec(Redacted.value(url)) ?? [];
  if (host === undefined)
    return SettingsError.make({
      message: "CHANNEL_RTMP_URL must be an rtmp:// or rtmps:// URL with no user or password",
    });
  const mark = rest.indexOf("?");
  const path = mark < 0 ? rest : rest.slice(0, mark);
  const query = mark < 0 ? "" : rest.slice(mark + 1);
  const last = path.slice(path.lastIndexOf("/") + 1);
  return Effect.succeed({
    url,
    host,
    hidden: Redacted.make([rest, last, query].filter((part) => part.length > 1)),
  });
};

/**
 * The channel's settings, read once from the environment.
 *
 * - `CHANNEL_NAME`: the name the page shows; `Slow TV`.
 * - `CHANNEL_MODE`: `simulated` (the default, offline and unpaid) or `live`.
 * - `CHANNEL_SESSION_LENGTH`: how long each session lives before it is
 *   renewed; 10 minutes live, 2 minutes simulated so a renewal is soon seen.
 * - `CHANNEL_RENEWAL_LEAD`: how long before a session ends its replacement
 *   is opened; 45 seconds.
 * - `CHANNEL_CLIP_SECONDS`: the length of every clip, within H3's request
 *   range; 8.
 * - `CHANNEL_RTMP_URL`: an rtmp:// or rtmps:// ingest the program also goes
 *   to; none by default.
 */
export class Settings extends Context.Service<
  Settings,
  {
    readonly name: string;
    readonly mode: "simulated" | "live";
    readonly sessionLength: Duration.Duration;
    readonly lead: Duration.Duration;
    readonly clipSeconds: number;
    readonly ingest: Ingest | undefined;
  }
>()("reactor-effect-example-livestream/Settings") {
  static readonly layer = Layer.effect(
    Settings,
    Effect.gen(function* () {
      const name = yield* Config.NonEmptyString("CHANNEL_NAME").pipe(Config.withDefault("Slow TV"));
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
        return yield* SettingsError.make({
          message: `CHANNEL_CLIP_SECONDS must be ${H3.requestSeconds.min} to ${H3.requestSeconds.max} seconds`,
        });
      // Admission keeps at most the lead, less one clip, committed, so the
      // lead must hold two clips and end before the session does.
      if (
        Duration.toSeconds(lead) < 2 * clipSeconds ||
        Duration.isGreaterThanOrEqualTo(lead, sessionLength)
      )
        return yield* SettingsError.make({
          message: "CHANNEL_RENEWAL_LEAD must hold two clips and be shorter than a session",
        });
      const url = yield* Config.Redacted("CHANNEL_RTMP_URL").pipe(Config.option);
      const ingest = Option.isSome(url) ? yield* ingestOf(url.value) : undefined;
      return Settings.of({ name, mode, sessionLength, lead, clipSeconds, ingest });
    }),
  );
}
