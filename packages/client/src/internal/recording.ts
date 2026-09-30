/**
 * Bounded transport of a prepared recording (`clip_ready`): poll its HLS
 * playlist, then fetch and join its segments within a wall deadline. It does
 * not decode or play the clip, and a deadline does not change the remote
 * generation's outcome.
 */
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { ReactorError } from "../ReactorError.js";
import * as Deadline from "./deadline.js";
import type { Descriptor } from "../CoordinatorClient.js";
import type { ClipReady } from "../Session.js";

export interface Segment {
  readonly url: string;
  readonly kind: "init" | "media";
}

export interface DownloadedClip {
  readonly bytes: Uint8Array<ArrayBuffer>;
  readonly segments: ReadonlyArray<Segment>;
}

export interface DownloadOptions {
  /** The whole download, playlist wait included; 60 seconds by default. */
  readonly downloadTimeout?: Duration.Input | undefined;
  readonly maxManifestBytes?: number | undefined;
  readonly maxSegmentBytes?: number | undefined;
  readonly maxTotalBytes?: number | undefined;
  readonly maxSegments?: number | undefined;
}

/** A response read within its byte bound. */
export interface Reply {
  readonly status: number;
  readonly bytes: Uint8Array;
  /** The response's `Retry-After`, when it names a delay in seconds. */
  readonly retryAfter?: Duration.Duration | undefined;
}

/** The coordinator calls a download needs. */
export interface Fetcher {
  readonly fetch: (
    operation: string,
    url: string,
    maxBytes: number,
  ) => Effect.Effect<Reply, ReactorError>;
  readonly read: (sessionId: string) => Effect.Effect<Descriptor, ReactorError>;
}

const unsupported =
  /^#EXT-X-(STREAM-INF|I-FRAME-STREAM-INF|BYTERANGE|DISCONTINUITY|PART|PRELOAD-HINT|SKIP|GAP|SESSION-KEY|DEFINE)(:|$)/;

/**
 * The segments of a media playlist that byte concatenation can play: an
 * optional unencrypted init section, then its media segments, as http(s) URLs
 * with no embedded credentials.
 */
export const parsePlaylist = (playlist: {
  readonly text: string;
  readonly baseUrl: string;
  readonly maxSegments?: number | undefined;
}): Result.Result<ReadonlyArray<Segment>, ReactorError> => {
  const { text, baseUrl, maxSegments = 1024 } = playlist;
  const protocol = (message: string) => Result.fail(ReactorError.fromCode("Protocol", message));
  const refuse = (message: string) =>
    Result.fail(ReactorError.fromCode("UnsupportedCapability", message));
  const lines = text.split(/\r?\n/).map((line) => line.trim());
  if (lines.find((line) => line.length > 0) !== "#EXTM3U") return protocol("not an HLS playlist");
  let init: string | undefined;
  const media: Array<string> = [];
  for (const line of lines) {
    if (line.length === 0) continue;
    if (unsupported.test(line))
      return refuse(
        "master, byte-range, discontinuous, encrypted, gap, variable or low-latency playlists need an HLS player",
      );
    if (line.startsWith("#EXT-X-KEY:") && !/(?:^|,)METHOD=NONE(?:,|$)/.test(line.slice(11)))
      return refuse("encrypted clips are not assembled by this client");
    if (line.startsWith("#EXT-X-MAP:")) {
      if (line.includes("BYTERANGE=")) return refuse("byte-range init segments are unsupported");
      const uri = /(?:^|,)URI="([^"]+)"/.exec(line.slice(11))?.[1];
      if (uri === undefined || (init !== undefined && init !== uri))
        return protocol("missing or changing HLS init URI");
      init = uri;
    } else if (!line.startsWith("#")) {
      if (media.length >= maxSegments)
        return Result.fail(ReactorError.fromCode("Overflow", "clip segment count bound exceeded"));
      media.push(line);
    }
  }
  if (media.length === 0) return protocol("empty clip playlist");
  const resolved: Array<Segment> = [];
  for (const [kind, uri] of [
    ...(init === undefined ? [] : [["init", init] as const]),
    ...media.map((uri) => ["media", uri] as const),
  ]) {
    const url = URL.parse(uri, baseUrl);
    if (url === null) return protocol("invalid clip URL");
    if (
      !(url.protocol === "http:" || url.protocol === "https:") ||
      url.username !== "" ||
      url.password !== ""
    )
      return protocol("clip URL must use http(s) with no embedded credentials");
    if (kind === "media" && init === undefined && /\.(?:m4s|mp4|m4a|m4v)$/i.test(url.pathname))
      return protocol("fragmented MP4 playlist lacks an EXT-X-MAP init segment");
    resolved.push({ kind, url: url.href });
  }
  return Result.succeed(resolved);
};

/** How long past its predicted ready time a recording of an ended session is still waited for. */
const finishingGraceMs = 10_000;

/** A pending playlist is read again after its `Retry-After`, held to 200 ms to 2 s; 2 s if none. */
const pending = Schedule.spaced("2 seconds").pipe(
  Schedule.setInputType<Reply>(),
  Schedule.modifyDelay(({ input }) =>
    Effect.succeed(
      Duration.clamp(input.retryAfter ?? Duration.seconds(2), {
        minimum: Duration.millis(200),
        maximum: Duration.seconds(2),
      }),
    ),
  ),
);

const utf8 = new TextDecoder("utf-8", { fatal: true });

const Bound = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }));
/** A download's options, decoded once: a deadline, and whole, positive bounds. */
const Settings = Schema.Struct({
  downloadTimeout: Deadline.Deadline,
  maxManifestBytes: Bound,
  maxSegmentBytes: Bound,
  maxTotalBytes: Bound,
  maxSegments: Bound,
});

export const download = Effect.fnUntraced(function* (
  options: DownloadOptions & { readonly fetcher: Fetcher; readonly clip: ClipReady },
) {
  const { fetcher, clip } = options;
  const settings = yield* Schema.decodeEffect(Settings)({
    downloadTimeout: options.downloadTimeout ?? "60 seconds",
    maxManifestBytes: options.maxManifestBytes ?? 262_144,
    maxSegmentBytes: options.maxSegmentBytes ?? 16_777_216,
    maxTotalBytes: options.maxTotalBytes ?? 64 * 1024 * 1024,
    maxSegments: options.maxSegments ?? 1024,
  }).pipe(
    Effect.mapError((cause) =>
      ReactorError.fromCode(
        "InvalidInput",
        "a download needs a finite, non-negative deadline and whole, positive bounds",
        { outcome: "not-submitted", detail: cause },
      ),
    ),
  );
  const { maxSegments } = settings;
  const manifestBytes = settings.maxManifestBytes;
  const segmentBytes = settings.maxSegmentBytes;
  const totalBytes = settings.maxTotalBytes;
  // A recording can finish after its session ends, so an ended session is a
  // verdict only once the clip's predicted ready time, and a grace, have passed.
  const ended = Effect.gen(function* () {
    const descriptor = yield* fetcher.read(clip.sessionId);
    const due = Number(clip.predictedReadyAtMs) + finishingGraceMs;
    // Only CLOSED has ended (CoordinatorClient.isTerminal, which imports this module): an
    // INACTIVE session lost its connection and may still be reconnected.
    if (descriptor.state === "CLOSED" && (yield* Clock.currentTimeMillis) > due)
      return yield* ReactorError.fromCode(
        "TerminalSession",
        "session ended and its playlist was not available by the predicted time",
      );
  });
  const playlist = fetcher.fetch("clip playlist", clip.playlistUrl, manifestBytes).pipe(
    Effect.tap((reply) => (reply.status === 202 ? ended : Effect.void)),
    Effect.repeat({ schedule: pending, until: (reply) => reply.status !== 202 }),
    Effect.flatMap((reply) =>
      Effect.try({
        try: () => utf8.decode(reply.bytes),
        catch: () => ReactorError.fromCode("Protocol", "clip playlist is not UTF-8"),
      }),
    ),
    Effect.flatMap((text) =>
      Effect.fromResult(parsePlaylist({ text, baseUrl: clip.playlistUrl, maxSegments })),
    ),
  );
  const joined = Effect.gen(function* () {
    const segments = yield* playlist;
    const chunks: Array<Uint8Array> = [];
    let size = 0;
    for (const segment of segments) {
      const reply = yield* fetcher.fetch(
        "clip segment",
        segment.url,
        Math.max(1, Math.min(segmentBytes, totalBytes - size)),
      );
      if (reply.status === 202)
        return yield* ReactorError.fromCode(
          "Protocol",
          "playlist named a segment that is not ready",
        );
      size += reply.bytes.byteLength;
      if (size > totalBytes)
        return yield* ReactorError.fromCode("Overflow", "assembled clip byte bound exceeded");
      chunks.push(reply.bytes);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return { bytes, segments };
  });
  return yield* Effect.timeoutOrElse(joined, {
    duration: settings.downloadTimeout,
    orElse: () =>
      Effect.fail(
        ReactorError.fromCode("Timeout", "clip download deadline; the remote outcome is unchanged"),
      ),
  });
});
