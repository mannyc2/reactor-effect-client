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
import type * as Headers from "effect/unstable/http/Headers";
import { ReactorError } from "../ReactorError.js";
import type { Descriptor } from "../Coordinator.js";
import type { ClipReady } from "./wire.js";

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

interface Reply {
  readonly status: number;
  readonly headers: Headers.Headers;
  readonly bytes: Uint8Array<ArrayBuffer>;
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

const retryAfterMillis = (headers: Headers.Headers): number => {
  const seconds = Number(headers["retry-after"] ?? Number.NaN);
  return Number.isFinite(seconds) ? seconds * 1000 : 2000;
};

export const download = (
  options: DownloadOptions & { readonly fetcher: Fetcher; readonly clip: ClipReady },
): Effect.Effect<DownloadedClip, ReactorError> => {
  const { fetcher, clip } = options;
  const manifestBytes = options.maxManifestBytes ?? 262_144;
  const segmentBytes = options.maxSegmentBytes ?? 16_777_216;
  const totalBytes = options.maxTotalBytes ?? 64 * 1024 * 1024;
  const maxSegments = options.maxSegments ?? 1024;
  const playlist: Effect.Effect<ReadonlyArray<Segment>, ReactorError> = Effect.gen(function* () {
    const reply = yield* fetcher.fetch("clip playlist", clip.playlistUrl, manifestBytes);
    if (reply.status !== 202) {
      const text = yield* Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(reply.bytes),
        catch: () => ReactorError.fromCode("Protocol", "clip playlist is not UTF-8"),
      });
      return yield* Effect.fromResult(
        parsePlaylist({ text, baseUrl: clip.playlistUrl, maxSegments }),
      );
    }
    // A recording can finish after its session ends, so an ended session is a
    // verdict only once the clip's predicted ready time, and a grace, have passed.
    const descriptor = yield* fetcher.read(clip.sessionId);
    const due = Number(clip.predictedReadyAtMs) + finishingGraceMs;
    // Only CLOSED has ended (Coordinator.isTerminal, which imports this module): an
    // INACTIVE session lost its connection and may still be reconnected.
    if (descriptor.state === "CLOSED" && (yield* Clock.currentTimeMillis) > due)
      return yield* ReactorError.fromCode(
        "TerminalSession",
        "session ended and its playlist was not available by the predicted time",
      );
    yield* Effect.sleep(Math.max(200, Math.min(retryAfterMillis(reply.headers), 2000)));
    return yield* playlist;
  });
  return Effect.gen(function* () {
    const segments = yield* playlist;
    const chunks: Array<Uint8Array<ArrayBuffer>> = [];
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
  }).pipe(
    Effect.timeoutOrElse({
      duration: Duration.fromInputUnsafe(options.downloadTimeout ?? "60 seconds"),
      orElse: () =>
        Effect.fail(
          ReactorError.fromCode(
            "Timeout",
            "clip download deadline; the remote outcome is unchanged",
          ),
        ),
    }),
  );
};
