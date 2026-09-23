import * as Effect from "effect/Effect";
import { ReactorError } from "reactor-effect-client";
import {
  CoordinatorClient,
  errorOf,
  positiveLimit,
  retryAfterMs,
  terminal,
} from "reactor-effect-client/host";
import type { ClipReady } from "reactor-effect-client/wire";
export interface Segment {
  readonly url: string;
  readonly kind: "init" | "media";
}
export interface DownloadOptions {
  readonly timeoutMs?: number;
  readonly maxManifestBytes?: number;
  readonly maxSegmentBytes?: number;
  readonly maxTotalBytes?: number;
  readonly maxSegments?: number;
}
const pure = <A>(body: () => A): Effect.Effect<A, ReactorError> =>
  Effect.try({ try: body, catch: errorOf });
export const parsePlaylist = (
  text: string,
  baseUrl: string,
  maxSegments = 1024,
): readonly Segment[] => {
  positiveLimit(maxSegments, "clip segments", 16_384);
  if (text.trimStart().split(/\r?\n/, 1)[0]?.trim() !== "#EXTM3U")
    throw ReactorError.fromCode("Protocol", "not an HLS playlist");
  let init: string | undefined;
  const media: string[] = [];
  for (const line of text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean)) {
    if (
      /^#EXT-X-(STREAM-INF|I-FRAME-STREAM-INF|BYTERANGE|DISCONTINUITY|PART|PRELOAD-HINT|SKIP|GAP|SESSION-KEY|DEFINE)(:|$)/.test(
        line,
      )
    )
      throw ReactorError.fromCode(
        "UnsupportedCapability",
        "master, byte-range, discontinuous, encrypted, gap, variable, or low-latency playlists require an HLS player, not byte concatenation",
      );
    if (line.startsWith("#EXT-X-KEY:") && !/(?:^|,)METHOD=NONE(?:,|$)/.test(line.slice(11)))
      throw ReactorError.fromCode(
        "UnsupportedCapability",
        "encrypted clips are not assembled by this client",
      );
    if (line.startsWith("#EXT-X-MAP:")) {
      if (line.includes("BYTERANGE="))
        throw ReactorError.fromCode(
          "UnsupportedCapability",
          "byte-range init segments unsupported",
        );
      const match = /(?:^|,)URI="([^"]+)"/.exec(line.slice(11)),
        uri = match?.[1];
      if (uri === undefined || (init !== undefined && init !== uri))
        throw ReactorError.fromCode("Protocol", "missing or changing HLS init URI");
      init = uri;
    } else if (!line.startsWith("#")) {
      if (media.length >= maxSegments)
        throw ReactorError.fromCode("Overflow", "clip segment count bound exceeded");
      media.push(line);
    }
  }
  if (media.length === 0) throw ReactorError.fromCode("Protocol", "empty clip playlist");
  const resolve = (uri: string): string => {
    const url = new URL(uri, baseUrl);
    if (!(url.protocol === "http:" || url.protocol === "https:") || url.username || url.password)
      throw ReactorError.fromCode(
        "Protocol",
        "clip URL must use http(s) with no embedded credentials",
      );
    return url.href;
  };
  if (
    init === undefined &&
    media.some((uri) => /\.(?:m4s|mp4|m4a|m4v)$/i.test(new URL(uri, baseUrl).pathname))
  )
    throw ReactorError.fromCode("Protocol", "fragmented MP4 playlist lacks EXT-X-MAP init segment");
  const segments: Segment[] = init === undefined ? [] : [{ kind: "init", url: resolve(init) }];
  for (const uri of media) segments.push({ kind: "media", url: resolve(uri) });
  return Object.freeze(segments.map((s) => Object.freeze(s)));
};
/** Bounded transport of a prepared recording. Does not decode, play, or assert further generation.
 * This API intentionally uses a caller wall deadline, not an unbounded wait based on a prediction. */
export const downloadClip = (
  http: CoordinatorClient,
  clip: ClipReady,
  options: DownloadOptions = {},
): Effect.Effect<
  {
    readonly bytes: Uint8Array<ArrayBuffer>;
    readonly segments: readonly Segment[];
  },
  ReactorError
> =>
  Effect.suspend(() => {
    const action = Effect.gen(function* () {
      const bounds = yield* pure(() => ({
        timeout: positiveLimit(options.timeoutMs ?? 60_000, "clip timeout", 600_000),
        manifest: positiveLimit(options.maxManifestBytes ?? 262_144, "manifest bytes", 2_097_152),
        segment: positiveLimit(
          options.maxSegmentBytes ?? 16_777_216,
          "segment bytes",
          64 * 1024 * 1024,
        ),
        total: positiveLimit(
          options.maxTotalBytes ?? 64 * 1024 * 1024,
          "clip bytes",
          64 * 1024 * 1024,
        ),
        count: positiveLimit(options.maxSegments ?? 1024, "clip segments", 16_384),
      }));
      const fetchAndAssemble = Effect.gen(function* () {
        let segments: readonly Segment[];
        while (true) {
          const response = yield* http.request({
            operation: "clip playlist",
            url: clip.playlist_url,
            auth: "same-origin",
            maxBytes: bounds.manifest,
          });
          if (response.status !== 202) {
            segments = yield* pure(() =>
              parsePlaylist(
                new TextDecoder("utf-8", { fatal: true }).decode(response.bytes),
                clip.playlist_url,
                bounds.count,
              ),
            );
            break;
          }
          // A local peer disconnection does not prove stopped inference. Query the remote descriptor.
          const descriptor = yield* http.read(clip.session_id);
          if (terminal(descriptor.state))
            return yield* ReactorError.fromCode(
              "TerminalSession",
              "session terminated before playlist became available",
            );
          yield* Effect.sleep(
            Math.max(200, Math.min(retryAfterMs(response.headers) ?? 2000, 2000)),
          );
        }
        let size = 0;
        const chunks: Uint8Array[] = [];
        for (const segment of segments) {
          const response = yield* http.request({
            operation: "clip segment",
            url: segment.url,
            auth: "same-origin",
            maxBytes: Math.min(bounds.segment, bounds.total - size || 1),
          });
          if (response.status === 202)
            return yield* ReactorError.fromCode(
              "Protocol",
              "playlist referenced a segment that is not ready",
            );
          size += response.bytes.length;
          if (size > bounds.total)
            return yield* ReactorError.fromCode("Overflow", "assembled clip byte bound exceeded");
          chunks.push(response.bytes);
        }
        const bytes = new Uint8Array(size);
        let at = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, at);
          at += chunk.length;
        }
        return { bytes, segments };
      });
      return yield* fetchAndAssemble.pipe(
        Effect.timeoutOrElse({
          duration: bounds.timeout,
          orElse: () =>
            Effect.fail(
              ReactorError.fromCode(
                "Timeout",
                "clip download deadline; remote generation outcome unchanged",
              ),
            ),
        }),
      );
    });
    return action;
  });
