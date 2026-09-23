/** The portable recording transport: playlist parsing and bounded clip assembly. */
import { expect, test } from "vitest";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Http from "effect/unstable/http/HttpClient";
import * as Response from "effect/unstable/http/HttpClientResponse";
import * as Coordinator from "../src/coordinator/index.js";
import { ReactorError } from "../src/errors.js";
import type { ClipReady } from "../src/wire.generated.js";

const api = "https://api.fixture";
const cdn = "https://cdn.fixture";

const playlist = [
  "#EXTM3U",
  "#EXT-X-VERSION:7",
  '#EXT-X-MAP:URI="init.mp4"',
  "#EXTINF:1.0,",
  "seg-0.m4s",
  "#EXTINF:1.0,",
  `${cdn}/clips/seg-1.m4s`,
  "#EXT-X-ENDLIST",
].join("\n");

const codeOf = (thrown: () => unknown): string => {
  try {
    thrown();
  } catch (error) {
    if (ReactorError.is(error)) return error.reason._tag;
    throw error;
  }
  throw new Error("expected a ReactorError");
};

test("parsePlaylist resolves an fMP4 media playlist's init and media segments", () => {
  expect(Coordinator.parsePlaylist(playlist, `${api}/clips/c.m3u8`)).toEqual([
    { kind: "init", url: `${api}/clips/init.mp4` },
    { kind: "media", url: `${api}/clips/seg-0.m4s` },
    { kind: "media", url: `${cdn}/clips/seg-1.m4s` },
  ]);
});

test("parsePlaylist refuses what byte concatenation cannot assemble", () => {
  const base = `${api}/clips/c.m3u8`;
  expect(codeOf(() => Coordinator.parsePlaylist("not a playlist", base))).toBe("Protocol");
  expect(
    codeOf(() => Coordinator.parsePlaylist("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nv.m3u8", base)),
  ).toBe("UnsupportedCapability");
  expect(
    codeOf(() =>
      Coordinator.parsePlaylist('#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="k"\nseg.ts', base),
    ),
  ).toBe("UnsupportedCapability");
  expect(codeOf(() => Coordinator.parsePlaylist("#EXTM3U\nseg-0.m4s", base))).toBe("Protocol");
  expect(
    codeOf(() => Coordinator.parsePlaylist("#EXTM3U\nhttps://user:pass@cdn.fixture/seg.ts", base)),
  ).toBe("Protocol");
  expect(codeOf(() => Coordinator.parsePlaylist("#EXTM3U\na.ts\nb.ts", base, 1))).toBe("Overflow");
});

const clip = (fields: Partial<ClipReady> = {}): ClipReady => ({
  session_id: "session-1",
  kind: "recording",
  start_marker: 0,
  end_marker: 2,
  now_marker: 2,
  predicted_ready_at_ms: 0n,
  playlist_url: `${api}/clips/c.m3u8`,
  ...fields,
});

interface Served {
  readonly url: string;
  readonly authorized: boolean;
}
/** A coordinator and CDN: the playlist is not ready `pending` times, then served. */
const origin = (options: { readonly pending?: number; readonly segmentBytes?: number } = {}) => {
  const served: Served[] = [];
  let pending = options.pending ?? 0;
  const client = Http.make((request, url) =>
    Effect.sync(() => {
      served.push({ url: url.href, authorized: request.headers.authorization !== undefined });
      const reply = (response: globalThis.Response) => Response.fromWeb(request, response);
      if (url.pathname === "/clips/c.m3u8") {
        if (pending > 0) {
          pending--;
          return reply(
            new globalThis.Response(null, { status: 202, headers: { "retry-after": "0" } }),
          );
        }
        return reply(new globalThis.Response(playlist));
      }
      if (url.pathname === "/sessions/session-1")
        return reply(globalThis.Response.json({ session_id: "session-1", state: "ACTIVE" }));
      const size = options.segmentBytes ?? 4;
      const marker = url.pathname.endsWith("init.mp4")
        ? 9
        : url.pathname.endsWith("seg-0.m4s")
          ? 0
          : 1;
      return reply(new globalThis.Response(new Uint8Array(size).fill(marker)));
    }),
  );
  return { served, client };
};

const download = (
  client: Http.HttpClient,
  options?: Coordinator.DownloadOptions,
): Promise<Result.Result<Coordinator.DownloadedClip, ReactorError>> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const coordinator = yield* Coordinator.make({
        apiUrl: api,
        credential: Effect.succeed(Redacted.make("fixture-token")),
      });
      return yield* Effect.result(coordinator.downloadClip(clip(), options));
    }).pipe(Effect.provideService(Http.HttpClient, client)),
  );

test("downloadClip waits for the playlist, then concatenates its segments in order", async () => {
  const { served, client } = origin({ pending: 1 });
  const result = await download(client);
  expect(Result.isSuccess(result)).toBe(true);
  if (Result.isFailure(result)) return;
  expect(Array.from(result.success.bytes)).toEqual([9, 9, 9, 9, 0, 0, 0, 0, 1, 1, 1, 1]);
  expect(result.success.segments.map((segment) => segment.kind)).toEqual([
    "init",
    "media",
    "media",
  ]);
  // A pending playlist reads the session descriptor before polling again, and
  // only the coordinator's own origin receives the credential.
  expect(served.map((entry) => new URL(entry.url).pathname)).toEqual([
    "/clips/c.m3u8",
    "/sessions/session-1",
    "/clips/c.m3u8",
    "/clips/init.mp4",
    "/clips/seg-0.m4s",
    "/clips/seg-1.m4s",
  ]);
  expect(served.filter((entry) => entry.url.startsWith(cdn)).every((e) => !e.authorized)).toBe(
    true,
  );
  expect(served.filter((entry) => entry.url.startsWith(api)).every((e) => e.authorized)).toBe(true);
});

test("downloadClip bounds the assembled bytes and its wall deadline", async () => {
  const bounded = await download(origin({ segmentBytes: 8 }).client, { maxTotalBytes: 16 });
  expect(Result.isFailure(bounded) && bounded.failure.reason._tag).toBe("Overflow");
  const late = await download(origin({ pending: 1_000 }).client, { downloadTimeout: 50 });
  expect(Result.isFailure(late) && late.failure.reason._tag).toBe("Timeout");
});
