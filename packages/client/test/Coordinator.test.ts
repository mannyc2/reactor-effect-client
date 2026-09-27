/** Reactor's HTTP API over an in-memory origin: what counts as proof, and who gets the token. */
import { assert, describe, it } from "@effect/vitest";
import { Effect, Encoding, Fiber, Redacted, Result } from "effect";
import { TestClock } from "effect/testing";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import { Coordinator } from "../src/index.js";
import type { ClipReady } from "../src/Session.js";

const api = "https://api.fixture";
const cdn = "https://cdn.fixture";

interface Served {
  readonly method: string;
  readonly url: string;
  readonly authorized: boolean;
}

type Route = (method: string, url: URL) => Response;

/** An origin that answers from `route` and records every request it served. */
const origin = (route: Route) => {
  const served: Array<Served> = [];
  const client = HttpClient.make((request, url) =>
    Effect.sync(() => {
      served.push({
        method: request.method,
        url: url.href,
        authorized: request.headers.authorization !== undefined,
      });
      return HttpClientResponse.fromWeb(request, route(request.method, url));
    }),
  );
  return { served, client };
};

const coordinator = (client: HttpClient.HttpClient, options: Coordinator.Options = {}) =>
  Coordinator.make({
    apiUrl: api,
    credential: Effect.succeed(Redacted.make("session-token")),
    ...options,
  }).pipe(Effect.provideService(HttpClient.HttpClient, client));

describe("recordings", () => {
  const playlist = [
    "#EXTM3U",
    '#EXT-X-MAP:URI="init.mp4"',
    "#EXTINF:1.0,",
    "seg-0.m4s",
    "#EXTINF:1.0,",
    `${cdn}/clips/seg-1.m4s`,
    "#EXT-X-ENDLIST",
  ].join("\n");
  const base = `${api}/clips/c.m3u8`;
  const refusal = (text: string, maxSegments?: number) => {
    const parsed = Coordinator.parsePlaylist({ text, baseUrl: base, maxSegments });
    return Result.isFailure(parsed) ? parsed.failure.reason._tag : "parsed";
  };

  it("resolves an fMP4 playlist's init and media segments against its URL", () => {
    assert.deepStrictEqual(
      Coordinator.parsePlaylist({ text: playlist, baseUrl: base }),
      Result.succeed([
        { kind: "init", url: `${api}/clips/init.mp4` },
        { kind: "media", url: `${api}/clips/seg-0.m4s` },
        { kind: "media", url: `${cdn}/clips/seg-1.m4s` },
      ]),
    );
  });

  it("refuses what byte concatenation cannot assemble", () => {
    assert.strictEqual(refusal("not a playlist"), "Protocol");
    assert.strictEqual(
      refusal("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nv.m3u8"),
      "UnsupportedCapability",
    );
    assert.strictEqual(
      refusal('#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="k"\nseg.ts'),
      "UnsupportedCapability",
    );
    assert.strictEqual(refusal("#EXTM3U\nseg-0.m4s"), "Protocol");
    assert.strictEqual(refusal("#EXTM3U\nhttps://user:pass@cdn.fixture/seg.ts"), "Protocol");
    assert.strictEqual(refusal("#EXTM3U\na.ts\nb.ts", 1), "Overflow");
  });

  const clip: ClipReady = {
    session_id: "session-1",
    kind: "recording",
    start_marker: 0,
    end_marker: 2,
    now_marker: 2,
    predicted_ready_at_ms: 0n,
    playlist_url: `${api}/clips/c.m3u8`,
  };
  const recordings = (pending: number, segmentBytes = 4) => {
    let waiting = pending;
    return origin((_method, url) => {
      if (url.pathname === "/clips/c.m3u8") {
        if (waiting > 0) {
          waiting--;
          return new Response(null, { status: 202, headers: { "retry-after": "0" } });
        }
        return new Response(playlist);
      }
      if (url.pathname === "/sessions/session-1")
        return Response.json({ session_id: "session-1", state: "ACTIVE" });
      const marker = url.pathname.endsWith("init.mp4")
        ? 9
        : url.pathname.endsWith("seg-0.m4s")
          ? 0
          : 1;
      return new Response(new Uint8Array(segmentBytes).fill(marker));
    });
  };

  it.effect(
    "waits for the playlist, joins the segments, and sends the token only to its origin",
    () =>
      Effect.gen(function* () {
        const { served, client } = recordings(1);
        const service = yield* coordinator(client);
        const fiber = yield* Effect.forkChild(service.downloadClip(clip));
        yield* TestClock.adjust("1 second");
        const downloaded = yield* Fiber.join(fiber);
        assert.deepStrictEqual([...downloaded.bytes], [9, 9, 9, 9, 0, 0, 0, 0, 1, 1, 1, 1]);
        // A pending playlist reads the session before polling again.
        assert.deepStrictEqual(
          served.map((entry) => new URL(entry.url).pathname),
          [
            "/clips/c.m3u8",
            "/sessions/session-1",
            "/clips/c.m3u8",
            "/clips/init.mp4",
            "/clips/seg-0.m4s",
            "/clips/seg-1.m4s",
          ],
        );
        assert.isTrue(
          served.every((entry) => entry.authorized === entry.url.startsWith(service.apiUrl)),
        );
      }),
  );

  it.effect("bounds the joined bytes and the whole download", () =>
    Effect.gen(function* () {
      const bounded = yield* coordinator(recordings(0, 8).client).pipe(
        Effect.flatMap((api) => api.downloadClip(clip, { maxTotalBytes: 16 })),
        Effect.flip,
      );
      assert.strictEqual(bounded.reason._tag, "Overflow");
      const service = yield* coordinator(recordings(1_000).client);
      const late = yield* Effect.forkChild(
        Effect.flip(service.downloadClip(clip, { downloadTimeout: "5 seconds" })),
      );
      yield* TestClock.adjust("6 seconds");
      assert.strictEqual((yield* Fiber.join(late)).reason._tag, "Timeout");
    }),
  );
});

describe("termination", () => {
  const terminating = (removal: number, confirmation: Response) =>
    origin((method) =>
      method === "DELETE" ? new Response(null, { status: removal }) : confirmation,
    );

  it.effect("is confirmed by the independent read, not by the DELETE response", () =>
    Effect.gen(function* () {
      const gone = yield* (yield* coordinator(
        terminating(200, new Response(null, { status: 404 })).client,
      )).terminate("s1");
      assert.strictEqual(gone.confirmed, true);
      assert.strictEqual(gone.evidence, "absent");
      assert.strictEqual(gone.deleteStatus, 200);

      const running = yield* (yield* coordinator(
        terminating(200, Response.json({ session_id: "s1", state: "ACTIVE" })).client,
      )).terminate("s1");
      assert.strictEqual(running.confirmed, false);
      assert.strictEqual(running.state, "ACTIVE");

      const ended = yield* (yield* coordinator(
        terminating(202, Response.json({ session_id: "s1", state: "CLOSED" })).client,
      )).terminate("s1");
      assert.strictEqual(ended.evidence, "terminal");
    }),
  );

  it.effect("stays unconfirmed when the DELETE was refused, even if the session is gone", () =>
    Effect.gen(function* () {
      const refused = yield* (yield* coordinator(
        terminating(403, new Response(null, { status: 404 })).client,
      )).terminate("s1");
      assert.strictEqual(refused.confirmed, false);
      assert.strictEqual(refused.error?.reason, "Http");
    }),
  );
});

describe("tokens", () => {
  const jwt = (seconds: number, model = "reactor/model") => {
    const claims = {
      authorization_details: [
        {
          type: "session",
          resources: { models: { match: [model] } },
          constraints: { max_sessions: 1, max_session_duration_seconds: seconds },
        },
      ],
    };
    return `e30.${Encoding.encodeBase64Url(JSON.stringify(claims))}.sig`;
  };
  const issuing = (token: string, lifetimeSeconds: number) =>
    origin(() => Response.json({ jwt: token, expires_at: lifetimeSeconds }));
  const request = {
    apiKey: Redacted.make("key"),
    modelName: "reactor/model",
    maxSessionDuration: "60 seconds",
    expiresAfter: "10 minutes",
  } as const;

  it.effect("returns the grant the token proves, and never sends the API key as a bearer", () =>
    Effect.gen(function* () {
      const { served, client } = issuing(jwt(60), 3_600);
      const grant = yield* (yield* coordinator(client)).mintToken(request);
      assert.deepStrictEqual(grant.granted, { maxSessions: 1, maxSessionSeconds: 60 });
      assert.isFalse(served[0]?.authorized ?? true);
    }),
  );

  it.effect("refuses a token that grants more than was asked or cannot outlive cleanup", () =>
    Effect.gen(function* () {
      const wider = yield* (yield* coordinator(issuing(jwt(120), 3_600).client))
        .mintToken(request)
        .pipe(Effect.flip);
      assert.strictEqual(wider.reason._tag, "Protocol");
      const brief = yield* (yield* coordinator(issuing(jwt(60), 60).client))
        .mintToken(request)
        .pipe(Effect.flip);
      assert.strictEqual(brief.reason._tag, "Protocol");
      const unbounded = yield* (yield* coordinator(issuing(jwt(60), 3_600).client))
        .mintToken({ ...request, expiresAfter: "70 seconds" })
        .pipe(Effect.flip);
      assert.strictEqual(unbounded.context.outcome, "not-submitted");
    }),
  );
});

it.effect(
  "an HTTP refusal carries its status and delay, and keeps the body out of its message",
  () =>
    Effect.gen(function* () {
      const { client } = origin(
        () =>
          new Response("provider secret text", { status: 429, headers: { "retry-after": "2" } }),
      );
      const error = yield* (yield* coordinator(client)).inspect("s1").pipe(Effect.flip);
      assert.strictEqual(error.reason._tag, "Http");
      assert.isTrue(error.isRetryable);
      assert.isFalse(error.message.includes("secret"));
      assert.isFalse(JSON.stringify(error).includes("secret"));
      assert.strictEqual(
        error.reason._tag === "Http" && Redacted.value(error.reason.body ?? Redacted.make("")),
        "provider secret text",
      );
    }),
);
