/** Reactor's HTTP API over an in-memory origin: what counts as proof, and who gets the token. */
import { assert, describe, it, layer } from "@effect/vitest";
import { fileURLToPath } from "node:url";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { Effect, Fiber, FileSystem, Redacted, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import { Coordinator } from "../src/index.js";
import { create } from "@bufbuild/protobuf";
import { ClipReadySchema } from "../src/internal/wire.js";

const api = "https://api.fixture";
const cdn = "https://cdn.fixture";

interface Served {
  readonly method: string;
  readonly url: string;
  readonly authorized: boolean;
  readonly bearer: string | undefined;
  readonly body: string;
}

type Route = (method: string, url: URL) => Response;

/** An origin that answers from `route` and records every request it served. */
const origin = (route: Route) => {
  const served: Array<Served> = [];
  const client = HttpClient.make((request, url) =>
    Effect.sync(() => {
      const body = request.body;
      served.push({
        method: request.method,
        url: url.href,
        authorized: request.headers.authorization !== undefined,
        bearer: request.headers.authorization?.replace(/^Bearer /, ""),
        body: body._tag === "Uint8Array" ? new TextDecoder().decode(body.body) : "",
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

  const clip = create(ClipReadySchema, {
    sessionId: "session-1",
    kind: "recording",
    endMarker: 2,
    nowMarker: 2,
    playlistUrl: `${api}/clips/c.m3u8`,
  });
  const recordings = (pending: number, segmentBytes = 4, state = "ACTIVE") => {
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
        return Response.json({ session_id: "session-1", state });
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

  // Reactor's docs: a recording is kept 24 h and plays after its session ends; it may finish then.
  it.effect("waits for a recording of an ended session until its predicted ready time", () =>
    Effect.gen(function* () {
      const finishing = create(ClipReadySchema, { ...clip, predictedReadyAtMs: 5_000n });
      const service = yield* coordinator(recordings(2, 4, "CLOSED").client);
      const fiber = yield* Effect.forkChild(service.downloadClip(finishing));
      yield* TestClock.adjust("2 seconds");
      assert.strictEqual((yield* Fiber.join(fiber)).bytes.byteLength, 12);
      const overdue = yield* coordinator(recordings(1_000, 4, "CLOSED").client);
      const late = yield* Effect.forkChild(Effect.flip(overdue.downloadClip(finishing)));
      yield* TestClock.adjust("20 seconds");
      assert.strictEqual((yield* Fiber.join(late)).reason._tag, "TerminalSession");
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
  it.effect("with no session token, a server ends a session with its API key as the bearer", () =>
    Effect.gen(function* () {
      const { served, client } = origin((method) =>
        method === "DELETE"
          ? new Response(null, { status: 202 })
          : new Response(null, { status: 404 }),
      );
      const service = yield* Coordinator.make({ apiUrl: api, apiKey: Redacted.make("key") }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      const ended = yield* service.terminate("s1");
      assert.strictEqual(ended.confirmed, true);
      assert.deepStrictEqual(
        served.map((entry) => [entry.method, entry.bearer]),
        [
          ["DELETE", "key"],
          ["GET", "key"],
        ],
      );
    }),
  );

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
  const echo = (constraints: object, bind?: ReadonlyArray<string>) => [
    {
      type: "session",
      resources: {
        models: { match: ["reactor/model"] },
        ...(bind === undefined ? {} : { sessions: { bind } }),
      },
      constraints,
    },
  ];
  const issuing = (reply: object) => origin(() => Response.json({ jwt: "e30.e30.sig", ...reply }));
  const request = {
    apiKey: Redacted.make("key"),
    modelName: "reactor/model",
    maxSessionDuration: "60 seconds",
    expiresAfter: "10 minutes",
  } as const;
  const sent = (served: ReadonlyArray<Served>): unknown => JSON.parse(served[0]?.body ?? "null");

  it.effect("returns the grant Reactor echoes, and never sends the API key as a bearer", () =>
    Effect.gen(function* () {
      const { served, client } = issuing({
        expires_at: 3_600,
        authorization_details: echo({ max_sessions: 1, max_session_duration_seconds: 60 }),
      });
      const grant = yield* (yield* coordinator(client)).mintToken(request);
      assert.deepStrictEqual(grant.granted, {
        models: ["reactor/model"],
        maxSessions: 1,
        maxSessionSeconds: 60,
        bound: [],
      });
      assert.strictEqual(grant.maxSessionSeconds, 60);
      assert.isFalse(served[0]?.authorized ?? true);
      assert.deepStrictEqual(sent(served), {
        authorization_details: echo({ max_sessions: 1, max_session_duration_seconds: 60 }),
        expires_after: 600,
      });
    }),
  );

  // Reactor documents the reply's echo, not the token's claims; a session outlives its token.
  it.effect("takes a token shorter than its session, and one whose reply echoes nothing", () =>
    Effect.gen(function* () {
      const brief = yield* (yield* coordinator(issuing({ expires_at: 30 }).client)).mintToken(
        request,
      );
      assert.deepStrictEqual([brief.expiresAt, brief.granted], [30, undefined]);
    }),
  );

  it.effect("refuses a grant wider than asked, and a token already expired", () =>
    Effect.gen(function* () {
      const refusal = (reply: object) =>
        Effect.gen(function* () {
          const error = yield* (yield* coordinator(issuing(reply).client))
            .mintToken({ ...request, bind: ["s1"], maxSessions: 2 })
            .pipe(Effect.flip);
          return `${error.reason._tag}/${error.context.outcome ?? ""}`;
        });
      const refused = "Protocol/replied";
      assert.strictEqual(
        yield* refusal({
          expires_at: 3_600,
          authorization_details: echo({ max_session_duration_seconds: 120 }, ["s1"]),
        }),
        refused,
      );
      assert.strictEqual(
        yield* refusal({
          expires_at: 3_600,
          authorization_details: echo({ max_session_duration_seconds: null }, ["s1"]),
        }),
        refused,
      );
      assert.strictEqual(
        yield* refusal({ expires_at: 3_600, authorization_details: echo({}, ["s1", "s2"]) }),
        refused,
      );
      assert.strictEqual(yield* refusal({ expires_at: 0 }), refused);
    }),
  );

  it.effect("mints an uncapped session only when asked, and a bound token that creates none", () =>
    Effect.gen(function* () {
      const missing = yield* (yield* coordinator(issuing({ expires_at: 3_600 }).client))
        .mintToken({ ...request, maxSessionDuration: undefined })
        .pipe(Effect.flip);
      assert.deepStrictEqual(
        [missing.reason._tag, missing.context.outcome],
        ["InvalidInput", "not-submitted"],
      );
      const uncapped = issuing({ expires_at: 3_600 });
      yield* (yield* coordinator(uncapped.client)).mintToken({
        ...request,
        maxSessionDuration: "unlimited",
      });
      assert.deepStrictEqual(sent(uncapped.served), {
        authorization_details: echo({ max_sessions: 1 }),
        expires_after: 600,
      });
      const bound = issuing({ expires_at: 3_600 });
      const service = yield* coordinator(bound.client);
      yield* service.tokens({ ...request, maxSessionDuration: "unlimited" }).bind("s1");
      assert.deepStrictEqual(sent(bound.served), {
        authorization_details: [
          {
            type: "session",
            resources: { models: { match: ["reactor/model"] }, sessions: { bind: ["s1"] } },
          },
        ],
        expires_after: 600,
      });
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
      const json = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(error);
      assert.isFalse(json.includes("secret"));
      assert.strictEqual(
        error.reason._tag === "Http" && Redacted.value(error.reason.body ?? Redacted.make("")),
        "provider secret text",
      );
    }),
);

// Reactor's JS SDK types class a 5xx SERVER_ERROR as recoverable, and 401/403 and 409 as not.
it.effect("a server error is retryable; a refusal of authority or a conflict is not", () =>
  Effect.gen(function* () {
    const retryable = (status: number) =>
      Effect.gen(function* () {
        const { client } = origin(() => new Response(null, { status }));
        const error = yield* (yield* coordinator(client)).inspect("s1").pipe(Effect.flip);
        return error.isRetryable;
      });
    assert.deepStrictEqual(
      [yield* retryable(502), yield* retryable(504), yield* retryable(403), yield* retryable(409)],
      [true, true, false, false],
    );
  }),
);

// Reactor's FAQ asks for the SDK version in bug reports; its logs read it from client_info.
layer(NodeFileSystem.layer)("the SDK version", (it) => {
  it.effect("names this package's version to Reactor", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const manifest = yield* fs.readFileString(
        fileURLToPath(new URL("../package.json", import.meta.url)),
      );
      const { version } = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
      )(manifest);
      const { served, client } = origin(() =>
        Response.json({ session_id: "s1", state: "PENDING" }),
      );
      const service = yield* coordinator(client);
      yield* service.signaling(Effect.undefined).create({ name: "reactor/model" });
      assert.include(served[0]?.body ?? "", `"sdk_version":"${version}"`);
    }),
  );
});
