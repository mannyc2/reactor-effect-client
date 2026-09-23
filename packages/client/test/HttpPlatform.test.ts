import { expect, test } from "vitest";
import { Cause, Effect, Encoding, Exit, Fiber, Redacted, Stream } from "effect";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as Response from "effect/unstable/http/HttpClientResponse";
import * as Fetch from "effect/unstable/http/FetchHttpClient";
import * as Http from "../src/coordinator/_internal/client.js";
import * as Coordinator from "../src/coordinator/index.js";
import * as FetchHttp from "../src/FetchHttp.js";
import { ReactorError } from "../src/errors.js";

test("coordinator exchanges use the supplied Effect HttpClient", async () => {
  const calls: string[] = [];
  const platform = PlatformHttp.make((request, url) =>
    Effect.sync(() => {
      calls.push(`${request.method} ${url.pathname}`);
      return Response.fromWeb(
        request,
        new globalThis.Response(JSON.stringify({ session_id: "session-one", state: "WAITING" })),
      );
    }),
  );
  const session = await Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* Http.make({ apiUrl: "https://injected.invalid" });
      return yield* client.create({ name: "selected/model" });
    }).pipe(Effect.provideService(PlatformHttp.HttpClient, platform)),
  );
  expect(session.sessionId).toBe("session-one");
  expect(calls).toEqual(["POST /sessions"]);
});

test("create resolves on the session id alone; describe fails with that id", async () => {
  let reply: unknown = { session_id: "session-one", state: "WAITING", capabilities: [] };
  const platform = PlatformHttp.make((request) =>
    Effect.sync(() => Response.fromWeb(request, globalThis.Response.json(reply))),
  );
  const client = new Http.CoordinatorClient({ apiUrl: "https://injected.invalid" }, platform);
  const allocation = await Effect.runPromise(client.create({ name: "selected/model" }));
  expect(allocation.sessionId).toBe("session-one");
  const failure = await Effect.runPromise(Effect.flip(client.describe(allocation)));
  expect(failure).toMatchObject({
    reason: { _tag: "Protocol" },
    context: { operation: "create session", sessionId: "session-one", outcome: "replied" },
  });
  // Without a usable id the allocation itself fails, and names nothing to own.
  for (const unnamed of [{ state: "WAITING" }, { session_id: "", state: "WAITING" }, []]) {
    reply = unnamed;
    const error = await Effect.runPromise(Effect.flip(client.create({ name: "selected/model" })));
    expect(error.reason._tag).toBe("Protocol");
    expect(error.context.sessionId).toBeUndefined();
  }
});

test("empty session ids and model names are lazy typed failures, never defects", async () => {
  let requests = 0;
  const platform = PlatformHttp.make((request) =>
    Effect.sync(() => {
      requests++;
      return Response.fromWeb(request, new globalThis.Response("{}"));
    }),
  );
  const client = new Http.CoordinatorClient({ apiUrl: "https://injected.invalid" }, platform);
  const operations: readonly Effect.Effect<unknown, ReactorError>[] = [
    client.read(""),
    client.create({ name: "" }),
  ];
  for (const operation of operations) {
    const exit = await Effect.runPromiseExit(operation);
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const expected = Cause.findErrorOption(exit.cause);
      expect(expected._tag).toBe("Some");
      if (expected._tag === "Some") expect(expected.value).toBeInstanceOf(ReactorError);
    }
  }
  expect(requests).toBe(0);
});

test("empty bodies differ from JSON null and actual response bytes are bounded", async () => {
  for (const body of ["", "null", "x".repeat(64)]) {
    const platform = PlatformHttp.make((request) =>
      Effect.succeed(Response.fromWeb(request, new globalThis.Response(body))),
    );
    const client = new Http.CoordinatorClient(
      { apiUrl: "https://injected.invalid", maxResponseBytes: 16 },
      platform,
    );
    const result = await Effect.runPromise(Effect.result(client.create({ name: "model" })));
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure")
      expect(result.failure.reason._tag).toBe(body.length > 16 ? "Overflow" : "Protocol");
  }
});

test("request interruption aborts Fetch and releases an owned response reader", async () => {
  let signal: AbortSignal | undefined;
  let body: ReadableStream<Uint8Array> | undefined;
  let cancelled = 0;
  const fakeFetch: typeof globalThis.fetch = Object.assign(
    async (_input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      signal = init?.signal ?? undefined;
      body = new ReadableStream<Uint8Array>({
        cancel() {
          cancelled++;
        },
      });
      return new globalThis.Response(body);
    },
    { preconnect: (_url: string | URL) => undefined },
  );
  const operation = Effect.gen(function* () {
    const client = yield* Http.make({ apiUrl: "https://injected.invalid" });
    return yield* client.create({ name: "model" });
  }).pipe(Effect.provide(FetchHttp.layer), Effect.provideService(Fetch.Fetch, fakeFetch));
  const caller = Effect.runFork(operation);
  const deadline = Date.now() + 2_000;
  while (body?.locked !== true && Date.now() < deadline)
    await new Promise((resolve) => setTimeout(resolve, 1));
  await Effect.runPromise(Fiber.interrupt(caller));
  expect(signal?.aborted).toBe(true);
  expect(body?.locked).toBe(false);
  expect(cancelled).toBe(1);
});

test("a lost DELETE reply is followed by independent remote-termination confirmation", async () => {
  const calls: string[] = [];
  const platform = PlatformHttp.make((request) =>
    Effect.suspend(() => {
      calls.push(request.method);
      if (request.method === "DELETE") return Effect.never;
      return Effect.succeed(
        Response.fromWeb(request, new globalThis.Response("", { status: 404 })),
      );
    }),
  );
  const client = new Http.CoordinatorClient(
    { apiUrl: "https://injected.invalid", requestTimeout: 5 },
    platform,
  );
  const result = await Effect.runPromise(client.terminate("owned-session"));
  expect(result.confirmed).toBe(true);
  expect(result.responseReceived).toBe(false);
  expect(result.evidence).toBe("absent");
  expect(calls).toEqual(["DELETE", "GET"]);
});

const matrixUrl = "https://coordinator.matrix.test/base";
const matrixId = "session / one";
const matrixSessionUrl = `${matrixUrl}/sessions/session%20%2F%20one`;
const matrixOptions: Coordinator.TokenOptions = {
  apiKey: Redacted.make("matrix-key-secret"),
  modelName: "matrix/model",
  maxSessionDuration: "60 seconds",
  expiresAfter: "300 seconds",
};
const matrixToken = {
  jwt: `header.${Encoding.encodeBase64Url(
    JSON.stringify({
      authorization_details: [
        {
          type: "session",
          resources: { models: { match: [matrixOptions.modelName] } },
          constraints: { max_sessions: 1, max_session_duration_seconds: 60 },
        },
      ],
    }),
  )}.signature`,
  expires_at: Date.now() / 1_000 + 3_600,
};
const matrixDescriptor = {
  session_id: matrixId,
  state: "WAITING",
  capabilities: { protocol_version: "1.0", tracks: [] },
  selected_transport: { protocol: "webrtc", version: "1.0" },
};
interface MatrixOperation {
  readonly name: string;
  readonly url: string;
  readonly valid: unknown;
  readonly termination?: boolean;
  readonly nullIsValid?: boolean;
  readonly run: (
    session: Http.CoordinatorClient,
    coordinator: Coordinator.Client,
  ) => Effect.Effect<unknown, ReactorError>;
}
const matrixOperations: readonly MatrixOperation[] = [
  {
    name: "session create",
    url: `${matrixUrl}/sessions`,
    valid: matrixDescriptor,
    run: (session) => session.create({ name: "matrix/model" }),
  },
  {
    name: "session read",
    url: matrixSessionUrl,
    valid: matrixDescriptor,
    run: (session) => session.read(matrixId),
  },
  {
    name: "session ready",
    url: matrixSessionUrl,
    valid: matrixDescriptor,
    run: (session) => session.ready(matrixId),
  },
  {
    name: "session register",
    url: `${matrixSessionUrl}/transport/webrtc/connections`,
    valid: { connection_id: 7 },
    run: (session) => session.register(matrixId),
  },
  {
    name: "session answer",
    url: `${matrixSessionUrl}/transport/webrtc/connections/7/sdp_params`,
    valid: { sdp_answer: "fixture answer" },
    run: (session) => session.answer(matrixId, 7),
  },
  {
    name: "coordinator pricing",
    url: `${matrixUrl}/pricing`,
    valid: { published: true },
    nullIsValid: true,
    run: (_, coordinator) => coordinator.pricing,
  },
  {
    name: "coordinator token",
    url: `${matrixUrl}/tokens`,
    valid: matrixToken,
    run: (_, coordinator) => coordinator.mintToken(matrixOptions),
  },
  {
    name: "coordinator inspect",
    url: matrixSessionUrl,
    valid: matrixDescriptor,
    run: (_, coordinator) => coordinator.inspect(matrixId),
  },
  {
    name: "session terminate",
    url: matrixSessionUrl,
    valid: { state: "CLOSED" },
    termination: true,
    run: (session) => session.terminate(matrixId),
  },
  {
    name: "coordinator terminate",
    url: matrixSessionUrl,
    valid: { state: "CLOSED" },
    termination: true,
    run: (_, coordinator) => coordinator.terminate(matrixId),
  },
];
const responseCases = [
  {
    name: "valid document",
    body: (value: unknown) => JSON.stringify(value),
    status: 200,
    code: undefined,
  },
  { name: "literal null", body: () => "null", status: 200, code: "Protocol" },
  { name: "absent body", body: () => null, status: 200, code: "Protocol" },
  { name: "empty body", body: () => "", status: 200, code: "Protocol" },
  { name: "whitespace body", body: () => " \n ", status: 200, code: "Protocol" },
  { name: "invalid UTF8", body: () => new Uint8Array([0xff]), status: 200, code: "Protocol" },
  { name: "truncated JSON", body: () => '{"state":"CLOSED"', status: 200, code: "Protocol" },
  { name: "success exceeds bytes", body: () => "x".repeat(1025), status: 200, code: "Overflow" },
  { name: "error exceeds bytes", body: () => "x".repeat(1025), status: 503, code: "Overflow" },
  { name: "bodyless HTTP error", body: () => null, status: 503, code: "Http" },
  { name: "empty chunks exceed count", body: () => null, status: 200, code: "Overflow" },
] as const;

for (const operation of matrixOperations)
  for (const sample of responseCases) {
    test(`shared response matrix: ${operation.name}, ${sample.name}`, async () => {
      const calls: string[] = [];
      const platform = PlatformHttp.make((request, url) =>
        Effect.sync(() => {
          calls.push(`${request.method} ${url.href}`);
          if (operation.termination && request.method === "DELETE")
            return Response.fromWeb(request, new globalThis.Response(null, { status: 204 }));
          // Content-Length must never replace accounting for actual streamed bytes.
          const response = Response.fromWeb(
            request,
            new globalThis.Response(sample.body(operation.valid), {
              status: sample.status,
              headers: { "content-length": "1" },
            }),
          );
          return sample.name === "empty chunks exceed count"
            ? (Object.create(response, {
                stream: {
                  value: Stream.fromIterable(Array.from({ length: 5 }, () => new Uint8Array())),
                },
              }) as Response.HttpClientResponse)
            : response;
        }),
      );
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const coordinator = yield* Coordinator.make({
            apiUrl: matrixUrl,
            maxResponseBytes: 1024,
            maxResponseChunks: 4,
            credential: Effect.succeed(Redacted.make("matrix-jwt-secret")),
          });
          const session = new Http.CoordinatorClient(
            {
              apiUrl: matrixUrl,
              maxResponseBytes: 1024,
              maxResponseChunks: 4,
              credential: Effect.succeed("matrix-jwt-secret"),
            },
            platform,
          );
          return yield* Effect.result(operation.run(session, coordinator));
        }).pipe(Effect.provideService(PlatformHttp.HttpClient, platform)),
      );
      const code =
        sample.name === "literal null" && operation.nullIsValid ? undefined : sample.code;
      if (operation.termination) {
        expect(result._tag).toBe("Success");
        if (result._tag !== "Success")
          throw new Error("Termination must retain its evidence report");
        const report = result.success as Coordinator.Termination;
        expect(report.confirmed).toBe(code === undefined);
        expect(report.evidence).toBe(code === undefined ? "terminal" : null);
        expect(report.error?.reason._tag).toBe(code);
        expect(report.deleteStatus).toBe(204);
        expect(calls).toEqual([`DELETE ${operation.url}`, `GET ${operation.url}`]);
      } else {
        expect(result._tag).toBe(code === undefined ? "Success" : "Failure");
        if (result._tag === "Failure") {
          if (code === undefined) throw new Error("A valid response must not fail");
          expect(result.failure.reason._tag).toBe(code);
        }
        if (sample.name === "literal null" && result._tag === "Success")
          expect(result.success).toBeNull();
        expect(calls).toHaveLength(1);
        expect(calls[0]?.slice(calls[0].indexOf(" ") + 1)).toBe(operation.url);
      }
      expect(JSON.stringify(result)).not.toContain("matrix-key-secret");
      expect(JSON.stringify(result)).not.toContain("matrix-jwt-secret");
    });
  }

for (const publicClient of [false, true]) {
  test(`termination evidence matrix through ${publicClient ? "coordinator" : "session"} client`, async () => {
    for (const [deleteStatus, getStatus, body, confirmed, evidence] of [
      [401, 404, null, false, null],
      [403, 404, null, false, null],
      [403, 200, { state: "CLOSED" }, true, "terminal"],
      [404, 200, { state: "ACTIVE" }, false, null],
      [503, 404, null, true, "absent"],
      [204, 200, { session_id: "another", state: "CLOSED" }, false, null],
      [204, 200, { state: "FUTURE_STATE" }, false, null],
      [204, 200, { state: "CLOSED", capabilities: {} }, true, "terminal"],
    ] as const) {
      const calls: string[] = [];
      const platform = PlatformHttp.make((request) =>
        Effect.sync(() => {
          calls.push(request.method);
          return Response.fromWeb(
            request,
            request.method === "DELETE"
              ? new globalThis.Response(null, { status: deleteStatus })
              : new globalThis.Response(body === null ? null : JSON.stringify(body), {
                  status: getStatus,
                }),
          );
        }),
      );
      const report = await Effect.runPromise(
        Effect.gen(function* () {
          const client = publicClient
            ? yield* Coordinator.make({ apiUrl: matrixUrl })
            : new Http.CoordinatorClient({ apiUrl: matrixUrl }, platform);
          return yield* client.terminate(matrixId);
        }).pipe(Effect.provideService(PlatformHttp.HttpClient, platform)),
      );
      expect(report).toMatchObject({
        attempted: true,
        responseReceived: true,
        deleteStatus,
        confirmed,
        evidence,
      });
      expect(calls).toEqual(["DELETE", "GET"]);
      if (deleteStatus === 401 || (deleteStatus === 403 && getStatus === 404))
        expect(report.error).toBeInstanceOf(ReactorError);
      if (body !== null && "session_id" in body) expect(report.error?.reason._tag).toBe("Protocol");
    }
  });
}

test("response bytes are owned even when a supplied client reuses its chunk buffer", async () => {
  const shared = new Uint8Array([1]);
  const platform = PlatformHttp.make((request) =>
    Effect.sync(() => {
      const response = Response.fromWeb(request, new globalThis.Response(null));
      return Object.create(response, {
        stream: {
          value: Stream.concat(
            Stream.succeed(shared),
            Stream.fromEffect(
              Effect.sync(() => {
                shared[0] = 2;
                return shared;
              }),
            ),
          ),
        },
      }) as Response.HttpClientResponse;
    }),
  );
  const client = new Http.CoordinatorClient(
    { apiUrl: matrixUrl, maxResponseChunks: 2, maxResponseBytes: 2 },
    platform,
  );
  const reply = await Effect.runPromise(
    client.request({ operation: "owned body", url: `${matrixUrl}/body` }),
  );
  shared[0] = 3;
  expect([...reply.bytes]).toEqual([1, 2]);
});

test("cross-origin uploads and public coordinator endpoints never receive the session credential", async () => {
  const calls: { url: string; authorization: string | undefined; key: string | undefined }[] = [];
  let credentials = 0;
  const platform = PlatformHttp.make((request, url) =>
    Effect.sync(() => {
      calls.push({
        url: url.href,
        authorization: request.headers.authorization,
        key: request.headers["reactor-api-key"],
      });
      return Response.fromWeb(
        request,
        url.hostname === "upload.matrix.test"
          ? new globalThis.Response(null, { status: 204 })
          : globalThis.Response.json(
              url.pathname.endsWith("/tokens") ? matrixToken : { published: true },
            ),
      );
    }),
  );
  const client = new Http.CoordinatorClient(
    { apiUrl: matrixUrl, credential: Effect.sync(() => `secret-${++credentials}`) },
    platform,
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* client.pricing;
      yield* client.mintToken(matrixOptions);
      yield* client.putUpload(
        {
          presigned_id: "upload",
          presigned_url: "https://upload.matrix.test/object",
          path: "object",
        },
        new Uint8Array([1]),
        "image/png",
      );
    }),
  );
  expect(credentials).toBe(0);
  expect(calls.map(({ authorization }) => authorization)).toEqual([
    undefined,
    undefined,
    undefined,
  ]);
  expect(calls.map(({ key }) => key)).toEqual([undefined, "matrix-key-secret", undefined]);
});
