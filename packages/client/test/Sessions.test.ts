import { describe, expect, test } from "bun:test";
import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Encoding,
  Exit,
  Fiber,
  Layer,
  Redacted,
  Stream,
  Tracer,
} from "effect";
import { TestClock } from "effect/testing";
import {
  FetchHttpClient,
  Headers as HttpHeaders,
  HttpClient,
  HttpClientResponse,
} from "effect/unstable/http";
import * as Coordinator from "../src/coordinator/index.js";
import { ReactorError } from "../src/errors.js";

type RequestFixture = (url: string, init: RequestInit) => Promise<Response>;
const fetchLayer = (request: RequestFixture) =>
  Layer.fresh(FetchHttpClient.layer).pipe(
    Layer.provide(
      Layer.succeed(
        FetchHttpClient.Fetch,
        Object.assign(
          (input: string | Request | URL, init?: RequestInit) => request(String(input), init ?? {}),
          { preconnect: fetch.preconnect },
        ),
      ),
    ),
  );

const options: Coordinator.TokenOptions = {
  apiKey: Redacted.make("rk_secret_fixture"),
  modelName: "reactor/fast-h3",
  maxSessionDurationSeconds: 120,
  expiresAfterSeconds: 300,
};
const tokenWith = (claims: unknown) =>
  `${Encoding.encodeBase64Url('{"alg":"HS256","typ":"JWT"}')}.${Encoding.encodeBase64Url(JSON.stringify(claims))}.secret_token_fixture`;
const authorization = (
  constraints: unknown = { max_sessions: 1, max_session_duration_seconds: 120 },
) => ({
  type: "session",
  resources: { models: { match: [options.modelName] } },
  constraints,
});
const fixtureJwt = tokenWith({ authorization_details: [authorization()] });
const fresh = () => ({ jwt: fixtureJwt, expires_at: Date.now() / 1_000 + 300 });
const inspectionToken = Redacted.make("secret_token_fixture");

// These helpers compose the configured public API; all operations still execute
// on the canonical class that the session core uses.
const client = (jwt: Redacted.Redacted<string> = inspectionToken) =>
  Coordinator.make({
    apiUrl: "https://configured.fixture/api",
    credential: Effect.succeed(jwt),
  });
const pricing = () => client().pipe(Effect.flatMap((coordinator) => coordinator.pricing()));
const mintToken = (input: Coordinator.TokenOptions) =>
  client().pipe(Effect.flatMap((coordinator) => coordinator.mintToken(input)));
const inspect = (jwt: Redacted.Redacted<string>, sessionId: string) =>
  client(jwt).pipe(Effect.flatMap((coordinator) => coordinator.inspect(sessionId)));
const terminate = (jwt: Redacted.Redacted<string>, sessionId: string) =>
  client(jwt).pipe(Effect.flatMap((coordinator) => coordinator.terminate(sessionId)));

describe("Reactor HTTP session contract (offline)", () => {
  test("model pricing preserves exact credit ratios and rejects ambiguous economics", async () => {
    const entry = {
      name: options.modelName,
      rate: { amount_per_sec: 7, unit: "credits", denomination: "second" },
    };
    const facts = {
      settings: { currency_code: "USD", credits_per_dollar: 1_000 },
      models: [entry],
    };
    expect(await Effect.runPromise(Coordinator.modelRate(facts, options.modelName))).toEqual({
      creditsPerDollar: 1_000,
      creditsPerSecond: 7,
    });
    for (const invalid of [
      { ...facts, models: [] },
      { ...facts, models: [entry, entry] },
      { ...facts, settings: { ...facts.settings, currency_code: "EUR" } },
      { ...facts, settings: { ...facts.settings, credits_per_dollar: 1.5 } },
      ...[0, -1, 1.5, Infinity].map((amount_per_sec) => ({
        ...facts,
        models: [{ ...entry, rate: { ...entry.rate, amount_per_sec } }],
      })),
      { ...facts, models: [{ ...entry, rate: { ...entry.rate, unit: "dollars" } }] },
      { ...facts, models: [{ ...entry, rate: { ...entry.rate, denomination: "minute" } }] },
    ]) {
      const failure = await Effect.runPromise(
        Effect.flip(Coordinator.modelRate(invalid, options.modelName)),
      );
      expect(failure).toBeInstanceOf(ReactorError);
      expect(failure).toMatchObject({ code: "Protocol", context: { operation: "pricing" } });
    }
  });

  test("reads public pricing through the supplied client without applying spending policy", async () => {
    const facts = {
      settings: { currency_code: "USD", credits_per_dollar: 1_000 },
      models: [],
      unpublished: { rate: 7 },
    };
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const value = await Effect.runPromise(
      pricing().pipe(
        Effect.provide(
          fetchLayer(async (url, init) => {
            calls.push({ url, init });
            return Response.json(facts);
          }),
        ),
      ),
    );
    expect(value).toEqual(facts);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe("https://configured.fixture/api/pricing");
    expect(calls[0]?.init.method).toBe("GET");
    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.has("authorization")).toBe(false);
    expect(headers.has("reactor-api-key")).toBe(false);
  });

  test("mints one model-scoped, server-capped token without exposing the credential", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const request: RequestFixture = async (url, init) => {
      calls.push({ url, init });
      return Response.json(fresh());
    };
    const token = await Effect.runPromise(
      mintToken(options).pipe(Effect.provide(fetchLayer(request))),
    );
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe("https://configured.fixture/api/tokens");
    expect(JSON.parse(await new Response(calls[0]?.init.body).text())).toEqual({
      expires_after: 300,
      authorization_details: [
        {
          type: "session",
          resources: { models: { match: ["reactor/fast-h3"] } },
          constraints: { max_sessions: 1, max_session_duration_seconds: 120 },
        },
      ],
    });
    expect(Redacted.value(token.jwt)).toBe(fixtureJwt);
    expect(token.granted).toEqual({ maxSessions: 1, maxSessionSeconds: 120 });
    expect(JSON.stringify(token)).not.toContain("secret_token_fixture");
    expect(calls[0]?.init.headers).toMatchObject({
      "reactor-api-key": "rk_secret_fixture",
      "content-type": "application/json",
    });
  });

  test("pricing requires a JSON document while preserving literal JSON null", async () => {
    const value = await Effect.runPromise(
      pricing().pipe(Effect.provide(fetchLayer(async () => new Response("null")))),
    );
    expect(value).toBeNull();
    for (const body of ["", "  ", "secret_token_fixture"]) {
      const failure = await Effect.runPromise(
        Effect.flip(pricing().pipe(Effect.provide(fetchLayer(async () => new Response(body))))),
      );
      expect(failure).toBeInstanceOf(ReactorError);
      expect(failure).toMatchObject({
        context: { operation: "pricing" },
        message: "Reactor pricing request or response failed",
      });
      expect(JSON.stringify(failure)).not.toContain("secret_token_fixture");
    }
  });

  test("rejects malformed responses, insufficient expiry and HTTP failure with sanitized errors", async () => {
    for (const response of [
      Response.json({ jwt: 12, expires_at: "secret_token_fixture" }),
      Response.json({ ...fresh(), expires_at: Date.now() / 1_000 + 10 }),
      ...[401, 429, 503].map((status) => new Response("secret_token_fixture", { status })),
      new Response("secret_token_fixture"),
    ]) {
      let calls = 0;
      const exit = await Effect.runPromiseExit(
        mintToken(options).pipe(
          Effect.provide(
            fetchLayer(async () => {
              calls++;
              return response;
            }),
          ),
        ),
      );
      expect(exit._tag).toBe("Failure");
      expect(JSON.stringify(exit)).not.toContain("secret_token_fixture");
      expect(JSON.stringify(exit)).not.toContain("rk_secret_fixture");
      expect(calls).toBe(1);
    }
  });

  test("rejects unreadable tokens instead of treating missing grant information as a cap", async () => {
    for (const jwt of [
      "fixture-token",
      "header..signature",
      "header.$$$.signature",
      `${fixtureJwt}.extra`,
      "header.bnVsbA.signature",
      tokenWith({}),
    ]) {
      let calls = 0;
      const failure = await Effect.runPromise(
        Effect.flip(
          mintToken(options).pipe(
            Effect.provide(
              fetchLayer(async () => {
                calls++;
                return Response.json({ ...fresh(), jwt });
              }),
            ),
          ),
        ),
      );
      expect(failure).toBeInstanceOf(ReactorError);
      expect(failure).toMatchObject({
        context: { operation: "token" },
        message: "Reactor returned an invalid or unbounded session grant",
      });
      expect(JSON.stringify(failure)).not.toContain(jwt);
      expect(calls).toBe(1);
    }
  });

  test("requires one bounded grant for exactly the requested model and no additional authority", async () => {
    const valid = authorization();
    const invalid: ReadonlyArray<unknown> = [
      null,
      {},
      [],
      [null],
      [valid, valid],
      [valid, authorization({ max_sessions: 10, max_session_duration_seconds: 600 })],
      [{ ...valid, type: "account" }],
      [{ ...valid, actions: ["*"] }],
      [{ ...valid, resources: {} }],
      [{ ...valid, resources: { models: { match: [] } } }],
      [{ ...valid, resources: { models: { match: ["reactor/another-model"] } } }],
      [
        {
          ...valid,
          resources: { models: { match: [options.modelName, "reactor/another-model"] } },
        },
      ],
      [{ ...valid, resources: { ...valid.resources, sessions: { bind: ["another-session"] } } }],
      [authorization({})],
      [authorization({ max_sessions: 1 })],
      ...[0, -1, 2, 1.5, "1", null].map((max_sessions) => [
        authorization({ max_sessions, max_session_duration_seconds: 120 }),
      ]),
      ...[0, -1, 121, 1.5, "120", null].map((max_session_duration_seconds) => [
        authorization({ max_sessions: 1, max_session_duration_seconds }),
      ]),
    ];
    for (const authorization_details of invalid) {
      const jwt = tokenWith({ authorization_details });
      let calls = 0;
      const failure = await Effect.runPromise(
        Effect.flip(
          mintToken(options).pipe(
            Effect.provide(
              fetchLayer(async () => {
                calls++;
                return Response.json({ ...fresh(), jwt });
              }),
            ),
          ),
        ),
      );
      expect(failure).toMatchObject({
        context: { operation: "token" },
        message: "Reactor returned an invalid or unbounded session grant",
      });
      expect(JSON.stringify(failure)).not.toContain(jwt);
      expect(JSON.stringify(failure)).not.toContain("secret_token_fixture");
      expect(calls).toBe(1);
    }
  });

  test("returns the actual validated cap while allowing unrelated JWT identity claims", async () => {
    for (const seconds of [1, 60, 120]) {
      const jwt = tokenWith({
        sub: "fixture subject ☀",
        exp: Date.now() / 1_000 + 300,
        authorization_details: [
          authorization({ max_sessions: 1, max_session_duration_seconds: seconds }),
        ],
      });
      const token = await Effect.runPromise(
        mintToken(options).pipe(
          Effect.provide(fetchLayer(async () => Response.json({ ...fresh(), jwt }))),
        ),
      );
      expect(token.granted).toEqual({ maxSessions: 1, maxSessionSeconds: seconds });
      expect(Redacted.value(token.jwt)).toBe(jwt);
    }
  });

  test("keeps rejected issued grants out of the public error and tracing failure", async () => {
    const jwt = tokenWith({
      authorization_details: [
        authorization({ max_sessions: 1, max_session_duration_seconds: "secret_token_fixture" }),
      ],
    });
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const failure = await Effect.runPromise(
      Effect.flip(
        mintToken(options).pipe(
          Effect.provide(fetchLayer(async () => Response.json({ ...fresh(), jwt }))),
          Effect.provideService(Tracer.Tracer, tracer),
        ),
      ),
    );
    expect(failure).toMatchObject({ context: { operation: "token" } });
    expect(spans.map((span) => span.name)).toEqual(["reactor.coordinator.mintToken"]);
    const status = spans[0]?.status;
    if (status?._tag !== "Ended" || !Exit.isFailure(status.exit))
      throw new Error("Expected the failed Coordinator span");
    const recorded = JSON.stringify(failure) + Cause.pretty(status.exit.cause);
    expect(recorded).not.toContain(jwt);
    expect(recorded).not.toContain("secret_token_fixture");
    expect(recorded).not.toContain("rk_secret_fixture");
  });

  test("uses the injected clock for token cleanup lifetime and inspection timestamps", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* TestClock.setTime(100_000);
        const token = yield* mintToken(options);
        expect(token.expiresAt).toBe(250);
        // Exactly enough cleanup time is valid; advancing one millisecond must
        // reject the same grant even when the host wall clock is years later.
        yield* TestClock.adjust(1);
        const expired = yield* Effect.flip(mintToken(options));
        expect(expired.message).toContain("without enough lifetime");
        const inspection = yield* inspect(inspectionToken, "session");
        expect(inspection.observedAt).toBe(100_001);
        expect(inspection.observedAt).toBe(yield* Clock.currentTimeMillis);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            fetchLayer(async (url) =>
              url.endsWith("/tokens")
                ? Response.json({ jwt: fixtureJwt, expires_at: 250 })
                : Response.json({ session_id: "session", state: "WAITING" }),
            ),
            TestClock.layer(),
          ),
        ),
      ),
    );
  });

  test("redacts custom API keys and standard bearer headers when the supplied client inspects requests", async () => {
    const headers: Array<Record<string, string | Redacted.Redacted>> = [];
    const client = HttpClient.make((request, url, _signal, fiber) =>
      Effect.sync(() => {
        headers.push(
          HttpHeaders.redact(request.headers, fiber.getRef(HttpHeaders.CurrentRedactedNames)),
        );
        return HttpClientResponse.fromWeb(
          request,
          Response.json(
            url.pathname === "/api/tokens" ? fresh() : { session_id: "session", state: "WAITING" },
          ),
        );
      }),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* mintToken(options);
        yield* inspect(inspectionToken, "session");
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(headers).toHaveLength(2);
    expect(Redacted.isRedacted(headers[0]?.["reactor-api-key"])).toBe(true);
    expect(Redacted.isRedacted(headers[1]?.authorization)).toBe(true);
    expect(JSON.stringify(headers)).not.toContain("rk_secret_fixture");
    expect(JSON.stringify(headers)).not.toContain("secret_token_fixture");
  });

  test("does not retry token issuance or expose transport failure credentials through tracing", async () => {
    let calls = 0;
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const failure = await Effect.runPromise(
      Effect.flip(
        mintToken(options).pipe(
          Effect.provide(
            fetchLayer(async () => {
              calls++;
              throw new Error("Reactor-API-Key: rk_secret_fixture; secret_token_fixture");
            }),
          ),
          Effect.provideService(Tracer.Tracer, tracer),
        ),
      ),
    );
    expect(calls).toBe(1);
    expect(failure).toMatchObject({ context: { operation: "token" } });
    expect(JSON.stringify(failure)).not.toContain("rk_secret_fixture");
    expect(JSON.stringify(failure)).not.toContain("secret_token_fixture");
    expect(spans.map((span) => span.name)).toEqual(["reactor.coordinator.mintToken"]);
    const status = spans[0]?.status;
    if (status?._tag !== "Ended" || !Exit.isFailure(status.exit))
      throw new Error("Expected the failed Coordinator span");
    expect(Cause.pretty(status.exit.cause)).not.toContain("rk_secret_fixture");
    expect(Cause.pretty(status.exit.cause)).not.toContain("secret_token_fixture");
  });

  test("keeps credential-bearing response headers and token bodies out of successful traces", async () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    await Effect.runPromise(
      mintToken(options).pipe(
        Effect.provide(
          fetchLayer(async () =>
            Response.json(fresh(), { headers: { "x-provider-debug": "secret_token_fixture" } }),
          ),
        ),
        Effect.provideService(Tracer.Tracer, tracer),
      ),
    );
    expect(spans.map((span) => span.name)).toEqual(["reactor.coordinator.mintToken"]);
    const status = spans[0]?.status;
    if (status?._tag !== "Ended" || !Exit.isSuccess(status.exit))
      throw new Error("Expected the successful Coordinator span");
    const recorded = JSON.stringify(
      spans.map((span) => ({
        name: span.name,
        attributes: Object.fromEntries(span.attributes),
        exit: span.status._tag === "Ended" ? span.status.exit : undefined,
      })),
    );
    expect(recorded).not.toContain("secret_token_fixture");
    expect(recorded).not.toContain("rk_secret_fixture");
  });

  test("rejects invalid bounds before making a request", async () => {
    let calls = 0;
    const request: RequestFixture = async () => {
      calls++;
      return Response.json(fresh());
    };
    for (const invalid of [
      { ...options, maxSessionDurationSeconds: 0 },
      { ...options, expiresAfterSeconds: 130 },
      { ...options, modelName: "" },
    ]) {
      await expect(
        Effect.runPromise(mintToken(invalid).pipe(Effect.provide(fetchLayer(request)))),
      ).rejects.toThrow();
    }
    expect(calls).toBe(0);
  });

  test("cancelling a request aborts its actual fetch signal", async () => {
    let entered = false;
    let aborted = false;
    const request: RequestFixture = (_, init) =>
      new Promise((_resolve, reject) => {
        entered = true;
        init.signal?.addEventListener(
          "abort",
          () => {
            aborted = true;
            reject(new Error("fixture aborted"));
          },
          { once: true },
        );
      });
    await Effect.runPromise(
      Effect.gen(function* () {
        const pending = yield* mintToken(options).pipe(
          Effect.provide(fetchLayer(request)),
          Effect.forkChild,
        );
        while (!entered) yield* Effect.yieldNow;
        yield* Fiber.interrupt(pending);
      }),
    );
    expect(aborted).toBe(true);
  });

  test("inspection only reads the observed session and projects safe coordinator facts", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const before = Date.now();
    const result = await Effect.runPromise(
      inspect(inspectionToken, "session / one").pipe(
        Effect.provide(
          fetchLayer(async (url, init) => {
            calls.push({ url, init });
            return Response.json({
              session_id: "session / one",
              state: "WAITING",
              jwt: "secret_token_fixture",
              capabilities: {
                protocol_version: "1.0",
                tracks: [],
                private: "secret_token_fixture",
              },
              selected_transport: {
                protocol: "webrtc",
                version: "1.0",
                credentials: "secret_token_fixture",
              },
              signaling_url: "https://private.example/secret_token_fixture",
              // A real coordinator reports placement alongside state; a distant zone is
              // what distinguishes a slow readiness poll from a broken one.
              cluster: "416e6639-c40b-4329-ba7d-82df1b4cdfb2",
              zone: "ap-southeast",
              server_info: { server_version: "1.20260911.25926" },
              // Unmodeled scalars such as a close reason must survive; nested values
              // and credential-like names must not.
              close_reason: "max_duration",
              created_at: 1789358543,
              billable: true,
              ended_at: null,
              auth_context: "secret_token_fixture",
              ice_servers: [{ credential: "secret_token_fixture" }],
              description: `${"long ".repeat(100)}secret_token_fixture`,
            });
          }),
        ),
      ),
    );
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe("https://configured.fixture/api/sessions/session%20%2F%20one");
    expect(calls[0]?.init.method).toBe("GET");
    expect(calls[0]?.init.body).toBeUndefined();
    expect(calls[0]?.init.headers).toMatchObject({
      authorization: "Bearer secret_token_fixture",
      "reactor-api-version": "1",
      "reactor-api-accept-version": "1",
    });
    expect(result).toEqual({
      observedAt: result.observedAt,
      state: "WAITING",
      hasCapabilities: true,
      selectedTransport: { protocol: "webrtc", version: "1.0" },
      cluster: "416e6639-c40b-4329-ba7d-82df1b4cdfb2",
      zone: "ap-southeast",
      serverVersion: "1.20260911.25926",
      additional: {
        close_reason: "max_duration",
        created_at: 1789358543,
        billable: true,
        ended_at: null,
        description: "long ".repeat(40),
      },
    });
    expect(result.observedAt).toBeGreaterThanOrEqual(before);
    expect(result.observedAt).toBeLessThanOrEqual(Date.now());
    expect(JSON.stringify(result)).not.toContain("secret_token_fixture");
  });

  test("inspection distinguishes absent and null fields from partially published connection facts", async () => {
    const transport = { protocol: "webrtc", version: "1.0" };
    for (const [fields, hasCapabilities, selectedTransport] of [
      [{}, false, null],
      [{ capabilities: null, selected_transport: null }, false, null],
      [{ capabilities: {}, selected_transport: null }, true, null],
      [{ capabilities: null, selected_transport: transport }, false, transport],
    ] as const) {
      const result = await Effect.runPromise(
        inspect(inspectionToken, "session").pipe(
          Effect.provide(
            fetchLayer(async () =>
              Response.json({ session_id: "session", state: "WAITING", ...fields }),
            ),
          ),
        ),
      );
      expect(result).toMatchObject({ state: "WAITING", hasCapabilities, selectedTransport });
    }
  });

  test("inspection rejects invalid input before any request", async () => {
    let calls = 0;
    const request: RequestFixture = async () => {
      calls++;
      return Response.json({ session_id: "session", state: "WAITING" });
    };
    for (const [token, sessionId] of [
      [inspectionToken, ""],
      [null as unknown as Redacted.Redacted<string>, "session"],
    ] as const) {
      const error = await Effect.runPromise(
        Effect.flip(inspect(token, sessionId).pipe(Effect.provide(fetchLayer(request)))),
      );
      expect(error).toMatchObject({ context: { operation: "inspect" } });
    }
    expect(calls).toBe(0);
  });

  test("inspection preserves HTTP status without exposing response bodies or retrying", async () => {
    for (const status of [401, 403, 404, 429, 503]) {
      let calls = 0;
      const error = await Effect.runPromise(
        Effect.flip(
          inspect(inspectionToken, "session").pipe(
            Effect.provide(
              fetchLayer(async () => {
                calls++;
                return new Response("secret_token_fixture", { status });
              }),
            ),
          ),
        ),
      );
      expect(error).toMatchObject({ context: { operation: "inspect", status } });
      expect(error.message).toContain(`HTTP ${status}`);
      expect(JSON.stringify(error)).not.toContain("secret_token_fixture");
      expect(calls).toBe(1);
    }
  });

  test("inspection rejects mismatched identity and malformed descriptions without exposing input", async () => {
    for (const [response, message] of [
      [
        Response.json({ session_id: "secret_token_fixture", state: "WAITING" }),
        "different session identity",
      ],
      [
        Response.json({ state: "WAITING", private: "secret_token_fixture" }),
        "invalid session description",
      ],
      [
        Response.json({ session_id: "session", state: 1, private: "secret_token_fixture" }),
        "invalid session description",
      ],
      [
        Response.json({
          session_id: "session",
          state: "WAITING",
          capabilities: ["secret_token_fixture"],
        }),
        "invalid session description",
      ],
      [
        Response.json({
          session_id: "session",
          state: "WAITING",
          selected_transport: { protocol: "secret_token_fixture" },
        }),
        "invalid session description",
      ],
      [new Response("secret_token_fixture"), "request or response failed"],
    ] as const) {
      let calls = 0;
      const error = await Effect.runPromise(
        Effect.flip(
          inspect(inspectionToken, "session").pipe(
            Effect.provide(
              fetchLayer(async () => {
                calls++;
                return response;
              }),
            ),
          ),
        ),
      );
      expect(error).toMatchObject({ context: { operation: "inspect" } });
      expect(error.message).toContain(message);
      expect(error.context.status).toBe(message === "request or response failed" ? 200 : undefined);
      expect(JSON.stringify(error)).not.toContain("secret_token_fixture");
      expect(calls).toBe(1);
    }
    const error = await Effect.runPromise(
      Effect.flip(
        inspect(inspectionToken, "session").pipe(
          Effect.provide(
            fetchLayer(async () => {
              throw new Error("Authorization: Bearer secret_token_fixture");
            }),
          ),
        ),
      ),
    );
    expect(error).toMatchObject({ context: { operation: "inspect" } });
    expect(JSON.stringify(error)).not.toContain("secret_token_fixture");
  });

  test("cancelling inspection aborts the in-flight request", async () => {
    let signal: AbortSignal | undefined;
    let calls = 0;
    const request: RequestFixture = (_, init) =>
      new Promise((_resolve, reject) => {
        calls++;
        signal = init.signal ?? undefined;
        signal?.addEventListener("abort", () => reject(new Error("secret_token_fixture")), {
          once: true,
        });
      });
    await Effect.runPromise(
      Effect.gen(function* () {
        const pending = yield* inspect(inspectionToken, "session").pipe(
          Effect.provide(fetchLayer(request)),
          Effect.forkChild,
        );
        while (signal === undefined) yield* Effect.yieldNow;
        yield* Fiber.interrupt(pending);
      }),
    );
    expect(signal?.aborted).toBe(true);
    expect(calls).toBe(1);
  });

  for (const [operation, deadline] of [
    ["pricing", 8_000],
    ["token", 8_000],
    ["inspect", 1_000],
    ["terminate", 3_000],
  ] as const) {
    for (const phase of ["headers", "body"] as const) {
      test(`${operation}'s Effect deadline includes stalled ${phase}`, async () => {
        let bodyStarted = false;
        let completed = false;
        const methods: Array<string> = [];
        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            methods.push(request.method);
            if (operation === "terminate" && request.method === "DELETE")
              return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }));
            if (phase === "headers") return yield* Effect.never;
            // Consuming part of the budget before headers arrive distinguishes one
            // end-to-end deadline from a fresh timeout attached to body decoding.
            yield* Effect.sleep(deadline / 2);
            const body = Effect.gen(function* () {
              bodyStarted = true;
              return yield* Effect.never;
            });
            return Object.defineProperties(HttpClientResponse.fromWeb(request, Response.json({})), {
              stream: { value: Stream.fromEffect(body) },
            });
          }),
        );
        const request: Effect.Effect<unknown, ReactorError, HttpClient.HttpClient> =
          operation === "pricing"
            ? pricing()
            : operation === "token"
              ? mintToken(options)
              : inspect(inspectionToken, "session");
        // Termination uncertainty moved from the error channel into its shared
        // evidence report; retain the same typed-error and deadline assertions.
        const failed: Effect.Effect<ReactorError, unknown, HttpClient.HttpClient> =
          operation === "terminate"
            ? terminate(inspectionToken, "session").pipe(
                Effect.map((report) => {
                  expect(report.confirmed).toBe(false);
                  expect(report.evidence).toBeNull();
                  if (report.error === undefined)
                    throw new Error("Expected termination timeout evidence");
                  return report.error;
                }),
              )
            : Effect.flip(request);
        await Effect.runPromise(
          Effect.gen(function* () {
            const pending = yield* failed.pipe(
              Effect.tap(
                Effect.sync(() => {
                  completed = true;
                }),
              ),
              Effect.forkChild,
            );
            yield* TestClock.adjust(deadline - 1);
            expect(completed).toBe(false);
            expect(bodyStarted).toBe(phase === "body");
            yield* TestClock.adjust(1);
            const failure = yield* Fiber.join(pending);
            expect(failure).toBeInstanceOf(ReactorError);
            expect(failure.context.operation).toBe(operation);
            expect(completed).toBe(true);
            expect(methods).toEqual(
              operation === "terminate"
                ? ["DELETE", "GET"]
                : [operation === "token" ? "POST" : "GET"],
            );
          }).pipe(
            Effect.provideService(HttpClient.HttpClient, client),
            Effect.provide(TestClock.layer()),
            Effect.timeout(2_000),
          ),
        );
      });
    }
  }

  test("termination bounds both requests and aborts them inside an uninterruptible finalizer", async () => {
    const methods: Array<string> = [];
    const signals: Array<AbortSignal> = [];
    let completed = false;
    const client = HttpClient.make((request, _url, signal) => {
      methods.push(request.method);
      signals.push(signal);
      return Effect.never;
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const pending = yield* Effect.scoped(
          Effect.addFinalizer(() =>
            Effect.exit(terminate(inspectionToken, "session")).pipe(
              Effect.tap((exit) =>
                Effect.sync(() => {
                  expect(exit).toMatchObject({
                    _tag: "Success",
                    value: {
                      attempted: true,
                      responseReceived: false,
                      confirmed: false,
                      evidence: null,
                      deleteStatus: null,
                      state: null,
                      error: { code: "Timeout", context: { operation: "terminate" } },
                    },
                  });
                  completed = true;
                }),
              ),
              Effect.asVoid,
            ),
          ),
        ).pipe(Effect.forkChild);
        yield* TestClock.adjust(3_000);
        expect(methods).toEqual(["DELETE", "GET"]);
        expect(signals[0]?.aborted).toBe(true);
        expect(signals[1]?.aborted).toBe(false);
        yield* TestClock.adjust(2_999);
        expect(completed).toBe(false);
        yield* TestClock.adjust(1);
        yield* Fiber.join(pending);
        expect(completed).toBe(true);
        expect(signals.every((signal) => signal.aborted)).toBe(true);
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(TestClock.layer()),
        Effect.timeout(2_000),
      ),
    );
  });

  test("an interrupted owning scope still confirms termination after its DELETE times out", async () => {
    const methods: Array<string> = [];
    const signals: Array<AbortSignal> = [];
    let result: Exit.Exit<Coordinator.Termination, ReactorError> | undefined;
    const client = HttpClient.make((request, _url, signal) => {
      methods.push(request.method);
      signals.push(signal);
      return request.method === "DELETE"
        ? Effect.never
        : Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 404 })));
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const ready = yield* Deferred.make<void>();
        const owner = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.exit(terminate(inspectionToken, "session")).pipe(
                Effect.tap((exit) =>
                  Effect.sync(() => {
                    result = exit;
                  }),
                ),
                Effect.asVoid,
              ),
            );
            yield* Deferred.succeed(ready, undefined);
            return yield* Effect.never;
          }),
        ).pipe(Effect.forkChild);
        yield* Deferred.await(ready);
        const interrupted = yield* Fiber.interrupt(owner).pipe(Effect.forkChild);
        yield* TestClock.adjust(2_999);
        expect(methods).toEqual(["DELETE"]);
        expect(result).toBeUndefined();
        expect(signals[0]?.aborted).toBe(false);
        yield* TestClock.adjust(1);
        yield* Fiber.join(interrupted);
        expect(methods).toEqual(["DELETE", "GET"]);
        expect(result).toMatchObject({
          _tag: "Success",
          value: { confirmed: true, deleteStatus: null, state: null },
        });
        expect(signals.every((signal) => signal.aborted)).toBe(true);
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(TestClock.layer()),
        Effect.timeout(2_000),
      ),
    );
  });

  for (const phase of ["headers", "body"] as const) {
    test(`inspection's one-second fetch bound includes stalled response ${phase}`, async () => {
      let release: (() => void) | undefined;
      let headersReceived = false;
      let signal: AbortSignal | undefined;
      const methods: Array<string> = [];
      // Real Fetch associates its body with the request signal; a hand-built
      // Response in an injected request would not exercise that cancellation.
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(request) {
          methods.push(request.method);
          if (phase === "headers")
            return new Promise<Response>((resolve) => {
              release = () => resolve(new Response(null, { status: 503 }));
            });
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new TextEncoder().encode('{"session_id":"session",'));
                release = () => controller.error(new Error("fixture teardown"));
              },
            }),
            { headers: { "Content-Type": "application/json" } },
          );
        },
      });
      try {
        const started = performance.now();
        const error = await Effect.runPromise(
          Effect.flip(
            inspect(inspectionToken, "session")
              .pipe(
                Effect.provide(
                  fetchLayer(async (_url, init) => {
                    signal = init.signal ?? undefined;
                    const response = await fetch(`http://127.0.0.1:${server.port}/session`, init);
                    headersReceived = true;
                    return response;
                  }),
                ),
              )
              .pipe(Effect.timeout(2_000)),
          ),
        );
        expect(error).toBeInstanceOf(ReactorError);
        expect(error).toMatchObject({ context: { operation: "inspect" } });
        expect(performance.now() - started).toBeGreaterThanOrEqual(900);
        expect(signal?.aborted).toBe(true);
        expect(headersReceived).toBe(phase === "body");
        expect(methods).toEqual(["GET"]);
        expect(JSON.stringify(error)).not.toContain("secret_token_fixture");
      } finally {
        release?.();
        await server.stop(true);
      }
    });
  }

  test("confirms terminal states and URL-encodes the observed session identity", async () => {
    for (const state of ["INACTIVE", "CLOSED", "missing"]) {
      const calls: Array<{ url: string; init: RequestInit }> = [];
      const result = await Effect.runPromise(
        terminate(Redacted.make("secret_token_fixture"), "session / one").pipe(
          Effect.provide(
            fetchLayer(async (url, init) => {
              calls.push({ url, init });
              return init.method === "DELETE"
                ? new Response(null, { status: 204 })
                : state === "missing"
                  ? new Response(null, { status: 404 })
                  : Response.json({ state });
            }),
          ),
        ),
      );
      expect(result).toEqual({
        attempted: true,
        responseReceived: true,
        confirmed: true,
        evidence: state === "missing" ? "absent" : "terminal",
        deleteStatus: 204,
        state: state === "missing" ? null : state,
      });
      expect(calls.map((call) => call.init.method)).toEqual(["DELETE", "GET"]);
      expect(calls[1]?.url).toBe("https://configured.fixture/api/sessions/session%20%2F%20one");
      expect(calls[1]?.init.headers).toMatchObject({
        authorization: "Bearer secret_token_fixture",
        "reactor-api-version": "1",
        "reactor-api-accept-version": "1",
      });
    }
  });

  test("still checks termination after losing the DELETE response", async () => {
    let calls = 0;
    expect(
      await Effect.runPromise(
        terminate(Redacted.make("jwt"), "session").pipe(
          Effect.provide(
            fetchLayer(async (_, init) => {
              calls++;
              if (init.method === "DELETE") throw new Error("lost response");
              return new Response(null, { status: 404 });
            }),
          ),
        ),
      ),
    ).toEqual({
      attempted: true,
      responseReceived: false,
      confirmed: true,
      evidence: "absent",
      deleteStatus: null,
      state: null,
    });
    expect(calls).toBe(2);
  });

  // An absent session and a session this token may not address are the same 404.
  test("refuses to read an absent session as terminated when the DELETE was refused on authority", async () => {
    for (const status of [401, 403]) {
      const report = await Effect.runPromise(
        terminate(Redacted.make("secret_token_fixture"), "session").pipe(
          Effect.provide(
            fetchLayer(
              async (_, init) =>
                new Response(null, { status: init.method === "DELETE" ? status : 404 }),
            ),
          ),
        ),
      );
      expect(report).toMatchObject({
        confirmed: false,
        evidence: null,
        deleteStatus: status,
        state: null,
      });
      expect(report.error).toBeInstanceOf(ReactorError);
      expect(report.error?.context.status).toBe(status);
      expect(JSON.stringify(report)).not.toContain("secret_token_fixture");
    }
    // Any other unsuccessful DELETE leaves the GET as the better evidence.
    expect(
      await Effect.runPromise(
        terminate(Redacted.make("jwt"), "session").pipe(
          Effect.provide(
            fetchLayer(
              async (_, init) =>
                new Response(null, { status: init.method === "DELETE" ? 503 : 404 }),
            ),
          ),
        ),
      ),
    ).toEqual({
      attempted: true,
      responseReceived: true,
      confirmed: true,
      evidence: "absent",
      deleteStatus: 503,
      state: null,
    });
  });

  test("rejects a confirmation that names a different session, and reports the status that refused it", async () => {
    const report = await Effect.runPromise(
      terminate(Redacted.make("secret_token_fixture"), "session").pipe(
        Effect.provide(
          fetchLayer(async (_, init) =>
            init.method === "DELETE"
              ? new Response(null, { status: 204 })
              : Response.json({ session_id: "another", state: "CLOSED" }),
          ),
        ),
      ),
    );
    expect(report).toMatchObject({
      confirmed: false,
      evidence: null,
      state: null,
      error: { code: "Protocol" },
    });
    expect(report.error?.message).toContain("different session identity");
    expect(JSON.stringify(report)).not.toContain("secret_token_fixture");
    const refused = await Effect.runPromise(
      terminate(Redacted.make("jwt"), "session").pipe(
        Effect.provide(
          fetchLayer(async (_, init) =>
            init.method === "DELETE"
              ? new Response(null, { status: 204 })
              : new Response(null, { status: 502 }),
          ),
        ),
      ),
    );
    expect(refused).toMatchObject({
      confirmed: false,
      evidence: null,
      state: null,
      error: { code: "Http", context: { status: 502 } },
    });
    expect(JSON.stringify(refused)).toContain("502");
  });

  test("does not confuse accepted deletion with confirmed termination", async () => {
    expect(
      await Effect.runPromise(
        terminate(Redacted.make("jwt"), "session").pipe(
          Effect.provide(fetchLayer(async () => Response.json({ state: "ACTIVE" }))),
        ),
      ),
    ).toMatchObject({ confirmed: false, state: "ACTIVE" });
    for (const response of [
      new Response(null, { status: 403 }),
      Response.json({ wrong: "secret_token_fixture" }),
    ]) {
      const report = await Effect.runPromise(
        terminate(Redacted.make("secret_token_fixture"), "session").pipe(
          Effect.provide(
            fetchLayer(async (_, init) =>
              init.method === "DELETE" ? new Response(null, { status: 204 }) : response,
            ),
          ),
        ),
      );
      expect(report).toMatchObject({ confirmed: false, evidence: null, state: null });
      expect(report.error).toBeInstanceOf(ReactorError);
      expect(JSON.stringify(report)).not.toContain("secret_token_fixture");
    }
  });
});
