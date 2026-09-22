import { expect, test } from "bun:test";
import { Cause, Effect, Exit, Fiber } from "effect";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as Response from "effect/unstable/http/HttpClientResponse";
import * as Fetch from "effect/unstable/http/FetchHttpClient";
import * as Http from "../src/http.js";
import * as FetchHttp from "../src/FetchHttp.js";
import { ReactorError } from "../src/errors.js";

test("coordinator exchanges use the supplied Effect HttpClient", async () => {
  const calls: string[] = [];
  const platform = PlatformHttp.make((request, url) => Effect.sync(() => {
    calls.push(`${request.method} ${url.pathname}`);
    return Response.fromWeb(request, new globalThis.Response(JSON.stringify({ session_id: "session-one", state: "WAITING" })));
  }));
  const session = await Effect.runPromise(Effect.gen(function* () {
    const client = yield* Http.make({ apiUrl: "https://injected.invalid" });
    return yield* client.create({ name: "selected/model" });
  }).pipe(Effect.provideService(PlatformHttp.HttpClient, platform)));
  expect(session.session_id).toBe("session-one");
  expect(calls).toEqual(["POST /sessions"]);
});

test("empty session ids and model names are lazy typed failures, never defects", async () => {
  let requests = 0;
  const platform = PlatformHttp.make((request) => Effect.sync(() => {
    requests++;
    return Response.fromWeb(request, new globalThis.Response("{}"));
  }));
  const client = new Http.HttpClient({ apiUrl: "https://injected.invalid" }, platform);
  for (const operation of [client.read(""), client.create({ name: "" })]) {
    const exit = await Effect.runPromise(Effect.exit(operation));
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
    const platform = PlatformHttp.make((request) => Effect.succeed(Response.fromWeb(request, new globalThis.Response(body))));
    const client = new Http.HttpClient({ apiUrl: "https://injected.invalid", maxResponseBytes: 16 }, platform);
    const result = await Effect.runPromise(Effect.result(client.create({ name: "model" })));
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(result.failure.code).toBe(body.length > 16 ? "Overflow" : "Protocol");
  }
});

test("request interruption aborts Fetch and releases an owned response reader", async () => {
  let signal: AbortSignal | undefined;
  let body: ReadableStream<Uint8Array> | undefined;
  let cancelled = 0;
  const fakeFetch: typeof globalThis.fetch = Object.assign(async (_input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    signal = init?.signal ?? undefined;
    body = new ReadableStream<Uint8Array>({ cancel() { cancelled++; } });
    return new globalThis.Response(body);
  }, { preconnect: (_url: string | URL) => undefined });
  const operation = Effect.gen(function* () {
    const client = yield* Http.make({ apiUrl: "https://injected.invalid" });
    return yield* client.create({ name: "model" });
  }).pipe(Effect.provide(FetchHttp.layer), Effect.provideService(Fetch.Fetch, fakeFetch));
  const caller = Effect.runFork(operation);
  const deadline = Date.now() + 2_000;
  while (body?.locked !== true && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 1));
  await Effect.runPromise(Fiber.interrupt(caller));
  expect(signal?.aborted).toBe(true);
  expect(body?.locked).toBe(false);
  expect(cancelled).toBe(1);
});

test("a lost DELETE reply is followed by independent remote-termination confirmation", async () => {
  const calls: string[] = [];
  const platform = PlatformHttp.make((request) => Effect.suspend(() => {
    calls.push(request.method);
    if (request.method === "DELETE") return Effect.never;
    return Effect.succeed(Response.fromWeb(request, new globalThis.Response("", { status: 404 })));
  }));
  const client = new Http.HttpClient({ apiUrl: "https://injected.invalid", requestTimeoutMs: 5 }, platform);
  const result = await Effect.runPromise(client.terminate("owned-session"));
  expect(result.confirmed).toBe(true);
  expect(result.responseReceived).toBe(false);
  expect(result.evidence).toBe("absent");
  expect(calls).toEqual(["DELETE", "GET"]);
});
