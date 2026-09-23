import { test } from "vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import { tokenBody } from "../src/coordinator/_internal/client.js";
import { retryAfterMs } from "../src/coordinator/_internal/response.js";
import { TestHttpClient as HttpClient } from "./fixtures.js";
import { parseDescriptor, parseCapabilities } from "../src/contract.js";
import { record } from "../src/json.js";
import { withFixture, jsonResponse, stall } from "./fixtures.js";
import { assert, equal, failure, run, throws, eventually } from "./harness.js";
test("HTTP source headers, cloud create shape, readiness 202/backoff and per-request credentials", ({
  signal,
}) =>
  withFixture(async (f) => {
    f.createSlim = true;
    f.readsUntilReady = 1;
    let credentials = 0;
    const http = new HttpClient({
      apiUrl: "https://coordinator.fixture",
      credential: Effect.sync(() => `token-${++credentials}`),
      sessionPoll: { attempts: 5, initialDelay: 1, maxDelay: 2 },
    });
    const allocation = await run(
      http.create({ name: "owner/model", version: "v1" }, { temperature: 1 }),
      { signal },
    );
    const initial = await run(http.describe(allocation), { signal });
    equal(initial.state, "CREATED");
    const ready = await run(http.ready(allocation.sessionId, initial), { signal });
    assert(ready.capabilities !== undefined);
    const first = f.calls[0];
    assert(first !== undefined);
    const body: unknown = JSON.parse(first.body);
    equal(body, {
      model: { name: "owner/model", version: "v1" },
      client_info: { sdk_version: "0.1.0", sdk_type: "typescript-effect-independent" },
      supported_transports: [{ protocol: "webrtc", version: "1.0" }],
      extra_args: { temperature: 1 },
    });
    equal(first.headers.get("Reactor-API-Version"), "1");
    equal(first.headers.get("Reactor-API-Accept-Version"), "1");
    equal(first.headers.get("authorization"), "Bearer token-1");
    equal(credentials, f.calls.length);
    equal(ready.raw.future_extension, { retained: true });
  }));
test("HTTP local start excludes model and auth; local signaling still uses auth", ({ signal }) =>
  withFixture(async (f) => {
    const http = new HttpClient({
      apiUrl: "https://coordinator.fixture",
      local: true,
      credential: Effect.succeed("local-signaling-jwt"),
    });
    await run(http.create({ name: "ignored" }, { key: 1 }), { signal });
    await run(http.read(f.sessionId), { signal });
    await run(http.iceServers(f.sessionId), { signal });
    equal(
      f.calls.map((c) => [c.url.pathname, c.headers.get("authorization")]),
      [
        ["/start_session", null],
        ["/session", null],
        [`/sessions/${f.sessionId}/transport/webrtc/ice_servers`, "Bearer local-signaling-jwt"],
      ],
    );
    equal(JSON.parse(f.calls[0]?.body ?? "null"), { extra_args: { key: 1 } });
    equal(f.calls[2]?.headers.get("Reactor-WebRTC-Version"), "1.0");
    equal((await failure(http.read("another-session"), { signal })).reason._tag, "Protocol");
  }));
test("HTTP descriptor readiness is capabilities+transport, not ACTIVE; terminal and unknown states", ({
  signal,
}) =>
  withFixture(async (f) => {
    f.state = "WAITING";
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
    equal((await run(http.ready(f.sessionId), { signal })).state, "WAITING");
    f.state = "INACTIVE";
    equal((await failure(http.ready(f.sessionId), { signal })).reason._tag, "TerminalSession");
    f.state = "FUTURE_STATE";
    equal(parseDescriptor(f.descriptor).state, "FUTURE_STATE");
    throws(
      () =>
        parseCapabilities({
          protocol_version: "1",
          tracks: [{ name: "x", kind: "future", direction: "recvonly" }],
        }),
      "Protocol",
    );
    throws(
      () => parseCapabilities({ protocol_version: "1", tracks: [f.tracks[0], f.tracks[0]] }),
      "Protocol",
    );
  }));
test("HTTP 426/501 are nonretryable version mismatches that keep body/status for inspection", ({
  signal,
}) =>
  withFixture(async (f) => {
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
    for (const status of [426, 501]) {
      f.hook = () =>
        new Response("wire version unsupported", { status, headers: { "retry-after": "1.25" } });
      const e = await failure(http.ready(f.sessionId), { signal });
      equal(e.reason._tag, "VersionMismatch");
      equal(e.context.detail, { status, body: "wire version unsupported" });
    }
    equal(f.calls.length, 2);
  }));
test("HTTP refusals keep status, Retry-After and body in the Http reason, not the message", ({
  signal,
}) =>
  withFixture(async (f) => {
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
    for (const status of [429, 503]) {
      f.hook = () =>
        new Response("provider says slow down", { status, headers: { "retry-after": "1.25" } });
      const e = await failure(http.ready(f.sessionId), { signal });
      assert(e.reason._tag === "Http");
      equal(e.reason.status, status);
      equal(
        e.reason.retryAfter === undefined ? undefined : Duration.toMillis(e.reason.retryAfter),
        1250,
      );
      equal(e.reason.body, "provider says slow down");
      assert(!e.message.includes("slow down"));
      assert(!JSON.stringify(e).includes("slow down"));
    }
  }));
test("HTTP deadlines cover response reads and cancel stalled readers", ({ signal }) =>
  withFixture(async (f) => {
    let cancelled = false;
    f.hook = () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(c) {
            c.enqueue(new Uint8Array([123]));
          },
          cancel() {
            cancelled = true;
          },
        }),
      );
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture", requestTimeout: 10 });
    equal((await failure(http.read(f.sessionId), { signal })).reason._tag, "Timeout");
    await eventually(() => cancelled);
  }));
test("HTTP response byte limit and strict UTF8/JSON decoding", ({ signal }) =>
  withFixture(async (f) => {
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture", maxResponseBytes: 16 });
    f.hook = () => new Response("x".repeat(17));
    equal((await failure(http.read(f.sessionId), { signal })).reason._tag, "Overflow");
    f.hook = () => new Response(new Uint8Array([0xff]));
    equal((await failure(http.read(f.sessionId), { signal })).reason._tag, "Protocol");
  }));
test("HTTP no unsafe retries of creation after a network/ack timeout", ({ signal }) =>
  withFixture(async (f) => {
    f.hook = (c) => stall(c.signal);
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture", requestTimeout: 10 });
    const e = await failure(http.create({ name: "owner/model" }), { signal });
    equal(e.reason._tag, "Timeout");
    equal(e.context.outcome, "unknown");
    equal(f.calls.length, 1);
  }));
test("token source contract: null, scoped restrictions, exact uint64, reject typo", ({ signal }) =>
  withFixture(async (f) => {
    equal(tokenBody({}), "null");
    equal(tokenBody({ max_sessions: 3 }), "null");
    const body = tokenBody({
      models: ["owner/model"],
      max_sessions: 1,
      max_session_duration_seconds: 60,
      expires_after: 18446744073709551615n,
    });
    assert(body.includes('"expires_after":18446744073709551615'));
    assert(body.includes('"authorization_details"'));
    const typo = { expires_after: 1n, extra: true };
    throws(() => tokenBody(typo), "Protocol");
    const http = new HttpClient({
      apiUrl: "https://coordinator.fixture",
      credential: Effect.succeed("must-not-leak"),
    });
    equal(await run(http.exchangeKey("fixture-key"), { signal }), "fixture-jwt");
    equal(f.calls[0]?.body, "null");
    equal(f.calls[0]?.headers.get("authorization"), null);
    equal(f.calls[0]?.headers.get("Reactor-API-Key"), "fixture-key");
    f.hook = () => jsonResponse({ jwt: "" });
    equal((await failure(http.exchangeKey("x"), { signal })).reason._tag, "Protocol");
  }));
test("HTTP termination request response and terminal confirmation are separate facts", ({
  signal,
}) =>
  withFixture(async (f) => {
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
    f.terminateConfirms = false;
    equal(await run(http.terminate(f.sessionId), { signal }), {
      attempted: true,
      responseReceived: true,
      confirmed: false,
      evidence: null,
      deleteStatus: 202,
      state: "ACTIVE",
    });
    f.terminateConfirms = true;
    equal((await run(http.terminate(f.sessionId), { signal })).evidence, "terminal");
    f.hook = () => new Response(null, { status: 404 });
    equal((await run(http.terminate(f.sessionId), { signal })).evidence, "absent");
  }));
test("Retry-After accepts only finite nonnegative delta seconds", () => {
  for (const raw of ["-1", "Infinity", "Wed, 21 Oct 2015 07:28:00 GMT", "", "1e9"])
    equal(retryAfterMs(new Headers({ "retry-after": raw })), undefined);
  equal(retryAfterMs(new Headers({ "retry-after": "0.2" })), 200);
});
test("source ICE and SDP HTTP paths/bodies and registration full uint32", ({ signal }) =>
  withFixture(async (f) => {
    const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
    f.hook = (c) =>
      c.url.pathname.endsWith("/connections")
        ? jsonResponse({ connection_id: 4294967295 })
        : undefined;
    const cid = await run(http.register(f.sessionId), { signal });
    equal(cid, 4294967295);
    await run(http.ice(f.sessionId, cid, [], true), { signal });
    await run(http.offer(f.sessionId, cid, "offer", [], true), { signal });
    const ice = f.calls[1];
    assert(ice !== undefined);
    equal(record(JSON.parse(ice.body)).is_final, true);
    equal(f.calls[2]?.method, "PUT");
    equal(f.calls[0]?.body, "{}");
  }));
