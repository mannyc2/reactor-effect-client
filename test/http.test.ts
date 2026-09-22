import * as Effect from "effect/Effect";
import { tokenBody, retryAfterMs } from "../src/http.js";
import { TestHttpClient as HttpClient } from "./fixtures.js";
import { parseDescriptor, parseCapabilities } from "../src/contract.js";
import { record } from "../src/json.js";
import { withFixture, jsonResponse, stall } from "./fixtures.js";
import { test, assert, equal, failure, run, throws, eventually } from "./harness.js";
test("HTTP source headers, cloud create shape, readiness 202/backoff and per-request credentials", () => withFixture(async (f) => {
  f.createSlim = true; f.readsUntilReady = 1;
  let credentials = 0;
  const http = new HttpClient({ apiUrl: "https://coordinator.fixture", credential: Effect.sync(() => `token-${++credentials}`), sessionPoll: { attempts: 5, initialMs: 1, maxMs: 2 } });
  const initial = await run(http.create({ name: "owner/model", version: "v1" }, { temperature: 1 }));
  equal(initial.state, "CREATED");
  const ready = await run(http.ready(initial.session_id, initial));assert(ready.capabilities !== undefined);
  const first = f.calls[0]; assert(first !== undefined);
  const body: unknown = JSON.parse(first.body);
  equal(body, { model: { name: "owner/model", version: "v1" }, client_info: { sdk_version: "0.1.0", sdk_type: "typescript-effect-independent" }, supported_transports: [{ protocol: "webrtc", version: "1.0" }], extra_args: { temperature: 1 } });
  equal(first.headers.get("Reactor-API-Version"), "1");equal(first.headers.get("Reactor-API-Accept-Version"), "1");
  equal(first.headers.get("authorization"), "Bearer token-1");equal(credentials, f.calls.length);
  equal(ready.raw.future_extension, { retained: true });
}));
test("HTTP local start excludes model and auth; local signaling still uses auth", () => withFixture(async (f) => {
  const http = new HttpClient({ apiUrl: "https://coordinator.fixture", local: true, credential: Effect.succeed("local-signaling-jwt") });
  await run(http.create({ name: "ignored" }, { key: 1 }));await run(http.read(f.sessionId));await run(http.iceServers(f.sessionId));
  equal(f.calls.map((c) => [c.url.pathname, c.headers.get("authorization")]), [["/start_session", null], ["/session", null], [`/sessions/${f.sessionId}/transport/webrtc/ice_servers`, "Bearer local-signaling-jwt"]]);
  equal(JSON.parse(f.calls[0]?.body ?? "null"), { extra_args: { key: 1 } });
  equal(f.calls[2]?.headers.get("Reactor-WebRTC-Version"), "1.0");
  equal((await failure(http.read("another-session"))).code, "Protocol");
}));
test("HTTP descriptor readiness is capabilities+transport, not ACTIVE; terminal and unknown states", () => withFixture(async (f) => {
  f.state = "WAITING";const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
  equal((await run(http.ready(f.sessionId))).state, "WAITING");
  f.state = "INACTIVE";equal((await failure(http.ready(f.sessionId))).code, "TerminalSession");
  f.state = "FUTURE_STATE";equal(parseDescriptor(f.descriptor).state, "FUTURE_STATE");
  throws(() => parseCapabilities({ protocol_version: "1", tracks: [{ name: "x", kind: "future", direction: "recvonly" }] }), "Protocol");
  throws(() => parseCapabilities({ protocol_version: "1", tracks: [f.tracks[0], f.tracks[0]] }), "Protocol");
}));
test("HTTP 426/501 are nonretryable version mismatches and preserve body/status", () => withFixture(async (f) => {
  const http = new HttpClient({ apiUrl: "https://coordinator.fixture" });
  for (const status of [426, 501]) {
    f.hook = () => new Response("wire version unsupported", { status, headers: { "retry-after": "1.25" } });
    const e = await failure(http.ready(f.sessionId));equal(e.code, "VersionMismatch");equal(e.context.status, status);equal(e.context.retryAfterMs, 1250);equal(e.context.body, "wire version unsupported");
  }
  equal(f.calls.length, 2);
}));
test("HTTP deadlines cover response reads and cancel stalled readers", () => withFixture(async (f) => {
  let cancelled = false;
  f.hook = () => new Response(new ReadableStream<Uint8Array>({ start(c) { c.enqueue(new Uint8Array([123])); }, cancel() { cancelled = true; } }));
  const http = new HttpClient({ apiUrl: "https://coordinator.fixture", requestTimeoutMs: 10 });
  equal((await failure(http.read(f.sessionId))).code, "Timeout");await eventually(() => cancelled);
}));
test("HTTP response byte limit and strict UTF8/JSON decoding", () => withFixture(async (f) => {
  const http = new HttpClient({ apiUrl: "https://coordinator.fixture", maxResponseBytes: 16 });
  f.hook = () => new Response("x".repeat(17));equal((await failure(http.read(f.sessionId))).code, "Overflow");
  f.hook = () => new Response(new Uint8Array([0xff]));equal((await failure(http.read(f.sessionId))).code, "Protocol");
}));
test("HTTP no unsafe retries of creation after a network/ack timeout", () => withFixture(async (f) => {
  f.hook = (c) => stall(c.signal);const http = new HttpClient({ apiUrl: "https://coordinator.fixture", requestTimeoutMs: 10 });
  const e = await failure(http.create({ name: "owner/model" }));equal(e.code, "Timeout");equal(e.context.outcome, "unknown");equal(f.calls.length, 1);
}));
test("token source contract: null, scoped restrictions, exact uint64, reject typo", () => withFixture(async (f) => {
  equal(tokenBody({}), "null");equal(tokenBody({ max_sessions: 3 }), "null");
  const body = tokenBody({ models: ["owner/model"], max_sessions: 1, max_session_duration_seconds: 60, expires_after: 18446744073709551615n });
  assert(body.includes('"expires_after":18446744073709551615'));assert(body.includes('"authorization_details"'));
  const typo = { expires_after: 1n, extra: true };throws(() => tokenBody(typo), "Protocol");
  const http = new HttpClient({ apiUrl: "https://coordinator.fixture", credential: Effect.succeed("must-not-leak") });
  equal(await run(http.exchangeKey("fixture-key")), "fixture-jwt");equal(f.calls[0]?.body,"null");equal(f.calls[0]?.headers.get("authorization"),null);equal(f.calls[0]?.headers.get("Reactor-API-Key"),"fixture-key");
  f.hook=()=>jsonResponse({jwt:""});equal((await failure(http.exchangeKey("x"))).code,"Protocol");
}));
test("HTTP termination request response and terminal confirmation are separate facts", () => withFixture(async (f) => {
  const http=new HttpClient({apiUrl:"https://coordinator.fixture"});f.terminateConfirms=false;
  equal(await run(http.terminate(f.sessionId)),{attempted:true,responseReceived:true,confirmed:false});
  f.terminateConfirms=true;equal((await run(http.terminate(f.sessionId))).evidence,"terminal");
  f.hook=()=>new Response(null,{status:404});equal((await run(http.terminate(f.sessionId))).evidence,"absent");
}));
test("Retry-After accepts only finite nonnegative delta seconds",()=>{
  for(const raw of ["-1","Infinity","Wed, 21 Oct 2015 07:28:00 GMT","", "1e9"]) equal(retryAfterMs(new Headers({"retry-after":raw})),undefined);
  equal(retryAfterMs(new Headers({"retry-after":"0.2"})),200);
});
test("source ICE and SDP HTTP paths/bodies and registration full uint32",()=>withFixture(async(f)=>{
  const http=new HttpClient({apiUrl:"https://coordinator.fixture"});
  f.hook=(c)=>c.url.pathname.endsWith('/connections')?jsonResponse({connection_id:4294967295}):undefined;
  const cid=await run(http.register(f.sessionId));equal(cid,4294967295);
  await run(http.ice(f.sessionId,cid,[],true));await run(http.offer(f.sessionId,cid,"offer",[],true));
  const ice=f.calls[1];assert(ice!==undefined);equal(record(JSON.parse(ice.body)).is_final,true);equal(f.calls[2]?.method,"PUT");
  equal(f.calls[0]?.body,"{}");
}));
