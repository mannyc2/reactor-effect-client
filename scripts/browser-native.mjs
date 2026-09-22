import { createServer } from "node:http";
import { readFileSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { checkNativeBridge, NativeBridge, NativeCall, encodeNativeJson } from "../dist/native-bridge.js";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const bundle = resolve(process.argv[2] ?? join(root, ".check/browser-native/browser.js"));
const libraryName = process.platform === "darwin" ? "libreactor_effect_native.dylib" : process.platform === "win32" ? "reactor_effect_native.dll" : "libreactor_effect_native.so";
const libraryPath = join(root, "dist/native", `${process.platform}-${process.arch}`, libraryName);
const expectedBrowserToNative = { control: [0xb1, 0x01, 0x02], data: [0xb2, 0x03, 0x04, 0x05] };
const nativeToBrowser = { control: [0xa1, 0x10, 0x11], data: [0xa2, 0x20, 0x21, 0x22] };

const fail = (message) => { throw new Error(`browser/native integration: ${message}`); };
const forceRelay = process.env.BROWSER_NATIVE_FORCE_RELAY === "1";
const turnUrl = process.env.BROWSER_NATIVE_TURN_URL ?? "";
const turnUsername = process.env.BROWSER_NATIVE_TURN_USERNAME ?? "";
const turnPassword = process.env.BROWSER_NATIVE_TURN_PASSWORD ?? "";
const configuredTurn = [turnUrl, turnUsername, turnPassword].some((value) => value.length > 0);
if (process.env.BROWSER_NATIVE_FORCE_RELAY !== undefined && process.env.BROWSER_NATIVE_FORCE_RELAY !== "0" && process.env.BROWSER_NATIVE_FORCE_RELAY !== "1")
  fail("BROWSER_NATIVE_FORCE_RELAY must be 0 or 1");
if (!forceRelay && configuredTurn) fail("TURN fixture credentials require BROWSER_NATIVE_FORCE_RELAY=1");
if (forceRelay && (!/^turns?:/i.test(turnUrl) || turnUsername.length === 0 || turnPassword.length === 0))
  fail("forced TURN requires BROWSER_NATIVE_TURN_URL (turn:/turns:), BROWSER_NATIVE_TURN_USERNAME and BROWSER_NATIVE_TURN_PASSWORD");
const nativeIceServers = forceRelay ? [{ urls: [turnUrl], username: turnUsername, credential: turnPassword }] : [];
const browserFixture = forceRelay
  ? { forceRelay: true, iceServers: [{ urls: turnUrl, username: turnUsername, credential: turnPassword }] }
  : { forceRelay: false, iceServers: [] };
const waitUntil = async (check, timeoutMs, label) => {
  const end = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() >= end) fail(`${label}: deadline`);
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
  }
};
const bytesEqual = (actual, expected) => Array.isArray(actual) && actual.length === expected.length && actual.every((value, index) => value === expected[index]);
const fnv = (bytes) => { let value = 2166136261; for (const byte of bytes) value = Math.imul(value ^ byte, 16777619); return (value >>> 0).toString(16); };
const pcmRms = (payload) => {
  if (payload.length === 0 || payload.length % 2 !== 0) return 0;
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  let sum = 0, count = 0;
  for (let offset = 0; offset < payload.byteLength; offset += 2) { const value = view.getInt16(offset, true) / 32768; sum += value * value; count++; }
  return Math.sqrt(sum / Math.max(count, 1));
};

const browserExecutable = () => {
  const requested = process.env.BROWSER_EXECUTABLE;
  const candidates = [requested,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser",
  ].filter((value) => typeof value === "string" && value.length > 0);
  return candidates.find((candidate) => existsSync(candidate)) ?? fail(`no reusable Chrome/Chromium executable found; checked ${candidates.join(", ")}`);
};

if (!existsSync(bundle)) fail(`browser bundle missing: ${bundle}`);
if (!existsSync(libraryPath)) fail(`release native bridge missing: ${libraryPath}`);
let bridge;
let prepared;

const nativeCandidates = [];
let nativeRelayCandidates = 0;
let nativeFilteredCandidates = 0;
let nativeIceComplete = false;
const nativeChannels = new Set();
const browserMessages = new Map();
const videoFrames = [];
const videoHashes = new Set();
let metadataBytes = 0;
let metadataFrames = 0;
let maxAudioRms = 0;
let audioPackets = 0;
let nativeFailure;
let browserReport;
let browserClosed = false;
let finish = false;
let stopping = false;

const pumpEvents = async () => {
  try {
    while (!stopping) {
      if (bridge === undefined) fail("native event pump started before bridge acquisition");
      const result = await bridge.pollEvent(100);
      if (result._tag === "Again") continue;
      if (result._tag === "Closed") return;
      const header = result.packet.header;
      if (header.type === "ice") {
        if (header.candidate === undefined || header.candidate === null) nativeIceComplete = true;
        else {
          const candidate = header.candidate;
          const relay = typeof candidate.candidate === "string" && /(?:^|\s)typ relay(?:\s|$)/i.test(candidate.candidate);
          if (relay) nativeRelayCandidates++;
          if (forceRelay && !relay) { nativeFilteredCandidates++; continue; }
          nativeCandidates.push({
            candidate: candidate.candidate,
            ...(candidate.sdp_mid == null ? {} : { sdpMid: candidate.sdp_mid }),
            ...(candidate.sdp_mline_index == null ? {} : { sdpMLineIndex: candidate.sdp_mline_index }),
          });
        }
      } else if (header.type === "channel" && header.open === true) nativeChannels.add(header.channel);
      else if (header.type === "message") browserMessages.set(header.channel, [...result.packet.payload]);
      else if (header.type === "error") nativeFailure = new Error(`native event error ${header.code ?? "Native"}: ${header.message ?? "unknown"}`);
    }
  } catch (error) { if (!stopping) nativeFailure = error instanceof Error ? error : new Error(String(error)); }
};
const pumpVideo = async () => {
  try {
    while (!stopping) {
      if (bridge === undefined) fail("native video pump started before bridge acquisition");
      const result = await bridge.pollVideo(100);
      if (result._tag === "Again") continue;
      if (result._tag === "Closed") return;
      const header = result.packet.header;
      if (header.type !== "video" || header.format !== "BGRA") fail("native video poll returned unexpected packet");
      const dataLength = Number(header.dataLength), metadataLength = Number(header.metadataLength);
      if (header.width !== 160 || header.height !== 96 || dataLength !== 160 * 96 * 4 || dataLength + metadataLength !== result.packet.payload.length) fail("native decoded video shape mismatch");
      videoFrames.push({ frameId: String(header.frameId), timestampMicros: String(header.timestampMicros), metadataLength });
      videoHashes.add(fnv(result.packet.payload.subarray(0, dataLength)));
      metadataBytes += metadataLength;
      if (metadataLength > 0) metadataFrames++;
    }
  } catch (error) { if (!stopping) nativeFailure = error instanceof Error ? error : new Error(String(error)); }
};
const pumpAudio = async () => {
  try {
    while (!stopping) {
      if (bridge === undefined) fail("native audio pump started before bridge acquisition");
      const result = await bridge.pollAudio(100);
      if (result._tag === "Again") continue;
      if (result._tag === "Closed") return;
      const header = result.packet.header;
      if (header.type !== "audio" || header.format !== "s16le" || header.sampleRate !== 48000 || header.channels !== 1) fail(`native decoded audio shape mismatch: ${JSON.stringify(header)}`);
      audioPackets++;
      maxAudioRms = Math.max(maxAudioRms, pcmRms(result.packet.payload));
    }
  } catch (error) { if (!stopping) nativeFailure = error instanceof Error ? error : new Error(String(error)); }
};

const readJson = async (request) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const text = Buffer.concat(chunks).toString("utf8");
  return text.length === 0 ? undefined : JSON.parse(text);
};
const sendJson = (response, status, value) => {
  const body = Buffer.from(JSON.stringify(value));
  response.writeHead(status, { "Content-Type": "application/json", "Content-Length": body.length, "Cache-Control": "no-store" });
  response.end(body);
};
const page = Buffer.from("<!doctype html><meta charset=utf-8><title>reactor browser/native local qualification</title><script type=module src=/browser.js></script>");
const browserJs = readFileSync(bundle);
let server;
let profile;
let browser;
let browserExit;
let browserSpawnError;
let resolveBrowserExit;
let browserExitPromise;
let browserStderr = "";
let eventTask;
let videoTask;
let audioTask;
let summary;
let runError;

const requestHandler = async (request, response) => {
  try {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method === "GET" && url.pathname === "/") { response.writeHead(200, { "Content-Type": "text/html", "Content-Length": page.length }); response.end(page); return; }
    if (request.method === "GET" && url.pathname === "/browser.js") { response.writeHead(200, { "Content-Type": "text/javascript", "Content-Length": browserJs.length, "Cache-Control": "no-store" }); response.end(browserJs); return; }
    if (request.method === "GET" && url.pathname === "/native-offer") { sendJson(response, 200, { ...prepared, fixture: browserFixture }); return; }
    if (request.method === "GET" && url.pathname === "/native-ice") {
      const cursor = Number(url.searchParams.get("cursor") ?? "0");
      if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > nativeCandidates.length) { sendJson(response, 400, { error: "invalid ICE cursor" }); return; }
      sendJson(response, 200, { candidates: nativeCandidates.slice(cursor), complete: nativeIceComplete }); return;
    }
    if (request.method === "POST" && url.pathname === "/native-answer") {
      const body = await readJson(request);
      if (body === null || typeof body !== "object" || typeof body.sdp !== "string") { sendJson(response, 400, { error: "missing SDP" }); return; }
      if (bridge === undefined) fail("native bridge unavailable while applying browser answer");
      await bridge.call(NativeCall.Answer, new TextEncoder().encode(body.sdp));
      sendJson(response, 200, { applied: true }); return;
    }
    if (request.method === "POST" && url.pathname === "/browser-report") { browserReport = await readJson(request); sendJson(response, 200, { stored: true }); return; }
    if (request.method === "POST" && url.pathname === "/browser-closed") { browserClosed = true; sendJson(response, 200, { stored: true }); return; }
    if (request.method === "GET" && url.pathname === "/finish") { sendJson(response, 200, { finish }); return; }
    sendJson(response, 404, { error: "not found" });
  } catch (error) { sendJson(response, 500, { error: error instanceof Error ? error.message : String(error) }); }
};

try {
  await checkNativeBridge(libraryPath);
  bridge = new NativeBridge(libraryPath);
  prepared = await bridge.call(NativeCall.Prepare, encodeNativeJson({
    servers: nativeIceServers,
    tracks: [
      { name: "browser_video", kind: "video", direction: "recvonly" },
      { name: "browser_audio", kind: "audio", direction: "recvonly" },
    ],
  }));
  if (prepared === null || typeof prepared !== "object" || typeof prepared.sdp !== "string" || !Array.isArray(prepared.mapping)) fail("native prepare returned invalid offer");

  server = createServer(requestHandler);
  await new Promise((resolveListen, rejectListen) => { server.once("error", rejectListen); server.listen(0, "127.0.0.1", resolveListen); });
  const address = server.address();
  if (address === null || typeof address === "string") fail("local HTTP server did not bind an IPv4 port");
  const url = `http://127.0.0.1:${address.port}/`;
  profile = mkdtempSync(join(tmpdir(), "reactor-browser-native-"));
  const chrome = browserExecutable();
  browser = spawn(chrome, [
    "--headless=new", "--autoplay-policy=no-user-gesture-required", "--disable-background-networking", "--disable-component-update",
    "--disable-default-apps", "--disable-sync", "--no-first-run", "--no-default-browser-check", "--metrics-recording-only",
    "--disable-features=WebRtcHideLocalIpsWithMdns", `--user-data-dir=${profile}`, url,
  ], { stdio: ["ignore", "pipe", "pipe"] });
  browserExitPromise = new Promise((resolveExit) => { resolveBrowserExit = resolveExit; });
  browser.stderr?.setEncoding("utf8");
  browser.stderr?.on("data", (chunk) => { browserStderr = `${browserStderr}${chunk}`.slice(-16_384); });
  browser.on("error", (error) => { browserSpawnError ??= error; resolveBrowserExit?.({ error }); });
  browser.on("exit", (code, signal) => { browserExit = { code, signal }; resolveBrowserExit?.(browserExit); });

  eventTask = pumpEvents();
  videoTask = pumpVideo();
  audioTask = pumpAudio();

  await waitUntil(() => nativeFailure !== undefined || browserSpawnError !== undefined || nativeChannels.size === 2 || browserExit !== undefined, 15_000, "native data channels open");
  if (nativeFailure !== undefined) throw nativeFailure;
  if (browserSpawnError !== undefined) fail(`Chrome spawn failed: ${browserSpawnError.message}`);
  if (browserExit !== undefined) fail(`Chrome exited before native channels opened (${JSON.stringify(browserExit)}): ${browserStderr}`);
  await bridge.send("control", Uint8Array.from(nativeToBrowser.control));
  await bridge.send("data", Uint8Array.from(nativeToBrowser.data));

  await waitUntil(() => nativeFailure !== undefined || browserSpawnError !== undefined || browserReport !== undefined || browserExit !== undefined, 10_000, "browser report");
  if (nativeFailure !== undefined) throw nativeFailure;
  if (browserSpawnError !== undefined) fail(`Chrome spawn failed: ${browserSpawnError.message}`);
  if (browserExit !== undefined && browserReport === undefined) fail(`Chrome exited before reporting (${JSON.stringify(browserExit)}): ${browserStderr}`);
  if (browserReport?.ok !== true) fail(`browser qualification failed: ${browserReport?.error ?? JSON.stringify(browserReport)}`);

  await waitUntil(() => nativeFailure !== undefined || (
    bytesEqual(browserMessages.get("control"), expectedBrowserToNative.control) && bytesEqual(browserMessages.get("data"), expectedBrowserToNative.data) &&
    videoFrames.length >= 12 && videoHashes.size >= 3 && audioPackets >= 12 && maxAudioRms > 0.001
  ), 12_000, "native browser messages and decoded media");
  if (nativeFailure !== undefined) throw nativeFailure;

  let relayEvidence;
  if (forceRelay) {
    if (nativeRelayCandidates === 0) fail("forced TURN gathered no native relay candidate");
    const stats = await bridge.call(NativeCall.Stats);
    if (!Array.isArray(stats)) fail("native stats response is not an array");
    const entries = new Map(stats.filter((entry) => entry !== null && typeof entry === "object" && typeof entry.id === "string").map((entry) => [entry.id, entry]));
    const pairs = stats.filter((entry) => entry !== null && typeof entry === "object" && entry.type === "candidate-pair" && entry.state === "succeeded" && entry.nominated === true && entry.writable === true);
    if (pairs.length === 0) fail("native stats omitted nominated writable succeeded ICE pair");
    const nativePairs = pairs.map((pair) => {
      if (typeof pair.id !== "string" || typeof pair.localCandidateId !== "string") fail("native active ICE pair omitted ids");
      const local = entries.get(pair.localCandidateId);
      if (local === undefined || local.candidateType !== "relay") fail(`native active ICE pair local candidate is not relay: ${local?.candidateType ?? "missing"}`);
      return { pairId: pair.id, localCandidateId: pair.localCandidateId, localCandidateType: local.candidateType, relayProtocol: local.relayProtocol ?? "" };
    });
    const browserRelay = browserReport?.native?.relay;
    if (browserRelay?.forced !== true || browserRelay.selected?.localCandidateType !== "relay" || browserRelay.selected?.remoteCandidateType !== "relay")
      fail(`browser selected pair did not prove relay/relay: ${JSON.stringify(browserRelay)}`);
    relayEvidence = {
      forced: true,
      native: { activePairs: nativePairs, gatheredRelayCandidates: nativeRelayCandidates, filteredNonRelayCandidates: nativeFilteredCandidates },
      browser: browserRelay.selected,
    };
  }

  finish = true;
  await waitUntil(() => browserClosed || browserExit !== undefined, 5000, "browser cleanup report");

  summary = [
    "browser-native-ok",
    `browser ${chrome}`,
    `browser-local ${JSON.stringify(browserReport.localPeer)}`,
    `browser-native ${JSON.stringify(browserReport.native)}`,
    `native-channels ${[...nativeChannels].sort().join(",")}`,
    `native-browser-messages control=${JSON.stringify(browserMessages.get("control"))} data=${JSON.stringify(browserMessages.get("data"))}`,
    `native-video frames=${videoFrames.length} distinctHashes=${videoHashes.size} metadataFrames=${metadataFrames} metadataBytes=${metadataBytes}`,
    `native-audio packets=${audioPackets} maxRms=${maxAudioRms.toFixed(6)}`,
    `turn-relay ${relayEvidence === undefined ? "disabled" : JSON.stringify(relayEvidence)}`,
    `metadata-limit ${metadataBytes === 0 ? "standards browser emitted no Reactor custom frame metadata; decoded A/V remains genuine" : "browser/native path delivered Reactor frame metadata"}`,
  ];
} catch (error) {
  runError = error;
}

const cleanupErrors = [];
try {
  finish = true;
  stopping = true;
  const tasks = [eventTask, videoTask, audioTask].filter((task) => task !== undefined);
  if (tasks.length > 0) await Promise.allSettled(tasks);
  if (bridge !== undefined) {
    try { await bridge.shutdown(); } catch (error) { cleanupErrors.push(error instanceof Error ? error : new Error(String(error))); }
  }
  if (browser !== undefined && browserSpawnError === undefined && browserExit === undefined) {
    browser.kill("SIGTERM");
    await Promise.race([browserExitPromise, new Promise((resolveDelay) => setTimeout(resolveDelay, 1000))]);
  }
  if (browser !== undefined && browserSpawnError === undefined && browserExit === undefined) {
    browser.kill("SIGKILL");
    await Promise.race([browserExitPromise, new Promise((resolveDelay) => setTimeout(resolveDelay, 2000))]);
  }
  if (browser !== undefined && browserSpawnError === undefined && browserExit === undefined) cleanupErrors.push(new Error("Chrome did not exit after bounded TERM/KILL cleanup"));
} catch (error) {
  cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
} finally {
  if (server !== undefined) {
    try { server.closeAllConnections?.(); } catch (error) { cleanupErrors.push(error instanceof Error ? error : new Error(String(error))); }
    try {
      await new Promise((resolveClose, rejectClose) => server.close((error) => {
        if (error !== undefined && error.code !== "ERR_SERVER_NOT_RUNNING") rejectClose(error);
        else resolveClose();
      }));
    } catch (error) { cleanupErrors.push(error instanceof Error ? error : new Error(String(error))); }
  }
  if (profile !== undefined) {
    try { rmSync(profile, { recursive: true, force: true }); } catch (error) { cleanupErrors.push(error instanceof Error ? error : new Error(String(error))); }
  }
}

if (runError !== undefined && cleanupErrors.length > 0) throw new AggregateError([runError, ...cleanupErrors], "browser/native integration failed and cleanup also failed");
if (runError !== undefined) throw runError;
if (cleanupErrors.length === 1) throw cleanupErrors[0];
if (cleanupErrors.length > 1) throw new AggregateError(cleanupErrors, "browser/native integration cleanup failed");
for (const line of summary ?? []) console.log(line);
