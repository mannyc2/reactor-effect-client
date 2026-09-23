import { createServer } from "node:http";
import { readFileSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Native from "reactor-effect-native";
import { FetchHttp, ReactorError, make as makeClient } from "reactor-effect-client";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const bundle = resolve(process.argv[2] ?? join(root, "../.check/browser-native/browser.js"));
/** @param {string} message @returns {never} */
const fail = (message) => {
  throw new Error(`browser/native integration: ${message}`);
};
/** @type {(value: unknown, message: string) => asserts value} */
const assert = (value, message) => {
  if (!value) fail(message);
};
const forceRelay = process.env.BROWSER_NATIVE_FORCE_RELAY === "1";
const turnUrl = process.env.BROWSER_NATIVE_TURN_URL ?? "";
const turnUsername = process.env.BROWSER_NATIVE_TURN_USERNAME ?? "";
const turnPassword = process.env.BROWSER_NATIVE_TURN_PASSWORD ?? "";
const configuredTurn = [turnUrl, turnUsername, turnPassword].some((value) => value.length > 0);
if (
  process.env.BROWSER_NATIVE_FORCE_RELAY !== undefined &&
  !["0", "1"].includes(process.env.BROWSER_NATIVE_FORCE_RELAY)
)
  fail("BROWSER_NATIVE_FORCE_RELAY must be 0 or 1");
if (!forceRelay && configuredTurn)
  fail("TURN fixture credentials require BROWSER_NATIVE_FORCE_RELAY=1");
if (
  forceRelay &&
  (!/^turns?:/i.test(turnUrl) || turnUsername.length === 0 || turnPassword.length === 0)
)
  fail("forced TURN requires a TURN URL, username and password");
const iceServers = forceRelay
  ? [{ urls: [turnUrl], username: turnUsername, credential: turnPassword }]
  : [];
const browserFixture = { forceRelay, iceServers };
/** @param {() => boolean} check @param {number} timeoutMs @param {string} label */
const waitUntil = async (check, timeoutMs, label) => {
  const end = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() >= end) fail(`${label}: deadline`);
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
  }
};
/** @param {Uint8Array} bytes */
const fnv = (bytes) => {
  let value = 2166136261;
  for (const byte of bytes) value = Math.imul(value ^ byte, 16777619);
  return (value >>> 0).toString(16);
};
/** @param {Int16Array} samples */
const pcmRms = (samples) => {
  let sum = 0;
  for (const sample of samples) sum += (sample / 32768) ** 2;
  return Math.sqrt(sum / Math.max(samples.length, 1));
};
const browserExecutable = () => {
  const candidates = [
    process.env.BROWSER_EXECUTABLE,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ]
    .filter((value) => typeof value === "string")
    .filter((value) => value.length > 0);
  return (
    candidates.find((candidate) => existsSync(candidate)) ??
    fail("no reusable Chrome/Chromium executable found")
  );
};
assert(existsSync(bundle), `browser bundle missing: ${bundle}`);
const browserJs = readFileSync(bundle);
const page = Buffer.from(
  "<!doctype html><meta charset=utf-8><title>Reactor public session local qualification</title><script type=module src=/browser.js></script>",
);
const sessionId = "sess_native_browser_fixture";
const tracks = [
  { name: "browser_video", kind: "video", direction: "recvonly" },
  { name: "browser_audio", kind: "audio", direction: "recvonly" },
];
/** @typedef {{localCandidateType?: string, remoteCandidateType?: string}} SelectedPair */
/** @typedef {{ok: true, localPeer: object, native: {relay?: {selected?: SelectedPair}}} | {ok: false, error: string}} BrowserReport */
/** @typedef {{sdp_offer: string, track_mapping: import("reactor-effect-client").Mapping[]}} OfferRequest */
/** @typedef {{candidates: import("reactor-effect-client/host").IceCandidate[], is_final?: boolean}} IceRequest */
/** @typedef {{sdp: string, mapping: import("reactor-effect-client").Mapping[], fixture: typeof browserFixture}} Prepared */
/** @type {Prepared | undefined} */
let prepared;
/** @type {string | undefined} */
let answer;
let remoteClosed = false,
  deletes = 0,
  allocations = 0;
/** @type {RTCIceCandidateInit[]} */
const nativeCandidates = [];
let nativeRelayCandidates = 0,
  nativeFilteredCandidates = 0,
  nativeIceComplete = false;
/** @type {ReactorError | undefined} */
let nativeFailure;
/** @type {BrowserReport | undefined} */
let browserReport;
/** @returns {BrowserReport | undefined} */
const reportNow = () => browserReport;
let browserClosed = false,
  finish = false,
  stopping = false;
let videoFrames = 0,
  metadataBytes = 0,
  metadataFrames = 0,
  audioPackets = 0,
  maxAudioRms = 0;
const videoHashes = new Set();
/** @type {import("reactor-effect-native").VideoFrame | undefined} */
let firstVideo;
/** @type {string | undefined} */
let firstVideoHash;
/** @type {import("node:http").Server | undefined} */
let server;
/** @type {string | undefined} */
let profile;
/** @type {import("node:child_process").ChildProcess | undefined} */
let browser;
/** @typedef {{code: number | null, signal: NodeJS.Signals | null} | {error: Error}} BrowserExit */
/** @type {BrowserExit | undefined} */
let browserExit;
/** @type {Error | undefined} */
let browserSpawnError;
/** @type {((value: BrowserExit) => void) | undefined} */
let resolveBrowserExit;
let browserStderr = "";
/** @type {Promise<BrowserExit> | undefined} */
let browserExitPromise;
const descriptor = () => ({
  session_id: sessionId,
  state: remoteClosed ? "CLOSED" : "ACTIVE",
  capabilities: {
    protocol_version: "1.0",
    tracks,
    commands: [
      { name: "ack", schema: {} },
      { name: "echo", schema: {} },
    ],
  },
  selected_transport: { protocol: "webrtc", version: "1.0" },
});
/** @param {import("node:http").IncomingMessage} request @returns {Promise<unknown>} */
const readJson = async (request) => {
  const chunks = [];
  let length = 0;
  for await (const chunk of request) {
    length += chunk.length;
    assert(length <= 2 * 1024 * 1024, "fixture HTTP body exceeded its bound");
    chunks.push(chunk);
  }
  const body = Buffer.concat(chunks).toString("utf8");
  return body.length === 0 ? undefined : JSON.parse(body);
};
/** @param {import("node:http").ServerResponse} response @param {number} status @param {unknown} value */
const sendJson = (response, status, value) => {
  const body = Buffer.from(
    JSON.stringify(value, (_key, entry) => (typeof entry === "bigint" ? String(entry) : entry)),
  );
  response.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": body.length,
    "Cache-Control": "no-store",
  });
  response.end(body);
};
/** @param {import("node:http").IncomingMessage} request @param {import("node:http").ServerResponse} response */
const requestHandler = async (request, response) => {
  try {
    const url = new URL(request.url ?? "/", "http://127.0.0.1"),
      path = url.pathname;
    if (request.method === "GET" && path === "/") {
      response.writeHead(200, { "Content-Type": "text/html", "Content-Length": page.length });
      response.end(page);
      return;
    }
    if (request.method === "GET" && path === "/browser.js") {
      response.writeHead(200, {
        "Content-Type": "text/javascript",
        "Content-Length": browserJs.length,
        "Cache-Control": "no-store",
      });
      response.end(browserJs);
      return;
    }
    if (path === "/sessions" && request.method === "POST") {
      allocations++;
      sendJson(response, 200, descriptor());
      return;
    }
    if (path === `/sessions/${sessionId}`) {
      if (request.method === "DELETE") {
        remoteClosed = true;
        deletes++;
        sendJson(response, 202, {});
        return;
      }
      sendJson(response, 200, descriptor());
      return;
    }
    if (path.endsWith("/ice_servers")) {
      sendJson(response, 200, { ice_servers: iceServers });
      return;
    }
    if (path.endsWith("/connections")) {
      sendJson(response, 200, { connection_id: 1001 });
      return;
    }
    if (path.endsWith("/sdp_params")) {
      if (request.method === "GET") {
        if (browserReport?.ok === false) {
          sendJson(response, 500, { error: "browser fixture failed before answering" });
          return;
        }
        sendJson(
          response,
          answer === undefined ? 202 : 200,
          answer === undefined ? {} : { sdp_answer: answer },
        );
        return;
      }
      const body = /** @type {OfferRequest} */ (await readJson(request));
      assert(
        typeof body?.sdp_offer === "string" && Array.isArray(body?.track_mapping),
        "public native owner submitted invalid SDP/mapping",
      );
      prepared = { sdp: body.sdp_offer, mapping: body.track_mapping, fixture: browserFixture };
      sendJson(response, 200, {});
      return;
    }
    if (path.endsWith("/ice_candidates")) {
      const body = /** @type {IceRequest} */ (await readJson(request));
      assert(Array.isArray(body?.candidates), "native owner omitted ICE candidate batch");
      for (const candidate of body.candidates) {
        const relay = /(?:^|\s)typ relay(?:\s|$)/i.test(candidate.candidate);
        if (relay) nativeRelayCandidates++;
        if (forceRelay && !relay) {
          nativeFilteredCandidates++;
          continue;
        }
        assert(nativeCandidates.length < 1024, "native candidate fixture bound exceeded");
        nativeCandidates.push({
          candidate: candidate.candidate,
          ...(candidate.sdp_mid == null ? {} : { sdpMid: candidate.sdp_mid }),
          ...(candidate.sdp_mline_index == null
            ? {}
            : { sdpMLineIndex: candidate.sdp_mline_index }),
        });
      }
      nativeIceComplete ||= body.is_final === true;
      sendJson(response, 200, {});
      return;
    }
    if (path === "/native-offer") {
      sendJson(response, prepared === undefined ? 202 : 200, prepared ?? {});
      return;
    }
    if (path === "/native-ice") {
      const cursor = Number(url.searchParams.get("cursor") ?? "0");
      assert(
        Number.isSafeInteger(cursor) && cursor >= 0 && cursor <= nativeCandidates.length,
        "invalid ICE cursor",
      );
      sendJson(response, 200, {
        candidates: nativeCandidates.slice(cursor),
        complete: nativeIceComplete,
      });
      return;
    }
    if (path === "/native-answer" && request.method === "POST") {
      const body = /** @type {{sdp: string}} */ (await readJson(request));
      assert(typeof body?.sdp === "string", "browser omitted answer SDP");
      answer = body.sdp;
      sendJson(response, 200, { stored: true });
      return;
    }
    if (path === "/browser-report" && request.method === "POST") {
      browserReport = /** @type {BrowserReport} */ (await readJson(request));
      sendJson(response, 200, { stored: true });
      return;
    }
    if (path === "/browser-closed" && request.method === "POST") {
      browserClosed = true;
      sendJson(response, 200, { stored: true });
      return;
    }
    if (path === "/finish") {
      sendJson(response, 200, { finish });
      return;
    }
    sendJson(response, 404, { error: "unhandled local fixture route" });
  } catch (error) {
    sendJson(response, 500, { error: error instanceof Error ? error.message : String(error) });
  }
};
const hostFailure = () => {
  if (nativeFailure !== undefined) throw nativeFailure;
  if (browserSpawnError !== undefined) throw browserSpawnError;
  if (browserReport?.ok === false) fail(`browser qualification failed: ${browserReport.error}`);
  if (browserExit !== undefined && !browserClosed)
    fail(`Chrome exited before cleanup: ${JSON.stringify(browserExit)}; ${browserStderr}`);
};
/** @template T @param {() => T} run @returns {Effect.Effect<T, ReactorError>} */
const testEffect = (run) =>
  Effect.try({
    try: run,
    catch: (cause) =>
      ReactorError.fromCode("Protocol", "public native media fixture assertion failed", {
        detail: cause,
      }),
  });
/** @template T @param {Stream.Stream<T, ReactorError>} source @param {(frame: T) => void} visit @returns {Effect.Effect<void>} */
const joinMedia = (source, visit) =>
  source.pipe(
    Stream.runForEach((frame) => testEffect(() => visit(frame))),
    Effect.catch((error) =>
      Effect.sync(() => {
        if (!stopping) nativeFailure ??= error;
      }),
    ),
  );
let summary, runError;
try {
  // requestHandler answers its own failures, so its promise is not awaited.
  const fixtureServer = createServer((request, response) => void requestHandler(request, response));
  server = fixtureServer;
  await new Promise((resolveListen, rejectListen) => {
    fixtureServer.once("error", rejectListen);
    fixtureServer.listen(0, "127.0.0.1", () => resolveListen(undefined));
  });
  const address = server.address();
  assert(address !== null && typeof address !== "string", "fixture server did not bind IPv4");
  const url = `http://127.0.0.1:${address.port}`;
  profile = mkdtempSync(join(tmpdir(), "reactor-public-browser-native-"));
  const chrome = browserExecutable();
  browser = spawn(
    chrome,
    [
      "--headless=new",
      "--autoplay-policy=no-user-gesture-required",
      "--disable-background-networking",
      "--disable-component-update",
      "--disable-default-apps",
      "--disable-sync",
      "--no-first-run",
      "--no-default-browser-check",
      "--metrics-recording-only",
      "--disable-features=WebRtcHideLocalIpsWithMdns",
      "--disable-dev-shm-usage",
      ...(process.env.BROWSER_NATIVE_NO_SANDBOX === "1" ? ["--no-sandbox"] : []),
      `--user-data-dir=${profile}`,
      `${url}/`,
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  browserExitPromise = new Promise((resolveExit) => {
    resolveBrowserExit = resolveExit;
  });
  browser.stderr?.setEncoding("utf8");
  browser.stderr?.on("data", (chunk) => {
    browserStderr = `${browserStderr}${chunk}`.slice(-16_384);
  });
  browser.on("error", (error) => {
    browserSpawnError ??= error;
    resolveBrowserExit?.({ error });
  });
  browser.on("exit", (code, signal) => {
    browserExit = { code, signal };
    resolveBrowserExit?.(browserExit);
  });
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const peers = yield* Layer.build(Native.layer());
        const factory = yield* makeClient({
          apiUrl: url,
          sdpPoll: { attempts: 200, initialMs: 50, maxMs: 200 },
          session: { connectTimeoutMs: 45_000, readyTimeoutMs: 45_000, commandTimeoutMs: 5000 },
        }).pipe(Effect.provide(peers));
        const session = yield* factory.createConnected({ model: "fixture/native-browser" });
        const ready = yield* session.ready;
        assert(ready.remote.ownership === "owned", "native public session lost ownership");
        /** @type {import("reactor-effect-client").CommandReply[]} */
        const events = [];
        yield* Effect.forkScoped(
          session.events().pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                if (event._tag === "Model") {
                  assert(events.length < 64, "model observation fixture bound exceeded");
                  events.push(event);
                }
              }),
            ),
          ),
        );
        const media = yield* Native.media(session);
        assert(
          media.generation === ready.generation,
          "native public media did not capture ready generation",
        );
        yield* Effect.forkScoped(
          joinMedia(media.video("browser_video"), (frame) => {
            assert(
              frame._tag === "VideoFrame" &&
                frame.width === 160 &&
                frame.height === 96 &&
                frame.data.length === 160 * 96 * 4,
              "native decoded video shape mismatch",
            );
            assert(
              typeof frame.frameId === "bigint" && typeof frame.timestampMicros === "bigint",
              "native video lost 64-bit metadata",
            );
            const hash = fnv(frame.data);
            if (videoHashes.size < 256) videoHashes.add(hash);
            videoFrames++;
            metadataBytes += frame.metadata.length;
            if (frame.metadata.length > 0) metadataFrames++;
            if (firstVideo === undefined) {
              firstVideo = frame;
              firstVideoHash = hash;
            }
          }),
        );
        yield* Effect.forkScoped(
          joinMedia(media.audio("browser_audio"), (frame) => {
            assert(
              frame._tag === "AudioFrame" && frame.sampleRate === 48000 && frame.channels === 1,
              "native decoded PCM shape mismatch",
            );
            audioPackets++;
            maxAudioRms = Math.max(maxAudioRms, pcmRms(frame.samples));
          }),
        );
        yield* Effect.yieldNow;
        const schema = yield* session.schema;
        const ack = yield* session.command("ack", {});
        const reply = yield* session.command("echo", { bytes: [0xa2, 0x20, 0x21, 0x22] });
        yield* Effect.promise(() =>
          waitUntil(
            () => events.includes(ack) && events.includes(reply),
            1000,
            "native model observation delivery",
          ),
        );
        assert(schema.openapi?.openapi === "3.1.0", "native control channel lost schema response");
        assert(
          ack.kind === "ack" && reply.kind === "message" && reply.type === "echo",
          "native command lost ACK/reply distinction",
        );
        assert(
          events.includes(ack) && events.includes(reply),
          "native return/event attribution is not the same object",
        );
        assert(
          ack.sequence < reply.sequence && reply.generation === ready.generation,
          "native reply sequence/generation invalid",
        );
        assert(
          JSON.stringify(reply.data) === JSON.stringify({ bytes: [0xa2, 0x20, 0x21, 0x22] }),
          "native echo data changed",
        );
        yield* Effect.promise(() =>
          waitUntil(
            () => {
              hostFailure();
              return (
                browserReport !== undefined &&
                videoFrames >= 12 &&
                videoHashes.size >= 3 &&
                audioPackets >= 12 &&
                maxAudioRms > 0.001
              );
            },
            15_000,
            "public native/browser messages and decoded media",
          ),
        );
        hostFailure();
        const statistics = yield* session.stats;
        let relay;
        if (forceRelay) {
          assert(nativeRelayCandidates > 0, "native gathered no relay candidates");
          assert(
            statistics.pair?.localCandidateType === "relay" &&
              statistics.pair?.remoteCandidateType === "relay",
            "native active nominated/succeeded pair is not relay/relay",
          );
          const report = reportNow();
          assert(report?.ok === true, "browser omitted successful relay report");
          const selected = report.native.relay?.selected;
          assert(
            selected?.localCandidateType === "relay" && selected?.remoteCandidateType === "relay",
            "browser selected ICE pair is not relay/relay",
          );
          relay = {
            forced: true,
            native: statistics.pair,
            browser: selected,
            gatheredRelayCandidates: nativeRelayCandidates,
            filteredNonRelayCandidates: nativeFilteredCandidates,
            writableProof: "bidirectional SCTP plus decoded RTP",
          };
        }
        const pressure = yield* media.snapshot;
        stopping = true;
        const close = yield* session.close;
        assert(
          close.localClosed && close.localErrors.length === 0 && close.remote.confirmed,
          "native joined cleanup/remote confirmation failed",
        );
        assert(
          deletes === 1 && allocations === 1,
          "native session ownership did not allocate/terminate exactly once",
        );
        assert(
          firstVideo !== undefined && fnv(firstVideo.data) === firstVideoHash,
          "decoded frame bytes did not survive native destruction",
        );
        finish = true;
        yield* Effect.promise(() =>
          waitUntil(
            () => browserClosed || browserExit !== undefined,
            5000,
            "browser cleanup report",
          ),
        );
        assert(browserClosed, "browser fixture did not join its media cleanup");
        return {
          generation: String(ready.generation),
          schema: schema.openapi.openapi,
          ack: ack.kind,
          reply: reply.kind,
          sameAttributedObjects: true,
          close,
          pressure,
          relay,
        };
      }),
    ).pipe(Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer))),
  );
  const identity = JSON.parse(
    readFileSync(
      join(
        root,
        "../packages/native/lib",
        `${process.platform}-${process.arch}`,
        "native-identity.json",
      ),
      "utf8",
    ),
  );
  const report = reportNow();
  assert(report?.ok === true, "browser omitted its successful public session report");
  summary = [
    "browser-native-ok",
    `host ${process.platform}-${process.arch}`,
    `browser ${chrome}`,
    `native-artifact sha256=${identity.sha256} sourceSha256=${identity.build.sourceSha256} abi=${identity.build.abiVersion}`,
    `browser-public ${JSON.stringify(report.localPeer)}`,
    `browser-native ${JSON.stringify(report.native)}`,
    `native-public ${JSON.stringify(result, (_key, value) => (typeof value === "bigint" ? String(value) : value))}`,
    `native-video frames=${videoFrames} distinctHashes=${videoHashes.size} metadataFrames=${metadataFrames} metadataBytes=${metadataBytes}`,
    `native-audio packets=${audioPackets} maxRms=${maxAudioRms.toFixed(6)}`,
    `turn-relay ${forceRelay ? "qualified through public session statistics and media" : "disabled"}`,
    `metadata-limit ${metadataBytes === 0 ? "standards browser emitted no Reactor custom frame metadata; decoded A/V is real" : "custom frame metadata delivered"}`,
  ];
} catch (error) {
  runError = error;
}
const cleanupErrors = [];
try {
  finish = true;
  stopping = true;
  if (browser !== undefined && browserSpawnError === undefined && browserExit === undefined) {
    browser.kill("SIGTERM");
    await Promise.race([
      browserExitPromise,
      new Promise((resolveDelay) => setTimeout(resolveDelay, 1000)),
    ]);
  }
  if (browser !== undefined && browserSpawnError === undefined && browserExit === undefined) {
    browser.kill("SIGKILL");
    await Promise.race([
      browserExitPromise,
      new Promise((resolveDelay) => setTimeout(resolveDelay, 2000)),
    ]);
  }
  if (browser !== undefined && browserSpawnError === undefined && browserExit === undefined)
    cleanupErrors.push(new Error("Chrome did not join bounded TERM/KILL cleanup"));
} catch (error) {
  cleanupErrors.push(error);
} finally {
  if (server !== undefined) {
    const fixtureServer = server;
    try {
      fixtureServer.closeAllConnections?.();
      await new Promise((resolveClose, rejectClose) =>
        fixtureServer.close((error) =>
          error === undefined ? resolveClose(undefined) : rejectClose(error),
        ),
      );
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (profile !== undefined) {
    try {
      // Chrome's helper processes can still be writing the profile cache for a
      // moment after the main process has exited; retry instead of racing them.
      rmSync(profile, { recursive: true, force: true, maxRetries: 20, retryDelay: 250 });
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
}
if (runError !== undefined) {
  const report = reportNow();
  if (report?.ok === false) console.error(`browser-failure ${report.error}`);
  if (cleanupErrors.length > 0)
    throw new AggregateError(
      [runError, ...cleanupErrors],
      "browser/native qualification and cleanup failed",
    );
  throw runError instanceof Error
    ? runError
    : new Error("browser/native qualification failed", { cause: runError });
}
if (cleanupErrors.length > 0)
  throw new AggregateError(cleanupErrors, "browser/native cleanup failed");
for (const line of summary ?? []) console.log(line);
