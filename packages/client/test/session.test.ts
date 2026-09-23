import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import { ReactorError } from "../src/errors.js";
import type { CloseReport } from "../src/SessionTypes.js";
import { structFromObject, record } from "../src/json.js";
import * as W from "../src/wire.generated.js";
import { withFixture, makeSession, MockPeer, FakeTrack, stall, jsonResponse } from "./fixtures.js";
import { test, assert, equal, failure, run, eventually } from "./harness.js";
const peerAt = (peers: readonly MockPeer[], index = 0): MockPeer => {
  const p = peers[index];
  assert(p !== undefined, "mock peer missing");
  return p;
};
const dataSent = (peer: MockPeer): W.DataClientMessage[] =>
  peer.sent.filter((s) => s.channel === "data").map((s) => W.DataClientMessage.decode(s.bytes));
const controlSent = (peer: MockPeer): W.ControlClientMessage[] =>
  peer.sent
    .filter((s) => s.channel === "control")
    .map((s) => W.ControlClientMessage.decode(s.bytes));

test("session cleanup: a stalled publication release cannot prevent remote termination", ({
  signal,
}) =>
  withFixture(async (fixture) => {
    const { session, peers } = makeSession(fixture, { commandTimeoutMs: 20 });
    const release = Deferred.makeUnsafe<void>();
    let cleanup: Promise<CloseReport> | undefined;
    let finished = false;
    let interrupted = false;
    try {
      await run(session.start(), { signal });
      await run(session.publish("input_audio", new FakeTrack("audio")), { signal });
      const peer = peerAt(peers);
      const send = peer.send.bind(peer);
      peer.send = (channel, bytes) =>
        channel === "control" &&
        W.ControlClientMessage.decode(bytes).payload?.case === "unpublish_track"
          ? Deferred.await(release).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  interrupted = true;
                }),
              ),
            )
          : send(channel, bytes);
      const closing = run(session.close(), { signal });
      cleanup = closing;
      void closing.then(() => {
        finished = true;
      });
      await new Promise<void>((resolve) => setTimeout(resolve, 150));
      assert(finished, "close deadline inherited an uninterruptible publication wait");
      const report = await closing;
      assert(interrupted, "publication wait was not interrupted");
      assert(report.localErrors.some((error) => error.reason._tag === "Timeout"));
      assert(report.remote.confirmed, "remote cleanup was skipped after publication failure");
      equal(await run(session.close(), { signal }), report);
    } finally {
      Deferred.doneUnsafe(release, Effect.void);
      if (cleanup !== undefined) await cleanup;
      else await run(session.close());
    }
  }));

test("session cleanup: a publication release defect is reported and remote cleanup still runs", ({
  signal,
}) =>
  withFixture(async (fixture) => {
    const { session, peers } = makeSession(fixture);
    await run(session.start(), { signal });
    await run(session.publish("input_audio", new FakeTrack("audio")), { signal });
    const peer = peerAt(peers);
    const send = peer.send.bind(peer);
    peer.send = (channel, bytes) =>
      channel === "control" &&
      W.ControlClientMessage.decode(bytes).payload?.case === "unpublish_track"
        ? Effect.die("fixture publication cleanup defect")
        : send(channel, bytes);
    const report = await run(session.close(), { signal });
    equal(report.localClosed, false);
    assert(report.localErrors.some((error) => error.reason._tag === "Shutdown"));
    assert(report.remote.confirmed);
    equal(session.snapshot.status, "closed");
  }));

test("session cleanup: a coordinator defect preserves an unconfirmed report and resolves repeated close", ({
  signal,
}) =>
  withFixture(async (fixture) => {
    const { session } = makeSession(fixture);
    await run(session.start(), { signal });
    session.http.terminate = () => Effect.die("fixture coordinator cleanup defect");
    const report = await run(session.close(), { signal });
    equal(report.localClosed, true);
    equal(report.remote.confirmed, false);
    equal(report.remote.error?.reason._tag, "Shutdown");
    equal(report.remote.error?.context.outcome, "unknown");
    equal(await run(session.close(), { signal }), report);
    equal(session.snapshot.status, "closed");
  }));

test("session policy: source registration/ICE/SDP order, readiness and immediate correlated bodyless ACK", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      equal(s.snapshot.status, "ready");
      const reply = await run(s.command("echo", {}), { signal });
      equal(reply.kind, "ack");
      equal(reply.raw, { request_id: "data_1", kind: 2 });
      equal(reply.requestId, "data_1");
      equal(reply.generation, 1n);
      equal(reply.correlation, "matched");
      equal(reply.outcome, "replied");
      assert(reply.sequence > 0n);
      const reg = f.order.indexOf(`POST /sessions/${f.sessionId}/transport/webrtc/connections`),
        ice = f.order.indexOf(
          `POST /sessions/${f.sessionId}/transport/webrtc/connections/1001/ice_candidates`,
        ),
        offer = f.order.indexOf(
          `POST /sessions/${f.sessionId}/transport/webrtc/connections/1001/sdp_params`,
        );
      assert(
        f.order.indexOf("peer.prepare") < reg &&
          reg < ice &&
          ice < offer &&
          offer < f.order.indexOf("peer.answer"),
      );
      const iceCall = f.calls.find((c) => c.url.pathname.endsWith("/ice_candidates"));
      assert(iceCall !== undefined);
      equal(record(JSON.parse(iceCall.body)).is_final, true);
      equal(controlSent(peerAt(peers)).filter((m) => m.payload?.case === "resume_track").length, 2);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: 32 concurrent replies resolve by ID in reverse order, independent of observers", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 1000 });
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      const tasks = Array.from({ length: 32 }, (_, i) =>
        Effect.runPromise(Effect.result(s.command(`command-${i}`, { i }))),
      );
      await eventually(() => s.snapshot.pending.data === 32);
      for (const message of dataSent(p).reverse())
        p.replyData({
          request_id: message.request_id,
          kind: 2,
          payload: {
            case: "message",
            value: {
              type: message.payload?.case === "command" ? message.payload.value.type : "missing",
              data: structFromObject({ ok: true }),
            },
          },
        });
      const results = await Promise.all(tasks);
      results.forEach((r, i) => {
        assert(r._tag === "Success");
        assert(r.success.kind === "message");
        equal(r.success.type, `command-${i}`);
      });
      equal(s.snapshot.pending.data, 0);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: absent model data, empty Struct, null fields and correlated remote errors stay distinct", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      p.sendHook = (channel, bytes) => {
        if (channel !== "data") return;
        const m = W.DataClientMessage.decode(bytes);
        assert(m.payload?.case === "command");
        const kind = m.payload.value.type;
        p.replyData({
          request_id: m.request_id,
          kind: 2,
          payload:
            kind === "error"
              ? { case: "error", value: { code: "MODEL_ERROR", message: "reason" } }
              : {
                  case: "message",
                  value: {
                    type: "result",
                    ...(kind === "absent"
                      ? {}
                      : { data: structFromObject(kind === "empty" ? {} : { field: null }) }),
                  },
                },
        });
      };
      const absent = await run(s.command("absent", {}), { signal });
      assert(absent.kind === "message");
      assert(!("data" in absent));
      const empty = await run(s.command("empty", {}), { signal });
      assert(empty.kind === "message");
      equal(empty.data, {});
      const nullable = await run(s.command("null", {}), { signal });
      assert(nullable.kind === "message");
      equal(nullable.data, { field: null });
      const e = await failure(s.command("error", {}), { signal });
      assert(e.reason._tag === "Remote");
      equal(e.reason.remoteCode, "MODEL_ERROR");
      equal(e.context.outcome, "replied");
      // Provider text is kept for inspection, never in the message.
      equal(e.message, "remote command error MODEL_ERROR");
      equal(e.reason.body, "reason");
      const count = dataSent(p).length;
      // Caller data that is not a JSON object is invalid input, never submitted.
      const invalid = await failure(s.command("invalid", []), { signal });
      equal([invalid.reason._tag, invalid.context.outcome], ["InvalidInput", "not-submitted"]);
      equal(dataSent(p).length, count);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: bodyless control does not ACK; schema validates its correlated payload", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 10 });
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      p.sendHook = (channel, bytes) => {
        if (channel === "control") {
          const m = W.ControlClientMessage.decode(bytes);
          p.replyControl({ request_id: m.request_id, kind: 2 });
        }
      };
      equal((await failure(s.schema(), { signal })).reason._tag, "Timeout");
      equal(s.snapshot.pending.control, 1);
      p.autoReply = true;
      p.sendHook = undefined;
      equal((await run(s.schema(), { signal })).openapi, { openapi: "3.1.0", paths: {} });
    } finally {
      await run(s.close());
    }
  }));
test("session policy: readiness waits for connected peer AND BOTH channels, after HTTP answer", () =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, {}, (p) => {
      p.answerHook = () => Effect.void;
    });
    try {
      const task = Effect.runPromise(Effect.result(s.start()));
      await eventually(() => peers[0]?.answers === 1);
      const p = peerAt(peers);
      equal(s.snapshot.status, "waiting");
      p.emit({ type: "channel", channel: "data", open: true });
      p.emit({ type: "state", state: "connected" });
      equal(s.snapshot.status, "waiting");
      p.emit({ type: "channel", channel: "control", open: true });
      const result = await task;
      assert(result._tag === "Success");
      equal(s.snapshot.status, "ready");
    } finally {
      await run(s.close());
    }
  }));
test("session policy: failure during SDP HTTP exchange wins over answer/application errors", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    f.hook = (call) => {
      if (call.method === "GET" && call.url.pathname.endsWith("/sdp_params")) {
        peerAt(peers).emit({ type: "state", state: "failed" });
        return jsonResponse({ sdp_answer: "late answer" });
      }
      return undefined;
    };
    try {
      const e = await failure(s.start(), { signal });
      equal(e.reason._tag, "Disconnected");
      assert(e.message.includes("failed"));
      equal(peerAt(peers).answers, 0);
      equal(s.snapshot.status, "disconnected");
    } finally {
      await run(s.close());
    }
  }));
test("session policy: failure DURING setRemoteDescription preserves the actual peer teardown cause", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s } = makeSession(f, {}, (p) => {
      p.answerHook = () =>
        Effect.sync(() => p.emit({ type: "state", state: "failed" })).pipe(
          Effect.andThen(Effect.fail(ReactorError.fromCode("InvalidState", "generic closed peer"))),
        );
    });
    try {
      const e = await failure(s.start(), { signal });
      equal(e.reason._tag, "Disconnected");
      assert(e.message.includes("failed"));
    } finally {
      await run(s.close());
    }
  }));
test("session policy: a closed data channel fails the connection as ChannelClosed naming it", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      const generation = s.snapshot.generation;
      peerAt(peers).emit({ type: "channel", channel: "control", open: false });
      await eventually(() => s.snapshot.status === "disconnected");
      const error = s.snapshot.lastError;
      assert(error !== undefined, "channel closure left no diagnostic");
      equal(error.reason._tag, "ChannelClosed");
      equal(error.context.generation, generation);
      equal(error.context.detail, { channel: "control" });
    } finally {
      await run(s.close());
    }
  }));
test("session policy: missing readiness times out and releases the generation", ({ signal }) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { readyTimeoutMs: 10 }, (p) => {
      p.answerHook = () =>
        Effect.sync(() => p.emit({ type: "channel", channel: "data", open: true }));
    });
    try {
      equal((await failure(s.start(), { signal })).reason._tag, "Timeout");
      assert(peerAt(peers).closes > 0);
      equal(s.snapshot.status, "disconnected");
    } finally {
      await run(s.close());
    }
  }));
test("session policy: reconnect reuses connection with PUT, fails uncertain commands and fences retired callbacks", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 1000 });
    try {
      await run(s.start(), { signal });
      const old = peerAt(peers);
      old.autoReply = false;
      const pending = Effect.runPromise(Effect.result(s.command("side-effect", {})));
      await eventually(() => s.snapshot.pending.data === 1);
      await run(s.reconnect(), { signal });
      const lost = await pending;
      assert(lost._tag === "Failure");
      equal(lost.failure.context.outcome, "unknown");
      const next = peerAt(peers, 1);
      next.autoReply = false;
      const task = Effect.runPromise(Effect.result(s.command("new-command", {})));
      await eventually(() => s.snapshot.pending.data === 1);
      const message = dataSent(next)[0];
      assert(message !== undefined);
      old.replyData({ request_id: message.request_id, kind: 2 });
      equal(s.snapshot.pending.data, 1);
      next.replyData({ request_id: message.request_id, kind: 2 });
      assert((await task)._tag === "Success");
      equal(f.calls.filter((c) => c.url.pathname === "/sessions" && c.method === "POST").length, 1);
      equal(f.calls.filter((c) => c.url.pathname.endsWith("/connections")).length, 1);
      equal(
        f.calls
          .filter((c) => c.url.pathname.endsWith("/sdp_params") && c.method !== "GET")
          .map((c) => c.method),
        ["POST", "PUT"],
      );
      equal(dataSent(old).length, 1);
      equal(dataSent(next).length, 1);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: late/duplicate model replies are observable, never settle a second command", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 10 });
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      const events = Effect.runPromise(
        Effect.result(Stream.runCollect(s.events().pipe(Stream.take(4)))),
      );
      await eventually(() => s.snapshot.subscribers === 1);
      equal((await failure(s.command("late", {}), { signal })).reason._tag, "Timeout");
      const old = dataSent(p)[0];
      assert(old !== undefined);
      p.replyData({
        request_id: old.request_id,
        kind: 2,
        payload: { case: "message", value: { type: "late-result" } },
      });
      p.autoReply = true;
      await run(s.command("matched", {}), { signal });
      const matched = dataSent(p)[1];
      assert(matched !== undefined);
      const reply = {
        request_id: matched.request_id,
        kind: 2,
        payload: { case: "message" as const, value: { type: "result-after-ack" } },
      };
      p.replyData(reply);
      p.replyData(reply);
      const result = await events;
      assert(result._tag === "Success");
      equal(
        result.success.map((e) => (e._tag === "Model" ? e.correlation : e._tag)),
        ["late", "matched", "late", "duplicate"],
      );
      equal(s.snapshot.pending.data, 0);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: observation overflow fails only the slow subscriber, not correlated replies", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      p.sendHook = (channel, bytes) => {
        if (channel === "data") {
          const m = W.DataClientMessage.decode(bytes);
          p.replyData({
            request_id: m.request_id,
            kind: 2,
            payload: {
              case: "message",
              value: { type: "ok", data: structFromObject({ large: "x".repeat(100) }) },
            },
          });
        }
      };
      const events = Effect.runPromise(
        Effect.result(Stream.runDrain(s.events({ maxBytes: 1, capacity: 1 }))),
      );
      await eventually(() => s.snapshot.subscribers === 1);
      const reply = await run(s.command("echo", {}), { signal });
      assert(reply.kind === "message");
      const result = await events;
      assert(result._tag === "Failure");
      equal(result.failure.reason._tag, "Overflow");
      equal(s.snapshot.pending.data, 0);
      equal(s.snapshot.observationOverflows, 1n);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: interrupted observer releases its registration while submitted command remains session-owned", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 1000 });
    try {
      await run(s.start(), { signal });
      peerAt(peers).autoReply = false;
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const command = yield* Effect.forkScoped(s.command("cancel-me", {}));
            const observer = yield* Effect.forkScoped(Stream.runDrain(s.events()));
            while (s.snapshot.pending.data !== 1 || s.snapshot.subscribers !== 1)
              yield* Effect.sleep(1);
            yield* Fiber.interrupt(command);
            yield* Fiber.interrupt(observer);
            equal(s.snapshot.pending.data, 1);
            equal(s.snapshot.subscribers, 0);
            equal(s.snapshot.status, "ready");
          }),
        ),
      );
      equal(f.calls.filter((c) => c.method === "DELETE").length, 0);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: pending count overflow does not send an extra side effect", ({ signal }) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { maxPending: 1, commandTimeoutMs: 1000 });
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      const first = Effect.runPromise(Effect.result(s.command("one", {})));
      await eventually(() => s.snapshot.pending.data === 1);
      const error = await failure(s.command("two", {}), { signal });
      equal(error.reason._tag, "Overflow");
      equal(error.context.outcome, "not-submitted");
      // Local backpressure proves no dispatch, so the command can wait and retry.
      equal(error.isRetryable, true);
      equal(dataSent(p).length, 1);
      p.emit({ type: "state", state: "failed" });
      const result = await first;
      assert(result._tag === "Failure");
      equal(result.failure.context.outcome, "unknown");
      // A lost connection alone would be retryable; the unknown outcome forbids it.
      equal([result.failure.reason._tag, result.failure.isRetryable], ["Disconnected", false]);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: allocation cancelled by close records unknown remote outcome, without retry", ({
  signal,
}) =>
  withFixture(async (f) => {
    f.hook = (c) =>
      c.url.pathname === "/sessions" && c.method === "POST" ? stall(c.signal) : undefined;
    const { session: s } = makeSession(f, { requestTimeoutMs: 1000 });
    const task = Effect.runPromise(Effect.result(s.start()));
    await eventually(() => f.calls.some((c) => c.url.pathname === "/sessions"));
    const close = await run(s.close(), { signal });
    equal(close.allocation, "unknown");
    equal(close.remote.attempted, false);
    assert((await task)._tag === "Failure");
    equal(f.calls.filter((c) => c.method === "POST").length, 1);
    equal(s.snapshot.status, "closed");
  }));
test("session policy: concurrent connect/reconnect calls reject illegal state transitions", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, {}, (p) => {
      p.answerHook = () => Effect.void;
    });
    try {
      const first = Effect.runPromise(Effect.result(s.start()));
      await eventually(() => peers[0]?.answers === 1);
      equal((await failure(s.start(), { signal })).reason._tag, "InvalidState");
      equal((await failure(s.reconnect(), { signal })).reason._tag, "InvalidState");
      peerAt(peers).emit({ type: "state", state: "failed" });
      assert((await first)._tag === "Failure");
    } finally {
      await run(s.close());
    }
  }));
test("session policy: owned close is idempotent; accepted deletion alone is not confirmed termination", ({
  signal,
}) =>
  withFixture(async (f) => {
    f.terminateConfirms = false;
    const { session: s } = makeSession(f);
    await run(s.start(), { signal });
    const [a, b] = await Promise.all([run(s.close(), { signal }), run(s.close(), { signal })]);
    equal(a, b);
    equal(a.localClosed, true);
    equal(a.remote.attempted, true);
    equal(a.remote.responseReceived, true);
    equal(a.remote.confirmed, false);
    equal(a.remote.evidence, null);
    equal(a.remote.deleteStatus, 202);
    equal(a.remote.state, "ACTIVE");
    equal(f.calls.filter((c) => c.method === "DELETE").length, 1);
  }));
test("session policy: attached/pre-registered connection is never deleted and initially uses SDP POST", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s } = makeSession(f, {
      intent: { _tag: "Attach", sessionId: f.sessionId, connectionId: 2345 },
    });
    await run(s.start(), { signal });
    const report = await run(s.close(), { signal });
    equal(report.ownership, "attached");
    equal(report.remote.attempted, false);
    equal(
      f.calls.filter(
        (c) =>
          c.method === "DELETE" ||
          c.url.pathname.endsWith("/connections") ||
          c.url.pathname === "/sessions",
      ).length,
      0,
    );
    assert(f.calls.some((c) => c.method === "POST" && c.url.pathname.endsWith("/2345/sdp_params")));
  }));
test("session policy: close has a bounded termination request even inside an uninterruptible finalizer", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s } = makeSession(f, { requestTimeoutMs: 10 });
    await run(s.start(), { signal });
    f.hook = (c) => (c.method === "DELETE" ? stall(c.signal) : undefined);
    const report = await run(s.close(), { signal });
    equal(report.localClosed, true);
    equal(report.remote.confirmed, false);
    equal(report.remote.error?.reason._tag, "Timeout");
    equal(s.snapshot.status, "closed");
  }));
test("session policy: heartbeat is immediate, generation-scoped, and stops after local close", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { heartbeatMs: 5 });
    await run(s.start(), { signal });
    const p = peerAt(peers);
    const pings = (): number => controlSent(p).filter((m) => m.payload?.case === "ping").length;
    await eventually(() => pings() >= 2);
    await run(s.close(), { signal });
    const count = pings();
    await new Promise<void>((r) => setTimeout(r, 20));
    equal(pings(), count);
  }));
test("session policy: upload sequence allocates once, omits credentials on PUT, and reports notification submission only", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      const uploaded = await run(
        s.upload("fixture.bin", "application/octet-stream", new Uint8Array([0, 128, 255])),
        { signal },
      );
      equal(uploaded.file.size, 3n);
      equal(uploaded.notification, "submitted");
      const put = f.calls.find((c) => c.method === "PUT" && c.url.hostname === "upload.fixture");
      assert(put !== undefined);
      equal(put.headers.get("authorization"), null);
      const uploadedMessage = controlSent(peerAt(peers)).find(
        (m) => m.payload?.case === "file_uploaded",
      );
      assert(uploadedMessage !== undefined);
      equal(uploadedMessage.kind, 3);
      equal(
        (await failure(s.upload("empty", "x", new Uint8Array()), { signal })).reason._tag,
        "Upload",
      );
      equal(f.calls.filter((c) => c.url.pathname.endsWith("/uploads")).length, 1);
    } finally {
      await run(s.close());
    }
  }));
test("session policy: disconnect during upload aborts transfer and retains allocation/unknown-transfer recovery facts", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      f.hook = (c) => (c.url.hostname === "upload.fixture" ? stall(c.signal) : undefined);
      const task = Effect.runPromise(Effect.result(s.upload("file", "x", new Uint8Array([1]))));
      await eventually(() => f.calls.some((c) => c.url.hostname === "upload.fixture"));
      peerAt(peers).emit({ type: "state", state: "failed" });
      const result = await task;
      assert(result._tag === "Failure");
      const detail = record(result.failure.context.detail),
        progress = record(detail.progress);
      equal(progress.allocation, "confirmed");
      equal(progress.transfer, "unknown");
      equal(record(progress.file).upload_id, "up_fixture");
      equal(
        controlSent(peerAt(peers)).filter((m) => m.payload?.case === "file_uploaded").length,
        0,
      );
    } finally {
      await run(s.close());
    }
  }));
test("session policy: publisher slot, cloned outgoing/leased incoming track ownership, and cleanup", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    const source = new FakeTrack("audio");
    await run(s.start(), { signal });
    const p = peerAt(peers);
    await run(s.publish("input_audio", source), { signal });
    equal(s.snapshot.claimedTracks, ["input_audio"]);
    assert(source.clones.length === 1);
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const track = yield* s.track("main_video");
          equal(track.readyState, "live");
          yield* s.setMaxBitrate("input_audio", 64_000);
        }),
      ),
    );
    equal(p.leases.size, 0);
    const report = await run(s.close(), { signal });
    equal(source.readyState, "live");
    equal(source.clones[0]?.readyState, "ended");
    equal(report.unpublishSubmitted, ["input_audio"]);
  }));
test("session policy: pause tracks changes local direction before notification; failed send does not falsify local state", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.sendHook = () => {
        throw ReactorError.fromCode("Overflow", "send buffer full", { outcome: "not-submitted" });
      };
      equal(
        (await failure(s.setTrackActive("main_video", false), { signal })).reason._tag,
        "Overflow",
      );
      assert(s.snapshot.pausedLocally.includes("main_video"));
    } finally {
      await run(s.close());
    }
  }));
test("session policy: recording/clip correlations and recorder-disabled error mapping", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f);
    try {
      await run(s.start(), { signal });
      equal(
        (await run(s.requestClip(1), { signal })).playlist_url,
        "https://coordinator.fixture/clip.m3u8",
      );
      equal((await run(s.recording(), { signal })).predicted_ready_at_ms, 1n);
      equal((await failure(s.requestClip(NaN), { signal })).reason._tag, "Protocol");
      const p = peerAt(peers);
      p.autoReply = false;
      p.sendHook = (channel, bytes) => {
        if (channel === "control") {
          const m = W.ControlClientMessage.decode(bytes);
          p.replyControl({
            request_id: m.request_id,
            kind: 2,
            payload: { case: "clip_failed", value: { reason: "ENCODER CRASHED" } },
          });
        }
      };
      const disabled = await failure(s.recording(), { signal });
      assert(disabled.reason._tag === "RecorderDisabled");
      equal(disabled.message, "clip failed");
      equal(disabled.reason.body, "ENCODER CRASHED");
    } finally {
      await run(s.close());
    }
  }));
// Publication ownership policies. Native peers/media are exercised separately by
// the emitted-package browser fixture, not established by these modeled senders.
test("publication session: replacement reuses a confirmed claim, owns clones, and unpublish is a notification", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f),
      first = new FakeTrack("audio"),
      second = new FakeTrack("audio");
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      await run(s.publish("input_audio", first), { signal });
      await run(s.publish("input_audio", second), { signal });
      equal(first.readyState, "live");
      equal(second.readyState, "live");
      equal(first.clones[0]?.readyState, "ended");
      equal(second.clones[0]?.readyState, "live");
      equal(controlSent(p).filter((m) => m.payload?.case === "publish_track").length, 1);
      await run(s.unpublish("input_audio"), { signal });
      equal(second.clones[0]?.readyState, "ended");
      equal(s.snapshot.claimedTracks, []);
      const notice = controlSent(p).find((m) => m.payload?.case === "unpublish_track");
      assert(notice !== undefined);
      equal(notice.kind, 3);
      equal(notice.request_id, "");
      await run(s.publish("input_audio", first), { signal });
      equal(controlSent(p).filter((m) => m.payload?.case === "publish_track").length, 2);
      equal(first.clones[1]?.readyState, "live");
    } finally {
      await run(s.close());
    }
    assert([...first.clones, ...second.clones].every((t) => t.readyState === "ended"));
    equal(first.readyState, "live");
    equal(second.readyState, "live");
  }));
test("publication session: rejected native replacement disposes candidate, preserves previous sender and confirmed remote claim", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f),
      first = new FakeTrack("audio"),
      next = new FakeTrack("audio");
    try {
      await run(s.start(), { signal });
      await run(s.publish("input_audio", first), { signal });
      const p = peerAt(peers);
      p.replaceHook = () =>
        Effect.fail(ReactorError.fromCode("InvalidState", "modeled native rejection"));
      equal(
        (await failure(s.publish("input_audio", next), { signal })).reason._tag,
        "InvalidState",
      );
      equal(next.clones[0]?.readyState, "ended");
      equal(first.clones[0]?.readyState, "live");
      equal(s.snapshot.status, "ready");
      equal(s.snapshot.claimedTracks, ["input_audio"]);
      equal(next.readyState, "live");
      p.replaceHook = undefined;
      await run(s.unpublish("input_audio"), { signal });
    } finally {
      await run(s.close());
    }
  }));
test("publication session: rejected unpublish detach retains sender and sends no false retirement notification", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f),
      source = new FakeTrack("audio");
    try {
      await run(s.start(), { signal });
      await run(s.publish("input_audio", source), { signal });
      const p = peerAt(peers);
      p.replaceHook = () =>
        Effect.fail(ReactorError.fromCode("InvalidState", "modeled detach rejection"));
      equal((await failure(s.unpublish("input_audio"), { signal })).reason._tag, "InvalidState");
      equal(source.clones[0]?.readyState, "live");
      equal(s.snapshot.claimedTracks, ["input_audio"]);
      equal(controlSent(p).filter((m) => m.payload?.case === "unpublish_track").length, 0);
      p.replaceHook = undefined;
    } finally {
      await run(s.close());
    }
  }));
test("publication session: notification failure follows local sender retirement but does not confirm remote release", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f),
      source = new FakeTrack("audio");
    try {
      await run(s.start(), { signal });
      await run(s.publish("input_audio", source), { signal });
      const p = peerAt(peers);
      p.sendHook = (channel, bytes) => {
        if (
          channel === "control" &&
          W.ControlClientMessage.decode(bytes).payload?.case === "unpublish_track"
        )
          throw ReactorError.fromCode("Disconnected", "modeled submission failure", {
            outcome: "unknown",
          });
      };
      const error = await failure(s.unpublish("input_audio"), { signal });
      equal(error.context.outcome, "unknown");
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
      equal(s.snapshot.claimedTracks, ["input_audio"]);
      p.sendHook = undefined;
    } finally {
      await run(s.close());
    }
  }));
test("publication session: interrupted in-flight native replacement retires the generation and stops both owned clones", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 500 }),
      first = new FakeTrack("audio"),
      next = new FakeTrack("audio");
    let finish: (() => void) | undefined;
    try {
      await run(s.start(), { signal });
      await run(s.publish("input_audio", first), { signal });
      const p = peerAt(peers);
      p.replaceHook = () =>
        Effect.tryPromise({
          try: () =>
            new Promise<void>((resolve) => {
              finish = resolve;
            }),
          catch: () => ReactorError.fromCode("InvalidState", "modeled replace"),
        });
      const task = Effect.runFork(s.publish("input_audio", next));
      await eventually(() => finish !== undefined);
      await Effect.runPromise(Fiber.interrupt(task));
      equal(s.snapshot.status, "disconnected");
      equal(s.snapshot.lastError?.context.outcome, "unknown");
      equal(first.clones[0]?.readyState, "ended");
      equal(next.clones[0]?.readyState, "ended");
      assert(p.closes > 0);
      await run(s.reconnect(), { signal });
      finish?.();
      await new Promise((resolve) => setTimeout(resolve, 10));
      equal(s.snapshot.status, "ready");
      equal(s.snapshot.claimedTracks, []);
      equal(peers[1]?.replacements.length, 0);
      equal(first.readyState, "live");
      equal(next.readyState, "live");
    } finally {
      finish?.();
      await run(s.close());
    }
  }));
test("publication session: stalled native replacement has a deadline and cannot leave a live candidate outside ownership", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 35 }),
      source = new FakeTrack("audio");
    try {
      await run(s.start(), { signal });
      peerAt(peers).replaceHook = () => Effect.never;
      const error = await failure(
        s.publish("input_audio", source).pipe(
          Effect.timeoutOrElse({
            duration: 200,
            orElse: () => Effect.fail(ReactorError.fromCode("Protocol", "missing sender deadline")),
          }),
        ),
        { signal },
      );
      equal(error.reason._tag, "Timeout");
      equal(s.snapshot.status, "disconnected");
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
    } finally {
      await run(s.close());
    }
  }));
test("publication session: interruption while claiming sends no media and cannot replay a late accepted claim", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 500 }),
      source = new FakeTrack("audio");
    try {
      await run(s.start(), { signal });
      const p = peerAt(peers);
      p.autoReply = false;
      const task = Effect.runFork(s.publish("input_audio", source));
      await eventually(() => s.snapshot.pending.control === 1);
      await Effect.runPromise(Fiber.interrupt(task));
      equal(source.clones.length, 0);
      equal(p.replacements.length, 0);
      equal(s.snapshot.pending.control, 1);
      const claim = controlSent(p).find((m) => m.payload?.case === "publish_track");
      assert(claim !== undefined);
      p.replyControl({
        request_id: claim.request_id,
        kind: 2,
        payload: { case: "publish_track", value: { name: "input_audio" } },
      });
      equal(s.snapshot.claimedTracks, ["input_audio"]);
      equal(s.snapshot.unresolvedPublications, []);
      equal(s.snapshot.pending.control, 0);
      equal(p.replacements.length, 0);
      equal(s.snapshot.status, "ready");
    } finally {
      await run(s.close());
    }
  }));
test("publication session: concurrent sender mutations are excluded; close releases an in-flight candidate", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 500 }),
      source = new FakeTrack("audio");
    await run(s.start(), { signal });
    const p = peerAt(peers);
    p.replaceHook = () => Effect.never;
    const task = Effect.runFork(Effect.result(s.publish("input_audio", source)));
    try {
      await eventually(() => p.replacements.length === 1);
      equal((await failure(s.unpublish("input_audio"), { signal })).reason._tag, "InvalidState");
      await run(s.close(), { signal });
      await Effect.runPromise(Fiber.join(task));
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
      equal(s.snapshot.status, "closed");
    } finally {
      await Effect.runPromise(Fiber.interrupt(task));
      await run(s.close());
    }
  }));

test("publication session: interrupted native unpublish retires local generation without reporting remote release", ({
  signal,
}) =>
  withFixture(async (f) => {
    const { session: s, peers } = makeSession(f, { commandTimeoutMs: 500 }),
      source = new FakeTrack("audio");
    let finish: (() => void) | undefined;
    try {
      await run(s.start(), { signal });
      await run(s.publish("input_audio", source), { signal });
      const p = peerAt(peers);
      p.replaceHook = () =>
        Effect.tryPromise({
          try: () =>
            new Promise<void>((resolve) => {
              finish = resolve;
            }),
          catch: () => ReactorError.fromCode("InvalidState", "modeled detach"),
        });
      const task = Effect.runFork(s.unpublish("input_audio"));
      await eventually(() => finish !== undefined);
      await Effect.runPromise(Fiber.interrupt(task));
      equal(s.snapshot.status, "disconnected");
      equal(s.snapshot.lastError?.context.outcome, "unknown");
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
      equal(controlSent(p).filter((m) => m.payload?.case === "unpublish_track").length, 0);
      await run(s.reconnect(), { signal });
      finish?.();
      await new Promise((resolve) => setTimeout(resolve, 10));
      equal(s.snapshot.status, "ready");
      equal(s.snapshot.claimedTracks, []);
      equal(
        controlSent(peerAt(peers, 1)).filter((m) => m.payload?.case === "unpublish_track").length,
        0,
      );
    } finally {
      finish?.();
      await run(s.close());
    }
  }));
