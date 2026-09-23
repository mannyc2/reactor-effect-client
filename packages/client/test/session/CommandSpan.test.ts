/**
 * The span on a request's session-owned execution, exported through Effect's own
 * OtlpTracer with a captured HTTP exporter. The export carries identity, dispatch
 * evidence and the failure category, never the input, the reply or provider text,
 * while the caller still reads provider text from its reason's `body`.
 */
import { expect, test } from "vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as OtlpExporter from "effect/unstable/observability/OtlpExporter";
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization";
import * as OtlpTracer from "effect/unstable/observability/OtlpTracer";
import { structFromObject } from "../../src/json.js";
import type { Session, SessionOptions } from "../../src/session.js";
import * as W from "../../src/wire.generated.js";
import { makeSession, withFixture, type MockPeer } from "../fixtures.js";
import { eventually, run } from "../harness.js";

const INPUT = "SECRET-INPUT-7f3a";
const REPLY = "SECRET-REPLY-91bc";
const ERROR = "SECRET-ERROR-c0de";

type Span = OtlpTracer.ScopeSpan["spans"][number];
const CLIENT = 3;
const OK = 1;
const ERROR_STATUS = 2;

const only = (spans: readonly Span[], name: string): Span => {
  const found = spans.filter((span) => span.name === name);
  expect(found).toHaveLength(1);
  return found[0]!;
};
const attributes = (span: Span): Record<string, unknown> =>
  Object.fromEntries(span.attributes.map(({ key, value }) => [key, Object.values(value)[0]]));
const failed = <A, E>(exit: Exit.Exit<A, E>): E => {
  const found = Exit.findError(exit);
  if (found._tag !== "Success") throw new Error("expected a typed failure");
  return found.success;
};

/** Runs `effect` inside an application span and returns everything the exporter sent. */
const traced = async <A, E>(
  effect: Effect.Effect<A, E>,
  settled: (spans: readonly Span[]) => boolean = () => true,
) => {
  const bodies: string[] = [];
  const spans = () =>
    bodies.flatMap((body) =>
      (JSON.parse(body) as OtlpTracer.TraceData).resourceSpans.flatMap((resource) =>
        resource.scopeSpans.flatMap((scope) => scope.spans),
      ),
    );
  const exporter = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.body._tag === "Uint8Array")
        bodies.push(new TextDecoder().decode(request.body.body));
      return HttpClientResponse.fromWeb(request, new Response(null, { status: 200 }));
    }),
  );
  const exit = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const tracer = yield* OtlpTracer.make({
          url: "http://otlp.invalid/v1/traces",
          resource: { serviceName: "reactor-effect-client-test" },
          maxBatchSize: 1,
          exportInterval: "1 hour",
        });
        const exit = yield* Effect.exit(effect.pipe(Effect.withSpan("app.submit"))).pipe(
          Effect.withTracer(tracer),
        );
        // A request's span can end after its caller has stopped waiting.
        yield* Effect.promise(() => eventually(() => settled(spans()), "span export deadline"));
        return exit;
      }),
    ).pipe(
      Effect.provide(Layer.merge(OtlpSerialization.layerJson, OtlpExporter.layerFlusher)),
      Effect.provideService(HttpClient.HttpClient, exporter),
    ),
  );
  // Statuses, exception events, attributes and stacks: the whole export.
  return { exit, spans: spans(), exported: bodies.join("\n") };
};

const withSession = (
  signal: AbortSignal,
  body: (session: Session, peer: MockPeer) => Promise<void>,
  options: Partial<SessionOptions> = {},
) =>
  withFixture(async (fixture) => {
    const { session, peers } = makeSession(fixture, options);
    try {
      await run(session.start(), { signal });
      const peer = peers[0];
      if (peer === undefined) throw new Error("missing fixture peer");
      peer.autoReply = false;
      await body(session, peer);
    } finally {
      await run(session.close());
    }
  });
const replyData = (peer: MockPeer, payload: NonNullable<W.DataServerMessage["payload"]>) => {
  peer.sendHook = (channel, bytes) => {
    if (channel === "data")
      peer.replyData({
        request_id: W.DataClientMessage.decode(bytes).request_id,
        kind: 2,
        payload,
      });
  };
};
const replyControl = (peer: MockPeer, payload: NonNullable<W.ControlServerMessage["payload"]>) => {
  peer.sendHook = (channel, bytes) => {
    if (channel === "control")
      peer.replyControl({
        request_id: W.ControlClientMessage.decode(bytes).request_id,
        kind: 2,
        payload,
      });
  };
};

test("command span: a reply exports identity and outcome, never the input or the reply", ({
  signal,
}) =>
  withSession(signal, async (session, peer) => {
    replyData(peer, {
      case: "message",
      value: { type: "result", data: structFromObject({ text: REPLY }) },
    });
    const { exit, spans, exported } = await traced(
      session.command("generate_clip", { prompt: INPUT }),
    );
    if (!Exit.isSuccess(exit)) throw new Error("expected a reply");
    const command = only(spans, "reactor.session.command");
    expect(command.parentSpanId).toBe(only(spans, "app.submit").spanId);
    expect(command.kind).toBe(CLIENT);
    expect(command.status).toEqual({ code: OK });
    expect(attributes(command)).toEqual({
      "reactor.operation": "generate_clip",
      "reactor.request.id": exit.value.requestId,
      "reactor.connection.generation": String(exit.value.generation),
      "reactor.command.outcome": "replied",
    });
    expect(exported).not.toContain(INPUT);
    expect(exported).not.toContain(REPLY);
  }));

test("command span: a remote error exports its code and outcome; only the caller reads provider text", ({
  signal,
}) =>
  withSession(signal, async (session, peer) => {
    replyData(peer, {
      case: "error",
      value: { code: "MODEL_ERROR", message: `refused: ${ERROR}` },
    });
    const { exit, spans, exported } = await traced(
      session.command("generate_clip", { prompt: INPUT }),
    );
    const error = failed(exit);
    const command = only(spans, "reactor.session.command");
    expect(command.status).toEqual({
      code: ERROR_STATUS,
      message: "remote command error MODEL_ERROR",
    });
    expect(attributes(command)).toEqual({
      "reactor.operation": "generate_clip",
      "reactor.request.id": error.context.requestId,
      "reactor.connection.generation": String(error.context.generation),
      "reactor.command.outcome": "replied",
      "error.type": "Remote",
    });
    // Its exception event carries the message and stack. Neither the command span
    // nor the application's enclosing span exports provider text.
    expect(command.events.map((event) => event.name)).toEqual(["exception"]);
    expect(only(spans, "app.submit").status.message).toBe("remote command error MODEL_ERROR");
    expect(exported).not.toContain(ERROR);
    expect(exported).not.toContain(INPUT);
    expect(error.message).toBe("remote command error MODEL_ERROR");
    expect(error.reason).toMatchObject({
      _tag: "Remote",
      remoteCode: "MODEL_ERROR",
      body: `refused: ${ERROR}`,
    });
    expect(error.context.outcome).toBe("replied");
  }));

test("control span: a remote control error is exported by code; the caller reads its text", ({
  signal,
}) =>
  withSession(signal, async (session, peer) => {
    replyControl(peer, {
      case: "error",
      value: { code: "SCHEMA_UNAVAILABLE", message: `unavailable: ${ERROR}` },
    });
    const { exit, spans, exported } = await traced(session.schema());
    const error = failed(exit);
    const control = only(spans, "reactor.session.control");
    expect(control.kind).toBe(CLIENT);
    expect(control.status).toEqual({
      code: ERROR_STATUS,
      message: "remote command error SCHEMA_UNAVAILABLE",
    });
    expect(attributes(control)).toEqual({
      "reactor.operation": "request_schema",
      "reactor.request.id": error.context.requestId,
      "reactor.connection.generation": String(error.context.generation),
      "reactor.command.outcome": "replied",
      "error.type": "Remote",
    });
    expect(exported).not.toContain(ERROR);
    expect(error.reason).toMatchObject({
      _tag: "Remote",
      remoteCode: "SCHEMA_UNAVAILABLE",
      body: `unavailable: ${ERROR}`,
    });
  }));

test("control span: a failed clip is a replied request; its reason reaches only the caller", ({
  signal,
}) =>
  withSession(signal, async (session, peer) => {
    replyControl(peer, { case: "clip_failed", value: { reason: `recorder disabled: ${ERROR}` } });
    const { exit, spans, exported } = await traced(session.recording());
    const control = only(spans, "reactor.session.control");
    expect(control.status).toEqual({ code: OK });
    expect(attributes(control)).toMatchObject({
      "reactor.operation": "request_recording",
      "reactor.command.outcome": "replied",
    });
    expect(only(spans, "app.submit").status).toEqual({
      code: ERROR_STATUS,
      message: "clip failed",
    });
    expect(exported).not.toContain(ERROR);
    const error = failed(exit);
    expect([
      error.reason._tag,
      error.message,
      error.reason._tag === "RecorderDisabled" && error.reason.body,
    ]).toEqual(["RecorderDisabled", "clip failed", `recorder disabled: ${ERROR}`]);
  }));

test("command span: after the caller stops waiting, the child span ends Timeout/unknown at the command's own deadline", ({
  signal,
}) =>
  withSession(
    signal,
    async (session, peer) => {
      const { exit, spans, exported } = await traced(
        session.command("generate_clip", { prompt: INPUT }).pipe(Effect.timeout(5)),
        (spans) => spans.some((span) => span.name === "reactor.session.command"),
      );
      expect(Cause.isTimeoutError(failed(exit))).toBe(true);
      const app = only(spans, "app.submit");
      const command = only(spans, "reactor.session.command");
      expect(command.traceId).toBe(app.traceId);
      expect(command.parentSpanId).toBe(app.spanId);
      expect(BigInt(command.endTimeUnixNano)).toBeGreaterThan(BigInt(app.endTimeUnixNano));
      expect(command.status).toEqual({ code: ERROR_STATUS, message: "generate_clip: deadline" });
      const sent = peer.sent.find((message) => message.channel === "data");
      if (sent === undefined) throw new Error("command was not sent");
      expect(attributes(command)).toEqual({
        "reactor.operation": "generate_clip",
        "reactor.request.id": W.DataClientMessage.decode(sent.bytes).request_id,
        "reactor.connection.generation": String(session.snapshot.generation),
        "reactor.command.outcome": "unknown",
        "error.type": "Timeout",
      });
      // The submitted request keeps its slot until a late reply or retirement.
      expect(session.snapshot.pending.data).toBe(1);
      expect(exported).not.toContain(INPUT);
    },
    { commandTimeoutMs: 50 },
  ));

test("command span: pre-dispatch rejections and notifications open no span", ({ signal }) =>
  withSession(
    signal,
    async (session) => {
      const invalid = await traced(
        session.command("generate_clip", { prompt: INPUT }, undefined, 0),
      );
      expect(failed(invalid.exit)).toMatchObject({
        reason: { _tag: "InvalidInput" },
        context: { outcome: "not-submitted" },
      });
      expect(invalid.spans.map((span) => span.name)).toEqual(["app.submit"]);
      // Hold the only pending slot, outside the exporter, so the next command overflows.
      const held = run(Effect.result(session.command("hold", {})), { signal });
      await eventually(() => session.snapshot.pending.data === 1);
      const overflow = await traced(session.command("generate_clip", { prompt: INPUT }));
      expect(failed(overflow.exit)).toMatchObject({
        reason: { _tag: "Overflow" },
        context: { outcome: "not-submitted" },
      });
      expect(overflow.spans.map((span) => span.name)).toEqual(["app.submit"]);
      await held;
      const ping = await traced(session.ping());
      expect(Exit.isSuccess(ping.exit)).toBe(true);
      expect(ping.spans.map((span) => span.name)).toEqual(["app.submit"]);
    },
    { maxPending: 1 },
  ));
