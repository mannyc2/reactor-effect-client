/**
 * The span set: a caller-boundary span for each operation a caller can cancel,
 * a span on each owned execution, and none on ticks, frames or heartbeats.
 */
import { expect, test } from "vitest";
import { Effect, Exit, Layer, Redacted, Scope } from "effect";
import { TestClock } from "effect/testing";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Http from "effect/unstable/http/HttpClient";
import * as Response from "effect/unstable/http/HttpClientResponse";
import { ReactorError } from "../src/errors.js";
import * as H3 from "../src/h3/index.js";
import { PeerFactory } from "../src/PeerFactory.js";
import * as Client from "../src/session/index.js";
import * as W from "../src/wire.generated.js";
import { makeSession, withFixture } from "./fixtures.js";
import { fixture as h3Fixture } from "./h3/ProviderSession.js";
import { eventually, run } from "./harness.js";
import { renewalFixture } from "./orchestration/RenewalFixture.js";
import { runClock } from "./orchestration/SourceFixture.js";
import { CLIENT, attributes, only, recording, traced } from "./otlp.js";

/** A coordinator that allocates, reads and terminates sessions, with no peer. */
const allocationOnly = () => {
  const open = new Set<string>();
  const platform = Http.make((request, url) =>
    Effect.sync(() => {
      if (url.pathname === "/sessions" && request.method === "POST") {
        open.add("session-1");
        return Response.fromWeb(
          request,
          globalThis.Response.json({ session_id: "session-1", state: "WAITING" }),
        );
      }
      if (request.method === "DELETE") {
        open.delete("session-1");
        return Response.fromWeb(request, new globalThis.Response(null, { status: 204 }));
      }
      return Response.fromWeb(request, new globalThis.Response(null, { status: 404 }));
    }),
  );
  return Layer.mergeAll(
    Layer.succeed(Http.HttpClient, platform),
    NodeCrypto.layer,
    Layer.succeed(PeerFactory, {
      make: () => {
        throw ReactorError.fromCode("InvalidState", "no connection in this test");
      },
    }),
  );
};

test("create names the model and its session; close carries the termination verdict", async () => {
  const { exit, spans, exported } = await traced(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const scope = yield* Scope.make();
        yield* client
          .create({ model: "selected/model", jwt: Redacted.make("SECRET-JWT-5e1f") })
          .pipe(Scope.provide(scope));
        yield* Scope.close(scope, Exit.void);
      }),
    ).pipe(Effect.provide(allocationOnly())),
    (spans) => spans.some((span) => span.name === "reactor.session.close"),
  );
  expect(Exit.isSuccess(exit)).toBe(true);
  const create = only(spans, "reactor.session.create");
  expect(create.kind).toBe(CLIENT);
  expect(attributes(create)).toMatchObject({
    "reactor.model.name": "selected/model",
    "reactor.session.id": "session-1",
    "reactor.connect": false,
  });
  const close = only(spans, "reactor.session.close");
  expect(attributes(close)).toMatchObject({
    "reactor.session.id": "session-1",
    "reactor.close.local_closed": true,
    "reactor.close.allocation": "known",
    "reactor.termination.attempted": true,
    "reactor.termination.confirmed": true,
    "reactor.termination.evidence": "absent",
  });
  const terminate = only(spans, "reactor.coordinator.terminate");
  expect(terminate.parentSpanId).toBe(close.spanId);
  expect(attributes(terminate)).toMatchObject({
    "reactor.session.id": "session-1",
    "reactor.termination.confirmed": true,
  });
  expect(exported).not.toContain("SECRET-JWT-5e1f");
});

test("connect marks each phase in order and names its generation", ({ signal }) =>
  withFixture(async (fixture) => {
    const { session } = makeSession(fixture);
    try {
      const { exit, spans } = await traced(session.start());
      expect(Exit.isSuccess(exit)).toBe(true);
      const connect = only(spans, "reactor.session.connect");
      expect(connect.kind).toBe(CLIENT);
      expect(attributes(connect)).toMatchObject({
        "reactor.session.id": fixture.sessionId,
        "reactor.connection.generation": "1",
      });
      expect(connect.events.map((event) => event.name)).toEqual([
        "reactor.connect.described",
        "reactor.connect.prepared",
        "reactor.connect.registered",
        "reactor.connect.offered",
        "reactor.connect.answered",
        "reactor.connect.ready",
      ]);
    } finally {
      await run(session.close(), { signal });
    }
  }));

test("an upload names its MIME type and size, never its name or bytes", ({ signal }) =>
  withFixture(async (fixture) => {
    const { session } = makeSession(fixture);
    try {
      await run(session.start(), { signal });
      const { exit, spans, exported } = await traced(
        session.upload("SECRET-NAME-77aa.bin", "application/octet-stream", new Uint8Array(3)),
      );
      expect(Exit.isSuccess(exit)).toBe(true);
      expect(attributes(only(spans, "reactor.session.upload"))).toEqual({
        "reactor.upload.mime_type": "application/octet-stream",
        "reactor.upload.size": 3,
      });
      expect(exported).not.toContain("SECRET-NAME-77aa");
    } finally {
      await run(session.close(), { signal });
    }
  }));

test("heartbeats are not traced", ({ signal }) =>
  withFixture(async (fixture) => {
    const { session, peers } = makeSession(fixture, { heartbeatInterval: 5 });
    const pings = () =>
      (peers[0]?.sent ?? []).filter(
        ({ channel, bytes }) =>
          channel === "control" && W.ControlClientMessage.decode(bytes).payload?.case === "ping",
      ).length;
    try {
      const { spans } = await traced(
        session.start().pipe(Effect.andThen(Effect.promise(() => eventually(() => pings() >= 3)))),
      );
      expect(spans.map((span) => span.name).sort()).toEqual([
        "app.submit",
        "reactor.session.connect",
      ]);
    } finally {
      await run(session.close(), { signal });
    }
  }));

test("an H3 enqueue is traced on its own execution, with its acceptance wait", () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { tracer, spans } = recording();
        const fake = yield* h3Fixture();
        const provider = yield* H3.make(fake.session, { replyTimeout: 1000 });
        yield* provider
          .enqueue({ prompt: "A blue paper boat on clear water.", seconds: 7 })
          .pipe(Effect.withTracer(tracer));
        const enqueue = spans.find((span) => span.name === "reactor.h3.enqueue");
        const reconcile = spans.find((span) => span.name === "reactor.h3.reconcile");
        expect(enqueue?.attributes.get("reactor.command.outcome")).toBe("replied");
        expect(enqueue?.attributes.get("reactor.h3.submission.id")).toEqual(expect.any(String));
        expect(reconcile?.parent._tag === "Some" && reconcile.parent.value).toBe(enqueue);
      }),
    ).pipe(Effect.provide(NodeCrypto.layer)),
  ));

test("renewal traces each opened source, and its tick traces nothing", () =>
  runClock(
    Effect.gen(function* () {
      const { tracer, spans } = recording();
      const { warm } = yield* renewalFixture().pipe(Effect.withTracer(tracer));
      yield* warm.pipe(Effect.withTracer(tracer));
      const opened = spans.filter((span) => span.name === "reactor.orchestration.renewal.open");
      expect(opened.map((span) => span.attributes.get("reactor.session.id"))).toEqual([
        "source-1",
        "source-2",
      ]);
      const count = spans.length;
      yield* TestClock.adjust(200).pipe(Effect.withTracer(tracer));
      expect(spans.length).toBe(count);
    }),
  ));
