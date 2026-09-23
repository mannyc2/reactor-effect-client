/**
 * Each failure hoists its reason as the native cause, and exporters render that
 * chain: OtlpTracer exports every error's message and its stack with the causes
 * appended. Provider and backend text therefore lives only where an exporter
 * never looks: a Redacted `backendMessage`, a reason's `body`, or
 * `context.detail`. A caller still reads it deliberately.
 */
import { expect, test } from "vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as OtlpExporter from "effect/unstable/observability/OtlpExporter";
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization";
import * as OtlpTracer from "effect/unstable/observability/OtlpTracer";
import * as Coordinator from "../src/coordinator/index.js";
import { CommandFailure, Native, ReactorError } from "../src/index.js";
import type { ReactorFailure } from "../src/index.js";

const BACKEND = "a=ice-pwd:S3CR3TPWD";
const BODY = "S3CR3T-BODY-5a1d";

/** Runs `effect` in a span under Effect's OtlpTracer and returns the exit and the whole export. */
const exported = async <A>(effect: Effect.Effect<A, ReactorFailure, HttpClient.HttpClient>) => {
  const bodies: string[] = [];
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
        return yield* Effect.exit(effect.pipe(Effect.withSpan("app.failure"))).pipe(
          Effect.withTracer(tracer),
        );
      }),
    ).pipe(
      Effect.provide(Layer.merge(OtlpSerialization.layerJson, OtlpExporter.layerFlusher)),
      Effect.provideService(HttpClient.HttpClient, exporter),
    ),
  );
  return { exit, export: bodies.join("\n") };
};
const failureOf = <A, E>(exit: Exit.Exit<A, E>): E => {
  const found = Exit.findError(exit);
  if (found._tag !== "Success") throw new Error("expected a typed failure");
  return found.success;
};

test("native backend text stays Redacted: absent from Cause.pretty and the OTLP export", async () => {
  // The shape reactor-effect-native builds for an unclassified libwebrtc failure.
  const native = CommandFailure.from(
    new ReactorError({
      reason: new Native({
        message: "native call:answer failed (Native)",
        status: -2,
        backendMessage: Redacted.make(BACKEND),
      }),
    }),
    { operation: "answer", outcome: "unknown", requestId: "request-1", generation: 1n },
  );
  const { exit, export: otlp } = await exported(Effect.fail(native));
  expect(otlp).toContain("native call:answer failed (Native)");
  expect(otlp).not.toContain("S3CR3TPWD");
  expect(Cause.pretty(Cause.fail(native))).not.toContain("S3CR3TPWD");
  expect(JSON.stringify(native)).not.toContain("S3CR3TPWD");
  // A caller that asks for it still reads it.
  const reason = failureOf(exit).reason;
  expect(
    reason._tag === "Native" && reason.backendMessage && Redacted.value(reason.backendMessage),
  ).toBe(BACKEND);
});

test("a JSON SyntaxError quoting the body stays in detail: absent from Cause.pretty and OTLP", async () => {
  const inspect = Coordinator.make({
    apiUrl: "https://configured.fixture/api",
    credential: Effect.succeed(Redacted.make("inspection-token")),
  }).pipe(
    Effect.flatMap((coordinator) => coordinator.inspect("session")),
    Effect.provide(
      Layer.fresh(FetchHttpClient.layer).pipe(
        Layer.provide(
          Layer.succeed(
            FetchHttpClient.Fetch,
            Object.assign(async () => new Response(`{"state": ${BODY}`, { status: 200 }), {
              preconnect: fetch.preconnect,
            }),
          ),
        ),
      ),
    ),
  );
  const { exit, export: otlp } = await exported(inspect);
  const failure = failureOf(exit);
  expect(failure.reason._tag).toBe("Protocol");
  // The body fragment is only in the SyntaxError, which is inspection-only detail.
  const detail = failure.context.detail;
  expect(detail instanceof SyntaxError && detail.message).toContain("S3CR3T");
  expect(otlp).toContain(failure.message);
  expect(otlp).not.toContain("S3CR3T");
  expect(Cause.pretty(Cause.fail(failure))).not.toContain("S3CR3T");
  expect(JSON.stringify(failure)).not.toContain("S3CR3T");
});
