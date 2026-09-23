/**
 * Spans exported through Effect's own OtlpTracer to a captured HTTP exporter,
 * so a test asserts on exactly what a collector would receive.
 */
import { expect } from "vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Tracer from "effect/Tracer";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as OtlpExporter from "effect/unstable/observability/OtlpExporter";
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization";
import * as OtlpTracer from "effect/unstable/observability/OtlpTracer";
import { eventually } from "./harness.js";

export type Span = OtlpTracer.ScopeSpan["spans"][number];
export const CLIENT = 3;
export const OK = 1;
export const ERROR_STATUS = 2;

export const only = (spans: readonly Span[], name: string): Span => {
  const found = spans.filter((span) => span.name === name);
  expect(found).toHaveLength(1);
  return found[0]!;
};
export const attributes = (span: Span): Record<string, unknown> =>
  Object.fromEntries(span.attributes.map(({ key, value }) => [key, Object.values(value)[0]]));
export const failed = <A, E>(exit: Exit.Exit<A, E>): E => {
  const found = Exit.findError(exit);
  if (found._tag !== "Success") throw new Error("expected a typed failure");
  return found.success;
};

/** Runs `effect` inside an application span and returns everything the exporter sent. */
export const traced = async <A, E>(
  effect: Effect.Effect<A, E>,
  settled: (spans: readonly Span[]) => boolean = () => true,
): Promise<{
  readonly exit: Exit.Exit<A, E>;
  readonly spans: readonly Span[];
  readonly exported: string;
}> => {
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

/** An in-memory tracer, for effects whose clock is a TestClock. */
export const recording = (): {
  readonly tracer: Tracer.Tracer;
  readonly spans: Tracer.NativeSpan[];
} => {
  const spans: Tracer.NativeSpan[] = [];
  return {
    spans,
    tracer: Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    }),
  };
};
