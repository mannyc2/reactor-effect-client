/**
 * The trace identity of work that outlives the fiber that set it going: a queued command or clip,
 * a session's open, reconnect or recovery. Such work keeps only a span's ids and its sampling
 * decision, never the caller's span or services, and its own span is either a child of the
 * caller that caused it or a root of its own, linked to the acquisition that set it going.
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";

/**
 * The current span's identity, or undefined outside any span. A span whose propagation is
 * disabled is passed over for its parent, as Effect passes it over when it picks a parent, so no
 * placeholder ids are kept.
 */
export const currentParent: Effect.Effect<Tracer.ExternalSpan | undefined> = Effect.map(
  Effect.serviceOption(Tracer.ParentSpan),
  (parent) => {
    let span = Option.getOrUndefined(parent);
    while (span !== undefined && Context.get(span.annotations, Tracer.DisablePropagation))
      span = span._tag === "Span" ? Option.getOrUndefined(span.parent) : undefined;
    return span === undefined
      ? undefined
      : Tracer.externalSpan({ traceId: span.traceId, spanId: span.spanId, sampled: span.sampled });
  },
);

/**
 * Options for the span of work that `acquisition` set going and `parent`, if anything, caused: a
 * child of `parent`, which takes its sampling decision, or else a root. Either is linked to
 * `acquisition` unless that is its parent. A root keeps an acquisition's decision not to sample,
 * so tracing a playout or session unsampled exports none of its work; it never forces a sampled
 * one, which would bypass `MinimumTraceLevel`.
 */
export const spanOptions = ({
  acquisition,
  parent,
}: {
  readonly acquisition: Tracer.ExternalSpan | undefined;
  readonly parent?: Tracer.ExternalSpan | undefined;
}): Tracer.SpanOptionsNoTrace => ({
  parent,
  root: parent === undefined,
  links:
    acquisition === undefined ||
    (parent?.traceId === acquisition.traceId && parent.spanId === acquisition.spanId)
      ? []
      : [{ span: acquisition, attributes: {} }],
  ...(parent === undefined && acquisition?.sampled === false ? { sampled: false } : {}),
});
