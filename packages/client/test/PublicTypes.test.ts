/**
 * Every type a public signature names is exported from the entry point that
 * exposes the signature, so an application can annotate what it holds. These
 * assertions are checked by the typecheck; at runtime they only import.
 */
import { describe, expectTypeOf, it } from "vitest";
import type * as Effect from "effect/Effect";
import type * as Duration from "effect/Duration";
import type * as Reactor from "../src/index.js";
import type * as H3 from "../src/h3/index.js";
import type * as Orchestration from "../src/orchestration/index.js";

describe("public types", () => {
  it("accepts an optional duration for the scheduler's unknown recovery deadline", () => {
    expectTypeOf<
      Parameters<typeof Orchestration.makeScheduler>[0]["unknownRecoveryTimeout"]
    >().toEqualTypeOf<Duration.Input | undefined>();
  });
  it("names the session's observation, readiness and attribution", () => {
    expectTypeOf<Reactor.Session["observe"]>().toEqualTypeOf<Reactor.Observe>();
    expectTypeOf<
      Effect.Success<ReturnType<Reactor.Observe>>
    >().toEqualTypeOf<Reactor.Observation>();
    expectTypeOf<
      Reactor.ReadyState["remote"]["descriptor"]
    >().toEqualTypeOf<Reactor.ReadyDescriptor>();
    expectTypeOf<Reactor.CommandReply["correlation"]>().toEqualTypeOf<Reactor.Correlation>();
    expectTypeOf<Reactor.CommandReply>().toExtend<Reactor.Attribution>();
    expectTypeOf<Reactor.EventPayload>().toExtend<{ readonly _tag: string }>();
    expectTypeOf<"owned">().toExtend<Reactor.Ownership>();
  });

  it("names an H3 clip operation and its facts", () => {
    expectTypeOf<
      Effect.Success<ReturnType<H3.Provider["operation"]>>
    >().toEqualTypeOf<H3.ClipOperation>();
    expectTypeOf<Parameters<H3.ClipOperation["reached"]>[0]>().toEqualTypeOf<H3.ClipPhase>();
    expectTypeOf<Effect.Success<H3.ClipOperation["ended"]>>().toEqualTypeOf<H3.ClipFact>();
    expectTypeOf<Effect.Success<H3.ClipOperation["facts"]>>().toEqualTypeOf<H3.OperationFacts>();
  });

  it("names an engine's observation and a source's routed request", () => {
    expectTypeOf<
      Orchestration.EngineShape["observe"]
    >().toEqualTypeOf<Orchestration.ObserveEngine>();
    expectTypeOf<
      Effect.Success<ReturnType<Orchestration.ObserveEngine>>
    >().toEqualTypeOf<Orchestration.EngineObservation>();
    expectTypeOf<
      Parameters<Orchestration.Source["prepareRouted"]>[0]
    >().toEqualTypeOf<Orchestration.RoutedRequest>();
    expectTypeOf<Parameters<Orchestration.Source["prepareRouted"]>[1]>().toEqualTypeOf<
      Orchestration.EnqueueHooks | undefined
    >();
  });

  it("keeps historical switch events assignable and exposes handoff evidence through Renewal", () => {
    type Switched = Extract<Orchestration.Renewal, { readonly _tag: "Switched" }>;
    type Handoff = NonNullable<Switched["handoff"]>;
    expectTypeOf<Omit<Switched, "handoff">>().toExtend<Switched>();
    expectTypeOf<undefined>().toExtend<Switched["handoff"]>();
    expectTypeOf<Handoff["replacementSessionId"]>().toEqualTypeOf<string>();
    expectTypeOf<Handoff["decision"]>().toEqualTypeOf<
      "no-observed-start" | "count-complete" | "grace-elapsed"
    >();
    expectTypeOf<
      Extract<Handoff["finalClip"], { readonly _tag: "Observed" }>["clipId"]
    >().toEqualTypeOf<Orchestration.ClipId>();
    expectTypeOf<Handoff["grace"]["_tag"]>().toEqualTypeOf<"NotObserved" | "Observed">();
    expectTypeOf<
      Extract<Handoff["grace"], { readonly _tag: "Observed" }>["origin"]
    >().toEqualTypeOf<"Ended" | "Idle">();
  });
});
