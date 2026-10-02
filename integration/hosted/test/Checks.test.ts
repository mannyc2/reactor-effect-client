import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Effect, Redacted, Ref } from "effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import { endHeld } from "../Checks.js";
import type { Evidence } from "../Evidence.js";
import { format } from "../Evidence.js";
import * as Run from "../Run.js";

/** A rehearsal holding two sessions whose ids hold `secret`, so no save of it succeeds. */
const holdingTwo = (secret: string): Evidence => ({
  format,
  runId: "0000abcd",
  check: "unconnected",
  mode: "rehearsal",
  startedAt: "2026-09-29T00:00:00.000Z",
  environment: {
    runtime: "bun 1.4.2",
    os: "linux x64",
    packages: {},
    network: "test",
    apiOrigin: "https://api.reactor.inc",
  },
  budget: { checkUsd: 3, totalUsd: 3, reservedBeforeUsd: 0, worstCaseUsd: 2.4375 },
  grants: [],
  sessions: [1, 2].map((n) => ({
    id: `sess_${secret}_${n}`,
    allocatedMs: n,
    capEndsAt: "2026-09-29T00:01:00.000Z",
    trail: [],
  })),
  milestones: [],
  outcomes: [],
  criteria: [],
  spans: [],
  reasons: [],
  missing: [],
});

/** A coordinator answering every DELETE 200 and every read with `state`, its requests counted. */
const answering = (state: string) =>
  HttpClient.make((request, url) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        request.method === "DELETE"
          ? new Response(null, { status: 200 })
          : Response.json({
              session_id: decodeURIComponent(url.pathname.split("/")[2] ?? ""),
              state,
            }),
      ),
    ),
  );

layer(NodeServices.layer)("the sessions a failed check left open", (it) => {
  // A state is provider text, so the close record keeps it only as a code.
  it.effect("are recorded with a state that is no code kept only by its length", () =>
    Effect.gen(function* () {
      const run = yield* Run.make(holdingTwo("held_record"), "never-written.json");
      yield* endHeld(
        CoordinatorClient.make({
          apiUrl: "https://api.reactor.test",
          apiKey: Redacted.make("reactor-test-key"),
        }).pipe(Effect.provideService(HttpClient.HttpClient, answering("10.0.0.7:8443"))),
      ).pipe(Effect.provideService(Run.Run, run));
      const evidence = yield* run.evidence;
      assert.deepStrictEqual(
        evidence.sessions.map((session) => session.close?.termination?.state),
        ["(text, 13 chars)", "(text, 13 chars)"],
      );
    }),
  );

  it.effect("are each sent their DELETE before any is recorded, so a failed save skips none", () =>
    Effect.gen(function* () {
      const deleted = yield* Ref.make<ReadonlyArray<string>>([]);
      // The coordinator answers every request 404, so each end reads as confirmed.
      const http = HttpClient.make((request, url) =>
        Effect.as(
          request.method === "DELETE"
            ? Ref.update(deleted, (paths) => [...paths, url.pathname])
            : Effect.void,
          HttpClientResponse.fromWeb(request, new Response(null, { status: 404 })),
        ),
      );
      const run = yield* Run.make(holdingTwo("held_secret"), "never-written.json");
      yield* run.secret(Redacted.make("held_secret"));
      yield* endHeld(
        CoordinatorClient.make({
          apiUrl: "https://api.reactor.test",
          apiKey: Redacted.make("reactor-test-key"),
        }).pipe(Effect.provideService(HttpClient.HttpClient, http)),
      ).pipe(Effect.provideService(Run.Run, run));
      assert.deepStrictEqual(yield* Ref.get(deleted), [
        "/sessions/sess_held_secret_1",
        "/sessions/sess_held_secret_2",
      ]);
      // A failed save loses no record: each end is in the evidence the next save writes.
      const evidence = yield* run.evidence;
      assert.deepStrictEqual(
        evidence.sessions.map((session) => session.close?.confirmed),
        [true, true],
      );
    }),
  );
});

// Timed by the real clock: the coordinator's answers take real milliseconds.
layer(NodeServices.layer, { excludeTestServices: true })(
  "the sessions a failed check left open, in real time",
  (it) => {
    // A session held for a second one's slow tries bills the while: every DELETE goes out at once.
    it.effect("are ended together, and each end is recorded when it was confirmed", () =>
      Effect.gen(function* () {
        let inFlight = 0;
        let most = 0;
        const http = HttpClient.make((request, url) =>
          Effect.gen(function* () {
            inFlight += 1;
            most = Math.max(most, inFlight);
            yield* Effect.sleep(url.pathname.endsWith("_1") ? "20 millis" : "200 millis");
            inFlight -= 1;
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }));
          }),
        );
        const run = yield* Run.make(holdingTwo("held_timing"), "never-written.json");
        yield* endHeld(
          CoordinatorClient.make({
            apiUrl: "https://api.reactor.test",
            apiKey: Redacted.make("reactor-test-key"),
          }).pipe(Effect.provideService(HttpClient.HttpClient, http)),
        ).pipe(Effect.provideService(Run.Run, run));
        const [first, second] = (yield* run.evidence).sessions;
        assert.strictEqual(most, 2);
        assert.isBelow(
          first?.close?.reportedMs ?? Infinity,
          (second?.close?.reportedMs ?? 0) - 100,
        );
      }),
    );
  },
);
