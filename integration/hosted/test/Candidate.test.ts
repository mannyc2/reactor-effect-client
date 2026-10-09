/** The requested candidate check uses the paid runner and real ReactorTest boundary. */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Effect, FileSystem, Layer, Ref, Schema } from "effect";
import * as HttpClient from "effect/http/HttpClient";
import { ReactorTest } from "reactor-effect-client";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Reactor from "reactor-effect-client/Reactor";
import { execute } from "../Qualify.js";
import { Check } from "../Spend.js";
import * as Target from "../Target.js";

const candidate = (faults: ReadonlyArray<ReactorTest.Fault> = [], timing?: ReactorTest.Timing) =>
  Target.rehearsal({ faults, candidate: "host", timing }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );

const run = Effect.gen(function* () {
  const check = yield* Schema.decodeEffect(Check)("candidate");
  yield* ReactorTest.flow().pipe(Effect.forkScoped);
  const fs = yield* FileSystem.FileSystem;
  const ledger = yield* fs.makeTempDirectoryScoped({ prefix: "candidate-test-" });
  return yield* execute({
    authorization: { check, budgetUsd: 8.4, totalUsd: 8.4 },
    ledger,
  });
});

/** The run, with every coordinator request going through `client`. */
const runThrough = Effect.fnUntraced(function* (client: HttpClient.HttpClient) {
  const coordinator = yield* CoordinatorClient.make().pipe(
    Effect.provideService(HttpClient.HttpClient, client),
  );
  const reactor = yield* Reactor.make().pipe(
    Effect.provideService(CoordinatorClient.CoordinatorClient, coordinator),
  );
  return yield* run.pipe(
    Effect.provideService(HttpClient.HttpClient, client),
    Effect.provideService(CoordinatorClient.CoordinatorClient, coordinator),
    Effect.provideService(Reactor.Reactor, reactor),
  );
});

layer(candidate())("candidate", (it) => {
  it.effect(
    "qualifies both models in one capped run",
    () =>
      Effect.gen(function* () {
        const evidence = yield* run;
        assert.strictEqual(evidence.verdict, "pass", evidence.reasons.join("; "));
        assert.lengthOf(evidence.sessions, 2);
        assert.isTrue(evidence.sessions.every((session) => session.close?.confirmed === true));
      }),
    // CI's two-session rehearsal exceeded five seconds of wall time while driving simulated time.
    { timeout: 20_000 },
  );
});

layer(candidate([{ _tag: "Video", video: "black" }]))("candidate media failure", (it) => {
  it.effect("ends its first session before refusing to allocate the second", () =>
    Effect.gen(function* () {
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "fail");
      assert.lengthOf(evidence.sessions, 1);
      assert.isTrue(evidence.sessions[0]?.close?.confirmed);
    }),
  );
});

layer(candidate([{ _tag: "IgnoreDelete" }]))("candidate unconfirmed end", (it) => {
  it.effect(
    "keeps an unconfirmed first end and allocates no second session",
    () =>
      Effect.gen(function* () {
        const evidence = yield* run;
        assert.strictEqual(evidence.verdict, "fail");
        assert.lengthOf(evidence.sessions, 1);
        assert.isFalse(evidence.sessions[0]?.close?.confirmed);
      }),
    // macOS CI's Node run took 4.95 s of the default five.
    { timeout: 20_000 },
  );
});

layer(candidate([{ _tag: "RefuseConnect" }]))("candidate partial acquisition", (it) => {
  it.effect("records the cleanup report of a session that never connected", () =>
    Effect.gen(function* () {
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "fail");
      assert.lengthOf(evidence.sessions, 1);
      assert.isTrue(evidence.sessions[0]?.close?.confirmed);
    }),
  );
});

layer(candidate())("candidate absent end", (it) => {
  it.effect(
    "independently confirms a gone session only after two consecutive reads",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        const http = yield* HttpClient.HttpClient;
        const armed = yield* Ref.make(false);
        const client = HttpClient.transform(http, (response, request) => {
          if (request.method !== "DELETE") return response;
          return response.pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                if (yield* Ref.getAndSet(armed, true)) return;
                // The source reads first after DELETE; its independent reads follow.
                yield* test.inject({ _tag: "MissingSession", nth: 2 });
                yield* test.inject({ _tag: "MissingSession", nth: 3 });
              }),
            ),
          );
        });
        const evidence = yield* runThrough(client);
        assert.strictEqual(evidence.verdict, "pass", evidence.reasons.join("; "));
        assert.lengthOf(evidence.sessions, 2);
        assert.deepStrictEqual(
          evidence.candidate?.phases[0]?.endReads.map((read) => read.state),
          ["absent", "absent"],
        );
      }),
    { timeout: 20_000 },
  );
});

// Fitted to the paid FastH3 phase of 2026-10-09: ready 2.32 s after allocation, short clips built
// in 3.8 to 4.6 s, and five starts 0.46 s late on average from the firm clip's Ready to p2's end.
// That firm clip was Ready 14.87 s after its submission, 3 s later than this speed builds it, so
// its frame's upload takes those 3 s. The H3 phase builds at FastH3's speed too.
const fastH3 = ReactorTest.Timing.fixed({
  buildSpeed: 1.25,
  http: "120 millis",
  channel: "20 millis",
  allocation: "200 millis",
  negotiation: "400 millis",
  connect: "950 millis",
  seam: "456 millis",
});

layer(candidate([], fastH3))("candidate at hosted FastH3 timing", (it) => {
  it.effect(
    "finishes each phase within its deadline",
    () =>
      Effect.gen(function* () {
        const test = yield* ReactorTest.ReactorTest;
        const http = yield* HttpClient.HttpClient;
        const client = HttpClient.transform(http, (response, request) =>
          request.method === "POST" && request.url.endsWith("/uploads")
            ? Effect.gen(function* () {
                // FastH3, the second session, uploads only its firm clip's frame.
                if ((yield* test.sessions).length === 2) yield* Effect.sleep("3 seconds");
                return yield* response;
              })
            : response,
        );
        const evidence = yield* runThrough(client);
        assert.strictEqual(evidence.verdict, "pass", evidence.reasons.join("; "));
        const allocated = evidence.milestones.filter((step) => step.step === "allocated")[1]?.atMs;
        const ready = evidence.sessionEvents?.find(
          (event) =>
            event.atMs >= (allocated ?? Infinity) &&
            event.tag === "Status" &&
            event.detail === "ready",
        );
        assert.closeTo((ready?.atMs ?? Infinity) - (allocated ?? 0), 2_320, 250);
        const built = (name: string) => {
          const item = evidence.candidate?.phases[1]?.items.find(
            (item) => item.key === `fast-h3:${name}`,
          );
          return (item?.readyMs ?? Infinity) - (item?.submittedMs ?? 0);
        };
        assert.closeTo(built("firm"), 14_870, 250);
        assert.closeTo(built("ready"), 5_830, 250);
      }),
    // CI's two-session rehearsal exceeded five seconds of wall time while driving simulated time.
    { timeout: 20_000 },
  );
});
