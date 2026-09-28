import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it, layer } from "@effect/vitest";
import { Effect, Exit, FileSystem, Path, Redacted, Schema } from "effect";
import type { Evidence } from "../Evidence.js";
import { EvidenceJson, format, judged, missing } from "../Evidence.js";
import * as Ledger from "../Ledger.js";

/** A paid vertical run as it stands once admitted. */
const admitted: Evidence = {
  format,
  runId: "0000abcd",
  check: "vertical",
  mode: "paid",
  startedAt: "2026-09-28T00:00:00.000Z",
  environment: {
    runtime: "bun 1.4.2",
    os: "linux x64",
    packages: {},
    network: "test",
    apiOrigin: "https://api.reactor.inc",
  },
  budget: { checkUsd: 0.75, totalUsd: 1.5, reservedBeforeUsd: 0, worstCaseUsd: 0.75 },
  grants: [],
  sessions: [],
  milestones: [],
  outcomes: [],
  criteria: [],
  spans: [],
  reasons: [],
  missing: [],
};

layer(NodeServices.layer)("a ledger", (it) => {
  const directory = Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.makeTempDirectoryScoped({ prefix: "ledger-test-" });
  });

  it.effect("one run at a time holds its lock, and releases it with its scope", () =>
    Effect.gen(function* () {
      const dir = yield* directory;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Ledger.lock(dir);
          const second = yield* Effect.exit(Effect.scoped(Ledger.lock(dir)));
          assert.isTrue(Exit.isFailure(second));
        }),
      );
      yield* Effect.scoped(Ledger.lock(dir));
    }),
  );

  it.effect("each run claims a new file; a save that would hold a secret writes nothing", () =>
    Effect.gen(function* () {
      const dir = yield* directory;
      const fs = yield* FileSystem.FileSystem;
      const file = (yield* Path.Path).join(dir, "run.json");
      const save = yield* Ledger.writer(file);
      yield* save(admitted, []);
      yield* save({ ...admitted, milestones: [{ atMs: 1, step: "minted" }] }, []);
      const leaked = yield* Effect.exit(
        save({ ...admitted, environment: { ...admitted.environment, network: "key-0123456789" } }, [
          Redacted.make("key-0123456789"),
        ]),
      );
      assert.isTrue(Exit.isFailure(leaked));
      const kept = yield* Schema.decodeEffect(EvidenceJson)(yield* fs.readFileString(file));
      assert.deepStrictEqual(kept.milestones, [{ atMs: 1, step: "minted" }]);
      // Another run cannot claim the same file.
      const other = yield* Ledger.writer(file);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(other(admitted, []))));
    }),
  );

  it.effect("earlier paid runs reserve their worst case; an unreadable file refuses", () =>
    Effect.gen(function* () {
      const dir = yield* directory;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const save = yield* Ledger.writer(path.join(dir, "a.json"));
      yield* save(admitted, []);
      const rehearsal = yield* Ledger.writer(path.join(dir, "b.json"));
      yield* rehearsal({ ...admitted, mode: "rehearsal" }, []);
      const runs = yield* Ledger.entries(dir);
      assert.strictEqual(
        runs.reduce((total, run) => total + Ledger.reserved(run), 0),
        0.75,
      );
      yield* fs.writeFileString(path.join(dir, "c.json"), "{}");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(Ledger.entries(dir))));
    }),
  );
});

describe("a verdict", () => {
  const passing: Evidence = {
    ...admitted,
    sessions: [
      {
        id: "s",
        allocatedMs: 0,
        capEndsAt: "2026-09-28T00:00:50.000Z",
        close: { requestedMs: 1, reportedMs: 2, confirmed: true },
        trail: [],
      },
    ],
    contract: {
      deploymentTitle: null,
      deploymentVersion: null,
      documentedVersion: "0.5.5",
      referenceAudio: true,
      messages: {},
      unknown: {},
      diagnostics: {},
      duplicates: 0,
      stale: 0,
    },
    server: { cluster: null, zone: null, serverVersion: null, transport: null },
    clip: { clipId: "c", acceptance: "correlated", submitMs: 0, acceptedMs: 1, echoes: {} },
    media: {
      audioOffered: false,
      video: { frames: 0, formats: [], sizes: [], lit: 0, distinct: 0, lost: 0, gaps: 0 },
    },
    network: { samples: [] },
    criteria: [{ name: "live video", passed: true }],
  };

  it("passes only with every criterion passed, nothing missing and every end confirmed", () => {
    const { media: _media, ...withoutMedia } = passing;
    const { worstCaseUsd: _worst, ...unreserved } = passing.budget;
    assert.strictEqual(judged({ evidence: passing, failure: undefined }).verdict, "pass");
    assert.deepStrictEqual(missing(passing), []);
    const cases: ReadonlyArray<Evidence> = [
      { ...passing, criteria: [] },
      { ...passing, criteria: [{ name: "live video", passed: false }] },
      { ...passing, outcomes: ["unknown"] },
      withoutMedia,
      {
        ...passing,
        sessions: passing.sessions.map((session) => ({
          ...session,
          close: { requestedMs: 1, reportedMs: 2, confirmed: false },
        })),
      },
      { ...passing, budget: unreserved },
    ];
    for (const evidence of cases)
      assert.strictEqual(judged({ evidence, failure: undefined }).verdict, "fail");
    assert.strictEqual(
      judged({ evidence: passing, failure: "a step ran past its deadline" }).verdict,
      "fail",
    );
  });
});
