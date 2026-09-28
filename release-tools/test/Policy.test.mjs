import assert from "node:assert/strict";
import { test } from "bun:test";
import { Effect, FileSystem, Path } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { makeInput } from "../input.mjs";
import { checkoutCommit, validateExecutionHost } from "../host.mjs";
import { confirmationFor, validateRun } from "../model.mjs";
import { visibility } from "../report.mjs";
import {
  applicationCommit,
  ciRunId,
  executionEnvironment,
  loadOffline,
  makeFixture,
  packageNames,
  preparationRun,
  prepared,
  runTest,
  sourceCommit,
  withEnvironment,
  workflowRun,
  writeJson,
} from "./Fixture.mjs";

test("only successful main CI and manual release run identities are admitted", () => {
  const ci = workflowRun();
  assert.equal(Effect.runSync(validateRun(ci, ciRunId, "ci")).head_sha, sourceCommit);
  assert.equal(
    Effect.runSync(validateRun({ ...ci, event: "workflow_dispatch" }, ciRunId, "ci")).id,
    7101,
  );
  const release = { ...ci, event: "workflow_dispatch", path: ".github/workflows/release.yml" };
  assert.equal(Effect.runSync(validateRun(release, ciRunId, "release")).head_sha, sourceCommit);
  assert.throws(() =>
    Effect.runSync(validateRun({ ...release, event: "push" }, ciRunId, "release")),
  );
  assert.throws(() => Effect.runSync(validateRun(ci, "7102", "ci")));
  assert.throws(() => Effect.runSync(validateRun(ci, "07101", "ci")));
  assert.throws(() => Effect.runSync(validateRun(ci, "7101\n", "ci")));
});

/** @type {Array<[string, Record<string, unknown>]>} */
const invalidRuns = [
  ["fork source", { head_repository: { full_name: "outsider/reactor-effect-client" } }],
  ["other repository", { repository: { full_name: "outsider/reactor-effect-client" } }],
  ["pull request", { event: "pull_request" }],
  ["pull request target", { event: "pull_request_target" }],
  ["non-main branch", { head_branch: "candidate" }],
  ["failed run", { conclusion: "failure" }],
  ["cancelled run", { conclusion: "cancelled" }],
  ["running job", { status: "in_progress" }],
  ["other workflow", { path: ".github/workflows/other.yml" }],
  ["wrong ID", { id: 7102 }],
  ["zero attempt", { run_attempt: 0 }],
  ["fractional attempt", { run_attempt: 1.5 }],
  ["abbreviated source", { head_sha: "1234567" }],
];
for (const [name, changes] of invalidRuns)
  test(`CI selection rejects ${name}`, () =>
    assert.throws(() =>
      Effect.runSync(validateRun({ ...workflowRun(), ...changes }, ciRunId, "ci")),
    ));

/** @param {string} version */
const confirming = (version) =>
  `publish ${packageNames.map((name) => `${name}@${version}`).join(" ")}`;
const confirmation = confirming("0.2.0");
const confirmationRequired = "Explicit package/version publication confirmation is required";

test("explicit confirmation of every package selects publication; observe cannot acquire publication authority", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const { identity } = yield* prepared(fixture);
      assert.equal(confirmationFor(identity.qualification), confirmation);
      const options = {
        ciRun: workflowRun(),
        candidateRun: preparationRun(),
        ciRunId,
        candidateRunId: "8101",
        executionHostCommit: applicationCommit,
        candidateDirectory: fixture.candidateDirectory,
        mode: "observe",
        confirmation: "",
      };
      assert.equal((yield* makeInput(identity, options)).authorize, false);
      assert.equal((yield* makeInput(identity, { ...options, confirmation })).authorize, false);
      const wrongConfirmations = [
        "",
        "publish",
        "publish reactor-effect-client@0.2.0",
        "publish reactor-effect-client@0.2.0 reactor-effect-browser@0.2.0",
        "publish reactor-effect-browser@0.2.0 reactor-effect-native@0.2.0",
        "publish reactor-effect-browser@0.2.0 reactor-effect-client@0.2.0 reactor-effect-native@0.2.0",
        "publish reactor-effect-client@0.2.0 reactor-effect-native@0.2.0 reactor-effect-browser@0.2.0",
        "publish reactor-effect-client@0.2.0 reactor-effect-browser@0.2.0 reactor-effect-native@0.2.1",
        "publish reactor-effect-client@0.2.1 reactor-effect-browser@0.2.0 reactor-effect-native@0.2.0",
        "publish other-package@0.2.0 reactor-effect-browser@0.2.0 reactor-effect-native@0.2.0",
        `${confirmation} other-package@0.2.0`,
        `${confirmation}\n`,
        ` ${confirmation}`,
        confirmation.replace(" reactor-effect-browser", "  reactor-effect-browser"),
        confirmation.toUpperCase(),
      ];
      for (const [index, wrong] of wrongConfirmations.entries()) {
        const refused = yield* Effect.flip(
          makeInput(identity, { ...options, mode: "publish", confirmation: wrong }),
        );
        assert.equal(refused.message, confirmationRequired, `wrong confirmation ${index}`);
      }
      const authorized = yield* makeInput(identity, { ...options, mode: "publish", confirmation });
      assert.equal(authorized.authorize, true);
      assert.equal(authorized.planId, identity.planId);
      assert.equal(authorized.bundleSha256, identity.bundleSha256);
      assert.deepEqual(Object.keys(authorized).sort(), [
        "applicationCommit",
        "authorize",
        "bundleSha256",
        "candidateDirectory",
        "candidateRunId",
        "ciRunId",
        "executionHostCommit",
        "planId",
        "sourceCommit",
      ]);
      yield* Effect.flip(makeInput(identity, { ...options, candidateRunId: "8102" }));
      yield* Effect.flip(makeInput(identity, { ...options, mode: "prepare" }));
      yield* Effect.flip(makeInput(identity, { ...options, executionHostCommit: "4".repeat(40) }));
      yield* Effect.flip(
        makeInput(identity, { ...options, ciRun: { ...workflowRun(), head_sha: "4".repeat(40) } }),
      );
      yield* Effect.flip(makeInput({ ...identity, unexpected: "input" }, options));
    }).pipe(withEnvironment(executionEnvironment())),
  ));

test("a prerelease confirmation names every package at the exact prerelease version", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture({ version: "0.2.0-rc.1" });
      const { identity } = yield* prepared(fixture);
      const expected = confirming("0.2.0-rc.1");
      assert.equal(confirmationFor(identity.qualification), expected);
      const options = {
        ciRun: workflowRun(),
        candidateRun: preparationRun(),
        ciRunId,
        candidateRunId: "8101",
        executionHostCommit: applicationCommit,
        candidateDirectory: fixture.candidateDirectory,
        mode: "publish",
        confirmation: expected,
      };
      assert.equal((yield* makeInput(identity, options)).authorize, true);
      const refused = yield* Effect.flip(makeInput(identity, { ...options, confirmation }));
      assert.equal(refused.message, confirmationRequired);
    }).pipe(withEnvironment(executionEnvironment())),
  ));

test("a newer dispatched main host admits the original candidate without changing its identities or bytes", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixture = yield* makeFixture();
      const { identity, input } = yield* prepared(fixture);
      const original = yield* loadOffline(input);
      const executionHostCommit = "4".repeat(40);
      const files = [
        "identity.json",
        "bundle.json",
        "plan.json",
        ...original.bundle.artifacts.flatMap((file) =>
          file._tag === "OwnedFile" ? [`content/${file.content.sha256}`] : [],
        ),
      ];
      const readAll = Effect.forEach(files, (file) =>
        fs.readFile(path.join(fixture.candidateDirectory, file)),
      );
      const before = yield* readAll;
      const resumed = yield* makeInput(identity, {
        ciRun: workflowRun(),
        candidateRun: preparationRun(),
        ciRunId,
        candidateRunId: "8101",
        executionHostCommit,
        candidateDirectory: fixture.candidateDirectory,
        mode: "publish",
        confirmation,
      }).pipe(withEnvironment(executionEnvironment(executionHostCommit)));
      assert.equal(resumed.executionHostCommit, executionHostCommit);
      assert.equal(resumed.applicationCommit, applicationCommit);
      assert.equal(resumed.sourceCommit, sourceCommit);
      assert.equal(resumed.bundleSha256, input.bundleSha256);
      assert.equal(resumed.planId, input.planId);
      assert.equal(resumed.candidateRunId, input.candidateRunId);
      const restored = yield* loadOffline(resumed);
      assert.deepEqual(restored.bundle, original.bundle);
      assert.deepEqual(restored.plan, original.plan);
      assert.equal(restored.plan.journalId, original.plan.journalId);
      assert.deepEqual(yield* readAll, before);
      // The new host cannot rewrite the original application/source or signed invocation.
      yield* Effect.flip(loadOffline({ ...resumed, applicationCommit: executionHostCommit }));
      yield* Effect.flip(loadOffline({ ...resumed, sourceCommit: executionHostCommit }));
      yield* Effect.flip(loadOffline({ ...resumed, candidateRunId: "8102" }));
    }),
  ));

test("retained selection binds both run attempts and the original preparation and source commits", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const { identity } = yield* prepared(fixture);
      const options = {
        ciRun: workflowRun(),
        candidateRun: preparationRun(),
        ciRunId,
        candidateRunId: "8101",
        executionHostCommit: applicationCommit,
        candidateDirectory: fixture.candidateDirectory,
        mode: "observe",
        confirmation: "",
      };
      for (const changes of [
        { ciRun: { ...workflowRun(), run_attempt: 2 } },
        { candidateRun: { ...preparationRun(), run_attempt: 2 } },
        { candidateRun: { ...preparationRun(), head_sha: "4".repeat(40) } },
        { candidateRun: { ...preparationRun(), path: ".github/workflows/ci.yml" } },
        { candidateRun: { ...preparationRun(), id: 8102 } },
      ])
        yield* Effect.flip(makeInput(identity, { ...options, ...changes }));
      for (const changes of [
        { provenance: { ...identity.provenance, runId: "8102" } },
        { provenance: { ...identity.provenance, runAttempt: "2" } },
        { provenance: { ...identity.provenance, sourceCommit: "4".repeat(40) } },
        { applicationCommit: "4".repeat(40) },
        { qualification: { ...identity.qualification, ciRunAttempt: "2" } },
      ])
        yield* Effect.flip(makeInput({ ...identity, ...changes }, options));
    }).pipe(withEnvironment(executionEnvironment())),
  ));

test("execution authority is restricted to the exact manually dispatched main checkout", () => {
  const environment = executionEnvironment();
  assert.equal(
    Effect.runSync(validateExecutionHost(applicationCommit).pipe(withEnvironment(environment))),
    applicationCommit,
  );
  for (const changes of [
    { GITHUB_REF: "refs/heads/recovery" },
    { GITHUB_REF: "refs/tags/v0.2.0" },
    { GITHUB_REPOSITORY: "other/reactor-effect-client" },
    { GITHUB_EVENT_NAME: "push" },
    { GITHUB_SHA: "main" },
    { GITHUB_SHA: "4".repeat(40) },
  ])
    assert.throws(() =>
      Effect.runSync(
        validateExecutionHost(applicationCommit).pipe(
          withEnvironment({ ...environment, ...changes }),
        ),
      ),
    );
});

test("workflow selection uses the dispatched host while retaining the original CI and preparation source", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fixture = yield* makeFixture();
      const host = yield* checkoutCommit;
      const ci = path.join(fixture.directory, "ci.json");
      const candidate = path.join(fixture.directory, "candidate-run.json");
      const output = path.join(fixture.directory, "output");
      const script = yield* path.fromFileUrl(new URL("../ci.mjs", import.meta.url));
      yield* writeJson(ci, workflowRun());
      yield* writeJson(candidate, preparationRun());
      /** The exit code of ci.mjs select in this mode and environment.
       * @param {string} mode @param {Record<string, string>} [environment] */
      const select = (mode, environment = {}) =>
        spawner.exitCode(
          ChildProcess.make(process.execPath, [script, "select", ci, candidate], {
            env: {
              ...executionEnvironment(host),
              CI_RUN_ID: ciRunId,
              CANDIDATE_RUN_ID: "8101",
              RELEASE_MODE: mode,
              GITHUB_OUTPUT: output,
              ...environment,
            },
            extendEnv: true,
            stdout: "ignore",
            stderr: "ignore",
          }),
        );
      assert.equal(yield* select("publish"), 0);
      assert.match(yield* fs.readFileString(output), new RegExp(`execution-host-commit=${host}`));
      assert.match(yield* fs.readFileString(output), new RegExp(`source-commit=${sourceCommit}`));
      assert.equal(yield* select("observe"), 0);
      assert.notEqual(yield* select("publish", { GITHUB_SHA: sourceCommit }), 0);
      assert.notEqual(yield* select("publish", { GITHUB_REF: "refs/heads/recovery" }), 0);
      assert.notEqual(yield* select("prepare"), 0);
      yield* writeJson(candidate, { ...preparationRun(), head_sha: "4".repeat(40) });
      assert.notEqual(yield* select("publish"), 0);
      yield* writeJson(ci, { ...workflowRun(), head_sha: host });
      assert.equal(yield* select("prepare"), 0);
    }),
  ));

const operationIds = ["client-operation", "browser-operation", "native-operation"];
/** @param {string} status @param {string} [operationId] @param {string} [planId] */
const observation = (status, operationId = "client-operation", planId = "selected-plan") => ({
  planId,
  body: { _tag: "ObservationRecorded", evidenceKind: "Observation", operationId, status },
});
/** @param {string} status @param {string} [planId] */
const everyPackage = (status, planId = "selected-plan") =>
  operationIds.map((operationId) => observation(status, operationId, planId));
/** @param {Parameters<typeof visibility>} args */
const status = (...args) => Effect.runSync(visibility(...args));

test("receipts, dispatch errors and other Plan or operation observations cannot prove visibility", () => {
  assert.equal(
    status("selected-plan", operationIds, [
      ...operationIds.map((operationId) => ({
        planId: "selected-plan",
        body: { _tag: "ReceiptAccepted", operationId, status: "Satisfied" },
      })),
      ...operationIds.map((operationId) => ({
        planId: "selected-plan",
        body: {
          _tag: "ObservationRecorded",
          evidenceKind: "DispatchError",
          operationId,
          status: "Satisfied",
        },
      })),
      ...everyPackage("Satisfied", "another-plan"),
      observation("Satisfied", "another-operation"),
    ]),
    "Unconfirmed",
  );
});

test("visibility requires the latest registry observation of every package to be satisfied", () => {
  assert.equal(status("selected-plan", operationIds, everyPackage("Satisfied")), "Satisfied");
  assert.equal(
    status("selected-plan", operationIds, [
      ...everyPackage("Satisfied"),
      observation("Conflict", "native-operation", "another-plan"),
    ]),
    "Satisfied",
  );
  // One package short of visibility, whether unobserved or not yet visible.
  for (const partial of [
    everyPackage("Satisfied").slice(0, 2),
    [observation("Satisfied", "client-operation")],
    [...everyPackage("Satisfied"), observation("Absent", "native-operation")],
    [...everyPackage("Satisfied"), observation("Pending", "browser-operation")],
    [...everyPackage("Satisfied"), observation("Inconclusive", "client-operation")],
    [...everyPackage("Absent"), observation("Satisfied", "client-operation")],
    everyPackage("Absent"),
    [],
  ])
    assert.equal(status("selected-plan", operationIds, partial), "Unconfirmed");
  // Earlier observations never outrank the latest one of the same package.
  assert.equal(
    status("selected-plan", operationIds, [
      ...everyPackage("Absent"),
      ...everyPackage("Satisfied"),
    ]),
    "Satisfied",
  );
  assert.equal(
    status("selected-plan", operationIds, [
      ...everyPackage("Conflict"),
      ...everyPackage("Satisfied"),
    ]),
    "Satisfied",
  );
});

test("one conflicting package fails visibility even while another package is unobserved", () => {
  assert.equal(
    status("selected-plan", operationIds, [
      ...everyPackage("Satisfied"),
      observation("Conflict", "native-operation"),
    ]),
    "Conflict",
  );
  assert.equal(
    status("selected-plan", operationIds, [observation("Conflict", "browser-operation")]),
    "Conflict",
  );
  assert.equal(
    status("selected-plan", operationIds, [
      observation("Satisfied", "client-operation"),
      observation("Conflict", "client-operation"),
      observation("Satisfied", "browser-operation"),
    ]),
    "Conflict",
  );
  assert.equal(
    status(
      "selected-plan",
      ["client-operation"],
      [observation("Satisfied", "client-operation"), observation("Conflict", "native-operation")],
    ),
    "Satisfied",
  );
});

test("visibility of a Plan without npm publications is a failure, not a success", () => {
  assert.throws(() => status("selected-plan", [], []));
  assert.throws(() => status("selected-plan", [], everyPackage("Satisfied")));
});
