import assert from "node:assert/strict";
import { test } from "bun:test";
import { makeInput } from "../input.mjs";
import { confirmationFor, validateRun } from "../model.mjs";
import { visibility } from "../report.mjs";
import {
  applicationCommit,
  ciRunId,
  prepared,
  sourceCommit,
  withFixture,
  workflowRun,
} from "./Fixture.mjs";

test("only successful main CI and manual release run identities are admitted", () => {
  const ci = workflowRun();
  assert.equal(validateRun(ci, ciRunId, "ci").head_sha, sourceCommit);
  assert.equal(validateRun({ ...ci, event: "workflow_dispatch" }, ciRunId, "ci").id, 7101);
  const release = { ...ci, event: "workflow_dispatch", path: ".github/workflows/release.yml" };
  assert.equal(validateRun(release, ciRunId, "release").head_sha, sourceCommit);
  assert.throws(() => validateRun({ ...release, event: "push" }, ciRunId, "release"));
  assert.throws(() => validateRun(ci, "7102", "ci"));
  assert.throws(() => validateRun(ci, "07101", "ci"));
  assert.throws(() => validateRun(ci, "7101\n", "ci"));
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
    assert.throws(() => validateRun({ ...workflowRun(), ...changes }, ciRunId, "ci")));

const confirmation =
  "publish reactor-effect-client@0.2.0 reactor-effect-browser@0.2.0 reactor-effect-native@0.2.0";

test("explicit confirmation of every package selects publication; observe cannot acquire publication authority", () =>
  withFixture(async (fixture) => {
    const { identity } = await prepared(fixture);
    assert.equal(confirmationFor(identity.qualification), confirmation);
    const options = {
      ciRun: workflowRun(),
      ciRunId,
      candidateRunId: "8101",
      applicationCommit,
      candidateDirectory: fixture.candidateDirectory,
      mode: "observe",
      confirmation: "",
    };
    assert.equal(makeInput(identity, options).authorize, false);
    assert.equal(makeInput(identity, { ...options, confirmation }).authorize, false);
    for (const wrong of [
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
    ])
      assert.throws(
        () => makeInput(identity, { ...options, mode: "publish", confirmation: wrong }),
        Error,
        JSON.stringify(wrong),
      );
    const authorized = makeInput(identity, { ...options, mode: "publish", confirmation });
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
      "planId",
      "sourceCommit",
    ]);
    assert.throws(() => makeInput(identity, { ...options, candidateRunId: "8102" }));
    assert.throws(() => makeInput(identity, { ...options, mode: "prepare" }));
    assert.throws(() => makeInput(identity, { ...options, applicationCommit: "4".repeat(40) }));
    assert.throws(() =>
      makeInput(identity, { ...options, ciRun: { ...workflowRun(), head_sha: "4".repeat(40) } }),
    );
    assert.throws(() => makeInput({ ...identity, unexpected: "input" }, options));
  }));

test("a prerelease confirmation names every package at the exact prerelease version", () =>
  withFixture(
    async (fixture) => {
      const { identity } = await prepared(fixture);
      const expected =
        "publish reactor-effect-client@0.2.0-rc.1 reactor-effect-browser@0.2.0-rc.1 reactor-effect-native@0.2.0-rc.1";
      assert.equal(confirmationFor(identity.qualification), expected);
      const options = {
        ciRun: workflowRun(),
        ciRunId,
        candidateRunId: "8101",
        applicationCommit,
        candidateDirectory: fixture.candidateDirectory,
        mode: "publish",
        confirmation: expected,
      };
      assert.equal(makeInput(identity, options).authorize, true);
      assert.throws(() => makeInput(identity, { ...options, confirmation }));
    },
    { version: "0.2.0-rc.1" },
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

test("receipts, dispatch errors and other Plan or operation observations cannot prove visibility", () => {
  assert.equal(
    visibility("selected-plan", operationIds, [
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
  assert.equal(visibility("selected-plan", operationIds, everyPackage("Satisfied")), "Satisfied");
  assert.equal(
    visibility("selected-plan", operationIds, [
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
    assert.equal(visibility("selected-plan", operationIds, partial), "Unconfirmed");
  // Earlier observations never outrank the latest one of the same package.
  assert.equal(
    visibility("selected-plan", operationIds, [
      ...everyPackage("Absent"),
      ...everyPackage("Satisfied"),
    ]),
    "Satisfied",
  );
  assert.equal(
    visibility("selected-plan", operationIds, [
      ...everyPackage("Conflict"),
      ...everyPackage("Satisfied"),
    ]),
    "Satisfied",
  );
});

test("one conflicting package fails visibility even while another package is unobserved", () => {
  assert.equal(
    visibility("selected-plan", operationIds, [
      ...everyPackage("Satisfied"),
      observation("Conflict", "native-operation"),
    ]),
    "Conflict",
  );
  assert.equal(
    visibility("selected-plan", operationIds, [observation("Conflict", "browser-operation")]),
    "Conflict",
  );
  assert.equal(
    visibility("selected-plan", operationIds, [
      observation("Satisfied", "client-operation"),
      observation("Conflict", "client-operation"),
      observation("Satisfied", "browser-operation"),
    ]),
    "Conflict",
  );
  assert.equal(
    visibility(
      "selected-plan",
      ["client-operation"],
      [observation("Satisfied", "client-operation"), observation("Conflict", "native-operation")],
    ),
    "Satisfied",
  );
});

test("visibility of a Plan without npm publications is a failure, not a success", () => {
  assert.throws(() => visibility("selected-plan", [], []));
  assert.throws(() => visibility("selected-plan", [], everyPackage("Satisfied")));
});
