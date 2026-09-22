import assert from "node:assert/strict";
import { test } from "bun:test";
import { makeInput } from "../input.mjs";
import { validateRun } from "../model.mjs";
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

test("explicit confirmation selects publication; observe cannot acquire publication authority", () =>
  withFixture(async (fixture) => {
    const { identity } = await prepared(fixture);
    const options = {
      ciRun: workflowRun(),
      ciRunId,
      applicationCommit,
      candidateDirectory: fixture.candidateDirectory,
      mode: "observe",
      confirmation: "",
    };
    assert.equal(makeInput(identity, options).authorize, false);
    assert.equal(
      makeInput(identity, { ...options, confirmation: "publish reactor-effect-client@0.2.0" })
        .authorize,
      false,
    );
    for (const confirmation of [
      "",
      "publish",
      "publish other-package@0.2.0",
      "publish reactor-effect-client@0.2.1",
      "publish reactor-effect-client@0.2.0\n",
    ])
      assert.throws(() => makeInput(identity, { ...options, mode: "publish", confirmation }));
    const authorized = makeInput(identity, {
      ...options,
      mode: "publish",
      confirmation: "publish reactor-effect-client@0.2.0",
    });
    assert.equal(authorized.authorize, true);
    assert.equal(authorized.planId, identity.planId);
    assert.equal(authorized.bundleSha256, identity.bundleSha256);
    assert.deepEqual(Object.keys(authorized).sort(), [
      "applicationCommit",
      "authorize",
      "bundleSha256",
      "candidateDirectory",
      "ciRunId",
      "planId",
      "sourceCommit",
    ]);
    assert.throws(() => makeInput(identity, { ...options, mode: "prepare" }));
    assert.throws(() => makeInput(identity, { ...options, applicationCommit: "4".repeat(40) }));
    assert.throws(() =>
      makeInput(identity, { ...options, ciRun: { ...workflowRun(), head_sha: "4".repeat(40) } }),
    );
    assert.throws(() => makeInput({ ...identity, unexpected: "input" }, options));
  }));

/** @param {string} status @param {string} [planId] @param {string} [operationId] */
const observation = (status, planId = "selected-plan", operationId = "selected-operation") => ({
  planId,
  body: { _tag: "ObservationRecorded", evidenceKind: "Observation", operationId, status },
});

test("receipts, dispatch errors and other Plan or operation observations cannot prove visibility", () => {
  assert.equal(
    visibility(
      "selected-plan",
      ["selected-operation"],
      [
        { planId: "selected-plan", body: { _tag: "ReceiptAccepted", status: "Satisfied" } },
        {
          planId: "selected-plan",
          body: {
            _tag: "ObservationRecorded",
            evidenceKind: "DispatchError",
            operationId: "selected-operation",
            status: "Satisfied",
          },
        },
        observation("Satisfied", "another-plan"),
        observation("Satisfied", "selected-plan", "another-operation"),
      ],
    ),
    "Unconfirmed",
  );
});

test("only the latest registry observation controls visibility and conflict", () => {
  for (const status of ["Absent", "Pending", "Inconclusive"])
    assert.equal(
      visibility(
        "selected-plan",
        ["selected-operation"],
        [observation("Satisfied"), observation(status)],
      ),
      "Unconfirmed",
    );
  assert.equal(
    visibility(
      "selected-plan",
      ["selected-operation"],
      [observation("Satisfied"), observation("Conflict")],
    ),
    "Conflict",
  );
  assert.equal(
    visibility(
      "selected-plan",
      ["selected-operation"],
      [observation("Absent"), observation("Satisfied")],
    ),
    "Satisfied",
  );
  assert.equal(
    visibility(
      "selected-plan",
      ["selected-operation"],
      [observation("Satisfied"), observation("Conflict", "another-plan")],
    ),
    "Satisfied",
  );
  assert.throws(() => visibility("selected-plan", [], []));
  assert.throws(() => visibility("selected-plan", ["one", "two"], []));
});
