import * as NodeServices from "@effect/platform-node/NodeServices";
import { Cause, Console, Effect, Exit, FileSystem, Path, Schema, Stdio } from "effect";
import { runApplication, runInterruptibleProcess } from "@mannyc1/ts-release/node";
import { ApplicationInput, jsonDocument, readJson, reject } from "./model.mjs";

/** Receipt acceptance is not registry visibility. Only the latest actual
 * observation of each publication in this Plan can establish visibility;
 * cached receipts cannot. Every package must be visible, and one conflict fails the release. */
export const visibility = Effect.fnUntraced(
  /** @param {string} planId @param {readonly string[]} operationIds
   * @param {readonly {planId: string, body: {_tag: string, evidenceKind?: string, operationId?: string, status?: string}}[]} events */
  function* (planId, operationIds, events) {
    if (operationIds.length === 0)
      return yield* reject("Expected at least one npm publication for visibility");
    /** @type {Map<string | undefined, string | undefined>} */
    const observations = new Map();
    for (const event of events)
      if (
        event.planId === planId &&
        event.body._tag === "ObservationRecorded" &&
        event.body.evidenceKind === "Observation"
      )
        observations.set(event.body.operationId, event.body.status);
    if (operationIds.some((id) => observations.get(id) === "Conflict")) return "Conflict";
    return operationIds.every((id) => observations.get(id) === "Satisfied")
      ? "Satisfied"
      : "Unconfirmed";
  },
);

/** Observe the original candidate: 0 once every package is visible, 2 while any stays unconfirmed. */
const observe = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const [inputFile, reportFile] = yield* (yield* Stdio.Stdio).args;
  if (inputFile === undefined || inputFile === "" || reportFile === undefined || reportFile === "")
    return yield* reject("usage: report.mjs input.json report.json");
  const input = yield* readJson(inputFile).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ApplicationInput, { onExcessProperty: "error" })),
  );
  const application = yield* path.fromFileUrl(new URL("./application.mjs", import.meta.url));
  for (let attempt = 0; attempt < 6; attempt++) {
    // Bounded observation only. No iteration can dispatch a PUT or authorize a replay.
    // ts-release reports a failed run by rejecting, which ends the observation as a failure.
    const report = yield* Effect.promise((signal) =>
      runApplication(application, { ...input, authorize: false }, signal, "observe"),
    );
    // Host evidence belongs to this invocation, never to the retained Plan or Bundle.
    const evidence = yield* jsonDocument({
      ...report,
      executionHostCommit: input.executionHostCommit,
    });
    yield* fs.writeFileString(reportFile, `${evidence}\n`, { mode: 0o600 });
    const status = yield* visibility(
      report.plan.planId,
      report.plan.operations.map((operation) => operation.operationId),
      report.journal.events,
    );
    if (status === "Conflict")
      return yield* reject("Registry content conflicts with the retained candidate");
    if (status === "Satisfied") return 0;
    if (attempt < 5) yield* Effect.sleep("5 seconds");
  }
  yield* Console.error(
    "Registry visibility remains unconfirmed. Observe the original candidate and journal; do not rebuild or blindly republish.",
  );
  return 2;
});

// ts-release owns this process's signals and exit code: 130 or 143 when a signal interrupts it.
if (import.meta.main)
  process.exitCode = await runInterruptibleProcess((signal, interrupted) =>
    Effect.runPromiseExit(
      observe.pipe(
        // The observation is this process's entry point.
        // @effect-diagnostics-next-line strictEffectProvide:off
        Effect.provide(NodeServices.layer),
      ),
      { signal },
    ).then((exit) => {
      if (Exit.isSuccess(exit)) return exit.value;
      if (signal.aborted) return interrupted();
      throw Cause.squash(exit.cause);
    }),
  );
