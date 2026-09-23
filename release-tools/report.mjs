import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { runApplication, runInterruptibleProcess } from "@mannyc1/ts-release/node";
import { readBytes, reject } from "./model.mjs";

/** Receipt acceptance is not registry visibility. Only the latest actual
 * observation of each publication in this Plan can establish visibility;
 * cached receipts cannot. Every package must be visible, and one conflict fails the release.
 * @param {string} planId @param {readonly string[]} operationIds
 * @param {readonly {planId: string, body: {_tag: string, evidenceKind?: string, operationId?: string, status?: string}}[]} events */
export const visibility = (planId, operationIds, events) => {
  if (operationIds.length === 0) reject("Expected at least one npm publication for visibility");
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
};

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [inputFile, reportFile] = process.argv.slice(2);
  if (!inputFile || !reportFile) reject("usage: report.mjs input.json report.json");
  process.exitCode = await runInterruptibleProcess(async (signal, interrupted) => {
    try {
      const input = JSON.parse(readBytes(inputFile).toString());
      for (let attempt = 0; attempt < 6; attempt++) {
        // Bounded observation only. No iteration can dispatch a PUT or authorize a replay.
        const report = await runApplication(
          fileURLToPath(new URL("./application.mjs", import.meta.url)),
          { ...input, authorize: false },
          signal,
          "observe",
        );
        writeFileSync(reportFile, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
        const status = visibility(
          report.plan.planId,
          report.plan.operations.map((operation) => operation.operationId),
          report.journal.events,
        );
        if (status === "Conflict") reject("Registry content conflicts with the retained candidate");
        if (status === "Satisfied") return 0;
        if (attempt < 5) await setTimeout(5_000, undefined, { signal });
      }
      console.error(
        "Registry visibility remains unconfirmed. Observe the original candidate and journal; do not rebuild or blindly republish.",
      );
      return 2;
    } catch (error) {
      if (signal.aborted) return interrupted();
      throw error;
    }
  });
}
