import { pathToFileURL } from "node:url";
import * as Effect from "effect/Effect";

const path = process.argv[2];
if (path === undefined) throw new Error("example smoke requires a compiled simulation module");
/** @type {{ simulateClip: Effect.Effect<{ clipId: string, joinedClipId: string, cleanup: import("reactor-effect-client/orchestration").CleanupReport, repeatedCleanup: import("reactor-effect-client/orchestration").CleanupReport }, unknown> }} */
const { simulateClip } = await import(pathToFileURL(path).href);
const result = await Effect.runPromise(simulateClip.pipe(Effect.timeout(10_000)));
if (result.clipId.length === 0 || result.joinedClipId !== result.clipId)
  throw new Error("compiled example did not join the same committed submission");
if (
  result.cleanup !== result.repeatedCleanup ||
  result.cleanup.sessions.length !== 1 ||
  result.cleanup.sessions.some(
    (entry) =>
      !entry.lease.localClosed ||
      entry.lease.localErrors.length !== 0 ||
      entry.lease.allocation !== "none",
  )
)
  throw new Error("compiled example did not join idempotent offline cleanup");
console.log("compiled-example-ok offline=simulation dispatch=joined cleanup=joined");
