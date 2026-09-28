import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Root from "reactor-effect-client";
import * as H3 from "reactor-effect-client/H3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import * as Testing from "reactor-effect-client/testing";
import * as Wire from "reactor-effect-client/wire";
import * as Host from "reactor-effect-client/host";

if (typeof Root.make !== "function")
  throw new Error("portable entry omitted its canonical factory");
for (const entry of [H3, Orchestration, Simulation, Testing, Wire, Host]) {
  if (Object.keys(entry).length === 0)
    throw new Error("a portable entry exported no public contract");
}
/** @type {Queue.Queue<number, Error>} */
const hostQueue = Effect.runSync(Queue.unbounded());
Queue.offerUnsafe(hostQueue, 1);
Queue.offerAllUnsafe(hostQueue, [2, 3]);
const first = Effect.runSync(Host.takeQueue(hostQueue));
const rest = Effect.runSync(Host.takeAllQueue(hostQueue));
if (first !== 1 || rest.length !== 2 || rest[0] !== 2 || rest[1] !== 3)
  throw new Error("host queue helpers did not preserve ordered single and batch reads");
const terminal = new Error("installed-consumer queue failure");
Effect.runSync(Queue.fail(hostQueue, terminal));
const singleFailure = Effect.runSync(Effect.result(Host.takeQueue(hostQueue)));
const batchFailure = Effect.runSync(Effect.result(Host.takeAllQueue(hostQueue)));
if (
  singleFailure._tag !== "Failure" ||
  singleFailure.failure !== terminal ||
  batchFailure._tag !== "Failure" ||
  batchFailure.failure !== terminal
)
  throw new Error("host queue helpers did not preserve the queue's terminal failure");
// Host packages are separate installs now; the client never exposes them as paths.
for (const legacy of ["Client", "Model", "engine", "http", "PeerFactory", "browser", "native"]) {
  try {
    await import(`reactor-effect-client/${legacy}`);
  } catch (error) {
    if (
      error !== null &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ERR_PACKAGE_PATH_NOT_EXPORTED"
    )
      continue;
    throw error;
  }
  throw new Error(`legacy package path remained public: ${legacy}`);
}
console.log("portable-import-ok");
