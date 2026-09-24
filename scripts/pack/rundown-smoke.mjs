import { pathToFileURL } from "node:url";
import { Crypto, Effect, Fiber, Layer, PlatformError } from "effect";
import { TestClock } from "effect/testing";
import * as Simulation from "reactor-effect-client/simulation";

/**
 * Runs the compiled client example, the Rundown, against the installed
 * simulation on the test clock: no coordinator, credentials, platform
 * package or native library, and seconds of programme in milliseconds.
 */
const path = process.argv[2];
if (path === undefined) throw new Error("example smoke requires the compiled Rundown module");
// The consumer holds the staged example source beside this fixture.
/** @type {typeof import("./packages/client/examples/src/Rundown.ts")} */
const { Rundown } = await import(pathToFileURL(path).href);

const webCrypto = Layer.sync(Crypto.Crypto, () =>
  Crypto.make({
    randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, bytes) =>
      Effect.tryPromise({
        try: async () =>
          new Uint8Array(await globalThis.crypto.subtle.digest(algorithm, Uint8Array.from(bytes))),
        catch: (cause) =>
          PlatformError.systemError({ _tag: "Unknown", module: "Crypto", method: "digest", cause }),
      }),
  }),
);

const program = Effect.gen(function* () {
  const rundown = yield* Rundown;
  const played = yield* rundown
    .play([
      { prompt: "one", seconds: 5 },
      { prompt: "two", seconds: 5 },
    ])
    .pipe(Effect.forkChild);
  yield* TestClock.adjust("5 minutes");
  return yield* Fiber.join(played);
}).pipe(
  Effect.provide(
    Rundown.layer().pipe(Layer.provide(Simulation.layerSim().pipe(Layer.provide(webCrypto)))),
  ),
  Effect.provide(TestClock.layer()),
);

const outcomes = await Effect.runPromise(program.pipe(Effect.timeout("10 seconds")));
const tags = outcomes.map((outcome) => outcome._tag).join(",");
if (tags !== "Played,Played") throw new Error(`compiled example played ${tags}`);
console.log("compiled-example-ok offline=simulation rundown=played,played");
