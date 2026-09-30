import { pathToFileURL } from "node:url";
import { Crypto, Effect, Layer, PlatformError } from "effect";
import { TestClock } from "effect/testing";
import {
  CoordinatorClient,
  H3,
  H3Source,
  Playout,
  Reactor,
  ReactorTest,
} from "reactor-effect-client";

/**
 * Runs the compiled client example, the Rundown, on a Playout over the
 * installed simulated Reactor and the test clock: no credentials, platform
 * package or native addon, and seconds of programme in milliseconds.
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

const open = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  return yield* H3Source.open({
    tokens: coordinator.tokens({
      apiKey: test.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: "10 minutes",
      expiresAfter: "15 minutes",
    }),
  });
});

const simulated = Rundown.layer.pipe(
  Layer.provideMerge(Playout.layer({ open, lanes: [{ name: "show" }] })),
  Layer.provideMerge(Reactor.layer()),
  Layer.provideMerge(CoordinatorClient.layer()),
  Layer.provideMerge(
    ReactorTest.layer({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "70 millis" }) }),
  ),
  Layer.provide(webCrypto),
);

const program = Effect.gen(function* () {
  yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
  return yield* (yield* Rundown).play([
    { prompt: "one", seconds: 5 },
    { prompt: "two", seconds: 5 },
  ]);
}).pipe(Effect.scoped, Effect.provide(simulated), Effect.provide(TestClock.layer()));

const outcomes = await Effect.runPromise(program.pipe(Effect.timeout("30 seconds")));
const tags = outcomes.map((outcome) => outcome._tag).join(",");
if (tags !== "Played,Played") throw new Error(`compiled example played ${tags}`);
console.log("compiled-example-ok offline=ReactorTest rundown=played,played");
