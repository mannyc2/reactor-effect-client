/** The real client over a simulated Reactor, shared by the suites that drive a session. */
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { Effect, FileSystem, Layer, Path } from "effect";
import * as H3 from "../../src/H3.js";
import { Coordinator, Reactor, ReactorTest } from "../../src/index.js";

/**
 * Each block gets its own simulated Reactor, so no session or bill carries over; `reconnect` is
 * the client's policy for its sessions.
 */
export const environment = ({
  reconnect,
  ...options
}: Parameters<typeof ReactorTest.layer>[0] & Pick<Reactor.Options, "reconnect">) =>
  Reactor.layer({ reconnect }).pipe(
    Layer.provideMerge(Coordinator.layer()),
    Layer.provideMerge(ReactorTest.layer(options)),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  );

/** Tokens from the simulated Reactor's key: sessions capped at five minutes, tokens of ten. */
export const tokens = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* Coordinator.Coordinator;
  return coordinator.tokens({
    apiKey: test.apiKey,
    modelName: H3.modelName,
    maxSessionDuration: "5 minutes",
    expiresAfter: "10 minutes",
  });
});

/** A connected H3 session. */
export const connect = Effect.gen(function* () {
  const reactor = yield* Reactor.Reactor;
  return yield* reactor.create({ model: H3.modelName, tokens: yield* tokens });
});

/** The simulated model's log, narrowed to the commands it received with one name. */
export const commands = (name: string) =>
  Effect.map(ReactorTest.ReactorTest.pipe(Effect.flatMap((test) => test.log)), (log) =>
    log.filter((entry) => entry.kind === "command" && entry.name === name),
  );
