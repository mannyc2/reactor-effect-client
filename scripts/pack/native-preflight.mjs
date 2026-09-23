import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Native from "reactor-effect-native";

const expected = JSON.parse(process.env.PACK_NATIVE_IDENTITY ?? "null");
if (expected === null) throw new Error("the pack runner must supply the qualified native identity");
const directory = join(
  process.cwd(),
  "node_modules",
  "reactor-effect-native",
  "lib",
  `${process.platform}-${process.arch}`,
);
const identity = JSON.parse(readFileSync(join(directory, "native-identity.json"), "utf8"));
if (JSON.stringify(identity) !== JSON.stringify(expected))
  throw new Error("installed native identity differs from the qualified package");
if (
  createHash("sha256")
    .update(readFileSync(join(directory, identity.library)))
    .digest("hex") !== expected.sha256
) {
  throw new Error("installed native bytes differ from the exact tested artifact");
}
let requests = 0,
  deleted = 0;
/** @type {typeof fetch} */
const fetchFixture = async (input, init) => {
  const request = new Request(input, init);
  if (new URL(request.url).origin !== "https://native.pack.fixture")
    throw new Error("native pack fixture attempted an unexpected coordinator");
  requests++;
  if (request.method === "DELETE") {
    deleted++;
    return new Response(null, { status: 202 });
  }
  return Response.json({
    session_id: "sess_native_installed",
    state: deleted > 0 ? "CLOSED" : "ACTIVE",
    capabilities: { protocol_version: "1.0", tracks: [], commands: [] },
    selected_transport: { protocol: "webrtc", version: "1.0" },
  });
};
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const invalid = yield* Native.make(
        { apiUrl: "https://native.pack.fixture" },
        { libraryPath: join(directory, "does-not-exist") },
      );
      const failed = yield* Effect.result(invalid.create({ model: "fixture/native-preflight" }));
      if (failed._tag !== "Failure" || requests !== 0)
        throw new Error("native preflight did not fail before remote allocation");
      const factory = yield* Native.make({ apiUrl: "https://native.pack.fixture" });
      const session = yield* factory.create({ model: "fixture/native-preflight" });
      const closed = yield* session.close;
      if (
        !closed.localClosed ||
        closed.localErrors.length !== 0 ||
        !closed.remote.confirmed ||
        deleted !== 1
      ) {
        throw new Error("installed public native owner did not close its fixture allocation");
      }
    }),
  ).pipe(
    Effect.provide(FetchHttpClient.layer),
    Effect.provideService(FetchHttpClient.Fetch, fetchFixture),
    Effect.provide(NodeServices.layer),
  ),
);
console.log(
  `native-preflight-ok sha256=${expected.sha256} sourceSha256=${expected.build.sourceSha256} abi=${expected.build.abiVersion}`,
);
