import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import { PeerFactory } from "reactor-effect-client/Peer";
import { NativePeer } from "reactor-effect-native";

const expected = JSON.parse(process.env.PACK_NATIVE_IDENTITY ?? "null");
if (expected === null) throw new Error("the pack runner must supply the qualified native identity");
// The binding's optional dependency installed this host's platform package.
const directory = join(process.cwd(), "node_modules", `reactor-effect-native-${expected.platform}`);
const identity = JSON.parse(readFileSync(join(directory, "native-identity.json"), "utf8"));
if (JSON.stringify(identity) !== JSON.stringify(expected))
  throw new Error("installed native identity differs from the qualified package");
if (
  createHash("sha256")
    .update(readFileSync(join(directory, identity.file)))
    .digest("hex") !== expected.sha256
) {
  throw new Error("installed native bytes differ from the exact tested artifact");
}
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      // Building the host layer is the preflight: an addon that cannot load
      // fails there, before any session could be allocated.
      const failed = yield* Effect.result(
        Layer.build(NativePeer.layer({ addon: join(directory, "does-not-exist.node") })),
      );
      if (failed._tag !== "Failure" || failed.failure.context.outcome !== "not-submitted")
        throw new Error("native preflight did not refuse an absent addon before allocation");
      // The installed addon negotiates an offer, and its peer shuts down with the scope.
      const factory = Context.get(yield* Layer.build(NativePeer.layer()), PeerFactory);
      yield* factory.check;
      const offer = yield* Effect.scoped(
        Effect.gen(function* () {
          const peer = yield* factory.make;
          const prepared = yield* peer.prepare(
            [],
            [
              { name: "main_video", kind: "video", direction: "recvonly" },
              { name: "main_audio", kind: "audio", direction: "recvonly" },
            ],
            () => {},
          );
          yield* peer.close;
          return prepared;
        }),
      );
      if (!offer.sdp.includes("m=video") || offer.mapping.length !== 2)
        throw new Error("installed native peer did not negotiate its declared tracks");
    }),
  ),
);
console.log(
  `native-preflight-ok sha256=${expected.sha256} sourceSha256=${expected.build.sourceSha256} target=${expected.build.target}`,
);
