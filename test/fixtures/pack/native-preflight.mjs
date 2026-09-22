import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Native from "reactor-effect-client/native";
import { PeerFactory } from "reactor-effect-client/PeerFactory";

await Effect.runPromise(Effect.scoped(
  Layer.build(Native.layer()).pipe(
    Effect.flatMap((context) => Context.get(context, PeerFactory).check),
  ),
));

console.log("native-preflight-ok");
