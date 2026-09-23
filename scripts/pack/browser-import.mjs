import * as Layer from "effect/Layer";
import * as Root from "reactor-effect-client";
import * as Browser from "reactor-effect-browser";
import * as H3 from "reactor-effect-client/h3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import * as Testing from "reactor-effect-client/testing";
import * as Wire from "reactor-effect-client/wire";

if (
  typeof Root.make !== "function" ||
  !Layer.isLayer(Browser.layer) ||
  typeof Browser.media !== "function"
) {
  throw new Error("root/browser import omitted its canonical host layer or media capability");
}
if (Object.keys(H3).length === 0) throw new Error("browser-safe H3 entry is empty");
for (const entry of [Orchestration, Simulation, Testing, Wire])
  if (Object.keys(entry).length === 0) throw new Error("a browser-safe public entry is empty");
if (!Testing.dataUri(Testing.pngBytes(2, 2)).startsWith("data:image/png;base64,iVBOR"))
  throw new Error("browser-safe PNG fixture did not produce PNG bytes");
console.log("browser-import-ok");
