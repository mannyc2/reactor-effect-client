import * as Layer from "effect/Layer";
import * as Client from "reactor-effect-client";
import * as Browser from "reactor-effect-browser";

/**
 * The installed browser host and every client module load together, and this
 * file is also bundled for the browser and run without Node's Buffer.
 */
if (!Layer.isLayer(Browser.BrowserPeer.layer))
  throw new Error("the browser host exports no PeerFactory layer");
if (
  typeof Browser.BrowserMedia.tracks !== "function" ||
  typeof Browser.BrowserMedia.play !== "function"
)
  throw new Error("the browser host exports no track media");
for (const [name, module] of Object.entries(Client))
  if (Object.keys(module).length === 0) throw new Error(`${name} exports nothing`);
const png = Client.ReactorTest.pngBytes({ width: 2, height: 2 });
if (png[1] !== 0x50 || png[2] !== 0x4e || png[3] !== 0x47)
  throw new Error("the client's PNG fixture did not produce PNG bytes");
console.log("browser-import-ok");
