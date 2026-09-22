import * as Root from "reactor-effect-client";
import * as Browser from "reactor-effect-client/browser";
import * as H3 from "reactor-effect-client/h3";

if (
  typeof Root.make !== "function" ||
  typeof Browser.make !== "function" ||
  typeof Browser.media !== "function"
) {
  throw new Error("root/browser import omitted its canonical host factory or media capability");
}
if (Object.keys(H3).length === 0) throw new Error("browser-safe H3 entry is empty");
console.log("browser-import-ok");
