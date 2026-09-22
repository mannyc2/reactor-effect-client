const browser = await import("reactor-effect-client/browser");

if (typeof browser.connect !== "function" || browser.layer == null) {
  throw new Error("browser export did not expose its public host surface");
}

console.log("browser-import-ok");
