const modules = await Promise.all([
  import("reactor-effect-client"),
  import("reactor-effect-client/Client"),
  import("reactor-effect-client/Sessions"),
  import("reactor-effect-client/h3"),
]);

if (modules.some((value) => value == null || typeof value !== "object")) {
  throw new Error("portable package import did not return module namespaces");
}

console.log("portable-import-ok");
