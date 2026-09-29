import * as Client from "reactor-effect-client";

/**
 * Every public module of the installed client loads without a host and exports
 * something, as the root's namespaces and as its own subpath; `internal/` and
 * the removed pre-0.8 subpaths are not reachable.
 */
const modules = Object.entries(Client);
if (modules.length === 0) throw new Error("the client's index exports no module");
for (const [name, namespace] of modules) {
  const module = await import(`reactor-effect-client/${name}`);
  if (Object.keys(module).length === 0) throw new Error(`${name} exports nothing`);
  if (module !== namespace) throw new Error(`${name} differs from the index's namespace`);
}
const unreachable = ["internal/wire", "internal/session", "index", "wire", "testing"];
for (const path of [...unreachable, "orchestration", "simulation", "host", "h3"]) {
  try {
    await import(`reactor-effect-client/${path}`);
  } catch (error) {
    const code = error !== null && typeof error === "object" && "code" in error ? error.code : "";
    if (code === "ERR_PACKAGE_PATH_NOT_EXPORTED" || code === "ERR_MODULE_NOT_FOUND") continue;
    throw error;
  }
  throw new Error(`reactor-effect-client/${path} is reachable`);
}
console.log(`portable-import-ok ${modules.length} modules`);
