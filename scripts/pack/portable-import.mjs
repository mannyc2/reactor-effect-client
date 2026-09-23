import * as Root from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import * as Testing from "reactor-effect-client/testing";
import * as Wire from "reactor-effect-client/wire";
import * as Host from "reactor-effect-client/host";

if (typeof Root.make !== "function")
  throw new Error("portable entry omitted its canonical factory");
for (const entry of [H3, Orchestration, Simulation, Testing, Wire, Host]) {
  if (Object.keys(entry).length === 0)
    throw new Error("a portable entry exported no public contract");
}
// Host packages are separate installs now; the client never exposes them as paths.
for (const legacy of ["Client", "Model", "engine", "http", "PeerFactory", "browser", "native"]) {
  try {
    await import(`reactor-effect-client/${legacy}`);
  } catch (error) {
    if (
      error !== null &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ERR_PACKAGE_PATH_NOT_EXPORTED"
    )
      continue;
    throw error;
  }
  throw new Error(`legacy package path remained public: ${legacy}`);
}
console.log("portable-import-ok");
