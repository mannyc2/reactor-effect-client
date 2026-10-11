/** Load Cubism Core before importing the Framework, which reads its global during evaluation. */
import type { Adapter } from "../src/Avatar.ts";

const loadCore = (src: string): Promise<void> =>
  "Live2DCubismCore" in globalThis
    ? Promise.resolve()
    : new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = src;
        script.onload = () => resolve();
        script.onerror = () => {
          script.remove();
          reject(new Error("Cubism Core did not load"));
        };
        document.head.append(script);
      });

export const load: Adapter["load"] = async (options) => {
  await loadCore(options.core);
  const model = await import("./model.ts");
  return model.load(options);
};
