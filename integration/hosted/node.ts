/**
 * Lets Node run this harness from its TypeScript sources, as Bun does:
 * `node --import ./node.ts main.ts`. Node strips the types but resolves
 * `./Run.js` only to a file of that name, so a relative `.js` import that names
 * no file resolves to the `.ts` source beside it. The adoption check's paid
 * owner starts this way, since the isolated native peer needs a Node parent.
 */
import { registerHooks } from "node:module";

const relativeScript = /^\.{1,2}\/.*\.js$/;

registerHooks({
  resolve: (specifier, context, nextResolve) => {
    try {
      return nextResolve(specifier, context);
    } catch (error) {
      if (!relativeScript.test(specifier)) throw error;
      return nextResolve(`${specifier.slice(0, -".js".length)}.ts`, context);
    }
  },
});
