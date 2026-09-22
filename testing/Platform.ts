import { Layer } from "effect"
import { BunHttpClient, BunServices } from "@effect/platform-bun"

/** Effect Platform services used by the offline H3 adapter tests. */
export const layer = Layer.mergeAll(BunServices.layer, BunHttpClient.layer)
