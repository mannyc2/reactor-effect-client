/** Test-only fault policies for the simulated engine. Production supplies no
 * faults at all, so these stay out of the shipped module. */
export { buildFailsEveryNth, sessionFailsAfter } from "../src/testing/Faults.js"
