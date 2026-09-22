import type { SimulatedFaults } from "../simulation/types.js";

export const buildFailsEveryNth = (n: number): SimulatedFaults => ({
  buildFails: (seq) => seq % n === 0,
});
export const sessionFailsAfter = (n: number): SimulatedFaults => ({
  sessionFails: (seq) => seq > n,
});
