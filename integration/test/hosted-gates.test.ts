/** The hosted qualification's gates refuse offline, before any network or credential. */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import {
  Refused,
  acceptGrant,
  admit,
  admitRelayCheck,
  admitTotal,
  authorize,
  billedUsd,
  liveVideo,
  options,
  stopFor,
  worstCaseUsd,
} from "../hosted/gates.js";

const refused = (evaluate: () => unknown): string => {
  try {
    evaluate();
  } catch (cause) {
    if (cause instanceof Refused) return cause.message;
    throw cause;
  }
  throw new Error("the gate admitted it");
};
const authorized = [
  "--i-authorize-paid-sessions",
  "--budget-usd=0.75",
  "--total-budget-usd=1.50",
  "--ledger=ledger",
  "--network=home fiber",
];
const without = (name: string) => authorized.filter((arg) => !arg.startsWith(name));

test("paid use needs a check, the authorization flag, bounded budgets, a ledger and a network", () => {
  expect(authorize(["vertical", ...authorized])).toEqual({
    check: "vertical",
    budgetUsd: 0.75,
    totalBudgetUsd: 1.5,
    ledger: "ledger",
    network: "home fiber",
  });
  expect(refused(() => authorize(authorized))).toContain("name the check");
  expect(refused(() => authorize(["vertical", ...without("--i-authorize")]))).toContain(
    "--i-authorize-paid-sessions",
  );
  for (const budget of ["0", "0.76", "NaN", "Infinity", "-1"])
    expect(
      refused(() => authorize(["turn", ...without("--budget-usd"), `--budget-usd=${budget}`])),
    ).toContain("--budget-usd");
  for (const total of ["0", "2.26", "NaN"])
    expect(
      refused(() =>
        authorize(["turn", ...without("--total-budget-usd"), `--total-budget-usd=${total}`]),
      ),
    ).toContain("--total-budget-usd");
  expect(
    refused(() =>
      authorize(["vertical", ...without("--total-budget-usd"), "--total-budget-usd=0.25"]),
    ),
  ).toContain("cannot exceed");
  expect(refused(() => authorize(["takeover", ...without("--ledger")]))).toContain("--ledger");
  expect(refused(() => authorize(["takeover", ...without("--network"), "--network= "]))).toContain(
    "--network",
  );
  expect(refused(() => authorize(["vertical", ...authorized, "--budget-usd=0.1"]))).toContain(
    "more than once",
  );
});

test("an unknown argument refuses, so a misspelled budget is never ignored", () => {
  expect(refused(() => authorize(["vertical", ...authorized, "--budget=0.1"]))).toContain(
    "unknown argument --budget=0.1",
  );
  expect(refused(() => options(["stray"], ["ledger"]))).toContain("unknown argument stray");
  expect(options(["--ledger=a=b"], ["ledger"]).get("ledger")).toBe("a=b");
});

test("session time bills by the minute, a started minute whole", () => {
  // Reactor's published rate on September 24, 2026: $0.75 a minute.
  const live = { creditsPerSecond: 125, creditsPerDollar: 10_000 };
  expect(billedUsd(live, 0)).toBe(0);
  expect(billedUsd(live, 1)).toBe(0.75);
  expect(billedUsd(live, 60)).toBe(0.75);
  expect(billedUsd(live, 60.5)).toBe(1.5);
  expect(billedUsd(live, -3)).toBe(0);
});

test("the published rate must fit the whole capped session, billed by the minute", () => {
  const live = { creditsPerSecond: 125, creditsPerDollar: 10_000 };
  // The 50 s cap still bills a whole minute.
  expect(worstCaseUsd(live)).toBe(0.75);
  expect(admit(live, 0.75)).toBe(0.75);
  expect(refused(() => admit(live, 0.5))).toContain("over the $0.5 budget");
  const cheap = { creditsPerSecond: 10, creditsPerDollar: 2000 };
  expect(worstCaseUsd(cheap)).toBeCloseTo(0.3);
});

test("the ledger admits a run only while every reserved worst case fits the total", () => {
  expect(admitTotal([], 0.75, 1.5)).toBe(0);
  // Three $0.75 runs fit $2.25 exactly, and binary fractions never refuse a fit.
  expect(admitTotal([0.75, 0.75], 0.75, 2.25)).toBeCloseTo(1.5);
  expect(admitTotal([0.1, 0.2], 1.2, 1.5)).toBeCloseTo(0.3);
  expect(refused(() => admitTotal([0.75, 0.75], 0.75, 1.5))).toContain("exceeds the $1.5 total");
  expect(refused(() => admitTotal([1.2], 0.31, 1.5))).toContain("$1.2000 of paid runs");
});

test("a token that grants more than one capped session is refused", () => {
  acceptGrant({ maxSessions: 1, maxSessionSeconds: 50 });
  expect(refused(() => acceptGrant({ maxSessions: 2, maxSessionSeconds: 50 }))).toContain("grants");
  expect(refused(() => acceptGrant({ maxSessions: 1, maxSessionSeconds: 51 }))).toContain("grants");
});

test("an unknown outcome or an unconfirmed termination stops the check", () => {
  expect(stopFor({ outcomes: ["replied", "unknown"], terminationConfirmed: true })).toContain(
    "unknown",
  );
  expect(stopFor({ outcomes: ["replied"], terminationConfirmed: false })).toContain("unconfirmed");
  expect(stopFor({ outcomes: ["replied"], terminationConfirmed: true })).toBeUndefined();
});

test("live video is BGRA, lit and changing", () => {
  const seen = { frames: 24, formats: ["BGRA"], lit: 24, distinct: 24 };
  expect(liveVideo(seen)).toBeUndefined();
  expect(liveVideo({ ...seen, frames: 1 })).toContain("fewer than two");
  expect(liveVideo({ ...seen, lit: 0 })).toContain("black");
  expect(liveVideo({ ...seen, distinct: 1 })).toContain("never changed");
  expect(liveVideo({ ...seen, formats: ["BGRA", "RGBA"] })).toContain("not BGRA");
});

test("the relay check runs only while no earlier paid run selected a relay pair", () => {
  admitRelayCheck([["srflx", "host"]]);
  expect(
    refused(() =>
      admitRelayCheck([
        ["host", "host"],
        ["relay", "srflx"],
      ]),
    ),
  ).toContain("already selected a relay pair");
});

test("the runner refuses without an explicit authorization, before any request", () => {
  const script = fileURLToPath(new URL("../hosted/qualify.ts", import.meta.url));
  const result = spawnSync(process.execPath, [script, "vertical"], {
    encoding: "utf8",
    env: { PATH: process.env.PATH ?? "" },
    timeout: 60_000,
  });
  expect(result.status).toBe(2);
  expect(result.stderr).toContain("hosted-qualification-refused");
});
