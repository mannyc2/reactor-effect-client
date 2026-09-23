/** The hosted qualification's gates refuse offline, before any network or credential. */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import {
  Refused,
  acceptGrant,
  admit,
  authorize,
  liveVideo,
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
const authorized = ["--i-authorize-paid-sessions", "--budget-usd=0.50", "--evidence=out.json"];

test("paid use needs a check, the authorization flag, a bounded budget and a new evidence path", () => {
  expect(authorize(["vertical", ...authorized])).toEqual({
    check: "vertical",
    budgetUsd: 0.5,
    evidence: "out.json",
  });
  expect(refused(() => authorize(authorized))).toContain("name the check");
  expect(refused(() => authorize(["vertical", "--budget-usd=0.5", "--evidence=x"]))).toContain(
    "--i-authorize-paid-sessions",
  );
  for (const budget of ["0", "0.51", "NaN", "Infinity", "-1"])
    expect(
      refused(() =>
        authorize([
          "turn",
          "--i-authorize-paid-sessions",
          `--budget-usd=${budget}`,
          "--evidence=x",
        ]),
      ),
    ).toContain("--budget-usd");
  expect(
    refused(() => authorize(["takeover", "--i-authorize-paid-sessions", "--budget-usd=0.2"])),
  ).toContain("--evidence");
  expect(refused(() => authorize(["vertical", ...authorized, "--budget-usd=0.1"]))).toContain(
    "more than once",
  );
});

test("the published rate must fit the whole capped session in the budget", () => {
  const rate = { creditsPerSecond: 10, creditsPerDollar: 2000 };
  expect(worstCaseUsd(rate)).toBeCloseTo(0.3);
  expect(admit(rate, 0.5)).toBeCloseTo(0.3);
  expect(refused(() => admit(rate, 0.25))).toContain("over the $0.25 budget");
});

test("a token that grants more than one capped session is refused", () => {
  acceptGrant({ maxSessions: 1, maxSessionSeconds: 60 });
  expect(refused(() => acceptGrant({ maxSessions: 2, maxSessionSeconds: 60 }))).toContain("grants");
  expect(refused(() => acceptGrant({ maxSessions: 1, maxSessionSeconds: 61 }))).toContain("grants");
});

test("an unknown outcome or an unconfirmed termination stops the check", () => {
  expect(stopFor({ outcomes: ["replied", "unknown"], terminationConfirmed: true })).toContain(
    "unknown",
  );
  expect(stopFor({ outcomes: ["replied"], terminationConfirmed: false })).toContain("unconfirmed");
  expect(stopFor({ outcomes: ["replied"], terminationConfirmed: true })).toBeUndefined();
});

test("live video is BGRA, not black and changing", () => {
  const frame = (fill: number, format = "BGRA") => ({
    format,
    data: new Uint8Array(16).fill(fill),
  });
  expect(liveVideo([frame(1), frame(2)])).toBeUndefined();
  expect(liveVideo([frame(1)])).toContain("fewer than two");
  expect(liveVideo([frame(0), frame(0)])).toContain("black");
  expect(liveVideo([frame(3), frame(3)])).toContain("never changed");
  expect(liveVideo([frame(1), frame(2, "RGBA")])).toContain("not BGRA");
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
