/** Reproduce the baseline inventory without requiring the removed legacy files. */
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { relative } from "node:path";
import { fileURLToPath } from "node:url";
import coverage from "./coverage-map.mjs";

// The formatter is a workspace root tool; this script runs from packages/client.
const oxfmt = fileURLToPath(new URL("../../../../node_modules/.bin/oxfmt", import.meta.url));

const baseline = "fb6271d5bace2e4e9dc2d2054ad274b56c8cf3b6";
const files = ["Adapter", "Renewing", "Engine", "References"];
const inventory = [];
// Tokenize identifiers and delimiters while excluding comments and string bodies.
// This inventory needs locations, not compiler evaluation or runtime test execution.
const tokenize = (text) => {
  const tokens = [];
  for (let index = 0; index < text.length;) {
    const start = index,
      char = text[index];
    if (/\s/.test(char)) {
      index++;
      continue;
    }
    if (text.startsWith("//", index)) {
      index = text.indexOf("\n", index);
      if (index < 0) break;
      continue;
    }
    if (text.startsWith("/*", index)) {
      const end = text.indexOf("*/", index + 2);
      index = end < 0 ? text.length : end + 2;
      continue;
    }
    if (["'", '"', "`"].includes(char)) {
      index++;
      while (index < text.length && text[index] !== char) index += text[index] === "\\" ? 2 : 1;
      index++;
    } else if (/[A-Za-z_$]/.test(char)) {
      while (index < text.length && /[A-Za-z0-9_$]/.test(text[index])) index++;
    } else index++;
    tokens.push({ text: text.slice(start, index), start, end: index });
  }
  return tokens;
};
const matching = (tokens, start) => {
  const opens = { "(": ")", "[": "]", "{": "}", "<": ">" };
  const stack = [opens[tokens[start].text]];
  for (let index = start + 1; index < tokens.length; index++) {
    const value = tokens[index].text;
    // Angle brackets only occur in expect's optional generic prefix here.
    if (["(", "[", "{"].includes(value)) stack.push(opens[value]);
    else if (value === stack.at(-1) && stack.pop() && stack.length === 0) return index;
  }
  throw new Error(`Unterminated inventory expression at ${tokens[start].start}`);
};
for (const name of files) {
  const path = `test/h3/${name}.test.ts`;
  const content = execFileSync("git", ["show", `${baseline}:${path}`], { encoding: "utf8" });
  const tokens = tokenize(content);
  const tests = [];
  const line = (offset) => content.slice(0, offset).split("\n").length;
  for (let index = 0; index < tokens.length; index++) {
    if (tokens[index].text !== "test" || tokens[index - 1]?.text === ".") continue;
    let open = index + 1,
      parameters;
    if (
      tokens[open]?.text === "." &&
      tokens[open + 1]?.text === "each" &&
      tokens[open + 2]?.text === "("
    ) {
      const parametersEnd = matching(tokens, open + 2);
      parameters = content.slice(tokens[open + 2].end, tokens[parametersEnd].start);
      open = parametersEnd + 1;
    }
    if (tokens[open]?.text !== "(") continue;
    const last = matching(tokens, open),
      assertions = [];
    for (let cursor = open + 1; cursor < last; cursor++) {
      if (tokens[cursor].text !== "expect") continue;
      let open = cursor + 1;
      if (tokens[open]?.text === "<") open = matching(tokens, open) + 1;
      if (tokens[open]?.text !== "(") continue;
      let end = matching(tokens, open);
      while (tokens[end + 1]?.text === ".") {
        end += 2;
        if (tokens[end + 1]?.text === "(") end = matching(tokens, end + 1);
      }
      assertions.push({
        line: line(tokens[cursor].start),
        endLine: line(tokens[end].end),
        expression: content.slice(tokens[cursor].start, tokens[end].end),
      });
    }
    const startLine = line(tokens[index].start);
    const mapping = coverage[name]?.[startLine];
    if (mapping === undefined)
      throw new Error(`Missing migration coverage for ${path}:${startLine}`);
    const checks = mapping.checks.map((check) => {
      const target = readFileSync(check.path, "utf8");
      const offset = target.indexOf(check.match);
      if (offset < 0)
        throw new Error(`Replacement assertion target is missing: ${check.path} :: ${check.match}`);
      return { ...check, line: target.slice(0, offset).split("\n").length };
    });
    tests.push({
      line: startLine,
      endLine: line(tokens[last].end),
      title: tokens[open + 1].text.slice(1, -1),
      ...(parameters === undefined ? {} : { parameters }),
      coverage: { contract: mapping.contract, checks },
      assertions,
    });
  }
  const expectedSites = tokens
    .filter(
      (token, index) => token.text === "expect" && ["(", "<"].includes(tokens[index + 1]?.text),
    )
    .map((token) => line(token.start));
  const accounted = tests.flatMap((entry) => entry.assertions.map((assertion) => assertion.line));
  if (JSON.stringify(expectedSites) !== JSON.stringify(accounted))
    throw new Error(`Unaccounted or duplicated expect call in ${path}`);
  if (Object.keys(coverage[name]).length !== tests.length)
    throw new Error(`Orphan migration entries in ${path}`);
  inventory.push({
    path,
    sha256: createHash("sha256").update(content).digest("hex"),
    declarations: tests.length,
    assertionSites: accounted.length,
    tests,
  });
}
const declarations = inventory.reduce((sum, file) => sum + file.declarations, 0);
const assertionSites = inventory.reduce((sum, file) => sum + file.assertionSites, 0);
if (declarations !== 57 || assertionSites !== 234)
  throw new Error("The frozen legacy inventory changed unexpectedly");
writeFileSync(
  "test/orchestration/legacy-assertions.json",
  `${JSON.stringify(
    {
      baseline,
      counting:
        "234 static expect call sites in 57 declarations. test.each parameters are retained explicitly; loop assertions remain quantified by the original frozen test body. Every assertion inherits its test's complete coverage entry.",
      files: inventory,
    },
    null,
    2,
  )}\n`,
);
const rows = inventory.flatMap((file) =>
  file.tests.map((test) => {
    const checks = test.coverage.checks
      .map(
        (check) =>
          `[${relative("test/", check.path)}:${check.line}](./${relative("test/orchestration", check.path)}#L${check.line}) — ${check.match}`,
      )
      .join("<br>");
    const original = `${file.path}:${test.line} — ${test.title}${test.parameters === undefined ? "" : `; parameters ${test.parameters}`}`;
    return `| ${original.replaceAll("|", "\\|")} | ${test.assertions.length} | ${checks.replaceAll("|", "\\|")} | ${test.coverage.contract.replaceAll("|", "\\|")} |`;
  }),
);
const markdown = `# Orchestration test migration ledger

Baseline: \`${baseline}\`. The four removed suites contain **57 test declarations and 234 static assertion sites**. The two-case boundary-frame declaration is recorded with both parameter values. Assertions inside loops remain quantified by the frozen source; these counts do not claim to be runtime assertion counts.

[legacy-assertions.json](./legacy-assertions.json) retains every original assertion expression and line range, each source file's SHA-256, and the coverage entry inherited by that assertion. \`node test/orchestration/coverage-ledger.mjs\` reconstructs it from Git, checks for missing or duplicate assertion sites, verifies every replacement target exists, and regenerates this table. The original files were unchanged from the baseline when inventoried.

The replacement assertions require canonical \`CommandFailure.context.outcome\`; local policy refusals additionally expose \`PolicyFailure.reason\`. Unknown commitment must never become retryable because of an error-code guess. Caller cancellation before commit can retry the same inert request; committed success/rejection/unknown must retain its one physical result through renewal and close. The focused test result, not this inventory, establishes whether the implementation meets these contracts.

The principal replacements are explicit, not removed checks: unsupported FastH3 boundary inputs fail before IO; prompt-only input is valid; real H3 never invents a build start or missed lifecycle event; named reply evidence is distinct from later queue facts; autoplay/hold/reset belong to explicit orchestration options; attached cleanup preserves the canonical lease; staging files do not exist; media handoff reports frame counts and unverified audio, while incomplete/drop/unknown evidence forces explicit replacement at expiry.

\`FakeTransport.ts\` contained the removed mixed-owner wire simulator, not independent assertions. Provider tests retain their canonical Session fixture unchanged; orchestration tests use \`SourceFixture.ts\`; production simulation tests exercise \`src/simulation\` directly. Existing \`Provider.test.ts\` and \`ProviderReferences.test.ts\` remain unchanged.

| Original test declaration | Assertion sites | Replacement checks | Contract disposition |
| --- | ---: | --- | --- |
${rows.join("\n")}
`;
writeFileSync("test/orchestration/coverage-ledger.md", markdown);
execFileSync(oxfmt, ["--write", "test/orchestration/coverage-ledger.md"], {
  encoding: "utf8",
});
console.log(
  JSON.stringify(
    {
      baseline,
      declarations,
      assertionSites,
      files: inventory.map(({ path, declarations, assertionSites }) => ({
        path,
        declarations,
        assertionSites,
      })),
    },
    null,
    2,
  ),
);
