# Working in reactor-effect

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the relevant package README before changing code. Check the working tree first and preserve unrelated work.

For Effect code, use the version installed for this checkout. Bun's isolated linker puts its guide at `packages/client/node_modules/effect/AGENTS.md`, not at the workspace root. Read that guide completely, then consult its `ai-docs`, `dist/*.d.ts`, and source for every version-sensitive API. If dependencies are absent, install the locked workspace dependencies first. Check the package pin again when switching branches.

For a service or architecture refactor, use the repository's `effect-development` architecture audit. For a proposed test or fixture, apply the `testing` skill's selection rules before adding one. Keep the result proportionate to the observed behavior; a new service, Schema, or test needs a real boundary or failure to own.

For scheduling work, follow [the scheduling workflow](CONTRIBUTING.md#scheduling-work). Establish queue order, time and capacity semantics, cancellation, renewal, and uncertain outcomes before implementation. The SDK owns generic provider and queue mechanics; applications own editorial priority and proof of presented output.

Use the repository's validation commands in CONTRIBUTING, including the final relevant `verify` profile, and report checks that could not run.
