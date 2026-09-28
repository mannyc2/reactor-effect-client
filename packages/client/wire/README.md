# Wire sources

The six files under proto/reactor_wire/v1 are copied from the canonical runtime pin in the root README. Their supplied licenses and notices are under ../notices.

`bun run generate:wire` generates `src/internal/proto` from them with `buf generate` and `protoc-gen-es` (see `buf.yaml` and `buf.gen.yaml`); `bun run generate:check` fails if the committed code differs. `src/internal/wire.ts` holds the bounds received messages are decoded under.
