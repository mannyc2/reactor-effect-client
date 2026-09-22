# Wire fixtures

The six files under proto/reactor_wire/v1 are copied from the canonical runtime pin in the root README. Their supplied licenses/notices are under ../notices. wire-descriptor.pb was produced by protoc 3.13.0 using these files and Google’s Struct descriptor; it is not a recovered client engine.

`python3 scripts/oracle.py generate` uses Google protobuf (development only) to generate semantic vectors. `bun run test` decodes them and writes TypeScript re-encodings. `python3 scripts/oracle.py verify` cross-decodes those re-encodings with Google’s implementation and checks message/unknown-field equality. This is semantic conformance testing, not a claim that every protobuf serialization has a unique byte order.

The mock peer in `test/fixtures.ts` exercises policy only and intentionally uses non-real SDP. It does not establish WebRTC transport or media support.
