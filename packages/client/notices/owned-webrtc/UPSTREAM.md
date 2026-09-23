# Attribution and dependency boundaries

Production code under `src/` is newly authored for this task from the protocol specifications, informed by inspection of the following pinned sources. No Werift or str0m implementation is imported, bundled or invoked by that code.

| Reference | Pin | Use and obligation |
| --- | --- | --- |
| Effect | `4.0.0-rc.115`; source `4a05d4914fa2327a42bd75fe77c22c188becf3b4` | The sole production package dependency; MIT notice retained. Socket/Scope/Queue/Clock/Worker patterns studied, not vendored adapters. |
| Werift | `2174d88ece8ecdc6e804e9303028496833f1fb2e` | ICE/DTLS/SCTP/RTP/SRTP source research. Unmodified focused SCTP/common/SRTP reference files are used ONLY under `oracles/werift`, with MIT notice and per-source SHA-256 manifest. Not in the production tarball. |
| str0m | `1183e271b40a34b387a29cfad10696aae0c8dcad` | `lib.rs` / `examples/http-post.rs` input/output/deadline architecture studied. No Rust code, compiled library, or WASM adopted. Supplied MIT notice retained. |
| IETF | RFCs in DESIGN.md | Wire algorithms are implemented, not replaced by library calls. RFC 5769 and RFC 3711 vector notices are retained. |

The source archive includes offline development tooling: TypeScript 5.9.3, the real Effect package, official `@types/node` 25.1.0 and its `undici-types` declarations. These declarations are not runtime implementations or shims. Their original package manifests/licenses are retained. The copied Node declarations are deliberately **not** evidence of Bun functionality.

The independent oracle additionally uses Python/pyOpenSSL/cryptography/audioop and a Node child running the unmodified Werift SCTP/SRTP modules. The development copy of `debug` is 4.4.3 (available offline), rather than Werift's manifest pin 4.4.0; it affects logging and is disclosed rather than represented as a fully reproduced Werift install. Its dependency `ms` and both original licenses are retained under `oracles/werift/node_modules`. OpenSSL and Python native codecs are test tools only. None participates in the standalone package's production execution.

No entire external WebRTC stack is hidden in the artifact. Package installation files are explicitly limited by `package.json`; the production archive contains only `dist/src`, package metadata, this README/license material, and notices. Run `scripts/lint.mjs` and `scripts/clean-consumer.mjs` to inspect that distinction.

## VP8 in revision 0.0.3

See [VP8-ADAPTATION.md](VP8-ADAPTATION.md) for the focused RFC 6386 decoder algorithms and normative tables adapted into owned TypeScript, the tagged WebM prediction-clamp cross-check, and their complete retained BSD licenses/patent grants. No additional production package or engine is added.
