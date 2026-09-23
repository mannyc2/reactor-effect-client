# Native Rust dependency notices

The packaged native shared library is built from the dependency versions pinned
by `native/Cargo.lock`. The linked Rust dependency closure below was verified
from `cargo tree --locked --edges normal` for the current native package.

| Package | Version | License used for this distribution | Upstream |
| --- | --- | --- | --- |
| reactor-webrtc | 0.18.0 | Apache-2.0 | https://github.com/reactor-team/reactor-webrtc |
| reactor-webrtc-sys | 0.18.0 | Apache-2.0 | https://github.com/reactor-team/reactor-webrtc |
| prost | 0.13.5 | Apache-2.0 | https://github.com/tokio-rs/prost |
| bytes | 1.12.1 | MIT | https://github.com/tokio-rs/bytes |
| serde | 1.0.229 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/serde-rs/serde |
| serde_core | 1.0.229 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/serde-rs/serde |
| serde_json | 1.0.151 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/serde-rs/json |
| itoa | 1.0.18 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/dtolnay/itoa |
| memchr | 2.8.3 | MIT option of Unlicense OR MIT | https://github.com/BurntSushi/memchr |
| zmij | 1.0.23 | MIT | https://github.com/dtolnay/zmij |

The exact upstream Apache or MIT license file used for each non-Reactor package
is retained in this directory. `serde_core` shares the retained Serde Apache
license. The exact reactor-webrtc Apache license is retained separately as
`notices/reactor-webrtc-LICENSE`.

Build-time-only procedural macro and compiler helper crates are not linked into
the distributed native shared library and are therefore not listed as runtime
components here.
