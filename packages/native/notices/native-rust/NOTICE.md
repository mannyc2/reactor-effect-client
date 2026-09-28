# Native Rust dependency notices

The packaged native addon is built from the dependency versions pinned by
`rust/Cargo.lock`. The linked Rust dependency closure below was verified from
`cargo tree --locked --edges normal,no-proc-macro` for both published targets,
`x86_64-unknown-linux-gnu` and `aarch64-apple-darwin`, which link the same set.

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
| napi | 3.13.0 | MIT | https://github.com/napi-rs/napi-rs |
| napi-sys | 3.3.2 | MIT | https://github.com/napi-rs/napi-rs |
| libloading | 0.9.0 | ISC | https://github.com/nagisa/rust_libloading |
| ctor | 1.0.13 | Apache-2.0 option of Apache-2.0 OR MIT | https://github.com/mmastrac/linktime |
| bitflags | 2.13.2 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/bitflags/bitflags |
| cfg-if | 1.0.5 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/rust-lang/cfg-if |
| libc | 0.2.189 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/rust-lang/libc |
| futures | 0.3.34 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/rust-lang/futures-rs |
| futures-channel, futures-core, futures-executor, futures-io, futures-sink, futures-task, futures-util | 0.3.34 | Apache-2.0 option of MIT OR Apache-2.0 | https://github.com/rust-lang/futures-rs |
| nohash-hasher | 0.2.0 | Apache-2.0 option of Apache-2.0 OR MIT | https://github.com/paritytech/nohash-hasher |
| pin-project-lite | 0.2.17 | Apache-2.0 option of Apache-2.0 OR MIT | https://github.com/taiki-e/pin-project-lite |
| rustc-hash | 2.1.3 | Apache-2.0 option of Apache-2.0 OR MIT | https://github.com/rust-lang/rustc-hash |
| slab | 0.4.12 | MIT | https://github.com/tokio-rs/slab |

The exact upstream Apache or MIT license file used for each non-Reactor package
is retained in this directory. `serde_core` shares the retained Serde Apache
license, and the `futures-*` crates share the retained futures Apache license.
The `napi` and `napi-sys` crates ship no license file; `napi-3.13.0-LICENSE`
is the MIT license of their repository, napi-rs/napi-rs, as `@napi-rs/cli`
3.10.5 ships it from that repository, and it covers both. The exact reactor-webrtc Apache license is retained separately as
`notices/reactor-webrtc-LICENSE`.

Build-time-only procedural macro and compiler helper crates, `napi-derive` and
`napi-build` among them, are not linked into the distributed native addon and are therefore not listed as runtime
components here.
