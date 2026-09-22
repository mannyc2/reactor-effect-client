# syntax=docker/dockerfile:1.7
FROM --platform=linux/amd64 rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS build

WORKDIR /work
COPY native/install-linux-toolchain.sh ./native/install-linux-toolchain.sh
RUN chmod 755 native/install-linux-toolchain.sh \
    && native/install-linux-toolchain.sh
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        file \
        git \
        tar \
        zstd \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add clippy rustfmt

COPY native/Cargo.toml native/Cargo.lock native/build.rs ./native/
COPY native/.cargo/config.toml ./native/.cargo/config.toml
COPY native/src ./native/src
COPY native/include ./native/include

ENV CARGO_TARGET_DIR=/target \
    CC=clang-21 \
    CXX=clang++-21

# reactor-webrtc-sys downloads the pinned Linux x64 libwebrtc prebuilt,
# verifies its published SHA-256, and compiles its C++ glue with pinned LLVM 21.
RUN cargo fmt --manifest-path native/Cargo.toml -- --check
RUN cargo test --locked --manifest-path native/Cargo.toml -- --nocapture
RUN cargo clippy --locked --manifest-path native/Cargo.toml --all-targets -- -D warnings
RUN cargo build --locked --manifest-path native/Cargo.toml --release
RUN test "$(uname -m)" = "x86_64" \
    && file /target/release/libreactor_effect_native.so \
    && ldd /target/release/libreactor_effect_native.so

FROM scratch AS artifact
COPY --from=build /target/release/libreactor_effect_native.so /libreactor_effect_native.so
