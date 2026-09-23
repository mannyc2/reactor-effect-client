# reactor-webrtc and Google WebRTC

The native transport depends on `reactor-webrtc` and `reactor-webrtc-sys` at
revision `bebf63e42624ee440e066b692494dd3229d29ae7`.

`reactor-webrtc` and `reactor-webrtc-sys` are licensed under Apache-2.0.

Their native prebuilt is derived from Google's WebRTC project:

- License: BSD-3-Clause
- Additional IP terms: WebRTC Software License / patent grant
- Upstream: https://webrtc.googlesource.com/src
- Pinned WebRTC commit: `a5ddff6086d96fd2356ad6c521525ea2803f7988`
- Reactor prebuilt tag: `webrtc-7907-a5ddff60-p9`

The exact third-party components, versions, source references and license
identifiers for each retained prebuilt are recorded in the CycloneDX SBOMs
under `notices/reactor-webrtc/`. That directory also retains the WebRTC
`LICENSE` and `PATENTS` files from the pinned upstream commit.

The prebuilt archives do not themselves contain the component license/notice
files enumerated by those SBOMs. Exact redistribution texts for all 31 unique
components in the retained macOS arm64 and Linux x64 SBOMs are therefore kept
under `notices/reactor-webrtc/third-party/`. Each component directory contains
the pinned `README.chromium` licensing metadata and every file named by its
`License File:` field. `notices/reactor-webrtc/third-party/README.md` maps each
directory to the pinned Chromium `src/third_party` revision or the exact WebRTC
`DEPS` source revision used to fetch that text. The SBOM license identifiers are
inventory metadata and are not treated as a substitute for these texts.

The native build downloads the prebuilt selected by the pinned reactor-webrtc
revision.
