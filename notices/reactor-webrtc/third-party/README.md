# libwebrtc third-party redistribution texts

These files correspond to the shipped third-party components enumerated by the
retained macOS arm64 and Linux x64 CycloneDX SBOMs for Reactor's
`webrtc-7907-a5ddff60-p9` prebuilt.

The WebRTC source commit is
`a5ddff6086d96fd2356ad6c521525ea2803f7988`. Its `DEPS` pins Chromium's
`src/third_party` metadata repository at
`badac9a00faad01312d758916a208554e9fbd5b7` and separately pins source
checkouts used below. `README.chromium` is retained beside every license file so
its component name, declared license, `License File` path, and source metadata
remain reviewable.

For components whose declared license file is owned by the pinned Chromium
`src/third_party` repository, the source below is `chromium-third-party`. For a
license file inside a separately checked-out dependency, the exact repository
and WebRTC `DEPS` revision are named explicitly.

| Component directory | License text source | Retained declared files |
| --- | --- | --- |
| `abseil-cpp` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `boringssl` | boringssl `606d3a344dae49672ea567f6719e1fc092a4ae6a` | `README.chromium`, `LICENSE` |
| `compiler-rt` | compiler-rt `242e3a3fe72644e47bfd432ed03bcd5b38360309` | `README.chromium`, `LICENSE.TXT` |
| `cpuinfo` | cpuinfo `ea6b9f1bb6e1001d8b21574d5bc78ddef62e499d` | `README.chromium`, `LICENSE` |
| `dav1d` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `farmhash` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `fft2d` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `flatbuffers` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `fp16` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `fxdiv` | FXdiv `63058eff77e11aa15bf531df5dd34395ec3017c8` | `README.chromium`, `LICENSE` |
| `gemmlowp` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `grpc` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `jsoncpp` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `libaom` | AOM `91f00106883401aef4e052c7fec2163d5f71d44e` | `README.chromium`, `LICENSE`, `PATENTS` |
| `libgav1` | libgav1 `66ac17620652635392f6ab24065c77b035e281c9` | `README.chromium`, `LICENSE` |
| `libjpeg_turbo` | Chromium libjpeg-turbo wrapper `640f254ad0fa03f6b1f29f89b7dd9366f2f6e533` | `README.chromium`, `LICENSE.md.chromium` |
| `libsrtp` | Chromium libsrtp wrapper `cd5d177bf1fde755ddb4c7f0d9ff7693f8b49e5e` | `README.chromium`, `LICENSE` |
| `libvpx` | libvpx `572f663c893499db9e7f69bb2fec821eae6b1c40` | `README.chromium`, `LICENSE`, `PATENTS` |
| `libyuv` | libyuv `d23308a2a7442be8e559b1b471862fd7588d6a57` | `README.chromium`, `LICENSE` |
| `neon_2_sse` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `opus` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `COPYING` |
| `perfetto` | Perfetto `c0ded1eb0575360eb130f72bf78c079ee15debbe` | `README.chromium`, `LICENSE` |
| `pffft` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `protobuf` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `pthreadpool` | pthreadpool `02460584c6092e527c8b89f7df4de143d70e801f` | `README.chromium`, `LICENSE` |
| `re2` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `rnnoise` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `COPYING` |
| `ruy` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `tflite` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |
| `xnnpack` | XNNPACK `4b7c368f5e48d9292c5834593c2f83e66b55ce83` | `README.chromium`, `LICENSE` |
| `zlib` | chromium-third-party `badac9a00faad01312d758916a208554e9fbd5b7` | `README.chromium`, `LICENSE` |

`compiler-rt` appears only in the retained Linux x64 SBOM; the other 30
components are present in the retained macOS arm64 SBOM. The SBOMs remain the
artifact-specific inventory. This directory supplies the corresponding declared
license/copyright and patent texts; the SBOM license identifiers alone are not
treated as redistribution text.
