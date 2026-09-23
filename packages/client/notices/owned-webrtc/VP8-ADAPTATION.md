# Owned VP8 adaptation and provenance

The scalar decoder in `src/vp8*.ts` is TypeScript code owned and executed by this package. It is **not a binding, whole-engine bundle, native/WASM translation or subprocess**. It reads ordinary VP8 samples, arithmetic-decodes syntax/tokens, reconstructs reference pictures and returns freshly allocated I420 planes. It never reads fixtures or expected outputs. `vp8-tables.ts` contains normative probability, tree and quantization constants, not pictures.

## RFC 6386 code components

The decoding guide and Attachment One source were supplied unchanged in `codec-reference-inputs.zip`. The source ZIP retains the entire RFC text under `references/codecs/rfc6386.txt`; see its embedded original notices. Focused algorithms/tables were ported from:

| Reference subsystem | TypeScript adaptation |
| --- | --- |
| `bool_decoder.h`, `idct_add.c` | `vp8-core.ts`: bool arithmetic decoding, inverse DCT and Walsh-Hadamard |
| `modemv.c`, `modemv_data.h` | `vp8-modes.ts`, `vp8-tables.ts`: mode probabilities, motion predictors, split vectors and sign bias |
| `tokens.c`, `dequant_data.h`, `vp8_prob_data.h` | `vp8-tokens.ts`, `vp8-tables.ts`: coefficient contexts, trees, dequantization |
| `predict.c` | `vp8-intra.ts`, `vp8-predict.ts`: intra prediction, subpixel motion compensation, edge extension |
| `dixie_loopfilter.c` | `vp8-filter.ts`: simple/normal filtering and segment/reference/mode deltas |
| `dixie.c` and decoding chapters | `vp8.ts`: headers, partitions, segmentation, entropy persistence, reference refresh/copies and output crop |

The original Google copyright, BSD license, patent grant and authors from Attachment One are retained as `VP8-LICENSE.txt`, `VP8-PATENTS.txt`, `VP8-AUTHORS.txt`. The adaptation replaces C pointers and storage with bounded owned typed arrays, adds bounds/work budgets and explicit invalidation/close. No upstream build system, platform abstraction, native wrapper or codec dependency is imported.

## Prediction-only motion clamping cross-check

`predictionMotion` and its call placement were checked/adapted against WebM's **libvpx v1.12.0** `vp8/common/reconinter.c`, specifically the luma/chroma unrestricted-motion-vector boundary helpers and prediction copies. Decoded MV context must not be overwritten when limiting a temporary prediction vector. The footprint uses the stated 19/18-pixel limits, followed by chroma rounding, not a blanket change to stored NEWMV values.

Source: https://github.com/webmproject/libvpx/blob/v1.12.0/vp8/common/reconinter.c

This focused helper adaptation retains `libvpx-v1.12.0-LICENSE.txt` and `libvpx-v1.12.0-PATENTS.txt`. Attribution: Copyright (c) 2010, The WebM Project authors. Contributors are credited in the upstream AUTHORS file: https://github.com/webmproject/libvpx/blob/v1.12.0/AUTHORS . These license/grant texts were transcribed from the official tagged files returned by the web reader; a container raw download failed DNS, so no byte-for-byte source-download claim is made. `references/codecs/libvpx-v1.12.0/provenance.json` retains those download failures.

## Protocol and test material

RFC 7741 is retained unchanged under `references/codecs/rfc7741.txt`; `video.ts` implements the specified VP8 payload descriptor and owned frame reconstruction. Existing protocol notices still apply. The BSD terms are shipped with both source and binary package; the package license is `MIT AND BSD-3-Clause`.

Original deterministic synthetic fixture inputs/expected outputs are **development-only**. Supplied fixtures retain their generation commands/offsets/hashes. `scripts/generate-vp8-oracles.py` creates additional mathematical sources and uses local FFmpeg/libvpx **only as independent development oracles**; neither tool nor generated YUV is reachable from the production module graph or included in the production tarball. Bundled Effect, TypeScript and oracle reference licenses remain separate. This notice is not an assertion of comprehensive patent clearance or full codec conformance.
