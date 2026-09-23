# Security policy

## Reporting a vulnerability

Please report security vulnerabilities privately through the repository's private security-advisory reporting channel when it is available. Include the affected version/commit, impact, reproduction details, and any suggested mitigation.

Do not place credentials, tokens, private session material, proof-of-concept secrets, or exploitable details in a public issue. If private security reporting is not enabled for the repository, use an existing private maintainer contact path rather than publishing the report.

There is no declared long-term-support branch policy yet. Security fixes target the current maintained branch and the latest published package version, if one exists.

## Security boundaries

The SDK deliberately keeps several boundaries explicit:

- Reactor credentials are supplied through Effect/Redacted values and must not be logged.
- The root, browser, H3, orchestration, simulation, testing, and wire entry points do not implicitly load the native FFI backend. Host preflight is explicit through the native entry point.
- Native WebRTC runs behind a narrow C ABI. libwebrtc callbacks are copied into bounded queues and shutdown waits for callback quiescence before native ownership is released.
- Native control/data and media queues are bounded. Overflow is a typed failure or an explicit drop policy rather than unbounded retention.
- Remote command errors distinguish `not-submitted`, `unknown`, and `replied` outcomes. A local cancellation after submission is not treated as proof that the remote mutation did not happen.
- One canonical session owns the allocation or attachment. Its close report retains local cleanup failures and unconfirmed remote termination; closing an attachment does not acquire authority over the remote allocation.
- H3 provider facts and acceptance evidence remain separate from orchestration's local annotations and playback/renewal policy. Acknowledgement alone cannot invent a model state transition.
- Package/release validation installs the three npm tarballs into isolated consumers so undeclared workspace dependencies cannot be hidden by a parent `node_modules` tree.

## Supply chain

GitHub Actions in this repository are pinned to immutable commit SHAs. Native `reactor-webrtc` is pinned to a Git revision; its libwebrtc prebuilt is selected by that revision and checksum-verified by its build script. The Linux native build also pins the Rust builder image by digest. Staged native libraries carry an embedded ABI/source/build identity and a SHA-256 sidecar. Pack validation checks the library bytes and native source identity before exercising the installed public native factory.

The npm release workflow is manual-only and never builds the SDK. It publishes only the three archives (`reactor-effect-client`, `reactor-effect-browser` and `reactor-effect-native`, one shared version) that a successful main CI run qualified after both native artifacts were assembled, in two separate runs: preparation signs Sigstore provenance for each archive and retains an immutable ts-release candidate, and publication promotes that exact candidate only after an explicit per-package confirmation. Both runs use GitHub's OIDC identity, preparation for Sigstore signing and publication for the npm trusted-publisher exchange configured for `release.yml` with no GitHub environment; there is no protected environment or approval gate, so branch protection and account access controls remain the maintainers' responsibility. Long-lived npm write tokens are not used and should not be introduced. See [release-tools/README.md](./release-tools/README.md).

## Validation limits

Passing local tests does not prove the security or behavior of hosted Reactor infrastructure, TURN relays, browser implementations, or third-party networks. Provider and cross-runtime integration evidence should be tracked separately from local SDK tests.
