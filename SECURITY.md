# Security policy

## Reporting a vulnerability

Please report security vulnerabilities privately through the repository's private security-advisory reporting channel when it is available. Include the affected version/commit, impact, reproduction details, and any suggested mitigation.

Do not place credentials, tokens, private session material, proof-of-concept secrets, or exploitable details in a public issue. If private security reporting is not enabled for the repository, use an existing private maintainer contact path rather than publishing the report.

There is no declared long-term-support branch policy yet. Security fixes target the current maintained branch and the latest published package version, if one exists.

## Security boundaries

The SDK deliberately keeps several boundaries explicit:

- Reactor credentials are supplied through Effect/Redacted values and must not be logged.
- The portable root, coordinator helpers and H3 adapter do not implicitly load the native FFI backend.
- Native WebRTC runs behind a narrow C ABI. libwebrtc callbacks are copied into bounded queues and shutdown waits for callback quiescence before native ownership is released.
- Native control/data and media queues are bounded. Overflow is a typed failure or an explicit drop policy rather than unbounded retention.
- Remote command errors distinguish `not-submitted`, `unknown`, and `replied` outcomes. A local cancellation after submission is not treated as proof that the remote mutation did not happen.
- Package/release validation installs the npm tarball into isolated consumers so undeclared workspace dependencies cannot be hidden by a parent `node_modules` tree.

## Supply chain

GitHub Actions in this repository are pinned to immutable commit SHAs. Native `reactor-webrtc` is pinned to a Git revision; its libwebrtc prebuilt is selected by that revision and checksum-verified by its build script. The Linux native build also pins the Rust builder image by digest.

The npm release workflow is manual-only, validates the exact version tag and repository identity, and publishes only the tarball that passed the isolated package smoke after both native artifacts were assembled. Its publish job references the protected GitHub Environment `npm` and uses OIDC. Maintainers must configure that environment with required reviewers/prevent-self-review and configure npm Trusted Publishing for `release.yml` plus environment `npm` before enabling a release. Long-lived npm write tokens should not be introduced when OIDC trusted publishing is available.

## Validation limits

Passing local tests does not prove the security or behavior of hosted Reactor infrastructure, TURN relays, browser implementations, or third-party networks. Provider and cross-runtime integration evidence should be tracked separately from local SDK tests.
