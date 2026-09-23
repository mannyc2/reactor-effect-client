# Compiled examples

These examples import only the supported package paths of `reactor-effect-client`, `reactor-effect-browser` and `reactor-effect-native`. They are a private workspace member that depends on those packages with `workspace:*`, so they compile against the real built declarations rather than a tutorial copy; the pack smoke compiles the same sources again inside isolated installations of the packed archives.

`portable/session.mts` demonstrates explicit allocation, connection, readiness and cleanup evidence through the canonical Client. The Node and browser session examples provide its host dependencies. `portable/h3.mts` consumes an already connected session and returns accepted clip evidence, without adding playback policy. `portable/renewal.mts` accepts an explicit physical-source opener and shows prepared submission and cleanup through orchestration.

`portable/simulation.mts` runs the same production orchestration contract without coordinator access or native libraries. It demonstrates joining one committed submission and joining the same cleanup report. `portable/web-crypto.mts` supplies the explicit Crypto dependency using Web Crypto.

## Local checks

```sh
bun run build
bun run check:examples
```

The checker emits real JavaScript into `examples/dist/<profile>` for both host profiles. Node compilation excludes DOM types; browser compilation excludes Node types. Only the offline simulation is executed, under both Node and Bun, by `check/simulation-smoke.mjs`. No example command implicitly runs a paid session. The host session and H3 functions must be deliberately run by an application with its own credentials.

The local configs follow the workspace's `skipLibCheck` setting. Final `test:pack` qualification is stronger: the same sources are copied into the clean installed Node, browser and native consumers, compiled with `skipLibCheck: false`, and checked for resolutions outside each consumer. The existing, narrowly checked Effect rc.115 `TextDecoderOptions` no-DOM declaration exception is retained; no SDK or example diagnostic is suppressed. The installed offline example runs again under both runtimes.

The Node host example requires `@effect/platform-node@4.0.0-rc.115`, which is a dependency of the application, not of the SDK packages. An npm application using that prerelease should also retain the workspace's root override `"@effect/platform-node-shared": "4.0.0-rc.115"`. The platform's caret range otherwise permits later prereleases with a different Effect peer. The native package fixture applies this same override at its own root and checks every installed Effect/platform version; a package's override does not propagate into a parent application's npm installation. Browser use requires a secure context with WebRTC and Web Crypto. Applications own source selection, credentials, paid-session authorization and media consumption.
