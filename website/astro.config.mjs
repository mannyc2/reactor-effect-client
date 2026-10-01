// @ts-check
import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";
import starlightLinksValidator from "starlight-links-validator";
import starlightLlmsTxt from "starlight-llms-txt";

const repository = "https://github.com/mannyc2/reactor-effect-client";

export default defineConfig({
  site: "https://mannyc2.github.io",
  base: "/reactor-effect-client",
  // The pages show the repository's examples by importing their sources.
  vite: { server: { fs: { allow: [".."] } } },
  integrations: [
    starlight({
      title: "reactor-effect",
      description:
        "An Effect SDK for Reactor's real-time video models: sessions that renew themselves, a playout for 24/7 channels, decoded media in Node and Bun, and Reactor simulated offline.",
      logo: { src: "./src/assets/logo.svg", replacesTitle: false },
      favicon: "/favicon.svg",
      social: [{ icon: "github", label: "GitHub", href: repository }],
      head: [
        {
          tag: "meta",
          attrs: {
            property: "og:image",
            content: "https://mannyc2.github.io/reactor-effect-client/og.png",
          },
        },
        { tag: "meta", attrs: { name: "twitter:card", content: "summary_large_image" } },
      ],
      editLink: { baseUrl: `${repository}/edit/main/website/` },
      lastUpdated: true,
      customCss: ["./src/styles/theme.css"],
      plugins: [
        // Fails the build on a broken internal link or anchor.
        starlightLinksValidator({
          exclude: ["/playground/", "/reactor-effect-client/playground/"],
        }),
        starlightLlmsTxt({
          projectName: "reactor-effect",
          description:
            "reactor-effect is an independent Effect (effect-ts v4) SDK for Reactor's real-time video models. Packages: reactor-effect-client (portable core: sessions, tokens, the H3 provider, Playout, ReactorTest), reactor-effect-browser (RTCPeerConnection host), reactor-effect-native (libwebrtc Node-API host with decoded frames).",
          details:
            'reactor-effect is an independent, Apache-2.0 project, not affiliated with Reactor; "official documentation" below means this site. Every package pins Effect 4.0.0-rc.117 exactly. Import modules by subpath (reactor-effect-client/Playout) or as namespaces from the root. ReactorTest runs the same application offline on the Effect clock, with no API key.\n\n## Pages\n\n- [Quickstart](https://mannyc2.github.io/reactor-effect-client/start/quickstart/): one H3 clip offline with no key, then live\n- [Installation](https://mannyc2.github.io/reactor-effect-client/start/installation/): packages per host, the exact Effect pin and the platform override\n- [Going live](https://mannyc2.github.io/reactor-effect-client/start/going-live/): API key, session tokens, caps and what a session costs\n- [Sessions and tokens](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/): Reactor, Session, CoordinatorClient, reconnects and close reports\n- [The H3 provider](https://mannyc2.github.io/reactor-effect-client/concepts/h3/): requests, references, the queue and each clip\'s operation facts\n- [Playout](https://mannyc2.github.io/reactor-effect-client/concepts/playout/): lanes, filler, windows, edits, placement and renewal across sessions\n- [Sources](https://mannyc2.github.io/reactor-effect-client/concepts/sources/): H3Source and LocalSource\n- [Media](https://mannyc2.github.io/reactor-effect-client/concepts/media/): decoded frames, PCM and browser tracks\n- [Errors and dispatch evidence](https://mannyc2.github.io/reactor-effect-client/concepts/errors/): tagged reasons and not-submitted / replied / unknown outcomes\n- [Test offline with ReactorTest](https://mannyc2.github.io/reactor-effect-client/guides/testing-offline/): the simulated Reactor on TestClock, with faults\n- [Run a 24/7 channel](https://mannyc2.github.io/reactor-effect-client/guides/channel/): one playout broadcast to many viewers\n- [Sessions in the browser](https://mannyc2.github.io/reactor-effect-client/guides/browser/): BrowserPeer with a token server\n- [Frames in Node and Bun](https://mannyc2.github.io/reactor-effect-client/guides/native/): the libwebrtc addon, in process or isolated\n- [Cost control](https://mannyc2.github.io/reactor-effect-client/guides/cost-control/): caps, confirmed termination and budgets\n- [Modules](https://mannyc2.github.io/reactor-effect-client/reference/modules/): every public module and its exports\n- [Hosted evidence](https://mannyc2.github.io/reactor-effect-client/reference/hosted-evidence/): every paid run behind the hosted claims',
          optionalLinks: [
            {
              label: "Reactor's own documentation",
              url: "https://docs.reactor.inc/llms.txt",
              description: "models, commands, pricing and limits",
            },
            {
              label: "Source and examples",
              url: "https://github.com/mannyc2/reactor-effect-client",
            },
          ],
        }),
      ],
      sidebar: [
        {
          label: "Start here",
          items: [
            { slug: "start/introduction" },
            { slug: "start/quickstart" },
            { slug: "start/installation" },
            { slug: "start/going-live" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { slug: "concepts/sessions" },
            { slug: "concepts/h3" },
            { slug: "concepts/playout" },
            { slug: "concepts/sources" },
            { slug: "concepts/media" },
            { slug: "concepts/errors" },
          ],
        },
        {
          label: "Guides",
          items: [
            { slug: "guides/testing-offline" },
            { slug: "guides/channel" },
            { slug: "guides/browser" },
            { slug: "guides/native" },
            { slug: "guides/cost-control" },
            { slug: "guides/plain-typescript" },
          ],
        },
        {
          label: "Examples",
          items: [
            { slug: "examples" },
            {
              label: "Playground: H3 Studio offline",
              link: "/playground/",
              attrs: { target: "_blank" },
            },
          ],
        },
        {
          label: "Reference",
          items: [
            { slug: "reference/modules" },
            { slug: "reference/tracing" },
            { slug: "reference/hosted-evidence" },
            { slug: "reference/limits" },
            { slug: "reference/agents" },
            { slug: "reference/faq" },
          ],
        },
      ],
    }),
  ],
});
