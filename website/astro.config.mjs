// @ts-check
import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";
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
        starlightLlmsTxt({
          projectName: "reactor-effect",
          description:
            "reactor-effect is an independent Effect (effect-ts v4) SDK for Reactor's real-time video models. Packages: reactor-effect-client (portable core: sessions, tokens, the H3 provider, Playout, ReactorTest), reactor-effect-browser (RTCPeerConnection host), reactor-effect-native (libwebrtc Node-API host with decoded frames).",
          details:
            "Every package pins Effect 4.0.0-rc.117 exactly. Import modules by subpath (reactor-effect-client/Playout) or as namespaces from the root. ReactorTest runs the same application offline on the Effect clock, with no API key.",
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
