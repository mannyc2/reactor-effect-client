> ## Documentation Index
> Fetch the complete documentation index at: https://docs.reactor.inc/llms.txt
> Use this file to discover all available pages before exploring further.

> ## Agent Instructions
> To build and serve your own model, start at /deploy/development/quickstart and /deploy/development/overview. Deploying is the default path: reactor init scaffolds a workspace, reactor auth login authenticates, and reactor model deploy registers the model, publishes the release with the weights/ folder, and activates it on Reactor's GPUs, in one command from that workspace. Docker must be running, because the publish step builds the image locally. Bump model.version in reactor.yaml before redeploying a change, because a release that already has an image is reactivated as it is. Deployment access is granted per account, so contact team@reactor.inc if a deploy is refused. Every key in reactor.yaml is documented at /deploy/platform/reactor-yaml. Model code imports reactor_runtime; Python client code imports reactor_sdk. The runtime overview explains the model interface. Running the model on your own machine with reactor run is optional and needs a GPU you attach with --gpus; /deploy/development/local-testing covers that loop and pairs a complete brightness model with a Python client test in a separate brightness-test workspace.
> Reactor hosts multiple models, each with its own connect slug (modelName) and command/event schema. The catalog of every model — slug, typed SDK package, and links to its schema — is at /model-api-reference/overview. Some models expose one slug per experience (e.g. HappyOyster); always take the slug from the model's own pages, never guess it.
> Fastest path to a working app: `npx create-reactor-app my-app --model=<slug>` scaffolds a complete app with secure auth wired up. Typed TypeScript SDKs are published as @reactor-models/<model>; Python uses the base reactor-sdk package.
> Auth: exchange an API key (rk_...) for a JWT via POST https://api.reactor.inc/tokens from your server. Never put the API key in client-side code.
> Append .md to any docs URL for clean Markdown. Search these docs via the MCP server at https://docs.reactor.inc/mcp.

# Changelog

> Release notes for the H3 Reference Turbo Realtime model API.

Changes to H3 Reference Turbo Realtime's commands, messages, tracks, and defaults, newest first. SDK and platform
releases are in [Release Notes](/changelog/overview).

<Update label="September 18, 2026" description="0.5.5">
  ### New

  * **Prompt-only clips.** Reference images are now optional. Omit [`reference_image` and `reference_images`](/model-api-reference/h3-reference-to-video-turbo-realtime/schema#enqueue), or send an empty list, and the clip comes from the prompt alone. A list still holds up to nine images.
  * **Audio on a continued clip.** A clip that carries audio no longer needs an image of its own. A `continue_from_clip_id` whose clip the session still holds stands in for one, because that clip's own soundtrack carries across the boundary.

  ### Changed

  * When a clip carries audio and no image, an unknown or dropped `continue_from_clip_id` is refused with `command_error`. Without audio, such an ID still falls back to an independent clip.
</Update>

<Update label="September 16, 2026" description="0.4.3">
  ### New

  * **Reference audio.** `enqueue` now takes audio references that condition the generated soundtrack: one [`reference_audio`](/model-api-reference/h3-reference-to-video-turbo-realtime/schema#enqueue), or an ordered `reference_audios` list of up to three. Each clip is 2–15 seconds, mono or stereo, in WAV, MP3, AAC/M4A, OGG/Opus, FLAC, or WebM. Audio is optional and must accompany at least one reference image, and a clip carries at most twelve references in total.
  * **Two roles for an audio reference.** Name each clip in the prompt as `Audio 1`, `Audio 2`, and so on, then say whether it is a character's voice or a track to reuse. A label alone does not tell the model what the audio is for. The [prompt guide](/model-api-reference/h3-reference-to-video-turbo-realtime/prompt-guide#refer-to-your-audio) has both patterns.
  * **Two clip fields.** Every clip message and queue entry now reports `has_reference_audio` and `reference_audio_count` beside the existing image fields.

  ### Changed

  * A clip queued with `continue_from_clip_id` uses one of its three audio references for the previous clip's soundtrack, so it accepts at most two of your own. Its budget of nine reference images is unchanged.
</Update>
