> ## Documentation Index
> Fetch the complete documentation index at: https://docs.reactor.inc/llms.txt
> Use this file to discover all available pages before exploring further.

> ## Agent Instructions
> To build and serve your own model, start at /deploy/development/quickstart and /deploy/development/overview. Deploying is the default path: reactor init scaffolds a workspace, reactor auth login authenticates, and reactor model deploy registers the model, publishes the release with the weights/ folder, and activates it on Reactor's GPUs, in one command from that workspace. Docker must be running, because the publish step builds the image locally. Bump model.version in reactor.yaml before redeploying a change, because a release that already has an image is reactivated as it is. Deployment access is granted per account, so contact team@reactor.inc if a deploy is refused. Every key in reactor.yaml is documented at /deploy/platform/reactor-yaml. Model code imports reactor_runtime; Python client code imports reactor_sdk. The runtime overview explains the model interface. Running the model on your own machine with reactor run is optional and needs a GPU you attach with --gpus; /deploy/development/local-testing covers that loop and pairs a complete brightness model with a Python client test in a separate brightness-test workspace.
> Reactor hosts multiple models, each with its own connect slug (modelName) and command/event schema. The catalog of every model — slug, typed SDK package, and links to its schema — is at /model-api-reference/overview. Some models expose one slug per experience (e.g. HappyOyster); always take the slug from the model's own pages, never guess it.
> Fastest path to a working app: `npx create-reactor-app my-app --model=<slug>` scaffolds a complete app with secure auth wired up. Typed TypeScript SDKs are published as @reactor-models/<model>; Python uses the base reactor-sdk package.
> Auth: exchange an API key (rk_...) for a JWT via POST https://api.reactor.inc/tokens from your server. Never put the API key in client-side code.
> Append .md to any docs URL for clean Markdown. Search these docs via the MCP server at https://docs.reactor.inc/mcp.

# H3 Reference Turbo Realtime schema

> Tracks, commands, replies, and events for H3 Reference Turbo Realtime.

Use these commands to queue clips from a prompt and its references, control playback, and read
session state.

## Tracks

| Track        | Direction | Type  |
| ------------ | --------- | ----- |
| `main_video` | Output    | Video |
| `main_audio` | Output    | Audio |

## Commands

Commands with a named reply return that correlated message. `play` and `stop` declare acceptance
without a response payload; their events report playback changes. `command_error` reports a refusal
without changing queues or defaults. `valid_commands` reflects session state, but arguments still
need validation.

| Command                 | Description                                                                                                             | Reply or event                |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `enqueue`               | Queue a clip with its own prompt, up to nine optional reference images, and up to three optional reference audio clips. | `clip_queued`                 |
| `move`                  | Reorder a clip within its current queue.                                                                                | `clip_moved`                  |
| `pop`                   | Remove a queued clip.                                                                                                   | `clip_popped`                 |
| `play`                  | Play a ready clip.                                                                                                      | Accepted; emits clip\_started |
| `stop`                  | Cut playback, leaving queues intact.                                                                                    | Accepted; emits clip\_stopped |
| `set_seed`              | Set the advancing default seed for future clips.                                                                        | `seed_accepted`               |
| `set_clip_seconds`      | Set the default duration for future clips.                                                                              | `clip_length_accepted`        |
| `set_canvas`            | Set the canvas while queues and playback are empty.                                                                     | `canvas_accepted`             |
| `set_autoplay`          | Choose automatic or manual playback of ready clips.                                                                     | `autoplay_accepted`           |
| `set_flush_on_clip_end` | Choose black or a held last frame at boundaries.                                                                        | `flush_accepted`              |
| `get_queue`             | Read both queues.                                                                                                       | `queue_update`                |
| `get_state`             | Read defaults, capacities, progress, and permitted commands.                                                            | `state_update`                |
| `reset`                 | Clear queues, playback, tracks, and restore defaults.                                                                   | `session_reset`               |

### `enqueue`

Supply a nonempty prompt. References are optional, so a prompt on its own generates a clip from the
text. For images, supply either `reference_image` or `reference_images`, not both. A clip can also
carry audio references, as either `reference_audio` or `reference_audios`, again not both. A clip
that has audio needs at least one image, or a `continue_from_clip_id` whose clip the session still
holds. Each clip retains its own prompt and ordered references. References guide appearance rather
than fixing the first or last frame. A clip carries at most twelve references in total, counting its
images, its audio, and any continuation. Empty text, an invalid image, audio without an image or a
continuation, too many references, invalid parameters, and a full generation queue produce
`command_error`.

| Parameter               | Type                           | Default | Description                                                                                                                                                                                                               |
| ----------------------- | ------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `prompt`                | string                         | `""`    | What the clip should show and sound like, stored unchanged and never truncated. Empty text is refused. No character limit; a prompt past the model's text budget of about 2,000 tokens fails the clip with `clip_failed`. |
| `reference_image`       | Upload reference or null       | `null`  | One optional JPEG, PNG, or WebP reference. Use this or `reference_images`. Omit both for a clip from the prompt alone.                                                                                                    |
| `reference_images`      | Upload reference array or null | `null`  | Ordered list of up to 9 JPEG, PNG, or WebP references. Omitted, null, or empty means no image. `Picture 1` names the first, `Picture 2` the second, and so on.                                                            |
| `reference_audio`       | Upload reference or null       | `null`  | One optional audio reference, 2–15 seconds, whose voice or soundscape conditions the generated soundtrack. Use this or `reference_audios`.                                                                                |
| `reference_audios`      | Upload reference array or null | `null`  | Ordered list of up to 3 optional audio references, each 2–15 seconds. `Audio 1` names the first, `Audio 2` the second, and so on.                                                                                         |
| `seconds`               | number or null                 | `null`  | Request 5–15.084 seconds; null uses the session default. Rounded to frames, then aligned upward to a supported length. Actual output ranges from 5.167 to 15.083 seconds.                                                 |
| `seed`                  | integer or null                | `null`  | Nonnegative. Omitted or null uses and advances the default seed. An explicit seed leaves the default unchanged.                                                                                                           |
| `position`              | integer or null                | `null`  | Nonnegative generation queue position. Zero is next; omitted or past the end appends. The running build is unaffected.                                                                                                    |
| `metadata`              | string                         | `""`    | Opaque client string, up to 2,000 characters, echoed unchanged with the clip.                                                                                                                                             |
| `continue_from_clip_id` | string                         | `""`    | UUID of a generated clip to continue from. Blank creates an independent clip. Unknown or dropped IDs fall back to an independent clip. When the clip carries audio and no image, such an ID is refused instead.           |

Each audio reference is a WAV, MP3, AAC/M4A, OGG/Opus, FLAC, or WebM upload, at most 25 MiB, mono
or stereo, and 2–15 seconds long. The model conditions the generated soundtrack on it, so name each
one in the prompt and say what it is for. See the
[prompt guide](/model-api-reference/h3-reference-to-video-turbo-realtime/prompt-guide#refer-to-your-audio)
for the two roles an audio reference can take. Audio outside these bounds is refused with `command_error`
before the clip is queued.

To continue a scene, set `continue_from_clip_id` to a clip that has finished generating. Motion,
camera, and audio carry across the boundary while the new clip's own references guide its
appearance. A continued clip uses one of its three audio references for the previous clip's
soundtrack, so it accepts at most two of your own. Its budget of nine images is unchanged.

Replies `clip_queued` and broadcasts `queue_update` and `state_update`. Completion emits
`clip_generated` when the clip enters the playout queue.

### `move`

| Parameter  | Type    | Default | Description                                                                         |
| ---------- | ------- | ------- | ----------------------------------------------------------------------------------- |
| `clip_id`  | string  | `""`    | UUID of a queued clip. Missing or unknown IDs are refused.                          |
| `position` | integer | `0`     | Nonnegative position within its current queue. Zero is front; past the end appends. |

A running build is unaffected. Replies `clip_moved` with the resulting queue and position and emits
`queue_update`. Invalid positions produce `command_error`.

### `pop`

| Parameter | Type   | Default | Description                                                                       |
| --------- | ------ | ------- | --------------------------------------------------------------------------------- |
| `clip_id` | string | `""`    | UUID of the clip to remove from either queue. Missing or unknown IDs are refused. |

Replies `clip_popped` and emits `queue_update` and `state_update`. A running build finishes but its
result is discarded. Use `stop` for a playing clip.

### `play`

| Parameter | Type   | Default | Description                                                         |
| --------- | ------ | ------- | ------------------------------------------------------------------- |
| `clip_id` | string | `""`    | UUID of a ready clip. Blank selects the front of the playout queue. |

Consumes the ready clip and emits `clip_started`, `queue_update`, and `state_update`. Refuses when
already playing or when no matching ready clip exists.

### `stop`

Send `{}` to cut playback. Emits `clip_stopped` and `state_update`; queues remain intact. Under
autoplay, this skips to the next clip. Refuses when nothing is playing. A stopped clip cannot
resume.

### `set_seed`

| Parameter | Type    | Default | Description                                                             |
| --------- | ------- | ------- | ----------------------------------------------------------------------- |
| `seed`    | integer | `1000`  | Nonnegative default for future enqueues. Queued clips keep their seeds. |

Replies `seed_accepted` and emits `state_update`. Explicit per-clip seeds do not advance this
default. Invalid seeds produce `command_error`.

### `set_clip_seconds`

| Parameter | Type   | Default | Description                                                                           |
| --------- | ------ | ------- | ------------------------------------------------------------------------------------- |
| `seconds` | number | `15`    | Request 5–15.084 seconds, rounded to frames and aligned upward to a supported length. |

Replies `clip_length_accepted` with the actual `clip_seconds` and `frames`, and emits
`state_update`. Only future enqueues use the new default; queued clips keep their lengths. Invalid
lengths produce `command_error`.

### `set_canvas`

| Parameter | Type   | Default  | Description                                                                         |
| --------- | ------ | -------- | ----------------------------------------------------------------------------------- |
| `aspect`  | string | `"16:9"` | `16:9` (1344 × 768), `1:1` (768 × 768), `9:16` (768 × 1344), or `4:3` (1024 × 768). |

Both queues and playback must be empty. Replies `canvas_accepted` and emits `state_update`.
Unsupported aspects and nonempty sessions produce `command_error`.

### `set_autoplay`

| Parameter | Type    | Default | Description                                                                    |
| --------- | ------- | ------- | ------------------------------------------------------------------------------ |
| `enabled` | boolean | `false` | True starts ready clips whenever idle; false waits for `play` after each clip. |

Empty queues wait; autoplay does not generate prompts. Replies `autoplay_accepted` and emits
`state_update`.

### `set_flush_on_clip_end`

| Parameter | Type    | Default | Description                                                                                         |
| --------- | ------- | ------- | --------------------------------------------------------------------------------------------------- |
| `enabled` | boolean | `true`  | True flushes to black; false holds the last frame and avoids flushing between ready autoplay clips. |

With flushing off, the playback clock carries across ready autoplay clips. This does not guarantee
visual continuity. Reset always clears tracks. Replies `flush_accepted` and emits `state_update`.

### `get_queue`

Send `{}` at any time. Replies `queue_update` with the complete generation and playout queues.
`history` is always empty: a played clip is not listed and cannot be replayed. To continue from a
generated clip, pass its ID as `continue_from_clip_id` on the next `enqueue`.

### `get_state`

Send `{}` at any time. Replies `state_update` with defaults, capacities, playback progress, and
`valid_commands`.

### `reset`

Send `{}` to drop both queues, cut playback, restore defaults, and clear tracks. Replies
`session_reset`; emits `queue_update`, `state_update`, and `clip_stopped` if playing. In-flight
results are discarded.

## Messages

| Message                | Description                                                    | Delivery               | Payload                            |
| ---------------------- | -------------------------------------------------------------- | ---------------------- | ---------------------------------- |
| `clip_queued`          | Clip accepted by `enqueue`, with `ready: false`.               | Command reply          | `clip`                             |
| `clip_moved`           | Clip repositioned within its queue.                            | Command reply          | `clip`, `queue`, `position`        |
| `clip_popped`          | Queued clip removed.                                           | Command reply          | `clip`                             |
| `clip_generated`       | Built clip entered the playout queue.                          | Event                  | `clip`                             |
| `clip_failed`          | Generation failed; the clip is removed and the queue moves on. | Event                  | `clip`, `reason`                   |
| `clip_started`         | Clip started sending to output tracks.                         | Event                  | `clip`                             |
| `clip_finished`        | All frames and synchronized audio sent.                        | Event                  | `clip`, `seconds_sent`             |
| `clip_stopped`         | Stop or reset cut a clip; it cannot resume.                    | Event                  | `clip`, `seconds_sent`             |
| `queue_update`         | Full queues on connect, queue changes, or `get_queue`.         | Event or command reply | `generation`, `playout`, `history` |
| `state_update`         | State on connect, state changes, or `get_state`.               | Event or command reply | See state table below.             |
| `command_error`        | Command refused without changing queues or defaults.           | Error                  | `command`, `reason`                |
| `seed_accepted`        | Default seed accepted.                                         | Command reply          | `seed`                             |
| `clip_length_accepted` | Default duration accepted.                                     | Command reply          | `clip_seconds`, `frames`           |
| `canvas_accepted`      | Canvas accepted.                                               | Command reply          | `aspect`, `width`, `height`        |
| `autoplay_accepted`    | Autoplay setting accepted.                                     | Command reply          | `enabled`                          |
| `flush_accepted`       | Boundary-flush setting accepted.                               | Command reply          | `enabled`                          |
| `session_reset`        | Queues and defaults reset.                                     | Command reply          | `cleared_clips`, `was_playing`     |

All listed payload fields are required. `queue` is a string identifying `generation` or `playout`;
`position` is the resulting integer index, with zero at the front. `reason` and `command` are
strings. `seconds_sent` is a number measuring the session total, not the duration of one clip.

`seed`, `frames`, `width`, `height`, and `cleared_clips` are integers; `clip_seconds` is a number,
`aspect` is a string, and `enabled` and `was_playing` are booleans. `cleared_clips` counts clips
removed from both queues, excluding the playing clip. `was_playing` includes an armed or playing
clip.

### Clip object

The same clip object appears in clip messages and queue arrays.

| Field                   | Type    | Required | Description                                         |
| ----------------------- | ------- | -------- | --------------------------------------------------- |
| `clip_id`               | string  | Yes      | Clip UUID.                                          |
| `prompt`                | string  | Yes      | Prompt stored unchanged.                            |
| `metadata`              | string  | Yes      | Client string echoed unchanged.                     |
| `frames`                | integer | Yes      | Effective frame count.                              |
| `seconds`               | number  | Yes      | Effective duration.                                 |
| `seed`                  | integer | Yes      | Seed assigned to the clip.                          |
| `ready`                 | boolean | Yes      | False when accepted; built clips are ready to play. |
| `has_reference_image`   | boolean | No       | Whether the clip has a reference image.             |
| `reference_image_count` | integer | No       | Number of reference images.                         |
| `has_reference_audio`   | boolean | No       | Whether the clip has an audio reference.            |
| `reference_audio_count` | integer | No       | Number of audio references.                         |

### `queue_update`

All three fields are required arrays of clip objects:

| Field        | Description                                                       |
| ------------ | ----------------------------------------------------------------- |
| `generation` | Waiting and in-flight builds, front first.                        |
| `playout`    | Built clips ready to play, front first.                           |
| `history`    | Always empty. A played clip is not listed and cannot be replayed. |

### `state_update`

All fields are required, including the nullable `playing_clip_id`.

| Field                 | Type           | Description                                                                                         |
| --------------------- | -------------- | --------------------------------------------------------------------------------------------------- |
| `clip_seconds`        | number         | Actual default duration for enqueues without their own `seconds`.                                   |
| `clip_seconds_min`    | number         | Minimum accepted requested duration.                                                                |
| `clip_seconds_max`    | number         | Maximum accepted requested duration.                                                                |
| `seed`                | integer        | Next automatically assigned seed.                                                                   |
| `autoplay`            | boolean        | Ready clips start automatically when idle.                                                          |
| `flush_on_clip_end`   | boolean        | True flushes to black; false holds the last frame and avoids flushing between ready autoplay clips. |
| `aspect`              | string         | Session aspect ratio.                                                                               |
| `width`               | integer        | Output width in pixels.                                                                             |
| `height`              | integer        | Output height in pixels.                                                                            |
| `playing`             | boolean        | A clip is armed or playing.                                                                         |
| `playing_clip_id`     | string or null | Playing clip UUID, or null when idle.                                                               |
| `generation_queued`   | integer        | Generation queue size, including the running build.                                                 |
| `generation_capacity` | integer        | Maximum generation queue size.                                                                      |
| `playout_queued`      | integer        | Ready clips waiting to play.                                                                        |
| `playout_capacity`    | integer        | Maximum ready queue size. Building pauses while full.                                               |
| `clips_played`        | integer        | Clips finished or stopped during the session.                                                       |
| `seconds_sent`        | number         | Seconds sent on the tracks during the session.                                                      |
| `valid_commands`      | string array   | Commands permitted by queue and playback state; arguments still need validation.                    |

### Upload reference

Image and audio parameters take references returned by the Reactor upload protocol. All fields are
required.

| Field       | Type        |
| ----------- | ----------- |
| `upload_id` | UUID string |
| `name`      | string      |
| `mime_type` | string      |
| `size`      | integer     |
