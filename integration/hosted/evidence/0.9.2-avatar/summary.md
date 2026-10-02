| Run      | Check  | Mode | Verdict    | Started                  | Worst case | Estimated |
| -------- | ------ | ---- | ---------- | ------------------------ | ---------- | --------- |
| 527bf9ae | avatar | paid | unfinished | 2026-10-01T15:05:11.238Z | $0.840     | $1.155    |
| 3ae28dd8 | avatar | paid | pass       | 2026-10-02T12:07:01.545Z | $1.260     | $0.406    |

### avatar: unfinished (paid, run 527bf9ae, 2026-10-01T15:05:11.238Z)

- **Environment:** reactor-effect-client 0.9.2, reactor-effect-native 0.9.2, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit becfca52
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.840, estimated $1.155 at 70 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.10 s · allocated 0.46 s · connected 2.71 s · contract read 3.03 s · avatar ready 31.24 s · live 35.53 s · reconnecting for the picture 40.54 s · call ended 72.64 s · live 76.75 s · reconnecting for the picture 81.92 s · second call failed 164.08 s · closed 164.69 s · settled 164.80 s
- **Pair:** prflx
- **Photo:** jpeg, 321196 bytes
- **Contract:** title vidu-s2-avatar, version (text, 6 chars); declares clone_voice, end_call, interrupt, list_voices, set_reference_images, say, create_avatar, attach_avatar, get_state, start_call, update_call, clear_reference_images; clone_voice declared
- **First snapshot:** A+2.29 s, phase idle; set phase, voice, persona_set, warmup_attempts, control_ready, video_receiving, audio_receiving, mic_forwarding, camera_forwarding, reference_images; null avatar_id, avatar_status, avatar_name, call_mode, call_started_at, call_max_seconds, call_elapsed_seconds, last_frame_age_ms, end_reason, last_error; left out none; undocumented none
- **get_state:** message session_state in 0.06 s
- **list_voices:** message voices in 0.23 s; 56 system voice(s): Tina, Cindy, Sunnybobi, Raymond, Ethan, Serena, Harvey, Maia, Evan, Qiao, Momo, Wil, Angel, Mia, Joyner, Gold, Katerina, Ryan, Jennifer, Aiden, Mione, Sunny, Dylan, Eric, Peter, Marcus, Li, Kiki, Rocky, Sohee, Lenn, Sonrisa, Bodega, Emilien, Andre, Alek, Rizky, Roya, Arda, Hana, Dolce, Jakub, Griet, Marina, Siiri, Ingrid, Sigga, Bea, Chloe; cloned present, default_voice present
- **Avatar:** upload submitted in 1.04 s; create_avatar ack in 2.23 s; preparing_avatar +0.35 s, avatar_ready +26.71 s; avatar_status ready; an avatar_id of 19 characters
- **Call 1:** start_call ack in 0.07 s; starting +0.07 s, warming_up +2.42 s, live +4.29 s (live at A+35.08 s); at live warmup_attempts 1, call_max_seconds 7200, control_ready true, video_receiving false, audio_receiving false, mic_forwarding false
- **Call 1, picture and sound:** first frame never, first block 0.00 s after live; no frame within 5 s, so one reconnect at A+40.08 s, ready in 2.12 s; no frame within 5 s of its ready; 0 frames of none (0 lit, 0 distinct, 0 lost); 502 blocks at 48000 Hz, 1 channel(s), peak RMS 0.2102; speech A+39.15 s–40.15 s
- **Greeting:** sound 4.07 s after live; the first character transcript 2.61 s after live; 1 character transcript(s), final true
- **Call 1, say:** ack in 0.04 s; the user's transcript 0.24 s after the send; the answer's sound never came
- **Interrupt:** ack in 0.03 s, sent with no answer heard; silent 0.09 s after it; the cut answer's character transcript came 5.95 s before it, final true, 41 characters
- **Voice change:** update_call message call_updated in 0.24 s to Cindy; applied voice; the state's voice changed
- **Call 1, end:** end_call no answer by its deadline (outcome unknown) in 10.00 s; end_reason unread, duration_seconds unread; ending +0.03 s; ended never reported
- **Attach:** attach_avatar ack in 1.40 s; ended +1.18 s, preparing_avatar +1.40 s, avatar_ready +1.40 s
- **Call 2:** start_call ack in 0.03 s; starting +0.03 s, warming_up +1.42 s, live +2.71 s (live at A+76.30 s); at live warmup_attempts 1, call_max_seconds 7200, control_ready true, video_receiving false, audio_receiving false, mic_forwarding false
- **Call 2, picture and sound:** first frame never, first block never; no frame within 5 s, so one reconnect at A+81.46 s; no frame within 5 s of its ready; 0 frames of none (0 lit, 0 distinct, 0 lost); 0 blocks at none Hz, none channel(s), peak RMS 0; speech none
- **Last state:** get_state failed with InvalidState (outcome not-submitted) in 0.00 s, phase unread
- **Phases, with the frames and blocks that arrived in each:** idle A+2.29 s (0, 105) › preparing_avatar A+4.43 s (0, 2390) › avatar_ready A+30.78 s (0, 7) › starting A+30.85 s (0, 235) › warming_up A+33.21 s (0, 187) › live A+35.08 s (0, 502) › ending A+60.21 s (0, 0) › ended A+73.37 s (0, 0) › preparing_avatar A+73.58 s (0, 0) › avatar_ready A+73.58 s (0, 0) › starting A+73.61 s (0, 0) › warming_up A+75.00 s (0, 0) › live A+76.30 s (0, 0)
- **Transcripts:** 4: 1 user, 3 character; 4 final, 0 not final, 0 that did not say
- **Messages:** session_state 75, voices 1, command_error 3, transcript 4, call_updated 1, call_ended 1; 120 session events
- **Termination:** 9fa8a820-c89d-4952-9ef9-3a75623e8b4b confirmed (trail CLOSED)
- **Criteria:** ✗ second call · ✓ the session's first snapshot arrived · ✓ the avatar became ready · ✓ the first call went live · ✗ character video arrived during the first call · ✓ character audio arrived during the first call · ✗ end_call was answered with call_ended · ✗ the second call went live with video · ✓ confirmed termination

**Refused commands** (each watched until its command_error and next session_state came, or 2 s after its answer):

| Probe                            | On the wire                 | Answered in | command_error                                       | Before the answer | trace_id | Next state's last_error | Changed after it |
| -------------------------------- | --------------------------- | ----------- | --------------------------------------------------- | ----------------- | -------- | ----------------------- | ---------------- |
| clone_voice with an invalid name | ack                         | 0.11 s      | CLONING_DISABLED (state, clone_voice, retryable)    | yes               | none     | CLONING_DISABLED        | –                |
| say before any call              | ack                         | 0.04 s      | NOT_LIVE (state, say, retryable)                    | yes               | none     | NOT_LIVE                | –                |
| update_call with no field        | ack                         | 0.03 s      | INVALID_INPUT (request, update_call, not retryable) | yes               | none     | INVALID_INPUT           | –                |
| say with no text                 | error frame invalid_command | 0.03 s      | none                                                | –                 | –        | INVALID_INPUT           | –                |
| update_call with a null voice    | error frame invalid_command | 0.03 s      | none                                                | –                 | –        | INVALID_INPUT           | none             |

**What this run answers:**

- A refused command, on the wire: clone_voice with an invalid name, ack; say before any call, ack; update_call with no field, ack; say with no text, error frame invalid_command; update_call with a null voice, error frame invalid_command.
- command_error against its command's answer: clone_voice with an invalid name, before it; say before any call, before it; update_call with no field, before it.
- A command carrying an explicit null: error frame invalid_command; nothing read otherwise after it.
- Answer times, against the SDK's 10 s default: create_avatar 2.23 s; start_call 0.07 s and 0.03 s; end_call 10.00 s; clone_voice 0.11 s; attach_avatar 1.40 s.
- The picture after live: call 1 had no frame within 5 s of live; none came within 5 s of the one reconnect either; call 2 had no frame within 5 s of live; none came within 5 s of the one reconnect either.
- The call's limit at live: call 1 call_max_seconds 7200, call 2 call_max_seconds 7200.
- The picture's size: none.
- Voice cloning: clone_voice declared, answered ack; voices.cloned present.
- Partial transcripts: 0 of 4 not final.
- The deployment's version: (text, 6 chars).

### avatar: pass (paid, run 3ae28dd8, 2026-10-02T12:07:01.545Z)

- **Environment:** reactor-effect-client 0.9.2, reactor-effect-native 0.9.2, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit b7ce932a
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $1.260, estimated $0.406 at 70 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.10 s · allocated 0.43 s · connected 2.91 s · contract read 3.56 s · avatar ready 6.70 s · live 10.78 s · call ended 36.28 s · live 41.02 s · call ended 57.41 s · closed 57.87 s · avatar observed 57.87 s · settled 58.02 s
- **Pair:** prflx
- **Photo:** jpeg, 321196 bytes
- **Contract:** title vidu-s2-avatar, version (text, 6 chars); declares set_reference_images, start_call, interrupt, clone_voice, update_call, say, create_avatar, end_call, get_state, clear_reference_images, list_voices, attach_avatar; clone_voice declared
- **First snapshot:** A+2.51 s, phase idle; set phase, voice, persona_set, warmup_attempts, control_ready, video_receiving, audio_receiving, mic_forwarding, camera_forwarding, reference_images; null avatar_id, avatar_status, avatar_name, call_mode, call_started_at, call_max_seconds, call_elapsed_seconds, last_frame_age_ms, end_reason, last_error; left out none; undocumented none
- **get_state:** message session_state in 0.03 s
- **list_voices:** message voices in 0.49 s; 56 system voice(s): Tina, Cindy, Sunnybobi, Raymond, Ethan, Serena, Harvey, Maia, Evan, Qiao, Momo, Wil, Angel, Mia, Joyner, Gold, Katerina, Ryan, Jennifer, Aiden, Mione, Sunny, Dylan, Eric, Peter, Marcus, Li, Kiki, Rocky, Sohee, Lenn, Sonrisa, Bodega, Emilien, Andre, Alek, Rizky, Roya, Arda, Hana, Dolce, Jakub, Griet, Marina, Siiri, Ingrid, Sigga, Bea, Chloe; cloned present, default_voice present
- **Avatar:** upload submitted in 0.42 s; create_avatar ack in 2.13 s; preparing_avatar +0.29 s, avatar_ready +2.14 s; avatar_status ready; an avatar_id of 19 characters
- **Call 1:** start_call ack in 0.03 s; starting +0.03 s, warming_up +1.46 s, live +4.08 s (live at A+10.35 s); at live warmup_attempts 1, call_max_seconds 7200, control_ready true, video_receiving false, audio_receiving false, mic_forwarding false
- **Call 1, picture and sound:** first frame 0.20 s after live, first block 0.00 s after live; both tracks resumed at A+10.35 s: the first frame 0.20 s after, 13 blocks; 408 frames of 640x360, 928x1088 at 24.9 fps (408 lit, 355 distinct, 0 lost); 1013 blocks at 48000 Hz, 1 channel(s), peak RMS 0.3111; speech A+14.77 s–16.27 s, A+19.97 s–20.47 s, A+21.17 s–21.87 s, A+22.87 s–23.37 s
- **Greeting:** sound 4.42 s after live; the first character transcript 2.52 s after live; 1 character transcript(s), final true
- **Call 1, say:** ack in 0.03 s; the user's transcript 0.23 s after the send; the answer's sound 3.12 s after the send
- **Interrupt:** ack in 0.03 s, sent 1.50 s into the answer's sound; silent 0.40 s after it; the cut answer's character transcript came 2.00 s before it, final true, 46 characters
- **Voice change:** update_call message call_updated in 0.24 s to Cindy; applied voice; the state's voice changed
- **Call 1, end:** end_call message call_ended in 6.94 s; end_reason ended_by_client, duration_seconds 16; ending +0.04 s, ended +6.94 s; 9 frames and 100 blocks in the 2.00 s after ended
- **Attach:** attach_avatar ack in 0.25 s; preparing_avatar +0.25 s, avatar_ready +0.25 s
- **Call 2:** start_call ack in 0.03 s; starting +0.03 s, warming_up +1.41 s, live +4.48 s (live at A+40.59 s); at live warmup_attempts 2, call_max_seconds 7200, control_ready true, video_receiving false, audio_receiving false, mic_forwarding false
- **Call 2, picture and sound:** first frame 0.11 s after live, first block 0.00 s after live; nothing done at A+40.59 s: the first frame 0.11 s after, 13 blocks; 173 frames of 640x360, 928x1088 at 24.9 fps (173 lit, 120 distinct, 0 lost); 498 blocks at 48000 Hz, 1 channel(s), peak RMS 0.3321; speech A+44.27 s–47.17 s
- **Call 2, say:** ack in 0.03 s; the user's transcript 0.25 s after the send; the answer's sound 3.43 s after the send
- **Call 2, end:** end_call message call_ended in 7.36 s; end_reason ended_by_client, duration_seconds 7; ending +0.03 s, ended +7.36 s; 10 frames and 100 blocks in the 2.00 s after ended
- **Last state:** get_state message session_state in 0.03 s, phase ended
- **Phases, with the frames and blocks that arrived in each:** idle A+2.51 s (9, 107) › preparing_avatar A+4.42 s (9, 92) › avatar_ready A+6.26 s (0, 2) › starting A+6.29 s (0, 71) › warming_up A+7.72 s (0, 131) › live A+10.35 s (409, 1015) › ending A+26.94 s (7, 346) › ended A+33.85 s (11, 112) › preparing_avatar A+36.10 s (0, 0) › avatar_ready A+36.10 s (0, 1) › starting A+36.13 s (1, 69) › warming_up A+37.51 s (0, 154) › live A+40.59 s (173, 500) › ending A+47.65 s (5, 366) › ended A+54.98 s (10, 102)
- **Transcripts:** 5: 2 user, 3 character; 5 final, 0 not final, 0 that did not say
- **Messages:** session_state 55, voices 1, command_error 3, transcript 5, call_updated 1, call_ended 2; 90 session events
- **Termination:** 18c870a2-de68-417e-b362-2d711a7ed4da confirmed (trail CLOSED)
- **Criteria:** ✓ the session's first snapshot arrived · ✓ the avatar became ready · ✓ the first call went live · ✓ character video arrived during the first call · ✓ character audio arrived during the first call · ✓ end_call was answered with call_ended · ✓ the second call went live with video · ✓ confirmed termination

**Refused commands** (each watched until its command_error and next session_state came, or 2 s after its answer):

| Probe                            | On the wire                 | Answered in | command_error                                       | Before the answer | trace_id | Next state's last_error | Changed after it |
| -------------------------------- | --------------------------- | ----------- | --------------------------------------------------- | ----------------- | -------- | ----------------------- | ---------------- |
| clone_voice with an invalid name | ack                         | 0.03 s      | CLONING_DISABLED (state, clone_voice, retryable)    | yes               | none     | CLONING_DISABLED        | –                |
| say before any call              | ack                         | 0.03 s      | NOT_LIVE (state, say, retryable)                    | yes               | none     | NOT_LIVE                | –                |
| update_call with no field        | ack                         | 0.03 s      | INVALID_INPUT (request, update_call, not retryable) | yes               | none     | INVALID_INPUT           | –                |
| say with no text                 | error frame invalid_command | 0.03 s      | none                                                | –                 | –        | INVALID_INPUT           | –                |
| update_call with a null voice    | error frame invalid_command | 0.03 s      | none                                                | –                 | –        | INVALID_INPUT           | none             |

**What this run answers:**

- A refused command, on the wire: clone_voice with an invalid name, ack; say before any call, ack; update_call with no field, ack; say with no text, error frame invalid_command; update_call with a null voice, error frame invalid_command.
- command_error against its command's answer: clone_voice with an invalid name, before it; say before any call, before it; update_call with no field, before it.
- A command carrying an explicit null: error frame invalid_command; nothing read otherwise after it.
- Answer times, against the SDK's 10 s default: create_avatar 2.13 s; start_call 0.03 s and 0.03 s; end_call 6.94 s and 7.36 s; clone_voice 0.03 s; attach_avatar 0.25 s.
- The picture after live: call 1's picture came 0.20 s after both tracks resumed; call 2's picture came 0.11 s after nothing done.
- The call's limit at live: call 1 call_max_seconds 7200, call 2 call_max_seconds 7200.
- The picture's size: 640x360, 928x1088.
- Voice cloning: clone_voice declared, answered ack; voices.cloned present.
- Partial transcripts: 0 of 5 not final.
- The deployment's version: (text, 6 chars).

## The commits

Both runs ran on branch `claude/vidu`, on Effect 4.0.0-rc.117. The branch was then rebased onto
the move to Effect 4.0.0, so the trees differ from the ones the runs name: Effect 4.0.0's import
paths, and its queue and race changes in the client and the native host's TypeScript. The native
source is unchanged, and the addon both runs loaded is the one the later `character` runs loaded.
The commits the runs name stay reachable from the tag `archive/vidu-pre-effect-4`, and each is on
the branch as:

| The evidence names | On the branch | Runs                |
| ------------------ | ------------- | ------------------- |
| becfca52           | 7071adf       | 527bf9ae (`avatar`) |
| b7ce932a           | 5a0d092       | 3ae28dd8 (`avatar`) |
