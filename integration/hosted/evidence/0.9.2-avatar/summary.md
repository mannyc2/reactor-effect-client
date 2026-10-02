| Run      | Check  | Mode | Verdict    | Started                  | Worst case | Estimated |
| -------- | ------ | ---- | ---------- | ------------------------ | ---------- | --------- |
| 527bf9ae | avatar | paid | unfinished | 2026-10-01T15:05:11.238Z | $0.840     | $1.155    |

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
