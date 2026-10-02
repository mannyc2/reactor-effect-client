| Run      | Check     | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | --------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 41493f77 | character | paid | fail    | 2026-10-02T12:49:44.675Z | $0.525     | $0.028    |
| c61ffc99 | character | paid | pass    | 2026-10-02T12:58:12.436Z | $0.525     | $0.196    |

### character: fail (paid, run 41493f77, 2026-10-02T12:49:44.675Z)

- **Environment:** reactor-effect-client 0.9.2, reactor-effect-native 0.9.2, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit 819d45b3
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.525, estimated $0.028 at 70 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.15 s · allocated 0.45 s · session 2.99 s · make 3.02 s · closed 3.50 s · character observed 3.50 s · settled 3.64 s
- **Photo:** jpeg, 321196 bytes
- **Operations:** session ok in 2.84 s at A-0.30 s; make Protocol: native peer failed (Protocol) (outcome unknown) in 0.02 s at A+2.54 s
- **Phases:** none seen
- **Transcripts:** 0: 0 final from the user, 0 from the character
- **Provider events:** command errors none; diagnostics none
- **Termination:** e9972f0c-601e-4377-81f8-3b1554719399 confirmed (trail CLOSED)
- **Criteria:** ✗ the provider made the avatar from the photo · ✗ the provider returned the call live · ✗ the character's picture came after live · ✗ the character's sound came after live · ✗ the character answered a say · ✗ endCall returned the call's end · ✓ confirmed termination
  - an outcome is unknown, so the run fails and is not repeated
  - the provider made the avatar from the photo: createAvatar never ran
  - the provider returned the call live: startCall never ran
  - the character's picture came after live: startCall never ran
  - the character's sound came after live: startCall never ran
  - the character answered a say: say never ran
  - endCall returned the call's end: endCall never ran

### character: pass (paid, run c61ffc99, 2026-10-02T12:58:12.436Z)

- **Environment:** reactor-effect-client 0.9.2, reactor-effect-native 0.9.2, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit 32394681
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.525, estimated $0.196 at 70 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.10 s · allocated 0.39 s · session 2.63 s · make 2.67 s · createAvatar 5.96 s · startCall 9.12 s · say 15.15 s · endCall 27.06 s · closed 27.47 s · events unread 27.47 s · character observed 27.47 s · settled 27.61 s
- **Pair:** prflx
- **Photo:** jpeg, 321196 bytes
- **Operations:** session ok in 2.53 s at A-0.28 s; make ok in 0.04 s at A+2.25 s; createAvatar ok in 3.29 s at A+2.28 s; startCall ok in 3.16 s at A+5.58 s; say ok in 0.03 s at A+14.74 s; endCall ok in 8.65 s at A+18.02 s
- **Phases:** idle A+2.28 s › preparing_avatar A+3.55 s › avatar_ready A+5.58 s › starting A+5.60 s › warming_up A+6.98 s › live A+8.73 s › ending A+18.05 s › ended A+26.67 s
- **Call:** live at A+8.73 s, call_max_seconds 7200; the first frame 0.17 s after live; the greeting's sound 5.10 s after live, silent 5.66 s after live; 227 frames of 640x360, 928x1088 at 24.9 fps (227 lit, 107 distinct, 0 lost); 504 blocks at 48000 Hz, peak RMS 0.2153
- **Say:** sent at A+14.74 s; the user's transcript 0.24 s after; the answer's sound 3.14 s after; the character's transcript 1.46 s after
- **End:** ended_by_client, the call live 9 s
- **Transcripts:** 3: 1 final from the user, 2 from the character
- **Provider events:** command errors none; diagnostics none
- **Termination:** 2cd989d8-9c74-41a6-a790-dac163d7e983 confirmed (trail CLOSED)
- **Criteria:** ✓ the provider made the avatar from the photo · ✓ the provider returned the call live · ✓ the character's picture came after live · ✓ the character's sound came after live · ✓ the character answered a say · ✓ endCall returned the call's end · ✓ confirmed termination
