| Run      | Check  | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | ------ | ---- | ------- | ------------------------ | ---------- | --------- |
| 5e507c87 | rejoin | paid | pass    | 2026-10-03T14:48:38.765Z | $0.840     | $0.245    |

### rejoin: pass (paid, run 5e507c87, 2026-10-03T14:48:38.765Z)

- **Environment:** reactor-effect-client 0.10.0, reactor-effect-native 0.10.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit ab4a7924
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux workstation, no VPN, outbound UDP open
- **Cost:** worst case $0.840, estimated $0.245 at 70 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.11 s · allocated 0.44 s · session 2.62 s · make 2.65 s · createAvatar 5.70 s · startCall 8.72 s · reconnecting during the call 8.97 s · reconnect 10.78 s · reconnect observed 10.78 s · decoded after reconnect 10.79 s · media after reconnect observed 20.78 s · endCall 34.81 s · closed 35.14 s · events unread 35.14 s · rejoin observed 35.14 s · settled 35.24 s
- **Liveness:** the runner's 1 s timer fired at most 0.00 s late; 42 session events kept
- **Pair:** prflx
- **Photo:** jpeg, 321196 bytes
- **Operations:** session ok in 2.51 s; make ok in 0.03 s; createAvatar ok in 3.04 s; startCall ok in 3.02 s; reconnect ok in 1.81 s; decoded after reconnect ok in 0.00 s; endCall ok in 14.03 s
- **Phases:** idle A+2.22 s › preparing_avatar A+3.37 s › avatar_ready A+5.26 s › starting A+5.29 s › warming_up A+6.69 s › live A+8.28 s › ending A+20.37 s › ended A+34.37 s
- **Before reconnect:** live at A+8.28 s, generation 1; first frame 0.16 s after live, first audio block 0.00 s after live; 18 frames (18 lit), 328 blocks
- **Reconnect:** ok in 1.81 s, started A+8.53 s; generation 1 → 2; provider phase live
- **Liveness around reconnect:** the 1 s timer fired at most 0.00 s late, at A+9.57 s
- **After reconnect:** generation 2, observed 9.99 s; first frame 0.08 s after reconnect, first audio block 0.05 s after reconnect; 246 frames (246 lit), 804 blocks
- **Provider diagnostics:** none
- **End:** ended_by_client, the call live 12 s
- **Termination:** 1f225c34-be87-4a61-b811-a46527811d93 confirmed (trail CLOSED)
- **Criteria:** ✓ the call went live with picture · ✓ the reconnect returned within 30 s · ✓ the session was confirmed ended · ✓ confirmed termination

## What this run answered

One approved paid `rejoin` run on 2026-10-03 passed from clean checkout `ab4a7924`, with native
source hash `1da0d5fb`. The photo was the same approved Vermeer JPEG used for the earlier Vidu
runs; the evidence keeps its type and 321,196-byte size, never its bytes or path.

- **No reconnect hang in this run.** The recorded reconnect interval was 1.814 s, from A+8.530 s
  to A+10.344 s. The provider remained `live`, and the session moved from generation 1 to 2.
- **Decoded media returned.** Generation 2's first video frame arrived 76 ms and its first audio
  block 53 ms after reconnect. Its fresh readers observed 9.994 s, keeping 246 video frames
  (all lit, 199 distinct) and 804 audio blocks, with no recorded loss. Audio peak RMS was 0.2736;
  the first block's arrival alone does not establish speech.
- **The runner did not show the earlier long stall.** Maximum 1 s ticker lateness was 0.3 ms,
  recorded at 10.006 s and 35.010 s on the run's timeline (A+9.569 s and A+34.573 s). The first
  sample overlaps the reconnect; its maximum was also 0.3 ms.
- **No provider diagnostic or Protocol failure was recorded.** The run kept 42 session events.
  All four criteria passed, with no failure reasons or missing evidence. `endCall` returned
  `ended_by_client`, and close was confirmed with no local errors and a `CLOSED` trail. The
  `events unread` milestone's `Closed` detail at 35.137 s followed explicit confirmed close at
  35.136 s: the observer's stream had closed normally.

This is one successful reconnect, not a reliability measurement or an explanation of run
`527bf9ae` on 2026-10-01. That probe reconnected because picture was absent and resumed tracks
differently; one reconnect took 82 s and a replacing connection lacked picture and
voice. This run deliberately reconnected after live picture arrived through the current provider.
It does not establish that the older hang or media loss cannot recur. Decoded media is the
observation here, not proof of presented or recorded output.

## Billing

At the observed 70 credits/s and 10,000 credits/$, the harness counted 35 started seconds for its
$0.245 estimate. Its allocation-to-confirmed-close interval was 34.699 s, rounded up to a second
for that estimate. The $0.84 reservation remains in the ledger; an estimate does not refund it.

| Run      | Reserved | Harness estimate | Estimated billed seconds | Dashboard duration | Dashboard charge |
| -------- | -------- | ---------------- | ------------------------ | ------------------ | ---------------- |
| 5e507c87 | $0.840   | $0.245           | 35 s                     | unread             | unread           |

The dashboard charge has not been read. The estimate is not a measured charge, and no additional
paid run is covered by this run's approval.
