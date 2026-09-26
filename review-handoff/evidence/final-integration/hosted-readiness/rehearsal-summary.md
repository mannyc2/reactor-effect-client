| Run | Check | Mode | Verdict | Started | Worst case | Estimated |
|---|---|---|---|---|---|---|
| 6cef9a49 | scheduler-renewal | rehearsal | pass | 2026-09-26T21:50:40.237Z | $0.200 | $0.200 |
| 3872bcc3 | scheduler-renewal | rehearsal | pass | 2026-09-26T21:50:22.931Z | $0.200 | $0.200 |

### scheduler-renewal: pass (rehearsal, run 6cef9a49, 2026-09-26T21:50:40.237Z)

- **Environment:** reactor-effect-client 0.5.0, reactor-effect-native 0.5.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, darwin arm64 24.0.0, commit fc1d74210c7a
- **Network:** loopback twin
- **Cost:** worst case $0.200, estimated $0.200 at 5 credits/s and 3000 credits/$
- **Timeline:** admitted 0.06 s · minted 0.07 s
- **Connect phases:** described +0.00 s, prepared +0.00 s, registered +0.00 s, offered +0.00 s, answered +0.20 s, ready +0.01 s
- **Public scheduler renewal:** continuous constructor; 2 open attempts; monotonic clock; 1 filler requests and 0 filler observations
- **Keyed playback:** qualification-A on sess_a9688372-584f-4fa3-8177-7e39afe2b1a8: Accepted@0.32 s → Building@0.34 s → Ready@0.84 s → Started@1.36 s → Ended@6.52 s; qualification-B on sess_d7fc3466-29df-4155-8d77-89174c702f77: Accepted@10.62 s → Building@10.63 s → Ready@11.13 s → Started@11.73 s → Ended@16.90 s
- **Planned switch:** sess_a9688372-584f-4fa3-8177-7e39afe2b1a8 → sess_d7fc3466-29df-4155-8d77-89174c702f77: count-complete; 263/124 local final-clip frames; Ended grace 4709/250 ms
- **Logical decoded media:** 400 video frames; attribution complete; boundary gap 21 ms; audio completeness unverified; no encoded or viewer-output claim
- **Accepted drain:** completed; allocated sources 2 → 2
- **Renewal cleanup:** Continuous; 2 canonical source reports; complete
- **Continuous retention:** keep 1 successes; unresolved limit 2
- **Cleanup summary:** 2 retirements; 1 retained (0 incomplete); 1 omitted complete owned terminations; exhausted false
- **Source 1:** sess_a9688372-584f-4fa3-8177-7e39afe2b1a8; canonical owned termination confirmed; cap expiry 2026-09-26T21:51:30.310Z
- **Source 2:** sess_d7fc3466-29df-4155-8d77-89174c702f77; canonical owned termination confirmed; cap expiry 2026-09-26T21:51:40.601Z
- **Criteria:** ✓ public renewal preparation · ✓ keyed playback order · ✓ planned switch · ✓ attributed logical media · ✓ accepted drain · ✓ owned lease cleanup · ✓ bounded allocation

### scheduler-renewal: pass (rehearsal, run 3872bcc3, 2026-09-26T21:50:22.931Z)

- **Environment:** reactor-effect-client 0.5.0, reactor-effect-native 0.5.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, darwin arm64 24.0.0, commit fc1d74210c7a
- **Network:** loopback twin
- **Cost:** worst case $0.200, estimated $0.200 at 5 credits/s and 3000 credits/$
- **Timeline:** admitted 0.17 s · minted 0.19 s
- **Connect phases:** described +0.00 s, prepared +0.01 s, registered +0.00 s, offered +0.01 s, answered +0.21 s, ready +0.01 s
- **Public scheduler renewal:** legacy constructor; 2 open attempts; monotonic clock; 1 filler requests and 0 filler observations
- **Keyed playback:** qualification-A on sess_1624d24a-dabe-4baf-9db9-f348e98105c6: Accepted@0.77 s → Building@0.84 s → Ready@1.34 s → Started@1.81 s → Ended@7.02 s; qualification-B on sess_24cc3e54-f591-4a51-ac35-4af40ca8a7bf: Accepted@10.73 s → Building@10.73 s → Ready@11.24 s → Started@11.75 s → Ended@16.92 s
- **Planned switch:** sess_1624d24a-dabe-4baf-9db9-f348e98105c6 → sess_24cc3e54-f591-4a51-ac35-4af40ca8a7bf: count-complete; 253/124 local final-clip frames; Ended grace 4232/250 ms
- **Logical decoded media:** 365 video frames; attribution complete; boundary gap 59 ms; audio completeness unverified; no encoded or viewer-output claim
- **Accepted drain:** completed; allocated sources 2 → 2
- **Renewal cleanup:** Legacy; 2 canonical source reports; complete
- **Source 1:** sess_1624d24a-dabe-4baf-9db9-f348e98105c6; canonical owned termination confirmed; cap expiry 2026-09-26T21:51:13.136Z
- **Source 2:** sess_24cc3e54-f591-4a51-ac35-4af40ca8a7bf; canonical owned termination confirmed; cap expiry 2026-09-26T21:51:23.477Z
- **Criteria:** ✓ public renewal preparation · ✓ keyed playback order · ✓ planned switch · ✓ attributed logical media · ✓ accepted drain · ✓ owned lease cleanup · ✓ bounded allocation
