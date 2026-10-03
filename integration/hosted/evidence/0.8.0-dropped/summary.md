| Run      | Check   | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | ------- | ---- | ------- | ------------------------ | ---------- | --------- |
| e87da9b1 | dropped | paid | pass    | 2026-09-30T01:12:07.255Z | $1.000     | $0.800    |

### dropped: pass (paid, run e87da9b1, 2026-09-30T01:12:07.255Z)

- **Environment:** reactor-effect-client 0.8.0, reactor-effect-native 0.8.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit b64aff33
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $1.000, estimated $0.800 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.21 s · owner started 0.21 s · owner connected 3.87 s · owner killed 3.88 s · read ACTIVE 3.97 s · read INACTIVE 10.09 s · closed 64.51 s · dropped observed 64.51 s · settled 64.72 s
- **Owner:** node v24.14.1, the isolated native peer; 8de21707-9d81-4ca2-9b52-3e2e16a0fd52 allocated 2026-09-30T01:12:08.476Z, connected 2.66 s later, killed 2.66 s after allocation, 2026-09-30T01:12:11.140Z
- **Answer:** not ended by Reactor within 60.00 s of the kill; the key ended it; the first read that found it INACTIVE came 6.21 s after the kill
- **Window:** reads until 60.00 s after the kill, 62.66 s after allocation
- **Reads from the kill:** ACTIVE from 0.10 s to 4.21 s (3 reads) > INACTIVE from 6.21 s to 60.21 s (28 reads)
- **Termination:** 8de21707-9d81-4ca2-9b52-3e2e16a0fd52 confirmed (trail CLOSED)
- **Criteria:** ✓ the owner connected · ✓ the watch completed · ✓ confirmed termination

**Billing, for the dashboard:** seconds from the session's allocation, 2026-09-30T01:12:08.476Z. Its end lies between the two times given: for Reactor's, the last read that found the session running and the first that found it ended; for the key's, its DELETE and the read that confirmed it; and before a DELETE that found no session, the last read that found it running and that DELETE. Fill in the last two columns from the Reactor dashboard.

| Session                              | Connected | Killed | First INACTIVE read | Window closed | Ended by | Ended         | Killed to ended | Dashboard duration | Dashboard charge |
| ------------------------------------ | --------- | ------ | ------------------- | ------------- | -------- | ------------- | --------------- | ------------------ | ---------------- |
| 8de21707-9d81-4ca2-9b52-3e2e16a0fd52 | 2.66 s    | 2.66 s | 8.88 s              | 62.66 s       | the key  | 62.88–63.30 s | 60.21–60.64 s   | 1m 3s              | not shown        |

## The dashboard

Read on September 30, 2026. The Usage page lists the session `CLOSED`, at **1m 3s**, which matches the 62.88–63.30 s from allocation to the key's end. So Reactor bills the time a session reads `INACTIVE` after its owner dies, not only the time something is connected. The page shows no charge per session. The balance read $9.24, against $9.32 on September 29 at about 14:15 UTC, before this run and the 0.8.0-rc `show` charges. Those charges have not posted, so what Reactor charged for them is still open.
