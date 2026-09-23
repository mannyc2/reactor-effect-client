"""Render the decision document's result rows from results/*.json."""
import json, sys, glob, os

def load(path):
    try:
        return json.load(open(path))
    except Exception:
        return None

def row(name, r):
    v, a, res, dc = r["video"], r["audio"], r["resources"], r["dataChannel"]["rttMs"]
    wall = res["wallSeconds"]
    child = res["childCpuSeconds"] / wall * 100 if wall else 0
    cpu = f'{res["cpuPercentOfOneCore"]:.1f}' + (f" + {child:.1f} (ffmpeg)" if res["childCpuSeconds"] else "")
    rss = f'{res["rssMiB"]["peak"]}' + (f' + {res["childRssMiB"]["peak"]}' if res["childRssMiB"]["peak"] else "")
    lat = v["latencyMs"]
    snaps = r["shutdown"].get("nativeSnapshots") or []
    native = []
    for s in snaps:
        if "droppedVideo" in s:
            native.append(f'v{s["droppedVideo"]}/a{s["droppedAudio"]}')
        elif "video" in s:
            native.append(f'v{s["video"]["dropped"]}/a{s["audio"]["dropped"]}')
    return (f'| {name} | {v["received"]:,} / {v["farEncodedFramesSent"]:,} | {a["received"]:,} / {a["pushedByFarPeer"]:,} '
            f'| {lat["p50"]:.0f} / {lat["p95"]:.0f} / {lat["max"]:.0f} | {cpu} | {rss} | {dc["p50"]} / {dc["p95"]} '
            f'| {res["threadpoolStatMs"]["p95"]} / {res["threadpoolStatMs"]["max"]} | {", ".join(native) or "—"} | {r["shutdown"].get("ms")} |')

print("| Run | Video recv / encoded | Audio recv / pushed | Latency p50 / p95 / max (ms) | CPU % of one core | RSS peak MiB | DC RTT p50 / p95 (ms) | libuv probe p95 / max (ms) | Native drops (video/audio) | Shutdown ms |")
print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.json"))):
    r = load(path)
    name = os.path.basename(path)[:-5]
    if r is None or "video" not in r:
        print(f"| {name} | no result (see {name}.err) | | | | | | | | |")
        continue
    print(row(name, r))
