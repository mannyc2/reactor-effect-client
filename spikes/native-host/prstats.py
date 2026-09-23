import subprocess, re, sys, statistics, datetime, collections
repo = sys.argv[1]
def git(*a): return subprocess.run(["git","-C",repo,"-c","gc.auto=0",*a],capture_output=True,text=True).stdout
merged = {}
for line in git("log","origin/HEAD","--first-parent","--format=%H\t%ct\t%an\t%s").splitlines():
    h, ct, an, s = line.split("\t",3)
    m = re.search(r"\(#(\d+)\)\s*$", s) or re.search(r"Merge pull request #(\d+)", s)
    if m: merged[int(m.group(1))] = (int(ct), an, s)
prs = {}
for ref in git("for-each-ref","refs/remotes/pr","--format=%(refname:short)").split():
    n = int(ref.split("/")[-1])
    base = git("merge-base","origin/HEAD",ref).strip()
    rng = f"{base}..{ref}" if base else ref
    commits = git("log",rng,"--format=%at\t%an\t%ae").splitlines()
    if not commits: commits = git("log","-1",ref,"--format=%at\t%an\t%ae").splitlines()
    first = min(int(c.split("\t")[0]) for c in commits)
    authors = collections.Counter(c.split("\t")[1] for c in commits)
    emails = set(c.split("\t")[2] for c in commits)
    prs[n] = dict(first=first, author=authors.most_common(1)[0][0], emails=emails)
rows=[]
for n,p in sorted(prs.items()):
    m = merged.get(n)
    rows.append((n,p["author"],",".join(sorted(p["emails"]))[:60], "merged" if m else "not merged", round((m[0]-p["first"])/3600,1) if m else None))
by = collections.defaultdict(list); unmerged = collections.Counter(); total=collections.Counter()
for n,a,e,st,h in rows:
    total[a]+=1
    if h is not None: by[a].append(h)
    else: unmerged[a]+=1
print(f"PR heads: {len(prs)}; merged (by #N on main first-parent): {sum(1 for r in rows if r[3]=='merged')}")
for a in total:
    hs = by[a]
    print(f"  {a}: PRs={total[a]} merged={len(hs)} unmerged/open={unmerged[a]} median_hours_first_commit_to_merge={statistics.median(hs) if hs else None} max={max(hs) if hs else None}")
print("non-core PRs:")
for r in rows:
    if r[1] not in ("Douglas Ferreira","Douglas Amorim Ferreira","Germán Goldenstein","ggoldens","dependabot[bot]"):
        print("  ", r)
