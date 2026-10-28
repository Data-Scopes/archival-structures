"""Counts for the report section of step 19a, from the hand-made TSVs in this directory.

Run: .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19a/summary.py
Reads: side_verdicts.tsv, entry_gold.tsv, entry_sides.tsv, s_start_reading.tsv (hand tallies),
and planning/reads_sides.tsv (stratum column only) and planning/reads_entries.tsv.
"""
import csv
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).parent
PLAN = HERE.parent / "planning"


def tsv(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


verd = tsv(HERE / "side_verdicts.tsv")
sides = {r["read_id"]: r for r in tsv(PLAN / "reads_sides.tsv")}
gold = tsv(HERE / "entry_gold.tsv")
es = tsv(HERE / "entry_sides.tsv")
ents = tsv(PLAN / "reads_entries.tsv")
starts = tsv(HERE / "s_start_reading.tsv")

assert len(verd) == 60 and sorted(int(r["read_id"]) for r in verd) == list(range(1, 61))
print("== Side verdicts per stratum ==")
vals = ["index", "not index", "unclear", "unreadable input"]
by = defaultdict(Counter)
for r in verd:
    assert r["verdict"] in vals
    by[sides[r["read_id"]]["stratum"]][r["verdict"]] += 1
print("stratum", *vals, "total", sep="\t")
tot = Counter()
for s in sorted(by):
    print(s, *[by[s][v] for v in vals], sum(by[s].values()), sep="\t")
    tot.update(by[s])
print("all", *[tot[v] for v in vals], sum(tot.values()), sep="\t")
print("unclear + unreadable:", tot["unclear"] + tot["unreadable input"], "of 60")

print("\n== Kind counts (text before first parenthesis or 'under') ==")
kinds = Counter(r["kind"].split(" (")[0].split(" under")[0] for r in verd)
for k, n in kinds.most_common():
    print(f"{n:3d}  {k}")

print("\n== Entry sides ==")
roles = Counter((r["split"], r["role"]) for r in ents)
print("listed:", dict(roles))
used = [r for r in es]
print("sides labelled:", len(used), "| reserves used:", sum(r["role"] == "reserve" for r in used))
print("index sides per split:", dict(Counter(r["split"] for r in used if r["side_verdict"] == "index")))
print("not-index entry sides:", sum(r["side_verdict"] != "index" for r in used))

lab = defaultdict(Counter)
grp = {r["read_id"]: r["group"] for r in es}
for g in gold:
    lab[grp[g["read_id"]]][g["label"]] += 1
print("\n== Entry gold per group ==")
print("group", "sides", "lines", "S", "C", "H", "?", "mean_entry_len(lines)", sep="\t")
allc = Counter()
for g in sorted(lab):
    c = lab[g]
    n = sum(c.values())
    ns = sum(1 for r in es if r["group"] == g)
    print(g, ns, n, c["S"], c["C"], c["H"], c["?"], f"{(c['S'] + c['C']) / c['S']:.2f}", sep="\t")
    allc.update(c)
n = sum(allc.values())
print("all", len(es), n, allc["S"], allc["C"], allc["H"], allc["?"], f"{(allc['S'] + allc['C']) / allc['S']:.2f}", sep="\t")
print("share of ? :", f"{allc['?'] / n:.3%}", "| min n_S per side:", min(int(r["n_S"]) for r in es))
for sp in ("dev", "test"):
    c = Counter()
    for g in gold:
        if next(r for r in es if r["read_id"] == g["read_id"])["split"] == sp:
            c[g["label"]] += 1
    print(sp, "lines", sum(c.values()), dict(c))

merged = [g for g in gold if g["note"].startswith("merged")]
per = Counter(g["read_id"] for g in merged)
print("\nlines labelled merged:", len(merged), "in", len(per), "of", len(es), "sides; per side:", dict(sorted(per.items(), key=lambda x: int(x[0]))))
print("merged per side (all sides):", f"{len(merged) / len(es):.2f}")

print("\n== How entries start (hand tally over all S lines) ==")
cols = ["n_S", "date_start", "prev_ends_number_any", "prev_ends_number_excl", "further_left_excl", "none_excl"]
t = {c: sum(int(r[c]) for r in starts) for c in cols}
for r in starts:
    assert int(r["date_start"]) + int(r["prev_ends_number_excl"]) + int(r["further_left_excl"]) + int(r["none_excl"]) == int(r["n_S"]), r
assert t["n_S"] == allc["S"], (t["n_S"], allc["S"])
for c in cols:
    print(f"{c:24s} {t[c]:4d}  {t[c] / t['n_S']:.1%}")
print("(excl = exclusive cascade date > previous line ends in number > further left (dx <= -25 px) > none)")
