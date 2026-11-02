"""Step 23b: draw the hand-read samples (seed 20261101) from the v6 layer. Writes joins_sample.tsv (80 merge boundaries
whose left source line ends in ¬), guarded_sample.tsv (20 guarded ¬ boundaries) and changed_sample.tsv (10 changed
units, 22a text against v6 text around the first change).

    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23b/sample.py
"""
import collections
import csv
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import NEW, NOT, OLD, classify_nots, inventories, rd  # noqa: E402

HERE = Path(__file__).parent
SEED = 20261101


def write(path, header, rows):
    with open(path, "w", encoding="utf8", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writerow(header)
        w.writerows(rows)


def main():
    cand, guarded, changed = [], [], []
    for inv in inventories(NEW):
        units = {r["unit_id"]: r["text"] for r in rd(NEW / "join" / "units" / f"{inv}.tsv.gz")}
        old = {r["unit_id"]: r["text"] for r in rd(OLD / "join" / "units" / f"{inv}.tsv.gz")}
        lines = {r["line_id"]: r["text"] for r in rd(NEW / "lines" / f"{inv}.lines.tsv.gz")}
        segs = collections.defaultdict(list)
        for s in rd(NEW / "join" / "segments" / f"{inv}.tsv.gz"):
            segs[s["unit_id"]].append(s)
        for uid, ss in segs.items():
            ss.sort(key=lambda s: int(s["seg_order"]))
            t = units[uid]
            for k, s in enumerate(ss[:-1]):
                if s["merged_with_next"] == "1" and lines.get(s["line_id"], "").rstrip(" ").endswith(NOT):
                    cand.append((inv, uid, s["line_id"], ss[k + 1]["line_id"], int(s["char_end"])))
            if NOT in t:
                for i, c, s in classify_nots(t, ss):
                    if c == "guarded":
                        k = ss.index(s)
                        guarded.append((inv, uid, s["line_id"], ss[k + 1]["line_id"], i, s["boundary_kind"]))
            if uid in old and old[uid] != t:
                changed.append((inv, uid))
        # lines text for guarded/cand rows are looked up in a second pass below
    rng = random.Random(SEED)
    pick = rng.sample(cand, 80)
    gp = rng.sample(guarded, min(20, len(guarded)))
    cp = rng.sample(changed, 10)
    need = collections.defaultdict(set)
    for r in pick + gp:
        need[r[0]].update([r[2], r[3]])
    for inv in {r[0] for r in pick + gp}:
        pass
    ltext = {}
    for inv in need:
        for r in rd(NEW / "lines" / f"{inv}.lines.tsv.gz"):
            if r["line_id"] in need[inv]:
                ltext[(inv, r["line_id"])] = r["text"]
    utext = {}
    rows = []
    for k, (inv, uid, l1, l2, ce) in enumerate(pick, 1):
        t = {r["unit_id"]: r["text"] for r in rd(NEW / "join" / "units" / f"{inv}.tsv.gz")}[uid] if (inv, uid) not in utext else utext[(inv, uid)]
        rows.append([k, inv, l1, ltext[(inv, l1)], ltext[(inv, l2)], t[max(0, ce - 30):ce + 30], "", ""])
    write(HERE / "joins_sample.tsv", ["k", "inv", "line_id", "left", "right", "joined", "verdict", "note"], rows)
    rows = []
    for k, (inv, uid, l1, l2, i, kind) in enumerate(gp, 1):
        t = {r["unit_id"]: r["text"] for r in rd(NEW / "join" / "units" / f"{inv}.tsv.gz")}[uid]
        rows.append([k, inv, l1, kind, ltext[(inv, l1)], ltext[(inv, l2)], t[max(0, i - 30):i + 31], "", "", ""])
    write(HERE / "guarded_sample.tsv", ["k", "inv", "line_id", "boundary_kind", "left", "right", "joined", "category",
                                        "should_join", "note"], rows)
    rows = []
    for k, (inv, uid) in enumerate(cp, 1):
        o = {r["unit_id"]: r["text"] for r in rd(OLD / "join" / "units" / f"{inv}.tsv.gz")}[uid]
        n = {r["unit_id"]: r["text"] for r in rd(NEW / "join" / "units" / f"{inv}.tsv.gz")}[uid]
        j = next(x for x in range(min(len(o), len(n))) if o[x] != n[x])
        rows.append([k, inv, uid, len(o) - len(n), o[max(0, j - 30):j + 40], n[max(0, j - 30):j + 40]])
    write(HERE / "changed_sample.tsv", ["k", "inv", "unit_id", "len_diff", "text_22a", "text_v6"], rows)
    print(len(cand), "merge candidates;", len(guarded), "guarded; ", len(changed), "changed units")


if __name__ == "__main__":
    main()
