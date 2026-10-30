"""Step 21b: folio map v2 evaluation. Run from the AS root with `.venv/bin/python -I`.

    eval_map.py --part volumes   # volumes.tsv, loo.tsv, acceptance 2 (v1 tables) and 3 (planner numbers)  [default]
    eval_map.py --part dev       # dev_eval.tsv from the 21a dev targets (needs step-21a/target_gold.tsv)
    eval_map.py --part test      # bar F, ONCE: writes test_done.json first and refuses if it exists

Reads (read only): data/text_layer_v2_dev/19e-a/..._text_layer_v5, the phase-14 front_index_volumes.tsv (inv column),
data/index_links_dev/20r-a/..._index_links_v1/folio_map/<inv>.tsv.gz, and for dev/test the 21a target_gold.tsv.
Writes only into this directory (stdout of each part is tee'd by the caller into eval_map.txt).
"""
import argparse
import csv
import datetime
import json
import math
import re
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path

import numpy as np

sys.path.insert(0, ".")
from archival_structures.analysis.folio_map import FOLIO_MAP_V2, FolioMapParams, build_folio_map  # noqa: E402
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs  # noqa: E402
from archival_structures.analysis.page_numbering import interpolate_folio_to_scan  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

LAYER = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
V1DIR = Path("data/index_links_dev/20r-a/NL-HaNA_1.01.02_index_links_v1/folio_map")
P14 = Path("reports/data/phase-14-index-links")
P15 = Path("reports/data/phase-15-index-links-v2")
OUT = Path(__file__).parent
GOLD = P15 / "step-21a/target_gold.tsv"
V1 = FolioMapParams()
V1_COLS = "number front_scan_id front_page back_scan_id back_page position method anchor_dist confidence".split()


def all41():
    return [r["inv"] for r in csv.DictReader(open(P14 / "planning/front_index_volumes.tsv"), delimiter="\t")]


def load(inv):
    v = read_volume_inputs(LAYER, inv)
    first, last, end = front_index_run(v.flags)
    excl = {s for i, s in enumerate(v.sides) if i <= last or v.flags[i]}
    return v, excl


def loo(chain):
    ok = tot = 0
    for i in range(1, len(chain) - 1):
        s, _, _ = interpolate_folio_to_scan(chain[i, 1], np.delete(chain, i, axis=0))
        tot += 1
        ok += math.floor(s + 0.5) == int(chain[i, 0])
    return ok, tot


def wilson(k, n, z=1.96):
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def part_volumes():
    rows, loorows, tot = [], [], Counter()
    changed, mismatch_vols = [], []
    for inv in all41():
        v, excl = load(inv)
        m1 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=V1)
        m2 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=FOLIO_MAP_V2)
        res = {}
        for tag, m in (("v1", m1), ("v2", m2)):
            o, t = loo(m.chain) if m.mode != "none" else (0, 0)
            res[tag] = (o, t)
            tot[f"o{tag}"] += o
            tot[f"t{tag}"] += t
            loorows.append([inv, tag, m.mode, o, t])
        l1 = int(m1.chain[-1, 1]) if len(m1.chain) else ""
        l2 = int(m2.chain[-1, 1]) if len(m2.chain) else ""
        if l1 != l2:
            changed.append((inv, l1, l2))
        tot["dup"] += m2.n_dup_dropped
        tot["dupvols"] += m2.n_dup_dropped > 0
        tot["modechg"] += m1.mode != m2.mode
        rows.append([inv, m1.mode, m2.mode, len(m1.chain), len(m2.chain), l1, l2, len(v.scan_ids),
                     m2.n_dup_dropped, "%d/%d" % res["v1"], "%d/%d" % res["v2"]])
        # acceptance 2: v1 table equals the stored 20r-a folio_map table
        f = V1DIR / f"{inv}.tsv.gz"
        stored = [[r[c] for c in V1_COLS] for r in read_tsv_gz(f)] if f.exists() else []
        mine = [[str(r[c]) for c in V1_COLS] for r in m1.table()]
        tot["tables_equal"] += stored == mine
        tot["rows"] += len(mine)
        if stored != mine:
            mismatch_vols.append(inv)
    with open(OUT / "volumes.tsv", "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow("inv mode_v1 mode_v2 chain_v1 chain_v2 last_v1 last_v2 n_scans dup_numbers_dropped loo_v1 loo_v2".split())
        w.writerows(rows)
    with open(OUT / "loo.tsv", "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow("inv version mode loo_ok loo_n".split())
        w.writerows(loorows)
    print(f"LOO v1 {tot['ov1']}/{tot['tv1']} = {tot['ov1']/tot['tv1']:.4f}")
    print(f"LOO v2 {tot['ov2']}/{tot['tv2']} = {tot['ov2']/tot['tv2']:.4f}")
    print(f"tied numbers dropped: {tot['dup']} in {tot['dupvols']} volumes")
    print(f"volumes whose last chain number changes ({len(changed)}):", changed)
    print(f"volumes changing mode: {tot['modechg']}")
    for r in rows:
        if r[0] == "3712":
            print("3712:", r)
    print(f"acceptance 2: v1 tables identical to 20r-a in {tot['tables_equal']}/41 volumes ({tot['rows']} rows); "
          f"mismatches: {mismatch_vols}")


def gold_rows(split):
    out = []
    for r in csv.DictReader(open(GOLD), delimiter="\t"):
        if (r["split"] == split and r["target_scan_id"] not in ("", "none") and r["folio_evidence"] != "unclear"
                and r["target_locator"].strip().isdigit()):
            out.append(r)
    return out


def part_eval(split):
    maps, cache = {}, {}
    rows, cov = [], Counter()
    cols = "target_id inv N gold_pos v1_pos v1_method v1_conf v2_pos v2_method v2_conf v2_hit".split()
    for g in gold_rows(split):
        inv = g["inv"]
        if inv not in maps:
            v, excl = load(inv)
            cache[inv] = v
            maps[inv] = (build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=V1),
                         build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=FOLIO_MAP_V2))
        v = cache[inv]
        pos = {s: i + 1 for i, s in enumerate(v.scan_ids)}
        n = int(g["target_locator"])
        gp = pos[g["target_scan_id"]]
        out = [g["target_id"], inv, n, gp]
        for m in maps[inv]:
            t = m.locate(n)
            out += [pos[t.front[0]], t.method, t.confidence] if t else ["", "none", "none"]
        out.append(int(out[7] == gp))
        rows.append(out)
        if g["content"] != "unclear":
            toks = set(re.findall(r"[a-z]+", g["content_sides"]))
            cov["leaf" if toks & {"front", "back"} else "opening_only" if "opening" in toks
                else "next_only" if "next" in toks else "other"] += 1
    with open(OUT / f"{split}_eval.tsv", "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(cols)
        w.writerows(rows)
    n = len(rows)
    h2 = sum(r[-1] for r in rows)
    h1 = sum(r[4] == r[3] for r in rows)
    lo, hi = wilson(h2, n)
    print(f"{split}: v2 {h2}/{n} = {h2/max(n,1):.4f}, Wilson 95% [{lo:.3f}, {hi:.3f}]; v1 (reported only) {h1}/{n}")
    for c in ("high", "medium", "low", "none"):
        sel = [r for r in rows if r[9] == c]
        print(f"  v2_conf={c}: {sum(r[-1] for r in sel)}/{len(sel)}")
    print("  opening coverage (content != unclear):", dict(cov))
    if split == "test":
        print(f"BAR F (>= 0.95): {'PASS' if n and h2 / n >= 0.95 else 'FAIL'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", choices=["volumes", "dev", "test"], default="volumes")
    a = ap.parse_args()
    if a.part == "volumes":
        part_volumes()
    elif a.part == "dev":
        part_eval("dev")
    else:
        done = OUT / "test_done.json"
        if done.exists():
            sys.exit(f"{done} exists: the test part has run; refusing (bar F is run once).")
        json.dump({"run_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "params": asdict(FOLIO_MAP_V2)},
                  open(done, "w"), indent=1)
        part_eval("test")


if __name__ == "__main__":
    main()
