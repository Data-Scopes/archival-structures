"""Step 15h: fit the print paragraph cues (UnitParams) against REPUBLIC paragraphs.
Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15h/fit_paragraphs.py [LAYER]
Dev = 455 scans with an even scan number; test = 455 odd plus all of 456. The grid is fixed in
advance; the best dev F1 wins (ties: the earlier grid point); test is reported once.
Writes fit_paragraphs.json (grid, chosen point, test P/R/F1, unmapped line_starts) and
pairs_<split>.tsv.gz (every evaluated pair with its features, reference and prediction)."""
import itertools
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz
from archival_structures.text_layer.units import (
    REPUBLIC_PARAGRAPHS_DIR, UnitParams, cues_fired, load_segments, paragraph_features)

LAYER = Path(sys.argv[1] if len(sys.argv) > 1 else "data/text_layer_v2_dev/15h-a")
ROOT = LAYER / "print" / "NL-HaNA_1.10.94_text_layer_v2"
OUT = Path(__file__).parent
GRID = list(itertools.product((0.5, 1.0, 1.5), (0.1, 0.2, None), (1.5, 2.0, None)))
HEADER_TYPES = ("date_header", "running_header")


def split_of(scan_id):
    inv, num = re.match(r".*_(\d+)_(\d+)$", scan_id).groups()
    return "dev" if inv == "455" and int(num) % 2 == 0 else "test"


def collect():
    """pairs[split] = list of dict(scan, line_id, feat, ref)."""
    pairs = {"dev": [], "test": []}
    unmapped = {"dev": set(), "test": set()}
    n_starts = {"dev": set(), "test": set()}
    for inv in ("455", "456"):
        cw = {r["republic_line_id"]: r for r in read_tsv_gz(ROOT / "units" / "republic_lines" / f"{inv}.tsv.gz")}
        starts = set()
        import csv, gzip
        with gzip.open(REPUBLIC_PARAGRAPHS_DIR / f"paragraphs-{inv}.tsv.gz", "rt", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE):
                starts.add(row["line_start"])
        mapped = {}
        for rid in starts:
            r = cw.get(rid)
            sc = re.match(r"^(.*)-line-", rid)
            sc = sc.group(1) if sc else ""
            if r and r["line_id"]:
                mapped[(r["scan_id"], r["line_id"])] = r["match"]
            elif sc:
                unmapped[split_of(sc)].add(rid)
            n_starts[split_of(sc) if sc else "test"].add(rid)
        scans_with = {k[0] for k in mapped}
        first_seg = defaultdict(lambda: 10**9)
        by_region = load_segments(ROOT, inv)
        for segs in by_region.values():
            for s in segs:
                k = (s["scan_id"], s["line_id"])
                first_seg[k] = min(first_seg[k], s["seg_idx"])
        for (scan, page, rid), segs in by_region.items():
            if scan not in scans_with:
                continue
            segs = [s for s in segs]
            mains = [(i, s) for i, s in enumerate(segs) if s["line_type"] == "main_text"]
            if len(mains) < 2:
                continue
            ms = [s for _, s in mains]
            hb = [False] * len(ms)
            for j in range(1, len(ms)):
                hb[j] = any(segs[p]["line_type"] in HEADER_TYPES for p in range(mains[j - 1][0] + 1, mains[j][0]))
            for j, feat in enumerate(paragraph_features(ms, hb), start=1):
                s = ms[j]
                k = (s["scan_id"], s["line_id"])
                ref = k in mapped and s["seg_idx"] == first_seg[k]
                pairs[split_of(scan)].append({"scan_id": scan, "line_id": s["line_id"], "seg_idx": s["seg_idx"],
                                              "feat": feat, "ref": ref})
    return pairs, unmapped, n_starts


def prf(pairs, params):
    tp = fp = fn = 0
    for p in pairs:
        pred = bool(cues_fired(p["feat"], params))
        tp += pred and p["ref"]
        fp += pred and not p["ref"]
        fn += (not pred) and p["ref"]
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4)}


def main():
    pairs, unmapped, n_starts = collect()
    grid = []
    for ind, sh, gp in GRID:
        params = UnitParams(indent_frac=ind, short_frac=sh, gap_factor=gp)
        grid.append({"indent_frac": ind, "short_frac": sh, "gap_factor": gp, **prf(pairs["dev"], params)})
    best = max(enumerate(grid), key=lambda t: (t[1]["f1"], -t[0]))[1]
    chosen = UnitParams(best["indent_frac"], best["short_frac"], best["gap_factor"])
    test = prf(pairs["test"], chosen)
    header_only = {s: prf(pairs[s], UnitParams()) for s in pairs}
    res = {"grid_dev": grid, "chosen": chosen.to_dict(), "dev": prf(pairs["dev"], chosen), "test": test,
           "header_only": header_only,
           "n_pairs": {s: len(v) for s, v in pairs.items()}, "n_ref": {s: sum(p["ref"] for p in v) for s, v in pairs.items()},
           "n_line_starts": {s: len(v) for s, v in n_starts.items()}, "n_line_starts_unmapped": {s: len(v) for s, v in unmapped.items()}}
    (OUT / "fit_paragraphs.json").write_text(json.dumps(res, indent=1))
    for split, ps in pairs.items():
        rows = [[p["scan_id"], p["line_id"], p["seg_idx"], round(p["feat"]["indent"], 3), round(p["feat"]["short"], 3),
                 round(p["feat"]["gap"], 3), int(p["feat"]["header"]), int(p["ref"]),
                 int(bool(cues_fired(p["feat"], chosen)))] for p in ps]
        write_tsv_gz(OUT / f"pairs_{split}.tsv.gz", ["scan_id", "line_id", "seg_idx", "indent", "short", "gap",
                                                    "header", "ref", "pred"], rows)
    print(json.dumps({k: res[k] for k in ("chosen", "dev", "test", "header_only", "n_pairs", "n_ref", "n_line_starts", "n_line_starts_unmapped")}, indent=1))
    for g in grid:
        print(g)


if __name__ == "__main__":
    main()
