"""Step 15x: suppressed cuts = lines that start a main_text unit in 15wr-a but sit inside a unit in 15x-a.

Matching by (scan_id, line_id, seg_idx). Writes suppressed.tsv, counts per volume/series, units per scan
(all unit types, as step-15wr/counts.py) and median main_text unit lines per series, before and after.

usage (from the AS root):
  .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15x/suppressed.py
"""
import collections
import csv
import gzip
import statistics
from pathlib import Path

csv.field_size_limit(10**9)
R = "NL-HaNA_1.01.02_text_layer_v4"
OUT = Path("reports/data/phase-12-hand-print-box-height/step-15x")
GATE = Path("reports/data/phase-12-hand-print-box-height/step-15wr/gate.tsv")


def rd(layer, sub, inv, suffix=".tsv.gz"):
    p = f"data/text_layer_v2_dev/{layer}/1.01.02/{R}/{sub}/{inv}{suffix}"
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


def seg_text(layer, inv):
    """(scan, line, seg_idx) -> text of the segment."""
    text = {r["line_id"]: r["text"] for r in rd(layer, "lines", inv, ".lines.tsv.gz")}
    return {(r["scan_id"], r["line_id"], r["seg_idx"]):
            text.get(r["line_id"], "")[int(r["char_start"]):int(r["char_end"])]
            for r in rd(layer, "types/segments", inv)}


def unit_segs(layer, inv):
    """units, {unit_id: [(seg_order, key)]} for main_text units."""
    units = rd(layer, "units/units", inv)
    mt = {(u["scan_id"], u["unit_id"]) for u in units if u["unit_type"] == "main_text"}
    by = collections.defaultdict(list)
    for r in rd(layer, "units/segments", inv):
        if (r["scan_id"], r["unit_id"]) in mt:
            by[(r["scan_id"], r["unit_id"])].append((int(r["seg_order"]), (r["scan_id"], r["line_id"], r["seg_idx"])))
    for v in by.values():
        v.sort()
    return units, by


def main():
    gate = [r for r in csv.DictReader(open(GATE), delimiter="\t") if r["split"] in ("True", "False")]
    rows, per_vol = [], []
    agg = collections.defaultdict(collections.Counter)
    lens = collections.defaultdict(lambda: {"15wr-a": [], "15x-a": []})
    for g in gate:
        inv, series = g["inv"], g["series"]
        key = series[:12] + (" (3463)" if inv == "3463" else "")
        txt = seg_text("15x-a", inv)
        res = {}
        for layer in ("15wr-a", "15x-a"):
            units, by = unit_segs(layer, inv)
            res[layer] = (units, by)
            agg[key][layer + " units"] += len(units)
            agg[key][layer + " scans"] += len({u["scan_id"] for u in units})
            lens[key][layer] += [len(v) for v in by.values()]
        starts_a = {v[0][1] for v in res["15wr-a"][1].values()}
        starts_b = {v[0][1] for v in res["15x-a"][1].values()}
        n = 0
        for v in res["15x-a"][1].values():
            for j, (_, k) in enumerate(v):
                if j > 0 and k in starts_a:
                    n += 1
                    rows.append([inv, key, k[0], k[1], k[2], txt.get(v[j - 1][1], ""), txt.get(k, "")])
        per_vol.append((inv, key, n, len(starts_a), len(starts_b)))
        agg[key]["suppressed"] += n
        agg[key]["cuts_before"] += len(starts_a)
    with open(OUT / "suppressed.tsv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writerow(["inv", "series", "scan_id", "line_id", "seg_idx", "prev_text", "text"])
        w.writerows(rows)
    with open(OUT / "counts_per_volume.tsv", "w") as f:
        f.write("inv\tseries\tsuppressed\tmain_unit_starts_before\tmain_unit_starts_after\n")
        for r in per_vol:
            f.write("\t".join(map(str, r)) + "\n")
    with open(OUT / "counts_per_series.txt", "w") as f:
        for k, c in sorted(agg.items()):
            line = (f"{k}: suppressed {c['suppressed']} of {c['cuts_before']} unit starts; "
                    f"units/scan {c['15wr-a units'] / c['15wr-a scans']:.2f} -> {c['15x-a units'] / c['15x-a scans']:.2f}; "
                    f"median main_text unit lines {statistics.median(lens[k]['15wr-a']):g} -> "
                    f"{statistics.median(lens[k]['15x-a']):g}")
            print(line)
            f.write(line + "\n")


if __name__ == "__main__":
    main()
