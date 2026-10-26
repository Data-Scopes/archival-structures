"""Acceptance 4 and 5 of step 15v: printed 1.01.02 volumes (v4 print mode) next to 455/456.

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/metrics.py
"""
import gzip
import json
from pathlib import Path

B = Path("data/text_layer_v2_dev/15v-a")
LAYERS = {"1.01.02": B / "1.01.02/NL-HaNA_1.01.02_text_layer_v4", "1.10.94": B / "1.10.94/NL-HaNA_1.10.94_text_layer_v4"}
INVS = [("1.01.02", i) for i in "3771 3772 3773 3821 3822 3823".split()] + [("1.10.94", "455"), ("1.10.94", "456")]
OUT = Path("reports/data/phase-12-hand-print-box-height/step-15v")


def column_share(root, inv):
    n = c = 0
    with gzip.open(root / "lines" / f"{inv}.lines.tsv.gz", "rt", encoding="utf-8") as fh:
        h = fh.readline().rstrip("\n").split("\t")
        ci, ti = h.index("column"), h.index("text_raw")
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if f[ti] == "":   # share among lines with text (reproduces the brief's 455: 0.992, 456: 1.000)
                continue
            n += 1
            c += f[ci] != ""
    return n, c / n


rows = []
for arch, inv in INVS:
    r = LAYERS[arch]
    j = lambda p: json.loads((r / p).read_text())
    ss, lay, ty, un = j(f"split_stats/{inv}.json"), j("layout/stats.json")[inv], j("types/stats.json")[inv], j("units/stats.json")[inv]
    n_lines, col = column_share(r, inv)
    pm = un["units_per_method"]
    nu = un["n_units"]
    scans = lay["n_scans"]
    cw = un["republic_crosswalk"]
    rows.append({
        "inv": inv, "n_scans": scans, "n_lines": n_lines,
        "cand_before": ss["n_candidates_before"], "split_hyphen": ss["n_split_hyphen"], "split_bigram": ss["n_split_bigram"],
        "cand_after": ss["n_candidates_after"], "cand_before_per_1k_lines": round(1000 * ss["n_candidates_before"] / n_lines, 2),
        "split_per_1k_lines": round(1000 * (ss["n_split_hyphen"] + ss["n_split_bigram"]) / n_lines, 2),
        "column_share": round(col, 4), "layout_merges": lay["n_merges"], "merges_per_scan": round(lay["n_merges"] / scans, 3),
        "units": nu, "units_per_page": round(nu / scans, 2),
        "print_share": round(sum(v for k, v in pm.items() if k.startswith("print:")) / nu, 4),
        "page_number": ty["methods"] and sum(v for k, v in ty["methods"].items() if k.startswith("page_number|")),
        "other": sum(v for k, v in ty["methods"].items() if k.startswith("other|")),
        "units_page_number": un["units_per_type"].get("page_number", 0), "units_other": un["units_per_type"].get("other", 0),
        "crosswalk": json.dumps(cw, sort_keys=True) if cw else "",
    })
cols = list(rows[0])
with open(OUT / "print_vs_455_456.tsv", "w") as fh:
    fh.write("\t".join(cols) + "\n")
    for r in rows:
        fh.write("\t".join(str(r[c]) for c in cols) + "\n")
for c in cols:
    print(f"{c:26s}" + " ".join(f"{str(r[c])[:11]:>12s}" for r in rows))
