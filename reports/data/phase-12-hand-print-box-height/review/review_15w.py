"""Phase-12 review: independent re-run of the 15w acceptance numbers (reads only).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15w.py

1. every `mode/<inv>.json` of 15w-a against the 168 reference values (own join, not values_check.py);
   mode counts per archive and per silver label;
2. the 15v-a comparison on the 16 shared inventories (step-15v/compare.py), written to review/compare16_review.json;
3. the EAD title of every 1.01.02 volume within 10 px of the cut (data/metadata/eads/NL-HaNA_1.01.02.xml).
"""
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height")
L = Path("data/text_layer_v2_dev/15w-a")
OUT = D / "review"


def references():
    refs = {}
    for name in ("fit", "test", "diagnostic"):
        t = pd.read_csv(D / f"step-15u/{name}_volumes.tsv", sep="\t", dtype={"inv": str})
        refs.update({r.inv: float(r.value) for r in t.itertuples()})
    h = pd.read_csv("reports/data/phase-10-hand-print-per-region/review/h_by_volume.tsv", sep="\t",
                    dtype={"inv": str})
    refs.update({r.inv: float(r.h_w) for r in h[h["set"] == "staged"].itertuples()})
    return refs


def modes():
    out = {}
    for arch in ("1.01.02", "1.10.94"):
        for p in sorted((L / arch).glob("NL-HaNA_*_text_layer_v4/mode/*.json")):
            m = json.loads(p.read_text())
            out[m["inv"]] = (arch, m["mode"], m["method"], m["value"], m.get("margin"))
    return out


def ead_titles(invs):
    from lxml import etree
    t = etree.parse("data/metadata/eads/NL-HaNA_1.01.02.xml")
    titles = {}
    for c in t.iter():
        if isinstance(c.tag, str) and c.tag.endswith("unitid") and (c.text or "").strip() in invs:
            did = c.getparent()
            titles[c.text.strip()] = " ".join(" ".join(
                "".join(x.itertext()) for x in did
                if isinstance(x.tag, str) and x.tag.endswith("unittitle")).split())
    return titles


def main():
    refs, ms = references(), modes()
    eq = [i for i, v in refs.items() if i in ms and abs(ms[i][3] - v) < 1e-9]
    print(f"1. references {len(refs)}; equal {len(eq)}; unequal "
          f"{[i for i in refs if i in ms and i not in eq]}; missing {[i for i in refs if i not in ms]}")
    df = pd.DataFrame([(i, *v) for i, v in ms.items()], columns=["inv", "archive", "mode", "method", "value", "margin"])
    print(df.groupby(["archive", "mode", "method"]).size().to_string())
    inv = pd.read_csv("reports/data/phase-10-hand-print-per-region/step-15p/inventories.tsv", sep="\t",
                      dtype={"inv": str}, usecols=["inv", "silver"])
    d1 = df[df.archive == "1.01.02"].merge(inv, on="inv", how="left")
    print(pd.crosstab(d1.silver.fillna("none"), d1["mode"]).to_string())
    band = d1[(d1.value - 87.75).abs() <= 10]
    titles = ead_titles(set(band.inv))
    print(f"3. band |margin| <= 10: {len(band)} 1.01.02 volumes")
    for r in band.sort_values("value").itertuples():
        if r.value >= 86.0:
            print(f"   {r.inv} {r.mode} {r.value} silver={r.silver} title={titles.get(r.inv, '')[:60]!r}")

    sys.path.insert(0, "reports/data/phase-09-text-layer-v3-prep/planning")
    spec = importlib.util.spec_from_file_location("compare15v", D / "step-15v/compare.py")
    C = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(C)
    shared = set("3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281 455 456".split())
    res = {}
    for sub in ("1.01.02", "1.10.94"):
        r = C.compare("data/text_layer_v2_dev/15v-a", "data/text_layer_v2_dev/15w-a", sub, sub)
        keep = [m for m in r["missing"] if C.inv_of(Path(m[2:])) in shared or C.inv_of(Path(m[2:])) is None]
        res[sub] = {"equal": r["equal"], "differ": sorted(r["differ"]), "missing_shared_or_corpus": keep}
        print(f"2. {sub}: {r['equal']} equal, {len(r['differ'])} differ, {len(keep)} missing (shared/corpus)")
    (OUT / "compare16_review.json").write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
