import sys, csv, re
from collections import Counter
sys.path.insert(0, ".")
from archival_structures.analysis.index_links import read_volume_inputs
from archival_structures.text_layer.io import read_tsv_gz
from pathlib import Path
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
G = list(csv.DictReader(open("reports/data/phase-14-index-links/step-20c/entry_gold.tsv"), delimiter="\t"))
c = Counter(); cache = {}
for g in G:
    if g["target_scan_id"] in ("", "none"): continue
    inv, n, sc = g["inv"], int(g["target_locator"]), g["target_scan_id"]
    if inv not in cache:
        v = read_volume_inputs(L, inv)
        page = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(L/"layout"/"lines"/f"{inv}.tsv.gz")}
        lines = {}
        for r in read_tsv_gz(L/"lines"/f"{inv}.lines.tsv.gz"):
            lines.setdefault((r["scan_id"], page.get((r["scan_id"], r["line_id"]))), []).append(r["text"])
        cache[inv] = (v, lines)
    v, lines = cache[inv]
    pn = v.numbers.get((sc, "recto"), [])
    pn_hit = any(re.fullmatch(r"\D{0,3}%d\D{0,3}" % n, t.strip()) for t in pn)
    any_hit = any(re.search(r"(?<!\d)%d(?!\d)" % n, t) for t in lines.get((sc, "recto"), [])[:3] + lines.get((sc, "recto"), [])[-0:0])
    c[(g["split"], g["used_for"] or "read", g["folio_evidence"], "pn_typed" if pn_hit else ("number in first 3 lines" if any_hit else "not printed"))] += 1
for k in sorted(c): print(k, c[k])
