"""Step 19g error analysis: check layout causes against BASE layout/regions (x, w, n_lines, role) and layout/lines, and
count the FN-nounit lines of the fresh sides by line_type and layout_role (all of them, not the sample).
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/layout_check.py"""
import csv
import importlib.util
from collections import Counter
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
spec = importlib.util.spec_from_file_location("e19d", A / "step-19d/entry_eval.py")
E = importlib.util.module_from_spec(spec); spec.loader.exec_module(E)
meta = {r["read_id"]: r for r in csv.DictReader(open(A / "review/fresh_entry_sides.tsv"), delimiter="\t")}
rows = list(csv.DictReader(open(A / "step-19g/fresh_lines.tsv"), delimiter="\t"))
cache = E.Cache()
cnt = Counter()
reg_cache = {}
for r in rows:
    if r["label"] == "S" and r["X4"] == "nounit":
        m = meta[r["read_id"]]
        regions_by, segs_by = cache.get(E.CLONE[m["layer"]], m["inv"])
        for reg in regions_by[(r["scan_id"], r["page"])]:
            for s in segs_by.get((r["scan_id"], r["page"], reg["layout_region_id"]), []):
                if s["line_id"] == r["line_id"]:
                    cnt[(s["line_type"], reg["layout_role"])] += 1
print("FN-nounit by (line_type, layout_role):", dict(cnt), sum(cnt.values()))
# two-column claims: region width vs page columns
for rid, inv, layer, sid, page, regid in [
    ("110", "3283", "15y-b", "NL-HaNA_1.01.02_3283_0043", "recto", "3283_0043-text_region-2578-2020-2234-328"),
    ("114", "3539", "15y-b", "NL-HaNA_1.01.02_3539_0014", "verso", "3539_0014-text_region-469-2492-2267-522"),
    ("119", "3351", "15y-b", "NL-HaNA_1.01.02_3351_0007", "verso", "3351_0007-text_region-348-845-2243-545"),
    ("121", "3265", "15y-b", "NL-HaNA_1.01.02_3265_0032", "verso", "3265_0032-text_region-360-334-2271-2894"),
    ("120", "3492", "15y-b", "NL-HaNA_1.01.02_3492_0146", "recto", "3492_0146-text_region-2901-281-2269-1404"),
    ("115", "3127", "15y-b", "NL-HaNA_1.01.02_3127_0026", "recto", "3127_0026-text_region-2506-37-2410-887"),
    ("112", "3726", "15z-b", "NL-HaNA_1.01.02_3726_0005", "recto", "3726_0005-text_region-3649-625-1111-2435"),
]:
    regions_by, segs_by = cache.get(E.CLONE[layer], inv)
    allr = regions_by[(sid, page)]
    for reg in allr:
        if reg["layout_region_id"].endswith(regid):
            ss = segs_by.get((sid, page, reg["layout_region_id"]), [])
            xs = sorted(s["x"] for s in ss)
            print(rid, reg["layout_region_id"][-40:], "x", reg["x"], "w", reg["w"], "n_lines", reg["n_lines"], "role", reg["layout_role"],
                  "change", reg["change"], "col", reg["layout_column"], "| seg x-range", xs[0], xs[-1],
                  "n regions on side", len(allr))
