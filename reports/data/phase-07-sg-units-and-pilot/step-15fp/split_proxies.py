"""Step 15fp: proxies for the two page-split failures found in the error analysis.

* margin_near_gutter: openings with >= 1 `extra` region on the verso whose right edge lies within
  0.04·page_w left of separation_x. SG hand pages have their margin column on the left, so an
  extra region at the verso's right edge is most likely the recto's margin cut off (F07-15fp.4).
* page_missed: openings where |separation_x/page_w - gutter_rel| > 0.08, or where one page holds
  < 5% of the scan's text characters while the other holds two main columns.

Run from the AS root: .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/split_proxies.py
"""

import json
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

ROOT = Path("data/text_layer_v2_dev/15fp-a")
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}

print("| inv | openings with text | method | margin near gutter (verso) | far from gutter_rel | one page < 5% chars |")
for layer, (name, invs) in LAYERS.items():
    root = ROOT / layer / name
    stats = json.loads((root / "layout" / "stats.json").read_text())
    for inv in invs:
        g = stats[inv]["gutter_rel"]
        scans = {r["scan_id"]: r for r in read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz")}
        lscans = {r["scan_id"]: r for r in read_tsv_gz(root / "layout" / "scans" / f"{inv}.tsv.gz")}
        text = {(l["scan_id"], l["line_id"]): len(l["text"]) for l in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        chars = defaultdict(Counter)
        main_cols = defaultdict(set)
        for l in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz"):
            chars[l["scan_id"]][l["page"]] += text[(l["scan_id"], l["line_id"])]
            if l["layout_role"] == "main":
                main_cols[(l["scan_id"], l["page"])].add(l["layout_column"])
        near, far, missed = set(), set(), set()
        for r in read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"):
            s = lscans[r["scan_id"]]
            if s["page_mode"] != "opening" or s["split_method"] == "none":
                continue
            W, sep = int(scans[r["scan_id"]]["page_w"]), float(s["separation_x"])
            if r["page"] == "verso" and r["layout_role"] == "extra":
                right = int(r["x"]) + int(r["w"])
                if sep - 0.04 * W <= right <= sep + 1:
                    near.add(r["scan_id"])
        openings = [sid for sid, s in lscans.items() if s["page_mode"] == "opening" and s["split_method"] != "none"]
        for sid in openings:
            W, sep = int(scans[sid]["page_w"]), float(lscans[sid]["separation_x"])
            if abs(sep / W - g) > 0.08:
                far.add(sid)
            total = sum(chars[sid].values())
            for page, other in (("verso", "recto"), ("recto", "verso")):
                if total and chars[sid][page] < 0.05 * total and len(main_cols[(sid, other)]) >= 2:
                    missed.add(sid)
        methods = Counter(lscans[s]["split_method"] for s in openings)
        print(f"| {inv} | {len(openings)} | gap {methods['gap']}, min_cov {methods['min_coverage']} | "
              f"{len(near)} ({len(near) / max(1, len(openings)):.0%}) | {len(far)} | {len(missed)} |")
