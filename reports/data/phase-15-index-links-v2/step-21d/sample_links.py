"""Step 21d: 20 random located v2 links (random.Random(2104)) from the 21d-a dev build, with the entry text, N and the
first lines of the target sides (front, back, opening). Run from the AS root:
.venv/bin/python -I reports/data/phase-15-index-links-v2/step-21d/sample_links.py > .../sample_links.txt"""
import random
from collections import defaultdict
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
V2 = Path("data/index_links_dev/21d-a/NL-HaNA_1.01.02_index_links_v2")
invs = [l.split("\t")[0] for l in (V2 / "volumes.tsv").read_text().splitlines()[1:]]
allrows = []
for inv in invs:
    for r in read_tsv_gz(V2 / "links" / f"{inv}.tsv.gz"):
        if r["method"] != "none":
            allrows.append((inv, r))
rng = random.Random(2104)
pick = rng.sample(allrows, 20)
cache = {}


def side_lines(inv):
    if inv not in cache:
        page_of = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(L / "layout" / "lines" / f"{inv}.tsv.gz")}
        d = defaultdict(list)
        for r in read_tsv_gz(L / "lines" / f"{inv}.lines.tsv.gz"):
            pg = page_of.get((r["scan_id"], r["line_id"]))
            if pg:
                d[(r["scan_id"], pg)].append((int(r["y"]), r["text"]))
        cache[inv] = {k: [t for _, t in sorted(v)] for k, v in d.items()}
    return cache[inv]


for i, (inv, r) in enumerate(pick, 1):
    ent = {(e["scan_id"], e["entry_id"]): e for e in read_tsv_gz(V2 / "entries" / f"{inv}.tsv.gz")}[(r["scan_id"], r["entry_id"])]
    print(f"=== {i}. inv {inv} {r['scan_id'][-4:]} {r['page']} {r['entry_id']} line {r['line_id']} N={r['number']} -> "
          f"{r['method']}/{r['confidence']} dist {r['anchor_dist']}")
    print("ENTRY:", ent["text"][:300])
    sl = side_lines(inv)
    for lab, s, p in (("front", r["front_scan_id"], r["front_page"]), ("back", r["back_scan_id"], r["back_page"]),
                      ("opening", r["opening_scan_id"], r["opening_page"])):
        if s:
            print(f"  {lab} {s[-4:]} {p}:", " / ".join(t.strip() for t in sl.get((s, p), [])[:4])[:240])
