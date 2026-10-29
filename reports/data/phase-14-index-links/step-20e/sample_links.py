"""Step 20e error analysis: 20 random located links of the dev build (random.Random(20261202)) with target-side lines
that share a >=6-letter word with the entry. Run from the AS root: .venv/bin/python -I <this file>."""
import random, re, sys
from pathlib import Path
sys.path.insert(0, "."); sys.path.insert(0, str(Path(__file__).parent))
from common import *
B = Path("data/index_links_dev/20e-a/NL-HaNA_1.01.02_index_links_v1")
WORD = re.compile(r"[^\W\d_]{6,}")
links = []
for inv in [l.split("\t")[0] for l in open(B / "volumes.tsv").read().splitlines()[1:]]:
    for r in read_tsv_gz(B / "links" / f"{inv}.tsv.gz"):
        if r["method"] != "none":
            links.append((inv, r))
print(len(links), "located links")
sel = random.Random(20261202).sample(links, 20)
cache = {}
for inv, r in sel:
    if inv not in cache:
        pg = {(x["scan_id"], x["line_id"]): x["page"] for x in read_tsv_gz(L / "layout" / "lines" / f"{inv}.tsv.gz")}
        d = {}
        for x in read_tsv_gz(L / "lines" / f"{inv}.lines.tsv.gz"):
            p = pg.get((x["scan_id"], x["line_id"]))
            if p: d.setdefault((x["scan_id"], p), []).append(x["text"])
        cache[inv] = (d, unit_texts(inv))
    d, ut = cache[inv]
    t = ut[(r["scan_id"], r["unit_id"])]
    words = set(WORD.findall(t.lower()))
    print(f"\n[{inv}] #{r['number']} {r['method']}/{r['confidence']} front={r['front_scan_id'][-4:]}/{r['front_page']} back={r['back_scan_id'][-4:]}/{r['back_page']}")
    print("  ENTRY:", t[:170].replace("\n", " "))
    for s in [(r["front_scan_id"], r["front_page"])] + ([(r["back_scan_id"], r["back_page"])] if r["back_scan_id"] else []):
        hits = [l for l in d.get(s, []) if words & set(WORD.findall(l.lower()))]
        print(f"  {s[0][-4:]}/{s[1]}: {len(d.get(s, []))} lines, {len(hits)} sharing a word:", " | ".join(h[:70] for h in hits[:3]))
