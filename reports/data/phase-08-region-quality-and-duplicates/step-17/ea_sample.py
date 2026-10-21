"""Error-analysis sample for step 17 (read-only). Run from the LotA root with LotA's venv:

    venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/step-17/ea_sample.py \
        output/integrity/as-step17/1.01.02-duplicate-candidates.tsv <out.tsv>

Samples (seed 20261021): A = 30 non-adjacent rows outside inventories 638-643; B = 20 adjacent
rows that are not among the 23 documented re-shoot rows; C = the rows with no scan_number_b.
Writes sample, row index and the first 150 chars of both page texts. Reads only the staged
paragraph TSVs; writes only <out.tsv>.
"""
import sys
from collections import defaultdict

import pandas as pd
from archival_structures.model.inventory_number import InventoryNumber

from archival_language.config import load_config
from archival_language.corpus.integrity import _page_text, page_units
from archival_language.corpus.tsv_source import TSVParagraphSource
from archival_language.ead.adapters import load_adapter

SEED = 20261021
RESHOOTS = [('3777', 246), ('3779', 65), ('3780', 138), ('3781', 352), ('3788', 246), ('3860', 91),
            ('3767', 65), ('3773', 276), ('3803', 187), ('3816', 386), ('3822', 390), ('3826', 200),
            ('3838', 235)]

df = pd.read_csv(sys.argv[1], sep="\t", dtype={"inventory_number": str})
documented = df.apply(lambda r: any(r.inventory_number == i and {r.scan_number_a, r.scan_number_b} == {a, a + 1}
                                    for i, a in RESHOOTS), axis=1)
inv_num = df.inventory_number.astype(int)
A = df[(~df.adjacent) & ~inv_num.between(638, 643)].sample(30, random_state=SEED)
B = df[df.adjacent & ~documented].sample(20, random_state=SEED)
C = df[df.scan_number_b.isna()]
print(len(df), "rows; documented", int(documented.sum()), "; pool A", int(((~df.adjacent) & ~inv_num.between(638, 643)).sum()),
      "; pool B", int((df.adjacent & ~documented).sum()), "; C", len(C))
cfg = load_config()
src = TSVParagraphSource("1.01.02", config=cfg)
ad = load_adapter(cfg.archives["1.01.02"])
want = defaultdict(list)
parts = [("A", A), ("B", B), ("C", C)]
for name, part in parts:
    for idx, r in part.iterrows():
        want[r.inventory_number].append((name, idx, r))
texts = {}
for inv, items in want.items():
    regions = list(src.iter_regions(InventoryNumber(inv)))
    pages = page_units(regions, adapter=ad)
    by_id = {r.region_id: r for r in regions}
    ids = {r.page_id_a for _, _, r in items} | {r.page_id_b for _, _, r in items}
    for p in pages:
        if p.page_id in ids:
            texts[p.page_id] = _page_text(p, by_id)
rows = []
for name, part in parts:
    for idx, r in part.iterrows():
        t = lambda s: texts.get(s, "<missing>")[:150].replace("\t", " ").replace("\n", " ")
        rows.append({"sample": name, "row": idx, "inventory_number": r.inventory_number, "page_id_a": r.page_id_a,
                     "page_id_b": r.page_id_b, "scan_a": r.scan_number_a, "scan_b": r.scan_number_b,
                     "n_a": r.n_shingles_a, "n_b": r.n_shingles_b, "jaccard_exact": round(r.jaccard_exact, 3),
                     "adjacent": r.adjacent, "text_a": t(r.page_id_a), "text_b": t(r.page_id_b)})
pd.DataFrame(rows).to_csv(sys.argv[2], sep="\t", index=False)
print("wrote", sys.argv[2], len(rows))
