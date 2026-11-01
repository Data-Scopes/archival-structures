"""Phase-16 planning (2026-10-31): 'Table of contents' scan gold in the 22 GLOBALISE inventories, the dev/test split
for the index steps (random.Random(20261031).sample of the sorted 22, 11 dev), and a peek at DEV TOC text only
(v1 text layer regions, first 12 regions of the first two TOC scans of each dev volume). Test volumes are not opened.

Run from the AS root: .venv/bin/python reports/data/phase-16-voc-generalisation/planning/toc_split_and_peek.py
"""
import glob
import random
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
GT = "ground_truth/NL-HaNA_1.04.02/globalise_document_segmentation/source/*.csv"


def main():
    rows = []
    for f in sorted(glob.glob(GT)):
        inv = Path(f).name.split(" ")[0]
        d = pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str, keep_default_na=False)
        for i, r in d.iterrows():
            t = r["Type of non-document page"].strip().lower()
            b = r["TANAP Boundaries"].strip().upper()
            rows.append(dict(inv=inv, row=i + 1, scan_id=r["Scan File_Name"].strip(), toc=int(t.startswith("table")),
                             start=int(b in ("START", "END/START"))))
    g = pd.DataFrame(rows)
    invs = sorted(g.inv.unique())
    dev = set(random.Random(20261031).sample(invs, 11))
    g["split"] = g.inv.map(lambda i: "dev" if i in dev else "test")
    g.to_csv(HERE / "toc_gold.tsv", sep="\t", index=False)
    print(g.groupby("split").agg(invs=("inv", "nunique"), scans=("scan_id", "size"), toc=("toc", "sum"),
                                 starts=("start", "sum")))
    print("dev:", sorted(dev))
    peek = []
    for inv in sorted(dev):
        sc = g[(g.inv == inv) & (g.toc == 1)].scan_id.tolist()[:2]
        if not sc:
            continue
        reg = pd.read_csv(f"data/voc_text_layer_v1/regions/{inv}.tsv.gz", sep="\t", dtype=str, quoting=3,
                          keep_default_na=False, usecols=["scan_id", "region_tag", "region_text"])
        for s in sc:
            r = reg[reg.scan_id.str.startswith(s)]
            for t in r.head(12).itertuples():
                peek.append(dict(inv=inv, scan_id=s, tag=t.region_tag, text=t.region_text[:160]))
    pd.DataFrame(peek).to_csv(HERE / "toc_peek_dev.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
