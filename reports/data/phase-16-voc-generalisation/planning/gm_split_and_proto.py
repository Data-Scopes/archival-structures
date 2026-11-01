"""Phase-16 planning (2026-10-31): the Generale Missiven folio gold, its dev/test split, and a dev-only prototype.

1. Anchors: one (inv, folio, scan, kind) row per begin and per end of each missive in the GM overview source CSV.
2. Split by inventory, stratified by scan layout (Enkel / Dubbel / Dubbel/Enkel), random.Random(20261031):
   dev = floor(n/2) of each stratum (sorted list, then sample), test = the rest. Written to gm_split.tsv.
   Anchors (1171 excluded, deduplicated on (inv, folio, scan)) -> gm_anchors.tsv.
3. Prototype on DEV ONLY (test is never scored here): scan-level anchors from the Laypa `page-number` regions
   of shared-data/voc_text_layer_v1 (analysis.page_numbering defaults), longest increasing chain, interpolation.
   strict hit = predicted scan == gold scan; leaf hit = gold scan in {p, p+1}. Written to gm_proto_dev.tsv.

Run from the AS root: .venv/bin/python reports/data/phase-16-voc-generalisation/planning/gm_split_and_proto.py
"""
import csv  # noqa: F401  (quoting=3 is csv.QUOTE_NONE)
import random
import re
from pathlib import Path

import numpy as np
import pandas as pd

from archival_structures.analysis.page_numbering import (
    page_number_anchors, anchors_array, main_chain, interpolate_folio_to_scan, numbering_mode)

HERE = Path(__file__).parent
SRC = Path("ground_truth/NL-HaNA_1.04.02/generale_missiven_overview/source/Overzicht van Generale Missiven in 1_04_02.csv")
V1 = Path("data/voc_text_layer_v1/regions")
SEED = 20261031


def inv_of(name):
    m = re.match(r"NL-HaNA_1\.04\.02_([0-9]+[A-Z]*)_0*(\d+)", str(name))
    return (m.group(1), int(m.group(2))) if m else (None, None)


def gold_anchors():
    df = pd.read_csv(SRC, sep=None, engine="python")
    df.columns = [c.strip("﻿") for c in df.columns]
    rows = []
    for r in df.itertuples(index=False):
        d = dict(zip(df.columns, r))
        lay = d["Enkele of dubbele pagina op scan"]
        for kind, fcol, scol, ncol in (("begin", "Beginfolio", "Beginscan", "Bestandsnaam van eerste scan"),
                                       ("end", "Eindfolio", "Eindscan", "Bestandsnaam van laatste scan")):
            inv, _ = inv_of(d["Bestandsnaam van eerste scan"])
            f, s = pd.to_numeric(d[fcol], errors="coerce"), pd.to_numeric(d[scol], errors="coerce")
            if inv is None or pd.isna(f) or pd.isna(s):
                continue
            rows.append(dict(inv=inv, gm_id=d["ID"], kind=kind, folio=int(f), scan=int(s), layout=lay))
    return pd.DataFrame(rows)


def split(invs_layout):
    rng = random.Random(SEED)
    dev = set()
    for lay, g in sorted(invs_layout.groupby("layout")):
        invs = sorted(g.inv.unique())
        dev |= set(rng.sample(invs, len(invs) // 2))
    out = invs_layout.drop_duplicates("inv")[["inv", "layout"]].copy()
    out["split"] = np.where(out.inv.isin(dev), "dev", "test")
    return out.sort_values("inv")


def main():
    a = gold_anchors()
    lay = a.groupby("inv").layout.agg(lambda s: s.mode().iloc[0]).reset_index()
    sp = split(lay)
    sp.to_csv(HERE / "gm_split.tsv", sep="\t", index=False)
    print(sp.groupby(["split", "layout"]).size())
    a = a.merge(sp[["inv", "split"]], on="inv")
    # 1171: the gold names scans NL-HaNA_1.04.02_1171_*, the PageXML exists only as 1171A/1171B -> excluded.
    a = a[a.inv != "1171"]
    # one anchor per (inv, folio, scan); duplicate and nested registrations repeat pairs. gm_id/kind of the first.
    a = a.sort_values(["inv", "scan", "folio", "gm_id", "kind"]).drop_duplicates(["inv", "folio", "scan"])
    a.to_csv(HERE / "gm_anchors.tsv", sep="\t", index=False)
    print("anchors by split:", a.groupby("split").size().to_dict(), "inventories:", a.groupby("split").inv.nunique().to_dict())
    dev = a[a.split == "dev"]
    out = []
    for inv, g in dev.groupby("inv"):
        p = V1 / f"{inv}.tsv.gz"
        if not p.exists():
            out += [dict(r, pred=None, strict=0, leaf=0, chain_len=0, note="no v1 regions") for r in g.to_dict("records")]
            continue
        reg = pd.read_csv(p, sep="\t", usecols=["scan_id", "region_tag", "region_text"], dtype=str,
                          quoting=3, keep_default_na=False)
        anc = page_number_anchors(reg)
        if len(anc) < 3:
            out += [dict(r, pred=None, strict=0, leaf=0, chain_len=len(anc), note="<3 anchors") for r in g.to_dict("records")]
            continue
        chain, _ = main_chain(anchors_array(anc))
        mode, slope = numbering_mode(chain)
        for r in g.to_dict("records"):
            s, dist, inside = interpolate_folio_to_scan(r["folio"], chain, extrap=True)
            p_ = int(np.floor(s + 0.5))
            out.append(dict(r, pred=p_, strict=int(p_ == r["scan"]), leaf=int(r["scan"] in (p_, p_ + 1)),
                            chain_len=len(chain), inside=int(inside), mode=mode, slope=round(slope, 3), note=""))
    o = pd.DataFrame(out)
    o.to_csv(HERE / "gm_proto_dev.tsv", sep="\t", index=False)
    print(o.groupby("layout")[["strict", "leaf"]].agg(["mean", "sum", "size"]))
    print("all dev:", o.strict.mean().round(4), o.leaf.mean().round(4), len(o))
    print(o.note.value_counts())
    print(o.drop_duplicates("inv").groupby(["layout", "mode"]).size())


if __name__ == "__main__":
    main()
