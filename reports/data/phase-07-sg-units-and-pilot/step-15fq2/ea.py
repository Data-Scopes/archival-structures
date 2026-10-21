"""Step 15fq2 error analysis: dump 30 formerly mixed regions (seed 21) and column-blocked pairs.

    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fq2/ea.py > ea_dump.txt
"""
import random
from collections import defaultdict
from pathlib import Path

import functools

import pandas as pd

P = Path("data/text_layer_v2_dev")
NAME = "NL-HaNA_1.10.94_text_layer_v2"


@functools.lru_cache(maxsize=None)
def rd(layer, kind, inv):
    root = P / layer / "print" / NAME
    path = root / "lines" / f"{inv}.lines.tsv.gz" if kind == "src" else root / "layout" / kind / f"{inv}.tsv.gz"
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)


SRC = {i: rd('15fq-c', 'src', i).set_index(['scan_id', 'line_id']).to_dict('index') for i in ('455', '456')}
rng = random.Random(21)
mixed = []
for inv in ("455", "456"):
    src = SRC[inv]
    lb = rd("15fq-b", "lines", inv)
    vals = defaultdict(set)
    for s, l, r in zip(lb.scan_id, lb.line_id, lb.layout_region_id):
        c = src[(s, l)]["column"]
        if c:
            vals[(s, r)].add(c)
    mixed.append((inv, sorted(k for k, v in vals.items() if len(v) > 1)))
pick = []
for inv, ms in mixed:
    for k in sorted(rng.sample(ms, 15)):
        pick.append((inv, k))
for inv, (scan, rid) in pick:
    src = SRC[inv]
    lb = rd("15fq-b", "lines", inv)
    lc = rd("15fq-c", "lines", inv)
    ids = lb[(lb.scan_id == scan) & (lb.layout_region_id == rid)]
    members = set(ids.line_id)
    nc = lc[(lc.scan_id == scan) & lc.line_id.isin(members)]
    print(f"=== {inv} {scan} old {rid}  ({len(ids)} lines)")
    for r2, g in nc.groupby("layout_region_id", sort=False):
        g = g.sort_values("layout_line_idx", key=lambda s: s.astype(int))
        cols = [src[(scan, l)]["column"] for l in g.line_id]
        print(f"  -> {r2.split('-text_region-')[-1]}  n={len(g)} cols={''.join(c or '_' for c in cols)}")
        for l, c in zip(g.line_id, cols):
            t = src[(scan, l)]
            print(f"       [{c or '_'}] x={t['x']} y={t['y']} w={t['w']} | {t['text'][:70]}")
