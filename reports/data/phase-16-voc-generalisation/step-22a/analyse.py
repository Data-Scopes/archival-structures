"""Step 22a error-analysis samples (seed 20261031). Writes samples.txt next to this file.
Run from the AS root: .venv/bin/python reports/data/phase-16-voc-generalisation/step-22a/analyse.py"""
import csv
import json
import random
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
LAYER = ROOT / "data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5"
PLAN = ROOT / "reports/data/phase-16-voc-generalisation/planning"
GLOB = set("1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split())
invs = (OUT / "inventories.txt").read_text().split()
out = []


def p(*a):
    out.append(" ".join(str(x) for x in a))


modes = {i: json.load(open(LAYER / "mode" / f"{i}.json")) for i in invs}
# 1. print volumes
pv = [i for i in invs if modes[i]["mode"] == "print"]
p("## print volumes", len(pv), pv)
for i in pv[:10]:
    lines = [r for r in read_tsv_gz(LAYER / "lines" / f"{i}.lines.tsv.gz") if len(r["text"]) > 20]
    rng = random.Random(20261031)
    p(f"### {i} value={modes[i]['value']} n_chars={modes[i]['n_chars']} minority={modes[i]['scan_minority_share']}")
    for sid in sorted({r["scan_id"] for r in lines})[:0]:
        pass
    scans = sorted({r["scan_id"] for r in lines})
    for sid in rng.sample(scans, min(2, len(scans))):
        p(" scan", sid)
        for r in [r for r in lines if r["scan_id"] == sid][:6]:
            p("   ", r["text"][:120])
# 2. sides disagreements
gm = {r["inv"]: r["layout"] for r in csv.DictReader(open(PLAN / "gm_split.tsv"), delimiter="\t")}
dis = []
for r in csv.DictReader(open(OUT / "sides.tsv"), delimiter="\t"):
    if r["agrees"] == "0":
        dis.append(r)
p("## sides disagreeing", len(dis))
for r in dis[:10]:
    rows = list(read_tsv_gz(LAYER / "scans" / f"{r['inv']}.scans.tsv.gz"))
    rng = random.Random(20261031)
    smp = rng.sample(rows, min(3, len(rows)))
    p(f"### {r['inv']} {r['gm_layout']} single={r['single']} verso={r['verso']} recto={r['recto']}",
      [(s["scan_id"][-4:], s["page_w"], s["page_h"]) for s in smp])
# 3. page numbers
rng = random.Random(20261031)
text = {}
pn_all, pn_lp = [], []
for i in invs:
    lt = {}
    for r in read_tsv_gz(LAYER / "lines" / f"{i}.lines.tsv.gz"):
        lt[r["line_id"]] = (r["text"], r["region_id"])
    lp = {r["region_id"] for r in read_tsv_gz(LAYER / "regions_meta" / f"{i}.tsv.gz") if r["region_tag"] == "page-number"}
    pn = set()
    for r in read_tsv_gz(LAYER / "types/segments" / f"{i}.tsv.gz"):
        if r["line_type"] == "page_number":
            pn.add(r["line_id"])
            pn_all.append((i, r["scan_id"], r["line_id"], r["type_method"], lt.get(r["line_id"], ("", ""))[0], r["page"]))
    for lid, (t, rg) in lt.items():
        if rg in lp and lid not in pn:
            pn_lp.append((i, lid, t))
p("## page_number segments", len(pn_all))
for x in rng.sample(pn_all, 30):
    p("  ", x[0], x[1][-4:], x[3], x[5], repr(x[4][:60]))
p("## laypa page-number lines without a segment", len(pn_lp))
for x in rng.sample(pn_lp, min(20, len(pn_lp))):
    p("  ", x[0], repr(x[2][:60]))
# 4. units on 10 GM sides
rng = random.Random(20261031)
gmi = [i for i in invs if i not in GLOB]
cands = []
for i in rng.sample(gmi, 40):
    for r in read_tsv_gz(LAYER / "units/units" / f"{i}.tsv.gz"):
        pass
    cnt = {}
    for r in read_tsv_gz(LAYER / "lines" / f"{i}.lines.tsv.gz"):
        cnt[r["scan_id"], r["page_side"]] = cnt.get((r["scan_id"], r["page_side"]), 0) + 1
    big = [k for k, v in cnt.items() if v >= 10]
    if big:
        cands.append((i, rng.choice(big)))
    if len(cands) >= 10:
        break
p("## units sample", cands)
for i, (sid, side) in cands:
    jt = {(r["scan_id"], r["unit_id"]): r for r in read_tsv_gz(LAYER / "join/units" / f"{i}.tsv.gz")}
    p(f"### {i} {sid[-4:]} {side}")
    for r in read_tsv_gz(LAYER / "units/units" / f"{i}.tsv.gz"):
        if r["scan_id"] == sid:
            j = jt.get((sid, r["unit_id"]))
            p(f"   {r['unit_order']} {r['unit_type']} {r['unit_method']} p={r['page']} y={r['y']} lines={r['n_lines']} :: {(j['text'] if j else '')[:110]!r}")
# 5. word breaks
rng = random.Random(20261031)
brk = []
for i in rng.sample(invs, 60):
    prev = None
    rows = list(read_tsv_gz(LAYER / "lines" / f"{i}.lines.tsv.gz"))
    for k, r in enumerate(rows):
        if r["text"].rstrip().endswith("¬") and k + 1 < len(rows):
            brk.append((i, r["text"], rows[k + 1]["text"], r["scan_id"], r["line_id"]))
p("## ¬-ending lines in sampled invs", len(brk))
smp = rng.sample(brk, min(10, len(brk)))
for x in smp:
    jt = [r for r in read_tsv_gz(LAYER / "join/units" / f"{x[0]}.tsv.gz") if r["scan_id"] == x[3]]
    first = x[1].rstrip()[-25:]
    stem = x[1].rstrip()[:-1].split()[-1] if x[1].rstrip()[:-1].split() else ""
    hit = [r["text"] for r in jt if stem and stem in r["text"]]
    kept = any(stem + "¬" in h for h in hit)
    p("  ", x[0], x[3][-4:], repr(x[1][-30:]), "|", repr(x[2][:30]), "| stem", repr(stem), "| stem+¬ still in join text:", kept,
      "| ctx:", repr(next((h[max(0, h.find(stem) - 5): h.find(stem) + 40] for h in hit), "")))
(OUT / "samples.txt").write_text("\n".join(out) + "\n")
print(len(out), "lines")
