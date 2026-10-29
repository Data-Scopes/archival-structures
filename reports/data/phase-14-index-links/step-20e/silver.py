"""Step 20e silver checks (reported only, no bar): do the entry's words / REPUBLIC resolutions sit on the target sides?
Run from the AS root: .venv/bin/python -I reports/data/phase-14-index-links/step-20e/silver.py
Reads data/index_links_dev/20e-a (dev build), the v5 layer (lines, layout/lines, join/units),
data/republic_segmentation_v1 (scoring only). Writes silver.tsv (per volume) and silver_words_links.tsv (per link)."""
import re, sys
from collections import defaultdict
from pathlib import Path
import pandas as pd
sys.path.insert(0, "."); sys.path.insert(0, str(Path(__file__).parent))
from common import *
from archival_structures.text_layer.io import read_tsv_gz

B = Path("data/index_links_dev/20e-a/NL-HaNA_1.01.02_index_links_v1")
REP = Path("data/republic_segmentation_v1/lines")
WORD = re.compile(r"[^\W\d_]{6,}")
rows_out, per_link = [], []
tot = defaultdict(float)
vols = [l.split("\t")[0] for l in open(B / "volumes.tsv").read().splitlines()[1:]]
for inv in vols:
    fmap = {int(r["number"]): r for r in read_tsv_gz(B / "folio_map" / f"{inv}.tsv.gz")}
    page_of = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(L / "layout" / "lines" / f"{inv}.tsv.gz")}
    side_words = defaultdict(set)
    for r in read_tsv_gz(L / "lines" / f"{inv}.lines.tsv.gz"):
        pg = page_of.get((r["scan_id"], r["line_id"]))
        if pg:
            side_words[(r["scan_id"], pg)].update(WORD.findall(r["text"].lower()))
    texts = unit_texts(inv)
    rep_res = None
    p = REP / f"inv={inv}" / "part-0.parquet"
    if p.exists():
        d = pd.read_parquet(p, columns=["page_id", "resolution_id"])
        rep_res = set(d.loc[d.resolution_id.notna() & (d.resolution_id != ""), "page_id"])

    def sides(n):
        r = fmap.get(n)
        if r is None:
            return None
        s = [(r["front_scan_id"], r["front_page"])]
        if r["back_scan_id"]:
            s.append((r["back_scan_id"], r["back_page"]))
        return s

    def has_res(ss):
        for sc, pg in ss:
            n = int(sc[-4:])
            ks = [2 * n - 2] if pg == "verso" else [2 * n - 1] if pg == "recto" else [2 * n - 2, 2 * n - 1]
            if any(f"{sc}-page-{k}" in rep_res for k in ks):
                return True
        return False

    c = defaultdict(float)
    for r in read_tsv_gz(B / "links" / f"{inv}.tsv.gz"):
        if r["method"] == "none":
            continue
        n = int(r["number"])
        ts, cs = sides(n), sides(n + 3)
        words = set(WORD.findall(texts[(r["scan_id"], r["unit_id"])].lower()))
        if ts is None or not words:
            continue
        tw = set().union(*(side_words.get(s, set()) for s in ts))
        c["links"] += 1
        c["target_words_all_links"] += len(words & tw); c["words_all_links"] += len(words)
        if cs is not None:
            cw = set().union(*(side_words.get(s, set()) for s in cs))
            c["links_ctrl"] += 1; c["words"] += len(words)
            c["target_words"] += len(words & tw); c["ctrl_words"] += len(words & cw)
            per_link.append([inv, r["scan_id"], r["unit_id"], n, len(words), len(words & tw), len(words & cw)])
        if rep_res is not None:
            c["rep_links"] += 1; c["rep_target"] += has_res(ts)
            if cs is not None:
                c["rep_links_ctrl"] += 1; c["rep_target_c"] += has_res(ts); c["rep_ctrl"] += has_res(cs)
    for k, v in c.items():
        tot[k] += v
    rows_out.append([inv, int(c["links"]), int(c["links_ctrl"]), int(c["words"]), int(c["target_words"]), int(c["ctrl_words"]),
                     f"{c['target_words']/c['words']:.4f}" if c["words"] else "", f"{c['ctrl_words']/c['words']:.4f}" if c["words"] else "",
                     int(c["rep_links_ctrl"]), int(c["rep_target_c"]), int(c["rep_ctrl"])])
hdr = "inv links links_with_control words target_hits control_hits target_share control_share rep_links rep_target rep_control".split()
with open(OUT / "silver.tsv", "w") as f:
    f.write("\t".join(hdr) + "\n")
    for r in rows_out:
        f.write("\t".join(map(str, r)) + "\n")
    f.write("\t".join(["TOTAL", str(int(tot["links"])), str(int(tot["links_ctrl"])), str(int(tot["words"])), str(int(tot["target_words"])),
                       str(int(tot["ctrl_words"])), f"{tot['target_words']/tot['words']:.4f}", f"{tot['ctrl_words']/tot['words']:.4f}",
                       str(int(tot["rep_links_ctrl"])), str(int(tot["rep_target_c"])), str(int(tot["rep_ctrl"]))]) + "\n")
with open(OUT / "silver_words_links.tsv", "w") as f:
    f.write("inv\tscan_id\tunit_id\tnumber\twords\ttarget_hits\tcontrol_hits\n")
    for r in per_link:
        f.write("\t".join(map(str, r)) + "\n")
print(f"words (links with control {int(tot['links_ctrl'])}): target {tot['target_words']/tot['words']:.4f} ({int(tot['target_words'])}/{int(tot['words'])}) control {tot['ctrl_words']/tot['words']:.4f} ({int(tot['ctrl_words'])})")
print(f"all located links {int(tot['links'])}: target words {tot['target_words_all_links']/tot['words_all_links']:.4f}")
print(f"REPUBLIC ({int(tot['rep_links_ctrl'])} links w/ control): target {tot['rep_target_c']/tot['rep_links_ctrl']:.4f} ({int(tot['rep_target_c'])}) control {tot['rep_ctrl']/tot['rep_links_ctrl']:.4f} ({int(tot['rep_ctrl'])}); all REPUBLIC links {int(tot['rep_links'])} target {tot['rep_target']/tot['rep_links']:.4f}")
