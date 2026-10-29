"""For single-locator entries with >= 3 words: at which folio offset d in -3..3 do most entry words appear?"""
import re, sys, csv, random
from collections import defaultdict, Counter
sys.path.insert(0, ".")
from archival_structures.text_layer.io import read_tsv_gz
from pathlib import Path
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
B = Path("data/index_links_dev/20e-a/NL-HaNA_1.01.02_index_links_v1")
WORD = re.compile(r"[^\W\d_]{6,}")
vols = [r["inv"] for r in csv.DictReader(open(B/"volumes.tsv"), delimiter="\t")]
argc = Counter(); bymeth = defaultdict(Counter); pervol = defaultdict(Counter); ex = defaultdict(list)
for inv in vols:
    fmap = {int(r["number"]): r for r in read_tsv_gz(B/"folio_map"/f"{inv}.tsv.gz")}
    page_of = {(r["scan_id"], r["line_id"]): r["page"] for r in read_tsv_gz(L/"layout"/"lines"/f"{inv}.tsv.gz")}
    sw = defaultdict(set)
    for r in read_tsv_gz(L/"lines"/f"{inv}.lines.tsv.gz"):
        pg = page_of.get((r["scan_id"], r["line_id"]))
        if pg: sw[(r["scan_id"], pg)].update(WORD.findall(r["text"].lower()))
    texts = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(L/"join"/"units"/f"{inv}.tsv.gz")}
    links = list(read_tsv_gz(B/"links"/f"{inv}.tsv.gz"))
    k = Counter((r["scan_id"], r["unit_id"]) for r in links)
    def words_at(n):
        r = fmap.get(n)
        if r is None: return None
        s = sw.get((r["front_scan_id"], r["front_page"]), set())
        if r["back_scan_id"]: s = s | sw.get((r["back_scan_id"], r["back_page"]), set())
        return s
    for r in links:
        if r["method"] == "none" or k[(r["scan_id"], r["unit_id"])] != 1: continue
        w = set(WORD.findall(texts[(r["scan_id"], r["unit_id"])].lower()))
        if len(w) < 3: continue
        n = int(r["number"]); hits = {}
        for d in range(-3, 4):
            s = words_at(n + d)
            if s is not None: hits[d] = len(w & s)
        if 0 not in hits or max(hits.values()) == 0: argc["no hit"] += 1; bymeth[r["method"]]["no hit"] += 1; continue
        best = max(hits.values()); arg = [d for d in hits if hits[d] == best]
        lab = "0" if 0 in arg else ("+1" if arg == [1] else "-1" if arg == [-1] else "other")
        if hits[0] == best and len(arg) > 1: lab = "0 (tie)"
        argc[lab] += 1; bymeth[r["method"]][lab] += 1; pervol[inv][lab] += 1
        if lab in ("+1", "-1", "other"): ex[lab].append((inv, n, r["method"], r["confidence"], hits, texts[(r["scan_id"], r["unit_id"])][-80:]))
print("all", sum(argc.values()), dict(argc))
for m, c in bymeth.items(): print(m, dict(c))
worst = sorted(pervol.items(), key=lambda kv: -(kv[1]["+1"] + kv[1]["-1"] + kv[1]["other"]) / sum(kv[1].values()))[:6]
for inv, c in worst: print("vol", inv, dict(c))
rnd = random.Random(20261230)
for lab in ("+1", "-1"):
    for x in rnd.sample(ex[lab], min(6, len(ex[lab]))): print(lab, x)
