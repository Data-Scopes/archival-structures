"""Planning check of step 20d's LOO floor under the brief's definition: scan positions from the scans table, anchors =
first page-number segment matching ^\\D{0,3}(\\d{1,4})\\D{0,3}$ per side outside the front index and flagged sides, recto
anchors only (foliation), main chain; each inner chain anchor's position predicted from the chain without it
(np.interp, round half up). Run from the AS root with .venv/bin/python -I."""
import math, re, sys
from pathlib import Path
import numpy as np
sys.path.insert(0, ".")
from archival_structures.analysis.page_numbering import longest_increasing_chain
from archival_structures.text_layer.io import read_tsv_gz
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
NUM = re.compile(r"^\D{0,3}(\d{1,4})\D{0,3}$")
rows = [l.rstrip("\n").split("\t") for l in open(Path(__file__).parent / "front_index_volumes.tsv")][1:]
tot = ok = 0; per = []
for r in rows:
    inv, last = r[0], int(r[4])
    scans = [s["scan_id"] for s in read_tsv_gz(L / "scans" / f"{inv}.scans.tsv.gz")]
    pos = {s: i + 1 for i, s in enumerate(scans)}
    pages = list(read_tsv_gz(L / "layout" / "pages" / f"{inv}.tsv.gz"))
    excl = {(p["scan_id"], p["page"]) for i, p in enumerate(pages) if i <= last or p["index_side"] == "1"}
    text = {(l["scan_id"], l["line_id"]): l["text"] for l in read_tsv_gz(L / "lines" / f"{inv}.lines.tsv.gz")}
    first = {}
    for s in read_tsv_gz(L / "types" / "segments" / f"{inv}.tsv.gz"):
        k = (s["scan_id"], s["page"])
        if s["line_type"] != "page_number" or k in excl or k in first or s["page"] != "recto":
            continue
        m = NUM.match(text.get((s["scan_id"], s["line_id"]), "")[int(s["char_start"]):int(s["char_end"])].strip())
        if m:
            first[k] = int(m.group(1))
    anc = sorted((pos[k[0]], n) for k, n in first.items())
    ch = np.array([anc[i] for i in longest_increasing_chain([a[1] for a in anc])], float)
    n_ok = n = 0
    for i in range(1, len(ch) - 1):
        red = np.delete(ch, i, axis=0)
        p = math.floor(float(np.interp(ch[i, 1], red[:, 1], red[:, 0])) + 0.5)
        n += 1; n_ok += p == int(ch[i, 0])
    tot += n; ok += n_ok; per.append((inv, n, round(n_ok / n, 4) if n else None))
print("pooled", ok, tot, round(ok / tot, 4)); print(sorted(per, key=lambda x: x[2])[:6])
