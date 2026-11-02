"""Step 23a (behaviour 4): draw the link lines from the merged TOC labels.  Run from the AS root after
gold/toc_labels.tsv exists (inv scan_id scan_num label list_type note reader):
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/sample_lines.py \
        [--toc gold/toc_labels.tsv] [--out gold/link_lines.tsv] [--per-volume 8] [--seed 20261101]
Volumes used: those with a front register TOC (>= 1 scan with label `toc` and list_type `register` among scans 1-60).
Candidates: lines (from lines/) on scans labelled `toc` with list_type `register`, with >= 2 non-space characters.
Draws min(8, n) per volume with random.Random(f"{seed}-{inv}") from the candidates in (scan_num, file order).
Writes link_lines.tsv: inv scan_id page line_id line_idx text gold_folios status note (the last three empty,
filled by readers). Row grain: one row per sampled line. `page` = layout/lines page (single/verso/recto)."""
import argparse, csv, gzip, os, random
from pathlib import Path

D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
LAYER = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
csv.field_size_limit(10**9)


def tsv(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


ap = argparse.ArgumentParser()
ap.add_argument("--toc", default=str(D / "gold/toc_labels.tsv"))
ap.add_argument("--out", default=str(D / "gold/link_lines.tsv"))
ap.add_argument("--per-volume", type=int, default=8)
ap.add_argument("--seed", default="20261101")
a = ap.parse_args()
lab = list(csv.DictReader(open(a.toc), delimiter="\t"))
reg = {}
for r in lab:
    if r["label"] == "toc" and r["list_type"] == "register":
        reg.setdefault(r["inv"], set()).add(r["scan_id"])
front = sorted({r["inv"] for r in lab if r["label"] == "toc" and r["list_type"] == "register" and int(r["scan_num"]) <= 60})
rows = []
for inv in front:
    page = {(r["scan_id"], r["line_id"]): r["page"] for r in tsv(LAYER / "layout" / "lines" / f"{inv}.tsv.gz")}
    num = {r["scan_id"]: int(r["scan_num"]) for r in tsv(LAYER / "scans" / f"{inv}.scans.tsv.gz")}
    cand = []
    for r in tsv(LAYER / "lines" / f"{inv}.lines.tsv.gz"):
        t = r["text"].replace("\t", " ").replace("\n", " ").strip()
        if r["scan_id"] in reg[inv] and len("".join(t.split())) >= 2:
            cand.append((num[r["scan_id"]], len(cand), r["scan_id"], page.get((r["scan_id"], r["line_id"]), "single"),
                         r["line_id"], r["line_idx"], t))
    cand.sort(key=lambda c: (c[0], c[1]))
    pick = cand if len(cand) <= a.per_volume else random.Random(f"{a.seed}-{inv}").sample(cand, a.per_volume)
    for c in sorted(pick, key=lambda c: (c[0], c[1])):
        rows.append((inv, c[2], c[3], c[4], c[5], c[6], "", "", ""))
    print(inv, "candidates", len(cand), "drawn", len(pick))
Path(a.out).parent.mkdir(parents=True, exist_ok=True)
with open(a.out, "w") as f:
    f.write("inv\tscan_id\tpage\tline_id\tline_idx\ttext\tgold_folios\tstatus\tnote\n")
    for r in rows:
        f.write("\t".join(r) + "\n")
os.chmod(a.out, os.stat(a.out).st_mode | 0o020)
print(len(rows), "lines from", len(front), "volumes ->", a.out)
