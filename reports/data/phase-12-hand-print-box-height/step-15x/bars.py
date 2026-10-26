"""Step 15x bars (pre-registered in the brief; nothing here is tuned).

  bars.py B                  bar B: decision/Dat lines that start a main_text unit in 15wr-a and still do in 15x-a
  bars.py reads OUT.txt      A/C read file for the 20 scans of review/reads_15x.tsv: every suppressed cut with
                             context (2 lines before/after, x offset in line heights) and both builds' unit lists
                             with FULL unit text. Writes the sample list of bar A to sampleA.tsv
  bars.py diag OUT.txt       diagnostic: 10 suppressed cuts per series nouvelles / plakkaten (seed 20261028)

Run from the AS root with .venv/bin/python -I.
"""
import collections
import csv
import gzip
import random
import re
import sys
from pathlib import Path

csv.field_size_limit(10**9)
R = "NL-HaNA_1.01.02_text_layer_v4"
D = Path("reports/data/phase-12-hand-print-box-height")
OUT = D / "step-15x"
PAT = re.compile(r"^(W[AE]+R op|Dat |DAt |IS (na|goet)|Is (na|goet))")


def rd(layer, sub, inv, suffix=".tsv.gz"):
    p = f"data/text_layer_v2_dev/{layer}/1.01.02/{R}/{sub}/{inv}{suffix}"
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


class Vol:
    def __init__(self, layer, inv):
        text = {r["line_id"]: r["text"] for r in rd(layer, "lines", inv, ".lines.tsv.gz")}
        self.seg = {}
        for r in rd(layer, "types/segments", inv):
            self.seg[(r["scan_id"], r["line_id"], r["seg_idx"])] = (
                text.get(r["line_id"], "")[int(r["char_start"]):int(r["char_end"])], int(r["x"]), int(r["h"]),
                r["line_type"])
        units = rd(layer, "units/units", inv)
        self.units = [u for u in units if u["unit_type"] == "main_text"]
        by = collections.defaultdict(list)
        for r in rd(layer, "units/segments", inv):
            by[(r["scan_id"], r["unit_id"])].append((int(r["seg_order"]), (r["scan_id"], r["line_id"], r["seg_idx"])))
        self.by = {k: [x[1] for x in sorted(v)] for k, v in by.items()}
        self.page = collections.defaultdict(list)   # scan -> [(unit_order, method, [keys])]
        for u in self.units:
            self.page[u["scan_id"]].append((int(u["unit_order"]), u["unit_method"], self.by[(u["scan_id"], u["unit_id"])]))
        for v in self.page.values():
            v.sort(key=lambda t: t[0])
        self.starts = {v[0] for ps in self.page.values() for _, _, segs in ps for v in [segs]}

    def t(self, k):
        return self.seg[k][0]


def gate_rows():
    return [r for r in csv.DictReader(open(D / "step-15wr/gate.tsv"), delimiter="\t") if r["split"] in ("True", "False")]


def barB():
    tot = kept = 0
    miss = []
    for g in gate_rows():
        if not g["series"].startswith("De gedrukte"):
            continue
        a, b = Vol("15wr-a", g["inv"]), Vol("15x-a", g["inv"])
        for k in a.starts:
            if PAT.match(a.t(k)):
                tot += 1
                if k in b.starts:
                    kept += 1
                else:
                    miss.append((g["inv"], k[0], a.t(k)[:60]))
    print(f"bar B: {kept}/{tot} = {kept / tot:.4f} (pass iff >= 0.97)")
    with open(OUT / "barB_lost.tsv", "w") as f:
        f.write("inv\tscan_id\ttext\n")
        for m in miss:
            f.write("\t".join(m) + "\n")


def suppressed_for(inv_scans):
    """[(inv, scan, key)] suppressed cuts on the given {inv: set(scans) or None}."""
    out = []
    for inv, scans in inv_scans.items():
        a, b = Vol("15wr-a", inv), Vol("15x-a", inv)
        for scan, ps in sorted(b.page.items()):
            if scans is not None and scan not in scans:
                continue
            for _, _, segs in ps:
                for j in range(1, len(segs)):
                    if segs[j] in a.starts:
                        out.append((inv, scan, segs[j]))
    return out


def context(b, inv, scan, key, n=2):
    flat = [k for _, _, segs in b.page[scan] for k in segs]
    i = flat.index(key)
    colx = min(b.seg[k][1] for k in flat)
    lines = []
    for j in range(max(0, i - n), min(len(flat), i + n + 1)):
        k = flat[j]
        t, x, h, _ = b.seg[k]
        lines.append(f"{'>>' if j == i else '  '} [x+{(x - colx) / h:.1f}h] {t}")
    return "\n".join(lines)


def reads(outp):
    rows = list(csv.DictReader(open(D / "review/reads_15x.tsv"), delimiter="\t"))
    want = collections.defaultdict(set)
    for r in rows:
        want[r["inv"]].add(r["scan_id"])
    sup = suppressed_for(want)
    sample = sup
    if len(sup) > 60:
        sample = random.Random(20261029).sample(sorted(sup), 60)
    sample_set = set(sample)
    vols = {inv: (Vol("15wr-a", inv), Vol("15x-a", inv)) for inv in want}
    with open(OUT / "sampleA.tsv", "w") as f:
        f.write("inv\tscan_id\tline_id\tseg_idx\n")
        for inv, scan, k in sorted(sample):
            f.write(f"{inv}\t{scan}\t{k[1]}\t{k[2]}\n")
    with open(outp, "w") as f:
        f.write(f"suppressed on the 20 scans: {len(sup)}; sample read for bar A: {len(sample)}\n\n")
        for r in rows:
            inv, scan = r["inv"], r["scan_id"]
            a, b = vols[inv]
            f.write(f"##### {inv} {scan}\n")
            for inv_, scan_, k in sorted(sup):
                if scan_ == scan and (inv_, scan_, k) in sample_set:
                    f.write(f"--- suppressed cut{' (in sample)' if (inv_, scan_, k) in sample_set else ''}: line {k[1][-12:]}\n")
                    f.write(context(b, inv, scan, k) + "\n")
            ta = {tuple(segs) for _, _, segs in a.page[scan]}
            tb = {tuple(segs) for _, _, segs in b.page[scan]}
            for name, v, other in (("15wr-a", a, tb), ("15x-a", b, ta)):
                f.write(f"=== units {name}: {len(v.page[scan])} (units identical in the other build shown as '=')\n")
                for n, (_, meth, segs) in enumerate(v.page[scan]):
                    if tuple(segs) in other:
                        f.write(f"[{n}] = ({len(segs)} lines) {v.t(segs[0])[:40]}\n")
                    else:
                        f.write(f"[{n}] {meth} ({len(segs)} lines) " + " / ".join(v.t(k) for k in segs) + "\n")
            f.write("\n")


def diag(outp):
    gate = gate_rows()
    series = {"nouvelles": "Gedrukte nou", "plakkaten": "Gedrukte pla"}
    with open(outp, "w") as f:
        for name, pre in series.items():
            invs = {g["inv"]: None for g in gate if g["series"].startswith(pre)}
            sup = sorted(suppressed_for(invs))
            pick = sup if len(sup) <= 10 else random.Random(20261028).sample(sup, 10)
            f.write(f"##### {name}: {len(sup)} suppressed cuts, {len(pick)} read\n")
            vols = {}
            for inv, scan, k in sorted(pick):
                b = vols.setdefault(inv, Vol("15x-a", inv))
                f.write(f"--- {inv} {scan} line {k[1][-12:]}\n{context(b, inv, scan, k, 3)}\n")
            f.write("\n")


if __name__ == "__main__":
    {"B": lambda: barB(), "reads": lambda: reads(sys.argv[2]), "diag": lambda: diag(sys.argv[2])}[sys.argv[1]]()
