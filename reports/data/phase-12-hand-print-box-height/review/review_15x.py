"""Review of step 15x (Opus 5.5, 2026-10-26): own re-computation, independent of step-15x/bars.py.

  review_15x.py compare        decompressed-byte compare 15wr-a vs 15x-a, grouped by inventory class
  review_15x.py bars           suppressed cuts, bar B (segment and whole-line text), bar A sample and diagnostic
                               draws re-drawn from the definitions in P19 / the brief, checked against the step files
  review_15x.py extra SEED N   draw N further suppressed cuts per series (nouvelles, plakkaten) with SEED and write a
                               read file with 3 lines of context either side and the 15x-a unit around the cut
  review_15x.py lost N SEED    read file for N of the 279 lost bar-B starts (context, both builds' units)

Run from the AS root with .venv/bin/python -I.
"""
import collections
import csv
import gzip
import json
import random
import re
import sys
from pathlib import Path

csv.field_size_limit(10**9)
D = Path("reports/data/phase-12-hand-print-box-height")
S = D / "step-15x"
REV = D / "review"
L = Path("data/text_layer_v2_dev")
R = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
PAT = re.compile(r"^(W[AE]+R op|Dat |DAt |IS (na|goet)|Is (na|goet))")
DROP = {"seconds"}


def tsv(p):
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


def content(p):
    b = gzip.open(p).read() if p.suffix == ".gz" else p.read_bytes()
    if p.suffix == ".json":
        def strip(o):
            if isinstance(o, dict):
                return {k: strip(v) for k, v in o.items() if k not in DROP and not k.startswith("elapsed")}
            if isinstance(o, list):
                return [strip(v) for v in o]
            return o
        return json.dumps(strip(json.loads(b)), sort_keys=True)
    return b


def compare():
    out = collections.Counter()
    diffs = collections.defaultdict(set)
    for sub in ("1.01.02", "1.10.94"):
        a_root, b_root = L / "15wr-a" / sub, L / "15x-a" / sub
        modes = {}
        for m in a_root.glob("*/mode/*.json"):
            modes[m.stem] = json.loads(m.read_text())["mode"]
        fa = {p.relative_to(a_root) for p in a_root.rglob("*") if p.is_file()}
        fb = {p.relative_to(b_root) for p in b_root.rglob("*") if p.is_file()}
        for rel in sorted(fa | fb):
            if rel.name in ("progress.log", "status.tsv", "manifest.json", "run_v4.log"):
                continue
            inv = rel.name.split(".")[0]
            cls = (f"{sub}:{modes[inv]}" if inv in modes else f"{sub}:corpus")
            if sub == "1.10.94" and inv in modes:
                cls = "1.10.94:inv"
            kind = "mode" if "mode" in rel.parts else rel.parts[1] if len(rel.parts) > 2 else rel.name
            if rel not in fa or rel not in fb:
                out[(cls, "only one side")] += 1
                diffs[cls].add(("only", str(rel)))
                continue
            same = content(a_root / rel) == content(b_root / rel)
            out[(cls, "equal" if same else "differ")] += 1
            if not same:
                diffs[cls].add(("/".join(rel.parts[1:-1]) or rel.name, ""))
            if kind == "mode":
                out[("mode files", "equal" if same else "differ")] += 1
    for k, v in sorted(out.items()):
        print(k, v)
    for cls, s in diffs.items():
        kinds = collections.Counter(x[0] for x in s)
        print(cls, dict(kinds) if len(kinds) < 30 else len(s))


class Vol:
    def __init__(self, layer, inv, need_text=True):
        self.inv = inv
        base = L / layer / R
        self.line = {}
        if need_text:
            for r in tsv(base / "lines" / f"{inv}.lines.tsv.gz"):
                self.line[(r["scan_id"], r["line_id"])] = r["text"]
        self.seg = {}
        for r in tsv(base / "types/segments" / f"{inv}.tsv.gz"):
            self.seg[(r["scan_id"], r["line_id"], r["seg_idx"])] = r
        main = {(u["scan_id"], u["unit_id"]): u for u in tsv(base / "units/units" / f"{inv}.tsv.gz")}
        by = collections.defaultdict(list)
        for r in tsv(base / "units/segments" / f"{inv}.tsv.gz"):
            by[(r["scan_id"], r["unit_id"])].append((int(r["seg_order"]), (r["scan_id"], r["line_id"], r["seg_idx"])))
        self.units = {}   # (scan, unit_id) -> (order, type, method, [keys])
        for k, v in by.items():
            u = main[k]
            self.units[k] = (int(u["unit_order"]), u["unit_type"], u["unit_method"], [x[1] for x in sorted(v)])
        self.starts = {v[3][0] for v in self.units.values() if v[1] == "main_text"}
        self.unit_of = {key: k for k, v in self.units.items() for key in v[3]}

    def stext(self, key):
        r = self.seg[key]
        return self.line.get(key[:2], "")[int(r["char_start"]):int(r["char_end"])]

    def page(self, scan):
        return sorted((v for k, v in self.units.items() if k[0] == scan), key=lambda v: v[0])


def gate():
    return [r for r in csv.DictReader(open(D / "step-15wr/gate.tsv"), delimiter="\t") if r["split"] in ("True", "False")]


def bars():
    g = gate()
    ser = {r["inv"]: r["series"] for r in g}
    want = collections.defaultdict(set)
    for r in csv.DictReader(open(REV / "reads_15x.tsv"), delimiter="\t"):
        want[r["inv"]].add(r["scan_id"])
    sup_all, per_series = [], collections.Counter()
    b_tot = b_kept = b_tot_line = b_kept_line = 0
    lost = []
    for inv in sorted(ser):
        res = ser[inv].startswith("De gedrukte")
        a, b = Vol("15wr-a", inv, True), Vol("15x-a", inv, True)
        gone = a.starts - b.starts
        new = b.starts - a.starts
        assert not new, (inv, len(new))
        for k in gone:
            assert b.units[b.unit_of[k]][1] == "main_text"
            sup_all.append((inv, k[0], k))
        per_series[ser[inv][:12]] += len(gone)
        if res:
            for k in a.starts:
                if PAT.match(a.stext(k)):
                    b_tot += 1
                    b_kept += k in b.starts
                    if k not in b.starts:
                        lost.append((inv, k[0], a.stext(k)[:60]))
                if PAT.match(a.line.get(k[:2], "")):
                    b_tot_line += 1
                    b_kept_line += k in b.starts
    print("suppressed total", len(sup_all), dict(per_series))
    print(f"bar B (segment text): {b_kept}/{b_tot} = {b_kept / b_tot:.4f}")
    print(f"bar B (whole-line text): {b_kept_line}/{b_tot_line} = {b_kept_line / b_tot_line:.4f}")
    step_lost = list(csv.DictReader(open(S / "barB_lost.tsv"), delimiter="\t"))
    print("lost equal to step file:", sorted((r["inv"], r["scan_id"], r["text"]) for r in step_lost) == sorted(lost))
    step_sup = sum(1 for _ in open(S / "suppressed.tsv")) - 1
    print("suppressed.tsv rows", step_sup)
    on20 = sorted(t for t in sup_all if t[1] in want.get(t[0], ()))
    sample = random.Random(20261029).sample(on20, 60) if len(on20) > 60 else on20
    mine = sorted((s[0], s[1], s[2][1], s[2][2]) for s in sample)
    step = sorted(tuple(r.values()) for r in csv.DictReader(open(S / "sampleA.tsv"), delimiter="\t"))
    print("bar A: on 20 scans", len(on20), "sample equal to sampleA.tsv:", mine == step)
    for name, pre in (("nouvelles", "Gedrukte nou"), ("plakkaten", "Gedrukte pla")):
        sup = sorted(t for t in sup_all if ser[t[0]].startswith(pre))
        pick = random.Random(20261028).sample(sup, 10)
        print("diag", name, len(sup), sorted((p[0], p[1], p[2][1][-12:]) for p in pick))
    with open(REV / "suppressed_15x_review.tsv", "w") as f:
        f.write("inv\tseries\tscan_id\tline_id\tseg_idx\n")
        for inv, scan, k in sorted(sup_all):
            f.write(f"{inv}\t{ser[inv]}\t{scan}\t{k[1]}\t{k[2]}\n")
    v = list(csv.DictReader(open(S / "verdicts.tsv"), delimiter="\t"))
    c = collections.Counter((r["bar"], r["verdict"]) for r in v)
    pg = collections.Counter(r["verdict"] for r in csv.DictReader(open(S / "pages.tsv"), delimiter="\t"))
    print("verdicts", dict(c), "pages", dict(pg))


def ctx(b, scan, key, n=3):
    flat = [k for v in b.page(scan) if v[1] == "main_text" for k in v[3]]
    i = flat.index(key)
    colx = min(int(b.seg[k]["x"]) for k in flat)
    rows = []
    for j in range(max(0, i - n), min(len(flat), i + n + 1)):
        k = flat[j]
        r = b.seg[k]
        rows.append(f"{'>>' if j == i else '  '} [x+{(int(r['x']) - colx) / max(int(r['h']), 1):.1f}h] {b.stext(k)}")
    return "\n".join(rows)


def extra(seed, n):
    rows = list(csv.DictReader(open(REV / "suppressed_15x_review.tsv"), delimiter="\t"))
    diag = {r["line_id"][-12:].lstrip(".") for r in csv.DictReader(open(S / "verdicts.tsv"), delimiter="\t")
            if r["bar"].startswith("diag")}
    out = open(REV / f"reads_15x_extra_{seed}.txt", "w")
    for name, pre in (("nouvelles", "Gedrukte nou"), ("plakkaten", "Gedrukte pla")):
        sup = sorted((r["inv"], r["scan_id"], r["line_id"], r["seg_idx"]) for r in rows
                     if r["series"].startswith(pre) and r["line_id"][-12:] not in diag)
        pick = sorted(random.Random(seed).sample(sup, n))
        out.write(f"##### {name}: {len(sup)} suppressed cuts (diagnostic 10 excluded), {n} read, seed {seed}\n")
        vols = {}
        for i, (inv, scan, line, seg) in enumerate(pick, 1):
            b = vols.setdefault(inv, Vol("15x-a", inv))
            k = (scan, line, seg)
            u = b.units[b.unit_of[k]]
            out.write(f"--- {name[0]}{i:02d} {inv} {scan} line {line[-12:]} (unit {u[2]}, {len(u[3])} lines)\n")
            out.write(ctx(b, scan, k) + "\n")
            out.write("UNIT: " + " / ".join(b.stext(x) for x in u[3]) + "\n")
    out.close()


def lost(n, seed):
    rows = list(csv.DictReader(open(S / "barB_lost.tsv"), delimiter="\t"))
    pick = sorted(random.Random(seed).sample(range(len(rows)), n))
    out = open(REV / f"reads_15x_lost_{seed}.txt", "w")
    vols = {}
    for j, i in enumerate(pick, 1):
        r = rows[i]
        inv = r["inv"]
        if inv not in vols:
            vols[inv] = (Vol("15wr-a", inv), Vol("15x-a", inv))
        a, b = vols[inv]
        # the text can repeat on a page ("WAAR op geen resolutie is gevallen."): take a start that is lost
        key = next(k for k in sorted(a.starts) if k[0] == r["scan_id"] and a.stext(k)[:60] == r["text"]
                   and k not in b.starts)
        ua, ub = a.units[a.unit_of[key]], b.units[b.unit_of[key]]
        pos = ub[3].index(key)
        prev = a.units[a.unit_of[ub[3][pos - 1]]]
        out.write(f"--- L{j:02d} {inv} {r['scan_id']} pos {pos} in 15x unit; prev 15wr unit {prev[2]} "
                  f"({len(prev[3])} lines); lost unit {ua[2]}\n")
        out.write(ctx(b, r["scan_id"], key) + "\n")
        out.write("15x UNIT: " + " / ".join(b.stext(x) for x in ub[3][:12]) + (" ..." if len(ub[3]) > 12 else "") + "\n")
    out.close()


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "compare":
        compare()
    elif cmd == "bars":
        bars()
    elif cmd == "extra":
        extra(int(sys.argv[2]), int(sys.argv[3]))
    elif cmd == "lost":
        lost(int(sys.argv[2]), int(sys.argv[3]))
