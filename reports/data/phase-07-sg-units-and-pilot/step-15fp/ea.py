"""Step 15fp error-analysis samples (random.Random(15), a fresh instance per sample).

Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/ea.py merges OUT.txt
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/ea.py pageside OUT.txt
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/ea.py mincov OUT.txt
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/ea.py roles OUT.txt

Samples:
* merges: 30 hand merge sets; 30 print sets, of which 10 (or all, if fewer) from 455's index
  pages and the rest from the other print pages. An index page is a scan with >= 10 lines
  containing the word "pag" (index entries end in "pag. NNN"): 455_0003-0005. Laypa's `index`
  region tag is not used: it is on resolution text throughout the volume;
* pageside: 30 opening scans with at least one line whose page differs from the old page_side;
* mincov: 30 scans with split_method=min_coverage;
* roles: 30 pages (scan, page) with at least one `extra` region.
Verdicts are in ea_verdicts.tsv next to this script.
"""

import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

from archival_structures.text_layer.io import read_tsv_gz

ROOT = Path("data/text_layer_v2_dev/15fp-a")
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}
PRINT = {"455", "456"}


def load(inv):
    for layer, (name, invs) in LAYERS.items():
        if inv in invs:
            root = ROOT / layer / name
    d = {"inv": inv}
    d["scans"] = {r["scan_id"]: r for r in read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz")}
    d["meta"] = {r["region_id"]: r for r in read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz")}
    d["lines"] = {(r["scan_id"], r["line_id"]): r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
    d["lscans"] = {r["scan_id"]: r for r in read_tsv_gz(root / "layout" / "scans" / f"{inv}.tsv.gz")}
    d["lregions"] = list(read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"))
    d["llines"] = list(read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz"))
    d["lines_by_layout"] = defaultdict(list)
    for l in d["llines"]:
        d["lines_by_layout"][(l["scan_id"], l["layout_region_id"])].append(l)
    for v in d["lines_by_layout"].values():
        v.sort(key=lambda l: int(l["layout_line_idx"]))
    return d


def all_invs():
    return [inv for _, invs in LAYERS.values() for inv in invs]


def index_scans(d):
    pag = re.compile(r"\bpag\b", re.I)
    n = Counter(sid for (sid, _), l in d["lines"].items() if pag.search(l["text"]))
    return {sid for sid, k in n.items() if k >= 10}


def fmt_line(d, l, mark=""):
    src = d["lines"][(l["scan_id"], l["line_id"])]
    return (f"    {mark:3s} x={src['x']:>5} y={src['y']:>5} w={src['w']:>5} h={src['h']:>4} "
            f"{l['page'][:1]} {l['layout_role'][:2]} | {src['text'][:90]}")


def dump_merge(d, r, out):
    sources = r["source_region_ids"].split()
    letters = {s: chr(65 + i) for i, s in enumerate(sources)}
    out.append(f"## {d['inv']} {r['scan_id']} {r['page']} {r['change']} role={r['layout_role']} "
               f"col={r['layout_column']} box={r['x']},{r['y']},{r['w']},{r['h']}")
    for s in sources:
        m = d["meta"][s]
        out.append(f"   {letters[s]}: box={m['x']},{m['y']},{m['w']},{m['h']} n_lines={m['n_lines']} tag={m['region_tag']}")
    lines = d["lines_by_layout"][(r["scan_id"], r["layout_region_id"])]
    marks = [letters[l["region_id"]] for l in lines]
    majority = Counter(marks).most_common(1)[0][0]
    keep = set()
    for i, m in enumerate(marks):
        if m != majority:
            keep.update(range(max(0, i - 3), min(len(lines), i + 4)))
    if len(lines) <= 14:
        keep = set(range(len(lines)))
    prev = -1
    for i in sorted(keep):
        if i != prev + 1:
            out.append("    ...")
        out.append(fmt_line(d, lines[i], marks[i]))
        prev = i
    if prev != len(lines) - 1:
        out.append(f"    ... ({len(lines)} lines)")
    out.append("")


def merges(path):
    pools = {"hand": [], "print": [], "455index": []}
    data = {}
    for inv in all_invs():
        d = data[inv] = load(inv)
        idx = index_scans(d) if inv == "455" else set()
        for r in d["lregions"]:
            if r["change"] in ("merged", "merged+split"):
                key = "hand" if inv not in PRINT else ("455index" if r["scan_id"] in idx else "print")
                pools[key].append((inv, r))
    out = [f"pools: { {k: len(v) for k, v in pools.items()} }", ""]
    n_index = min(10, len(pools["455index"]))
    for key, n in (("hand", 30), ("455index", n_index), ("print", 30 - n_index)):
        sample = random.Random(15).sample(pools[key], min(n, len(pools[key])))
        out.append(f"# {key} ({len(sample)} of {len(pools[key])})")
        for i, (inv, r) in enumerate(sample, 1):
            out.append(f"[{key} {i}]")
            dump_merge(data[inv], r, out)
    Path(path).write_text("\n".join(out) + "\n")


def profile(d, sid):
    s = d["scans"][sid]
    W = int(s["page_w"])
    cov = np.zeros(W, int)
    for (sc, _), l in d["lines"].items():
        if sc == sid and l["text"].strip():
            cov[int(l["x"]):int(l["x"]) + max(1, int(l["w"]))] += 1
    sep = d["lscans"][sid]["separation_x"]
    sep = float(sep) if sep else -1
    cells = []
    for i in range(80):
        c = cov[int(i * W / 80)]
        bar = "|" if abs((i + 0.5) * W / 80 - sep) < W / 160 else " "
        cells.append(bar + ("." if c == 0 else str(min(9, c // 5))))
    return "".join(cells)


def dump_scan(d, sid, out):
    ls = d["lscans"][sid]
    s = d["scans"][sid]
    W = int(s["page_w"])
    sep = float(ls["separation_x"])
    out.append(f"## {d['inv']} {sid} W={W} H={s['page_h']} method={ls['split_method']} "
               f"sep={sep:.0f} ({sep / W:.3f}) crossing={ls['n_lines_crossing']}")
    out.append("   " + profile(d, sid))
    regs = [r for r in d["lregions"] if r["scan_id"] == sid and r["layout_role"] != "empty"]
    for r in sorted(regs, key=lambda r: (r["page"] != "verso", int(r["x"]))):
        lines = d["lines_by_layout"][(sid, r["layout_region_id"])]
        first = d["lines"][(sid, lines[0]["line_id"])]["text"][:50]
        last = d["lines"][(sid, lines[-1]["line_id"])]["text"][:50]
        out.append(f"   {r['page'][:1]} col={r['layout_column']} {r['layout_role'][:2]} "
                   f"x={int(r['x']) / W:.3f}-{(int(r['x']) + int(r['w'])) / W:.3f} y={r['y']:>5} "
                   f"n={r['n_lines']:>3} {r['change'][:6]} | {first!r} … {last!r}")
    near = [l for l in d["llines"] if l["scan_id"] == sid]
    near = [l for l in near if abs(int(d["lines"][(sid, l["line_id"])]["x"]) - sep) < 0.08 * W
            or abs(int(d["lines"][(sid, l["line_id"])]["x"]) + int(d["lines"][(sid, l["line_id"])]["w"]) - sep) < 0.04 * W]
    old = {"left": "v", "right": "r"}
    for l in sorted(near, key=lambda l: int(d["lines"][(sid, l["line_id"])]["y"]))[:25]:
        src = d["lines"][(sid, l["line_id"])]
        flag = "≠" if old.get(src["page_side"]) != l["page"][:1] else " "
        out.append(f"    {flag} x={src['x']:>5}-{int(src['x']) + int(src['w']):>5} y={src['y']:>5} "
                   f"{l['page'][:1]} {l['layout_role'][:2]} | {src['text'][:70]}")
    out.append("")


def scans_sample(path, which):
    pool, data = [], {}
    for inv in all_invs():
        d = data[inv] = load(inv)
        if which == "mincov":
            sids = [s for s, r in d["lscans"].items() if r["split_method"] == "min_coverage"]
        else:
            old = {"left": "verso", "right": "recto"}
            sids = sorted({l["scan_id"] for l in d["llines"] if l["page"] != "single" and
                           old.get(d["lines"][(l["scan_id"], l["line_id"])]["page_side"]) != l["page"]})
        pool += [(inv, s) for s in sids]
    sample = random.Random(15).sample(pool, min(30, len(pool)))
    out = [f"pool {len(pool)}; by inventory {dict(Counter(i for i, _ in pool))}", ""]
    for i, (inv, sid) in enumerate(sample, 1):
        out.append(f"[{which} {i}]")
        dump_scan(data[inv], sid, out)
    Path(path).write_text("\n".join(out) + "\n")


def roles(path):
    pool, data = [], {}
    for inv in all_invs():
        d = data[inv] = load(inv)
        pages = sorted({(r["scan_id"], r["page"]) for r in d["lregions"] if r["layout_role"] == "extra"})
        pool += [(inv, p) for p in pages]
    sample = random.Random(15).sample(pool, min(30, len(pool)))
    out = [f"pool {len(pool)}; by inventory {dict(Counter(i for i, _ in pool))}", ""]
    for i, (inv, (sid, page)) in enumerate(sample, 1):
        d = data[inv]
        W = int(d["scans"][sid]["page_w"])
        out.append(f"[roles {i}] {inv} {sid} {page} W={W}")
        regs = [r for r in d["lregions"] if r["scan_id"] == sid and r["page"] == page
                and r["layout_role"] in ("main", "extra")]
        for r in sorted(regs, key=lambda r: (int(r["layout_column"]), int(r["y"]))):
            lines = d["lines_by_layout"][(sid, r["layout_region_id"])]
            chars = sum(len(d["lines"][(sid, l["line_id"])]["text"]) for l in lines)
            out.append(f"  col={r['layout_column']} {r['layout_role']:5s} x={r['x']:>5}-{int(r['x']) + int(r['w']):>5} "
                       f"y={r['y']:>5}-{int(r['y']) + int(r['h']):>5} n={r['n_lines']:>3} chars={chars}")
            show = lines if r["layout_role"] == "extra" else lines[:2] + lines[-1:]
            for l in show[:8]:
                src = d["lines"][(sid, l["line_id"])]
                out.append(f"        x={src['x']:>5} y={src['y']:>5} w={src['w']:>5} | {src['text'][:80]}")
        out.append("")
    Path(path).write_text("\n".join(out) + "\n")


if __name__ == "__main__":
    kind, path = sys.argv[1], sys.argv[2]
    {"merges": merges, "roles": roles}.get(kind, lambda p: scans_sample(p, kind))(path)
