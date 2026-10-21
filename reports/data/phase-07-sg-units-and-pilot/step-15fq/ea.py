"""Step 15fq error-analysis samples (random.Random(16), a fresh instance per sample).

Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fq/ea.py KIND OUT.txt [LAYER_DIR]
KIND: merges | pagesplit | devscans | furniture | colsplit | guard | roles
LAYER_DIR defaults to data/text_layer_v2_dev/15fq-a. All samples exclude every scan of 15fp's
ea_verdicts.tsv (the dev set); `devscans` dumps the 5 + 1 doubtful dev scans on purpose.

* merges: 30 hand and 30 print layout regions whose change contains `merged`;
* pagesplit: 30 openings where a line's page differs from 15fp (20 from 3349-3353, 10 from
  11182/11281) and 20 random main_edge openings without a difference;
* furniture: 30 regions with furniture=1 (random over all 16 inventories);
* colsplit: 20 print regions with change containing split_at_column;
* guard: 15 hand and 15 print guard-blocked pairs (re-run of the layout of the sampled scans);
* roles: the main char share and extra/page are in acceptance.py; roles are not re-read.
"""

import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from archival_structures.analysis.page_structure import (
    LayoutParams, choose_gutter_rule, fit_gutter, inventory_line_width_rel, is_opening)
from archival_structures.text_layer import layout as lay
from archival_structures.text_layer.io import read_tsv_gz

PARENT = Path("data/text_layer_v2_dev")
LAYER = Path(sys.argv[3]) if len(sys.argv) > 3 else PARENT / "15fq-a"
HERE = Path(__file__).parent
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}
PRINT = {"455", "456"}
HAND_MAIN_EDGE = ["3349", "3350", "3351", "3352", "3353", "11182", "11281"]


def dev_scans():
    v = pd.read_csv(HERE.parent / "step-15fp" / "ea_verdicts.tsv", sep="\t", dtype=str, keep_default_na=False)
    return set(v.scan)          # like "3350_0625"


DEV = dev_scans()


def short(scan_id):
    inv = scan_id.split("_")[2]
    return inv + "_" + scan_id.split("_")[3]


def rd(path):
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)


def root_of(inv):
    for layer, (name, invs) in LAYERS.items():
        if inv in invs:
            return LAYER / layer / name


def load(inv):
    root = root_of(inv)
    d = {"inv": inv, "root": root}
    d["scans"] = {r["scan_id"]: r for r in read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz")}
    d["lines"] = {(r["scan_id"], r["line_id"]): r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
    d["lscans"] = {r["scan_id"]: r for r in read_tsv_gz(root / "layout" / "scans" / f"{inv}.tsv.gz")}
    d["lregions"] = list(read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"))
    d["llines"] = list(read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz"))
    d["by_layout"] = defaultdict(list)
    for l in d["llines"]:
        d["by_layout"][(l["scan_id"], l["layout_region_id"])].append(l)
    for v in d["by_layout"].values():
        v.sort(key=lambda l: int(l["layout_line_idx"]))
    return d


def all_invs():
    return [inv for _, invs in LAYERS.values() for inv in invs]


def fmt(d, l, mark="", page=True):
    s = d["lines"][(l["scan_id"], l["line_id"])]
    col = s.get("column", "")
    return (f"    {mark:2s}{col:1s} x={s['x']:>5} y={s['y']:>5} w={s['w']:>5} h={s['h']:>4} "
            f"{l['page'][:1] if page else ''} | {s['text'][:70]}")


# --- merges ---------------------------------------------------------------------------------

def dump_merge(d, r, out):
    lines = d["by_layout"][(r["scan_id"], r["layout_region_id"])]
    sources = sorted({l["region_id"] for l in lines})
    letters = {s: chr(65 + i) for i, s in enumerate(sources)}
    marks = [letters[l["region_id"]] for l in lines]
    cols = Counter(d["lines"][(l["scan_id"], l["line_id"])].get("column", "") for l in lines)
    out.append(f"## {d['inv']} {short(r['scan_id'])} {r['page']} {r['change']} role={r['layout_role']} "
               f"box={r['x']},{r['y']},{r['w']},{r['h']} n={len(lines)} sources={len(sources)} cols={dict(cols)}")
    majority = Counter(marks).most_common(1)[0][0]
    keep = set()
    for i, m in enumerate(marks):
        if m != majority:
            keep.update(range(max(0, i - 2), min(len(lines), i + 3)))
    if len(lines) <= 14:
        keep = set(range(len(lines)))
    if not keep:
        keep = set(range(min(10, len(lines))))
    prev = -1
    for i in sorted(keep)[:40]:
        if i != prev + 1:
            out.append("    ...")
        out.append(fmt(d, lines[i], marks[i], page=False))
        prev = i
    if prev != len(lines) - 1:
        out.append(f"    ... ({len(lines)} lines)")
    out.append("")


def merges(path):
    pools = {"hand": [], "print": []}
    data = {}
    for inv in all_invs():
        d = data[inv] = load(inv)
        for r in d["lregions"]:
            if "merged" in r["change"].split("+") and short(r["scan_id"]) not in DEV:
                pools["print" if inv in PRINT else "hand"].append((inv, r))
    out = [f"pools: { {k: len(v) for k, v in pools.items()} }", ""]
    for key in ("hand", "print"):
        sample = random.Random(16).sample(pools[key], min(30, len(pools[key])))
        out.append(f"# {key} ({len(sample)} of {len(pools[key])})")
        for i, (inv, r) in enumerate(sample, 1):
            out.append(f"[{key} {i}]")
            dump_merge(data[inv], r, out)
    Path(path).write_text("\n".join(out) + "\n")


# --- page split -----------------------------------------------------------------------------

def profile(d, sid):
    import numpy as np
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


def dump_scan(d, sid, out, moved=()):
    ls = d["lscans"][sid]
    s = d["scans"][sid]
    W = int(s["page_w"])
    sep = float(ls["separation_x"])
    out.append(f"## {d['inv']} {short(sid)} W={W} H={s['page_h']} method={ls['split_method']} "
               f"sep={sep:.0f} ({sep / W:.3f}) moved_vs_15fp={len(moved)}")
    out.append("   " + profile(d, sid))
    regs = [r for r in d["lregions"] if r["scan_id"] == sid and r["layout_role"] != "empty"]
    for r in sorted(regs, key=lambda r: (r["page"] != "verso", int(r["x"]))):
        lines = d["by_layout"][(sid, r["layout_region_id"])]
        first = d["lines"][(sid, lines[0]["line_id"])]["text"][:40]
        last = d["lines"][(sid, lines[-1]["line_id"])]["text"][:40]
        out.append(f"   {r['page'][:1]} {r['layout_role'][:2]} x={int(r['x']) / W:.3f}-{(int(r['x']) + int(r['w'])) / W:.3f} "
                   f"y={r['y']:>5} n={r['n_lines']:>3} f={r['furniture']} | {first!r} … {last!r}")
    ll = [l for l in d["llines"] if l["scan_id"] == sid]
    near = []
    for l in ll:
        src = d["lines"][(sid, l["line_id"])]
        x, w = int(src["x"]), int(src["w"])
        if (l["line_id"] in moved) or abs(x - sep) < 0.08 * W or abs(x + w - sep) < 0.04 * W:
            near.append((l, src))
    near.sort(key=lambda t: (t[0]["line_id"] not in moved, int(t[1]["y"])))
    for l, src in near[:max(30, len(moved))]:
        flag = "*" if l["line_id"] in moved else " "
        out.append(f"    {flag} x={src['x']:>5}-{int(src['x']) + int(src['w']):>5} y={src['y']:>5} "
                   f"{l['page'][:1]} {l['layout_role'][:2]} | {src['text'][:60]}")
    out.append("")


def page_diffs(inv):
    p = HERE / f"page_diff_{inv}.tsv"
    df = rd(p)
    m = defaultdict(set)
    for s, l in zip(df.scan_id, df.line_id):
        m[s].add(l)
    return m


def pagesplit(path):
    data, pool_diff, pool_same = {}, [], []
    for inv in HAND_MAIN_EDGE:
        d = data[inv] = load(inv)
        diffs = page_diffs(inv)
        for sid, ls in d["lscans"].items():
            if short(sid) in DEV or ls["split_method"] != "main_edge":
                continue
            (pool_diff if sid in diffs else pool_same).append((inv, sid))
    sel = []
    for group, n in ((["3349", "3350", "3351", "3352", "3353"], 20), (["11182", "11281"], 10)):
        pool = [p for p in pool_diff if p[0] in group]
        sel += random.Random(16).sample(pool, min(n, len(pool)))
    same = random.Random(16).sample(pool_same, min(20, len(pool_same)))
    out = [f"diff pool {Counter(i for i, _ in pool_diff)}; same pool {len(pool_same)}", ""]
    for i, (inv, sid) in enumerate(sel, 1):
        out.append(f"[diff {i}]")
        dump_scan(data[inv], sid, out, page_diffs(inv)[sid])
    for i, (inv, sid) in enumerate(same, 1):
        out.append(f"[same {i}]")
        dump_scan(data[inv], sid, out)
    Path(path).write_text("\n".join(out) + "\n")


def devscans(path):
    out = []
    for key in ("11182_0709", "11182_0306", "11182_0538", "11182_0524", "11281_0923", "11281_0292"):
        inv = key.split("_")[0]
        d = load(inv)
        sid = [s for s in d["lscans"] if short(s) == key][0]
        out.append(f"[dev {key}]")
        dump_scan(d, sid, out, page_diffs(inv)[sid])
    Path(path).write_text("\n".join(out) + "\n")


# --- furniture ------------------------------------------------------------------------------

def furniture(path):
    pool, data = [], {}
    for inv in all_invs():
        d = data[inv] = load(inv)
        pool += [(inv, r) for r in d["lregions"] if r["furniture"] == "1" and short(r["scan_id"]) not in DEV]
    sample = random.Random(16).sample(pool, min(30, len(pool)))
    out = [f"pool {len(pool)}; by inventory {dict(Counter(i for i, _ in pool))}", ""]
    for i, (inv, r) in enumerate(sample, 1):
        d = data[inv]
        H = int(d["scans"][r["scan_id"]]["page_h"])
        W = int(d["scans"][r["scan_id"]]["page_w"])
        lines = d["by_layout"][(r["scan_id"], r["layout_region_id"])]
        text = " / ".join(d["lines"][(r["scan_id"], l["line_id"])]["text"] for l in lines)
        # context: the nearest non-furniture region above on the same page
        out.append(f"[furn {i}] {inv} {short(r['scan_id'])} {r['page']} role={r['layout_role']} "
                   f"x={int(r['x']) / W:.3f} y={int(r['y']) / H:.3f} w={int(r['w']) / W:.3f} n={r['n_lines']} | {text!r}")
    Path(path).write_text("\n".join(out) + "\n")


# --- column split ---------------------------------------------------------------------------

def colsplit(path):
    pool, data = [], {}
    for inv in sorted(PRINT):
        d = data[inv] = load(inv)
        pool += [(inv, r) for r in d["lregions"]
                 if "split_at_column" in r["change"].split("+") and short(r["scan_id"]) not in DEV]
    sample = random.Random(16).sample(pool, min(20, len(pool)))
    out = [f"pool {len(pool)}", ""]
    for i, (inv, r) in enumerate(sample, 1):
        d = data[inv]
        lines = d["by_layout"][(r["scan_id"], r["layout_region_id"])]
        W = int(d["scans"][r["scan_id"]]["page_w"])
        cols = Counter(d["lines"][(r["scan_id"], l["line_id"])]["column"] for l in lines)
        out.append(f"[colsplit {i}] {inv} {short(r['scan_id'])} {r['page']} {r['change']} role={r['layout_role']} "
                   f"colval={r['column_value']!r} cols={dict(cols)} n={len(lines)} x={int(r['x']) / W:.3f}-{(int(r['x']) + int(r['w'])) / W:.3f}")
        for l in lines[:4] + lines[-2:] if len(lines) > 6 else lines:
            out.append(fmt(d, l))
        out.append("")
    Path(path).write_text("\n".join(out) + "\n")


# --- guard ----------------------------------------------------------------------------------

def rerun_blocked(d, inv, sid):
    """Re-run layout_scan for one scan with the inventory's gutter rule; return blocked pairs."""
    root = d["root"]
    params = LayoutParams.from_dict(json.loads((root / "layout" / "params.json").read_text()))
    scans = list(read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz"))
    metas = defaultdict(list)
    for r in read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz"):
        metas[r["scan_id"]].append(r)
    lines_by_region = defaultdict(list)
    for l in d["lines"].values():
        lines_by_region[l["region_id"]].append(l)
    nonempty = {rid for rid, ls in lines_by_region.items() if any(l["text"].strip() for l in ls)}
    landscape = []
    for s in scans:
        if is_opening(int(s["page_w"] or 0), int(s["page_h"] or 0), params):
            boxes = [(int(l["x"]), int(l["w"])) for r in metas[s["scan_id"]]
                     if r["region_id"] in nonempty for l in lines_by_region[r["region_id"]]]
            landscape.append((int(s["page_w"]), boxes))
    gutter_rel, n_gap = fit_gutter(landscape, params)
    rule = choose_gutter_rule(n_gap, sum(bool(b) for _, b in landscape), params)
    med = inventory_line_width_rel(landscape)
    blocked = []
    row = [s for s in scans if s["scan_id"] == sid][0]
    lay.layout_scan(row, metas[sid], lines_by_region, gutter_rel, params, gutter_rule=rule,
                    med_w_rel=med, blocked_out=blocked)
    return blocked


def guard(path):
    out, pools = [], {"hand": [], "print": []}
    data = {}
    for inv in all_invs():
        d = data[inv] = load(inv)
        for sid, s in d["lscans"].items():
            if int(s["n_guard_blocked"]) > 0 and short(sid) not in DEV:
                pools["print" if inv in PRINT else "hand"].append((inv, sid, int(s["n_guard_blocked"])))
    out.append(f"scan pools: { {k: (len(v), sum(x[2] for x in v)) for k, v in pools.items()} }")
    out.append("")
    for key in ("hand", "print"):
        # sample scans weighted by nothing: random scans, one random pair each
        sample = random.Random(16).sample(pools[key], min(15, len(pools[key])))
        for i, (inv, sid, n) in enumerate(sample, 1):
            d = data[inv]
            blocked = rerun_blocked(d, inv, sid)
            page, a, b = random.Random(16 + i).choice(blocked)
            out.append(f"[guard {key} {i}] {inv} {short(sid)} {page} scan has {n} blocked pairs")
            # layout region ids of a and b are source/layout ids pre-merge: show their lines from the
            # source table by region_id (sources) when they exist, else by layout table
            for tag, rid in (("S/L", a), ("S/L", b)):
                ls = d["by_layout"].get((sid, rid))
                if ls is None:
                    ls = [l for l in d["llines"] if l["scan_id"] == sid and l["region_id"] == rid]
                out.append(f"   {tag} region {rid[-40:]} n={len(ls)}")
                for l in ls[:6]:
                    out.append(fmt(d, l))
                if len(ls) > 6:
                    out.append(f"    ... ({len(ls)} lines)")
            out.append("")
    Path(path).write_text("\n".join(out) + "\n")


if __name__ == "__main__":
    kind, path = sys.argv[1], sys.argv[2]
    {"merges": merges, "pagesplit": pagesplit, "devscans": devscans, "furniture": furniture,
     "colsplit": colsplit, "guard": guard}[kind](path)
