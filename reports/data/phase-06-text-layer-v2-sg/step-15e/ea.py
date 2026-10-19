"""Step 15e error analysis helpers (seed 16).

Usage: .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15e/ea.py CMD RUN_DIR [OUT]

RUN_DIR is a split run, e.g. data/text_layer_v2_dev/15e-a/print/NL-HaNA_1.10.94_text_layer_v2.

  splits     sample 15 hyphen + 15 bigram splits per inventory; write original, L, R and the
             neighbours the splitter used (the nearest line above R in column 2, the nearest
             line below L in column 1) to OUT
  unsplit    sample candidate lines that are still unsplit (all if <= 30 per inventory)
  columns    per inventory: shares of column 1/2/x/blank over participating lines, lines on the
             wrong side of the side midpoint, and 10 random pages with splits, compact view
  check      D15_DIR as third argument (15d's layer dir with the unsplit lines): text conservation,
             geometry, column coverage and line-count identities of the split run (criteria 3, 4)
  compare    NOTEBOOK_DIR as third argument: for the sampled splits of RUN_DIR, whether the
             notebook-parameter run split the same original at the same place
"""
import csv
import gzip
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(1 << 24)
INVS = ("455", "456")


def read_rows(run, inv):
    with gzip.open(Path(run) / "lines" / f"{inv}.lines.tsv.gz", "rt", newline="") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE))
    for r in rows:
        for k in ("x", "y", "w", "h"):
            r[k] = int(r[k]) if r[k] != "" else 0
    return rows


def pairs(rows):
    by_orig = defaultdict(dict)
    for r in rows:
        if r["split_from"]:
            by_orig[(r["scan_id"], r["split_from"])][r["line_id"][-1]] = r
    return {k: v for k, v in by_orig.items() if "L" in v and "R" in v}


def neighbours(rows_by_page, L, R):
    page = rows_by_page[(L["scan_id"], L["page_side"])]
    above = [r for r in page if r["column"] == "2" and r["y"] < R["y"] and r is not R]
    below = [r for r in page if r["column"] == "1" and r["y"] > L["y"] and r is not L]
    a = max(above, key=lambda r: r["y"]) if above else None
    b = min(below, key=lambda r: r["y"]) if below else None
    return a, b


def pages_index(rows):
    idx = defaultdict(list)
    for r in rows:
        idx[(r["scan_id"], r["page_side"])].append(r)
    return idx


def sample_splits(run, rng):
    out = []
    for inv in INVS:
        rows = read_rows(run, inv)
        idx = pages_index(rows)
        prs = pairs(rows)
        for kind in ("hyphen", "bigram"):
            keys = sorted(k for k, v in prs.items() if v["L"]["edit_flags"] == f"split:{kind}")
            for k in rng.sample(keys, min(15, len(keys))):
                out.append((inv, kind, k, prs[k], idx))
    return out


def fmt_split(inv, kind, key, pr, idx):
    L, R = pr["L"], pr["R"]
    a, b = neighbours(None or idx, L, R) if False else neighbours(idx, L, R)
    lines = [f"## {inv} {kind} {key[0][-9:]} {L['page_side']} box=({L['x']},{L['y']},{R['x']+R['w']-L['x']},{L['h']})",
             f"  ORIG : {L['text_raw']} {R['text_raw']}",
             f"  L    : {L['text_raw']}",
             f"  R    : {R['text_raw']}",
             f"  above R (col2): ...{a['text_raw'][-45:] if a else None}",
             f"  below L (col1): {b['text_raw'][:45] + '...' if b else None}"]
    return "\n".join(lines)


def cmd_splits(run, out):
    rng = random.Random(16)
    txt = []
    for i, item in enumerate(sample_splits(run, rng)):
        txt.append(f"[{i + 1}] " + fmt_split(*item))
    Path(out).write_text("\n".join(txt) + "\n")


def cmd_unsplit(run, out, params_name="split_model"):
    rng = random.Random(16)
    txt = []
    n = 0
    for inv in INVS:
        rows = read_rows(run, inv)
        model = json.load(open(Path(run) / "split_model" / f"{inv}.json"))
        idx = pages_index(rows)
        cands = []
        for r in rows:
            p = model["params"].get(r["page_side"])
            if p and len(r["text_raw"]) >= p["default_line_num_chars"] and r["w"] >= p["min_merged_width"] \
                    and r["column"] and not r["split_from"]:
                cands.append(r)
        txt.append(f"### {inv}: {len(cands)} unsplit candidates among participating lines")
        for r in rng.sample(cands, min(30, len(cands))):
            page = idx[(r["scan_id"], r["page_side"])]
            above = [q for q in page if q["y"] < r["y"] and q is not r]
            below = [q for q in page if q["y"] > r["y"] and q is not r]
            a = max(above, key=lambda q: q["y"]) if above else None
            b = min(below, key=lambda q: q["y"]) if below else None
            n += 1
            txt.append(f"[{n}] {inv} {r['scan_id'][-9:]} {r['page_side']} x={r['x']} y={r['y']} w={r['w']} "
                       f"conf={r['conf']} col={r['column']} flags={r['edit_flags']!r}\n"
                       f"  TEXT : {r['text_raw']}\n  above: {a['text_raw'][:60] if a else None}\n"
                       f"  below: {b['text_raw'][:60] if b else None}")
    Path(out).write_text("\n".join(txt) + "\n")


def cmd_columns(run, out):
    rng = random.Random(16)
    txt = []
    for inv in INVS:
        rows = read_rows(run, inv)
        total = len(rows)
        cnt = defaultdict(int)
        wrong = 0
        pages = pages_index(rows)
        for r in rows:
            cnt[r["column"] or "blank"] += 1
        for (scan, side), pr in pages.items():
            lab = [r for r in pr if r["column"] in ("1", "2")]
            if not lab:
                continue
            xs = sorted((r["x"] + r["w"] / 2) for r in lab)
            mid = (min(r["x"] for r in lab) + max(r["x"] + r["w"] for r in lab)) / 2
            for r in lab:
                c = r["x"] + r["w"] / 2
                if (r["column"] == "1" and r["x"] > mid) or (r["column"] == "2" and r["x"] + r["w"] < mid):
                    wrong += 1
        txt.append(f"### {inv}: lines {total}; column counts {dict(cnt)}; "
                   f"lines wholly on the wrong side of the page-side midpoint: {wrong}")
        split_pages = sorted({(r["scan_id"], r["page_side"]) for r in rows if r["split_from"]})
        for scan, side in rng.sample(split_pages, min(10, len(split_pages))):
            pr = sorted(pages[(scan, side)], key=lambda r: (r["y"], r["x"]))
            txt.append(f"#### {inv} {scan[-9:]} {side}: {len(pr)} lines")
            first = next(i for i, r in enumerate(pr) if r["split_from"])
            for r in pr[max(0, first - 4):first + 10]:
                txt.append(f"  col={r['column'] or '-'} x={r['x']:5d} w={r['w']:5d} y={r['y']:5d} "
                           f"{r['edit_flags']:12s} {r['text_raw'][:60]}")
    Path(out).write_text("\n".join(txt) + "\n")


def cmd_compare(run, out, nb_run):
    rng = random.Random(16)
    items = sample_splits(run, rng)
    nb = {inv: pairs(read_rows(nb_run, inv)) for inv in ("456",)}
    nb_rows = {inv: {(r["scan_id"], r["line_id"]) for r in read_rows(nb_run, inv)} for inv in ("456",)}
    txt, same, diff, notsplit, n = [], 0, 0, 0, 0
    for inv, kind, key, pr, idx in items:
        if inv != "456":
            continue
        n += 1
        other = nb[inv].get(key)
        if other is None:
            notsplit += 1
            txt.append(f"NOT SPLIT with notebook params: {inv} {kind} {key[1]}  ORIG {pr['L']['text_raw']} {pr['R']['text_raw']}")
        elif other["L"]["text_raw"] == pr["L"]["text_raw"]:
            same += 1
        else:
            diff += 1
            txt.append(f"DIFFERENT {inv} {kind} {key[1]}\n  derived : {pr['L']['text_raw']} || {pr['R']['text_raw']}\n"
                       f"  notebook: {other['L']['text_raw']} || {other['R']['text_raw']}")
    txt.insert(0, f"456 sample: {n} splits; same split in notebook run {same}; different {diff}; not split {notsplit}")
    Path(out).write_text("\n".join(txt) + "\n")


def cmd_check(run, out, d15):
    import re
    tags = ("index", "resolution", "untagged", "Text")
    txt = []
    for inv in INVS:
        rows = read_rows(run, inv)
        orig_rows = read_rows(d15, inv)
        orig = {(r["scan_id"], r["line_id"]): r for r in orig_rows}
        with gzip.open(Path(d15) / "regions_meta" / f"{inv}.tsv.gz", "rt", newline="") as fh:
            tag_of = {r["region_id"]: r["region_tag"]
                      for r in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)}
        prs = pairs(rows)
        n = len(prs)
        cons_bad, geo_bad, order_bad, missing = [], [], 0, 0
        norm = lambda t: re.sub(r" +", " ", t)
        for (scan, oid), v in prs.items():
            o = orig.get((scan, oid))
            if o is None:
                missing += 1
                continue
            L, R = v["L"], v["R"]
            if norm(L["text_raw"] + " " + R["text_raw"]) != norm(o["text_raw"]):
                cons_bad.append((oid, o["text_raw"], L["text_raw"], R["text_raw"]))
            ok = True
            for h in (L, R):
                if not (h["x"] >= o["x"] - 2 and h["y"] >= o["y"] - 2 and
                        h["x"] + h["w"] <= o["x"] + o["w"] + 2 and h["y"] + h["h"] <= o["y"] + o["h"] + 2):
                    ok = False
            if not ok:
                geo_bad.append((oid, (o["x"], o["y"], o["w"], o["h"]), (L["x"], L["y"], L["w"], L["h"]),
                                (R["x"], R["y"], R["w"], R["h"])))
            if not L["x"] < R["x"]:
                order_bad += 1
        split_sides = {(r["scan_id"], r["page_side"]) for r in rows if r["split_from"]}
        part = [r for r in rows if tag_of.get(r["region_id"]) in tags]
        on_split = [r for r in part if (r["scan_id"], r["page_side"]) in split_sides]
        with_col = sum(1 for r in on_split if r["column"] in ("1", "2", "x"))
        all_with = sum(1 for r in part if r["column"] in ("1", "2", "x"))
        txt.append(f"### {inv}: splits {n}; original lines in 15d {len(orig_rows)}, in split run {len(rows)}, "
                   f"difference {len(rows) - len(orig_rows)} (= splits {n}: {len(rows) - len(orig_rows) == n})")
        txt.append(f"  text conservation exceptions: {len(cons_bad)} of {n}; missing originals {missing}")
        for e in cons_bad[:10]:
            txt.append(f"    {e}")
        txt.append(f"  geometry (halves within original box +-2 px) exceptions: {len(geo_bad)} of {n} "
                   f"({100 * (n - len(geo_bad)) / n:.3f}% ok); L.x >= R.x: {order_bad}")
        for e in geo_bad[:5]:
            txt.append(f"    {e}")
        txt.append(f"  participating lines {len(part)}; with column in 1/2/x: {all_with} "
                   f"({100 * all_with / len(part):.3f}%); on sides with >= 1 split: {len(on_split)}, "
                   f"with column: {with_col} ({100 * with_col / len(on_split):.3f}%)")
        blank = [r for r in on_split if r["column"] not in ("1", "2", "x")]
        for r in blank[:5]:
            txt.append(f"    blank: {r['scan_id']} {r['page_side']} {r['line_id']} {r['text_raw'][:40]!r}")
        side_stats = defaultdict(int)
        sp_by_scan = defaultdict(int)
        txt.append(f"  edit_flags: " + str({f: sum(1 for r in rows if r['edit_flags'] == f)
                                         for f in sorted({r['edit_flags'] for r in rows})}))
    Path(out).write_text("\n".join(txt) + "\n")


if __name__ == "__main__":
    cmd, run, out = sys.argv[1:4]
    {"splits": cmd_splits, "unsplit": cmd_unsplit, "columns": cmd_columns}.get(cmd, None) and \
        {"splits": cmd_splits, "unsplit": cmd_unsplit, "columns": cmd_columns}[cmd](run, out)
    if cmd == "check":
        cmd_check(run, out, sys.argv[4])
    if cmd == "compare":
        cmd_compare(run, out, sys.argv[4])
