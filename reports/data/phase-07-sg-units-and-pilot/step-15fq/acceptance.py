"""Step 15fq acceptance numbers (criteria 1-7) over the 16 inventories.

Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fq/acceptance.py [LAYER_DIR]
LAYER_DIR defaults to data/text_layer_v2_dev/15fq-a. Compares to 15fq-legacy and 15fp-a (same
parent directory). Writes acceptance[_<layer>].json next to this script and prints the tables.
"""

import json
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from pagexml.helper.pagexml_helper import regions_overlap
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.page_structure import _guard_blocks
from archival_structures.text_layer.io import read_tsv_gz

PARENT = Path("data/text_layer_v2_dev")
LAYER = Path(sys.argv[1]) if len(sys.argv) > 1 else PARENT / "15fq-a"
LEGACY = PARENT / "15fq-legacy"
BASE = PARENT / "15fp-a"
HERE = Path(__file__).parent
OUT = HERE / ("acceptance.json" if LAYER.name == "15fq-a" else f"acceptance_{LAYER.name}.json")
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}
GAP_INVS = {"3354", "3771", "3772", "3773", "3821", "3822", "3823", "455", "456"}
SCAN15 = ["scan_id", "page_mode", "separation_x", "split_method", "n_lines_crossing",
          "n_regions_in", "n_regions_out", "n_merges", "n_gutter_splits"]
REG15 = ["scan_id", "page", "layout_region_id", "x", "y", "w", "h", "layout_column", "layout_role",
         "n_lines", "source_region_ids", "change"]
LINE15 = ["scan_id", "line_id", "region_id", "page", "layout_region_id", "layout_line_idx",
          "layout_column", "layout_role"]


def table(root, kind, inv):
    path = {"scans": root / "layout" / "scans" / f"{inv}.tsv.gz",
            "regions": root / "layout" / "regions" / f"{inv}.tsv.gz",
            "lines": root / "layout" / "lines" / f"{inv}.tsv.gz",
            "src_lines": root / "lines" / f"{inv}.lines.tsv.gz"}[kind]
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)


class Box:
    def __init__(self, x, y, w, h):
        self.coords = pdm.Coords.coords_from_box_params(int(x), int(y), int(w), int(h))


def roots(layer_dir, name, layer):
    return layer_dir / layer / name


def inventory(layer, name, inv):
    root = roots(LAYER, name, layer)
    leg = roots(LEGACY, name, layer)
    base = roots(BASE, name, layer)
    res = {}
    src = table(root, "src_lines", inv)
    scans, regs, lines = table(root, "scans", inv), table(root, "regions", inv), table(root, "lines", inv)
    # 1 conservation and ids
    keys_in = Counter(zip(src.scan_id, src.line_id))
    keys_out = Counter(zip(lines.scan_id, lines.line_id))
    res["lines_in"], res["lines_out"] = len(src), len(lines)
    res["c1_conservation_ok"] = keys_in == keys_out and max(keys_out.values(), default=1) == 1
    ids = Counter(regs.layout_region_id)
    res["c1_dup_layout_region_ids"] = sum(v > 1 for v in ids.values())
    un = regs[regs.change == "unchanged"]
    res["c1_unchanged_rows"] = len(un)
    res["c1_unchanged_id_mismatch"] = int((un.layout_region_id != un.source_region_ids).sum())
    # 2 regression: legacy vs 15fp
    reg_ok = {}
    for kind, cols in (("scans", SCAN15), ("regions", REG15), ("lines", LINE15)):
        a = table(leg, kind, inv)[cols].copy()
        b = table(base, kind, inv)[cols].copy()
        if "change" in a:
            a["change"] = a["change"].replace({"merged+split": "merged+split_at_gutter"})
            b["change"] = b["change"].replace({"merged+split": "merged+split_at_gutter"})
        reg_ok[kind] = bool(a.equals(b))
    res["c2_regression_equal"] = reg_ok
    # 3/4 page differences vs 15fp
    b_scans, b_lines = table(base, "scans", inv), table(base, "lines", inv)
    m = lines[["scan_id", "line_id", "page"]].merge(b_lines[["scan_id", "line_id", "page"]],
                                                   on=["scan_id", "line_id"], suffixes=("", "_15fp"))
    diff = m[m.page != m.page_15fp]
    res["c34_lines_page_differs"] = len(diff)
    res["c34_scans_page_differs"] = int(diff.scan_id.nunique())
    sc = scans.merge(b_scans, on="scan_id", suffixes=("", "_15fp"))
    res["c3_sep_or_method_differs"] = int(((sc.separation_x != sc.separation_x_15fp) |
                                           (sc.split_method != sc.split_method_15fp)).sum())
    res["split_method"] = dict(Counter(scans.split_method))
    res["gutter_rule"] = scans.gutter_rule.iloc[0] if len(scans) else ""
    diff[["scan_id", "line_id", "page", "page_15fp"]].to_csv(HERE / f"page_diff_{inv}.tsv", sep="\t", index=False)
    # 6 print column purity
    col = dict(zip(zip(src.scan_id, src.line_id), src["column"]))
    both = defaultdict(set)
    for s, l, r in zip(lines.scan_id, lines.line_id, lines.layout_region_id):
        v = col[(s, l)]
        if v:
            both[(s, r)].add(v)
    res["c6_regions_with_both_columns"] = sum(len(v) > 1 for v in both.values())
    # 7 furniture and residual overlaps
    nonempty = regs[regs.layout_role != "empty"]
    res["c7_furniture_regions"] = int((nonempty.furniture == "1").sum())
    res["c7_furniture_merged"] = int(((nonempty.furniture == "1") &
                                      nonempty.change.str.contains("merged")).sum())
    srcg = src.set_index(["scan_id", "line_id"])
    geo = defaultdict(list)
    for s, l, r in zip(lines.scan_id, lines.line_id, lines.layout_region_id):
        geo[(s, r)].append((s, l))
    fur = set(zip(nonempty[nonempty.furniture == "1"].scan_id, nonempty[nonempty.furniture == "1"].layout_region_id))
    by_page = defaultdict(list)
    for r in nonempty.itertuples():
        by_page[(r.scan_id, r.page)].append(r)
    resid = Counter()
    resid_detail = []
    for (sid, page), rs in by_page.items():
        boxes = [Box(r.x, r.y, r.w, r.h) for r in rs]
        for (i, a), (j, b) in combinations(list(enumerate(rs)), 2):
            if not regions_overlap(boxes[i], boxes[j], 0.5):
                continue
            kind = "other"
            if (sid, a.layout_region_id) in fur or (sid, b.layout_region_id) in fur:
                kind = "furniture"
            else:
                objs = []
                for r in (a, b):
                    ls = [srcg.loc[k] for k in geo[(sid, r.layout_region_id)]]
                    objs.append(SimpleNamespace(lines=[SimpleNamespace(coords=SimpleNamespace(
                        w=int(x["w"]), y=int(x["y"]), h=int(x["h"]))) for x in ls]))
                if _guard_blocks(objs[0], objs[1], 0.5):
                    kind = "guard"
            resid[kind] += 1
            if kind == "other":
                resid_detail.append((sid, page, a.layout_region_id, b.layout_region_id))
    res["c7_residual_pairs"] = dict(resid)
    res["c7_residual_other"] = resid_detail[:20]
    # counts and roles
    for k in ("n_regions_in", "n_regions_out", "n_merges", "n_gutter_splits", "n_column_splits",
              "n_furniture", "n_guard_blocked", "n_lines_crossing"):
        res[k] = int(pd.to_numeric(scans[k]).sum())
    res["change"] = dict(Counter(nonempty.change))
    text_len = dict(zip(zip(src.scan_id, src.line_id), src.text.str.len()))
    chars = Counter()
    for s, l, role in zip(lines.scan_id, lines.line_id, lines.layout_role):
        chars[role] += text_len[(s, l)]
    res["main_char_share"] = chars["main"] / max(1, chars["main"] + chars["extra"])
    pages = {(r.scan_id, r.page) for r in nonempty.itertuples()}
    extra = nonempty[nonempty.layout_role == "extra"]
    res["pages_with_text"] = len(pages)
    res["extra_regions"] = len(extra)
    res["extra_per_page"] = len(extra) / max(1, len(pages))
    return res


def dev_check():
    """Criterion 5: the 60 read page-split scans of 15fp (pageside + mincov samples)."""
    v = pd.read_csv(HERE.parent / "step-15fp" / "ea_verdicts.tsv", sep="\t", dtype=str, keep_default_na=False)
    v = v[v["sample"].isin(["pageside", "mincov"])]
    rows = []
    for r in v.itertuples():
        inv = r.scan.split("_")[0]
        for layer, (name, invs) in LAYERS.items():
            if inv in invs:
                d = pd.read_csv(HERE / f"page_diff_{inv}.tsv", sep="\t", dtype=str, keep_default_na=False)
                sid = [s for s in d.scan_id.unique() if s.endswith("_" + r.scan.split("_", 1)[1])]
                n = int((d.scan_id == sid[0]).sum()) if sid else 0
                rows.append((r.sample, r.scan, r.verdict, n))
    return rows


def main():
    out = {}
    for layer, (name, invs) in LAYERS.items():
        for inv in invs:
            out[inv] = inventory(layer, name, inv)
    OUT.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    print("| inv | rule | lines | c1 conserv. | dup ids | unch. id ≠ | c2 regression (scans/regions/lines) | c7 furn. regions | furn. merged | resid. pairs | c6 both-col regions |")
    for inv, r in out.items():
        print(f"| {inv} | {r['gutter_rule']} | {r['lines_in']:,} | {'ok' if r['c1_conservation_ok'] else 'FAIL'} | "
              f"{r['c1_dup_layout_region_ids']} | {r['c1_unchanged_id_mismatch']} | "
              f"{'/'.join('ok' if r['c2_regression_equal'][k] else 'FAIL' for k in ('scans', 'regions', 'lines'))} | "
              f"{r['c7_furniture_regions']} | {r['c7_furniture_merged']} | {r['c7_residual_pairs']} | "
              f"{r['c6_regions_with_both_columns']} |")
    print()
    print("| inv | rule | split_method | sep/method differs vs 15fp | lines page ≠ 15fp (scans) |")
    for inv, r in out.items():
        print(f"| {inv} | {r['gutter_rule']} | {r['split_method']} | {r['c3_sep_or_method_differs']} | "
              f"{r['c34_lines_page_differs']:,} ({r['c34_scans_page_differs']}) |")
    print()
    print("| inv | regions in → out | merges | gutter splits | column splits | furniture | guard-blocked | main char share | extra/page |")
    for inv, r in out.items():
        print(f"| {inv} | {r['n_regions_in']:,} → {r['n_regions_out']:,} | {r['n_merges']} | {r['n_gutter_splits']} | "
              f"{r['n_column_splits']} | {r['n_furniture']} | {r['n_guard_blocked']} | {r['main_char_share']:.3f} | {r['extra_per_page']:.2f} |")
    print()
    rows = dev_check()
    by = defaultdict(lambda: [0, 0])
    for sample, scan, verdict, n in rows:
        by[verdict][0] += 1
        by[verdict][1] += n > 0
    print("| 15fp verdict | scans | scans changed |")
    for k, (n, c) in by.items():
        print(f"| {k} | {n} | {c} |")
    print("changed scans:", [(s, v, n) for _, s, v, n in rows if n])


if __name__ == "__main__":
    main()
