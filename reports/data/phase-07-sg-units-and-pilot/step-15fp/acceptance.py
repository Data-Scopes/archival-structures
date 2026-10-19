"""Step 15fp acceptance numbers over the 16 inventories of data/text_layer_v2_dev/15fp-a/.

Run from the AS root: .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/acceptance.py
Writes acceptance.json next to this script and prints the report tables.
"""

import json
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import regions_overlap

from archival_structures.text_layer.io import read_tsv_gz

ROOT = Path("data/text_layer_v2_dev/15fp-a")
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}
OUT = Path(__file__).with_name("acceptance.json")


class Box:
    def __init__(self, x, y, w, h):
        self.coords = pdm.Coords.coords_from_box_params(int(x), int(y), int(w), int(h))


def area_overlap(a, b):
    """Planning criterion: bbox intersection >= 0.5 of the smaller box."""
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    iw = max(0, min(ax + aw, bx + bw) - max(ax, bx))
    ih = max(0, min(ay + ah, by + bh) - max(ay, by))
    small = min(aw * ah, bw * bh)
    return small > 0 and iw * ih >= 0.5 * small


def hull_box(lines):
    x0 = min(int(l["x"]) for l in lines)
    y0 = min(int(l["y"]) for l in lines)
    x1 = max(int(l["x"]) + int(l["w"]) for l in lines)
    y1 = max(int(l["y"]) + int(l["h"]) for l in lines)
    return (x0, y0, x1 - x0, y1 - y0)


def inventory(layer_root, inv):
    lines_in = list(read_tsv_gz(layer_root / "lines" / f"{inv}.lines.tsv.gz"))
    meta = list(read_tsv_gz(layer_root / "regions_meta" / f"{inv}.tsv.gz"))
    l_scans = {r["scan_id"]: r for r in read_tsv_gz(layer_root / "layout" / "scans" / f"{inv}.tsv.gz")}
    l_regions = list(read_tsv_gz(layer_root / "layout" / "regions" / f"{inv}.tsv.gz"))
    l_lines = list(read_tsv_gz(layer_root / "layout" / "lines" / f"{inv}.tsv.gz"))
    res = {}

    # 1 conservation
    keys_in = Counter((r["scan_id"], r["line_id"]) for r in lines_in)
    keys_out = Counter((r["scan_id"], r["line_id"]) for r in l_lines)
    res["lines_in"], res["lines_out"] = len(lines_in), len(l_lines)
    res["conservation_ok"] = keys_in == keys_out and max(keys_out.values(), default=1) == 1
    ids = Counter(r["layout_region_id"] for r in l_regions)
    res["dup_layout_region_ids"] = sum(v > 1 for v in ids.values())
    # 2 stability
    res["unchanged_rows"] = sum(r["change"] == "unchanged" for r in l_regions)
    res["unchanged_id_mismatch"] = sum(r["change"] == "unchanged" and
                                       r["layout_region_id"] != r["source_region_ids"]
                                       for r in l_regions)
    # 3 merges
    st = Counter()
    for s in l_scans.values():
        for k in ("n_regions_in", "n_regions_out", "n_merges", "n_gutter_splits", "n_lines_crossing"):
            st[k] += int(s[k])
    res.update(st)
    res["change"] = dict(Counter(r["change"] for r in l_regions if r["layout_role"] != "empty"))
    by_page = defaultdict(list)
    for r in l_regions:
        if r["layout_role"] != "empty":
            by_page[(r["scan_id"], r["page"])].append(Box(r["x"], r["y"], r["w"], r["h"]))
    res["post_overlap_pairs"] = sum(regions_overlap(a, b, 0.5) for regs in by_page.values()
                                    for a, b in combinations(regs, 2))
    # overlap before, both ways (regions with >= 1 line, all lines; planning criterion), and
    # regions_overlap on line hulls (what the merge uses)
    lines_by_region = defaultdict(list)
    for l in lines_in:
        lines_by_region[l["region_id"]].append(l)
    meta_by_scan = defaultdict(list)
    for r in meta:
        if lines_by_region.get(r["region_id"]):
            meta_by_scan[r["scan_id"]].append(r)
    scans_box = scans_hull = scans_ro = 0
    for sid, regs in meta_by_scan.items():
        boxes = [(int(r["x"]), int(r["y"]), int(r["w"]), int(r["h"])) for r in regs]
        hulls = [hull_box(lines_by_region[r["region_id"]]) for r in regs]
        scans_box += any(area_overlap(a, b) for a, b in combinations(boxes, 2))
        scans_hull += any(area_overlap(a, b) for a, b in combinations(hulls, 2))
        hb = [Box(*h) for h in hulls]
        scans_ro += any(regions_overlap(a, b, 0.5) for a, b in combinations(hb, 2))
    res["scans_with_lines"] = len(meta_by_scan)
    res["scans_pair_regionbox"] = scans_box
    res["scans_pair_linehull"] = scans_hull
    res["scans_pair_regions_overlap_hull"] = scans_ro
    # 4 page split
    res["split_method"] = dict(Counter(s["split_method"] for s in l_scans.values()))
    res["page_mode"] = dict(Counter(s["page_mode"] for s in l_scans.values()))
    res["scans_crossing"] = sum(int(s["n_lines_crossing"]) > 0 for s in l_scans.values())
    res["n_scans"] = len(l_scans)
    side = {(l["scan_id"], l["line_id"]): l["page_side"] for l in lines_in}
    expect = {"left": "verso", "right": "recto"}
    diff_open = diff_scans = 0
    diff_scan_set = set()
    single_lines = 0
    for l in l_lines:
        if l["page"] == "single":
            single_lines += 1
            continue
        if expect.get(side[(l["scan_id"], l["line_id"])]) != l["page"]:
            diff_open += 1
            diff_scan_set.add(l["scan_id"])
    res["lines_page_differs"] = diff_open
    res["scans_page_differs"] = len(diff_scan_set)
    res["lines_single"] = single_lines
    # 5 roles (chars of text in layout lines)
    text = {(l["scan_id"], l["line_id"]): len(l["text"]) for l in lines_in}
    chars = Counter()
    for l in l_lines:
        chars[l["layout_role"]] += text[(l["scan_id"], l["line_id"])]
    res["chars"] = dict(chars)
    res["main_char_share"] = chars["main"] / max(1, chars["main"] + chars["extra"])
    pages = Counter((r["scan_id"], r["page"]) for r in l_regions if r["layout_role"] in ("main", "extra"))
    extras = Counter((r["scan_id"], r["page"]) for r in l_regions if r["layout_role"] == "extra")
    res["pages_with_text"] = len(pages)
    res["extra_regions"] = sum(extras.values())
    res["extra_per_page"] = sum(extras.values()) / max(1, len(pages))
    res["pages_with_extra"] = len(extras)
    res["main_regions"] = sum(r["layout_role"] == "main" for r in l_regions)
    return res


def main():
    out = {}
    for layer, (name, invs) in LAYERS.items():
        root = ROOT / layer / name
        stats = json.loads((root / "layout" / "stats.json").read_text())
        for inv in invs:
            out[inv] = inventory(root, inv)
            out[inv]["gutter_rel"] = stats[inv]["gutter_rel"]
            out[inv]["n_scans_with_gap"] = stats[inv]["n_scans_with_gap"]
            out[inv]["max_merge_passes"] = stats[inv]["max_merge_passes"]
    OUT.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    print("| inv | lines | conserv. | dup ids | unchanged id ≠ | regions in → out | merges | gutter splits | passes | post-overlap pairs |")
    for inv, r in out.items():
        print(f"| {inv} | {r['lines_in']:,} | {'ok' if r['conservation_ok'] else 'FAIL'} | {r['dup_layout_region_ids']} | "
              f"{r['unchanged_id_mismatch']} | {r['n_regions_in']:,} → {r['n_regions_out']:,} | {r['n_merges']} | "
              f"{r['n_gutter_splits']} | {r['max_merge_passes']} | {r['post_overlap_pairs']} |")
    print()
    print("| inv | scans w/ lines | pair: region box | pair: line hull | regions_overlap on hulls |")
    for inv, r in out.items():
        print(f"| {inv} | {r['scans_with_lines']} | {r['scans_pair_regionbox']} | {r['scans_pair_linehull']} | {r['scans_pair_regions_overlap_hull']} |")
    print()
    print("| inv | scans | single | gap | min_coverage | none | gutter_rel | lines crossing | scans crossing | lines page ≠ page_side (scans) |")
    for inv, r in out.items():
        sm = r["split_method"]
        print(f"| {inv} | {r['n_scans']} | {r['page_mode'].get('single', 0)} | {sm.get('gap', 0)} | {sm.get('min_coverage', 0)} | "
              f"{sm.get('none', 0)} | {r['gutter_rel']:.3f} | {r['n_lines_crossing']} | "
              f"{r['scans_crossing']} ({r['scans_crossing'] / r['n_scans']:.1%}) | "
              f"{r['lines_page_differs']:,} ({r['scans_page_differs']}) |")
    print()
    print("| inv | pages w/ text | main chars share | extra regions | extra/page | pages with extra |")
    for inv, r in out.items():
        print(f"| {inv} | {r['pages_with_text']} | {r['main_char_share']:.3f} | {r['extra_regions']} | "
              f"{r['extra_per_page']:.2f} | {r['pages_with_extra']} |")


if __name__ == "__main__":
    main()
