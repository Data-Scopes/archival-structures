"""Step 15g acceptance numbers. Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g/acceptance.py [LAYER_DIR]
LAYER_DIR defaults to data/text_layer_v2_dev/15g-a. Writes acceptance.json next to this file."""

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import ALL_INVS, PARENT, layer_root, lines_by_id, seg_text  # noqa: E402

from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

LAYER = Path(sys.argv[1]) if len(sys.argv) > 1 else PARENT / "15g-a"
OUT = Path(__file__).parent / f"acceptance_{LAYER.name}.json"
TYPES = ("main_text", "page_number", "running_header", "date_header", "catch_word",
         "signature_mark", "marginalia", "index", "other")


def coverage():
    res = {}
    for inv in ALL_INVS:
        root = layer_root(LAYER, inv)
        segs = defaultdict(list)
        for s in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
            segs[(s["scan_id"], s["line_id"])].append(s)
        n_text = n_missing = n_overlap = n_mismatch = 0
        for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
            key = (r["scan_id"], r["line_id"])
            if not r["text"].strip():
                if key in segs:
                    n_missing += 1      # blank line with segments
                continue
            n_text += 1
            got = segs.get(key)
            if not got:
                n_missing += 1
                continue
            if any(int(a["char_end"]) > int(b["char_start"]) for a, b in zip(got, got[1:])):
                n_overlap += 1
            joined = " ".join(" ".join(r["text"][int(s["char_start"]):int(s["char_end"])].split())
                              for s in got)
            if joined != " ".join(r["text"].split()):
                n_mismatch += 1
        res[inv] = {"text_lines": n_text, "missing_or_blank_with_segments": n_missing,
                    "overlap": n_overlap, "text_mismatch": n_mismatch}
    return res


def show(inv, scan, needles):
    root = layer_root(LAYER, inv)
    lines = lines_by_id(root, inv, scan)
    out = []
    for s in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
        key = (s["scan_id"], s["line_id"])
        if key not in lines or scan not in s["scan_id"]:
            continue
        line = lines[key]
        if any(n in line["text"] for n in needles):
            out.append((s["seg_idx"], s["line_type"], s["type_method"], seg_text(line, s),
                        int(s["x"]), int(s["w"])))
    return out


ROWS = [
    # row, inv, scan, line needle, expected (type, text) list
    (50, "3349", "3349_0945", "909.", [("date_header", "28. Junij"), ("main_text", None), ("page_number", "909.")]),
    (52, "3353", "3353_0881", "847.", [("date_header", "24. Decemb:"), ("main_text", None), ("page_number", "847.")]),
    (55, "3353", "3353_0857", "823.", [("date_header", "20: Decemb:"), ("main_text", None), ("page_number", "823.")]),
    ("55b", "3353", "3353_0857", "Nieuwe jaren", [("marginalia", "Nieuwe jaren")]),
    (134, "3349", "3349_0601", "565.", [("date_header", "21. April"), ("main_text", None), ("page_number", "565.")]),
    ("134b", "3349", "3349_0601", "Diesbagh", [("marginalia", "Diesbagh")]),
    (19, "3350", "3350_0284", "247.", [("date_header", "Augt."), ("main_text", None), ("page_number", "247.")]),
    ("9a", "3350", "3350_0287", "29. Augti", [("date_header", "29. Augti.")]),
    ("9b", "3350", "3350_0287", "de Ruijter", None),
]


def r4_rows():
    out = {}
    for row, inv, scan, needle, expected in ROWS:
        got = show(inv, scan, [needle])
        if expected is None:       # row 9 marginalia: list marginalia segments with the needles
            got = [g for g in show(inv, scan, ["de Ruijter", "betalinge der", "Luijck"]) if g[1] == "marginalia"]
            ok = {g[3] for g in got} >= {"de Ruijter", "betalinge der", "Luijck"}
        else:
            ok = len(got) >= len(expected) and any(
                all(g[1] == e[0] and (e[1] is None or g[3] == e[1]) for g, e in zip(got[i:], expected))
                for i in range(len(got) - len(expected) + 1))
            got = [g for g in got]
        out[str(row)] = {"ok": ok, "segments": got}
    return out


def audit_3771_0100():
    root = layer_root(LAYER, "3771")
    lines = lines_by_id(root, "3771", "3771_0100")
    out = []
    for s in read_tsv_gz(root / "types" / "segments" / "3771.tsv.gz"):
        key = (s["scan_id"], s["line_id"])
        if key in lines and "3771_0100" in s["scan_id"]:
            out.append((s["line_type"], seg_text(lines[key], s)[:60]))
    return out


def per_inventory():
    res = {}
    for inv in ALL_INVS:
        root = layer_root(LAYER, inv)
        st = json.loads((root / "types" / "stats.json").read_text())[inv]
        chars = st["chars_per_type"]
        tot = sum(chars.values())
        st["main_text_char_share"] = round(chars.get("main_text", 0) / tot, 4)
        res[inv] = st
    return res


def laypa_455():
    res = defaultdict(Counter)
    for inv in ("455",):
        root = layer_root(LAYER, inv)
        tag = {r["region_id"]: r["region_tag"] for r in read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz")}
        lines = {(r["scan_id"], r["line_id"]): r["region_id"]
                 for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        for s in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
            res[tag[lines[(s["scan_id"], s["line_id"])]]][s["line_type"]] += 1
    return {t: dict(c) for t, c in res.items()}


def ua008():
    """Print layout regions whose lines carry both values of the line `column` field (UA-008):
    their region_type (char majority) and the segment types of their lines by column."""
    out = {}
    for inv in ("455", "456"):
        root = layer_root(LAYER, inv)
        col = {(r["scan_id"], r["line_id"]): r["column"]
               for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        reg_cols = defaultdict(set)
        seg_rows = []
        for s in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
            c = col[(s["scan_id"], s["line_id"])]
            reg_cols[(s["scan_id"], s["page"], s["layout_region_id"])].add(c)
            seg_rows.append((s, c))
        mixed = {k for k, v in reg_cols.items() if len({x for x in v if x}) > 1}
        rtype = {(r["scan_id"], r["page"], r["layout_region_id"]): r["region_type"]
                 for r in read_tsv_gz(root / "types" / "regions" / f"{inv}.tsv.gz")}
        by_col = defaultdict(Counter)
        for s, c in seg_rows:
            if (s["scan_id"], s["page"], s["layout_region_id"]) in mixed:
                by_col[c][s["line_type"]] += 1
        out[inv] = {"n_mixed_regions": len(mixed),
                    "region_type": dict(Counter(rtype[k] for k in mixed if k in rtype)),
                    "segment_types_by_column": {c: dict(v) for c, v in by_col.items()}}
    return out


def main():
    result = {"ua008": ua008(), "coverage": coverage(), "r4_rows": r4_rows(), "audit_3771_0100": audit_3771_0100(),
              "per_inventory": per_inventory(), "laypa_455": laypa_455()}
    OUT.write_text(json.dumps(result, indent=1, ensure_ascii=False), encoding="utf-8")
    cov = result["coverage"]
    print("coverage ok:", all(v["missing_or_blank_with_segments"] == v["overlap"] == v["text_mismatch"] == 0
                              for v in cov.values()), {k: v for k, v in cov.items() if any(list(v.values())[1:])})
    for row, v in result["r4_rows"].items():
        print("row", row, "OK" if v["ok"] else "FAIL", v["segments"])
    print("3771_0100:", result["audit_3771_0100"])
    print("inv  segs  main%  chain/cands  rectos_pn/rectos  idx_pages  splits")
    for inv, st in result["per_inventory"].items():
        print(inv, st["n_segments"], st["main_text_char_share"], f"{st['chain_length']}/{st['chain_candidates']}",
              f"{st['n_rectos_with_page_number']}/{st['n_rectos']}", st["n_index_pages"], st["splits_per_rule"])
    print("ua008:", result["ua008"])
    print("laypa 455:")
    for t, c in result["laypa_455"].items():
        print(" ", t, c)


if __name__ == "__main__":
    main()
