"""Step 15i A5: evidence on the 116 labelled rows of LotA's 02d gold sheet, mapped onto the pilot layer.

    .venv/bin/python rows116.py [--layer data/text_layer_v2_dev/15i-a] [--out rows116.tsv]
    .venv/bin/python rows116.py --self-test

`rows116_verdicts.tsv` (row_no, predicted, reason) holds the hand-read verdicts for the 51 artefact rows;
the other rows are decided here (unchanged_free if both strings are found unchanged, else changed).
"""
import argparse
import collections
import csv
import difflib
import json
import re
import sys
import tempfile
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

HERE = Path(__file__).parent
LOTA = Path("/Users/Shared/claude_projects/data_scopes/language-of-the-archive")
HAND_INVS = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
ARTEFACT = {"print_ocr_error", "segmentation", "hand_htr_error", "misaligned", "unclear"}
PREDICTED = {"fixed", "persists_glue", "persists_htr_ocr", "needs_realignment", "unclear"}
MIN_RATIO = 0.8
GLUE_LETTERS = 18


def layer_root(base):
    return next(Path(base).glob("*_text_layer_v2"))


class Layer:
    """Lazy per-inventory readers over one layer root."""

    def __init__(self, root):
        self.root = Path(root)
        self._c = {}

    def table(self, name, inv):
        key = (name, inv)
        if key not in self._c:
            p = {"scans": f"scans/{inv}.scans.tsv.gz", "lines": f"lines/{inv}.lines.tsv.gz",
                 "layout_lines": f"layout/lines/{inv}.tsv.gz", "tseg": f"types/segments/{inv}.tsv.gz",
                 "units": f"units/units/{inv}.tsv.gz", "dmap": f"units/doc_id_map/{inv}.tsv.gz",
                 "rep": f"units/republic_lines/{inv}.tsv.gz", "jun": f"join/units/{inv}.tsv.gz",
                 "jseg": f"join/segments/{inv}.tsv.gz"}[name]
            self._c[key] = list(read_tsv_gz(self.root / p)) if (self.root / p).exists() else []
        return self._c[key]

    def index(self, name, inv, keyf, multi=False):
        key = ("idx", name, inv)
        if key not in self._c:
            d = collections.defaultdict(list) if multi else {}
            for r in self.table(name, inv):
                if multi:
                    d[keyf(r)].append(r)
                else:
                    d[keyf(r)] = r
            self._c[key] = d
        return self._c[key]


def normalise(parts):
    """parts: [(key, text)] -> (norm string, mapping[i] = (part_idx, orig_offset) or None for separators)."""
    out, mp = [], []
    for pi, (_, text) in enumerate(parts):
        if pi:
            out.append(" ")
            mp.append(None)
        prev_space = True
        for oi, ch in enumerate(text):
            if ch.isspace():
                if not prev_space:
                    out.append(" ")
                    mp.append((pi, oi))
                prev_space = True
            else:
                out.append(ch)
                mp.append((pi, oi))
                prev_space = False
    return "".join(out), mp


def locate(tokens, parts):
    """Find `tokens` in the parts' texts. Returns dict(method exact|fuzzy|none, ratio, match, spans {part_idx: (s, e)})."""
    tok = " ".join(str(tokens).split())
    norm, mp = normalise(parts)
    res = {"method": "none", "ratio": 0.0, "match": "", "spans": {}}
    if not tok:
        return res
    pos = norm.find(tok)
    if pos >= 0:
        s, e = pos, pos + len(tok)
        res.update(method="exact", ratio=1.0)
    else:
        words = [(m.start(), m.end()) for m in re.finditer(r"\S+", norm)]
        k = len(tok.split())
        best = (0.0, None)
        sm = difflib.SequenceMatcher(None, autojunk=False)
        sm.set_seq1(tok)
        for i in range(len(words)):
            for n in range(max(1, k - 2), k + 2):
                if i + n > len(words):
                    continue
                a, b = words[i][0], words[i + n - 1][1]
                sm.set_seq2(norm[a:b])
                if sm.real_quick_ratio() < MIN_RATIO or sm.quick_ratio() < MIN_RATIO:
                    continue
                r = sm.ratio()
                if r > best[0]:
                    best = (r, (a, b))
        if best[1] is None or best[0] < MIN_RATIO:
            res["ratio"] = round(best[0], 3)
            return res
        s, e = best[1]
        res.update(method="fuzzy", ratio=round(best[0], 3))
    res["match"] = norm[s:e]
    for c in range(s, e):
        if mp[c] is not None:
            pi, oi = mp[c]
            lo, hi = res["spans"].get(pi, (oi, oi + 1))
            res["spans"][pi] = (min(lo, oi), max(hi, oi + 1))
    return res


def span_segments(layer, inv, unit_key, s, e):
    """join segments of the unit overlapping the unit-text span [s, e): list of (jseg row, tseg row)."""
    segs = layer.index("jseg", inv, lambda r: (r["scan_id"], r["unit_id"]), multi=True).get(unit_key, [])
    tseg = layer.index("tseg", inv, lambda r: (r["scan_id"], r["line_id"], r["seg_idx"]))
    return [(g, tseg[(g["scan_id"], g["line_id"], g["seg_idx"])]) for g in segs
            if int(g["char_start"]) < e and int(g["char_end"]) > s]


def evidence(layer, inv, units, tokens):
    """units: list of unit rows (scan_id, unit_id, unit_type). Locate tokens; return the evidence dict."""
    jun = layer.index("jun", inv, lambda r: (r["scan_id"], r["unit_id"]))
    parts = [((u["scan_id"], u["unit_id"]), jun[(u["scan_id"], u["unit_id"])]["text"]) for u in units
             if (u["scan_id"], u["unit_id"]) in jun]
    ok_units = [u for u in units if (u["scan_id"], u["unit_id"]) in jun]
    res = locate(tokens, parts)
    segs = []
    for pi, (s, e) in sorted(res["spans"].items()):
        segs.extend(span_segments(layer, inv, parts[pi][0], s, e))
    res["unit_types"] = sorted({ok_units[pi]["unit_type"] for pi in res["spans"]})
    res["unit_ids"] = [ok_units[pi]["unit_id"] for pi in sorted(res["spans"])]
    res["boxes"] = [[g["scan_id"], int(t["x"]), int(t["y"]), int(t["w"]), int(t["h"]), g["line_id"], t["geom_method"]]
                    for g, t in segs]
    ll = layer.index("layout_lines", inv, lambda r: (r["scan_id"], r["line_id"]))
    res["columns"] = sorted({ll[(g["scan_id"], g["line_id"])]["layout_column"] for g, _ in segs})
    res["pages"] = sorted({ll[(g["scan_id"], g["line_id"])]["page"] for g, _ in segs})
    res["glue"] = any(sum(ch.isalpha() for ch in w) >= GLUE_LETTERS for w in res["match"].split())
    res["scan_ids"] = sorted({b[0] for b in res["boxes"]})
    res["n_candidate_units"] = len(parts)
    return res


def hand_units(layer, inv, doc_id):
    dm = layer.index("dmap", inv, lambda r: r["old_doc_id"], multi=True).get(doc_id, [])
    uids = [r["unit_id"] for r in dm if r["unit_id"]]
    if not uids:
        return [], "no_unit"
    scan = doc_id.split("-text_region")[0]
    units = sorted(layer.index("units", inv, lambda r: r["scan_id"], multi=True).get(scan, []),
                   key=lambda r: int(r["unit_order"]))
    idx = [i for i, u in enumerate(units) if u["unit_id"] in uids]
    if not idx:
        return [], "no_unit"
    # the mapped units, then the next 2 in unit_order
    return [units[i] for i in range(min(idx), min(len(units), max(idx) + 3))], "ok"


def print_units(layer, inv, para, paras):
    ls, le = paras[para]
    rep = layer.index("rep", inv, lambda r: r["republic_line_id"])
    a, b = rep.get(ls), rep.get(le)
    if a is None or b is None or a["match"] == "unmatched" or b["match"] == "unmatched":
        return [], f"crosswalk_{'start' if a is None or a['match']=='unmatched' else 'end'}_unmatched"
    unit_of = layer.index("units", inv, lambda r: r["unit_id"])
    seg_unit = {}
    for r in layer.table("units_segments", inv) if False else read_unit_segments(layer, inv):
        seg_unit[(r["scan_id"], r["line_id"])] = r["unit_id"]
    ua, ub = seg_unit.get((a["scan_id"], a["line_id"])), seg_unit.get((b["scan_id"], b["line_id"]))
    if ua is None or ub is None:
        return [], "line_without_unit"
    allu = sorted(layer.table("units", inv), key=lambda r: (r["scan_id"], int(r["unit_order"])))
    ids = [u["unit_id"] for u in allu]
    i, j = ids.index(ua), ids.index(ub)
    if j < i:
        i, j = j, i
    if j - i > 40:
        return [], "span_too_long"
    return allu[i:j + 1], "ok"


def read_unit_segments(layer, inv):
    key = ("useg", inv)
    if key not in layer._c:
        layer._c[key] = list(read_tsv_gz(layer.root / "units" / "segments" / f"{inv}.tsv.gz"))
    return layer._c[key]


def load_gold(extra=()):
    import pandas as pd
    g = pd.read_csv(LOTA / "gold/02d-hand-print-differences-partial-extended-rescaled.tsv", sep="\t", dtype=str,
                    usecols=["row_no", "unit_id", "hand_tokens", "print_tokens", "label", "second_label", "notes",
                             "hand_filename", "print_filename"])
    g = g[g.label.notna() | g.row_no.isin(list(extra))].copy()
    d = pd.read_csv(LOTA / "output/parallel_text/1.10.94-1.01.02/difference_units.tsv.gz", sep="\t", dtype=str,
                    usecols=["unit_id", "para_id", "hand_doc_id"]).drop_duplicates("unit_id")
    return g.merge(d, on="unit_id", how="left").fillna("")


def run(layer_dir, out, extra=()):
    hand, prn = Layer(layer_root(Path(layer_dir) / "hand")), Layer(layer_root(Path(layer_dir) / "print"))
    paras = {}
    for inv in ("455", "456"):
        import pandas as pd
        p = pd.read_csv(LOTA / f"data/paragraphs/NL-HaNA_1.10.94/paragraphs-{inv}.tsv.gz", sep="\t", dtype=str,
                        usecols=["para_id", "line_start", "line_end"])
        paras[inv] = {r.para_id: (r.line_start, r.line_end) for r in p.itertuples()}
    verdicts = {}
    vp = HERE / "rows116_verdicts.tsv"
    if vp.exists():
        for r in csv.DictReader(open(vp), delimiter="\t"):
            verdicts[r["row_no"]] = r
    rows = []
    for g in load_gold(extra).itertuples():
        hinv = g.hand_doc_id.split("_")[2].split("-")[0] if g.hand_doc_id else ""
        pinv = g.unit_id.split("-")[0]
        hu, hs = hand_units(hand, hinv, g.hand_doc_id)
        he = evidence(hand, hinv, hu, g.hand_tokens) if hu else {"method": "none", "ratio": 0, "match": "", "boxes": [],
                                                                 "unit_types": [], "columns": [], "glue": False, "scan_ids": [],
                                                                 "unit_ids": [], "pages": [], "n_candidate_units": 0}
        pu, ps = print_units(prn, pinv, g.para_id, paras[pinv])
        pe = evidence(prn, pinv, pu, g.print_tokens) if pu else {"method": "none", "ratio": 0, "match": "", "boxes": [],
                                                                 "unit_types": [], "columns": [], "glue": False, "scan_ids": [],
                                                                 "unit_ids": [], "pages": [], "n_candidate_units": 0}
        pscope = "paragraph_units"
        if pu and pe["method"] == "none":  # widen to the whole scan(s) of the span, exact matches only
            scans = {u["scan_id"] for u in pu}
            wide = sorted([u for u in prn.table("units", pinv) if u["scan_id"] in scans],
                          key=lambda u: (u["scan_id"], int(u["unit_order"])))
            pw = evidence(prn, pinv, wide, g.print_tokens)
            if pw["method"] == "exact":
                pe, pscope = pw, "scan_units"
        flags = []
        if any(t != "main_text" for t in he["unit_types"]):
            flags.append("hand_non_main_text")
        if len(pe["columns"]) > 1:
            flags.append("print_multi_column")
        if pe["glue"]:
            flags.append("print_glue_token")
        art = g.label in ARTEFACT
        v = verdicts.get(g.row_no)
        if art:
            pred, reason = (v["predicted"], v["reason"]) if v else ("", "")
        else:
            pred = "unchanged_free" if he["method"] == "exact" and pe["method"] == "exact" else "changed"
            reason = ""
        rows.append({
            "row_no": g.row_no, "label": g.label, "second_label": g.second_label, "artefact": int(art),
            "notes": g.notes, "hand_tokens": g.hand_tokens, "print_tokens": g.print_tokens,
            "hand_inv": hinv, "hand_doc_id": g.hand_doc_id, "hand_status": hs, "hand_found": he["method"],
            "hand_ratio": he["ratio"], "hand_match": he["match"], "hand_unit_types": ",".join(he["unit_types"]),
            "hand_unit_ids": ";".join(he["unit_ids"]), "hand_boxes": json.dumps(he["boxes"]),
            "hand_filename": g.hand_filename, "print_inv": pinv, "para_id": g.para_id, "print_status": ps,
            "print_found": pe["method"], "print_ratio": pe["ratio"], "print_match": pe["match"],
            "print_unit_types": ",".join(pe["unit_types"]), "print_unit_ids": ";".join(pe["unit_ids"]),
            "print_boxes": json.dumps(pe["boxes"]), "print_filename": g.print_filename,
            "print_n_candidate_units": pe["n_candidate_units"], "print_scope": pscope, "print_columns": ",".join(pe["columns"]),
            "flags": ",".join(flags), "predicted": pred, "reason": reason})
    for path, part in ((out, [r for r in rows if r["label"]]),
                       (str(Path(out).with_name("rows_extra.tsv")), [r for r in rows if not r["label"]])):
        if not part:
            continue
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(part[0]), delimiter="\t", lineterminator="\n")
            w.writeheader()
            w.writerows([{k: str(v).replace("\t", " ").replace("\n", " ") for k, v in r.items()} for r in part])
    summarise([r for r in rows if r["label"]])


def summarise(rows):
    n = len(rows)
    c = collections.Counter(r["predicted"] for r in rows)
    hf = sum(r["hand_found"] != "none" for r in rows)
    pf = sum(r["print_found"] != "none" for r in rows)
    free = (c["unchanged_free"] + c["fixed"]) / n
    ub = (c["unchanged_free"] + c["fixed"] + c["needs_realignment"]) / n
    print(f"rows={n} hand_found={hf} print_found={pf} predicted={dict(c)} share_free={free:.3f} upper_bound={ub:.3f}")


def self_test():
    parts = [(("s", "u1"), "het selve  goed"), (("s", "u2"), "ende de vrijheyt")]
    r = locate("selve goed", parts)
    assert r["method"] == "exact" and r["spans"][0] == (4, 15), r
    r = locate("goed ende de", parts)
    assert r["method"] == "exact" and set(r["spans"]) == {0, 1}, r
    r = locate("de vrijheit", parts)
    assert r["method"] == "fuzzy" and r["ratio"] >= 0.8 and r["spans"][1][1] == 16, r
    assert locate("zzzzzz qqqq", parts)["method"] == "none"
    from archival_structures.text_layer import build
    import shutil
    fx = Path(__file__).resolve().parents[4] / "tests/fixtures/text_layer/sg/9999"
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        shutil.copytree(fx, td / "px" / "NL-HaNA_1.01.02_9999")
        assert build.main(["--pagexml-dir", str(td / "px"), "--out-dir", str(td / "out"), "--preset", "sg_1.01.02_v2",
                           "--stage", "all", "--workers", "1"]) == 0
        L = Layer(layer_root(td / "out"))
        units = [u for u in L.table("units", "9999") if u["unit_type"] == "main_text"]
        jun = L.index("jun", "9999", lambda r: (r["scan_id"], r["unit_id"]))
        u = units[0]
        text = jun[(u["scan_id"], u["unit_id"])]["text"]
        probe = " ".join(text.split()[:3])
        ev = evidence(L, "9999", [u], probe)
        assert ev["method"] == "exact" and ev["boxes"], ev
        dm = L.table("dmap", "9999")
        old = next(r["old_doc_id"] for r in dm if r["unit_id"])
        hu, st = hand_units(L, "9999", old)
        assert st == "ok" and hu
    print("self-test ok")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", default="data/text_layer_v2_dev/15i-a")
    ap.add_argument("--out", default=str(HERE / "rows116.tsv"))
    ap.add_argument("--extra-rows", default="", help="comma-separated unlabelled gold row_no for the crops (rows_extra.tsv)")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args(argv)
    if a.self_test:
        return self_test()
    run(a.layer, a.out, [x for x in a.extra_rows.split(',') if x])


if __name__ == "__main__":
    sys.exit(main())
