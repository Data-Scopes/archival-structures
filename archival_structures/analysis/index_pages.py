"""Index-page detector per page side (phase 13, step 19c).

Index sections of an archive (lemma headings with entries ending in a page number or starting with a
date, in two columns) are detected per layout page side from the lines alone, then smoothed with a
contiguity fill (index sections are runs of pages). Generic: archive knowledge enters only through
the parameters (``end_pattern``, ``date_pattern``).
"""
import re
from typing import Sequence

INDEX_SIDES_DEFAULTS = {"col_min_chars": 20, "col_min_lines": 15, "mid_frac": 0.4, "text_min_chars": 8,
                        "col_share_min": 0.25, "shape_min": 0.15, "shape_only_min": None, "shape_only_lines": 5,
                        "gap": 0, "min_run": 1, "end_pattern": r"\d+\s*[.,:;]?\s*$", "date_pattern": None}


def side_features(lines, cfg) -> dict:
    """Features of one page side. ``lines``: dicts with string ``x``, ``w`` and ``text``."""
    c = {**INDEX_SIDES_DEFAULTS, **(cfg or {})}
    t = [l["text"].strip() for l in lines if len(l["text"].strip()) >= c["text_min_chars"]]
    s = [l for l in lines if len(l["text"]) >= c["col_min_chars"]]
    n_lines = sum(bool(l["text"].strip()) for l in lines)
    col_share = None
    if len(s) >= c["col_min_lines"]:
        xs = sorted(int(l["x"]) for l in s)
        lo = xs[len(xs) // 20]
        hi = max(int(l["x"]) + int(l["w"]) for l in s)
        cut = lo + c["mid_frac"] * (hi - lo)
        col_share = sum(int(l["x"]) >= cut for l in s) / len(s)
    if t:
        end_num = sum(bool(re.search(c["end_pattern"], x)) for x in t) / len(t)
        if c["date_pattern"] is None:
            date_start = 0.0
        else:
            date_start = sum(bool(re.match(c["date_pattern"], x, re.IGNORECASE)) for x in t) / len(t)
    else:
        end_num = date_start = 0.0
    return {"n_lines": n_lines, "n_text8": len(t), "n_text20": len(s), "col_share": col_share,
            "end_num": end_num, "date_start": date_start}


def side_rule(feat, cfg) -> bool:
    c = {**INDEX_SIDES_DEFAULTS, **(cfg or {})}
    shape = max(feat["end_num"], feat["date_start"])
    cs = feat["col_share"]
    if cs is not None and cs >= c["col_share_min"] and shape >= c["shape_min"]:
        return True
    return (c["shape_only_min"] is not None and feat["n_text8"] >= c["shape_only_lines"]
            and shape >= c["shape_only_min"])


def fill_runs(flags, gap, min_run) -> list:
    """Fill runs of at most ``gap`` false flags between true flags, then drop true runs shorter than ``min_run``."""
    q = list(flags)
    idx = [i for i, x in enumerate(q) if x]
    for a, b in zip(idx, idx[1:]):
        if b - a - 1 <= gap:
            for k in range(a + 1, b):
                q[k] = True
    i = 0
    while i < len(q):
        if q[i]:
            j = i
            while j < len(q) and q[j]:
                j += 1
            if j - i < min_run:
                for k in range(i, j):
                    q[k] = False
            i = j
        else:
            i += 1
    return q


# --- One-column VOC table-of-contents detector (phase 17, step 23d) ---------------------------------------------
# Month alternation copied verbatim from text_layer.presets (not imported: page_structure imports this module).
_TOC_MONTHS = (r"(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|"
               r"sept|oct|okt|nov|dec)")
_TOC_NUM = r"(?:fo\.?|fol\.?|f\.|pag\.?)?\s*\d{1,4}"
_TOC_RANGE = _TOC_NUM + r"\s*(?:verso\s*)?(?:a|à|tot|en|—|–|-|/|\.\s*\.\s*\.?)\s*\d{1,4}"
TOC_SIDES_DEFAULTS = {
    "window": 60, "min_lines": 3, "text_min_chars": 3, "toc_min": 0.2, "amount_max": None, "title": True, "gap": 4,
    "folio_line_pattern": r"^\W{0,3}(?:" + _TOC_RANGE + r"|" + _TOC_NUM + r")\W{0,3}(?:verso\W*)?$",
    "lead_pattern": r"^\W{0,3}" + _TOC_RANGE + r"\b",
    "tail_pattern": _TOC_RANGE + r"\W{0,3}(?:verso\W*)?$",
    "date_pattern": r"^\W{0,2}(?:den\s+)?\d{1,2}\s*[.,:'\"]*\s*" + _TOC_MONTHS,
    "amount_pattern": r"ƒ|\d+\s*[:.]\s*\d{1,2}\s*[:.]\s*\d{1,2}\b|transport|per\s+saldo|debet|credit",
    "title_pattern": r"regist|regyst|inhoud|indice|index"}


def _toc_cfg(cfg) -> dict:
    cfg = cfg or {}
    unknown = set(cfg) - set(TOC_SIDES_DEFAULTS)
    if unknown:
        raise ValueError(f"unknown TOC_SIDES key(s): {sorted(unknown)}")
    return {**TOC_SIDES_DEFAULTS, **cfg}


def toc_side_features(texts: Sequence[str], cfg=None) -> dict:
    """Features of one page side for a one-column TOC: share of folio-number lines, dated lines, amount lines, title."""
    c = _toc_cfg(cfg)
    ls = [t.strip() for t in texts if len(t.strip()) >= c["text_min_chars"]]
    n = len(ls)
    if n == 0:
        return dict(n=0, fol=0.0, date=0.0, amt=0.0, title=False)
    fl, ld, tl = (re.compile(c[k], re.I) for k in ("folio_line_pattern", "lead_pattern", "tail_pattern"))
    dp, ap, tp = (re.compile(c[k], re.I) for k in ("date_pattern", "amount_pattern", "title_pattern"))
    fol = sum(bool(fl.match(x) or ld.match(x) or tl.search(x)) for x in ls) / n
    return dict(n=n, fol=fol, date=sum(bool(dp.match(x)) for x in ls) / n,
                amt=sum(bool(ap.search(x)) for x in ls) / n, title=any(tp.search(x) for x in ls[:5]))


def toc_side_rule(feat, scan_index: int, cfg=None) -> bool:
    """Flag a side in the front window with folio/date shape (and no amounts), or with a register title."""
    c = _toc_cfg(cfg)
    if scan_index >= c["window"]:
        return False
    if c["amount_max"] is not None and not feat["amt"] < c["amount_max"]:
        return False
    return bool((feat["n"] >= c["min_lines"] and feat["fol"] + feat["date"] >= c["toc_min"])
                or (c["title"] and feat["title"]))


def toc_sides(side_texts, side_scan_index, cfg=None) -> list:
    """Per-side TOC flags: the rule on each side, then ``fill_runs`` with ``gap``.
    ``side_texts``: per side a list of line texts; ``side_scan_index``: the 0-based position of its scan."""
    c = _toc_cfg(cfg)
    rules = [toc_side_rule(toc_side_features(t, c), i, c) for t, i in zip(side_texts, side_scan_index)]
    return fill_runs(rules, c["gap"], 1)
