"""Index-page detector per page side (phase 13, step 19c).

Index sections of an archive (lemma headings with entries ending in a page number or starting with a
date, in two columns) are detected per layout page side from the lines alone, then smoothed with a
contiguity fill (index sections are runs of pages). Generic: archive knowledge enters only through
the parameters (``end_pattern``, ``date_pattern``).
"""
import re

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
