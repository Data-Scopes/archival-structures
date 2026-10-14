"""Page-number anchors and folio -> scan alignment.

Generic core ported verbatim from NLP's ``globalise_alignment``: anchors are
``(scan_no, page_number)`` pairs read by HTR in page-number regions. The longest strictly
increasing chain of anchors, the numbering mode (foliation, pagination, spread) and a linear
interpolation map a folio or page number of an index to a scan, with a confidence band.

Quirks kept on purpose, because downstream outputs must stay byte-identical:

* the longest increasing chain uses ``np.searchsorted(side="left")``: strictly increasing, and
  an equal page number replaces the tail (a repeated number keeps the **later** scan);
* ``int(round(s))`` is Python's banker's rounding (x.5 goes to the even number);
* ``max(1.0, min(s, max_scan or s))``: a ``max_scan`` of 0 or None means "no upper clamp";
* the ``"+extrapolated"`` method suffix is added whenever the folio lies outside the anchor
  range, even when only ``np.interp``'s end clamping applied (``extrap=False``);
* restart runs are interpolated without ``extrap``/``max_scan``;
* fewer than 3 anchors give ``("unknown", nan)`` from :func:`numbering_mode`.

Annotations are never used here. No archive-specific constants live in this module.
"""
import re
from dataclasses import dataclass

import numpy as np
import pandas as pd

from archival_structures.model.scan_id import scan_number

CONFIDENCE_BANDS = (3, 10)  # nearest-anchor distance (scans): <=3 high, <=10 medium, else low


@dataclass(frozen=True)
class AlignmentConfig:
    mode: bool = False
    runs: bool = False
    extrap: bool = False     # linear extrapolation beyond the first/last anchor (else clamp)
    dup_first: bool = False  # repeated consecutive page number: keep the FIRST scan


# ---------------------------------------------------------------- anchors
def page_number_anchors(regions, *, tag="page-number", pattern=r"^\d+$", tag_column="region_tag",
                        text_column="region_text", scan_column="scan_id", scan_number=scan_number):
    """Anchors from a region table (one row per region, in document order).

    Per scan the last non-empty region with ``tag`` wins; it is kept only if its stripped text
    matches ``pattern`` (no fallback to an earlier region). Returns ``scan_id, scan_no,
    page_number`` sorted (stably) by ``scan_no``.
    """
    cols = ["scan_id", "scan_no", "page_number"]
    sub = regions[regions[tag_column] == tag]
    text = sub[text_column].map(lambda t: "" if pd.isna(t) else str(t).strip())
    sub = pd.DataFrame({"scan_id": sub[scan_column].to_numpy(), "text": text.to_numpy()})
    sub = sub[sub["text"] != ""]
    last = sub.drop_duplicates("scan_id", keep="last")
    last = last[last["text"].map(lambda t: re.match(pattern, t) is not None)]
    if last.empty:
        return pd.DataFrame({"scan_id": pd.Series(dtype=object), "scan_no": pd.Series(dtype=int),
                             "page_number": pd.Series(dtype=int)}, columns=cols)
    out = pd.DataFrame({"scan_id": last["scan_id"].to_numpy(),
                        "scan_no": [scan_number(s) for s in last["scan_id"]],
                        "page_number": [int(t) for t in last["text"]]})
    return out.sort_values("scan_no", kind="stable").reset_index(drop=True)


def anchors_array(anchors_df):
    """-> float array (n, 2) ``[scan_no, page_number]`` sorted (stably) by ``scan_no``."""
    return anchors_df.sort_values("scan_no", kind="stable")[["scan_no", "page_number"]].to_numpy(float)


# ---------------------------------------------------------------- chains
def longest_increasing_chain(values):
    """Indices of a longest strictly increasing subsequence (patience sorting)."""
    tails, tail_idx, prev = [], [], [-1] * len(values)
    for i, v in enumerate(values):
        k = int(np.searchsorted(tails, v, side="left"))
        if k == len(tails):
            tails.append(v); tail_idx.append(i)
        else:
            tails[k] = v; tail_idx[k] = i
        prev[i] = tail_idx[k - 1] if k else -1
    out, i = [], tail_idx[-1] if tail_idx else -1
    while i >= 0:
        out.append(i); i = prev[i]
    return out[::-1]


def main_chain(anchors):
    """anchors: (n,2) [scan_no, page_number] in scan order -> (chain array, kept mask)."""
    idx = longest_increasing_chain(anchors[:, 1].tolist())
    keep = np.zeros(len(anchors), bool); keep[idx] = True
    return anchors[keep], keep


def numbering_restarts(anchors, keep, min_len=3, max_rounds=4):
    """Additional monotone runs from the anchors the main chain discarded (numbering restarts)."""
    runs = []
    left = anchors[~keep]
    for _ in range(max_rounds):
        if len(left) < min_len:
            break
        idx = longest_increasing_chain(left[:, 1].tolist())
        if len(idx) < min_len:
            break
        mask = np.zeros(len(left), bool); mask[idx] = True
        runs.append(left[mask]); left = left[~mask]
    return runs


def numbering_mode(chain, *, foliation=1.5, pagination=0.75):
    """foliation (~2 scans/number), pagination (~1), spread (<1 scan/number) -> (mode, slope)."""
    if len(chain) < 3:
        return "unknown", np.nan
    d_scan, d_page = np.diff(chain[:, 0]), np.diff(chain[:, 1])
    slope = float(np.median(d_scan / d_page))
    if slope >= foliation:
        return "foliation", slope
    if slope >= pagination:
        return "pagination", slope
    return "spread", slope


def interpolate_folio_to_scan(folio, run, extrap=False, max_scan=None, *, edge=6):
    """-> (scan float, distance to nearest anchor scan, inside the anchor range).

    Outside the range ``np.interp`` clamps to the end anchor; with ``extrap`` (and >= 3 anchors)
    the median slope of the ``edge`` nearest anchors extrapolates, clamped to [1, max_scan]
    (``max_scan`` falsy: no upper clamp).
    """
    pages, scans = run[:, 1], run[:, 0]
    inside = pages[0] <= folio <= pages[-1]
    s = float(np.interp(folio, pages, scans))
    if extrap and not inside and len(run) >= 3:
        part = run[:edge] if folio < pages[0] else run[-edge:]
        slope = float(np.median(np.diff(part[:, 0]) / np.diff(part[:, 1])))
        s = (scans[0] + (folio - pages[0]) * slope if folio < pages[0]
             else scans[-1] + (folio - pages[-1]) * slope)
        s = max(1.0, min(s, max_scan or s))
    dist = float(np.min(np.abs(scans - s)))
    return s, dist, inside


def confidence_band(dist, inside, bands=CONFIDENCE_BANDS):
    if not inside:
        return "low"
    return "high" if dist <= bands[0] else "medium" if dist <= bands[1] else "low"


def _locate(f, main, runs, prev_scan, cfg, max_scan=None, run_window=3):
    """-> (scan float, dist, inside, method). Baseline = clamped interp on the main chain."""
    if not cfg.runs or not runs:
        s, d, ins = interpolate_folio_to_scan(f, main, cfg.extrap, max_scan)
        return s, d, ins, "chain"
    cands = []
    for r in [main] + runs:
        if len(r) >= 2 and r[0, 1] - run_window <= f <= r[-1, 1] + run_window:
            sc, d, ins = interpolate_folio_to_scan(f, r)
            cands.append((sc, d, ins, "chain" if r is main else "run"))
    ok = [c for c in cands if c[0] >= prev_scan - 1 and c[2]]
    main_ok = [c for c in ok if c[3] == "chain"]
    if main_ok:  # the main chain wins whenever it is in range and keeps scan order monotone
        return main_ok[0]
    if ok:       # a restart: the nearest following run
        return min(ok, key=lambda c: c[0] - prev_scan)
    s, d, ins = interpolate_folio_to_scan(f, main, cfg.extrap, max_scan)
    return s, d, ins, "chain_fallback"


def align_documents(docs, anchors, cfg=AlignmentConfig(), max_scan=None):
    """docs: index rows of ONE inventory in index order (columns ``folio_start``, ``folio_end``).

    -> DataFrame (index of ``docs``) with ``start_scan, start_scan_alt, end_scan, numbering_mode,
    method, nearest_anchor_dist, confidence``.
    """
    out = []
    if anchors is None or len(anchors) < 2:
        for _ in range(len(docs)):
            out.append(dict(start_scan=np.nan, start_scan_alt=np.nan, end_scan=np.nan, numbering_mode="no_anchors",
                            method="no_anchors", nearest_anchor_dist=np.nan, confidence="none"))
        return pd.DataFrame(out, index=docs.index)
    if cfg.dup_first:
        anchors = anchors[np.r_[True, np.diff(anchors[:, 1]) != 0]]
    main, keep = main_chain(anchors)
    runs = numbering_restarts(anchors, keep) if cfg.runs else []
    mode, _ = numbering_mode(main) if cfg.mode else ("unused", np.nan)
    prev = -np.inf
    for fs, fe in zip(docs["folio_start"], docs["folio_end"]):
        if np.isnan(fs) or len(main) < 2:
            out.append(dict(start_scan=np.nan, start_scan_alt=np.nan, end_scan=np.nan, numbering_mode=mode,
                            method="no_folio", nearest_anchor_dist=np.nan, confidence="none"))
            continue
        s, d, ins, method = _locate(fs, main, runs, prev, cfg, max_scan)
        start = int(round(s))
        prev = max(prev, s) if cfg.runs else prev
        alt = start + 1 if mode == "foliation" else np.nan
        end = np.nan
        if not np.isnan(fe):
            e, _, _, _ = _locate(fe, main, runs, s, cfg, max_scan)
            end = int(round(e)) + (1 if mode == "foliation" else 0)
        out.append(dict(start_scan=start, start_scan_alt=alt, end_scan=end, numbering_mode=mode,
                        method=method + ("" if ins else "+extrapolated"), nearest_anchor_dist=d,
                        confidence=confidence_band(d, ins)))
    return pd.DataFrame(out, index=docs.index)


def align_all(index_df, anchors_by_inv, cfg=AlignmentConfig(), max_scans=None, *,
              inventory_column="inventory_number"):
    """Align every inventory of ``index_df``; returns ``index_df`` joined with the alignment columns.

    ``index_df.index`` must be unique (a join on duplicates would silently multiply rows).
    """
    if not index_df.index.is_unique:
        raise ValueError("index_df.index must be unique (use reset_index(drop=True))")
    max_scans = max_scans or {}
    parts = [align_documents(g, anchors_by_inv.get(inv), cfg, max_scans.get(inv))
             for inv, g in index_df.groupby(inventory_column, sort=False)]
    res = pd.concat(parts)
    return index_df.join(res)
