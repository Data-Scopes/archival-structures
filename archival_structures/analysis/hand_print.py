"""Hand or print: one mode per inventory, decided from line geometry (phase 10: 15p/15q; phase 12: 15u).

Step 15p showed that text density cannot tell handwriting from print (F10-15p.1). The re-plan of 15q
(F10-P.6, decisions P2/P3) found one size-independent feature that separates volumes: ``hrel`` =
line-box height / baseline pitch (on the dev volumes, printed line boxes are lower relative to their
pitch than handwritten ones; F10-R.1 shows why that does not generalise). This module
holds the rule:

1. ``region_geometry`` / ``region_features``: per region (>= ``min_lines`` non-empty lines), ``hrel``
   from the median line height and the median baseline pitch.
2. ``scan_value`` / ``scan_label``: per scan, the character-weighted median of the region ``hrel``,
   labelled ``print`` / ``hand`` / ``uncertain`` against two thresholds (``unlabelled`` below
   ``min_scan_chars``).
3. ``fit_thresholds``: thresholds from the 0.95 / 0.05 quantiles of print / hand development scans.
4. ``volume_mode``: ``print`` if the share of print among print + hand scans is >= ``print_share``.

Region and scan labels are diagnostics, not rules; only the volume mode is meant for use downstream.

**Status of the ``hrel`` rule: negative result.** The rule failed its pre-registered out-of-sample test (step 15q,
F10-15q.1: weighted agreement 0.768 hand / 0.893 print against 0.90) and does not transfer to
another segmentation model (F10-15q.4). Nothing in the library uses it; do not switch build rules
on it. It is kept so that the 15q numbers can be reproduced.

**Box-height rule (phase 12, step 15u, F10-R.3).** The absolute line-box height (raw pixels, ``h_med`` of a
region) separated all 76 labelled volumes seen in phase 10. ``volume_box_height`` is the character-weighted
median of ``h_med`` over qualifying regions (``hrel`` present and >= ``min_region_chars`` characters);
``box_height_mode`` compares it with ``BoxHeightParams.cut`` (``print`` if value <= cut). The cut is a required
parameter fitted by ``fit_box_height_cut``; nothing here is specific to an archive. ``scan_box_heights`` gives
per-scan values as diagnostics. This rule passed its pre-registered test (step 15u, F12-15u.1: 60/60); it uses raw
pixels, so it assumes one scan scale across the archive (F12-R.1).

The module knows nothing about an archive: the thresholds are required parameters. Pure Python plus
numpy; it does not import ``text_layer``.

Input rows are the v2 line rows (``region_id, scan_id, h, baseline, text_raw``); ``baseline`` is the
string ``"x,y x,y ..."``.
"""
from __future__ import annotations

from bisect import bisect_left
from dataclasses import asdict, dataclass, fields
from typing import Iterable, Optional, Sequence

import numpy as np

SCAN_LABELS = ("hand", "print", "uncertain", "unlabelled")
MODES = ("hand", "print")
MODE_METHODS = ("geometry", "default", "fixed")
BOX_HEIGHT_METHODS = ("box_height", "default", "fixed")


@dataclass(frozen=True)
class VolumeModeParams:
    t_lo: float
    t_hi: float
    min_lines: int = 3
    min_scan_chars: int = 200
    print_share: float = 0.5

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "VolumeModeParams":
        names = {f.name for f in fields(cls)}
        unknown = set(d) - names
        if unknown:
            raise ValueError(f"unknown VolumeModeParams keys: {sorted(unknown)}")
        p = cls(**d)
        if p.t_lo > p.t_hi:
            raise ValueError(f"t_lo {p.t_lo} > t_hi {p.t_hi}")
        return p

    def __post_init__(self):
        if self.t_lo > self.t_hi:
            raise ValueError(f"t_lo {self.t_lo} > t_hi {self.t_hi}")


def _baseline_y(baseline) -> Optional[float]:
    if not isinstance(baseline, str) or not baseline.strip():
        return None
    ys = []
    for pt in baseline.split():
        parts = pt.split(",")
        if len(parts) != 2:
            continue
        try:
            ys.append(float(parts[1]))
        except ValueError:
            continue
    if len(ys) < 2:
        return None
    return float(sum(ys) / len(ys))


def _num(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def region_geometry(lines: Sequence[dict], min_lines: int = 3) -> Optional[dict]:
    """Geometry of one region from its line rows (any order), or None without a feature.

    Only lines with non-empty ``text_raw`` count. Needs >= ``min_lines`` of them, a line height, and
    >= 2 positive baseline pitches."""
    ne = [ln for ln in lines if (ln.get("text_raw") or "") != ""]
    if len(ne) < min_lines:
        return None
    hs = [h for h in (_num(ln.get("h")) for ln in ne) if h is not None]
    if not hs:
        return None
    ys = sorted(y for y in (_baseline_y(ln.get("baseline")) for ln in ne) if y is not None)
    pitches = [b - a for a, b in zip(ys, ys[1:]) if b - a > 0]
    if len(pitches) < 2:
        return None
    h_med = float(np.median(hs))
    pitch_med = float(np.median(pitches))
    return {"n_lines": len(ne), "n_chars": sum(len(ln["text_raw"]) for ln in ne),
            "h_med": h_med, "pitch_med": pitch_med, "hrel": h_med / pitch_med}


def region_features(line_rows: Iterable[dict], min_lines: int = 3) -> list[dict]:
    """One dict per region (first-seen order): region_id, scan_id, n_lines, n_chars, h_med,
    pitch_med, hrel. Regions without a feature are included with ``hrel`` None."""
    groups: dict = {}
    for row in line_rows:
        key = row["region_id"]
        if key not in groups:
            groups[key] = (row.get("scan_id"), [])
        groups[key][1].append(row)
    out = []
    for rid, (sid, lines) in groups.items():
        g = region_geometry(lines, min_lines)
        if g is None:
            ne = [ln for ln in lines if (ln.get("text_raw") or "") != ""]
            g = {"n_lines": len(ne), "n_chars": sum(len(ln["text_raw"]) for ln in ne),
                 "h_med": None, "pitch_med": None, "hrel": None}
        out.append({"region_id": rid, "scan_id": sid, **g})
    return out


def scan_value(features: Sequence[dict]) -> tuple[Optional[float], int]:
    """(char-weighted median of hrel over featured regions or None, featured chars)."""
    feat = sorted(((f["hrel"], f["n_chars"]) for f in features if f.get("hrel") is not None),
                  key=lambda t: (t[0], t[1]))
    if not feat:
        return None, 0
    cum = np.cumsum([c for _, c in feat]).tolist()
    total = cum[-1]
    idx = min(bisect_left(cum, total / 2), len(feat) - 1)
    return feat[idx][0], int(total)


def scan_label(value: Optional[float], n_chars: int, params: VolumeModeParams) -> str:
    if value is None or n_chars < params.min_scan_chars:
        return "unlabelled"
    if value <= params.t_lo:
        return "print"
    if value >= params.t_hi:
        return "hand"
    return "uncertain"


def fit_thresholds(hand_values: Sequence[float], print_values: Sequence[float]) -> tuple[float, float]:
    """(t_lo, t_hi) from the 0.95 quantile of print and 0.05 quantile of hand scan values,
    sorted, rounded to 3 decimals."""
    t_print = float(np.quantile(np.asarray(print_values, dtype=float), 0.95))
    t_hand = float(np.quantile(np.asarray(hand_values, dtype=float), 0.05))
    lo, hi = sorted((t_print, t_hand))
    return round(lo, 3), round(hi, 3)


def volume_mode(line_rows: Iterable[dict], params: VolumeModeParams) -> dict:
    """Mode of one inventory from its line rows. See the module docstring."""
    return volume_mode_from_features(region_features(line_rows, params.min_lines), params)


def volume_mode_from_features(feats: Sequence[dict], params: VolumeModeParams) -> dict:
    """``volume_mode`` from ``region_features`` output (e.g. read back from a cache)."""
    by_scan: dict = {}
    for f in feats:
        by_scan.setdefault(f["scan_id"], []).append(f)
    scans = []
    counts = {"print": 0, "hand": 0, "uncertain": 0, "unlabelled": 0}
    for sid, fs in by_scan.items():
        value, n_chars = scan_value(fs)
        label = scan_label(value, n_chars, params)
        counts[label] += 1
        scans.append({"scan_id": sid, "n_regions_feat": sum(f["hrel"] is not None for f in fs),
                      "n_chars": n_chars, "value": value, "label": label})
    n_p, n_h = counts["print"], counts["hand"]
    if n_p + n_h == 0:
        mode, method, share = "hand", "default", None
    else:
        share = n_p / (n_p + n_h)
        mode, method = ("print" if share >= params.print_share else "hand"), "geometry"
    return {"mode": mode, "method": method, "print_scan_share": share,
            "n_scans": len(scans), "n_scans_labelled": n_p + n_h + counts["uncertain"],
            "n_scans_print": n_p, "n_scans_hand": n_h, "n_scans_uncertain": counts["uncertain"],
            "params": params.to_dict(), "scans": scans, "regions": feats}


# ---------------------------------------------------------------- box-height rule (phase 12, step 15u)
@dataclass(frozen=True)
class BoxHeightParams:
    cut: float
    min_region_chars: int = 100

    def __post_init__(self):
        if not self.cut > 0:
            raise ValueError(f"cut must be > 0, got {self.cut}")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "BoxHeightParams":
        names = {f.name for f in fields(cls)}
        unknown = set(d) - names
        if unknown:
            raise ValueError(f"unknown BoxHeightParams keys: {sorted(unknown)}")
        return cls(**d)


def _qualifying(features: Sequence[dict], min_region_chars: int) -> list[dict]:
    return [f for f in features if f.get("hrel") is not None and f.get("h_med") is not None
            and f["n_chars"] >= min_region_chars]


def _weighted_median(pairs: Sequence[tuple]) -> Optional[float]:
    """pairs of (value, n_chars); the scan_value rule."""
    if not pairs:
        return None
    srt = sorted(pairs, key=lambda t: (t[0], t[1]))
    cum = np.cumsum([c for _, c in srt]).tolist()
    idx = min(bisect_left(cum, cum[-1] / 2), len(srt) - 1)
    return srt[idx][0]


def volume_box_height(features: Sequence[dict], min_region_chars: int = 100) -> tuple[Optional[float], int, int]:
    """(char-weighted median of ``h_med`` over qualifying regions or None, their chars, their count)."""
    q = _qualifying(features, min_region_chars)
    if not q:
        return None, 0, 0
    value = _weighted_median([(f["h_med"], f["n_chars"]) for f in q])
    return value, int(sum(f["n_chars"] for f in q)), len(q)


def scan_box_heights(features: Sequence[dict], min_region_chars: int = 100) -> list[dict]:
    """Per scan (first-seen order): {scan_id, value, n_chars, n_regions} over qualifying regions."""
    by: dict = {}
    for f in features:
        by.setdefault(f["scan_id"], []).append(f)
    out = []
    for sid, fs in by.items():
        v, n, k = volume_box_height(fs, min_region_chars)
        out.append({"scan_id": sid, "value": v, "n_chars": n, "n_regions": k})
    return out


def fit_box_height_cut(hand_values: Sequence[float], print_values: Sequence[float]) -> float:
    """Cut between print (<= cut) and hand (> cut) volume values, rounded to 2 decimals.

    Separable: midpoint of max(print) and min(hand). Otherwise the midpoint of consecutive distinct pooled
    values with the lowest balanced error ((share of print > cut + share of hand <= cut) / 2); ties: lowest."""
    hv = sorted(float(v) for v in hand_values)
    pv = sorted(float(v) for v in print_values)
    if not hv or not pv:
        raise ValueError("need at least one hand and one print value")
    if pv[-1] < hv[0]:
        return round((pv[-1] + hv[0]) / 2, 2)
    pooled = sorted(set(hv) | set(pv))
    best, best_err = None, None
    for a, b in zip(pooled, pooled[1:]):
        c = (a + b) / 2
        err = (sum(v > c for v in pv) / len(pv) + sum(v <= c for v in hv) / len(hv)) / 2
        if best_err is None or err < best_err - 1e-12:
            best, best_err = c, err
    if best is None:   # a single distinct value: no candidate
        raise ValueError("no candidate cut: all values equal")
    return round(best, 2)


def box_height_mode(features: Sequence[dict], params: BoxHeightParams) -> dict:
    """Volume mode from region features: ``print`` if value <= cut else ``hand`` (method ``box_height``);
    no qualifying region -> ``hand`` / ``default``."""
    value, n_chars, n_regions = volume_box_height(features, params.min_region_chars)
    if value is None:
        mode, method, margin = "hand", "default", None
    else:
        mode, method, margin = ("print" if value <= params.cut else "hand"), "box_height", value - params.cut
    return {"mode": mode, "method": method, "value": value, "margin": margin, "n_chars": n_chars,
            "n_regions": n_regions, "params": params.to_dict()}
