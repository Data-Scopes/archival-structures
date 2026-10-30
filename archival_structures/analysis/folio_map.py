"""Folio map: which scan carries folio N, from the printed page numbers of a volume.

Anchors are the first page-number segment of each side that parses as a number; the monotone main chain
(longest increasing subsequence) is interpolated for the gaps. Handwritten resolutions are foliated (numbers on
rectos only, folio N = recto of scan S plus verso of the next scan); a paginated volume numbers every side.

Map v2 (phase 15, `FOLIO_MAP_V2`) adds four opt-in rules, all off in the default `FolioMapParams()`:
G1 jump guard, G2 tied numbers, G3 gap-aware confidence, G4 opening side (see `FolioMapParams`).
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Collection, Mapping, Optional, Sequence

import numpy as np

from archival_structures.analysis.page_numbering import (
    CONFIDENCE_BANDS, confidence_band, interpolate_folio_to_scan, longest_increasing_chain)

NUMBER_PATTERN = r"^\D{0,3}(\d{1,4})\D{0,3}$"


@dataclass(frozen=True)
class FolioMapParams:
    number_pattern: str = NUMBER_PATTERN
    foliation_recto_share: float = 0.9
    min_chain: int = 3
    extrapolate: int = 6
    edge: int = 6
    bands: tuple = CONFIDENCE_BANDS
    # G1 jump guard (after the LIS): a chain step with dn > jump_k * dpos + jump_c cannot be a folio sequence
    # (typically a year or inventory number read as the last anchor); the chain is cut there and the longest piece
    # is kept. Off when None.
    jump_k: Optional[float] = None
    jump_c: float = 10.0
    # G2 tied numbers (before the LIS): a number printed at two positions at most dup_window apart is a misread of
    # a neighbour; every copy is dropped, since the earlier or the later one may be the wrong one. Off when None.
    dup_window: Optional[int] = None
    # G3 gap-aware confidence: an interpolated number is `high`/`medium` only if the bracketing anchors agree on the
    # step (|dn - dpos| <= gap_slack / 2 * gap_slack); a missing page between them makes the interpolation unsafe.
    gap_slack: Optional[float] = None
    # G4 opening side: in foliation, FolioTarget.opening is the verso of the target's own scan (the facing page).
    opening: bool = False


# Folio map v2, fixed at phase-15 planning with no tuning: G1 answers F14-R.3 (year-like chain ends: 94 links
# interpolated onto the last scans as `high`), G2 answers F14-20d.3 (a number misread on two nearby rectos, 3590),
# G3 answers F14-R.4 (confidence saw only the scan distance), G4 adds the opening side.
FOLIO_MAP_V2 = FolioMapParams(jump_k=1.0, jump_c=10.0, dup_window=2, gap_slack=2.0, opening=True)


@dataclass(frozen=True)
class FolioTarget:
    number: int
    front: tuple
    back: Optional[tuple]
    position: int
    method: str
    anchor_dist: float
    confidence: str
    opening: Optional[tuple] = None


@dataclass
class FolioMap:
    mode: str
    chain: np.ndarray
    n_anchors: int
    recto_share: float
    scan_ids: tuple = ()
    sides: tuple = ()
    params: FolioMapParams = FolioMapParams()
    n_dup_dropped: int = 0   # numbers G2 dropped (only set when dup_window is on)
    chain_cut: bool = False  # G1 cut the chain (only set when jump_k is on)

    @property
    def n_positions(self) -> int:
        return len(self.scan_ids) if self.mode == "foliation" else len(self.sides)

    def _target(self, number, position, method, dist, conf) -> FolioTarget:
        if self.mode == "pagination":
            return FolioTarget(number, tuple(self.sides[position - 1]), None, position, method, dist, conf)
        sid = self.scan_ids[position - 1]
        have = {p for s, p in self.sides if s == sid}
        page = "recto" if "recto" in have else "single" if "single" in have else None
        if page is None:
            page = next((p for s, p in self.sides if s == sid), "recto")
        back = None
        if position < len(self.scan_ids) and (self.scan_ids[position], "verso") in set(self.sides):
            back = (self.scan_ids[position], "verso")
        opening = (sid, "verso") if self.params.opening and (sid, "verso") in set(self.sides) else None
        return FolioTarget(number, (sid, page), back, position, method, dist, conf, opening)

    def locate(self, number: int) -> Optional[FolioTarget]:
        if self.mode == "none" or len(self.chain) == 0 or number < 1:
            return None
        p = self.params
        hit = np.nonzero(self.chain[:, 1] == number)[0]
        if len(hit):
            return self._target(number, int(self.chain[hit[0], 0]), "printed", 0.0,
                                confidence_band(0, True, p.bands))
        first, last = self.chain[0, 1], self.chain[-1, 1]
        if number < first - p.extrapolate or number > last + p.extrapolate:
            return None
        s, dist, inside = interpolate_folio_to_scan(number, self.chain, extrap=True,
                                                    max_scan=self.n_positions, edge=p.edge)
        pos = min(max(math.floor(s + 0.5), 1), self.n_positions)
        conf = confidence_band(dist, inside, p.bands)
        if p.gap_slack is not None and inside:
            j = int(np.searchsorted(self.chain[:, 1], number))
            off = abs((self.chain[j, 1] - self.chain[j - 1, 1]) - (self.chain[j, 0] - self.chain[j - 1, 0]))
            conf = ("high" if dist <= p.bands[0] and off <= p.gap_slack
                    else "medium" if dist <= p.bands[1] and off <= 2 * p.gap_slack else "low")
        return self._target(number, pos, "interpolated" if inside else "extrapolated", float(dist), conf)

    def table(self) -> list:
        if self.mode == "none" or len(self.chain) == 0:
            return []
        e = self.params.extrapolate
        rows = []
        for n in range(max(1, int(self.chain[0, 1]) - e), int(self.chain[-1, 1]) + e + 1):
            t = self.locate(n)
            if t is None:
                continue
            row = dict(number=n, front_scan_id=t.front[0], front_page=t.front[1],
                       back_scan_id=t.back[0] if t.back else "", back_page=t.back[1] if t.back else "",
                       position=t.position, method=t.method, anchor_dist=round(t.anchor_dist, 2),
                       confidence=t.confidence)
            if self.params.opening:
                row.update(opening_scan_id=t.opening[0] if t.opening else "",
                           opening_page=t.opening[1] if t.opening else "")
            rows.append(row)
        return rows


def build_folio_map(scan_ids: Sequence[str], sides: Sequence[tuple], numbers: Mapping,
                    *, exclude: Collection = (), params: FolioMapParams = FolioMapParams()) -> FolioMap:
    scan_pos = {s: i + 1 for i, s in enumerate(scan_ids)}
    pat = re.compile(params.number_pattern)
    excl = set(exclude)
    anchors = []  # (side position, scan position, page, number)
    for i, side in enumerate(sides):
        if side in excl or side[0] not in scan_pos:
            continue
        for text in numbers.get(side, ()):
            m = pat.match(text.strip())
            if m:
                anchors.append((i + 1, scan_pos[side[0]], side[1], int(m.group(1))))
                break
    n_anchors = len(anchors)
    share = sum(a[2] == "recto" for a in anchors) / n_anchors if n_anchors else float("nan")
    foliation = n_anchors > 0 and share >= params.foliation_recto_share
    if foliation:
        pts = [(a[1], a[3]) for a in anchors if a[2] == "recto"]
    else:
        pts = [(a[0], a[3]) for a in anchors]
    pts.sort()
    n_dup = 0
    if params.dup_window is not None:
        by_n: dict = {}
        for p, n in pts:
            by_n.setdefault(n, []).append(p)
        tied = {n for n, ps in by_n.items()
                if len(ps) > 1 and min(b - a for a, b in zip(ps, ps[1:])) <= params.dup_window}
        n_dup = len(tied)
        pts = [(p, n) for p, n in pts if n not in tied]
    idx = longest_increasing_chain([n for _, n in pts])
    chain = np.array([pts[i] for i in idx], float).reshape(-1, 2)
    cut = False
    if params.jump_k is not None and len(chain) > 1:
        d = np.diff(chain, axis=0)
        cuts = np.nonzero(d[:, 1] > params.jump_k * d[:, 0] + params.jump_c)[0]
        if len(cuts):
            cut = True
            bounds = [0] + [int(c) + 1 for c in cuts] + [len(chain)]
            a0, b0 = max(zip(bounds, bounds[1:]), key=lambda ab: (ab[1] - ab[0], -ab[0]))
            chain = chain[a0:b0]
    mode = ("foliation" if foliation else "pagination") if len(chain) >= params.min_chain else "none"
    if mode == "none":
        chain = np.empty((0, 2))
    return FolioMap(mode, chain, n_anchors, share, tuple(scan_ids), tuple(tuple(s) for s in sides), params,
                    n_dup_dropped=n_dup, chain_cut=cut)
