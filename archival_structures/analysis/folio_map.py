"""Folio map: which scan carries folio N, from the printed page numbers of a volume.

Anchors are the first page-number segment of each side that parses as a number; the monotone main chain
(longest increasing subsequence) is interpolated for the gaps. Handwritten resolutions are foliated (numbers on
rectos only, folio N = recto of scan S plus verso of the next scan); a paginated volume numbers every side.
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


@dataclass(frozen=True)
class FolioTarget:
    number: int
    front: tuple
    back: Optional[tuple]
    position: int
    method: str
    anchor_dist: float
    confidence: str


@dataclass
class FolioMap:
    mode: str
    chain: np.ndarray
    n_anchors: int
    recto_share: float
    scan_ids: tuple = ()
    sides: tuple = ()
    params: FolioMapParams = FolioMapParams()

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
        return FolioTarget(number, (sid, page), back, position, method, dist, conf)

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
        return self._target(number, pos, "interpolated" if inside else "extrapolated", float(dist),
                            confidence_band(dist, inside, p.bands))

    def table(self) -> list:
        if self.mode == "none" or len(self.chain) == 0:
            return []
        e = self.params.extrapolate
        rows = []
        for n in range(max(1, int(self.chain[0, 1]) - e), int(self.chain[-1, 1]) + e + 1):
            t = self.locate(n)
            if t is None:
                continue
            rows.append(dict(number=n, front_scan_id=t.front[0], front_page=t.front[1],
                             back_scan_id=t.back[0] if t.back else "", back_page=t.back[1] if t.back else "",
                             position=t.position, method=t.method, anchor_dist=round(t.anchor_dist, 2),
                             confidence=t.confidence))
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
    idx = longest_increasing_chain([n for _, n in pts])
    chain = np.array([pts[i] for i in idx], float).reshape(-1, 2)
    mode = ("foliation" if foliation else "pagination") if len(chain) >= params.min_chain else "none"
    if mode == "none":
        chain = np.empty((0, 2))
    return FolioMap(mode, chain, n_anchors, share, tuple(scan_ids), tuple(tuple(s) for s in sides), params)
