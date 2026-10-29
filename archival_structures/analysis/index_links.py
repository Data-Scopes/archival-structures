"""Index -> resolution links: the front-index run, a volume's inputs, folio locators in entries and their links."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence

from archival_structures.analysis.folio_map import FolioMapParams, build_folio_map
from archival_structures.text_layer.io import read_tsv_gz


def front_index_run(flags: Sequence[bool], *, min_flagged: int = 10, max_first: int = 10, gap: int = 20,
                    min_after: int = 300, max_share: float = 0.6) -> Optional[tuple]:
    """-> (first flagged, last flagged, end) as 0-based side indices, or None when there is no front index."""
    n = len(flags)
    if n == 0 or sum(bool(f) for f in flags) / n > max_share:
        return None
    end, run = None, 0
    for i, f in enumerate(flags):
        run = 0 if f else run + 1
        if run >= gap:
            end = i - gap + 1
            break
    if end is None:
        return None
    idx = [i for i in range(end) if flags[i]]
    if len(idx) < min_flagged or idx[0] >= max_first or n - end < min_after:
        return None
    return idx[0], idx[-1], end


@dataclass
class VolumeInputs:
    inv: str
    scan_ids: list
    sides: list
    flags: list
    numbers: dict


def read_volume_inputs(layer_root: Path, inv: str) -> VolumeInputs:
    root = Path(layer_root)
    pages_path = root / "layout" / "pages" / f"{inv}.tsv.gz"
    if not pages_path.exists():
        raise FileNotFoundError(pages_path)
    scan_ids = [r["scan_id"] for r in read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz")]
    sides, flags = [], []
    for r in read_tsv_gz(pages_path):
        sides.append((r["scan_id"], r["page"]))
        flags.append(r["index_side"] == "1")
    text = {(r["scan_id"], r["line_id"]): r["text"]
            for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
    numbers: dict = {}
    for s in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
        if s["line_type"] != "page_number":
            continue
        t = text.get((s["scan_id"], s["line_id"]), "")
        numbers.setdefault((s["scan_id"], s["page"]), []).append(t[int(s["char_start"]):int(s["char_end"])])
    return VolumeInputs(inv, scan_ids, sides, flags, numbers)


# --- locators -------------------------------------------------------------------------------------------------

# same alternation as presets._TYPE_MONTHS (private there)
SG_MONTHS = (r"(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|"
             r"oct|okt|nov|dec)")

DEFAULT_LOCATOR_VARIANT = "L2"  # chosen on dev by step 20e (F1 tie L2=L3=L4 -> lower)


@dataclass(frozen=True)
class Locator:
    number: int
    char_start: int          # offsets in the unit text
    char_end: int


@dataclass(frozen=True)
class LocatorParams:
    variant: str             # "L1" | "L2" | "L3" | "L4"
    separators: str = r"[\s.,:;]+"
    ordinal_after: str = ""  # L3+: matched right after the digits
    month_after: str = ""    # L3+: matched after r"[\s.,:]*" following the number
    quantity_after: str = "" # L3+: matched after r"[\s.,:]*" following the number
    amount_before: str = ""  # L3+: searched in the 4 characters before the number
    range_slack: int = 6     # L4


SG_LOCATOR_PARAMS = {v: LocatorParams(variant=v, ordinal_after=r"(e|en|ste|de)\b", month_after=SG_MONTHS,
                                      quantity_after=r"(maenden|maanden|jaren|jaeren|dagen|daegen|weken|man|schepen|"
                                                     r"gulden|guldens|ponden|stuyvers|stuivers)\b",
                                      amount_before=r"(f|\u0192|fl|gl|\u00a3)\s*\.?\s*$")
                     for v in ("L1", "L2", "L3", "L4")}

_CAND = re.compile(r"(?<!\d)\d{1,4}(?!\d)")
_AFTER_SEP = r"[\s.,:]*"


def extract_locators(text: str, params: LocatorParams, *,
                     folio_range: Optional[tuple] = None) -> list:
    """Folio numbers at the end of an entry (or of a segment of it), by variant L1-L4 (see docs/index_links.md)."""
    if params.variant not in ("L1", "L2", "L3", "L4"):
        raise ValueError(params.variant)
    sep = re.compile(params.separators)
    cands = list(_CAND.finditer(text))
    runs, cur = [], []
    for m in cands:
        if cur and sep.fullmatch(text[cur[-1].end():m.start()]):
            cur.append(m)
        else:
            if cur:
                runs.append(cur)
            cur = [m]
    if cur:
        runs.append(cur)
    out = []
    for run in runs:
        rest = text[run[-1].end():]
        lead = sep.match(rest)
        rest = rest[lead.end():] if lead else rest
        if params.variant == "L1":
            if rest == "":
                out.extend(run)
            continue
        if rest == "" or rest[0].isupper():
            out.extend(run)
    if params.variant in ("L3", "L4"):
        keep = []
        for m in out:
            after = text[m.end():]
            if params.ordinal_after and re.match(params.ordinal_after, after, re.I):
                continue
            tail = re.match(_AFTER_SEP, after).end()
            if params.month_after and re.match(params.month_after, after[tail:], re.I):
                continue
            if params.quantity_after and re.match(params.quantity_after, after[tail:], re.I):
                continue
            if params.amount_before and re.search(params.amount_before, text[max(0, m.start() - 4):m.start()], re.I):
                continue
            keep.append(m)
        out = keep
    if params.variant == "L4" and folio_range is not None:
        lo, hi = folio_range[0] - params.range_slack, folio_range[1] + params.range_slack
        out = [m for m in out if lo <= int(m.group()) <= hi]
    return [Locator(int(m.group()), m.start(), m.end()) for m in out]


LINK_COLUMNS = ["scan_id", "page", "unit_id", "loc_idx", "number", "char_start", "char_end", "front_scan_id",
                "front_page", "back_scan_id", "back_page", "method", "anchor_dist", "confidence"]


def volume_links(inputs: VolumeInputs, units: Sequence[dict], texts: Mapping,
                 params: LocatorParams, map_params: FolioMapParams = FolioMapParams()) -> Optional[dict]:
    """Links of one volume's front-index entries (unit_type main_text on flagged sides up to the last flagged one).

    None when the volume has no front index. Else {"front": (first, last, end), "map": FolioMap,
    "entries": [unit dicts], "links": [row dicts with LINK_COLUMNS]}.
    """
    run = front_index_run(inputs.flags)
    if run is None:
        return None
    _, last, _ = run
    front_sides = {s for i, s in enumerate(inputs.sides) if i <= last and inputs.flags[i]}
    excl = {s for i, s in enumerate(inputs.sides) if i <= last or inputs.flags[i]}
    fmap = build_folio_map(inputs.scan_ids, inputs.sides, inputs.numbers, exclude=excl, params=map_params)
    rng = (int(fmap.chain[0, 1]), int(fmap.chain[-1, 1])) if len(fmap.chain) else None
    entries = [u for u in units if (u["scan_id"], u["page"]) in front_sides and u["unit_type"] == "main_text"]
    links = []
    for u in entries:
        text = texts.get((u["scan_id"], u["unit_id"]), "")
        for k, loc in enumerate(extract_locators(text, params, folio_range=rng)):
            t = fmap.locate(loc.number)
            row = dict(scan_id=u["scan_id"], page=u["page"], unit_id=u["unit_id"], loc_idx=k, number=loc.number,
                       char_start=loc.char_start, char_end=loc.char_end)
            if t is None:
                row.update(front_scan_id="", front_page="", back_scan_id="", back_page="", method="none",
                           anchor_dist="", confidence="")
            else:
                row.update(front_scan_id=t.front[0], front_page=t.front[1],
                           back_scan_id=t.back[0] if t.back else "", back_page=t.back[1] if t.back else "",
                           method=t.method, anchor_dist=round(t.anchor_dist, 2), confidence=t.confidence)
            links.append(row)
    return {"front": run, "map": fmap, "entries": entries, "links": links}
