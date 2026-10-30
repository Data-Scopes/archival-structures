"""Index -> resolution links: the front-index run, a volume's inputs, folio locators in entries and their links."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping, Optional, Sequence

from archival_structures.analysis import page_structure
from archival_structures.analysis.folio_map import FOLIO_MAP_V2, FolioMapParams, build_folio_map
from archival_structures.text_layer.presets import SG_HAND_COLUMN_ORDER_V5, SG_INDEX_NO_CUT
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
    variant: str             # "L1" ... "L7"
    separators: str = r"[\s.,:;]+"
    ordinal_after: str = ""  # L3+: matched right after the digits
    month_after: str = ""    # L3+: matched after r"[\s.,:]*" following the number
    quantity_after: str = "" # L3+: matched after r"[\s.,:]*" following the number
    amount_before: str = ""  # L3+: searched in the 4 characters before the number
    range_slack: int = 6     # L4, L7
    sub_entry_punct: bool = False  # L5+: a run followed by a capital needs one of .,:; in the separator (not a bare space)
    left_boundary: bool = False    # L6+: drop a number whose first digit follows a letter; amount_before on the whole prefix


SG_LOCATOR_PARAMS = {v: LocatorParams(variant=v, ordinal_after=r"(e|en|ste|de)\b", month_after=SG_MONTHS,
                                      quantity_after=r"(maenden|maanden|jaren|jaeren|dagen|daegen|weken|man|schepen|"
                                                     r"gulden|guldens|ponden|stuyvers|stuivers)\b",
                                      amount_before=r"(f|\u0192|fl|gl|\u00a3)\s*\.?\s*$")
                     for v in ("L1", "L2", "L3", "L4")}
# Step 21c: amount marker not preceded by a letter (so "Denhof 354." keeps 354), searched in the whole prefix.
SG_AMOUNT_BEFORE_V2 = r"(?<![^\W\d_])(f|\u0192|fl|gl|\u00a3)\s*\.?\s*$"
_V1 = SG_LOCATOR_PARAMS["L2"]
SG_LOCATOR_PARAMS["L5"] = LocatorParams(variant="L5", sub_entry_punct=True)
SG_LOCATOR_PARAMS["L6"] = LocatorParams(
    variant="L6", ordinal_after=_V1.ordinal_after, month_after=_V1.month_after, quantity_after=_V1.quantity_after,
    amount_before=SG_AMOUNT_BEFORE_V2, sub_entry_punct=True, left_boundary=True)
SG_LOCATOR_PARAMS["L7"] = LocatorParams(
    variant="L7", ordinal_after=_V1.ordinal_after, month_after=_V1.month_after, quantity_after=_V1.quantity_after,
    amount_before=SG_AMOUNT_BEFORE_V2, sub_entry_punct=True, left_boundary=True)
_EXCLUDING = ("L3", "L4", "L6", "L7")
_RANGED = ("L4", "L7")
_PUNCT = re.compile(r"[.,:;]")

_CAND = re.compile(r"(?<!\d)\d{1,4}(?!\d)")
_AFTER_SEP = r"[\s.,:]*"


def extract_locators(text: str, params: LocatorParams, *,
                     folio_range: Optional[tuple] = None) -> list:
    """Folio numbers at the end of an entry (or of a segment of it), by variant L1-L7 (see docs/index_links.md)."""
    if params.variant not in ("L1", "L2", "L3", "L4", "L5", "L6", "L7"):
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
        if rest == "" or (rest[0].isupper() and (not params.sub_entry_punct or _PUNCT.search(lead.group() if lead else ""))):
            out.extend(run)
    if params.variant in _EXCLUDING:
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
            if params.left_boundary and m.start() > 0 and text[m.start() - 1].isalpha():
                continue
            window = text[:m.start()] if params.left_boundary else text[max(0, m.start() - 4):m.start()]
            if params.amount_before and re.search(params.amount_before, window, re.I):
                continue
            keep.append(m)
        out = keep
    if params.variant in _RANGED and folio_range is not None:
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


# --- lines of a side and entry assembly (step 21c) ---------------------------------------------------------------

SG_ENTRY_END = r"(?<!\w)\d{1,4}[\s.,:;]*$"


@dataclass(frozen=True)
class LineBox:
    line_id: str
    text: str
    x: int
    y: int
    w: int
    h: int
    unit_id: str
    unit_type: str
    unit_order: int
    seg_order: int


@dataclass(frozen=True)
class Entry:
    kind: str                # "entry" | "heading"
    line_ids: tuple
    text: str
    spans: tuple             # (line_id, start, end) offsets into text


def read_side_lines(layer_root: Path, inv: str, sides: Sequence[tuple], *,
                    unit_types: Sequence[str] = ("main_text", "marginalia")) -> dict:
    """{(scan_id, page): [LineBox]} in base order (unit_order, seg_order) for the units of the given types.

    A line that is in two units appears once, at its first position. Furniture units are left out.
    """
    root = Path(layer_root)
    want = {tuple(s) for s in sides}
    units = {}
    for u in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz"):
        if (u["scan_id"], u["page"]) in want and u["unit_type"] in unit_types:
            units[(u["scan_id"], u["unit_id"])] = u
    lines = {}
    for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
        lines[(r["scan_id"], r["line_id"])] = r
    found: dict = {s: [] for s in want}
    for g in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz"):
        u = units.get((g["scan_id"], g["unit_id"]))
        ln = lines.get((g["scan_id"], g["line_id"]))
        if u is None or ln is None:
            continue
        key = (int(u["unit_order"]), int(g["seg_order"]), int(g.get("seg_idx") or 0))
        found[(u["scan_id"], u["page"])].append((key, LineBox(
            g["line_id"], ln["text"], int(ln["x"]), int(ln["y"]), int(ln["w"]), int(ln["h"]), g["unit_id"],
            u["unit_type"], int(u["unit_order"]), int(g["seg_order"]))))
    out = {}
    for side, items in found.items():
        items.sort(key=lambda t: t[0])
        seen, res = set(), []
        for _, lb in items:
            if lb.line_id not in seen:
                seen.add(lb.line_id)
                res.append(lb)
        out[side] = res
    return out


def side_reading_order(lines: Sequence[LineBox], cfg: dict) -> tuple:
    """-> (lines in reading order, reordered?). The phase-13 19f column geometry (``page_structure._reorder_lines``),
    run once per side on duck-typed lines and without the F13-R.10 guards; the base order and False when it keeps it."""
    objs = [SimpleNamespace(id=lb.line_id, text=lb.text, metadata={}, baseline=None,
                            coords=SimpleNamespace(x=lb.x, y=lb.y, w=lb.w, h=lb.h)) for lb in lines]
    new = page_structure._reorder_lines(SimpleNamespace(lines=objs, id="side", metadata={}), cfg, None)
    if new is None:
        return list(lines), False
    by_id = {id(o): lb for o, lb in zip(objs, lines)}
    return [by_id[id(o)] for o in new], True


def _entry(kind: str, parts: Sequence[tuple], wbc: str) -> Entry:
    """parts: [(line_id, stripped text)]. A line ending in a word-break character (not the last) is joined without it."""
    text, spans, glue = "", [], ""
    for i, (lid, t) in enumerate(parts):
        text += glue
        next_glue = " "
        if i < len(parts) - 1 and t and t[-1] in wbc:
            t, next_glue = t[:-1], ""
        spans.append((lid, len(text), len(text) + len(t)))
        text += t
        glue = next_glue
    return Entry(kind, tuple(p[0] for p in parts), text, tuple(spans))


def assemble_entries(lines: Sequence[LineBox], method: str, *, end_pattern: str = SG_ENTRY_END,
                     no_cut: str = SG_INDEX_NO_CUT, heading_max_words: int = 4, wbc: str = "-=„",
                     column_cfg: Optional[dict] = None) -> list:
    """Entries of one side from its lines. A1: the line rule on the base order; A2: column reorder first; A3: A2 plus
    the heading rule (a digit-free line of at most ``heading_max_words`` words at an entry start is a heading; known cost:
    a one-word first line such as "Voor" before a line ending in a number is also taken)."""
    if method not in ("A1", "A2", "A3"):
        raise ValueError(method)
    if method in ("A2", "A3"):
        lines, _ = side_reading_order(lines, column_cfg)
    end_re, cut_re = re.compile(end_pattern), re.compile(no_cut)
    done: list = []       # [kind, [(line_id, text)]]
    cur: list = []
    for lb in lines:
        t = (lb.text or "").strip()
        if not t:
            continue
        if not cur and done and done[-1][0] == "entry" and cut_re.fullmatch(t):
            done[-1][1].append((lb.line_id, t))
            continue
        if (method == "A3" and not cur and not re.search(r"\d", t) and any(c.isalpha() for c in t)
                and len(t.split()) <= heading_max_words):
            done.append(["heading", [(lb.line_id, t)]])
            continue
        cur.append((lb.line_id, t))
        if end_re.search(t):
            done.append(["entry", cur])
            cur = []
    if cur:
        done.append(["entry", cur])
    return [_entry(k, parts, wbc) for k, parts in done]


def entry_locators(entry: Entry, params: LocatorParams, *, folio_range: Optional[tuple] = None) -> list:
    """[(Locator, line_id)] of an entry; the line is the span that contains the locator's char_start. Headings give none."""
    if entry.kind != "entry":
        return []
    out = []
    for loc in extract_locators(entry.text, params, folio_range=folio_range):
        lid = entry.spans[-1][0]
        for sid, a, b in entry.spans:
            if a <= loc.char_start < b:
                lid = sid
                break
        out.append((loc, lid))
    return out


# --- the v2 layer (step 21d) -----------------------------------------------------------------------------------

# Selected by step 21c on dev (fixed rule, max dev link F1): A0 (v1 main_text units) + L7, dev link F1 0.830. Test bars
# (once): locator precision 404/412 pass; locator recall 404/472 = 0.856, attribution 355/398 = 0.892 and end-to-end
# 52/63 = 0.825 fail. Folio-map bar F passed in 21b (62/63).
V2_CONFIG = dict(map_params=FOLIO_MAP_V2, method="A0", variant="L7", column_cfg=SG_HAND_COLUMN_ORDER_V5)

ENTRY_COLUMNS = ["scan_id", "page", "entry_id", "kind", "first_line_id", "n_lines", "line_ids", "text"]
LINK_COLUMNS_V2 = ["scan_id", "page", "entry_id", "loc_idx", "number", "char_start", "char_end", "line_id",
                   "front_scan_id", "front_page", "back_scan_id", "back_page", "opening_scan_id", "opening_page",
                   "method", "anchor_dist", "confidence"]


def _a0_entries(front_sides: set, units: Sequence[dict], texts: Mapping, unit_segments: Mapping) -> list:
    """A0: [(scan_id, page, entry_id, text, [(line_id, char_start, char_end)])] for main_text units on the front sides."""
    us = [u for u in units if (u["scan_id"], u["page"]) in front_sides and u["unit_type"] == "main_text"]
    us.sort(key=lambda u: int(u.get("unit_order") or 0))   # stable: table order when there is no unit_order
    return [(u["scan_id"], u["page"], u["unit_id"], texts.get((u["scan_id"], u["unit_id"]), ""),
             list(unit_segments.get((u["scan_id"], u["unit_id"]), ()))) for u in us]


def volume_links_v2(inputs: VolumeInputs, side_lines: Mapping, units: Sequence[dict], texts: Mapping, *,
                    unit_segments: Optional[Mapping] = None, config: Mapping = V2_CONFIG, wbc: str = "-=„") -> Optional[dict]:
    """Entries and links of one volume's front index by ``config`` (map params, method A0-A3, locator variant).

    ``side_lines``: {(scan_id, page): [LineBox]} of the front sides (A1-A3 only). ``unit_segments``:
    {(scan_id, unit_id): [(line_id, char_start, char_end)]} from join/segments (A0 only). None when the volume has no
    front index. Else {"front", "map", "entries": [ENTRY_COLUMNS rows], "links": [LINK_COLUMNS_V2 rows]}; headings
    (A3) are entries with kind "heading" and no links.
    """
    run = front_index_run(inputs.flags)
    if run is None:
        return None
    method, variant = config["method"], config["variant"]
    if method not in ("A0", "A1", "A2", "A3"):
        raise ValueError(method)
    params = SG_LOCATOR_PARAMS[variant]
    _, last, _ = run
    front_sides = {s for i, s in enumerate(inputs.sides) if i <= last and inputs.flags[i]}
    excl = {s for i, s in enumerate(inputs.sides) if i <= last or inputs.flags[i]}
    fmap = build_folio_map(inputs.scan_ids, inputs.sides, inputs.numbers, exclude=excl, params=config["map_params"])
    rng = (int(fmap.chain[0, 1]), int(fmap.chain[-1, 1])) if len(fmap.chain) and variant in _RANGED else None
    # (scan_id, page, entry_id, kind, line_ids, text, [(Locator, line_id)])
    ents = []
    if method == "A0":
        for sid, page, eid, text, segs in _a0_entries(front_sides, units, texts, unit_segments or {}):
            locs = []
            for loc in extract_locators(text, params, folio_range=rng):
                lid = next((g[0] for g in segs if g[1] <= loc.char_start < g[2]), None)
                if lid is None and segs:    # 21c's fallback: the last segment starting before the locator, else the first
                    before = [g for g in segs if g[1] <= loc.char_start]
                    lid = before[-1][0] if before else segs[0][0]
                locs.append((loc, lid or ""))
            ents.append((sid, page, eid, "entry", tuple(g[0] for g in segs), text, locs))
    else:
        for side in sorted(front_sides, key=inputs.sides.index):
            for n, e in enumerate(assemble_entries(side_lines.get(side, []), method, wbc=wbc,
                                                   column_cfg=config["column_cfg"]), 1):
                ents.append((side[0], side[1], f"{side[0]}:{side[1]}:e{n:03d}", e.kind, e.line_ids, e.text,
                             entry_locators(e, params, folio_range=rng)))
    entries, links = [], []
    for sid, page, eid, kind, line_ids, text, locs in ents:
        entries.append(dict(scan_id=sid, page=page, entry_id=eid, kind=kind, first_line_id=line_ids[0] if line_ids else "",
                            n_lines=len(line_ids), line_ids="|".join(line_ids), text=text))
        for k, (loc, lid) in enumerate(locs):
            t = fmap.locate(loc.number)
            row = dict(scan_id=sid, page=page, entry_id=eid, loc_idx=k, number=loc.number, char_start=loc.char_start,
                       char_end=loc.char_end, line_id=lid)
            if t is None:
                row.update(front_scan_id="", front_page="", back_scan_id="", back_page="", opening_scan_id="",
                           opening_page="", method="none", anchor_dist="", confidence="")
            else:
                row.update(front_scan_id=t.front[0], front_page=t.front[1],
                           back_scan_id=t.back[0] if t.back else "", back_page=t.back[1] if t.back else "",
                           opening_scan_id=t.opening[0] if t.opening else "",
                           opening_page=t.opening[1] if t.opening else "",
                           method=t.method, anchor_dist=round(t.anchor_dist, 2), confidence=t.confidence)
            links.append(row)
    return {"front": run, "map": fmap, "entries": entries, "links": links}
