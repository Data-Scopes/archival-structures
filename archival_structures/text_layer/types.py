"""Types stage of the v2 text layer (step 15g, requirement R4): line-segment and region types.

Reads ``scans/``, ``regions_meta/``, ``lines/`` and ``layout/`` of one inventory (never
rewritten) and writes

* ``types/segments/<inv>.tsv.gz``  one row per line segment (a line is split only by the three
  in-line rules below), with a type and the rule that gave it;
* ``types/regions/<inv>.tsv.gz``   one row per non-empty layout region, typed by character majority;
* ``types/stats.json`` and ``types/params.json`` (written by the build stage).

The old ``region_type`` stays retired. Schemas and rules: ``docs/text_layer.md``, *Type tables
(step 15g)*.
"""

import bisect
import dataclasses
import json
import re
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.analysis.page_numbering import longest_increasing_chain
from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz

TYPES = ("main_text", "page_number", "running_header", "date_header", "catch_word",
         "signature_mark", "marginalia", "index", "other")
COLUMN_OVERRIDE_TYPES = ("page_number", "running_header", "date_header", "catch_word",
                         "signature_mark")
TYPE_SEGMENT_COLUMNS = [
    "scan_id", "line_id", "seg_idx", "char_start", "char_end", "x", "y", "w", "h", "geom_method",
    "page", "layout_region_id", "line_type", "type_method", "column_override"]
TYPE_REGION_COLUMNS = [
    "scan_id", "page", "layout_region_id", "region_type", "type_method", "n_segments", "type_counts"]
PATTERN_FIELDS = ("pn_line", "pn_suffix", "date_line", "date_prefix", "month_only", "year_line",
                  "signature")
PAGE_ORDER = {"verso": 0, "recto": 1, "single": 1}


@dataclasses.dataclass(frozen=True)
class TypeParams:
    # patterns: empty = rule disabled (core defaults); the SG presets supply them
    pn_line: str = ""
    pn_suffix: str = ""
    date_line: str = ""
    date_prefix: str = ""
    month_only: str = ""
    year_line: str = ""
    signature: str = ""
    top_n: int = 3                  # top band: the n topmost text lines of a page
    bottom_n: int = 2               # bottom band
    long_line_frac: float = 0.6     # long line: w/page_w >= frac * med_w_rel
    margin_tol: float = 0.02        # a line starts in the margin if left of main left by > this * page_w
    cut_max_dist: float = 0.04      # margin_prefix cut: the space nearest to main left, within this * page_w
    edge_tol: float = 0.01          # page_margin: right edge <= main left + this * page_w
    header_min_letters: int = 4
    header_upper_frac: float = 0.8
    header_max_tokens: int = 6
    catch_max_chars: int = 15
    noise_min_letters: int = 2
    noise_min_digits: int = 2
    col_min_char_frac: float = 0.25  # a main column with this share of the main characters has its own reference
    index_rule: bool = True
    index_short_frac: float = 0.5   # a line is short if w/page_w < frac * med_w_rel
    index_min_short: float = 0.5    # share of short lines on an index page
    index_min_number: float = 0.3   # share of lines ending in a number
    column_override: bool = False   # print presets: "x" for furniture types
    margin_cut: str = "month_only"  # rule 2: "month_only" | "all" (15g) | "off"
    offchain_retype: bool = False   # 15k: decide `rule:pn_line_offchain` segments (gap / year / band)
    offchain_other_bands: tuple = ()  # bands ("top", "bottom") whose off-chain numbers become `other`
    offchain_years_other: bool = False  # off-chain year lines become `other` (`rule:year_offchain`)

    def __post_init__(self):
        if self.margin_cut not in ("month_only", "all", "off"):
            raise ValueError(f"margin_cut must be 'month_only', 'all' or 'off', "
                             f"got {self.margin_cut!r}")
        if not set(self.offchain_other_bands) <= {"top", "bottom"}:
            raise ValueError(f"offchain_other_bands must be a subset of ('top', 'bottom'), "
                             f"got {self.offchain_other_bands!r}")
        if self.top_n < 1 or self.bottom_n < 1:
            raise ValueError(f"top_n and bottom_n must be >= 1, got {self.top_n}, {self.bottom_n}")

    def to_dict(self):
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        if "offchain_other_bands" in d:
            d["offchain_other_bands"] = tuple(d["offchain_other_bands"])
        return cls(**d)


def _compile(params):
    flags = re.IGNORECASE
    out = {}
    for name in PATTERN_FIELDS:
        pattern = getattr(params, name)
        out[name] = re.compile(pattern, 0 if name == "signature" else flags) if pattern else None
    return out


def _letters(text):
    return sum(c.isalpha() for c in text)


def _digits(text):
    return sum(c.isdigit() for c in text)


def _span(text, start, end):
    """Shrink [start, end) to exclude surrounding whitespace; None if empty."""
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    return (start, end) if end > start else None


class Line:
    """One non-blank text line with its layout fields."""
    __slots__ = ("scan_id", "line_id", "text", "x", "y", "w", "h", "page", "layout_region_id",
                 "role", "column", "order")

    def __init__(self, row, lay, order):
        self.scan_id = row["scan_id"]
        self.line_id = row["line_id"]
        self.text = row["text"]
        self.x, self.y, self.w, self.h = (int(row[k]) for k in ("x", "y", "w", "h"))
        self.page = lay["page"]
        self.layout_region_id = lay["layout_region_id"]
        self.role = lay["layout_role"]
        # print: the line's own `column` field (1/2) wins over the layout column, because
        # 145 layout regions hold both columns (UA-008); hand lines have no such field
        self.column = f"f{row.get('column')}" if row.get("column") else f"l{lay['layout_column']}"
        self.order = order


def _ref(pool, page_w, med_w_rel, params):
    """(main left, main centre) of a line pool: medians over its long lines (all lines if none)."""
    long_lines = [l for l in pool if l.w / page_w >= params.long_line_frac * med_w_rel]
    use = long_lines or pool
    if not use:
        return None, None
    return (statistics.median(l.x for l in use), statistics.median(l.x + l.w / 2 for l in use))


def _page_context(lines, page_w, med_w_rel, params):
    """Return (ordered lines, page reference, column references) for one page.

    The page reference (main left, main centre) comes from the main column with the most
    characters. On a page with several text columns (two-column print, UA-008), every main column
    holding >= `col_min_char_frac` of the main characters has its own reference, which its lines
    use instead (a deviation from the brief, which had one reference per page; see the report)."""
    ordered = sorted(lines, key=lambda l: (l.y, l.x))
    mains = [l for l in ordered if l.role == "main"]
    if not mains:
        return ordered, _ref(ordered, page_w, med_w_rel, params), {}
    chars = Counter()
    for l in mains:
        chars[l.column] += len(l.text)
    best = max(chars, key=lambda c: (chars[c], c))
    total = sum(chars.values())
    default = _ref([l for l in mains if l.column == best], page_w, med_w_rel, params)
    cols = {c: _ref([l for l in mains if l.column == c], page_w, med_w_rel, params)
            for c in chars if chars[c] >= params.col_min_char_frac * total}
    return ordered, default, cols


def _pn_candidate(ordered, pats, params):
    """The page-number candidate of a page: (number, kind, line) or None."""
    bands = ordered[:params.top_n] + [l for l in ordered[-params.bottom_n:] if l not in ordered[:params.top_n]]
    top_band = ordered[:params.top_n]
    for line in bands:
        t = line.text.strip()
        if pats["pn_line"] and pats["pn_line"].fullmatch(t):
            if line in top_band and _is_year(t, pats):
                continue
            m = re.search(r"\d{1,4}", t)
            return int(m.group()), "pn_line", line
    if ordered and pats["pn_suffix"]:
        m = pats["pn_suffix"].search(ordered[0].text)
        if m and m.start() > 0 and ordered[0].text[:m.start()].strip():
            return int(m.group(1)), "pn_suffix", ordered[0]
    return None


def _is_year(text, pats):
    return bool(pats["year_line"] and pats["year_line"].fullmatch(text))


def segment_page(ordered, page_w, med_w_rel, default_ref, col_refs, params, pats,
                 chain_ok, index_page):
    """Segment and type the lines of one page. Returns a list of segment dicts (in `ordered`
    order, then split order) and a Counter of in-line splits per rule.

    `chain_ok` is True when the page's page-number candidate is a pn_suffix on the topmost line
    and its number is in the inventory's chain. `index_page` marks a page typed by the index rule."""
    splits = Counter()
    segs = []
    top_band = set(id(l) for l in ordered[:params.top_n])
    bottom_band = set(id(l) for l in ordered[-params.bottom_n:])
    bottom_most = ordered[-1] if ordered else None
    for pos, line in enumerate(ordered):
        text = line.text
        main_left, main_centre = (col_refs.get(line.column, default_ref) if line.role == "main"
                                  else default_ref)
        full = _span(text, 0, len(text))
        if full is None:
            continue
        pieces = []          # (start, end, type or None, method or None)
        start, end = full
        is_top = pos == 0
        in_margin = (main_left is not None
                     and line.x < main_left - params.margin_tol * page_w)
        # in-line rules 1 and 2 (prefix)
        done_prefix = False
        whole = any(pats[k] and pats[k].fullmatch(text.strip())
                    for k in ("date_line", "pn_line", "year_line"))
        if (is_top or in_margin) and pats["date_prefix"] and not whole:  # 15ir: whole date line stays one
            m = pats["date_prefix"].match(text, start)
            if m and m.end() < end and text[m.end():end].strip():
                sp = _span(text, start, m.end())
                if sp:
                    pieces.append((sp[0], sp[1], "date_header", "rule:date_prefix"))
                    splits["rule:date_prefix"] += 1
                    nxt = _span(text, m.end(), end)
                    start, end = nxt
                    done_prefix = True
        if (not done_prefix and in_margin and not whole and params.margin_cut != "off"
                and main_left is not None and line.x + line.w > main_left):
            n = len(text)
            best = None
            for i in range(start + 1, end):
                if text[i].isspace():
                    dist = abs(line.x + line.w * i / n - main_left)
                    if best is None or dist < best[0]:
                        best = (dist, i)
            if best is not None and best[0] <= params.cut_max_dist * page_w:
                i = best[1]
                sp = _span(text, start, i)
                nxt = _span(text, i, end)
                prefix = text[sp[0]:sp[1]] if sp else ""
                is_month = bool(pats["month_only"] and pats["month_only"].fullmatch(prefix))
                if sp and nxt and (is_month or params.margin_cut == "all"):
                    pieces.append((sp[0], sp[1], "date_header" if is_month else "marginalia",
                                   "rule:margin_prefix"))
                    splits["rule:margin_prefix"] += 1
                    start, end = nxt
        # in-line rule 3 (suffix page number)
        suffix = None
        if is_top and chain_ok and pats["pn_suffix"]:
            m = pats["pn_suffix"].search(text, start, end)
            if m and text[start:m.start()].strip():
                sp = _span(text, m.start(), end)
                suffix = (sp[0], sp[1], "page_number", "rule:pn_suffix")
                splits["rule:pn_suffix"] += 1
                end = _span(text, start, m.start())[1]
        # whole-line rules on the remaining main piece
        mt = text[start:end]
        typ, method = _whole_line(line, mt, pos, is_top, top_band, bottom_band, bottom_most,
                                  page_w, med_w_rel, main_left, main_centre, params, pats,
                                  index_page)
        pieces.append((start, end, typ, method))
        if suffix:
            pieces.append(suffix)
        n = len(text)
        split = len(pieces) > 1
        for seg_idx, (s, e, t, mth) in enumerate(pieces):
            if split:
                x = line.x + round(line.w * s / n)
                w = round(line.w * (e - s) / n)
                geom = "char_proportion"
            else:
                x, w, geom = line.x, line.w, "line"
            segs.append({
                "scan_id": line.scan_id, "line_id": line.line_id, "seg_idx": seg_idx,
                "char_start": s, "char_end": e, "x": x, "y": line.y, "w": w, "h": line.h,
                "geom_method": geom, "page": line.page, "layout_region_id": line.layout_region_id,
                "line_type": t, "type_method": mth,
                "column_override": "x" if params.column_override and t in COLUMN_OVERRIDE_TYPES else "",
                "_order": line.order})
    return segs, splits


def _whole_line(line, text, pos, is_top, top_band, bottom_band, bottom_most, page_w, med_w_rel,
                main_left, main_centre, params, pats, index_page):
    t = text.strip()
    in_top = id(line) in top_band
    in_bottom = id(line) in bottom_band
    # 1 page number (a running year in the top band is a header, see Brief defects)
    if pats["pn_line"] and (in_top or in_bottom) and pats["pn_line"].fullmatch(t):
        if not (in_top and _is_year(t, pats)):
            return "page_number", "rule:pn_line"        # the chain pass may relabel to _offchain
    if pats["date_line"] and pats["date_line"].fullmatch(t):
        return "date_header", "rule:date_line"
    if in_top:
        letters = [c for c in t if c.isalpha()]
        if _is_year(t, pats) or (
                len(letters) >= params.header_min_letters
                and sum(c.isupper() for c in letters) / len(letters) >= params.header_upper_frac
                and len(t.split()) <= params.header_max_tokens):
            return "running_header", "rule:header"
    if (line is bottom_most and len(t.split()) == 1 and len(t) <= params.catch_max_chars
            and main_centre is not None and line.x + line.w / 2 > main_centre):
        return "catch_word", "rule:catch_word"
    if in_bottom and pats["signature"] and pats["signature"].fullmatch(t):
        return "signature_mark", "rule:signature"
    if index_page:
        return "index", "rule:index_page"
    if line.role == "extra":
        return "marginalia", "rule:extra_role"
    if main_left is not None and line.x + line.w <= main_left + params.edge_tol * page_w:
        return "marginalia", "rule:page_margin"
    if _letters(t) < params.noise_min_letters and _digits(t) < params.noise_min_digits:
        return "other", "rule:noise"
    return "main_text", "rule:default"


def _is_index_page(ordered, page_w, med_w_rel, params, pats):
    if not params.index_rule or not ordered:
        return False
    short = sum(l.w / page_w < params.index_short_frac * med_w_rel for l in ordered)
    ends = sum(bool(re.search(r"\d{1,4}\.?$", l.text.strip())) for l in ordered)
    return short / len(ordered) >= params.index_min_short and ends / len(ordered) >= params.index_min_number


def _retype_offchain(page_segs, key, ordered, pats, params, here, page_keys,
                      chain_sorted, chain_num):
    """15k: decide the ``rule:pn_line_offchain`` segments of one page.

    1. the number lies strictly between the chain numbers of the nearest chain pages before and
       after the page: it is this page's number that lost the candidate slot (``rule:pn_line_gap``,
       stays ``page_number``);
    2. a year line, with ``offchain_years_other``: ``other`` / ``rule:year_offchain``;
    3. the line is in a band of ``offchain_other_bands``: ``other`` (method unchanged).
    """
    by_id = {l.line_id: l for l in ordered}
    top_ids = {l.line_id for l in ordered[:params.top_n]}
    i = bisect.bisect_left(chain_sorted, here)
    j = bisect.bisect_right(chain_sorted, here)
    p_num = chain_num[page_keys[chain_sorted[i - 1]]] if i > 0 else None
    q_num = chain_num[page_keys[chain_sorted[j]]] if j < len(chain_sorted) else None
    for s in page_segs:
        if s["type_method"] != "rule:pn_line_offchain":
            continue
        # the segment's own text: a pn_suffix-style piece such as "1718" of "Maart 1718" is a year
        text = by_id[s["line_id"]].text[s["char_start"]:s["char_end"]].strip()
        m = re.search(r"\d{1,4}", text)
        n = int(m.group()) if m else None
        if n is not None and p_num is not None and q_num is not None and p_num < n < q_num:
            s["type_method"] = "rule:pn_line_gap"
        elif params.offchain_years_other and _is_year(text, pats):
            s["line_type"] = "other"
            s["type_method"] = "rule:year_offchain"
        elif ("top" if s["line_id"] in top_ids else "bottom") in params.offchain_other_bands:
            s["line_type"] = "other"


def type_inventory(inv, layer_root, params):
    """Write the ``types/`` tables of one inventory; return its stats dict."""
    start_time = time.monotonic()
    root = Path(layer_root)
    pats = _compile(params)
    scans = list(read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz"))
    scan_order = {r["scan_id"]: i for i, r in enumerate(scans)}
    page_w = {r["scan_id"]: int(r["page_w"]) for r in scans}
    lay_lines = {(r["scan_id"], r["line_id"]): r
                 for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz")}
    lay_region_keys = [(r["scan_id"], r["page"], r["layout_region_id"])
                       for r in read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz")]
    stats_path = root / "layout" / "stats.json"
    med_w_rel = None
    if stats_path.exists():
        med_w_rel = json.loads(stats_path.read_text(encoding="utf-8")).get(inv, {}).get("med_w_rel")

    pages = defaultdict(list)
    n_lines = n_blank = 0
    order = 0
    for row in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
        n_lines += 1
        if not row["text"].strip():
            n_blank += 1
            continue
        pages[(row["scan_id"], lay_lines[(row["scan_id"], row["line_id"])]["page"])].append(
            Line(row, lay_lines[(row["scan_id"], row["line_id"])], order))
        order += 1
    if med_w_rel is None:
        rel = [l.w / page_w[l.scan_id] for ls in pages.values() for l in ls]
        med_w_rel = statistics.median(rel) if rel else 0.0

    # pass 1: page context and the page-number chain
    contexts = {}
    cands = []
    for key in sorted(pages, key=lambda k: (scan_order[k[0]], PAGE_ORDER.get(k[1], 1))):
        ordered, ref, cols = _page_context(pages[key], page_w[key[0]], med_w_rel, params)
        contexts[key] = (ordered, ref, cols)
        cand = _pn_candidate(ordered, pats, params)
        if cand:
            cands.append((key, cand))
    chain = longest_increasing_chain([c[1][0] for c in cands]) if cands else []
    cand_by_key = dict(cands)
    chain_set = {cands[i][0] for i in chain}
    chain_suffix = {k for k, c in cands if k in chain_set and c[1] == "pn_suffix"}
    page_keys = sorted(pages, key=lambda k: (scan_order[k[0]], PAGE_ORDER.get(k[1], 1)))
    page_pos = {k: i for i, k in enumerate(page_keys)}
    chain_num = {cands[i][0]: cands[i][1][0] for i in chain}   # chain page key -> number
    chain_sorted = sorted(page_pos[k] for k in chain_num)

    # pass 2: segments
    segs = []
    splits = Counter()
    n_index_pages = 0
    index_pages = []
    for key in page_keys:
        ordered, ref, cols = contexts[key]
        pw = page_w[key[0]]
        idx = _is_index_page(ordered, pw, med_w_rel, params, pats)
        if idx:
            n_index_pages += 1
            index_pages.append(list(key))
        page_segs, sp = segment_page(ordered, pw, med_w_rel, ref, cols, params, pats,
                                     key in chain_suffix, idx)
        # 15ir: a pn_line-shaped line is confirmed only when it is the in-chain candidate line
        cand = cand_by_key.get(key)
        confirmed_id = (cand[2].line_id if key in chain_set and cand and cand[1] == "pn_line"
                        else None)
        for s in page_segs:
            if s["type_method"] == "rule:pn_line" and s["line_id"] != confirmed_id:
                s["type_method"] = "rule:pn_line_offchain"
        if params.offchain_retype:
            _retype_offchain(page_segs, key, contexts[key][0], pats, params, page_pos[key],
                             page_keys, chain_sorted, chain_num)
        segs.extend(page_segs)
        splits.update(sp)
    segs.sort(key=lambda s: (s["_order"], s["seg_idx"]))

    n_seg = write_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz", TYPE_SEGMENT_COLUMNS,
                         ([s[c] for c in TYPE_SEGMENT_COLUMNS] for s in segs))

    # region types by character majority
    reg_chars = defaultdict(Counter)
    for s in segs:
        reg_chars[(s["scan_id"], s["page"], s["layout_region_id"])][s["line_type"]] += s["char_end"] - s["char_start"]
    reg_n = Counter((s["scan_id"], s["page"], s["layout_region_id"]) for s in segs)
    rows = []
    for key in lay_region_keys:
        counts = reg_chars.get(key)
        if not counts:
            continue
        best = max(TYPES, key=lambda t: (counts.get(t, 0), -TYPES.index(t)))
        rows.append([*key, best, "char_majority", reg_n[key],
                     ";".join(f"{t}:{counts[t]}" for t in TYPES if counts.get(t))])
    write_tsv_gz(root / "types" / "regions" / f"{inv}.tsv.gz", TYPE_REGION_COLUMNS, rows)

    seg_per_type = Counter(s["line_type"] for s in segs)
    chars_per_type = Counter()
    for s in segs:
        chars_per_type[s["line_type"]] += s["char_end"] - s["char_start"]
    rectos = {k for k in pages if k[1] in ("recto", "single")}
    with_pn = {(s["scan_id"], s["page"]) for s in segs if s["line_type"] == "page_number"}
    return {
        "n_lines": n_lines, "n_lines_blank": n_blank, "n_lines_text": order, "n_segments": n_seg,
        "n_pages": len(pages), "segments_per_type": dict(seg_per_type),
        "chars_per_type": dict(chars_per_type), "splits_per_rule": dict(splits),
        "methods": dict(Counter(f"{s['line_type']}|{s['type_method']}" for s in segs)),
        "chain_candidates": len(cands), "chain_length": len(chain),
        "n_candidates_pn_line": sum(c[1] == "pn_line" for _, c in cands),
        "n_candidates_pn_suffix": sum(c[1] == "pn_suffix" for _, c in cands),
        "n_rectos": len(rectos), "n_rectos_with_page_number": len(rectos & with_pn),
        "n_index_pages": n_index_pages, "med_w_rel": med_w_rel,
        "n_regions_typed": len(rows), "seconds": round(time.monotonic() - start_time, 3)}
