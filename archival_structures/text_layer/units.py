"""Units stage of the v2 text layer (step 15h, requirements R1 and R5).

Reads ``scans/``, ``regions_meta/``, ``lines/``, ``layout/`` and ``types/`` of one inventory
(never rewritten) and writes

* ``units/units/<inv>.tsv.gz``       one row per unit (hand: a layout region with its furniture
  pulled out; print: a paragraph inside one page column);
* ``units/segments/<inv>.tsv.gz``    the segments of every unit, in order;
* ``units/doc_id_map/<inv>.tsv.gz``  old (legacy region) ``doc_id`` -> ``unit_id``;
* ``units/republic_lines/<inv>.tsv.gz``  print only: REPUBLIC line ids -> v2 line ids;
* ``units/stats.json`` and ``units/params.json`` (written by the build stage).

No text is joined here (step 15hj does that). Schemas and rules: ``docs/text_layer.md``,
*Units and the doc_id mapping (step 15h)*.
"""

import csv
import dataclasses
import gzip
import re
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

from archival_structures.text_layer.extract_v2 import legacy_doc_id
from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz
from archival_structures.text_layer.types import TYPES

FURNITURE_TYPES = ("page_number", "running_header", "date_header", "catch_word", "signature_mark")
HEADER_TYPES = ("date_header", "running_header")
PAGE_ORDER = {"verso": 0, "recto": 1, "single": 1}
NEAR_PX = 5
# Step 19d. A hand unit is one layout region minus furniture, but on index pages one region holds many
# entries (F12-R.21, F12-15z.3: hand-pinned index volumes lost 26-50% of their entry starts against print).
# On sides flagged `index_side` by step 19c the mains are also cut at entry cues (phase 13 P11/P12).
INDEX_CUES = ("outdent", "indent", "short_prev", "date_start", "end_ref_prev")
INDEX_ENTRY_DEFAULTS = {"cues": ["outdent", "date_start", "end_ref_prev"], "outdent_frac": 0.5,
                        "indent_frac": 0.5, "short_frac": 0.1, "end_pattern": r"\d+\s*[.,:;]?\s*$",
                        "date_pattern": None,
                        # Step 19g (phase 13 review F13-R.2, F13-R.5): a main line that is only page numbers and
                        # punctuation is a reference list that ran over, never an entry start (None = off).
                        "no_cut_pattern": None,
                        # Step 19g: line types that join the mains on an index side. v4 types whole index pages
                        # `index` (rule:index_page), so those lines were in no main unit.
                        "main_types": ["main_text"]}
SPLIT_HALF_IOU = 0.5

UNIT_COLUMNS = ["scan_id", "page", "unit_id", "unit_order", "unit_type", "unit_method",
                "layout_region_id", "layout_column", "x", "y", "w", "h", "n_segments", "n_lines",
                "first_line_id", "last_line_id"]
UNIT_SEGMENT_COLUMNS = ["scan_id", "unit_id", "seg_order", "line_id", "seg_idx"]
DOC_ID_MAP_COLUMNS = ["old_doc_id", "unit_id", "link", "old_status", "n_segments_shared",
                      "n_chars_shared"]
REPUBLIC_COLUMNS = ["republic_line_id", "scan_id", "line_id", "match", "role"]


@dataclasses.dataclass(frozen=True)
class UnitParams:
    """Print paragraph cues; ``None`` switches a cue off. The ``header`` cue is always on."""
    indent_frac: float = None   # indent: x_i - col_left >= indent_frac * med_h
    short_frac: float = None    # short_prev: previous line ends left of col_right - frac * col_width
    gap_factor: float = None    # gap: vertical gap > gap_factor * med_h
    # step 15x: an `indent`-only cut within this many main lines of the last kept cut is dropped (a line
    # beside a dropped initial); None = off
    drop_cap_lines: int = None
    # step 19d (phase 13 P11/P12): entry cues for hand units on index sides (flagged by step 19c);
    # None = off. Keys: INDEX_ENTRY_DEFAULTS; ``cues`` is a subset of INDEX_CUES.
    index_entry: Optional[dict] = dataclasses.field(default=None, hash=False)

    def __post_init__(self):
        if self.index_entry is not None:
            unknown = set(self.index_entry) - set(INDEX_ENTRY_DEFAULTS)
            if unknown:
                raise ValueError(f"unknown index_entry keys: {sorted(unknown)}")
            bad = [c for c in self.index_entry.get("cues", INDEX_ENTRY_DEFAULTS["cues"])
                   if c not in INDEX_CUES]
            if bad:
                raise ValueError(f"unknown index_entry cues: {bad}")

    def to_dict(self):
        d = dataclasses.asdict(self)
        if d["drop_cap_lines"] is None:
            del d["drop_cap_lines"]
        if d["index_entry"] is None:
            del d["index_entry"]
        return d

    @classmethod
    def from_dict(cls, d):
        known = {f.name for f in dataclasses.fields(cls)}
        unknown = set(d) - known
        if unknown:
            raise ValueError(f"unknown UnitParams fields: {sorted(unknown)}")
        return cls(**d)


def _int(row, key):
    return int(row[key])


# ---------------------------------------------------------------- paragraph cues (print)

def paragraph_features(mains, header_between):
    """Cue values for each main segment i > 0 of a region.

    ``mains`` are segments with int x, y, w, h; ``header_between[i]`` says a date/running header
    segment lies between mains[i-1] and mains[i]. Returns a list (index i-1) of dicts with
    ``indent`` (in line heights), ``short`` (gap between the previous right edge and the column
    right edge, as a fraction of the column width), ``gap`` (vertical gap in line heights) and
    ``header``."""
    if len(mains) < 2:
        return []
    med_h = statistics.median(m["h"] for m in mains) or 1
    col_left = statistics.median(m["x"] for m in mains)
    col_right = statistics.median(m["x"] + m["w"] for m in mains)
    col_w = (col_right - col_left) or 1
    out = []
    for i in range(1, len(mains)):
        prev, cur = mains[i - 1], mains[i]
        out.append({"indent": (cur["x"] - col_left) / med_h,
                    "short": (col_right - (prev["x"] + prev["w"])) / col_w,
                    "gap": (cur["y"] - (prev["y"] + prev["h"])) / med_h,
                    "header": bool(header_between[i])})
    return out


def entry_cues(mains, header_between, cfg):
    """Index-entry cues (step 19d) for each main segment i >= 1 of a region.

    ``mains`` carry int x, y, w, h and ``text``; ``cfg`` is an ``index_entry`` dict (missing keys take
    INDEX_ENTRY_DEFAULTS). Returns a list (index i-1) of cue-name lists in INDEX_CUES order, ``header``
    last (always on). Only the cues in ``cfg["cues"]`` fire."""
    cfg = {**INDEX_ENTRY_DEFAULTS, **cfg}
    if len(mains) < 2:
        return []
    on = set(cfg["cues"])
    med_h = statistics.median(m["h"] for m in mains) or 1
    col_left = statistics.median(m["x"] for m in mains)
    col_right = statistics.median(m["x"] + m["w"] for m in mains)
    col_w = (col_right - col_left) or 1
    out = []
    for i in range(1, len(mains)):
        prev, cur = mains[i - 1], mains[i]
        t_cur = (cur.get("text") or "").strip()
        t_prev = (prev.get("text") or "").strip()
        fired = []
        if cfg["no_cut_pattern"] is not None and re.fullmatch(cfg["no_cut_pattern"], t_cur):
            # step 19g: a page-reference list that ran over (`358, 540, 546.`) never starts an entry;
            # only a header (a date/running header between the lines) still cuts
            if header_between[i]:
                fired.append("header")
            out.append(fired)
            continue
        if "outdent" in on and (prev["x"] - cur["x"]) / med_h >= cfg["outdent_frac"]:
            fired.append("outdent")
        if "indent" in on and (cur["x"] - col_left) / med_h >= cfg["indent_frac"]:
            fired.append("indent")
        if "short_prev" in on and (col_right - (prev["x"] + prev["w"])) / col_w > cfg["short_frac"]:
            fired.append("short_prev")
        if ("date_start" in on and cfg["date_pattern"] is not None
                and re.match(cfg["date_pattern"], t_cur, re.IGNORECASE)):
            fired.append("date_start")
        if "end_ref_prev" in on and re.search(cfg["end_pattern"], t_prev):
            fired.append("end_ref_prev")
        if header_between[i]:
            fired.append("header")
        out.append(fired)
    return out


def cues_fired(feat, params):
    cues = []
    if params.indent_frac is not None and feat["indent"] >= params.indent_frac:
        cues.append("indent")
    if params.short_frac is not None and feat["short"] > params.short_frac:
        cues.append("short_prev")
    if params.gap_factor is not None and feat["gap"] > params.gap_factor:
        cues.append("gap")
    if feat["header"]:
        cues.append("header")
    return cues


# ---------------------------------------------------------------- units of one layout region

def split_region(segs, params, print_mode, index_side=False):
    """Split the segments of one layout region (in region order) into units.

    Returns a list of dicts ``{"type", "method", "segs"}`` ordered by their first segment."""
    segs = [dict(s) for s in segs]
    if not print_mode and index_side and params.index_entry is not None:
        # step 19g: lines of the configured types (default only main_text) are the mains of an index side
        main_types = {**INDEX_ENTRY_DEFAULTS, **params.index_entry}["main_types"]
        for s in segs:
            if s["line_type"] in main_types:
                s["line_type"] = "main_text"
    for pos, s in enumerate(segs):
        s["_pos"] = pos
    groups = []   # (first_pos, type, method, segs)
    prefix = "print" if print_mode else "hand"
    # runs of one non-main type, adjacent in region order
    run = None
    for s in segs:
        if s["line_type"] == "main_text":
            run = None
            continue
        if run is not None and run[1] == s["line_type"] and run[3][-1]["_pos"] == s["_pos"] - 1:
            run[3].append(s)
        else:
            run = [s["_pos"], s["line_type"], f"{prefix}:run", [s]]
            groups.append(run)
    # main text
    mains = [s for s in segs if s["line_type"] == "main_text"]
    if mains:
        header_between = [False] * len(mains)
        for i in range(1, len(mains)):
            header_between[i] = any(segs[p]["line_type"] in HEADER_TYPES
                                    for p in range(mains[i - 1]["_pos"] + 1, mains[i]["_pos"]))
        cuts = {}   # i -> cues
        if print_mode:
            last = 0   # index of the last kept cut (0 = the region's first main line)
            for i, feat in enumerate(paragraph_features(mains, header_between), start=1):
                cues = cues_fired(feat, params)
                if (cues == ["indent"] and params.drop_cap_lines is not None
                        and i - last <= params.drop_cap_lines):
                    continue   # step 15x: a line beside a dropped initial, not a new paragraph
                if cues:
                    cuts[i] = cues
                    last = i
        elif index_side and params.index_entry is not None:
            # step 19d: cut an index side's mains at entry cues
            for i, cues in enumerate(entry_cues(mains, header_between, params.index_entry), start=1):
                if cues:
                    cuts[i] = cues
        else:
            for i in range(1, len(mains)):
                if header_between[i]:
                    cuts[i] = ["header"]
        entry_mode = not print_mode and index_side and params.index_entry is not None
        cur = [mains[0]]
        pieces = [(None, cur)]
        for i in range(1, len(mains)):
            if i in cuts:
                cur = []
                pieces.append((cuts[i], cur))
            cur.append(mains[i])
        for cues, piece in pieces:
            if len(pieces) == 1:
                method = f"{prefix}:region"
            elif entry_mode:
                method = "hand:index_start" if cues is None else "hand:index:" + "+".join(cues)
            elif not print_mode:
                method = "hand:region_cut_at_header"
            elif cues is None:
                method = "print:region_start"
            else:
                method = "print:" + "+".join(cues)
            groups.append([piece[0]["_pos"], "main_text", method, piece])
    groups.sort(key=lambda g: g[0])
    return [{"type": g[1], "method": g[2], "segs": g[3]} for g in groups]


def _chars(s):
    return s["char_end"] - s["char_start"]


def _bbox(segs):
    x0 = min(s["x"] for s in segs)
    y0 = min(s["y"] for s in segs)
    x1 = max(s["x"] + s["w"] for s in segs)
    y1 = max(s["y"] + s["h"] for s in segs)
    return x0, y0, x1 - x0, y1 - y0


def build_units_page(scan_id, page, regions, params, print_mode, taken, claimable=None,
                     index_side=False):
    """Units of one page.

    ``regions`` is a list of ``(region_row, segs)``: ``region_row`` has ``layout_region_id``,
    ``layout_column`` (int), ``layout_role``; ``segs`` are the segments in (layout_line_idx,
    seg_idx) order with int geometry and char offsets. ``taken`` is the set of ids already in use
    in the scan (modified: new ids are added). ``claimable`` (step 15ir, optional) maps legacy
    region ids that are not layout region ids of the scan to the frozenset of their line ids;
    a unit whose own-box id is claimable and that holds all of those lines takes that id (the
    entry is removed from ``claimable``, so one unit holds it). Returns ``(units, n_collisions)``; each unit has
    ``unit_id, unit_type, unit_method, layout_region_id, layout_column, bbox, segs`` and the
    sort key ``_key``."""
    units = []
    n_collisions = 0
    pending = []
    for region_order, (region, segs) in enumerate(regions):
        if region["layout_role"] == "empty" or not segs:
            continue
        parts = split_region(segs, params, print_mode, index_side)
        mains = [i for i, p in enumerate(parts) if p["type"] == "main_text"]
        pool = mains or range(len(parts))
        best = max(pool, key=lambda i: (sum(_chars(s) for s in parts[i]["segs"]), -i))
        for i, p in enumerate(parts):
            u = {"unit_type": p["type"], "unit_method": p["method"], "segs": p["segs"],
                 "layout_region_id": region["layout_region_id"],
                 "layout_column": region["layout_column"], "scan_id": scan_id, "page": page,
                 "bbox": _bbox(p["segs"])}
            u["_key"] = (PAGE_ORDER.get(page, 1), region["layout_column"], u["bbox"][1],
                         region_order, p["segs"][0]["_pos"])
            if i == best:
                u["unit_id"] = region["layout_region_id"]
                taken.add(u["unit_id"])
            else:
                pending.append(u)
            units.append(u)
    for u in pending:
        x, y, w, h = u["bbox"]
        if claimable:
            own = legacy_doc_id(scan_id, {"x": x, "y": y, "w": w, "h": h})
            if own in claimable and claimable[own] <= {s["line_id"] for s in u["segs"]}:
                del claimable[own]
                u["unit_id"] = own
                taken.add(own)
                continue
        while True:
            uid = legacy_doc_id(scan_id, {"x": x, "y": y, "w": w, "h": h})
            if uid not in taken:
                break
            n_collisions += 1
            w += 1
        u["unit_id"] = uid
        taken.add(uid)
    return units, n_collisions


# ---------------------------------------------------------------- REPUBLIC crosswalk

_REPUBLIC_ID = re.compile(r"^(?P<scan>.+)-line-(?P<x>\d+)-(?P<y>\d+)-(?P<w>\d+)-(?P<h>\d+)$")


def parse_republic_id(rid):
    m = _REPUBLIC_ID.match(rid)
    if not m:
        return None
    return m["scan"], tuple(int(m[k]) for k in ("x", "y", "w", "h"))


def republic_crosswalk(republic_ids, lines_by_scan, tol=NEAR_PX, roles=None):
    """Map REPUBLIC line ids to v2 line ids.

    ``lines_by_scan``: scan_id -> list of ``(line_id, (x, y, w, h), split_from)`` in line order.
    ``roles``: optional id -> ``start``/``end``/``both`` (``read_republic_roles``); it fills the
    ``role`` column (empty without it). Returns rows ``[republic_line_id, scan_id, line_id, match,
    role]`` with match in exact, near,
    split_original (mapped to the first half; to the last half when the role is ``end``: REPUBLIC
    reads an unsplit merged line as part of the paragraph, so a paragraph that ends there ends in
    our last half), split_half (best overlap with one of our split halves,
    IoU >= 0.5; REPUBLIC's own split differs from ours) and unmatched (``line_id`` empty)."""
    exact_index = {}
    halves = {}
    for scan, lines in lines_by_scan.items():
        for line_id, box, split_from in lines:
            exact_index.setdefault((scan, box), line_id)
            if split_from:
                halves.setdefault(scan, {}).setdefault(split_from, []).append((line_id, box))
    rows = []
    for rid in republic_ids:
        role = roles.get(rid, "") if roles else ""
        parsed = parse_republic_id(rid)
        if parsed is None or parsed[0] not in lines_by_scan:
            rows.append([rid, parsed[0] if parsed else "", "", "unmatched", role])
            continue
        scan, box = parsed
        if (scan, box) in exact_index:
            rows.append([rid, scan, exact_index[(scan, box)], "exact", role])
            continue
        best = None
        for line_id, b, _ in lines_by_scan[scan]:
            diffs = [abs(p - q) for p, q in zip(box, b)]
            if max(diffs) <= tol and (best is None or sum(diffs) < best[0]):
                best = (sum(diffs), line_id)
        if best:
            rows.append([rid, scan, best[1], "near", role])
            continue
        x, y, w, h = box
        found = None
        for parts in halves.get(scan, {}).values():
            if len(parts) < 2:
                continue
            inside = [lid for lid, (bx, by, bw, bh) in parts
                      if bx >= x - tol and by >= y - tol and bx + bw <= x + w + tol
                      and by + bh <= y + h + tol]
            if len(inside) >= 2:
                found = inside[-1] if role == "end" else inside[0]
                break
        if found:
            rows.append([rid, scan, found, "split_original", role])
            continue
        # REPUBLIC splits merged print lines itself (inventory 456): its halves differ from ours
        # by 10-30 px in width, so take the split half with the best box overlap
        best = None
        for line_id, b, split_from in lines_by_scan[scan]:
            if not split_from:
                continue
            v = _iou(box, b)
            if v >= SPLIT_HALF_IOU and (best is None or v > best[0]):
                best = (v, line_id)
        if best:
            rows.append([rid, scan, best[1], "split_half", role])
        else:
            rows.append([rid, scan, "", "unmatched", role])
    return rows


def _iou(a, b):
    x0, y0 = max(a[0], b[0]), max(a[1], b[1])
    x1, y1 = min(a[0] + a[2], b[0] + b[2]), min(a[1] + a[3], b[1] + b[3])
    inter = max(0, x1 - x0) * max(0, y1 - y0)
    union = a[2] * a[3] + b[2] * b[3] - inter
    return inter / union if union else 0.0


def read_republic_ids(path):
    """Distinct ``line_start``/``line_end`` ids of a REPUBLIC paragraphs file (first-seen order)."""
    seen = {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE):
            for key in ("line_start", "line_end"):
                if row[key]:
                    seen.setdefault(row[key], None)
    return list(seen)


def read_republic_roles(path):
    """Id -> ``start`` (only ``line_start``), ``end`` (only ``line_end``) or ``both``, in the same
    first-seen order as ``read_republic_ids``."""
    roles = {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE):
            for key, role in (("line_start", "start"), ("line_end", "end")):
                rid = row[key]
                if rid:
                    roles[rid] = role if roles.get(rid, role) == role else "both"
    return roles


# ---------------------------------------------------------------- inventory

def _old_status(rows_for_region, old_doc_id, unit_sizes, total_segs):
    if not rows_for_region:
        return "empty"
    has_same = any(r["unit_id"] == old_doc_id for r in rows_for_region)
    if not has_same:
        return "changed"
    if len(rows_for_region) == 1:
        r = rows_for_region[0]
        if r["n_segments_shared"] == total_segs and unit_sizes[r["unit_id"]] == total_segs:
            return "identical"
    return "trimmed"


def doc_id_map_rows(old_regions, line_region, seg_unit, seg_chars, unit_sizes):
    """Mapping rows for every legacy region.

    ``old_regions``: legacy ids; ``line_region``: (scan, line_id) -> old id; ``seg_unit``:
    (scan, line_id, seg_idx) -> unit_id; ``seg_chars``: same key -> n chars; ``unit_sizes``:
    unit_id -> n segments."""
    per_region = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    total = Counter()
    for key, uid in seg_unit.items():
        old = line_region.get(key[:2])
        if old is None:
            continue
        cell = per_region[old][uid]
        cell[0] += 1
        cell[1] += seg_chars[key]
        total[old] += 1
    rows = []
    for old in old_regions:
        cells = per_region.get(old, {})
        recs = [{"unit_id": uid, "n_segments_shared": c[0], "n_chars_shared": c[1]}
                for uid, c in cells.items()]
        recs.sort(key=lambda r: (r["unit_id"] != old, r["unit_id"]))
        status = _old_status(recs, old, unit_sizes, total[old])
        if not recs:
            rows.append([old, "", "none", status, 0, 0])
            continue
        for r in recs:
            rows.append([old, r["unit_id"], "same_id" if r["unit_id"] == old else "part", status,
                         r["n_segments_shared"], r["n_chars_shared"]])
    return rows


def load_segments(root, inv):
    """Segments of types/segments with int fields plus layout_line_idx, grouped by
    (scan_id, page, layout_region_id) in region order."""
    lay_idx = {(r["scan_id"], r["line_id"]): int(r["layout_line_idx"])
               for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz")}
    by_region = defaultdict(list)
    for r in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
        s = dict(r)
        for k in ("seg_idx", "char_start", "char_end", "x", "y", "w", "h"):
            s[k] = int(r[k])
        s["layout_line_idx"] = lay_idx[(r["scan_id"], r["line_id"])]
        by_region[(r["scan_id"], r["page"], r["layout_region_id"])].append(s)
    for segs in by_region.values():
        segs.sort(key=lambda s: (s["layout_line_idx"], s["seg_idx"]))
    return by_region


def unit_inventory(inv, layer_root, params, *, print_mode=False, republic_path=None):
    """Write the ``units/`` tables of one inventory; return its stats dict."""
    start = time.monotonic()
    root = Path(layer_root)
    scans = list(read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz"))
    legacy = list(read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz"))
    layout_regions = list(read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"))
    by_region = load_segments(root, inv)

    lines = list(read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"))
    legacy_by_scan = defaultdict(list)
    for r in legacy:
        legacy_by_scan[r["scan_id"]].append(r)
    legacy_lines = defaultdict(set)
    for r in lines:
        if r["text"].strip():      # blank lines are in no unit
            legacy_lines[(r["scan_id"], r["region_id"])].add(r["line_id"])
    reserved = defaultdict(set)
    for r in legacy:
        reserved[r["scan_id"]].add(r["region_id"])
    regions_by_scan = defaultdict(lambda: defaultdict(list))
    for r in layout_regions:
        reserved[r["scan_id"]].add(r["layout_region_id"])
        regions_by_scan[r["scan_id"]][r["page"]].append(r)

    # step 19d: segment text (the entry cues need it); changes no output
    line_text = {(r["scan_id"], r["line_id"]): r["text"] for r in lines}
    for segs in by_region.values():
        for sg in segs:
            sg["text"] = line_text.get((sg["scan_id"], sg["line_id"]), "")[sg["char_start"]:sg["char_end"]]
    flagged = None
    if params.index_entry is not None and not print_mode:
        pages_path = root / "layout" / "pages" / f"{inv}.tsv.gz"
        if not pages_path.exists():
            raise FileNotFoundError(f"{pages_path}: run layout with index_sides (step 19c)")
        flagged = {(r["scan_id"], r["page"]) for r in read_tsv_gz(pages_path) if r["index_side"] == "1"}

    all_units = []
    n_collisions = 0
    for scan in scans:
        sid = scan["scan_id"]
        layout_ids = {r["layout_region_id"] for rr in regions_by_scan.get(sid, {}).values()
                      for r in rr}
        claimable = {r["region_id"]: frozenset(legacy_lines[(sid, r["region_id"])])
                     for r in legacy_by_scan[sid] if r["region_id"] not in layout_ids
                     and legacy_lines.get((sid, r["region_id"]))}
        scan_units = []
        for page, rrows in regions_by_scan.get(sid, {}).items():
            regions = []
            for r in rrows:
                reg = {"layout_region_id": r["layout_region_id"], "layout_role": r["layout_role"],
                       "layout_column": int(r["layout_column"]) if r["layout_column"] else 0}
                regions.append((reg, by_region.get((sid, page, r["layout_region_id"]), [])))
            units, nc = build_units_page(sid, page, regions, params, print_mode, reserved[sid],
                                         claimable,
                                         index_side=flagged is not None and (sid, page) in flagged)
            n_collisions += nc
            scan_units += units
        scan_units.sort(key=lambda u: u["_key"])
        for order, u in enumerate(scan_units):
            u["unit_order"] = order
        all_units += scan_units

    unit_rows, seg_rows = [], []
    seg_unit, seg_chars, unit_sizes = {}, {}, {}
    for u in all_units:
        x, y, w, h = u["bbox"]
        unit_rows.append([u["scan_id"], u["page"], u["unit_id"], u["unit_order"], u["unit_type"],
                          u["unit_method"], u["layout_region_id"], u["layout_column"], x, y, w, h,
                          len(u["segs"]), len({s["line_id"] for s in u["segs"]}),
                          u["segs"][0]["line_id"], u["segs"][-1]["line_id"]])
        unit_sizes[u["unit_id"]] = len(u["segs"])
        for order, s in enumerate(u["segs"]):
            seg_rows.append([u["scan_id"], u["unit_id"], order, s["line_id"], s["seg_idx"]])
            key = (u["scan_id"], s["line_id"], s["seg_idx"])
            seg_unit[key] = u["unit_id"]
            seg_chars[key] = _chars(s)

    n_seg_in = sum(len(v) for v in by_region.values())
    line_region = {(r["scan_id"], r["line_id"]): r["region_id"] for r in lines}
    map_rows = doc_id_map_rows([r["region_id"] for r in legacy], line_region, seg_unit, seg_chars,
                               unit_sizes)

    out = root / "units"
    write_tsv_gz(out / "units" / f"{inv}.tsv.gz", UNIT_COLUMNS, unit_rows)
    write_tsv_gz(out / "segments" / f"{inv}.tsv.gz", UNIT_SEGMENT_COLUMNS, seg_rows)
    write_tsv_gz(out / "doc_id_map" / f"{inv}.tsv.gz", DOC_ID_MAP_COLUMNS, map_rows)

    status_per_region = {}
    for r in map_rows:
        status_per_region[r[0]] = r[3]
    crosswalk = crosswalk_roles = None
    if print_mode and republic_path is not None and Path(republic_path).exists():
        lines_by_scan = defaultdict(list)
        for r in lines:
            if r["text"].strip():
                lines_by_scan[r["scan_id"]].append(
                    (r["line_id"], (int(r["x"]), int(r["y"]), int(r["w"]), int(r["h"])),
                     r["split_from"]))
        roles = read_republic_roles(republic_path)
        cw = republic_crosswalk(list(roles), lines_by_scan, roles=roles)
        write_tsv_gz(out / "republic_lines" / f"{inv}.tsv.gz", REPUBLIC_COLUMNS, cw)
        crosswalk = dict(Counter(r[3] for r in cw))
        crosswalk_roles = dict(Counter(f"{r[3]}|{r[4]}" for r in cw))
    else:   # step 15y: a crosswalk of an earlier build (e.g. as a print volume) is stale
        (out / "republic_lines" / f"{inv}.tsv.gz").unlink(missing_ok=True)

    per_type = Counter(u["unit_type"] for u in all_units)
    main_lens = [len({s["line_id"] for s in u["segs"]}) for u in all_units
                 if u["unit_type"] == "main_text"]
    return {
        "n_units": len(all_units), "n_segments": n_seg_in, "n_segments_in_units": len(seg_rows),
        "units_per_type": dict(per_type),
        "units_per_method": dict(Counter(u["unit_method"] for u in all_units)),
        "n_id_collisions": n_collisions,
        "median_main_unit_lines": statistics.median(main_lens) if main_lens else 0,
        "old_status": dict(Counter(status_per_region.values())),
        "n_old_regions": len(legacy), "republic_crosswalk": crosswalk,
        "republic_crosswalk_roles": crosswalk_roles,
        "seconds": round(time.monotonic() - start, 3)}
