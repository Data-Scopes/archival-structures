"""v2 word breaks and unit text with character offsets (step 15hj).

``join_unit`` joins the segment texts of one unit exactly like ``join.join_region`` and also
returns, per segment, where it sits in the unit text, so that a token span maps back to line boxes
(R1). ``write_word_break_stats_part_v2`` and ``join_inventory_v2`` are the per-inventory workers of
``--stage wbc`` and ``--stage join`` for v2 layers. pagexml and pandas are imported inside functions.
"""

import re
import time
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import (
    read_tsv_gz, write_json_atomic, write_plain_tsv, write_tsv_gz)
from archival_structures.text_layer.join import ALL_GUARDS, _make_line, classify_boundary
from archival_structures.text_layer.word_breaks import (
    WORD_BREAK_STATS_COLUMNS, format_stat, word_break_stats)

WORD_BREAK_STATS_COLUMNS_V2 = WORD_BREAK_STATS_COLUMNS + ["all_freq_robust", "end_rel_frac_robust"]

JOIN_UNIT_COLUMNS = ["scan_id", "unit_id", "unit_type", "word_break_chars", "n_breaks_merged",
                     "ends_with_break_char", "text"]
JOIN_SEGMENT_COLUMNS = ["scan_id", "unit_id", "seg_order", "line_id", "seg_idx", "char_start",
                        "char_end", "line_offset", "break_char_end", "merged_with_next",
                        "boundary_kind"]


def join_unit(texts, wbcs, guards=ALL_GUARDS, both_ends_fix=True, lead_marks="",
              lead_marks_lower="", upper_guard=False):
    """Join a unit's segment texts (all non-empty, in ``seg_order``) like ``join_region``.

    Returns ``(text, spans, boundaries)``. ``spans[i] = (char_start, char_end, lead_dropped)`` with
    ``text[char_start:char_end] == texts[i][lead_dropped:]`` minus the trailing word-break char(s) the
    library removed when boundary *i* is a merge (two for ``--``, ``=-``, ``-„``). ``boundaries[i] = (kind, char)`` is the boundary
    after segment *i* (``("", "")`` after the last; ``char`` is ``""`` for ``plain``).

    ``lead_marks`` (step 15ir): on a merge boundary, a character of this set at the start of the
    next text, followed by a letter, is dropped as well (a break mark repeated on the next line
    that the HTR read as e.g. ``,``). ``lead_dropped`` is always the real number of leading
    characters missing from the unit text: it also counts a second leading break character the
    library strips by itself (``["vande„", "„„selve", "meer"]``). Step 15l: ``lead_marks`` strips one
    or two marks and an optional single space (``", rant"``, ``",, den"``); ``lead_marks_lower``
    strips one such character when a lower-case letter follows (``"Jdende"``, the repeated hyphen
    read as ``J``; not before a capital); ``upper_guard`` refuses a merge whose right side then
    starts upper-case (boundary kind ``g_upper_right``, both lines kept as they were).
    """
    from pagexml.helper.pagexml_helper import make_text_region_text
    n = len(texts)
    if n == 0:
        return "", [], []
    lines = list(texts)
    lead = [0] * n
    boundaries = [("", "")] * n
    runs, current = [], [0]
    for i in range(n - 1):
        prev, nxt = lines[i], lines[i + 1]
        kind, char = classify_boundary(prev, nxt, wbcs, guards)
        boundaries[i] = (kind, char or "")
        if kind == "plain":
            current.append(i + 1)
            continue
        if kind != "merge":
            runs.append(current)
            current = [i + 1]
            continue
        if both_ends_fix and nxt[0] in wbcs:
            lines[i + 1] = nxt[1:]
        t = lines[i + 1]
        if lead_marks:   # step 15l: one or two marks, an optional space, then a letter
            m = re.match(f"[{re.escape(lead_marks)}]{{1,2}} ?(?=[^\\W\\d_])", t)
            if m:
                t = t[m.end():]
        if lead_marks_lower and t[:1] in lead_marks_lower and t[1:2].islower():
            t = t[1:]
        if upper_guard and t[:1].isupper():   # step 15l: undo, no merge
            boundaries[i] = ("g_upper_right", char or "")
            lines[i + 1] = nxt
            runs.append(current)
            current = [i + 1]
            continue
        lines[i + 1] = t
        lead[i + 1] = len(nxt) - len(t)
        current.append(i + 1)
    runs.append(current)

    spans = [None] * n
    pieces, base = [], 0
    for run in runs:
        run_text, ranges = make_text_region_text(
            [_make_line(lines[i], k) for k, i in enumerate(run)], word_break_chars=set(wbcs))
        run_text = run_text or ""
        for k, i in enumerate(run):
            r = ranges[k]
            end = r["end"]
            if k < len(run) - 1 and boundaries[i][0] == "plain":
                end -= 1   # the separating space is not part of the segment
            seg = run_text[r["start"]:end]
            fed = lines[i]
            extra = 0      # leading break chars the library strips by itself
            if fed[:1] in wbcs:
                while extra < len(fed) and fed[extra] in wbcs and not fed[extra:].startswith(seg):
                    extra += 1
                if not fed[extra:].startswith(seg):
                    extra = 0
            spans[i] = (base + r["start"], base + end, lead[i] + extra)
        pieces.append(run_text)
        base += len(run_text) + 1
    return " ".join(pieces), spans, boundaries


# ------------------------------------------------------------------ reading a layer

def read_unit_texts(layer_root, inv):
    """Yield ``(unit_row, segs)`` for every unit in ``unit_order`` (file order of the units table).

    ``segs`` is a list of dicts ``{seg_order, line_id, seg_idx, line_start, text}`` in ``seg_order``;
    ``text`` is ``lines.text[char_start:char_end]`` and ``line_start`` is ``char_start``. Segments
    with empty text are kept in the list (callers drop them before joining)."""
    root = Path(layer_root)
    line_text = {}
    for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
        line_text[(r["scan_id"], r["line_id"])] = r["text"]
    cuts = {}
    for r in read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"):
        cuts[(r["scan_id"], r["line_id"], r["seg_idx"])] = (int(r["char_start"]), int(r["char_end"]))
    by_unit = defaultdict(list)
    for r in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz"):
        by_unit[(r["scan_id"], r["unit_id"])].append(r)
    for unit in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz"):
        rows = sorted(by_unit.get((unit["scan_id"], unit["unit_id"]), []),
                      key=lambda r: int(r["seg_order"]))
        segs = []
        for r in rows:
            start, end = cuts[(r["scan_id"], r["line_id"], r["seg_idx"])]
            text = line_text[(r["scan_id"], r["line_id"])][start:end]
            segs.append({"seg_order": int(r["seg_order"]), "line_id": r["line_id"],
                         "seg_idx": r["seg_idx"], "line_start": start, "text": text})
        yield unit, segs


# ------------------------------------------------------------------ stage wbc worker

def write_word_break_stats_part_v2(inv, layer_root, robust=True):
    """Stage wbc, part for one inventory: wbc/parts/<inv>.tsv from the ``main_text`` units."""
    regions = [[s["text"] for s in segs if s["text"]]
               for unit, segs in read_unit_texts(layer_root, inv) if unit["unit_type"] == "main_text"]
    rows, info = word_break_stats(regions, inv, robust=robust)
    columns = WORD_BREAK_STATS_COLUMNS_V2 if robust else WORD_BREAK_STATS_COLUMNS
    write_plain_tsv(Path(layer_root) / "wbc" / "parts" / f"{inv}.tsv", columns,
                    [[format_stat(r[c]) for c in columns] for r in rows])
    return {"inventory_number": inv, "stage": "wbc", "status": "done", "n_rows": len(rows),
            "n_lines": info["n_lines"], "seconds": info["seconds"]}


# ------------------------------------------------------------------ stage join worker

def join_inventory_v2(inv, layer_root, wbcs, lead_marks="", lead_marks_lower="", upper_guard=False):
    """Stage join for one inventory: join/units, join/segments and join/stats."""
    start = time.monotonic()
    root = Path(layer_root)
    wbc_str = "".join(sorted(wbcs))
    unit_rows, seg_rows = [], []
    kinds = Counter()
    kinds_by_type = defaultdict(Counter)
    n_empty = n_segs = n_violations = n_units = 0
    for unit, segs in read_unit_texts(root, inv):
        n_units += 1
        full = [s for s in segs if s["text"]]
        n_empty += len(segs) - len(full)
        texts = [s["text"] for s in full]
        text, spans, bounds = join_unit(texts, wbcs, lead_marks=lead_marks,
                                       lead_marks_lower=lead_marks_lower, upper_guard=upper_guard)
        merged = 0
        for i, (s, (cs, ce, lead_dropped)) in enumerate(zip(full, spans)):
            kind, char = bounds[i]
            is_merge = kind == "merge"
            merged += is_merge
            if kind:
                kinds[kind] += 1
                kinds_by_type[unit["unit_type"]][kind] += 1
            seg_text = texts[i]
            # full span: what the unit text holds, plus only the trailing break chars the
            # library dropped on a merge (at most two), equals the segment text
            tail = seg_text[lead_dropped + (ce - cs):]
            if (text[cs:ce] != seg_text[lead_dropped:lead_dropped + (ce - cs)]
                    or len(tail) > (2 if is_merge else 0) or any(c not in wbcs for c in tail)):
                n_violations += 1
            seg_rows.append((unit["scan_id"], unit["unit_id"], s["seg_order"], s["line_id"],
                             s["seg_idx"], cs, ce, s["line_start"] + lead_dropped,
                             seg_text[-1] if seg_text[-1] in wbcs else "", int(is_merge), kind))
            n_segs += 1
        unit_rows.append((unit["scan_id"], unit["unit_id"], unit["unit_type"], wbc_str, merged,
                          int(bool(texts) and texts[-1][-1] in wbcs), text))
    write_tsv_gz(root / "join" / "units" / f"{inv}.tsv.gz", JOIN_UNIT_COLUMNS, unit_rows)
    write_tsv_gz(root / "join" / "segments" / f"{inv}.tsv.gz", JOIN_SEGMENT_COLUMNS, seg_rows)
    write_json_atomic(root / "join" / "stats" / f"{inv}.json", {
        "inventory_number": inv, "wbc_set": wbc_str, "guards": list(ALL_GUARDS),
        "lead_marks": lead_marks, "lead_marks_lower": lead_marks_lower, "upper_guard": upper_guard,
        "n_units": n_units, "n_segments": n_segs, "n_empty_segments_dropped": n_empty,
        "n_offset_violations": n_violations,
        "boundary_kinds": dict(sorted(kinds.items())),
        "boundary_kinds_by_unit_type": {t: dict(sorted(c.items()))
                                        for t, c in sorted(kinds_by_type.items())},
    })
    return {"inventory_number": inv, "stage": "join", "status": "done", "n_rows": n_units,
            "n_lines": n_segs, "seconds": round(time.monotonic() - start, 3)}
