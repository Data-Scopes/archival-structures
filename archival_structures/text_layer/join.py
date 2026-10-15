"""Stage 3 of the text layer: guarded running text per region (ported from NLP, step 15).

Reads ``lines/`` and ``regions_meta/`` through ``read_region_lines``, writes ``regions/``,
``join_stats/`` and optionally ``join_events/``. pagexml is imported inside functions only.
"""

import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_region_lines, write_json_atomic, write_tsv_gz
from archival_structures.text_layer.word_breaks import (
    first_non_wbc, joined_word, mid_line_vocabulary)

REGION_COLUMNS = ["scan_id", "region_id", "region_box", "region_tag", "region_text"]


def _make_line(text, idx):
    import pagexml.model.physical_document_model as pdm
    from pagexml.model.coords import Coords
    return pdm.PageXMLTextLine(doc_id=f"l{idx}", coords=Coords([(0, 0), (1, 0), (1, 1), (0, 1)]),
                               text=text)


def library_join(texts, wbcs):
    """make_text_region_text on bare strings."""
    from pagexml.helper.pagexml_helper import make_text_region_text
    text, _ = make_text_region_text([_make_line(t, i) for i, t in enumerate(texts)],
                                    word_break_chars=set(wbcs))
    return text or ""


# g_standalone and g_digit are the step file's guards. The other three were
# added after the pass-1 error analysis (report/74 §5): merges whose left side
# is not a letter, or whose right side starts with a digit, were 39/40 and
# 9/10 wrong (ledger splices such as `2500„` + `tesamen`). With them, a WBC
# joins only where it is letter-attached -- the same condition its Stage-2
# statistics are measured on.
SPEC_GUARDS = ("g_standalone", "g_digit")
ALL_GUARDS = SPEC_GUARDS + ("g_digit_left", "g_punct_left", "g_digit_right")


def classify_boundary(prev, nxt, wbcs, guards=ALL_GUARDS):
    """Decide what happens between two non-empty lines of one region.

    Returns (kind, char): kind is one of
      'plain'          -- prev does not end in a WBC; the library adds a space
      'merge'          -- prev ends in a WBC, no guard: the library joins
      'g_standalone'   -- trailing WBC preceded by whitespace, or prev (or nxt)
                          is only WBCs/whitespace: a filler or dash, not a hyphen
      'g_digit'        -- digit before the trailing WBC run and a digit at the
                          start of nxt (after its leading WBCs): ledger notation
      'g_digit_left'   -- digit before the trailing WBC run, nxt not a digit
      'g_punct_left'   -- neither letter, digit nor whitespace before it
      'g_digit_right'  -- letter before it, nxt starts with a digit
    """
    if not prev or not nxt or prev[-1] not in wbcs:
        return "plain", None
    last = prev[-1]
    wbc_str = "".join(wbcs)
    body = prev.rstrip(wbc_str)
    if "g_standalone" in guards and (not body.strip() or body[-1].isspace()
                                     or not nxt.strip(wbc_str).strip()):
        return "g_standalone", last
    right_digit = first_non_wbc(nxt, wbcs).isdigit()
    if "g_digit" in guards and body[-1:].isdigit() and right_digit:
        return "g_digit", last
    if "g_digit_left" in guards and body[-1:].isdigit():
        return "g_digit_left", last
    if "g_punct_left" in guards and body and not body[-1].isalpha() and not body[-1].isdigit():
        return "g_punct_left", last
    if "g_digit_right" in guards and right_digit:
        return "g_digit_right", last
    return "merge", last


def join_region(texts, wbcs, stats=None, events=None, vocab=None, hyph_vocab=(), context=None,
                guards=ALL_GUARDS, both_ends_fix=True):
    """Guarded running text for one region's line texts (Stage 3).

    1. drop empty lines; 2. both-ends fix on merge boundaries; 3. split into
    runs at guarded boundaries, `make_text_region_text` per run, runs joined
    with one space. Region edges are never touched.

    `vocab` (mid-line words) and `hyph_vocab` (mid-line hyphenated words) only
    feed the diagnostics; they never change the text. `guards` and
    `both_ends_fix` exist to measure what each step changes; the layer always
    runs with the defaults.
    """
    stats = stats if stats is not None else Counter()
    lines = [t for t in texts if t]
    stats["lines_empty_dropped"] += len(texts) - len(lines)
    if not lines:
        stats["regions_empty_text"] += 1
        return ""
    lines = list(lines)
    runs, current = [], [lines[0]]
    for i in range(len(lines) - 1):
        prev, nxt = lines[i], lines[i + 1]
        kind, char = classify_boundary(prev, nxt, wbcs, guards)
        if kind == "plain":
            if nxt[:1] in wbcs and nxt:
                stats[f"start_only|{nxt[0]}"] += 1
                _event(events, context, i, "start_only", nxt[0], prev, nxt)
            current.append(nxt)
            continue
        if kind != "merge":
            stats[f"{kind}|{char}"] += 1
            _event(events, context, i, kind, char, prev, nxt)
            runs.append(current)
            current = [nxt]
            continue
        # merge
        stats[f"merge|{char}"] += 1
        fixed = nxt
        if both_ends_fix and nxt[0] in wbcs:
            fixed = nxt[1:]
            where = "last_line" if i + 1 == len(lines) - 1 else "inner_line"
            stats[f"both_ends_fix|{nxt[0]}|{where}"] += 1
            _event(events, context, i, f"both_ends_fix_{where}", nxt[0], prev, nxt)
            lines[i + 1] = fixed
        body = prev.rstrip("".join(wbcs)) or " "
        left_class = "letter" if body[-1].isalpha() else "digit" if body[-1].isdigit() else "other"
        right_first = fixed.lstrip("".join(wbcs))[:1]
        right_class = ("lower" if right_first.islower() else "upper" if right_first.isupper()
                       else "digit" if right_first.isdigit() else "other")
        stats[f"merge_context|{left_class}>{right_class}"] += 1
        if left_class != "letter":
            _event(events, context, i, f"merge_after_{left_class}", char, prev, nxt)
        left, right, joined = joined_word(prev, fixed, char)
        if vocab is not None and joined:
            if (joined.lower() not in vocab and right.lower() in vocab
                    and right.lower().startswith(left.lower())):
                stats[f"diag_syllable_repetition|{char}"] += 1
                _event(events, context, i, "diag_syllable_repetition", char, prev, nxt, joined)
            if char == "-" and (right[:1].isupper() or f"{left}-{right}".lower() in hyph_vocab):
                stats["diag_hyphen_compound_lost"] += 1
                _event(events, context, i, "diag_hyphen_compound_lost", char, prev, nxt, joined)
        _event(events, context, i, "merge", char, prev, nxt, joined)
        current.append(fixed)
    runs.append(current)
    return " ".join(library_join(run, wbcs) for run in runs)


def _event(events, context, idx, kind, char, prev, nxt, joined=""):
    if events is not None:
        scan_id, region_id = context or ("", "")
        events.append((scan_id, region_id, idx, kind, char, prev, nxt, joined or ""))


def join_vocabulary(line_texts):
    """(mid-line words, mid-line hyphenated words), each seen at least twice."""
    hyph = Counter()
    for text in line_texts:
        for token in text.split()[1:-1]:
            if re.fullmatch(r"[^\W\d_]+-[^\W\d_]+", token.strip(".,;:")):
                hyph[token.strip(".,;:").lower()] += 1
    return mid_line_vocabulary(line_texts), {w for w, n in hyph.items() if n >= 2}


EVENT_COLUMNS = ["scan_id", "region_id", "boundary_idx", "event", "char", "left_line",
                 "right_line", "joined_word"]


def join_inventory(inv, out_root, wbcs, write_events=False):
    """Stage 3 for one inventory. Returns a status dict."""
    start = time.monotonic()
    out_root = Path(out_root)
    regions = read_region_lines(out_root, inv)
    vocab, hyph_vocab = join_vocabulary([t for _, texts in regions for t in texts if t])
    stats = Counter()
    events = [] if write_events else None
    rows = []
    for meta, texts in regions:
        stats["regions"] += 1
        context = (meta["scan_id"], meta["region_id"])
        text = join_region(texts, wbcs, stats=stats, events=events, vocab=vocab,
                           hyph_vocab=hyph_vocab, context=context)
        rows.append((meta["scan_id"], meta["region_id"], meta["region_box"], meta["region_tag"], text))
    n = write_tsv_gz(out_root / "regions" / f"{inv}.tsv.gz", REGION_COLUMNS, rows)
    if write_events:
        write_tsv_gz(out_root / "join_events" / f"{inv}.tsv.gz", EVENT_COLUMNS, events)
    extract = json.loads((out_root / "extract_stats" / f"{inv}.json").read_text(encoding="utf-8"))
    nested = defaultdict(dict)
    for key, value in sorted(stats.items()):
        parts = key.split("|")
        if len(parts) == 1:
            nested[key] = value
        elif len(parts) == 2:
            nested[parts[0]][parts[1]] = value
        else:
            nested[parts[0]].setdefault(parts[2], {})[parts[1]] = value
    join_stats = {
        "inventory_number": inv,
        "wbc_set": "".join(sorted(wbcs)),
        "guards": list(ALL_GUARDS),
        "n_regions": n,
        "n_lines": extract["n_lines"],
        "join": dict(nested),
        "sanitised": extract["sanitised"],
        "extract_counts": extract["counts"],
        "n_parse_errors": len(extract["parse_errors"]),
    }
    write_json_atomic(out_root / "join_stats" / f"{inv}.json", join_stats)
    return {"inventory_number": inv, "stage": "join", "status": "done", "n_rows": n,
            "n_lines": extract["n_lines"], "seconds": round(time.monotonic() - start, 3)}
