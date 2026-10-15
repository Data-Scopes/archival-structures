"""Per-inventory detection of word-break characters (WBCs) in a text layer.

Method: for every punctuation or symbol character that ends a line, measure (a) how often it
is attached to a word end (a letter right before it), (b) how strongly it is enriched at line
end relative to its overall frequency, and (c) how often removing it and joining the two
fragments gives a word attested mid-line in the same inventory. A character is a word break in
an inventory when all three pass their thresholds; inventories with too few lines, and
characters with too little evidence, take the corpus default set.

The thresholds (``DEFAULT_WORD_BREAK_RULE``) were calibrated on 22 VOC inventories (NLP
report/74) and are parameters, not constants of the method. Statistics are measured at line
ends inside regions only. Region ends are not measured: that is a v2 extension.

Ported verbatim from NLP's ``startpage/voc_text_layer.py``. pagexml and pandas are imported
inside the functions that need them.
"""

import json
import re
import time
import unicodedata
from collections import Counter
from pathlib import Path

from archival_structures.text_layer.io import read_region_lines, write_plain_tsv

LETTERS = re.compile(r"[^\W\d_]+")
TRAILING_LETTERS = re.compile(r"([^\W\d_]+)$")
LEADING_LETTERS = re.compile(r"^([^\W\d_]+)")


# --------------------------------------------------------------------------
# Decision rule for word-break characters. Fixed after printing the
# pooled distribution over the 22 annotated inventories and BEFORE any
# per-inventory decision was looked at (report/74 §2). Each threshold carries
# its one-sentence reason; all of this is written to corpus_default.json.
# --------------------------------------------------------------------------
DEFAULT_WORD_BREAK_RULE = {
    "min_end_attached": {
        "value": 30,
        "reason": "Below 30 letter-attached line-final occurrences the two fractions are too "
                  "noisy to decide on (a 95% interval on a share is roughly +-0.18 at n=30), so "
                  "the char takes its corpus-default membership instead.",
    },
    "min_join_vocab_frac": {
        "value": 0.25,
        "reason": "On the pooled 22 the genuine marks join into an attested mid-line word 0.51-0.55 "
                  "of the time (capped by a one-inventory vocabulary, not by the mark) and every "
                  "other char clearing the other two thresholds does so at <= 0.04 (abbreviation "
                  "marks ':' '.' glue an abbreviation to the next word); 0.25 sits mid-gap, where "
                  "the plan's starting value 0.5 would sit on top of the genuine marks.",
    },
    "min_end_rel_frac": {
        "value": 5.0,
        "reason": "A line-wrap mark is strongly enriched at line end relative to its overall "
                  "frequency (LotA's statistic); 5x separates the dedicated marks from "
                  "punctuation that merely often ends a clause.",
    },
    "min_inventory_lines": {
        "value": 2000,
        "reason": "Inventories with fewer lines take the corpus default set whole.",
    },
    "corpus_default_min_share": {
        "value": 0.5,
        "reason": "A char is in the corpus default if it is derived_in in at least half the "
                  "inventories where it had enough evidence.",
    },
}

SANITY_IN = ["¬"]
SANITY_OUT = [":", "."]


def is_candidate_char(char):
    return unicodedata.category(char)[0] in "PS"


def char_label(char):
    return f"U+{ord(char):04X} {unicodedata.name(char, '?')}"


def mid_line_vocabulary(line_texts, min_count=2):
    """Lower-cased all-letter words that occur as neither the first nor the last
    whitespace token of a line, at least `min_count` times."""
    counts = Counter()
    for text in line_texts:
        tokens = text.split()
        for token in tokens[1:-1]:
            word = token.strip("".join(c for c in set(token) if is_candidate_char(c)))
            if word and word.isalpha():
                counts[word.lower()] += 1
    return {w for w, n in counts.items() if n >= min_count}


def ends_attached(text, char):
    """Line ends in `char` with a letter immediately before it."""
    return len(text) >= 2 and text[-1] == char and text[-2].isalpha()


def strip_leading_marks(text):
    """Drop the leading run of punctuation/symbol chars. Scribes mix marks
    across a break (`pro¬` + `„visie`), so the right side is read after any
    leading mark, not only after the same char."""
    i = 0
    while i < len(text) and is_candidate_char(text[i]):
        i += 1
    return text[i:]


def joined_word(left_text, right_text, char):
    """Left fragment (trailing letters before `char`) + right word (leading
    letters after the leading mark run). None if either side is empty."""
    left = TRAILING_LETTERS.search(left_text[:-1])
    right = LEADING_LETTERS.match(strip_leading_marks(right_text))
    if not left or not right:
        return None, None, None
    return left.group(1), right.group(1), left.group(1) + right.group(1)


def first_non_wbc(text, wbcs):
    stripped = text.lstrip("".join(wbcs)) if wbcs else text
    return stripped[:1]


def word_break_stats(regions, inventory_number):
    """Per-candidate statistics for one inventory.

    `regions` is an iterable of per-region line-text sequences (walked twice, so it is
    materialised first). Returns (rows, info)."""
    from pagexml.analysis.text_stats import LineCharAnalyser

    inv = inventory_number
    start = time.monotonic()
    regions = [list(t) for t in regions]
    all_lines = [t for texts in regions for t in texts if t]
    analyser = LineCharAnalyser()
    if all_lines:
        try:
            analyser.analyse_line_chars(all_lines)
        except ZeroDivisionError:
            pass  # freq counters are filled before set_stats(); we compute fractions ourselves
    freq = analyser.freq
    total_all = sum(freq["all"].values())
    total_start = sum(freq["start"].values())
    total_end = sum(freq["end"].values())
    vocab = mid_line_vocabulary(all_lines)
    candidates = sorted(c for c in freq["end"] if is_candidate_char(c))
    cand_set = set(candidates)
    per = {c: Counter() for c in candidates}
    for texts in regions:
        texts = [t for t in texts if t]
        for i, text in enumerate(texts):
            nxt = texts[i + 1] if i + 1 < len(texts) else None
            prev = texts[i - 1] if i > 0 else None
            last = text[-1]
            if last in cand_set:
                c = last
                if nxt is not None and nxt[0] == c:
                    per[c]["n_both_ends"] += 1
                if nxt is not None and is_candidate_char(nxt[0]):
                    per[c]["n_both_ends_any"] += 1
                if (len(text) >= 2 and text[-2].isdigit() and nxt is not None
                        and first_non_wbc(nxt, c)[:1].isdigit()):
                    per[c]["n_digit_both_sides"] += 1
                if ends_attached(text, c):
                    per[c]["n_end_attached"] += 1
                    if nxt is not None:
                        per[c]["n_end_attached_with_next"] += 1
                        if strip_leading_marks(nxt)[:1].islower():
                            per[c]["n_cont_lower"] += 1
                        _, _, joined = joined_word(text, nxt, c)
                        if joined and joined.lower() in vocab:
                            per[c]["n_join_vocab"] += 1
            first = text[0]
            if first in cand_set and prev is not None and prev[-1] not in cand_set:
                per[first]["n_start_only"] += 1
    rows = []
    for c in candidates:
        counter = per[c]
        all_f, start_f, end_f = freq["all"][c], freq["start"][c], freq["end"][c]
        all_frac = all_f / total_all if total_all else 0.0
        denom = counter["n_end_attached_with_next"]
        rows.append({
            "inventory_number": inv, "char": c, "char_name": char_label(c),
            "n_lines": len(all_lines), "n_chars": total_all,
            "all_freq": all_f, "start_freq": start_f, "end_freq": end_f,
            "total_all": total_all, "total_start": total_start, "total_end": total_end,
            "end_rel_frac": (end_f / total_end) / all_frac if total_end and all_frac else 0.0,
            "start_rel_frac": (start_f / total_start) / all_frac if total_start and all_frac else 0.0,
            "n_end_attached": counter["n_end_attached"],
            "n_end_attached_with_next": denom,
            "n_cont_lower": counter["n_cont_lower"],
            "cont_lower_frac": counter["n_cont_lower"] / denom if denom else 0.0,
            "n_join_vocab": counter["n_join_vocab"],
            "join_vocab_frac": counter["n_join_vocab"] / denom if denom else 0.0,
            "n_both_ends": counter["n_both_ends"],
            "n_both_ends_any": counter["n_both_ends_any"],
            "n_start_only": counter["n_start_only"],
            "n_digit_both_sides": counter["n_digit_both_sides"],
        })
    return rows, {"n_lines": len(all_lines), "n_chars": total_all,
                  "seconds": round(time.monotonic() - start, 3)}


def inventory_word_break_stats(inv, layer_root):
    """`word_break_stats` for one inventory of a text layer on disk (lines sidecar only)."""
    return word_break_stats([texts for _, texts in read_region_lines(layer_root, inv)], inv)


WORD_BREAK_STATS_COLUMNS = [
    "inventory_number", "char", "char_name", "n_lines", "n_chars", "all_freq", "start_freq",
    "end_freq", "total_all", "total_start", "total_end", "end_rel_frac", "start_rel_frac",
    "n_end_attached", "n_end_attached_with_next", "n_cont_lower", "cont_lower_frac",
    "n_join_vocab", "join_vocab_frac", "n_both_ends", "n_both_ends_any", "n_start_only",
    "n_digit_both_sides",
]


def format_stat(value):
    return f"{value:.6g}" if isinstance(value, float) else str(value)


def write_word_break_stats_part(inv, layer_root):
    """Stage 2a for one inventory: writes wbc/parts/<inv>.tsv. Returns a status dict."""
    rows, info = inventory_word_break_stats(inv, layer_root)
    write_plain_tsv(Path(layer_root) / "wbc" / "parts" / f"{inv}.tsv", WORD_BREAK_STATS_COLUMNS,
                    [[format_stat(r[c]) for c in WORD_BREAK_STATS_COLUMNS] for r in rows])
    return {"inventory_number": inv, "stage": "wbc", "status": "done", "n_rows": len(rows),
            "n_lines": info["n_lines"], "seconds": info["seconds"]}


def pooled_stats(stats):
    """Pooled per-char statistics from a wbc_stats frame (sums of counts)."""
    import pandas as pd
    totals = stats.drop_duplicates("inventory_number")[["total_all", "total_start", "total_end"]].sum()
    g = stats.groupby("char").agg(
        n_inventories=("inventory_number", "nunique"),
        all_freq=("all_freq", "sum"), start_freq=("start_freq", "sum"), end_freq=("end_freq", "sum"),
        n_end_attached=("n_end_attached", "sum"),
        n_end_attached_with_next=("n_end_attached_with_next", "sum"),
        n_cont_lower=("n_cont_lower", "sum"), n_join_vocab=("n_join_vocab", "sum"),
        n_both_ends=("n_both_ends", "sum"), n_both_ends_any=("n_both_ends_any", "sum"),
        n_start_only=("n_start_only", "sum"),
        n_digit_both_sides=("n_digit_both_sides", "sum"),
    )
    all_frac = g["all_freq"] / totals["total_all"]
    g["end_rel_frac"] = (g["end_freq"] / totals["total_end"]) / all_frac
    g["start_rel_frac"] = (g["start_freq"] / totals["total_start"]) / all_frac
    denom = g["n_end_attached_with_next"].where(g["n_end_attached_with_next"] > 0)
    g["cont_lower_frac"] = (g["n_cont_lower"] / denom).fillna(0.0)
    g["join_vocab_frac"] = (g["n_join_vocab"] / denom).fillna(0.0)
    g["char_name"] = [char_label(c) for c in g.index]
    return pd.DataFrame(g).sort_values("end_freq", ascending=False)


def rule_passes(n_end_attached, join_vocab_frac, end_rel_frac, rule=DEFAULT_WORD_BREAK_RULE):
    return (n_end_attached >= rule["min_end_attached"]["value"]
            and join_vocab_frac >= rule["min_join_vocab_frac"]["value"]
            and end_rel_frac >= rule["min_end_rel_frac"]["value"])


def decide_word_breaks(stats, inventory_lines, rule=DEFAULT_WORD_BREAK_RULE, *,
                       sanity_in=SANITY_IN, sanity_out=SANITY_OUT):
    """Apply the rule. `inventory_lines` is {inv: n_lines} for every inventory in
    the run (some have no candidate rows). Returns (per_inventory_rows,
    corpus_default_dict)."""
    min_att = rule["min_end_attached"]["value"]
    min_lines = rule["min_inventory_lines"]["value"]
    share = rule["corpus_default_min_share"]["value"]

    eligible = stats[(stats["n_lines"] >= min_lines) & (stats["n_end_attached"] >= min_att)].copy()
    eligible["derived_in"] = [rule_passes(a, j, e, rule) for a, j, e in
                              zip(eligible["n_end_attached"], eligible["join_vocab_frac"],
                                  eligible["end_rel_frac"])]
    by_char = eligible.groupby("char")["derived_in"].agg(["sum", "count"])
    by_char["share_in"] = by_char["sum"] / by_char["count"]
    default_set = sorted(c for c, r in by_char.iterrows() if r["share_in"] >= share)

    pooled = pooled_stats(stats)
    pooled_set = sorted(c for c, r in pooled.iterrows()
                        if rule_passes(r["n_end_attached"], r["join_vocab_frac"], r["end_rel_frac"], rule))

    per_inventory = []
    groups = dict(tuple(stats.groupby("inventory_number", sort=False)))
    for inv, n_lines in inventory_lines.items():
        group = groups.get(inv)
        if n_lines < min_lines or group is None:
            statuses = {c: "default" for c in default_set}
            wbc = set(default_set)
            mode = "whole_default"
        else:
            mode = "per_char"
            statuses = {}
            g = group.set_index("char")
            considered = set(default_set) | set(g.index[g["n_end_attached"] >= min_att])
            for c in considered:
                if c in g.index and g.at[c, "n_end_attached"] >= min_att:
                    ok = rule_passes(g.at[c, "n_end_attached"], g.at[c, "join_vocab_frac"],
                                     g.at[c, "end_rel_frac"], rule)
                    statuses[c] = "derived_in" if ok else "derived_out"
                else:
                    statuses[c] = "default"
            wbc = {c for c, s in statuses.items()
                   if s == "derived_in" or (s == "default" and c in default_set)}
        per_inventory.append({
            "inventory_number": inv, "n_lines": n_lines, "mode": mode,
            "wbc_set": "".join(sorted(wbc)),
            "differs_from_default": sorted(wbc) != default_set,
            "char_status": " ".join(f"{c}:{statuses[c]}" for c in sorted(statuses)),
        })
    corpus = {
        "default_set": default_set,
        "default_set_names": [char_label(c) for c in default_set],
        "pooled_rule_set": pooled_set,
        "pooled_rule_set_names": [char_label(c) for c in pooled_set],
        "rule": rule,
        "rule_text": ("c is a WBC in inventory I iff n_end_attached >= min_end_attached and "
                      "join_vocab_frac >= min_join_vocab_frac and end_rel_frac >= "
                      "min_end_rel_frac; inventories with n_lines < min_inventory_lines take "
                      "default_set whole; chars with n_end_attached < min_end_attached take "
                      "their default membership."),
        "sanity_in": sanity_in, "sanity_out": sanity_out,
        "sanity_pass": (all(c in pooled_set for c in sanity_in)
                        and not any(c in pooled_set for c in sanity_out)),
        "per_char_share_in": {c: {"n_in": int(r["sum"]), "n_eligible": int(r["count"]),
                                  "share_in": round(float(r["share_in"]), 4)}
                              for c, r in by_char.sort_values("count", ascending=False).iterrows()},
        "pooled": {c: {k: (round(float(v), 4) if isinstance(v, float) else int(v))
                       for k, v in r.items() if k != "char_name"}
                   for c, r in pooled.head(40).iterrows()},
        "n_inventories": int(stats["inventory_number"].nunique()),
    }
    return per_inventory, corpus


def read_word_break_sets(layer_root):
    """{inv: set_of_chars} from wbc/wbc_per_inventory.tsv, plus the default set."""
    path = Path(layer_root) / "wbc" / "wbc_per_inventory.tsv"
    sets = {}
    with open(path, encoding="utf-8", newline="\n") as handle:
        header = handle.readline().rstrip("\n").split("\t")
        for line in handle:
            row = dict(zip(header, line.rstrip("\n").split("\t")))
            sets[row["inventory_number"]] = set(row["wbc_set"])
    corpus = json.loads((Path(layer_root) / "wbc" / "corpus_default.json").read_text(encoding="utf-8"))
    return sets, set(corpus["default_set"])
