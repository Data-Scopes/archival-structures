"""Glued-word repair for HTR lines (phase 06, step 15f).

HTR often drops the space between words (`Vranckrijcktoeleggen`). This module holds a
conservative unigram segmenter that only INSERTS spaces: removing the inserted spaces gives
`text_raw` back. Pure Python (collections, math); no dependency on pagexml.

Method: a token whose letter core is long and not attested in the vocabulary (count below
`min_part_count`) is segmented by a Viterbi search into 2..max_parts parts, each attested at
least `min_part_count` times, scoring sum(log p(part)) - split_penalty*(parts-1); the split
is made only if it beats the unsplit score log(0.5/total).
"""

import collections
import math
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class WordSplitParams:
    min_token_len: int = 10
    min_part_len: int = 2
    min_part_count: int = 10
    max_parts: int = 4
    split_penalty: float = 4.0

    def to_dict(self):
        return asdict(self)


def token_core(token):
    """(leading non-letters, core, trailing non-letters); a trailing `-` or `„` stays in the
    trailing part (it is a word-break mark, not part of the word)."""
    start = 0
    while start < len(token) and not token[start].isalpha():
        start += 1
    end = len(token)
    while end > start and not token[end - 1].isalpha():
        end -= 1
    return token[:start], token[start:end], token[end:]


def _is_word_core(core):
    return bool(core) and all(ch.isalpha() for ch in core)


def build_vocabulary(line_texts):
    """Counter of lower-cased cores of alphabetic tokens over an iterable of line texts."""
    vocab = collections.Counter()
    for text in line_texts:
        for token in text.split(" "):
            _, core, _ = token_core(token)
            if _is_word_core(core):
                vocab[core.lower()] += 1
    return vocab


def is_candidate(core, vocab, params):
    return (_is_word_core(core) and len(core) >= params.min_token_len
            and vocab.get(core.lower(), 0) < params.min_part_count)


def segment(core, vocab, total, params):
    """Best segmentation of `core` into >= 2 attested parts (list of strings, original casing)
    or None. Does not check candidacy (see `is_candidate`)."""
    n = len(core)
    low = core.lower()
    if n < 2 * params.min_part_len or total <= 0:
        return None
    logt = math.log(total)
    mpc, mpl = params.min_part_count, params.min_part_len
    # part score for low[i:j]
    ps = {}
    for i in range(n):
        for j in range(i + mpl, n + 1):
            c = vocab.get(low[i:j], 0)
            if c >= mpc:
                ps[(i, j)] = math.log(c) - logt
    if not ps:
        return None
    NEG = None
    # best[k][i]: best (score, first_end) segmenting low[i:] into exactly k parts
    best = [[NEG] * (n + 1) for _ in range(params.max_parts + 1)]
    for i in range(n):
        s = ps.get((i, n))
        if s is not None:
            best[1][i] = (s, n)
    for k in range(2, params.max_parts + 1):
        for i in range(n - 1, -1, -1):
            cur = None
            for j in range(i + mpl, n):  # ascending j: leftmost first split wins ties
                s = ps.get((i, j))
                nxt = best[k - 1][j]
                if s is None or nxt is None:
                    continue
                cand = s + nxt[0]
                if cur is None or cand > cur[0]:
                    cur = (cand, j)
            best[k][i] = cur
    unsplit = math.log(0.5) - logt
    chosen = None
    for k in range(2, params.max_parts + 1):  # fewer parts win ties
        b = best[k][0]
        if b is None:
            continue
        score = b[0] - params.split_penalty * (k - 1)
        if score > unsplit and (chosen is None or score > chosen[0]):
            chosen = (score, k)
    if chosen is None:
        return None
    parts, i, k = [], 0, chosen[1]
    while k >= 1:
        j = best[k][i][1]
        parts.append(core[i:j])
        i, k = j, k - 1
    return parts


def maybe_segment(core, vocab, total, params):
    if not is_candidate(core, vocab, params):
        return None
    return segment(core, vocab, total, params)


def repair_line(text, vocab, total, params):
    """(new text, [(token_index, original token, parts)]). Only spaces are inserted."""
    tokens = text.split(" ")
    out, edits = [], []
    for idx, token in enumerate(tokens):
        lead, core, trail = token_core(token)
        parts = maybe_segment(core, vocab, total, params) if core else None
        if parts is None:
            out.append(token)
            continue
        out.append(lead + " ".join(parts) + trail)
        edits.append((idx, token, parts))
    return " ".join(out), edits


def _strip_wordsplit(flags):
    return [f for f in flags.split(";") if f and not f.startswith("wordsplit:")]


def apply_word_split(rows, vocab, params, edits_out=None):
    """Return new rows with `text` recomputed from `text_raw` and the `wordsplit:<n>` flag
    (earlier wordsplit flags replaced, other flags kept). Idempotent. If `edits_out` is a
    list, (line_id, token_index, original, replacement) tuples are appended to it."""
    total = sum(vocab.values())
    result = []
    for row in rows:
        new_text, edits = repair_line(row["text_raw"], vocab, total, params)
        flags = _strip_wordsplit(row["edit_flags"])
        if edits:
            flags.append(f"wordsplit:{len(edits)}")
            if edits_out is not None:
                for idx, orig, parts in edits:
                    edits_out.append((row["line_id"], idx, orig, " ".join(parts)))
        new = dict(row)
        new["text"] = new_text
        new["edit_flags"] = ";".join(flags)
        result.append(new)
    return result
