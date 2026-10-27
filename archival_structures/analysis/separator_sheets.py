"""Numbered separator sheets and a convention-aware start-page rule.

Some volumes (found in VOC 1.04.02, 2555 above all) interleave near-blank sheets whose
only text is a document number ("No: 7.", "No. 63") with the documents. Ground truth treats
them inconsistently ACROSS inventories: in 2555 the sheet itself is the start page (68% of
sheets, the following page 6%), in 1274 and 3891 it is the first written page after the
sheet (sheet 0%, next page 100%), in 2775 mixed. A model reading page text cannot know
which, and when it cannot it lands one page off.

The rule is therefore convention-aware and acts on the *unit* {sheet, next page}: a unit is
a start if either member scored at or above the threshold, and the start is placed on the
member the convention names ("sheet" or "next"; "none" leaves predictions untouched).

Detection is label-free (a regex on the page's own text). Labels are never read here: the
convention per inventory is an input, decided elsewhere (NLP infers it in its own evaluation
scripts). The sheet regex and length cap are parameters; the VOC values are the named
defaults ``VOC_SHEET_PATTERN`` and ``VOC_MAX_SHEET_CHARS``.

Ported from NLP ``src/startpage/separator_sheets.py`` (AS phase 11, step 18c).
"""

import re
from typing import Callable, Optional

import numpy as np
import pandas as pd

from archival_structures.model.scan_id import ScanId

VOC_SHEET_PATTERN: re.Pattern = re.compile(
    r"^\s*(N\s*[:=°º.o]{1,3}\s*o?\.?|No|Nr|Num\w*)\s*[:=.]?\s*\d{1,4}\s*[.:]?\s*[a-z]?\s*$", re.I
)
VOC_MAX_SHEET_CHARS: int = 12
SHEET_CONVENTIONS = ("none", "sheet", "next")

_LAST_PART_DIGITS = re.compile(r"\d+")


def is_separator_sheet(
    text, *, pattern: re.Pattern = VOC_SHEET_PATTERN, max_chars: int = VOC_MAX_SHEET_CHARS
) -> bool:
    """True if the stripped text is non-empty, at most ``max_chars`` long and matches ``pattern``."""
    if not isinstance(text, str):
        return False
    t = text.strip()
    return 0 < len(t) <= max_chars and bool(pattern.match(t))


def detect_sheets(
    page_texts: pd.DataFrame,
    *,
    pattern: re.Pattern = VOC_SHEET_PATTERN,
    max_chars: int = VOC_MAX_SHEET_CHARS,
    inventory_col: str = "inventory_number",
    page_col: str = "page_id",
    text_col: str = "text",
) -> set[tuple[str, str]]:
    """Return ``{(str(inventory), page_id)}`` for every page whose text is a separator sheet."""
    return {
        (str(inv), pid)
        for inv, pid, text in zip(page_texts[inventory_col], page_texts[page_col], page_texts[text_col])
        if is_separator_sheet(text, pattern=pattern, max_chars=max_chars)
    }


def next_scan_id(page_id: str) -> Optional[str]:
    """Scan id of the following scan, keeping the digit width and any extension.

    Ids that do not parse as scan ids get ``None`` (the unit is then skipped by
    ``apply_rule``); NLP's version would raise or build a wrong id.
    """
    x = ScanId.try_parse(page_id, digits=None)
    if x is None:
        return None
    m = _LAST_PART_DIGITS.match(page_id.rsplit("_", 1)[1])
    if m is None:
        return None
    return x.with_number(x.number + 1).format(digits=len(m.group(0)))


def apply_rule(
    df: pd.DataFrame,
    sheets: set,
    convention,
    threshold: float = 0.5,
    *,
    next_page_id: Callable[[str], Optional[str]] = next_scan_id,
    inventory_col: str = "inventory_number",
    page_col: str = "page_id",
    prob_col: str = "pred_prob",
) -> np.ndarray:
    """Start prediction with the separator-sheet rule, aligned with ``df``'s row order.

    ``convention``: "none" | "sheet" | "next" | dict {inventory: convention} (missing
    inventories count as "none"). An unknown convention raises ``ValueError`` (NLP treated
    anything but "sheet" as "next").
    """
    prob = df[prob_col].to_numpy()
    pred = (prob >= threshold).copy()
    idx = {(str(i), p): k for k, (i, p) in enumerate(zip(df[inventory_col], df[page_col]))}
    # sorted: with two consecutive sheets the result must not depend on set order (phase-11 review)
    for inv, pid in sorted(sheets):
        k = idx.get((inv, pid))
        if k is None:
            continue
        conv = convention.get(inv, "none") if isinstance(convention, dict) else convention
        if conv not in SHEET_CONVENTIONS:
            raise ValueError(f"unknown convention {conv!r}; expected one of {SHEET_CONVENTIONS}")
        if conv == "none":
            continue
        nid = next_page_id(pid)
        nxt = idx.get((inv, nid)) if nid is not None else None
        if nxt is None:
            continue
        unit = max(prob[k], prob[nxt]) >= threshold
        pred[k], pred[nxt] = (unit, False) if conv == "sheet" else (False, unit)
    return pred
