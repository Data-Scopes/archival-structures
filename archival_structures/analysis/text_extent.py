"""
Text-extent features for detecting full-text pages and stable text boundaries.

These features measure how much of each page's vertical (and horizontal) space
is occupied by text, expressed as relative margins from each edge.  A page whose
top and bottom margins are both close to the inventory's *stable* (typical
minimum) margins is classified as a *full-text* page; pages with larger margins
are either *late-start*, *early-end*, or *short* pages.

The stable margin is estimated from the corpus distribution (low percentile) so
that it represents normal pages rather than the extremes.

Two additional per-page features help distinguish *running-text* pages from
*table* pages even when both have stable top/bottom margins:

* ``mean_width`` -- mean relative line width (line bbox width / page width).
  Table cells are narrow fragments of a wider column grid; running-text lines
  span most of the page width.  This is the strongest single discriminator.

* ``equal_frac`` -- fraction of consecutive horizontal line-group pairs whose
  horizontal extents are Allen-``equal`` within a tolerance (default 2% of page
  width).  Running-text lines that fill the full column tend to start and end at
  the same horizontal positions from line to line, giving a high ``equal_frac``.
  Table cells within the same column share the same *left* boundary but have
  variable *right* extents (depending on cell content), suppressing ``equal_frac``.

Two additive options (NLP report/84, UA-014): ``region_types`` on
:func:`compute_page_text_extent` / :func:`compute_corpus_text_extents` restricts the
extent to lines whose parent region has one of the given types (e.g. body text
without page numbers or catch-words), and :func:`text_extent_table` returns the
continuous margin gaps (``top_gap``, ``bot_gap``) next to the class.  Defaults leave
all earlier outputs unchanged.
"""
from __future__ import annotations

from typing import Collection, Dict, FrozenSet, List, Optional

import numpy as np
import pandas as pd
import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import horizontal_group_lines

#: NLP report/84's body-text region types for VOC 1.04.02 (Laypa labels).  A named
#: example, not a default anywhere.
BODY_REGION_TYPES_VOC: FrozenSet[str] = frozenset({"paragraph", "signature-mark", "header"})


def line_region_type(line) -> Optional[str]:
    """Type of the line's direct parent region (NLP ``_rtype``, report/84).

    A list with more than one element gives its last element; a one-element list
    gives ``"text_region"``; a string is returned as is; a line without parent
    gives ``None``.
    """
    t = line.parent.type if line.parent else None
    if isinstance(t, list):
        return t[-1] if len(t) > 1 else "text_region"
    return t


def _equal_frac(lines: list, page_width: int, tol_frac: float = 0.02) -> float:
    """Fraction of consecutive horizontal line-group pairs that are Allen-``equal``
    within *tol_frac* × *page_width* pixels on each boundary.

    Uses ``horizontal_group_lines`` to collect lines that share the same vertical
    band (the same row), then compares each consecutive pair of groups.  Two lines
    are considered equal when both their left and right boundaries agree within the
    pixel tolerance.
    """
    if not lines:
        return 0.0
    tol = tol_frac * page_width
    # text_only=False: include lines without transcribed text (we compare geometry, not content)
    groups = horizontal_group_lines(lines, text_only=False)
    n_equal = n_total = 0
    prev: list = []
    for group in groups:
        if prev:
            for pl in prev:
                for cl in group:
                    n_total += 1
                    if (abs(pl.coords.left  - cl.coords.left)  <= tol and
                            abs(pl.coords.right - cl.coords.right) <= tol):
                        n_equal += 1
        prev = group
    return n_equal / n_total if n_total else 0.0


def compute_page_text_extent(page: pdm.PageXMLScan,
                             equal_tol: float = 0.02,
                             region_types: Optional[Collection[str]] = None) -> Optional[Dict]:
    """Compute relative text-extent features for one page.

    Lines that lack valid coords are silently skipped.  If no lines remain,
    ``None`` is returned (the caller can use this to identify empty pages).

    Returns a dict with keys:

    * ``page_id``    – page identifier
    * ``n_lines``    – number of lines with valid coords
    * ``top``        – ``min(line.top) / page.height``
    * ``bot``        – ``(page.height − max(line.bottom)) / page.height``
    * ``left``       – ``min(line.left) / page.width``
    * ``right``      – ``(page.width − max(line.right)) / page.width``
    * ``mean_width`` – mean ``(line.right − line.left) / page.width`` across all lines
    * ``equal_frac`` – fraction of consecutive row-group pairs whose horizontal extents
                       are Allen-``equal`` within *equal_tol* × page width

    The first four margin values are in [0, 1] for pages where text stays within
    the page boundary; HTR coordinates may occasionally stray slightly outside.

    Args:
        page: the page to analyse.
        equal_tol: tolerance for the Allen-equal test, as a fraction of page width
            (default 0.02 = 2%).
        region_types: if given, keep only lines whose direct parent region type (as
            returned by :func:`line_region_type`, i.e. the PageXML/Laypa ``type``;
            untyped regions have type ``"text_region"``) is in this collection.  All
            returned values are computed over the kept lines.  Stable margins for a
            filtered run must be estimated from a filtered frame (don't mix).
            ``None`` (default) keeps all lines.
    """
    lines = [ln for ln in page.get_lines() if ln.coords]
    if region_types is not None:
        lines = [ln for ln in lines if line_region_type(ln) in region_types]
    if not lines:
        return None
    ph = page.coords.height
    pw = page.coords.width
    if ph == 0 or pw == 0:
        return None
    return {
        'page_id':    page.id,
        'n_lines':    len(lines),
        'top':        min(ln.coords.top for ln in lines) / ph,
        'bot':        (ph - max(ln.coords.bottom for ln in lines)) / ph,
        'left':       min(ln.coords.left for ln in lines) / pw,
        'right':      (pw - max(ln.coords.right for ln in lines)) / pw,
        'mean_width': float(np.mean([(ln.coords.right - ln.coords.left) / pw for ln in lines])),
        'equal_frac': _equal_frac(lines, pw, equal_tol),
    }


def compute_corpus_text_extents(pages: List[pdm.PageXMLScan],
                                region_types: Optional[Collection[str]] = None,
                                equal_tol: float = 0.02) -> pd.DataFrame:
    """Compute text-extent features for every non-empty page in *pages*.

    Empty pages (for which :func:`compute_page_text_extent` returns ``None``) are
    omitted.  The returned DataFrame is indexed by ``page_id`` and has columns
    ``n_lines``, ``top``, ``bot``, ``left``, ``right``, ``mean_width``, ``equal_frac``.

    Args:
        pages: the pages to analyse.
        region_types: see :func:`compute_page_text_extent`; pages without a line of
            these types are omitted.
        equal_tol: see :func:`compute_page_text_extent`.
    """
    rows = [compute_page_text_extent(p, equal_tol, region_types) for p in pages]
    rows = [r for r in rows if r is not None]
    if not rows:
        return pd.DataFrame(columns=['page_id', 'n_lines', 'top', 'bot', 'left', 'right',
                                     'mean_width', 'equal_frac']).set_index('page_id')
    df = pd.DataFrame(rows)
    return df.set_index('page_id')


def estimate_stable_margins(df: pd.DataFrame, percentile: float = 10.0) -> Dict[str, float]:
    """Estimate the 'stable' (typical-minimum) margin for each side.

    The *percentile*-th percentile of each margin column represents the typical
    minimum margin on pages where text occupies most of the page space.  A low
    percentile (default 10) is robust to a small number of outliers while still
    reflecting where text normally starts/ends in that inventory.

    Args:
        df: DataFrame from :func:`compute_corpus_text_extents`.
        percentile: which percentile to use as the stable-margin estimate.

    Returns:
        dict with keys ``'top'``, ``'bot'``, ``'left'``, ``'right'``.
    """
    return {
        col: float(np.percentile(df[col].dropna(), percentile))
        for col in ('top', 'bot', 'left', 'right')
    }


def classify_text_extent(
    row: Dict[str, float],
    stable: Dict[str, float],
    top_tol: float = 0.05,
    bot_tol: float = 0.05,
) -> str:
    """Classify one page by how its text extent relates to the stable margins.

    A page is ``'full_text'`` when its top and bottom margins are both within
    *top_tol* / *bot_tol* of the stable values.  Deviations above the tolerance
    are labelled accordingly:

    * ``'full_text'``  – both margins close to stable (text runs top-to-bottom)
    * ``'late_start'`` – top margin larger than stable (text begins later)
    * ``'early_end'``  – bottom margin larger than stable (text ends earlier)
    * ``'short'``      – both margins deviate (text occupies only a central band)

    Args:
        row: dict (or dict-like DataFrame row) with ``'top'`` and ``'bot'`` values.
        stable: dict with ``'top'`` and ``'bot'`` from :func:`estimate_stable_margins`.
        top_tol: tolerance above ``stable['top']`` still counted as on-time.
        bot_tol: tolerance above ``stable['bot']`` still counted as full text.
    """
    late = row['top'] > stable['top'] + top_tol
    early = row['bot'] > stable['bot'] + bot_tol
    if late and early:
        return 'short'
    if late:
        return 'late_start'
    if early:
        return 'early_end'
    return 'full_text'


def classify_corpus_text_extents(
    df: pd.DataFrame,
    stable: Optional[Dict[str, float]] = None,
    top_tol: float = 0.05,
    bot_tol: float = 0.05,
) -> pd.Series:
    """Classify every page in *df* by its text-extent category.

    If *stable* is not supplied it is estimated from *df* itself via
    :func:`estimate_stable_margins`.

    Returns a ``pd.Series`` indexed like *df* with string labels
    (``'full_text'``, ``'late_start'``, ``'early_end'``, ``'short'``).
    """
    if stable is None:
        stable = estimate_stable_margins(df)
    return df.apply(
        lambda row: classify_text_extent(row, stable, top_tol, bot_tol),
        axis=1,
    ).rename('text_extent')


def full_text_page_fraction(
    df: pd.DataFrame,
    stable: Optional[Dict[str, float]] = None,
    top_tol: float = 0.05,
    bot_tol: float = 0.05,
) -> float:
    """Fraction of non-empty pages classified as ``'full_text'``.

    Args:
        df: DataFrame from :func:`compute_corpus_text_extents`.
        stable: optional pre-computed stable margins; estimated from *df* if absent.
        top_tol: tolerance for the top margin.
        bot_tol: tolerance for the bottom margin.

    Returns:
        float in [0, 1]; 0.0 for an empty DataFrame.
    """
    if df.empty:
        return 0.0
    classes = classify_corpus_text_extents(df, stable, top_tol, bot_tol)
    return float((classes == 'full_text').mean())


def margin_gaps(row, stable: Dict[str, float]) -> Dict[str, float]:
    """Continuous margin gaps of one page relative to the stable margins.

    Raw differences, no tolerance subtracted.  Positive means more white space
    than stable.

    Returns:
        ``{'top_gap': row['top'] - stable['top'], 'bot_gap': row['bot'] - stable['bot']}``
    """
    return {'top_gap': row['top'] - stable['top'], 'bot_gap': row['bot'] - stable['bot']}


def text_extent_table(
    df: pd.DataFrame,
    stable: Optional[Dict[str, float]] = None,
    top_tol: float = 0.05,
    bot_tol: float = 0.05,
) -> pd.DataFrame:
    """Class and margin gaps for every page in *df*.

    Returns a frame indexed like *df* with columns ``text_extent`` (identical to
    :func:`classify_corpus_text_extents`; the class keeps its own comparison and is
    never derived from the gaps), ``top_gap`` and ``bot_gap`` (see
    :func:`margin_gaps`).  *stable* is estimated from *df* if ``None``.
    """
    if df.empty:
        return pd.DataFrame(columns=['text_extent', 'top_gap', 'bot_gap'], index=df.index)
    if stable is None:
        stable = estimate_stable_margins(df)
    out = pd.DataFrame({
        'text_extent': classify_corpus_text_extents(df, stable, top_tol, bot_tol),
        'top_gap': df['top'] - stable['top'],
        'bot_gap': df['bot'] - stable['bot'],
    }, index=df.index)
    return out
