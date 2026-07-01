"""Cross-page empty-page detection and page-sequence boundary analysis.

Structural boundaries in archival inventories often materialise as *empty pages*: a blank verso
or recto half in a book-of-openings where a new document starts, or a run of empty scans at the
start or end of a bound volume. These pages carry no text content but their position in the page
sequence is structurally significant -- the page-layout clusters that immediately precede or
follow an empty page mark the boundary.

`is_empty_page` identifies pages with no meaningful text (zero lines, or only very short
fragments that are likely OCR noise). `build_page_sequence` assembles the per-corpus page
sequence with cluster labels and empty-page flags. `empty_page_neighbor_clusters` and
`empty_page_transitions` then summarise which page-layout clusters systematically appear near
empty pages, answering "what cluster typically ends/begins a document section?"
"""

from typing import List, Tuple

import pandas as pd
import pagexml.model.physical_document_model as pdm


def is_empty_page(page: pdm.PageXMLScan, max_total_text: int = 20) -> bool:
    """True if `page` contains fewer than `max_total_text` characters of text in total.

    Covers both zero-line pages and pages whose only content is very short OCR noise
    fragments (a few stray characters per line). The default threshold of 20 characters
    treats anything shorter than a single short word as effectively empty.
    """
    total = sum(len(line.text or '') for line in page.get_lines())
    return total < max_total_text


def build_page_sequence(
    pages: List[pdm.PageXMLScan],
    page_clusters: pd.Series,
    max_total_text: int = 20,
) -> pd.DataFrame:
    """One row per page, in list order, with cluster label and empty-page flag.

    :param pages: pages in corpus order (output of `split_inventory_into_pages` is
        already in reading order).
    :param page_clusters: cluster labels indexed by page id, e.g. from
        `cluster_page_layouts`. Pages missing from the index get NaN.
    :return: DataFrame with columns ``page_id``, ``cluster``, ``is_empty``,
        ``position`` (0-based integer position in the sequence).
    """
    rows = []
    for pos, page in enumerate(pages):
        rows.append({
            'page_id': page.id,
            'cluster': page_clusters.get(page.id, pd.NA),
            'is_empty': is_empty_page(page, max_total_text=max_total_text),
            'position': pos,
        })
    return pd.DataFrame(rows)


def empty_page_neighbor_clusters(
    sequence_df: pd.DataFrame,
    window: int = 3,
) -> pd.DataFrame:
    """For each non-empty page within `window` positions of any empty page, record its
    cluster and relative position.

    Relative position is negative for pages *before* an empty page and positive for pages
    *after* it. A page can appear multiple times if it is near multiple empty pages.

    :return: DataFrame with columns ``cluster``, ``rel_position`` -- each row is one
        (non-empty page, empty page) proximity pair.
    """
    empty_positions = set(sequence_df.loc[sequence_df['is_empty'], 'position'])
    rows = []
    for _, row in sequence_df.iterrows():
        if row['is_empty']:
            continue
        pos = row['position']
        for ep in empty_positions:
            rel = pos - ep
            if 0 < abs(rel) <= window:
                rows.append({'cluster': row['cluster'], 'rel_position': rel})
    return pd.DataFrame(rows)


def empty_page_transitions(
    sequence_df: pd.DataFrame,
) -> Tuple[pd.Series, pd.Series]:
    """Cluster distributions immediately before and immediately after each empty page.

    ``before``: value_counts of the closest non-empty cluster *before* each empty page.
    ``after``: value_counts of the closest non-empty cluster *after* each empty page.

    Answers: "what layout cluster typically ends/begins a document section?"
    """
    seq = sequence_df.reset_index(drop=True)
    before_labels = []
    after_labels = []
    for idx, row in seq.iterrows():
        if not row['is_empty']:
            continue
        for bi in range(idx - 1, -1, -1):
            if not seq.loc[bi, 'is_empty']:
                before_labels.append(seq.loc[bi, 'cluster'])
                break
        for ai in range(idx + 1, len(seq)):
            if not seq.loc[ai, 'is_empty']:
                after_labels.append(seq.loc[ai, 'cluster'])
                break
    before = pd.Series(before_labels, name='cluster').value_counts()
    after = pd.Series(after_labels, name='cluster').value_counts()
    return before, after
