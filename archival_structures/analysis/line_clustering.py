"""Cluster individual PageXML text lines by their spatial geometry (left indentation, width,
and -- with caution -- height), to build a vocabulary of recurring line types (body text,
indented closing lines, marginalia, ...) that `sequence_patterns.py` can use.

Two clustering approaches are offered:

- `cluster_lines`: HDBSCAN over a feature matrix, the same idiom used by
  `archival_structures.clustering.colour_clustering.cluster_features`. Good when indentation
  and width should be considered jointly.
- `cluster_lines_by_peaks`: wraps `archival_structures.clustering.peaks.cluster_by_group_peaks`,
  which buckets a single 1D column into named peak-based bins per group (e.g. per scan).
  Indentation levels tend to form a small number of discrete peaks, which is exactly what that
  function was built for, and it's considerably cheaper than HDBSCAN for that single-column case.

Height is included as an optional, not default, feature: it comes from rather generous image
cutouts in the source PageXML, so it's less reliable than left/width for distinguishing line
types.
"""
from typing import Iterable, List, Sequence, Union

import hdbscan
import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.clustering.peaks import cluster_by_group_peaks

HDBSCAN_MIN_CLUSTER_SIZE = 10


def _line_geometry(line: pdm.PageXMLTextLine):
    """Return (left, width, height) for `line`, preferring the baseline's left/width (which is
    less affected by generous image cutouts) over the coords bounding box, falling back to coords
    when there's no baseline. Height always comes from `coords.height`, since baselines have no
    height of their own."""
    if line.baseline is not None and line.baseline.points is not None:
        left, width = line.baseline.left, line.baseline.width
    else:
        left, width = line.coords.left, line.coords.width
    height = line.coords.height
    return left, width, height


def extract_line_features(scan: pdm.PageXMLScan) -> pd.DataFrame:
    """One row per text line in `scan`, with columns:

    scan_id, line_id, left, width, height, left_norm, width_norm, height_norm

    `left`/`width` are normalised by the scan's width, `height` by the scan's height, so that
    indentation/width/height are comparable across differently-sized scans.
    """
    rows = []
    for line in scan.get_lines():
        left, width, height = _line_geometry(line)
        rows.append({
            'scan_id': scan.id, 'line_id': line.id,
            'left': left, 'width': width, 'height': height,
            'left_norm': left / scan.coords.width, 'width_norm': width / scan.coords.width,
            'height_norm': height / scan.coords.height,
        })
    columns = ['scan_id', 'line_id', 'left', 'width', 'height',
              'left_norm', 'width_norm', 'height_norm']
    return pd.DataFrame(rows, columns=columns)


def extract_corpus_line_features(scans: Iterable[pdm.PageXMLScan]) -> pd.DataFrame:
    """`extract_line_features` for every scan in `scans`, concatenated into one DataFrame."""
    return pd.concat([extract_line_features(scan) for scan in scans], ignore_index=True)


def cluster_lines(line_df: pd.DataFrame, features: Sequence[str] = ('left_norm', 'width_norm'),
                  min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
                  min_samples: int = None) -> pd.Series:
    """Cluster the rows of `line_df` (as produced by `extract_line_features`) by `features`
    using HDBSCAN. Returns a `pd.Series` of integer cluster labels (-1 = noise), aligned to
    `line_df`'s index.

    `height_norm` is deliberately not a default feature -- see the module docstring -- but can
    be added via `features` if it turns out useful for a particular collection.
    """
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples,
                                metric='euclidean', cluster_selection_method='eom')
    labels = clusterer.fit_predict(line_df[list(features)].to_numpy())
    return pd.Series(labels, index=line_df.index, name='cluster')


def cluster_lines_by_peaks(line_df: pd.DataFrame, cluster_col: str = 'left_norm',
                           group_cols: Union[str, List[str]] = 'scan_id',
                           cluster_prefix: str = None, width_bin_size: float = 0.02,
                           min_prominence: float = None, min_prominence_frac: float = None,
                           rel_height: float = 1.0) -> pd.Series:
    """Cluster the rows of `line_df` by binning `cluster_col` into peaks, computed separately
    per `group_cols` (by default, per scan -- so indentation levels are found relative to that
    scan's own layout). Returns a `pd.Series` of string cluster labels, aligned to `line_df`'s
    index. See `archival_structures.clustering.peaks.cluster_by_group_peaks` for parameter details.

    Note `width_bin_size` is in the same units as `cluster_col`: the default of 0.02 suits the
    normalised `*_norm` columns (0-1 range); for the raw pixel columns (`left`, `width`, `height`)
    use a pixel-scale value (e.g. 50, matching `peaks.py`'s own default).
    """
    cluster_prefix = cluster_prefix or cluster_col
    return cluster_by_group_peaks(line_df, cluster_col=cluster_col, cluster_prefix=cluster_prefix,
                                  group_cols=group_cols, width_bin_size=width_bin_size,
                                  min_prominence=min_prominence, min_prominence_frac=min_prominence_frac,
                                  rel_height=rel_height)
