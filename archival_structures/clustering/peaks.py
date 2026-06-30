"""1D peak detection for bucketing a numeric column (e.g. line indentation) into named,
recurring value ranges.

`compute_peaks` bins `values` at `width_bin_size` resolution, finds local-maximum peaks in the
resulting frequency distribution (`scipy.signal.find_peaks`), and widens each peak to its base
(where the surrounding frequency drops back to baseline), trimming overlaps between adjacent
peaks (`adjust_peak_widths`). `cluster_by_group_peaks` applies this per group (e.g. per scan)
and labels each value by which peak's range it falls into -- a lighter-weight alternative to
HDBSCAN for values that naturally cluster into a small number of discrete levels, such as
indentation. See `archival_structures.analysis.line_clustering.cluster_lines_by_peaks`.
"""

import copy
import decimal
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Union

import matplotlib.pyplot as plt
import numpy as np
import numpy.typing as npt
import pandas as pd
import seaborn as sns
from scipy.signal import find_peaks, peak_widths


@dataclass
class Dist:
    """A value's frequency distribution, binned at a fixed width (see `prep_dist`)."""

    val_freq: Counter
    values: np.array
    freqs: np.array
    min_val: Union[float, int]
    max_val: Union[float, int]


@dataclass
class Peak:
    """One local-maximum peak in a `Dist`, with its base range (where it widens out to) and
    prominence (how much it stands out above its base), as found by `compute_peaks`."""

    peak_x: Union[float, int]
    peak_freq: int
    base_freq: int
    peak_n: int
    base_left: Union[float, int]
    base_right: Union[float, int]
    prominence: Union[float, int]
    prominence_frac: float


def map_value_to_cluster(value: Union[float, int], peaks: List[Peak], prefix: str) -> str:
    """`f"{prefix}_{base_left}_{base_right}"` for whichever `peaks` entry's base range contains
    `value`, or `f"{prefix}_NA"` if none does."""
    for peak in peaks:
        if peak.base_left <= value <= peak.base_right:
            left, right = peak.base_left, peak.base_right
            return f"{prefix}_{left}_{right}"
    return f"{prefix}_NA"


def determine_decimals(width_bin_size: Union[float, int]) -> int:
    """Number of decimal places needed to represent `width_bin_size` exactly (0 for an integer
    or any bin size > 1.0), used to round bin edges consistently."""
    if isinstance(width_bin_size, int) or width_bin_size > 1.0:
        return 0
    else:
        return -decimal.Decimal(str(width_bin_size)).as_tuple().exponent


def prep_dist(values: List[Union[float, int]], width_bin_size: Union[float, int],
              decimals: int, debug: int = 0) -> Dist:
    """Bin `values` into a `Dist`: a regular grid spanning `[min(values) - width_bin_size,
    max(values) + width_bin_size]` at `width_bin_size` resolution, with the count of `values`
    at (rounded to `decimals`) each grid point.

    Note: bins only count values that round to *exactly* a grid point, rather than aggregating
    everything within a bin's range -- fine for densely-sampled pixel coordinates (the original
    use case) but not a true histogram for sparse/irregularly-spaced values."""
    val_freq = Counter(values)
    min_val: Union[float, int] = min(val_freq)
    max_val: Union[float, int] = max(val_freq)
    start_range = min_val - width_bin_size
    end_range = max_val + width_bin_size + 1e-10
    x_range = np.arange(start_range, end_range, width_bin_size)
    values = np.array([np.round(x, decimals) for x in x_range])
    dist = np.array([val_freq[np.round(x, decimals)] for x in x_range])
    if debug > 2:
        print(f"val_freq: {val_freq}")
        print(f"values: {values}")
        print(f"dist: {dist}")
        print(f"decimals: {decimals}")
    return Dist(val_freq, values, dist, min_val, max_val)


def adjust_peak_widths(peaks: List[Peak], width_bin_size: Union[float, int],
                       decimals: int, debug: int = 0) -> List[Peak]:
    """Trim overlapping base ranges between consecutive `peaks` (sorted by position) so each
    value falls in exactly one peak's range: an overlap is split at the midpoint between the
    two peaks' centres, snapped to the nearest `width_bin_size` grid line. Returns a new list
    (does not mutate `peaks`)."""
    prev_right = 0
    peaks = copy.deepcopy(peaks)
    for pi, curr_peak in enumerate(peaks):
        if debug > 0:
            print(f"\nadjust_peak_widths - curr_peak: {curr_peak}")
        if prev_right > curr_peak.base_left:
            if debug > 0:
                print(f"adjust_peak_widths - adjust curr_peak.base_left {curr_peak.base_left} "
                      f"to prev_right {prev_right}")
            curr_peak.base_left = prev_right
        if pi < len(peaks) - 1:
            next_peak = peaks[pi+1]
            if curr_peak.base_right > next_peak.base_left:
                mid_peaks = np.round((curr_peak.peak_x + next_peak.peak_x) / 2, decimals)
                if debug > 0:
                    print(f"adjust_peak_widths - curr_peak.base_right {curr_peak.base_right} bigger than "
                          f"next_peak.base_left {next_peak.base_left}\n\tadjusting to mid_peaks: {mid_peaks}")
                if mid_peaks % width_bin_size == 0:
                    curr_peak.base_right = np.round(mid_peaks, decimals)
                    next_peak.base_left = np.round(mid_peaks + width_bin_size, decimals)
                else:
                    curr_peak.base_right = np.round((mid_peaks // width_bin_size) * width_bin_size, decimals)
                    next_peak.base_left = np.round(curr_peak.base_right + width_bin_size, decimals)
        prev_right = curr_peak.base_right
        if debug > 0:
            print(f"adjust_peak_widths - prev_right: {prev_right}")
    if decimals == 0:
        for peak in peaks:
            for field in ['base_left', 'base_right', 'peak_x']:
                peak.__setattr__(field, int(peak.__getattribute__(field)))
    return peaks


def _plot_peaks(peaks: List[Peak], values: List[Union[float, int]],
               width_bin_size: Union[float, int], ax: plt.Axes, label: str = None):
    decimals = determine_decimals(width_bin_size)
    dist = prep_dist(values, width_bin_size=width_bin_size, decimals=decimals)
    ax.plot(dist.values, dist.freqs, label=label)
    ax.plot([peak.peak_x for peak in peaks], [peak.peak_freq for peak in peaks], 'x')
    ax.hlines([peak.base_freq for peak in peaks],
              [peak.base_left for peak in peaks],
              [peak.base_right for peak in peaks], color="C2")
    return ax


def plot_peaks(peaks: List[Peak], values: List[Union[float, int]],
               width_bin_size: Union[float, int], title: str = None,
               ax: plt.Axes = None, label: str = None):
    """Plot `values`' frequency distribution with `peaks` marked (peak positions as 'x', base
    ranges as horizontal lines), on `ax` (a new figure/axes if not given)."""
    if ax is None:
        fig, ax = plt.subplots()
    _plot_peaks(peaks, values, width_bin_size, ax, label=label)
    if title is not None:
        ax.set_title(title)
    return ax


def compute_peaks(values: Union[List[Union[float, int]], npt.NDArray], width_bin_size: Union[float, int],
                  min_prominence: Union[float, int] = 0, min_prominence_frac: float = None,
                  rel_height: float = 1.0, debug: int = 0) -> List[Peak]:
    """Find local-maximum peaks in `values`' frequency distribution (binned at
    `width_bin_size`), widen each to its base via `scipy.signal.peak_widths` (`rel_height`
    controls how far down from the peak the base extends), and trim overlaps
    (`adjust_peak_widths`). Peaks below `min_prominence` (absolute) or `min_prominence_frac`
    (relative to the peak's own frequency) are dropped."""
    decimals = determine_decimals(width_bin_size)
    dist = prep_dist(values, width_bin_size=width_bin_size, decimals=decimals)
    min_val, max_val = dist.min_val, dist.max_val
    peaks, properties = find_peaks(dist.freqs)
    width_info = peak_widths(dist.freqs, peaks, rel_height=rel_height)
    peak_freq = np.array(dist.freqs)[peaks]

    base_freq = [x for x in width_info[1]]

    def map_to_values(x):
        """Convert a `scipy.signal.peak_widths` bin index `x` back into `values`' own units."""
        return (min_val - width_bin_size) + np.round(x, decimals) * width_bin_size

    base_left = [map_to_values(x) for x in width_info[2]]
    base_right = [map_to_values(x) for x in width_info[3]]
    peak_x = np.array(dist.values)[peaks]
    if debug > 0:
        print(f"compute_peaks - values: {dist.values}\n\tfreqs: {dist.freqs}")
        print(f"compute_peaks - peaks: {peaks}\n\tpeak_freq: {peak_freq}")
        print(f"compute_peaks - peak x: {peak_x}")
        print(f"compute_base_left - base_left: {base_left}\n\tbase_right: {base_right}")
    # check that base_left is lower than peak_x and base_right is higher than peak_x
    for bl, px, br in zip(base_left, peak_x, base_right):
        if bl > px:
            print(f"width_info[0]: {width_info[0]}")
            print(f"width_info[1]: {width_info[1]}")
            print(f"width_info[2]: {width_info[2]}")
            print(f"width_info[3]: {width_info[3]}")
            raise ValueError(f"base_left value ({bl}) cannot be higher than peak x ({px})")
        if br < px:
            print(f"width_info[0]: {width_info[0]}")
            print(f"width_info[1]: {width_info[1]}")
            print(f"width_info[2]: {width_info[2]}")
            print(f"width_info[3]: {width_info[3]}")
            raise ValueError(f"base_right value ({br}) cannot be lower than peak x ({px})")
    peaks = []
    for pi in range(len(peak_x)):
        bl = np.round(base_left[pi], decimals) if base_left[pi] >= min_val else min_val
        br = np.round(base_right[pi], decimals) if base_right[pi] <= max_val else max_val
        prom = peak_freq[pi] - base_freq[pi]
        prom_frac = prom / peak_freq[pi]
        # print(f"peak_x: {peak_x[pi]}\nbase_left: {base_left[pi]}\nbase_right: {base_right[pi]}")
        # print(f"bl: {bl}\nbr: {br}\npeak_freq: {peak_freq[pi]}\nbase_freq: {base_freq[pi]}")
        # print(f"dist.freqs ({len(dist.freqs)}): {dist.freqs}")
        peak_n = sum(dist.val_freq[x] for x in range(int(bl), int(br) + 1))
        # print(f"peak_n: {peak_n}\n")
        peak = Peak(peak_x[pi], peak_freq[pi], base_freq[pi], peak_n, bl, br, prom, prom_frac)
        peaks.append(peak)
    # filter peak with minimum prominence fraction
    if min_prominence_frac is not None:
        peaks = [peak for peak in peaks if peak.prominence_frac > min_prominence_frac]
    peaks = [peak for peak in peaks if peak.prominence > min_prominence]
    return adjust_peak_widths(peaks, width_bin_size=width_bin_size, decimals=decimals, debug=debug)


def cluster_by_group_peaks(doc_df: pd.DataFrame, cluster_col: any, cluster_prefix: str,
                           group_cols: Union[str, List[str]] = None, width_bin_size: Union[float, int] = 50,
                           min_prominence: Union[float, int] = None, min_prominence_frac: float = None,
                           rel_height: float = 1.0, summary: bool = False, plot: bool = False) -> pd.Series:
    """Label each row of `doc_df` by which peak its `cluster_col` value falls into
    (`map_value_to_cluster`), computing peaks separately per group (`group_cols`, e.g. one
    scan) rather than corpus-wide -- so peak labels are group-relative, not globally comparable
    (a `left_645_745` cluster on one scan isn't necessarily the same position on another scan
    with a different layout). If `min_prominence` isn't given, it defaults to `group_size /
    100`, scaling the prominence threshold to each group's own size. Set `summary`/`plot` to
    print/plot each group's peaks for inspection."""
    group_dfs = list(doc_df.groupby(group_cols)) if group_cols else [('', doc_df)]
    clusters = []
    if plot is True:
        if len(group_dfs) == 1:
            num_cols, num_rows = 1, 1
        else:
            num_cols = 2
            num_rows = int(len(group_dfs) / num_cols)
            if len(group_dfs) % num_cols > 0:
                num_rows += 1
        fig, ax = plt.subplots(num_cols, num_rows)
    for gi, group in enumerate(group_dfs):
        group_idx, group_df = group
        num_docs = len(group_df)
        group_min_prom = min_prominence if min_prominence is not None else num_docs / 100
        peaks = compute_peaks(group_df[cluster_col], width_bin_size=width_bin_size,
                              min_prominence=group_min_prom, min_prominence_frac=min_prominence_frac,
                              rel_height=rel_height)
        cluster = group_df[cluster_col].apply(lambda val: map_value_to_cluster(val, peaks, cluster_prefix))
        clusters.append(cluster)

        if summary is False:
            continue
        # print a summary of the peaks
        print(f"\n-----------------\n{group_idx}\tnum_docs: {len(group_df)}\tmin_prominence: {group_min_prom}\n\n")

        for peak in peaks:
            if width_bin_size > 1.0:
                print(f"left: {peak.base_left: >8.0f}\tpeak: {peak.peak_x: >8.0f}"
                      f"\tright: {peak.base_right: >8.0f}\tfreq: {peak.peak_freq: >8.0f}"
                      f"\tprominence: {peak.prominence: >8.0f}\tprom_frac: {peak.prominence_frac: >4.2f}")
            else:
                print(f"left: {peak.base_left: >4.2f}\tpeak: {peak.peak_x: >4.2f}"
                      f"\tright: {peak.base_right: >4.2f}\tfreq: {peak.peak_freq: >8.0f}"
                      f"\tprominence: {peak.prominence: >8.0f}\tprom_frac: {peak.prominence_frac: >4.2f}")
        if plot is True:
            row = gi // num_cols
            col = gi % num_cols
            if num_cols == 1:
                group_ax = ax
            elif num_rows == 1:
                group_ax = ax[col]
            else:
                group_ax = ax[col][row]
            group_ax = _plot_peaks(peaks, group_df[cluster_col], width_bin_size=width_bin_size, ax=group_ax)
            group_ax.set_title(group_idx)
    return pd.concat(clusters)
