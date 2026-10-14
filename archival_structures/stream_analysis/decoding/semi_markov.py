"""Semi-Markov (segment-length) decoding of start-page scores.

A Viterbi-style DP over "where are the document starts", scored as emission evidence (the
start score at each chosen start, the negated start score summed over every page in between,
which approximates the complement of "start") plus the log probability of each segment's
length under a fitted duration prior. The prior is a log-normal fitted on training segment
lengths (a statistical fit, not a gradient-trained parameter), used at decode time only.

No left/right censoring correction is applied: position 0 is always a start and the last
segment is scored as a complete one. Ported from NLP's ``startpage/semi_markov.py``.
"""
import numpy as np
from scipy.special import ndtr

EPS = 1e-9

__all__ = ["EPS", "fit_lognormal_duration", "log_duration_pmf", "decode_semi_markov"]


def fit_lognormal_duration(lengths):
    """MLE log-normal fit (mu, sigma of log(length)) over positive integer lengths."""
    lengths = np.asarray(lengths, dtype=float)
    assert np.all(lengths >= 1), "document lengths must be >= 1"
    log_lengths = np.log(lengths)
    return {"mu": float(log_lengths.mean()), "sigma": float(max(log_lengths.std(), EPS))}


def _lognormal_cdf(x, mu, sigma):
    x = np.asarray(x, dtype=float)
    out = np.zeros_like(x)
    positive = x > 0
    out[positive] = ndtr((np.log(x[positive]) - mu) / sigma)
    return out


def log_duration_pmf(mu, sigma, max_duration):
    """Discretized, truncated-and-renormalized log P(duration=d) for d=1..max_duration,
    as a length-``max_duration`` array indexed by d-1.

    Raises ValueError if the log-normal has no mass within ``max_duration``."""
    d = np.arange(1, max_duration + 1, dtype=float)
    lo = _lognormal_cdf(d - 0.5, mu, sigma)
    hi = _lognormal_cdf(d + 0.5, mu, sigma)
    pmf = np.clip(hi - lo, 0.0, None)
    total = pmf.sum()
    if not total > 0:
        raise ValueError("no probability mass within max_duration; raise max_duration")
    pmf = pmf / total
    return np.log(np.clip(pmf, EPS, None))


def decode_semi_markov(start_scores, log_pmf, max_duration):
    """Decode start pages with a duration prior.

    start_scores: (T,) per-page log-odds that the page is a document start (e.g. emission
    score of the start state minus the logsumexp of the others).
    log_pmf: (>= max_duration,) array from ``log_duration_pmf``, indexed by d-1.
    Returns a length-T ``list[bool]`` (True = start) found by backtracking the DP that
    maximises, over all start positions 0 = b_0 < b_1 < ... < b_k = T:
        sum_j [ start_scores[b_j] - sum_{i in (b_j, b_{j+1})} start_scores[i] ]
        + sum_j log_pmf[b_{j+1} - b_j - 1]
    Position 0 is always a start; T = 0 gives [] and T = 1 gives [True]. If max_duration < T
    segments are capped at max_duration, so there are at least ceil(T / max_duration) starts.
    The last segment is scored as complete (no censoring). Ties go to the first maximum
    (np.argmax), i.e. the longest candidate segment ending at t.

    Raises ValueError if max_duration < 1 or len(log_pmf) < max_duration.
    """
    if max_duration < 1:
        raise ValueError("max_duration must be >= 1")
    if len(log_pmf) < max_duration:
        raise ValueError(
            f"log_pmf has length {len(log_pmf)}, shorter than max_duration={max_duration}")
    start_scores = np.asarray(start_scores, dtype=float)
    t_total = len(start_scores)
    nonstart = -start_scores
    prefix = np.concatenate([[0.0], np.cumsum(nonstart)])  # prefix[i] = sum_{0}^{i-1} nonstart

    best = np.full(t_total + 1, -np.inf)
    best[0] = 0.0
    backptr = np.full(t_total + 1, -1, dtype=int)

    for t in range(1, t_total + 1):
        d_max_here = min(t, max_duration)
        s0 = t - d_max_here  # s ranges s0..t-1, d = t - s ranges d_max_here..1
        s_range = np.arange(s0, t)
        d_range = t - s_range  # decreasing from d_max_here to 1
        cand = (best[s_range] + start_scores[s_range]
                + (prefix[t] - prefix[s_range + 1])
                + log_pmf[d_range - 1])
        best_idx = int(np.argmax(cand))
        best[t] = cand[best_idx]
        backptr[t] = s_range[best_idx]

    starts = set()
    t = t_total
    while t > 0:
        s = int(backptr[t])
        starts.add(s)
        t = s
    return [i in starts for i in range(t_total)]
