"""Region-quality filter: classify the text of an HTR region as usable, fragment or noise.

A rule cascade over region-local features (character mix, token shape, lexicon hits). It is a
filter that labels, not one that deletes: every region keeps its verdict and each consumer picks
its own ``keep`` view (``is_kept``). The functions take the **text** of a region (and optionally
its bounding box) and an injected lexicon (a set of lower-case words), so this module needs no
gensim, no corpus model and no sibling configuration.

The thresholds were tuned on SG (NL-HaNA 1.01.02) Dutch HTR, on the odd ``row_no`` half of a
hand-labelled gold sheet (LotA reports 05 and 09), and are not validated for other archives or
languages. Ported from LotA ``archival_language.corpus.quality`` (phase 08, step 16) with
identical arithmetic; LotA keeps ``load_lexicon`` and its source-level helpers.

No rule reads a region type or the bounding box: the bbox features are recorded only.
"""

from __future__ import annotations

import math
import re
import statistics
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import asdict, dataclass, fields

import pandas as pd

QUALITY_CLASSES: tuple[str, ...] = ("usable", "fragment", "noise")

#: Default ``keep`` view: ``noise`` is dropped, ``fragment`` is kept alongside ``usable``. The
#: gold set's fragments are short structural evidence (date stamps at the page edge, folio references,
#: catchwords) that downstream structure analysis must not lose. A consumer that wants ``usable``
#: only may filter further (the filter labels, it never deletes).
DEFAULT_KEEP_CLASSES: frozenset[str] = frozenset({"usable", "fragment"})

FEATURE_NAMES: tuple[str, ...] = (
    "n_tokens",
    "n_chars",
    "alpha_ratio",
    "digit_ratio",
    "punct_ratio",
    "uppercase_ratio",
    "mean_token_len",
    "max_token_len",
    "singleton_token_ratio",
    "longest_nonalpha_run",
    "lexicon_hit_rate",
    "lexicon_hit_rate_len_ge4",
    "bbox_aspect_ratio",
    "bbox_w",
    "bbox_h",
    "bbox_area",
)

#: Strip leading/trailing non-word characters before a lexicon lookup only; the stripped, lowered
#: form is a local value and is never stored on anything returned.
_STRIP_FOR_LOOKUP_RE = re.compile(r"^\W+|\W+$")


def is_kept(quality_class: str, *, keep_classes: frozenset[str] = DEFAULT_KEEP_CLASSES) -> bool:
    """Whether a verdict's class is in the default (or a caller-chosen) ``keep`` view."""
    return quality_class in keep_classes


@dataclass(frozen=True)
class QualityThresholds:
    """Every threshold of the rule cascade (the defaults are the SG-tuned LotA values)."""

    lexicon_hit_rate_len_ge4_strong: float = 0.5
    lexicon_hit_rate_len_ge4_weak: float = 0.2
    min_tokens_for_no_lexicon_penalty: float = 6
    singleton_token_ratio_noise: float = 0.5
    longest_nonalpha_run_noise: float = 6
    alpha_ratio_low: float = 0.3
    max_tokens_for_short: float = 2
    digit_vs_punct_fraction_for_numeric_exempt: float = 0.5
    score_cutoff_noise_fragment: float = 0.35
    score_cutoff_fragment_usable: float = 0.50

    def as_dict(self) -> dict[str, float]:
        return asdict(self)

    @classmethod
    def from_dict(cls, values: Mapping[str, float]) -> QualityThresholds:
        """Build from a mapping; an unknown key raises ``ValueError``, missing keys keep defaults."""
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(values) - known)
        if unknown:
            raise ValueError(f"unknown threshold key(s): {unknown}")
        return cls(**dict(values))


DEFAULT_QUALITY_THRESHOLDS = QualityThresholds()


@dataclass(frozen=True)
class QualityVerdict:
    region_id: str
    quality_class: str  # one of QUALITY_CLASSES
    score: float  # 0.0-1.0, monotone: higher = more usable
    rules: tuple[str, ...]  # every rule that fired, in firing order (provenance)


def lexicon_lookup_form(token: str) -> str:
    """Lowercased, punctuation-trimmed *local copy* of ``token``, for a lexicon lookup only."""
    return _STRIP_FOR_LOOKUP_RE.sub("", token).lower()


def region_features(
    text: str,
    *,
    lexicon: AbstractSet[str],
    bbox: tuple[float, float, float, float] | None = None,
) -> dict[str, float]:
    """Region-local features: everything is computable from ``text`` (and ``bbox`` = (x, y, w, h),
    recorded only). ``lexicon`` entries are expected lower-case and are never altered.

    Every token in ``lexicon`` counts as a hit, numerals included. LotA's fastText vocabulary
    holds 9,298 all-digit terms, so a tally such as "3761 -10 3" fires ``strong_lexicon_hits``
    (phase 08, F08-R.1). Pass a lexicon of words if numbers should not count."""
    n_chars = len(text)
    tokens = text.split()
    n_tokens = len(tokens)

    n_alpha = sum(1 for c in text if c.isalpha())
    n_digit = sum(1 for c in text if c.isdigit())
    n_space = sum(1 for c in text if c.isspace())
    n_punct = n_chars - n_alpha - n_digit - n_space
    n_upper = sum(1 for c in text if c.isupper())

    alpha_ratio = n_alpha / n_chars if n_chars else 0.0
    digit_ratio = n_digit / n_chars if n_chars else 0.0
    punct_ratio = n_punct / n_chars if n_chars else 0.0
    uppercase_ratio = n_upper / n_alpha if n_alpha else 0.0

    token_lens = [len(t) for t in tokens]
    mean_token_len = statistics.fmean(token_lens) if token_lens else 0.0
    max_token_len = float(max(token_lens)) if token_lens else 0.0
    singleton_token_ratio = (
        sum(1 for length in token_lens if length == 1) / n_tokens if n_tokens else 0.0
    )

    non_space = "".join(ch for ch in text if not ch.isspace())
    longest_nonalpha_run = 0
    current_run = 0
    for ch in non_space:
        if ch.isalpha():
            current_run = 0
        else:
            current_run += 1
            longest_nonalpha_run = max(longest_nonalpha_run, current_run)

    cleaned = [lexicon_lookup_form(t) for t in tokens]
    cleaned = [c for c in cleaned if c]
    hits_all = [c in lexicon for c in cleaned]
    lexicon_hit_rate = sum(hits_all) / len(hits_all) if hits_all else 0.0
    hits_ge4 = [c in lexicon for c in cleaned if len(c) >= 4]
    lexicon_hit_rate_len_ge4 = sum(hits_ge4) / len(hits_ge4) if hits_ge4 else 0.0

    if bbox is None:
        bbox_w = bbox_h = bbox_area = bbox_aspect_ratio = float("nan")
    else:
        _, _, w, h = bbox
        bbox_w = float(w)
        bbox_h = float(h)
        bbox_area = float(w * h)
        bbox_aspect_ratio = float(w / h) if h != 0 else math.inf

    return {
        "n_tokens": float(n_tokens),
        "n_chars": float(n_chars),
        "alpha_ratio": alpha_ratio,
        "digit_ratio": digit_ratio,
        "punct_ratio": punct_ratio,
        "uppercase_ratio": uppercase_ratio,
        "mean_token_len": mean_token_len,
        "max_token_len": max_token_len,
        "singleton_token_ratio": singleton_token_ratio,
        "longest_nonalpha_run": float(longest_nonalpha_run),
        "lexicon_hit_rate": lexicon_hit_rate,
        "lexicon_hit_rate_len_ge4": lexicon_hit_rate_len_ge4,
        "bbox_aspect_ratio": bbox_aspect_ratio,
        "bbox_w": bbox_w,
        "bbox_h": bbox_h,
        "bbox_area": bbox_area,
    }


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def classify_features(
    features: Mapping[str, float],
    *,
    region_id: str = "",
    thresholds: QualityThresholds = DEFAULT_QUALITY_THRESHOLDS,
) -> QualityVerdict:
    """The named, deterministic rule cascade over an already-computed feature mapping.

    ``score`` starts at 0.5 (undecided) and every fired rule nudges it toward 0 (noise) or 1
    (usable); it is clipped to ``[0, 1]``. The class comes from two cutoffs on the final score.
    No rule is decisive on its own: disagreement between rules is visible in ``rules``.
    """
    t = thresholds
    score = 0.5
    fired: list[str] = []

    if features["lexicon_hit_rate_len_ge4"] >= t.lexicon_hit_rate_len_ge4_strong:
        score += 0.30
        fired.append("strong_lexicon_hits")
    elif features["lexicon_hit_rate_len_ge4"] >= t.lexicon_hit_rate_len_ge4_weak:
        score += 0.15
        fired.append("weak_lexicon_hits")
    elif (
        features["n_tokens"] >= t.min_tokens_for_no_lexicon_penalty
        and features["lexicon_hit_rate_len_ge4"] == 0.0
    ):
        score -= 0.10
        fired.append("no_lexicon_hits_despite_length")

    if features["singleton_token_ratio"] >= t.singleton_token_ratio_noise:
        score -= 0.20
        fired.append("mostly_singleton_tokens")

    if features["longest_nonalpha_run"] >= t.longest_nonalpha_run_noise:
        score -= 0.30
        fired.append("long_nonalpha_run")

    if features["alpha_ratio"] < t.alpha_ratio_low:
        digit_ratio = features["digit_ratio"]
        punct_ratio = features["punct_ratio"]
        denom = digit_ratio + punct_ratio
        digit_vs_punct_fraction = digit_ratio / denom if denom else 0.0
        is_numeric_shaped = (
            digit_ratio > 0.0
            and digit_vs_punct_fraction >= t.digit_vs_punct_fraction_for_numeric_exempt
        )
        if is_numeric_shaped:
            # Named exemption, not a silent no-op (provenance): a rule that declines to fire
            # for a reason says so.
            fired.append("numeric_content_exempt")
        else:
            score -= 0.15
            fired.append("low_alpha_content")

    if features["n_tokens"] <= t.max_tokens_for_short:
        score -= 0.10
        fired.append("short_region")

    score = _clip(score)

    if score < t.score_cutoff_noise_fragment:
        quality_class = "noise"
    elif score < t.score_cutoff_fragment_usable:
        quality_class = "fragment"
    else:
        quality_class = "usable"

    return QualityVerdict(
        region_id=region_id, quality_class=quality_class, score=score, rules=tuple(fired)
    )


def score_region(
    text: str,
    *,
    lexicon: AbstractSet[str],
    region_id: str = "",
    bbox: tuple[float, float, float, float] | None = None,
    thresholds: QualityThresholds = DEFAULT_QUALITY_THRESHOLDS,
) -> QualityVerdict:
    """``classify_features(region_features(text, ...), ...)``: the single-region form."""
    features = region_features(text, lexicon=lexicon, bbox=bbox)
    return classify_features(features, region_id=region_id, thresholds=thresholds)


def quality_frame(
    regions: pd.DataFrame,
    *,
    lexicon: AbstractSet[str],
    text_col: str = "text",
    id_col: str | None = "region_id",
    thresholds: QualityThresholds = DEFAULT_QUALITY_THRESHOLDS,
    with_features: bool = True,
) -> pd.DataFrame:
    """A copy of ``regions`` (same row order) plus ``quality_class``, ``score``, ``rules``
    (``"|"``-joined) and, with ``with_features``, the ``FEATURE_NAMES`` columns."""
    out = regions.copy()
    classes: list[str] = []
    scores: list[float] = []
    rules: list[str] = []
    feats: list[dict[str, float]] = []
    texts = regions[text_col].tolist()
    ids = regions[id_col].tolist() if id_col is not None else [""] * len(texts)
    for text, rid in zip(texts, ids, strict=True):
        features = region_features(text, lexicon=lexicon)
        verdict = classify_features(features, region_id=str(rid), thresholds=thresholds)
        classes.append(verdict.quality_class)
        scores.append(verdict.score)
        rules.append("|".join(verdict.rules))
        feats.append(features)
    out["quality_class"] = pd.Series(classes, index=out.index, dtype=object)
    out["score"] = pd.Series(scores, index=out.index, dtype=float)
    out["rules"] = pd.Series(rules, index=out.index, dtype=object)
    if with_features:
        for name in FEATURE_NAMES:
            out[name] = pd.Series([f[name] for f in feats], index=out.index, dtype=float)
    return out
