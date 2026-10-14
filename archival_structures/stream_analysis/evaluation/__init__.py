"""Segmentation evaluation suite (C6) and chunk splits / split validators (C7).

Import from here: ``from archival_structures.stream_analysis.evaluation import pk, ...``.
Labels are ``bool`` (``True`` = start page).
"""

from archival_structures.stream_analysis.evaluation import aggregate, baselines, segmentation, splits
from archival_structures.stream_analysis.evaluation.aggregate import *  # noqa: F401,F403
from archival_structures.stream_analysis.evaluation.baselines import *  # noqa: F401,F403
from archival_structures.stream_analysis.evaluation.segmentation import *  # noqa: F401,F403
from archival_structures.stream_analysis.evaluation.splits import *  # noqa: F401,F403

__all__ = list(segmentation.__all__) + list(aggregate.__all__) + list(baselines.__all__) + list(splits.__all__)
