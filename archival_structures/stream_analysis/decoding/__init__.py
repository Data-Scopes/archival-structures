"""Sequence decoders for start-page detection: semi-Markov duration decoding (numpy +
scipy), and a linear-chain CRF and BiLSTM tagger (torch).

``semi_markov`` names are imported eagerly. The ``crf`` and ``bilstm`` names resolve lazily
(PEP 562), so ``import archival_structures.stream_analysis.decoding`` never imports torch.
"""
from importlib import import_module

from .semi_markov import EPS, decode_semi_markov, fit_lognormal_duration, log_duration_pmf

_LAZY = {
    "NEG_INF": "crf", "CRFSequence": "crf", "LinearChainCRF": "crf",
    "start_score_from_emissions": "crf", "collapsed_start_f1": "crf",
    "viterbi_start_f1": "crf", "train_crf": "crf", "decode_core": "crf",
    "BiLSTMTagger": "bilstm",
}

__all__ = ["EPS", "decode_semi_markov", "fit_lognormal_duration", "log_duration_pmf",
           *_LAZY]


def __getattr__(name):
    if name in _LAZY:
        return getattr(import_module(f".{_LAZY[name]}", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__))
