"""Partially-labelled linear-chain CRF over a sequence of pages, with training and decoding.

Ported from NLP's ``startpage/crf_sequence.py`` and ``scripts/train_multitask_crf.py``. The
state layout is the caller's: ``start_states`` names which state ids mean "is a start page".
Training sequences may contain positions whose label must not be used (``mask`` False, e.g.
a read-only context window): the partition sums freely over all states, the numerator is
clamped to the gold state at masked-in positions only. One sequence at a time, CPU, no
batching, no end scores. Imports torch.
"""
from typing import NotRequired, TypedDict

import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import f1_score

NEG_INF = -1.0e9

__all__ = ["NEG_INF", "CRFSequence", "LinearChainCRF", "start_score_from_emissions",
           "collapsed_start_f1", "viterbi_start_f1", "train_crf", "decode_core"]


class CRFSequence(TypedDict):
    features: np.ndarray  # (T, F) float32
    labels: np.ndarray  # (T,) int64 state ids
    mask: np.ndarray  # (T,) bool, True = position is scored
    page_ids: NotRequired[list]  # list[str]
    inventory_number: NotRequired[str]


class LinearChainCRF(nn.Module):
    def __init__(self, n_features, n_states=3):
        super().__init__()
        self.n_states = n_states
        self.emission = nn.Linear(n_features, n_states)
        self.transitions = nn.Parameter(torch.zeros(n_states, n_states))
        self.start_scores = nn.Parameter(torch.zeros(n_states))

    def emission_scores(self, features):
        return self.emission(features)

    def _partition(self, emissions):
        """logZ, summing freely over all states at every position."""
        alpha = self.start_scores + emissions[0]
        for t in range(1, emissions.shape[0]):
            scores = alpha.unsqueeze(1) + self.transitions + emissions[t].unsqueeze(0)
            alpha = torch.logsumexp(scores, dim=0)
        return torch.logsumexp(alpha, dim=0)

    def _constrained_partition(self, emissions, mask, labels):
        """Same recursion, clamped to the gold label at masked (labelled) positions."""
        def clamp(a, t):
            if not mask[t]:
                return a
            out = torch.full_like(a, NEG_INF)
            out[labels[t]] = a[labels[t]]
            return out

        alpha = clamp(self.start_scores + emissions[0], 0)
        for t in range(1, emissions.shape[0]):
            scores = alpha.unsqueeze(1) + self.transitions + emissions[t].unsqueeze(0)
            alpha = torch.logsumexp(scores, dim=0)
            alpha = clamp(alpha, t)
        return torch.logsumexp(alpha, dim=0)

    def nll(self, features, mask, labels):
        """features: (T, n_features); mask: (T,) bool, True = labelled position;
        labels: (T,) long, gold state index (meaningless where mask is False)."""
        emissions = self.emission_scores(features)
        log_z = self._partition(emissions)
        log_num = self._constrained_partition(emissions, mask, labels)
        return log_z - log_num

    @torch.no_grad()
    def viterbi(self, features):
        """Most likely state sequence under the full CRF (emissions + transitions)."""
        emissions = self.emission_scores(features)
        T, n = emissions.shape
        delta = self.start_scores + emissions[0]
        backpointers = []
        for t in range(1, T):
            scores = delta.unsqueeze(1) + self.transitions  # (prev, cur)
            best_prev = torch.argmax(scores, dim=0)
            best_scores = scores[best_prev, torch.arange(n)]
            delta = best_scores + emissions[t]
            backpointers.append(best_prev)
        path = [int(torch.argmax(delta))]
        for bp in reversed(backpointers):
            path.append(int(bp[path[-1]]))
        path.reverse()
        return path

    @torch.no_grad()
    def argmax_emissions(self, features):
        """No-transition ablation: decode each position independently from the same
        emission scores, ignoring the transition matrix."""
        emissions = self.emission_scores(features)
        return torch.argmax(emissions, dim=1).tolist()


def start_score_from_emissions(model, features, start_states=(0,)):
    """(T,) log-odds of "any start state" against the remaining states, as input for
    ``decode_semi_markov``.

    With one start state the start term is that emission column itself; with several it is
    a logsumexp over them. The rest is a logsumexp over the other states (ascending)."""
    start_states = sorted(set(int(s) for s in start_states))
    with torch.no_grad():
        emissions = model.emission_scores(features)
        others = [s for s in range(model.n_states) if s not in start_states]
        if not start_states or not others:
            raise ValueError("start_states must be a non-empty proper subset of the states")
        if len(start_states) == 1:
            start = emissions[:, start_states[0]]
        else:
            start = torch.logsumexp(emissions[:, start_states], dim=1)
        other = torch.logsumexp(emissions[:, others], dim=1)
        return (start - other).numpy()


def collapsed_start_f1(y_true_states, y_pred_states, start_states):
    """F1 of the "is a start state" class after collapsing state ids to bool."""
    start_states = set(start_states)
    y_true = [s in start_states for s in y_true_states]
    y_pred = [s in start_states for s in y_pred_states]
    return f1_score(y_true, y_pred, pos_label=True, zero_division=0)


def viterbi_start_f1(model, sequences, start_states):
    """Viterbi-decode each sequence; start F1 over the masked-in positions only."""
    y_true, y_pred = [], []
    for seq in sequences:
        features = torch.tensor(seq["features"])
        path = model.viterbi(features)
        for s, p, m in zip(seq["labels"], path, seq["mask"]):
            if m:
                y_true.append(s)
                y_pred.append(p)
    return collapsed_start_f1(y_true, y_pred, start_states)


def train_crf(train_sequences, dev_sequences, n_features, n_states=3, start_states=(0,),
              seed=0, *, lr=0.05, max_epochs=40, patience=6):
    """Train with Adam, one optimiser step per sequence, early stopping on dev start F1.
    Returns (model with the best-dev-F1 weights loaded, best dev F1)."""
    torch.manual_seed(seed)
    model = LinearChainCRF(n_features=n_features, n_states=n_states)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    rng = np.random.RandomState(seed)
    best_f1, best_state, epochs_since_improve = -1.0, None, 0

    for epoch in range(max_epochs):
        order = rng.permutation(len(train_sequences))
        for idx in order:
            seq = train_sequences[idx]
            features = torch.tensor(seq["features"])
            labels = torch.tensor(seq["labels"])
            mask = seq["mask"]
            optimizer.zero_grad()
            loss = model.nll(features, mask, labels)
            loss.backward()
            optimizer.step()

        f1 = viterbi_start_f1(model, dev_sequences, start_states)
        if f1 > best_f1:
            best_f1 = f1
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
            epochs_since_improve = 0
        else:
            epochs_since_improve += 1
            if epochs_since_improve >= patience:
                break

    model.load_state_dict(best_state)
    return model, best_f1


def decode_core(model, sequences, start_states, sep="/"):
    """Viterbi-decode and keep masked-in ("core") positions only.

    Returns (y_true, y_pred, scan_ids, gold_states, pred_states): bool start labels, scan ids
    ``f"{inventory_number}{sep}{page_id}"`` and the raw state ids."""
    start_states = set(start_states)
    y_true, y_pred, scan_ids, gold_states, pred_states = [], [], [], [], []
    for i, seq in enumerate(sequences):
        for key in ("page_ids", "inventory_number"):
            if key not in seq:
                raise KeyError(f"sequence {i} has no {key!r}; decode_core needs it for scan ids")
        features = torch.tensor(seq["features"])
        path = model.viterbi(features)
        for pid, s, p, m in zip(seq["page_ids"], seq["labels"], path, seq["mask"]):
            if not m:
                continue
            y_true.append(s in start_states)
            y_pred.append(p in start_states)
            scan_ids.append(f"{seq['inventory_number']}{sep}{pid}")
            gold_states.append(s)
            pred_states.append(p)
    return y_true, y_pred, scan_ids, gold_states, pred_states
