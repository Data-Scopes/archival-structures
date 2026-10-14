import numpy as np
import pytest
from scipy.stats import norm

from archival_structures.stream_analysis.decoding import (
    decode_semi_markov, fit_lognormal_duration, log_duration_pmf)

NLP_PRIORS = [(1.1900895953077164, 0.9684310847297153),
              (1.225606847236485, 0.99154906715885)]


def test_fit_lognormal_duration_recovers_known_parameters():
    rng = np.random.RandomState(0)
    true_mu, true_sigma = 1.3, 0.5
    lengths = np.round(np.exp(rng.normal(true_mu, true_sigma, size=5000)))
    lengths = np.clip(lengths, 1, None)
    fit = fit_lognormal_duration(lengths)
    assert abs(fit["mu"] - true_mu) < 0.05
    assert abs(fit["sigma"] - true_sigma) < 0.05


def test_log_duration_pmf_is_normalized_and_peaks_near_mode():
    fit = fit_lognormal_duration([2, 3, 3, 4, 4, 4, 5, 5, 6, 20])
    log_pmf = log_duration_pmf(fit["mu"], fit["sigma"], max_duration=100)
    pmf = np.exp(log_pmf)
    assert abs(pmf.sum() - 1.0) < 1e-6
    mode_d = int(np.argmax(pmf)) + 1
    assert 2 <= mode_d <= 6


def test_decode_semi_markov_recovers_strong_unambiguous_boundaries():
    start_scores = np.array([5.0, -5.0, -5.0, -5.0, 5.0, -5.0, -5.0, -5.0])
    fit = fit_lognormal_duration([4, 4, 4, 4, 4])
    log_pmf = log_duration_pmf(fit["mu"], fit["sigma"], max_duration=20)
    pred = decode_semi_markov(start_scores, log_pmf, max_duration=20)
    assert pred == [True, False, False, False, True, False, False, False]


def test_decode_semi_markov_prefers_fitted_length_when_evidence_is_weak():
    rng = np.random.RandomState(1)
    start_scores = rng.normal(0.0, 0.3, size=40)
    fit = fit_lognormal_duration([4] * 20)
    log_pmf = log_duration_pmf(fit["mu"], fit["sigma"], max_duration=30)
    pred = decode_semi_markov(start_scores, log_pmf, max_duration=30)
    starts = [i for i, p in enumerate(pred) if p]
    gaps = np.diff(starts)
    assert len(gaps) > 0
    assert abs(gaps.mean() - 4) < 1.5


def _pmf():
    return log_duration_pmf(*NLP_PRIORS[0], max_duration=20)


def test_decode_empty_and_single():
    assert decode_semi_markov([], _pmf(), 20) == []
    assert decode_semi_markov([-3.0], _pmf(), 20) == [True]


def test_max_duration_shorter_than_sequence_forces_starts():
    pred = decode_semi_markov(np.full(10, -50.0), _pmf(), 3)
    starts = [i for i, p in enumerate(pred) if p]
    assert len(starts) >= 4 and starts[0] == 0
    assert max(np.diff(starts + [10])) <= 3


def test_short_log_pmf_raises():
    with pytest.raises(ValueError):
        decode_semi_markov(np.zeros(5), np.zeros(3), 4)


def test_max_duration_below_one_raises():
    with pytest.raises(ValueError):
        decode_semi_markov(np.zeros(5), _pmf(), 0)


def test_output_is_bool_and_first_is_start():
    for seed in range(5):
        scores = np.random.default_rng(seed).normal(0, 2, size=30)
        pred = decode_semi_markov(scores, _pmf(), 20)
        assert isinstance(pred, list) and len(pred) == 30
        assert all(isinstance(p, bool) for p in pred)
        assert pred[0] is True


def _reference_pmf(mu, sigma, max_duration):
    """The NLP original, with scipy.stats.norm.cdf."""
    def cdf(x):
        out = np.zeros_like(x)
        pos = x > 0
        out[pos] = norm.cdf((np.log(x[pos]) - mu) / sigma)
        return out
    d = np.arange(1, max_duration + 1, dtype=float)
    pmf = np.clip(cdf(d + 0.5) - cdf(d - 0.5), 0.0, None)
    pmf = pmf / pmf.sum()
    return np.log(np.clip(pmf, 1e-9, None))


@pytest.mark.parametrize("mu,sigma", NLP_PRIORS)
def test_pmf_matches_scipy_norm_reference(mu, sigma):
    assert np.array_equal(log_duration_pmf(mu, sigma, 250), _reference_pmf(mu, sigma, 250))


def test_zero_mass_raises():
    with pytest.raises(ValueError):
        log_duration_pmf(20.0, 0.1, 5)
