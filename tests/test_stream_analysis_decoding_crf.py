import itertools

import numpy as np
import pytest
from sklearn.metrics import f1_score

torch = pytest.importorskip("torch")

from archival_structures.stream_analysis.decoding import crf as crf_mod  # noqa: E402
from archival_structures.stream_analysis.decoding.bilstm import BiLSTMTagger  # noqa: E402
from archival_structures.stream_analysis.decoding.crf import (  # noqa: E402
    LinearChainCRF, collapsed_start_f1, decode_core, start_score_from_emissions,
    train_crf, viterbi_start_f1)

F = 4
CASES = [(n, T, seed) for n in (2, 3) for T in range(1, 6) for seed in range(2)]


def random_model(n_states, seed):
    torch.manual_seed(seed)
    model = LinearChainCRF(F, n_states)
    with torch.no_grad():
        for p in model.parameters():
            p.copy_(torch.randn_like(p))
    return model


def path_score(model, em, path):
    em = em.detach()
    s = model.start_scores.detach()[path[0]] + em[0, path[0]]
    for t in range(1, len(path)):
        s = s + model.transitions.detach()[path[t - 1], path[t]] + em[t, path[t]]
    return s


@pytest.mark.parametrize("n,T,seed", CASES)
def test_viterbi_equals_brute_force(n, T, seed):
    model = random_model(n, seed)
    x = torch.randn(T, F)
    em = model.emission_scores(x)
    best = max(itertools.product(range(n), repeat=T),
               key=lambda p: float(path_score(model, em, p)))
    got = model.viterbi(x)
    assert float(path_score(model, em, got)) == pytest.approx(
        float(path_score(model, em, best)), abs=1e-5)


@pytest.mark.parametrize("n,T,seed", CASES)
def test_partition_equals_brute_force_logsumexp(n, T, seed):
    model = random_model(n, seed)
    em = model.emission_scores(torch.randn(T, F))
    scores = torch.stack([path_score(model, em, p)
                          for p in itertools.product(range(n), repeat=T)])
    assert float(model._partition(em).detach()) == pytest.approx(
        float(torch.logsumexp(scores, 0)), abs=1e-4)


@pytest.mark.parametrize("n,T,seed", CASES)
def test_constrained_partition_full_mask_is_gold_path_score(n, T, seed):
    model = random_model(n, seed)
    em = model.emission_scores(torch.randn(T, F))
    labels = torch.randint(0, n, (T,))
    mask = np.ones(T, dtype=bool)
    got = model._constrained_partition(em, mask, labels)
    assert float(got.detach()) == pytest.approx(float(path_score(model, em, labels.tolist())), abs=1e-4)


@pytest.mark.parametrize("n,T,seed", CASES)
def test_constrained_partition_empty_mask_is_partition(n, T, seed):
    model = random_model(n, seed)
    em = model.emission_scores(torch.randn(T, F))
    labels = torch.randint(0, n, (T,))
    mask = np.zeros(T, dtype=bool)
    assert float(model._constrained_partition(em, mask, labels).detach()) == pytest.approx(
        float(model._partition(em).detach()), abs=1e-5)


@pytest.mark.parametrize("n,T,seed", CASES)
def test_nll_nonnegative_and_differentiable(n, T, seed):
    model = random_model(n, seed)
    labels = torch.randint(0, n, (T,))
    mask = np.random.RandomState(seed).rand(T) > 0.3
    loss = model.nll(torch.randn(T, F), mask, labels)
    assert float(loss.detach()) >= -1e-5
    loss.backward()
    assert model.emission.weight.grad is not None


def test_argmax_emissions_ignores_transitions():
    model = random_model(3, 0)
    x = torch.randn(6, F)
    expected = torch.argmax(model.emission_scores(x), dim=1).tolist()
    with torch.no_grad():
        model.transitions.mul_(100.0)
    assert model.argmax_emissions(x) == expected


def test_start_score_from_emissions_matches_three_state_formula():
    model = random_model(3, 1)
    x = torch.randn(7, F)
    with torch.no_grad():
        em = model.emission_scores(x)
        original = (em[:, 0] - torch.logsumexp(em[:, [1, 2]], dim=1)).numpy()
    got = start_score_from_emissions(model, x, (0,))
    assert got.shape == (7,)
    assert torch.equal(torch.from_numpy(got), torch.from_numpy(original))


def test_start_score_from_emissions_multi_start_states():
    model = random_model(4, 2)
    x = torch.randn(7, F)
    with torch.no_grad():
        em = model.emission_scores(x)
        expected = (torch.logsumexp(em[:, [0, 2]], dim=1)
                    - torch.logsumexp(em[:, [1, 3]], dim=1)).numpy()
    np.testing.assert_allclose(start_score_from_emissions(model, x, (2, 0)), expected,
                               rtol=1e-6, atol=1e-6)


def test_collapsed_start_f1_matches_string_version():
    rng = np.random.RandomState(0)
    yt, yp = rng.randint(0, 4, 100), rng.randint(0, 4, 100)
    ss = {0, 3}
    yts = ["start_page" if s in ss else "not_start_page" for s in yt]
    yps = ["start_page" if s in ss else "not_start_page" for s in yp]
    expected = f1_score(yts, yps, pos_label="start_page", zero_division=0)
    assert collapsed_start_f1(yt, yp, ss) == expected
    assert collapsed_start_f1([1, 1], [1, 1], {0}) == 0.0


def synthetic(n_seq, seed, n_features=3):
    rng = np.random.RandomState(seed)
    seqs = []
    for i in range(n_seq):
        T = rng.randint(8, 21)
        labels = np.ones(T, dtype=np.int64)
        labels[0] = 0
        labels[rng.rand(T) < 0.2] = 0
        feats = rng.normal(0, 0.5, (T, n_features)).astype(np.float32)
        feats[:, 0] += np.where(labels == 0, 2.0, -2.0)
        seqs.append({"features": feats, "labels": labels, "mask": np.ones(T, dtype=bool),
                     "page_ids": [f"p{j}" for j in range(T)], "inventory_number": f"inv{i}"})
    return seqs


def test_train_crf_is_deterministic():
    train, dev = synthetic(30, 0), synthetic(10, 1)
    m1, f1a = train_crf(train, dev, 3, 2, (0,), seed=3)
    m2, f1b = train_crf(train, dev, 3, 2, (0,), seed=3)
    assert f1a == f1b >= 0.9
    for k, v in m1.state_dict().items():
        assert torch.equal(v, m2.state_dict()[k])


def test_train_crf_respects_mask():
    train, dev = synthetic(30, 0), synthetic(10, 1)
    base_model, base_f1 = train_crf(train, dev, 3, 2, (0,), seed=0)
    corrupted = []
    for seq in dev:
        s = dict(seq)
        mask = s["mask"].copy()
        mask[::3] = False
        s["mask"] = mask
        corrupted.append(s)
    f1_masked = viterbi_start_f1(base_model, corrupted, (0,))
    wrong = []
    for seq in corrupted:
        s = dict(seq)
        labels = s["labels"].copy()
        labels[~s["mask"]] = 1 - labels[~s["mask"]]
        s["labels"] = labels
        wrong.append(s)
    assert viterbi_start_f1(base_model, wrong, (0,)) == f1_masked


def test_decode_core_returns_core_pages_only():
    train = synthetic(10, 0)
    model, _ = train_crf(train, train, 3, 2, (0,), seed=0)
    seqs = synthetic(2, 5)
    seqs[0]["mask"] = seqs[0]["mask"].copy()
    seqs[0]["mask"][:2] = False
    yt, yp, ids, gold, pred = decode_core(model, seqs, {0}, sep="#")
    n_core = sum(int(s["mask"].sum()) for s in seqs)
    assert len(yt) == len(yp) == len(ids) == len(gold) == len(pred) == n_core
    assert all(isinstance(v, bool) for v in yt + yp)
    assert ids[0] == "inv0#p2"
    assert yt == [g == 0 for g in gold]


def test_decode_core_missing_page_ids_raises():
    model = random_model(2, 0)
    seq = {"features": np.zeros((3, F), dtype=np.float32), "labels": np.zeros(3, dtype=np.int64),
           "mask": np.ones(3, dtype=bool)}
    with pytest.raises(KeyError, match="page_ids"):
        decode_core(model, [seq], {0})
    seq["page_ids"] = ["a", "b", "c"]
    with pytest.raises(KeyError, match="inventory_number"):
        decode_core(model, [seq], {0})


def test_bilstm_shapes_and_predict_bools():
    torch.manual_seed(0)
    m = BiLSTMTagger(F, hidden_size=8)
    x = torch.randn(6, F)
    assert m(x).shape == (6,)
    pred = m.predict(x)
    assert len(pred) == 6 and all(isinstance(p, bool) for p in pred)


def test_package_exports_resolve_lazily():
    from archival_structures.stream_analysis import decoding
    assert decoding.LinearChainCRF is crf_mod.LinearChainCRF
    assert decoding.BiLSTMTagger is BiLSTMTagger
    with pytest.raises(AttributeError):
        decoding.nope
