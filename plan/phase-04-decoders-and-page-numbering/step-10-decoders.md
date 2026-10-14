# Step 10: sequence decoders in AS (`stream_analysis/decoding/`)

> **Phase:** 04, Decoders and page numbering. **Depends on:** nothing (phase 03 is done).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP has three small decoder modules (semi-Markov duration decoding, a linear-chain CRF, a
BiLSTM tagger) and a CRF training loop that lives in its scripts. This step builds them as a
generic AS package, `archival_structures.stream_analysis.decoding`, with tests: the 4 ported
semi-Markov tests, edge cases, and new brute-force CRF tests. **NLP is not edited in this
step.** Step 10b turns NLP's modules into shims and proves that NLP's stored numbers reproduce.

## Inputs

NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`). Read
these NLP files (read-only; you port from them):

| File | Anchor (lines as of 2026-10-14) | What to take |
|---|---|---|
| `<NLP>/src/startpage/semi_markov.py` (102 lines) | `EPS = 1e-9` L24, `fit_lognormal_duration` L27, `_lognormal_cdf` L36, `log_duration_pmf` L45, `decode_semi_markov` L58 | everything, verbatim apart from the changes below |
| `<NLP>/src/startpage/crf_sequence.py` (106 lines) | `NEG_INF = -1.0e9` L29, `class LinearChainCRF` L32 (`emission_scores` L40, `_partition` L43, `_constrained_partition` L51, `nll` L68, `viterbi` L77, `argmax_emissions` L97) | the class, verbatim. **Not** the constants `START, INSIDE, END`, `STATE_NAMES` (NLP's state layout; they stay in NLP) |
| `<NLP>/src/startpage/bilstm_sequence.py` (32 lines) | `class BiLSTMTagger` L18 | verbatim |
| `<NLP>/scripts/train_multitask_crf.py` | `collapsed_f1` L173, `dev_f1` L179, `train_crf` L191, `decode_sequences` L225; `MAX_EPOCHS = 40`, `PATIENCE = 6` | the generic loop (changes below) |
| `<NLP>/scripts/train_semi_markov_duration.py` | `start_score_from_emissions` L98 | generalised (below) |
| `<NLP>/tests/test_semi_markov.py` (48 lines, 4 tests) | | port all 4 |

AS facts:
- `archival_structures/stream_analysis/__init__.py` exports names lazily (PEP 562,
  `_LAZY_EXPORTS`). Don't add the decoders there; import them from `...decoding`.
- `tests/test_packaging_core_imports.py` runs imports in a subprocess where torch and other
  extras are blocked; `test_core_modules_import_without_extras` has a `names = [...]` list.
- `pyproject.toml` `[project] dependencies` (core) has no scipy; scipy is only in the `vision`
  extra. scikit-learn (core) already requires `scipy>=1.10`.
- AS `.venv`: torch 2.12.1, scipy 1.16.3, numpy 2.3.5. Labels in AS are `list[bool]`
  ("is start"), never strings (phase-03 decision P1).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/stream_analysis/decoding/__init__.py` | new. Eagerly re-exports `semi_markov`'s public names. `crf`/`bilstm` names resolve **lazily** via module `__getattr__` (PEP 562), so `import ...decoding` never imports torch. `__all__` lists both groups |
| `.../decoding/semi_markov.py` | new, numpy + scipy only |
| `.../decoding/crf.py` | new, imports torch at module top |
| `.../decoding/bilstm.py` | new, imports torch at module top |
| `pyproject.toml` | add `"scipy>=1.10"` to the **core** `dependencies` list (user decision 2026-10-14). Leave the `vision` extra's scipy line alone. Don't run `poetry lock` |
| `tests/test_stream_analysis_decoding_semi_markov.py` | new |
| `tests/test_stream_analysis_decoding_crf.py` | new; `torch = pytest.importorskip("torch")` at the top |
| `tests/test_packaging_core_imports.py` | add `"archival_structures.stream_analysis.decoding"` and `"archival_structures.stream_analysis.decoding.semi_markov"` to `names` in `test_core_modules_import_without_extras` (one-line edit) |
| `docs/shared_library/changelog.md` | `### Step 10: sequence decoders (C9)` under `## Unreleased`. The file's top section is currently `## v0.7.0 (2026-10-14)`; if there's no `## Unreleased` heading yet, insert one above it. Use Edit |
| `reports/phase-04-decoders-and-page-numbering.md` | your `### Step 10` section and findings `F04-10.n` |

## Behaviour to implement

**`semi_markov.py`.** Public: `EPS`, `fit_lognormal_duration`, `log_duration_pmf`,
`decode_semi_markov`. Keep `_lognormal_cdf` private.
- `fit_lognormal_duration(lengths) -> dict` with keys `"mu"`, `"sigma"`: verbatim.
- `_lognormal_cdf(x, mu, sigma)`: replace `from scipy.stats import norm` / `norm.cdf(z)` by
  `from scipy.special import ndtr` / `ndtr(z)` (module-level import). Planning checked that
  `ndtr` equals `norm.cdf` bit for bit on 200,001 points in [−40, 40]. Nothing else changes.
- `log_duration_pmf(mu, sigma, max_duration) -> np.ndarray` shape `(max_duration,)`, indexed
  `d−1`: verbatim, but replace the `assert total > 0` with
  `raise ValueError("no probability mass within max_duration; raise max_duration")`.
- `decode_semi_markov(start_scores, log_pmf, max_duration) -> list[bool]`: the DP verbatim
  (same `argmax` tie-breaking, same `prefix` sums, same backtracking), with two changes:
  1. **returns `list[bool]`** (`True` = start) instead of 0/1 ints; NLP's shim converts back;
  2. validates its input: `ValueError` if `max_duration < 1` or `len(log_pmf) < max_duration`
     (the original raised an IndexError deep in the loop, or silently used a short pmf).
  Keep: position 0 is always a start; `T = 0` returns `[]`; `T = 1` returns `[True]`; when
  `max_duration < T`, segments are capped, so there are at least `ceil(T / max_duration)`
  starts; the last segment is scored as complete (no censoring). Say so in the docstring.

**`crf.py`.** Public: `NEG_INF`, `LinearChainCRF`, `start_score_from_emissions`,
`collapsed_start_f1`, `viterbi_start_f1`, `train_crf`, `decode_core`, and a `CRFSequence`
`TypedDict` (`features: np.ndarray (T, F) float32`, `labels: np.ndarray (T,) int64` state ids,
`mask: np.ndarray (T,) bool` "is scored", optional `page_ids: list[str]`,
`inventory_number: str`). This is exactly the dict shape NLP builds.
- `LinearChainCRF(n_features, n_states=3)`: verbatim, including parameter creation order
  (`emission`, `transitions`, `start_scores`), so that `torch.manual_seed` gives NLP's init.
  One sequence at a time, `(T, F)`.
- `start_score_from_emissions(model, features, start_states=(0,)) -> np.ndarray` `(T,)`:
  log-odds of "any start state" against the rest, under `torch.no_grad()`. **Bit-identity
  rule:** when `len(start_states) == 1`, the start term is `emissions[:, s]` itself (not a
  logsumexp over one column), and the rest is
  `torch.logsumexp(emissions[:, others], dim=1)` with `others` in ascending order. For NLP's
  3-state layout this is exactly the original
  `emissions[:, START] - torch.logsumexp(emissions[:, [INSIDE, END]], dim=1)`. With several
  start states, use a logsumexp for the start term too. Return `.numpy()`.
- `collapsed_start_f1(y_true_states, y_pred_states, start_states) -> float`: map each state to
  `s in start_states` (bool), then `sklearn.metrics.f1_score(y_true, y_pred, pos_label=True,
  zero_division=0)`. This equals NLP's string version.
- `viterbi_start_f1(model, sequences, start_states) -> float`: NLP's `dev_f1` (L179): Viterbi
  on each sequence, collect masked positions only, then `collapsed_start_f1`.
- `train_crf(train_sequences, dev_sequences, n_features, n_states=3, start_states=(0,),
  seed=0, *, lr=0.05, max_epochs=40, patience=6) -> tuple[LinearChainCRF, float]`: NLP's
  `train_crf` (L191), statement for statement. Keep the order: `torch.manual_seed(seed)` →
  model → `Adam(lr)` → `np.random.RandomState(seed)`; one `rng.permutation` per epoch; one
  optimiser step per sequence on `model.nll(torch.tensor(features), mask, torch.tensor(labels))`;
  strict `>` improvement from `-1.0`; `{k: v.clone()}` snapshot; `load_state_dict` at the end.
  No device selection (CPU), no `set_num_threads` (the caller decides).
- `decode_core(model, sequences, start_states, sep="/") -> tuple[list[bool], list[bool],
  list[str], list[int], list[int]]`: NLP's `decode_sequences` (L225), returning bool labels.
  Scan ids are `f"{seq['inventory_number']}{sep}{page_id}"`. Raise `KeyError` with a clear
  message if a sequence lacks `page_ids` or `inventory_number`.

**`bilstm.py`.** `BiLSTMTagger(n_features, hidden_size=64)`, verbatim: `forward(features)`
gives `(T,)` logits; `predict(features) -> list[bool]` (`(sigmoid > 0.5).tolist()`, already
bools in NLP).

## Constraints

- Port behaviour exactly. Step 10b will demand byte-identical NLP outputs, so don't "improve"
  the DP, the loss, the seeding order or the tie-breaking. The only intended changes are the
  ones listed above (bool outputs, `ndtr`, input validation, generalised start states).
- No batching and no end scores; those are possible later steps.
- `import archival_structures.stream_analysis.decoding` must not import torch.
- Never `git commit`; never bump the version; `chmod g+w` new files and the new directory.
- **NLP is read-only in this step.** Don't run NLP scripts.

## Tests

`tests/test_stream_analysis_decoding_semi_markov.py`:
- the 4 ported tests, same names and tolerances: `test_fit_lognormal_duration_recovers_known_parameters`,
  `test_log_duration_pmf_is_normalized_and_peaks_near_mode`,
  `test_decode_semi_markov_recovers_strong_unambiguous_boundaries` (expect
  `[True, False, False, False, True, False, False, False]`),
  `test_decode_semi_markov_prefers_fitted_length_when_evidence_is_weak`;
- `test_decode_empty_and_single` (`[]`, `[True]`);
- `test_max_duration_shorter_than_sequence_forces_starts` (T=10, D=3, strongly negative scores
  → at least 4 starts; no segment longer than 3);
- `test_short_log_pmf_raises`, `test_max_duration_below_one_raises`;
- `test_output_is_bool_and_first_is_start` (random scores, 5 seeds);
- `test_pmf_matches_scipy_norm_reference`: build the reference with `scipy.stats.norm.cdf`
  exactly as the NLP original did and assert `np.array_equal` for (mu, sigma) =
  (1.1900895953077164, 0.9684310847297153) and (1.225606847236485, 0.99154906715885),
  `max_duration=250` (NLP's two fitted priors);
- `test_zero_mass_raises` (mu = 20, max_duration = 5).

`tests/test_stream_analysis_decoding_crf.py` (random parameters via `torch.manual_seed`;
`n_states` in {2, 3}; T in 1..5; `F = 4`):
- `test_viterbi_equals_brute_force`: the path maximising
  `start_scores[y0] + em[0,y0] + Σ_t (transitions[y_{t-1}, y_t] + em[t, y_t])` over all
  `n_states**T` paths;
- `test_partition_equals_brute_force_logsumexp`;
- `test_constrained_partition_full_mask_is_gold_path_score` and `..._empty_mask_is_partition`;
- `test_nll_nonnegative_and_differentiable` (nll ≥ −1e-5; `backward()` runs);
- `test_argmax_emissions_ignores_transitions`;
- `test_start_score_from_emissions_matches_three_state_formula` (`torch.equal` against the
  original expression) and `..._multi_start_states`;
- `test_collapsed_start_f1_matches_string_version` (sklearn on `"start_page"` strings);
- `test_train_crf_is_deterministic`: two runs, same seed, on a synthetic separable task (e.g.
  30 sequences of length 8–20, feature 0 = +2 on starts and −2 elsewhere plus noise, mask
  all True), give identical `state_dict` tensors and best F1; best F1 ≥ 0.9;
- `test_train_crf_respects_mask` (masked-out positions with wrong labels don't change F1);
- `test_decode_core_returns_core_pages_only` and `..._missing_page_ids_raises`;
- `test_bilstm_shapes_and_predict_bools`.

Packaging: the existing two tests in `tests/test_packaging_core_imports.py` still pass with
the new names.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_decoding_semi_markov.py tests/test_stream_analysis_decoding_crf.py tests/test_packaging_core_imports.py`,
then the full suite `.venv/bin/python -m pytest -q -p no:cacheprovider`. Take counts from
`pytest --collect-only -q`.

## Acceptance criteria

1. All new tests pass; the 4 ported semi-Markov tests pass unchanged in name and tolerance.
2. `test_pmf_matches_scipy_norm_reference` passes with `np.array_equal` (exact).
3. **Equivalence with NLP originals (scratch script, not a test).** In the scratchpad, write
   a script run with `<NLP>/venv/bin/python` (it sees AS through the editable install) that
   imports NLP's original modules from `<NLP>/src` and AS's ports, and checks:
   - `decode_semi_markov`: 200 random cases (`np.random.default_rng(0)`, T in 1..300, scores
     N(0, 2), both fitted priors above, `max_duration` 250 and 7): AS output equals
     `[bool(x) for x in nlp_output]` in all 200;
   - `log_duration_pmf`: `np.array_equal` for both priors;
   - `LinearChainCRF`: after the same `torch.manual_seed(s)` (s = 0..4, `n_features=6`,
     `n_states` 2 and 3), `state_dict`s are `torch.equal`; `viterbi`, `nll`, `_partition` equal
     on 20 random sequences;
   - `train_crf`: AS `train_crf(..., n_states=3, start_states=(0,), seed=s)` against NLP
     `train_multitask_crf.train_crf(train, dev, n_features, 3, {0}, s)` on the synthetic task,
     s = 0, 1, 2: identical `state_dict`s and best F1. (Importing that script needs `<NLP>/src`
     and `<NLP>/scripts` on `sys.path`; it runs nothing at import.)
   Report each count as "k/k identical" in the report.
4. `import archival_structures.stream_analysis.decoding` with torch blocked succeeds
   (packaging test).
5. Full AS suite green: **717 passed** before this step, plus your new tests.

The user decides when the step is done.

## Error analysis

- Read the 4 ported tests' original assertions next to yours. Note any assertion you had to
  change (there should be none apart from bool vs int) and why.
- For the `max_duration < T` edge case, print one decoded sequence and its segment lengths,
  and state in the report where the forced starts land and why. `np.argmax` returns the
  first maximum, and the candidate starts `s_range` are in ascending order; read the code and
  say which segment length wins a tie.
- If any equivalence case in criterion 3 fails, stop and record it: read the first failing
  case and classify it (tie-breaking, dtype, seeding order, other).
- Expected findings: the bool-label change (behaviour-change), the new input validation
  (behaviour-change), the scipy core dependency (method), and the equivalence counts (result).

## Do not touch

- Any NLP or LotA file. `stream_analysis/evaluation/`, `stream_analysis/corruption/`,
  `stream_analysis/__init__.py`. `analysis/`, `model/`, `datasets/` (steps 11 and 12 own the
  new modules there).

## When done

1. Add your `### Step 10: sequence decoders in AS` section to
   `reports/phase-04-decoders-and-page-numbering.md`, following `plan/templates/REPORT.md`.
   Add one row per finding to its *Findings* table (ID `F04-10.n`, status `draft`). Don't edit
   other steps' sections or the *Summary*/*Review* sections. Run `make reports` from the AS root
   and fix any warning it prints.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: step 10 done, next step, open questions. Put the final
   public signatures under *Facts later steps need* if they differ from this brief.

```markdown
### Step 10: sequence decoders in AS
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 10](../../reports/phase-04-decoders-and-page-numbering.md#step-10-sequence-decoders-in-as);
  findings F04-10.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
