# Phase 03: Evaluation for NLP

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> not read this file. Seeded 2026-10-14 from `docs/shared_library/upstream_inventory.md`;
> planned 2026-10-14 (Opus).

## Goal

AS gets one segmentation-evaluation package, `stream_analysis/evaluation/`. It is built from
NLP's `evaluation.py` (C6) and merged with AS's `corruption/metrics.py`, and it adds generic
chunk splits (C7) and trivial baselines (C8). NLP imports these through shims, and
**reproduces its stored numbers exactly**.

## Owns

- **AS:**
  - `stream_analysis/evaluation/` (new: `__init__.py`, `segmentation.py`, `aggregate.py`,
    `splits.py`, `baselines.py`);
  - `stream_analysis/corruption/metrics.py` and `corruption/__init__.py`;
  - `docs/challenges/simulation_harness.md` §5;
  - their tests, plus one line in `tests/test_packaging_core_imports.py`.
- **NLP:**
  - `src/startpage/evaluation.py`, `long_subsequence_chunk_split.py`, `splits.py`;
  - `scripts/run_trivial_baselines.py`;
  - new `scripts/verify_as_evaluation_shim.py` and `scripts/verify_as_splits.py`;
  - new `output/as-phase-03-originals/`.

Phase 02 owns `parsers/` and LotA; there is no overlap.

## Sources

The planning session read these sections, and no others.

- `upstream_inventory.md`: glossary L183–203; C6 L270–283; C7 L285–296; C8 L298–309; §4 steps
  7–9 L451–453; §6 D3 and D9 at L505–508 and L532–533.
- NLP `CLAUDE.md`, for the leakage and split rules. NLP's tests are **pytest-only**: there is no
  pytest config and no `__main__` runner, contrary to what the seed assumed.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 07 | C6 suite in AS; `corruption/metrics` wraps it | — | 16 NLP tests ported; AS's 17 metric tests **unchanged**; 7-case shared WD fixture; AS `evaluate_all(k_convention="pages")` deep-equal to NLP's original on 12 CRF result sets | [step-07](step-07-evaluation-suite.md) |
| 07b | NLP `evaluation.py` → shim; reproduction script | 07 | `verify_as_evaluation_shim.py` exit 0: CRF 12 rows exact; trivial baselines 344/360 cells exact (16 excluded, P7); A/B with fixed seed deep-equal; NLP `test_evaluation.py` 17 passed, unchanged | [step-07b](step-07b-nlp-evaluation-shim.md) |
| 08 | C7 splits, held-out builder, validators, `check_nested_split` | 07 (package) | new synthetic tests; `verify_as_splits.py`: 7/7 cached split CSVs byte-identical, cache untouched | [step-08](step-08-splits.md) |
| 09 | C8 baselines (oracle labelled, D9); harness floors | 07, 07b, 08 | new tests; NLP rerun matches 344/360 cells (majority Pk = WD = 0.30327993644426438) | [step-09](step-09-trivial-baselines.md) |

All four briefs were written at planning time; 07's API is pinned in its brief, so 08 and 09
don't wait on an as-built check. 07b and 08 may run in parallel; they share only the changelog
and the report.

## Why this order

07 fixes the label and k conventions (bool labels, `k_convention`) that 07b, 08 and 09 use.
07b proves the conventions on NLP's real outputs before anything else moves. 09 comes last
because it needs the 07b shim (it scores through it) and 08's `core_rows`/`eval_scan_ids`.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| D3 | AS default `k = round((n−1)/(2S))`; NLP passes its own `round((n/S)/2)` explicitly | `default_k(labels, convention="gaps"\|"pages")`; `pk`, `window_diff`, `evaluate_all` take `k=None, *, k_convention="gaps"`. NLP's shim passes `"pages"` | NLP overrides AS's generic default; per-inventory k makes an int `k` insufficient, so the override is a convention name |
| D3b | NaN for n<2, raise on length mismatch, clip k with a warning | Clipping keeps NLP's values and emits one `WindowClippedWarning` per call | Adopted with D3 |
| P1 | Canonical labels are `list[bool]` "is start"; NLP adapts strings | `check_labels` raises TypeError on non-bool (strings, ints) | Bool labels give NLP all-zero gaps today, and strings are all truthy in AS |
| D9 | The oracle baseline moves into AS | `oracle_every_k_own_rate`, method `every_k_own_rate_ORACLE`, `is_oracle=True` | It is not a legitimate predictor |
| P2 | Split code stays reproducible | Keep the 10-slot striding and greedy rebalance; no opt-in corrected strategy this phase | Every NLP number depends on the cached splits |
| P3 | `corruption.metrics.window_diff` keeps its strict contract (raise on n<2 and k>gaps) and delegates the computation | AS's 17 metric tests unchanged | User, 2026-10-14: backwards compatible |
| P4 | NLP `tests/test_evaluation.py` stays unchanged and runs against the shim | All 17 pass through the shim; the bool ports live in AS | User, 2026-10-14: the strongest check that the adapter is faithful; trimming is a follow-up |
| P5 | Trivial-baseline CSV: 16 cells (CI columns of the 4 every-k/oracle rows, bootstrapped with `seed=None`) are excluded; NLP's seeding is unchanged | Acceptance is exact equality on the other 344 cells, plus an A/B with a fixed seed | User, 2026-10-14 |
| P6 | Context-row guard (hazard 3) | `build_eval_frame` raises on duplicate scan ids; `splits.core_rows` / `eval_scan_ids` / `check_context_not_scored` | `evaluate_all` sees no frame with `role`, so a duplicate is the detectable symptom |
| P7 | NLP's VOC end-id grammar (scan number − 1) is replaced by physical adjacency (`pages_from_starts`) | The byte check proves equality | Planning check: both rules give the same 1,135 end pages in all 22 inventories |
| P8 | The C6 step is split into 07 (AS) and 07b (NLP shim and reproduction) | — | One brief would exceed the budget; this also isolates AS from the NLP edits |

## Hazards

1. **Inputs are local** (checked 2026-10-14): `output/trivial_baseline_metrics.csv`,
   `output/splits/` (7 caches), `output/start_page_position.csv`, `annotations/` (22 CSVs),
   and `output/crf_sequence_{decoded,metrics}_scorefix.csv`.
   - At planning time, a rebuild with the current code reproduced all 7 split CSVs
     byte-identically.
   - Rescoring the CRF decodes reproduced all 12 metric rows exactly.
2. **Untracked NLP files:** `tests/`, `scripts/run_trivial_baselines.py`, `output/splits/` and
   the output CSVs are not in git. 07b copies the originals to `output/as-phase-03-originals/`
   before editing. The split cache is keyed by `(kind, seed)` only: `long_chunk_seed1` holds
   W=1. A careless `load_or_build_split(..., force=True)` overwrites the cache, so every rebuild
   goes to a scratch `SPLITS_DIR`.
3. **NLP pytest** fails for the `claude-agent` account (`No module named 'pygments'`). The
   user's pygments sits in their own user site-packages, which the agent account doesn't see.
   Fix before 07b (see `STATUS.md`).
4. `test_page_level_prf_reproduces_existing_knn_numbers` needs a 60 MB `.npz` and torch
   training, and stays in NLP. Torch 2.12.1 imports in NLP's venv.
5. C8: the length prior needs seed-for-seed equality with `np.random.default_rng`, seeds 42–44,
   with one rng shared across inventories in `full_sequences` order.
6. **Leakage** (`CLAUDE.md`): `check_nested_split` encodes "a component's training split nests
   inside the downstream split". The review checks that `train_start_rate` is fitted on train
   core rows only.
7. NLP's `startpage` is not installed (scripts use `sys.path` inserts). NLP's venv sees AS
   through the Homebrew editable install, with stale metadata (0.1.0). Shims work regardless.
   The phase-01 precondition "install NLP and AS editable into `venv/`" stays open.

## Open questions for planning (settled 2026-10-14)

- **Reproduction target for `evaluate_all`:** `crf_sequence_decoded_scorefix.csv` →
  `crf_sequence_metrics_scorefix.csv` (12 rows, including CIs; ~2 s on CPU), plus the trivial
  baselines.
- **Is NLP's `pyproject.toml` installed?** No. It doesn't block, see hazard 7.
- **Do the C6–C8 line numbers still hold?** Yes. The modules match the 2026-10-13 numbers;
  the briefs carry anchors.
- **How do NLP tests run?** `venv/bin/python -m pytest -q -p no:cacheprovider tests/…`, after
  the pygments fix. The shim acceptance runs `venv/bin/python scripts/verify_as_evaluation_shim.py`.

## Review checklist

- [ ] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, re-run by the reviewer.
- [ ] AS suite and NLP tests green.
- [ ] Reproductions re-run by the reviewer: both verify scripts exit 0; baseline cells diffed.
- [ ] D3: AS tests use the "gaps" default; the NLP shim passes `"pages"` everywhere
      (`grep -n k_convention src/startpage/evaluation.py`).
- [ ] String labels raise in AS (P1); `corruption.metrics` tests untouched (P3).
- [ ] No VOC constants in AS core (held-out set, coverage gaps, scan-name grammar).
- [ ] Leakage: `train_start_rate` on train core rows only; `check_nested_split` tested.
- [ ] `output/splits/` and `trivial_baseline_metrics.csv` untouched (mtimes).
- [ ] `changelog.md` has steps 7–9, with NLP imports listed.
- [ ] `reports/phase-03-evaluation-for-nlp.md` final (one section per step with its error
      analysis; findings confirmed); findings appended to `reports/index.md`; `make reports`
      clean.
- [ ] `Brief defects` entries answered.

## Hand-off

Reviewed 2026-10-14: accept with follow-ups ([REVIEW](REVIEW.md)). For phase 04 (decoders, C9):

- **Score through `archival_structures.stream_analysis.evaluation`.**
  - Labels are `list[bool]`.
  - NLP numbers need `k_convention="pages"`; NLP's shim already passes it.
  - Read stored metric CSVs with `float_precision="round_trip"`.
- **Splits.**
  - Decoders consume `long_chunk` sub-sequences. Use `core_rows` / `eval_scan_ids`, and assert
    `check_context_not_scored`.
  - `output/long_sequence_subsequences.csv` is stale (F03-P.3); use `load_or_build_split`.
  - A component trained under one split must not be scored under another; call
    `check_nested_split` (F03-08.5).
- **Floors.** Report `baseline_suite` / `baseline_floors` next to every decoder number. Majority
  has the lowest Pk (F03-09.2).
- **Reproduction pattern** that worked:
  - copy the originals first;
  - write a verify script that runs the original against the new code, plus an A/B with a
    fixed seed;
  - hash the protected outputs before and after (now in `plan/templates/STEP.md`).
- **Environment.** NLP's text-layer tests fail on a `fuzzy_search` import (F03-R.3). Phase 05
  should fix that first.
