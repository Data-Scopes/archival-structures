# Phase 03 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-14. This session also planned the phase and orchestrated the
  four Sonnet step sessions.
- **Verdict:** accept with follow-ups.
  - One pre-existing AS defect, the `intra_document_swap` provenance bug (F03-09.5), was fixed
    here with a test, at the user's request; it is effectively a one-line fix.
  - One process incident: step 09 overwrote NLP's stored `trivial_baseline_metrics.csv`
    (F03-09.7). Every deterministic cell is unchanged (F03-R.2); 16 random CI cells hold a new
    draw.
  - No review-fixes brief is needed.

## Checklist

- [x] **Acceptance re-run** (table below). Every logged number reproduced.
- [x] **Suites.**
  - AS `.venv/bin/python -m pytest -q -p no:cacheprovider`: **717 passed** (99 s). That is 696
    after step 09, plus the review's fix tests: 1 start-label test and 20 parametrised
    invariant cases.
  - NLP `tests/test_evaluation.py` + `tests/test_semi_markov.py`: **21 passed**.
  - NLP `tests/test_voc_text_layer.py`: 21 tests fail on import
    (`ModuleNotFoundError: fuzzy_search.similarity`, raised from pagexml-tools'
    `text_helper.py`). That is an environment problem outside this phase's modules (F03-R.3).
  - NLP `tests/` is unchanged against `HEAD`.
- [x] **NLP reproductions re-run by the reviewer.**
  - `verify_as_evaluation_shim.py`: `SUMMARY: 16/16 checks passed -> PASS`.
  - `verify_as_splits.py`: 7/7 `csv identical, manifest identical`, `cache untouched: True`.
  - `run_trivial_baselines.py --out <scratch>` matches the **pre-phase** file (F03-R.2).
- [x] **D3.** The AS tests use the "gaps" default. NLP's shim has `_K_CONVENTION = "pages"`
  (`src/startpage/evaluation.py:30`), and every wrapper (`default_k`, `pk`, `window_diff`,
  `evaluate_all`) defaults to it.
- [x] **P1/P3.** `check_labels` raises TypeError on str/int (tested).
  `git diff --stat tests/test_stream_analysis_corruption_metrics.py` is empty, and the strict
  wrapper delegates (`corruption/metrics.py`).
- [x] **No VOC constants in AS core.**
  `grep -rnE "8834|8023|3891|start_page_position|NL-HaNA|_[0-9]{4}\b|DEFAULT_HELD_OUT" archival_structures/stream_analysis/evaluation/`
  finds nothing.
- [x] **Leakage.**
  - `baselines.train_start_rate` filters `core_rows(...)` and then `split == "train"`, and it is
    tested with starts only in dev and in context rows (rate 0.0).
  - `check_nested_split` is tested (pass, violation, context ignored), and it reports 2,445 /
    2,957 / 5,066 violations on the real splits, re-measured by the reviewer.
  - The oracle is named `every_k_own_rate_ORACLE`, has `is_oracle=True`, and its docstring
    says "not a legitimate predictor".
- [ ] **`output/splits/` and `trivial_baseline_metrics.csv` untouched.**
  - Splits: yes, 14 files hashed by every verify run.
  - The CSV: **no**, it was overwritten by step 09 (F03-09.7). It has been stable since:
    sha256 `8be16b00…`, mtime unchanged across all of the reviewer's runs.
- [x] **Changelog.** It has Step 7 (with 7b), Step 8 and Step 9, each listing NLP's imports.
  The review added an entry for the operator fix.
- [x] **Report** final, `reports/index.md` appended, `make reports` clean.
- [x] **Brief defects** answered below.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 07 | AS ≡ NLP original on 12 CRF result sets | 12/12 | 12 rows × 19 cols, 0 differ, for original and shim (verify script) | yes |
| 07 | metric tests unchanged; 7-case WD fixture | 17 pass; 14/14 | file diff empty; in the 717 | yes |
| 07 | no clipping on CRF data | 0 inventories | 0/22 long-chunk dev, 0/5 held-out test; tightest 3891 (k=3, 12-page run) | yes |
| 07 | k differs only for 2448 | 8 vs 9 | not re-run (07b and the verify A/B depend on it) | — |
| 07b | verify script | 11/11 | 16/16 (09 added 5) | yes |
| 07b | NLP `test_evaluation.py` 17 passed, unchanged | yes | 17 passed; `cmp` identical | yes |
| 07b | default parser error | up to 96 ulp | max 96 ulp (71 of 216 float cells over all float columns) | yes |
| 08 | 7/7 byte-identical, cache untouched | yes | yes | yes |
| 08 | 8023 sole gap; 1529 test 614 / dev 364 / train 220; 238 chunks (3 / 70 / 710) | yes | identical | yes |
| 08 | nesting violations | 2,445 / 2,957 / 5,066 | identical | yes |
| 09 | NLP rerun vs stored, 344 cells | 0 differ | 0 differ (verify script) | yes |
| 09 | rerun vs **pre-phase** stored values | not possible (file lost) | every deterministic cell in the 6 columns printed at planning prints identically (F03-R.2) | yes |
| 09 | oracle vs majority Pk worse/better/tied | 16/3/3 | 16/3/3; 2448 0.198→0.802, 1267 0.338→0.662 | yes |
| 09 | floors under `intra_document_swap` | identical to uncorrupted | after the fix they move (F03-R.1) | revised |
| P.3 | stale `long_sequence_subsequences.csv` | 22,202 vs 22,201 rows | identical; `1539_1048` occurs 2× vs 1× | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (fixed) | `corruption/operators.py` `intra_document_swap` | Provenance entries were tagged but not swapped with their units. `observed_start_labels` was wrong after any swap, and so was `swap_and_insertion`. Corrupted-stream YAMLs written with these operators carry wrong provenance | Fixed in review: entries move with their units. New tests `test_start_label_follows_a_moved_start_page` and `test_provenance_points_at_the_unit_in_its_position` (4 operators × 5 seeds), which fail on the old code (11 failures) and pass now. Changelog entry added |
| 2 | debt | NLP `output/trivial_baseline_metrics.csv` | Overwritten by step 09 (F03-09.7). Deterministic cells verified identical; 16 random cells redrawn | Restore from the user's backup if wanted. Template rule added (`plan/templates/STEP.md`, *Protected outputs*) |
| 3 | observation | `evaluation/baselines.py` `baseline_floors` | With `train_start_rate=None`, every-k and the length prior are rate-oracles, but `is_oracle` stays False; only `rate_from_eval_labels` says so | Documented in the docstring; acceptable |
| 4 | observation | NLP `scripts/verify_as_evaluation_shim.py` | Its trivial-baseline check now compares against the CSV that step 09 rewrote, so it proves original ≡ shim but no longer compares with the pre-phase file | F03-R.2 covers the gap once; nothing to do |
| 5 | debt | NLP venv | `fuzzy_search.similarity` is missing for pagexml-tools; 21 text-layer tests fail on import | Follow-up for NLP / phase 05 (text layer) |

## Error analysis read by the reviewer

- **Corrupted stream with the fixed operator** (step 09's demo: 60 units, 10 documents,
  `Random(0)`, rate 0.5). 12 units move. The true starts are now at
  `[0, 6, 12, 18, 24, 31, 36, 42, 48, 59]`, which follows `s30` to position 31 and `s54` to 59.
  Every-k and the oracle fall from F1 1.0 to 0.8 (Pk 0.105). Majority Pk changes from 0.4737 to
  0.4386, because the reference itself changed. Step 09's conclusion "floors identical" was an
  artefact of the bug (F03-R.1).
- **Oracle against majority, per inventory** (long-chunk dev, all 22): re-computed with AS code,
  giving 16 worse / 3 better / 3 tied. 2448 and 1267 were read individually. The step's reading
  holds: few starts make the oracle's fixed period place spurious boundaries.
- **Clipping, per inventory and run** (not pooled): 0 clipped runs. The margins are 8–10 gaps on
  long-chunk dev (3891, 8697, 8023) and over 600 on held-out test.
- **Pre-phase CSV cells** (F03-R.2): the 12 × 6 table printed at planning time was diffed as text
  against the rerun, formatted identically. Only 3 lines differ, all in `f1_macro_ci_lo` of the
  seed-less every-k/oracle rows. Row 7's random cell happens to equal the old draw.
- **Stale subsequence CSV** (F03-P.3): confirmed. It has one extra `1539_1048` row, the
  duplicate retired on 2026-09-30.

## Brief quality

- **07, 07b "read 10 clipped runs":** there are none. That is a valid negative result, and both
  steps reported the distribution instead. No change needed; a brief should hedge "if any".
- **07b round_trip:** the brief didn't anticipate pandas' inexact parser. It was propagated to
  brief 09 before 09 ran (rule 10). It is now a fact in `STATUS.md`.
- **09 overwrite:** brief 09 forbade overwriting the CSV but didn't say that the 07b verify
  script reassigns `RESULTS_CSV_PATH` and calls `main()`. Fixed in the template (*Protected
  outputs*: hash before and after; explicit scratch paths; resolve patched defaults at call time).
- **09 corrupted-stream expectation:** the brief's expectation (labels follow the canonical
  start) was right, and the code was wrong. The step reported the gap rather than rationalising
  it, which is how the bug was found.
- **Parallel 07b/08:** 07b used a scripted read-modify-write on the report. The reviewer checked
  that all of 08's rows and sections survived, even though its prompt asked for Edit. No
  template change: with parallel steps, the orchestrator checks shared files afterwards.

## Updates applied

- [x] Phase report `reports/phase-03-evaluation-for-nlp.md`: *Summary*, *Review* and
      *Follow-ups* written; every finding is `confirmed`, `revised` or `withdrawn`; status
      *final*
- [x] `reports/index.md`: the phase's findings appended (with Edit), and its phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief not needed (the operator fix was done in review, with tests)
- [x] `docs/shared_library/changelog.md` complete for this phase (plus the review-fix entry)
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] Release recommendation: **v0.7.0** (minor: new public `stream_analysis.evaluation`
      package; behaviour change in `corruption.metrics`; operator bug fix)
