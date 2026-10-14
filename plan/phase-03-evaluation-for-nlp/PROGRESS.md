# Phase 03 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Seeded
- **Session:** Opus, 2026-10-14 (the session that set up `plan/`)
- `PLAN.md` seeded from `upstream_inventory.md` C6–C8, §4 and §6. No sibling code read.

### Phase planning
- **Session:** Opus, 2026-10-14. No code changed.
- **Read:**
  - the sources listed in `PLAN.md`, and NLP `CLAUDE.md`;
  - AS `corruption/metrics.py`, its tests, `stream_analysis/__init__.py` and the packaging
    test, directly;
  - NLP evaluation, splits and baselines through two Explore subagents (signatures, line
    ranges, tests, importers, stored outputs).
- **Briefs written:** 07, 07b (new: 07 split into its AS and NLP halves), 08, 09.
- **Open questions settled:**
  - the `evaluate_all` target is the CRF scorefix decode → metrics (12 rows, including CIs);
  - NLP is not installed (it uses `sys.path`), which doesn't block;
  - the C6–C8 line numbers still hold;
  - NLP tests are pytest-only.
- **User decisions:**
  - the corruption `window_diff` wrapper stays strict (P3);
  - NLP `test_evaluation.py` is kept unchanged and runs against the shim (P4);
  - the 16 unseeded CI cells are excluded, and NLP's seeding is unchanged (P5);
  - pygments: diagnosed (it is in the user's site-packages, which the agent account can't
    see); the fix is in `STATUS.md`.
- **Planning decisions:**
  - `k_convention="gaps"|"pages"` implements D3; a per-inventory k rules out an int override;
  - the duplicate-scan-id guard handles context rows (P6);
  - adjacency replaces the VOC end-id grammar (P7).
- **Checked by prototype (scratchpad, read-only on NLP):**
  - Shared WindowDiff fixture: 7 cases give equal values in the AS and NLP originals.
  - `default_k`: "gaps" and "pages" differ only at n=6, S=1 (2 vs 3) among the 4 cases.
  - The current NLP code rebuilds all 7 cached split CSVs byte-identically. The manifests are
    equal apart from `git_head`, and the cache sha256 and mtimes are unchanged.
  - The VOC end-id rule and adjacency give the same 1,135 ends in all 22 inventories.
  - P2 toy values: K=10 on 10×10 pages gives `[train×3, test, dev, train×4, test]`, at 70/10/20.
  - The CRF rescoring reproduces 12/12 rows (subagent check). The trivial baselines reproduce
    except for the 16 unseeded CI cells.
- **Baselines measured:** AS suite 618 passed (100 s).
- **Planning findings:** F03-P.1–5, in the report's *Planning* section.
- **Next:** step 07 (Sonnet).

### Step 07: C6 evaluation suite in AS; `corruption/metrics` becomes a wrapper
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/stream_analysis/evaluation/{__init__,segmentation,aggregate}.py` (new);
  `stream_analysis/corruption/metrics.py`; `tests/test_stream_analysis_evaluation_{segmentation,aggregate}.py` (new);
  `tests/test_packaging_core_imports.py`; `docs/shared_library/changelog.md`; `reports/phase-03-evaluation-for-nlp.md`
- **Public API:** `evaluation.segmentation`: `check_labels`, `WindowClippedWarning`, `page_level_prf`,
  `page_level_counts`, `page_level_prf_with_counts`, `iter_contiguous_runs`, `tolerance_prf`, `tolerance_prf_runs`,
  `default_k(labels, convention="gaps")`, `pk`/`window_diff(..., *, k_convention="gaps")`, `segment_exact_match`.
  `evaluation.aggregate`: `build_eval_frame(y_true, y_pred, scan_ids, sequences, sep="/")`, `PerInventoryResult`,
  `per_inventory`, `start_rate_calibration`, `bootstrap_ci`, `bootstrap_pooled_f1`,
  `evaluate_all(..., *, k=None, k_convention="gaps", sep="/")`, `format_report`.
- **Tests:** 4 targeted files: 69 passed; full AS suite: 666 passed (618 + 48).
- **Acceptance:** 16 ported tests pass; 17 corruption tests pass unchanged; 7/7 WD cases via both wrappers;
  packaging passes; NLP equivalence 12/12 deep-equal; suite green.
- **Report:** [`reports/phase-03-evaluation-for-nlp.md` § Step 07](../../reports/phase-03-evaluation-for-nlp.md);
  findings F03-07.1-5
- **Error analysis:** no window is clipped on any CRF group (min run 12 / 642); default k differs only for
  inventory 2448 (8 vs 9); "gaps" k lowers long-chunk macro Pk/WD by 0.002-0.004, held-out unchanged.
- **Deviations from brief:** none
- **Brief defects:** the brief asked to read 10 clipped runs, but none exist; `scanname`->`page_id` means the
  07b shim must rename the column back for NLP's tests if they use it.
- **Open questions for the user:** none

### Step 08: C7 generic splits, held-out builder, public validators, `check_nested_split`
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/stream_analysis/evaluation/{splits,__init__}.py`;
  `tests/test_stream_analysis_evaluation_splits.py` (new); `docs/shared_library/changelog.md`;
  `reports/phase-03-evaluation-for-nlp.md`; NLP `src/startpage/{long_subsequence_chunk_split,splits}.py`,
  `scripts/verify_as_splits.py` (new)
- **Public API:** `evaluation.splits` (also re-exported): `Page`, `Document`, `Chunk`, `SubSequence`,
  `LongSequenceSplit`, `parse_documents`, `partition_file_into_chunks`, `assign_splits_and_pad_context`,
  `build_archival_dataset_splits`, `build_subsequences`, `validate_subsequences`, `pages_from_starts`,
  `build_long_sequence_split`, `build_inventory_held_out`, `core_rows(split_df)`,
  `eval_scan_ids(split_df, split, sep="/")`, `SplitValidationError`, `check_no_duplicate_page_ids`,
  `check_no_document_crosses_partitions`, `check_context_not_scored`, `check_split_coverage`,
  `check_nested_split`.
- **Tests:** new file 19 passed; full AS suite 685 passed (666 + 19), 75 s.
- **Acceptance:** verify script 7/7 identical before and after the NLP edit, cache untouched both times;
  grep for VOC constants empty; NLP `long_chunk` shape (22201, 7) from cache.
- **Report:** [`reports/phase-03-evaluation-for-nlp.md` § Step 08](../../reports/phase-03-evaluation-for-nlp.md);
  findings F03-08.1-5
- **Error analysis:** only 8023 lacks a partition (5 chunks, no test slot); 49 of 238 chunks are oversized
  single documents (max 710 pages); long_chunk and held-out splits are not nested (2,445 / 5,066 violations).
- **Deviations from brief:** none
- **Brief defects:** none
- **Open questions for the user:** none

### Step 07b: NLP `evaluation.py` becomes a shim over AS; stored numbers reproduced
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** NLP `src/startpage/evaluation.py` (shim), `scripts/verify_as_evaluation_shim.py` (new),
  `output/as-phase-03-originals/{evaluation.py,test_evaluation.py,run_trivial_baselines.py}` (new);
  AS `docs/shared_library/changelog.md`, `reports/phase-03-evaluation-for-nlp.md`
- **Public API:** NLP `startpage.evaluation` keeps all 17 names; labels str or bool; `pk`/`window_diff`/
  `default_k` default to the "pages" k; `build_eval_frame` keeps `scanname`. No AS change.
- **Tests:** NLP `tests/test_evaluation.py` 17 passed (file unchanged); AS suite 685 passed (includes step 08 tests)
- **Acceptance:** verify script 11/11 OK: CRF 12 x 19 cells exact (original and shim); trivial baselines
  344 cells exact + 16 excluded (both); A/B 4/4 deep-equal; 14 split files unchanged.
- **Report:** [`reports/phase-03-evaluation-for-nlp.md` § Step 07b](../../reports/phase-03-evaluation-for-nlp.md#step-07b-nlp-evaluationpy-becomes-a-shim-over-as-stored-numbers-reproduced);
  findings F03-07b.1-5
- **Error analysis:** labels are only the two expected values; 0 clipping warnings; "gaps" k would move
  long-chunk seed-0 macro Pk by -0.0017 and WD by -0.0017, held-out unchanged.
- **Deviations from brief:** none
- **Brief defects:** stored CSVs must be read with `float_precision="round_trip"` (default parser is off by
  up to 96 ulp); the brief expected clipping warnings to read, but none occur.
- **Open questions for the user:** none

### Step 09: C8 trivial baselines (oracle clearly labelled); the corruption harness gets floors
- **Session:** Sonnet, 2026-10-14. **Status:** done (with one open issue, see below)
- **Files changed:** AS `evaluation/baselines.py` (new), `evaluation/aggregate.py`, `evaluation/__init__.py`,
  `corruption/metrics.py`, `corruption/__init__.py`, `tests/test_stream_analysis_evaluation_baselines.py` (new),
  `docs/challenges/simulation_harness.md`, `docs/shared_library/changelog.md`, report; NLP
  `scripts/run_trivial_baselines.py`, `scripts/verify_as_evaluation_shim.py`
- **Public API:** `majority`, `every_k`, `length_prior`, `oracle_every_k_own_rate`, `train_start_rate`,
  `BaselineRun`, `baseline_suite`, `baseline_floors`, `flatten_metrics`, `start_classification_floors`
- **Tests:** new file 11 passed; full AS suite 696 passed (685 + 11), 101 s
- **Acceptance:** NLP rerun 344/344 comparable cells equal; verify script `SUMMARY: 16/16 checks passed -> PASS`;
  `verify_as_splits.py` OK; splits sha256 unchanged; **stored CSV mtime changed** (F03-09.7)
- **Report:** [`reports/phase-03-evaluation-for-nlp.md` § Step 09](../../reports/phase-03-evaluation-for-nlp.md#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors);
  findings F03-09.1-7
- **Error analysis:** majority Pk beats the oracle (0.303 vs 0.515); oracle worse on 16/22 inventories.
  `intra_document_swap` does not permute provenance, so floors do not move under it.
- **Deviations from brief:** `main(out_path=None)` resolves `RESULTS_CSV_PATH` at call time (see defects)
- **Brief defects:** `--out` default bound at definition time clobbered the stored CSV when
  `verify_as_evaluation_shim.py` (which reassigns `RESULTS_CSV_PATH`) ran; the verify script's `check_ab`
  used helpers the brief removes, so it now loads them from `output/as-phase-03-originals/`.
  Brief's "floors move because labels follow canonical start" does not hold for `intra_document_swap` (bug).
- **Open questions for the user:** (1) stored `trivial_baseline_metrics.csv` was overwritten, restore from backup?
  (2) fix `intra_document_swap` provenance?

### Phase review
- **Session:** Opus, 2026-10-14 (the same session planned the phase and ran the Sonnet steps
  07, then 07b ∥ 08, then 09).
- **Verdict:** accept with follow-ups ([REVIEW.md](REVIEW.md)). No review-fixes brief.
- **Fixed in review (user request):** `corruption.operators.intra_document_swap` now moves the
  provenance entries with their units. The tests `test_start_label_follows_a_moved_start_page`
  and `test_provenance_points_at_the_unit_in_its_position` (4 operators × 5 seeds) give 11
  failures on the old code and pass now. Changelog entry added.
- **Re-run:** AS 717 passed. NLP `test_evaluation` + `test_semi_markov` 21 passed.
  `verify_as_evaluation_shim.py` 16/16, `verify_as_splits.py` 7/7. The baseline rerun went to
  scratch, and the stored CSV hash was stable.
- **Own error analysis:**
  - the rerun against the pre-phase CSV values printed at planning: every deterministic cell is
    identical (F03-R.2);
  - the fixed-operator demo: the floors move (F03-R.1);
  - oracle against majority, 16/3/3;
  - per-inventory clipping: 0 runs (F03-R.5);
  - the 08 split facts and F03-P.3, re-measured.
- **Findings:** 32 in total (30 confirmed, 2 revised: F03-P.4 and F03-09.5), appended to
  `reports/index.md`. Report final; `make reports` clean.
- **Template:** `plan/templates/STEP.md` gained a *Protected outputs* rule (F03-R.4).
- **Release:** v0.7.0 recommended. Next: plan phase 04.
