# Phase 11 progress

Append-only, one block per session. Steps 18a–18c run in parallel and each replaces only its own placeholder line.

### Phase planning
- **Session:** Opus 5.5, 2026-10-27. **Status:** done
- **Trigger:** UA-031 (a): plan phase 11 while phase-12 step 15y waits for the full v4 run's files.
- **Read:** `plan/README.md`, `plan/PLAN.md`, `plan/CONTEXT.md`, inventory C10/C12/C18 and §4 row 18,
  `docs/from-nlp-start-page-detection.md` §1, UA-014, `docs/cross-project-review.md` §0 and §1; AS `text_extent.py`,
  `subsequence_detection.py`, `model/scan_id.py`, `evaluation/splits.py`; NLP code via one Explore agent.
- **Checked by running:** NLP's interpreter with `PYTHONPATH=<AS>` imports the AS working tree (pagexml-tools 0.9.0
  in both venvs); END target = `end_last` (1,379), early_end → END P 0.13978 / R 0.31545 from `page_table.csv`; the 8
  bottom-gap bins of `supp_end_rate_by_bot_bin.csv` re-computed exactly through AS `estimate_stable_margins`; NLP's
  `calibrate_length_penalty_weight` = 0.0293559853524414 (23.5 s) and its DP/TextTiling reproduce
  `unsupervised_changepoints.csv` 41,810/41,810; NLP's separator detector hits 169 of 20,912 page texts; AS
  `ScanId` round-trips all 20,912 NLP scannames. AS suite 1241 passed, 11 skipped.
- **Decisions:** P1–P10 in `PLAN.md` (P2 C10 keeps NLP's start-index convention plus converters; P3 no NLP edits,
  NLP shims wait for UA-032; P9 parallel steps with placeholders).
- **Findings:** none (planning facts are in the briefs).
- **User actions:** UA-031 resolved; new UA-032 (NLP editable AS install), UA-033 (`make docs`).

### Step 18a: text_extent region types and margin gaps
- **Session:** Sonnet 5.5, 2026-10-27. **Status:** done
- **Files changed:** `archival_structures/analysis/text_extent.py`, `tests/test_analysis_text_extent.py`, `reports/data/phase-11-text-extent-and-nlp-ports/step-18a/` (`recheck_report84.py`, `error_analysis.py`, `recheck.tsv`, `bins.tsv`, `extents_all.tsv.gz`, `extents_body.tsv.gz`, `class_*.tsv.gz`, `ea_*.tsv`, `run.log`), `docs/shared_library/changelog.md`, report, this file, STATUS row
- **Public API:** `line_region_type(line) -> Optional[str]`; `BODY_REGION_TYPES_VOC`; `compute_page_text_extent(page, equal_tol=0.02, region_types=None)`; `compute_corpus_text_extents(pages, region_types=None, equal_tol=0.02)`; `margin_gaps(row, stable) -> dict`; `text_extent_table(df, stable=None, top_tol=0.05, bot_tol=0.05) -> DataFrame`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1300 passed, 11 skipped (text_extent file: 31, 9 new)
- **Acceptance:** extents 20,912/20,912 matched, max diff 1.1e-16; classes 19,836/19,836; all-lines 3,112 / P 0.1397814910025707 / R 0.3154459753444525; body 2,762 / 0.17052860246198406 / 0.34155184916606235; bins 8/8; all exact
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18a](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18a-text_extent-region-types-and-margin-gaps); findings F11-18a.1–4
- **Error analysis:** catch-word sets the lowest line on 74% of 5,221 class-changing pages; largest-gap non-END pages are single-line page numbers
- **Deviations from brief:** added `error_analysis.py` and per-mode `class_*.tsv.gz` outputs; ran with NLP's interpreter via coreshare (1 worker granted)
- **Brief defects:** NLP `page_extents*.csv` include the 1,076 pages with `n_lines == 0` (brief said absent); script emits `n_lines=0` rows for them
- **Open questions for the user:** none

### Step 18b: C10, DP and TextTiling segmentation
- **Session:** Sonnet 5.5, 2026-10-27. **Status:** done
- **Files changed:** `archival_structures/stream_analysis/overview/subsequence_detection.py`, `archival_structures/stream_analysis/__init__.py`, `tests/test_subsequence_detection_unsupervised.py`, `reports/data/phase-11-text-extent-and-nlp-ports/step-18b/` (script, `compare.tsv`, `run.log`, `weight_sensitivity.tsv`, `segment_lengths.tsv`, `dp_starts_2555.tsv`), `docs/shared_library/changelog.md`, report, this file, STATUS row
- **Public API:** `segment_dp(similarities, prior_mean_length=4.0, length_penalty_weight=1.0, max_segment_length=200) -> list[int]`; `calibrate_length_penalty_weight(similarities_by_inventory, prior_mean_length=4.0, target_mean_segment_length=4.0, candidates=...) -> float`; `textiling_depth_scores(similarities, window=3, depth_threshold=None) -> (ndarray, list[int])`; `predicted_start_rate(boundaries, n_pages)`; `starts_to_boundaries(starts)`; `boundaries_to_starts(boundaries)`; `MAX_SEGMENT_LENGTH`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1300 passed, 11 skipped (new file: 23 passed)
- **Acceptance:** weight 0.0293559853524414 both; segment_dp 0/44, textiling 0/22, start rate 0/22 mismatches; 41,810/41,810 rows; diff to existing functions is additions only
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18b](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18b-c10-dp-and-texttiling-segmentation); findings F11-18b.1–4
- **Error analysis:** DP over-segments (2555: 510 starts vs 128 labelled, median segment 1); sampled starts are separator sheets, dated entry heads, empty pages. Weight is fragile above 0.25.
- **Deviations from brief:** none
- **Brief defects:** `page_text.csv.gz` read with pandas (macOS `zcat` fails on it); tests for TextTiling use zero-baseline input because zero-padded convolution creates edge starts on constant non-zero input (verbatim NLP quirk).
- **Open questions for the user:** none

### Step 18c: C12, separator-sheet rule
- **Session:** Sonnet 5.5, 2026-10-27. **Status:** done
- **Files changed:** `archival_structures/analysis/separator_sheets.py`, `tests/test_analysis_separator_sheets.py`, `reports/data/phase-11-text-extent-and-nlp-ports/step-18c/` (compare_nlp.py, compare.tsv, sheets.tsv, run.log), `docs/shared_library/changelog.md`, `reports/phase-11-text-extent-and-nlp-ports.md`
- **Public API:** `is_separator_sheet(text, *, pattern, max_chars)`, `detect_sheets(page_texts, *, pattern, max_chars, inventory_col, page_col, text_col)`, `next_scan_id(page_id)`, `apply_rule(df, sheets, convention, threshold=0.5, *, next_page_id, inventory_col, page_col, prob_col)`, `VOC_SHEET_PATTERN`, `VOC_MAX_SHEET_CHARS`, `SHEET_CONVENTIONS`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_separator_sheets.py` → 27 passed. Full suite: 1299 passed, 11 skipped, 1 failed (`tests/test_analysis_text_extent.py::TestRegionTypes::test_filtered_margins`, step 18a work in progress, not mine)
- **Acceptance:** 169 sheets; is_separator_sheet, detect_sheets, apply_rule (4 conventions x 3 thresholds), next_scan_id: 0 mismatches over 20,912 pages; tests green
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18c](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18c-c12-separator-sheet-rule); findings F11-18c.1–3
- **Error analysis:** 169 hits all clear `No`/`N:o` + number (0 suffix, 0 odd). 19 near misses: 5 likely missed (OCR residue), 1 ambiguous, 4 header+number, 9 correctly rejected.
- **Deviations from brief:** none
- **Brief defects:** none
- **Open questions for the user:** none

### Step 18d: C18 docs, leakage and splits, granularity hypothesis
- **Session:** Sonnet 5.5, 2026-10-27. **Status:** done
- **Files changed:** `docs/challenges/leakage_and_splits.md` (new), `docs/challenges/index.md`, `docs/challenges/document_segmentation.md`, `docs/stream_analysis.md`, `reports/phase-11-text-extent-and-nlp-ports.md`
- **Checks:** one `.venv/bin/python -c "from ... import ..."` over all API names written in the docs -> exit 0; `make reports` -> clean, no warnings
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18d](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18d-c18-docs-leakage-and-splits-granularity-hypothesis); finding F11-18d.1
- **Deviations from brief:** none
- **Brief defects:** none
- **Open questions for the user:** none


### Phase review
- **Session:** Opus 5.5, 2026-10-27. **Status:** done; verdict accept with follow-ups ([REVIEW](REVIEW.md))
- **Re-run:** 18a `recheck_report84.py` from scratch → `recheck.tsv`, `bins.tsv` identical; 18b, 18c compare scripts →
  0 mismatches; AS suite 1301 passed, 11 skipped; NLP 66 passed, no NLP file changed.
- **Fixed in review:** `apply_rule` sorted sheet order + test (F11-R.3, changelog line); leakage page link outside
  the Sphinx tree; hypothesis-section wording (counts, not "mostly"); TextTiling edge note in `stream_analysis.md`;
  dated *Done* row in the inventory's C10 entry (F11-18d.1).
- **Reviewer findings:** F11-R.1 (catch-word bottom = anti-END cue: END 0.025/0.113 vs 0.188/0.278), F11-R.2
  (TextTiling edge starts, 21 of 22 inventories), F11-R.3. Revised: F11-18a.2, F11-18a.4, F11-18d.1.
- **Updated:** report final, `reports/index.md`, `plan/PLAN.md`, `plan/CONTEXT.md`, phase `PLAN.md` hand-off,
  `docs/from-nlp-start-page-detection.md` (AS status).
- **Release:** v0.16.0 recommended (additive API in three modules; one empty-frame fix).
