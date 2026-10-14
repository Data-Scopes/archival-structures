# Phase 04 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Phase planning
- **Session:** Opus, 2026-10-14. No code changed. Phase directory created from
  `plan/templates/`; `PLAN.md` seeded from `upstream_inventory.md` C9, C11, C13 and planned
  in the same session.
- **Read:** the sources in `PLAN.md`; NLP decoders and alignment, and LotA adapters, through
  three Explore subagents (plus one follow-up for the training loops); AS `pyproject.toml`,
  the packaging test, `evaluation/__init__`, `datasets/__init__`, the changelog head.
- **Briefs written:** 10, 10b, 11, 12, 12b (the inventory's 10 and 12 split into AS and NLP
  halves, as in phase 03).
- **User decisions:**
  - scipy becomes a core dependency (P1);
  - `train_crf` moves to AS and both NLP copies delegate (P2);
  - C13 covers AS and the LotA adapters only (P5);
  - 8 torch threads (P9).
- **Planning decisions:** bool outputs (P3); port the quirks exactly (P4); the `ScanId` grammar
  and LotA's `None` behaviour (P6); `AlignmentConfig` has exactly 4 fields (P7); the
  reproduction keeps the mined anchors (P8).
- **Checked by prototype** (scratchpad; sibling outputs hashed before and after, unchanged):
  - semi-Markov script 4/4 CSVs byte-identical (75.6 s);
  - `start_only` 6/6 rows equal (79.4 s);
  - alignment build 9/9 CSVs byte-identical (manifest step fails outside the repo cwd);
  - `ndtr` ≡ `norm.cdf`; erfc drift ≤6e-13;
  - `InventoryNumber` order ≡ NLP sort keys on 6,914 inventories;
  - GT 21,623 rows all match the grammar;
  - LotA adapters 11 passed, suite 316 passed (excl. `test_derive_voc`, fuzzy_search);
    AS 717 passed.
- **Findings:** F04-P.1–P.7 in the report's *Planning* section.
- **Report:** `reports/phase-04-decoders-and-page-numbering.md` created (draft).

### Step 10: sequence decoders in AS
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/stream_analysis/decoding/{__init__,semi_markov,crf,bilstm}.py`,
  `pyproject.toml` (core `scipy>=1.10`), `tests/test_stream_analysis_decoding_semi_markov.py`,
  `tests/test_stream_analysis_decoding_crf.py`, `tests/test_packaging_core_imports.py`,
  `docs/shared_library/changelog.md`, the phase report and plan files
- **Public API:** `decoding`: `EPS`, `fit_lognormal_duration`, `log_duration_pmf`,
  `decode_semi_markov(start_scores, log_pmf, max_duration) -> list[bool]`; lazy: `NEG_INF`,
  `CRFSequence`, `LinearChainCRF(n_features, n_states=3)`,
  `start_score_from_emissions(model, features, start_states=(0,))`,
  `collapsed_start_f1`, `viterbi_start_f1`,
  `train_crf(train, dev, n_features, n_states=3, start_states=(0,), seed=0, *, lr=0.05, max_epochs=40, patience=6)`,
  `decode_core(model, sequences, start_states, sep="/")`, `BiLSTMTagger(n_features, hidden_size=64)`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 867 passed (717 before;
  +122 mine, +28 step 11 in parallel); the 3 target modules 126 passed
- **Acceptance:** 1 pass (122 new); 2 `np.array_equal` 2/2; 3 decode 200/200, pmf 2/2,
  state_dict 10/10, viterbi/nll/partition 200/200 each, train_crf 3/3; 4 pass; 5 pass
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 10](../../reports/phase-04-decoders-and-page-numbering.md#step-10-sequence-decoders-in-as);
  findings F04-10.1–6
- **Error analysis:** no equivalence mismatches; forced starts at `max_duration` < T follow the prior, and exact ties give the longest segment, so the short one lands first.
- **Deviations from brief:** none
- **Brief defects:** `test_zero_mass_raises` (mu=20, `max_duration`=5) needs a sigma: 0.5 does not underflow, used 0.1. `STATUS.md` said the changelog had an `## Unreleased` heading: true. The brief's synthetic task gives F1 1.0 for every seed (weak F1 evidence).
- **Open questions for the user:** none

### Step 11: scan-id helpers
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/model/scan_id.py`, `tests/test_model_scan_id.py`, `docs/shared_library/changelog.md`, `reports/phase-04-decoders-and-page-numbering.md`; LotA `archival_language/ead/adapters.py`
- **Public API:** `ScanId` (`parse(scan_id, *, digits=4, prefix=None)`, `try_parse`, `from_prefix`, `format`, `with_number`, `prefix`), `InvalidScanIdError`, `scan_number(scan_id, *, digits=None)`, `parse_doc_id(doc_id, *, marker)`, `NA_INSTITUTION`, `DEFAULT_DIGITS`
- **Tests:** AS targeted 44 passed; AS full 867 passed; LotA `test_adapters.py` 11 passed; LotA `--ignore=tests/test_derive_voc.py` 316 passed
- **Acceptance:** 1 pass; 2 pass (11, 316); 3 21,623/21,623 rows, 20,905 distinct, 0 fail; 4 162,468/162,468 ids in 254 inventories, 0 fail, 0 scan_number mismatches
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 11](../../reports/phase-04-decoders-and-page-numbering.md#step-11-scan-id-helpers);
  findings F04-11.1–4
- **Error analysis:** no failures to classify; suffixed inventories always carry the suffixed number, some start at 0002/0003/0007.
- **Deviations from brief:** none
- **Brief defects:** none
- **Open questions for the user:** none

### Step 12: page numbering core
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/analysis/page_numbering.py`, `archival_structures/datasets/globalise_index.py`, `tests/test_analysis_page_numbering.py`, `tests/test_datasets_globalise_index.py`, `tests/test_packaging_core_imports.py`, `docs/shared_library/changelog.md`, `reports/phase-04-decoders-and-page-numbering.md`, `reports/data/phase-04-decoders-and-page-numbering/step12-*.tsv`
- **Public API:** `AlignmentConfig(mode, runs, extrap, dup_first)`, `CONFIDENCE_BANDS`, `longest_increasing_chain(values)`, `main_chain(anchors)`, `numbering_restarts(anchors, keep, min_len=3, max_rounds=4)`, `numbering_mode(chain, *, foliation=1.5, pagination=0.75)`, `interpolate_folio_to_scan(folio, run, extrap=False, max_scan=None, *, edge=6)`, `confidence_band(dist, inside, bands)`, `align_documents(docs, anchors, cfg, max_scan)`, `align_all(index_df, anchors_by_inv, cfg, max_scans, *, inventory_column)`, `anchors_array(df)`, `page_number_anchors(regions, *, tag, pattern, tag_column, text_column, scan_column, scan_number)`; `GLOBALISE_INDEX_COLUMNS`, `load_globalise_index(path, inventories=None, columns=...)`
- **Tests:** targeted (2 new files + packaging) 27 passed; full AS suite 890 passed
- **Acceptance:** 1 pass (23 new); 2 index equal (227,526 rows, also filtered 22), `align_all` 6/6 configs equal, `anchors_array` 467/467; 3 region anchors: 466/467 inventories identical, 64,505 pairs both, 0 only mined, 1 only regions (10209_0969); 2555 374/374
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 12](../../reports/phase-04-decoders-and-page-numbering.md#step-12-page-numbering-core);
  findings F04-12.1–6
- **Error analysis:** region and mined anchors agree; region-anchor accuracy equals mined (baseline .6190/.8743/.9298, chosen .7794/.9104/.9479). 42 of 1,281 high-confidence docs are off >2, mostly misread digits.
- **Deviations from brief:** none
- **Brief defects:** the seeded sample of 30 differing pairs is empty (1 difference); sampled multi-region scans instead. The brief doesn't say whether the "chosen" accuracy uses `use_alt`; reported both.
- **Open questions for the user:** none

### Step 12b: NLP alignment shim
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** NLP `src/startpage/globalise_alignment.py` (shim), `scripts/verify_as_globalise_alignment.py` (new), `output/as-phase-04-originals/src/globalise_alignment.py` (copy); AS `docs/shared_library/changelog.md`, `reports/phase-04-decoders-and-page-numbering.md`
- **Public API:** none in AS; NLP names unchanged
- **Tests:** verify script (both modes) exit 0, ~12 s; NLP `PYTHONPATH=src venv/bin/python -m pytest tests/test_semi_markov.py tests/test_evaluation.py` -> 21 passed; AS suite 890 passed
- **Acceptance:** 1 pass (stored 9/9 CSVs byte-identical, manifest equal without git_head, A/B 10/10, 13 protected files unchanged); 2 pass; 3 pass (21; 890)
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 12b](../../reports/phase-04-decoders-and-page-numbering.md#step-12b-nlp-alignment-shim);
  findings F04-12b.1-3
- **Error analysis:** strict `scan_no` rejects none (64,505 anchors, 269,383 spine, 21,623 annotation rows); 1,769 `+extrapolated` rows are all low, 1,749 genuine extrapolation (chosen config has extrap on).
- **Deviations from brief:** originals mode runs the copy's source with `__file__` set to the real module path (see brief defects); changelog edited by a single read-modify-write script, not the Edit tool.
- **Brief defects:** the copy under `output/` makes `parents[2]` resolve to `output/`, so `spec_from_file_location` alone fails (INDEX_PATH wrong); the `+extrapolated` question assumed `extrap` off, but the stored run uses the chosen config (extrap on); runtime is ~12 s, not ~1 min; NLP pytest needs `PYTHONPATH=src`.
- **Open questions for the user:** none

### Step 10b: NLP decoder shims
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** NLP `src/startpage/{semi_markov,crf_sequence,bilstm_sequence}.py` (re-exports), `scripts/{train_crf_sequence,train_multitask_crf,train_semi_markov_duration}.py` (wrappers), `scripts/verify_as_decoders.py` (new), `output/as-phase-04-originals/{src,scripts,tests}/` (copies); AS `docs/shared_library/changelog.md`, `reports/phase-04-decoders-and-page-numbering.md`
- **Public API:** none in AS; NLP names unchanged
- **Tests:** NLP `PYTHONPATH=src venv/bin/python -m pytest tests/test_semi_markov.py tests/test_evaluation.py` -> 21 passed; AS suite 890 passed; verify script exit 0
- **Acceptance:** 1 pass (stored 4/4 byte-identical, `start_only` 6x23 equal, A/B 5/5, BiLSTM 5/5, 24 protected files unchanged; 325 s for both modes); 2 pass; 3 pass (3 re-export lines, 0 `optim.Adam`)
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 10b](../../reports/phase-04-decoders-and-page-numbering.md#step-10b-nlp-decoder-shims);
  findings F04-10b.1-5
- **Error analysis:** long-chunk dev: semi-Markov F1 0.7829 = CRF 0.7814 (114/9,270 pages differ, a wash). Held-out: semi-Markov predicts 1-4.5% starts against 11.7% true, segments up to the 250 cap; the prior's median (3) matches gold, so emission scale in the DP is the likely cause (not isolated).
- **Deviations from brief:** none
- **Brief defects:** NLP pytest needs `PYTHONPATH=src` (`startpage` not installed); the error-analysis "3 inventories" did not say which split/seed (used long-chunk dev seed 0; only 2 disagreeing pages found, so pooled counts added); "worst" held-out inventory is a tie (8121, 8834 both F1 0.000 at seed 42; took 8834).
- **Open questions for the user:** none

### Phase review
- **Session:** Opus 5.5, 2026-10-14 (the planning session; it orchestrated the five Sonnet
  steps: 10 ∥ 11, then 10b ∥ 12, then 12b). **Verdict:** accept with follow-ups.
- **Re-run:**
  - AS 890 passed; NLP 21 passed; LotA 11 + 316 passed;
  - `verify_as_decoders.py` exit 0 (7 PASS); `verify_as_globalise_alignment.py` exit 0 (6 PASS);
  - the step 10 and 12 equivalence scripts reproduce;
  - the planning hashes of NLP's protected outputs match.
- **Own error analysis:** the held-out semi-Markov collapse is its input. Emission-only start
  log-odds have median −0.65 at held-out gold starts (22% > 0), +4.63 on long-chunk (84%), and
  are just as low on the held-out training inventories (F04-R.1;
  `reports/data/phase-04-decoders-and-page-numbering/review_heldout_scores.py`).
- **Findings:**
  - 28 confirmed;
  - 3 revised: 10b.4 (cause, mechanism), 10b.5 (collection order), 12b.2 (headroom 0.001);
  - added F04-R.1–3 (held-out cause; LotA `ead/` untracked; NLP test order).
- **Fixed in review:** 2 stale changelog lines. Template `STEP.md` gained three rules (`__file__`
  for saved originals, "if any" samples, Edit-only shared files in parallel runs).
- **Written:** `REVIEW.md`; report final (Summary, Review, Follow-ups, Reproduce); 34 rows in
  `reports/index.md` and the phase row; `PLAN.md` hand-off; `plan/PLAN.md` row; `STATUS.md`.
  `make reports` clean.
- **Release recommendation:** v0.8.0 (minor: new public modules, scipy in core).
