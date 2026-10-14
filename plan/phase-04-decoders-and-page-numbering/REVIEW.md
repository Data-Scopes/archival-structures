# Phase 04 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-14. The same session planned the phase and orchestrated the
  five Sonnet step sessions (10 ∥ 11, then 10b ∥ 12, then 12b).
- **Verdict:** accept with follow-ups. No defect needs a review-fixes brief.

## Checklist

- [x] **Every step's acceptance met, re-run by the reviewer:**
  - `verify_as_decoders.py` exit 0, all 7 PASS (stored 4/4 ×2 modes, start_only 6×23 ×2,
    A/B 5/5, BiLSTM 5/5, 24 protected files);
  - `verify_as_globalise_alignment.py` exit 0, all 6 PASS (9/9, manifest, A/B 10/10, 13
    protected files);
  - the step 10 equivalence script (200/200, 2/2, 10/10, 200/200 ×3, 3/3);
  - the step 12 equivalence script (index equal, 6/6, 467/467).
- [x] **Suites:**
  - AS `.venv/bin/python -m pytest -q -p no:cacheprovider` → 890 passed (717 + 173 new);
  - NLP `tests/test_evaluation.py tests/test_semi_markov.py` → 21 passed;
  - LotA `tests/test_adapters.py` 11 passed; `--ignore=tests/test_derive_voc.py` 316 passed.
- [x] **`decoding` doesn't import torch:** `decoding/__init__.py` resolves `crf`/`bilstm` names
  via PEP 562, and `test_core_modules_import_without_extras` imports `decoding` and
  `decoding.semi_markov` with torch blocked.
- [x] **No VOC constants in `page_numbering.py`, `scan_id.py` or `decoding/`:** the grep finds
  only `NA_INSTITUTION = "NL-HaNA"` (a named default) and a docstring example. The GLOBALISE
  column map is in `datasets/globalise_index.py`.
- [x] **Protected NLP outputs unchanged:** the planning hash lists (decoder outputs,
  `output/splits/*`, `output/globalise_alignment/*`, the anchors CSV) all check OK, and the
  `globalise_alignment/` mtimes are identical. Modified `output/embeddings/*.npz` in NLP's git
  status date from 2026-10-05 (the user's), not from this phase.
- [x] **Leakage:**
  - the duration prior is still fitted per split kind on training documents
    (`fit_duration_long_chunk`, `fit_duration_inventory_held_out`, unchanged);
  - held-out early stopping uses the train-internal slice;
  - no AS function fits anything on evaluation data;
  - the shims don't touch split code, so `check_nested_split` isn't bypassed.
- [x] **`changelog.md`:** steps 10–12 are under *Unreleased*, with NLP (10b, 12b) and LotA (11)
  imports. Two stale sentences were fixed in review.
- [x] **Report final:** findings set, `reports/index.md` appended, `make reports` clean.
- [x] **`Brief defects` answered:** see *Brief quality*.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 10 | equivalence with NLP originals | 200/200, 2/2, 10/10, 200/200×3, 3/3 | same | yes |
| 10 | AS suite | 867 (mid-phase) | 890 at end | yes |
| 10b | verify script | exit 0, 7 PASS, 325 s | exit 0, 7 PASS | yes |
| 10b | NLP tests | 21 (with `PYTHONPATH=src`) | 21 (evaluation file first, no `PYTHONPATH`) | yes |
| 11 | LotA adapters / suite | 11 / 316 | 11 / 316 | yes |
| 11 | GT parse | 21,623/21,623 | covered by 12b's stricter `scan_no` (21,623 annotation rows, none raised) | yes |
| 12 | equivalence | index, 6/6, 467/467 | same | yes |
| 12b | verify script | exit 0, 6 PASS | exit 0, 6 PASS | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | observation | `decoding/crf.py` `start_score_from_emissions` (L101) | Ported faithfully, but on NLP's held-out setup its emission-only log-odds are negative at most gold starts (median −0.65; long-chunk +4.63), which explains the semi-Markov collapse (F04-R.1). Not an AS defect | Follow-up for NLP: posterior marginals as start scores |
| 2 | debt | LotA `archival_language/ead/adapters.py` L56–63 | The whole `ead/` directory is untracked in LotA's git; step 11's edit has no diff and no saved original (F04-R.2) | User commits LotA `ead/` |
| 3 | debt | NLP `tests/test_semi_markov.py` L3 | Imports `startpage` without the `sys.path` insert that `test_evaluation.py` has; passes only in that order or with `PYTHONPATH=src` (F04-R.3). Predates the phase | NLP conftest / packaging |
| 4 | debt (fixed) | `docs/shared_library/changelog.md` step 10 and 12 entries | "Affected sibling imports: none yet" and "NLP is unchanged in this step" were stale after 10b/12b | Fixed in review |
| 5 | observation | `tests/test_analysis_token_analysis.py` L51–58 | A transient failure during step 12's parallel run; not reproducible (5 reruns, 60 hash seeds). It uses `list(set)[0]` and a cwd-relative wordlist path | None now; candidate for a cleanup step |
| 6 | observation | `analysis/page_numbering.py` `align_documents` | The new keyword knobs (`edge`, `run_window`, bands, slopes) can't be passed through `align_documents`/`align_all`; only the low-level functions take them (as the brief specified, to keep `AlignmentConfig` at 4 fields) | Document; extend when a second archive needs other values |
| 7 | debt | `pyproject.toml` | `scipy>=1.10` added to core; `poetry.lock` not regenerated (phase-01 precondition) | User: `poetry lock` |

## Error analysis read by the reviewer

- **The held-out semi-Markov collapse** (step 10b's open issue). I retrained the CRF as the
  script does (long-chunk seed 0; held-out seed 42) and read the start log-odds per page; the
  table is in the report's *Review* section. Held-out gold starts have median −0.65 (22%
  positive), the training inventories too (−0.64, 23%), against +4.63 (84%) on long-chunk dev.
  - The DP charges a start −2·s[i] plus one extra log-pmf term (about 1.7–2.6 nats), so few
    held-out starts pay off: 36 against Viterbi's 267 and 424 gold.
  - Step 10b's conclusion (not the prior) holds, but its mechanism ("a start must beat a long
    run of mildly negative scores") is wrong. F04-10b.4 is revised.
- **Shims read line by line:**
  - NLP `semi_markov.py`, `crf_sequence.py`, `bilstm_sequence.py`, `globalise_alignment.py`;
  - the `train_multitask_crf.py` diff.
  - Names and signatures are kept, and string labels are restored in `decode_sequences`.
  - `align_all` adds back `n_sections`, and the gold loaders stay in NLP.
- **AS code read in full:**
  - `scan_id.py`;
  - `page_numbering.py`;
  - `globalise_index.py`;
  - `crf.py` L95–200;
  - `decoding/__init__.py`.
  - The quirks named in brief 12 are present and documented in the module docstring.
  - The LotA adapter keeps returning `None` for ids with an extension.
- **Step 12's multi-region claim:** that document order equals reading order on all 3,946 scans
  with several page-number regions follows from the pair totals: 64,505 pairs in both, 0 only
  in mined, so any disagreement would have shown as a pair in one source only. Accepted.
- **Step 11** sampled 254 of 6,914 inventories. The other 6,660 were not parsed, so F04-11.2 is
  stated for the sample, which is correct as written.

## Brief quality

- **10, `test_zero_mass_raises`:** the brief gave no sigma, and sigma 0.5 doesn't underflow. The
  session used 0.1. A brief that asks for an edge-case test should give every parameter.
- **10, synthetic task too easy** (F1 1.0, F04-10.6): the weight equality is the real evidence,
  and step 10b covered F1 on real data. No change.
- **10b and 12b, NLP pytest command:** the order put `test_semi_markov.py` first. The phase
  `STATUS.md` now gives the working order (F04-R.3).
- **10b, the disagreement reading didn't name split and seed.** The session chose sensibly and
  said so.
- **12, "seeded sample of 30 differing pairs" when only 1 differs:** the session read the one and
  sampled the closest population instead. Briefs should hedge with "if any", as phase 03's
  review already noted. Repeated here; added to the template.
- **12b, `spec_from_file_location` on a copied module that derives paths from `__file__`:**
  `parents[2]` pointed into `output/`. The session set `__file__` to the real module path.
  **Template updated** (`plan/templates/STEP.md`, *Protected outputs*).
- **12b, the `+extrapolated` premise:** the stored output uses the chosen config (extrapolation
  on). The planner's mistake; the session corrected it with counts.
- **12b, runtime estimate:** about 6 s per mode, not 1 minute (the build reuses cached inputs).
  No change.
- **Shared-file edits in parallel runs:** two sessions used a Python read-modify-write instead
  of Edit. Nothing was lost (checked: all sections and 30 step rows were present before review).
  The orchestration prompt said "Edit only"; the template now says it too, with the reason.

## Updates applied

- [x] Phase report: *Summary*, *Review* and *Follow-ups* written; 28 findings confirmed, 3
      revised, 3 review findings added; status *final*
- [x] `reports/index.md`: phase row and findings appended
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed
- [x] `docs/shared_library/changelog.md` complete for this phase (2 stale lines fixed)
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [ ] sibling `STATUS.md` notes: none written (neither brief asked); the user commits NLP and LotA
- [x] Release recommendation: in the final message to the user
