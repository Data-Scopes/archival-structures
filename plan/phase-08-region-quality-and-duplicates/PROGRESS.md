# Phase 08 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning
- **Session:** Opus, 2026-10-21. **Status:** done
- **Created:** the phase dir (no seed existed), `PLAN.md`, `STATUS.md`, briefs 16, 17 and 17b,
  `reports/phase-08-region-quality-and-duplicates.md` (draft), and planning scripts in
  `reports/data/phase-08-region-quality-and-duplicates/planning/`. `plan/PLAN.md`: rows 08–10
  updated (P1).
- **Code checked:**
  - an Explore subagent read LotA `corpus/quality.py` (436 lines) and `corpus/integrity.py` (554),
    their tests (15 + 16), and LotA reports 05 and 09;
  - a second Explore subagent read AS `analysis/`, `utils/wordlist.py` (a function-word list, not
    a lexicon) and `stream_analysis/corruption/` (61 tests);
  - neither module exists in AS yet; gensim and datasketch are not installed.
- **User decisions** (2026-10-21):
  - P1: the phase-07 follow-ups go to a new phase 09;
  - P2: `page_duplication` only, with the copy right after the original;
  - P3: a start label at the first occurrence only.
- **Measured** (scripts and README in `…/planning/`):
  - LotA's current code reproduces 4 × 150/150 stored gold scorings (`-after`, and `-before` with
    the exemption threshold set to `inf`);
  - the report numbers recomputed: sheet 02 held-out keep P/R 0.973/0.973 before the retune;
    combined held-out keep/drop accuracy 0.807 → 0.893, 3-way 0.727 → 0.820;
  - `inventory_quality` on 200 seeded inventories: 0 mismatches (428,544 regions);
  - `duplicate_candidates` on 7 inventories: identical to the stored table;
  - 13/13 re-shoots: 23 rows, all adjacent and same-side.
- **Report:** planning findings F08-P.1–P.8.
- **Open questions for the user:** none.

### Step 17b
- **Session:** Sonnet, 2026-10-21. **Status:** done (user decides)
- **Changed:** `corruption/operators.py` (`page_duplication`), `metrics.py`
  (`observed_start_labels`: first occurrence only), `simulate.py` (`CASE_NAMES` + branch),
  `__init__.py`; 11 new tests; docs `page_order_problems.md` (Case 5), `simulation_harness.md`;
  changelog entry under Unreleased (no sibling imports affected).
- **Tests:** corruption 72 passed (61 + 11); AS full suite 1,153 passed.
- **Acceptance 3:** 3771 copy fractions 0.096-0.115 / 0.285-0.290 / 0.483-0.504 at rates
  0.1 / 0.3 / 0.5, all within +-0.06.
- **Error analysis:** old rule vs first-occurrence labels, 9 runs on 3771 and 9 on gm 1363;
  10 copies read by hand at seed 0 rate 0.3 per stream.
- **Report:** `reports/phase-08-region-quality-and-duplicates.md` Step 17b, F08-17b.1-3.
- **Brief defects:** none. **Open questions:** none.

### Step 16
- **Session:** Sonnet, 2026-10-21. **Status:** done (user decides)
- **Changed (AS):** `analysis/region_quality.py` (port, text + injected lexicon), `tests/test_analysis_region_quality.py`
  (19), `tests/fixtures/region_quality/` (300 gold rows, 2,837-term lexicon subset, README), step-16 scripts under
  `reports/data/phase-08-.../step-16/`, changelog entry. **Changed (LotA):** `corpus/quality.py` wrappers (+`__all__`
  for re-exports), `tests/test_corpus_quality.py` (+1), `docs/from-archival-structures.md` section 7.
- **Tests:** AS full suite 1,153 passed; LotA `make test` 368 passed + 1 xfailed, `make lint` clean.
- **Acceptance:** metrics 0.973/0.973 -> 0.973/0.986; 0.807 -> 0.893; 0.727 -> 0.820; `lota_check` 0 of 150 x4;
  `iq_check` 0 mismatches over 428,544 regions; protected outputs unchanged.
- **Error analysis:** all 16 keep/drop errors and 10 of 11 usable/fragment confusions read.
- **Report:** `reports/phase-08-region-quality-and-duplicates.md` Step 16, F08-16.1-4.
- **Brief defects:** none material (the `lota_check.py` copy had to drop the lexicon-subset tail that used a removed
  private function; `quality_frame` has no bbox column, so bbox features are nan there; test count is 19 because
  test 7 is parametrised).
- **Open questions:** none.

### Step 17
- **Session:** Sonnet, 2026-10-21. **Status:** done (user decides)
- **Changed (AS):** `analysis/duplicate_pages.py` (port, position-based adjacency), `tests/test_analysis_duplicate_pages.py`
  (13), `reports/data/phase-08-.../step-17/` (coeff_pin, protect, ea_sample and verdicts), changelog entry.
  **Changed (LotA):** `corpus/integrity.py` wrapper (constants, MinHash and LSH removed), `docs/from-archival-structures.md`
  section 7. New LotA output dir `output/integrity/as-step17/` (identical copy of the stored tables).
- **Tests:** AS full suite 1,166 passed; LotA `make test` 368 passed + 1 xfailed, `make lint` clean.
- **Acceptance:** `dup_check` 17/17 same; full SG run 4,105.6 s, candidate table (48,473 rows) and manifest byte-identical;
  `reshoots.py` 13/13, 23 rows; protected outputs unchanged.
- **Error analysis:** 59 rows read. A (30 non-adjacent): 4 re-shoot, 19 formula, 2 form, 5 parallel copy. B (20 adjacent):
  19 re-shoot, 1 formula. C (9 without scan number): 9 re-shoot (`deelopname` scans).
- **Report:** `reports/phase-08-region-quality-and-duplicates.md` Step 17, F08-17.1-5.
- **Brief defects:** none material. **Open questions:** none (the `adjacent` filter question is noted in the report, not decided).


### Review
- **Session:** Opus, 2026-10-21 (the session that ran 16, 17 and 17b on Sonnet subagents).
  **Verdict:** accept with follow-ups ([REVIEW](REVIEW.md)).
- **Re-run:** every acceptance check except the 68-min full run. Its TSVs were `cmp`-ed, and their
  mtime postdates the `integrity.py` edit. All numbers match (F08-R.3). AS 1,166 passed; LotA
  368 + 1 xfailed; lint clean.
- **Own error analysis:**
  - 16: all held-out errors recomputed; numerals in the lexicon found (F08-R.1);
  - 17: 13 rows read in full (`difflib`); Jaccard overlap (F08-R.2);
  - 17b: tables re-run.
  - Revised: F08-16.3, 16.4, 17.3, 17.4. Withdrawn: none.
- **Fixed in review** (docs and comments only, F08-R.4):
  - AS `duplicate_pages` docstring: SG calibration notes;
  - `region_features` docstring: numeral note;
  - LotA `quality.py`: P2-12 comment, full `__all__`;
  - changelog: two behaviour-change lines.
  - Tests and lint re-run after the edits.
- **Updated:** report final; 24 findings in `reports/index.md`; `make reports` clean;
  `plan/PLAN.md` rows 08 and 10; `plan/CONTEXT.md` §4–5; the hand-off; `STATUS.md`.
- **Also this session:** UA-014 resolved (a) → phase 10 step (`docs/USER_ACTIONS_resolved.md`).
