# Phase 08 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus, 2026-10-21. The same session ran steps 16, 17 and 17b on Sonnet subagents,
  so this reviewer saw the orchestration but wrote none of the code.
- **Verdict:** accept with follow-ups. No review-fixes brief: the defects were in docs and
  comments, and they were fixed in the review (row 1).

## Checklist

- [x] **Acceptance re-run by the reviewer:** every check except the 68-min full SG run (table
      below). For that run, both TSVs were `cmp`-ed, and their mtime (18:31) postdates the edit of
      LotA `integrity.py` (17:20).
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1166 passed.
- [x] **LotA tests and lint:** `make test` → 368 passed, 1 xfailed (the xfail is phase 02's,
      F02-R.6); `make lint` → all checks passed. Both re-run after the review edits: quality and
      integrity tests 32/32, lint clean.
- [x] **Error analysis:** each step read individual cases. The reviewer's own reading is below.
      Four step findings were revised (F08-16.3, 16.4, 17.3, 17.4) and none withdrawn.
- [x] **Report:** final; 24 findings in `reports/index.md`; `make reports` gives 0 warnings.
- [x] **Changelog:** complete after two lines added at review (row 3).
- [x] **Ports came with their tests:** 19 + 13 new AS tests. LotA switched only after they passed
      (PROGRESS blocks; LotA suite green).
- [x] **No archive literals in AS core:**
  - `test_input_not_mutated_and_no_archive_literals` and `test_no_region_type_literal_in_rules`
    pass;
  - `region_quality.py:9` documents SG Dutch tuning;
  - `duplicate_pages.py` now documents its SG calibration (row 1). It names "States General" in
    words, because the test forbids `1.01.02` and `NL-HaNA`.
- [x] **LotA copies deleted:** `_clean_token_for_lookup`, `_clip`, `_STRIP_FOR_LOOKUP_RE`,
      `_shingles`, `_minhash_*`, `_lsh_candidates` and the constants are gone (diff). The wrappers
      keep their signatures, and `cli/region_quality.py` and `cli/geometry_probe.py` still import.
- [x] **Leakage:** `metrics.py` scores the even `row_no` (held-out) rows; the thresholds come from
      the odd rows (LotA report/05, 09); the fixture lexicon subset is built from gold text, not
      labels. No threshold was tuned.
- [x] **Brief defects:** two minor ones, both answered (see *Brief quality*).
- [x] **17:** full-run TSVs `cmp`-identical; `reshoots.py` 13/13, 23 rows.
- [x] **17b:** `git diff` of the three corruption test files removes exactly one line,
      `test_default_cases_are_all_four`; 72 passed.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 16 | `lota_check.py` | 0 of 150 ×4 | 0 of 150 ×4 | yes |
| 16 | `iq_check.py 20261021 200` | `mismatches []`, 428,544 | same, 37 s | yes |
| 16 | `metrics.py` sheet 02 held-out keep P/R | 0.973/0.973 → 0.973/0.986 | same | yes |
| 16 | combined held-out keep/drop, 3-way | 0.807 → 0.893, 0.727 → 0.820 | same | yes |
| 16 | AS gold tests 16–18 | 19 passed | 19 passed | yes |
| 17 | `dup_check.py` on 17 inventories | 17 × `same True` | 17 | yes |
| 17 | full SG run `cmp` (candidates, manifest) | identical | `cmp` silent on both; run not repeated | yes |
| 17 | `reshoots.py` on the new table | 13/13, 23 rows | 13/13, 23 rows, 14,994 adjacent | yes |
| 17b | copy fractions on 3771, 9 runs | 0.096–0.504 | identical table | yes |
| 17b | F1, WindowDiff, pair share, 3771 and gm 1363 | report tables | identical | yes |
| all | AS suite / LotA suite / lint | 1,166 / 368+1x / clean | same | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (docs) | `archival_structures/analysis/duplicate_pages.py:1`; LotA `corpus/integrity.py:58` | The port dropped LotA's calibration note for the 100-shingle floor (755 smoke-set candidates, the gap at 117 shingles). LotA's new comment says the notes are in the AS docstring, but they weren't. LotA's P2-12 rationale for `DEFAULT_KEEP_CLASSES` was deleted, and the AS comment kept it without the decision ID or numbers | Fixed in review: AS docstring paragraph (SG calibration, plus the threshold's closeness to re-shoots, F08-R.2); P2-12 comment restored in LotA `quality.py` (F08-R.4) |
| 2 | defect (minor) | LotA `corpus/quality.py:40` | `__all__` listed only the five AS re-exports, so star-imports and tools saw `load_lexicon`, `quality_frame`, `inventory_quality` and others as private | Fixed in review: `__all__` lists all 12 public names |
| 3 | defect (changelog) | `docs/shared_library/changelog.md` | Two behaviour changes were not listed: default `simulate_inventory` now runs a fifth case, and LotA `duplicate_candidates` now raises `ValueError` on a threshold outside [0, 1] or a repeated page id | Fixed in review: two lines added |
| 4 | observation | `region_quality.py:149–154` | Numerals in the injected lexicon count as hits: 9,298 all-digit terms in LotA's vocabulary; "3761 -10 3" (gold noise) is predicted usable (F08-R.1) | Docstring note added; whether to filter numerals is LotA's decision (follow-up) |
| 5 | observation | `duplicate_pages.py:33` | The 0.6 threshold sits just below the lowest documented re-shoot (0.615), and formulaic pages reach 0.763 (F08-R.2). Recall below 0.6 is unmeasured | Follow-up (phase 10 candidate) |
| 6 | observation | LotA `corpus/quality.py:162–168` | `QualityThresholds.from_dict(THRESHOLDS)` is built for every region | No action: `iq_check` time is unchanged at 37 s, and the design keeps a patched `THRESHOLDS` working |
| 7 | observation | LotA `output/integrity/as-step17/` | A 6.9 MB copy of the stored tables | Kept as the evidence for F08-17.1 |

## Error analysis read by the reviewer

- **Step 16.** I recomputed every combined-held-out error from `step-16/predictions.tsv`.
  - **Keep/drop:** the 16 errors and their categories hold.
  - **Usable/fragment:** there are 11 confusions, and 9 are gold-usable at 0.4 (8 `short_region`,
    1 `no_lexicon_hits_despite_length`). The step said 8 and "all `short_region`".
  - **1–3-token band, before → after:** 25 → 10 predicted noise. 15 moved out of noise (14 of them
    gold-keep) and 0 moved in. The step said the retune moved regions "into noise elsewhere",
    which an exemption cannot do.
  - **Unexplained in the step:** 02c 58 "3761 -10 3" (gold noise) is predicted usable. That is
    `strong_lexicon_hits` on "3761", which led to F08-R.1.
- **Step 17.** I read the opening text of all 30 sample-A rows. For 13 rows I also read the full
  texts, with `difflib` matching blocks (scratchpad script, read-only from the LotA root).
  - **Sample A:**
    - 12460 24/120 is the resolution of 18 June 1680 and its register extract: a real parallel
      copy;
    - 12020 149 page-1/page-2 is two letters of 2 April 1733 to the envoys at Vienna and London,
      with the same wording: a formula;
    - 12290 (commissions), 3616 (Dubbeldemuts requests for different ships), 2922 and 3622
      (passports) are formulas, as the step said.
  - **Sample B:** 1560 297/298 (J 0.628), 8579 170/171, 8744 319/320, 11705 766/767 and 12141
    367/368 each have the same body and the same last 120 characters. They are re-shoots despite
    different openings.
  - **Sample C:** 5751A 0568 and 5751B P001 are two shots of the same 1626 letter (*t'Schip het
    Wapen van Amsterdam*, ratio 0.74). 12168 0620 and its `deelopname1` are the same *Ordre ende
    Reglement* (ratio 0.98).
  - **Verdict:** the step's verdicts hold apart from the 12020 row, and its conclusion stands.
- **Step 17b.** Both `run17b.py` tables reproduce exactly. The two gm 1363 start-copy cases
  (observed positions 237 and 351) follow from the first-occurrence rule as described.

## Brief quality

- **16:** `lota_check.py` was to be "a copy of `gold_check.py`", but the copy's last block called
  the removed `_clean_token_for_lookup`. Lesson for briefs: when a script is copied from a planning
  script and the step deletes a private name, say which block of the copy to drop.
- **16:** `quality_frame` has no bbox input, so its bbox feature columns are always `nan`. The
  brief's API implied this; no change.
- **17:** `protect.py` was simplified. Fine.
- **All three:** the briefs were complete, and no step had to explore the code base.
- **Recurring defect, new:** a port must carry the *rationale* comments of the code it replaces
  (calibration notes, decision IDs), not only the code. Added to `plan/CONTEXT.md` §5.

## Updates applied

- [x] Phase report `reports/phase-08-region-quality-and-duplicates.md`: *Summary*, *Review* and
      *Follow-ups* written; every finding confirmed or revised; status *final*
- [x] `reports/index.md`: 24 findings appended, phase row added
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed (rows 1–3 fixed in review, docs and comments only)
- [x] `docs/shared_library/changelog.md` complete for this phase
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] sibling notes: LotA `docs/from-archival-structures.md` §7 (written by steps 16 and 17) is
      accurate
- [x] Release recommendation: **v0.13.0**, with `analysis.region_quality`,
      `analysis.duplicate_pages`, `stream_analysis.corruption.page_duplication` and
      first-occurrence start labels
