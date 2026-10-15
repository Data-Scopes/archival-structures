# Phase 05 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-15. A fresh session; it did not plan or orchestrate the phase.
- **Verdict:** accept with follow-ups. The text layer is correct: every acceptance check
  reproduces. A short review-fixes step (15r) should run **before the release**: two pandas-3
  failures outside the text layer, the run-time pagexml-tools floor, and two doc fixes.

## Checklist

- [x] **Every step's acceptance re-run by the reviewer** (table below). Commands are in
  `reports/phase-05-text-layer.md` § Reproduce. They ran sequentially from one script, with
  `protect.py` snapshotting before and verifying after:
  - step-15 `check.py ref ab` → 157/157 and 140/140; `scanid` → 0 of 4,798,666;
  - the 15c CLI run on the 22 plus `check.py` → 157/157, manifest a superset, the rerun a no-op;
  - `recipe.sh` → `IDENTICAL (15 comparisons)`;
  - NLP `verify_as_text_layer.py` → exit 0, `ALL PASS` (second run; see *Error analysis* for
    the first).
- [~] **Suites.** NLP `test_voc_text_layer.py` 29 passed; `test_evaluation.py
  test_semi_markov.py` 21 passed; LotA full suite 327 passed. **The AS suite can't be
  reproduced in today's `.venv`.** The user reinstalled packages on 2026-10-15 09:45–09:50
  (pandas 3.0.6, a new `archival_structures-0.8.0.dist-info`):
  - pytest 9.1.1 lost its `py.py` shim (listed in its `RECORD`, absent on disk), so pytest
    doesn't start at all;
  - the `vision` extra is gone (hdbscan, cv2, PIL, skimage), so 13 test modules can't be
    collected.

  With the shim restored on `PYTHONPATH` from the scratchpad (the venv was not touched):
  - the full suite gives 737 passed, **2 failed**, 3 skipped, 13 collection errors. Both
    failures come from pandas 3, in phase-02/04 code (F05-R.2);
  - the text-layer targeted run (`tests/test_text_layer_*.py`,
    `test_packaging_core_imports.py`, `test_analysis_neighbourhood_analysis.py`) gives 101
    passed and 1 skipped (torch).
- [x] **No pagexml (or pandas) at import time:** `test_text_layer_modules_do_not_import_pagexml`
  (`tests/test_packaging_core_imports.py` L70) imports all 8 modules with pagexml and pandas
  blocked; passes. `compare` imports numpy (a core dependency) at module level.
- [x] **No VOC constants outside `presets.py`** apart from the documented calibration. The
  grep for `NL-HaNA_1.04.02|voc_text_layer|report/74|unclassified` finds only docstrings,
  `extract.DEFAULT_UNTAGGED_TAG = "unclassified"` (Laypa's name for an untyped region, a named
  default), and `build.py`'s `--preset` default `voc_v1`. The calibrated parts are
  `word_breaks.DEFAULT_WORD_BREAK_RULE` and `SANITY_IN`/`SANITY_OUT`, and they are not part of
  `LayerPreset`. That is debt for the SG preset (finding 7).
- [x] **Protected NLP outputs unchanged:** `protect.py` (7,079 entries: sha256 of the 160 `<REF>`
  files; size and mtime of v1's 6,917 `wbc/` files, `manifest.json` and `status.tsv`) shows 0
  differences across all runs. The
  verify script's own check (41,692 files) shows 0 changed on its second run.
- [x] **Changelog:** steps 13, 14, 15, 15c and the 15b switch are under *Unreleased*, with NLP's
  old → new names. Corrected in review: three headings said "not yet switched"; step 13 gained
  the pagexml-tools 0.9.0 behaviour changes that the pin forces on callers and a note on
  `MIN_PAGEXML_TOOLS`. Checked against the code: every name listed exists, with the signature
  given. Unlisted public names are minor (`io`'s regexes, `compare.COLUMNS`/`BOOLEANS`,
  `build.is_done`/`recorded_done`/`extracted`/`now`).
- [x] **P3d recorded:** the user's v2.1 sample of 20 inventories, run 2026-10-15, gave
  `IDENTICAL (100 comparisons)` (`STATUS.md`). It is now F05-R.1 in the report.
- [x] **Report final; findings in `reports/index.md`; `make reports` clean.**
- [x] **Brief defects answered** (below).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run (2026-10-15) | Match |
|---|---|---|---|---|
| 14 | `corpus_default.json`, `wbc_per_inventory.tsv` vs v1 | 2/2 | 2/2; default {¬, „}, 1,461 differ, 172 whole default | yes |
| 14 | `wbc_stats.tsv` rewrite; parts from v1 lines; sort order | identical; 52/52; 6,914 | identical; 52/52 (16 s); 6,914/6,914 | yes |
| 15 | 22 inventories vs `<REF>` | 157/157 | 157/157 (extract 37 s, wbc + join 4 s) | yes |
| 15 | A/B vs original module, 20 inventories | 140/140 | 140/140 (wall 43 s; CPU AS 141 s, NLP 134 s) | yes |
| 15 | `scan_id_matches` over v1 | 0 of 4,798,666 | 0 of 4,798,666 in 6,914 inventories (10 s) | yes |
| 15c | CLI on the 22 | 157/157, 34.6 s | 157/157: TSV 110/110, JSON 44/44 (byte 22/44), run files 3/3; 36.6 s | yes |
| 15c | manifest superset | new − ref = {archival_structures, preset} | the same in all 3 stages; the 5 layer-level keys equal | yes |
| 15c | rerun is a no-op | 0 to do, 0.4 s | 0 to do in each stage, 0.42 s, 44 mtimes unchanged | yes |
| 15c | v2.1 recipe on 3 inventories | exit 0, 15/15 | `IDENTICAL (15 comparisons)` | yes |
| 15c | `--help`, `--estimate` | 14 flags; 0.5 min | 14 flags; "0.01 h (0.5 min) on 8 workers" | yes |
| 13 | environments import the joiner | 3/3 | 3/3; all three now report metadata 0.9.0 (the siblings said 0.8.0, F05-13.4) | yes |
| 13 | LotA suite | 327 | 327 | yes |
| 15b | `verify_as_text_layer.py` | exit 0 | 1st run: check 5 FAIL (the user's D5 move ran at the same time); 2nd run: exit 0, ALL PASS, builds 32.1 s / 32.7 s | yes |
| 15b | NLP tests | 29 + 21 | 29 + 21 | yes |
| all | AS full suite | 976 passed | not reproducible in today's `.venv` (see Checklist); text-layer targeted 101 passed | partial |

The text-layer outputs are now byte-identical under pandas 3.0.6 too: the AS runs used 3.0.6
and NLP's `venv` uses 2.3.3, and the steps ran on the pandas that `.venv` had on 2026-10-14.

## Findings

Most serious first. **Defect** means must fix (it goes into a review-fixes brief), **debt**
means recorded and deferred, and **observation** means no action.

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect | `archival_structures/analysis/sequence_patterns.py:187` | Under pandas 3 a `None` label in a str `Series` comes back as NaN. `label is None or label == -1` misses it, so NaN becomes a label of its own and the run isn't broken (3 elements instead of 2). `pyproject.toml` allows pandas < 4, and LotA already runs pandas 3.0.6 (F05-R.2) | step 15r §1 |
| 2 | defect | `tests/test_datasets_globalise_index.py:29` | asserts `dtype == object`; pandas 3's `read_csv(dtype=str)` gives `StringDtype`. Test-only | step 15r §2 |
| 3 | defect (minor) | `archival_structures/text_layer/provenance.py:6` | `MIN_PAGEXML_TOOLS = (0, 8, 0)` while the pin is `>=0.9.0`; `build.py` L19 documents 0.8.0. Under 0.8.x with fuzzy-search 3 the joiner can't import (F05-P.5), so the check passes a broken environment. Safe to raise now that all three environments report 0.9.0 | step 15r §3 |
| 4 | debt | `archival_structures/text_layer/build.py:270–306` | `stage_wbc` re-derives and overwrites `corpus_default.json` and `wbc_per_inventory.tsv` from the run's inventories only. A subset run with `--out-dir` pointing at the layer's parent (now `shared-data/`) would overwrite v1's decision, and `join` would then rewrite regions. This is inherited from NLP and kept verbatim (P5) (F05-R.5) | docs in 15r §4; a guard in phase 06 |
| 5 | debt | NLP/LotA environments | archival-structures' own dist-info is stale: 0.1.0 in NLP, 0.3.0 in LotA. Layers built through NLP's wrapper record `archival_structures.version` 0.1.0 (seen in the review's verify manifest); `git_head` is right (F05-R.3) | user: `pip install --no-deps -e <AS>` with both interpreters (phase-01 precondition) |
| 6 | debt | `word_breaks.py` (rule) | The rule systematically misses `„`. Of 696 per-char inventories without `„`, 446 fail only `end_rel_frac`, because `„` is frequent overall there (median 8,361 occurrences against 711 where `end_rel_frac` passes). In a fresh sample, 13 of 18 line pairs are genuine breaks (F05-R.4) | phase 06 (v2 rule), not tuned in v1 |
| 7 | debt | `presets.py`, `build.py:303` | `LayerPreset` doesn't carry the word-break rule or the sanity anchors; `stage_wbc` always uses `DEFAULT_WORD_BREAK_RULE` with `¬` in and `:`/`.` out. An SG preset needs its own | phase 06 |
| 8 | observation | `compare.py:94`, `compare.py:147` | Without `--inventories`, `compare` uses the intersection of both sides' `lines/`, so an inventory missing on one side is skipped silently. `summarise` prints `content_equal 0` for JSON kinds (NA). `_compare_tsv` decompresses each file twice | phase 06 if `compare` is reused |
| 9 | observation | NLP `src/startpage/voc_text_layer.py` | The shim re-exports four private AS names (`_assert_clean`, `_is_pagexml_member`, `_event`, `_make_line`). No NLP importer uses them (grep), so AS can still rename them | none |
| 10 | observation | `provenance.py:42` | `check_min_version` uses `assert`, which `python -O` strips (inherited from NLP) | none |
| 11 | observation | AS `.venv` | Since 2026-10-15: pytest's `py.py` is missing and the `vision` extra is uninstalled (see Checklist) | user: `pip install --force-reinstall --no-deps pytest==9.1.1`; `pip install -e '.[vision]'` |
| 12 | observation | `<DS>/shared-data/README.md` | The D5 move is done (NLP and LotA symlinks repointed 13:20–13:21 on 2026-10-15; 6,914 files in each per-inventory directory; mtimes kept), but the README still says "Not moved yet" | user (D5) |

One-liners fixed in the review: the stale `vertical_distance` docstring in
`analysis/neighbourhood_analysis.py` L74–80 (step 13 left it), and the changelog edits above.

## Error analysis read by the reviewer

- **Word breaks (step 14).** The six quoted statistics reproduce exactly from v1's
  `wbc_stats.tsv`. The reviewer's own pass looked at all 696 per-char inventories whose set lacks
  `„` (590 + 32 + 74 of the 78 add-and-drop, consistent with F05-14.2):
  - 446 fail only `end_rel_frac`, 148 fail only `join_vocab_frac`, and 102 fail both.
    Quartiles among all that fail `end_rel_frac`: 3.21 / 3.99 / 4.56 against 5.0. Among all
    that fail `join_vocab_frac`: 0.143 / 0.19 / 0.23 against 0.25;
  - a fresh sample (`random.Random(505)`: 3427, 4398, 7565, 4434, 7838, 9603) had 3 attached
    `„` line ends each read. 13 of 18 are genuine breaks (`gequali„ | ficeert`,
    `gene„ | „raal`, `bewindheb„ | beren`, `per„ | zoonen`), 2 are not (`Comptoir„ | van`, a
    signature `Eij„ | Samarang`), and 3 are ambiguous.

  Step 14's conclusion holds and generalises. The miss is systematic, not a few edge cases:
  `„` doubles as an abbreviation mark (`a„o`, `J„n`, `p„s`), which dilutes its line-end
  enrichment. Leakage: step 14's 10-inventory sample and this one are disjoint from the 22
  inventories on which the thresholds were set, and the rule uses no labels.
- **Input order (step 15).** On the 22, line multisets are equal in all 190,954 regions. A
  fresh sample (seed 505) of 4 regions with a different line order:
  - 8834: v1 `al„ | „leen in 23 p„s`, which joins to *alleen*. The rebuild moves `„leen…` to
    the region end, so the word can't be joined;
  - 1547: v1 keeps rank and name paired (`1: rolleser — | abdul van battavia`); the rebuild
    moves the rank line;
  - 1274: table fragments, undecidable;
  - 1539: a one-word fragment `rmijn` swapped, undecidable.

  So v1 is right in 2 and undecidable in 2, which agrees with step 15's 9/10. The 8834 case
  shows that line order changes the join itself, not only reading order.
- **Scan ids.** 0 of 4,798,666, as logged. The fallback test covers only 3 ids
  (1186 ×2, 9817).
- **Manifests.** The 15c diff was read key by key; it matches F05-15c.2. In the NLP verify
  manifest, `archival_structures.version` is 0.1.0 (finding 5); `pagexml_tools.version` is now
  0.9.0, so F05-15b.3's pagexml part is resolved.
- **The failed first verify run.** 41,492 of 41,692 files under v1 were "changed" between the
  before and after snapshots. `data/voc_text_layer_v1` became a symlink at 13:20 (the user's
  D5 move), inside the 13:20:39–13:22:10 run window. Read-only checks afterwards: file counts
  6,914 per directory, 2026-10-09 mtimes kept, and `protect.py`'s v1 entries unchanged. The
  second run passed. This is not a code defect.

## Brief quality

| Step | Brief defect | Answer |
|---|---|---|
| 14 | `read_region_lines` docstring misdescribes its return | Still wrong in `io.py` L162. Left (ported verbatim). The fix is in a later AS step, or in 15r if the user wants it |
| 15 | acceptance 3 assumed 0 irregular scan ids; EA counts quoted for 34 inventories, not 42; NLP A/B names not listed; `join_inventory` range off by one; runtime estimate stale | Briefs must run every acceptance query in planning on the full data, not on a sample (the 98 ids were findable from v1's stats) |
| 15c | `.venv/bin/pip` missing; "byte-equal" vs JSON-equal; `compare_run_files` labels; pandas forbidden by the packaging test; recipe target | `plan/templates/STEP.md` now says `.venv/bin/python -m pip` and asks for each criterion's comparison mode (bytes or JSON) |
| 13 | release was 0.9.0, not 0.8.2; neighbourhood test not named; sibling `CLAUDE.md` | Amendment covered it. A pin change must grep the AS tests for the upstream behaviour it changes |
| 15b | runner needs `sys.argv`; columns "as above" pointed to an unread brief; `rm -rf` denied | `STEP.md` now notes: no `nohup`/`&`, `rm -rf` is denied, give every scratch run its own new directory |

## Updates applied

- [x] Phase report `reports/phase-05-text-layer.md`: *Summary*, *Review* and *Follow-ups*
      written; every finding `confirmed` or `revised`; status *final*
- [x] `reports/index.md`: the phase's findings appended (with Edit) and its phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief written: `step-15r-review-fixes.md`
- [x] `docs/shared_library/changelog.md` complete for this phase (corrected as above)
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [ ] sibling `STATUS.md` notes: not needed (NLP's changes are listed in AS `STATUS.md`)
- [x] Release recommendation for the user: after 15r, `make bump-minor` → v0.9.0 (see `STATUS.md`)
