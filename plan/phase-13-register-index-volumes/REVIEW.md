# Phase 13 review (steps 19a–19g; final)

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.
> **Scope:** three sessions on 2026-10-28: steps 19a–19d (first part below), steps 19f and 19g (addendum), and step 19e
> with the phase close (last section, *Step 19e review and phase close*).

- **Session:** Opus 5.5, 2026-10-28
- **Verdict:** accept 19a–19d with follow-ups. The code does what the briefs say and every logged number reproduces.
  Two bars failed, and the review traces both failures to the pre-registered rules, not to the implementation. The
  error analysis of 19d is partly wrong (F13-R.1): column interleaving explains less than a third of the entry errors,
  and fixing it alone cannot lift E5 over the bar. Follow-up briefs 19f and 19g (conditional on UA-042) are written.

## Checklist

- [x] **Acceptance re-run** (table below): 19a `summary.py`; 19b interleave on `15y-b`/`15z-b` and `19b-a`/`19b-z` (outputs
      `cmp`-equal to the step files); 19c grid on fit, V05 on test (recomputed through `select.py`'s own functions,
      since `select.py test` refuses a second run) and D2; 19d `entry_eval.py dev` and `test --variant E5` into the
      scratchpad (`eval_*_lines.tsv` `cmp`-equal). All numbers match.
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,344 passed, 11 skipped (1,355 collected),
      equal to 19d's log. LotA untouched (no sibling edit), not run.
- [x] **Error analysis** read individual cases in every step and is in the report. 19d's categorisation does not hold
      up (F13-R.1); the reviewer's analysis is in the report's *Review* section.
- [ ] **Phase report final:** no. 19e is pending, so the report stays *draft*, no *Summary*, no `reports/index.md` rows.
      Finding statuses are set for 19a–19d and planning. `make reports` clean.
- [x] **Changelog:** *Unreleased* has steps 19b, 19c, 19d; each entry matches the diff (fields, defaults, `to_dict`
      omission, new functions and arguments, the `layout/pages` requirement, `unit_method` values); "affected
      sibling imports: none" is right (LotA reads published layers only).
- [x] **No archive knowledge in core:** date patterns and V05/E5 values live in `presets.py`
      (`SG_INDEX_DATE_START`, `SG_INDEX_SIDES_V5`, `SG_INDEX_ENTRY_V5`); core defaults are generic (`end_pattern`
      `\d+\s*[.,:;]?\s*$`, `date_pattern=None`). One soft assumption: `layout._write_pages_table` sorts sides by
      `int(scan_id.rsplit("_", 1)[1])` (numeric scan suffix), true for every NL-HaNA scan id (observation).
- [x] **Leakage:** `side_features` reads only line `x`, `w`, `text` (`index_pages.py`); `select.py` scores fit
      volumes only and guards the test run with `test_done.json`; 19d chose on dev sides only. The planner's pooled look
      (F13-P.5) is weighed in F13-R.6: test P/R are flat over the neighbouring thresholds, so it cannot have bought the
      pass. The review's own post-hoc runs of E1–E4 and re-splits on the 19d test sides spend that gold for good;
      every later entry test needs fresh gold (drawn: `review/fresh_entry_sides.tsv`).
- [~] **v1–v4 identical:** `LayoutParams`/`UnitParams` built from the `sg_1.01.02_v4` hand and print presets give
      `to_dict()` equal (JSON) to `15y-b`'s `layout/params.json` and `units/params.json`; 19b's byte check
      (`19b-v4check`) and 19d's unflagged-unit check are logged. *v5 print equal to v4*: pending 19e.
- [ ] **Gold quality, user's blind check:** pending (UA-039). Reviewer's own check in *Error analysis* below.
- [x] **Brief defects** answered below.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 19a | verdicts; gold lines | 28/32/0/0; 1,590 (S 671, C 810, H 100, ? 9) | same (`summary.py`) | yes |
| 19b | interleave baseline | 399,714 / 2,612 | 399,714 / 2,612, per-volume file equal | yes |
| 19b | interleave with split | 410,351 / 980 | 410,351 / 980, file equal | yes |
| 19b | B1 | 29 right / 9 wrong (0.763), 4 wrong not-index-like | verdict file recounted: same | yes |
| 19c | grid on fit, V05 | P 0.9808 R 0.9297, 12 lines | 12 lines equal | yes |
| 19c | D1 (test) | P 0.9711 R 0.9490, sides 675/12/64 | same | yes |
| 19c | D2 | 54/60, 1 FP in S3h+S3a | same; 6 disagreements equal | yes |
| 19d | dev E1–E5, choice | F1 0.770/0.814/0.816/0.820/0.833, E5 | same, E5 | yes |
| 19d | test E5 | P 0.8683 R 0.7092 F1 0.7807; v4 hand 0.6158 | same; per-line file equal | yes |
| all | suite | 1,344 passed, 11 skipped | same | yes |

Not re-run: the protected-hash lists, 19b's split statistics and `19b-v4check` byte check, 19c `flagged.py`, 19d
`units_stats.py` (the flagged-side total 8,873 agrees with the reviewer's own count over `layout/pages`).

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (analysis) | report § Step 19d; `step-19d/error_verdicts.tsv` | 14 of 19d's 26 "interleave" verdicts sit in layout regions that hold one column only (reads 5, 7, 15, 25, 27; e.g. 3256_0009 has 14 regions, one per column block). The reader saw the side's two x ranges in gold order, not the regions. The UA-041 text and F13-19d.4/.6 rest on it | report revised (F13-R.1); UA-041 reviewer note |
| 2 | debt (design) | `text_layer/types.py` L310 (`rule:index_page`), `units.split_region` | v4 already types whole index pages `index` (963 sides, 17,845 segments in the 125 hand volumes; 408 sides on detector-flagged pages). Those lines are in no main unit, so the entry rule never reaches them (30 of the 38 no-unit FN) | 19g brief (`main_types`) |
| 3 | debt (design) | `units.entry_cues` (`indent`), `presets.SG_INDEX_ENTRY_V5` | The print `indent` cue fires on hanging-indent continuation lines, the opposite of an index entry start (21 of 29 FP in one-column regions) | 19g drops it |
| 4 | debt (design) | `page_structure._column_pieces` | P6 classifies by line extent and orders pieces through the page-level column grouping; span pieces bridge the column hulls (`_joins_column`, `col_min_ratio` 0.5), so the right piece can precede the left one | 19f brief (reorder mode) |
| 5 | observation | `layout.py` L47–50 | `PAGE_COLUMNS` follows `_region_id` without the two blank lines PEP 8 asks for | none |
| 6 | observation | `layout._write_pages_table` | side order assumes a numeric scan suffix | none (all SG ids are numeric) |
| 7 | observation | `units.unit_inventory` | every segment now gets `text` in every mode; no output change, small cost | none |
| 8 | process | 19b | `git stash`/`git stash pop` while 19a worked in the same tree (to count tests before the change). No damage found (the one stash entry is the user's own, 2026-01-09; 19a's files intact) but a stash moves every tracked edit of a parallel session | rule added to the 19f/19g briefs |
| 9 | process | STATUS orchestrator notes | 19c on v4 clones: sound (the detector reads only line geometry and text, which the split does not change). 19d before UA-040 was answered: against P13 ("B1 fail: 19c–19e wait for the UA"); it produced useful evidence but spent the 20 test sides | recorded; fresh gold drawn |

## Error analysis read by the reviewer

Scripts and outputs in `reports/data/phase-13-register-index-volumes/review/` (details and numbers in the report's
*Review* section).

- **19d, all 200 E5 test errors** classified by the layout region of each gold line (column of each line by 2-means
  on the side's line starts; a region is *mixed* when it holds ≥ 2 main lines of each column): 58 errors in mixed
  regions (33 FN, 25 FP), 38 FN in no main unit (30 typed `index`, 8 `marginalia`), 104 in one-column regions
  (75 FN: 42 after a line ending in a number, 24 date starts, 9 neither; 29 FP: 21 `indent` on continuations). Read in
  full: the regions of reads 5, 7, 15, 18, 25, 27 (`dump_gold_side.py`). The 14 misattributed verdicts are listed in
  the report.
- **19b**: the B1 file and the listings of cases 13 (all three pieces in layout column 0, ordered by top: right column
  before left), 08 and 11 (ordinary left lines classed span). The cause is the rule (P6), which the code follows step by
  step. `split_v2_on_b1.py` applies the start-x rule to the 40 regions (mechanical only).
- **19c**: per-volume test counts (only 10 of 20 test volumes have positives or false flags; 4831 carries all 562
  false-positive lines), a volume bootstrap and the threshold neighbourhood (`detector_recheck.py`). 19c's FP/FN
  categories agree with the per-volume counts (3127 and 4617 hold 712 of the 1,166 FN lines).
- **19a**: gold lines against simple cues: 10 of 810 `C` lines start with a date (7 the 12297 date column 19a
  documented), 198 of 671 `S` lines do. Spot reads of 3256_0009, 3126_0013, 3547_0047, 3349_0037, 3439_0156,
  3442_0125: labels follow the brief's rules; dated follow-ups in chronological indexes are `S` by rule ("own date").

## Brief quality

- **19a** (heading-tail merged lines; undated sub-entries; exclusive cues): judgement calls, recorded. The 19g brief
  copies 19a's rules plus the two decisions 19a made, so fresh gold is labelled the same way.
- **19b** (inventory list for the interleave metric; span-piece definition; `wrong` definition): the 19f brief fixes
  the verdict rule in advance, including order, and names the hand lists.
- **19c** (volume groups; weighted sample; protect wording): 19f/19g name their groups and samples exactly.
- **19d** (base layer when B1 fails; `date_header` pairing; print baseline only 6 sides; thin FP sample): 19g pairs as
  19d did, samples FP and FN separately, and has its base fixed by UA-040/UA-042 before it starts. New template rule
  for both briefs: an error category that names a layout cause is checked against the layout table, not inferred
  from the listing (F13-R.1).
- **Planning (P13)**: "19c–19e wait for the UA" was overridden for 19d; future briefs should say what a step does
  when a blocking UA is open (wait, or run on the default as a diagnostic that spends no held-out gold).

## Updates applied

- [x] Phase report: *Review* and *Follow-ups* written, statuses set for 19a–19d and planning; **status stays draft**,
      no *Summary* (19e pending)
- [ ] `reports/index.md`: not yet (after 19e)
- [x] `make reports` runs without warnings
- [x] Follow-up briefs `step-19f-column-order-v2.md` and `step-19g-index-entry-v2.md` (conditional on UA-042)
- [x] `docs/shared_library/changelog.md` complete for 19b–19d
- [~] phase `PLAN.md` *Hand-off*: after 19e; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [ ] Release recommendation: none yet. The library changes are off by default and backwards compatible; a release
      can wait for the phase end (or v0.18.0 now if a sibling needs `analysis.index_pages`; none does).

## Steps 19f and 19g review (2026-10-28)

- **Session:** Opus 5.5, 2026-10-28 (addendum; 19e still pending). Scripts: `reports/data/phase-13-register-index-volumes/review/review_19fg.py`
  (read-only on every layer; outputs went to the scratchpad).
- **Verdict:** accept 19f and 19g. The code does what the briefs say and every logged number reproduces. **19f:** B2's failure
  is a fair negative (the one `unclear` that would flip it is not `right` under the rubric; two `right` calls are lenient), so
  the reorder stays out of v5 by default (UA-043). **19g:** the test is sound (Part A before any code, fresh sides disjoint
  from the spent 30 by side *and* volume, one fresh run), so the entry rule `SG_INDEX_ENTRY_V5B` ships in v5. A post-hoc
  diagnostic shows the entry rule does not depend on the reorder (F1 0.9242 on the 19f layout vs 0.9244), so UA-043 no
  longer gates anything but the reorder itself. 19e re-planned.

### Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| all | suite | 1,365 collected, 1,354 passed, 11 skipped | same | yes |
| 19f | pieces mode = 19b (3471, 3579) | equal | `layout/{regions,lines}` of `19f-piecescheck` `cmp`-equal to `19b-a` | yes |
| 19f | interleave | 399,429 / 843 (0.2111%) | same; `interleave_new.tsv` and `.ids.txt` `cmp`-equal | yes |
| 19f | no region added, ids | 707,748 rows in both; 0 duplicates | 707,748 / 707,748; 0 duplicate `layout_region_id` per scan | yes |
| 19f | reordered regions | 3,066 (1,696 flagged, 1,370 other) | same; `layout/pages` flags equal to `19c-*` in all 125 | yes |
| 19f | B2 | 35 / 4 / 1; flagged 18/2/0, other 17/2/1 | recounted from `b2_verdicts.tsv`: same | yes |
| 19g | selection X1–X4 | F1 0.8795 / 0.9027 / 0.8805 / 0.9040, X4 | same (19g's functions, scratchpad); `select_lines.tsv` `cmp`-equal | yes |
| 19g | fresh test (once) | X4 P 0.9530 R 0.8974 F1 0.9244; v4 hand 0.5009; E5 0.6277 | recomputed **without** the guarded script: from `fresh_lines.tsv`, from the production units of `19g-a/z`, and from `build_units_page` on `19c`: all three equal, 0 of 868 cells differ | yes |
| 19g | units elsewhere unchanged | 676,744 units on unflagged pages identical | same (rows without `unit_order`, segment rows), 0 inventories differ | yes |
| 19g | gold hashes | sha256 in `PROGRESS.md` | `fresh_gold.tsv` dd2313e0…, `fresh_sides.tsv` f1a86178…: equal | yes |

### Test integrity (19g)

- **Order** (19g's session transcript and file mtimes, UTC): listings of the 22 sides 10:59:17–11:01:00; labels written
  (`write_fresh_gold.py`, hand labels as literals) 11:02:00; hashes 11:02:08; Part A in `PROGRESS.md` 11:02:19; first code
  edit (`units.py`) 11:02:36; `select` 11:03:40; `fresh --variant X4` **once** 11:04:00 (`test_done.json`); presets edited
  after the test. `show_side.py` prints only line id, x, y, w and text (no type, unit or prediction).
- **Disjoint:** the 19 fresh volumes share none with the 30 spent volumes (and no side). Reserves: 108 (G1, not index) →
  121 and 109 (G3, unreadable) → 125, the first reserve of each group: by the brief's rule.
- **Leakage:** X4's cues and the no-cut guard were designed by the earlier review on the 30 spent sides, and the selection
  used only those; the fresh sides were drawn (10:51 UTC) before any 19g code existed. No feature reads a label.
- **Gold quality:** 106, 113, 117 read line by line against the listings: labels follow 19a's rubric (dated entries `S`,
  reference-list overflow `C`, merged lines by their first part, date-at-end entries in 4617). The 22 sides were read in
  about three minutes of wall time by one model reader; there is no human check of entry labels.

### Code notes

| # | Kind | File | Finding | Action |
|---|---|---|---|---|
| 10 | observation | `page_structure._reorder_regions_by_column`, `layout.layout_scan` | in reorder mode the returned count (reordered regions) is added to `n_column_splits`, so layout `stats.json` cannot tell reorders from splits | none (19e docs say so if v5 ships the reorder) |
| 11 | observation | `units.UnitParams.__post_init__` | `main_types` values are not validated (a typo silently turns nothing into a main); `cues` are | none |
| 12 | observation | `units.split_region` | on flagged sides `index`-typed lines become `main_text` units while `types/segments` keeps `index`: a reader that joins units with types sees both | 19e documents it |
| 13 | observation | `page_structure._reorder_lines` | follows the brief step by step (start-x classes, wide lines at their row in the left run, guards, unchanged = same object); `_column_geometry` is shared and pieces mode reproduces 19b byte for byte | none |

### Error analysis read by the reviewer

- **19f B2:** read cases 07, 10, 16, 22, 23, 25, 30 in full. The rubric is applied consistently. Case 23 (`unclear`) is not
  `right`: the text opens at the top of the *right* column (`Zijnde ter vergaderinge gecompareerd`) and the lower-left town
  list belongs to the right text, so no left-then-right order reads; the bar's fail does not hinge on it. 22 and 07 are
  `right` only because the rubric lets a `W` line carry the other column's text (22: 6 of 22 lines are margin + body
  merges, so the body is torn between the runs). Fixes for the two failure kinds are in the report (F13-R.10).
- **19g:** FN line types recounted from `19c` `types/segments`: 31 of 44 FN are typed `marginalia` (all no-unit FN), and
  they cluster: 25 in 3127 (10, two sides), 3265 (8, one side), 3280 (5), 3283 (2). Of the 9 FP in "two-column regions",
  7 sit in wide regions of 3–8 lines (110, 114, 119) that the reorder's guards keep whole, and 2 in a 40-line region of
  121 that the reorder does fix (1 FP). Diagnostics on spent gold (no bar): X4 on the 19f layout F1 0.9242; X4 with
  `marginalia` as mains fresh P 0.890, R 0.977 (F1 0.931): not a free fix.

### Brief quality

- **19f** (tags unspecified; zsh quoting; guard recompute on the layout region; `unclear` vs `wrong` when the text starts
  in the right column): later read bars define the "text opens in the other column" case as `wrong (b)`; the 19e brief
  quotes `'{workers}'`.
- **19g** (fresh row loader; layer → BASE mapping; `ref_before_lemma`; mid-line starts): the conventions 19g chose are
  recorded in the report and the fresh gold notes; the 19e brief names every layer mapping it needs.

### Updates applied

- [x] Phase report: *Review* (19f/19g part), *Follow-ups*, F13-R.9–R.12, statuses of F13-19f.* and F13-19g.*; still *draft*
- [x] `make reports` clean
- [x] Reviewer notes on UA-043 and UA-039; new UA-044 (Parquet reader for the optional silver check)
- [x] `step-19e-v5-build-and-run-kit.md` rewritten (gated on UA-039 and UA-043)
- [x] phase `PLAN.md` (steps 19f, 19g, 19e; P15–P17), `STATUS.md`, `PROGRESS.md`, `plan/PLAN.md` row, `plan/CONTEXT.md` trimmed
- [ ] Release recommendation: unchanged (after 19e; all new keys are off by default and backwards compatible)

## Step 19e review and phase close (2026-10-28)

- **Session:** Opus 5.5, 2026-10-28 (final). Script: `reports/data/phase-13-register-index-volumes/review/review_19e.py`
  (read-only on every layer; outputs in `review/19e/`). The 19e scripts were re-run from copies that write into the
  scratchpad, so no step file was overwritten.
- **Verdict:** accept 19e and close the phase. The v5 preset is the brief's code line for line (branch A), the run script's
  `--version` is correct and tested, every logged number reproduces, and the build equals the clones it was checked
  against. One defect in the run kit (the final audit of a full run did not know the v5 checks) is fixed in this review
  (a one-line change, below). The detector's false positives on rosters and account pages cost units, not text, on a
  small share of the layer (F13-R.13): acceptable for a v5 pilot; a guard is a follow-up with its own bar.

### Checklist (phase, final)

- [x] **Acceptance re-run** (table below), all steps; 19a–19d and 19f/19g in the sections above.
- [x] **AS tests:** 1,368 collected, 1,357 passed, 11 skipped (equal to 19e's log); after the review fix the run-script and
      modes tests again 35 passed. LotA untouched (no sibling edit), not run.
- [x] **Error analysis** in every step section of the report; the review's own for 19e is in the report's *Review*.
- [x] **Phase report final**, findings in `reports/index.md`, `make reports` clean.
- [x] **Changelog:** *Unreleased* has 19b, 19c, 19d, 19f, 19g, 19e; each matches the diff (every new public name of
      `git diff archival_structures` is listed: `HAND_COLUMN_SPLIT_DEFAULTS`, `split_regions_into_columns`,
      `LayoutParams.hand_column_split`/`index_sides`, `analysis.index_pages`, `layout.PAGE_COLUMNS` and `layout/pages`,
      `units.INDEX_CUES`/`INDEX_ENTRY_DEFAULTS`/`entry_cues`, `index_side` arguments, `UnitParams.index_entry`, the presets,
      the run script's `--version`). "Affected sibling imports: none" is right for all six. The review's `AUDIT` change is
      added to the 19e entry.
- [x] **No archive knowledge in core:** unchanged since the first part; the v5 values live in `presets.py`.
- [x] **Leakage:** none new in 19e (it scores spent gold without a bar and uses the silver layer for scoring only).
- [x] **v1–v4 identical; v5 print = v4:** the 106 print inventories equal v4 in every per-inventory file (`scans` differs
      only in `member`, the input path); the hand files upstream of `units` equal v4; v1–v4 presets unchanged (test).
- [x] **Gold quality:** UA-039 (a): 27 of 27 judged sides agree; 3 `unsure` (below).
- [x] **Silver check:** done (19e, UA-044 (a)); reproduced.
- [x] **Brief defects** answered (below).

### Acceptance, re-run (19e)

| Criterion | Logged | Re-run | Match |
|---|---|---|---|
| suite | 1,368 / 1,357 passed / 11 skipped | same | yes |
| `--check-only --version v5` (local sources, scratchpad out/work dirs) | 231 inventories, no duplicate | 231 found, 231 to build, 2,772 MiB predicted | yes |
| UA-045 commands parse (placeholder paths, `parse_args` + `build_cmds` only; the drive was not touched) | – | all three parse; `--preset sg_1.01.02_v5`, label `v5-1.01.02`; `--version v5 --only 1.10.94` exits 2 | yes |
| `compare_v4.py` | print 106 equal; hand equal through `types`; units equal 19g | `compare_v4.json` `cmp`-equal | yes |
| audit | 4,196 pass, 47 reported, 0 judged; 4 v5 checks pass; 8,873 flagged | same counts; the 4 v5 invariants pass | yes |
| metrics a–d | 728 / 481,391; 8,873 sides (3,648 gap fill); 80,344 → 162,021; 19a P 0.8768 R 0.9329, 19g P 0.9530 R 0.8974 | all `metrics_*` files `cmp`-equal | yes |
| silver | 18 of 34,428 | `silver.json`, `silver_sides.tsv`, `silver_reads.txt` `cmp`-equal | yes |

### Findings (19e and phase close)

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 14 | defect (run kit) | `scripts/run_text_layer_v4_full.py` `AUDIT` | A full v5 run ended with the 15w audit, which does not know the v5 invariants (`layout/pages` per hand inventory, duplicate region ids, `hand:index*` only on flagged sides, no `index` unit on a flagged side), so UA-045 would report "0 judged" without checking them | **fixed in review** (one line): `AUDIT` = the 19e copy, identical for v4 layers; docstring and changelog updated; self-test ok; the two audits took 223 s and 224 s on `19e-a`, so `AUDIT_S_PER_INVENTORY` stands |
| 15 | defect (UA text) | `docs/USER_ACTIONS.md` UA-045 | the smoke command writes to `<drive>/layer_v5_smoke`, which must exist (the script refuses a missing `--out-dir`), but the UA names only `layer_v5` and `layer_v5_inputs` | reviewer note on UA-045 |
| 16 | observation | `scripts/run_text_layer_v4_full.py` | a v5 run still logs to `run_v4.log` and the probe file is `.write_test_v4` (documented in the docstring) | none |
| 17 | process | `step-19e/silver.py` | the silver check reads `shared-data/…/republic_segmentation_v1/` by absolute path: no link under `data/` and AS is not in the catalogue row's *used by* (shared-data protocol §3, CLAUDE.md) | recorded in `plan/CONTEXT.md` §5; the next step that uses it links it |
| 18 | observation | `step-19e/` | the brief's `user_check.py` deliverable does not exist; the UA-039 comparison is recorded in the report and in `USER_ACTIONS_resolved.md` and reproduces (27/27) | none |
| 19 | observation | `docs/text_layer.md` | the uncommitted diff also holds a paragraph on `republic_segmentation_v1` for 1.10.94 that comes from another session, not from phase 13 | none (the user stages it with phase 13 or separately) |

### Error analysis read by the reviewer (19e)

Details and numbers in the report's *Review* (F13-R.13–R.16).
- **Damage outside index pages** (`review_19e.py damage`): every detector-flagged side known not to be an index page (GT
  `resolution_page`, silver negatives, 19a/19g reads, and the flagged sides of 11 non-index volumes the reviewer read):
  54 sides in 18 of the 125 volumes (30 by the rule, 24 by gap fill): rosters, accounts, letters, running text, garbled
  scans. Their `main_text` units go 228 → 1,091 and 207 v4 `index` units are re-cut into mains; 99 of the 228 v4 units are
  split. That is 0.6% of the flagged sides and 0.18% of all hand `main_text` units; no text is lost or moved.
- **Gap fill** (`gapfill`, seed 20261130, pool 658 sides in 17 volumes outside the GT and the entry gold): 17 of 20 are
  index pages, 2 hold only a page number, 1 is an account page (8706). The gap fill recovers one-column dated-summary
  register indexes (3463, 3472) whose sides the rule cannot measure.
- **The 3 `unsure` sides** (17, 23, 54): read with the user's notes; they are a definition question (dated summary
  indexes without page numbers, a side whose locators the HTR lost), not a disagreement. D2 over the 57 sides the user
  judged: 52/57 = 0.912.
- **Remaining interleave:** 722 of the 728 interleaved units are on unflagged sides (equal to v4 there); on flagged sides v4
  had 1,890, v5 has 6.

### Brief quality (19e)

- Expected interleave was v4's number (a planner error: the expectation was not measured on the clone the step
  reproduces); `--check-only` needs `--out-dir` to exist; `scans.member` holds input paths; the metric (b) groups were
  not listed; `hand:index:header` exists; the run kit's audit. CONTEXT §5 already says "measure each expected number on
  the layer the step starts from"; it now adds "a run kit's final audit must know the new layer's checks" and "read
  shared data through a link under `data/`".

### Updates applied (final)

- [x] Phase report: *Summary*, *Review* (19e part, F13-R.13–R.16), *Follow-ups*, every finding status; status *final*
- [x] `reports/index.md`: phase row and the phase's findings appended (with Edit)
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed (the one defect is a one-liner, fixed here)
- [x] `docs/shared_library/changelog.md` complete for the phase (19b–19g, 19e; the review's `AUDIT` line)
- [x] phase `PLAN.md` *Hand-off*; `plan/PLAN.md` row; `plan/CONTEXT.md`; phase `STATUS.md`
- [x] Reviewer notes on UA-045 and UA-046
- [x] Release recommendation: **v0.18.0** (minor: new public API, all off by default, plus the v5 preset and run kit)
