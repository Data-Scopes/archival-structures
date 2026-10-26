# Phase 12 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning (Opus 5.5, 2026-10-23)
- **Seeded** from UA-022 = (a) (a pre-registered box-height test on a fresh sample) and the phase-10 hand-off
  (F10-R.3, F10-R.5). Same session as the phase-10 review.
- **User decision (chat, 2026-10-23):** fit the cut on the 76 labelled volumes (14 dev, 455/456, the 60 spent
  phase-10 sample), not on the 14 dev volumes only; this replaces phase-10 P4's "never fit on silver" (P2 here). No
  leakage: the test sample is fresh and disjoint.
- **Drawn:** `reports/data/phase-12-hand-print-box-height/planning/test_sample.tsv` (`draw_sample.py`, seed
  20261212): 60 silver volumes in the phase-10 strata (10/20/10/10/10; pools 112/400/89/54/64 after exclusions; non-silver pool 8,301) + 5
  ordered reserves each; 20 non-silver diagnostic volumes + 5 reserves. 0 overlap with the 60 spent, the 12 UA-017
  or the dev volumes; none already staged. Strata = EAD `ancestor_3`; pool sizes reproduce 123/421/99/64/76.
- **Checked:** feature definition = `h_by_volume.py` (F10-R.3), so the planning cut 87.75 reproduces by
  construction; cached `h_med` exists only where `hrel` does (rule P1 says so); `extract_scan_v2`'s scan row has
  `page_w`/`page_h`; `protect.py` does not cover the 15q caches (brief adds a sha256 list).
- **Fact for the nouvelles question:** the EAD series is *Gedrukte nouvelles van Nederlandse gezanten in den
  vreemde* (printed reports of envoys), which fits the letter-like text and F10-R.4.
- **Written:** `PLAN.md` (P1–P8), `step-15u-box-height-test.md`, `STATUS.md`, report skeleton; UA-023; rows in
  `plan/PLAN.md` and `reports/index.md`. 15v/15w are written only after a pass.
- **Next:** UA-023, then 15u (Sonnet).

## 2026-10-25: UA-023 resolved (Opus 5.5)

- **User:** UA-023 (a), staged. **Checked:** all 110 tars from `test_sample.tsv` exist in
  `shared-data/NL-HaNA_1.01.02/pagexml/`, are readable (`-rw-rw-r--`, 2.96 GB) and open as tar archives. No sample tar
  is missing, so no reserve stands in. No test geometry has been read: 15u starts from a clean slate.
- **Next:** 15u (Sonnet).

### Step 15u, part B frozen
- **Session:** Sonnet 5.5, 2026-10-25. Acceptance 2 holds: all 76 fit values equal `h_w` of `h_by_volume.tsv` (max |diff| 0.0), max print 86.0 (12135), min hand 89.5 (3492), cut = 87.75, fit agreement 76/76, 455 = 75.0, 456 = 82.5; fit set disjoint from the 110 test/diagnostic volumes.
- **`cut.json`:** `reports/data/phase-12-hand-print-box-height/step-15u/cut.json`, sha256 `ce58c3338549239f80fe891803091d08cfff2a3ff8f3e2ba4bcf5caf83ccb572`, mtime 2026-10-25T16:48:30 (recorded at 16:48:36). `data/hand_print_geom/15u-a/` did not exist at this point.
- Rule, cut and features are frozen from here on. Protect baselines: `step-15u/protect_before.txt`, `protect15q_before.txt`.

### Step 15u: box-height test
- **Session:** Sonnet 5.5, 2026-10-25. **Status:** done (stop rule passed)
- **Files changed:** `archival_structures/analysis/hand_print.py`, `tests/test_analysis_hand_print.py`, `docs/shared_library/changelog.md`, `reports/phase-12-hand-print-box-height.md`, `reports/data/phase-12-hand-print-box-height/step-15u/` (`validate.py`, `analysis.py`, `cut.json`, `fit_volumes.tsv`, `substitutions_{test,diagnostic}.tsv`, `{test,diagnostic}_volumes.tsv`, `{test,diagnostic}_scans.tsv.gz`, `stop_rule.json`, `verdicts.tsv`, `hrel_comparison.tsv`, `protect*_{before,after}.txt`), `data/hand_print_geom/15u-a/` (new cache), `plan/phase-12-.../{PROGRESS,STATUS}.md`
- **Public API:** `BoxHeightParams(cut, min_region_chars=100)`, `volume_box_height`, `scan_box_heights`, `fit_box_height_cut`, `box_height_mode`, `BOX_HEIGHT_METHODS`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` -> 1,215 passed, 11 skipped (1,226 collected); `test_analysis_hand_print.py` 13 passed
- **Acceptance:** 1 ok. 2 ok (76/76, max |diff| 0.0, cut 87.75). 3 ok (sha256 above, cut.json mtime 16:48:30 before first cache file). 4: 60 volumes, 0 substitutions, 0 parse failures. 5: weighted agreement print 1.000, hand 1.000, pass; all strata 100%. 6: 20 diagnostic volumes, all `hand` (111.0-173.0). 7: protected data unchanged (diffs identical).
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15u](../../reports/phase-12-hand-print-box-height.md#step-15u-box-height-test); findings F12-15u.1-7
- **Error analysis:** no disagreements; 6 near-cut volumes (margin -1.25 to -4.75) and 95 minority scans from 19 volumes are printed (6 scans OCR junk), none handwritten. 16 of 30 test print volumes have page_w below the fit print range (plakkaten about 4000 px) and still pass.
- **Deviations from brief:** substitution files are per set (`substitutions_test.tsv`, `substitutions_diagnostic.tsv`, both empty); the brief names one `substitutions.tsv`. Fit-set resolution covers only the 16 volumes with a `15m-a` scan table (the brief's source), not the 60 phase-10 sample volumes.
- **Brief defects:** none that blocked the step.
- **Open questions for the user:** none

## 2026-10-25: Review of 15u (Opus 5.5)

- **Verdict:** accepted; stop rule passed (60/60; weighted 1.000 / 1.000). Script:
  `reports/data/phase-12-hand-print-box-height/review/review_15u.py`.
- **Re-run:** all 80 volume values recomputed from the 15u caches without the library (0 mismatches); stop rule
  reproduces; `fit` re-run byte-identical, `cut.json` sha256 unchanged; suite 1,215 passed, 11 skipped.
- **Leakage:** fit reads only the 15q caches; `cut.json` mtime precedes `data/hand_print_geom/15u-a/`; my own pre-launch
  sha256 snapshot of the 15q caches, phase-10 data/report and `text_layer/` is unchanged.
- **Reads:** 12166, 12122, 3193 confirm the agent's verdicts (minority scans are printed type; 3193 is handwriting
  with some regions at 80-86 px).
- **Report:** F12-15u.5 revised (page width mixes page size and DPI); F12-R.1 (normalising by page width breaks the
  separation; one scan scale assumed), F12-R.2 (about 5 of 783 silver volumes expected on the wrong side, mostly
  early *eerste serie* -> `print`). Section *Review of step 15u*; two follow-ups for 15w.
- **Tidied:** `hand_print.py` docstring (title line; `hrel` status scoped; box-height status); stray `__pycache__` in
  `step-15u/` removed. Index rows for F12 findings wait for the phase review.
- **Next:** an Opus session writes 15v/15w from the phase-10 briefs, adding the audit list from the review.

### Planning 15v/15w (Opus 5.5, 2026-10-25)
- **Read:** plan README/PLAN/CONTEXT, phase PLAN/STATUS/PROGRESS, the 15u brief and report, phase-10 PLAN (steps,
  decisions, hand-off), REVIEW, the unrun briefs 15r/15s. Code anchors checked by an Explore subagent (AS + LotA).
- **Code checks against the phase-10 briefs:** anchors in `presets.py`, `line_split.py`, `build.py`, `extract_v2.py`,
  `io.py` unchanged since 2026-10-23 (`build.py` 813 lines; `stage_units` republic block now L444–454). The six rule
  fields re-measured by `asdict` (same six). New: `prepass_rows` must return **raw** rows, because 15u's values come
  from raw `extract_scan_v2` rows and `fit_split_model` applies `_int_row` (P10). `stage_units` passes a
  `republic_path` to every inventory, so mode runs null it for hand ones. LotA reads no `params.json`, manifest
  `*_params` or `mode/`. No discovering audit exists (15o's manifest script also hard-codes the lists).
- **Data checks:** shared-data 1.01.02 `pagexml/` holds 188 tars (5.0 GB) = 6 pilot + 182 staged; `print_paragraphs_v1`
  covers 3760–3864 (the 6 pilot and all 25 staged *gedrukte resoluties*); 455 crosswalk 27,033 exact / 7 unmatched;
  146 GB free. Expected 15w modes from the 168 reference values: 70 `print` in 1.01.02 (incl. 3463 silver hand 86.5,
  9486 no silver 79.0), 96 `hand`; 39 1.01.02 volumes + 456 within ±10 px.
- **Settled (no user decision needed):** P9–P14 in `PLAN.md` (rule/params, raw rows, carried-over design, crosswalk bar
  18/20, scope 198 volumes, read lists). User decisions come at the end of 15w as UA-024/UA-025.
- **Written:** `step-15v-volume-mode-build.md`, `step-15w-v4-build-and-run-kit.md`; `PLAN.md` (steps, P9–P14, hazards,
  owns, checklist); `STATUS.md`; report header; `plan/PLAN.md` row 12.
- **Next:** 15v (Sonnet).

### Step 15v: volume-mode build (v4)
- **Session:** Sonnet 5.5, 2026-10-25. **Status:** done (acceptance 1-3, 6 met; 4 and 5 measured, outside the 455/456 range, explained; crosswalk bar failed)
- **Files changed:** `archival_structures/text_layer/{presets,line_split,build}.py`, new `archival_structures/text_layer/modes.py`,
  new `tests/test_text_layer_modes.py`, `docs/text_layer.md`, `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-026, UA-027),
  `reports/phase-12-hand-print-box-height.md`, `reports/data/phase-12-hand-print-box-height/step-15v/` (scripts, tables, verdict samples,
  protect snapshots), data `data/text_layer_v2_dev/15v-a/{1.01.02,1.10.94}`
- **Public API:** `LayerPreset.mode_presets`, `.volume_mode` (last fields, default None); `presets.MODE_RULE_FIELDS`, `BOX_HEIGHT_V4`,
  `inventory_preset(preset, mode)`, `SG_1_01_02_V4`, `SG_1_10_94_V4`; `line_split.prepass_rows(inv, source, preset)`;
  `modes.decide_mode/write_mode/read_mode/extract_inventory_v2_modes`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests` -> 1,222 passed, 11 skipped (1,233 collected; 7 new); line_split 14/14
- **Acceptance:** 1 pass. 2 pass: 0 unexplained (hand 8 inventories all files equal; 455/456 48 equal + 3 params.json; six printed 93 differ + 18 new).
  3 pass: 16/16 values equal 15u, modes as expected, 455 = 75.0, 456 = 82.5. 4 split candidates 0-218 vs 11,515/64,299, units/page 43.6-70.1 vs
  33.6-38.8 (outside, explained), rest inside. 5 exact 0, near 5-62, unmatched 99.4-99.9% vs 455 exact 27,033. 6 pass (only shared-data changed, by another project)
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15v](../../reports/phase-12-hand-print-box-height.md#step-15v-volume-mode-build-v4); findings F12-15v.1-6
- **Error analysis:** crosswalk bar 2/20 strict (20/20 lenient) -> not usable; unmatched 10: 8 same text with offset box, 2 other line. 3771: 20/20 splits right,
  10/10 merges coherent, 15 units 9 right/4 wrong/2 unclear; 10 pages vs hand build: 8 worse, 2 mixed, 0 better (print unit rules cut paragraphs after the drop-cap lines)
- **Deviations from brief:** column share measured over lines with text (the brief's 455 0.992 / 456 1.000 reproduce only that way); one scripted `sed` fix of 3 numbers in the report (own rows)
- **Brief defects:** (1) "share of lines with a `column` value" did not say over which lines; (2) the compare command needs an inventory-level explanation switch, added as
  `--explained-invs` to `compare.py` (the brief only named `--sub-map`, `--ignore-paths`); (3) the `protect.py` before/after diff is not clean when other projects write to shared-data (acceptance 6 should name that); (4) the `Hou.`-type unit over-segmentation was not anticipated by the brief (see UA-026)
- **Open questions for the user:** UA-026 (unit rules for printed 1.01.02), UA-027 (reading of the crosswalk bar)

## 2026-10-25: Review of 15v (Opus 5.5)

- **Verdict:** accepted. Suite 1,222 passed, 11 skipped; crosswalk stats and `mode/3771.json` as reported; my pre-launch
  sha256 snapshot (15m-a, 15n-a, 15o-pub, `data/hand_print_geom/`, 15u and phase-10 data, untouched `text_layer`
  modules, `hand_print.py`) unchanged. `build.py`/`presets.py` diff read: non-mode paths unchanged, hand inventories
  get no `republic_path` in mode runs.
- **Reads:** 3771 (0394, 0418), 3822 (0080, 0168), print vs hand units. The drop-cap cut holds, but the print rules
  also separate decisions and *Dat …* clauses that the hand rules lump: F12-15v.4 revised, F12-R.3 (open issue).
- **Decided:** the crosswalk bar meant the line-level reading (20/20 right), but coverage is 0.5%, so the crosswalk is
  unusable either way: no `--republic-dir` for 1.01.02 in 15w. UA-027 withdrawn (moved to resolved, no Response).
  UA-026 options rewritten (hand `unit_params` would not give the hand rules; `print_mode` decides that).
- **15w brief updated:** no 1.01.02 REPUBLIC; the extra expected differences against `15v-a` (republic_lines,
  crosswalk keys); comparer flags; test count; the known unit issue in UA-024; Edit-only for shared files (15v used
  `sed` and `cat >>`).
- **Next:** 15w (Sonnet).

## 2026-10-25: UA-026 answered (Opus 5.5)

- **Response:** (a) now and (b) as the follow-up.
- **Recorded:** decision P15 in `PLAN.md`; new step row 15x (drop-cap-aware print unit rule, brief written by Opus
  after 15w); the 15w brief now says UA-026 (a), adds a *Known issue* paragraph to its `docs/text_layer.md` row and
  names 15x in the full-run UA. UA-026 moved to `docs/USER_ACTIONS_resolved.md`.
- **Next:** 15w (Sonnet), then the 15x brief (Opus).

### Step 15w: v4 dev build, audit, run kit
- **Session:** Sonnet 5.5, 2026-10-25. **Status:** done
- **Files changed:** `scripts/run_text_layer_v4_full.py` (new), `tests/test_run_text_layer_v4_full.py` (new),
  `reports/data/phase-12-hand-print-box-height/step-15w/` (audit.py, values_check.py, compare16.py, json_keydiff.py,
  cost.py, read_scans.py, audit_failure_rows.py and their outputs), `data/text_layer_v2_dev/15w-a/`, `15w-smoke/`,
  `data/text_layer_v4_staged/NL-HaNA_1.01.02/` (182 symlinks), `data/text_layer_v4_full_inputs/` (links),
  `docs/text_layer.md`, `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-024, UA-025), `.gitignore`,
  `reports/phase-12-hand-print-box-height.md`, `STATUS.md`
- **Public API:** none (script and audit only)
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` -> 1,235 passed, 11 skipped (1,246 collected; before
  1,233 / 1,222); the two run-script files 21/21
- **Acceptance:** 1 ok; 2 168/168 values equal, modes 111 hand + 87 print (168: 96 hand, 70 + 2 print), `15w-a` = `15v-a` up
  to listed paths; 3 audit `15w-a` 3,412 pass / 75 fail (all reported shares), `15m-a` 178 / 4 same four; 4 prediction (400 GB
  XML: about 42 GiB, 2.3-7 h at 8 workers) and UA-024/025 written; 5 protected data identical (shared-data changed by others)
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15w](../../reports/phase-12-hand-print-box-height.md#step-15w-v4-dev-build-audit-run-kit); findings F12-15w.1-9
- **Error analysis:** 51 band volumes (147 scans): 50 right, 1 wrong (3463, handwritten register called `print`); 160 minority scans:
  135 printed, 15 hand (in hand volumes), 6 unclear (probably handwritten tables of contents); 20 print-rule units: 10 ok, 3 bad boundary
- **Deviations from brief:** the merge-share invariant stays in the audit with `reported: true` (so `15m-a` reproduces 178/4)
- **Brief defects:** UA-024/025 free although 027 is the highest; band is 51 not 40 with the reserves; `compare.py` needed a
  wrapper for 198 vs 16 volumes
- **Open questions for the user:** UA-024 (full run), UA-025 (publish the pilot)

## 2026-10-25: Phase review (Opus 5.5)

- **Verdict:** accept with follow-ups ([REVIEW](REVIEW.md)); one defect → step 15wr. Report *final*, addenda 15wr/15x to come
  (as phases 07 and 09 handled post-review steps).
- **Re-run:** AS 1,235 passed, 11 skipped (1,246); LotA 381 passed, 1 xfailed; 168/168 values (own join); modes 111/85/2;
  `15w-a` vs `15v-a` 258/33/6 and 55/2/0; audit 3,412/75 (0 judged) and `15m-a` 178/4; self-test ok; `cut.json` unchanged.
- **Found (F12-R.4, defect):** the print splitter cuts ordinary lines in the single-column print series (*nouvelles*
  885,967 splits, *plakkaten* 169,527, 9486 4,745; 46/46 sampled wrong); the halves land in separate layout columns.
  F12-15w.7's "interleaved columns" are this. Column-start share separates the layouts (0.496–0.501 vs ≤ 0.225).
- **Found:** F12-R.5 (the near-cut hand volumes are EAD *index* volumes; ~9 of 251 expected `print` at full scale),
  F12-R.6 (drop-cap cuts = 44.9% of print cuts, 1.1% of decision/*Dat* cuts), F12-R.7 (audit passes on wrong splits).
  Revised: F12-R.2, F12-15v.3, F12-15w.5, .7, .8. Accepted deviation: the `reported: true` share.
- **Written:** `REVIEW.md`; report *Summary*, *Review*, *Follow-ups*, statuses; 30 rows in `reports/index.md`; `make reports`
  clean; `step-15wr-review-fixes.md` (split gate, P16/P17) and `step-15x-drop-cap-units.md` (P18/P19), with read lists
  drawn now (`review/reads_15wr.tsv`, `reads_15x.tsv`); P16–P20, hand-off; `plan/PLAN.md` row; `plan/CONTEXT.md`
  (rewritten to 145 lines); UA-028; review notes in UA-024/UA-025. Review scripts in `reports/data/…/review/`.
- **Release:** hold v0.15.0 until the 15x review.
- **Next:** 15wr (Sonnet), then 15x (Sonnet).

### Step 15wr: split gate for single-column print volumes (review fix)
- **Session:** Sonnet 5.5, 2026-10-25/26. **Status:** done
- **Files changed:** `archival_structures/text_layer/modes.py`, `presets.py`; `tests/test_text_layer_modes.py`; `reports/data/phase-12-hand-print-box-height/step-15w/audit.py`; `scripts/run_text_layer_v4_full.py`; `docs/text_layer.md`; `docs/shared_library/changelog.md`; `docs/USER_ACTIONS.md` (UA-024 evidence); `reports/phase-12-hand-print-box-height.md`; new `data/text_layer_v2_dev/15wr-a/`, `reports/data/phase-12-hand-print-box-height/step-15wr/`
- **Public API:** `modes.column_start_share`; `presets.SPLIT_GATE_V4`; `sg_1.01.02_v4` behaviour (split gate)
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,237 passed, 11 skipped (1,248 collected)
- **Acceptance:** 1 ok; 2 53 gated / 32 split, max gated share 0.226, min split 0.415; 3 hand, 455/456, 1.10.94 equal, 32 split differ only by `split_gate` key, 53 gated explained; 4 0 `split_from` in gated, audit 3,593 pass, 0 judged fail; 5 read bar 19/20 passed, protected data unchanged (only an external LotA gold file moved)
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15wr](../../reports/phase-12-hand-print-box-height.md#step-15wr-split-gate-for-single-column-print-volumes-review-fix); findings F12-15wr.1–4
- **Error analysis:** 20 scans read: 19 in order, 1 unclear (garbled OCR), 0 half lines; 4 of 5 random gated lines were split before, whole now; 3771 unchanged.
- **Deviations from brief:** test uses `min_chars: 1`; random-line sample drew from all lines of gated volumes (1 of 5 was not split before)
- **Brief defects:** fixture lines shorter than min_chars; 885,967 counts splits but `split_from` rows are 2 per split; compare stats must ignore `seconds`
- **Open questions for the user:** none

### Review of step 15wr (Opus 5.5, 2026-10-26)
- **Verdict:** accept ([REVIEW § Step 15wr review](REVIEW.md#step-15wr-review-2026-10-26)); no fixes brief.
- **Re-run:** AS 1,237 passed, 11 skipped; LotA 381 passed, 1 xfailed; 53 gated / 32 split from `mode/*.json`; own
  decompressed-byte compare vs `15w-a`: hand, 455/456, 1.10.94 equal, 32 split differ only in `mode/<inv>.json`
  (`split_gate` key); 0 `split_from` in 1,947,176 gated lines; audit 3,593 / 32 reported / 0 judged, self-test ok;
  protected hashes as the step reported.
- **Reads:** all 20 bar units in full: 20 in order on my reading (12226 too); the count stays 19/20, so the bar holds. 5 formerly
  split lines (split lines only, seed 20261030) whole and right; 3463's 12 sampled splits: 8 cut a phrase, 0 clearly right.
- **Findings:** F12-15wr.2, .4 confirmed; .1 (share gap 0.0076, not 0.001), .3 (2,120,486 rows) revised; new F12-R.8
  (re-run), R.9 (3463 splits wrong; P21), R.10 (derived WBC sets recovered `-`). Report: *Review of step 15wr* in
  § Review, findings table, Follow-ups, Summary addendum; `reports/index.md` rows; `make reports` clean.
- **Fixed in review:** `test_extract_split_gate` gains the unmeasurable-share case (no test reached `share is None`);
  changelog entry gives the return value; UA-024 review note (the "interleaved columns" were the splits).
- **15x brief** corrected in place (Rule 10): anchors, `gate.tsv` layout, `15wr-a` facts, the build and protect commands
  written out, test baseline, compare/audit notes, full-unit reads, diagnostic wording. Bars, seeds, read lists unchanged.
- **Next:** 15x (Sonnet). No new UA.

### Step 15x: drop-cap-aware print unit rule
- **Session:** Sonnet 5.5, 2026-10-26. **Status:** done
- **Files changed:** `archival_structures/text_layer/units.py`, `presets.py`; `tests/test_text_layer_units.py`, `test_text_layer_modes.py`; `docs/text_layer.md`, `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-029); `reports/phase-12-hand-print-box-height.md`; `reports/data/phase-12-hand-print-box-height/step-15x/` (scripts, compare, audit, `suppressed.tsv`, `verdicts.tsv`, `pages.tsv`, read files); `data/text_layer_v2_dev/15x-a/`
- **Public API:** `UnitParams.drop_cap_lines`; `LayerPreset.mode_overrides`; `inventory_preset` applies overrides; `sg_1.01.02_v4` print units
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests` → 1,240 passed, 11 skipped (1,251 collected)
- **Acceptance:** 1 ok; 2 hand, 455/456, 1.10.94, `mode/*` equal to `15wr-a`, 85 print volumes differ only in 7 allowed files each, corpus only params/stats/wbc; 3 audit 3,592 pass / 33 reported / 0 judged (12118 share 0.9541 to 0.9496); 4 bar A 59/60 (0.983), bar B 37,897/38,176 (0.9927), bar C 19 better, 1 same, 0 worse: all pass, stop rule not fired
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15x](../../reports/phase-12-hand-print-box-height.md#step-15x-drop-cap-aware-print-unit-rule); findings F12-15x.1–4
- **Error analysis:** 60 dropped cuts and 20 pages read (every differing unit in full): 1 wrong cut (a decision merged into a page-top tail); lost decision/*Dat* starts 279 (0.7%); diagnostic nouvelles 7/10, plakkaten 6/10 (4 unreadable)
- **Deviations from brief:** none
- **Brief defects:** "units per page" undefined (all unit types per scan used for the table); audit baseline 32 reported became 33 (12118)
- **Open questions for the user:** UA-029

### Review of step 15x (Opus 5.5, 2026-10-26)
- **Verdict:** accept ([REVIEW § Step 15x review](REVIEW.md#step-15x-review-2026-10-26)); no fixes brief. **Phase done.**
- **Re-run** (own `review/review_15x.py`): AS 1,251 / 1,240 passed / 11 skipped; LotA 381 passed, 1 xfailed; byte compare vs
  `15wr-a`: hand 2,109, 1.10.94 57, `mode/` 594 equal, 85 × 7 print files differ, corpus params/stats/wbc pooled only;
  179,412 suppressed; bar A sample = `sampleA.tsv` (seed 20261029), B 37,897/38,176 (segment and line text), C 19/1;
  diagnostic draws identical; audit 3,592 / 33 / 0 judged; `protect2` unchanged.
- **Reads:** 21 bar-A cuts + 5 pages in full (20 right, 1 wrong; 5 better); all 210 cuts on the 20 scans: 206 right;
  30 lost bar-B starts (10 short pass entries, 8 decision after a 1–2-line tail, 7 *Dat* clauses, 5 after junk);
  30 more per single-column series (seed 20261031): *plakkaten* 21/1/8 unreadable, *nouvelles* 23/4/3; 12118 traced.
- **Findings:** F12-15x.1, .2 confirmed; .3, .4 revised; new F12-R.11–R.15. Report *Review of step 15x*, Summary
  addendum, Follow-ups, findings table; 9 rows in `reports/index.md`; `make reports` clean.
- **Fixed in review:** changelog note (`asdict` keys); `docs/text_layer.md` *Known issues* (precision per series).
- **UAs:** review notes in UA-029 (recommendation (b) → (a), costs per option, new option (d)), UA-024 ((a) now, after
  the release), UA-025 (`15x-a`). No new UA.
- **Written:** phase `PLAN.md` (status, 15x row, hand-off), `plan/PLAN.md` row 12, `plan/CONTEXT.md`, `STATUS.md`.
- **Release:** v0.15.0 now (15q–15x). **Next:** the user's UA-024 run; then plan the P21 pin step.

## 2026-10-26 — full-run check and `types` fix (Opus 5.5; implementation Sonnet)

- **UA-025/030:** pilot published as `15x-a` (1.01.02) + `15w-a` copy (1.10.94, data identical); shared-data README rows; P23.
  UA-029 (a): P22.
- **Full run (UA-024), staged files in `data/text_layer_v4_full/`:** incomplete. extract and layout done for 9,122
  (278 `print`, 8,844 `hand`); `types` failed for 6 (1, 9302, 11102, 11103, 11191, 11265), each with one unparseable
  `doc.xml` in its tar (extract `n_errors` 1); `units` then exited ("run types first"), so no units, wbc, join, no
  1.10.94 and no `audit.json`.
- **Cause:** a `parse_error` scan row has an empty `page_w`; `type_inventory` did `int(r["page_w"])` (`types.py:368`);
  layout already used `int(... or 0)`. **Fix:** the same guard; no output change for other inventories (the row has
  no lines). Test `test_stages_survive_parse_error_scan` (fails before, passes after; layout → join run on the fixture
  with the bad scan). Suite 1,241 passed, 11 skipped. Changelog *Unreleased*.
- **Next:** user releases v0.15.1, reruns the UA-024 full-run command (resumes: 6 types, then units, wbc, join,
  1.10.94, audit) and stages `audit.json` + updated `status.tsv`, `manifest.json`, `run_v4.log`.
