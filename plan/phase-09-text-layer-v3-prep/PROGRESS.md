# Phase 09 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning
- **Session:** Opus 5.5, 2026-10-21. **Status:** done (planned; nothing implemented)
- **Seeded:** no phase dir existed; created from `plan/templates/` with the follow-ups of
  `plan/CONTEXT.md` §4. Slug `text-layer-v3-prep` (user; the prompt's `pepare_text_layer_v3` was a typo).
- **Code checked** (3 Explore agents, 2026-10-21): current anchors for `page_structure.py`,
  `layout.py`, `types.py`, `units.py`, `join.py`, `join_v2.py`, `build.py`, `presets.py`; full suite
  1,166 collected; pagexml-tools 0.9.0 installed, 0.9.1 not on PyPI; no sibling imports any name
  this phase changes; AS at v0.13.0 (changelog has no *Unreleased*).
- **Measured on `15ir-a`** (scripts and outputs in `reports/data/phase-09-text-layer-v3-prep/planning/`):
  print line order per class and the simulated row sort (`order.py`); interleave bridges
  (`bridge.py`); page-number segments by band, shape and gap (`pn_offchain.py`); merge right sides
  (`merges.py`); crosswalk match × role (`crosswalk.py`). Planning findings F09-P.1–P.7 in the report.
- **User decisions** (2026-10-21): slug; print `unchanged` regions sorted and flagged, hand kept
  (P2); phase ends with a dev layer and run kit, no publication (P1); `g_upper_right` bar
  27/40 per sub-layer (P3).
- **Planner decisions:** P4 (row key 0.2 × median h), P5 (bridge not fixed), P6 (gap rule +
  per-band bars), P7 (role column), P8 (no REPUBLIC default), P9 (lead marks, 455 `J` pin),
  P10 (from-scratch layer per step), P11 (v3 presets), P12 (pin only if released).
- **Steps:** 15j, 15k, 15l, 15m (four briefs, 173–206 lines each).
- **Files:** `plan/phase-09-text-layer-v3-prep/{PLAN,STATUS,PROGRESS}.md`, four `step-*.md`;
  `reports/phase-09-text-layer-v3-prep.md` (draft, *Planning* section filled);
  `reports/data/phase-09-text-layer-v3-prep/planning/` (8 scripts + outputs); row 09 in
  `plan/PLAN.md` and `reports/index.md`; `plan/CONTEXT.md` §4 pointer.
- **Open questions for the user:** none.

### Step 15j: row-tolerant line order
- **Session:** Sonnet 5.5, 2026-10-22. **Status:** done
- **Files changed:** `archival_structures/analysis/page_structure.py`, `archival_structures/text_layer/{layout,presets}.py`,
  `tests/test_analysis_page_structure.py`, `tests/test_text_layer_layout.py`, `docs/text_layer.md`,
  `docs/shared_library/changelog.md`, `reports/phase-09-text-layer-v3-prep.md`,
  `reports/data/phase-09-text-layer-v3-prep/step-15j/*`; dev layer `data/text_layer_v2_dev/15j-a`
- **Public API:** `LayoutParams.{line_sort,row_tol_frac,sort_unchanged_lines}`, `sort_lines_by_row`, `sort_lines`,
  `row_tol_frac=None` kwarg on `merge_regions` / `split_regions_on_line_field`, `SG_PRINT_LAYOUT_PARAMS`,
  columns `lines_resorted` / `n_regions_resorted`
- **Tests:** full suite 1,172 passed (1,172 collected; 6 new tests); NLP 66 passed; v1 TOTAL 157 / 157
- **Acceptance:** same-row swaps 455 561 to 12, 456 1,380 to 14, 0 jump regions; `unchanged` jumps 455 130 to 0;
  `lines_resorted` 182 / 7 = `n_regions_resorted`; hand differs only by added columns/keys; units 455 27,956
  (-49), 456 24,515 (-195), 0 id collisions; protected diff empty
- **Report:** [`reports/phase-09-text-layer-v3-prep.md` § Step 15j](../../reports/phase-09-text-layer-v3-prep.md#step-15j-row-tolerant-line-order);
  findings F09-15j.1-5
- **Error analysis:** 20 sorted + 27 `unchanged` + 26 remaining swaps read: 44 of 47 reorders right, 3 ambiguous, 0 worse;
  remaining swaps are header rows (8), numeric table cells (11), row-end fragments (7)
- **Deviations from brief:** none (no existing merge test needed `line_sort="baseline"`); `test_build_stage_layout`
  header assertion updated to the new last scan column
- **Brief defects:** `$CS` shell-variable command in the build snippet does not word-split in zsh (ran the command
  inline); `order.py` needs `--sub print --inv 455,456` for print only
- **Open questions for the user:** none

### Step 15k: page-number types, crosswalk role, units
- **Session:** Sonnet 5.5, 2026-10-22. **Status:** done
- **Files changed:** `archival_structures/text_layer/{types,presets,units,build}.py`, `tests/test_text_layer_{types,units}.py`,
  `docs/text_layer.md`, `docs/shared_library/changelog.md`, `reports/phase-09-text-layer-v3-prep.md`,
  `reports/data/phase-09-text-layer-v3-prep/step-15k/*`; dev layers `data/text_layer_v2_dev/15k-a` (and the first build `15k-a-first`)
- **Public API:** `TypeParams.{offchain_retype,offchain_other_bands,offchain_years_other}`, `type_method` `rule:pn_line_gap` /
  `rule:year_offchain`, `units.read_republic_roles`, `republic_crosswalk(roles=)`, `role` column, `REPUBLIC_PARAGRAPHS_DIR` removed;
  print builds need `--republic-dir`
- **Tests:** full suite 1,179 passed (1,179 collected; 7 new); NLP 66 passed; v1 TOTAL 157 / 157; protected diff empty
- **Bars (Part 1):** hand-top 6/20, hand-bottom 12/20, print-top 3/20 not_pn: kept; print-bottom 19/20: `other`; years 37/37: `other`
  (both presets); gap 18 pn / 2 unclear of 20
- **Acceptance:** gap 188; band `other` print 367 (planning 366, band approximation); years 37; units `unit_type` only plus 1 split
  (hand 11281) and 1 join (456 scan 0509); 455 units `seconds` 4.2; crosswalk 513 ids to the last half
- **Report:** [`reports/phase-09-text-layer-v3-prep.md` § Step 15k](../../reports/phase-09-text-layer-v3-prep.md#step-15k-page-number-types-crosswalk-role-units);
  findings F09-15k.1-6
- **Error analysis:** 137 verdict rows, 10+10 retyped segments, 20 crosswalk rows (20 right)
- **Deviations from brief:** the year test and number use the segment text, not the line text (brief step 1/3); first build discarded
- **Brief defects:** year rule on line text gives 33 hand years, not 34 ("Maart 1718"); the brief said the unit changes are joins,
  one is a split; print `other` count 367 vs 366 (planning band approximation); zsh `$CS` again not used
- **Open questions for the user:** none

### Step 15l: continuation marks and the upper-case guard
- **Session:** Sonnet 5.5, 2026-10-22. **Status:** done
- **Files changed:** `archival_structures/text_layer/{join_v2,presets,build}.py`, `tests/test_text_layer_join_v2.py`,
  `docs/text_layer.md`, `docs/shared_library/changelog.md`, `reports/phase-09-text-layer-v3-prep.md`,
  `reports/data/phase-09-text-layer-v3-prep/step-15l/*`; dev layer `data/text_layer_v2_dev/15l-a`
- **Public API:** `LayerPreset.{join_lead_marks_lower,join_upper_guard}`, `join_unit(lead_marks_lower, upper_guard)`,
  `join_inventory_v2(...)` same kwargs, boundary kind `g_upper_right`, stats keys `lead_marks_lower`, `upper_guard`;
  extended `,` rule changes hand output
- **Tests:** full suite 1,204 passed (was 1,179; +25); NLP 66 passed; v1 TOTAL 157 / 157; protected diff empty
- **Bars (Part 1):** 455 `J` mark 30/30 adopted; 456 `J` mark 2/32 (no stop); hand commas mark 67/70 (bar 63) kept;
  upper wrong hand 24/40, print 20/40 (bar 27): guard off in both
- **Acceptance:** commas 70 to 0; 455 `J`+lower 2,172 to 0, lower share 0.892 to 0.948; merges unchanged (guard off);
  other tables identical; 0 offset violations
- **Report:** [`reports/phase-09-text-layer-v3-prep.md` § Step 15l](../../reports/phase-09-text-layer-v3-prep.md#step-15l-continuation-marks-and-the-upper-case-guard);
  findings F09-15l.1-5
- **Error analysis:** 215 verdict rows, 20 built spans read (Random(91))
- **Deviations from brief:** none
- **Brief defects:** none; baseline counts drift slightly (print upper 2,036/2,411, 455 J 2,172, expected)
- **Open questions for the user:** none

### Step 15m: v3 presets, integrated dev build, run kit
- **Session:** Sonnet 5.5, 2026-10-22. **Status:** done
- **Files changed:** `archival_structures/text_layer/presets.py`, `tests/test_text_layer_provenance.py`, `docs/text_layer.md`,
  `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-015), `reports/phase-09-text-layer-v3-prep.md`,
  `reports/data/phase-09-text-layer-v3-prep/step-15m/*`; LotA `docs/from-archival-structures.md` section 8 and an
  entry in LotA `STATUS.md` (docs only); dev layers `data/text_layer_v2_dev/15m-a`, `15m-smoke`
- **Public API:** presets `sg_1.01.02_v3`, `sg_1.10.94_v3` (`SG_HAND_V3`, `SG_PRINT_V3`), equal to v2 but name/description
- **Tests:** AS 1,205 passed (+1); NLP 66 passed; v1 TOTAL 157 / 157; protected diff empty
- **Part 0:** pagexml-tools 0.9.1 not on PyPI (2026-10-22): no pin, UA-007 stays open
- **Acceptance:** `15m-a` equals `15l-a` (hand 237 + print 51 tables, 0 differ); audit 178 / 182 (same 4 failures; 455 now
  0.9477); 116 rows 0.526 / 0.560 unchanged, 0 predictions differ (8 rows differ in cells; 23 and 188 stale); smoke runs exit 0
- **Report:** [`reports/phase-09-text-layer-v3-prep.md` § Step 15m](../../reports/phase-09-text-layer-v3-prep.md#step-15m-v3-presets-integrated-dev-build-run-kit);
  findings F09-15m.1-7
- **Error analysis:** 8 changed 116-rows read, 20 of 1,640 changed unit texts (Random(101)): 13 `J` mark better, 6 order better, 1 neutral, 0 worse
- **Deviations from brief:** none
- **Brief defects:** `rows116.py` also needs `rows116_verdicts.tsv` beside it (without it the share reads 0.474); `order.py`
  takes `--out`, not `--json`; hand `shared-data` holds only 6 of the 14 hand inventories; "116-row verdict changed" is
  static (hand-read verdicts), so a changed-verdict reading needs relabelling by hand; `--estimate` has no throughput fit for the SG presets
- **Open questions for the user:** UA-015 (stage the inputs for the full run)

### Phase review
- **Session:** Opus 5.5, 2026-10-22. **Status:** done. **Verdict:** accept with follow-ups
  ([REVIEW](REVIEW.md)); no review-fixes step.
- **How the steps ran:** 15j–15m as Sonnet subagents, one at a time; after each, the reviewer re-ran
  the step's key numbers before starting the next. After 15j, the `$CS` zsh defect was fixed in the
  15k–15m briefs (rule 10).
- **Re-run:** AS 1,205 passed; NLP 66; v1 157/157 (`09R-v1check`); `15l-a` = `15m-a` (0 of 288);
  order, page-number, merge and crosswalk counts as logged; audit 178/182.
- **Own error analysis:** rows 23 and 188 read as fixed (F09-R.3); `J` error ≤ 0.2% over all 2,172
  strips (F09-R.4); 40 hand-band verdicts re-read (F09-R.5); 15m's "30 unmatched" not reproduced
  (F09-R.2).
- **Fixed in review:** UA-015's staging command (symlink copy → `tar -s` loop, tested by a build on
  11182; F09-R.6) and its options.
- **Updated:** report final (Summary, Review, Follow-ups; 30 step findings confirmed, F09-R.1–R.7);
  37 rows in `reports/index.md`; `make reports` clean; phase `PLAN.md` hand-off; `plan/PLAN.md` row 09;
  `plan/CONTEXT.md` §1, §2, §3, §4 and §5 (163 lines).
- **Release:** v0.14.0 recommended.
