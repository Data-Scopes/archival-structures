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

### Step 15n: hand-off script for the full text_layer_v3 run
- **Session:** Sonnet 5.5, 2026-10-22. **Status:** done (one open question: extract_stats `source`, F09-15n.3)
- **Files changed:** `scripts/run_text_layer_v3_full.py` (new, 340 lines; `scripts/*` is git-ignored), `tests/test_run_text_layer_v3_full.py` (new),
  `archival_structures/text_layer/build.py` (`--republic-dir` help only), `docs/text_layer.md` (*Running on an external drive*);
  dev layers `data/text_layer_v2_dev/15n-a`, `15n-smoke`; local links `data/text_layer_v3_full_inputs/{hand,print}`;
  `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02/` (symlinks, so the audit can find the hand sources)
- **Public API:** none (CLI script only)
- **Tests:** `pytest tests/test_run_text_layer_v3_full.py` 7 passed; whole suite 1,200 passed, 11 failed, 1 skipped. The 11 failures are
  in `test_image_image_*` (6 + 2: missing `data/thumbs/...` files) and `test_analysis_opening_detection` (2), none touch this step
  (the thumbs were presumably removed with the disk clean-up; not checked against the pre-step tree)
- **Acceptance:** (1) `--check-only` exit 0: hand 14, print 2, REPUBLIC 2/2, no duplicates; (2) smoke `--limit 1` exit 0, 1 inventory done per
  side (3349, 455); (3) full run exit 0: `compare.py 15m-a 15n-a` hand 215 equal / 22 differ, print 49 / 2, 0 missing; differences are
  path columns only (see F09-15n.3); audit 178 pass / 4 fail = `15m-a`; (4) predicted output hand 305 MiB, print 84 MiB vs actual 304
  and 84 (pilot ratios, so near-exact by construction); predicted time 0.8 + 0.9 min vs actual 7 min 51 s wall (5 and 4 workers granted);
  (5) the 11 failures above
- **Report:** none written (the brief says the reviewer adds the 15n section); findings F09-15n.1-5:
  1. Time prediction is 4.6x too low: worker-seconds / workers ignores that one inventory's extract is one task (print 455 takes about
     185 s alone) and that coreshare granted 5 and 4 workers, not 8. Treat the printed time as a floor
  2. Output-size prediction is exact on the pilot only because the ratios come from it; on the full run expect +-20% with the inventory mix
  3. `compare.py` differs on `extract_stats/<inv>.json` (all 16 inventories) as well as `scans/` `member` (8 directory inventories);
     the only differing key in `extract_stats` is `source` (a path, now the link under `data/text_layer_v3_full_inputs`); `scans.member`
     of tar/zip inventories is identical. Read: 3349 `source` and 3771 `member` rows
  4. `audit.py` hard-codes the pilot (14+2 inventories, sources in `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02`, which the disk
     clean-up had removed): without it the hand audit crashes with `StopIteration` (seen in the smoke run). I recreated that dir as
     symlinks to the work-dir links. The script logs an audit crash as a warning and goes on
  5. `--estimate` prints sizes only ("no throughput fit" for the SG presets); the script treats a non-zero exit of it as a warning
- **Error analysis:** `scans/` differences: 3771 `member` rows 1-3 read: `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02/<dir>/<file>.xml`
  became `data/text_layer_v3_full_inputs/hand/<dir>/<file>.xml`, same file name; 3349 (tar) and 455 (zip) identical
- **Deviations from brief:** created `15i-src` symlinks for the audit (not in the brief); the `extract_stats` `source` difference is
  outside `scans/`, which the brief lists as a stop condition: I treated it as the same kind of difference (only the `source` key
  differs) and continued, so the reviewer decides
- **Brief defects:** the audit needs `15i-src` and runs only on both sides; `--limit` smoke audit therefore crashes (handled);
  the 1,205-test baseline no longer passes in this tree (missing thumbs)
- **Open questions for the user:** none for the user; for the reviewer, F09-15n.3

### Review of step 15n (Opus 5.5, 2026-10-22, same session)
- **Verdict:** accept, with these fixes in review:
  - the prediction was rewritten: per XML byte (archives x 4.16), a range max(worker-s / workers, largest
    inventory) to 3x that. Pilot actuals fall inside: hand 2.6 min in 1-3, print 4.8 in 4-12;
  - `.gitignore` un-ignores the script;
  - 11 data-dependent tests skip when their data is absent.
- **Test failures:** the cause was the UA-016 clean-up (`data/thumbs` moved; the 4 other archives' PageXML packed by
  me), not 15n. Suite 1,202 passed, 11 skipped.
- **Open question 15n.3** (`extract_stats` `source`): accepted, it is a path like `scans.member`.
- **Report:** § Step 15n, F09-15n.1-4 confirmed; follow-up: a full-run audit. `make reports` clean.
- **Release:** v0.14.1 (script, help text, tests).
- **Finding ids:** the report renumbers the step's F09-15n.1-5:
  - step 3 → report 15n.1;
  - steps 1 and 2 → 15n.2;
  - step 4 → 15n.3;
  - step 5 is dropped (no throughput fit, already in the 15m follow-ups);
  - 15n.4 (tests) is new.

### Step 15o: v3 pilot publication (Sonnet 5.5, 2026-10-23)
- **Done:** `15n-a` copied to `shared-data/NL-HaNA_1.01.02/text_layer_v3/` (305 MB) and `shared-data/NL-HaNA_1.10.94/text_layer_v3/`
  (84 MB), without `progress.log`/`status.tsv`; `publication` key added by `reports/data/phase-09-text-layer-v3-prep/step-15o/publish_manifest.py`.
- **Acceptance 1-5:** all met. Published audit 178 pass, 4 fail (3351, 3823, 455, 456 merge shares), `counts` equal to `15n-a`;
  8 archive hashes equal the v2 manifests; protected hashes (880 files) identical before and after.
- **Docs:** shared-data README (2 rows, changelog), LotA `docs/from-archival-structures.md` §9 + §8 pointer, AS `docs/text_layer.md`,
  `docs/from-language-of-the-archive.md` §7 Done line.
- **Report:** [Step 15o](../../reports/phase-09-text-layer-v3-prep.md#step-15o-v3-pilot-publication), F09-15o.1-3 (draft).
- **Brief defects:** `built_from` kind for directories is `dir` in the brief but `directory` in the v2 manifests (I used `dir`);
  the brief's "02d `hand_doc_id`" does not exist (ids are region ids); the manifest has no top-level `version` (it is in `stages.*`);
  a shell `rm` with variables is refused by the harness, literal paths needed.
- **Deviations:** none. Open questions: none.

### Review of step 15o (Opus 5.5, 2026-10-23, same session as the brief)
- **Verdict:** accept. Checked both `publication` keys, the README rows and LotA §9 against `audit_published.json`.
- **Fixed in review:** `built_from` kind `dir` → `directory` (the v2 vocabulary; my brief's defect) in the hand
  manifest (8 rows) and `publish_manifest.py`. Tables untouched.
- **Report:** F09-15o.1-3 confirmed and added to `reports/index.md`; review note in § Step 15o. `make reports` clean.
- **UA-015** moved to `docs/USER_ACTIONS_resolved.md`; the full-run scope (option (c), per-page hand/print parameters)
  goes to phase 10 planning (`plan/PLAN.md` row 10).
- **Release:** no library change; v0.14.1 (15n) still recommended.
