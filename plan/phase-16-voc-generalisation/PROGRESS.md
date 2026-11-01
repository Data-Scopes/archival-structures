# Phase 16 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning (Opus 5.5, 2026-10-31)
- **User decisions (2026-10-31):**
  - 2.10.50 goes to a later phase;
  - the seed's question 3 (document starts) is dropped as NLP's territory;
  - all 546 GM inventories are built on the drive;
  - GLOBALISE starts may serve as link targets, for scoring only.
- **Read:** `plan/README.md`, `plan/PLAN.md`, `CONTEXT.md`, the seeded phase `PLAN.md`/`STATUS.md`, the VOC rows of
  `ground_truth/README.md`, the staging lists, the GM source CSV, the GLOBALISE segmentation CSVs and the OBP index
  header.
- **Explorers:**
  - AS: preset fields, CLI, stages, the `folio_map`/`index_links`/`index_pages` APIs, the audit's checks, test counts
    (1,413 collected).
  - NLP: splits, spent gold, report/76 and report/83; NLP never used the GM folio fields.
  - Suite at planning: 1,402 passed, 11 skipped.
- **Probes** (`reports/data/phase-16-voc-generalisation/planning/`):
  - `gm_split_and_proto.py`: GM anchors (1,841 after dedup and dropping 1171); split by inventory, stratified by
    layout, seed 20261031; dev-only prototype (v1 Laypa `page-number` anchors, scan level): leaf 0.647, strict 0.505.
    Eight misses read (F16-P.2).
  - `toc_split_and_peek.py`: TOC gold for the 22 (186 scans in 21 volumes); split 11/11; dev TOC text read → the
    `VOC_TOC_CUE` and L8 patterns.
  - A smoke run of the 22a preset (runtime-registered) on 8023 and 2775: `failures=0` in 18 s, both `hand`, all
    sides `single`, the 2775 TOC unflagged. This made 2775 planner-seen (P5).
  - The GM ratio scans/folio is 2.0 (`Enkel`) and 1.0 (`Dubbel`); 11 volume parts have folio decreasing along scans.
- **Written:** briefs 22a–22e, phase `PLAN.md` (P1–P13), `STATUS.md`, report skeleton
  `reports/phase-16-voc-generalisation.md` with planning findings F16-P.1–P.6, and the `plan/PLAN.md` row.
- **Side effects:**
  - link `data/voc_text_layer_v1` → `shared-data/voc_text_layer_v1`, with AS added to its shared-data catalogue row;
  - planning dir `reports/data/phase-16-voc-generalisation/planning/` (g+w).
- **Open for the user:** none.

### Step 22a (Sonnet 5.5, 2026-11-01)
- **Done:** preset `voc_1.04.02_v5sg` (SG v5, archive plumbing only, inventory pins dropped); 5 new tests; full suite
  1,407 passed, 11 skipped. Layer built over **567** inventories (545 GM + 22 GLOBALISE; the brief's 546/568 was a
  header-line miscount), `failures=0`, 2 workers (machine load held the coreshare grant), 1 h 51 min; 6.0 GB on the drive.
- **Audit unchanged:** `n_fail_judged = 0`; 148 reported-only fails of one check (lower-case right side of merges < 0.95).
- **Measured:** all 567 `hand` (box height GM 109 / 177.5 / 273.5, GLOBALISE 131 / 181 / 231); sides agree with the GM label in
  483/545 (Enkel 340/343, Dubbel 143/202; 57 Dubbel volumes are portrait single-page scans); 342,451 `page_number` segments
  (42% `pn_line_offchain`); Laypa agreement P 0.650 R 0.825; `¬` never joined (116,881 left, 6.64 per 1,000 lines); SG index
  flags TP 1, FP 121, FN 185 against 186 gold TOC scans.
- **Report:** `reports/phase-16-voc-generalisation.md#step-22a-...`, findings F16-22a.1 to .7 (draft). Protect diff: empty.
- **Files:** `archival_structures/text_layer/presets.py`, `tests/test_text_layer_voc_preset.py`, `docs/text_layer.md`,
  `docs/shared_library/changelog.md`, `reports/data/phase-16-voc-generalisation/step-22a/`, `data/text_layer_voc_inputs/`,
  link `data/text_layer_voc_22a`.
- **Open for the user:** none.

### Step 22c (Sonnet 5.5, 2026-11-01)
- **Done:** index-side detector measured on VOC TOCs; no library code, no constants (bar D not met). Full suite 1,411 passed,
  11 skipped (22b may have added tests). `make reports`: no warnings.
- **Replica:** `_write_pages_table` + `SG_INDEX_SIDES_V5` equals the layer's `layout/pages` in all 22 volumes (0 diffs).
- **Grid/selection:** 32 cells on dev; selected `tmc8_som0.25_cue_gap2` (dev P 0.197, R 0.624, F1 0.300; unchanged 0 TP);
  `select.json` before `test_done.json`; test run once.
- **Bar D (test, 10 volumes, 61 TOC scans):** unchanged P 1/53, R 1/61; selected P 24/357 = 0.067, R 24/61 = 0.393; verdict
  `fails on VOC`. 2775 apart: unchanged 0/3, 0/8; selected 1/19, 1/8.
- **Front runs:** unchanged 0, selected 4, oracle 8 of 22 volumes; oracle `None` from `min_flagged` (14) and `min_after` (6).
- **Error analysis:** 135 cases read; FN are one-column TOCs with numbers apart; FP are account pages (Transport, "Van X onder dato").
- **Report:** `reports/phase-16-voc-generalisation.md#step-22c-...`, F16-22c.1 to .6 (draft).
- **Files:** `reports/data/phase-16-voc-generalisation/step-22c/` (`eval_toc.py`, `cases_label.py`, tables), report, STATUS.
- **Open for the user:** none.

### Step 22b (Sonnet 5.5, 2026-11-01)
- **Done:** folio map measured on the GM gold; two opt-in params added (`single_foliation`/`single_slope`, `number_tags`), defaults
  unchanged (SG inv 3444 inputs equal). 4 new tests; full suite 1,411 passed, 11 skipped. `make reports`: no warnings.
- **Grid/selection:** 8 cells on dev (929 readable of 937 anchors); selected (V0, off, TL), dev leaf 0.696; `select.json` before
  `test_done.json`; test run once.
- **Bar F (test, 904 anchors, 273 volumes), leaf:** (V0,off,T) 550/904 = 0.608 (0.576-0.640); (V2,off,T) 523/904 = 0.579
  (0.546-0.610); selected 591/904 = 0.654 (0.622-0.684). Verdict `fails on VOC`.
- **Archive questions:** `single` volumes foliated on every other scan (90.2% of steps); `openings` median recto share 0.903, 74/146 reach 0.9;
  restart criterion of the brief flags 509/545 (noise), 6 compact; LOO dev 0.8837; 11 decreasing parts: 3 gold error, 2 nested, 1 restart,
  5 without decrease.
- **Error analysis:** 70 misses read (40 dev, 30 test); tied/duplicated 16, restart 10, gap 10, gold error 9. Check page UA-059.
- **Report:** `reports/phase-16-voc-generalisation.md#step-22b-the-folio-map-on-voc`, F16-22b.1 to .8 (draft).
- **Files:** `archival_structures/analysis/folio_map.py`, `index_links.py`, two test files, `docs/index_links.md`, changelog,
  `reports/data/phase-16-voc-generalisation/step-22b/`.
- **Open for the user:** UA-059 (blocks nothing).

### Step 22d (Sonnet 5.5, 2026-11-01)
- **Done:** locator variant L8 (`lead_pattern`, `standalone_pattern`, `VOC_LOCATOR_PARAMS`) added to `index_links.py`; 2 tests; full suite
  1,413 passed, 11 skipped. SG check: inv 3444 links of L1-L7 and `volume_links_v2` identical before and after (sha256 `30c9319dc2142311`).
- **Grid/selection:** 5 cells (U1, U2, C1, C2, C3) on 11 dev volumes with oracle TOC sides; selected C3 (L8 lines, V0/off/`page-number`),
  dev F1 0.505 (C2 0.488, U1 0.220, C1 0.098, U2 0.085); `select.json` before `test_done.json`; test run once.
- **Bar E (test, 9 volumes with locators, 2775 apart), target 0.90:** U1 138/411 = 0.336 (0.292-0.383); U2 88/163 = 0.540
  (0.463-0.615); C3 274/433 = 0.633 (0.586-0.677). Coverage 0.273/0.219/0.574; chance +3: 0.187/0.252/0.238. Verdict `fails on VOC`.
- **2775:** C3 36/143 = 0.252; U1 0/50; U2 no locators. **Silver (C3):** P 287/365, R 287/458. **End to end:** 0/22 volumes get a front run.
- **Error analysis:** 30 dev + 30 test non-hits and 15 no-locator lines read. Test: not a folio 13, range end 4, map error 10, unclear 3.
  L8 gaps: "a" connectors "à", " / ", dots; "verso" after the range end.
- **Report:** `reports/phase-16-voc-generalisation.md#step-22d-...`, F16-22d.1 to .8 (draft).
- **Files:** `archival_structures/analysis/index_links.py`, `tests/test_analysis_index_links.py`, `docs/index_links.md`, changelog,
  `reports/data/phase-16-voc-generalisation/step-22d/` (`eval_links.py`, `label_cases.py`, tables).
- **Open for the user:** none.

### Step 22e (Sonnet 5.5, 2026-11-01)
- **Done:** no code. Generalisation table (10 rows) in the report and in `reports/data/phase-16-voc-generalisation/step-22e/generalisation.tsv`.
  Verdicts: 1 unchanged; 2-6 and 10 runs, no bar; 7, 8, 9 fails on VOC (copied from 22b, 22c, 22d). No table/step disagreement.
- **Notes:** three *used by* cells in `ground_truth/README.md` (GM row: "none yet" replaced); NLP `docs/from-archival-structures.md` section 5 appended (Edit).
- **Report:** `reports/phase-16-voc-generalisation.md#step-22e-generalisation-table`, F16-22e.1 to .3 (draft).
- **Open for the user:** none. Next: phase review (Opus, `plan/PROMPTS.md` section 3).

### Step 22r (Sonnet 5.5, 2026-11-01)
- **Done:** fixed `load_gold()` in `step-22d/eval_links.py` (max of toc/start over a scan's several gold rows); old outputs moved to `step-22d/superseded_22r/`; reran `dev` then `test`. Reviewer's numbers reproduced.
- **Numbers:** dev C3 selected (F1 0.5106, E 689/1,275; C2 0.4948). Test E: U1 142/411 (Wilson 0.301-0.393, chance 77, cov 130/478), U2 90/163 (0.476-0.626, 41, 103/478), C3 279/433 = 0.644 (0.598-0.688, 103, 275/478). 2775 C3 41/143, cov 33/52, chance 22. Silver byte-identical. Verdict `fails on VOC`.
- **Flipped read cases:** 0 of the 60 (`cases_flipped_22r.tsv` is header only); error-analysis counts stand.
- **Report:** 22d section (tables, 2775, per volume, new "Review fix (22r)" paragraph), F16-22d.1, .3, .4, .8 (.2 coverage too); 22e row 9, F16-22e.1, `generalisation.tsv` row 9. Removed `step-22c/__pycache__`.
- **Left stale (brief: change nothing else in 22e):** 22e bullet "L8 triples coverage but stays at 0.633 (F16-22d.3)" should read 0.644.
- **Open for the user:** none.

### Review (Opus 5.5, 2026-11-01)
- **Verdict:** accept with follow-ups ([REVIEW](REVIEW.md)). The same session planned the phase, ran 22a–22e and 22r as
  Sonnet subagents, and reviewed them.
- **Re-runs (scratchpad only):**
  - the 22a audit is equal on all 18,996 keys;
  - 22b 550 / 523 / 591 of 904, with my own scorer;
  - 22c from the layer flags;
  - SG defaults against `HEAD` copies on 3444: inputs, `locate` 0–1,499 (V0, V2) and L1–L7 on 9,166 units are equal;
  - AS suite 1,413 passed, 11 skipped.
- **Defect:** 22d `load_gold()` kept the last of a scan's gold rows (698 scans have 2–4 rows), so starts were lost. A
  fixed copy gave the same dev selection. Brief `step-22r-review-fixes.md` was run by a Sonnet session: C3 test E
  274 → 279/433, and none of the read cases flips (F16-R.1).
- **Error analysis read:**
  - 22d's 5 disagreeing rows;
  - 22b's `Dubbel` single-page portrait stratum (leaf 19/66; 7529, 1122, 1132, 1245) → F16-R.3;
  - 22b's method mix and its decreasing parts.
- **Revised:** F16-P.1, F16-P.3, F16-22e.2, and the 22r-changed rows of 22d/22e. **New:** F16-R.1 to R.6. Report final;
  findings appended to `reports/index.md`; `make reports` clean, and every link resolves.
- **Also updated:**
  - `plan/templates/STEP.md` (gold row grain; a planning run of every new option);
  - `plan/CONTEXT.md` (§1–§5 for phase 16; 184 lines, over the ~150 budget);
  - `plan/PLAN.md` row 16;
  - `docs/USER_ACTIONS.md` UA-060 (next phase).
- **Release:** v0.21.0 recommended (additive: VOC preset, `single_foliation`, `number_tags`, L8).
