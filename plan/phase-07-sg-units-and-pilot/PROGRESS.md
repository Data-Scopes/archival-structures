# Phase 07 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Pre-planning: step 15fp (republic layout port)
- **Session:** Opus, 2026-10-18. **Status:** done (brief written; 15g–15i not yet planned)
- **Asked by the user:**
  - port republic's PageXML preprocessing (overlapping-region merge, scan → pages, main columns
    + `extra`, date headers) before phase 07;
  - check pagexml-tools first;
  - overlaps also occur in print, especially in the index part of resolution books.
- **Read:**
  - republic `pagexml_helper.py` L100–340 and `page_date_parser.py` L240–364 and L800–896;
  - two Explore reports (one on pagexml-tools 0.9.0, one on republic's scan → page pipeline);
  - AS `analysis/opening_detection.py`, `analysis/page.py`, `text_layer/extract_v2.py`,
    `build.py` (wordfix stage, CLI).
- **Measured:** region types, overlap rates, the original merge and page split under the
  current pins, the AS opening classifier and the separation-point disagreement. The results
  are in the report's [*Planning*](../../reports/phase-07-sg-units-and-pilot.md#planning)
  section, F07-P.1–P.8.
- **Decisions:**
  - (D1) the new step 15fp runs first in phase 07; 15g–15i keep their numbers, which phase-06
    documents cite;
  - (D2) a separate `layout` stage writes side tables and leaves the extract tables
    byte-identical;
  - (D3) generic code goes in `analysis/page_structure.py`, the adapter in `text_layer/layout.py`;
  - (D4) the date-header machinery is not ported (F07-P.8);
  - (D5) work around the pagexml-tools traps; UA-005 asks the user about an upstream fix.
- **Files:**
  - `step-15fp-page-layout.md` (new);
  - the phase `PLAN.md` (Owns, Steps, Sources, a pre-planning section);
  - `STATUS.md`;
  - this file (new);
  - `reports/phase-07-sg-units-and-pilot.md` (new skeleton with the Planning section);
  - `plan/PLAN.md` row 07;
  - `docs/USER_ACTIONS.md` UA-005.

### Step 15fp: page layout (page split, region merge, columns, roles)
- **Session:** Opus 5.5 (the user ran the step in Opus), 2026-10-19. **Status:** done; its
  validation results are negative (see Error analysis).
- **Files changed:**
  - new: `archival_structures/analysis/page_structure.py`, `archival_structures/text_layer/layout.py`,
    `tests/test_analysis_page_structure.py`, `tests/test_text_layer_layout.py`, and
    `reports/data/phase-07-sg-units-and-pilot/step-15fp/{acceptance.py, acceptance.json, ea.py,
    split_proxies.py, ea_verdicts.tsv}`;
  - edited: `archival_structures/text_layer/build.py` (stage `layout`, `--layout-params`),
    `docs/text_layer.md`, `docs/shared_library/changelog.md`, the phase report, and
    `docs/USER_ACTIONS.md` (UA-006);
  - data: `data/text_layer_v2_dev/15fp-a/{hand_tar,hand_local,print}/` (copies plus `layout/`)
    and `15fp-protected-{before,after}.txt`.
- **Public API:** additive, listed in the changelog:
  - `analysis.page_structure`: `LayoutParams`, `get_overlapping_region_sets`, `merge_regions`,
    `merge_overlapping_text_regions`, `fit_gutter`, `find_separation_x`,
    `split_scan_into_pages`, `group_regions_into_columns`, `assign_column_roles`, `region_box_id`,
    `hull_coords`, `is_opening`, `region_chars`;
  - `text_layer.layout`: `layout_inventory`, `layout_scan` and the `LAYOUT_*_COLUMNS` lists.
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_structure.py
  tests/test_text_layer_layout.py` → 13 passed; the full suite → 1023 passed (1010 + 13).
- **Acceptance:**
  - (1) all 16 inventories conserve every line, with unique layout region ids;
  - (2) 0 unchanged rows have an id mismatch;
  - (3) 0 overlapping pairs after the layout. The 455 reduction 9,343 → 2,953 is explained in
    the report: 1,251 of 1,784 merges join `index`- and `resolution`-tagged regions;
  - (4) and (5) are reported per inventory in the report table;
  - (6) the suite is green and the protected hashes are identical.

  The planning overlap counts reproduce exactly.
- **Report:** [`reports/phase-07-sg-units-and-pilot.md` § Step 15fp](../../reports/phase-07-sg-units-and-pilot.md#step-15fp-page-layout-page-split-region-merge-columns-roles);
  findings F07-15fp.1–11.
- **Error analysis:**
  - merge precision is a negative result: hand 0.82 counting absorbed furniture as right, print
    0.13, because print regions are page-level;
  - page split: 44 of 60 scans have the gutter right, 14 cut off the recto's margin column, 1
    misses the gutter by a page;
  - roles: 4 of 60 extra regions are main text.
- **Deviations from brief:**
  - (1) The merge's line-sort key adds the tie-breakers (box top, line id). Without them the
    5-shuffle determinism test fails on equal baseline y and left.
  - (2) The 455 index stratum is defined by text: ≥ 10 lines with "pag." (scans 0003–0005, 3
    merge sets, all taken). The other 27 print sets fill the 30.
  - (3) The page-split categories gained "gutter missed by a whole page" and "undecided".
  - (4) The PageXMLPage gets the page box as coords (new trap, F07-15fp.11).
  - (5) `n_regions_in`/`out` count non-empty regions only. `n_merges` counts output regions
    that are merges.
  - (6) Protected `shared-data/` is covered by sha256 of the SG source archives (as in 15d and
    15fr) plus size and mtime of every readable file (52,115 entries). The step does not read
    `shared-data/`.
- **Brief defects:**
  - (1) "the index part of the volume is where the regions overlap most" does not hold for
    455. Its index part is 3 scans. Laypa's `index` tag sits on resolution text throughout
    (F07-15fp.10).
  - (2) The brief did not say that print regions span both text columns. The merge-set "two
    columns" criterion therefore mostly measures the input regions (F07-15fp.3).
  - (3) The brief's fixture is described as "3-scan"; it has 4 scans, 1 of them a portrait
    `deelopname`.
  - (4) The brief does not say how `split_method` is set for openings without text. They get
    `none`, with separation `gutter_rel·page_w`.
  - (5) `PageXMLPage` raises on an empty page, a fourth pagexml-tools trap the brief didn't
    list.
- **Open questions for the user:** UA-006 (how 15g–15i use the layout). UA-005 was answered (a):
  add the new trap to the upstream fixes.
- **Addendum 2026-10-20:** UA-006 was answered (a). A fix step 15fq comes before 15g: (1) hand
  gutter just left of the recto's margin column, (2) split print layout regions on the line
  `column` field, (3) keep page furniture out of merges, with fresh validation on new seeded
  samples. UA-006 moved to `docs/USER_ACTIONS_resolved.md`.

### Phase planning: 15fq, 15g, 15h, 15hj, 15i
- **Session:** Opus 5.5, 2026-10-20. **Status:** done (briefs written; nothing implemented)
- **Asked by the user:** prompt 1: write the 15fq brief first, then 15g–15i on its output; also
  do UA-004. During the session the user reported a `PageXMLPage.stats` fix (checked: still
  raises, UA-007).
- **Read:** `plan/README.md`, `plan/PLAN.md`, the phase `PLAN.md`/`STATUS.md`/`PROGRESS.md`, the
  15fp brief and report; LotA hand-off R1–R6 §3–4; SG audit §2–3; phase-06 `PLAN.md` hazards and
  hand-off, its findings table; phase-05 `REVIEW.md` rows 4, 6, 7; `join.py`, `word_breaks.py`
  L120–345, `build.py` L393–487, `page_numbering.py` L41–93. Two Explore reports (AS code and
  data; LotA, pagexml-tools, shared-data).
- **Measured** (scratchpad `p07/`, on the 15fp tables, LotA gold read only): the `main_edge` gutter
  rule against 15fp's 60 read scans and per inventory (F07-P.11); the gap share per inventory
  (F07-P.12); the furniture pattern rule on 15fp's merges (F07-P.13); merged top lines and the
  page-number chain (F07-P.10); the 7 R4 test rows resolved to regions and lines; the 51 artefact
  rows' notes (F07-P.14); the local pagexml-tools (F07-P.9).
- **Decisions:** D6–D19 in the phase `PLAN.md`. Asked the user: the pilot path (`text_layer_v2`,
  full run v3), the interleave guard (add it), and the fallback (merges off).
- **Files:**
  - new: `step-15fq-layout-fixes.md`, `step-15g-line-region-types.md`,
    `step-15h-units-and-mapping.md`, `step-15hj-word-breaks-and-join.md`,
    `step-15i-pilot-build-and-publish.md`;
  - edited: the phase `PLAN.md` (Owns, Steps, Decisions, Hazards, open questions settled),
    `STATUS.md`, this file, `plan/PLAN.md` row 07, the report (Planning findings F07-P.9–14, empty
    step sections), `docs/findings.md` (UA-004: a new section with five phase-06 findings),
    `docs/USER_ACTIONS.md` (UA-004 and UA-005 resolved, UA-007 added),
    `docs/USER_ACTIONS_resolved.md`.
- **Open questions for the user:** UA-007.

### Step 15fq: layout fixes (hand gutter, print column split, furniture, interleave guard)
- **Session:** Sonnet 5.5 (subagent), 2026-10-21. **Status:** done, with criterion 6 missed (UA-008)
  and the brief's merges-off fallback applied to hand. Final layer: `data/text_layer_v2_dev/15fq-b/`.
- **Files changed:**
  - edited: `archival_structures/analysis/page_structure.py`, `text_layer/layout.py`,
    `text_layer/presets.py`, `text_layer/build.py`, `tests/test_analysis_page_structure.py`,
    `tests/test_text_layer_layout.py`, `docs/text_layer.md`, `docs/shared_library/changelog.md`
    (`## Unreleased`, Step 15fq), the phase report, `docs/USER_ACTIONS.md` (UA-008);
  - new: `reports/data/phase-07-sg-units-and-pilot/step-15fq/{acceptance.py, acceptance.json,
    acceptance_15fq-b.json, ea.py, ea_verdicts.tsv, page_diff_<inv>.tsv}`;
  - data: `data/text_layer_v2_dev/{15fq-a, 15fq-legacy, 15fq-b}/{hand_tar,hand_local,print}/`,
    `15fq-ea/` (sample dumps), `15fq-protected-{before,after}.txt`.
- **Public API:** additive, listed in the changelog. `LayoutParams` +13 fields, `from_dict`,
  `LEGACY_15FP`; `inventory_line_width_rel`, `choose_gutter_rule`, `find_main_edge_x`,
  `split_regions_on_line_field`, `furniture_region_ids`; keyword-only additions to
  `find_separation_x`, `get_overlapping_region_sets`, `merge_overlapping_text_regions`,
  `layout_scan`; `LayerPreset.layout_params`. Behaviour change: default layout output (use
  `LEGACY_15FP` for 15fp's).
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_structure.py
  tests/test_text_layer_layout.py` → 25 passed; full suite → 1035 passed (1,023 + 12).
- **Acceptance:** (1) 16/16 conserve lines, 0 duplicate ids, 0 unchanged-id mismatches; (2)
  `15fq-legacy` = `15fp-a` on all three tables; (3) 0 differences in the 9 gap inventories; (4) lines
  with a different `page`: 343, 1,615, 4, 14, 52, 236, 268 (3349, 3350, 3351, 3352, 3353, 11182,
  11281), exactly the planning numbers; (5) 14/14 cut-offs and 3350_0856 change, 5 of 44 "right"
  change (+ the undecided one); (6) **145** print regions with both columns (target 0); (7) 0
  merged regions with a furniture member, residual pairs all furniture or guard in 15fq-a; (8)
  suite green, protected hashes identical.
- **Report:** [`reports/phase-07-sg-units-and-pilot.md` § Step 15fq](../../reports/phase-07-sg-units-and-pilot.md#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard);
  findings F07-15fq.1–9.
- **Error analysis:** merge precision hand 12/23 = 0.52 (fallback: hand merges off), print 28/30 = 0.93;
  page split 30/30 changed scans right, the 5 dev "gutter right" scans overturned; furniture 0 body
  text excluded in 30; column split 17/20 pure; guard right 16 of 30 (5 wrong, 9 ambiguous).
- **Deviations from brief:**
  - (1) `SG_HAND_V2` now has `merge=False` in its `layout_params` (the fallback "through the
    preset"), so 15fq-a needs `--layout-params '{"merge": true}'` to reproduce;
  - (2) `layout_scan` has a `blocked_out` list and `ea.py` re-runs scans to get the blocked pairs;
  - (3) the report's merge strata are hand/print only (no 455 index stratum).
- **Brief defects:** (1) criterion 6 (0 mixed-column regions) does not hold: the brief did not say
  how column pieces that overlap the other column's piece are treated in the merge; (2) the brief
  gives no method for the protected hashes beyond "as 15fp": the format is `sha256 mtime path` for the
  SG source archives and `size= mtime path` for all other files; (3) `furniture_region_ids` needs a
  `page_text_bottom` that the brief does not define: used the lowest line bottom of the page's regions.
- **Open questions for the user:** UA-008.

### Step 15g: line and region types (2026-10-21, Sonnet)

- **Done:** the `types` stage (`--stage types`, `--types-params`), segment and region tables, on
  `data/text_layer_v2_dev/15g-a/{hand_tar,hand_local,print}/` (copy of `15fq-b` + `types/`).
- **Files changed:**
  - new: `archival_structures/text_layer/types.py`, `tests/test_text_layer_types.py`,
    `reports/data/phase-07-sg-units-and-pilot/step-15g/{acceptance.py, common.py, ea.py,
    acceptance_15g-a.json, ea_verdicts.tsv}`;
  - edited: `text_layer/presets.py` (`types_params`, `SG_TYPE_PATTERNS`), `text_layer/build.py`,
    `docs/text_layer.md` (*Type tables*), `docs/shared_library/changelog.md` (Step 15g, additive),
    the phase report, `docs/USER_ACTIONS.md` (UA-009);
  - data: `15g-a/`, `15g-protected-{before,after}.txt` (identical).
- **Public API:** additive (`TypeParams`, `LayerPreset.types_params`, `--stage types`).
- **Tests:** `tests/test_text_layer_types.py` 10 passed; full suite 1,045 passed (1,035 + 10).
- **Acceptance:** (1) 1,644,839 text lines, 1,668,542 segments, 0 missing/overlap/mismatch;
  (2) R4 rows 50, 52, 55, 134, 19, 9 all reproduce; (3) 3771_0100 right, `main_text` char share
  91.5% (3351) to 99.2% (455, 456), 11182 98.0%; (4) per-inventory table in the report; (5) suite
  green, hashes identical.
- **Report:** [§ Step 15g](../../reports/phase-07-sg-units-and-pilot.md#step-15g-line-and-region-types);
  findings F07-15g.1–.10.
- **Error analysis (seed 17):** precision page_number 19/20 (0.95), date_header 13/13 decided (7
  ambiguous register dates), running_header, catch_word, signature_mark, other 20/20, index 19/19,
  marginalia 7/13 (0.54); splits: date_prefix 10/10, pn_suffix 10/10, margin_prefix 2/9 (negative
  result); band recall: furniture left as `main_text` 0/20 hand, 3/20 print (page numbers with
  extra OCR digits); index pages 14/15 decided (0.93), so `index_rule` stays on.
- **Deviations from brief:** (1) margin_prefix cuts at the space nearest the main left edge (within
  `cut_max_dist` 0.04 `page_w`), not the first space past it (the brief's rule gave "Augt. haer |"
  in row 19); (2) per-column main left/centre on pages with several main columns
  (`col_min_char_frac` 0.25), keyed by the print line `column` field where set (UA-008 effect);
  (3) margin rule skipped for lines that fullmatch `date_line`/`pn_line`/`year_line`; (4) a
  running year in the top band is `running_header`, not `pn_line`; (5) `maert` in the month list;
  (6) `TypeParams` has `column_override` (print preset sets it) and `col_min_char_frac`.
  Layer paths and column names are as the brief says, so 15h/15hj/15i needed no path edits.
- **Brief defects:** (1) the rule-2 cut ("first space with x >= main left - 0.005 `page_w`") does
  not give row 19; (2) one main reference per page types the left column of two-column pages as
  marginalia (3771–3823, print); (3) "pn_line before year_line" makes the header rule for years
  unreachable; (4) the protected-hash method was not given (used 15fq's: sha256 of the SG source
  archives, size and mtime of all other files, now also 15fq-*); (5) the "register pages the
  index rule missed" sample (>= 30% digit-ending lines, 3821–3823) is nearly circular: it matched
  1 page, so it says little about the rule's recall.
- **Open questions for the user:** UA-009 (margin_prefix). UA-008 has a response "b"
  (follow-up 15fq2); it is not mine to execute.

### Step 15fq2: column merge guard (2026-10-21, Sonnet)

- **Done:** `LayoutParams.column_merge_guard` (union-find skip when two components' non-empty
  `column` values differ), `n_column_blocked`, layers `15fq-c`, `15fq2-noguard`, `15g-b`.
- **Files changed:** `archival_structures/analysis/page_structure.py`,
  `archival_structures/text_layer/layout.py`, `tests/test_analysis_page_structure.py`,
  `tests/test_text_layer_layout.py`, `docs/text_layer.md`, `docs/shared_library/changelog.md`,
  `reports/phase-07-sg-units-and-pilot.md`, `reports/data/phase-07-sg-units-and-pilot/step-15fq2/`,
  brief 15h (layer `15g-b`, protected list), UA-008 resolved.
- **Public API:** additive (`column_merge_guard`, `column_field`, `column_blocked`); default print
  layout output changes.
- **Tests:** full suite 1,050 passed (1,045 + 5).
- **Acceptance:** (1) 16/16; (2) hand equal in 14/14, `n_column_blocked` 0; (3) noguard equals
  `15fq-b` print; (4) 0 mixed regions (was 36 / 109); (5) 0 merged furniture, residuals furniture
  1,694 / 218, guard 769 / 659, column 39 / 107, other 0; (6) 303 new, 145 lost, 0 locality
  exceptions, `n_column_blocked` 62 / 113; (7) hand types equal, print 53 / 106 type changes,
  main share 0.9921 / 0.9923; (8) hashes identical.
- **Report:** [§ Step 15fq2](../../reports/phase-07-sg-units-and-pilot.md#step-15fq2-column-merge-guard);
  findings F07-15fq2.1-.4.
- **Error analysis (seed 21):** 30 formerly mixed regions: 27 right, 3 ambiguous, 0 wrong; both
  15fq print merge errors fixed; 10 changed 15g segments: 7 better, 3 neutral. Line-end fragments
  labelled with the other column now stand alone (56 of 198 conflicting overlaps).
- **Deviations from brief:** none.
- **Brief defects:** (1) the protected-hash method is described only as "15fq's method"; I used
  `sha256 mtime path` for the SG archives and `size=N mtime path` for every other file, over
  `15d-a, 15fr-a, 15fp-a, 15fq-a, 15fq-b, 15fq-legacy, 15g-a` and all of `shared-data/`;
  (2) `15hj` and `15i` name no 15g layer (generic "every dev layer"), so only 15h was edited;
  (3) the brief's `ea.py` asks for no residual-pair sizes; added `ea_residual.py`.
- **Open questions for the user:** none (UA-009 unchanged).

### Step 15g2: margin cut month-only (2026-10-21, Sonnet)

- **Done:** `TypeParams.margin_cut` (`month_only` default, `all`, `off`); layers `15g-c` (types for 15h)
  and `15g2-all` (regression); tests 1,051 pass.
- **Acceptance:** (1) `15g2-all` equals `15g-b` in 32/32 tables; (2) 423 `rule:margin_prefix`
  segments in `15g-c`, all month `date_header`, equal to `15g-b`'s month cuts per inventory;
  (3) 18,552 changed lines (18,975 - 423; 18,983 was 15g-a's count), 0 unchanged lines differ,
  26 lines gain a `pn_suffix` split; (4) coverage clean, R4 rows all ok, marginalia -18,448,
  main_text +69; (5) hashes identical.
- **Report:** [Step 15g2](../../reports/phase-07-sg-units-and-pilot.md#step-15g2-margin-cut-month-only);
  findings F07-15g2.1-.3.
- **Error analysis (seed 22):** 30 formerly cut lines: 18 right main, 10 right marginalia, 2
  ambiguous, 0 wrong. 15g's two right cuts are now one `main_text` line each.
- **Deviations from brief:** none.
- **Brief defects:** (1) expected change count 18,983 - 423 used 15g-a's figure; `15g-b` had 18,975,
  so 18,552; (2) the brief's `ea.py` right-cut scans need the full `NL-HaNA_1.01.02_` scan_id prefix;
  (3) `acceptance.py` of 15g writes `acceptance_15g-c.json` into `step-15g/`, as the brief says.
- **Open questions for the user:** none (UA-009 resolved).

### Step 15h: units, reading order and the doc_id mapping (2026-10-21, Sonnet)

- **Done:** `text_layer/units.py`, `--stage units` (`--unit-params`, `--republic-dir`),
  `LayerPreset.unit_params`; layer `data/text_layer_v2_dev/15h-a/` (copy of `15g-c` plus `units/`);
  tests 1,060 pass (9 new).
- **Acceptance:** (1) coverage clean in 16/16 (181,472 hand + 53,109 print units); (2) R5: 0 unchanged
  non-empty regions without `same_id`; `old_status` hand identical 129,686 / trimmed 21,523 /
  changed 37 / empty 2,775, 455 4,344 / 499 / 4,500 / 130,751, 456 1,031 / 124 / 1,854 / 90;
  (3) 300/300 gold `hand_doc_id`s present (214 identical, 86 trimmed); (4) row 80 units 24 and 21,
  not adjacent; (5) crosswalk 455: 27,033 exact, 1,864 near, 189 split_original, 55 split_half, 7
  unmatched; 456: 20,192, 13, 512, 13,848, 23; (6) fitted `UnitParams` `indent_frac=0.5,
  short_frac=0.1, gap_factor=None`: dev F1 0.784, test P 0.688 / R 0.708 / F1 0.698; (7) hashes identical.
- **Report:** [Step 15h](../../reports/phase-07-sg-units-and-pilot.md#step-15h-units-reading-order-and-the-doc_id-mapping);
  findings F07-15h.1-.8.
- **Error analysis (seed 18):** 30 hand `trimmed`: 26 right, 1 right with split margin note, 1
  doubtful, 3 unread; print test 20 FP: cue wrong 17; 20 FN: REPUBLIC wrong 14, cue misses 6; 20
  `changed`: all recoverable via the map; 30 unmatched crosswalk ids: 15 are our unsplit lines.
- **Deviations from brief:** crosswalk class `split_half` added (IoU >= 0.5 with one of our split
  halves): the brief's rules left 13,871 of 456's ids unmatched. Briefs 15hj and 15i updated.
- **Brief defects:** (1) 5 px `near` is too tight for 456 (REPUBLIC splits merged lines itself); (2)
  "expect `n_id_collisions` 0" is wrong for print (58 / 32: a unit's bbox equals a legacy region's
  box that is not a unit); (3) the protected-hash method is again only "15fq's method" (used
  `size mtime path`, sha256 for `shared-data` archives; script `step-15h/protected_hash.py`);
  (4) the brief does not say where the REPUBLIC paragraphs path is read in the stage (default is
  LotA's `data/paragraphs/NL-HaNA_1.10.94`, `--republic-dir` overrides); (5) `ea.py` also reads
  `pairs_test.tsv.gz`, which `fit_paragraphs.py` writes.
- **Open questions for the user:** none.

### Step 15hj: word breaks and unit text with offsets (2026-10-21, Sonnet)

- **Done:** `text_layer/join_v2.py` (`join_unit`, `read_unit_texts`, v2 wbc/join workers); `robust_counts`
  and `robust=` in `word_breaks.py`; preset fields `word_break_rule/_stat`, `wbc_sanity_in/_out`; `build.py`
  v2 `wbc`/`join`, `--allow-subset-wbc`, manifest `history`, v2 `--stage all` = extract -> ... -> join.
  Layer `data/text_layer_v2_dev/15hj-a/{hand_tar,hand_local,print}/` (15h-a + `wbc/` + `join/`).
- **Acceptance:** (1) 0 offset violations in 1,650,009 segments / 234,581 units; (2) `join_unit` == `join_region`
  in 0 differences; (3) legacy text: 129,686 hand `identical` units, 0 failures; (4) default == robust in
  all 16, not adopted, sanity hand_tar True / hand_local False (`:`) / print False (`.`), digit-digit merges 0;
  (5) lower-case share hand 0.954 overall (3822 0.779, 11182 0.699, 3823 0.941 below 0.95); (6) v1
  `TOTAL 157 / 157`, suite 1,098 passed, protected hashes identical.
- **Report:** [Step 15hj](../../reports/phase-07-sg-units-and-pilot.md#step-15hj-word-breaks-and-unit-text-with-offsets);
  findings F07-15hj.1-.8.
- **Error analysis (seed 19):** hand merges 15/15 right, print 15/15; guarded 11 right / 6 wrong / 3 unclear;
  upper/digit-right merges 4 right / 15 wrong; `.` merges 455 11/20, 3822 4/10; `:` 11281 9/10; `=` 7/10;
  `„` line ends in 3351/3354 12/12 genuine (the rule misses them).
- **Deviations from brief:** none in paths, columns or constants (15i got a note on `--ignore-sanity`).
- **Brief defects:** see the report (rule "copy" has no column; three layer roots so wbc is per root; no
  instruction for failing sanity; an old test pinned the v2 refusal; fixture dir name).
- **Open questions for the user:** UA-010 (WBC sets for the pilot).

### Step 15hj2: pinned WBC sets (2026-10-21, Sonnet)

- **Done:** `LayerPreset.wbc_pinned`; SG presets pin `-=„` (11281 `-:=„`); `build.decide_v2` applies the pin
  (`mode pinned`, `char_status` suffixes, corpus `pinned/derived_default_set/pooled_rule_sanity_pass`,
  `sanity_pass` on the pinned default set); manifest `wbc.pinned`. Two new tests in
  `tests/test_text_layer_word_breaks.py`. Layer `data/text_layer_v2_dev/15hj-b/` = 15hj-a with wbc + join re-run.
- **Acceptance:** (1) pinned sets in all 16, `sanity_pass` true x3, `ignore_sanity` false; (2) all upstream
  tables + `wbc/parts` + `wbc_stats.tsv` byte-identical to 15hj-a; (3) offsets 0 violations, join==region 0,
  legacy 0 failures, 0 digit-digit; (4) 3822 21,486 -> 10,929 merges (0.779 -> 0.957), 455 50,164 -> 38,932
  (0.877 -> 0.891), 3351 0 -> 174, 3354 0 -> 126, hand_tar `=` +821 (0.74 lower); (5) v1 157/157, suite 1,100
  passed, protected hashes unchanged (except a concurrent LotA `output/titles` write).
- **Report:** [Step 15hj2](../../reports/phase-07-sg-units-and-pilot.md#step-15hj2-pinned-wbc-sets); F07-15hj2.1-.4.
- **Error analysis (seed 23):** new hand_tar merges 11 right / 5 wrong / 4 unclear; `.` removal right 6/10
  (455) and 7/10 (3822); 11281 `:` 5/5 right.
- **Brief defects:** changelog placement (newest first); `-` barely present in hand_tar; hash format unspecified.
- **Open questions for the user:** none (UA-010 resolved).

### Step 15i Part A: pilot build, audit, 116-row evidence (2026-10-21, Sonnet)

- **Done:** step 0 skipped (0.9.1 not on PyPI; UA-007 open; 0.9.0 kept). Pilot `data/text_layer_v2_dev/15i-a/{hand,print}/`
  (hand 14 inventories from `15i-src/`, 2:31; print 4:44; exit 0). Scripts in
  `reports/data/phase-07-sg-units-and-pilot/step-15i/` (`protect.py`, `repro.py`, `audit.py`, `rows116.py`,
  `rows116_verdicts.tsv`, `crops.py`); no library code or test changes.
- **Acceptance:** (1) byte-identical to 15hj-b except the `member`/`source` dev path of the 8 local hand inventories
  and `char_status` strings (hand_tar); (2) audit 176/182, the 6 failures are the known merge lower-case share < 0.95;
  (3) hand tokens found 111/116, print 110/116; predicted share free 0.526 (61/116), upper bound 0.560, target 0.80
  not reachable (40 of 51 artefact rows persist: 26 glue, 14 HTR/OCR); (4) crops for 33 rows, `crops/index.html`.
  Suite 1,100 passed. Protected hashes unchanged (two LotA `output/test-staged-fixture-2.5b` mtimes from a concurrent
  session).
- **Report:** [Step 15i](../../reports/phase-07-sg-units-and-pilot.md#step-15i-pilot-build-audit-116-row-evidence-and-publication);
  F07-15i.1-.6.
- **Brief defects:** row 60 (user example) is unlabelled in 02d (evidence in `rows_extra.tsv`); "scans with text lines have
  >= 1 unit" needs `status == ok` (`partial_shot_dropped` scans carry counts but no rows); the brief gives no way to find
  the print paragraph's units between `line_start` and `line_end` (done by unit order, with a scan-wide fallback for exact
  matches); `rows116_verdicts.tsv` holds the hand-read verdicts (not in the brief); print `member`/`source` columns of
  the dev layer hold dev paths (15i-src symlink path), relevant for Part B's manifest; the protect-hash script is new
  (`protect.py`, format `size mtime path`).
- **Open questions for the user:** UA-011 (crops, evidence, publish decision, `=` pin in hand_tar).

### Step 15i2: hand_tar pin without = (2026-10-21, Sonnet)

- **Session:** Sonnet, 2026-10-21. **Status:** done
- **Files changed:** `archival_structures/text_layer/presets.py` (`SG_HAND_V2.wbc_pinned`), `tests/test_text_layer_word_breaks.py`
  (`test_sg_hand_pins`), `docs/text_layer.md`, `docs/shared_library/changelog.md`; new layer `data/text_layer_v2_dev/15i-b/{hand,print}/`;
  `reports/data/phase-07-sg-units-and-pilot/step-15i2/`; report; brief 15i Part B; UA-011 resolved, UA-012 added.
- **Public API:** preset data only.
- **Tests:** `.venv/bin/python -m pytest -q` -> 1,101 passed.
- **Acceptance:** (1) build exit 0, sets as specified, `sanity_pass` true; (2) all tables except `join/` identical in 14 inventories;
  `join/` identical in 3352 + 8 hand_local, differs in 3349, 3350, 3353 **and 3351, 3354** (they have `=` line ends too); print
  byte-equal (54 files); (3) 820 merges removed (276, 226, 318; 818 at `=`), 3 in 3351, 0 in 3354; lower-case share
  0.9806 -> 0.9887; (4) audit 176/182, same 6 failures; (5) 116 rows, hand 111, print 110, no row differs, share free 0.526,
  bound 0.560; (6) protected hashes: no difference.
- **Report:** [Step 15i2](../../reports/phase-07-sg-units-and-pilot.md#step-15i2-hand_tar-pin-without-); F07-15i2.1-.4.
- **Error analysis:** 20 removed `=` merges: 11 right to split, 6 wrong to split (compounds), 3 unclear.
- **Brief defects:** 3351 and 3354 do change (3 merges in 3351); the "no `=` merges" claim was not measured. `protect.py` of
  15i skips every `15i-*` dir, so a copy (`step-15i2/protect.py`) skips only `15i-b`, `15i-src` and `15i2-*`. `repro.py` of
  15i compares to 15hj-b; a new all-files comparison was written. The brief names no way to sample the removed merges; `deltas.py` writes `removed_eq.tsv`.
- **Open questions for the user:** UA-012 (publish `15i-b`?).


### Step 15i Part B: publication (2026-10-21, Sonnet)

- **Session:** Sonnet, 2026-10-21. **Status:** done (UA-012 answered a)
- **Files changed:** outside AS: `shared-data/NL-HaNA_1.01.02/text_layer_v2/` (305 MB), `shared-data/NL-HaNA_1.10.94/text_layer_v2/` (84 MB),
  shared-data `README.md` (2 catalogue rows, 1 changelog line), LotA `docs/from-archival-structures.md` (new section 6). In AS:
  `docs/text_layer.md` (section *Published SG pilot*), report, `step-15i/{publish_manifest.py,audit_published.json,15i-protected-B-*.txt}`,
  UA-012 moved to resolved. No library code.
- **Tests:** not run (no code changed).
- **Acceptance 5:** published copies pass the audit 176/182 (same 6 merge-share failures as `15i-b`; invariants identical); manifests carry
  `publication`; protected hashes: only shared-data README and the new layer files changed.
- **Report:** [Step 15i](../../reports/phase-07-sg-units-and-pilot.md#step-15i-pilot-build-audit-116-row-evidence-and-publication), Part B paragraph.
- **Brief defects:** `audit.py` globs `*_text_layer_v2`, so the audit of the published dirs needed symlinks with that suffix; the brief's
  "3351 and 3354 changed" holds only for 3351 (3 merges). `as_version` is the build's recorded 0.9.0 (pyproject says 0.10.0).
- **Open questions for the user:** none.

### Phase review (2026-10-21, Opus)
- **Session:** Opus 5.5, 2026-10-21. **Status:** done; verdict accept with follow-ups ([REVIEW](REVIEW.md)).
- **Asked by the user:** review the Sonnet code of phase 07, and propagate the accepted deviations
  into `plan/PLAN.md` and `plan/CONTEXT.md`. `CONTEXT.md` did not exist, so it was created and wired
  into `plan/README.md` and `PROMPTS.md` (prompts 1 and 3).
- **Read:** the phase `PLAN.md`, `STATUS.md` and `PROGRESS.md`, the report's findings table and its
  15i/15i2 sections, `join_v2.py`, and the parts of `page_structure.py`, `layout.py`, `types.py`,
  `units.py`, `build.py` and `presets.py` named in REVIEW. Four review subagents covered the layout,
  types, units and join/wbc/build diffs.
- **Re-run:**
  - AS suite: 1,101 passed. NLP: 56 passed. LotA: 367 passed, 1 xfailed;
  - v1: 157 / 157;
  - audit and `rows116` on `15i-b`: reproduce exactly;
  - published dirs equal `15i-b`;
  - offsets re-derived: 0 mismatches.
- **Found:** F07-R.1–R.9, five of them defects:
  - (R.1) print line order;
  - (R.2) the `,` continuation mark;
  - (R.3) legacy-twin ids;
  - (R.4) page-number tags;
  - (R.5) inflated blocked counters.

  Leakage check of the 15h fit: clean (F07-R.6).
- **Report:** status final. 73 findings confirmed, 8 revised (F07-15fq.7, 15fq.8, 15fq2.1, 15g.6,
  15h.3, 15h.8, 15hj.5, 15i.2). *Summary*, *Review* and *Follow-ups* written. 90 rows appended to
  `reports/index.md`, plus the phase row. `make reports` is clean, with no broken anchors.
- **Files:**
  - new: `REVIEW.md`, `step-15ir-review-fixes.md`, `plan/CONTEXT.md`;
  - edited: the phase `PLAN.md` (status, 15ir row, D24, Hand-off), `STATUS.md`, `plan/PLAN.md`
    (rows 06–08, CONTEXT pointer), `plan/README.md`, `plan/PROMPTS.md`, `plan/templates/STEP.md`
    (protected-hash method), `docs/USER_ACTIONS.md` (UA-013).
- **Not done:** `docs/findings.md` (the candidates are listed in the report's *Review*). The changelog
  gaps go into 15ir.
- **Open questions for the user:** UA-013; the commit and v0.11.0 release; UA-007.

### Step 15ir: phase 07 review fixes (2026-10-21, Sonnet)

- **Session:** Sonnet 5.5, 2026-10-21. **Status:** done; acceptance 1-9 met, with the readings below (criteria 3 and 5 differ from the brief's wording or number).
- **Files changed:** `archival_structures/analysis/page_structure.py`, `text_layer/{layout,types,units,join_v2,build,presets}.py`;
  tests `test_analysis_page_structure.py`, `test_text_layer_{layout,types,units,join_v2,word_breaks}.py` (22 new tests);
  `docs/text_layer.md`, `docs/shared_library/changelog.md` (new `## Unreleased` + `### Step 15ir`, 15fq2 correction);
  LotA `docs/from-archival-structures.md` §6 (*Known issues*); report section + F07-15ir.1-.10; new dev layer
  `data/text_layer_v2_dev/15ir-a/{hand,print}`, `15ir-v1check/`, `15ir-protected-{before,after}.txt`;
  `reports/data/phase-07-sg-units-and-pilot/step-15ir/` (scripts, json, samples).
- **Public API:** additive. `LayoutParams.sort_split_lines`, `split_regions_on_line_field(sort_lines=)`, `build_units_page(claimable=)`,
  `join_unit(lead_marks=)`, `join_inventory_v2(lead_marks=)`, `LayerPreset.join_lead_marks`; new `ValueError` validation in
  `LayoutParams` and `TypeParams`; output counts and `type_method` change. No sibling imports the names.
- **Tests:** AS 1,123 passed (was 1,101); NLP 56 passed; v1 `TOTAL 157 / 157`; protected hashes identical.
- **Acceptance:** hand identical except `types` (type_method, 18 date lines) and `join` (comma strip); print `layout/regions` identical,
  `layout/lines` differ in 22,115 `layout_line_idx`; units with a jump 335 -> 77 (455), 160 -> 7 (456); `n_id_collisions` 58 -> 0, 32 -> 0;
  `pn_line` 10,327 -> 8,826; 1,856 commas stripped, 11182 share 0.699 -> 0.975; 0 offset violations; audit 178/182; 116 rows unchanged.
- **Error analysis:** fix 1 17 right / 2 ambiguous / 1 wrong; fix 5 29 / 1 unclear; fix 3b 18 not page numbers / 1 real / 1 unclear; twins 10 / 10.
- **Report:** [Step 15ir](../../reports/phase-07-sg-units-and-pilot.md#step-15ir-phase-07-review-fixes); F07-15ir.1-.10.
- **Brief defects:** 11 hand date lines is 18 (19 with 455); the comma counts of fix 5 include `,` + space (65) and `,,` (5); n_id_collisions 13 holds
  for fix 4 alone, 0 with fix 1; the jump metric by box top y leaves 15 drop-capital boxes (0 by baseline y); *Unreleased* heading was missing.
- **Not done:** `line_type` of the 1,501 off-chain `pn_line` segments; `, ` + letter lead mark; row-tolerant sort; sorting `unchanged` regions.
- **Open questions for the user:** none.

### Review of step 15ir (2026-10-21, Opus)

- **Session:** Opus 5.5, the phase-review session. UA-013 was answered (a): it was recorded in the brief, `STATUS.md`
  and `plan/CONTEXT.md`, and moved to `docs/USER_ACTIONS_resolved.md`. 15ir then ran as a Sonnet subagent with prompt 2.
- **Verdict:** accept ([REVIEW](REVIEW.md#step-15ir-review-2026-10-21)). Release v0.12.0.
- **Re-run:**
  - AS: 1,123 passed.
  - Audit (the 15i original) on `15ir-a`: 178 / 182.
  - The 116 rows differ from `15i-b` in 8 cells. The predictions are unchanged.
  - Commas between letters: 11182 1,315 → 15, 3352 342 → 6.
  - Offsets re-derived for 11182, 3352 and 455: 0 bad.
  - Split regions with a backward baseline jump: 410 → 0 (455), 526 → 0 (456).
  - `make reports`: 0 warnings.
- **New findings:**
  - F07-R.10: the re-run.
  - F07-R.11: `claimable` takes quadratic time.
  - The step's F07-15ir.1–.10 are confirmed.
- **Updated:**
  - the report (header, Summary, finding statuses, Review, Follow-ups) and `reports/index.md` (12 rows);
  - `REVIEW.md`, `plan/CONTEXT.md` (§1, §2 and §4 now record the 15ir behaviour; §5 has the brief defects);
  - `plan/PLAN.md` row 07, the phase `PLAN.md` (status, 15ir row, hand-off) and `STATUS.md`.
- **Open questions for the user:** the commit and v0.12.0; UA-007.

