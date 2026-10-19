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
