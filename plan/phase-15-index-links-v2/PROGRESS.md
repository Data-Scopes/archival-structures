# Phase 15 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning (Opus 5.5, 2026-10-29/30)
- **User decisions:**
  - phase 15 = index-links v2;
  - gold from the 29 local untouched front-index volumes only (nothing staged);
  - the archive switch is phase 16, after 15, with 1.04.02 VOC first (seeded; staging UA-054).
- **Read:** the phase-14 PLAN/STATUS/report (*Review*, *After the review*, *Follow-ups*), CONTEXT §1–§5 and the
  phase-13 column-order results (B1/B2, F13-R.10). An Explore agent supplied the current
  `folio_map`/`index_links`/builder API, the tests (13 + 10), the v5 headers and the 20c gold columns. No sibling
  imports either module.
- **Probes** (`reports/data/phase-15-index-links-v2/planning/`):
  - `probe_pool.py`: 29 volumes, 2,472 front sides, 51,808 v1 entries; the no-locator unit share is 0.11–0.47, and
    ≥ 0.25 in 13 volumes. The chain ends above the scan count in 7 of 41 dev volumes and 32 of 412 full-run volumes.
  - `peek_side.py` (2 sides, excluded from the draw): 3444 interleaves two index columns line by line; 3459's right
    column is typed `marginalia`.
  - `map_v2_proto.py`: it reproduces the phase-14 LOO (21,010/21,334). Measured on v2:
    - LOO v2 20,952/21,176;
    - 7 chain ends fixed;
    - 166 tied numbers dropped;
    - 119 `high` links above the scan count → 0 (54 legitimate 3712 links stay high);
    - spent 20c gold 49 → 50 of 51.

    A scan- or side-count bound was tried and rejected: 3712's numbering continues at 348.
  - `assembly_proto.py` (spent volumes only): the line rule and the per-side reorder run on duck-typed lines. The
    reorder fires on 26–85% of front sides, and 96–99% of line entries end in a number.
  - At planning, L3's `amount_before` drops 252 in `Hof 252.`.
- **Draw** (`draw_sides.py`, seed 20261030): 13 dev and 16 test volumes, stratified by no-locator share; 1 read +
  1 reserve side each; 579 dev and 684 test lines to read.
- **Written:**
  - `PLAN.md` (P1–P12);
  - briefs 21a–21d;
  - `STATUS.md`;
  - the report skeleton `reports/phase-15-index-links-v2.md`;
  - `planning/protect15.py` (run once, 7.5 s);
  - the phase-16 seed `plan/phase-16-voc-generalisation/` and UA-054 with `docs/staging/voc_phase16_inventories.{tsv,txt}`
    (63 inventories);
  - the rows in `plan/PLAN.md`, and a note in `plan/CONTEXT.md` §4.
- **Brief sizes:** 21b and 21c run slightly over ~250 lines (copied-in API); not split.
- **Open:** the v0.19.0 release (user) before 21b's changelog edit.

### Step 21b: folio map v2
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** done
- **Files changed:** `archival_structures/analysis/folio_map.py`, `tests/test_analysis_folio_map.py`, `docs/shared_library/changelog.md` (Unreleased), `reports/phase-15-index-links-v2.md`, `reports/data/phase-15-index-links-v2/step-21b/` (`eval_map.py`, `error_analysis.py`, `volumes.tsv`, `loo.tsv`, `eval_map.txt`, `error_analysis.txt`, `protect_{before,after}.txt`)
- **Public API:** `FolioMapParams.jump_k/jump_c/dup_window/gap_slack/opening`, `FolioTarget.opening`, `FolioMap.n_dup_dropped/chain_cut`, `FOLIO_MAP_V2`; table gains `opening_scan_id/opening_page` only with `opening=True`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests` → 1,388 passed, 11 skipped (folio_map module: 20)
- **Acceptance:** 1 pass; 2 v1 tables identical in 41/41 volumes (36,349 rows); 3 reproduces exactly (LOO 21,010/21,334 and 20,952/21,176; 166 ties in 29 volumes; the 7 chain ends; 3712 pagination 205→203 ending 684; no mode change); 4 bar F PASS: v2 62/63 = 0.984 (Wilson [0.915, 0.997]), v1 62/63, dev 26/26 both; 5 only 21a and 21c files differ; suite 1,397 passed, 11 skipped
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21b](../../reports/phase-15-index-links-v2.md#step-21b-folio-map-v2); findings F15-21b.1–5
- **Error analysis:** the 7 dropped chain ends are years/inventory/garbles; of 10 seeded G3 downgrades, 8 are real inconsistencies, 1 a G2 distance effect, 1 a 3712 false alarm. Test: one miss (T034 3274, garbled printed numbers), no v1/v2 disagreement.
- **Deviations from brief:** none (parts 3-4 not run by instruction; `eval_map.py --part volumes|dev|test`)
- **Brief defects:** `content_sides` value format is unknown (parsed as lower-case word tokens containing front/back/opening/next); target_gold read untested. The G3 sample pool also contains links changed by G2/G1, not only G3.
- **Open questions for the user:** none

### Step 21a: fresh gold reads (session 1)
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** partial (dev lines done; dev targets, test sides, targets, confirm page to do)
- **Files changed:** `reports/data/phase-15-index-links-v2/step-21a/` (`_common.py`, `show_side.py`, `show_scan.py`, `draw_targets.py`, `write_side.py`, `validate_gold.py`, `side_verdicts.tsv`, `line_gold.tsv`); `plan/phase-15-index-links-v2/STATUS.md`
- **Public API:** none
- **Tests:** none (`validate_gold.py` passes on S01-S13)
- **Acceptance:** 1: dev part holds (13 `index` sides, 572 lines, 260 entries, 389 locators; validator OK). 2: dev 13 >= 11. 3, 4: not reached (targets not drawn, test not read).
- **Report:** not yet written (later session)
- **Error analysis:** read line by line; hard cases are in the `note` column of `line_gold.tsv`.
- **Deviations from brief:** added helpers `_common.py` and `write_side.py` (joins my hand-written labels to line ids; decides nothing). `draw_targets.py` tested on a scratch copy only; `targets.tsv` does not exist yet.
- **Brief defects:** (1) a merged line can hold a whole entry of the other column that has no line of its own; one label per line means that entry cannot get an `S` line, so it is left unlabelled (note only; 13 such entries: S02 3, S03 2, S09 7, S10 1). (2) S10 has a third, left column fragment that the L/R/S `col` values cannot express. (3) `N@E` is also used for numbers of another entry on a merged continuation line.
- **Open questions for the user:** none yet (UA for the confirm page comes later)

### Step 21a: fresh gold reads (session 3)
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** partial (test lines S27-S34 done; S35-S42, dev targets, test targets, confirm page, hashes, report to do)
- **Files changed:** `reports/data/phase-15-index-links-v2/step-21a/` (`side_verdicts.tsv`, `line_gold.tsv`, `embedded_entries.tsv` appended)
- **Public API:** none
- **Tests:** none (`validate_gold.py` OK on S27-S34)
- **Acceptance:** 1: S27-S34 all `index`, no reserve; 8 sides, 310 lines, 188 entries, 4 embedded entries (S29, S30, S31, S33). Lines/entries/locators per side: S27 53/35/57, S28 28/13/22, S29 35/16/39, S30 41/27/41, S31 19/11/13, S32 27/21/26, S33 60/41/63, S34 47/24/38.
- **Report:** not yet written
- **Error analysis:** read line by line; hard cases in `note`.
- **Deviations from brief:** none
- **Brief defects:** (1) a side with sub-lemmas (S33 Patenten/Petitien, S34 petitie headings) has multi-line headings; labelled `H` per line. (2) Short marginal letters/stray marks (S32 k13-14) labelled `F`. (3) Entries starting on the previous page (S32 k1) get `S` with a note. (4) A short right-aligned fragment (S30 k32 `491: 4`) is `C`.
- **Open questions for the user:** none

### Step 21c: locators v2 and entry assembly
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** done (bars L-recall, A, E failed: negative result)
- **Files changed:** archival_structures/analysis/index_links.py, tests/test_analysis_index_links.py, docs/shared_library/changelog.md, reports/data/phase-15-index-links-v2/step-21c/{eval_links.py,spent_check.py,dev_grid.tsv,select.json,dev_errors.tsv,dev_extra.json,eval_links.txt,spent_check.txt,protect_*.txt}, reports/phase-15-index-links-v2.md
- **Public API:** L5-L7, LocatorParams.sub_entry_punct/left_boundary, SG_AMOUNT_BEFORE_V2, SG_ENTRY_END, LineBox, Entry, read_side_lines, side_reading_order, assemble_entries, entry_locators
- **Tests:** pytest tests/test_analysis_index_links.py -> 19 passed; full suite 1397 passed, 11 skipped (1408 collected)
- **Acceptance:** 1 holds. 2: dev_grid.tsv has 16 rows; selection A0 + L7 (dev link F1 0.8303) follows the rule; test_done.json not written yet. 3: test once (A0 + L7): P 404/412 = 0.981 pass; R 404/472 = 0.856 fail; A 355/398 = 0.892 fail; E 52/63 = 0.825 fail; v1: 0.967, 0.862, 0.893, 0.825. 4: protected trees differ only in 21a and 21b files. Full suite 1397 passed, 11 skipped.
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21c](../../reports/phase-15-index-links-v2.md#step-21c-locators-v2-and-entry-assembly); findings F15-21c.1-7
- **Error analysis:** test: FN 67 (53 marginalia), FP 8, attribution 43 (22 cut, 12 heading, 9 merged), E misses 11 (locator 5, entry 5, map 1). Dev: A0 52 attribution errors (41 cut mid-entry, 11 merged); A1 139 (67 heading attached); A3 158 headings vs 47 gold; spent 3590/3611: 6/10 whole.
- **Deviations from brief:** new test helper `_write_side_layer` instead of extending `_write_layer`; heading rule also needs a letter in the line.
- **Brief defects:** "previous entry" for a numbers-only line read as the previous non-heading entry; `used_for` reserves not defined (all index sides of the split counted).
- **Open questions for the user:** none

### Step 21a: fresh gold reads (session 6)
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** partial (test targets T059–T090 read; T027–T058 by the parallel session; confirm page, hashes, report, UA to do)
- **Files changed:** `reports/data/phase-15-index-links-v2/step-21a/target_gold.tsv` (32 appended rows T059–T090); `STATUS.md` (21a row only)
- **Public API:** none
- **Tests:** none
- **Acceptance:** 3 (partial): of 32 rows, 31 have a scan and `folio_evidence` printed; T083 has `target_scan_id` none (beyond volume, 1182 > about 694).
- **Report:** not yet written
- **Error analysis:** test: FN 67 (53 marginalia), FP 8, attribution 43 (22 cut, 12 heading, 9 merged), E misses 11 (locator 5, entry 5, map 1). Dev: `content`: 29 leaf, 2 absent (T075, T089: text-searched neighbouring scans), 1 unclear (T083, no scan). `content_sides`: back 9, back;next 7, front 5, front;back 5, front;back;next 3. `scans_opened` mean 2.8 (90/32; same convention as session 2: scans read individually, bulk number listings not counted; a repeat target on an already-open scan counts 1). Volume offsets: 3353 35; 3438 43; 3459 46 (scan 100) then 49; 3507 49; 3522 44 (scan 100), 50 (122-514), 49 (247-300); 3547 58; 3572 41-42; 3630 55.
- **Deviations from brief:** T083 `folio_evidence` is `printed` (the printed numbers of the volume were read, the target N is beyond it); a strict reader may prefer `unclear`; scan is `none` as the brief requires. T071 (index "Sarreij" vs text "de Lameij") and T073 ("la Croix" vs "la Cron") judged same person.
- **Brief defects:** `show_scan.py` does not list the folio number on recto when it is the last token of a long first line (volume 3353, 3459 partly); I read it from the first recto line. The brief gives no `content` value for a `none` target.
- **Open questions for the user:** none

### Step 21a: fresh gold reads (session 5)
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** partial (test targets T027–T058 done; confirm page, hashes, report, UA to do)
- **Files changed:** `reports/data/phase-15-index-links-v2/step-21a/target_gold.tsv` (32 appended rows T027–T058); `STATUS.md` (21a row only)
- **Public API:** none
- **Tests:** none
- **Acceptance:** 3 (partial): 32 rows, all with a scan; `folio_evidence` 18 printed, 14 neighbours, 0 unclear.
- **Report:** not yet written
- **Error analysis:** test: FN 67 (53 marginalia), FP 8, attribution 43 (22 cut, 12 heading, 9 merged), E misses 11 (locator 5, entry 5, map 1). Dev: `content`: 30 leaf, 1 absent (T028), 1 unclear (T037). `content_sides`: front 11, back 7, front;back 6, back;next 4, front;back;next 2, opening;front 1, none 2. `scans_opened` mean 3.4. T028 (vercoopers van haver, garot): nothing on the four sides or scans 58-64. T037 (Nieuwe wervinge van militie): front only has "verstercken vande militie", no recruitment wording. T030 and T033 are garbled index words read through a likely match (Ingeloste Heerlickheden; remplacement of a lost ship).
- **Deviations from brief:** `scans_opened` counts the scans opened individually (first guess, the target, the neighbours used), not the bulk number listings; I used a local shell loop of `show_scan.py --full` for those listings. `target_gold.tsv` rows with `evidence` > 80 characters were cut at 80 (T040, T053).
- **Brief defects:** (1) `show_scan.py` `[num]` lines only catch a folio number that stands alone on its line. Volumes 3349 and 3350 (and partly 3265, 3290, 3303) print the number at the END of a long text line ("... tot 200."), so I first calibrated from sparse standalone numbers. Several rows I wrote as `neighbours` before I found the line-end numbers are in fact printed: T029 (121), T031 (650), T038 (450), T043 (409), T047 (18), T048 (19), T049 (25), T050 (42). I did not rewrite the file (the other session appends to it); the orchestrator may change these 8 cells to `printed`. (2) Folio offsets drift inside one volume: 3349 offset 38 near scan 104, 35 by scan 243; 3265 offset 46 early, 47 by scan 785; 3283 29 then 28; so each target needs local calibration. (3) The brief has no value for an entry whose words are garbled beyond a safe match (T030, T037); I used `leaf`/`unclear` with the doubt in `note`.
- **Open questions for the user:** none

### Step 21a: fresh gold reads (session 7)
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** done (gold complete, hashed, frozen)
- **Files changed:** `reports/data/phase-15-index-links-v2/step-21a/` (`target_gold.tsv` sorted and 9 cells edited, `pre_freeze_edits.tsv`, `show_scan.py`, `validate_gold.py`, `summary.py`, `summary_output.txt`, `build_check_page.py`, `check_page.html`, `check_items.json`, `hashes.txt`); `docs/USER_ACTIONS.md` (UA-055); `reports/phase-15-index-links-v2.md` (§ Step 21a, F15-21a.1-7); `STATUS.md`
- **Public API:** none
- **Tests:** none (`validate_gold.py` OK on all 29 sides, 21 embedded rows and 90 target rows; `make reports` clean)
- **Acceptance:** 1 holds: 29 `index` verdicts (dev 13, test 16), no reserve; 1,251 lines labelled with the five labels, 595 entries each with one `S` line, 19 `@` tokens all on the same side. 2: dev 13 >= 11, test 16 >= 14. 3: 90/90 targets read; usable test 63 >= 50, usable dev 26 >= 20 (`folio_evidence` printed 81, neighbours 8, unclear 1). 4: confirm page 20 items from the 63-row test pool (seed 20261031), 3 scan links per item, CheckStore key `as_phase15_21a_links_v1`, JS syntax ok (`osascript`); UA-055 written; `hashes.txt` written (5 files).
- **Pre-freeze edits** (`pre_freeze_edits.tsv`, 9 cells): T029, T031, T038, T043, T047, T048, T049, T050 `folio_evidence` `neighbours` to `printed` (each checked by printing the first 2 lines of the target recto: the number stands at the end of line 1 or 2, e.g. T029 `ende gearresteert sijn de depesches 121`); T083 `printed` to `unclear` (note `beyond volume` kept). `target_gold.tsv` sorted by `target_id`; T001-T090 appear once and match `targets.tsv`.
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21a](../../reports/phase-15-index-links-v2.md#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets); findings F15-21a.1-7
- **Error analysis:** test: FN 67 (53 marginalia), FP 8, attribution 43 (22 cut, 12 heading, 9 merged), E misses 11 (locator 5, entry 5, map 1). Dev: all 1,251 lines and 90 targets read; index accuracy 82/89 (matter on front or back of folio N), matter on the front in only 45/89; 7 of 29 volumes change their scan-folio offset; 21 of 595 entries are split by another entry in `show_side` order.
- **Deviations from brief:** the confirm page has no image crops (the phase-14 page had them for UA-050; the brief does not ask for them and they need the network); it has fold-outs with the extracted text of the two target pages. `summary.py` also writes `summary_output.txt` when redirected (no code change; the file is a saved run).
- **Brief defects (all sessions):**
  1. A merged line can hold a whole entry of the other column with no line of its own, so it cannot get an `S` line (session 1; amended 2026-10-30 with `embedded_entries.tsv`, 21 rows).
  2. A third, left-column fragment (S10) cannot be expressed by the `col` values `L`/`R`/`S`.
  3. `N@E` also marks numbers of another entry on a merged continuation line, not only the end of a mid-line entry.
  4. Multi-line headings and sub-lemmas (S33 Patenten/Petitien, S34, S35) are labelled `H` per line; short marginal letters and stray marks (S32 k13-14) are `F`; an entry that starts on the previous page (S32 k1) gets `S` with a note; a short right-aligned fragment (S30 k32 `491: 4`) is `C`.
  5. `show_scan.py` listed only folio numbers standing alone on a line; volumes 3265, 3274, 3280, 3290, 3303, 3349, 3350 print the number at the end of a long first line. Fixed in session 7 (`[end-num]` lines for the first and last line of a side).
  6. Folio offsets drift inside one volume (3349, 3265, 3283, 3459, 3522, 3303, 3572), so each target needs local calibration; the brief's "jump by the difference" procedure assumes a constant offset.
  7. The brief has no `content` value for a target with `target_scan_id` `none` (T083) or for an entry whose words are garbled beyond a safe match (T030, T037); `folio_evidence` for `none` was ambiguous (set to `unclear`).
  8. The brief counts 1,263 layout lines; the non-empty text lines of the 29 sides are 1,251.
  9. The `scans_opened` convention is not defined (bulk number listings); sessions 2, 5 and 6 counted scans read individually.
  10. Sessions 2 and 4 wrote no `PROGRESS.md` block; their facts are in the `STATUS.md` row (session 2: dev embedded entries and T001-T026; session 4: S35-S42, 8 sides, 369 lines, 147 entries, 4 embedded).
  11. `validate_gold.py` had no check of `target_gold.tsv` against `targets.tsv`; added in session 7.
- **Open questions for the user:** UA-055 (confirm page)

### Step 21d: the index_links_v2 layer
- **Session:** Sonnet 5.5, 2026-10-30. **Status:** done
- **Files changed:** `archival_structures/analysis/index_links.py`, `scripts/build_index_links.py`, `tests/test_analysis_index_links.py`, `docs/index_links.md`, `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-056), `reports/phase-15-index-links-v2.md`, `data/index_links_dev/21d-a/` (v2), `21d-v1/` (v1 rebuild), `reports/data/phase-15-index-links-v2/step-21d/`
- **Public API:** `volume_links_v2`, `V2_CONFIG` (A0 + L7 + `FOLIO_MAP_V2`), `ENTRY_COLUMNS`, `LINK_COLUMNS_V2`; builder `--version {v1,v2}` (default v1; `--variant` is an error with v2)
- **Tests:** `pytest tests/test_analysis_index_links.py` -> 24 passed; full suite 1402 passed, 11 skipped (1413 collected)
- **Acceptance:** (1) green; (2) v1 rebuild equals `20r-a` in 41+41+41 files and `volumes.tsv`; (3) v2 dev: 41 done, 0 failed, 0 no_front_index, 72,558 entries, 100,019 locators, 99,997 located, every link's entry in `entries/`, 0 links beyond chain +-6; manifest has version/config/bars; (4) compare, silver, sample, docs, UA written; (5) protect lists identical
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21d](../../reports/phase-15-index-links-v2.md#step-21d-the-index_links_v2-layer); findings F15-21d.1-6
- **Error analysis:** 20 sampled links: 16 right (1 probable), 2 wrong scan (interpolated, drifting offsets), 2 untestable. Silver: v2 0.464 vs v1 0.455 overall, 0.368 vs 0.357 in the high stratum (gain from L7/map, not assembly). Top-5 locator changes (3623, 3712, 3611, 3630, 3587) are all L7 range/left-boundary/exclusions; G2 moves 31-35 targets in 3630 and 3587.
- **Deviations from brief:** A0 selected, so `read_side_lines` is not called in the builder (only for A1-A3); chain-end check counts 21 v1 `high` links as still located (`extrapolated`/`low` within +6 of the new end) where the brief expected none; the 71 others are dropped at the locator step.
- **Brief defects:** (1) the brief's `ENTRY_COLUMNS` order is used as given, but the A0 `first_line_id` is the first segment's line, undefined for a unit with no segments (empty string; none occur on dev); (2) `process()` signature has a new `version` argument not named in the brief (default v1, tests unaffected); (3) the brief says "7 volumes whose chain end changes in 21b": found 3126 3265 3290 3307 3320 3471 3522 (not the 7 in STATUS fact "numbering surprises"); (4) the brief's "expected: none located" does not hold, see Deviations; (5) the brief gives no rule for `unit_order` ordering when the table lacks it (synthetic layers): table order is kept.
- **Open questions for the user:** UA-056 (full run)

### Phase review (Opus 5.5, 2026-10-30)
- **Orchestration:** the same Opus session ran the steps as Sonnet subagents.
  - 21a ran in 7 sessions: dev lines; dev embedded entries and dev targets; test lines in two halves; test targets in
    two halves; finalisation.
  - 21b and 21c ran in parts around the gold.
  - 21d ran after both.
  - Mid-phase change: the `embedded_entries.tsv` convention (21a/21c briefs amended before any test side was read).
  - The test target draw was run by the orchestrator after all test sides were written.
- **Re-runs:** all match (REVIEW *Acceptance, re-run*):
  - bar F, re-scored independently;
  - the 21c P/R/A/E and v1 baseline, re-scored independently;
  - fresh v1 build = `20r-a` (123 files + `volumes.tsv`), fresh v2 build = `21d-a` (164 files);
  - suite 1,402 passed, 11 skipped.
- **Own error analysis:**
  - 2 gold sides were sound;
  - A1 entries on S01/S04 show heading attachment;
  - 3280 N=334 is one scan late behind a misread anchor;
  - 3283 N=593 is right, so F15-21d.5 is revised (F15-R.3).
- **Fixed in review:**
  - `docs/index_links.md` L5/L6 description and the opening rationale (F15-R.7);
  - 21c findings moved into the main *Findings* table.
- **Written:**
  - REVIEW.md;
  - the report's Summary, Review and Follow-ups; statuses set (37 findings, F15-R.1–7); status final;
  - `reports/index.md` (37 rows plus the phase row); `make reports` clean;
  - UA-057 (publication, rec. a); UA-056 options reframed;
  - hand-off; `plan/PLAN.md` row; `plan/CONTEXT.md` (§1, §2, §3, §4, §5);
  - STATUS.md.
- **Release:** v0.20.0.
