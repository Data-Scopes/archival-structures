# Phase 14 progress

Append-only, one block per session.

### Planning (Opus 5.5, 2026-10-29)
- **Scope (user, 2026-10-29):** index → resolution links (P1); maintenance (pagexml-tools 0.9.1 pin, NLP switch to the
  phase-11 ports, shared-data link) as the first steps (P2).
- **Measured:** 41 front-index volumes in `19e-a` (`draw_reads.py`); entries = `main_text` units on front-index sides
  (3274/3290 entries are `hand:region`); read list 72 rows, seed 20261129, dev 3254 3471 3394 3308, test 3256 3456 3401
  3611 3320 3712 3590 3352. Folio chain (`folio_proto.py`): 21,432 of 21,437 anchors on rectos, chain/recto 0.7562;
  LOO under 20d's definition (`loo_check.py`, scans-table positions) 0.9858 (21,030 / 21,334). `layout/pages` omits
  `no_regions` scans (5–12 per volume): positions from the scans table.
- **Explorers:** page numbering API (`interpolate_folio_to_scan`, `longest_increasing_chain`, `confidence_band`;
  `numbering_mode` misreads SG foliation as pagination); no per-side page-number table in the layer; inventory GT has no
  printed numbers for hand volumes; `republic_segmentation_v1` (13 of 41 covered; other HTR line ids, 0 exact
  matches); NLP shim pattern (re-export, originals under `output/as-phase-NN-originals/`, verify script); pagexml-tools
  sites (`page_structure.py` L825–832, provenance `MIN_PAGEXML_TOOLS`, tests); the 24 stale split files are already
  gone; AS not in the REPUBLIC row's *used by*.
- **Checked:** the 0.9.1 wheel (PyPI) has `extra` in `CHILD_PROPERTIES` and the `stats` fix; the 20a build command
  (`--check-only`: 231 inventories, 43–70 min, 40,862 MiB free; out dir `20a-pin` created); `compare.py --help`;
  `protect.py` arguments; the locator variant rules on 9 sample texts (expected lists in the 20e brief).
- **Written:** `PLAN.md` (P1–P13), briefs 20a–20e, `STATUS.md`, report skeleton `reports/phase-14-index-links.md`
  (F14-P.1–P.9), `reports/index.md` phase row, `plan/PLAN.md` row, `plan/CONTEXT.md` (§1 split files, §4 phase 14),
  UA-048.
- **Open for the user:** UA-048.

### Step 20a: pagexml-tools 0.9.1 pin, data link
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** partial (part 4 not run; stopped at nested regions, UA-049)
- **Files changed:** `pyproject.toml`, `text_layer/provenance.py`, `tests/test_text_layer_provenance.py`, `analysis/page_structure.py`, `text_layer/build.py` (docstring), `docs/text_layer.md`, `docs/shared_library/changelog.md`, shared-data `README.md` (one cell), `data/republic_segmentation_v1` (symlink), `reports/data/phase-14-index-links/step-20a/`, report, `docs/USER_ACTIONS.md`
- **Public API:** `MIN_PAGEXML_TOOLS` (0, 9, 1); requirement pagexml-tools >= 0.9.1
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests` → 1,351 passed, 7 failed, 11 skipped (7 = nested regions in `test_text_layer_extract.py`); NLP 66 passed; LotA 380 passed, 1 skipped, 1 xfailed
- **Acceptance:** 1 met (0.9.1, pin, 2 of 2 workarounds dropped); 2 not met (7 failures); 3 not run; 4 met (README + 2 LotA fixture mtimes); 5 met; 6 met (NLP 66, LotA 380+1 skipped+1 xfailed)
- **Report:** [`reports/phase-14-index-links.md` § Step 20a](../../reports/phase-14-index-links.md#step-20a-pagexml-tools-091-pin-data-link); findings F14-20a.1–3
- **Error analysis:** all 7 failures have one cause: 9999A_0003 nested regions now parse (regions 6 not 4, n_scans 4 not 3). Region without Coords/lines still dropped.
- **Deviations from brief:** regression build (part 4) not run, per the brief's stop rule.
- **Brief defects:** LotA expectation "381 passed" is 380 passed + 1 skipped; `sed -i` without `''` fails on macOS (use python edits).
- **Open questions for the user:** UA-049

### Step 20b: NLP switches to the phase-11 ports
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** done
- **Files changed:** NLP `src/startpage/unsupervised_segmentation.py`, `src/startpage/separator_sheets.py`, `scripts/verify_as_segmentation_shim.py` (new), `output/as-phase-14-originals/src/*` (copies); AS `reports/data/phase-14-index-links/step-20b/`, `reports/phase-14-index-links.md`, `docs/shared_library/changelog.md`
- **Public API:** none in AS; NLP modules re-export AS names
- **Tests:** NLP suite → 66 passed; AS two test files → 51 passed
- **Acceptance:** pre-edit parity 0 mismatches (weight 0.0293559853524414, 41,810 rows, 169 sheets); verify both 15/15 PASS, exit 0; all original names importable; tests as above
- **Report:** [`reports/phase-14-index-links.md` § Step 20b](../../reports/phase-14-index-links.md#step-20b-nlp-switches-to-the-phase-11-ports); findings F14-20b.1
- **Error analysis:** no differences; read the 169 sheets (140 in 2555) and rule effect (10,416 vs 10,384 starts at 0.5)
- **Deviations from brief:** none
- **Brief defects:** AS test count is 51, not 25
- **Open questions for the user:** none

### Step 20c: gold reads (locators and link targets)
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** partial (acceptance 2 not met on test: 28 of 30; stopped per the brief)
- **Files changed:** `reports/data/phase-14-index-links/step-20c/` (`common.py`, `show_entry.py`, `show_scan.py`, `entry_gold.tsv`, `summary.py`, `build_check_page.py`, `check_page.html`, `check_items.json`), `docs/USER_ACTIONS.md` (UA-050), `reports/phase-14-index-links.md`
- **Public API:** none
- **Tests:** none (summary.py output in the report)
- **Acceptance:** 1 met (60 rows, 60 unique); 2 not met on test (28 rows with target and evidence != unclear, need 30; dev 16, need 15 met); 3 met (20 items, links, JS syntax ok, UA-050)
- **Report:** [`reports/phase-14-index-links.md` § Step 20c](../../reports/phase-14-index-links.md#step-20c-gold-reads-locators-and-link-targets); findings F14-20c.1-7
- **Error analysis:** all 60 units and 45 target folios read; index right for 43 of 45 (recto 15, verso 22, both 6); 15 units have no locator (7 first-line fragments, 4 headings, 4 date-led rows of 3712)
- **Deviations from brief:** no reserve used (no `not entry`); locators taken from the unit text only; `scans_opened` is approximate (volume probes counted on the first entry of a volume); extra helper `common.py`
- **Brief defects:** (1) no rule for first-line fragments whose numbers are in other units (kept as `entry`, no locator); (2) volume 3712 has a dated index without folio locators, so 5 of 40 test reads cannot give a target; (3) the brief expects every read row to be a folio entry, but 15 of 60 give no target, so the 30/40 test bar is tight; (4) `sed -i` without `''` fails on macOS
- **Open questions for the user:** UA-050 (confirm page); whether 20d may proceed on 28 test targets (STATUS.md)

### Step 20c (resumed): 8 test reserves read as extra gold
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** done
- **Decision:** orchestrator (planner role): read the 8 test reserves (ids 30, 36, 42, 48, 54, 60, 66, 72), same procedure, `used_for` = `extra`, before any phase-14 code; dev reserves not read.
- **Files changed:** `entry_gold.tsv` (68 rows), `summary.py` (read / extra / together), report section and findings F14-20c.1-6 counts, STATUS.md
- **Acceptance:** 2 now met: test 35 rows with a target and evidence != unclear over read + extra (read alone 28); dev 16. Confirm page unchanged (20 read rows).
- **Result of the extras:** 7 of 8 have a target (5 printed, 2 neighbours); content verso 3, both 3, recto 1; 3712 reserve is another dated fragment without locator.
- **Open questions for the user:** none beyond UA-050

### Step 20d: folio map
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** done (bar F not met: negative result)
- **Files changed:** `archival_structures/analysis/folio_map.py`, `archival_structures/analysis/index_links.py`, `tests/test_analysis_folio_map.py`, `tests/test_analysis_index_links.py`, `reports/data/phase-14-index-links/step-20d/` (eval_folio.py/.log, volumes.tsv, loo.tsv, gold_eval.tsv, loo_misses.py/.txt), `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-051), `reports/phase-14-index-links.md`, `plan/phase-14-index-links/STATUS.md`
- **Public API:** `analysis.folio_map` (`FolioMapParams`, `FolioTarget`, `FolioMap`, `build_folio_map`, `NUMBER_PATTERN`); `analysis.index_links` (`front_index_run`, `VolumeInputs`, `read_volume_inputs`)
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` -> 1375 passed, 11 skipped (new files: 17 passed)
- **Acceptance:** 1 met (41/41 front_first/last/n_after); 2 met (LOO 21,010/21,334 = 0.9848, planning 0.9858; lowest 3712 0.847, 3611 0.925, 3265 0.928); 3 NOT met: test 33/35 = 0.943 (read-only 27/28; printed 24/25, interpolated 9/10), dev 16/16; 4 met
- **Report:** [`reports/phase-14-index-links.md` § Step 20d](../../reports/phase-14-index-links.md#step-20d-folio-map); findings F14-20d.1-5
- **Error analysis:** both bar F misses read (duplicated printed number; skipped printed numbers with the number untyped); 30 of 41 LOO misses read: 21 pagination mode of 3712, 7 skip/duplicate, 2 chain error. 3712 is foliated but verso years push it to `pagination`
- **Deviations from brief:** bar F on read + extra rows (orchestrator decision); `used_for` column added to `gold_eval.tsv`
- **Brief defects:** (1) the recto-share rule misclassifies 3712 (verso years pass the number pattern), so the planning LOO 0.946 for 3712 is not what the library gives (0.847); (2) the brief's "within 0.002" LOO check differs by 0.0010 for this reason; (3) tie rule of `longest_increasing_chain` for equal numbers is unspecified
- **Open questions for the user:** UA-051 (proceed with 20e after the bar F miss?)

### Step 20a resumed (UA-049 a), 2026-10-29
- **Status:** done. Regression build `20a-pin` vs `19e-a`: 4,591 equal, 0 differ, 0 missing; audit n_fail_judged 0, n_pass 4,196 (4 extra v5 layer-level invariants, all pass; brief said 4,192), n_fail 47 as in 19e-a.
- 7 extract tests updated to 0.9.1 (`test_nested_regions_fail_in_pagexml` -> `test_nested_regions_parse`); `docs/text_layer.md` section now "Known pagexml-tools 0.9.1 limits"; changelog entry finished. AS suite 1,375 passed, 11 skipped. protect diff: README + 2 LotA fixture mtimes only. Findings F14-20a.1-3 (F14-20a.2 reworded).

### Step 20e: locators, links and index_links_v1
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** done (bar L precision not met: negative result; bar E met)
- **Files changed:** `archival_structures/analysis/index_links.py`, `scripts/build_index_links.py` (new), `tests/test_analysis_index_links.py`, `reports/data/phase-14-index-links/step-20e/` (new), `data/index_links_dev/20e-a/` (new), `docs/index_links.md` (new), `docs/text_layer.md` (one line), `docs/shared_library/changelog.md`, `docs/USER_ACTIONS.md` (UA-052), `reports/phase-14-index-links.md`, `plan/phase-14-index-links/STATUS.md`
- **Public API:** `analysis.index_links` (`Locator`, `LocatorParams`, `SG_MONTHS`, `SG_LOCATOR_PARAMS`, `DEFAULT_LOCATOR_VARIANT` = `L2`, `LINK_COLUMNS`, `extract_locators`, `volume_links`); `scripts/build_index_links.py`
- **Tests:** `.venv/bin/python -m pytest tests -q` -> 1380 passed, 11 skipped (new: 8 in `test_analysis_index_links.py`)
- **Acceptance:** 1 selection on 20 dev rows: L1 P 1.000 R 0.964, L2=L3=L4 P 0.966 R 1.000 -> L2 (tie rule); 2 bar L NOT met: P 61/66 = 0.924, R 61/61 (read only 50/55, 50/50); bar E met 33/35 = 0.943 (read only 27/28); 3 dev build 41 volumes, 72,558 entries, 102,588 locators, 101,282 linked, 19.4 s (0.47 s per volume); 4 silver words target 0.455 vs control 0.100, REPUBLIC 0.9999 vs 0.9977 (ceiling); 5 met
- **Report:** [`reports/phase-14-index-links.md` § Step 20e](../../reports/phase-14-index-links.md#step-20e-locators-links-and-index_links_v1); findings F14-20e.1-8
- **Error analysis:** 6 FP (3 dates in 3712, 1 count, 1 heading, 1 garbled digit) and 0 FN under L2; both bar E misses are the map (20d's two). 14 of 20 random links have the matter on the target sides, 3 untestable, 3 unconfirmed.
- **Deviations from brief:** runs on the frozen map after the bar F miss (UA-051 (a)); bar L/E on read + extra rows with read-only numbers beside; the test script was run twice (first with the unset placeholder default L3, by my mistake; log kept as `test_bars_L3_misrun.log`, disclosed as F14-20e.4); added `meta/<inv>.json` per volume to rebuild `volumes.tsv`; no before/after protect hash pair (no baseline taken)
- **Brief defects:** (1) "20d's bar F passed" is wrong (UA-051); (2) the brief's "rebuilt from the per-inventory files" cannot give `mode`/`n_anchors`/`recto_share` from the map table alone (sidecar added); (3) `amount_before` has no word boundary and drops "Denhof 354." under L3; (4) the brief asks for the module default to be set from selection but gives no place to record the tie when the dev rows lack date/ordinal cases
- **Open questions for the user:** UA-052 (full run; recommended: wait for the phase review's variant decision)

### Phase review (Opus 5.5, 2026-10-29)
- **Verdict:** accept with follow-ups ([REVIEW](REVIEW.md)). Every acceptance number re-run and equal (suites AS 1,380 +
  11 skipped, NLP 66, LotA 380 + 1 skipped + 1 xfailed; 20a compare 4,591 equal; 20b verify 15/15; 20d/20e outputs
  `cmp`-equal; dev build rebuilt byte-identical; protect diff vs 20a's after-list: only 2 LotA fixture mtimes).
- **Decisions:** L2 stays the default (L3 post-hoc, F14-R.2); the 20c extra rows were sound (F14-R.1).
- **Own error analysis:** year-like anchors link years `high` (F14-R.3); link-level silver 0.707 vs 0.218 and offsets
  (F14-R.4); extrapolated links are mostly single digits (F14-R.5). Scripts in `reports/data/phase-14-index-links/review/`.
- **Fixed in review:** `.gitignore` un-ignores `scripts/build_index_links.py` (F14-R.6).
- **Written:** `REVIEW.md`, report *Summary*/*Review*/*Follow-ups*/*Reproduce*, statuses (F14-20e.3 revised), F14-R.1–R.7,
  `reports/index.md` (40 rows appended, phase row), `make reports` clean; `step-20r-review-fixes.md`; PLAN hand-off;
  `plan/PLAN.md` row; `plan/CONTEXT.md` (154 lines); `STATUS.md`; UA-052 updated, UA-053 new.

### Step 20r: review fixes
- **Session:** Sonnet 5.5, 2026-10-29. **Status:** done
- **Files changed:** scripts/build_index_links.py, tests/test_analysis_index_links.py, docs/index_links.md, docs/text_layer.md, docs/shared_library/changelog.md, reports/phase-14-index-links.md, plan/phase-14-index-links/PROGRESS.md, STATUS.md; new data/index_links_dev/20r-a/
- **Public API:** none (script behaviour: failed volumes recorded, exit 1, `progress.log`)
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` -> 1,381 passed, 11 skipped
- **Acceptance:** 1 ok; 2 rebuild of 41 volumes: 82 gz files + volumes.tsv byte-identical (decompressed), last_run.failed {}, package.version 0.18.0; 3 version 0.18.0; 4 docs/changelog done, make reports clean. See report section.
- **Error analysis:** progress.log 41 x done; failing-volume message names the missing units file.
- **Deviations from brief:** none
- **Brief defects:** test `_load_builder` had to register the module in `sys.modules`/`sys.path` or the process pool cannot pickle `run_one`; `make update_install` exits 1 on `chmod -R g+w .venv` (not writable files under share/jupyter) although the install succeeded.
- **Open questions for the user:** none

### Confirm page: image crops (Opus 5.5, 2026-10-29, user request on UA-050, outside the step protocol)
- `step-20c/build_check_page.py` now adds three Nationaal Archief IIIF crops per item (shared-tools `handprint` helpers:
  `na_iiif_bases`, `image_size`, `region_box`, `crop`; cache `step-20c/iiif_cache/`, 9.1 MB): the entry's lines
  (`join/segments` line ids), the top of the target recto widened to the page edge and 300 px up (the folio number sits
  in the corner, above the first HTR line), and the lines on folio N that best match the agent's evidence (token overlap
  + `SequenceMatcher`, best line ±1). Crops: entry 20, number 20, evidence 19 (item 7 is `absent`: none shown). Items,
  questions and the `CheckStore` key unchanged. Checked by eye: item 1 entry, number (383) and evidence crops; item 30
  number (255). JS syntax ok.

### Confirm page: full page text (Opus 5.5, 2026-10-29, user request on UA-050, outside the step protocol)
- `build_check_page.py` adds two fold-outs per item with the full HTR text of the target recto (first scan) and verso
  (second scan): main lines by column then height, margin (`extra`) lines interleaved by height and marked, the
  evidence-crop lines highlighted; fold state survives re-renders. Items and `CheckStore` key unchanged.

### UA-050 resolved: user check of 20 links (Opus 5.5, 2026-10-29)
- `step-20c/check_verdicts.json` (user export), `check_summary.py` -> `check_summary.txt`: q1 18/20 Yes, q2 17/20 Yes
  (1 Unsure); all 11 test items Yes/Yes; the 2 wrong links (3471 locators 94 and 452) are entry-side errors (an HTR
  artefact taken as a locator; a column-merged unit with a false content match), not folio errors. F14-20c.8 added,
  F14-20c.2 marked revised; report § Step 20c "User check". `entry_gold.tsv` not edited. UA-053's context updated.

### UA-052 and UA-053 resolved (Opus 5.5, 2026-10-29)
- UA-052: the user ran the full build; `data/index_links_full/{volumes.tsv,manifest.json}`: 412 of 8,856 inventories
  with a front index, 0 failed, 629,912 entries, 914,994 locators, 902,031 linked; the 41 dev volumes identical to
  `20r-a`. Error analysis on `volumes.tsv` only (links on the drive): 3681 and 3601 cite folios outside their chain;
  22 volumes with year-like/impossible chain ends by an estimated rule. F14-20e.9, F14-20e.10; report section
  *After the review*; `docs/index_links.md` *Full run*.
- UA-053: (a), not published. Recorded in the report, `docs/index_links.md`, `plan/PLAN.md`, phase PLAN, `plan/CONTEXT.md`.
