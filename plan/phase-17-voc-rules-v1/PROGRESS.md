# Phase 17 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning (Opus 5.5, 2026-11-01)
- **User decisions (2026-11-01):**
  - scope: all four components, in 5 steps; VOC unit order goes to a later phase;
  - folio gold: the user reads about 240 page-top crops (UA-061, opened by 23a).
- **Settled without asking:**
  - all phase-16 gold, 2775 included, is dev now (P2);
  - the test gold comes from 24 fresh volumes (P3);
  - the bars keep the phase 14–16 thresholds, with a paired comparison against phase 16 (P7).
- **Read:**
  - `plan/README.md`, `plan/PLAN.md`, `CONTEXT.md`, the seeded `PLAN.md`/`STATUS.md`;
  - the phase-16 report (summary, findings, 22a–22e, review, UA-059, follow-ups) and the phase-16 decisions;
  - the GT README VOC rows, the shared-tools README (`checkpage`, `handprint`), NLP `CLAUDE.md`.
- **Explorer:** the `folio_map`/`index_links`/`index_pages` APIs, the preset/wbc/join paths and CLI, the audit's v5
  gate, the 22b–22d eval scripts, `build_check_page.py`. 1,424 tests collected; no `## Unreleased` in the changelog.
- **Probes** (`reports/data/phase-17-voc-rules-v1/planning/`; phase-16 data only):
  - `draw_fresh.py` → `fresh_volumes.tsv`: 4,394 OBP volumes on the drive outside the 567; 24 + 16 drawn (seed
    20261101); only file names and sizes read;
  - `wbc_probe.py`: with `-=„¬`, 856/870 `¬` boundaries merge, and the next-line `„` is dropped (F17-P.2);
  - `cache_gm.py` + `map_proto.py`: the harness reproduces 22b (647/929, 591/904). The options move dev leaf by
    −1 to +16 anchors; all combined reach 0.731 dev / 0.691 test (F17-P.3);
  - `offsets.py`: the GM begin/end convention against the printed recto (F17-P.4);
  - `toc_proto.py`: a front window plus a title cue give F1 0.624 against 22c's 0.300; the guard is inert inside the
    window (F17-P.5);
  - `front_proto.py`: the scaled front run finds 21/22 against 7/22 (F17-P.6);
  - `l9_proto.py`: E/coverage L8 0.545/0.518, L9 0.606/0.636, L9g + mono 0.816/0.568 (F17-P.7). A month-before year
    guard was added at planning after "dato 14 Nov. 1670." slipped through.
  - `protect_voc.py`: protect list for the VOC trees and gold (11,929 lines at planning).
- **Written:**
  - briefs 23a–23e;
  - the phase `PLAN.md` (P1–P13);
  - `STATUS.md`;
  - the report skeleton `reports/phase-17-voc-rules-v1.md` (planning findings F17-P.1–P.7);
  - the `plan/PLAN.md` row.
- **Side effects:** the planning dir `reports/data/phase-17-voc-rules-v1/planning/` (g+w). Scratch caches stay in the
  session scratchpad.
- **Open for the user:** run the step sessions (see `STATUS.md`). UA-061 comes from 23a.

## Step 23c: VOC folio map (2026-11-01, Sonnet 5.5) - dev done, test waiting on UA-061

- **Code:** `analysis/folio_map.py` gets `pick_by_chain`, `tie_keep`/`tie_window`, `clean_tol`, `pieces`/`piece_min`/`piece_slope`
  and `FolioMap.pieces`; defaults byte-equal (`sg_check.json`: 0 differences on 3444, `FolioMapParams()` and `FOLIO_MAP_V2`).
  Six new tests (module 29); suite 1,438 collected (1,424 at planning + 6 mine + 8 from parallel steps), 1,427 passed, 11 skipped.
  Docs: `docs/index_links.md` paragraph, changelog entry under *Unreleased*.
- **Dev:** `reports/data/phase-17-voc-rules-v1/step-23c/eval_map.py` (stages sg, dev, test, context). BASE reproduces 22b
  (647/929, 591/904 uncorrected). 48 cells on 1,833 corrected anchors: SEL = pick + tie first + clean 3 (pieces off, extrapolate 6):
  leaf 1311 (0.715), strict 1057, LOO 0.884; BASE 1245 (0.679). Best cell 1313 (all five, extrapolate 30).
- **Error analysis:** 15 fixes / 15 breaks read (`misses_read.tsv`, `context_dev.txt`); pick is real, ties are a gold-convention coin
  flip, clean is a wash. Report section *Step 23c*, findings F17-23c.1-6 (draft).
- **Test not run:** `step-23a/gold/folio_gold.done` absent. Later: `.venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23c/eval_map.py --stage test`
  (from the AS root), then `--stage context`. No `VOC_FOLIO_MAP_V1` added (waits on the bar). `protect_voc.py` before/after: only
  `data/text_layer_voc_23a/` paths (23a's build) differ.

### Step 23a: Fresh gold
- **Session:** Sonnet 5.5, 2026-11-01. **Status:** done, apart from UA-061 (folio crops read by the user). Readers were run by the orchestrating session.
- **Files changed:** `reports/data/phase-17-voc-rules-v1/step-23a/` (scripts `stage_inputs dump show folio_sample build_folio_page
  summarise_folio merge_toc sample_lines merge_links`; `dumps/`, `reads/`, `gold/`, `folio_page.html`, `folio_sample.tsv`, `audit.json`,
  `protect_*`); `data/text_layer_voc_23a` (link to the drive, 40 volumes), `data/text_layer_voc_inputs_23a/`; `docs/USER_ACTIONS.md` (UA-061);
  the report section and findings F17-23a.1-6.
- **Public API:** none.
- **Tests:** none (no library code). `make reports`: no warnings.
- **Acceptance:** 40 staged, disjoint, `failures=0`, `n_fail_judged = 0`; toc_labels 1,579 rows, 0.13% unclear, self-check 30/30; 21 of 24 volumes
  have a front register TOC (no reserves read); link_lines 168 (8 x 21), 25 with a folio, 141 none, 2 unreadable; link_targets 25 (found 21:
  sure 13, likely 8; not_found 4); folio page 240 items, blind, JS ok; gold frozen (`gold/agent_gold.done`, SHA256SUMS).
- **Report:** [`reports/phase-17-voc-rules-v1.md` § Step 23a](../../reports/phase-17-voc-rules-v1.md#step-23a-fresh-gold); findings F17-23a.1-6
- **Error analysis:** the self-check has no disagreements; 4 not_found targets (3 beyond the 2538 volume, 1 dropped table), 2 unreadable lines; 25 consistency edits to 14 link lines (rule b took 10 folios away).
- **Deviations from brief:** readers run by the orchestrator (2 volumes per TOC reader, 3 per target reader); TOC harmonisation rules i-iv, `loc=` note prefix and link rules a-c were orchestrator amendments after the reads.
- **Brief defects:** `read_volume_inputs` opens `layout/pages/`; dump text column and "front register TOC" were not defined; `line_` id prefix in one target file.
- **Open questions for the user:** UA-061 (read 240 crops, export, run `summarise_folio.py`).

### Step 23d: One-column TOC detector
- **Session:** Sonnet 5.5, 2026-11-01 (dev run, then test run after `agent_gold.done`). **Status:** done; bar D fails (negative result).
- **Files changed:** `archival_structures/analysis/index_pages.py` (`TOC_SIDES_DEFAULTS`, `toc_side_features`, `toc_side_rule`, `toc_sides`);
  `archival_structures/analysis/index_links.py` (`front_index_run(..., min_after_frac=None)`, edited in the first run only); tests in
  `tests/test_analysis_index_pages.py` (3) and `tests/test_analysis_index_links.py` (1); `docs/index_links.md`; changelog `### Step 23d`;
  `reports/data/phase-17-voc-rules-v1/step-23d/`; report section and findings F17-23d.1-6.
- **Public API:** additive only; no sibling imports affected. `VOC_TOC_SIDES_V1`, `VOC_FRONT_RUN_V1` not added (bar failed).
- **Tests:** 1,438 collected, all green; `make reports` no warnings.
- **Acceptance:** planning cell 106/48/80 reproduces; default `front_index_run` unchanged (22 volumes, 3,000 random lists); fresh SEL P 0.583
  (95/163), R 0.276 (95/344); PH16 P 0.706, R 0.244; McNemar b 94, c 116, p 0.147; front runs dev 21/22 vs 7/22, fresh 23/24 vs 4/24.
- **Report:** [`reports/phase-17-voc-rules-v1.md` Step 23d](../../reports/phase-17-voc-rules-v1.md#step-23d-one-column-toc-detector)
- **Error analysis:** 15+15 dev, 30+30 test cases read; FN are summary registers, enclosure lists, single line-end locators, beyond window
  (49 of 249); FP are prose with paragraph numbers (2487: 35 of 68), title cue, gap fill. Guard inert.
- **Deviations from brief:** added recall by 23a's `loc` stratum; `cfg=None` default instead of a mutable default.
- **Brief defects:** gap-fill artefacts have no FP category; fewer-differences tie rule selects the defaults; `unclear`/`no_text` handling taken from the brief (left out).
- **Open questions for the user:** none.

### Step 23e: Locator L9 and VOC links
- **Session:** Sonnet 5.5, 2026-11-01 (l8check, dev, test once, errors). **Status:** done; bars L and E fail (negative result, 25 gold folios).
- **Files changed:** `archival_structures/analysis/index_links.py` (`LocatorParams` +4 fields, `L9`, `L9g`, `monotone_mask`, `_extract_l8` arms); tests in
  `tests/test_analysis_index_links.py` (5 new; the unknown-variant test now uses "L10"); `docs/index_links.md`; changelog `### Step 23e`;
  `reports/data/phase-17-voc-rules-v1/step-23e/` (`eval_links17.py`, `l8check.json`, `c3_reproduction.tsv`, `dev_grid.tsv`, `select.json`, `test_done.json`,
  `test_links.tsv`, `test_lines.tsv`, `end_to_end.tsv`, `silver.tsv`, `errors_dump.txt`, `cases_read.tsv`); report section, findings F17-23e.1-7.
- **Public API:** additive only; no sibling imports affected.
- **Tests:** 1,432 passed, 11 skipped (full suite); `make reports` no warnings.
- **Acceptance:** l8check 0 differences (7,715 lines); six SEL22B prototype rows reproduce; 12-cell grid, SEL = L9g+mono+SEL22B; fresh SEL L P 10/18 = 0.556, R 10/25 = 0.400,
  E 9/17 = 0.529; C3 P 0.333, R 0.360, E 7/26; E_map 18/21; end to end E 8/8 on 66 lines; silver P 0.718 R 0.424.
- **Report:** [`reports/phase-17-voc-rules-v1.md` Step 23e](../../reports/phase-17-voc-rules-v1.md#step-23e-locator-l9-and-voc-links)
- **Error analysis:** 8 E misses (7 entry numbers, 1 second number), 15 L false negatives (range filter 5, pattern gaps 5, mono 3, HTR 2), 9 hits, 10 mono drops read.
- **Deviations from brief:** added `c3_reproduction.tsv`; mono-off diagnostic derived post hoc from `mono_dropped`, not a second test run.
- **Brief defects:** L8 on "5038. . . 5107." gives [5038, 5107], not [] (pinned actual values); the existing `LocatorParams(variant="L9")` raises test had to change; prototype table covers 21 volumes in the prototype script but reproduces on the 22 listed.
- **Open questions for the user:** none.

### Step 23b: VOC break set with ¬
- **Session:** Sonnet 5.5, 2026-11-01. **Status:** done; join bar missed (negative result, layer kept).
- **Files changed:** `archival_structures/text_layer/presets.py` (`VOC_1_04_02_V6`, registry `voc_1.04.02_v6`); tests in `tests/test_text_layer_voc_preset.py` (2) and
  `tests/test_text_layer_join_v2.py` (2); `docs/text_layer.md`; changelog `### Step 23b`; `reports/data/phase-17-voc-rules-v1/step-23b/` (`audit.py`,
  `audit.json`, `common.py`, `compare.py`, `compare.json`, `sample.py`, `joins_sample.tsv`, `joins_read.tsv`, `guarded_sample.tsv`, `guarded_read.tsv`,
  `changed_sample.tsv`, protect lists); report section, findings F17-23b.1-5. Layer: `data/text_layer_voc_23b` (drive, `23b-a`).
- **Public API:** additive only; no sibling imports affected.
- **Tests:** 1,443 collected; 4 new pass; full suite 1,431 passed, 1 flaky failure under parallel sessions (passes alone); `make reports` no warnings.
- **Acceptance:** build failures=0; audit 0 judged fails (143 reported-only); upstream 0 differences in 5,670 files; 68,534 changed units, 0 without `¬`;
  `¬` left 10,329 (unit_end 9,525, guarded 779, not_line_final 25, other 0); bar 56/60 = 0.933 (Wilson 0.841-0.974), missed; protect clean apart from 23a paths.
- **Report:** [`reports/phase-17-voc-rules-v1.md` Step 23b](../../reports/phase-17-voc-rules-v1.md#step-23b-voc-break-set-with-)
- **Error analysis:** 65 joins read (56 right, 4 wrong, 5 unclear), 20 guarded, 10 changed units.
- **Deviations from brief:** sample/read helpers `common.py`, `sample.py` added; the 22a `¬` count is 116,920 in unit texts (brief: 116,881).
- **Brief defects:** `common.classify_nots` class rules not specified (taken as: not at segment end = `not_line_final`; last segment = `unit_end`; non-merge/plain boundary = `guarded`); the brief does not say which line column (`text`, not `text_raw`) ends in `¬`.
- **Open questions for the user:** none.

### Phase review (Opus 5.5, 2026-11-01)
- **Verdict:** accept with follow-ups ([REVIEW](REVIEW.md)); 23c's test stage (bar F) waits on UA-061, then an addendum.
- **Re-run:**
  - suite 1,432 passed, 11 skipped;
  - SG check 0 differences; l8check 0 / 7,715;
  - 23b `compare.py` and audit re-runs identical (0 judged);
  - gold hashes 4/4; bars D, L and E recomputed (same counts); no sibling file changed today.
- **Own error analysis:**
  - 23d stratified by `loc=`, 2282 and 2167 (F17-R.1);
  - all 80 drawn `¬` joins read: k = 47 is wrong, so 55/60 (F17-R.3; F17-23b.1 and .5 revised);
  - 23e misses confirmed;
  - link gold too small to decide L and E (F17-R.2);
  - gold-rule amendments logged (F17-R.4);
  - `¬` at unit ends (F17-R.5).
- **Written:**
  - `REVIEW.md`;
  - report *Summary*, *Review*, *Follow-ups* and F17-R.1–.5; other findings confirmed; status final except 23c test;
  - `reports/index.md` (42 rows and the phase row); `make reports` clean;
  - PLAN hand-off, the `plan/PLAN.md` row, `CONTEXT.md` (§1, §3–§5), `STATUS.md` (overwritten).
- **No review-fixes brief** (no defects). Release recommendation: v0.22.0.

## 2026-11-02 — UA-061 page: in-page IIIF zoom (Opus 5.5, at the user's request)

- `step-23a/build_folio_page.py`: each item gains `zoom` (IIIF base, image size, crop height); the crop is now a pan/zoom
  viewport (wheel/pinch, drag, double-click reset, *whole page*) that fetches a sharp IIIF region of the view after it
  settles. Items otherwise identical (checked against a backup); storage key `as_phase17_23a_folio_v1` unchanged, so
  answers already given are kept. Page stays blind (no text, no flags). JS syntax checked; IIIF requests tested (200).
- Export fix: the Export button called `store.exportJSON(..., KEY, ...)` but `KEY` is a Python name, never defined in the
  page's JS, so the click threw a ReferenceError and nothing downloaded (a 23a build defect, present since the page was
  first built). Now passes the key string. Rebuilt; JS syntax ok; items unchanged; stored answers unaffected.
- 2026-11-02 UA-061 done by the user: `folio_verdicts.json` exported (240 items); `summarise_folio.py` wrote
  `gold/folio_gold.tsv` (sha256 7411576…a7af; 109 number, 131 none, 0 unreadable, 0 missing; 18 notes),
  the `SHA256SUMS` line and `gold/folio_gold.done` (16:23:30Z), then stopped at the final `chmod` of `SHA256SUMS`
  (run as the user, file owned by claude-agent: only the owner may chmod). Checked: hash in `SHA256SUMS` and `.done`
  equals the file; gold frozen. Script fixed to chmod only when the bit is missing and to report, not fail, when not
  the owner. Left: `folio_gold.done` is the user's and not group-writable.

## 2026-11-02 — step 23c test stage (Sonnet)

- Gold sha256 7411576...a7af verified. Protect before, then `eval_map.py --stage test` (once; 16:26:54Z, `test_done.json`),
  then `--stage context`, then protect after: `diff` of the two outputs is empty (12,751 lines each).
- Numbers (109 `number` anchors, all main, none layered): BASE leaf 81 = 0.743 (Wilson 0.654-0.816), strict 81; SEL leaf 79 =
  0.725 (0.634-0.800), strict 78. McNemar b 2, c 0, p 0.5. Bar F (0.95) missed: negative result, no retuning, no
  `VOC_FOLIO_MAP_V1` constant, no library change. Openings 17/23 both; single 64/86 BASE, 62/86 SEL.
- Error analysis: 30 SEL misses classed (HTR misread/blank 12, tied/duplicated 10, several numbers 2, restart 2, unclear 4);
  2 discordant anchors, both ties where `tie_keep=first` is wrong (2954-0017, 3793-1105); 131 `none` items by inventory and layout.
  Files: `step-23c/test_eval.tsv`, `test_done.json`, `context_test.txt`, `test_misses_read.tsv`, `protect_voc_*_test.txt`.
- Report: 23c status line, Test paragraph, Test error analysis, F17-23c.7 to .11. `make reports` clean. Suite: 1,432 passed, 11 skipped.
- Odd: the `context` stage rewrote the dev `misses_read.tsv` (sha 0eedae4f... before, b345651d... after; `context_dev.txt`
  identical). The old file had the dev hand classification columns (category, real_fix_or_break_note); the rewrite leaves
  them empty and I had not copied the old one aside (I checked after, not before). The dev reading survives in the report's
  dev error analysis table; the per-anchor notes in that tsv are lost (untracked, so not in git).

## 2026-11-02 — review addendum for the 23c test stage (Opus 5.5)

- Test results recomputed from `test_eval.tsv` (BASE 81/109, SEL 79/109, discordant 2, p 0.5): match. Gold hash and
  select-before-test checked.
- Dev `misses_read.tsv` restored: the `context` stage had blanked its read columns; the 23c session's annotation script
  (from its transcript) re-applied to identical rows; sha256 back to 0eedae4f (the pre-overwrite hash).
- Revised F17-23c.9/.10/.11 (3 "blank" misses are restarts, 1 an offset; `none` items are mostly versos); .7/.8 confirmed.
  Report (class table, none paragraph, Summary, Review addendum, Follow-ups), index (5 rows + phase row), REVIEW addendum,
  Hand-off, `plan/PLAN.md`, CONTEXT §3/§4/§5, STATUS updated; `make reports` clean. Phase closed.
