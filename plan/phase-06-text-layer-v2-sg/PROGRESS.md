# Phase 06 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning (Opus, 2026-10-17)
- **Trigger:** LotA's hand-off `docs/from-language-of-the-archive.md` (R1–R6). The user said: AS
  regenerates, AS ports republic's splitter, and all SG inventories are covered.
- **Read:**
  - the hand-off, `docs/from-republic.md`, the SG audit §1–3, inventory C14/C15/§6, and the
    phase-05 hand-off and REVIEW;
  - three Explore surveys (the AS `text_layer` API, LotA's consumer side, data and the splitter).
- **Planning checks** (scratchpad `p06/`): C1–C5 in `PLAN.md`; findings F06-P.1–4.
  - The staged PageXML reproduces the TSVs' `doc_id`s and text 100% (4 inventories).
  - The `Word` elements carry no lost spaces.
  - 3.9% of print lines are merge candidates.
  - REPUBLIC print line ids match the PageXML lines in 94% of cases.
  - The original splitter runs under AS `.venv`.
- **Decisions:** P1–P12 (`PLAN.md`). The work is split over phase 06 (15d–15f) and phase 07
  (types, units, mapping, pilot, acceptance). Old phases 07 and 08 become 08 and 09.
- **Written:**
  - `PLAN.md`, the briefs 15d/15e/15f, `STATUS.md`;
  - the report skeleton `reports/phase-06-text-layer-v2-sg.md`;
  - phase 07 seeded (`plan/phase-07-sg-units-and-pilot/`);
  - `plan/PLAN.md` rows;
  - `docs/USER_ACTIONS.md` (UA-001).

### Step 15d: v2 extract
- **Session:** Sonnet 5.5, 2026-10-17. **Status:** done
- **Files changed:** `archival_structures/text_layer/{io,presets,build}.py`, new `extract_v2.py`; `tests/test_text_layer_extract_v2.py`, `tests/fixtures/text_layer/sg/9999/` (4 XML); `docs/text_layer.md`; `docs/shared_library/changelog.md`; `reports/phase-06-text-layer-v2-sg.md`; `reports/data/phase-06-text-layer-v2-sg/step-15d/{legacy_check,ea}.py`
- **Public API:** `iter_pagexml(source, *, skip_resource_forks=False)` (+ `.tar.gz`/`.tgz`; `input_bytes` too); `discover_inventories_v2(pagexml_dir, pattern)`; `LayerPreset` + 5 fields; `SG_HAND_V2`, `SG_PRINT_V2`; `extract_v2.{extract_inventory_v2, extract_scan_v2, legacy_doc_id, legacy_region_text, *_COLUMNS_V2}`
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_model_scan_id.py` → 124 passed; full suite 988 passed, 0 collection errors
- **Acceptance:** 1 14/14 inventories, doc_id set equal, text equal 151,246/151,246 rows; 2 no duplicate region/line ids (16 inv); 3 scan rows = XML members 16/16, scan_stats identical for 14; 4 `TOTAL 157 / 157`; 5 16/16 exit 0, wall 21.2+28.0+41.4 = 90.6 s; 6 green
- **Report:** [`reports/phase-06-text-layer-v2-sg.md` § Step 15d](../../reports/phase-06-text-layer-v2-sg.md#step-15d-v2-extract); findings F06-15d.1–6
- **Error analysis:** hand and print 456 lines always inside their region (region = line union); print 455 regions are separate polygons (67% of lines exceed them, mostly < 30 px) and 93% of its regions are empty specks. Single-page scans explain 35/65 side shares in 3351/3354.
- **Deviations from brief:** none (the unlisted `input_bytes` also got `.tar.gz` support; needed by the CLI)
- **Brief defects:** (1) `skip_resource_forks` is redundant: the existing `_is_pagexml_member` already skips `._*` and `__MACOSX`; the flag is kept as an explicit guard. (2) `test_legacy_region_text` hint "no `-`/`„` at a region end" is wrong: a trailing WBC is kept by the library join (`["x-"]` gives `"x-"`); tested as such. (3) `deelopname` scan ids do not parse with `ScanId`, so `scan_id_ok` is 0 and `inv_num` falls back to `inv_dir` (as specified). (4) The stated suite baseline (6 collection errors) no longer holds: all modules import in `.venv`.
- **Open questions for the user:** none

### Planner check before 15e (Opus, 2026-10-17)
- Spot-checked 15d: the fixture tests pass (11), and `15d-a/legacy_check.txt` coverage is 16/16.
- On 15d's print lines, found two defects in the 15e brief (F06-P.5):
  - 455 tags 81% of lines `index` (scans 4–717), and 456 tags everything `Text`. So
    `split_region_tags` became `("index","resolution","untagged","Text")`.
  - 58% of 456 lines are merged, so the plain median width is the merged width. The parameters
    are now derived from column lines within 0.35–0.6 of the side's text span (about 900 px and
    45 characters on both inventories).
- 15e brief updated.
- UA-001 answered (a): the target is ≥ 80% of the 116 rows. Recorded in phase 07 and moved to
  `docs/USER_ACTIONS_resolved.md`.

### Step 15e: merged-line splitter
- **Session:** Sonnet 5.5, 2026-10-17. **Status:** done (all acceptance numbers reproduce; UA-002 open, blocks nothing)
- **Files changed:** new `archival_structures/text_layer/line_split.py`; `text_layer/{presets,build}.py`; `pyproject.toml` (+`nltk (>=3.9)`); new `tests/test_text_layer_line_split.py`; `docs/text_layer.md`; `docs/shared_library/changelog.md`; `docs/USER_ACTIONS.md` (UA-002); `reports/phase-06-text-layer-v2-sg.md`; `reports/data/phase-06-text-layer-v2-sg/step-15e/{ab_republic,run_notebook_params,rates,ea}.py`
- **Public API:** `line_split.{SplitParams, derive_split_params, train_bigram_finder, make_pmi_scorer, merged_line_candidates, SplitModel, fit_split_model(inv, source, preset, lines_path=None, params=None), fit_split_model_from_rows, make_split_hook, SplitHook, extract_inventory_v2_split}` + all ported names; `LayerPreset.split_region_tags`
- **Tests:** `pytest tests/test_text_layer_*.py` 110 passed (14 new, in `tests/test_text_layer_line_split.py`); full suite 1002 passed, 0 collection errors
- **Acceptance:** 1 A/B 455 1,421/1,421, 456 1,435/1,435 identical, 0 raising (needs the pagexml 0.7 `get_all_text_regions` behaviour for the original; unpatched it raises on 50/50); 2 455: 11,470 cand, 2,398 hyphen + 9,064 bigram, 8 unsplit; 456: 64,096 cand, 12,670 + 51,393, 36 unsplit; 0 page errors; 3 text conservation 0 exceptions; 4 geometry 4/64,063 outside box (456), L.x<R.x 100%, column 100% of non-empty lines; 5 456 derived 924/45/1127 vs notebook 900/45/1100, 63,896/64,063 splits identical, notebook splits 192 more (real merged lines); 6 rates in report; 7 `TOTAL 157 / 157`, protected inputs unchanged
- **Report:** [`reports/phase-06-text-layer-v2-sg.md` § Step 15e](../../reports/phase-06-text-layer-v2-sg.md#step-15e-merged-line-splitter); F06-15e.1-8
- **Error analysis:** hyphen splits 30/30 correct; bigram 22/30 correct, 6 off by 1-2 boundary words; 26 of 38 unsplit candidates are single wide-box lines, 12 real merged (indent 3, page bottom 3, degenerate 3, glued hyphen 1, fragments 1, unknown 1)
- **Deviations from brief:** (1) one crash fix in the port (`_page_text_regions`); (2) the hook collapses runs of spaces in the copies it feeds to the splitter (the ported code raises on text != joined words); (3) a split that leaves a half without text is dropped (`n_degenerate_splits`); (4) steps 1-2 are skipped on a page side with no candidates (same result); (5) `fit_split_model` also takes `params=`; (6) agent shell: started the two A/B runs with `&` once; `pip download` put wheels in the repo root by mistake (moved to scratchpad, nothing left in the repo)
- **Brief defects:** (1) "the original imports cleanly under AS .venv" is true but `page_split_columns` fails on pagexml-tools >= 0.8.0; (2) the brief expects hand inventories near 0 candidates; 5 of 8 are 40-87% (rule presumes two columns); (3) `x` never occurs (`extra_lines` is unreachable in the original); (4) `copy_line` drops `conf`, the hook restores it; (5) lines without text get no `column`
- **Open questions for the user:** UA-002 (threshold factor 1.22 vs 1.19)

### Step 15f: glued-word repair
- **Session:** Sonnet, 2026-10-17. **Status:** done (negative result; presets off)
- **Files:** `archival_structures/text_layer/word_split.py` (new), `build.py` (stage `wordfix`, `--word-vocab`, `--word-params`, `--pagexml-dir` optional for wordfix), `presets.py` (`word_split`, `word_split_params`), `tests/test_text_layer_word_split.py`, `reports/data/phase-06-text-layer-v2-sg/step-15f/{evaluate,ea}.py`, `docs/text_layer.md`, changelog, `docs/USER_ACTIONS.md` (UA-003)
- **Public API:** `word_split.*`, `LayerPreset.word_split[_params]`, CLI `--stage wordfix`; backwards compatible
- **Tests:** 8 new; text-layer modules 118 passed; full suite 1010 passed, 0 collection errors; v1 `TOTAL 157 / 157` (`15f-v1check`)
- **Acceptance:** chosen (min_token_len 8, min_part_count 20, split_penalty 0) for both. Test exact 0.310 print / 0.376 hand, boundary P 0.984 / 0.9995, R 0.273 / 0.288. Production: print 17,334 lines / 18,502 tokens changed, hand 4,563 / 4,607; conservation 0 violations. Hand-read precision print 31/50 = 0.62, hand 8/50 = 0.16 -> both OFF
- **Report:** `reports/phase-06-text-layer-v2-sg.md#step-15f-glued-word-repair` (F06-15f.1-4)
- **Error analysis:** genuine words broken at a frequent prefix/suffix (print 17/50, hand 41/50); remaining long tokens are mostly glued but unsplittable under the unsplit score (2,910 of 5,961)
- **Deviations from brief:** wordfix needs no pagexml dir (CLI change); `is_candidate`/`maybe_segment` added; evaluation draws one synthetic sample per `min_token_len`
- **Brief defects:** (1) the brief names no CLI handling for a stage without PageXML input; (2) "hand-read ... min(50, all)" is silent on how to judge ambiguous compounds (counted as genuine); (3) the unsplit score `log(0.5/total)` makes the brief's `max_parts=4` nearly unreachable
- **Open questions for the user:** UA-003

### Phase review (Opus, 2026-10-18)
- **Verdict:** accept with follow-ups ([REVIEW](REVIEW.md)). Step 15fr runs before the v0.10.0 release.
- **Re-run** (fresh dirs `data/text_layer_v2_dev/06R-{a,v1check,ab,15f}/`; 10 protected inputs unchanged):
  - hand tables 56/56 and print tables 12/12 are byte-identical to 15d-a/15e-a;
  - legacy check identical (151,246/151,246); A/B 1,421/1,421 and 1,435/1,435;
  - v1 `TOTAL 157 / 157`; 15f test JSON equal.
- **Suites:** AS 1010 passed; NLP `tests/` 56 passed; LotA 318 passed and 9 failed (LotA's
  `shared-data` path roots; not AS).
- **Own error analysis:** bigram 11/16 correct (5 off by 1–3 words); hyphen 12/12; the UA-002
  margin in 455 adds 45 candidates, about 23 of 25 of them merged; 15f hand verdicts hold.
  Findings F06-R.1–5. F06-P.3 and F06-15e.5 revised, the other 21 confirmed.
- **User actions:** UA-002 (b) goes into 15fr; UA-003 (a) needs no change. Both moved to
  `docs/USER_ACTIONS_resolved.md`.
- **Written:**
  - `REVIEW.md`, `step-15fr-review-fixes.md`;
  - the report's Summary, Review and Follow-ups, with status final;
  - `reports/index.md` (row 06 + 28 findings), `make reports` clean;
  - this `PLAN.md` checklist and hand-off, `plan/PLAN.md` rows 05/06, `STATUS.md`;
  - phase 07 `PLAN.md` (review carry-overs; UA-003 closed);
  - `plan/templates/STEP.md` (3 brief-quality rules);
  - the one-line usage fix in `step-15e/ab_republic.py`.

### Step 15fr: review fixes (Sonnet, 2026-10-18)
- **Files changed:** `text_layer/line_split.py` (factor 1.19 keyword, attribution), `text_layer/word_split.py` (`KEEP_TRAIL` removed), `tests/test_text_layer_line_split.py`, `docs/text_layer.md`, `docs/shared_library/changelog.md` (Step 15fr entry), `reports/data/phase-06-text-layer-v2-sg/step-15fr/ea15fr.py` (new)
- **Public API:** `derive_split_params(..., merged_width_factor=1.19)`; default changes `sg_1.10.94_v2` line tables (more splits); no sibling import changes
- **Tests:** 1 test changed, 0 new; full suite 1010 passed, 0 collection errors
- **Acceptance:** re-run exit 0, 0 page errors. 455: +45 candidates, +42 splits. 456: +203 candidates, +202 splits (expected ~190). Conservation 0 exceptions; halves outside box 0 (455) / 4 (456); `L.x >= R.x` 0. Rates table: 16 rows. Protected inputs equal.
- **Report:** `reports/phase-06-text-layer-v2-sg.md#addendum-step-15fr-review-fixes`; `make reports` run
- **Error analysis:** 40 new splits read: 33 correct, 1 off by ~3 words, 2 OCR-noise, 4 not verifiable, 0 confirmed false. 4 new unsplit candidates: 3 failed merged lines, 1 single wide line. 455 new splits concentrate on scan 0222 (18 of 42), a narrow sub-column French insert.
- **Deviations from brief:** none
- **Brief defects:** (1) `ea.py check` OUT is a file, not a directory (I first created a directory and removed it with `rmdir`); (2) the brief gives no tool for the new-splits sample, so `step-15fr/ea15fr.py` was written (reuses `step-15e/ea.py`)
- **Open questions for the user:** none
