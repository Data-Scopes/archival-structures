# Phase 18 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning
- **Session:** Opus 5.5, 2026-11-02, after the discussion that produced [INPUTS](INPUTS.md) (user Q1–Q5) and UA-062
  (images: thumbs on T7_Data, per institution).
- **Read:** INPUTS, `plan/README.md`, `plan/PLAN.md`, `plan/CONTEXT.md`, the phase-17 plan and 23a brief,
  `docs/vocabulary.md`, `shared-tools/listlike.py`; code survey by an Explore agent (vocabulary, annotations, types,
  presets, io, checkpage, handprint, 23a's page builder; suite 1,443 tests).
- **Probes** (scratch, nothing kept but `planning/`): built one inventory of each new archive with a run-time preset
  (VOC v5sg + plumbing): 2.10.50 85, 2.10.01 1090, 1.05.06 8/45, AsdSAA 5023 4, 89 3.1, AsnDA 1: 0 failures (F18-P.1);
  `listlike` on those and VOC 1056/1120: flag rates and the 12-cell grid probe (F18-P.3); key coverage of the layer
  tables (SG 12212/10115, VOC 2361); NA IIIF lookup (1.05.06 has none, F18-P.6) and Memorix IIIF (HTTP 200).
- **Drawn:** `planning/volumes.tsv` (31 volume rows, seed 20261102; the draw was rerun twice at planning: once to skip
  AppleDouble members in the tar counts, once to restrict 1.05.06 to inventories with thumbs; no page was read).
- **Written:** `PLAN.md` (P1–P12), briefs 24a–24d, `STATUS.md`, the report skeleton with *Planning* findings
  F18-P.1–P.6, the `plan/PLAN.md` row, `plan/CONTEXT.md` (trimmed to budget, phase 18 added).
- **Decided with the user:** P1–P3, P7–P9 (INPUTS). **Planner's choices:** the two bars and their values (P10), the
  six-cell grid (P11; the case-sensitive numbering variant dropped after the probe), the dev/test design (P6), the
  `analysis/` home of `listlike` (not `text_layer/`: it is not a build stage).

### Step 24a: Vocabulary v2, listlike port, page_elements reader, archive presets
- **Session:** Sonnet 5.5 (subagent), 2026-11-02. **Status:** done
- **Files changed:** `archival_structures/datasets/vocabulary.py`, `archival_structures/analysis/listlike.py` (new),
  `archival_structures/analysis/page_elements.py` (new), `archival_structures/text_layer/presets.py`,
  `tests/test_datasets_vocabulary.py`, `tests/test_analysis_listlike.py`, `tests/test_analysis_page_elements.py`,
  `tests/test_text_layer_presets_archives.py` (new), `docs/vocabulary.md`, `docs/page_elements.md` (new), `docs/index.md`,
  `docs/shared_library/changelog.md`, `reports/phase-18-page-element-types.md`, STATUS.md
- **Public API:** `vocabulary.PLACEMENT, PLACEMENT_TYPES, LEGACY_PLACEMENT_TYPES, legacy_placement`; `analysis.listlike`
  (`window_features, list_score, list_flag, DEFAULT_CFG, DEFAULT_RULE, selftest, LISTLIKE_CFG_C1, RULE_R1, RULE_R2,
  LISTLIKE_CELLS, group_shares`); `analysis.page_elements` (`read_layer_lines, layer_call, listlike_records,
  type_family, LINE_TYPE_TO_GENERIC, REGION_TAG_TO_GENERIC`); seven `*_v5sg` presets.
- **Tests:** targeted 66 passed; full suite 1,457 passed, 11 skipped.
- **Acceptance:** 1-4 met (equivalence on all fixtures and six cells; old tags still known; seven preset keys listed).
- **Report:** [`reports/phase-18-page-element-types.md` § Step 24a](../../reports/phase-18-page-element-types.md#step-24a-vocabulary-v2-listlike-port-page_elements-reader-archive-presets);
  findings F18-24a.1-5
- **Error analysis:** C0R0 and C1R0 flag the same 10 of 13 fixture lines; C1 stops 3 year-ending lines counting as
  amount ends. Reader run on real SG 12212 (5,276 lines, no errors).
- **Deviations from brief:** none
- **Brief defects:** the brief's expectation that the fixture shows lines C1 stops flagging did not hold (flags equal); reported as F18-24a.3.
- **Open questions for the user:** none

### 2026-11-02 — orchestrator check of 24a (Opus 5.5)
- Diff read: `listlike` port verbatim apart from docstring/selftest path and the brief's constants; vocabulary and presets additive. Targeted tests 66 passed.
- Fixed: `vocabulary.py` docstring said "other three namespaces" for `doctype:` (now four).
- Brief fix in 24b §5: dumps keep lines with `text.strip() != ""`, the rule `read_layer_lines` uses, so 24d's 1:1 join holds on whitespace-only lines.

### Step 24b: Stage, build and sample
- **Session:** Sonnet 5.5 (subagent), 2026-11-02. **Status:** done
- **Files changed:** none in `archival_structures/`. New: `reports/data/phase-18-page-element-types/step-24b/`
  (`stage_inputs.py`, `build.sh`, `build_log.txt`, `draw_pages.py`, `sample_pages.tsv`, `SHA256SUMS`, `fetch_images.py`,
  `fetch_log.txt`, `image_checks.tsv`, `dump.py`, `dump_stats.tsv`, `dumps/` (100 pairs), `iiif_cache/`, `protect_*`);
  local `data/page_elements_{inputs,layers/24b-a,images}/`; `reports/phase-18-page-element-types.md`; STATUS.md
- **Public API:** none
- **Tests:** full suite 1,457 passed, 11 skipped (no new tests; scripts assert counts, hash, disjointness)
- **Acceptance:** 1-4 met. 7 layers / 18 inventories, 0 failures; 100 pages (60 test, 40 dev), no shortfall; 100 images,
  0 aspect mismatches; 100 dump pairs, line counts equal; VOC protect pair identical, main pair differs only in LotA
  `output/text_terms` (a running LotA job), none in AS.
- **Report:** [`reports/phase-18-page-element-types.md` § Step 24b](../../reports/phase-18-page-element-types.md#step-24b-stage-build-and-sample);
  findings F18-24b.1-6
- **Error analysis:** 10 dumps and images read; line order wrong inside P031 (letter) and P049 (letterhead); page numbers,
  running heads, form text and margin notes missing from PageXML in 6 of 10.
- **Deviations from brief:** builds ran one archive per `coreshare run` call (grants immediate); protect main pair
  differs in unrelated LotA files (see above); added `fetch_log.txt`, `image_checks.tsv`, `dump_stats.tsv`.
- **Brief defects:** `build.sh` "expect ~21 inventories" in the task was 18 in `volumes.tsv`; brief says "planning saw hand/default everywhere" but 1.05.06 (49, 1167, 1171) and AsdSAA 5023 4 get mode `print` (F18-24b.2); the protect check "only new paths may differ" cannot hold while LotA writes under `output/text_terms`; the 150-line cap leaves sg_print 3818/3799 with 2/7 eligible scans (F18-24b.3).
- **Open questions for the user:** none (the sg_print volumes 3818/3799 sampling issue is for the orchestrator)
- **Redraw (orchestrator decision on F18-24b.3):** `sg_print` upper cap 300 (others 150); P007-P011 and P066-P068 changed, images and dumps rewritten, sha256 now 57fbd6fd…3000; old draw kept as `sample_pages_v1_cap150.tsv`; all self-checks passed again, suite unchanged.

### 2026-11-02 — orchestrator check of 24b (Opus 5.5)
- Decided F18-24b.3 (planning defect): sg_print cap 300 lines (others 150); redrawn before any read, so no gold spent. 8 page ids changed (P007–P011, P066–P068); sha256 57fbd6fd…3000 checked; 6 test + 4 dev per stratum; 100 images, 200 dump files. A dump checked: header, geometry and text only; `dump.py` reads `lines/` only.
- F18-P.1 is contradicted by F18-24b.2 (1.05.06 49/1167/1171 and AsdSAA 5023 4 build as `print` by `box_height`): to correct at the phase review.

### Step 24c part A: reader prompt and tooling
- **Session:** Sonnet 5.5 (subagent), 2026-11-02. **Status:** part A done; waiting for reads (part B, orchestrator)
- **Files:** `reports/data/phase-18-page-element-types/step-24c/`: `reader_prompt.md`, `make_batches.py`, `reader_batches.tsv`
  (22 batches, columns `batch stratum page_ids n_pages n_lines`), `check_reads.py` (`--pages`), `merge_reads.py`, empty `reads/`;
  report section `### Step 24c: Element gold` (part A paragraph).
- **Tests:** scripts run on synthetic reads (P012, P052) in a scratch dir, good and bad cases; none left in `reads/`. No library code.
- **Deviations:** reader writes `line_id` as `-`, merge fills it from the dump; batches are one stratum each (22 batches, 2 per
  stratum) because 10 pages per stratum exceed 8; `check_reads.py` also checks subtypes and rejects `marginalia`.
- **Brief defects:** the brief does not say how a reader obtains `line_id` (the `.txt` dump shows only `L<k>`); `is_known_tag`
  does not validate subtypes; brief's check list has no rule for notes (added: required for unclear and custom).

### 2026-11-02 — 24c part B: agent reads (orchestrator, Opus 5.5)
- 22 Sonnet reader agents (batches B01–B22 of `reader_batches.tsv`; 20 at once, B21/B22 started when slots freed) with `reader_prompt.md` unchanged. All 100 reads written; `check_reads.py` OK (100 pages) on first submission: **0 reruns for check errors**.
- **Prompt defect found (orchestrator):** the prompt said readers *may* open the image, not *must*. Readers of B06, B10, B11, B12, B14, B15, B16, B17, B18 and B21 labelled some or all pages from the dump only; several filled line ranges by a default in a script. Every batch got the same follow-up (open every image; re-read any label set by rule or block, line by line; rewrite only where a label changes). The rubric was not changed.
- Follow-up changes (rows, as reported by the readers): B01–B05, B07–B10, B13, B18, B19, B20 0; B06 6 (catchwords, page number, signature); B14 8 (addition/interlinear, salutation, running_title:date, item starts); B15 6 (bottom → margin note); B17 41 (blocks main → margin); B21 ~41 (start flags in an account, merged notes, one closing); B22 6 (start flags); B16 103 (unclear flags only: range-wide flags reduced to uncertain lines); B12 179 (P030, P077 `table` → `form:value`/`form:label`); B11 ~245 (P028, P029, P078 `table` → form/note; P080 re-read line by line on a second follow-up: 2 rows).
- Final reads: 7,501 lines, 325 unclear. Families: running_text 3,782, table 953, list 757, form 637, closing 306, heading 249, noise 193, note 175, numbering 136, addition 64, salutation 56, endorsement 49, quire_marks 48, custom 43, running_title 34, reference 10, stamp 8, drop_capital 1. Placements: main 6,771, top 380, margin 248, bottom 68, interlinear 34.
- Caveats for part C and 24d: many readers wrote files through scratch scripts (labels by hand-chosen line ranges); several scratch scripts in shared `/private/tmp/claude-504/` dirs were overwritten by other readers (B12, B19 re-checked their files). The 30-line self-check of part C is the independent check on read quality; the `table`/`form` boundary differs between readers (B12 kept P079 as `table`, B11 P080 as `table`+`form:label` caption band) — both count as bar-L positives.

### Step 24c part C: self-check, dev gold, check page, summarise script, UA-063
- **Session:** Sonnet 5.5 (subagent), 2026-11-02. **Status:** part C done; waiting for UA-063 (user's 60 test pages), then part D run by the user
- **Files:** `reports/data/phase-18-page-element-types/step-24c/`: `annotations_list_before.txt` (3,138 files, before part C), `prelabels.tsv` (7,501 rows,
  (scan_id, line_id) unique), `selfcheck.tsv`, `gold/{dev_labels.tsv,SHA256SUMS,dev_gold.done}`, `build_element_page.py` + `element_page.{css,js,html}`,
  `summarise_elements.py`, `test_summarise_elements.py`; `docs/USER_ACTIONS.md` UA-063; report section; F18-24c.1-4
- **Self-check:** 29 of 30 agree; P044 k68 type wrong (low confidence, thumbnail). Dev gold frozen: 3,167 rows, sha256 e0f2a4de7c24f23ca596941e39b0b56eabc8ed603f0aa51b123498568d4dc904
- **Page:** `node --check` OK; Chrome headless: renders page 1, scripted keys/selection/undo/review/export OK, no script error; wheel/drag/remote zoom swap not exercised
- **Tests:** `test_summarise_elements.py` OK (synthetic export, rows, changed, element runs, refusal to overwrite); full suite 1,457 passed, 11 skipped. No library code changed.
- **Deviations:** none from the brief; `summarise_elements.py` keeps the export values of an unreviewed page in `test_labels.tsv` (`reviewed` = 0) and leaves it out of the annotations
- **Brief defects:** (1) does not say how the 30 lines are drawn: used `sorted(random.Random(20261102).sample(range(7501), 30))`; (2) "Reads:" paths for images and the `.html` path rule (relative vs absolute) not stated: relative `../../../../data/page_elements_images/`; (3) `summarise_elements.py` behaviour for unreviewed pages and the annotation directory layout (`<institute>/<archive column>/<inv>`) not stated: followed existing `data/annotations`; (4) brief says `annotations_list_before.txt` is `find ... | sort` + sizes: written as `size path` sorted by path.
- **Open for the user:** UA-063

### 2026-11-02 — orchestrator check of 24c part C (Opus 5.5)
- Verified: dev gold sha256 e0f2a4de…c904 matches `SHA256SUMS` and `dev_gold.done`; no `test_labels.tsv`/`test_gold.done` in `gold/` (the synthetic test ran elsewhere); UA-063 present with an empty Response; the page HTML carries no layer type, layout role or region tag (grep 0).
- Rendered `element_page.html` headless (Chrome, file://): instructions, P001 with image, boxes numbered by k, rows with type/placement chips. Wheel/drag zoom and the 2400 px swap not exercised.
- Self-check 29/30 (one type disagreement, P044 k68): dev gold frozen under the 6/30 stop rule set by the orchestrator.
- 24d dev stage launched (Sonnet); the test stage waits for UA-063.

### Step 24d dev stage: measure (Sonnet 5.5 subagent, 2026-11-02)
- **Status:** dev done; test stage not run (refuses: `gold/test_gold.done` missing). Report: `reports/phase-18-page-element-types.md#step-24d-measure`, F18-24d.1 to .6.
- **Files:** `reports/data/phase-18-page-element-types/step-24d/`: `eval_elements.py` (both stages, `--selftest` ok), `dev_lines.tsv`, `dev_grid.tsv`, `select.json`, `tables/dev_*.tsv` (5; `prelabel_agreement` is test-only), `cases_dev_*.tsv`.
- **Result:** join 1:1 on 40 pages / 3,167 lines. Grid F1: C0R0 .112, C0R2 .091, C0R1 .553, C1R0 .051, C1R2 .049, C1R1 .560; selected C0R1 (tie with C1R1 by 0.01 rule) P .862 R .407. Bar M dev P .227 R 1.000 (208 FP, 149 `extra_role`, 59 `page_margin`).
- **Brief defects:** (1) layer-types table: "right" rule for `date_header` given, but the denominators (all lines vs `unclear == 0`) are not: used all lines; (2) bar M keeps only gold placement `margin`/`main`, so `top`/`bottom`/`interlinear` lines are dropped (stated in the report); (3) the margin P/R in `layer_types` per-family list excludes `margin`; (4) `cases_*` context format and `prelabel_changes.tsv` (20 pre-to-gold changes) not specified: added; (5) test `reviewed == 0` pages are dropped before the join, so `test_lines.tsv` has only reviewed pages; (6) "most frequent (pre -> gold) changes" taken as family-level changes.
- **Deviations:** none from the brief; test-stage code is untested on real test rows (selftest covers refusals only).
- **Open for the user:** UA-063.

### 2026-11-02 — orchestrator check of 24d dev stage (Opus 5.5)
- Re-ran `eval_elements.py --selftest` (ok) and the full suite (1,457 passed, 11 skipped). `dev_grid.tsv` and `select.json` read: selection C0R1 follows the fixed tie rule (C1R1 max F1 0.5597; C0R1 0.5526 within 0.01 and earlier in the order); `select.json` carries the dev_labels hash e0f2a4de…c904.
- Dev only (agent gold): no cell reaches R 0.80 (best 0.41, R1 rules); the cells without `closed_share` flag ≤ 6% of positives. M on dev P 0.227, R 1.000 (208 FP, 161 on 2.10.50, mostly forms/tables by `rule:extra_role`). These are dev numbers on agent gold and say nothing yet about the test bars.
- Open: UA-063; then the 24d test stage (once), then the phase review.

## Orchestrator: rubric clarification during UA-063 (2026-11-05)

The user asked how to label table column headings (pre-labelled `running_title` on test items 25–26, `form:label` on
27–28; the reader rubric did not cover them). Ruling: a column heading is part of its table, type `table` (a Bar L
positive), `start = 1` on the heading line when it opens the table, note `colhead` so the 24d error analysis can split
them out. `form:label` stays for captions beside blanks in a pre-printed form (not column heads); `running_title` only for
a title line that repeats per page, not for column heads repeated per page. The dev gold (agent labels, spent) is not
relabelled; the review notes the inconsistency if dev used other labels.

Second ruling (same day): a note written inside a table is type `note` (placement `main` if written in the table area,
`interlinear` between rows, `margin` beside it), `start = 1`. Elements have no end marker (`summarise_elements.py`: a run
from a start line to the line before the next start, typed by its first line), so the first table line after the note
also gets `start = 1` and note `cont` (continues the table above). The bars are per line and do not read `start`. For
the phase review: the element model cannot express an element interrupted by another; a candidate for the annotation
format (e.g. a continues-element flag or element ids).

## Orchestrator: rubric clarification during UA-063 (2026-11-17, UA-070 and UA-071)

The user asked how to label probate-inventory items (AsdSAA 5075, test P043–P048): multi-line prose descriptions in the
main column with an aligned amount column. Readers split them (P045, P047: descriptions `list`, amounts `table`; P046 all
`table`); the user had P045 descriptions as `running_text`. Ruling (UA-070 a): the whole block is one `table` element
(description and amount lines), `start = 1` on its first line; description lines written as full sentences get note
`prose`. Reason: the rubric's `table` (aligned columns, amounts, ledger rows) and the frozen dev gold for 5075
(P089–P092 only `table`/`list`, both bar-L positives); `running_text` would make the same kind of line negative on test
and positive on dev. Ruling (UA-071 a): an insertion of omitted text is `addition` with its own placement
(`interlinear`/`margin`), whatever the host line's type (readers already did this, e.g. P047 k 9, 13).
For the phase review: line `type` holds the element type only; whether a line is prose continuing the previous line's
sentence is a separate property (applies inside tables and lists too), as are item boundaries inside a table. Both are
candidates for the phase-19 annotation format.

## UA-063 answered (2026-11-17, orchestrating session)

All 60 test pages reviewed; the user ran `summarise_elements.py`: `gold/test_labels.tsv` 4,334 rows, changed share 0.150,
sha256 286b3ad0…fb41, `gold/test_gold.done` 2026-11-17T10:57:25Z. During the review the check page got a stable arrow
position and a page-jump dropdown (`build_element_page.py`, `element_page.js`; storage key unchanged). UA-072 (a): no
`prose` pass in phase 18. Next: 24c part D and the 24d test stage (one Sonnet session).

## Step 24c part D and 24d test stage (2026-11-17, Sonnet 5.5)

- 24c part D: `annotations_list_after.txt` written, `diff` against `_before` empty (data/annotations untouched). After-UA-063 analysis in the report (Step 24c) and `step-24c/after/` (`analyse_after.py`): changed share 648 of 4,334 = 0.150; table 0.495, note 0.368, running_text 0.009; 2.10.50 0.453, AsdSAA 5075 0.401; 9 page notes; 175 unclear.
- 24d test stage (once, from the AS root; a first start from the step directory failed on a relative layer path before reading gold, wrote nothing): select.json C0R1 (2026-11-02) predates test gold; hashes checked. Bar L FAIL: C0R1 P 0.950 (boot 0.898 to 0.977) R 0.575 (boot 0.498 to 0.655); C0R0 P 0.967 R 0.060. Bar M FAIL: P 0.259 (boot 0.087 to 0.497) R 0.872 (boot 0.636 to 1.000). No retuning.
- Error analysis in the report: 44 L false positives all gold running_text; 619 false negatives (wrapped table or item lines); 234 M false positives (166 `extra` role, 160 table). Brief defects (6) listed there.
- Report: `reports/phase-18-page-element-types.md`, Steps 24c and 24d (findings F18-24c.5 to .6, F18-24d.7 to .14, draft); `make reports` clean. Files: `step-24d/test_*`, `cases_test_*`, `tables/*` (non-dev), `step-24c/after/`, `step-24c/analyse_after.py`.

### Phase review (Opus 5.5, 2026-11-17)
- **Read:** PLAN, STATUS, PROGRESS, the report, the diff (`vocabulary.py`, `presets.py`, new `listlike.py`,
  `page_elements.py`, tests, docs, changelog), the step scripts (24b draw, 24c page builder and summarise, 24d eval).
- **Re-ran:** AS suite (1,457 passed with the git safe.directory env; 1 env-only failure without), LotA 609 passed,
  NLP 66 passed; 24a acceptance; gold and sample hashes; both 24d stages into a scratch dir: all outputs byte-identical;
  `data/annotations/` listing equal; Sphinx build (5 new warnings from the `listlike` docstring).
- **Own error analysis:** all 44 L FPs, 25 fresh L FNs, all 12 M FNs, 15 fresh 2.10.50 M FPs, the `prose`/`cont`/
  `colhead` tokens, ruling sensitivity (UA-070 b: P 0.753), features on P036/P001/P045, self-check vs user gold, one image
  (P039). Findings F18-R.1–R.13. Steps' findings: 28 confirmed, 9 revised, 0 withdrawn.
- **Defects:** `bulk_tagging` lacks `placement` (F18-R.10); `region_tags_voc.tsv` pools all strata (F18-R.7);
  `listlike` docstring renders badly. Brief: `step-24r-review-fixes.md`.
- **Written:** `REVIEW.md`, report Summary/Review/Follow-ups/Reproduce + statuses (final), `reports/index.md` (row 18,
  50 findings), `make reports` clean, `step-24r-review-fixes.md`, PLAN hand-off and checklist, `plan/PLAN.md` row,
  `plan/CONTEXT.md` (§1–5), STATUS, UA-073 in `docs/USER_ACTIONS.md`.
- **Release:** v0.23.0 recommended after 24r (`make version`: 0.22.0, tag v0.22.0 on HEAD; needs a minor bump).

### Step 24r: Review fixes (2026-11-17, Sonnet 5.5)
- **Files:** `archival_structures/datasets/bulk_tagging.py`, `archival_structures/analysis/listlike.py` (docstring), `tests/test_datasets_bulk_tagging.py` (+3), `docs/shared_library/changelog.md`, `reports/data/phase-18-page-element-types/step-24r/{region_tags_voc.py,region_tags_voc_test.tsv,region_tags_voc_dev.tsv}`, report section Step 24r.
- **Tests:** targeted 35 passed; full suite 1,460 passed, 11 skipped (git safe.directory env). Selftest ok; Sphinx `listlike.py:docstring` warnings 0.
- **Acceptance:** VOC test 325 lines; `list` 69, `table` 154, `running_text` 67; dev `list` 51, `running_text` 90: all as the brief states.
- **Deviations / brief defects:** none. No new findings. See report Step 24r.
