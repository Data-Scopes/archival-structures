# Phase 18 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-11-17
- **Verdict:** accept with follow-ups (small review fix `step-24r-review-fixes.md`: one library defect in
  `datasets/bulk_tagging.py`, one mis-filtered reported table, one docs-rendering debt). Both bars fail on test and the
  failures are robust; nothing in the code changes a verdict.

## Checklist

- [x] **Acceptance numbers re-run; gold hashes match; `select.json` precedes `test_done.json`.** `shasum -a 256 -c
  gold/SHA256SUMS`: `dev_labels.tsv: OK`, `test_labels.tsv: OK` (e0f2a4de…c904, 286b3ad0…fb41); `step-24b/SHA256SUMS`
  OK (57fbd6fd…3000); `planning/volumes.tsv` 5b4d1cc5…c5ab. `select.json` 2026-11-02T22:07:38Z, `test_gold.done`
  2026-11-17T10:57:25Z, `test_done.json` 2026-11-17T11:04:42Z; `test_done.json` carries `select_sha256` c8c5fb32…f8d,
  which equals `shasum select.json` now. The reviewer re-ran **both 24d stages** into a scratch directory (module loaded
  from its path, `HERE` redirected, the same `select.json`): `test_eval.tsv`, `test_lines.tsv`, all `cases_test_*`,
  `tables/{bars_by_stratum,layer_types,prelabel_agreement,listlike_by_type}.tsv`, `dev_grid.tsv`, `dev_lines.tsv`,
  `tables/dev_bars_by_stratum.tsv`, `cases_dev_L_fn.tsv` are **byte-identical**; `select.json` equal except `time`.
  `eval_elements.py --selftest` ok.
- [x] **AS suite green; no sibling code changed; shared-tools unchanged; `data/annotations/` unchanged.** AS: 1,456
  passed, 11 skipped, 1 failed: `test_text_layer_provenance.py::test_package_info_archival_structures_has_git_head`,
  which fails only because git refuses the repo ("dubious ownership") in this session; with
  `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=<AS root>` it passes (9/9), so 1,457 passed,
  as logged. LotA (`PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider`): 609 passed, 1 skipped,
  1 xfailed. NLP (same, `tests`): 66 passed. The siblings' working trees carry their own sessions' edits; no sibling file
  imports `listlike`, `page_elements`, `GENERIC_TYPES`, `NAMESPACES` or a preset. shared-tools `listlike.py` mtime
  2026-11-01 22:25 (before planning); `checkpage.py` (2026-11-03) and `README.md` (2026-11-04, "user request") changed
  outside any phase-18 session (no PROGRESS block in that window). `data/annotations/`: a fresh `stat` listing (3,138
  files) equals `annotations_list_before.txt` (diff empty).
- [x] **`listlike` port equal to shared-tools (P4); old vocabulary tags known (P2/P3).** `diff` of the two files (trailing
  blanks stripped): only the shebang, the docstring header and self-test path, and the appended constants
  (`AMOUNT_END_LEADERS`, `LISTLIKE_CFG_C1`, `RULE_R1`, `RULE_R2`, `LISTLIKE_CELLS`, `group_shares`); defaults unchanged.
  Targeted tests (brief command, five files): 66 passed, 107 subtests. `is_known_tag('placement:interlinear')`,
  `is_known_tag('generic:marginalia:note')` True, `legacy_placement('generic:marginalia:note') == 'margin'`. Acceptance 4
  lists the seven `*_v5sg` keys and `voc_1.04.02_v5sg`.
- [x] **Gold quality.** Self-check 29/30 (it is 14 test + 16 dev lines, not 19 + 11 as the report says). User review: all
  60 pages reviewed, changed 648/4,334 = 0.150 (type 618, placement 2, start 61, verified on `test_labels.tsv`), unclear
  175 (4.0%), 0 pages unreviewed. Caveats in F18-R.3, F18-R.8, F18-R.9, F18-R.12.
- [x] **Leakage.** Readers: `reader_prompt.md` forbids layer tables, `data/annotations/`, `prelabels.tsv`, code;
  the check page embeds no `layout_role`/`line_type`/`region_tag`/`listlike` (grep 0; one `marginalia` is the rubric's
  "do not use"). SG test volumes: `draw_pages.py:86-91` asserts no (archive, inv) in `text_layer_v2_dev`; one-volume
  archives split by scan range (dev 0007–0109 vs test 0121–0226 for 5023 4, etc.). Selection on dev only (agent gold),
  before the test gold existed; the test stage ran once. No library code reads gold. One disclosed non-blindness: the
  UA-070 ruling was set by a session that knew the dev numbers; it decides bar L's *precision* verdict (F18-R.1).
- [x] **Error analysis read individual lines; `make reports` clean.** Each step's analysis is in the report; the reviewer's
  reads are below. Three step findings carry numeric slips (F18-24d.10, .12, .13; one is a code defect, F18-R.7).
  `make reports` builds all reports with no warning.
- [x] **Changelog `### Step 24a` under `## Unreleased`, "no sibling imports affected".** Present and accurate for
  `vocabulary`, `listlike`, `page_elements`, presets. Missing: `NAMESPACES` now feeds `bulk_tagging`'s namespace
  dropdown, which offers `placement` without type suggestions (F18-R.10; fixed and logged by 24r).
- [x] **Brief defects answered** (below).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 24a | targeted tests | 66 passed | 66 passed, 107 subtests | yes |
| 24a | full suite | 1,457 passed, 11 skipped | 1,456 + 1 env-only failure (git safe.directory); passes with the env override | yes |
| 24a | seven preset keys | listed | listed | yes |
| 24b | 7 layers, 0 failures | 7 logs `failures=0` | `grep -c 'end stage=all failures=0' build_log.txt` = 7 | yes |
| 24b | 60 test + 40 dev, hash | 57fbd6fd…3000 | split counts 60/40; `shasum -c` OK | yes |
| 24b | 100 images, 100 dump pairs | yes | 100 files in `data/page_elements_images/`, 200 in `dumps/` | yes |
| 24c | prelabels 7,501; dev 3,167; test 4,334 | yes | `wc -l` 7,502 / 3,168 / 4,335 with headers | yes |
| 24c | changed share | 0.150 | 648/4,334 | yes |
| 24c | annotations untouched | diff empty | diff empty (fresh listing) | yes |
| 24d | dev grid, selection C0R1 | F1 .553 vs C1R1 .560, tie rule | byte-identical `dev_grid.tsv`; `select.json` equal but `time` | yes |
| 24d | bar L C0R1 test | P .950 R .575 fail | P 0.950 R 0.5746 fail | yes |
| 24d | bar M test | P .259 R .872 fail | P 0.2595 R 0.8723 fail | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect | `archival_structures/datasets/bulk_tagging.py:42-48` | `_NAMESPACE_TYPES` is a local copy without `PLACEMENT`; `NAMESPACES` (now five) feeds the dropdown at l.211, so the user's bulk-tagging widget offers `placement` and `type_options_for_namespace('placement')` returns `[]` (F18-R.10) | 24r |
| 2 | defect | `reports/data/phase-18-page-element-types/step-24d/eval_elements.py:250` | "VOC region tags" table keeps `if r["region_tag"]`, but every line carries a tag (`Text` in SG/2.10.01, `untagged` elsewhere), so `tables/region_tags_voc.tsv` and `dev_region_tags_voc.tsv` pool all ten strata; F18-24d.13's numbers (210 of 278 list lines) are not VOC's (F18-R.7) | 24r: VOC-only tables from the existing `*_lines.tsv`, no stage re-run |
| 3 | debt | `archival_structures/analysis/listlike.py:1-36` | the verbatim docstring is not reST: Sphinx gives 5 warnings/errors on `page_elements.md`'s automodule and renders it garbled | 24r (literal block) |
| 4 | debt | `step-24d/eval_elements.py:3`, `step-24b/sample_pages.tsv` | relative `layer_root` paths make the script cwd-dependent (Brief defect 1); it failed once from the step dir before reading gold | recorded; CONTEXT §5 |
| 5 | debt | `step-24c/summarise_elements.py` (element runs) | elements = runs from a `start` line in dump order; on register spreads dump order is not reading order, and 12 of 14 table lines after a note on P029 have `start = 0`, so `gold/annotations/elements-*.json` there are not element boundaries (F18-R.9) | phase-19 annotation format |
| 6 | observation | `text_layer` mode (presets `*_v5sg`) | 2.10.50 gets `hand`/`default` because no region reaches `min_region_chars` 100 (`n_regions 0`); AsdSAA 5023 4, a handwritten volume (P039 image), gets `print` by box height (margin −4.75) (F18-R.11) | follow-up |
| 7 | observation | `analysis/page_elements.py:72-79` | `REGION_TAG_PLACEMENT` makes every tag but `marginalia`/`untagged` a `main` placement (incl. `page-number`, `header`): fine for bar-M style counts, wrong for top/bottom | none (reported baseline only) |
| 8 | observation | `text_layer/presets.py:260` | `_archive_v5sg` sits between two preset constants with one blank line; cosmetic | none |

## Error analysis read by the reviewer

Reads (all from `test_lines.tsv`/`dev_lines.tsv`, the gold and the dumps; one image, P039):

- **All 44 bar-L false positives** with both neighbours. The step's categories hold (all gold `running_text`, no gold
  error). Added: 30 of 44 have a fragment line (< 4 characters, or gold `noise`/`numbering`/`quire_marks`) within three
  lines; running-text lines with such a neighbour are flagged 30/361 = 0.083, without 14/1,792 = 0.008 (F18-R.4).
- **25 fresh bar-L false negatives** (seed 20261117): 12 register cells of 2.10.50 (names, "overleden", "1130.", one
  column head), 6 prose cells or amount lines of P045, P046, P059, 3 P036 numeric ledger rows, 2 P001 account lines,
  1 VOC dotted ledger row, 1 wrapped list item (P042). No gold error. Features computed on P036, P001, P045: P036 rows score 1 of 4 under R1 (cap_share 0: digits are not capitals,
  short 0, closed 1.0) – 53 FN on one page; 76 positives (0 negatives) end in an amount followed by `—`/`„` that the C0
  amount pattern misses (F18-R.5).
- **sg_hand recall 1/28**: 27 of the 28 positives are P001 (prose account lines the user relabelled `running_text` →
  `table`; page note: "perhaps table is not the type of a line") (F18-R.6).
- **All 12 bar-M false negatives** (step reading holds) and **15 fresh 2.10.50 false positives** with x/page width:
  all register cells mid-page (x 0.14–0.76); `rule:page_margin` fires mid-page (P025 "Genrit Anthonij" at 0.64). The
  gold is right that they are not margin. Bar M without 2.10.50: P 82/159 = 0.516; without 2.10.50 and 2.10.01: 0.651 –
  fails either way (F18-R.2).
- **Labelling rulings** (PROGRESS 2026-11-05 and 2026-11-17): sensitivity of bar L. Under UA-070 (b) (prose cells
  `running_text`) C0R1 gives P 0.753, R 0.573; without the prose cells P 0.938, R 0.573. Recall fails under every
  convention; precision passes only under the ruling taken (F18-R.1). Column heads (2026-11-05): 12 lines, all `table`,
  8 flagged. `colhead` and `cont` tokens were never written (F18-R.9).
- **`prose`**: on P045–P060 (the 16 pages reviewed after the UA-070 ruling) the user marked every line written as
  sentences whatever its type (running_text 318/334 – the 16 others are P048's preamble with another note –, table 298,
  closing 21, heading 4); on P001–P044 never. One property with partial coverage, not two meanings (F18-R.8).
- **2.10.50 types**: P028 (inv 85) keeps 71 `form:value` lines; P029/P030 (same volume, same column heads) are all
  `table`; dev P077–P080 mostly `form` (F18-R.3).
- **Self-check against the user**: the 14 test self-check lines: the user changed 2 (P045 k35, P047 k54, list → table,
  UA-070) and kept P044 k68 as `list`, the self-check's one "type wrong" (F18-R.12).
- **Region tags (VOC only)**: list 67 of 69 `paragraph`, table 153 of 154 `paragraph`; the provider has no list/table
  tag (F18-R.7).

Verdict on the steps' analyses: sound and line-based; the categories hold. Corrections are counts and one pooled
table, not conclusions.

## Brief quality

| Source | Defect | Answer / template change |
|---|---|---|
| 24a | fixture expected to show C1 flag changes; flags equal | planning probe should have run the fixture (CONTEXT §5 "run every new option once on dev" already says so); no change |
| 24b | "~21 inventories" (18); mode "hand/default everywhere" (F18-P.1 wrong) | a probe claim about a build stage is checked on the real build before it enters a brief; F18-P.1 revised |
| 24b | protect "only new paths differ" cannot hold while LotA writes | already in CONTEXT §5 (name expected changes in other projects); kept |
| 24b | 150-line cap left sg_print 2/7 eligible scans | planning computes eligible counts per stratum under the cap (added to CONTEXT §5) |
| 24c A | how a reader gets `line_id`; `is_known_tag` ignores subtypes; notes rule | the step's choices were right (merge fills ids; subtype check); kept for agent-read gold |
| 24c B | prompt said readers *may* open the image | CONTEXT §5: reader prompts say *must*, and the check script requires a per-page image attestation |
| 24c C | self-check draw, relative image paths, unreviewed-page handling, listing format | briefs give seed and code for draws, path rules and the layout of every written file |
| 24d dev | denominators of the type table; top/bottom/interlinear outside bar M; case-file formats; test pages with `reviewed == 0` | the step's choices are stated in the report; kept |
| 24d test 1 | no working directory | CONTEXT §5: scripts that read relative paths from a table resolve them against the AS root or the brief names the cwd |
| 24d test 2, 4 | `colhead`, `cont` never written | rulings made during a user review go into the check page's instruction block (rebuilt, same storage key) as fixed tokens, and the ruling records from which page they apply; the 4 "cont" matches are the word "continues" (no token) |
| 24d test 3 | `prose` beyond P045–P048 | one property applied from P045 on (F18-R.8); not inconsistent in meaning |
| 24d test 5, 6 | FN category for amount fragments; "20 most frequent changes" grain | accepted as the step handled them |

## Updates applied

- [x] Phase report `reports/phase-18-page-element-types.md`: *Summary*, *Review* and *Follow-ups* written; every finding
      is `confirmed`, `revised` or `withdrawn`; status set to *final*
- [x] `reports/index.md`: the phase's findings appended (with Edit), and its phase row updated
- [x] `make reports` runs without warnings; links checked in `reports/_build/`
- [x] Review-fixes brief written (`step-24r-review-fixes.md`)
- [x] `docs/shared_library/changelog.md` complete for this phase (24r adds the `bulk_tagging` line)
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] sibling `STATUS.md` notes: none needed (no sibling import affected)
- [x] Release recommendation for the user: **v0.23.0** (minor: new public API, additive), after 24r; contents in
      `STATUS.md` and the hand-off
