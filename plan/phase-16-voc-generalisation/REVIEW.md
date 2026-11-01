# Phase 16 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-11-01. The planner of this phase also reviewed it: the user asked one session to plan,
  run the step agents and review.
- **Verdict:** accept with follow-ups.
  - One defect was found: 22d's gold loader dropped the starts of scans with more than one gold row. It is fixed in
    22r. The fix changes 22d's numbers by about one point, but not its selection or verdict.
  - The phase's answer stands. On VOC, the SG text-layer build carries over unchanged. The three components with a bar
    (folio map, index-side detector, index links) fail it, even after VOC options chosen on dev.

## Checklist

| Item | Result | Evidence |
|---|---|---|
| Every step's acceptance met and re-run by the reviewer | pass, after 22r | Table below. |
| AS tests | pass | `.venv/bin/python -m pytest -q -p no:cacheprovider`: 1,413 passed, 11 skipped, 92 subtests (before 22r; re-run after 22r: 1,413 passed, 11 skipped). No sibling code touched. |
| Error analysis read individual cases and is written up, one section per step | pass | See the 22a–22d report sections: 22a read 30 page-number segments, 20 Laypa lines, 10 break lines and 10 sides; 22b 70 misses; 22c 135 cases; 22d 75 lines. My own reading is under *Error analysis read by the reviewer*. |
| Phase report final, findings in `reports/index.md`, `make reports` clean | pass | See *Updates applied*. |
| Every public API change is in the changelog | pass | `git diff -- archival_structures`: `VOC_SOURCE_PATTERN`, `VOC_1_04_02_V5SG`, `PRESETS["voc_1.04.02_v5sg"]`; `FolioMapParams.single_foliation`/`single_slope`; `read_volume_inputs(…, number_tags=())`; `LocatorParams.lead_pattern`/`standalone_pattern`, variant L8, `VOC_LOCATOR_PARAMS`. All four are under *Unreleased*, and all are additive. |
| No archive-specific knowledge hardcoded in AS core | pass | VOC values are named constants (`VOC_1_04_02_V5SG`, `VOC_LOCATOR_PARAMS`, `_VOC_PREFIX`, `_VOC_RANGE`). The defaults are SG's and unchanged. |
| Leakage | pass | Gold sha256: `gm_anchors.tsv` `a2b875d9b06e9972ec3e…` and `toc_gold.tsv` `095e7f3198ac3542…`, equal to planning. `select.json` precedes `test_done.json`: 22b 02:25:30 < 02:27:12, 22c 02:22:56 < 02:23:05, 22d 02:48:49 < 02:49:03. No library code reads gold or silver (grep finds no gold path under `archival_structures/`). 2775 is outside every test bar (22c and 22d report it apart). `VOC_TOC_CUE` and L8 were written by the planner from dev TOC text only. |
| SG defaults unchanged | pass | I ran my own check against `git show HEAD:` copies of `folio_map.py` and `index_links.py` on SG 19e-a 3444: `read_volume_inputs` is equal; `build_folio_map` with `FolioMapParams()` and `FOLIO_MAP_V2` gives the same `locate(n)` for n = 0–1,499; `extract_locators` L1–L7 differ on 0 of 9,166 units, with and without `folio_range`. |
| The 22a preset drops every inventory-keyed SG setting; the audit ran unedited | pass | A walk over the preset's 28 fields finds no inventory-like key or value. My audit re-run to scratch equals `step-22a/audit.json` on all 18,996 leaf keys (`n_fail_judged` 0). The protect before/after files are identical (`cmp`). |
| The generalisation table's verdicts follow P11 | pass, with one note | Rows 7–9 copy the steps' verdicts. Row 10 (front-index run) is `runs, no bar`, as P11 allows, but it finds a run in 0 of 22 volumes; F16-22e.2/.3 and F16-R.6 say so. |
| `Brief defects` answered | pass | See *Brief quality*. `plan/templates/STEP.md` gained two rules. |

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 22a | build coverage | 567, `failures=0` | 567 `mode/*.json`, as logged | yes |
| 22a | audit unchanged | `n_fail_judged` 0 (10,069 / 148) | the same, every key equal | yes |
| 22a | protect | identical | `cmp` identical | yes |
| 22b | Bar F, three test cells | 550 / 523 / 591 of 904 | 550 / 523 / 591, my own scorer from library calls | yes |
| 22b | SG inputs unchanged | equal | equal (3444) | yes |
| 22c | replica = `layout/pages` | 0 diffs in 22 volumes | `pred_unchanged` = layer flags, 0 diffs in 9,299 scans | yes |
| 22c | Bar D, test | TP 1 / 53 / 61; TP 24 / 357 / 61 | the same, from `scan_eval.tsv` | yes |
| 22d | Bar E, test | U1 138/411, U2 88/163, C3 274/433 | the same from `links_eval.tsv`; my own leaf check differs on 5 C3 rows: **defect** | no; fixed in 22r: 142/411, 90/163, 279/433 |
| 22d | SG links unchanged | sha256 `30c9319d…` | L1–L7 locators equal (above) | yes |
| 22e | table = steps | no disagreement | rows 7–9 copy the verdicts; row 9's numbers change with 22r | after 22r |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect | `reports/data/phase-16-voc-generalisation/step-22d/eval_links.py:76` | `load_gold()` keeps the last gold row per scan. `toc_gold.tsv` has up to 4 rows for each of 698 scans, and 691 of them disagree on `start`. Starts are lost (dev 953 → 996 starts to cover, test 462 → 478), which deflates E, coverage and chance in every cell. A fixed copy run by the reviewer gives the same dev selection (C3) and C3 test E 279/433 = 0.644. | 22r (fixed; F16-R.1) |
| 2 | debt | the planning brief 22b | The planner assumed every VOC side is `single`. 22a showed 146 volumes as openings. The brief was corrected before 22b ran, with per-side-layout shares added. | done before 22b |
| 3 | debt | 22b brief, *Behaviour* 1 | `single_foliation` cannot change the leaf share: at scan level the pagination map's leaf {front, next} already covers the same scans. The option is harmless and off by default. | kept; F16-R.5 |
| 4 | debt | 22b brief, *Archive questions* | The restart criterion flags 509 of 545 volumes. 5 of the 11 "decreasing" parts named at planning do not decrease in the frozen anchors; that list cannot be reproduced from `gm_anchors.tsv`. | F16-P.3 revised; F16-R.5 |
| 5 | observation | `presets.py` VOC preset | The `¬` break mark is never joined (116,881 left). That is the SG pin, as P2 required ("unchanged"). A VOC break set needs a new layer version. | follow-up |
| 6 | observation | `index_links.py` `_extract_l8` | The L8 range connector set lacks `à`, ` / ` and dots, and "verso" after the range end is not matched (F16-22d.6). These are known patterns for an L8 v2 with a bar on fresh TOC reads. | follow-up |
| 7 | observation | GM gold | Gold errors are about 4–5% of anchors (9 of 70 read misses, plus 3 of the 6 decreasing parts), close to the 5% slack of a 0.95 bar. A future VOC folio bar needs a checked subset (UA-059 is a start). | F16-R.4; follow-up |
| 8 | observation | `step-22c/__pycache__` | A stray bytecode directory. | 22r deleted it |

## Error analysis read by the reviewer

- **22d gold grain.** I recomputed leaf hits for C3 from `toc_gold.tsv` and found 5 test rows where the step's
  `leaf_hit` is 0 and mine is 1:
  - 1120 folio 459 at scan 534 and folio 666 at 805;
  - 1539 folios 232, 459 and 510.

  In each case the next scan (for example `1120_0535`) has two gold rows, the first with START and the second without.
  That is the defect above. The step's error analysis is not otherwise affected: its read cases are non-hits under the
  old gold, and 22r lists those that flip.
- **22b, the worst stratum.** Leaf share for the selected cell is 0.693 in `Dubbel` volumes with openings and 0.676 in
  `Enkel` single-page volumes, but 19/66 = 0.288 in `Dubbel` volumes whose scans are single portrait pages.
  - The GM ratio there is 1.01 scans per folio (median of 71 missive spans), as in `Dubbel` with openings. A single
    page per scan with one scan per number means the GM "folio" is a page number in those volumes.
  - 7529 (7 misses) shows numbers 861 and 863 two scans apart, and a second numbering from scan about 1216 (folio 19 at
    1216, 323 at 1523).
  - 1122, 1132 and 1245 miss folio 1–3 at the volume start, with no or garbled numbers.
  - So the misses there are restarts and volume-start extrapolation, consistent with 22b's classes. But the label
    mismatch is also a numbering-mode signal that the map does not use (F16-R.3).
- **22b method mix.** Printed hits are right 463/597 = 0.776; interpolated 111/257 = 0.432; extrapolated 17/37. This
  matches F16-22b.4 and F16-22b.8: the chain, not the lookup, is the weak point.
- **22c.** I did not re-read FP pages. The step's FP categories come from rules derived from 60 read pages (stated in
  its section), and its FN categories add up (dev 44, test 37).
- **22b decreasing parts.** I read `decreasing_parts.tsv`. Its classes are backed by printed numbers (for example 1341:
  folio 1 at scan 855 after folio 687 at 716, with no number printed on 849–858). The 22b classification holds.

## Brief quality

| Step | Brief defect | Changed |
|---|---|---|
| 22a | 546 GM / 568 total (545 / 567, since 1171 is excluded) | 22e brief corrected before it ran; `plan/PLAN.md` row corrected |
| 22a | `$TMPDIR` unset in the agent shell | `STEP.md`: name the log path |
| 22a | build time 111 min > `--expected-min 90` | none (machine load; the grant held 2 workers) |
| 22b | single-page assumption | brief corrected before the run; `STEP.md`: run every new option once at planning |
| 22b | restart criterion is noise; 5 of the 11 parts not reproducible | `STEP.md` (same rule); F16-P.3 revised |
| 22b | `single_foliation` cannot move the leaf share | `STEP.md` (same rule) |
| 22b | "every test miss up to 30" did not say which 30; `volumes.tsv` columns undefined | none needed: the existing `STEP.md` rule asks briefs to say how to sample |
| 22c | `min_after` 300 cannot be met in 148–220-side volumes | a data fact, now F16-22c.6; CONTEXT §3 |
| 22d | L8 connectors (`à`, dots) and "verso" after the range end | follow-up (L8 v2) |
| 22d | OBP "page's range" undefined | the step's reading accepted |
| 22d | (found at review) gold row grain not stated | `STEP.md`: state each gold file's row grain |
| 22e | no verdict rule for a "reported only" row; `#` column | accepted; CONTEXT §5 already says "reported only" and a count contradict |

## Updates applied

- [x] Phase report: *Summary*, *Review* and *Follow-ups* written; every finding `confirmed`, `revised` or `withdrawn`;
      status *final*
- [x] `reports/index.md`: the phase's findings appended (with Edit), and its phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief written: `step-22r-review-fixes.md`, run by a Sonnet session
- [x] `docs/shared_library/changelog.md` complete for this phase
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] sibling `STATUS.md` notes: none needed (22e appended the NLP note in `docs/from-archival-structures.md`)
- [x] Release recommendation for the user: v0.21.0 (additive: `voc_1.04.02_v5sg`, `single_foliation`/`single_slope`,
      `read_volume_inputs(number_tags=)`, `LocatorParams.lead_pattern`/`standalone_pattern`, L8, `VOC_LOCATOR_PARAMS`)
