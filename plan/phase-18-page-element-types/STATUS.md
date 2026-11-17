# Phase 18 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-11-17, by Sonnet 5.5 (step 24r); release fields by the phase-19 planner (Opus 5.5)
- **State:** reviewed, accept with follow-ups ([REVIEW](REVIEW.md)); 24r done; bars L and M both fail (negative results)
- **Released in:** v0.23.0 (24a and 24r)

## Steps

| Step | State | Note |
|---|---|---|
| 24a Vocabulary v2, `listlike` port, `page_elements`, archive presets | done, reviewed | changelog `### Step 24a` |
| 24b Stage, build and sample | done, reviewed | 18 inventories, 0 failures |
| 24c Agent pre-labels, dev gold, check page; UA-063 | done, reviewed | test gold 286b3ad0...fb41 |
| 24d Measure | done, reviewed | L P .950 R .575 fail; M P .259 R .872 fail |
| 24r Review fixes | done | `bulk_tagging` placement, VOC-only region-tag tables (`step-24r/`), `listlike` docstring; changelog `### Step 24r` |

## Next action

- None in phase 18. Phase 19 (page elements v2, UA-073 (a)) is planned: `plan/phase-19-page-elements-v2/`.

## Blockers and open questions for the user

- None.

## Facts later steps need

- AS root `/Users/Shared/claude_projects/data_scopes/archival-structures`; suite 1,460 passed, 11 skipped after 24r
  (the provenance test needs `GIT_CONFIG_*` safe.directory when git reports dubious ownership).
- Do not edit `step-24d/eval_elements.py` or its outputs; `test_done.json` exists, the test stage refuses to re-run.
- Test gold is spent for `listlike` cells and the layer margin call. Phase-19 bars need fresh gold.
- VOC-only region-tag tables: `reports/data/phase-18-page-element-types/step-24r/region_tags_voc_{test,dev}.tsv`
  (supersede the pooled 24d `region_tags_voc.tsv`).
- F18-R.7 and F18-R.10 fixes are in place; their status change belongs to the user or next review.
