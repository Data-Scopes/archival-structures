# Phase 16 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in `PROGRESS.md`.

- **Updated:** 2026-11-01, by the phase-16 review session after UA-059/UA-060 (Opus 5.5)
- **State:** done and reviewed: accept with follow-ups ([REVIEW](REVIEW.md)); report final
- **Released in:** v0.21.0

## Steps

| Step | State | Note |
|---|---|---|
| 22a VOC text layer, SG v5 unchanged | done | 567 volumes, `failures=0`, audit 0 judged; carries over |
| 22b Folio map on VOC (GM gold) | done | bar F fails: 0.608 / 0.579 / 0.654 (selected V0, off, TL) |
| 22c Index-side detector on VOC TOCs | done | bar D fails: P 0.067, R 0.393 at best; no VOC constants added |
| 22d Index links on VOC TOCs | done | bar E fails: C3 (L8) 279/433 = 0.644 after 22r |
| 22e Generalisation table, NLP and GT notes | done | 1 unchanged, 3 fail, 6 run with no bar |
| 22r Review fix: 22d gold grain | done | `load_gold()` takes the max over a scan's rows; selection and verdict unchanged |

## Next action

None for phase 16. Phase 17 (VOC rules v1, UA-060 (a)) is seeded: an Opus session plans it from
`plan/phase-17-voc-rules-v1/PLAN.md`.

## Blockers and open questions for the user

- None. UA-059 done (GM gold wrong in 7 of 9; F16-R.7, F16-R.8); UA-060 answered (a).

## Facts later phases need

- VOC layer: `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/` (link to T7_Data; 567 volumes).
- Gold: `reports/data/phase-16-voc-generalisation/planning/` (`gm_anchors.tsv`, `toc_gold.tsv`). Both test splits are
  now spent; 2775 is planner-seen. User-checked GM anchors: `step-22b/check_result.tsv`. `toc_gold.tsv` has up to 4 rows per scan: collapse with max before scoring.
- Tests after the phase: 1,413 passed, 11 skipped.
