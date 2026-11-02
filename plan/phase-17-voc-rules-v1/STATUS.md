# Phase 17 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`. Parallel steps: edit only your own row and add facts; don't rewrite others' lines.

- **Updated:** 2026-11-02, by the 23c review addendum (Opus 5.5)
- **State:** reviewed, closed (accept with follow-ups, [REVIEW](REVIEW.md) incl. the 23c addendum); no bar passed
- **Released in:** v0.22.0

## Steps

| Step | State | Note |
|---|---|---|
| 23a Fresh gold | done, reviewed | agent gold and the user's folio gold frozen; all spent |
| 23b `¬` break set (`voc_1.04.02_v6`) | done, reviewed | join bar fails: 56/60 (reviewer 55/60); layer kept |
| 23c VOC folio map (bar F) | done, reviewed (addendum) | fails: SEL 79/109 = 0.725, BASE 81/109; McNemar p 0.5; no constant |
| 23d One-column TOC detector (bar D) | done, reviewed | fails: P 0.583, R 0.276 |
| 23e L9 + monotone filter (bars L, E) | done, reviewed | fail: L P 0.556, R 0.400; E 0.529 (25 gold folios) |

## Next action

- The user releases v0.22.0 (`make bump-minor`, `make release`).
- An Opus session plans the next phase (`plan/PROMPTS.md` §1) from `plan/CONTEXT.md` §4; trim CONTEXT to its budget then
  (208 lines against ~150).

## Blockers and open questions for the user

- None.

## Facts later steps need

- 23c SEL (`step-23c/select.json`): `FolioMapParams(pick_by_chain=True, tie_keep="first", clean_tol=3.0)`; not adopted.
- Layers on T7_Data: fresh `data/text_layer_voc_23a/…_v5/` (40 volumes), v6 `data/text_layer_voc_23b/…_v6/` (567).
- Protect: `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT`.
- Suite: 1,432 passed, 11 skipped.
