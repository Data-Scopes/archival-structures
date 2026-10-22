# Phase 09 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-22, by the phase review (Opus 5.5)
- **State:** done (reviewed: accept with follow-ups; [REVIEW](REVIEW.md))
- **Released in:** v0.14.0

## Steps

| Step | State | Note |
|---|---|---|
| 15j | done | Row-tolerant line order; swaps 600 → 13 (455), 1,382 → 14 (456); `15j-a` |
| 15k | done | Gap rule (188), print bottom + years → `other`; crosswalk `role` (513 → last half); no REPUBLIC default; `15k-a` |
| 15l | done | `, `/`,,` and 455 `J` marks adopted; `g_upper_right` not adopted (negative result); `15l-a` |
| 15m | done | v3 presets; `15m-a` = `15l-a`; audit 178/182; 116 rows 0.526/0.560; run kit; UA-015 |

## Next action

The user commits the tree and releases v0.14.0 (`make bump-minor`, then `make release`). Then UA-015
(stage the inputs) before any `text_layer_v3` run; phase 10 planning (Opus, prompt 1 of
`plan/PROMPTS.md`).

## Blockers and open questions for the user

- UA-015: stage the full-run inputs (the command for the 8 hand tars was fixed and tested in review).
- UA-007: pagexml-tools 0.9.1 is not on PyPI yet.

## Facts later steps need

- Reference layer: `data/text_layer_v2_dev/15m-a/{hand,print}` (v3 presets); `15ir-a` is the phase start.
- The follow-ups are in `plan/CONTEXT.md` §4; review findings F09-R.1–R.7 are in the report.
