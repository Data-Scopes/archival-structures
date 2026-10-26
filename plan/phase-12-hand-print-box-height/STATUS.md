# Phase 12 status

> Overwrite at the end of every session. **40 lines max**; history belongs in `PROGRESS.md`.

- **Updated:** 2026-10-26, by the UA-025/UA-030 follow-up session (Opus 5.5)
- **State:** all steps reviewed. The user ran the full run (UA-024; on the drive, not verified here, built from 0.14.1 plus
  uncommitted changes) and published the pilot: `shared-data/*/text_layer_v4/` = `15x-a` (UA-025, UA-030 (a), P23).
  UA-029 (a): rule kept (P22). Left: the P21 pin step (as 15y), once the full run's files are staged locally
- **Released in:** v0.15.0

## Steps

| Step | State | Note |
|---|---|---|
| 15u | done, reviewed | passed 60/60 (F12-15u.1) |
| 15v | done, reviewed | v4 build; crosswalk unusable (F12-15v.5) |
| 15w | done, reviewed | `15w-a` (198) |
| 15wr | done, reviewed (accept) | `15wr-a`; split gate (P16), bar P17 passed 19/20; F12-R.8–R.10 |
| 15x | done, reviewed (accept) | `15x-a`; A 59/60, B 0.9927, C 19/0; F12-15x.1, .2 confirmed, .3, .4 revised; F12-R.11–R.15 |

## Next action

The user: release v0.15.0 and stage the full run's small files under `data/text_layer_v4_full/`. Then Opus plans
step 15y (P21 pins; P23: a new layer version or the full run replacing the pilot) and the throughput model.

## Blockers and open questions for the user

- Staging of the full run's `mode/`, `status.tsv`, `manifest.json`, logs and `audit.json` (blocks 15y).
- UA-031: plan phase 11 now, or 15y first.

## Facts later steps need

- v4 dev reference: `data/text_layer_v2_dev/15x-a` (196 + 2; protected, like `15wr-a`). Gated 53 (25 *nouvelles*, 27
  *plakkaten*, 9486; `step-15wr/gated.txt`), split 32 (31 *gedrukte resoluties*, 3463); `step-15wr/gate.tsv`.
- `BOX_HEIGHT_V4` cut 87.75; `SPLIT_GATE_V4` min_share 0.35; `sg_1.01.02_v4.mode_overrides` print `drop_cap_lines=2`.
- Tests: AS 1,251 collected, 1,240 passed, 11 skipped; LotA 381 passed, 1 xfailed (review re-run).
- Audit on `15x-a`: 3,592 pass, 33 reported (12118 0.9496, F12-R.14), 0 judged.
- Suppressed cuts 179,412 (`step-15x/suppressed.tsv`); lost decision/*Dat* starts 279 of 38,176 (`barB_lost.tsv`).
- P21 step input: every `print` volume of 1.01.02 outside the printed series in the full run's `mode/` (3463 here).
