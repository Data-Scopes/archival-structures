# Phase 12 status

> Overwrite at the end of every session. **40 lines max**; history belongs in `PROGRESS.md`.

- **Updated:** 2026-10-28, by the Opus review of 15yr (accept; `REVIEW.md` § Step 15yr review)
- **State:** **phase done.** 15u–15z and 15yr done and reviewed; the drive rebuild of the 16 pins is checked; the pilot in
  shared-data is `15y-b`
- **Released in:** v0.15.0 (15q–15x); v0.15.1 (`types` fix); v0.17.0 (15y, 15z, 15yr)

## Steps

| Step | State | Note |
|---|---|---|
| 15u-15x | done, reviewed | see PROGRESS.md |
| 15y | done, reviewed 2026-10-27 | F12-15y.1, .2 confirmed; .3-.5 revised |
| 15z | done, reviewed 2026-10-27 | F12-15z.1-.3, .5, .7-.10 confirmed; .4, .6, .11 revised |
| 15yr | done, reviewed 2026-10-28 | F12-15yr.1-.3, .6 confirmed; .4, .5 revised (`done_inventories` fixed in review) |

## Next action

1. The user's release v0.17.0 (15y, 15z, 15yr).
2. Optional: restage the drive layer with the fixed stager (removes the 24 stale split files; command in the 15yr review chat).
3. Plan the next text-layer phase: register and index volumes (F12-R.21, F12-R.22; `plan/CONTEXT.md` §4).

## Blockers and open questions for the user

- None from this phase. UA-007 is older and unchanged.

## Facts later steps need

- Drive layer = pinned: 1.01.02 has 266 `print` and 8,856 `hand`. The staged `data/text_layer_v4_full` holds the 16 pinned volumes,
  the six parse-error controls, `audit.json` (old key format: by inventory) and `wbc/`, plus 24 stale `split_model`/`split_stats`
  files of 12 hand pins (F12-15z.10) until the next staging.
- `audit.json` keys since 15yr: `<archive>/<inv>`; built = every stage seen in the sub-layer has a last row `done`.
  `after-15z-rebuild/check.py` reads the old format.
- Pins: `presets.MODE_PINS_V4` (16). References: `15y-b` (= the published pilot), `15z-b` (the 37 read volumes with pins), `15yr-a`
  (`15y-b` clone, 3463 rebuilt by the 15yr code).
- v4 extract `seconds` time the whole call since 15yr; older rows leave out the pre-pass (factor 1.817 for 1.01.02, 1.321 for 1.10.94).
- A v2 stage blocks an inventory whose upstream chain has a last row not `done`; builds reuse recorded sizes; the run script no
  longer calls `--estimate`. The pooled extract fit runs about 12% high on print-heavy subsets (F12-15yr.4).
- Tests after the review: AS 1,334 collected, 1,323 passed, 11 skipped; LotA 381 passed, 1 xfailed (2026-10-27; no sibling change since).
