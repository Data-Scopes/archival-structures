# Phase 13 status

> Overwrite at the end of every session. **40 lines max**; history in `PROGRESS.md`.

- **Updated:** 2026-10-28, by the final phase review (Opus)
- **State:** **phase done**, reviewed (accept). 
- **Released in:** v0.18.0
- Suite 1,368 collected (1,357 passed, 11 skipped). Report final; findings in `reports/index.md`.

## Steps
| Step | State | Note |
|---|---|---|
| 19a–19d | done, reviewed | detector passed (D1, D2); split (B1) and E5 failed |
| 19f, 19g | done, reviewed | reorder failed (B2 35/39); entry rule v2 passed (fresh gold) |
| 19e | done, reviewed | v5 preset, dev build `19e-a` (231), run kit `--version v5`; run-kit audit fixed in review (F13-R.16) |

## Next action
None in this phase. The user releases v0.18.0 (`make bump-minor`, then `make release`) and answers UA-045/UA-046.
Next phase to plan: see `plan/CONTEXT.md` §4 (detector false-positive guard, column order v3, or non-phase-13 items).

## Blockers and open questions for the user
- UA-045: full v5 run of 1.01.02 on the drive (default: nothing runs). Not blocked by the review.
- UA-046: publish `19e-a` as a v5 pilot in shared-data (default: nothing published).

## Facts later steps need
- v5 = `sg_1.01.02_v5`: v4 + hand override (`index_sides` V5, `index_entry` V5B), no column reorder (UA-043 a).
  Run: `scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 ...`; final audit = `step-19e/audit.py`.
- `19e-a` hand units equal `19g-a`/`19g-z`; print volumes equal v4; audit 0 judged failures; 8,873 flagged sides.
- Known false positives: 54 non-index flagged sides in 18 volumes, 0.18% of hand units (F13-R.13); gap fill 17/20 index
  pages in a sample outside GT and gold (F13-R.14).
- All phase-13 gold (19a sides and entries, 19g fresh sides, B1/B2 regions) is spent.
- zsh: quote `'{workers}'` before a redirect; rebuilds need `--force`. No `git stash`/`checkout`/`reset`.
