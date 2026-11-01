# Phase 15 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-30, by the session that handled UA-055 (Opus 5.5)
- **State:** done; reviewed (accept with follow-ups; [REVIEW](REVIEW.md)); report final
- **Released in:** v0.20.0

## Steps

| Step | State | Note |
|---|---|---|
| 21a | done | 29 `index` sides, no reserves; 1,251 lines, 595 entries, 851 locators, 21 embedded; 90 targets (63 test usable); hashed |
| 21b | done | `FOLIO_MAP_V2`; planner numbers reproduce; bar F 62/63 passed (v1 the same) |
| 21c | done | selected A0 + L7 (dev F1 0.830); test L P 0.981 passed; L R 0.856, A 0.892, E 0.825 failed |
| 21d | done | `index_links_v2` dev build `data/index_links_dev/21d-a/` (41 volumes); v1 rebuild = `20r-a` |
| review | done | every number re-run and matching; no review-fixes step; docs and F15-21d.5 fixed in review |

## Next action

The user releases v0.20.0 (`make bump-minor`, `make release`, then `make update_install`). Next phase to plan:
phase 16 (VOC, `plan/PROMPTS.md` §1); its staging UA-054 is answered (a).

## Blockers and open questions for the user

- UA-055: done 2026-10-30 (report *After the review: user check*, F15-21a.8, F15-21a.9; T089's locator is an HTR
  misread, 637 read as 697; gold left frozen).
- UA-056: done 2026-10-30 (full run in `data/index_links_full_v2/`; report *After the review*, F15-21d.7, F15-21d.8).
- UA-057: answered (a) on 2026-10-30; v2 is not published.

## Facts later steps need

- Gold spent in this phase: `reports/data/phase-15-index-links-v2/step-21a/` (all 29 local untouched front-index
  volumes). An index-links v3 needs new gold, from the drive-only front-index volumes.
- The v3 candidates are in `plan/CONTEXT.md` §4: v1 units plus `marginalia` lines, or A1 with a strict heading rule.
