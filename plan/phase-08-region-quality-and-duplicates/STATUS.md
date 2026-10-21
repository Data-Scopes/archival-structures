# Phase 08 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-21, by the phase review (Opus)
- **State:** done; reviewed (accept with follow-ups, [REVIEW](REVIEW.md))
- **Released in:** v0.13.0, prepared 2026-10-21 (changelog heading set; the bump and tag are the user's)

## Steps

| Step | State | Note |
|---|---|---|
| 16 | done, reviewed | `analysis/region_quality.py`; 600/600 gold rows, 428,544 regions reproduce; LotA wrappers |
| 17 | done, reviewed | `analysis/duplicate_pages.py`; full SG run byte-identical (48,473 rows); re-shoots 13/13 |
| 17b | done, reviewed | `page_duplication` operator, first-occurrence start labels; 72 corruption tests |

No review-fixes step: the review fixed the docs and comments itself (REVIEW rows 1–3, F08-R.4).

## Next action

The user commits and runs `make bump-minor && make release` (v0.13.0). Then phase 09 (*Text layer v3 preparation*) is planned in a new Opus
session (`plan/PROMPTS.md` §1).

## Blockers and open questions for the user

- none

## Facts later steps need

- AS suite 1,166 tests; LotA 368 passed + 1 xfailed (369).
- LotA `corpus.quality` and `corpus.integrity` import from AS `analysis.region_quality` and
  `analysis.duplicate_pages`. LotA needs the AS working tree, or v0.13.0 or later.
- A default `simulate_inventory` run now writes five cases.
- `plan/CONTEXT.md` is 170 lines (budget ~150). Phase-09 planning should trim §2 when it rewrites
  the text-layer state.
