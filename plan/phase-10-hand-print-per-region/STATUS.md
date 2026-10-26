# Phase 10 status

> Overwrite at the end of every session. **40 lines max**; history belongs in `PROGRESS.md`.

- **Updated:** 2026-10-23, by the phase review (Opus 5.5)
- **State:** **done, reviewed: negative result** (`REVIEW.md`: accept as a negative result; no review-fixes step)
- **Released in:** not yet. Optional v0.15.0 (minor: new, unused module `analysis/hand_print.py`); no sibling needs it

## Steps

| Step | State | Note |
|---|---|---|
| 15p | done (negative) | density: silver print 72.0%, hand 84.7% (< 90%) |
| 15q | done (negative) | `hrel`: weighted hand 0.768, print 0.893 (< 0.90); 455/456 read `hand` |
| 15r | not run | P10; brief kept, reusable |
| 15s | not run | depends on 15r; brief kept, reusable |

Void briefs (don't run): the old `step-15q-region-classifier.md`, `15r-mode-aware-*`, `15s-mode-aware-*`, `15t-*`.

## Next action

None in this phase. UA-022 = (a) (resolved 2026-10-23): phase 12 (`plan/phase-12-hand-print-box-height/`) tests a
box-height rule once. Phase 11 is independent.

## Blockers and open questions for the user

- UA-007 (pagexml-tools 0.9.1); nothing here needs it.

## Facts later steps need

- Frozen 15q thresholds `step-15q/thresholds.json` (sha256 `72cc1c04…49f3`): t_lo 1.529, t_hi 1.534. Spent.
- Region-feature caches (h_med, pitch_med, hrel, n_chars per region): `data/hand_print_geom/15q-a/{1.01.02,1.10.94}/`
  for 14 dev, 455/456, 60 sample, 12 staged volumes. Harness: `step-15q/validate.py` (prepass/fit/evaluate).
- Box height per volume (exploratory, F10-R.3): `reports/data/phase-10-hand-print-per-region/review/h_by_volume.tsv`.
- The 60-volume sample (`planning-15q/silver_sample.tsv`) is spent; a new test draws a new one (F10-R.5).
- 1.10.94 needs a fixed `print` mode in any v4 preset (P11, F10-15q.4).
- Staged tars (12 + 60), read only: `shared-data/NL-HaNA_1.01.02/pagexml/`. `plan/CONTEXT.md` is 174 lines
  (budget ~150): the next planning session prunes §2.
- Test count: AS 1,220 collected (1,209 passed, 11 skipped); LotA 368 passed, 1 xfailed.
