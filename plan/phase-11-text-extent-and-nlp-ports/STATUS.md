# Phase 11 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-27, by the phase review (Opus)
- **State:** done (reviewed: accept with follow-ups, [REVIEW](REVIEW.md))
- **Released in:** v0.16.0

## Steps

| Step | State | Note |
|---|---|---|
| 18a | done | `text_extent` `region_types`, `margin_gaps`, `text_extent_table`; NLP report/84 reproduced exactly (re-run by review) |
| 18b | done | C10 in `subsequence_detection.py`; AS = NLP (41,810/41,810); TextTiling edge starts documented (F11-R.2) |
| 18c | done | `analysis/separator_sheets.py`; AS = NLP (169 sheets); review fix: sorted sheet order (F11-R.3) |
| 18d | done | `leakage_and_splits.md`, granularity hypothesis, stream_analysis bullets; review doc fixes |

## Next action

Released as v0.16.0 (2026-10-27). UA-032 done (NLP imports AS 0.16.0 editable), so the NLP shim step can be planned
(a small addendum to this phase). Candidate findings are in `docs/findings.md` (2026-10-27). Phase 12's 15y (P21 pins)
runs once the full v4 run's files are staged (`scripts/stage_text_layer_v4_full.py`).

## Blockers and open questions for the user

- None (UA-032, UA-033 resolved 2026-10-27).

## Facts later steps need

- NLP checks run with `cd NLP && PYTHONNOUSERSITE=1 PYTHONPATH=<AS> venv/bin/python …`; NLP `page_extents*.csv`
  include 1,076 `n_lines == 0` rows.
