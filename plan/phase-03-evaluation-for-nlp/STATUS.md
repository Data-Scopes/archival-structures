# Phase 03 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-14, by the phase review (Opus)
- **State:** done (reviewed: accept with follow-ups, [REVIEW](REVIEW.md))
- **Released in:** not yet; recommended **v0.7.0**

## Steps

| Step | State | Note |
|---|---|---|
| 07 | done | AS evaluation package; NLP equivalence 12/12 |
| 07b | done | NLP shim; verify script 16/16 at review |
| 08 | done | splits + validators; 7/7 byte-identical at review |
| 09 | done | baselines; 344/344 cells; pre-phase cells identical (F03-R.2) |
| review | done | `intra_document_swap` provenance fixed + tests; AS 717 passed |

## Next action

The user releases v0.7.0 (`make bump-minor`, then `make release`), then commits NLP. Then plan
phase 04 (decoders), prompt 1 in `plan/PROMPTS.md`.

## Blockers and open questions for the user

- Restore NLP `output/trivial_baseline_metrics.csv` from backup if the old random CI draw
  matters (F03-09.7). The deterministic cells are verified identical.
- NLP's working tree is uncommitted: `src/startpage/{evaluation,splits,long_subsequence_chunk_split}.py`,
  `scripts/run_trivial_baselines.py`, and the new `scripts/verify_as_*.py`.

## Facts later steps need

- AS suite: **717 passed**. NLP: `test_evaluation` + `test_semi_markov` 21 passed;
  `test_voc_text_layer` fails on a `fuzzy_search` import (F03-R.3, environment).
- Phase-04 conventions are in `PLAN.md` *Hand-off*.
