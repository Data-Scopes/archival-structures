# Phase 04 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-14, by the phase review (Opus)
- **State:** done (reviewed: accept with follow-ups, [REVIEW](REVIEW.md))
- **Released in:** v0.8.0

## Steps

| Step | State | Note |
|---|---|---|
| 10 | done | AS `stream_analysis/decoding/`; equivalence with NLP originals exact |
| 10b | done | NLP decoder shims; `verify_as_decoders.py` 7/7 PASS at review |
| 11 | done | AS `model/scan_id.py`; LotA adapters delegate; LotA 11 + 316 passed |
| 12 | done | AS `analysis/page_numbering.py`, `datasets/globalise_index.py`; 6/6 configs |
| 12b | done | NLP alignment shim; `verify_as_globalise_alignment.py` 6/6 PASS at review |
| review | done | 28 findings confirmed, 3 revised, F04-R.1–3 added; no fixes brief |

## Next action

The user releases v0.8.0 (`make bump-minor`, then `make release`), runs `poetry lock`, and
commits NLP and LotA. Then plan phase 05 (text layer), prompt 1 in `plan/PROMPTS.md`.

## Blockers and open questions for the user

- NLP working tree (uncommitted):
  - `src/startpage/{semi_markov,crf_sequence,bilstm_sequence,globalise_alignment}.py`;
  - `scripts/{train_crf_sequence,train_multitask_crf,train_semi_markov_duration}.py`;
  - new `scripts/verify_as_{decoders,globalise_alignment}.py`, `output/as-phase-04-originals/`;
  - plus the phase-03 files.
- LotA: `archival_language/ead/` is untracked in git, including the edited `adapters.py`
  (F04-R.2).
- `poetry.lock` not regenerated after `scipy>=1.10` joined the core dependencies.

## Facts later steps need

- AS 890 passed; LotA 316 (`--ignore=tests/test_derive_voc.py`); NLP `tests/test_evaluation.py
  tests/test_semi_markov.py` (in that order) 21 passed. Phase-05 notes: `PLAN.md` *Hand-off*.
