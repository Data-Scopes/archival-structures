# Phase 02 status

- **Updated:** 2026-10-14, by the phase review (Opus 5.5)
- **State:** done (reviewed: accept with follow-ups; no review-fixes step)
- **Released in:** not yet. Recommended: **v0.6.0** (`make bump-minor`, then a release target)

## Steps

| Step | State | Note |
|---|---|---|
| 03b | done | `ead_views` `archive_id` arg; LotA uses AS node ids |
| 03d | done | `reports/` + `make reports` |
| 03c | done | baselines in LotA `output/baselines/{pre,post}-as-step-2/`, `post-quote-rule/` |
| 04 | done | `DigitisationSummary.has_evidence` |
| 05 | done | `parsers/ead_scope_notes`; `ScopeReference.target_archive` |
| 06 | done | `ead_fields.split_unittitle_dates`; `lb_adjusted` retired |
| review | done | [`REVIEW.md`](REVIEW.md); report final; 46 findings in `reports/index.md` |

## Next action

The user releases AS v0.6.0 and records it here and in `plan/PLAN.md`. Then: phase 03 planning
(Opus, prompt 1 in `plan/PROMPTS.md`).

## Blockers and open questions for the user

- **Release.** v0.6.0 contains steps 3b, 4, the 3c follow-up (quote rule), 5 and 6. The
  changelog has them under *Unreleased (after v0.5.0)*. The review added the missing `v0.5.0`
  heading (F02-R.9).
- **Decision, when convenient (debt 3).** Should title `prose` drop separators orphaned by a
  cut date? 45% of SG titles end in `,`/`;`/`:` (F02-R.4). This would change LotA output.
- **Candidates for `docs/findings.md`** (not edited; listed in the review's final message).

## Facts later steps need

- The deferred debts are in `REVIEW.md`. Debt 1 (P6 under prefixed ids, F02-R.6) must be fixed
  before LotA parses scope notes for an archive whose id is not a bare NA code, e.g.
  `NL-HaNA_2.10.01`.
- Test baselines: AS 618 passed; LotA 316 passed (`--ignore=tests/test_derive_voc.py`).
