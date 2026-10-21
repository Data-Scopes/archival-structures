# Phase 07 status

> Overwrite every session. **40 lines max**.
- **Updated:** 2026-10-21, by the phase review (Opus)
- **State:** reviewed: accept with follow-ups ([REVIEW](REVIEW.md)). 
- **Released in:** v0.11.0 (15ir in v0.12.0)

## Steps
| Step | State | Note |
|---|---|---|
| 15fp–15i2 | done, reviewed | pilot `15i-b`, published as `shared-data/…/text_layer_v2/` |
| 15ir | **next** (Sonnet, prompt 2) | [brief](step-15ir-review-fixes.md): F07-R.1–R.5 + tests/docs; dev layer `15ir-a`, no publication |

## Next action
1. The user commits the phase-07 tree and releases v0.11.0 (before 15ir changes the code).
2. Step 15ir (Sonnet), brief `step-15ir-review-fixes.md`.
3. Phase 08 planning (Opus, prompt 1), reading `plan/CONTEXT.md`.

## Blockers and open questions for the user
- UA-013: keep `text_layer_v2` as is (recommended) or publish a fixed pilot after 15ir.
- UA-007: pagexml-tools 0.9.1 not released; AS stays on 0.9.0.
- The 116-row sign-off waits for LotA's re-alignment (D19); predicted 0.526, bound 0.560.

## Facts later steps need
- **Published, never rebuild in place:** `shared-data/NL-HaNA_1.01.02/text_layer_v2/` (hand, 14 inv.),
  `shared-data/NL-HaNA_1.10.94/text_layer_v2/` (print 455, 456) = `data/text_layer_v2_dev/15i-b/` + `publication`.
- Review re-run: AS 1,101 passed, NLP 56, LotA 367 (+1 xfail), v1 157/157, audit 176/182, 116 rows unchanged.
- Baselines for 15ir (on `15i-b`): 455 regions with > 50 px backward jumps 546 (416 `split_at_column`,
  127 `unchanged`); units with jumps 335 (455) / 160 (456); `,` merges 11182 1,305, 3352 336, 11281 170;
  `n_id_collisions` 58 / 32; blocked counters 1,322 / 62 (455), 673 / 113 (456); `rule:pn_line` 10,327,
  `_offchain` 1,792; whole date lines split by rule 1: 19.
- `audit.py --layer <base>` needs `<base>/{hand,print}/*_text_layer_v2`.
- Standing context for planning: `plan/CONTEXT.md`.
