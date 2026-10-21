# Phase 07 status

> Overwrite every session. **40 lines max**.
- **Updated:** 2026-10-21, by the 15ir review (Opus)
- **State:** phase done and reviewed; 15ir reviewed: **accept** ([REVIEW](REVIEW.md#step-15ir-review-2026-10-21))
- **Released in:** v0.12.0

## Steps
| Step | State | Note |
|---|---|---|
| 15fp-15i2 | done, reviewed | pilot `15i-b`, published as `shared-data/.../text_layer_v2/` (unchanged) |
| 15ir | done, reviewed (accept) | dev layer `15ir-a`; AS 1,123 passed, NLP 56, v1 157/157, hashes unchanged; F07-15ir.1–.10 and F07-R.10–.11 confirmed |

## Next action
1. The user commits 15ir and releases v0.12.0 (`make bump-minor`, `make release`).
2. Phase 08 planning (Opus, prompt 1), reading `plan/CONTEXT.md`; carry the open issues below into it.

## Blockers and open questions for the user
- UA-013 answered (a): `text_layer_v2` stays as published with *Known issues*; fixes go to `text_layer_v3`.
- UA-007: pagexml-tools 0.9.1 not released; AS stays on 0.9.0.
- The 116-row sign-off waits for LotA's re-alignment (D19); predicted 0.526, bound 0.560 (unchanged by 15ir).

## Facts later steps need
- **Published, never rebuild in place:** `shared-data/NL-HaNA_1.01.02/text_layer_v2/`, `shared-data/NL-HaNA_1.10.94/text_layer_v2/`.
- Baselines now on `15ir-a` (old `15i-b` in brackets): units 455 28,005 (28,079), 456 24,710 (25,030); units with a > 50 px jump 77 / 7
  (335 / 160); `n_id_collisions` 0 / 0 (58 / 32); `n_guard_blocked` 769 / 659 (1,322 / 673), `n_column_blocked` 39 / 107 (62 / 113);
  `rule:pn_line` 8,826 (10,327), `_offchain` 3,293 (1,792); audit 178/182 (11182, 11281 now pass); hand join: 1,856 commas stripped.
- Open issues from 15ir: the 1,501 `_offchain` segments keep `line_type` page_number (18 of 20 read are not page numbers); 65 hand
  merges with `, ` + letter and 5 `,,` remain; baseline-y sort swaps same-row fragments (+49 / +564 pairs); `unchanged` regions
  (130 in 455) keep XML order, 81 units in 455 still jump; `g_upper_right` guard (needs `join.py`); `claimable` built in
  quadratic time (455 units stage 4.0 → 10.0 s, F07-R.11). All in `plan/CONTEXT.md` §4.
- `audit.py --layer <base>` needs `<base>/{hand,print}/*_text_layer_v2`. Standing context: `plan/CONTEXT.md`.
- Shell: unquoted heredocs expand backticks; write Python with backticks via a quoted heredoc (`<<'E'`).
