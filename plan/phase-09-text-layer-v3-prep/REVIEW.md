# Phase 09 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-22. The same session planned the phase and ran the four steps as
  Sonnet subagents (prompt 2), one after the other, checking each before starting the next.
- **Verdict:** **accept with follow-ups.** No code defects. One doc defect (the UA-015 staging
  command) was fixed in review. No review-fixes step is needed.

## Checklist

- [x] **Acceptance re-run:** see the table below. Every number I re-ran matches the step's log.
- [x] **Suites:** AS `.venv/bin/python -m pytest -q -p no:cacheprovider` gives 1,205 passed (97
  subtests). NLP `venv/bin/python -m pytest -q -p no:cacheprovider tests` gives 66 passed. LotA got
  a docs section and a `STATUS.md` note but no code change, so its suite was not run.
- [x] **Error analysis:** each step read individual cases with seeds and wrote a verdict file
  (`step-15j/ea_verdicts.tsv`, `step-15k/pn_verdicts.tsv`, `step-15l/merges_verdicts.tsv`,
  `step-15m/units_text_sample.txt`). Each is written up in the report.
- [x] **Report:** final; findings appended to `reports/index.md`; `make reports` clean.
- [x] **Changelog:** `## Unreleased` has entries for 15j–15m that match the diff. They cover the new
  `LayoutParams` / `TypeParams` / `LayerPreset` fields, `sort_lines_by_row`, `sort_lines`,
  `read_republic_roles`, the kwargs, the columns, and the removed `units.REPUBLIC_PARAGRAPHS_DIR`.
  Each entry lists "affected sibling imports: none". I re-checked that by grep over LotA and NLP
  `*.py` for the changed names: 0 hits.
- [x] **Ported code:** none in this phase.
- [x] **No archive-specific knowledge in core:**
  - `grep -rn language-of-the-archive archival_structures` → 0 hits; the REPUBLIC path is now
    `--republic-dir` only.
  - SG choices (bands, `J` for 455, the lead marks) live in `presets.py`, each with its bar outcome
    in a comment.
- [x] **Sibling copies:** none planned.
- [x] **Leakage:** no model is fitted. The bars are adoption decisions read on samples, fixed
  before reading. One circularity is noted in finding 3.
- [x] **Brief defects:** answered below; `plan/CONTEXT.md` §5 updated.
- [x] **Phase-specific:**
  - every bar has a verdict file with a verdict per row, and the presets match the bars
    (`presets.py` comments vs `pn_verdicts.tsv`, `merges_verdicts.tsv`);
  - `15m-a` equals `15l-a` (compare: 0 tables differ);
  - v1 gives 157 / 157, and the `LEGACY_15FP` tests pass unchanged;
  - `git diff --stat -- archival_structures/text_layer/join.py` is empty;
  - the run kit was smoke-tested with `--limit 1` (15m), and I built the hand tar path from a tar
    myself (finding 2).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| all | AS suite / NLP suite | 1,205 / 66 | 1,205 passed / 66 passed | yes |
| all | v1 | 157 / 157 | `09R-v1check`: `TOTAL 157 / 157` | yes |
| 15j | same-row swaps in sorted print classes; jump regions; `lines_resorted` | 12 / 14; 0; 182 / 7 | `order.py --layer 15j-a` and `15m-a`: 1+5+6 = 12, 14; 0; 182 / 7 | yes |
| 15k | `rule:pn_line_gap`; print `other`; years | 188; 367; 37 | `pn_offchain.py --layer 15k-a`: 184+4; 366+1; 34+3 | yes |
| 15k | crosswalk: rows that change | 513 `split_original\|end` | merge on `republic_line_id` (`15ir-a` vs `15m-a`): 135 + 378, no other row | yes (15m's "plus 30 `unmatched` rows" is not reproduced, finding 4) |
| 15l | 455 `J`+lower; hand comma right sides; `g_upper_right` | 0; 0; 0 | `merges.py --layer 15l-a`: 455 no `J+lower`; hand `comma_sp`/`comma2` absent; no `g_upper_right` kind | yes |
| 15m | `15l-a` vs `15m-a` | 0 differ | `compare.py`: hand 237, print 51 equal, 0 differ | yes |
| 15m | audit | 178 / 182 | `step-15m/audit.py --layer 15m-a`: 178 pass, 4 fail (3351 0.9415, 3823 0.9414, 455 0.9477, 456 0.932) | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (fixed in review) | `docs/USER_ACTIONS.md` UA-015 item 1 | The staging command `cp -Rn data/text_layer_v2_dev/15i-src/…` would copy the 8 hand sources as symlinks into the user's `data/PageXML` tree, and as flat directories, while `shared-data` holds `<inv>/page/*.xml` tars | Replaced by a `tar -czf … -s ",^NL-HaNA_1.01.02_<inv>,<inv>/page,"` loop. Tested on 11182: a build from the tar equals `15m-a` in every table except the archive `member` path in `scans/` (F09-R.6) |
| 2 | observation | `docs/USER_ACTIONS.md` UA-015 options | Option (b) "the inventories LotA aligns: 455, 456 and the hand 14" was the pilot set again, which gives nothing beyond `15m-a` | Options rewritten: all / a listed subset / pilot only (a publication of `15m-a`) / later |
| 3 | observation | `reports/…/step-15l`, audit | The 455 lower-case share rising from 0.892 to 0.948 is mechanical: the audit share counts exactly the right sides the `J` rule removes. The evidence for the rule is the reading (30/30, 13/13) and my count over all 2,172 strips (F09-R.4), not the share | Noted in the report *Review* (F09-R.7) |
| 4 | observation | report *Step 15m*, Part 3 table, crosswalk row | "plus 30 `unmatched` rows whose `line_id` also changed" is not reproduced: joining the two tables on `republic_line_id` gives only the 513 `end` rows | Cell annotated in the report (F09-R.2) |
| 5 | debt | `step-15k/pn_verdicts.tsv`, `presets.py` (`SG_HAND_TYPES_PARAMS`) | Hand-bottom: all 12 decidable rows are not page numbers. The bar missed only because 8 rows sit on pages with no chain number nearby, so their text alone can't decide them. About 890 hand-bottom off-chain numbers keep `page_number` | Follow-up: verdicts from crops for those rows, then re-apply the bar (F09-R.5) |
| 6 | debt | `types.py` `_pn_candidate` | The 188 gap rows are real page numbers that lost the candidate slot to another pn-shaped band line ("4767", "2716."). Choosing the candidate that fits the chain would put them in the chain itself | Follow-up |
| 7 | debt | `join_v2.py` (lead marks) | F09-15l.5: the repeated break mark is also read as a capital ("Secreta\|Pris", "aenge\|Ystelt"): 10/40 hand, 16/40 print of the upper-case merges. These are why `g_upper_right` fails its bar | Follow-up (design a mark rule for capitals; then re-test the guard) |
| 8 | observation | `page_structure.py` `merge_regions` docstring; `layout.py` import line | The docstring's first sentence still describes the baseline sort (the row sort is in an added sentence); one import line is > 100 chars | None |

## Error analysis read by the reviewer

- **116 rows, rows 23 and 188** (the two with a changed match):
  - row 23: the unit text in `455_0498` reads "Schout by Nacht vanJder Dussen" in `15ir-a` and "vander Dussen" in `15m-a`. The labelled print OCR artefact is gone;
  - row 188: the print tokens "torsijne narichtinge. daed in behoorlijcke forma" are now found `exact` (they were `none`), in reading order.

  Both read as `fixed`. That would give a share of 63/116 = 0.543 and a bound of 0.569 (F09-R.3). The
  verdict file is not edited: LotA checks after re-alignment (D19).
- **`J` rule precision.** I listed every distinct right-side token of the 2,172 455 strips (on `15k-a`).
  At most 3 could be real J-words ("Jacob", "Jacht", "Jonel"). Others that look like words are marks
  too ("Jaccordeert" = accordeert, "Jantwoordt"). The top tokens are "Jden" 160, "Jde" 105,
  "Jdende" 30. Error rate ≤ 0.2% (F09-R.4).
- **Off-chain verdicts:**
  - I read all 40 hand-band rows of `pn_verdicts.tsv`. The verdicts are consistent with the context
    columns. For example, "( 930 )" on the page before "( 901 )" is `pn` (900 misread), and "4768"
    beside "( 190 )" is `not_pn`;
  - hand-top has 8 `pn`, all parenthesised misreads of this page's number. Keeping it is right.
- **15j remaining swaps:** 4 blocks of `remaining_swaps.txt` read. "1320 )" sits before "Den 22. Maert."
  (page number 15 px above the date on the header row), and "7" before "'s Lands gerechtigheydt." (a
  margin number). This matches the step's categories.
- **Crosswalk:** see finding 4 and the re-run table.
- The steps' own error analyses hold up: seeds given, verdict files present, negative results reported
  as such (F09-15k.2, F09-15l.4).

## Brief quality

| Brief defect (reported by) | Cause | Fixed in |
|---|---|---|
| `$CS` holding the coreshare prefix doesn't word-split in zsh (15j) | Planner wrote a bash idiom | 15k–15m briefs (in review of 15j, before 15k ran); `plan/CONTEXT.md` §5 |
| Year rule on the line text misses "1718" of "Maart 1718" (15k: 33, not 34) | Brief said line text; the pn rule can fire on a segment remainder | Code uses the segment text (F09-15k.6); CONTEXT §5 |
| Planning band (y < page_h/2) vs the code's band (top_n lines): 367, not 366 (15k) | Planning script approximation, stated in the brief | none needed |
| Unit-row changes include one split, not only joins (15k) | Brief assumed joins only | none |
| `order.py` takes `--out`, not `--json`; `rows116.py` needs `rows116_verdicts.tsv` beside it (15m) | Planner didn't run the 15m commands as written | CONTEXT §5 |
| `--estimate` gives no time for SG presets (15m) | No throughput fit in the SG presets | Follow-up: fit after the full run |

## Updates applied

- [x] Phase report: *Summary*, *Review* and *Follow-ups* written; every finding `confirmed` or `revised`; status *final*
- [x] `reports/index.md`: phase-09 findings appended, phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief not needed (the one defect was a doc command, fixed and tested in review)
- [x] `docs/shared_library/changelog.md` complete for this phase
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated; `plan/CONTEXT.md` updated
- [x] phase `STATUS.md` overwritten
- [x] LotA: 15m appended §8 to `docs/from-archival-structures.md` and a note to LotA `STATUS.md`, per LotA's conventions
- [x] Release recommendation: below

## Release

Release **v0.14.0** (minor: new public API, and output changes for the SG presets):
- row-tolerant line order (15j);
- off-chain page-number retype, crosswalk `role`, and no REPUBLIC default path (15k);
- the extended lead marks and the 455 `J` mark (15l);
- the v3 presets (15m).

The changelog's `## Unreleased` becomes `## v0.14.0`. Nothing is published to `shared-data`.
