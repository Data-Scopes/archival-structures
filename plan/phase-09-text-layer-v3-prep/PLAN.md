# Phase 09: Text layer v3 preparation (phase-07 follow-ups)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **Status: done; reviewed** 2026-10-22 (accept with follow-ups; [REVIEW](REVIEW.md)). Planned 2026-10-21 (Opus). Seeded by this planning session from `plan/CONTEXT.md` §4
> (no seed directory existed).

## Goal

The phase-07 follow-ups are fixed in `text_layer/`, so the code is ready for the full SG run
`text_layer_v3`. The fixes cover print reading order, page-number types, bottom-band years, the
crosswalk role, the REPUBLIC path, continuation marks and the upper-case guard. A from-scratch dev
layer on the 16 pilot inventories (`15m-a`) shows each fix with numbers and read cases. A run kit
(v3 presets, commands, a UA listing what to stage) lets the user start the full run. Nothing is
published.

## Owns

- `archival_structures/analysis/page_structure.py` (line sort, `LayoutParams`),
  `text_layer/layout.py`, `types.py`, `units.py`, `join_v2.py`, `presets.py`, `build.py`
  (`stage_units`, `stage_join`, CLI), `pyproject.toml` / `provenance.py` (pin, 15m only);
- tests `tests/test_analysis_page_structure.py`, `test_text_layer_{layout,types,units,join_v2,build}.py`;
- `docs/text_layer.md`; the changelog (append only);
- `data/text_layer_v2_dev/15j-*` … `15m-*`; `reports/data/phase-09-text-layer-v3-prep/`;
- LotA `docs/from-archival-structures.md` (one new §, 15m only).
- **Not** `text_layer/join.py`, `extract_v2.py`, `line_split.py`, `word_breaks.py` (v1 paths), and
  not `shared-data/`.

## Sources

- `plan/CONTEXT.md` §1–§5 (all of it; 170 lines).
- Phase-07 `REVIEW.md` *Findings* rows 6–15 (L57–75) and *Step 15ir review* (L124–170).
- `reports/phase-07-sg-units-and-pilot.md` *Step 15ir* (L1272–1353) and *Follow-ups* (L1406–1422);
  findings F07-15ir.3, .6, .9, F07-R.11, F07-15hj.6 (grep the IDs).
- `docs/from-language-of-the-archive.md` R5 (L57–60).
- Phase-07 brief `step-15ir-review-fixes.md` (template for the build and v1 commands).

## Steps

Global numbers continue step 15 (inventory §4 has no row for this work). Run in order: each step
builds a dev layer from scratch with the code as it then is and compares it with the previous
step's layer (P10), and 15j–15l share `build.py` / `presets.py`.

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 15j | Row-tolerant line order | — | print same-row swaps in sorted pieces 561 → ≤ 25 (455), 1,380 → ≤ 40 (456); 0 new jumps; 455 `unchanged` jumps 130 → 0, `lines_resorted` ≈ 182 / 7; hand layout identical apart from the new zero columns; v1 157/157 | `step-15j-row-tolerant-line-order.md` |
| 15k | Page-number types, crosswalk role, units | 15j | off-chain retype by gap rule + per-band bar; 188 gap segments keep `page_number`; crosswalk `role`, 513 `end` ids → last half; `claimable` per scan, units output identical, 455 units stage ≤ 5 s; no REPUBLIC default in the library | `step-15k-types-units-followups.md` |
| 15l | Continuation marks and the upper-case guard | 15k | `, `/`,,` + letter merges in hand 70 → 0; 455 `J` + lower 2,175 → 0 if the bar passes; `g_upper_right` per sub-layer by the bar; 0 offset violations; v1 157/157 | `step-15l-join-followups.md` |
| 15m | v3 presets, integrated dev build, run kit | 15l | `sg_*_v3` presets; `15m-a` tables = `15l-a`; audit and 116 rows against `15ir-a`; docs + LotA note; UA to stage the full run; pin 0.9.1 only if on PyPI | `step-15m-v3-build-and-run-kit.md` |

## Why this order

Layout feeds every later stage, so 15j goes first, and 15k and 15l measure on its order. 15k and
15l touch different stages but share `build.py` and `presets.py`, so they run one after the
other. 15m only adds presets and docs; it checks that the chained steps give the same tables as one
from-scratch build, and it carries the pin, which may stay blocked (UA-007).

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | Slug `text-layer-v3-prep`; the phase ends with dev layer `15m-a` and a run kit, not a publication | No `shared-data` writes; the full run is a later step after the user stages inputs | User, 2026-10-21. Only 16 inventories are local (F09-P.7) |
| P2 | Print `unchanged` regions are sorted with the row key and flagged `lines_resorted=1`; their ids stay; hand keeps XML order | `LayoutParams.sort_unchanged_lines` (default False), True in the SG print preset only | User, 2026-10-21 (R5 keeps ids, not line order). 130 jump regions in 455; hand reorders 2–7% of regions unexamined (F09-P.6) |
| P3 | `g_upper_right` is built off; per sub-layer it is switched on only if ≥ 27 of 40 read upper-case merges are wrong | Bar fixed before reading; unclear counts as right | User, 2026-10-21. 27/40: Wilson lower bound > 0.5 |
| P4 | Row key: sort by baseline mean y; a line joins the current row when its baseline y − the row's first baseline y ≤ 0.2 × the median box height of the lines being sorted; rows by x. `LayoutParams.line_sort="row"` is the default, `LEGACY_15FP` keeps `"baseline"`. Applies to merged regions and column-split pieces | `row_tol_frac=0.2`, not tuned in the step | Planning: baseline gaps are < 15 px within a row, about 48 px between rows, box h about 75 px; 0.5 × h merged rows (246 regions with new jumps in 456) (F09-P.2) |
| P5 | The interleave-guard bridge is not fixed | No step | F09-P.1: the bridged pieces read are column fragments that belong in the merge |
| P6 | An off-chain `pn_line` segment whose number lies strictly between the nearest chain numbers before and after keeps `page_number` (`rule:pn_line_gap`). The others become `line_type="other"` per band (`TypeParams.offchain_other_bands`) only where ≥ 16 of 20 read are not page numbers; year-shaped ones become `other` / `rule:year_offchain` if ≥ 30 of 37 read are not page numbers | Bars fixed before reading; verdicts on the planning sample before the build | F09-P.3: 188 gap rows, 8 of 8 read real; 15ir: 18 of 20 off-chain read were not page numbers, but top-band misreads ("552 )") are real |
| P7 | `units/republic_lines` gains `role` (`start`/`end`/`both`); `split_original` maps `end` to the last half, `start` and `both` to the first | Append the column; one row per REPUBLIC id as before | F09-P.5: 553 end/both ids; 10 of 10 read end in the last half |
| P8 | No REPUBLIC default path in the library: the crosswalk runs only with `--republic-dir`; `units.REPUBLIC_PARAGRAPHS_DIR` is removed | Every print build passes `--republic-dir`; changelog lists the removal | REVIEW row 8; no sibling imports it (checked 2026-10-21) |
| P9 | Lead marks: the `,` rule takes one or two marks and an optional single space before the letter; a new per-inventory `LayerPreset.join_lead_marks_lower` (`J`, only before a lower-case letter) is set for 455 only if ≥ 27 of 30 read are the repeated mark | `join_unit` evaluates lead marks before the upper-case guard | F07-15ir.9 (70 left); F09-P.4: 455 2,175 `J`+lower, 456's 32 are real words |
| P10 | Each step builds `<step>-a/{hand,print}` from scratch (`--stage all`, about 8 min with 8 workers) and compares with the previous step's layer with `planning/compare.py` | Expected numbers come from `15ir-a`; a step first re-runs the planning script on its start layer and records any drift | CONTEXT §5 defect 2 |
| P11 | v3 presets `sg_1.01.02_v3` / `sg_1.10.94_v3` = the v2 presets with names `NL-HaNA_*_text_layer_v3`; `format_version` stays 2 | v2 presets stay (dev layers keep comparing) | D17: the full run is `text_layer_v3` |
| P12 | The 0.9.1 pin happens in 15m only if `pip index versions pagexml-tools` lists 0.9.1 | Else UA-007 stays open and the run kit records 0.9.0 | 0.9.1 not on PyPI on 2026-10-21 |

## Hazards

1. **Protected:** all of `shared-data/` (the published `text_layer_v2/` included), LotA `data/ gold/
   output/`, NLP `output/voc_text_layer`, every earlier dev layer. Hash with
   `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip <own prefixes>` before and
   after; `diff` must be empty.
2. **v1 byte-identical** (`TOTAL 157 / 157`) in every step; `join.py`'s `ALL_GUARDS` must not change
   (it is v1's guard set and the join currency check).
3. **`LEGACY_15FP` reproduces 15fp**: the legacy tests pass unchanged.
4. **REPUBLIC dir** has no default from 15k on: every print command passes `--republic-dir`.
5. **Pre-set bars**: verdicts are written before the preset is set; no re-reading to pass a bar.
6. **Workers**: through `coreshare.py run … --want 8 --min 2 --wait -- … --workers {workers}`.
7. **Agent shell:** no `rm -rf`, `nohup`, `&`; a new dir per run; `chmod g+w`; `.venv/bin/python`.
8. **Changelog:** v0.13.0 is on top with no `## Unreleased`; 15j adds the heading.

## Open questions for planning: settled 2026-10-21

| Question (CONTEXT §4) | Settled by |
|---|---|
| Row-tolerant line sort | P4 (measured), step 15j |
| Sorting `unchanged` print regions (R5) | P2 (user) |
| Retyping off-chain `pn_line` segments; bottom-band years | P6, step 15k |
| `, ` and `,,` lead marks | P9, step 15l |
| `claimable` per scan | step 15k (output identical) |
| `g_upper_right` guard | P3 (user), step 15l; P9 handles the 455 `J` artefact first |
| Crosswalk role column | P7, step 15k |
| Interleave guard bridged by a third region | P5: not fixed (F09-P.1) |
| Move the REPUBLIC default path out of the library | P8, step 15k |
| pagexml-tools 0.9.1 and its workarounds | P12, step 15m |
| Scope: full `text_layer_v3` run | P1 (user): run kit only |

## Review checklist

As in `plan/templates/PHASE_PLAN.md`, plus:
- [ ] Every bar's verdict file exists with a verdict per row, and the preset matches the bar.
- [ ] `15m-a` tables equal `15l-a` tables (only manifest/preset names differ).
- [ ] v1 157/157; `LEGACY_15FP` tests unchanged; `join.py` untouched (`git diff --stat`).
- [ ] No REPUBLIC path left in `archival_structures/` (`grep -rn language-of-the-archive archival_structures`).
- [ ] The run kit's commands run on `--limit 2` as written.

## Hand-off

Reviewed 2026-10-22 (Opus): **accept with follow-ups** ([REVIEW](REVIEW.md)); no review-fixes step.
Release as **v0.14.0** (not yet released).

- **For whoever builds or reads v3:** presets `sg_1.01.02_v3` / `sg_1.10.94_v3`; reference dev layer
  `data/text_layer_v2_dev/15m-a`; the output changes and schema additions are in `docs/text_layer.md`
  *Text layer v3 (phase 09)*, and for LotA in its `docs/from-archival-structures.md` §8. Print builds need
  `--republic-dir`. Nothing is published; the pilot `text_layer_v2` stays.
- **Bar outcomes** (presets): print bottom-band off-chain numbers and all off-chain years → `other`;
  hand bands and print top kept; hand `,`/`,,`/`, ` lead marks; 455 `J`; `g_upper_right` not adopted.
- **For the user:** UA-015 (stage inputs, then the full run with the run kit); UA-007 (0.9.1 pin).
- **For later planning** (`plan/CONTEXT.md` §4): the v3 full run and publication; hand-bottom crop
  verdicts (F09-R.5); chain-aware `_pn_candidate`; a capital-mark rule before re-testing the
  upper-case guard (F09-15l.5); an SG throughput fit.
