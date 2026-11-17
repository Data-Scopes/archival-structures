# Phase 19 report: Page elements v2

> **Status:** draft (step sessions add sections).
> **Phase plan:** [`plan/phase-19-page-elements-v2/PLAN.md`](../plan/phase-19-page-elements-v2/PLAN.md).
> **Code state:** AS v0.23.0; no phase-19 code yet.
> **Last updated:** 2026-11-17, by phase planning (Opus 5.5).

## Summary

<Written by the phase review.>

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F19-P.1 | Fresh test volumes exist for every multi-volume stratum only on the drive for 2.10.50 (853 zips; the four local volumes were all used in phase 18) and 1.05.06 (27 zips with thumbs and ≥ 40 scans); AsdSAA 5023/5075/89 and AsnDA stay single volumes, so their fresh pages share a volume with dev | data-property | sampling, 2.10.50, 1.05.06, asdsaa, asnda | draft | [Planning](#planning) |
| F19-P.2 | On dev-A the layer's margin call (TP 82, FP 234 of 94 margin lines) is refined best by dropping candidates on tabular sides (P 0.857, R 0.574); geometric alignment does not separate margin notes from table columns, because hand margin notes (5023) are long, dense and row-aligned too | negative-result | margin, page-grid | draft | [Planning](#planning) |
| F19-P.3 | Converting the phase-18 gold to element ids gives 802 elements on dev-A and 677 on dev-B; 156 and 186 of them open where the type family changes without a `start` flag | data-property | gold-format | draft | [Planning](#planning) |
| F19-P.4 | The 2.10.50 and 1.05.06 drive zips (`<inv>.zip`, nested members) do not match the presets' source patterns; extracting the xml members flat into `<archive>_<inv>/` builds with 0 failures (2.10.50 835: 70 files) | method | text-layer, staging | draft | [Planning](#planning) |
| F19-P.5 | On dev-A, smoothing `listlike` flags within row-paired layout-region groups lifts recall from 0.575 to 0.754 at P 0.913 (share ≥ 0.5) or to 0.815 at P 0.877 (≥ 0.3); plain table groups add 354 false positives (running text paired with its margin notes) | result | listlike, element-reader | draft | [Planning](#planning) |

## Steps

### Planning

Probes run by the planner on 2026-11-17 (scripts in
[`data/phase-19-page-elements-v2/planning/`](data/phase-19-page-elements-v2/planning/)). Dev-A is the phase-18 test set
(P001–P060, user-corrected); its errors were read in phase 18, so it is dev now.

- **Sample frame and draw** (F19-P.1): `draw_volumes.py` (seed 20261117) excludes every phase-18 volume and, for SG,
  the 233 inventories of any dev layer run (written to `dev_run_inventories.txt` first, so the draw does not depend on
  where those runs are stored, UA-074). Result `volumes.tsv` (22 rows; sha256 24a2b456…1a2a): 3 volumes in each of
  sg_hand (250, 4361, 4546), sg_print (12183, 3854, 12161), voc (1090, 3869, 3684), na_2.10.01 (478, 1464, 1196),
  na_2.10.50 (329, 835, 531), na_1.05.06 (972, 1182, 722), plus the four single volumes.
- **Build** (F19-P.4): 2.10.50 835 extracted from the drive zip into the scratchpad and built with `na_2.10.50_v5sg`,
  `--stage all`: 70 scans, 0 failures.
- **Margin cells** (F19-P.2; `probe_margin.py`): on dev-A per page side (layout `page`), the layer call and five
  refinements. True margin lines sit mostly in AsdSAA 5023 openings (61 of 94), near the middle of the scan, so the body
  band must be per side. M1 "inside the side's body band": TP 71, FP 130. Alignment tests (a candidate column whose
  lines align with main lines) remove true margin notes (67 of 82), because 5023's notes are long, dense, row-aligned
  blocks. Tabular side (median chars of non-margin lines < 25 or ≥ 3 layout columns): TP 54, FP 9. Both: TP 47, FP 0.
  Lost true margins are on account pages ("Memorie", "x £", P002–P004).
- **Element cells** (F19-P.5; `probe_elements.py`): blocks = runs of one layout region; groups = row-paired blocks
  (≥ 50% of the shorter block's lines y-overlap a line of the other, hulls overlapping < 10%). Table groups alone: P
  0.699, R 0.636; with group median < 30 chars: 0.912/0.619; plus smoothing ≥ 0.5: 0.913/0.754; ≥ 0.3: 0.877/0.815;
  plus short lines on tabular sides: 0.759/0.946.
- **Conversion** (F19-P.3): counted with the 25a rules on both phase-18 gold files; 12 column-head lines, 38 letterhead
  and 6 copymark lines; `prose` on 16 dev-A pages.

## Review

<Written by the phase review.>

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|

## Reproduce

Planning probes, from the AS root: `.venv/bin/python -I reports/data/phase-19-page-elements-v2/planning/draw_volumes.py`
(needs the drive), `…/planning/probe_margin.py`, `…/planning/probe_elements.py`.
