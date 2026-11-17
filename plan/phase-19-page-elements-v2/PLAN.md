# Phase 19: Page elements v2

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **State:** planned 2026-11-17 (Opus 5.5) from [INPUTS](INPUTS.md) (user decisions of 2026-11-17).

## Goal

AS gets a gold format v2 for page elements (element ids, item/row starts, a per-element prose property, `table:header`,
`letterhead`, `copymark`), a per-page-side geometry module (`page_grid`), a margin/table-column split
(`margin_split`) and an element reader (`element_reader`) that groups lines into elements and rows. Both detectors are
analysis modules over the existing layers. They are developed on the phase-18 pages (now dev) and tested once on 60
fresh pages with user-corrected gold: bar L2 (element reader) and bar M2 (margin split), the phase-18 bars on new
systems. Item starts are reported, no bar.

## Owns

- `archival_structures/datasets/vocabulary.py`, `datasets/annotations.py` (additive), new `datasets/element_gold.py`;
- `archival_structures/analysis/page_elements.py` (additive), new `analysis/page_grid.py`, `analysis/margin_split.py`,
  `analysis/element_reader.py`; their tests; `docs/page_elements.md`, `docs/vocabulary.md`;
- `reports/data/phase-19-page-elements-v2/`; local data `data/page_elements_inputs_v2/`, `data/page_elements_layers/25b-a/`,
  `data/page_elements_images_v2/`.
- Not owned: `data/annotations/` (the user's; never written), `shared-tools/`, every text-layer stage and preset, the
  phase-18 outputs under `reports/data/phase-18-page-element-types/` (read only; its gold files are hashed).

## Sources

Read at planning: [INPUTS](INPUTS.md); `plan/CONTEXT.md` (2026-11-17); the phase-18 report (Summary, Review,
Follow-ups); the 24b–24d scripts (survey by an Explore agent, 2026-11-17: signatures, columns, CLIs, test counts;
suite 1,471 collected, 1,460 passed after 24r). Planning probes: `reports/data/phase-19-page-elements-v2/planning/`
(F19-P.1–P.4 in the report's *Planning* section).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 25a | Gold format v2, dev conversion, scan sizes | nothing | vocabulary additive; `element_gold` validates the converted dev sets with 0 errors (dev-A 4,334 lines / 802 elements, dev-B 3,167 / 677); old element JSON unchanged | `step-25a-gold-format-v2.md` |
| 25b | Fresh test pages, v2 reader pre-labels, check page v2, test gold (UA) | 25a | 60 pages T001–T060 with image and dump; prelabels and dev item starts merged with 0 check errors; check page handed over; after the UA, `test_gold.done` | `step-25b-test-gold-v2.md` |
| 25c | `page_grid` and `margin_split`, dev grid for M2 | 25a | grid M0–M5 on dev-A; `select_margin.json`; M0 reproduces phase 18 (TP 82, FP 234, FN 12) | `step-25c-margin-split.md` |
| 25d | `element_reader`, dev grid for L2, item starts | 25c (page_grid); 25b part C (dev item starts) for the item report | grid E0–E5 on dev-A; `select_elements.json`; E0 reproduces phase 18 C0R1 on these pages (TP 836, FP 44, FN 619) | `step-25d-element-reader.md` |
| 25e | Measure on test: bars L2 and M2, items, baselines | 25b (`test_gold.done`), 25c, 25d | both select files older than `test_gold.done`; one test run; bar verdicts with bootstrap and Wilson intervals; error analysis | `step-25e-measure.md` |

## Why this order

- 25a fixes the format first: 25b's readers and check page, and the dev sets of 25c/25d, all use it.
- 25b runs in parts with the orchestrator between them (reader agents, CONTEXT §5) and then waits on the user. 25c and
  25d run while the user corrects the test pages; they never open the test gold.
- 25d reuses 25c's `page_grid`, so it follows 25c. The element reader does not take the margin calls (P7), so the two
  systems are scored independently.
- 25e opens the test gold once, after both selections are written.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Scope** (INPUTS 2, 5): format v2, margin split, element reader; analysis modules only. No layer version, no preset change; mode detection out | all | User, 2026-11-17 |
| P2 | **Dev and test.** dev-A = phase-18 test pages P001–P060 (user gold): selection and error analysis. dev-B = phase-18 dev pages P061–P100 (agent gold, other conventions): reported only. Test = 60 fresh pages T001–T060 | 25a–25e | Phase-18 test errors were read in detail (F18-R.13); designing on them spends them |
| P3 | **Gold v2 line table** `page_id stratum archive inv scan_id line_id k type placement element item_start prose unclear note`. `element` = page-local `e1…`, numbered by first line in k order; an interrupted element keeps its id. `item_start` 0/1 on list/table/form lines (first line of an item or row, header rows included), `""` = unknown (converted dev). `prose` 0/1 on list/table/form lines, constant within the element (cells written as sentences), `""` elsewhere or unknown. Element table derived (`page_id element type prose n_lines first_k`) | 25a, 25b, 25e | Fixes F18-R.8/.9: element boundaries and prose as fields, not notes |
| P4 | **Vocabulary additions** (additive): `table:header` (column-head row), `letterhead`, `copymark`. **Form vs table:** a ruled or pre-printed grid of repeated rows (registers, ledgers, inventories with an amount column) is `table`; `form` is a document with captions beside blanks filled once (`form:label`/`form:value`). UA-070 a, UA-071 a stand | 25a, 25b | F18-R.3, F18-24c.3 (`custom:letterhead` 38, `custom:copymark` 6 lines over dev-A + dev-B) |
| P5 | **v1 → v2 conversion** of the phase-18 gold: a new element where `start = 1` or the type family changes (dev-A 156, dev-B 186 forced splits); `table` + note "column head" → `table:header` (12 lines); `custom:letterhead`/`custom:copymark` → the new types; `item_start` `""`; dev-A `prose` from the note token on the 16 pages that carry it, `""` elsewhere | 25a | Measured at planning (F19-P.3) |
| P6 | **Test sample** (`planning/volumes.tsv`, sha256 24a2b456…1a2a, seed 20261117): 3 fresh volumes × 2 pages in sg_hand, sg_print, voc, na_2.10.01, na_2.10.50 (drive zips), na_1.05.06 (drive zips with ≥ 6 thumbs); 6 fresh pages in the single volumes of asdsaa_5023, 5075, 89, asnda_0114.11, excluding the phase-18 pages and scans within ±3 of them. SG excludes the 233 dev-run inventories (`planning/dev_run_inventories.txt`). Eligibility as phase 18 (status ok, 3 ≤ `n_lines_text` ≤ 150, 300 for sg_print, an image) | 25b | User Q2. 2.10.50 had only the four used local volumes; the drive holds 853 (F19-P.1) |
| P7 | **Detectors are independent.** `margin_split` refines the layer's margin call (it never adds a line, so its recall ≤ the layer's: 0.872 on dev-A). `element_reader` reads all lines of a side, margin calls included, and never sees the margin cells | 25c, 25d | Two bars, two systems; no coupling between their selections |
| P8 | **Margin grid fixed** (per page side, `layout` page): M0 layer call; M1 drop a candidate region whose median x-centre lies inside the side's body band; M2 drop all candidates on a tabular side (median chars of non-margin lines < 25, or ≥ 3 layout columns); M3 = M1 + M2; M4 drop only on a tabular side and if the region is inside or numeric (≥ 0.5 numeric lines); M5 = M3 with 20 chars. Tie band 0.01 on dev-A F1, order M0…M5 | 25c | Probe F19-P.2 on dev-A (94 positives): M0 TP 82 FP 234; M1 71/130; M2 54/9; M3 47/0; M4 78/132; M5 47/2. Best dev F1 is M2 (P .857, R .574): bar M2 is expected to fail on recall; a pure refinement cannot exceed R .872 |
| P9 | **Element grid fixed** (per page side; blocks = runs of one layout region; groups = row-paired blocks): E0 `listlike` C0R1 per line; E1 = E0 + short table groups (≥ 2 blocks, ≥ 3 rows, multi-line-row share ≥ 0.5, group median < 30 chars); E2 = E1 + smoothing (group of ≥ 4 lines with flag share ≥ 0.5 → all positive); E3 = E2 + list groups by indent (≥ 0.3) or item markers (≥ 0.4); E4 = E1 + smoothing at 0.3; E5 = E2 + lines < 40 chars on a tabular side. Tie band 0.01 on dev-A F1, order E0…E5 | 25d | Probe F19-P.5 on dev-A: E0 P .950 R .575; E1 .912/.619; E2 .913/.754; E4 .877/.815; E5 .759/.946 (E3 not probed). Plain table groups cost 354 FPs (running text paired with margin notes) |
| P10 | **Bars, fixed 2026-11-17 before any read of test.** **L2:** selected E cell; lines with `unclear = 0` and gold family `running_text` (negative) or `list`/`table`/`form` (positive): P ≥ 0.90, R ≥ 0.80. **M2:** selected M cell against gold placement `margin` vs `main`, `unclear = 0`: P ≥ 0.90, R ≥ 0.80. Pooled; 95% intervals by page-cluster bootstrap (2,000, seed 20261117) and Wilson | 25e | The phase-18 bars, so the gain is comparable |
| P11 | **Reported, no bar:** both bars per stratum and on the 6 fresh-volume strata alone (the 4 single-volume strata share a volume with dev); E0 and M0 on test (the phase-18 systems on fresh pages); item starts P/R/F1 per family (gold `item_start` on list/table/form lines) for the reader's rules I1/I2; L2 recall on `prose = 1` elements; dev-B numbers | 25d, 25e | Single-volume leakage disclosed, not hidden |
| P12 | **Pre-labels blind** (as phase 18 P9): readers see the dumps and the image only; never layers, `listlike`, `page_grid`, the detectors, region tags or `data/annotations/`. Dev item starts are read by agents given the dev gold types (gold, not a system under test) | 25b | Anchoring (F18 INPUTS 6) |
| P13 | **Rulings during the user's correction** go into `step-25b/rulings.md` (with the first page they apply to), shown on the check page; the page is rebuilt with the same storage key. Not only into the chat | 25b | F18-R.8/.9, CONTEXT §5 |

## Hazards

- **Drive.** Read only `/Volumes/T7_Data/Data/PageXML/Ministerie_van_Kolonien/2.10.{01,50}/scans/`,
  `/Volumes/T7_Data/Data/PageXML/NA/HTR results 1.05.06 PAGE/`, the thumbs under
  `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/thumbs/`, and the SG v5 / VOC v6 layers. Write
  nothing on the drive. A hook denial or `Permission denied` is final. If the drive is unmounted, stop.
- **Drive zip names.** The 2.10.50 and 1.05.06 zips are `<inv>.zip` with nested members; the presets match
  `NL-HaNA_2.10.50_<inv>` names, so 25b extracts the xml members flat into `<archive>_<inv>/` dirs (probe: 2.10.50 835,
  70 files, built with 0 failures, F19-P.4). 2.10.01 zips are symlinked (`<inv>.zip` matches its preset).
- **UA-074 (b) done:** 13 old SG dev runs are symlinks to the drive; the sample draw already used the manifest
  `planning/dev_run_inventories.txt` (233), so 25b reads that file, never the dev-run folders.
- **Protected.** `data/annotations/` file list before and after 25b; phase-18 gold `SHA256SUMS` checked by 25a, 25c, 25d,
  25e; `planning/volumes.tsv` hash in 25b. 25b's layer build writes only `data/page_elements_layers/25b-a/`.
- **Keys.** (scan_id, line_id) everywhere; AsdSAA line ids repeat across scans. Inventory `3.1` has a dot.
- **Blindness.** 25c/25d never open `step-25b/gold/`; 25b readers never open layers or AS analysis outputs.
- **Agent shell.** No `rm -rf`, `nohup`, `&` or `$TMPDIR`; zsh (quote `'{workers}'`); BSD `sed -i ''`; never
  `git stash`/`checkout`/`reset`; a new output dir per run; coreshare for more than one worker; `chmod g+w`. If git
  reports dubious ownership: `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=<AS root>`.
  Step scripts resolve relative paths against the AS root and say so in their docstring (F18 24d).
- **Changelog.** If no `## Unreleased` heading exists (after the v0.23.0 release), 25a adds it.

## Review checklist

- [ ] Acceptance numbers re-run; gold hashes match; `select_margin.json` and `select_elements.json` precede
      `test_gold.done`, which precedes `test_done.json`.
- [ ] AS suite green; no sibling code changed; shared-tools unchanged; `data/annotations/` unchanged.
- [ ] Old `Element` JSON byte-identical through `to_dict` (P3); old vocabulary tags still known (P4).
- [ ] Gold quality: the 25b self-check; the user's `changed` share per field (type, placement, element, item_start,
      prose); rulings file used as P13 says.
- [ ] Leakage: readers blind (P12); the single-volume strata reported apart (P11); no detector constant fitted on test.
- [ ] Error analysis read individual lines as each brief lists; `make reports` clean.
- [ ] Changelog `### Step 25a`, `### Step 25c`, `### Step 25d` under `## Unreleased`, "no sibling imports affected".
- [ ] `Brief defects` answered.

## Hand-off

<Filled in at review.>
