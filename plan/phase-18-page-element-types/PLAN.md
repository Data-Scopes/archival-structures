# Phase 18: Page element types v1

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **State:** planned 2026-11-02 (Opus 5.5) from [INPUTS](INPUTS.md) (user decisions of 2026-11-02).

## Goal

AS gets one vocabulary for page elements: the `generic:` types extended with the user's additions, and placement as a
separate axis (`placement:`). It gets `listlike` as an AS module, and whole-page gold across 10 strata in 9 archives.
The phase measures what AS already has against that gold: the text layer's line types and margin calls (mapped to the
vocabulary), and `listlike`. Two existing claims get a bar: L (`listlike` separates lists and tables from running text)
and M (the layer's `marginalia` call finds margin lines). Everything else is reported and read. Detectors for the new
types are phase 19, chosen from this phase's error analysis.

## Owns

- `archival_structures/datasets/vocabulary.py` (placement namespace, new types; additive);
- new `archival_structures/analysis/listlike.py` and `archival_structures/analysis/page_elements.py`;
- `archival_structures/text_layer/presets.py`: seven archive presets (additive; no existing preset changes);
- tests for these; `docs/vocabulary.md`, new `docs/page_elements.md` (in the docs toctree);
- `reports/data/phase-18-page-element-types/`; local data `data/page_elements_inputs/`, `data/page_elements_layers/`,
  `data/page_elements_images/`.
- Not owned: `data/annotations/` (the user's annotation store; never written), `shared-tools/` (unchanged; the
  `listlike.py` copy stays frozen), every text-layer stage.

## Sources

Read at planning: [INPUTS](INPUTS.md); `docs/vocabulary.md`; `docs/from-republic-segmentation.md` (2026-11-02 entry);
`shared-tools/listlike.py` (whole); `plan/CONTEXT.md`; the phase-17 plan and 23a brief (gold and check-page practice).
Code survey (Explore, 2026-11-02): `datasets/vocabulary.py` (148 lines, 17 tests), `datasets/annotations.py` (407 lines;
`ScanAnnotation`, `Element`/`ElementSpan`; 16 tests), `text_layer/types.py` (`TYPES`, segments schema), `presets.py`
(`PRESETS`), `io.py` (`discover_inventories_v2`, `iter_pagexml`), `analysis/page_structure.py` (`layout_role`
main/extra/empty), shared-tools `checkpage.py` and `handprint.py`, and 23a's `build_folio_page.py`. The suite at planning
collects 1,443 tests. Planning probes: `reports/data/phase-18-page-element-types/planning/` and the *Planning* section
of the report (F18-P.1–P.6).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 24a | Vocabulary v2, `listlike` port, `page_elements` reader, archive presets | nothing | features equal to the shared-tools copy on fixtures; vocabulary additive (old tags known); 7 presets build their probe inventory with 0 failures; suite green | `step-24a-vocabulary-listlike.md` |
| 24b | Stage, build and sample: layers for the 7 new archives, 100 pages drawn, page images and reader dumps | 24a | 7 layers built, 0 failures; `sample_pages.tsv` (60 test, 40 dev) hashed; every page has an image and a dump | `step-24b-sample-pages.md` |
| 24c | Run-labelling check page, agent pre-labels, gold freeze (UA-063) | 24a, 24b | readers label every line of the 100 pages; dev gold frozen; test page handed over; after UA-063, `test_gold.done` | `step-24c-element-gold.md` |
| 24d | Measure: `listlike` grid on dev, bars L and M on test, type and placement tables | 24c (dev: `dev_gold.done`; test: `test_gold.done`) | `select.json` before `test_done.json`; bar verdicts with page-cluster bootstrap intervals; tables per stratum and type; error analysis | `step-24d-measure.md` |

## Why this order

- 24a first: the presets let 24b build with the CLI, and the vocabulary fixes the label set that 24c shows.
- 24b draws pages only after the layers exist (page eligibility needs line counts), and fetches each page image once,
  for the readers and for the check page.
- 24c needs a page set and a label set. Its reader agents run in the orchestrating session (step sessions cannot spawn
  agents, CONTEXT §5). The user's corrections (UA-063) are the only user work in the phase.
- 24d's dev grid can run as soon as `dev_gold.done` exists; its test stage waits for `test_gold.done`.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Scope:** vocabulary, `listlike` port, gold, measurement; 4 steps. No new detector | all | User, 2026-11-02 (INPUTS §Decisions) |
| P2 | **Placement is its own namespace** `placement:` with `main`, `margin`, `top`, `bottom`, `interlinear`. `generic:marginalia` stays a known tag (legacy: placement margin, type unknown); new labels never use it | 24a, 24c | User Q1. `position:` is taken (start/continuation/end) |
| P3 | **New generic types and subtypes:** `heading` {title, date, summary}; `salutation`; `closing` {formula, dating, signature}; `endorsement`; `running_title` {date}; `numbering` {page, folio, item, other}; `reference`; `note`; `addition`; `noise`. All existing types and subtypes stay | 24a, 24c | User Q2. Additive, so every existing tag stays known |
| P4 | **`listlike` port is verbatim by default** (`analysis/listlike.py`, stdlib only): same features and flags as the shared-tools copy. Phase-18 variants are named constants, off by default: `LISTLIKE_CFG_C1` (amount end only after an amount word or a leader/locator) and `RULE_R1` / `RULE_R2` (adds `closed_share <= 0.4`, min 2 / min 3) | 24a, 24d | User Q1. The baseline must be the tool as handed over. Planning probe: C1 halves the flags, R1 multiplies them (F18-P.3); the case-sensitive numbering variant moved ≤ 152 lines per volume and is dropped |
| P5 | **Archive presets** = `VOC_1_04_02_V5SG` (the SG v5 rules unchanged) with archive plumbing only: name, `archive_id`, `scan_id_prefix` (NA archives) or `None`, `source_pattern`. Keys `na_1.05.06_v5sg`, `na_2.10.50_v5sg`, `na_2.10.01_v5sg`, `asdsaa_5023_v5sg`, `asdsaa_5075_v5sg`, `asdsaa_89_v5sg`, `asnda_0114.11_v5sg` | 24a, 24b | "Measure what exists": the same rules as VOC (phase 16 practice). Probes built one inventory of each with 0 failures (F18-P.1) |
| P6 | **Strata and volumes, drawn at planning** (seed 20261102, `planning/volumes.tsv`, sha256 5b4d1cc5…c5ab): SG hand and SG print (1.01.02, the v5 full layer on the drive, volumes outside every dev layer run), VOC (v6 layer), 1.05.06 (only inventories with local thumbs: no NA IIIF file list for 1.05.06, F18-P.6), 2.10.50, 2.10.01: 3 test + 2 dev volumes each (2.10.50: 2 + 2); AsdSAA 5023, 5075, 89 and AsnDA 0114.11: one volume each, split by scan halves (dev first half, test second half) | 24b | User Q2/Q5. Test and dev never share a volume, except in the one-volume archives, where they share no scan range |
| P7 | **Pages:** eligible = scan `status` ok, 3 ≤ `n_lines_text` ≤ 150, and an image (1.05.06, 5023, 5075, AsnDA: a local thumb). Test 6 pages per stratum (2 per test volume; 2.10.50 3 per volume; halves: 6), dev 4 (2 per dev volume; halves: 4). 60 test, 40 dev | 24b | ~55 pages proposed in INPUTS; 6 per stratum keeps every archive in the test |
| P8 | **Gold grain: one row per layer line** (`lines/` table row, key (archive, scan_id, line_id); line ids repeat across scans in AsdSAA, F18-P.4). Fields: `type` (generic type[:subtype]), `placement`, `start` (an element starts here), `unclear` (0/1), `note`, plus the pre-label and `changed` | 24c, 24d | User Q3 (whole pages). `start` gives element boundaries for phase 19 at little cost. Stored also as `ScanAnnotation` + `Element` JSON under the step's `gold/annotations/`, never in `data/annotations/` |
| P9 | **Pre-labels from Sonnet reader agents, not from AS code.** Readers see only the PageXML text and line geometry (24b dumps) and the page image; never the layer's types/layout, `listlike`, PageXML region tags, or the user's existing annotations. Dev gold = agent labels as read; test gold = the user's corrections of them (UA-063). `changed` is recorded per line | 24b–24d | User Q4. If the measured systems made the pre-labels, the test would measure anchoring (INPUTS decision 6) |
| P10 | **Bars, fixed 2026-11-02 before any read.** **L:** selected `listlike` cell, test lines whose gold type is `running_text` (negative) or `list`/`table`/`form` (positive), `unclear` excluded: P ≥ 0.90 and R ≥ 0.80. **M:** layer margin call (the line's `line_type` is `marginalia`) against gold placement `margin` (positive) vs `main` (negative), `unclear` excluded: P ≥ 0.90 and R ≥ 0.80. Pooled over strata. 95% intervals by page-cluster bootstrap (2,000 resamples, seed 20261102); Wilson intervals also given | 24d | Line-level Wilson intervals understate the error of a 60-page sample: lines on a page are not independent |
| P11 | **Grid fixed:** six cells {C0, C1} × {R0, R1, R2}, group = (scan_id, layout_region_id). Select max dev F1 (positive = list/table/form); tie band 0.01 → order C0R0, C0R2, C0R1, C1R0, C1R2, C1R1 (fewer changes first). `select.json` before `test_done.json`; one test run; a failed bar is a negative result, no retuning | 24d | Phase 13–17 practice (P8 of phase 17) |
| P12 | **Reported, no bar:** per gold type and stratum: the layer's mapped type call (P/R per type; `date_header` counts as right for `heading:date` and `running_title:date`), `listlike` flag rate per gold type, the agent pre-label agreement (type, placement, `start`), VOC `region_tag` (provider layout tags) mapped; the user's `changed` share | 24d | Phase 19 needs to know which types the rules already find and which need a detector |

## Hazards

- **Drive.** Read only `/Volumes/T7_Data/Data/PageXML/Ministerie_van_Kolonien/2.10.01/scans/`, the thumbs under
  `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/thumbs/`, and the SG v5 / VOC v6 layers. Write
  nothing on the drive. A hook denial or `Permission denied` is final. If the drive is unmounted, the SG/VOC layers and
  the 2.10.01 inputs are missing: stop.
- **Protected.** `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 24` before and after 24b;
  `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT` before and after 24b (the VOC v6 layer is read
  only). `planning/volumes.tsv` hash in every brief. `data/annotations/` is never written (hash its file list before and
  after 24c).
- **Keys.** (archive, scan_id, line_id) everywhere; AsdSAA scan ids are file stems (`0001_A14767000001`, `scan_num`
  empty) and line ids repeat across scans. Inventory `3.1` has a dot.
- **Tarballs.** Members starting with `._` (AppleDouble) are not PageXML; skip them when staging.
- **Images.** NA (SG, VOC, 2.10.50, 2.10.01): IIIF bases from the NA viewer (`handprint.na_iiif_bases`, network); 1.05.06 has none, so its 600 px `.tif` thumbs are converted to PNG. AsdSAA 89: Memorix IIIF
  `info.json` next to the thumb, matched by position (`NNNN_0NNNN.xml` ↔ `KLAB08969000NNN`), size checked against the
  PageXML. AsdSAA 5023/5075: 600 px png thumbs; AsnDA: 300 px png thumbs (no zoom; text panel carries the reading).
- **Blindness.** 24c readers never open `layout/`, `types/`, `regions_meta/` or `data/annotations/`; 24d opens
  `gold/test_labels.tsv` only in its test stage.
- **Agent shell.** No `rm -rf`, `nohup`, `&` or `$TMPDIR`; zsh (quote `'{workers}'`); BSD `sed -i ''`; never
  `git stash`/`checkout`/`reset`; a new output dir per run; coreshare for more than one worker; `chmod g+w`.
- **Changelog.** No `## Unreleased` heading at planning: 24a adds it.

## Review checklist

- [x] Acceptance numbers re-run; gold hashes match `gold/SHA256SUMS`; `select.json` precedes `test_done.json`.
- [x] AS suite green; no sibling code changed; shared-tools unchanged; `data/annotations/` unchanged.
- [x] `listlike` port equal to the shared-tools copy on the fixtures (P4); existing vocabulary tags still known (P2/P3).
- [x] Gold quality: the 24c self-check of 30 agent labels; the user's `changed` and `unclear` shares; pages not reviewed.
- [x] Leakage: readers blind as in P9; SG test volumes outside every dev layer run (assert in 24b).
- [x] Error analysis read individual lines as each brief lists, in the report; `make reports` clean.
- [x] Changelog `### Step 24a` under `## Unreleased`, "no sibling imports affected".
- [x] `Brief defects` answered.

## Hand-off

Reviewed 2026-11-17 (Opus 5.5, [REVIEW](REVIEW.md)): **accept with follow-ups**. Checklist above: all pass, with
evidence in REVIEW.

- **Delivered (library, additive):** vocabulary v2 (`placement:`, ten new generic types and subtypes,
  `legacy_placement`), `analysis/listlike.py` (verbatim port plus the C1/R1/R2 constants), `analysis/page_elements.py`,
  seven `*_v5sg` archive presets. Changelog `### Step 24a` under *Unreleased*; no sibling import affected.
- **Data:** 7 layers / 18 inventories in `data/page_elements_layers/24b-a/`; 100 pages with images and dumps; gold
  `step-24c/gold/` (dev agent-read e0f2a4de…, test user-corrected 286b3ad0…). **The test gold is spent** for `listlike`
  cells and the layer's margin call.
- **Bars:** L fails (C0R1 P 0.950, R 0.575; precision only passes under the UA-070 ruling, F18-R.1). M fails (P 0.259,
  R 0.872; robust without the registers, F18-R.2). Negative results, no retuning.
- **Review fix:** `step-24r-review-fixes.md` (Sonnet): `bulk_tagging` placement map (F18-R.10), VOC-only region-tag
  tables (F18-R.7), `listlike` docstring in the docs.
- **Release:** v0.23.0 (minor) after 24r: 24a's API plus 24r's `bulk_tagging` fix. Released in: v0.23.0.
- **Next:** phase 19, user decision UA-073. Recommended: page elements v2 (table-column/margin split, element-level
  list/table grouping, annotation format with element ids and per-element properties, fresh gold). Candidate
  alternative: NL-AsdSAA 5075 deed boundaries (UA-065).
