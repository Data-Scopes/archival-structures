# Phase 16: A second archive (1.04.02 VOC): what generalises

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **State:** seeded 2026-10-30 (phase-15 planning); **planned 2026-10-31** (Opus 5.5): 5 briefs, 22a–22e.

## Goal

Phases 05–15 tuned the text layer, page numbering, index pages and index links on the States General (1.01.02,
1.10.94). When this phase ends:
- the same components have run on the VOC archive (NL-HaNA 1.04.02) **unchanged**: 568 inventories, in one layer;
- each has been measured against VOC gold already in AS, with a bar fixed at planning;
- then a fixed set of VOC options has been selected on dev and tested once;
- a generalisation table records, per component, `unchanged` / `reparameterised` / `fails on VOC` / `runs, no bar`,
  and names the SG-specific rule.

No VOC layer is published and no fitted VOC preset is built (a follow-up). The user put VOC first among the
candidates (1.04.02, 2.10.01, 2.10.50) on 2026-10-30.

## Owns

- `archival_structures/text_layer/presets.py`: additive only. `VOC_SOURCE_PATTERN` and `VOC_1_04_02_V5SG` (22a); the
  constants `VOC_TOC_CUE`/`VOC_INDEX_SIDES_V1` only if bar D passes (22c).
- `archival_structures/analysis/folio_map.py`: `single_foliation`, `single_slope` (22b). `analysis/index_links.py`:
  `read_volume_inputs(number_tags=)` (22b); `LocatorParams.lead_pattern`/`standalone_pattern`, L8,
  `VOC_LOCATOR_PARAMS` (22d). Their tests, plus `tests/test_text_layer_voc_preset.py` (new).
- `docs/text_layer.md`, `docs/index_links.md` (VOC paragraphs); `ground_truth/README.md` *used by* cells (22e); one
  appended section in NLP `docs/from-archival-structures.md` (22e).
- Data: `data/text_layer_voc_inputs/`, `data/text_layer_voc_22a` (link) → T7_Data
  `claude-projects-external/archival-structures/data/text_layer_voc/22a-a/`, and `reports/data/phase-16-voc-generalisation/`.
- **Not owned** (imported only): `layout.py` (`_write_pages_table` called by 22c), `types.py`, `units.py`, `modes.py`,
  `index_pages.py`, `page_structure.py`, the phase-13 audit, every published or dev text layer. No other phase is
  active.

## Sources (read at planning, 2026-10-31)

- `plan/CONTEXT.md` §1–§5; `ground_truth/README.md` VOC rows; `docs/staging/generale_missiven_inventories.csv`,
  `docs/staging/voc_phase16_inventories.tsv`; the GM source CSV; the 22 GLOBALISE segmentation CSVs; the OBP index CSV
  (via `datasets/globalise_index.py`).
- Explorer reports (2026-10-31). AS: text-layer CLI and stages, preset fields, `VOC_V1`, v4/v5 presets, modes, types
  patterns, `folio_map`/`index_links`/`index_pages`/`hand_print`/`page_numbering` APIs, the audit's checks, run kits;
  1,413 tests collected (1,402 passed, 11 skipped). NLP: its splits, spent gold, report/76 (folio alignment), report/83
  (GM recall).
- Planning outputs in `reports/data/phase-16-voc-generalisation/planning/`:
  - `gm_split_and_proto.py` → `gm_split.tsv`, `gm_anchors.tsv` (sha256 `a2b875d9b06e9972…`), `gm_proto_dev.tsv`
    (dev only);
  - `toc_split_and_peek.py` → `toc_gold.tsv` (sha256 `095e7f3198ac3542…`), `toc_peek_dev.tsv` (dev only).
- A planner smoke run of the 22a preset (runtime-registered) on 8023 and 2775, in the scratchpad (not kept).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 22a | VOC text layer with the SG v5 rules unchanged | nothing | preset + tests; 568 inventories built (drive), `failures=0`; audit unchanged, `n_fail_judged` reported (0 = runs unchanged); 6 measurement tables | `step-22a-voc-text-layer.md` |
| 22b | Folio map on VOC (GM folio gold) | 22a | `single_foliation`, `number_tags` additive; 8-cell dev grid; test once: **F ≥ 0.95** (leaf) for unchanged V0, V2 and selected; rectos, restarts, LOO | `step-22b-folio-map-voc.md` |
| 22c | Index-side detector on VOC tables of contents | 22a | replica = `layout/pages`; 32-cell dev grid; test once: **D: scan P ≥ 0.95, R ≥ 0.90** (10 volumes); front runs | `step-22c-index-pages-voc.md` |
| 22d | Index links on VOC TOCs (oracle TOC sides) | 22a, 22b | L8 additive; cells U1, U2, C1–C3; test once: **E ≥ 0.90** (leaf holds a gold start); coverage, chance, silver, end to end | `step-22d-index-links-voc.md` |
| 22e | Generalisation table; notes for NLP and the GT store | 22a–22d | 10-row table with verdicts and finding IDs; GT README *used by*; NLP note | `step-22e-generalisation-table.md` |

## Why this order

- 22a comes first because every other step reads its layer. It tunes nothing, so it can run before any gold is
  looked at.
- 22b and 22c read disjoint gold (GM anchors; GLOBALISE TOC labels) and edit disjoint code, so they may run in
  parallel.
- 22d needs 22b's frozen map, but not 22c: oracle TOC sides isolate the link component from the detector. The
  detector's end-to-end effect is reported as is (`end_to_end.tsv`).
- 22e only compiles.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Scope: VOC only.** 2.10.50 goes to a later phase; the seed's question 3 (document starts) is dropped as NLP's territory | all | User, 2026-10-31. 2.10.50 has no AS gold. NLP measures starts on the 22 with the AS stack already |
| P2 | **Unchanged = SG v5 preset with only the archive plumbing changed:** archive id, source pattern `(?P<inv>[0-9]+[A-Z]*)\.zip`, scan-id prefix. All inventory-keyed SG settings dropped (`wbc_pinned` inventory keys, `MODE_PINS_V4`) | 22a | VOC inventory numbers overlap SG ones; a pin would hit another volume. Everything else (cut 87.75, split gate, types, units, index sides) unchanged |
| P3 | **One layer of 568 inventories** (546 GM on the drive + 22 GLOBALISE), built in-session through coreshare. Output on T7_Data `claude-projects-external`, linked under `data/` | 22a | User, 2026-10-31 (all 546 GM). The planner measured 18 s per 22 MB zipped, so about 1–1.5 h wall. 1171 is excluded (gold names `1171_*`, the PageXML is `1171A`/`1171B`) |
| P4 | **Gold frozen at planning, hashed:** `gm_anchors.tsv` (1,841 anchors, one per (inv, folio, scan); dev 937 / 272 inventories, test 904 / 273; stratified by layout, seed 20261031); `toc_gold.tsv` (22 inventories; dev 11 with 117 TOC scans, test 11 with 69) | 22b–22d | Splits by inventory; the planner prototyped on dev only |
| P5 | **2775 is planner-seen** (its TOC text was read during the smoke run): left out of the 22c/22d test bars and reported separately. 8023 (smoke, no TOC, 0 flags) stays | 22c, 22d | Gold is spent once a variant is shaped by it (CONTEXT §5). L8 and `VOC_TOC_CUE` were written from dev text; the leading-range style also occurs in dev (1274, 1388, 1557) |
| P6 | **Bar F (VOC):** test leaf share ≥ 0.95, where leaf = {front scan, next scan} of `locate(folio)` holds the gold scan. Strict (front only) is reported beside it | 22b | Phase 14/15's F threshold. A GM begin/end scan can be folio N's verso, which is the next scan in both layouts. Planning baseline (v1 Laypa anchors, scan level, dev): leaf 0.647, strict 0.505 |
| P7 | **Map grid fixed:** M {V0, V2} × S {single_foliation off, on} × N {types, types + Laypa `page-number` lines}; max dev leaf, ties to fewer changes; the unchanged cells (V0/V2, off, types) are tested too | 22b | S: VOC single-page scans make every volume `pagination` today. N: VOC PageXML has Laypa region tags that SG hand lacks |
| P8 | **Bar D (VOC):** test scan-level P ≥ 0.95, R ≥ 0.90 (a scan is predicted if any of its sides is flagged; gold "Table of contents"). Grid of 32 parameter cells (`text_min_chars`, `shape_only_min`, start cue, `gap`); max dev F1 | 22c | Phase-13 D1 thresholds. The gold is scan-level only. All knobs already exist in `INDEX_SIDES_DEFAULTS`; no new rule |
| P9 | **Links on oracle TOC sides; bar E (VOC):** test E ≥ 0.90, where E = locators whose leaf holds a GLOBALISE `START`/`END/START` scan; coverage and chance (+3) reported; OBP silver reported only | 22d | User, 2026-10-31: starts as link targets, **scoring only**. The phase-14/15 E threshold. Oracle sides separate locators and map from the detector |
| P10 | **L8 (VOC locator):** per TOC line, a standalone number or range, else a leading range, else L7; chain-range filter. Fixed at planning, with no variants | 22d | Dev TOCs put folio numbers in short lines or as leading ranges; SG L7 takes the range **end** ("288 a 291." → 291) |
| P11 | **Verdict rule:** `unchanged` if the unchanged configuration passes the VOC bar; `reparameterised` if only a selected configuration passes; `fails on VOC` if none does; `runs, no bar` without gold | 22b–22e | Makes the table mechanical and reviewable |
| P12 | **Stop rules:** each test runs once after `select.json`; a failed bar is a negative result with no further tuning; a replica mismatch (22c) or a changed SG default (22b, 22d) stops the step | all | Phase 13–15 practice |
| P13 | **Library changes are additive, with default = today's behaviour,** checked on an SG volume (19e-a 3444). Fitted VOC values become named constants only when their bar passes; no fitted VOC layer in this phase | 22b–22d | Shared-library rule; D17 versioning |

## Hazards

- **Drive:** inputs and the layer live on T7_Data. A hook denial or `Permission denied` is final. Write only under
  `claude-projects-external/archival-structures/data/text_layer_voc/`. If the drive is unmounted, the layer link is
  dangling: stop.
- **Scan ids:** the layer and the GLOBALISE CSVs have no `.jpg`; the converted GM YAMLs do. GM gold is a scan
  *number*: build `NL-HaNA_1.04.02_<inv>_<NNNN>`. Inventory ids may carry a suffix (`1179B`, `1457A`): sort with a
  (number, suffix) key.
- **Single-page scans:** VOC `Enkel` volumes get `single` sides. `FolioMap` then always uses `pagination` and sets
  `back` to None. `extract_v2._side` still sets `page_side` left/right by box centre.
- **Print mode** would route a VOC volume through the SG 1.10.94 line splitter, which prints "no ID" noise for VOC
  line ids (`line_split.py:617`). The planner's two volumes were `hand` (212 and 172 px).
- **NLP coordination:** starts are scoring only; no AS output may become a start-detection feature; NLP's
  `long_chunk` test pages are not singled out. GM was used by NLP once (report/83, start recall on 21 inventories).
- **Planner-seen data:** 2775 (TOC text), 8023 (flags). Steps must not open `planning/*_peek*`/`*_proto*`.
- **Gold quality:** the GM overview has 22 manual-check notes. 11 volume parts have folio decreasing along scans:
  nested registrations, restarts, or errors; 22b reads all 11.
- **Agent shell:** no `rm -rf`, `nohup` or `&` (use `run_in_background`); zsh, quote `'{workers}'`; no `sed -i`;
  coreshare for more than one worker; `chmod g+w` on local and drive files.
- **Disk:** local 128 GiB free, drive 543 GiB free (2026-10-31); the layer is expected at about 7 GiB.

## Open questions for planning

Settled 2026-10-31 by the user: 2.10.50 later; question 3 dropped; all 546 GM built on the drive; starts as link
targets for scoring. The planner settled P2–P13. None open.

## Review checklist

- [ ] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the reviewer rather than
      taken from the log.
- [ ] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`. No sibling code is touched; 22e edits one NLP
      doc only.
- [ ] Error analysis read individual cases, as listed in each brief, and is written up in the phase report, one
      section per step.
- [ ] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [ ] Every public API change is in `docs/shared_library/changelog.md` (additive; no sibling imports affected).
- [ ] No archive-specific knowledge hardcoded in AS core: VOC values are named parameters or constants.
- [ ] Leakage: gold hashes match the planning hashes; `select.json` precedes `test_done.json` in 22b–22d; no library
      code reads gold or silver; 2775 kept out of the test bars.
- [ ] SG defaults unchanged: 19e-a 3444 objects equal before and after (22b, 22d); `FolioMapParams()`, `FOLIO_MAP_V2`,
      L1–L7.
- [ ] The 22a preset drops every inventory-keyed SG setting; the audit was run unedited.
- [ ] The generalisation table's verdicts follow P11 from the steps' numbers.
- [ ] `Brief defects` entries answered: templates or briefs fixed.

## Hand-off

Reviewed 2026-11-01 (Opus 5.5): **accept with follow-ups**. See [REVIEW](REVIEW.md); the report is final.

One Opus session planned the phase, ran 22a–22e and the review fix 22r as Sonnet subagents (22b and 22c in parallel),
then reviewed them. Every bar reproduces from the reviewer's own runs. The SG defaults give identical outputs to `HEAD`
on 3444.

Result on VOC (1.04.02), against the SG bars:
- the text-layer build carries over unchanged (567 volumes, audit 0 judged);
- the folio map fails F (0.654 best, against 0.95);
- the index-side detector fails D (P 0.067, R 0.393 best);
- index links fail E (0.644 best with L8, against 0.90; after 22r).

22r fixed one scorer defect, the 22d gold grain (F16-R.1). The selection and the verdict are unchanged.

The checklist above is answered item by item in REVIEW.md. The library additions are all off by default:
- the preset `voc_1.04.02_v5sg`;
- `single_foliation`;
- `number_tags`;
- L8.

Release as v0.21.0. The GM and TOC test splits are now spent. The next phase is the user's choice (UA-060).
