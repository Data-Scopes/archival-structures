# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-11-17 (phase-18 review); cross-project follow-ups and 5075 2026-11-11.

## 1. What exists now (after phase 18)

- **Text-layer CLI:** `.venv/bin/as-build-text-layer --preset <key> --pagexml-dir D --out-dir O --stage all` (extract →
  layout → types → units → wbc → join; `--*-params` JSON merged over the preset; `--inventories`). Sources:
  `discover_inventories_v2(dir, preset.source_pattern)` (entries fullmatching the pattern, file wins over dir);
  `iter_pagexml` reads zip, tar.gz or a directory. **Schema:** `docs/text_layer.md` (LotA writes its reader, D18).
- **Presets** (`text_layer/presets.py`, the only module with archive constants; registry `PRESETS`): SG v2–v5
  (`sg_1.01.02_v5` = v4 + hand index sides and index-entry units; v4 mode per volume by box height, cut 87.75);
  `voc_1.04.02_v5sg` (SG v5 + plumbing only) and `voc_1.04.02_v6` (+ `¬` break); phase 18's seven `*_v5sg` presets
  (`na_1.05.06`, `na_2.10.50`, `na_2.10.01`, `asdsaa_5023`, `asdsaa_5075`, `asdsaa_89`, `asnda_0114.11`) are the same
  rules plus plumbing. New fields are left out of `to_dict` when `None`.
- **Tables per inventory:** `scans/`, `regions_meta/` (`region_tag`), `lines/` (key (scan_id, line_id); empty lines have
  no type), `layout/lines` (`layout_role` main/extra/empty), `types/segments` (`TYPES`: main_text, page_number,
  running_header, date_header, catch_word, signature_mark, marginalia, index, other; `type_method` names the rule),
  `units/`, `wbc/`, `join/`. Margin call: `rule:extra_role` or a line left of the main column by > 2% of page width.
- **Layers:**
  - SG dev runs `data/text_layer_v2_dev/<run>/` (protected; `19e-a` = v5 dev build, 231 volumes; 233 inventories over
    all runs); SG v5 full run (9,122 volumes) on the drive:
    `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_v5/1.01.02/…_v5/`; the local
    `data/text_layer_v5_full/` holds only 23 volumes' line tables. Published, never rebuilt (D17): v2–v5 pilots.
  - VOC: `data/text_layer_voc_22a/` (v5sg, 567), `data/text_layer_voc_23b/` (v6, 567), `data/text_layer_voc_23a/`
    (40 fresh), all links to the drive.
- **Audit/protect:** `reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer <base>`; protect scripts
  `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip <prefix>` and
  `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT`. pagexml-tools 0.9.1 pinned.
- **Index links (phases 14–17):** `analysis/folio_map.py` (`build_folio_map`, `FolioMap.locate`, `FOLIO_MAP_V2`; VOC
  options off by default), `analysis/index_links.py` (`front_index_run`, locators L1–L9, `volume_links_v2`),
  `analysis/index_pages.py` (`toc_sides`); `scripts/build_index_links.py`; `docs/index_links.md`. Full runs v1/v2 on 412
  SG volumes, unpublished. **Spent gold:** 20c, 21a, all of phase 16's test splits, all of `step-23a/gold/`, phase 18's
  `step-24c/gold/test_labels.tsv` (for `listlike` cells and the layer margin call).
- **Page elements (18):** `analysis/listlike.py` (shared-tools port, defaults unchanged; `LISTLIKE_CELLS`),
  `analysis/page_elements.py` (`read_layer_lines` keyed (scan_id, line_id), `layer_call`, `listlike_records`). Layers
  of the seven new archives in `data/page_elements_layers/24b-a/`; 100 pages (10 strata) with images and dumps; line gold
  in `reports/data/phase-18-page-element-types/step-24c/gold/`.
- **Annotation store:** `datasets/vocabulary.py` (`carrier:`/`generic:`/`position:`/`placement:`/`doctype:`, SegmOnto
  grammar), `datasets/annotations.py` (`ScanAnnotation`, `Element`; `data/annotations/` is the user's, never written).
- **Check pages:** shared-tools `checkpage.py` and `handprint.py` (`na_iiif_bases`, `crop`); IIIF zoom viewers in 23a's
  `build_folio_page.py` and 24c's run-labelling `build_element_page.py` (page-jump dropdown).

## 2. Accepted deviations: later briefs must assume these

**Layout/types/units (phases 07–13):** hand keeps XML order, so two-column hand regions interleave (F09-15j.4); print
sorts by row; gutter rule `auto`. Only `date_prefix`, `margin_prefix` and `pn_suffix` split a line. Hand units = layout
regions minus furniture, cut at headers; print units = paragraphs per column piece. 722 interleaved hand units remain
(F13-19e.5). Index-side detector false positives accepted (rosters, accounts, letters; F13-R.13); lemma headings typed
`marginalia` (F13-R.5).

**Index links (14–15):** F passed on SG fresh gold (62/63); L recall 0.856, A 0.892, E 0.825 failed; entries are v1's
`main_text` units. **VOC (16–17):** the SG rules carry over for the build; folio map (F 0.725 on the user's image gold),
TOC detector (P 0.583, R 0.276), locators (L P 0.556) and `¬` joins (56/60) all failed their bars; no VOC constant.

**Page elements (18):** bar L (`listlike` C0R1 on lines: P 0.950, R 0.575) and bar M (the layer's `marginalia` call:
P 0.259, R 0.872) failed. The margin call is layout role `extra` + a page-margin rule, so second table columns and
register cells count as margin; the layer has no list/table type. Gold conventions (orchestrator rulings during UA-063):
column heads and prose item descriptions of an amount table are `table` (UA-070 a), insertions `addition` (UA-071 a),
remarks about the text `note` + placement. The agent dev gold differs (column heads `form:label`, registers `form`);
form vs table is not stable even within the test gold (F18-R.3). `prose` notes exist only on P045–P060 (F18-R.8).

## 3. Data facts

- **SG 1.01.02.** No region labels in hand volumes; print 1.10.94 regions interleave columns and merge date header,
  first line and page number (F07-P.10). Break marks `-`, `=`, `„`. Hand/print by line-box height (print 65–87 px, hand
  88–182; hand < 100 px = index volumes, F12-R.5). Index pages: index volumes and the front of each *tweede serie*
  register; two styles (page-reference, dated summary). Resolutions foliated on rectos; numbering restarts after the
  index; ~¼ of printed numbers garbled or missing.
- **REPUBLIC segmentation v6** (replaces v1–v5; `docs/from-republic-segmentation.md`) in `shared-data/`: scoring only.
  Only `line_id` is stable across versions; unit ids go through `legacy_id_map`/`v5_id_map`.
- **VOC 1.04.02.** All 567 volumes `hand`; Laypa region tags (`paragraph`, `marginalia`, `header`, `catch-word`,
  `page-number`, `signature-mark`); 146 volumes are openings; folios on rectos with ties, restarts and second
  numberings (18 of 30 image-gold misses are map-side, F17-23c.10); ~half of single-page scans are unnumbered versos
  (F17-23c.11); 42% of fresh TOC pages are enclosure lists without folios.
- **New archives (phase 18 planning, F18-P.1–P.6).** 1.05.06, 2.10.50 (local tarballs), 2.10.01 (drive zips, 4,635
  inventories), AsdSAA 5023/5075/89, AsnDA 0114.11 (local tarballs, one inventory each; AppleDouble `._` members).
  They build with the SG rules; mode by box height, except 2.10.50 (`default`: no region of ≥ 100 characters), and
  handwritten AsdSAA 5023 4 is built `print` (F18-R.11). The types stage calls 10–31% of segments `marginalia`; no
  region structure types (SG, 2.10.01: `Text`; others `untagged`). AsdSAA scan ids are file stems and line ids repeat
  across scans. Dump (XML) order is not reading order on letters and register spreads (F18-24b.6, F18-R.9). Images: NA IIIF for 1.01.02, 1.04.02, 2.10.50, 2.10.01 (not 1.05.06); thumbs on the drive under
  `claude-projects-external/archival-structures/data/thumbs/<institution>/` (AsdSAA 89 with Memorix IIIF `info.json`).
- **The user's existing annotations:** 2.10.50 scan tags (table, cover, title page) in 192 inventories; AsnDA 123
  region tags; dev and sanity use only.

## 4. Open follow-ups

**From phase 18 (phase-19 choice: UA-073).** (1) Split second table columns and register cells from margin blocks
before calling margin (F18-24d.10, F18-R.2). (2) An element-level list/table reader that groups wrapped lines into rows
by column and indent (F18-24d.12); line-feature gaps: digit-initial ledger rows, amounts followed by `—`/`„`, fragment
lines in the window (F18-R.4/.5). (3) Annotation format v2: element ids or a continues flag (interleaved elements),
per-element "cells are prose", a form-vs-table ruling, `letterhead`/`copymark` types (F18-R.3/.8/.9, F18-24c.3).
(4) Mode detection outside SG (F18-R.11). All need fresh gold. Review fix 24r first (bulk_tagging `placement`, VOC-only
region-tag tables, `listlike` docstring). The user wants every shared-tools module in AS (`listlike` done; checkpage,
handprint, coreshare later).

**Cross-project (coordination #2–#12):** RC loses most to layout (reading order on shared rows, gutter-merged lines,
two-column hand pages as one stream, margin notes in the text; AS's hand layer shares it, F09-15j.4). Promised to RS
(#10): public `page_structure._coverage`/`_zero_gaps`, RS's `sort_rows` as an opt-in option when reading order is
planned. RS gold A4–A9 → `ground_truth/NL-HaNA_1.01.02/republic_segmentation_A<N>/`, AS writes `converted/` on first
use (#11). LotA waits for the full SG v5 run in `shared-data/`; hoekr needs a data route (#7).

**NL-AsdSAA 5075 deeds (UA-065 a, a phase-19 candidate in UA-073):** Golden Agents begin/end marks
(`ground_truth/NL-AsdSAA_5075/golden_agents_notarial_coordinates/`) cover 217 inventories (43,034 deeds, 83,289 pages);
precision measurable; `asdsaa_5075_v5sg` needs a pattern for `<inv>_<series>` folders (`docs/staging/NL-AsdSAA_5075_golden_agents.md`).

**VOC (from phases 16–17):** a folio map that answers from the numbering around a scan before a global chain, on new
image gold (F17-23c.10; bar F at 0.95 is out of reach for map rules alone: ceiling ~0.91); locators with a page-level
folio cue (F17-23e.3/.5) on larger `loc=yes` link gold (F17-R.2); TOC features for dated registers and enclosure lists,
or a decision that `loc=no` lists are not index sides (F17-23d.3, F17-R.1); a `¬` filler rule (F17-R.3); VOC unit order
(F16-22a.7, F17-R.5); layered sheets (F16-R.7/.8).

**SG (from phases 13–15):** index-links v3 with image-checked link gold (F15-21a.9), a G1 fix (F15-21d.8), entries with
`marginalia` lines or headings (F15-21c.6/.7), HTR-split folio numbers (F15-R.3); a detector false-positive guard
(F13-R.13); column order v3 (F13-R.10); continuation cuts after amounts (F13-19g.4); one-column and garbled indexes
(F13-19c.5); 3681/3601, dated indexes, separate index volumes. Each ships as a new layer version (D17). **Older:**
F12-15yr.4, P29, F09-R.5, F09-15l.5, F08-R.1, print REPUBLIC crosswalk (455/456).

## 5. Planning notes: recurring brief defects

- **Protected hashes:** protect scripts before and after any step that writes data; name expected changes (other
  projects' `shared-data/`); without a pair the review diffs against the last after-list.
- **Expected numbers** are measured on the starting layer under the step's exact definition; a failed ratio gives
  numerator and denominator before a cause (F10-R.1). One output prefix per subcommand; one root per sub-layer.
- **Changelog:** the first step after a release adds `## Unreleased` (Edit only). **Ports** carry their rationale and run
  the original once at planning (F08-R.4); say whether a rule reads the line or the segment text.
- **Agent shell:** no `rm -rf`, `nohup`, `&` or `$TMPDIR`; a new dir per run; `chmod g+w`; zsh (quote `'{workers}'`);
  never `git stash`/`checkout`/`reset`; BSD `sed -i ''`; macOS `zcat` fails on `.gz`. Sibling suites: LotA
  `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider` (609 passed, 2026-11-17), NLP the same with
  `tests` (66). If git reports dubious ownership, the AS provenance test fails: rerun it with `GIT_CONFIG_COUNT=1
  GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=<AS root>`; scripts reading relative paths from a table resolve
  them against the AS root or the brief names the cwd (F18 24d).
- **Run every command a brief quotes once at planning** (a "does not read" claim is checked); a run kit's audit knows
  the new layer's checks (F13-R.16). Run every new option or criterion once on dev to show it can move the measure
  (F16-R.5); check scan-layout assumptions beyond the smoke volumes.
- **Hand-read bars:** fixed before reading; one verdict file; an `unclear` rule; unreadable input outside the
  denominator (F12-R.15); the reader's context named; draws sized on the expected yield; **gold is spent once a variant
  is scored on it** (F13-R.9); a user check page states the label definitions (F13-R.15).
- **Agent-read gold** (phases 17–18): the orchestrator runs reader agents (steps cannot); the rubric names hard cases
  first (F17-R.4); readers *must* open the image and the check script asks for it (24c). An agent self-check misses
  convention errors (F18-R.12); rulings made during a user review go onto the check page as fixed tokens, and record
  from which page they apply (F18-R.8/.9). A probe claim about a build stage is checked on the real build (F18-P.1);
  eligibility caps are checked per stratum (F18-24b.3); a per-archive table filters on the archive, not on a non-empty
  column (F18-R.7). Parallel steps edit `STATUS.md` rows and the report with Edit only; regenerating a sampled-read file must
  not overwrite filled-in read columns.
- **Briefs** match the plan decision they operationalise and never assume an earlier bar passed (F14-R.2); a blocking UA
  says what the step does meanwhile; new `scripts/` files need a `!/scripts/<name>` line in `.gitignore` (F14-R.6).
- **Gold and joins:** state the row grain and how to collapse it (F16-R.1); side-level gold says where text without a
  line goes (F15-R.2); join lines on (scan_id, line_id) (F18-P.4); validate fitted models on held-out units (F12-R.18).
- **Layout causes** are checked against the layout tables (F13-R.1); on a new series read every stage's output that can
  fail (F12-R.7); selected volumes are not a sample (F12-R.5). **Shared data:** read through a link under `data/`;
  publishing there follows a Decide UA (write access, sibling links, `rm -r` the exact path, `chmod -R g+w`, changelog).
