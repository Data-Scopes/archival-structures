# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-21, phase-08 review (Opus).

## 1. Text layer v2: what exists now (after phase 07 and 15ir)

- **CLI:** `.venv/bin/as-build-text-layer --preset sg_1.01.02_v2|sg_1.10.94_v2 --pagexml-dir D
  --out-dir O --stage S`. The v2 `--stage all` runs extract (with the print splitter) → layout → types
  → units → wbc → join; each stage also runs alone on a layer (`--layout-params`, `--types-params`,
  `--unit-params` + `--republic-dir`; `wbc`/`join` take `--allow-subset-wbc`, `--allow-partial`,
  `--ignore-sanity` and, for v2, no `--pagexml-dir`). CLI JSON is merged over the preset's dict
  (`{**preset, **cli}`), so a partial JSON keeps the preset's other fields.
- **Schema:** `docs/text_layer.md` is the contract, with sections *Layout tables*, *Type tables*,
  units, join, manifest and *Published SG pilot*. LotA writes its own reader (D18).
- **Presets** (`text_layer/presets.py`):
  - `SG_HAND_V2`: layout `merge=False`; `wbc_pinned` `-=„` default, `-:=„` for 11281, `-„` for
    3349–3351, 3353 and 3354;
  - `SG_PRINT_V2`: `merged_line_split="on"`, `column_override=True`, fitted `unit_params`
    (`indent_frac=0.5, short_frac=0.1, gap_factor=None`), `wbc_pinned` `-=„`;
  - `VOC_V1`: none of the v2 fields. Its output is byte-identical to NLP's v1 (`TOTAL 157 / 157`,
    phase-05 check, command in step 15ir).
- **Layers:**
  - dev layers: `data/text_layer_v2_dev/<step>-<letter>/`, one per run, never overwritten. Sources:
    `15i-src/` (symlinks: 6 tar and 8 local hand inventories); print reads the `shared-data` zips;
  - the published pilot is `15i-b/{hand,print}/` (v0.11.0 code), as
    `shared-data/NL-HaNA_1.01.02/text_layer_v2/` and `NL-HaNA_1.10.94/text_layer_v2/`
    (`coverage: partial`, with a `publication` key in `manifest.json`). It keeps the F07-R.1–R.4
    defects, listed as *Known issues* in `docs/text_layer.md` and LotA §6 (UA-013 (a));
  - `15ir-a/{hand,print}/` is the current code's output (dev only, never published). Compare new
    work against it, not against `15i-b`;
  - never rebuild into a published dir. A new version gets a new name (D17); the full SG run is
    `text_layer_v3`.
- **Audit tooling:** `reports/data/phase-07-sg-units-and-pilot/step-15i/audit.py --layer <base>`
  expects `<base>/{hand,print}/*_text_layer_v2`. `rows116.py --layer <base>` gives the 116-row
  evidence. Both run in under 30 s.
- **pagexml-tools:** still 0.9.0 (UA-007 waits for 0.9.1). Its traps and their workarounds are in
  F07-P.7, F07-15fp.11 and F07-P.9.

## 2. Accepted deviations (phase 07): later briefs must assume these

**Layout** (`analysis/page_structure.py`, `text_layer/layout.py`):
- The merge's line-sort key is (baseline mean y, box left, box top, line id).
- `n_regions_in`/`out` count non-empty regions; openings without text get `split_method="none"`;
  the `PageXMLPage` gets the page box as coords.
- The gutter rule is `auto`: `gap` when ≥ 95% of openings have a gap (3354, 3771–3823, 455, 456),
  else `main_edge` (D6).
- **Hand runs without region merges** (`SG_HAND_V2` `merge=False`, F07-15fq.3). Hand layout regions
  are therefore the legacy regions, split only at the gutter.
- Print regions are split on the line `column` field. The column merge guard is on by default: the
  default `LayoutParams()` changed in 15fq2.
- Furniture: `page_text_bottom` is the lowest line bottom of the page. `metadata["column_split_from"]`
  is a list. `LEGACY_15FP` reproduces 15fp.
- 15ir: column-split pieces are sorted by `_line_sort_key` (`sort_split_lines`, off in
  `LEGACY_15FP`); unsplit (`unchanged`) regions keep XML order (130 jump in 455). The blocked counters
  count pairs of final regions only. `gutter_rule` is validated.

**Types** (`text_layer/types.py`):
- Segments, not lines, carry types. Only three rules split a line: `date_prefix`, `margin_prefix`
  and `pn_suffix`.
- `margin_prefix` cuts at the space nearest the main left edge, within 0.04·`page_w`, and **only
  month-only prefixes** (`margin_cut="month_only"`, D21).
- The main left edge and centre are per main column (`col_min_char_frac` 0.25), keyed by the print
  line `column` (`column_override`).
- Whole date, page-number or year lines skip the margin rule. A year in the top band is
  `running_header`. `maert` is in the month list.
- `page_w` is the scan width, not the page width.
- 15ir: a whole date/pn/year line is never split by rule 1. `rule:pn_line` is kept only on the
  in-chain candidate line; every other pn-shaped band line is `rule:pn_line_offchain`, but its
  `line_type` stays `page_number` (about 90% are not page numbers, F07-15ir.6). Consumers counting
  page numbers must read `type_method`. `TypeParams` validates `margin_cut`, `top_n`, `bottom_n`.

**Units** (`text_layer/units.py`):
- Hand units are layout regions minus furniture, cut at headers. Print units are paragraphs (any
  cue) within a column piece.
- The R5 rule (D13): the unit with the most main text inherits the layout region id. Every legacy
  and layout id of a scan is reserved, and collisions get `w+1`. Since 15ir a unit holding all lines of
  a legacy region whose box it shares takes that id (`claimable`): collisions 0 everywhere in `15ir-a`.
- `doc_id_map` covers every legacy region; empty ones get `unit_id=""` and `link=none`.
- Crosswalk classes: `exact`, `near` (5 px), `split_original` (always the first half), `split_half`
  (IoU ≥ 0.5, added in 15h after looking at 456) and `unmatched`.
- The REPUBLIC paragraphs default path is LotA's `data/paragraphs/NL-HaNA_1.10.94`, hard-coded in
  `units.py`. When the file is missing, the crosswalk is skipped with a warning, and
  `units/params.json` records `republic_dir` (or `null`).

**Word breaks and join** (`word_breaks.py`, `join_v2.py`, `build.py`):
- The `„`-robust statistic exists (`word_break_stat="robust"`) but is not adopted (F07-15hj.2).
- SG sets are pinned (`wbc_pinned`, D22, D23). `sanity_pass` is computed on the pinned default.
  `pooled_rule_sanity_pass` records the unpinned rule, which fails for hand_local and print.
- `wbc` runs per layer root: hand_tar, hand_local and print each get their own corpus default.
  15i built one hand root over all 14 inventories.
- `join_unit` equals `join_region` with `lead_marks=""`. Offsets are exact (`text[cs:ce] ==
  line.text[line_offset:…]`), and `n_offset_violations` checks the full span. A merge can drop two
  trailing break chars (51 rows).
- Manifests keep the history of each stage. The subset guard on `wbc` is on.
- `LayerPreset.join_lead_marks` (`","` in `SG_HAND_V2`, 15ir): a `,` + letter at the start of a merged
  next line is dropped (1,856 in hand, 29 of 30 right). `, ` + letter (65) and `,,` (5) remain.

## 3. Data facts that shape SG work

- Hand 1.01.02 and print 456 have no region labels. 455 has Laypa labels, but most typed regions
  are empty (F07-P.1, F07-P.2). 455 also has 130,737 empty legacy regions.
- Print regions are page-level blocks that interleave both columns (F07-15fp.3).
- The HTR merges a page's date header, first text line and page number into one TextLine
  (F07-P.10).
- Hand line-end words are often a separate region at the line's row (F07-15fq.9).
- Break marks:
  - the hand scribes use `-`, `=` and `„`, and `:` in 11281;
  - `=` is often a compound marker (F07-15hj2.3, F07-15i2.3);
  - `.` is a word break only about half the time (F07-15hj2.2);
  - a repeated mark at the line start is read as `,` (F07-R.2).
- 17,128 merges (5.4%) have an upper-case or digit right side, and 15 of 20 read are wrong. A guard
  for this needs `join.py` (F07-15hj.6).
- The 116-row acceptance: the predicted share free is 0.526, with a bound of 0.560. Of the 51
  artefact rows, 40 persist (26 glue, 14 HTR/OCR). Glue repair is off (UA-003, closed). LotA checks
  the share after re-alignment (D19).

## 4. Open follow-ups for the next phase

From phase 07 (report *Follow-ups*, REVIEW rows 6–15; the fixes reach SG data in `text_layer_v3`):
- From 15ir: a row-tolerant line sort (+564 same-row swaps in 456, F07-15ir.3); sorting `unchanged`
  print regions (needs an R5 decision); retyping off-chain `pn_line` segments (F07-15ir.6); `, ` and
  `,,` lead marks (F07-15ir.9); build `claimable` per scan (quadratic, F07-R.11).
- A `g_upper_right` join guard (F07-15hj.6).
- A role column in the crosswalk, so that a `line_end` id doesn't map to the first half (553 ids).
- The interleave guard can be bridged by a third region.
- Bottom-band years are typed `page_number` (about 40).
- Move the REPUBLIC default path out of the library.
- The pagexml-tools 0.9.1 pin and dropping its workarounds (UA-007).

These follow-ups are phase 09, *Text layer v3 preparation* (phase-08 decision P1), not planned yet.

From phase 08 (reviewed 2026-10-21; hand-off in its `PLAN.md`):
- LotA decides whether to filter duplicates on `adjacent` (0.95 re-shoots; the 9 `deelopname`
  re-shoots are never adjacent) and whether to drop numerals from its lexicon (F08-R.1).
- Phase-10 candidates: re-shoot recall below J 0.6 (F08-R.2); a classifier run under
  `page_duplication` (F08-17b.2).

From the siblings (recorded 2026-10-21):
- NLP: the `text_extent` options are a phase-10 step (UA-014 (a); `plan/PLAN.md` row 10).
- republic `docs/from-republic.md`:
  - §8 items 2–3 are still open: the `ground_truth/README.md` counts are stale (726,737
    resolutions, 121,124 sessions), and no SG `session` converter exists;
  - §10: `session_metadata` start lines miss mid-session meeting days (2,749 candidates; 95
    confirmed). An SG `session` layer built from it is `partial` for meeting days.

## 5. Planning notes: recurring brief defects

- **Protected hashes.** Name the method and the script. Copy
  `reports/data/phase-07-sg-units-and-pilot/step-15i2/protect.py` and set its skip list to the new
  step's own dirs. The format is `size=N mtime path` for every file, plus sha256 of the source
  archives. The method was missing from six phase-07 briefs.
- **Measure every expected number on the layer the step starts from,** not on an earlier one.
  Phase 07 got this wrong for 15g2's change count, 15h's "0 collisions" and 15i2's "3351/3354
  unchanged". When a step combines fixes, measure each number with all of them applied, or say it is
  for one fix alone (15ir: 13 collisions were 0 with fix 1; 11 date lines were 19 over all inventories).
- **The changelog** has no `## Unreleased` heading right after a release; the first step adds it.
- **Name the layer root per sub-layer** (hand_tar, hand_local, print), or the single pilot root.
  `wbc` decisions depend on it.
- **When a step re-runs a downstream stage, say which later briefs name its layer,** and update
  them (rule 10). Phase 07 inserted four follow-up steps mid-phase.
- **Ports carry their rationale.** A brief for a port must list the comments to keep:
  calibration notes and decision IDs. Phase 08 dropped LotA's 100-shingle calibration and the
  P2-12 keep rationale, and the review restored them (F08-R.4). When a step copies a planning
  script and deletes a private name, say which block of the copy to drop (16).
- **The agent shell:** no `rm -rf`, `nohup` or `&`, a new dir per run, and `chmod g+w`. Sibling
  suites run with their own `venv/bin/python`. NLP and LotA import AS from the working tree.
