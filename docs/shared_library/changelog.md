# Shared-library changelog

Changes to archival-structures' public API or packaging that affect the sibling projects
(language-of-the-archive = LotA, nlp-start-page-detection = NLP). Steps refer to
`upstream_inventory.md` §4. Newest first.

## v0.18.0 (2026-10-28)

### Step 19e: text layer v5 preset and run kit

- `text_layer.presets.SG_1_01_02_V5` and `PRESETS["sg_1.01.02_v5"]`: `SG_1_01_02_V4` plus a hand override (`index_sides`
  `SG_INDEX_SIDES_V5`, `index_entry` `SG_INDEX_ENTRY_V5B`; no column reorder, UA-043 (a)). No existing preset changes.
- `scripts/run_text_layer_v4_full.py`: `--version {v4,v5}` (default `v4`, behaviour unchanged), `ARCHIVES_BY_VERSION`
  (`ARCHIVES` stays the v4 map); `link_archive(..., version="v4")` gains a keyword; the coreshare label is
  `<version>-<archive>`; `--version v5` with 1.10.94 is an argparse error.
- Phase-13 review: the run script's final audit (`AUDIT`) is now `reports/data/phase-13-register-index-volumes/step-19e/
  audit.py`, the 15w audit plus the v5 checks on a `*_v5` layer (unchanged checks for v4 layers).
- Affected sibling imports: none (LotA reads published layers only; a v5 pilot is UA-046).

### Step 19g: index entry rule v2

- `text_layer.units.INDEX_ENTRY_DEFAULTS` gains `"no_cut_pattern": None` (a main line that fully matches the regex fires no
  entry cue except `header`, in `entry_cues`) and `"main_types": ["main_text"]` (line types that become `main_text` on an index
  side in `split_region`, entry mode only). Both defaults keep today's behaviour; `UnitParams.index_entry` accepts the new keys.
- `text_layer.presets.SG_INDEX_NO_CUT` and `SG_INDEX_ENTRY_V5B` (cues outdent, date_start, end_ref_prev, short_prev; no-cut guard;
  `main_types` main_text + index). No preset uses it yet.
- Affected sibling imports: none.

### Step 19f: column order v2

- `analysis.page_structure.HAND_COLUMN_SPLIT_DEFAULTS` gains `"mode": "pieces"` (default = 19b's behaviour, unchanged) and
  `"span_reach_frac": 0.5`. Mode `"reorder"` keeps a two-column hand region whole and re-orders its lines, left column then
  right column, classed by start x (new metadata key `column_reorder_from`; no new units). `LayoutParams.__post_init__` raises
  `ValueError` for a `mode` outside `("pieces", "reorder")`.
- `text_layer.layout._change`: new value `column_reordered` in `layout/regions.change`.
- `text_layer.presets.SG_HAND_COLUMN_ORDER_V5` (no preset uses it).
- Affected sibling imports: none.

### Step 19d: index entry units

- `text_layer.units.UnitParams.index_entry: Optional[dict] = None` (new field, `hash=False`); `None` = off. Keys are those of
  `INDEX_ENTRY_DEFAULTS`; unknown keys or cues outside `INDEX_CUES` raise `ValueError`. `to_dict()` leaves the key out when it is
  `None`, so every existing `units/params.json` stays byte-identical.
- New `units.INDEX_CUES`, `units.INDEX_ENTRY_DEFAULTS` and `units.entry_cues(mains, header_between, cfg)` (cues `outdent`, `indent`,
  `short_prev`, `date_start`, `end_ref_prev`, plus `header`).
- `units.split_region(segs, params, print_mode, index_side=False)` and `units.build_units_page(..., index_side=False)`: only with
  `index_side` true, not print mode and `params.index_entry` set are the mains cut at the entry cues. New `unit_method` values
  `hand:index_start` and `hand:index:<cue>+<cue>` (only then); otherwise unchanged.
- `unit_inventory` gives every segment a `text` key (no output change). With `params.index_entry` set (hand) it reads
  `layout/pages/<inv>.tsv.gz` (step 19c) and raises `FileNotFoundError` when it is missing.
- `text_layer.presets.SG_INDEX_ENTRY_V5` (constant, variant E5; its test bar failed, no preset uses it).
- Affected sibling imports: none.

### Step 19c: index page sides

- New module `analysis.index_pages`: `INDEX_SIDES_DEFAULTS`, `side_features(lines, cfg)`, `side_rule(feat, cfg)`, `fill_runs(flags, gap, min_run)`
  (per page side: share of long lines in the right part, share of lines ending in a number or starting with a date, and a contiguity fill).
- `analysis.page_structure.LayoutParams.index_sides: Optional[dict] = None` (new field, `hash=False`); `None` = off, `{}` = defaults.
  `to_dict()` leaves the key out when it is `None`, so every existing `layout/params.json` stays byte-identical. Unknown keys raise `ValueError`.
- `text_layer.layout.PAGE_COLUMNS` and the table `layout/pages/<inv>.tsv.gz` (one row per page side: features, `rule`, `index_side`), written
  only when `index_sides` is set; `n_index_sides` is added to the layout stats only then.
- `text_layer.presets.SG_INDEX_DATE_START` and `SG_INDEX_SIDES_V5` (constants; no preset uses them yet).
- Affected sibling imports: none.

### Step 19b: hand column split

- `analysis.page_structure.LayoutParams.hand_column_split: Optional[dict] = None` (new field, `hash=False`); `None` = off, `{}` = all
  defaults. `to_dict()` leaves the key out when it is `None`, so every existing `layout/params.json` stays byte-identical.
  Unknown keys raise `ValueError`.
- `analysis.page_structure.HAND_COLUMN_SPLIT_DEFAULTS` and `split_regions_into_columns(regions, cfg, *, id_fn, row_tol_frac)
  -> (regions, n_split)`: geometric split of regions with two side-by-side columns (pieces carry `column_split_from`).
- `text_layer.layout.layout_scan` calls it after the `split_on_line_field` block when `hand_column_split is not None`; pieces count
  in `n_column_splits` and show as `change == "split_at_column"`.
- Affected sibling imports: none (no preset sets it yet; bar B1 failed, see UA-040).

## v0.17.0 (2026-10-28)

### Step 15y: mode pins and in-place rebuild

- `text_layer.modes.decide_mode(line_rows, preset, inv=None)`: new optional `inv`; `volume_mode["pins"]` ({inv: "hand"|"print"})
  sets `mode` to the pin and `method` to `pinned`, the measured result stays in `box_height_mode`/`box_height_method`. A pin value
  outside `hand`/`print` raises `ValueError`. `presets.MODE_PINS_V4 = {"3463": "hand"}` is the pin set of `sg_1.01.02_v4`: **a
  behaviour change** (3463 is built `hand` instead of `print`; 106 wrong splits disappear). The published v4 pilot has had 3463
  pinned since 2026-10-27 (UA-037 (a), UA-038: `15y-b` copied over it).
- `text_layer.build`: the v2 stages `layout`, `types` and `units` now skip inventories whose done row is current (new
  `V2_UPSTREAM`, `is_current`, `v2_todo`; `Run.status()` rows carry `_seq`); `wbc` parts and `join` also need a row newer than the
  stage they read. A changed `params.json` redoes the whole stage. v1 (VOC) paths are byte-identical.
- Stale files are removed: a forced extract without split deletes `split_model/<inv>.json` and `split_stats/<inv>*.json`;
  `units.unit_inventory` deletes `units/republic_lines/<inv>.tsv.gz` when it writes no crosswalk.
- `scripts/run_text_layer_v4_full.py`: `--rebuild ARCHIVE=F`, `THROUGHPUT_V4` model (replaces `WORKER_SECONDS_PER_XML_BYTE`,
  `SLOW_FACTOR`), `predict(archive, sizes, want, min_workers)`, `build_cmds(..., rebuild_file=None)` returns `(estimate, [cmds])`.
- Affected sibling imports: none (NLP's `scripts/build_voc_text_layer.py` calls `build.main` with the v1 preset).

### Step 15z: v4 mode pins

- `sg_1.01.02_v4` `MODE_PINS_V4` gains 15 pins (3433, 3436, 3439, 3442, 3445, 3466, 3469, 3472, 3475, 3489, 3726, 4792, 12297 -> `hand`;
  12163, 12165 -> `print`) from read scans (`reports/data/phase-12-hand-print-box-height/step-15z/verdicts.tsv`: at least 4 of 5 read scans of
  the other class); 16 pins with 3463. Behaviour change of the preset: those volumes are built in the pinned mode. Of the 16, only
  3463 is in the published v4 pilot (pinned there since UA-038); the full layer on the drive has all 16. Affected sibling imports: none.

### Step 15yr: staleness gate, recorded sizes, extract timing

- `text_layer.build.upstream_ok(status, inv, stage)` (new): in the v2 stages `layout`, `types`, `units`, `wbc` and `join`, an
  inventory whose upstream stage (at any depth of `V2_UPSTREAM`) has a last row that is not `done` is **blocked**: not redone, no
  status row, files untouched, logged as `N blocked`. Fixes a failed forced extract followed by `--stage all` rebuilding the later
  stages on stale lines (F12-R.19). v1 paths unchanged.
- `build.recorded_sizes(status_path)` (new) and size reuse in `build.main` (v2 presets, non-local stages): the sizes of a source
  file come from the last `done` extract row when the file size equals the recorded `input_bytes`; log line `[sizes] N from
  status.tsv, M measured`. `--estimate` is unchanged.
- `modes.extract_inventory_v2_modes`: the result gains `prepass_seconds`, and `seconds` now times the whole call (pre-pass
  included), so v4 extract rows in `status.tsv` are about twice as long as before (F12-R.17).
- `scripts/run_text_layer_v4_full.py`: no longer calls `--estimate`; `build_cmds(...)` returns the list of commands (was
  `(estimate, cmds)`); `THROUGHPUT_V4` extract entries scaled by the pre-pass factor; new `whole_layer_seconds`.
- Audit (`step-15w/audit.py`): `modes`, `counts` and the invariant tags are keyed `<archive>/<inv>`; `done_inventories` uses the
  last row per stage. `scripts/stage_text_layer_v4_full.py` accepts `[ARCHIVE/INV]` tags and `ARCHIVE/INV` items and mirrors
  deletions (`--max-deletions`, default 100).
- Affected sibling imports: none (NLP's `build_voc_text_layer.py` uses the v1 preset).

## v0.16.0 (2026-10-27)

### Step 18a: `text_extent` region types and margin gaps

- New in `archival_structures.analysis.text_extent` (additive, UA-014 (a)): `line_region_type(line)`;
  `BODY_REGION_TYPES_VOC` (named example, not a default); kwarg `region_types=None` on
  `compute_page_text_extent(page, equal_tol, region_types)` and `compute_corpus_text_extents(pages, region_types,
  equal_tol)`; `margin_gaps(row, stable)`; `text_extent_table(df, stable, top_tol, bot_tol)` (columns `text_extent`,
  `top_gap`, `bot_gap`). Defaults leave all outputs unchanged. Identical to NLP report/84 on its 22 inventories.
- Bug fix (only change to existing output): `compute_corpus_text_extents([])` now returns all eight columns
  (`n_lines, top, bot, left, right, mean_width, equal_frac`), not five.
- Affected sibling imports: NLP `scripts/early_end_extract_body.py` (`_rtype`, `BODY`, own body extent code) can later
  use `line_region_type`, `BODY_REGION_TYPES_VOC` and `compute_page_text_extent(..., region_types=...)`; none changed yet.

### Step 18b: label-free segmentation (C10)

- New in `archival_structures.stream_analysis.overview.subsequence_detection` (also lazily exported from
  `archival_structures.stream_analysis`): `segment_dp`, `calibrate_length_penalty_weight`, `textiling_depth_scores`,
  `predicted_start_rate`, `MAX_SEGMENT_LENGTH`, `starts_to_boundaries`, `boundaries_to_starts`. Ported verbatim from NLP
  `startpage.unsupervised_segmentation`; identical output on NLP's 22 inventories (0 mismatches, 41,810 / 41,810 rows).
- Convention: the segmenters return segment start indices (always including 0), unlike the gap indices of
  `detect_boundaries_*`; use `starts_to_boundaries` to convert. No existing function changed.
- Affected sibling imports: NLP `from startpage.unsupervised_segmentation import segment_dp, textiling_depth_scores,
  calibrate_length_penalty_weight, predicted_start_rate, MAX_SEGMENT_LENGTH` (later shim step); none changed yet.

### Step 18c: separator-sheet rule (C12)

- New module `archival_structures.analysis.separator_sheets`: `is_separator_sheet`, `detect_sheets`, `next_scan_id`,
  `apply_rule`, `VOC_SHEET_PATTERN`, `VOC_MAX_SHEET_CHARS`, `SHEET_CONVENTIONS`. Ported from NLP
  `startpage.separator_sheets`; identical output on the 20,912 NLP pages (0 mismatches).
- Deliberate differences from NLP: `apply_rule` raises `ValueError` for an unknown convention (NLP treated any value
  other than "sheet" as "next"); ids that do not parse as scan ids have no next page, so their unit is skipped;
  sheets are processed in sorted order, so two consecutive sheets give the same result in every process (NLP iterates
  the set, whose order depends on `PYTHONHASHSEED`; the 22 NLP inventories have no consecutive sheets; phase-11
  review).
- Affected sibling imports: NLP `from startpage.separator_sheets import ...` (later shim step); none changed yet.

## v0.15.1 (2026-10-26)

- Text-layer types stage (`archival_structures.text_layer.types.type_inventory`) no longer fails on `parse_error` scan rows
  (empty `page_w`). No signature change. Affected sibling imports: none.

## v0.15.0 (2026-10-26)

### Step 15q: hand_print volume-mode classifier

- New module `archival_structures.analysis.hand_print` (pure Python plus numpy): `VolumeModeParams`, `region_geometry`,
  `region_features`, `scan_value`, `scan_label`, `fit_thresholds`, `volume_mode`, `volume_mode_from_features`, and the
  constants `SCAN_LABELS`, `MODES`, `MODE_METHODS`. Nothing existing changes. Status: the 15q stop rule fired (the
  rule is not accurate enough on the out-of-sample silver volumes), so nothing uses the module yet.
- Affected sibling imports: none.

### Step 15u: hand_print box-height rule

- `archival_structures.analysis.hand_print` gains (additive): `BoxHeightParams` (`cut` required, `min_region_chars`),
  `volume_box_height`, `scan_box_heights`, `fit_box_height_cut`, `box_height_mode`, and the constant
  `BOX_HEIGHT_METHODS`. `MODE_METHODS` and the 15q functions are unchanged. The cut is a parameter; the module holds no
  archive-specific value.
- Affected sibling imports: none.

### Step 15v: volume-mode build (v4)

- `text_layer.presets`: `LayerPreset` gains the last fields `mode_presets` and `volume_mode` (default `None`); new
  `MODE_RULE_FIELDS`, `BOX_HEIGHT_V4`, `inventory_preset(preset, mode)`, the presets `SG_1_01_02_V4`, `SG_1_10_94_V4`
  (`PRESETS` keys `sg_1.01.02_v4`, `sg_1.10.94_v4`). v1-v3 presets are field-for-field unchanged.
- `text_layer.line_split`: new `prepass_rows(inv, source, preset)` (raw line rows, tags, scan info); `fit_split_model`
  calls it, same result. New private `_write_split_files`, shared by `extract_inventory_v2_split` (unchanged behaviour).
- New `text_layer/modes.py`: `decide_mode`, `write_mode`, `read_mode`, `extract_inventory_v2_modes`.
- `text_layer.build`: with a mode preset, `extract` writes `mode/` tables and the later stages use each inventory's mode;
  `layout/`, `types/` and `units/` `params.json` and the manifest `*_params` entries are `{"hand": ..., "print": ...}`
  (v4 layers only). Output of non-mode presets is byte-identical.
- Affected sibling imports: none (LotA reads no `params.json`, manifest `*_params` or `mode/`).

### Step 15w: v4 run kit and audit

- New `scripts/run_text_layer_v4_full.py` (not part of the package): the v3 hand-off script by archive
  (`--src ARCHIVE=DIR`, `--republic-dir ARCHIVE=DIR`, `--inventories-file ARCHIVE=F`, `--only ARCHIVE`), log `run_v4.log`.
  `scripts/run_text_layer_v3_full.py` is unchanged.
- New audit `reports/data/phase-12-hand-print-box-height/step-15w/audit.py`: discovers sub-layers and inventories, adds the
  v4 `mode/` invariants. No package API change; no sibling import is affected.

### Step 15wr: split gate for single-column print volumes

- New `text_layer.modes.column_start_share(line_rows, min_chars=20, min_rows=50, mid_frac=0.4) -> (share, n_rows)`
  (`(None, n_kept)` when no page side has `min_rows` qualifying lines) and `text_layer.presets.SPLIT_GATE_V4`. `sg_1.01.02_v4.volume_mode` gains `split_gate`: a print-mode volume is split only if
  its column-start share is at least 0.35 (or unmeasurable). `mode/<inv>.json` gains `split_gate` for those volumes.
  `sg_1.10.94_v4`, hand volumes and all other presets are unchanged. Layers built before (`15w-a`) differ in the gated
  volumes (no `split_model/`, `split_stats/`, `split_from`).
- Audit `step-15w/audit.py`: split-model and `split_from` invariants follow the gate.
- Affected sibling imports: none.

### Step 15x: drop-cap-aware print units

- `text_layer.units.UnitParams` gains `drop_cap_lines` (default `None`; omitted from `to_dict` when `None`). With it
  set, an `indent`-only print cut within that many main lines of the last kept cut is dropped.
  `text_layer.presets.LayerPreset` gains `mode_overrides` (last field; per mode, rule fields from `MODE_RULE_FIELDS`
  that replace those of the mode preset; other keys raise `ValueError`); `inventory_preset` applies it and returns
  `mode_overrides=None`. `sg_1.01.02_v4` sets `drop_cap_lines=2` for print-mode volumes (unpublished preset; behaviour
  change: 22.9% fewer `main_text` units in the *gedrukte resoluties*). v1-v3 and `sg_1.10.94_v4` output unchanged;
  `units/params.json` of those layers is unchanged.
- Note (15x review): `dataclasses.asdict` of a `LayerPreset` now also has the key `mode_overrides` (`None` outside
  `sg_1.01.02_v4`), and `dataclasses.asdict(UnitParams)` has `drop_cap_lines`; only `UnitParams.to_dict` leaves it out.
  Code that compares presets field by field must drop the new key (as `test_v1_v3_presets_unchanged` does).
- Affected sibling imports: none.

## v0.14.1 (2026-10-23)

### Step 15n: hand-off script for the full `text_layer_v3` run

- New `scripts/run_text_layer_v3_full.py` (not part of the package). It runs the v3 build with inputs and output on
  an external drive. No public API change.
- `as-build-text-layer --republic-dir` help: the option has no default since 15k, and the text no longer names LotA's
  dir. No sibling import is affected.

## v0.14.0 (2026-10-22)

### Step 15m: v3 presets

- New presets `text_layer.presets.SG_HAND_V3` and `SG_PRINT_V3`, registered in `PRESETS` as `sg_1.01.02_v3` and
  `sg_1.10.94_v3` (CLI `--preset`). Every field equals the v2 preset except `name`
  (`NL-HaNA_1.01.02_text_layer_v3`, `NL-HaNA_1.10.94_text_layer_v3`) and `description`; `format_version` stays 2. The
  v2 presets are unchanged. Not pinned: pagexml-tools 0.9.1 is not on PyPI (2026-10-22).
- Affected sibling imports: none.

### Step 15l: continuation marks and the upper-case guard

- `text_layer.presets.LayerPreset` has two new fields: `join_lead_marks_lower: Optional[Mapping[str, str]] = None` (per
  inventory, keys an inventory or `"default"`) and `join_upper_guard: bool = False`. `SG_PRINT_V2` sets
  `join_lead_marks_lower={"455": "J"}`; `join_upper_guard` stays off in both SG presets (bar not met).
- `text_layer.join_v2.join_unit(..., lead_marks_lower="", upper_guard=False)` and
  `join_inventory_v2(inv, layer_root, wbcs, lead_marks="", lead_marks_lower="", upper_guard=False)`; defaults give the
  old output. New boundary kind `g_upper_right` (v2 only, only with `upper_guard`). `join/stats/<inv>.json` gains
  `lead_marks_lower` and `upper_guard`; `build.stage_join` re-joins when they differ.
- **Behaviour change (hand output):** `lead_marks` now also strips `,,` and `, ` before a letter (70 merges in `15k-a`).
  The print 455 `join/units` and `join/segments` change too (2,172 `J` strips). `join.py` and `ALL_GUARDS` are unchanged.
- Affected sibling imports: none (no sibling imports `join_unit` / `join_inventory_v2` / these preset fields).

### Step 15k: page-number types, crosswalk role, units

- `text_layer.types.TypeParams` has three new fields: `offchain_retype: bool = False`, `offchain_other_bands: tuple = ()`
  (validated, subset of `("top", "bottom")`, `ValueError`) and `offchain_years_other: bool = False`. Defaults leave the
  output unchanged. `from_dict` turns a list `offchain_other_bands` into a tuple. New `type_method` values
  `rule:pn_line_gap` (stays `page_number`) and `rule:year_offchain` (`other`); with `offchain_other_bands` a
  `rule:pn_line_offchain` segment can have `line_type` `other`.
- **Behaviour change:** `SG_HAND_TYPES_PARAMS` and `SG_PRINT_TYPES_PARAMS` (`text_layer.presets`) switch `offchain_retype` on
  (years `other` in both, bottom band `other` in print). Layers built with the SG presets differ from earlier ones in the
  values of two `types/segments` columns (`line_type`, `type_method`) and the units derived from them.
- `text_layer.units`: new `read_republic_roles(path)`; `republic_crosswalk(..., roles=None)` appends a `role` column and maps
  a `split_original` id with role `end` to the last half; `REPUBLIC_COLUMNS` gains `"role"`; `units/stats.json` gains
  `republic_crosswalk_roles`. `units.REPUBLIC_PARAGRAPHS_DIR` is removed (no sibling imports it, checked 2026-10-21).
  `unit_inventory` builds `claimable` per scan (same output).
- **Print builds now need `--republic-dir`:** without it the units stage logs a warning, writes `republic_dir: null` and no
  `republic_lines/`. The AS code no longer names a LotA path.
- Affected sibling imports: none.

### Step 15j: row-tolerant line order

- `analysis.page_structure.LayoutParams` has three new fields: `line_sort: str = "row"` (`"baseline"` | `"row"`),
  `row_tol_frac: float = 0.2` (validated, `0 < x < 1`) and `sort_unchanged_lines: bool = False`. `LEGACY_15FP`
  sets `line_sort="baseline"`.
- New functions `sort_lines_by_row(lines, row_tol_frac)` and `sort_lines(lines, params)`; new kwarg
  `row_tol_frac=None` on `merge_regions` and `split_regions_on_line_field` (`None` = the old baseline sort).
- **Behaviour change:** the default `LayoutParams()` now sorts the lines of merges and column-split pieces by
  printed row (same-row fragments left to right). Print output changes (455 +240 sorted regions reordered, 456
  +892). `text_layer.presets.SG_PRINT_LAYOUT_PARAMS` (new) sets `sort_unchanged_lines=True` for `sg_1.10.94_v2`.
- `text_layer.layout`: new `layout/regions` column `lines_resorted`, new `layout/scans` column and stat
  `n_regions_resorted` (appended at the end of `LAYOUT_REGION_COLUMNS`, `LAYOUT_SCAN_COLUMNS`, `STAT_KEYS`).
- Affected sibling imports: none (no sibling imports these names, checked 2026-10-21).

## v0.13.0 (2026-10-21)

Phase 08 (region quality and duplicates), reviewed 2026-10-21. LotA `corpus/quality.py` and
`corpus/integrity.py` import from this release's new modules, so LotA needs v0.13.0 or the AS
working tree.

### Step 16: region-quality filter (C16)

- New module `analysis.region_quality`, a port of LotA `corpus.quality`: `QUALITY_CLASSES`,
  `DEFAULT_KEEP_CLASSES`, `is_kept`, `FEATURE_NAMES`, `QualityThresholds` (`as_dict`, `from_dict`),
  `DEFAULT_QUALITY_THRESHOLDS`, `QualityVerdict`, `lexicon_lookup_form`, `region_features(text, *, lexicon,
  bbox=None)`, `classify_features`, `score_region(text, ...)` and `quality_frame(df, ...)`. It takes text and an
  injected lexicon (a set of lower-case words), not LotA's `Region`.
- Output is identical to LotA's: 600/600 gold rows and 428,544 regions (200 inventories) reproduce.
- Affected sibling imports (LotA only, backwards compatible): `archival_language/corpus/quality.py` now imports
  `QUALITY_CLASSES`, `DEFAULT_KEEP_CLASSES`, `is_kept`, `QualityVerdict`, `QualityThresholds`,
  `classify_features`, `region_features` from AS; its public names and signatures are unchanged (`_clean_token_for_lookup`
  and `_clip` are removed; nothing imported them). NLP: none.

### Step 17: duplicate-page detector (C17)

- New module `analysis.duplicate_pages`, a port of LotA `corpus.integrity` duplicate detection: `shingles`,
  `minhash_coefficients`, `minhash_sketch`, `lsh_candidates`, `duplicate_page_candidates(pages, *, threshold,
  adjacency, shingle_length, min_shingles, num_hashes, bands, seed)`, the `DEFAULT_*` constants and
  `CANDIDATE_COLUMNS`. It takes plain `(page_id, text, position)` triples; adjacency is by position (page at `p` pairs
  with every page at `p + 1`), never by sequence order.
- Output is identical to LotA's: the full SG run (9,122 inventories) gives a byte-identical candidate table (48,473
  rows) and manifest.
- Affected sibling imports (LotA only, backwards compatible): `archival_language/corpus/integrity.py` now imports
  `DEFAULT_DUPLICATE_THRESHOLD` and `duplicate_page_candidates` from AS; `duplicate_candidates` keeps its signature and
  its 11 columns. LotA's private `_shingles`, `_page_shingles`, `_minhash_*`, `_lsh_candidates` and the `_SHINGLE_*`,
  `_NUM_MINHASHES`, `_LSH_*`, `_MINHASH_*` constants are removed (nothing imported them). NLP: none.
- Behaviour change in LotA `duplicate_candidates` (added at review): it now raises `ValueError` for a
  threshold outside [0, 1] or a repeated page id, where it used to run silently. The full SG run hits
  neither.

### Step 17b: page_duplication operator

- New `stream_analysis.corruption.page_duplication(stream, provenance, rate, rng)` (Case 5,
  re-shoot; per-page rate), exported from the package; `'page_duplication'` appended to
  `CASE_NAMES` (last).
- `observed_start_labels` now labels only the first occurrence of a canonical index as a start.
  Output is identical for the four existing operators (none repeats an index).
- `simulate_inventory` with the default `cases=CASE_NAMES` now also runs `page_duplication`, so a
  default run writes a fifth case's outputs (added at review).
- Affected sibling imports: none (no sibling uses `stream_analysis.corruption`).

## v0.12.0 (2026-10-21)

### Step 15ir: phase 07 review fixes

No sibling imports change signature (`grep -rn "page_structure\|text_layer"` over LotA and NLP finds no import of
the names below). The **output of the v2 SG stages changes** (counts below); every fix is gated so that the v1
stages and `LEGACY_15FP` are byte-identical (v1 check 157/157).

- `analysis.page_structure`: `LayoutParams.sort_split_lines: bool = True` (`LEGACY_15FP` sets `False`) and
  `split_regions_on_line_field(..., *, sort_lines=False)`: with `sort_lines`, each piece of a column-split region
  is sorted by `_line_sort_key`; an unsplit region is returned as the same object, unchanged. `LayoutParams`
  validates `gutter_rule` (`gap`, `main_edge`, `auto`) and raises `ValueError`.
  `merge_overlapping_text_regions` fills `blocked` / `column_blocked` with the pairs of the **last** merge pass
  only (pairs of final regions); the layout tables are unchanged, but `n_guard_blocked` / `n_column_blocked` fall
  (455: 1,322 -> 769 and 62 -> 39; 456: 673 -> 659 and 113 -> 107).
- `text_layer.types`: `TypeParams` validates `margin_cut` (`month_only`, `all`, `off`), `top_n >= 1`,
  `bottom_n >= 1` (`ValueError`). A line that fullmatches `date_line`, `pn_line` or `year_line` is no longer split
  by `rule:date_prefix` (19 lines in the pilot). Every `rule:pn_line` segment that is not the in-chain page's
  candidate line becomes `rule:pn_line_offchain` (pilot: `rule:pn_line` 10,327 -> 8,826, `_offchain` 1,792 ->
  3,293); `line_type` is unchanged. Consumers that count `rule:pn_line` must read both tags.
- `text_layer.units`: `build_units_page(..., taken, claimable=None)`: legacy ids that are not layout region ids
  of the scan and whose lines all lie in a unit are taken by that unit (`n_id_collisions` 455 58 -> 0, 456 32 -> 0
  together with the order fix; the `doc_id_map` `identical` count rises by 46 / 32). `unit_inventory` builds
  `claimable`. `units/params.json` gains `republic_dir`; a missing REPUBLIC file is logged in print mode
  (`build.stage_units`).
- `text_layer.join_v2`: `join_unit(texts, wbcs, guards, both_ends_fix, lead_marks="")` and
  `join_inventory_v2(inv, layer_root, wbcs, lead_marks="")`; `lead_dropped` is the true number of dropped leading
  characters (a second leading break char the library strips is counted, fixing a latent offset error for `„„`);
  `join/stats/<inv>.json` has `lead_marks`; `n_offset_violations` checks the full span (the removed tail must be at
  most two break chars on a merge, none otherwise). With the default `lead_marks=""` the text still equals
  `join.join_region`.
- `text_layer.presets.LayerPreset.join_lead_marks: str = ""`; `SG_HAND_V2` sets `","` (11182 repeats the break mark
  at the start of the next line and the HTR reads a comma). `build.stage_join` passes it for v2 and re-joins when
  `lead_marks` in the stored stats differs. With `--allow-partial`, a v2 inventory without a decision takes its
  pinned set (`preset.wbc_pinned`) instead of the corpus default.
- Names missing from earlier entries: `text_layer.units.parse_republic_id`, `read_republic_ids`,
  `load_segments`, `REPUBLIC_PARAGRAPHS_DIR`, `FURNITURE_TYPES`, `HEADER_TYPES`, `NEAR_PX`, `SPLIT_HALF_IOU`
  (step 15h); `text_layer.build.decide_v2` (15hj, 15hj2); for v2 presets `--pagexml-dir` is optional in `--stage wbc`
  and `--stage join` (they read the unit tables).
- Docs: `docs/text_layer.md` (stage list, `type_method`, `join_lead_marks`, *Known issues* of the published
  `text_layer_v2`). The published copies in `shared-data/` are not rebuilt (UA-013 (a)); the fixes go into
  `text_layer_v3`.

## v0.11.0 (2026-10-21)

### Step 15i2: hand_tar WBC pin without =

Preset data only; no signature change, no sibling import changes. `SG_HAND_V2.wbc_pinned` adds `-„` for
3349, 3350, 3351, 3353 and 3354 (UA-011 b); other inventories and `SG_PRINT_V2` are unchanged. A rebuilt
hand layer has fewer `=` merges in 3349, 3350 and 3353 (`join/` differs there only).

### Step 15hj2: pinned WBC sets

Additive; v1 output is byte-identical (`TOTAL 157 / 157`). No sibling import changes.

- `LayerPreset.wbc_pinned: Optional[Mapping[str, str]] = None` (`{"default": chars, "<inv>": chars}`);
  `SG_HAND_V2` pins `-=„` (11281: `-:=„`), `SG_PRINT_V2` pins `-=„`.
- `build.decide_v2` applies the pin (`mode = pinned`, `corpus_default.json` gets `pinned`,
  `derived_default_set`, `pooled_rule_sanity_pass`; `sanity_pass` is checked on the pinned default set);
  the manifest `wbc` entry gets `pinned`. With `wbc_pinned=None` all output is unchanged.

### Step 15hj: v2 word breaks and join

Additive; v1 output is byte-identical (`TOTAL 157 / 157` on the 22 VOC inventories). No sibling import
changes. **Behaviour changes of the driver:** `--stage wbc` exits for a strict subset of the layer's
inventories unless `--allow-subset-wbc` (v1 and v2); `manifest.json` gets a `history` entry when a stage
is rerun; a v2 `--stage all` now runs extract (+ wordfix) -> layout -> types -> units -> wbc -> join, and
`wbc`/`join` no longer exit with status 2 for v2 presets.

- New module `text_layer/join_v2.py`: `join_unit(texts, wbcs, guards, both_ends_fix) -> (text, spans,
  boundaries)`, `read_unit_texts`, `write_word_break_stats_part_v2`, `join_inventory_v2`, the column lists.
- `text_layer/word_breaks.py`: `robust_counts(lines, chars)`; `word_break_stats(..., robust=False)`.
- `LayerPreset`: `word_break_rule`, `word_break_stat`, `wbc_sanity_in`, `wbc_sanity_out`
  (SG presets: `wbc_sanity_in=()`).

### Step 15fq: layout fixes

Additive in code; no sibling import changes (the 15fp layout stage has no sibling user yet).
The **default behaviour of the layout stage changes**: use `LEGACY_15FP` (as `--layout-params`
JSON) to reproduce the 15fp tables on the 15fp columns.

- `analysis.page_structure`: `LayoutParams` has 13 new fields (`gutter_rule`, `gap_share_min`,
  `long_line_frac`, `min_long_lines`, `edge_quantile`, `split_on_line_field`,
  `furniture_patterns`, `furniture_max_lines`, `furniture_max_chars`, `catchword_band`,
  `interleave_guard`, `interleave_width_frac`, `merge`), `LayoutParams.from_dict`, and the
  constant `LEGACY_15FP`. New functions `inventory_line_width_rel`, `choose_gutter_rule`,
  `find_main_edge_x`, `split_regions_on_line_field`, `furniture_region_ids`.
  `find_separation_x(..., *, rule="gap", lines_xw=None, med_w_rel=None)` (default unchanged);
  `get_overlapping_region_sets(..., *, exclude, guard_width_frac, blocked)` and
  `merge_overlapping_text_regions(..., *, exclude, blocked)` (defaults unchanged, but the
  default `LayoutParams` now has the interleave guard on).
- `text_layer.layout`: `layout_scan(..., *, gutter_rule, med_w_rel, blocked_out)`; new table
  columns (scans: `gutter_rule, n_column_splits, n_furniture, n_guard_blocked`; regions:
  `furniture, column_value`); `change` gains `split_at_column`, and 15fp's `merged+split` is
  now `merged+split_at_gutter`.
- `text_layer.presets.LayerPreset.layout_params`; the SG presets set the furniture patterns, and
  `sg_1.01.02_v2` sets `merge=False` (hand merge precision 0.52 in the 15fq error analysis).
  `--stage layout` merges preset and `--layout-params` (the latter wins).

### Step 15g: line and region types

Additive; no sibling import changes (the types stage has no sibling user yet).

- New module `text_layer/types.py`: `TypeParams` (frozen dataclass, `to_dict`, `from_dict`),
  `TYPES`, `TYPE_SEGMENT_COLUMNS`, `TYPE_REGION_COLUMNS`, `segment_page`, `type_inventory`.
- `text_layer.presets.LayerPreset.types_params` (default `None`); the SG presets set the type
  patterns (`SG_TYPE_PATTERNS`), the print preset also `column_override=True`.
- `text_layer.build`: `--stage types` (v2 presets only, not part of `all`) and `--types-params`.
  Tables: `types/segments/<inv>.tsv.gz`, `types/regions/<inv>.tsv.gz`, `types/stats.json`,
  `types/params.json`.

### Step 15fq2: column merge guard

Additive in code; no sibling import changes (`grep -rn page_structure` over the siblings'
`*.py` finds nothing). The **default behaviour of the print layout output changes**: regions
whose lines carry different non-empty `column` values are never merged (`LEGACY_15FP` has the
guard off).

- `analysis.page_structure`: `LayoutParams.column_merge_guard: bool = True`;
  `get_overlapping_region_sets(..., column_field="", column_blocked=None)` and
  `merge_overlapping_text_regions(..., column_blocked=None)` (the keyword defaults are unchanged, but the default
  `LayoutParams()` applies the column guard, so the default output changes; corrected in step 15ir).
- `text_layer.layout`: scans table column `n_column_blocked` (appended last); summed in
  `layout/stats.json`; `column_merge_guard` appears in `layout/params.json`.

### Step 15g2: margin cut month-only

Additive in code; the **default types output changes**. No sibling imports affected.

- `text_layer.types.TypeParams.margin_cut: str = "month_only"` (`"all"` = step 15g, `"off"`).
  Under the default, a `rule:margin_prefix` cut is made only when the prefix fullmatches
  `month_only`; other lines stay whole (18,552 lines in the 16 SG inventories; 423 month cuts
  remain). The field appears in `types/params.json`.


### Step 15h: units and doc_id mapping

Additive; no sibling import changes (the units stage has no sibling user yet).

- New module `text_layer/units.py`: `UnitParams` (frozen dataclass, `to_dict`, `from_dict`),
  `split_region`, `build_units_page`, `paragraph_features`, `cues_fired`, `doc_id_map_rows`,
  `republic_crosswalk`, `unit_inventory`, the table column lists.
- `text_layer.presets.LayerPreset.unit_params` (default `None`); `SG_PRINT_V2` sets
  `SG_PRINT_UNIT_PARAMS` (`indent_frac=0.5, short_frac=0.1`).
- `text_layer.build`: `--stage units` (v2 presets only, not part of `all`), `--unit-params`,
  `--republic-dir`. Tables: `units/{units,segments,doc_id_map,republic_lines}/<inv>.tsv.gz`,
  `units/stats.json`, `units/params.json`.

## v0.10.0 (2026-10-19)

### Step 15fp: page layout

Additive; existing stages and their output are unchanged. No sibling import changes.

- New module `analysis.page_structure` (pdm objects, no text-layer imports): `LayoutParams`,
  `get_overlapping_region_sets`, `merge_regions`, `merge_overlapping_text_regions`, `fit_gutter`,
  `find_separation_x`, `split_scan_into_pages`, `group_regions_into_columns`,
  `assign_column_roles`, plus the helpers `region_box_id`, `hull_coords`, `is_opening`,
  `region_chars`. Ports republic's overlapping-region merge (made deterministic, repeated to a
  fixed point) and its column criterion.
- New module `text_layer.layout`: `layout_inventory(inv, layer_root, params)`, `layout_scan`
  and the column lists `LAYOUT_SCAN_COLUMNS`, `LAYOUT_REGION_COLUMNS`, `LAYOUT_LINE_COLUMNS`.
- `as-build-text-layer`: new v2 stage `--stage layout` (`--layout-params`; `--pagexml-dir` not
  needed). Writes `layout/{scans,regions,lines}/<inv>.tsv.gz`, `layout/stats.json` and
  `layout/params.json`; not part of `--stage all`. Schemas in `docs/text_layer.md`.

### Step 15fr: phase 06 review fixes

- `text_layer.line_split.derive_split_params` has a new keyword `merged_width_factor`, default
  **1.19** (was a fixed 1.22). The `sg_1.10.94_v2` line tables change: more lines are split
  (counts in the phase-06 report, addendum 15fr). No sibling import changes.
- `manifest.json` of every layer (also `voc_v1`) has a new key `format_version` (1 or 2), since
  step 15d. The `voc_v1` tables are unchanged.
- Step 15e also made these names public: `SplitModel`, `SplitHook`, `fit_split_model_from_rows`,
  `read_line_rows`, `is_candidate_row`, `training_docs_from_lines`.

### Step 15f: glued-word repair

Backwards compatible; `voc_v1` output is byte-identical (`TOTAL 157 / 157`). No sibling import
changes.

- New module `text_layer.word_split`: `WordSplitParams`, `token_core`, `build_vocabulary`,
  `is_candidate`, `segment`, `maybe_segment`, `repair_line`, `apply_word_split`. Only inserts spaces.
- `text_layer.presets.LayerPreset`: new fields `word_split` (default `"off"`) and
  `word_split_params`. Both SG presets carry the dev-chosen parameters and stay `"off"`: hand-read
  precision of real repairs was 0.62 (print) and 0.16 (hand), under the 0.90 bar.
- `as-build-text-layer`: new v2 stage `--stage wordfix` (`--word-vocab`, `--word-params`;
  `--pagexml-dir` not needed for it). `--stage all` runs it after extract only when
  `preset.word_split == "on"`, so default output is unchanged.

### Step 15e: merged-line splitter

All additions are backwards compatible; `voc_v1` output is byte-identical (checked:
`TOTAL 157 / 157`). No sibling import changes. New core dependency: `nltk (>=3.9)`.

- New module `text_layer.line_split`: verbatim port of republic-project's
  `long_line_splitter.py` (one crash fix, see its docstring) and an adapter:
  `SplitParams`, `derive_split_params`, `train_bigram_finder`, `make_pmi_scorer`,
  `merged_line_candidates`, `fit_split_model`, `make_split_hook`, `extract_inventory_v2_split`.
  The module imports pagexml at module level; `text_layer/__init__` does not import it.
- `text_layer.presets.LayerPreset`: new field `split_region_tags` (default `()`);
  `SG_PRINT_V2` sets it to `("index", "resolution", "untagged", "Text")`.
- `as-build-text-layer --preset sg_1.10.94_v2` now splits merged lines during extract and
  writes `split_model/` and `split_stats/`; the v2 line tables of print inventories change
  (halves `<id>_L`/`<id>_R`, `column`, `edit_flags`, `split_from` filled). `sg_1.01.02_v2` output
  is unchanged.

### Step 15d: text layer v2 extract

All additions are backwards compatible; `voc_v1` output is byte-identical (checked:
`TOTAL 157 / 157` on the 22 NLP inventories). No sibling import changes.

- `text_layer.io.iter_pagexml(source, *, skip_resource_forks=False)`: new `.tar.gz`/`.tgz`
  sources; the keyword is new and the default keeps the old behaviour. `input_bytes` also
  accepts `.tar.gz`/`.tgz`. New `discover_inventories_v2(pagexml_dir, pattern)`.
- `text_layer.presets.LayerPreset`: new fields `format_version`, `archive_id`, `source_pattern`,
  `skip_resource_forks`, `merged_line_split` (defaults leave `VOC_V1` unchanged); new presets
  `sg_1.01.02_v2` and `sg_1.10.94_v2` in `PRESETS`.
- New module `text_layer.extract_v2`: `extract_inventory_v2`, `extract_scan_v2`,
  `legacy_doc_id`, `legacy_region_text`, `SCAN_COLUMNS_V2`, `REGION_COLUMNS_V2`,
  `LINE_COLUMNS_V2`.
- `as-build-text-layer` with a v2 preset runs `extract` only (`--stage all` = extract);
  `--stage wbc|join` exits with status 2.

## v0.9.0 (2026-10-15)

### Step 13: pagexml-tools 0.9.0

- Dependency pins only; no public function changes. `pagexml-tools (>=0.9.0,<1.0.0)` (was
  `>=0.8.0`) and `fuzzy-search (>=3.0.0,<4.0.0)` (was `>=2.5.0`). 0.9.0 imports
  `fuzzy_search.analysis.similarity`, which exists only in fuzzy-search 3.x, so AS can no longer
  run on fuzzy-search 2.x.
- Affected sibling imports: none changed. NLP's `voc_text_layer` and LotA's `derive_voc`
  import `pagexml.helper.pagexml_helper`, which now imports in both (their editable checkout is
  at the 0.9.0 release); their dist-info metadata still says 0.8.0.
- Output of the text layer is unchanged (157/157 against NLP's `voc_text_layer_v1`).
- Behaviour that the new floor forces on any caller of pagexml-tools (reviewed in phase 05):
  `pagexml.model.physical_document_model.vertical_distance` no longer returns the
  baseline-bottom offset for baseline-bearing lines (AS's `line_vertical_gap` was already
  correct; one AS test that pinned the old value now asserts 0), and `column_parser`'s derived
  columns now wrap their lines in a `PageXMLTextRegion`. AS calls neither.
- `text_layer.provenance.MIN_PAGEXML_TOOLS` is `(0, 9, 0)` (step 15r), matching the pin.

### Step 14: text-layer I/O and word-break detector (C14)

- New package `archival_structures.text_layer` (additive; no pagexml or pandas import at module level).
  - `text_layer.io`: `SANITISE_PATTERN`, `sanitise`, `format_box`, `write_tsv_gz`, `read_tsv_gz`,
    `write_text_atomic`, `write_json_atomic`, `write_plain_tsv`, `discover_inventories`,
    `input_bytes`, `iter_pagexml`, `read_region_lines(layer_root, inv)`,
    `inventory_sort_key` (via `InventoryNumber`, non-numbers last).
  - `text_layer.word_breaks`: `DEFAULT_WORD_BREAK_RULE`, `SANITY_IN`, `SANITY_OUT`,
    `word_break_stats(regions, inventory_number)`, `inventory_word_break_stats`,
    `WORD_BREAK_STATS_COLUMNS`, `format_stat`, `write_word_break_stats_part`, `pooled_stats`,
    `rule_passes`, `decide_word_breaks`, `read_word_break_sets`, and the helpers
    `is_candidate_char`, `char_label`, `mid_line_vocabulary`, `ends_attached`,
    `strip_leading_marks`, `joined_word`, `first_non_wbc`.
- Output equals NLP's `voc_text_layer` on v1: decision files, stats file and 52/52 stats parts
  byte-identical; sort order equal on 6,914 names.
- **NLP (switched in step 15b; old names kept by the shim):** `inventory_wbc_stats(inv, out_root)` ->
  `inventory_word_break_stats(inv, layer_root)`, `wbc_stats_part` -> `write_word_break_stats_part`,
  `decide_wbc` -> `decide_word_breaks`, `load_wbc_sets` -> `read_word_break_sets`,
  `WBC_RULE` -> `DEFAULT_WORD_BREAK_RULE`, `WBC_STATS_COLUMNS` -> `WORD_BREAK_STATS_COLUMNS`,
  `_fmt` -> `format_stat`.

### Step 15: text-layer library (C15)

- New modules in `archival_structures.text_layer` (additive; pagexml is imported inside functions,
  no pagexml or pandas import at module level):
  - `extract`: `LINES_COLUMNS`, `META_COLUMNS`, `DEFAULT_UNTAGGED_TAG`, `ROW_ORDER_STATEMENT`,
    `iter_text_regions`, `region_tag_of(region, counts, untagged_tag=...)`,
    `scan_id_matches(scan_id, inv, prefix=None)`,
    `extract_inventory(inv, source, out_root, *, untagged_tag=..., scan_id_prefix=None)`.
  - `join`: `REGION_COLUMNS`, `EVENT_COLUMNS`, `SPEC_GUARDS`, `ALL_GUARDS`, `library_join`,
    `classify_boundary`, `join_region`, `join_vocabulary`, `join_inventory(inv, out_root, wbcs,
    write_events=False)`.
  - `provenance`: `MIN_PAGEXML_TOOLS`, `package_info`, `pagexml_tools_info`, `check_min_version`,
    `check_pagexml_tools_version`.
  - `presets`: `LayerPreset`, `VOC_V1`, `PRESETS`, `predict_seconds(xml_bytes, stage, fit)`,
    `predict_wall(per_inventory_xml_bytes, workers, fit, stages=...)`. VOC constants live only here.
- Output equals NLP's `voc_text_layer` on the 22-inventory reference (157/157 files identical) and
  on 20 more local inventories (140/140 equal against a copy of the original module).
- **Behaviour differences from NLP's module:** the scan-id check is `scan_id_matches` (parses with `ScanId(digits=None)`, so any scan-number width and a checked prefix; ids that do not parse, such as `..._1186_0571A.jpg` and `..._9817_P01.jpg`, fall back to NLP's `_{inv}_` substring test); the untagged tag, the scan-id
  prefix and the throughput fit are arguments instead of module constants.
- **NLP (switched in step 15b; old names kept by the shim):** `extract_inventory(inv, source, out)` ->
  `extract_inventory(..., untagged_tag=VOC_V1.untagged_tag, scan_id_prefix=VOC_V1.scan_id_prefix)`,
  `UNTAGGED_TAG` -> `DEFAULT_UNTAGGED_TAG`, `THROUGHPUT_FIT`/`OUTPUT_BYTES_PER_XML_BYTE` ->
  `VOC_V1.throughput_fit`/`.output_bytes_per_xml_byte`, `predict_seconds(b, stage)` ->
  `predict_seconds(b, stage, fit)`, `predict_wall(b, w, stages)` -> `predict_wall(b, w, fit, stages)`,
  `pagexml_tools_info`, `check_pagexml_tools_version` from `text_layer.provenance`.
- **NLP switched (step 15b, 2026-10-14):** `src/startpage/voc_text_layer.py` is a shim over
  `archival_structures.text_layer.{io,word_breaks,extract,join,provenance,presets}` (all 62
  top-level names kept; `decide_wbc`, `predict_seconds`, `predict_wall`, `extract_inventory` are
  thin wrappers passing `VOC_V1` values), and `scripts/build_voc_text_layer.py` is a wrapper around
  `text_layer.build.main`. `scripts/validate_voc_text_layer.py` and `tests/test_voc_text_layer.py`
  are unchanged and pass. New `scripts/verify_as_text_layer.py` in NLP. No AS signature changed.

### Step 15c: text-layer driver and CLI

- New (additive): `archival_structures.text_layer.build` (`main(argv=None) -> int`, `Run`, `STAGES`,
  `STATUS_COLUMNS`, `select_inventories`, `run_pool`, `stage_extract`, `stage_wbc`, `stage_join`,
  `estimate`) and the console script `as-build-text-layer` (same flags as NLP's
  `scripts/build_voc_text_layer.py`, plus `--preset`, default `voc_v1`; the layer directory is
  `<out-dir>/<preset name>/`). `archival_structures.text_layer.compare` (`TSV_KINDS`, `JSON_KINDS`,
  `RUN_FILES`, `compare_layers`, `compare_run_files`, `summarise`, `main`). Neither imports pagexml
  at module level; `compare` imports pandas lazily.
- **Behaviour differences from NLP's script:** `FORCE` global is `Run.force` and `is_done(status,
  inv, stage, force)`; each manifest stage entry gains `archival_structures` and `preset`, and its
  `git_head` is archival-structures' HEAD, not the calling project's.
- **NLP (switched in step 15b; old names kept by the shim):** `scripts/build_voc_text_layer.py` becomes a wrapper around
  `archival_structures.text_layer.build.main`; `vtl.wbc_stats_part` ->
  `word_breaks.write_word_break_stats_part`, `vtl.decide_wbc` -> `word_breaks.decide_word_breaks`,
  `vtl.load_wbc_sets` -> `word_breaks.read_word_break_sets`, `vtl.WBC_STATS_COLUMNS` ->
  `word_breaks.WORD_BREAK_STATS_COLUMNS`, `vtl._fmt` -> `word_breaks.format_stat`.

### Step 15r: phase 05 review fixes

- `analysis.sequence_patterns.segment_into_elements` now also treats NaN/`pd.NA` labels as
  unlabelled (pandas 3 stores a `None` in a str Series as NaN). Behaviour under pandas 2 is unchanged.
- `text_layer.provenance.MIN_PAGEXML_TOOLS` is `(0, 9, 0)`, so `as-build-text-layer` and NLP's
  `build_voc_text_layer.py` refuse to run when the pagexml-tools metadata is older than 0.9.0.
- Affected sibling imports: NLP `startpage.voc_text_layer.MIN_PAGEXML_TOOLS` and
  `check_pagexml_tools_version` (re-exported by the shim).

## v0.8.0 (2026-10-14)

### Step 12: page numbering and GLOBALISE loader (C11)

- New module `archival_structures.analysis.page_numbering` (additive; numpy and pandas only):
  `AlignmentConfig(mode, runs, extrap, dup_first)`, `CONFIDENCE_BANDS`,
  `longest_increasing_chain`, `main_chain`, `numbering_restarts`, `numbering_mode`,
  `interpolate_folio_to_scan`, `confidence_band`, `align_documents`,
  `align_all(..., *, inventory_column="inventory_number")` (no `n_sections` column; raises
  `ValueError` on a non-unique index), `anchors_array`, `page_number_anchors` (anchors from a
  text-layer region table).
- New module `archival_structures.datasets.globalise_index`: `GLOBALISE_INDEX_COLUMNS`,
  `load_globalise_index(path, inventories=None, columns=...)`.
- Output equals NLP's `globalise_alignment` for 6/6 configs over the whole index.
- **NLP (step 12b):** `Config` -> `AlignmentConfig`, `load_index` -> `load_globalise_index`,
  `lis_indices`/`main_chain`/`extra_runs`/`numbering_mode`/`_interp`/`_band`/`align_inventory`/
  `align_all` -> the names above (the shim adds back `n_sections`), `scan_no` -> `scan_number`.
- **NLP switched (step 12b, 2026-10-14):** `startpage/globalise_alignment.py` is now a shim over these
  modules; the 9 CSVs and manifest of `scripts/build_globalise_alignment.py` are byte-identical to the
  stored ones and to a run of the original module (`scripts/verify_as_globalise_alignment.py`).

### Step 11: scan-id helpers (C13)

- New module `archival_structures.model.scan_id` (additive, nothing existing changes):
  `ScanId` (frozen, ordered dataclass: `institution`, `archive`, `inventory`, `number`, `ext`;
  `prefix`, `from_prefix`, `format(*, digits=4, ext=None)`, `with_number`,
  `parse(scan_id, *, digits=4, prefix=None)`, `try_parse`), `InvalidScanIdError`,
  `scan_number(scan_id, *, digits=None)`, `parse_doc_id(doc_id, *, marker="-text_region-")`,
  constants `NA_INSTITUTION`, `DEFAULT_DIGITS`.
- **LotA:** `archival_language/ead/adapters.py` now imports `ScanId` and `_BaseAdapter.scan_id` /
  `parse_scan_id` delegate to it; same signatures and results (`.jpg` ids still give `None`).
- NLP: `globalise_alignment.scan_no` now delegates to `scan_number` (step 12b); it rejects ids that
  do not follow `NL-HaNA_<archive>_<inv>_<NNNN>`; none of 64,505 anchor, 269,383 spine and 21,623
  annotation rows is rejected.

### Step 10: sequence decoders (C9)

- New package `archival_structures.stream_analysis.decoding`, ported from NLP's
  `startpage/semi_markov.py`, `crf_sequence.py`, `bilstm_sequence.py` and the generic parts of
  `scripts/train_multitask_crf.py` / `train_semi_markov_duration.py`. Not exported from
  `stream_analysis`. `semi_markov` names (`EPS`, `fit_lognormal_duration`, `log_duration_pmf`,
  `decode_semi_markov`) import eagerly (numpy + scipy); `crf` (`NEG_INF`, `CRFSequence`,
  `LinearChainCRF`, `start_score_from_emissions`, `collapsed_start_f1`, `viterbi_start_f1`,
  `train_crf`, `decode_core`) and `bilstm` (`BiLSTMTagger`) resolve lazily, so importing the
  package never imports torch.
- Differences from the NLP originals: `decode_semi_markov` returns `list[bool]` (NLP: 0/1
  ints) and raises `ValueError` for `max_duration < 1` or a `log_pmf` shorter than
  `max_duration`; `log_duration_pmf` raises `ValueError` (NLP: `assert`) when there is no
  mass; `_lognormal_cdf` uses `scipy.special.ndtr` (bit-identical to `norm.cdf`);
  `start_score_from_emissions(model, features, start_states=(0,))`, `collapsed_start_f1` and
  `train_crf(..., n_states=3, start_states=(0,), seed=0, *, lr, max_epochs, patience)` take
  state ids instead of NLP's constants and string labels; `decode_core` returns bool labels.
  `START/INSIDE/END` stay in NLP. Same weights and F1 as NLP for the same seed (see report).
- `scipy>=1.10` is now a core dependency in `pyproject.toml` (`poetry.lock` not regenerated).
- **Affected sibling imports:** NLP's `startpage.semi_markov`, `crf_sequence`,
  `bilstm_sequence` and the scripts' training loop (step 10b, below; the shim converts the bool
  output back to 0/1).
- **NLP switched (step 10b):** `src/startpage/semi_markov.py`, `crf_sequence.py` and
  `bilstm_sequence.py` re-export from AS; `scripts/train_crf_sequence.py` (`dev_f1`, `train_crf`),
  `scripts/train_multitask_crf.py` (`collapsed_f1`, `dev_f1`, `train_crf`, `decode_sequences`) and
  `scripts/train_semi_markov_duration.py` (`start_score_from_emissions`) delegate to
  `viterbi_start_f1`, `collapsed_start_f1`, `train_crf`, `decode_core` and
  `start_score_from_emissions`. Stored NLP outputs reproduce byte for byte
  (`scripts/verify_as_decoders.py`). NLP names and signatures are unchanged.

## v0.7.0 (2026-10-14)

### Phase 03 review fix: `intra_document_swap` provenance (bug fix)

- `corruption.operators.intra_document_swap` swapped the two units but left their provenance
  entries in place, so `provenance[k]` no longer described `observed[k]` and
  `observed_start_labels` did not follow a moved start page. The entries now move with their
  units. `swap_and_insertion` inherited the bug and is fixed with it. The other operators were
  already correct. For the same seed the corrupted stream is unchanged; only the provenance
  differs. **Corrupted-stream YAMLs written with `intra_document_swap` or `swap_and_insertion`
  before this fix carry wrong provenance and should be regenerated.** New test:
  `test_provenance_points_at_the_unit_in_its_position` checks all four operators, 5 seeds each.
- **Affected sibling imports:** none (no sibling uses the corruption operators).

### Step 7: segmentation evaluation suite (C6)

- New package `archival_structures.stream_analysis.evaluation` (`segmentation`, `aggregate`),
  ported from NLP's `startpage/evaluation.py`; import from the package, not from
  `stream_analysis` (no lazy export, because `window_diff` clashes with the corruption one).
  Labels are `bool` (`check_labels` raises `TypeError` otherwise). `default_k(labels,
  convention="gaps"|"pages")`; `pk`/`window_diff`/`evaluate_all` take `k=None, *,
  k_convention="gaps"`. NLP's numbers need `k_convention="pages"`. Length mismatches raise
  `ValueError`; `WindowClippedWarning` is emitted once per call when k is clipped.
  `build_eval_frame(..., sequences, sep="/")` renames `scanname` to `page_id`, takes
  `{inventory: [page_id, ...]}` and raises on duplicate scan ids.
- **Behaviour change in `corruption.metrics`:** `score_start_classification`,
  `score_same_document_pairs` and `window_diff` now raise `TypeError` on non-bool labels.
  `window_diff` keeps its strict `ValueError` contract; values are unchanged. `_to_gaps` removed
  (private).
- **Affected sibling imports:** none yet. NLP's `startpage.evaluation` becomes a shim in step 07b
  (string labels, `scanname` column, tuple-shaped `full_sequences`, `k_convention="pages"`).
- **Step 07b (NLP, no AS change):** NLP's `startpage.evaluation` is now a shim over this package.
  It keeps 17 names: `START`, `build_eval_frame`, `page_level_prf`, `page_level_prf_with_counts`,
  `iter_contiguous_runs`, `tolerance_prf`, `default_k`, `pk`, `window_diff`, `segment_exact_match`,
  `PerInventoryResult`, `per_inventory`, `start_rate_calibration`, `bootstrap_ci`,
  `bootstrap_pooled_f1`, `evaluate_all`, `format_report` (plus private `_tolerance_prf_runs`). It
  converts string labels to bool, extracts page ids from tuple-shaped `full_sequences`, renames
  `page_id` back to `scanname`, and passes `k_convention="pages"` explicitly (D3). The 21 NLP
  importers are unchanged; `scripts/verify_as_evaluation_shim.py` shows the stored numbers are
  reproduced exactly.

### Step 8: chunk splits and validators (C7)

- New module `archival_structures.stream_analysis.evaluation.splits` (also re-exported from
  `evaluation`). Ported verbatim from NLP's `long_subsequence_chunk_split.py`: `Page`,
  `Document`, `Chunk`, `SubSequence`, `LongSequenceSplit`, `parse_documents`,
  `partition_file_into_chunks`, `assign_splits_and_pad_context`,
  `build_archival_dataset_splits`, `build_subsequences`, `validate_subsequences`. `print`
  became `logging` (`info` for stats, `warning` for the duplicate NOTE); P2 edge behaviour
  (10-slot striding, greedy rebalance) is unchanged and documented.
- New: `pages_from_starts`, `build_long_sequence_split(archival_files, *, ...)`,
  `build_inventory_held_out(sequences, held_out=None, seed=0, n_held_out=5)`, `core_rows`,
  `eval_scan_ids`, `SplitValidationError` (an `AssertionError`), `check_no_duplicate_page_ids`,
  `check_no_document_crosses_partitions`, `check_context_not_scored`, `check_split_coverage`,
  `check_nested_split`.
- **Affected sibling imports (NLP):** `startpage/long_subsequence_chunk_split.py` re-exports the
  AS names and keeps `TRAIN_FRAC`, `DEV_FRAC` and `build_long_sequence_chunk_split` (same
  signature; the VOC end-id rule is replaced by page adjacency, identical on all 22
  inventories); `startpage/splits.py` private helpers (`_check_duplicate_page_ids`,
  `_assert_no_document_split_across_splits`, `_validate_split`, `_select_held_out` returns
  `None` for seed >= 1, `_build_inventory_held_out`) call AS. All seven cached split CSVs
  rebuild byte-identically (`scripts/verify_as_splits.py`). NLP's stdout lines "Total Chunks
  Generated", "Split Distribution" and the duplicate/gap NOTEs now go to `logging`.

### Step 9: trivial baselines (C8)

- New module `archival_structures.stream_analysis.evaluation.baselines` (re-exported from
  `evaluation`): `majority`, `every_k`, `length_prior`, `oracle_every_k_own_rate` (an ORACLE,
  not a legitimate predictor), `train_start_rate`, `BaselineRun`, `baseline_suite`,
  `baseline_floors`. Bool predictions; `length_prior` reproduces NLP's `default_rng` loop seed
  for seed.
- New: `evaluation.aggregate.flatten_metrics(results)` (NLP's 23 metric columns, verbatim) and
  `corruption.start_classification_floors(observed, provenance, canonical, **kwargs)`
  (re-exported from `corruption`; imports `baselines` lazily).
- **Affected sibling imports (NLP):** `scripts/run_trivial_baselines.py` imports
  `baseline_suite`, `eval_scan_ids`, `flatten_metrics`, `train_start_rate` from AS, drops its
  local copies and gains `--out PATH`. It reproduces the stored CSV in 344 of 360 cells (16
  random bootstrap cells excluded). `scripts/verify_as_evaluation_shim.py` now takes the
  pre-refactor helpers from `output/as-phase-03-originals/run_trivial_baselines.py`.

## v0.6.0 (2026-10-14)

### Step 6: `split_unittitle_dates` (C4); LotA `lb_adjusted` retired

- New function `archival_structures.parsers.ead_fields.split_unittitle_dates(unittitle_element)
  -> (prose, spans)`: one span per direct `<unitdate>` child (`None` for an empty one), into
  `element_text(unittitle_element)`; `prose` is that text with the spans cut out. Purely
  additive in AS.
- `parsers/ead_text.py` gets a private hook (`_SPAN_START`, `_SPAN_END`, `_raw_marked_text`; the
  `<lb/>` no-space-before regex ignores the markers). `element_text` and `raw_element_text` are
  unchanged.
- **LotA imports changed:** `facets/title_norm.py` calls `ead_fields.split_unittitle_dates`. Deleted
  there: `_structural_date_spans`, `_structural_text_and_prose`, `_collapse_with_map`,
  `_unitdate_children`, `_retag_ranges`, the sentinel constants and the regex fallback
  (`date_locator == "fallback"` can no longer occur). **Removed:** `NormalisedTitle.lb_adjusted`,
  the `lb_adjusted` column of `title_frame` / `*-normalised-titles.tsv.gz`, and the census column
  `n_lb_adjusted` (`cli/normalise_titles.py`). All other title columns and census cells are
  unchanged on the 9 archives (0 changed cells).

### Step 5: scope-note parser (C3) with cross-archive references

- New module `archival_structures.parsers.ead_scope_notes`: `ScopeNote`, `ScopeReference`,
  `ShapeRule`, `NA_SHAPE_RULES`, `classify_shape(text, tags, rules=NA_SHAPE_RULES)`,
  `ReferencePatterns`, `NA_REFERENCE_PATTERNS`, `descendant_tags`, `LB_TAG`,
  `parse_scope_notes(component, archive_id, *, source_path, tags, shape_rules, patterns)`,
  `iter_scope_notes(finding_aid, ...)` and `scope_notes_frame(finding_aid, ...)`. Purely
  additive in AS.
- `ScopeReference` gains `target_archive: str | None` (None = the current archive). The NA
  inventory pattern reads an optional archive code before the cue (`Zie Aanwinsten, 1.11.01.01,
  inv.nr. 687`), and the number list stops before an archive code that starts a new reference.
  **Behaviour change vs LotA's old parser:** such references now carry `target_archive` and must
  not be joined to the current archive's inventory.
- `target_archive` is also None when the written code *equals* the `archive_id` passed in
  (string equality). That holds for LotA's SG and VOC, whose ids are the bare NA codes
  `1.01.02`/`1.04.02`. A caller whose id carries a prefix (e.g. `NL-HaNA_2.10.01`) would see its
  own code as a target (phase-02 review, F02-R.6).
- `_segments` applies the `element_text` rule: a `<lb/>` directly before punctuation adds no
  space (`Eems<lb/>. Met` is `Eems. Met`). The AS copy differs from LotA's old `_segments` only
  here; SG and VOC `segments` are unchanged (0 cells).
- **LotA imports changed:** `ead/scopecontent.py` (`ScopeNote`, `ScopeReference`, and
  `parse_scope_notes` as `_parse_component`), `ead/scopecontent_survey.py` (`LB_TAG`,
  `NA_SHAPE_RULES`, `ShapeRule`, `classify_shape`, `descendant_tags`; `SHAPE_RULES` kept as an
  alias), `cli/parse_scopecontent.py` (appends `:{target_archive}` to a serialised reference
  only when it is set). On SG + VOC only the `references` column changes: 91 cells, all gained a
  suffix.

### Step 3c follow-up: `element_text` drops the `<lb/>` space before quotes

- `ead_text.element_text` / `raw_element_text`: a `<lb/>` directly before `'`, `"`, `\u2019` or
  `\u201d` now contributes nothing, like the existing `. , ; : ! ? ) ]` set (`1674<lb/>'` is
  `1674'`, not `1674 '`). Behaviour change, no signature change. Trade-off (user decision,
  2026-10-14): an ASCII quote that *opens* a quotation after a line break (`a<lb/>"b"`) now loses
  its space (`a"b"`).
- **Affected LotA imports:** `facets/title_norm.py`, `ead/scopecontent.py`,
  `ead/scopecontent_survey.py` (all via `element_text`/`raw_element_text`). Across the SG and
  VOC titles, 3 SG titles change (`1.01.02:5.2.0.4.2.69`, `5.2.4.85`, `5.2.4.186`); the 7 other
  archives and all scope notes are unchanged.

### Step 4: `DigitisationSummary.has_evidence(mechanisms)` (C5)

- Added `ead_digital.MECHANISM_NAMES` (`("dao", "daogrp", "assets", "handle")`) and
  `DigitisationSummary.has_evidence(mechanisms)`: True if the component carries any of the
  given mechanisms. A bare `str` raises `TypeError`; names outside `MECHANISM_NAMES` raise
  `ValueError`; an empty collection gives False. Backwards compatible: purely additive.
- `is_digitised` is unchanged (handle-inclusive); its docstring now warns that a handle is
  not evidence that scans exist.
- **Sibling code changed in the same step (LotA):** `ead/digitisation.py` and
  `corpus/coverage.py` delete their private `_has_evidence` helpers and call
  `summary.has_evidence(...)`; `NodeDigitisation` gains a last field `summary`
  (`compare=False`, `repr=False`).

### Step 3b: `archive_id` on the EAD frame builders; LotA uses AS node ids

- `ead_views.to_frame(finding_aid, level, archive_id=None)` and
  `ead_views.unit_of_description_frame(finding_aid, archive_id=None)` take an optional
  `archive_id`, the prefix of the `node_id` column. `None` means
  `finding_aid.default_archive_id`, so the default frames are unchanged. A caller passes its
  own id when it differs from the EAD's `<eadid>`. In 7 of LotA's 9 archives it differs: e.g.
  LotA `NL-AsdSAA_89` has eadid `89`, and `GB-3624_oh` has `gb3624-oh`.
- **Sibling code changed in the same step (LotA):**
  - `ead/hierarchy.py`: `_node_id` is deleted and replaced by `EADComponent.node_id`. The
    node index is keyed by the component itself, not `id(component)`. `frame()` takes the AS
    `node_id` column (built with `archive_id=self.archive_id`) instead of joining on `citation`.
  - `model/node.py`: `ArchiveNode.parent_id`/`ancestor_ids` use `make_node_id`.
  - No LotA output changes. Across all 9 archives, node ids and `frame()` rows and columns are
    identical. Details are in LotA `STATUS.md` (2026-10-14, AS step 3b).

## v0.5.0 (2026-10-14)

Steps 1–3 (heading added by the phase-02 review; the release was tagged with these entries still
under *Unreleased*).

### Step 2: EAD text across `<lb/>` (C1)

- New `parsers/ead_text.py` with `element_text(el)` and `raw_element_text(el)`, the
  pre-collapse form for character offsets. The rule:
  - `<lb/>` becomes a space, except directly before `. , ; : ! ? ) ]`;
  - comments and processing instructions contribute only their tail;
  - other markup is descended without adding spaces at element edges, so words like
    `Ge<emph>heim</emph>e` stay whole;
  - whitespace is collapsed once, at the end.
- **Behaviour change:** `EADComponent.title` (via `_text_of`) and `PhysDesc.raw_text` use it. In
  the 52-file corpus, 18 titles change, all of them `<lb/>`-glued (SG 10, VOC 2, 1.05.06 5,
  2.10.50 1): `eerstedeel` → `eerste deel`, `vanGent` → `van Gent`, `G.Voor` → `G. Voor`.
  Three titles gain a space before a probable closing quote (cosmetic). `ead_survey._text_of`,
  used only for structure surveys, is unchanged.
- **Sibling code changed in the same step (LotA):** `element_text` is now imported from AS, and
  the LotA copy in `ead/scopecontent_survey.py` is deleted. `facets/title_norm.py`
  `_structural_date_spans` uses `raw_element_text`, and `tests/test_title_norm.py` is updated.
  `lb_adjusted` is now always False. LotA's normalised-title and scope-note outputs need
  regenerating. Details are in LotA `STATUS.md` (2026-10-14).

### Step 3: EAD node ids and identity-hashable components (C2)

- **Behaviour change:** `EADComponent` now compares and hashes **by identity**
  (`@dataclass(eq=False)`). Before, it was unhashable, and its structural `==` never matched
  across parses because its lxml fields compare by identity. Components can now be used in sets
  and as dictionary keys.
- New in `parsers/ead_tree.py`:
  - `make_node_id(archive_id, index_path)`;
  - `EADComponent.node_id(archive_id)`, `.parent_id(archive_id)`, `.ancestor_ids(archive_id)`;
  - `EADComponent.persistent_id`;
  - `EADFindingAid.default_archive_id`, `.iter_node_ids(archive_id=None)`.

  The node id is `"<archive_id>:<0-based index path joined by '.'>"`, e.g. `1.01.02:0.0.1.1.1`.
  It is byte-identical to LotA's `_node_id` (checked on all 28,573 SG components) and stable
  across re-parses of the same file, though not guaranteed across exports. `persistent_id` is
  the component's `<unitid type="urn:uuid">`, which identifies the same archival unit across
  exports. It is present on every component in NL-HaNA 1.01.02 and 1.04.02, and absent
  elsewhere.
- **Behaviour change:** `ead_views.to_frame` and `unit_of_description_frame` gain two leading
  columns, `node_id` and `persistent_id`. `legacy_compatible_frame` is unchanged.
- **Affected sibling code:** LotA `archival_language/ead/hierarchy.py` (`_node_id` at L38–39,
  the `id(component)` keys at L72/100/103/113, and the citation join in `frame()` at L142–144)
  and `model/node.py` (`parent_id`/`ancestor_ids`) can switch to AS. Not changed yet.

### Step 1: packaging (light core plus extras)

- **Core dependencies:** pagexml-tools, numpy, pandas, lxml, fuzzy-search, orjson, pyyaml,
  scikit-learn, tqdm.
- **Optional extras:**
  - `[torch]`: torch, transformers;
  - `[vision]`: scikit-image, opencv-python, pillow, hdbscan, umap-learn, scipy, ruptures;
  - `[notebook]`: ipywidgets, ipycanvas, ipyevents, ipython, matplotlib, seaborn;
  - `[llm]`: anthropic;
  - `[all]`: everything.

  scipy and ruptures were imported but never declared before; ipython moved from core to
  `[notebook]`.
- **Bounds:** `scikit-learn>=1.8` and `torch>=2.11`, loosened; `pandas>=2.3.3,<4`, widened.
  pandas 3 is verified for core modules only (parsers, annotation, corruption, model: 230
  passed), not for the `[vision]` modules.
- `stream_analysis` and `datasets` now export their names lazily (PEP 562). Every existing
  `from archival_structures.stream_analysis import X` still works, and
  `stream_analysis.corruption`, `parsers.*`, `model.*` and `annotation.*` import without any
  extra. `model/image.py` imports cv2 and PIL only for type checking. Some `analysis` modules
  (the clustering modules, `empty_regions`) still need `[vision]` at import time.
- NLP has a new `pyproject.toml` (package `startpage`, src layout, `archival-structures>=0.4.1`).
- **Still to do (user):**
  - run `poetry lock` in AS;
  - LotA: install AS without `--no-deps`;
  - NLP: install AS and itself editable into `venv/`.
