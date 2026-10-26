# Step 15r: Mode-aware extract and layout (v4 presets)

> **VOID (2026-10-23).** Replaced by the re-plan after UA-019 (b) (one mode per volume): see the phase `PLAN.md`
> steps table (`step-15r-volume-mode-build.md`, `step-15s-v4-build-and-run-kit.md`). Don't run this brief.

> **Phase:** 10, Hand and print per region (text layer v4). **Depends on:** 15q.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md` (it has
> 15q's `t_lo`/`t_hi`), then this file. **Read nothing else unless this brief names it.** The code anchors below
> were checked on 2026-10-23; grep the anchor if a line number has drifted. Record any exploration under *Brief defects*.

## Context

The SG presets are keyed by archive: `sg_1.01.02_v3` runs the hand rules on every region of archive 1.01.02,
`sg_1.10.94_v3` the print rules on 1.10.94. 15q added `analysis/hand_print.py`, which gives every region a mode
(`hand`/`print`). This step adds **v4 presets** that pick the hand or print rule per region in the first two stages:
the merged-line split (extract) and region merging and line sorting (layout). Types, units and join follow in 15s.
v1–v3 presets and their output must not change.

## Inputs (code, as of 2026-10-23)

- `archival_structures/text_layer/presets.py`: `@dataclass(frozen=True) class LayerPreset` L9–43 (fields, defaults
  in order: `name, untagged_tag, scan_id_prefix, throughput_fit=None, output_bytes_per_xml_byte=None,
  description="", format_version=1, archive_id=None, source_pattern=None, skip_resource_forks=False,
  merged_line_split="off", split_region_tags=(), word_split="off", word_split_params=None, layout_params=None,
  types_params=None, unit_params=None, word_break_rule=None, word_break_stat="default", wbc_sanity_in=("¬",),
  wbc_sanity_out=(":", "."), wbc_pinned=None, join_lead_marks="", join_lead_marks_lower=None,
  join_upper_guard=False`). `SG_HAND_V2` L118–139, `SG_PRINT_V2` L145–166 (`merged_line_split="on"`,
  `split_region_tags=("index","resolution","untagged","Text")`, `layout_params` = furniture patterns +
  `sort_unchanged_lines=True`, merge default True), `SG_HAND_V3`/`SG_PRINT_V3` L170–173 (`dataclasses.replace`,
  name and description only), `PRESETS` L175–176. `SG_HAND_LAYOUT_PARAMS` L84 has `merge: False`.
- `build.py`: `stage_extract` L266 (chooses `extract_inventory_v2_split` if `merged_line_split == "on"`, L280);
  `stage_layout` L349 (`LayoutParams.from_dict({**preset.layout_params, **cli})`, calls `layout_inventory`).
- `line_split.py`: `fit_split_model_from_rows(line_rows, tag_by_region, split_region_tags, untagged_tag,
  min_uni_freq=100, params=None)` L1317 (only lines whose region's tag is in `split_region_tags` take part);
  `fit_split_model(inv, source, preset, lines_path=None, params=None)` L1386 (pre-pass: parses `source`, calls
  `extract_scan_v2(scan, inv=, inv_dir=, member=, preset=)` → `s_row, r_rows, l_rows`, fills
  `tag_by_region[r["region_id"]] = r["region_tag"]`); `SplitHook.__call__(scan)` L1456 (per region:
  `tag = region_tag_of(region, Counter(), m.untagged_tag)`, participates if `tag in m.split_region_tags`);
  `make_split_hook(model)` L1742; `extract_inventory_v2_split(inv, source, out_root, preset, *, lines_path=None,
  params=None, record_splits=False)` L1747 (fit, hook, `extract_inventory_v2(..., scan_hook=hook)`, writes
  `split_model/<inv>.json`, `split_stats/<inv>.json`).
- `extract_v2.py`: `extract_inventory_v2(inv, source, out_root, preset, *, scan_hook=None)` L180;
  `legacy_doc_id(scan_id, box)` L39 (the `region_id`: `<scan_id>-text_region-<x>-<y>-<w>-<h>`); a region without
  coords gets `f"{scan_id}-text_region-{xml_id}"` (L94); `legacy_region_text(line_texts)` L44;
  `REGION_COLUMNS_V2` ends `…, "n_lines", "n_lines_text"`.
- `analysis/page_structure.py`: `LayoutParams` L52–106 (`merge: bool = True`, `sort_unchanged_lines: bool = False`,
  `from_dict` rejects unknown keys).
- `layout.py`: `layout_scan(scan_row, region_rows, lines_by_region, gutter_rel, params, *, gutter_rule="gap",
  med_w_rel=None, blocked_out=None)` L91: per page, `merge_overlapping_text_regions(page_regions, params,
  union_keys=UNION_KEYS, id_fn=_region_id, exclude=furniture, blocked=…, column_blocked=…)` if `params.merge`
  (L141); `if change == "unchanged" and params.sort_unchanged_lines: lines = sort_lines(...)` (L167); output rows
  end `…, "column_value", "lines_resorted"`. Region objects carry `metadata["source_region_ids"]`.
- `analysis/hand_print.py` (15q): `HandPrintParams`, `classify_inventory_tables(region_rows, line_rows, params, join=" ".join)`.
- Reference layer: `data/text_layer_v2_dev/15m-a/{hand/NL-HaNA_1.01.02_text_layer_v3, print/NL-HaNA_1.10.94_text_layer_v3}`.
  Sources: `data/text_layer_v3_full_inputs/{hand,print}/` (symlinks to the 14 + 2 pilot sources).
  REPUBLIC: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/`.

## Deliverables and behaviour

| File | Change |
|---|---|
| `presets.py` | `LayerPreset` gains, appended last: `mode_presets: Optional[Dict[str, str]] = None`, `region_mode: Optional[Dict] = None`. Helper `mode_preset(preset, mode) -> LayerPreset` (`PRESETS[preset.mode_presets[mode]]`, or `preset` itself when `mode_presets` is None). New `SG_1_01_02_V4 = replace(SG_HAND_V3, name="NL-HaNA_1.01.02_text_layer_v4", …)` and `SG_1_10_94_V4 = replace(SG_PRINT_V3, name="NL-HaNA_1.10.94_text_layer_v4", …)`, both with `merged_line_split="mode"`, `split_region_tags=SG_PRINT_V2.split_region_tags`, `layout_params={**SG_PRINT_V2.layout_params, "merge_mode": "print"}`, `mode_presets={"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}`, `region_mode=HandPrintParams(t_lo=…, t_hi=…).to_dict()` (15q's values, in `STATUS.md`). Keys `sg_1.01.02_v4`, `sg_1.10.94_v4`. Everything else (archive identity, `wbc_pinned`, `join_lead_marks_lower`, types/unit params) as the v3 base: 15s makes those mode-aware |
| `line_split.py` | `SplitHook(model, mode_by_region=None)`, `make_split_hook(model, mode_by_region=None)`: when given, a region participates iff its tag ∈ `split_region_tags` **and** `mode_by_region.get(region_id) == "print"` (region_id built exactly as `extract_scan_v2` builds it; reuse `legacy_doc_id`/the L94 form, factor a helper if needed). New `extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False)`: one pre-pass as in `fit_split_model` collecting `r_rows`/`l_rows`; `modes = classify_inventory_tables(r_rows, l_rows, HandPrintParams.from_dict(preset.region_mode), join=legacy_region_text)`; split model from `fit_split_model_from_rows(l_rows of regions with tag ∈ tags and mode print, …)`; **if there are no such lines, or fitting raises on too little data, no model and no hook** (recorded); extract with the hook; then rewrite `regions_meta/<inv>.tsv.gz` with three columns appended: `mode`, `mode_score` (density, 4 decimals), `mode_method`; write `split_model`/`split_stats` (or `{"n_print_lines": 0, "model": null}`), and add `"modes"` to `extract_stats/<inv>.json`: region counts per label, per mode, per method; chars per mode; pages per page label |
| `build.py` | `stage_extract`: `merged_line_split == "mode"` → `extract_inventory_v2_modes`. `stage_layout` unchanged apart from passing params (below) |
| `page_structure.py` | `LayoutParams.merge_mode: Optional[str] = None` (appended; in `to_dict`/`from_dict`) |
| `layout.py` | when `params.merge_mode` is set and region rows have a `mode` column: regions whose mode ≠ `merge_mode` are added to `exclude` for merging, and `sort_unchanged_lines` applies only to regions of that mode. `layout/regions` gains a last column `mode` (unchanged/split → its source region's mode; merged → `print`; empty → its own). Without `merge_mode`, output is byte-identical to today (no `mode` column) |
| `tests/test_text_layer_modes.py` (new) | tests below |
| `docs/text_layer.md` | new section `## Text layer v4 (phase 10)` before `## Published SG pilot (text_layer_v2)`: presets, the rule (link 15p/15q), new columns |
| `docs/shared_library/changelog.md` | `### Step 15r: …` (new fields, functions, v4 presets, columns), with Edit |
| `reports/phase-10-hand-print-per-region.md` | `### Step 15r: mode-aware extract and layout`; findings `F10-15r.n` |

## Build and compare

Build `data/text_layer_v2_dev/15r-a/{1.01.02,1.10.94}` (new dirs) with `--stage extract` then `--stage layout`
(not `all`: types, units and join are 15s). Through coreshare, e.g.
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15r-1.01.02 --want 6 --min 2 --wait -- .venv/bin/as-build-text-layer --preset sg_1.01.02_v4 --pagexml-dir data/text_layer_v3_full_inputs/hand --out-dir data/text_layer_v2_dev/15r-a/1.01.02 --stage extract --workers {workers}
```
(same for `layout`, and for `sg_1.10.94_v4` from `data/text_layer_v3_full_inputs/print` into `…/1.10.94`).
Compare with `15m-a` table by table: copy `reports/data/phase-09-text-layer-v3-prep/planning/compare.py` into
`reports/data/phase-10-hand-print-per-region/step-15r/compare.py` and add `--sub-map hand:1.01.02,print:1.10.94`
plus an option to drop named appended columns before comparing (`--ignore-cols mode,mode_score,mode_method`).

Expected (measured by 15q on `15m-a` tables; take the exact shares from `STATUS.md`):
- hand 3349–3354, 11182, 11281: `regions_meta` and `lines` equal to `15m-a` after dropping the new columns, except
  in regions resolved `print` (those may split); `layout/*` equal except pages holding print regions;
- 3771–3773, 3821–3823: now split. Report per inventory `split_stats` (`n_candidates_before`, `n_split_hyphen`,
  `n_split_bigram`, `n_candidates_after`), the share of lines with a `column` value, and merges, next to 455/456 in
  `15m-a` (455: 0.992 of lines carry `column`, 456: 1.000);
- 455, 456: equal to `15m-a` except in regions resolved `hand` (unsplit, unmerged now). Count them.
Every difference outside these rules is a bug or a brief defect: stop and report.

## Tests (`tests/test_text_layer_modes.py`; fixture `tests/fixtures/text_layer/sg/9999/` has Text regions only)

- `test_v1_v3_presets_unchanged`: every v1–v3 preset's `asdict` minus the two new fields equals a frozen copy
  (write the copy from the current code before editing).
- `test_v4_presets`: the two v4 presets differ only in archive identity, name, description, `wbc_pinned`,
  `join_lead_marks_lower`, and the fields the v3 bases differ in (list them); `mode_preset` returns the v3 presets.
- `test_split_hook_mode_filter`: a scan with a Text region in mode print and one in mode hand: only the first's
  lines are candidates (`stats["n_lines_participating"]`).
- `test_extract_modes_writes_columns`: run `extract_inventory_v2_modes` on the fixture into `tmp_path`;
  `regions_meta` header ends with the three columns; values in their sets; no print lines → no model, no error.
- `test_layout_merge_mode`: two overlapping regions, one print one hand → not merged; both print → merged and
  `mode` = print; without `merge_mode` the header has no `mode`.
Keep the 8 existing `test_text_layer_layout.py` and 14 `test_text_layer_line_split.py` tests green unchanged.

## Acceptance criteria

1. New tests pass; full suite green (count before and after).
2. The `15r-a` comparison matches *Expected*, with every difference counted per inventory and table.
3. 3771–3823 are split and merged like print; their per-line `column` share and split rates sit within the 455/456
   range, or the difference is explained by reading cases.
4. `15m-a` and the published layers unchanged (protected hashes, as in 15q, `--skip 15r`).

## Error analysis

- 3771: 20 split lines (seed 17) and 10 merged regions: correct halves and column order? Compare with 455's
  phase-06 split error rate if the report has it (phase-06 report, `### Step 15e`).
- Hand dev inventories: every region resolved `print` (up to 30, seed 17): verdict as in 15p; did its split do harm?
- 455/456: every region resolved `hand` (up to 30): verdict; was excluding it from split/merge right?

## Constraints

- v1–v3 output byte-identical (P6). Don't touch types, units, join, wbc (15s), `join.py`, v1 paths, `text_extent.py`.
- No `shared-data` or sibling writes; no external drive. Never `git commit`; never bump the version; `chmod g+w`;
  `.venv/bin/python`; new dir per run; no `rm -rf`.

## When done

Report section with the comparison table per inventory and the reads; findings `F10-15r.n`; `make reports`.
`PROGRESS.md` block; `STATUS.md` (next: 15s; under *Facts later steps need*: the per-inventory counts of regions
by mode in `15r-a`, and any change to the column names).
