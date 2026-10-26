# Step 15r: Volume-mode build (v4 presets)

> **Phase:** 10, Hand and print per volume (text layer v4). **Depends on:** 15q, **only if its stop rule passed**
> (`STATUS.md` says so and gives the `VolumeModeParams` dict and the P11 outcome).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, this file.
> **Read nothing else unless this brief names it.** Anchors were checked on 2026-10-23; grep the quoted code if a line
> number has drifted. Record any exploration under *Brief defects*.

## Context

The SG presets are keyed by archive: `sg_1.01.02_v3` runs the hand rules on every 1.01.02 inventory, including the
six printed volumes 3771–3773 and 3821–3823 (F10-P.1). 15q added `analysis/hand_print.py`: `volume_mode(line_rows,
params)` decides `hand`/`print` per inventory from its own PageXML line geometry. This step adds **v4 presets** that,
per inventory, run one pre-pass, decide the mode, and build every stage with the hand or the print rules of the v3
presets. Rules never switch inside an inventory (decision P2, revised). v1–v3 presets and their output must not change.

## Inputs (code, as of 2026-10-23)

- `presets.py`: `@dataclass(frozen=True) class LayerPreset` L9–43; fields end with `…, join_lead_marks="",
  join_lead_marks_lower=None, join_upper_guard=False`. `SG_HAND_V3`, `SG_PRINT_V3` (L170–173, `dataclasses.replace`
  of the v2 presets), `PRESETS` dict L175–176 with keys `voc_v1, sg_1.01.02_v2, sg_1.10.94_v2, sg_1.01.02_v3, sg_1.10.94_v3`.
- Fields in which `sg_1.01.02_v3` and `sg_1.10.94_v3` differ (by `asdict`): identity `name, description, archive_id,
  scan_id_prefix, source_pattern`; per archive `wbc_pinned, join_lead_marks_lower`; **rules** `merged_line_split`
  (`off`/`on`), `split_region_tags` (`()` / `("index","resolution","untagged","Text")`), `layout_params`,
  `types_params`, `unit_params` (`None` / `{"indent_frac": 0.5, "short_frac": 0.1, "gap_factor": None}`),
  `join_lead_marks` (`","` / `""`).
- `line_split.py`: `fit_split_model(inv, source, preset, lines_path=None, params=None)` L1386, whose pre-pass is
  L1396–1410:
  ```python
  from collections import Counter
  from pagexml.parser import parse_pagexml_file
  from archival_structures.text_layer.extract_v2 import _DEELOPNAME, extract_scan_v2
  from archival_structures.text_layer.io import iter_pagexml
  rows, tag_by_region = [], {}
  for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):
      try:
          scan = parse_pagexml_file(member, pagexml_data=data)
      except Exception:
          continue
      s_row, r_rows, l_rows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
      for r in r_rows:
          tag_by_region[r["region_id"]] = r["region_tag"]
      rows += [_int_row(r) for r in l_rows]
  return fit_split_model_from_rows(rows, tag_by_region, preset.split_region_tags, preset.untagged_tag, params=params)
  ```
  `fit_split_model_from_rows(line_rows, tag_by_region, split_region_tags, untagged_tag="untagged",
  min_uni_freq=100, params=None)` L1317; `make_split_hook(model)` L1742 (`SplitHook`, has `record_splits`,
  `stats_dict()`, `splits`); `extract_inventory_v2_split(inv, source, out_root, preset, *, lines_path=None,
  params=None, record_splits=False)` L1747, whose tail (L1760–1768) writes `split_model/<inv>.json`
  (`model.to_dict()`) and `split_stats/<inv>.json` (`hook.stats_dict()`, plus `<inv>.splits.json` if recording) and
  sets `result["split"]` to the keys `n_candidates_before, n_split_hyphen, n_split_bigram, n_candidates_after, n_page_errors`.
- `extract_v2.py`: `extract_inventory_v2(inv, source, out_root, preset, *, scan_hook=None)` L180 → result dict.
- `build.py` (813 lines), every place a rule field is read:
  - `stage_extract` L266: `if run.preset.merged_line_split == "on": func = extract_inventory_v2_split` (L280), else
    `extract_inventory_v2`; tasks `(inv, str(source), str(run.root), run.preset)`; "done" check L270–273.
  - `stage_layout` L349: `params = LayoutParams.from_dict({**(run.preset.layout_params or {}), **cli})` L354, one
    `params.json`, `layout_inventory(inv, run.root, params)` L369, manifest `"layout_params": params.to_dict()` L385.
  - `stage_types` L389: same pattern with `TypeParams` (L393), `type_inventory(inv, run.root, params)` L409, L423.
  - `stage_units` L427: `UnitParams` L433; `print_mode = run.preset.merged_line_split == "on"` L434; REPUBLIC
    warnings L445–454; `unit_inventory(inv, run.root, params, print_mode=print_mode, republic_path=…)` L462; L478.
  - `stage_join` L646: lead marks `run.preset.join_lead_marks` in the re-run check L682 and the task tuple L687.
  - `stage_wbc` L498 and `stage_wordfix` L294 read no rule field (check with grep; if one does, stop and report).
  - `class Run` L120: `self.root = Path(args.out_dir) / preset.name`.

## Deliverables and behaviour

| File | Change |
|---|---|
| `presets.py` | `LayerPreset` gains, appended last: `mode_presets: Optional[Dict[str, str]] = None`, `volume_mode: Optional[Dict] = None`. `MODE_RULE_FIELDS = ("merged_line_split", "split_region_tags", "layout_params", "types_params", "unit_params", "join_lead_marks")`. `inventory_preset(preset, mode) -> LayerPreset`: `preset` itself if `mode_presets` is None; else `replace(preset, **{f: getattr(PRESETS[preset.mode_presets[mode]], f) for f in MODE_RULE_FIELDS}, mode_presets=None, volume_mode=None)`; `ValueError` for a mode not in `("hand", "print")`. `SG_1_01_02_V4 = replace(SG_HAND_V3, name="NL-HaNA_1.01.02_text_layer_v4", description=…, mode_presets={"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}, volume_mode={"method": "geometry", "params": <15q's VolumeModeParams dict from STATUS.md>})`; `SG_1_10_94_V4 = replace(SG_PRINT_V3, name="NL-HaNA_1.10.94_text_layer_v4", …same two fields)`, except that if 15q's P11 outcome says so, its `volume_mode = {"method": "fixed", "mode": "print", "params": <same dict>}`. Keys `sg_1.01.02_v4`, `sg_1.10.94_v4` |
| `line_split.py` | factor the pre-pass into `prepass_rows(inv, source, preset) -> tuple[list[dict], dict[str, str], dict]` (int-coerced line rows, `tag_by_region`, `{"n_scans": …, "n_parse_fail": …}`); `fit_split_model` calls it. Behaviour unchanged (the 14 `test_text_layer_line_split.py` tests stay green) |
| `text_layer/modes.py` (new) | `decide_mode(line_rows, preset) -> dict`: `volume_mode(line_rows, VolumeModeParams.from_dict(preset.volume_mode["params"]))`; with `method == "fixed"` the mode becomes `preset.volume_mode["mode"]`, `method = "fixed"`, and the geometry result is kept as `geometry_mode`/`geometry_share`. `write_mode(out_root, inv, result)`: `mode/<inv>.json` (all keys except `scans`/`regions`), `mode/scans/<inv>.tsv.gz` (`scan_id, n_regions_feat, n_chars, value, label`), `mode/regions/<inv>.tsv.gz` (`region_id, scan_id, n_lines, n_chars, h_med, pitch_med, hrel`; empty cells for None; floats 4 decimals), via the layer's atomic TSV/JSON writers in `text_layer/io.py`. `read_mode(layer_root, inv) -> str` (clear `FileNotFoundError` naming the file and "run extract first"). `extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False)`: `prepass_rows` → `decide_mode` → `write_mode` → `resolved = inventory_preset(preset, mode)`; if `resolved.merged_line_split == "on"`: `fit_split_model_from_rows(rows, tags, resolved.split_region_tags, resolved.untagged_tag)`, hook, `extract_inventory_v2(..., resolved, scan_hook=hook)`, then the split files and `result["split"]` exactly as `extract_inventory_v2_split` L1760–1768; else `extract_inventory_v2(..., resolved)`. Add `result["mode"]`, `result["mode_method"]` |
| `build.py` | `Run.preset_for(inv)`: `inventory_preset(self.preset, read_mode(self.root, inv))` when `self.preset.mode_presets`, else `self.preset`. `stage_extract`: mode presets → `extract_inventory_v2_modes`; the done check also requires `mode/<inv>.json`. `stage_layout`/`stage_types`/`stage_units`: in mode runs build the params once per mode (`{m: X.from_dict({**(inventory_preset(run.preset, m).<field> or {}), **cli}) for m in ("hand", "print")}`) and use `params[mode_of(inv)]`; `params.json` and the manifest entry become `{"hand": {...}, "print": {...}}` (plus `republic_dir` for units, as today); `print_mode = run.preset_for(inv).merged_line_split == "on"` per inventory; REPUBLIC warnings only about print inventories (log how many). `stage_join`: `run.preset_for(inv).join_lead_marks` at L682 and L687. Non-mode presets: every code path and output byte-identical to today |
| `tests/test_text_layer_modes.py` (new) | tests below |
| `docs/text_layer.md` | new section `## Text layer v4 (phase 10)` before `## Published SG pilot (text_layer_v2)`: the presets, the rule (link the report's 15q section), the `mode/` tables, the limit (one mode per volume) |
| `docs/shared_library/changelog.md` | `### Step 15r: volume-mode build` (new fields, `inventory_preset`, `prepass_rows`, `modes.py`, v4 presets, `mode/` tables, `params.json` per mode in v4 layers), with Edit |
| `reports/phase-10-hand-print-per-region.md` | fill the empty section `### Step 15r: volume-mode build`; findings `F10-15r.n` |

## Build and compare

Build `data/text_layer_v2_dev/15r-a/{1.01.02,1.10.94}` (new dirs) with `--stage all`, through coreshare, from the
sources `15m-a` used (so path columns match):
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15r-1.01.02 --want 6 --min 2 --wait -- .venv/bin/as-build-text-layer --preset sg_1.01.02_v4 --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir data/text_layer_v2_dev/15r-a/1.01.02 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_paragraphs_v1 --workers {workers}
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15r-1.10.94 --want 5 --min 2 --wait -- .venv/bin/as-build-text-layer --preset sg_1.10.94_v4 --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir data/text_layer_v2_dev/15r-a/1.10.94 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --workers {workers}
```
Compare with `15m-a`: copy `reports/data/phase-09-text-layer-v3-prep/planning/compare.py` (usage in its docstring:
`compare.py base_a base_b [--sub hand,print] [--out F.json]`) into `reports/data/phase-10-hand-print-per-region/step-15r/compare.py`
and add `--sub-map hand:1.01.02,print:1.10.94` (sub-layer `hand` of base_a against `1.01.02` of base_b) and
`--ignore-paths mode/` (files only in the v4 layer). Expected:
- **hand inventories** 3349–3354, 11182, 11281 (mode `hand` by 15q): every per-inventory file equal to `15m-a` on
  decompressed bytes. Corpus-level files (`wbc/corpus_default.json`, `wbc/wbc_per_inventory.tsv`, the `*/stats.json`
  and `params.json`) may differ only in the six printed inventories' entries and in the per-mode `params.json` form;
- **455, 456** (mode `print`, or `fixed`): every file equal to `15m-a` print, apart from `params.json`'s per-mode form;
- **3771–3773, 3821–3823**: now print-built. Report per inventory `split_stats` (`n_candidates_before`,
  `n_split_hyphen`, `n_split_bigram`, `n_candidates_after`), the share of lines with a `column` value, layout merges,
  units per page, the `print:*` share of unit methods, `column_override` and `page_number`/`other` type counts, next
  to 455/456 in `15m-a` (455: 0.992 of lines carry `column`, 456: 1.000);
- **crosswalk** (P8): the 3771–3823 match classes from `units/stats.json` (`exact`, `near`, `split_original`,
  `split_half`, `unmatched`, or whatever keys the stats hold) next to 455/456's. A low rate is a finding; then
  document that printed 1.01.02 volumes build without the crosswalk and leave `--republic-dir` optional.
Every other difference is a bug or a brief defect: stop and report.

## Tests (`tests/test_text_layer_modes.py`; fixture `tests/fixtures/text_layer/sg/9999/`)

- `test_v1_v3_presets_unchanged`: each v1–v3 preset's `asdict` minus the two new fields equals a frozen copy (write
  the copy from the current code **before** editing `presets.py`).
- `test_inventory_preset`: for `sg_1.01.02_v4`, `inventory_preset(·, "hand")` equals `sg_1.01.02_v3` except `name`,
  `description`; `inventory_preset(·, "print")` equals `sg_1.10.94_v3` except the identity and per-archive fields
  listed in *Inputs*; for `sg_1.10.94_v4` the mirror cases. Non-mode preset → itself; bad mode → `ValueError`.
- `test_prepass_rows_matches_fit`: on the fixture, `fit_split_model` before and after the refactor gives the same
  `to_dict()` (freeze it first).
- `test_extract_modes_hand_and_print`: run `extract_inventory_v2_modes` on the fixture into `tmp_path` twice, with a
  preset whose `volume_mode` is `fixed`/`hand` and one with `fixed`/`print`: hand → no `split_model/`, tables equal to
  an `sg_1.01.02_v3` extract of the same fixture; print → `split_model/<inv>.json` present (or the recorded fitting
  error if the fixture is too small; assert whichever the code does, documented); `mode/<inv>.json` keys and
  `mode/scans`, `mode/regions` headers.
- `test_build_v4_all_on_fixture`: `--stage all` with `sg_1.01.02_v4` on the fixture runs through; `layout/params.json`
  has keys `hand` and `print`; the inventory's mode is read from `mode/<inv>.json`.
- `test_read_mode_missing`: a clear error.

## Acceptance criteria

1. New tests pass; full suite green (count before and after; 15q's count is in `STATUS.md`).
2. The comparison matches *Build and compare*: zero unexplained differences, every explained one counted per
   inventory and table.
3. 3771–3823 statistics within 455/456's range, or each gap explained from read cases.
4. `15r-a`'s `mode/<inv>.json` modes equal 15q's dev modes for all 16 inventories.
5. Protected hashes unchanged (as in 15q, `--skip 15r`): `15m-a`, `15n-a`, `data/hand_print_census/15p-a`, `shared-data/`.

## Error analysis

- 3771: 20 split lines (seed 17) and 10 merged regions: right halves, right column order? Compare with 455's
  split error rate in the phase-06 report (`reports/phase-06-text-layer-v2-sg.md`, `### Step 15e`), if it has one.
- 3771: 15 units (seed 18) with their segments: are the paragraph boundaries right, compared with REPUBLIC where the
  crosswalk matched? 10 `unmatched` crosswalk rows (if any): why?
- 3771 vs its `15m-a` hand build: 10 pages (seed 17) side by side: what did the print rules change, for better or worse?

## Constraints

- v1–v3 output byte-identical (P6). Don't touch `layout.py`, `types.py`, `units.py`, `join_v2.py`,
  `page_structure.py`, `join.py`, v1 paths, `text_extent.py`.
- No `shared-data` or sibling writes; no external drive. Never `git commit`; never bump the version; `chmod g+w`;
  `.venv/bin/python`; new dir per run; no `rm -rf`.

## When done

Report section (comparison table per inventory, the 3771 vs 455/456 table, crosswalk classes, reads), findings
`F10-15r.n`; `make reports`. `PROGRESS.md` block; `STATUS.md` (next: 15s; *Facts later steps need*: `15r-a` is the
v4 reference layer, the crosswalk verdict for printed 1.01.02 volumes, the test count).
