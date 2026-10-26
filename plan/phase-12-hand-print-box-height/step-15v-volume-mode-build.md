# Step 15v: Volume-mode build (v4 presets) with the box-height rule

> **Phase:** 12, Hand and print by line-box height. **Depends on:** 15u (done, stop rule passed; reviewed 2026-10-25).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file. **Read nothing else unless this brief names it.** Don't read the phase `PLAN.md` or the phase-10 briefs: what
> you need is copied in here. Anchors were checked on 2026-10-25; grep the quoted code if a line number has drifted.
> Record any exploration under *Brief defects*.

## Context

The SG presets are keyed by archive: `sg_1.01.02_v3` runs the hand rules on every 1.01.02 inventory, including the
six printed volumes 3771–3773 and 3821–3823. Step 15u froze a per-volume rule and passed its pre-registered test
(60/60): the char-weighted median line-box height of a volume is ≤ 87.75 px for print, above it for hand
(`hand_print.box_height_mode`). This step adds **v4 presets** that, per inventory, run one pre-pass, decide the mode
with that rule, record it under `mode/`, and build every stage with the hand or the print rules of the v3 presets.
Rules never switch inside an inventory (one mode per volume). v1–v3 presets and their output must not change.

## Inputs (code as of 2026-10-25)

- `archival_structures/analysis/hand_print.py` (297 lines; no `text_layer` import, keep it so):
  ```python
  BOX_HEIGHT_METHODS = ("box_height", "default", "fixed")                               # L49
  def region_features(line_rows: Iterable[dict], min_lines: int = 3) -> list[dict]      # L126
      # v2 line rows (uses region_id, scan_id, h, baseline, text_raw; h coerced by _num) ->
      # [{region_id, scan_id, n_lines, n_chars, h_med, pitch_med, hrel}], None where no feature
  @dataclass(frozen=True) class BoxHeightParams: cut: float; min_region_chars: int = 100  # L208; from_dict L221
  def scan_box_heights(features, min_region_chars=100) -> list[dict]  # L253: {scan_id, value, n_chars, n_regions}
  def box_height_mode(features, params: BoxHeightParams) -> dict      # L288: mode, method, value, margin, n_chars, n_regions, params
  ```
- `text_layer/presets.py`: `@dataclass(frozen=True) class LayerPreset` L9–43, last fields `…, join_lead_marks: str =
  "", join_lead_marks_lower: Optional[Mapping[str, str]] = None, join_upper_guard: bool = False`. `SG_HAND_V3`,
  `SG_PRINT_V3` L170–173 (`dataclasses.replace` of the v2 presets); `PRESETS` L175–176, keys `voc_v1, sg_1.01.02_v2,
  sg_1.10.94_v2, sg_1.01.02_v3, sg_1.10.94_v3`.
- Fields where `sg_1.01.02_v3` and `sg_1.10.94_v3` differ (measured with `asdict`, 2026-10-25): identity `name,
  description, archive_id, scan_id_prefix, source_pattern`; per archive `wbc_pinned` (hand: 7 keys, print: `default`),
  `join_lead_marks_lower` (None / `{"455": "J"}`); **rules**: `merged_line_split` (`off`/`on`), `split_region_tags`
  (`()` / `("index","resolution","untagged","Text")`), `layout_params`, `types_params` (print adds `column_override`,
  `offchain_other_bands=("bottom",)`), `unit_params` (None / `{indent_frac 0.5, short_frac 0.1, gap_factor None}`),
  `join_lead_marks` (`","` / `""`).
- `text_layer/line_split.py` (1,769 lines): `_int_row(row)` L1310 (x/y/w/h → int); `fit_split_model_from_rows(line_rows,
  tag_by_region, split_region_tags, untagged_tag="untagged", min_uni_freq=100, params=None)` L1317; `fit_split_model(inv,
  source, preset, lines_path=None, params=None)` L1386, whose pre-pass (L1396–1412) is:
  ```python
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
  `make_split_hook(model)` L1742; `extract_inventory_v2_split(inv, source, out_root, preset, *, lines_path=None,
  params=None, record_splits=False)` L1747, tail L1758–1769: `result = extract_inventory_v2(…, scan_hook=hook)`, then
  `write_json_atomic(root/"split_model"/f"{inv}.json", model.to_dict())`, `split_stats/<inv>.json` (`hook.stats_dict()`),
  `<inv>.splits.json` if `record_splits`, and `result["split"] = {k: stats[k] for k in ("n_candidates_before",
  "n_split_hyphen", "n_split_bigram", "n_candidates_after", "n_page_errors")}`.
- `text_layer/extract_v2.py`: `extract_scan_v2(scan, *, inv, inv_dir, member, preset, …)` L64 → `(scan_row,
  region_rows, line_rows)`; scan row keys include `scan_id, page_w, page_h, n_lines_text`.
  `extract_inventory_v2(inv, source, out_root, preset, *, scan_hook=None)` L180.
- `text_layer/io.py`: `iter_pagexml(source, *, skip_resource_forks=False)` L160; atomic writers `write_tsv_gz(path,
  header, rows)` L59 (gzip mtime 0), `write_json_atomic(path, obj)` L97 (indent 1, sort_keys).
- `text_layer/build.py` (813 lines), every rule-field read: `class Run` L120 (`root = out_dir / preset.name` L125);
  `stage_extract` L266 (done check L270–273; L280 `if run.preset.merged_line_split == "on":` →
  `extract_inventory_v2_split`); `stage_layout` L349 (`LayoutParams.from_dict({**(run.preset.layout_params or {}),
  **cli})` L354, `params.json` L362, manifest `"layout_params"` L385); `stage_types` L389 (same: L393, L402, L423);
  `stage_units` L427 (`UnitParams` L433, `print_mode = run.preset.merged_line_split == "on"` L434, REPUBLIC warnings
  over all `inventories` L444–454, `params.json` L455 with `"republic_dir"`, `unit_inventory(…, print_mode=print_mode,
  republic_path=(republic_dir / f"paragraphs-{inv}.tsv.gz" if republic_dir else None))` L462–464, manifest L478);
  `stage_join` L646 (`run.preset.join_lead_marks` L682, L687). `stage_wordfix` L294 and `stage_wbc` L498 read only
  per-archive fields: leave them alone.
- LotA reads no `params.json`, manifest `*_params` key or `mode/` dir (checked 2026-10-25): the per-mode form is safe.
- Reference values: `reports/data/phase-12-hand-print-box-height/step-15u/fit_volumes.tsv` (`set, inv, cls, value,
  n_chars, n_regions`; 76 rows; `set` = `dev_eval` (14), `transfer` (455, 456), `sample`). The 15u values were computed
  from **raw** `extract_scan_v2` line rows (no `_int_row`, no split hook) with preset `sg_1.01.02_v3` (1.01.02) or
  `sg_1.10.94_v3` (455/456). Frozen params: `{"cut": 87.75, "min_region_chars": 100}` (`step-15u/cut.json`, sha256
  `ce58c3338549239f80fe891803091d08cfff2a3ff8f3e2ba4bcf5caf83ccb572`).
- Fixture `tests/fixtures/text_layer/sg/9999/` (4 XML files). Existing `--stage all` fixture tests call
  `build.main([...])` with `sg_1.01.02_v2` (e.g. `tests/test_text_layer_extract_v2.py::test_build_v2_cli_all_runs_the_chain`
  L212): copy their argument pattern.

## Deliverables

| File | Change |
|---|---|
| `text_layer/presets.py` | `LayerPreset` gains, appended last: `mode_presets: Optional[Mapping[str, str]] = None`, `volume_mode: Optional[Mapping] = None`. `MODE_RULE_FIELDS = ("merged_line_split", "split_region_tags", "layout_params", "types_params", "unit_params", "join_lead_marks")`. `inventory_preset(preset, mode) -> LayerPreset` (below). `SG_1_01_02_V4`, `SG_1_10_94_V4`; `PRESETS` keys `sg_1.01.02_v4`, `sg_1.10.94_v4` appended |
| `text_layer/line_split.py` | `prepass_rows(inv, source, preset) -> tuple[list[dict], dict[str, str], dict]`; `fit_split_model` calls it. Behaviour unchanged |
| `text_layer/modes.py` (new) | `decide_mode`, `write_mode`, `read_mode`, `extract_inventory_v2_modes` (below) |
| `text_layer/build.py` | per-inventory preset in mode runs (below); non-mode presets byte-identical |
| `tests/test_text_layer_modes.py` (new) | tests below |
| `docs/text_layer.md` | new `## Text layer v4 (phase 12)` before `## Published SG pilot (text_layer_v2)` (L457): presets, rule (link the report's 15u section), `mode/` tables, the limit (one mode per volume; raw pixels, one scan scale assumed, F12-R.1) |
| `docs/shared_library/changelog.md` | `### Step 15v: volume-mode build (v4)` under *Unreleased*, after the 15u entry, with Edit: new fields, `MODE_RULE_FIELDS`, `inventory_preset`, `prepass_rows`, `modes.py`, v4 presets, `mode/` tables, per-mode `params.json` in v4 layers; affected sibling imports: none |
| `reports/phase-12-hand-print-box-height.md` | fill `### Step 15v: volume-mode build (v4)`; findings `F12-15v.n` (`draft`) |

## Behaviour to implement

**Presets.** `inventory_preset(preset, mode)`: `ValueError` if `mode not in ("hand", "print")`; `preset` itself if
`preset.mode_presets is None`; else `dataclasses.replace(preset, **{f: getattr(PRESETS[preset.mode_presets[mode]], f)
for f in MODE_RULE_FIELDS}, mode_presets=None, volume_mode=None)`. Identity and per-archive fields stay the v4
preset's.
```python
BOX_HEIGHT_V4 = {"cut": 87.75, "min_region_chars": 100}   # step 15u, cut.json sha256 ce58c333…ccb572; F12-15u.1
SG_1_01_02_V4 = replace(SG_HAND_V3, name="NL-HaNA_1.01.02_text_layer_v4", description=<"… v4 (one hand/print mode per volume)">,
                        mode_presets={"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"},
                        volume_mode={"method": "box_height", "params": BOX_HEIGHT_V4})
SG_1_10_94_V4 = replace(SG_PRINT_V3, name="NL-HaNA_1.10.94_text_layer_v4", description=…, mode_presets=<same>,
                        volume_mode={"method": "fixed", "mode": "print", "params": BOX_HEIGHT_V4})  # phase-10 P11, phase-12 P7
```
Keep the two comments. `test_v3_presets_equal_v2_but_name` (`tests/test_text_layer_provenance.py:62`) and
`test_presets_voc_unchanged` (`tests/test_text_layer_extract_v2.py:200`) stay green unchanged.

**`prepass_rows(inv, source, preset)`**: the loop above, but it returns the **raw** `l_rows` (no `_int_row`),
`tag_by_region`, and `info = {"n_scans": <members seen>, "n_parse_fail": <parse exceptions>, "scans": [{"scan_id",
"page_w", "page_h", "n_lines_text"} per parsed scan, in iteration order]}`. `fit_split_model` does
`rows = [_int_row(r) for r in rows]` before `fit_split_model_from_rows`. Raw rows are what 15u measured, so the box
values reproduce exactly; `_int_row` is what the split model was fitted on, so splits stay the same.

**`text_layer/modes.py`** (imports `hand_print`; `hand_print` must not import `text_layer`):
- `decide_mode(line_rows, preset) -> dict`: `feats = region_features(line_rows)`; `params =
  BoxHeightParams.from_dict(preset.volume_mode["params"])`; `r = box_height_mode(feats, params)`. Method `box_height`:
  the result is `r`. Method `fixed`: `mode = preset.volume_mode["mode"]`, `method = "fixed"`, and `r`'s mode and method
  are kept as `box_height_mode`, `box_height_method` (value, margin, n_chars, n_regions stay). Any other method →
  `ValueError`. Add `"scans"`: `scan_box_heights(feats, params.min_region_chars)`, and `"regions"`: `feats`.
- `write_mode(out_root, inv, result, info)`: `mode/<inv>.json` = every key of `result` except `scans`/`regions`, plus
  `inv`, `n_scans`, `n_parse_fail`, `n_scans_sided`, `scan_minority_share` (share of sided scans whose side ≠ mode;
  None if no sided scan). `mode/scans/<inv>.tsv.gz`, header `scan_id, scan_order, page_w, page_h, n_lines_text, value,
  n_chars, n_regions, side`: one row per scan of `info["scans"]` (order = index), joined with `scan_box_heights` by
  `scan_id` (no feature → `value` empty, 0, 0); `side` = `print` if `n_chars >= 200` and value ≤ cut, `hand` if
  `n_chars >= 200` and value > cut, else empty (15u's rule). `mode/regions/<inv>.tsv.gz`, header `region_id, scan_id,
  n_lines, n_chars, h_med, pitch_med, hrel`. None → empty cell; floats `round(x, 4)`. Use `write_tsv_gz`,
  `write_json_atomic`.
- `read_mode(layer_root, inv) -> str`: the `mode` of `mode/<inv>.json`; `FileNotFoundError` naming the path and
  "run the extract stage first".
- `extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False)`: `prepass_rows` →
  `decide_mode` → `write_mode` → `resolved = inventory_preset(preset, mode)`. If `resolved.merged_line_split == "on"`:
  `model = fit_split_model_from_rows([_int_row(r) for r in rows], tags, resolved.split_region_tags,
  resolved.untagged_tag)`, `hook = make_split_hook(model)` (+ `hook.record_splits = True` if asked, as
  `extract_inventory_v2_split` does), `extract_inventory_v2(inv, source, out_root, resolved, scan_hook=hook)`, then the
  split files and `result["split"]` exactly as L1758–1769 (factor that tail into a private helper used by both). Else
  `extract_inventory_v2(inv, source, out_root, resolved)`. Add `result["mode"]`, `result["mode_method"]`.

**`build.py`** (mode runs = `run.preset.mode_presets` is not None; every other path unchanged):
- `Run.preset_for(inv)`: `inventory_preset(self.preset, read_mode(self.root, inv))` in mode runs, else `self.preset`.
- `stage_extract`: mode runs use `extract_inventory_v2_modes`; the done check also requires `mode/<inv>.json`.
- `stage_layout`/`stage_types`/`stage_units`: in mode runs build the params once per mode,
  `{m: X.from_dict({**(inventory_preset(run.preset, m).<field> or {}), **cli}) for m in ("hand", "print")}`, and use
  `params[read_mode(run.root, inv)]`; `params.json` and the manifest entry become `{"hand": {...}, "print": {...}}`
  (units adds `"republic_dir"` at top level, as today).
- `stage_units` in mode runs: `print_mode` per inventory = `run.preset_for(inv).merged_line_split == "on"`; the
  REPUBLIC warnings and `have`/`lacking` count only print-mode inventories (log how many print and hand inventories
  there are); `republic_path` is passed only for print-mode inventories, `None` for hand ones.
- `stage_join`: `run.preset_for(inv).join_lead_marks` at L682 and L687.

## Build and compare

Check `coreshare.py status` first. Build `data/text_layer_v2_dev/15v-a/{1.01.02,1.10.94}` (new dirs), from the
sources `15m-a` used, so path columns match:
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15v-1.01.02 --want 6 --min 2 --wait -- .venv/bin/as-build-text-layer --preset sg_1.01.02_v4 --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir data/text_layer_v2_dev/15v-a/1.01.02 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_paragraphs_v1 --workers {workers}
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15v-1.10.94 --want 5 --min 2 --wait -- .venv/bin/as-build-text-layer --preset sg_1.10.94_v4 --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir data/text_layer_v2_dev/15v-a/1.10.94 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --workers {workers}
```
(`15m-a/hand` had no `--republic-dir`, `15m-a/print` the LotA path; `print_paragraphs_v1` covers 3760–3864.)
Comparer: copy `reports/data/phase-09-text-layer-v3-prep/planning/compare.py` (90 lines; its `sys.path.insert(0,
str(Path(__file__).parent))` must point at that planning dir, for `common.py`) to `step-15v/compare.py`; add `--sub-map
A:B,…` (sub-layer `A` of `base_a` against `B` of `base_b`) and `--ignore-paths P,…` (path prefixes, counted, not
missing). Run: `… compare.py data/text_layer_v2_dev/15m-a data/text_layer_v2_dev/15v-a --sub-map hand:1.01.02,print:1.10.94 --ignore-paths mode/ --out reports/data/phase-12-hand-print-box-height/step-15v/compare_15m-a.json`. Expected:
- **hand inventories** 3349–3354, 11182, 11281: every per-inventory file equal to `15m-a/hand` on decompressed bytes.
  Corpus-level files (`wbc/` corpus files, `*/stats.json`, `*/params.json`) may differ only in the six printed
  inventories' entries and in the per-mode `params.json` form;
- **455, 456** (`fixed`): every file equal to `15m-a/print`, apart from `params.json`'s per-mode form;
- **3771–3773, 3821–3823**: print-built now, so their files differ: count them per table, don't list them as errors.
Every other difference is a bug or a brief defect: stop and report.

## Tests (`tests/test_text_layer_modes.py`)

- `test_v1_v3_presets_unchanged`: each v1–v3 preset's `asdict` minus `mode_presets`/`volume_mode` equals a frozen
  copy (write the copy from the current code **before** editing `presets.py`; store it as a literal in the test).
- `test_inventory_preset`: `inventory_preset(sg_1.01.02_v4, "hand")` equals `sg_1.01.02_v3` in every field except
  `name`, `description`, `mode_presets`, `volume_mode`; `(…, "print")` equals `sg_1.10.94_v3` in the six rule fields and
  `sg_1.01.02_v4` in all others; the mirror cases for `sg_1.10.94_v4`; non-mode preset → itself; `"mixed"` → `ValueError`;
  both v4 presets carry `{"cut": 87.75, "min_region_chars": 100}`, 1.10.94 `fixed`/`print`.
- `test_prepass_rows_matches_fit`: on the fixture, `fit_split_model(...).to_dict()` with `sg_1.10.94_v3` equals a copy
  frozen before the refactor; `prepass_rows` returns unrounded rows and `info` counts (4 members).
- `test_decide_mode`: synthetic line rows giving one value at the cut → `print`/`box_height`, above → `hand`; no
  qualifying region → `hand`/`default`; a `fixed`/`print` preset keeps `box_height_mode` and `box_height_method`.
- `test_extract_modes_hand_and_print`: `extract_inventory_v2_modes` on the fixture into `tmp_path`, with presets
  `replace(sg_1.01.02_v4, volume_mode={"method": "fixed", "mode": m, "params": …})` for `m` = hand and print: hand → no
  `split_model/`, `lines/` equal to an `sg_1.01.02_v3` extract of the fixture; print → `split_model/9999.json`
  present (or the error the code raises on a fixture this small; assert what it does and document it); `mode/` JSON
  keys and both TSV headers.
- `test_build_v4_all_on_fixture`: `build.main([... "--preset", "sg_1.01.02_v4", "--stage", "all", "--workers", "1"])`
  runs through; `layout/params.json` has keys `hand` and `print`; `mode/9999.json` exists and its mode is used.
- `test_read_mode_missing`: the error names the file.
Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_modes.py tests/test_text_layer_line_split.py`
(the latter has 14 tests, all stay green), then the full suite.

## Acceptance criteria

1. New tests pass; full suite green (before this step: 1,226 collected, 1,215 passed, 11 skipped; collect first).
2. The comparison matches *Build and compare*: zero unexplained differences; every explained one counted per inventory
   and table in the report.
3. `mode/<inv>.json` for all 16: `value` equals `fit_volumes.tsv` `value` exactly (same `n_chars`, `n_regions`); modes
   `print` for 3771–3773, 3821–3823, `hand` for the other 1.01.02 eight; 455 = 75.0 and 456 = 82.5, both `fixed`/`print`
   with `box_height_mode` `print`. A mismatch means the pre-pass differs from 15u: stop and report.
4. 3771–3823 next to 455/456 (`15m-a`): `split_stats` (`n_candidates_before`, `n_split_hyphen`, `n_split_bigram`,
   `n_candidates_after`), share of lines with a `column` value (455: 0.992, 456: 1.000), layout merges, units per page,
   `print:*` share of unit methods, `page_number`/`other` type counts. Within 455/456's range, or each gap explained from
   read cases.
5. Crosswalk (`units/stats.json` `republic_crosswalk`) for the six printed 1.01.02 volumes next to 455 (exact 27,033,
   near 1,864, split_half 55, split_original 189, unmatched 7).
6. Protected data unchanged, `diff` before/after: `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py <out> --skip 15v`
   (covers `15m-a`, `15n-a`, `15o-pub`, shared-data, LotA data) and
   `find data/hand_print_geom reports/data/phase-10-hand-print-per-region reports/data/phase-12-hand-print-box-height/step-15u -type f | sort | xargs shasum -a 256 > <out2>`
   (outputs in `reports/data/phase-12-hand-print-box-height/step-15v/`).

## Error analysis (verdict files beside the outputs; `unclear` when the text alone cannot decide)

- **Crosswalk bar (fixed now):** 20 matched crosswalk rows of the six printed 1.01.02 volumes (seed 17, any class but
  `unmatched`): unit text against the REPUBLIC paragraph text. **Usable iff ≥ 18 of 20 are the same paragraph**
  (`unclear` counts as wrong). Plus every `unmatched` row up to 10: why? The verdict goes to `STATUS.md`; 15w passes
  `--republic-dir` for 1.01.02 only if usable.
- 3771: 20 split lines (seed 17) and 10 merged regions: right halves, right column order?
- 3771: 15 units (seed 18) with their segments: are the paragraph boundaries right?
- 3771 against its `15m-a/hand` build: 10 pages (seed 17) side by side: what did the print rules change, for better or
  worse? Categories: better, worse, same, unclear, with counts and verbatim examples.

## Constraints

- v1–v3 output byte-identical. Don't touch `layout.py`, `types.py`, `units.py`, `join_v2.py`, `join.py`,
  `page_structure.py`, v1 paths, `text_extent.py`, or `hand_print.py` (except a bug, recorded with a test).
- No archive knowledge in `modes.py`: the cut lives in the preset.
- No `shared-data` or sibling writes; no external drive. Never `git commit`; never bump the version; `chmod g+w`;
  `.venv/bin/python`; a new dir per run; no `rm -rf`, `nohup` or `&`; zsh: write the `coreshare.py run …` prefix out.
- **Do not touch:** `data/text_layer_v2_dev/{15m-a,15n-a,15o-pub,15i-src}`, `data/hand_print_geom/`,
  `reports/data/phase-10-*`, `reports/data/phase-12-*/step-15u/`, the published `text_layer_v2`/`text_layer_v3` dirs.

## When done

1. Report section: the comparison table per inventory, the 3771–3823 vs 455/456 table, the crosswalk table and its
   bar verdict, the reads (categories, counts, verbatim examples, conclusion); findings `F12-15v.n` (`draft`), a
   negative result included. `make reports` without warnings.
2. Append the block below to `PROGRESS.md`; overwrite `STATUS.md` (next: 15w; *Facts later steps need*: `15v-a` is the
   v4 reference layer, the crosswalk verdict, the test count, anything that changed a name 15w uses).

```markdown
### Step 15v: volume-mode build (v4)
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <1–6, with numbers>
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15v](../../reports/phase-12-hand-print-box-height.md#step-15v-volume-mode-build-v4); findings F12-15v.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
