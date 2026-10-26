# Step 15s: Mode-aware types, units and join

> **VOID (2026-10-23).** Replaced by the re-plan after UA-019 (b) (one mode per volume): see the phase `PLAN.md`
> steps table (`step-15r-volume-mode-build.md`, `step-15s-v4-build-and-run-kit.md`). Don't run this brief.

> **Phase:** 10, Hand and print per region (text layer v4). **Depends on:** 15r.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md` (15r's
> facts), then this file. **Read nothing else unless this brief names it.** Anchors checked 2026-10-23; grep the
> anchor if a line number drifted. Record any exploration under *Brief defects*.

## Context

15r gave every region a mode (`regions_meta.mode`, `layout/regions.mode`) and made the split and the merges
mode-aware for the v4 presets `sg_1.01.02_v4` / `sg_1.10.94_v4`. The remaining hand/print differences live in
types, units and join. This step makes them mode-aware, so a v4 build applies to each region the rule its mode
calls for, wherever it sits. v1–v3 output must not change.

The remaining differences between the v3 hand and print presets (from `presets.py`, checked at planning):

| Field | hand (`sg_1.01.02_v3`) | print (`sg_1.10.94_v3`) | Granularity in v4 (decision P7) |
|---|---|---|---|
| `types_params.column_override` | False | True | per segment, by its layout region's mode |
| `types_params.offchain_other_bands` | `()` | `("bottom",)` | per page: print bands only on pages whose label is `print`; `mixed`/`uncertain`/`hand` pages use `()` |
| `unit_params` | None | `{"indent_frac": 0.5, "short_frac": 0.1, "gap_factor": None}` | v4 passes the print params everywhere; hand regions ignore them (see below) |
| units `print_mode` (from `merged_line_split == "on"`) | False | True | per layout region, by its mode |
| REPUBLIC crosswalk | off | on with `--republic-dir` | per inventory: on if the inventory has print units and `paragraphs-<inv>.tsv.gz` exists |
| `join_lead_marks` | `","` | `""` | per unit, by its mode |

All other `types_params` keys are equal on both sides (`offchain_retype`, `offchain_years_other`, the patterns,
`col_min_char_frac`); test it (below). `wbc_pinned` and `join_lead_marks_lower` stay per inventory on the v4 preset.

## Inputs (code, as of 2026-10-23)

- `build.py`: `stage_types` L389 (`TypeParams.from_dict({**preset.types_params, **cli})`, calls
  `type_inventory(inv, layer_root, params)`); `stage_units` L427 (`print_mode = run.preset.merged_line_split == "on"`
  L434; REPUBLIC warnings L445–456; `unit_inventory(inv, run.root, params, print_mode=print_mode,
  republic_path=…)` L462); `stage_join` L646 (`run.preset.join_lead_marks` at L682, L687; `lml` from
  `join_lead_marks_lower` L670–671; the "previous lead_marks" re-run check L682).
- `types.py`: `class TypeParams` L42 (`column_override: bool = False` L68, `offchain_other_bands: tuple = ()` L71,
  `from_dict` L90); `segment_page(...)` L193 writes `"column_override": "x" if params.column_override and t in
  COLUMN_OVERRIDE_TYPES else ""` (L282); `_retype_offchain(page_segs, key, ordered, pats, params, here, page_keys, …)`
  L329; `type_inventory(inv, layer_root, params)` L361 (reads `scans`, `lines`, `layout/lines`, `layout/regions`).
- `units.py`: `split_region(segs, params, print_mode)` L109 (with `print_mode=False` it reads **no** `UnitParams`
  field: hand cuts main text only at headers, methods `hand:*`); `build_units_page(scan_id, page, regions, params,
  print_mode, taken, claimable=None)` L179; `unit_inventory(inv, layer_root, params, *, print_mode=False,
  republic_path=None)` L409; crosswalk `if print_mode and republic_path is not None and Path(republic_path).exists()` L488.
  `units/units` columns end `…, "n_segments", "n_lines", "first_line_id", "last_line_id"`.
- `join_v2.py`: `join_unit(texts, wbcs, guards=ALL_GUARDS, both_ends_fix=True, lead_marks="", lead_marks_lower="",
  upper_guard=False)` L29; `join_inventory_v2(inv, layer_root, wbcs, lead_marks="", lead_marks_lower="",
  upper_guard=False)` L158 (calls `join_unit(texts, wbcs, lead_marks=lead_marks, …)` L172 per unit).
- `presets.py` (after 15r): `mode_preset(preset, mode)`, `LayerPreset.mode_presets`, v4 presets.
- `analysis/hand_print.py`: `page_label(labelled, params)` (pairs `(label, n_chars)`), `HandPrintParams.from_dict`.
- Reference: `data/text_layer_v2_dev/15m-a/{hand,print}`, and 15r's `15r-a/{1.01.02,1.10.94}` (extract + layout).
- REPUBLIC: 455/456 `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/`;
  1.01.02 printed volumes 3760–3864 `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_paragraphs_v1/paragraphs-<inv>.tsv.gz`
  (same columns as LotA's: `session_date, resolution_id, para_id, line_start, line_end, text`; ids are box-based
  `<scan_id>-line-<x>-<y>-<w>-<h>` as in 455; 0 of 9,768 3771 ids equal a layer `line_id`, which are XML UUIDs, so only
  the geometric match classes can work: planning measured nothing more).

## Deliverables and behaviour

| File | Change |
|---|---|
| `types.py` | `TypeParams.mode_rules: bool = False` (appended; `from_dict`/`to_dict`). When True and `layout/regions` has `mode`: `column_override` is applied only to segments whose layout region is `print`; per page, `page_label([(mode, n_chars)…])` over the page's layout regions (`n_chars` = sum of `len(text)` of their lines) decides the bands: `offchain_other_bands` only on `print` pages, `()` elsewhere. `types/` gains no column; `types/stats.json` per inventory gains `pages_by_label` |
| `units.py` | `unit_inventory(..., print_mode: bool | str = False)`: `"mode"` → per layout region `print_mode = (mode == "print")`; the crosswalk runs if any unit is print and the file exists. `units/units` gains a last column `mode` in mode runs only |
| `join_v2.py` | `join_inventory_v2(..., lead_marks: str | dict = "")`: a dict `{"hand": …, "print": …}` picks by the unit's `mode` |
| `build.py` | `print_mode = "mode"` when `merged_line_split == "mode"`; REPUBLIC warning only for inventories with print units (log their count); lead marks = `{m: mode_preset(preset, m).join_lead_marks for m in ("hand", "print")}` when `preset.mode_presets`; the re-run check compares a stable JSON string |
| `presets.py` | v4 presets: `types_params = {**SG_PRINT_TYPES_PARAMS, "mode_rules": True}`, `unit_params = SG_PRINT_UNIT_PARAMS` |
| `tests/test_text_layer_modes.py` | the tests below, appended |
| `docs/text_layer.md` | extend `## Text layer v4 (phase 10)`: per-rule granularity table (as above), new columns, the crosswalk for 1.01.02 printed volumes |
| `docs/shared_library/changelog.md` | `### Step 15s: …`, with Edit |
| `reports/phase-10-hand-print-per-region.md` | `### Step 15s: mode-aware types, units, join`; findings `F10-15s.n` |

## Build and compare

Build `data/text_layer_v2_dev/15s-a/{1.01.02,1.10.94}` from scratch with `--stage all` (v4 presets), through
coreshare as in 15r, with `--republic-dir` = the `print_paragraphs_v1` dir for 1.01.02 and LotA's dir for 1.10.94.
Compare with 15r's comparer (`reports/data/phase-10-hand-print-per-region/step-15r/compare.py`, `--sub-map
hand:1.01.02,print:1.10.94 --ignore-cols mode,mode_score,mode_method`) against `15m-a`:
- hand 3349–3354, 11182, 11281: every table equal to `15m-a`, except tables touched by their print-mode regions
  (15r's counts); trace each difference to such a region;
- 455, 456: equal except where 15r found hand-mode regions; their units now use hand cuts; count them;
- 3771–3773, 3821–3823: report units per page, unit methods (`print:*` share), types (`column_override` count,
  `page_number`/`other` counts), merges by boundary kind, against 455/456; and the crosswalk match classes
  (`exact`, `near`, `split_original`, `split_half`, `unmatched`) per inventory next to 455/456's.
- `wbc` and `join/` of hand inventories: equal except units whose mode changed.

## Tests (append to `tests/test_text_layer_modes.py`)

- `test_hand_print_types_params_differ_only_in_two_fields`: `SG_HAND_TYPES_PARAMS` vs `SG_PRINT_TYPES_PARAMS`.
- `test_types_mode_rules`: a page with a print region and a hand region: `column_override` only on the print
  region's furniture; an off-chain bottom number becomes `other` on a print page and stays `page_number` on a
  mixed page.
- `test_units_mode_per_region`: one print region (paragraph cue fires) and one hand region on a page: methods
  `print:*` and `hand:*`; `mode` column present; with `print_mode=False/True` the output equals today's.
- `test_join_lead_marks_by_mode`: `", rant"` after a merge is stripped in a hand unit, kept in a print unit.
- `test_v4_all_chain_on_fixture`: `--stage all` with `sg_1.01.02_v4` on `tests/fixtures/text_layer/sg/9999` runs
  through (the fixture's regions are short or hand; nothing is split).

## Acceptance criteria

1. Tests pass; full suite green; v1–v3 dev rebuild of one inventory (`sg_1.01.02_v3`, 3351, into
   `data/text_layer_v2_dev/15s-v3check/hand`) equals `15m-a`'s 3351 on decompressed bytes (path columns aside).
2. The comparison matches *Build and compare*, every difference counted and traced.
3. 3771–3823's statistics sit in 455/456's range or the gap is explained by read cases; the 3771 crosswalk
   classes reported (a negative result is fine: then 1.01.02 printed volumes build without crosswalk, documented).
4. `rows116.py` on `15s-a` (copy `reports/data/phase-09-text-layer-v3-prep/step-15m/rows116.py` and its
   `rows116_verdicts.tsv` into `step-15s/`, point it at `15s-a`; its layer glob is `*_text_layer_v*`, adapt the
   sub-layer names): the predicted share free equals 15m's 0.526 / 0.560 unless rows sit in mode-changed regions
   (list them).
5. Protected hashes unchanged (`--skip 15s`).

## Error analysis

- 3771: 15 units (seed 18) with their segments: paragraph boundaries right, compared with the REPUBLIC paragraphs
  where the crosswalk matched? 10 `unmatched` crosswalk rows: why?
- Every page whose bands changed because of the page label (up to 20): was `other` vs `page_number` right?
- Hand inventories: every unit whose `mode` is print (up to 20): read it.

## Constraints

- v1–v3 output byte-identical; `join.py`, v1 paths, `text_extent.py`, `page_structure.py` untouched.
- No `shared-data` or sibling writes; no external drive. Never `git commit`; never bump the version; `chmod g+w`;
  `.venv/bin/python`; new dir per run.

## When done

Report section (comparison table, the 3771 vs 455/456 table, crosswalk classes, reads), findings `F10-15s.n`;
`make reports`. `PROGRESS.md` block; `STATUS.md` (next: 15t; *Facts*: `15s-a` is the reference layer, the
crosswalk verdict for 1.01.02 printed volumes, the `rows116` numbers).
