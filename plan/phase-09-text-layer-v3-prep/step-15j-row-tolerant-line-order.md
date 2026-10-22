# Step 15j: row-tolerant line order

> **Phase:** 09, Text layer v3 preparation. **Depends on:** nothing (first step).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md`; what you need from it is copied in here. If you have to explore the code base
> anyway, do what you must and record it under *Brief defects*.

## Context

Print line order in v2 has two leftovers from phase 07 (F07-15ir.3). (1) The baseline-y sort of
merged regions and column-split pieces swaps fragments of one printed row whose baselines differ by
a few px ("7 ." for ". 7"). (2) `unchanged` print regions keep their XML order, and 130 of them in
455 jump back more than 50 px. This step adds a **row-tolerant sort** (default for merges and split
pieces) and, for the SG **print** preset only, sorts `unchanged` regions too. Ids stay as they are,
and a new flag `lines_resorted` marks the regions it reorders (user decision P2: R5 keeps ids, not
line order). Hand keeps XML order. You build dev layer `15j-a` from scratch and compare it with
`15ir-a`.

## Inputs

- Sources (symlinks; never write through them): hand `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02`,
  print `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml` (read only).
- Baseline: `data/text_layer_v2_dev/15ir-a/{hand,print}/` (current code before this step).
- Planning scripts (run from the AS root with `.venv/bin/python -I`; read their docstrings):
  `reports/data/phase-09-text-layer-v3-prep/planning/order.py` (jumps/swaps per `change` class;
  `--simulate` = the rule below), `compare.py A B` (table-by-table diff), `protect.py OUT --skip P…`.
  Planning output on `15ir-a`: `planning/order_15ir-a_print.json`.
- Code anchors (checked 2026-10-21):
  - `archival_structures/analysis/page_structure.py`: `LayoutParams` L52–97 (frozen dataclass;
    `__post_init__` L82–85 validates `gutter_rule`; `to_dict`/`from_dict` L87–97), `LEGACY_15FP`
    L100–102, `_line_sort_key` L125–130, `merge_regions` L246–276 (L255 sorts), 
    `merge_overlapping_text_regions` L279–314 (calls `merge_regions` at L313),
    `split_regions_on_line_field` L319–342 (sort at L337–338).
  - `archival_structures/text_layer/layout.py`: `LAYOUT_SCAN_COLUMNS` L30–33,
    `LAYOUT_REGION_COLUMNS` L34–36, `STAT_KEYS` L40–43, `layout_scan` L91–200 (split call
    L131–134; region loop L156–175; empty regions L176–191; scan row L192–199),
    `layout_inventory` sums L253–269.
  - `archival_structures/text_layer/presets.py`: `SG_LAYOUT_PARAMS` L73 (also the base of
    `SG_HAND_LAYOUT_PARAMS` L76, so **don't** add print-only keys to it), `SG_PRINT_V2` L123–141
    (`layout_params=SG_LAYOUT_PARAMS` L136).
  - `docs/text_layer.md` *Layout tables* L184–256 (params list ~L235, scans row ~L241, regions row ~L242).

```python
def _line_sort_key(line: pdm.PageXMLTextLine):
    if line.baseline is not None and line.baseline.points:
        y = sum(p[1] for p in line.baseline.points) / len(line.baseline.points)
    else:
        y = line.coords.y + line.coords.h / 2
    return (y, line.coords.x, line.coords.y, line.id or "")  # ties: box top, then id

LEGACY_15FP = LayoutParams(gutter_rule="gap", split_on_line_field="", furniture_patterns=(),
                           catchword_band=0.0, interleave_guard=False,
                           column_merge_guard=False, sort_split_lines=False)
def merge_regions(regions, *, union_keys=(), id_fn=region_box_id) -> pdm.PageXMLTextRegion
def split_regions_on_line_field(regions, field, *, id_fn=region_box_id, sort_lines=False
                                ) -> Tuple[List[pdm.PageXMLTextRegion], int]
# layout.py L171-175, what you change:
#     for i, line in enumerate(region.lines):
#         line_out[line.id] = {"page": page_name, "layout_region_id": region.id,
#                              "layout_line_idx": i, ...}
```

## Deliverables

| File | Change |
|---|---|
| `analysis/page_structure.py` | `LayoutParams.line_sort: str = "row"`, `row_tol_frac: float = 0.2`, `sort_unchanged_lines: bool = False`; validation; `LEGACY_15FP` adds `line_sort="baseline"`; `def sort_lines_by_row(lines, row_tol_frac) -> list`; `def sort_lines(lines, params) -> list`; kwarg `row_tol_frac=None` on `merge_regions` and `split_regions_on_line_field` |
| `text_layer/layout.py` | pass `row_tol_frac`; sort `unchanged` regions when `params.sort_unchanged_lines`; region column `lines_resorted`, scan column and stat `n_regions_resorted` |
| `text_layer/presets.py` | `SG_PRINT_LAYOUT_PARAMS = {**SG_LAYOUT_PARAMS, "sort_unchanged_lines": True}`; `SG_PRINT_V2.layout_params=SG_PRINT_LAYOUT_PARAMS` |
| tests | see *Tests* |
| `docs/text_layer.md` | *Layout tables*: the three params, the row rule, the two new columns |
| `docs/shared_library/changelog.md` | add `## Unreleased` above `## v0.13.0` (it is missing), then `### Step 15j: row-tolerant line order`, with Edit |
| `reports/phase-09-text-layer-v3-prep.md` | your `### Step 15j` section, findings `F09-15j.n` |

## Behaviour to implement

1. **`sort_lines_by_row(lines, row_tol_frac)`**: empty → `[]`. `tol = row_tol_frac × median(line.coords.h)`
   over the given lines. Walk the lines in `_line_sort_key` order; a line joins the current row when
   its baseline y (`_line_sort_key(line)[0]`) minus the baseline y of the row's **first** line is
   ≤ `tol`, else it starts a new row. Return the rows in order, each sorted by
   `(coords.x, baseline y, coords.y, id or "")`.
2. **`sort_lines(lines, params)`**: `sort_lines_by_row(lines, params.row_tol_frac)` when
   `params.line_sort == "row"`, else `sorted(lines, key=_line_sort_key)`.
3. **Validation** in `LayoutParams.__post_init__` (next to `gutter_rule`): `line_sort` in
   `("baseline", "row")`, `0 < row_tol_frac < 1`; else `ValueError`. `to_dict`/`from_dict` carry the
   new fields unchanged (they are plain values).
4. **`merge_regions(..., row_tol_frac=None)`**: `None` keeps the L255 sort exactly; else
   `sort_lines_by_row(all member lines, row_tol_frac)`. `merge_overlapping_text_regions` passes
   `row_tol_frac=params.row_tol_frac if params.line_sort == "row" else None`.
5. **`split_regions_on_line_field(..., sort_lines=False, row_tol_frac=None)`**: when `sort_lines`,
   sort each piece with the row rule if `row_tol_frac` is not None, else with `_line_sort_key`. An
   unsplit region is still returned as the same object. `layout_scan` passes
   `row_tol_frac=params.row_tol_frac if params.line_sort == "row" else None`.
6. **`unchanged` regions** in `layout_scan`, in the region loop: if `change == "unchanged"` and
   `params.sort_unchanged_lines`, `lines = sort_lines(region.lines, params)`, else
   `lines = region.lines`. `layout_line_idx` enumerates `lines`. `lines_resorted = 1` when the order of
   line ids changed, else 0. The id, `change`, box and `source_region_ids` don't change, and the
   `assert region.id == sources[0]` stays.
7. **Columns.** Append `"lines_resorted"` to `LAYOUT_REGION_COLUMNS` (0 for every other region,
   including the `empty` rows), `"n_regions_resorted"` to `LAYOUT_SCAN_COLUMNS` (per scan sum), and
   `"n_regions_resorted"` to `STAT_KEYS` and to both key tuples in `layout_inventory` (L255–256 and
   L267–269).
8. Name the rule in code comments: "step 15j, P4: rows within 0.2 × median box h; planning measured
   baseline gaps < 15 px within a row, about 48 px between rows, h about 75 px". Don't tune
   `row_tol_frac`.

## Constraints

- `LEGACY_15FP` output must not change: the existing legacy tests pass unchanged.
- v1 byte-identical: from the AS root,
  ```
  NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
  S=data/text_layer_v2_dev/15j-v1check; mkdir -p $S
  ls $NLP/output/voc_text_layer/voc_text_layer_v1/lines/ | sed 's/\.lines\.tsv\.gz$//' | paste -sd, - > $S/inv22.txt
  python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15j-v1 --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $S/r22 --stage all --inventories $(cat $S/inv22.txt) --write-events --workers {workers}
  .venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py $S      # TOTAL 157 / 157
  ```
- Build (check `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status` first):
  ```
  O=data/text_layer_v2_dev/15j-a; mkdir -p $O/hand $O/print
  CS="python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --want 8 --min 2 --wait"
  $CS --label 15j-hand -- .venv/bin/as-build-text-layer --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir $O/hand --preset sg_1.01.02_v2 --stage all --workers {workers}
  $CS --label 15j-print -- .venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir $O/print --preset sg_1.10.94_v2 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --workers {workers}
  ```
  About 3 min (hand) and 5 min (print) with 8 workers.
- **Protected**: before the first build and after the last, run
  `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py data/text_layer_v2_dev/15j-protected-{before,after}.txt --skip 15j-`; the `diff` must be empty.
  This covers `shared-data/`, LotA `data/ gold/ output/`, NLP `output/voc_text_layer`, and every earlier dev layer.
- Don't touch `join.py`, `extract_v2.py`, `line_split.py`, `word_breaks.py`, `types.py`, `units.py`.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`;
  no `rm -rf`, `nohup`, `&`. Sample sizes: "read N" means all of them if there are fewer.

## Tests

Add to `tests/test_analysis_page_structure.py` (29 tests now) and `tests/test_text_layer_layout.py` (6):
- `test_row_sort_same_row_fragments`: h=75 lines A (x 500, baseline y 1000), B (x 100, y 1006),
  C (x 100, y 1050) → row order B, A, C; `_line_sort_key` order A, B, C.
- `test_row_sort_keeps_separate_rows`: five lines with baselines 48 px apart, h 75, x descending →
  row order == `_line_sort_key` order.
- `test_line_sort_validated`: `line_sort="x"`, `row_tol_frac=0`, `row_tol_frac=1` raise `ValueError`.
- `test_merge_regions_row_key`: `merge_regions(..., row_tol_frac=0.2)` orders a same-row pair by x;
  `row_tol_frac=None` gives the old order.
- extend `test_split_sorts_lines` with `row_tol_frac=0.2`; extend `test_legacy_params_match_15fp_defaults`
  for `LEGACY_15FP.line_sort == "baseline"`.
- layout: `test_sort_unchanged_lines` (an unchanged region whose XML line order is out of y order:
  with `sort_unchanged_lines=True` `layout_line_idx` follows the row order, `lines_resorted == 1`,
  `layout_region_id`, `change` unchanged; with False XML order and 0) and
  `test_print_preset_sorts_unchanged` (`SG_PRINT_V2.layout_params["sort_unchanged_lines"] is True`,
  `"sort_unchanged_lines" not in SG_HAND_V2.layout_params`).
- If `test_legacy_matches_default_on_fixture` or `test_transitive_merge_deterministic` fails because
  the default is now `row`, read the fixture: if it has a same-row pair, set the test's params to
  `line_sort="baseline"` where it checks merge mechanics, keep a row-key assertion beside it, and
  record this under *Deviations*.
- Run `.venv/bin/python -m pytest -q -p no:cacheprovider` (full suite; 1,166 collected before; give the new
  count from `--collect-only -q`).

## Acceptance criteria

Measured by `order.py --layer <base> [--simulate]`, `compare.py`, and the build stats. Expected
values come from planning on `15ir-a` (`order_15ir-a_print.json`, `--simulate`). The simulation uses
non-blank lines only, so small differences are expected.
1. Both builds exit 0, 0 failures, `sanity_pass` true, no `--ignore-sanity`.
2. **Print, sorted pieces** (`change` ∈ `merged`, `merged+split_at_column`, `split_at_column`):
   same-row swaps 455 74+222+265 = 561 → ≤ 25 (simulated 12); 456 1+76+1,303 = 1,380 → ≤ 40
   (simulated 14); regions with a jump stay 0; regions whose order changed (from `compare.py` on
   `layout/lines` and your EA script) about 238 (455) and 863 (456).
3. **Print, `unchanged`**: jump regions 455 130 → 0, 456 0 → 0; `lines_resorted` 455 182 ± 20,
   456 7 ± 3; `n_regions_resorted` in `layout/stats.json` equals the column count.
4. **Hand** (`compare.py 15ir-a 15j-a --sub hand`): `layout/regions` and `layout/scans` differ only
   by the added columns (all 0), `layout/params.json` and `layout/stats.json` only by the new keys,
   `layout/lines` identical, `types/ units/ join/ wbc/` identical.
5. **Print downstream**: report the units count per inventory (15ir-a: 455 28,005, 456 24,710),
   `n_id_collisions`, and the number of `join/units` texts that differ.
6. AS suite green; v1 `TOTAL 157 / 157`; NLP `venv/bin/python -m pytest -q -p no:cacheprovider tests`
   (run in `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`) green; protected
   hashes unchanged.

## Error analysis

Write `reports/data/phase-09-text-layer-v3-prep/step-15j/order_diff.py`: for a region key it prints
the old (`15ir-a`) and new (`15j-a`) line order with baseline y, box x and `text[:40]`. Then read:
- 20 print regions from the sorted classes whose order changed (`random.Random(71)` over all of them);
- 20 resorted `unchanged` regions of 455 (`Random(72)`) and all of 456's;
- every remaining same-row swap in the sorted classes (≤ 25 + 40).

Categories: *right* (now reads like the page), *still wrong*, *worse*, *ambiguous* (drop capitals,
tables, title pages). Write the verdicts to `step-15j/ea_verdicts.tsv` (region, class, verdict, note).
Expected findings: the swap and jump counts (result), the share of right reorders, and what the
remaining swaps have in common.

## Do not touch

`shared-data/`, earlier dev layers, the v1 modules above, the report's *Summary*, *Review* and
*Follow-ups* sections, other steps' sections.

## When done

1. Fill `### Step 15j: row-tolerant line order` in `reports/phase-09-text-layer-v3-prep.md`
   (template `plan/templates/REPORT.md`): what changed, the results table (criteria 1–6), the error
   analysis, findings rows `F09-15j.n` (status `draft`). Run `make reports`; fix any warning.
2. Changelog entry (Edit): new `LayoutParams` fields and their defaults, `sort_lines_by_row`,
   `sort_lines`, the `row_tol_frac` kwargs, the two new columns, the print preset change, and that the
   default `LayoutParams()` now sorts merges and split pieces by rows (print output changes; no
   sibling imports these names, checked 2026-10-21).
3. Append the template `PROGRESS.md` block; overwrite `STATUS.md` (next: step 15k, which compares
   against `15j-a`). If a number in criteria 2–3 is off by more than the tolerance, stop and ask.
