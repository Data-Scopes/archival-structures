# Step 19b: hand column split (layout)

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** nothing (runs in parallel with 19a).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

The hand layout of text layers v2–v4 keeps each PageXML region as one layout region with its lines in XML order
(`merge=False`, no line sort). When a region holds two side-by-side text columns, its lines alternate between the
columns, and the hand unit (one per region) interleaves them: "28. Augr. Miss. Stadht. Kien; plaatsen van / van eenen
Os van Aaile naar Bakel" (3579). In the 125 local hand volumes 2,612 of 399,714 main_text units (0.65%) interleave
(phase-12 metric, below). The cases are index pages (two entry columns per page side), body pages whose region holds a
margin summary column beside the main text (3471 scans 0069, 0450, 0466), and bilingual insets (3192). This step adds a
geometric column split for hand regions (user decision: every hand region, not only index pages), off by default, and
tests it with a pre-registered read bar. Nothing is published; the v5 preset comes in 19e.

## Inputs

- `archival_structures/analysis/page_structure.py` (checked 2026-10-28):
  - `LayoutParams` L52–106, frozen dataclass; fields end with `line_sort: str = "row"`, `row_tol_frac: float = 0.2`,
    `sort_unchanged_lines: bool = False`; `__post_init__` validates `gutter_rule`, `line_sort`, `row_tol_frac`;
    `to_dict` = `dataclasses.asdict` with `furniture_patterns` as a list; `from_dict(cls, d)` turns
    `furniture_patterns` into a tuple and calls `cls(**d)`.
  - `_line_sort_key(line)` L135 (mean baseline y, else box centre y; then x, y, id); `sort_lines_by_row(lines,
    row_tol_frac) -> list` L143; `_new_region(lines, metadata, types=(), id_fn=region_box_id)` L173 (hull coords, id =
    `id_fn(metadata["scan_id"], coords)`); `region_box_id` L115.
  - `split_regions_on_line_field(regions, field, *, id_fn=region_box_id, sort_lines=False, row_tol_frac=None) ->
    (regions, n_splits)` L367–392: the model to follow (a region with one group is returned as the same object; pieces
    get `metadata = copy.deepcopy(region.metadata)`, `metadata["column_split_from"] = [region.id]`, and
    `_new_region(lines, metadata, sorted(region.types), id_fn)`).
- `archival_structures/text_layer/layout.py`: `layout_scan(...)` L91–208. Per page (L132–137):
  ```python
  for page_name, page_regions in pages.items():
      if params.split_on_line_field:
          page_regions, n_cs = split_regions_on_line_field(
              page_regions, params.split_on_line_field, id_fn=_region_id,
              sort_lines=params.sort_split_lines, row_tol_frac=row_tol)
          n_column_splits += n_cs
  ```
  with `row_tol = params.row_tol_frac if params.line_sort == "row" else None` (L130). `_change(region)` L78 maps
  `column_split_from` to `split_at_column`. Hand lines have `column == ""` (the merged-line splitter never runs in hand
  mode), so `split_regions_on_line_field` never splits a hand region.
- Build (`archival_structures/text_layer/build.py`): in a v4 mode run, `--layout-params JSON` is merged over the
  **hand and the print** layout params (`Run.mode_params`), and `layout/params.json` is written as `{"hand": …,
  "print": …}`; `v2_todo` reruns the listed inventories when a stage's `params.json` changes, and later stages rerun
  when their upstream row is newer.
- Tests to extend: `tests/test_analysis_page_structure.py` (33 tests; helpers `line(lid, x, y, w=400, h=40,
  text="tekst", column="")` L19 and `region(rid, lines)` L25), `tests/test_text_layer_layout.py` (8 tests; read
  `test_print_column_split_and_furniture` and `test_merge_off` as models). Full suite: 1,334 collected on 2026-10-28.
- Dev layers (read only): `data/text_layer_v2_dev/15y-b` and `15z-b` (`<base>/1.01.02/NL-HaNA_1.01.02_text_layer_v4/`).
- Planning (read only): `reports/data/phase-13-register-index-volumes/planning/hand_15y-b.txt` (111 inventories),
  `hand_15z-b.txt` (14), `interleave_v4_hand.tsv` (`inv, layer, units, interleaved`; totals 399,714 / 2,612),
  `side_features.tsv` (`inv, layer, scan_id, page, n_lines, n_text8, n_text20, col_share, end_num, date_start,
  gt_label`; read only the columns named here), `colsplit_prototype.py` (the planner's prototype on raw regions).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/page_structure.py` | `HAND_COLUMN_SPLIT_DEFAULTS`; `LayoutParams.hand_column_split`; `split_regions_into_columns(...)` |
| `archival_structures/text_layer/layout.py` | call it in `layout_scan` when `params.hand_column_split is not None` |
| `tests/test_analysis_page_structure.py`, `tests/test_text_layer_layout.py` | tests below |
| `reports/data/phase-13-register-index-volumes/step-19b/` | `interleave.py`, `split_stats.py`, `show_split.py`, `b1_reads.txt`, `b1_verdicts.tsv`, `remaining_reads.txt`, `protect_*.txt` |
| `docs/shared_library/changelog.md` | add `## Unreleased` above `## v0.17.0 (2026-10-28)` (none exists yet), then `### Step 19b: hand column split` |
| `reports/phase-13-register-index-volumes.md` | `### Step 19b: …` section, findings `F13-19b.n` |

## Behaviour to implement

**`LayoutParams`.** Add, after `sort_unchanged_lines`:
```python
# step 19b (phase 13, P2/P6): geometric split of hand regions that hold side-by-side columns; None = off
hand_column_split: Optional[dict] = dataclasses.field(default=None, hash=False)
```
`__post_init__`: if not `None`, every key must be in `HAND_COLUMN_SPLIT_DEFAULTS`, else `ValueError`. `to_dict`:
**leave the key out when it is `None`** (so every v1–v4 `layout/params.json` stays byte-identical); otherwise copy the
dict. `from_dict` accepts it. Always test `is not None`, never truthiness (`{}` means "all defaults").

```python
HAND_COLUMN_SPLIT_DEFAULTS = {"min_lines": 3, "min_gap_frac": 0.25, "max_col_frac": 0.65, "min_col_w_frac": 0.3,
                              "tol_h": 0.5, "min_overlap_h": 2.0, "max_span_frac": 0.34}
def split_regions_into_columns(regions, cfg: dict, *, id_fn=region_box_id, row_tol_frac=None) -> (list, int)
```
For each region, with `c = {**HAND_COLUMN_SPLIT_DEFAULTS, **cfg}` and `T` = its lines whose stripped text is
non-empty (all statistics use `T`; blank lines are assigned by geometry like the others):
1. `len(T) < 2 * min_lines` → unchanged. `x0 = min x`, `x1 = max(x + w)`, `W = x1 - x0`, `mh = median h` over `T`.
2. Candidates `N` = lines of `T` with `w <= max_col_frac * W`, sorted by `(x, y, id)`; `len(N) < 2 * min_lines` →
   unchanged.
3. Gaps between consecutive candidate `x`; take the largest (ties: the leftmost); `gap < min_gap_frac * W` → unchanged.
   `sx` = the `x` of the candidate right after that gap; `tol = tol_h * mh`.
4. Every line of the region: **left** if `x + w <= sx + tol`; else **right** if `x >= sx - tol`; else **span**.
5. Guards over `T`: ≥ `min_lines` left and ≥ `min_lines` right; span ≤ `max_span_frac * len(T)`; median `w` of the left
   lines and of the right lines each ≥ `min_col_w_frac * W`; vertical overlap `min(bottom_L, bottom_R) - max(top_L,
   top_R) >= min_overlap_h * mh` (bottom = max `y + h`, top = min `y`). Any guard fails → unchanged.
6. Pieces: walk all lines in `_line_sort_key` order. Keep a current left list and right list. A span line closes the
   current block: emit the left piece, then the right piece (each only if non-empty), then the span line goes into a
   span piece (consecutive span lines share one span piece). At the end emit the last block. Lines of each piece are
   sorted with `sort_lines_by_row(lines, row_tol_frac)` when `row_tol_frac` is given, else by `_line_sort_key`.
7. Each piece: `metadata = copy.deepcopy(region.metadata)`, `metadata["column_split_from"] = [region.id]`,
   `_new_region(lines, metadata, sorted(region.types), id_fn)`. An unchanged region is returned as the same object.
   Return `(regions, number of regions split)`.

Comment the function with the rationale: phase 13 P2/P6, F12-R.22, the prototype numbers (3471: 165 of 5,857 regions
split; the span guard removed the two fragmented-line splits of 3222).

**`layout_scan`.** Right after the `split_on_line_field` block:
```python
if params.hand_column_split is not None:   # step 19b
    page_regions, n_hs = split_regions_into_columns(page_regions, params.hand_column_split, id_fn=_region_id,
                                                    row_tol_frac=row_tol)
    n_column_splits += n_hs
```
No new table column: pieces show as `change == "split_at_column"` and count in `n_column_splits` (hand lines carry no
`column`, so in hand mode these are always geometric splits). Pieces then go through furniture, column grouping and
roles like any region; the unit stage needs no change.

## Constraints

- With `hand_column_split=None` every output of every preset is byte-identical (tables and `params.json`).
- Do not change `split_regions_on_line_field`, the merge code, `units.py`, `types.py`, `presets.py` or `build.py`.
- **Protected outputs:** before the first run, `.venv/bin/python -I
  reports/data/phase-09-text-layer-v3-prep/planning/protect.py reports/data/phase-13-register-index-volumes/step-19b/protect_before.txt
  --skip 19b` and `find data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/15z-b data/text_layer_v4_full -type f | sort | xargs shasum -a 256 >
  reports/data/phase-13-register-index-volumes/step-19b/protect2_before.txt`; the same `_after` at the end; `diff`
  both. `shared-data/` changes by other projects are expected; name them.
- Clones only (`cp -cR`, APFS copy-on-write); if `cp -c` fails, stop. Never write into `15y-b`/`15z-b`.
- More than one worker only through coreshare: `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py
  status`, then `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project
  archival-structures --label 19b-<stage> --want 4 --min 1 --wait -- <command> --workers {workers}`.
- A read sample with fewer cases than asked: read all of them. Never `git commit`; `chmod g+w` (`-R` on the clones);
  `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`. Shared files only with Edit; step 19a runs at the same time.

## Tests

New, in `tests/test_analysis_page_structure.py` (use the `line`/`region` helpers; `h=40`):
- `test_hand_column_split_two_columns`: 6 left lines (`x=100, w=400`) and 6 right lines (`x=600, w=400`), y 100..350
  step 50, given in alternating XML order L0 R0 L1 R1 …; result 2 pieces: left lines in y order, then right; each
  `metadata["column_split_from"] == ["R"]`; `n == 1`.
- `test_hand_column_split_single_column_unchanged`: 8 lines `x=100, w=900` → the same object, `n == 0`.
- `test_hand_column_split_heading_closes_block`: 3+3 lines, a span line `x=100, w=900` below them, then 3+3 lines →
  5 pieces in order L1, R1, S, L2, R2.
- `test_hand_column_split_guards`: no split for (a) only 2 right lines; (b) a left column of `w=150` beside `w=700`
  main lines (median width < 0.3·W); (c) span lines > 0.34 of the lines; (d) the left block entirely above the right
  block (no vertical overlap).
- `test_layout_params_hand_column_split`: `"hand_column_split" not in LayoutParams().to_dict()`; round trip
  `LayoutParams.from_dict(p.to_dict()) == p` with a dict; an unknown key raises `ValueError`;
  `hash(LayoutParams(hand_column_split={}))` works.
New in `tests/test_text_layer_layout.py`: `test_hand_column_split_layout_scan`, modelled on
`test_print_column_split_and_furniture`: a hand region with two interleaved columns is laid out as two regions with
`change == "split_at_column"`, different `layout_column`, lines in row order; with the field `None` the output equals
today's.
Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_structure.py
tests/test_text_layer_layout.py`, then the full suite (1,334 + your new tests; counts from `--collect-only`).

## Dev run (from the AS root)

1. Protect `_before`. Clones: `cp -cR data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/19b-a`, `cp -cR
   data/text_layer_v2_dev/15z-b data/text_layer_v2_dev/19b-z`, `cp -cR data/text_layer_v2_dev/15y-b
   data/text_layer_v2_dev/19b-v4check`; `chmod -R g+w` each.
2. **Off = unchanged:** `.venv/bin/as-build-text-layer --preset sg_1.01.02_v4 --out-dir
   data/text_layer_v2_dev/19b-v4check/1.01.02 --stage layout --force --inventories 3471,3579,46 --workers 1`; compare
   `layout/params.json` (bytes) and `layout/{scans,regions,lines}/<inv>.tsv.gz` (decompressed) with `15y-b`: equal.
3. With `SPLIT='{"hand_column_split": {"min_lines": 3, "min_gap_frac": 0.25, "max_col_frac": 0.65, "min_col_w_frac":
   0.3, "tol_h": 0.5, "min_overlap_h": 2.0, "max_span_frac": 0.34}}'` and `P=reports/data/phase-13-register-index-volumes/planning`,
   for `(19b-a, hand_15y-b.txt)` and `(19b-z, hand_15z-b.txt)`: `--stage layout --layout-params "$SPLIT"`, then
   `--stage types`, then `--stage units`, each `--preset sg_1.01.02_v4 --out-dir data/text_layer_v2_dev/<clone>/1.01.02
   --inventories-file $P/<list>` (coreshare as above). Only the hand inventories are rebuilt; print outputs stay v4.
4. `interleave.py BASE…`: the phase-12 metric over the 125 hand volumes. A main_text unit is **interleaved** if, over
   its segments in `seg_order`, keeping only lines with `w >= 500`, at least 3 consecutive pairs differ in `x` by more
   than 800 px. Inputs per inventory: `lines/<inv>.lines.tsv.gz` (`x`, `w`), `units/units/<inv>.tsv.gz`
   (`unit_type`), `units/segments/<inv>.tsv.gz` (`unit_id, seg_order, line_id`). Run it first on `15y-b`/`15z-b`: it
   must give exactly 399,714 units and 2,612 interleaved (else stop); then on `19b-a`/`19b-z`. Write per-volume rows.
5. `split_stats.py`: per volume the regions split, pieces, span pieces, lines in split regions; split regions on
   **index-like** sides (`side_features.tsv`: `col_share != ""` and `col_share >= 0.25` and `max(end_num, date_start)
   >= 0.15`, joined on `inv, scan_id, page`) and elsewhere; main_text units before/after.
6. **Bar B1** (`show_split.py`): the split regions are the groups of `layout/regions` rows with `change ==
   "split_at_column"` in the hand inventories, keyed by their `source_region_ids` (the original PageXML region). Sort
   the index-like and the other groups by `(int(inv), scan_id, region id)`; `random.Random(20261117)`: `sample` 20 of
   each (all, if fewer). Write `b1_reads.txt`: per case the original region's lines in XML order (`lines` table,
   `line_idx`) with `x y w | text`, then each piece in layout order (`layout_column`, piece, `layout_line_idx`). Read
   all 40 and write `b1_verdicts.tsv` (`case, inv, scan_id, region_id, side_class, verdict, kind, note`):
   - `right`: the region held side-by-side text (two index columns, a margin column beside the main text, parallel
     language columns) and each piece reads continuously in its new order; span pieces are headings or genuinely
     full-width lines;
   - `wrong`: the split cuts fragments of the same written lines apart, splits a single column, or leaves a piece that
     no longer reads continuously;
   - `unclear`; `unreadable input` (more than half the lines garbled).
7. 10 interleaved units that remain in `19b-a`/`19b-z` (`random.Random(20261118)` over their sorted ids; all, if
   fewer): print their lines (`remaining_reads.txt`) and classify the cause: HTR gutter merge (one line with both
   columns), a guard kept the region whole (which guard; recompute it), line fragments, other.
8. Protect `_after`; `diff`.

## Acceptance criteria

1. Tests green; full suite green (counts from `--collect-only`).
2. Step 2: all compared files equal.
3. Step 4: baseline reproduced exactly (399,714 / 2,612); the new count and share reported (no bar).
4. **Bar B1** (pre-registered 2026-10-28): `right / (right + wrong) >= 0.90` over the 40, and `wrong <= 2` among the
   20 not-index-like cases. More than 8 `unclear` + `unreadable input` → not judgeable: stop and report. A fail is a
   negative result: report it, no tuning, add a Decide UA (next free ID) on whether v5 ships the split; steps 19c–19e
   wait for its answer.
5. No duplicate `layout_region_id` within a scan in the rebuilt inventories (check and report).
6. Protected lists unchanged apart from the clones and other projects' `shared-data/` changes (name them).

The user decides when the step is done.

## Error analysis

The B1 reads and the 10 remaining units are the error analysis. Report: verdict counts per side class, the `kind`s
(index columns, margin column, bilingual, minuten half-page, fragments, …) with counts, every `wrong` case verbatim
(shortened), and what the remaining interleaved units have in common. Expected findings (not measured yet): the
interleave share after the split; how many split regions are on index-like sides; whether margin-column splits put the
margin summary before the main text (the layout column order decides; say what you see).

## Do not touch

`units.py`, `types.py`, `presets.py`, `build.py`, the published layers, `data/text_layer_v2_dev/{15y-b,15z-b,15x-a,15z-a}`
and older dev layers, `data/text_layer_v4_full/`, the planning files, `shared-data/`, the siblings. Never touch the
external drive.

## When done

1. Report: `### Step 19b: hand column split` in `reports/phase-13-register-index-volumes.md` (what changed, results
   table, B1 verdicts, error analysis, findings `F13-19b.n` as `draft`); `make reports` clean.
2. Changelog (Edit): `## Unreleased` + `### Step 19b: hand column split`: `LayoutParams.hand_column_split` (omitted from
   `to_dict` when `None`), `HAND_COLUMN_SPLIT_DEFAULTS`, `page_structure.split_regions_into_columns`, the
   `layout_scan` call. "Affected sibling imports: none (no preset sets it yet)."
3. `PROGRESS.md` block (template of `plan/templates/STEP.md`, title above); overwrite `STATUS.md`: 19b done, the B1
   result, facts for 19c (clone paths `19b-a`, `19b-z`; the `SPLIT` JSON; the new interleave counts).
