# Step 19f: column order v2 for two-column hand regions

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** 19b, 19c (code and clones).
> **Conditional:** run only if `docs/USER_ACTIONS.md` UA-042 (or UA-040) is answered with this step; the phase
> `STATUS.md` says so under *Next action*. Written by the phase review of 2026-10-28.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

A hand PageXML region that holds two side-by-side columns lists its lines alternating between them, so the hand
unit interleaves two texts (2,612 of 399,714 main_text units of the 125 local hand volumes). Step 19b split such
regions into pieces and failed its read bar B1 (29 right, 9 wrong). The review traced every wrong case to the rule:
lines were classed by their extent (ordinary left lines reaching past the gutter became "span" lines and stranded
1-line pieces), and pieces were ordered by the page-level column grouping, where a span piece joins both columns so
the right column could come first. This step adds a second mode that keeps the region whole and re-orders its lines
column by column, classifying each line by where it **starts**. No new units, so no piece-order problem. It is tested
with a fresh, pre-registered read bar.

## Inputs (checked 2026-10-28)

- `archival_structures/analysis/page_structure.py`:
  - `HAND_COLUMN_SPLIT_DEFAULTS` L53 = `{"min_lines": 3, "min_gap_frac": 0.25, "max_col_frac": 0.65,
    "min_col_w_frac": 0.3, "tol_h": 0.5, "min_overlap_h": 2.0, "max_span_frac": 0.34}`.
  - `LayoutParams.hand_column_split: Optional[dict] = dataclasses.field(default=None, hash=False)` L94;
    `__post_init__` L106 rejects keys not in the defaults; `to_dict` L118 drops it when `None`, else copies the dict.
  - `split_regions_into_columns(regions, cfg, *, id_fn=region_box_id, row_tol_frac=None) -> (list, int)` L423 and
    `_column_pieces(region, c, row_tol_frac)` L454 (anchor `def _column_pieces`): steps 1–3 compute `T` (lines with
    stripped text), `W`, `mh` (median `h`), candidates `N` (`w <= max_col_frac * W`, sorted by `(x, y, id)`), the
    largest gap between consecutive candidate `x` (ties leftmost), `sx` = the `x` after it, `tol = tol_h * mh`.
  - `_line_sort_key(line)` L163; `sort_lines_by_row(lines, row_tol_frac) -> list` L171; `_new_region(lines,
    metadata, types=(), id_fn=region_box_id)` L201 (hull coords, id = `id_fn(scan_id, coords)`).
- `archival_structures/text_layer/layout.py`: `_change(region)` L81 maps metadata keys to the `change` column
  (`merged_from` → `merged`, `split_from` → `split_at_gutter`, `column_split_from` → `split_at_column`, joined by
  `+`); `layout_scan` L94 calls `split_regions_into_columns(page_regions, params.hand_column_split,
  id_fn=_region_id, row_tol_frac=row_tol)` (L142) and adds the count to `n_column_splits`. No later stage reads
  `change`.
- `archival_structures/text_layer/presets.py`: `SG_INDEX_SIDES_V5` (dict; print it as JSON with `.venv/bin/python -I
  -c "import json; from archival_structures.text_layer.presets import SG_INDEX_SIDES_V5 as s; print(json.dumps(s))"`).
- Dev layers (read only): `data/text_layer_v2_dev/19c-a` (111 hand inventories of `15y-b` rebuilt with
  `index_sides`), `19c-z` (14 of `15z-b`), `19b-a` (19b's pieces), `15y-b`, `15z-b`. Sub-path
  `1.01.02/NL-HaNA_1.01.02_text_layer_v4/`. Lists: `reports/data/phase-13-register-index-volumes/planning/
  hand_15y-b.txt`, `hand_15z-b.txt`.
- 19b scripts to copy and adapt (read them): `reports/data/phase-13-register-index-volumes/step-19b/{common.py,
  interleave.py, show_split.py}`; 19b's read cases `step-19b/b1_verdicts.tsv` (column `region_id` = source region).
- Tests: `tests/test_analysis_page_structure.py` (38 tests; helpers `line(lid, x, y, w=400, h=40, text="tekst",
  column="")` and `region(rid, lines)`), `tests/test_text_layer_layout.py` (10 tests; model
  `test_hand_column_split_layout_scan`). Full suite: 1,355 collected on 2026-10-28.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/page_structure.py` | defaults gain `"mode": "pieces"`, `"span_reach_frac": 0.5`; mode `"reorder"` |
| `archival_structures/text_layer/layout.py` | `_change`: `column_reorder_from` → `column_reordered` |
| `archival_structures/text_layer/presets.py` | constant `SG_HAND_COLUMN_ORDER_V5` (no preset uses it) |
| tests (two modules above) | tests below |
| `reports/data/phase-13-register-index-volumes/step-19f/` | scripts, `b2_reads.txt`, `b2_verdicts.tsv`, `remaining_reads.txt`, tables, `protect_*.txt` |
| `docs/shared_library/changelog.md` | `### Step 19f: column order v2` under *Unreleased* (Edit) |
| `reports/phase-13-register-index-volumes.md` | `### Step 19f: column order v2` section after the 19e heading, findings `F13-19f.n` |

## Behaviour to implement

`HAND_COLUMN_SPLIT_DEFAULTS` gains `"mode": "pieces"` and `"span_reach_frac": 0.5`. `LayoutParams.__post_init__`
also raises `ValueError` when `mode` is given and not in `("pieces", "reorder")`. **Mode `"pieces"` (the default)
must stay exactly 19b's behaviour**; `to_dict` keeps copying the dict as given.

Mode `"reorder"`, per region (`c = {**HAND_COLUMN_SPLIT_DEFAULTS, **cfg}`):
1. Steps 1–3 of `_column_pieces` unchanged (same `T`, `W`, `mh`, `N`, gap guard, `sx`, `tol`). You may factor them
   into a helper used by both modes; acceptance 2 checks that pieces mode is unchanged.
2. Every line of the region (blank lines too): **left** if `x < sx - tol`, else **right**.
3. Over `T`: `R` = right lines; `len(R) < min_lines` → unchanged. `wR` = median `w` of `R`. **Wide** = left lines of
   `T` with `x + w > sx + span_reach_frac * wR` (a gutter merge or a heading over both columns). `Ln` = left lines of
   `T` that are not wide.
4. Guards (any fails → unchanged): `len(Ln) >= min_lines`; `len(wide) <= max_span_frac * len(T)`; median `w` of `Ln`
   and of `R` each `>= min_col_w_frac * W`; `min(bottom(Ln), bottom(R)) - max(top(Ln), top(R)) >= min_overlap_h * mh`
   (bottom = max `y + h`, top = min `y`).
5. New order = the left lines (wide lines included, at their row) sorted, then the right lines sorted; sort with
   `sort_lines_by_row(lines, row_tol_frac)` when `row_tol_frac` is given, else by `_line_sort_key`. If the new order
   equals the region's current line order → unchanged.
6. Otherwise one new region: `metadata = copy.deepcopy(region.metadata)`, `metadata["column_reorder_from"] =
   [region.id]`, `_new_region(new_order, metadata, sorted(region.types), id_fn)`; it counts in the returned number.
   An unchanged region is returned as the same object.

`layout._change`: add `("column_reorder_from", "column_reordered")` to the tuple. Comment both changes with the
rationale (phase 13 review, F13-R.7: B1 failed on extent-based span lines and piece order; reorder keeps one unit).
`presets.py`: `SG_HAND_COLUMN_ORDER_V5 = {**HAND_COLUMN_SPLIT_DEFAULTS, "mode": "reorder"}` written out as a literal
dict with a comment pointing to this step.

## Constraints

- With `hand_column_split=None` every output stays byte-identical; with 19b's dict (no `mode`) the output equals 19b's.
- Do not change `units.py`, `types.py`, `index_pages.py`, `build.py`, `group_regions_into_columns`, the merge code.
- **Protected outputs:** before the first run `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
  reports/data/phase-13-register-index-volumes/step-19f/protect_before.txt --skip 19b 19c 19d 19f`, and `find
  data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/15z-b data/text_layer_v2_dev/19b-a data/text_layer_v2_dev/19c-a
  data/text_layer_v2_dev/19c-z data/text_layer_v4_full -type f | sort | xargs shasum -a 256 >
  reports/data/phase-13-register-index-volumes/step-19f/protect2_before.txt`; the same `_after` at the end; `diff`
  both. `shared-data/` changes by other projects are expected; name them.
- Clones only (`cp -cR`, APFS copy-on-write; if `cp -c` fails, stop). Never write into a layer you did not clone.
- More than one worker only through coreshare: `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py
  status`, then `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project
  archival-structures --label 19f-<stage> --want 4 --min 1 --wait -- <command> --workers {workers}`.
- **Never run `git stash`, `checkout`, `reset` or anything else that changes the working tree** (another session may
  be editing it). Count tests with `pytest --collect-only -q`. Never `git commit`; `chmod g+w` (`-R` on clones);
  `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`. Shared files only with Edit. Never touch the external drive.

## Tests

In `tests/test_analysis_page_structure.py` (`h=40`, `cfg = {"mode": "reorder"}`):
- `test_hand_column_reorder_two_columns`: 6 left (`x=100, w=400`) and 6 right (`x=600, w=400`) lines, y 100..350
  step 50, XML order L0 R0 L1 R1 …; one region out, lines L0..L5 then R0..R5, `metadata["column_reorder_from"] ==
  ["R"]`, `n == 1`.
- `test_hand_column_reorder_start_not_extent`: the same, but the left lines have `w=560` (they reach past `sx`; mode
  `pieces` classes them span): reorder gives L0..L5, R0..R5 and no wide line.
- `test_hand_column_reorder_wide_line_stays_left`: a line `x=100, w=900` at y 50 above 4+4 lines: order wide, L…, R….
- `test_hand_column_reorder_unchanged`: lines already L0..L5, R0..R5 in XML order → the same object, `n == 0`; a
  single column of 8 lines → the same object.
- `test_hand_column_reorder_guards`: 2 right lines → unchanged; wide lines > 0.34 of the lines → unchanged.
- `test_layout_params_hand_column_split_mode`: `{"mode": "x"}` raises `ValueError`; `{"mode": "reorder"}` round
  trips through `to_dict`/`from_dict`.
In `tests/test_text_layer_layout.py`: `test_hand_column_reorder_layout_scan` (model: `test_hand_column_split_layout_scan`):
one layout region, `change == "column_reordered"`, `layout_line_idx` gives left lines then right lines.
The six 19b tests stay unchanged and green. Commands: the two modules, then the full suite (1,355 + new; counts from
`--collect-only`).

## Dev run (from the AS root; `P=reports/data/phase-13-register-index-volumes/planning`)

1. Protect `_before`. Clones: `cp -cR data/text_layer_v2_dev/19c-a data/text_layer_v2_dev/19f-a`, same `19c-z` →
   `19f-z`, and `cp -cR data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/19f-piecescheck`; `chmod -R g+w`.
2. **Pieces mode unchanged:** on `19f-piecescheck`, `--stage layout --inventories 3471,3579 --layout-params
   '{"hand_column_split": {"min_lines": 3, "min_gap_frac": 0.25, "max_col_frac": 0.65, "min_col_w_frac": 0.3,
   "tol_h": 0.5, "min_overlap_h": 2.0, "max_span_frac": 0.34}}' --workers 1` (`--preset sg_1.01.02_v4 --out-dir
   data/text_layer_v2_dev/19f-piecescheck/1.01.02`); `layout/{regions,lines}/<inv>.tsv.gz` (decompressed) equal to
   `19b-a`'s.
3. `LP='{"hand_column_split": <SG_HAND_COLUMN_ORDER_V5 as JSON>, "index_sides": <SG_INDEX_SIDES_V5 as JSON>}'` (both
   keys, so `layout/pages` stays current). For `(19f-a, hand_15y-b.txt)` and `(19f-z, hand_15z-b.txt)`: `--stage
   layout --layout-params "$LP"`, then `--stage types`, then `--stage units`, each with `--preset sg_1.01.02_v4
   --out-dir data/text_layer_v2_dev/<clone>/1.01.02 --inventories-file $P/<list>` (coreshare).
4. Interleave (copy `step-19b/interleave.py`): on `19f-a`/`19f-z`. Baseline 2,612 of 399,714 (v4); report the new
   count. main_text units per inventory against `19c-a`/`19c-z` (reorder adds no regions; list any inventory that
   differs and why).
5. Statistics: reordered regions per volume, on detector-flagged sides (`layout/pages`, `index_side == "1"`) and
   elsewhere; wide lines per reordered region; no duplicate `layout_region_id` within a scan.
6. **Bar B2** (`show_order.py`, adapted from `step-19b/show_split.py`): cases = `layout/regions` rows with `change`
   containing `column_reordered` in the hand inventories, keyed by `source_region_ids`; drop the 40 source regions
   in `step-19b/b1_verdicts.tsv`. Split by side: **flagged** (the row's `(scan_id, page)` has `index_side == "1"`) or
   **other**. Sort each by `(int(inv), scan_id, source region id)`; one `random.Random(20261125)`: `sample` 20 flagged,
   then 20 other (all, if fewer). `b2_reads.txt`: per case the original lines in XML order (`x y w | text`), then the
   new order with a tag per line `L`, `W` (wide) or `R`. Read all 40; write `b2_verdicts.tsv` (`case, inv, scan_id,
   region_id, side_class, verdict, kind, note`). Verdicts (fixed 2026-10-28):
   - `right`: the region held two side-by-side texts (index columns, a margin column beside the main text, parallel
     language columns, two text columns) and in the new order the left text reads continuously top to bottom, then
     the right text does; a `W` line is a gutter merge or a heading and sits at its row in the left run;
   - `wrong`: (a) a line of one text sits in the other text's run (other than a `W` line); (b) a run is out of
     top-to-bottom order; (c) the region did not hold two side-by-side texts (one column with indents, a table,
     fragments of the same written lines) and its reading order changed;
   - `unclear`; `unreadable input` (more than half the lines garbled).
7. 10 interleaved units that remain in `19f-a`/`19f-z` (`random.Random(20261126)` over their sorted ids; all, if fewer):
   print them (`remaining_reads.txt`) and classify: gutter merge, a guard kept the region (which; recompute it), not
   columns (metric false positive), other.
8. Protect `_after`; `diff`.

## Acceptance criteria

1. Tests green; full suite green (counts from `--collect-only`).
2. Step 2: equal files. `LayoutParams().to_dict()` has no `hand_column_split`.
3. Step 4: the new interleave count and share, and the unit counts against `19c-*` (no bar).
4. **Bar B2** (pre-registered 2026-10-28 by the phase review, same thresholds as B1): `right / (right + wrong) >= 0.90`
   over the 40 and `wrong <= 2` among the 20 `other` cases. More than 8 `unclear` + `unreadable input` → not judgeable:
   stop and report. A fail is a negative result: no tuning, report it, add a Decide UA (next free ID) on whether v5
   ships the reorder. A pass: STATUS says 19g may use `19f-a`/`19f-z` as its base if the user agrees.
5. No duplicate `layout_region_id` within a scan; protected lists unchanged apart from the clones and other projects'
   `shared-data/` changes (name them).

The user decides when the step is done.

## Error analysis

The B2 reads and the 10 remaining units are the error analysis. Report verdict counts per side class, the `kind`s
with counts, every `wrong` case verbatim (shortened), and the share of reordered regions whose right text starts above
the left one (where reading order is a choice). Expected (not measured): interleave falls to about the 19b level
(980) or below; unit counts barely change.

## Do not touch

`units.py`, `types.py`, `index_pages.py`, `build.py`, the published layers, every dev layer except your clones, the
planning, 19a–19d and review files, `shared-data/`, the siblings.

## When done

1. Report: `### Step 19f: column order v2` (what changed, results, B2 verdicts, error analysis, findings `F13-19f.n`
   as `draft`); `make reports` clean.
2. Changelog (Edit): `### Step 19f: column order v2`: the `mode` and `span_reach_frac` keys (default `"pieces"` =
   19b), the `column_reordered` change value, `presets.SG_HAND_COLUMN_ORDER_V5`. "Affected sibling imports: none."
3. `PROGRESS.md` block (template of `plan/templates/STEP.md`); overwrite `STATUS.md`: 19f done, the B2 result, the
   clone paths and `LP` JSON for 19g.
