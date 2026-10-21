# Step 15fq2: Column merge guard (UA-008 (b))

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15fq, 15g.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Step 15fq splits print layout regions on the line `column` field (`'1'`, `'2'`, `''`) and then
merges overlapping regions. Its criterion 6 (0 print layout regions whose lines carry both
`column` `1` and `2`) failed with 145 regions. The pieces of a column split overlap each other,
and the merge joins them again. The user chose UA-008 (b), 2026-10-21: a structural rule, with no
threshold, so that two regions whose lines carry different non-empty `column` values never end up
in one merge set. This step adds that rule, rebuilds the layout layer as `15fq-c`, and re-runs
15g's `types` stage on it as `15g-b`. 15h then runs on `15g-b`.

Measured on `data/text_layer_v2_dev/15fq-b/print/` (Opus, 2026-10-21; matches UA-008):

| inv | mixed regions | scans | `change` | median sources | `layout_role` | all merged |
|---|---|---|---|---|---|---|
| 455 | 36 | 35 | `merged+split_at_column` 32, `merged` 4 | 3 | main 36 | 1,784 |
| 456 | 109 | 106 | `merged+split_at_column` 109 | 1 | main 109 | 205 |

The 456 median of 1 source means that most of these merges glue the pieces of **one** source
region back together.

## Inputs

Code (as of 15fq; grep the anchors, line numbers drift):
- `archival_structures/analysis/page_structure.py`: `class LayoutParams` (L53, fields L54–74),
  `LEGACY_15FP` (L89), `_guard_blocks` (L134), `get_overlapping_region_sets` (L152–195),
  `merge_overlapping_text_regions` (L231–253), `split_regions_on_line_field` (L258).
- `archival_structures/text_layer/layout.py`: `LAYOUT_SCAN_COLUMNS` (L30–33), `layout_scan`
  (L91–197: the merge call is at `merge_overlapping_text_regions(` in the per-page loop, and the
  counter is `n_blocked`), `layout_inventory` (stats sums at `for k in ("n_lines_crossing"`, two
  places).
- `tests/test_analysis_page_structure.py`: helpers `line(lid, x, y, w=400, h=40, text="tekst",
  column="")`, `region(rid, lines)` and `column_region(rid, x, y0, n, w=400, step=50, prefix=None)`
  at the top of the file.
- `reports/data/phase-07-sg-units-and-pilot/step-15fq/acceptance.py LAYER_DIR` computes 15fq's
  criteria 1–7. Its `LEGACY`/`BASE` constants point at `15fq-legacy` and `15fp-a`, and its output
  name is `acceptance_<layer>.json`. Copy it to `step-15fq2/` and adapt it; don't edit the
  original.
- Step 15g's acceptance script under `reports/data/phase-07-sg-units-and-pilot/step-15g/`. See
  *15g re-run* below.

Current signatures:

```python
def get_overlapping_region_sets(regions, overlap_threshold: float = 0.5, *,
                                exclude: Collection[str] = (),
                                guard_width_frac: Optional[float] = None,
                                blocked: Optional[Dict[Tuple[str, str], None]] = None
                                ) -> List[List[pdm.PageXMLTextRegion]]
def merge_overlapping_text_regions(regions, params: LayoutParams = LayoutParams(), *,
                                   union_keys=(), id_fn=region_box_id, exclude=(),
                                   blocked=None) -> Tuple[List, int]
```

`get_overlapping_region_sets` builds a neighbour list over `combinations(range(n), 2)` and then
takes DFS components. Sets are ordered by their first member in input order, and members keep
input order. `merge_overlapping_text_regions` repeats until no set has more than one member, so a
merged region's lines carry every member's lines.

Layers (read only; see *Constraints*):
- `data/text_layer_v2_dev/15fq-b/{hand_tar,hand_local,print}/` is the final 15fq layer. Hand has
  merges off (`SG_HAND_V2.layout_params` has `merge=False`), and print has merges on.
- `data/text_layer_v2_dev/15g-a/` is 15g's types layer (a copy of `15fq-b` plus `types/`).

Build CLI (the run root is `<out-dir>/<preset name>/`, e.g.
`15fq-c/print/NL-HaNA_1.10.94_text_layer_v2/`):

```
.venv/bin/python -m archival_structures.text_layer.build --out-dir <dir> \
    --preset sg_1.10.94_v2 --stage layout --inventories 455,456 [--layout-params '<json>'] [--force]
```

Hand uses `--preset sg_1.01.02_v2`, with 3349–3354 under `hand_tar` and 3771, 3772, 3773, 3821,
3822, 3823, 11182, 11281 under `hand_local`. If the stage refuses to overwrite existing
`layout/` tables, add `--force`.

## Deliverables

| File | Change |
|---|---|
| `analysis/page_structure.py` | `LayoutParams.column_merge_guard: bool = True`; `LEGACY_15FP` gets `column_merge_guard=False`; `get_overlapping_region_sets(..., column_field: str = "", column_blocked=None)`; `merge_overlapping_text_regions(..., column_blocked=None)` passes the field |
| `text_layer/layout.py` | `n_column_blocked` appended at the **end** of `LAYOUT_SCAN_COLUMNS`; summed in `layout/stats.json` |
| `tests/test_analysis_page_structure.py` | See *Tests* |
| `docs/text_layer.md` | § *Layout tables (steps 15fp, 15fq)* → *(steps 15fp, 15fq, 15fq2)*: the rule, the param, the column |
| `docs/shared_library/changelog.md` | `### Step 15fq2: column merge guard` under `## Unreleased`, below Step 15fq's entry (and below any 15g entry), with Edit. Behaviour change: default print layout output; no sibling imports affected (siblings don't import `page_structure` yet; check with `grep -rn page_structure` in both siblings' `*.py`, read only) |
| `reports/data/phase-07-sg-units-and-pilot/step-15fq2/` | `acceptance.py`, `ea.py`, `ea_verdicts.tsv`, JSON outputs |
| `reports/phase-07-sg-units-and-pilot.md` | A new `### Step 15fq2: column merge guard` section **after** § Step 15g; findings `F07-15fq2.n` |

## Behaviour to implement

**The rule.** For a region R and a field f, let `vals(R)` be the set of non-empty
`line.metadata.get(f, "")` over R's lines. Two regions can end up in one merge set only if the
union of their `vals` has at most one element. A `''` line conflicts with nothing.

**In `get_overlapping_region_sets`**, with `column_field` non-empty: replace the DFS with a
union-find over the same edges, in the same order. These are the pairs `(i, j)` from
`combinations(range(n), 2)` that overlap, aren't excluded, and aren't guard-blocked. Each root
keeps the union of its members' `vals`. Before joining the roots of i and j, skip the edge if the
union of their two value sets has more than one element. Record `(regions[i].id, regions[j].id)`
in `column_blocked` (a dict used as an ordered set, like `blocked`). The check is per
**component**, not per pair. A chain `A('1') – B('') – C('2')` gives {A, B} and {C}, because the
edges are processed in order: A–B joins, then B–C is skipped since {1} ∪ {2} has two elements.
Output order is unchanged: sets by smallest member index, members in index order. With
`column_field == ""`, the result must be identical to today's. Either keep the DFS for that
case, or show by test that union-find gives the same sets in the same order.

**In `merge_overlapping_text_regions`**, pass
`column_field = params.split_on_line_field if params.column_merge_guard else ""`, and pass
`column_blocked` through. The fixed-point loop needs nothing new: a merged region carries its
members' lines, so its `vals` are already the union.

**In `layout_scan`**, count the column-blocked pairs per scan into `n_column_blocked`. Keep them
separate from `n_guard_blocked`. A pair blocked by the interleave guard is not also counted as
column-blocked. `blocked_out` stays as it is.

The guard does nothing on hand, where every `column` is `''`. It also does nothing when
`split_on_line_field == ""`, so `LEGACY_15FP` output is unchanged.

## Constraints

- No thresholds and no tuning. If criterion 4 still fails, stop and report the cases.
- Don't change `extract_v2.py`, `line_split.py`, `word_split.py`, `join.py`, `word_breaks.py`,
  `types.py`, `presets.py`, or the extract, wordfix and types code paths.
- Layers are copies. Create each new directory with `cp -R` from the source named here, and
  never write into an existing layer:
  - `data/text_layer_v2_dev/15fq-c/{hand_tar,hand_local,print}/` ← `15fq-b/…`, then
    `--stage layout` on all three with the preset defaults;
  - `data/text_layer_v2_dev/15fq2-noguard/print/` ← `15fq-b/print/`, then `--stage layout` with
    `--layout-params '{"column_merge_guard": false}'`;
  - `data/text_layer_v2_dev/15g-b/` (see *15g re-run*).
- **Protected:** `15d-a/`, `15fr-a/`, `15fp-a/`, `15fq-a/`, `15fq-b/`, `15fq-legacy/`, `15g-a/`,
  and all of `/Users/Shared/claude_projects/data_scopes/shared-data/`. Hash them before and after
  (15fq's method: sha256 of the SG source archives, a `size=` line for every other file), into
  `data/text_layer_v2_dev/15fq2-protected-{before,after}.txt`, and compare after every run.
- Never `git commit`; never bump the version; `chmod g+w` new files and directories;
  `.venv/bin/python`. `rm -rf`, `nohup` and `&` fail, so use new directories and run in the
  foreground.

## 15g re-run

After `15fq-c` passes criteria 1–5:
1. `cp -R data/text_layer_v2_dev/15fq-c data/text_layer_v2_dev/15g-b`, then on each sub-layer
   run `.venv/bin/python -m archival_structures.text_layer.build --out-dir
   data/text_layer_v2_dev/15g-b/<sub> --preset <preset> --stage types --inventories <invs>`
   (preset defaults, no `--types-params`; about 40 s in all). Output:
   `types/{segments,regions}/<inv>.tsv.gz`, `types/stats.json`, `types/params.json`.
2. Run `.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g/acceptance.py
   data/text_layer_v2_dev/15g-b`. It takes the layer as `argv[1]` and writes
   `step-15g/acceptance_15g-b.json`. That output is yours; `acceptance_15g-a.json` is protected.
   Its criterion 1 (coverage: 0 missing, overlap or mismatch) and criterion 2 (R4 rows 50, 52,
   55, 134, 19 and 9 reproduce) must still pass. Criterion 3's `main_text` character share for
   455/456 was 99.2% in `15g-a`; report the new value.
3. Compare `15g-b` with `15g-a`. Hand `types/{segments,regions}` must be equal (decompressed,
   same rows and order). For print, report the number of segment rows and region rows that
   differ, and the per-type segment counts before and after (15g-a's are in
   `acceptance_15g-a.json`). Read up to 10 changed print segments (seed 21) and say whether each
   type got better or worse. 15g keyed the print main-column reference by the line `column`
   field because of the mixed regions (its deviation 2), so a small change is expected. A large
   one is a finding.
4. Leave `types.py` as it is. UA-009 (the `margin_prefix` cut) is a separate decision; don't act
   on it.

## Tests

`tests/test_analysis_page_structure.py`:
- `test_column_guard_keeps_split_pieces_apart`: two overlapping regions with lines `column='1'`
  and `column='2'` give 2 regions, with one entry in `column_blocked`; with
  `LayoutParams(column_merge_guard=False)` they merge into 1.
- `test_column_guard_empty_value_joins`: `'1'` and `''` overlapping give 1 merged region.
- `test_column_guard_chain`: A(`'1'`) – B(`''`) – C(`'2'`), where A–B and B–C overlap and A–C
  does not, gives sets [A, B], [C].
- `test_column_guard_off_without_field`: with `split_on_line_field=""`, `'1'` and `'2'` merge.
- `test_column_guard_order_unchanged`: on the existing `test_transitive_merge_deterministic`
  fixture, with every column `''`, sets and order are identical with the guard on and off.
- `test_legacy_params_match_15fp_defaults` still passes.

`tests/test_text_layer_layout.py`: the fixture layer test still passes, `n_column_blocked` is in
the scans header, and `column_merge_guard` is in `layout/params.json`.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_structure.py
tests/test_text_layer_layout.py`, then the full suite. Take the expected count from
`--collect-only -q` before and after.

## Acceptance criteria

All on `15fq-c` unless stated otherwise. Compare tables decompressed, with pandas
(`dtype=str, keep_default_na=False`), in the same row order.
1. **Conservation and ids** (15fq's criterion 1): pass in 16/16.
2. **Hand unchanged:** for all 14 hand inventories, `15fq-c` equals `15fq-b` on every 15fq column
   of `layout/{scans,regions,lines}`, and `n_column_blocked` is 0 everywhere.
3. **Regression:** `15fq2-noguard/print` equals `15fq-b/print` on every 15fq column of the three
   tables.
4. **Criterion 6:** 0 non-empty print layout regions whose lines carry both `column` `1` and `2`
   (455 and 456). It was 36 and 109.
5. **Furniture and residuals** (15fq's criterion 7): 0 `merged` regions with a furniture member.
   Every residual overlapping pair is furniture, guard-blocked or column-blocked. Report each
   count per inventory.
6. **Locality** (expected, not measured): each print layout region of `15fq-c` that is not in
   `15fq-b` (by `(scan_id, layout_region_id)`) has all its `source_region_ids` within the sources
   of one of the 145 mixed regions. List and explain any exceptions. Report the number of new
   regions, the number of lost regions, and `n_column_blocked` per inventory.
7. **15g re-run:** hand `types/` equal; print differences reported (see *15g re-run*).
8. Full AS suite green; protected hashes unchanged.

## Error analysis

Use `random.Random(21)`. Read texts and boxes from the tables, not images.
- **30 of the 145 formerly mixed regions** (15 from 455 and 15 from 456, or all of them if
  fewer). For each, list the `15fq-c` regions that hold its lines. Are they column-pure? Is the
  line order right within each? Where did the `''` lines go (title, wide line, page furniture)?
  Classify each as *right*, *wrong* (a column cut mid-sentence, a column-`''` heading attached to
  the wrong column, broken order), or *ambiguous*. Give verbatim examples.
- Re-read the 2 print merge errors from 15fq's sample (`step-15fq/ea_verdicts.tsv`, print rows
  with verdict wrong). Is each one fixed, unchanged or worse?
- **New residual overlaps:** up to 20 column-blocked pairs. Is a region of one column now
  wrongly left out of its column's region?

Expected findings: criterion 6 now holds or fails (with counts); the error analysis precision;
the effect on 15g's print types.

## Do not touch

- `types.py`, `units.py`, `presets.py`; LotA, NLP, `shared-data/`, republic-project.

## When done

1. Write the `### Step 15fq2: column merge guard` section of the phase report, after § Step 15g.
   Give criteria 1–8 with numbers, then the error analysis, then findings `F07-15fq2.n` (status
   `draft`), with rows in the *Findings* table. Run `make reports` and fix any warnings.
2. Append the progress block (format in `plan/templates/STEP.md`) to `PROGRESS.md`.
3. Overwrite `STATUS.md`. The final layout layer is `15fq-c` and the types layer is `15g-b`.
   Next step: 15h on `15g-b`. Update brief 15h (rule 10): its *Layer* line (`15g-a` → `15g-b`) and
   its protected list (add `15fq-c`, `15g-a`, `15g-b`, `15fq2-noguard`). Also update brief 15hj's
   and 15i's protected lists if they name the 15g layer.
4. UA-008: move the entry from `docs/USER_ACTIONS.md` to `docs/USER_ACTIONS_resolved.md` with
   the date and the outcome (criterion 4's numbers).
