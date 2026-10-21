# Step 15fq: Layout fixes — hand gutter, print column split, furniture and interleave guards

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15fp (done).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you
> have to explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

Step 15fp added the v2 `layout` stage (page split, overlapping-region merge, columns, roles). Its
validation was negative (report § Step 15fp, F07-15fp.2–.5). The user chose to fix it before
15g (UA-006 (a), 2026-10-20) and added an interleave guard (planning, 2026-10-20). This step
makes four changes, keeps 15fp's behaviour reachable through parameters, and validates the result
on new seeded samples. 15g (types), 15h (units, `doc_id` mapping) and 15i (pilot) build on its
output. If the merges miss the bar again, the fallback is fixed in advance: see *Fallback*.

## Inputs

Code (checked 2026-10-20, AS HEAD `73e1517`):
- `archival_structures/analysis/page_structure.py` (358 lines). `LayoutParams` L51–62 (fields
  `overlap_threshold=0.5, col_min_ratio=0.5, col_max_ratio=0.75, main_min_char_frac=0.25,
  min_gap_px=20, gutter_window_frac=0.15, portrait_ratio=1.0`, frozen, `to_dict()`).
  `get_overlapping_region_sets` L106, `merge_regions` L136, `merge_overlapping_text_regions(regions,
  params=LayoutParams(), *, union_keys=(), id_fn=region_box_id) -> (regions, passes)` L169,
  `_coverage(boxes, page_w)` L189, `_zero_gaps` L200, `fit_gutter(scans, params) -> (gutter_rel,
  n_with_gap)` L211 (its 0.35/0.65 bounds are hardcoded), `find_separation_x(boxes, page_w,
  gutter_rel, params) -> (x, "gap"|"min_coverage")` L229, `split_scan_into_pages` L258,
  `group_regions_into_columns` L296, `assign_column_roles` L331. Read these functions before
  changing them (that is not exploring).
- `archival_structures/text_layer/layout.py` (236 lines): `LAYOUT_SCAN_COLUMNS`,
  `LAYOUT_REGION_COLUMNS`, `LAYOUT_LINE_COLUMNS` L29–37, `_line(row)` L53 (row → pdm line; it does
  **not** copy `column` yet), `_change(region)` L74, `layout_scan` L81–165, `layout_inventory(inv,
  layer_root, params=LayoutParams()) -> dict` L168–236 (it calls `fit_gutter`, then `layout_scan`).
- `archival_structures/text_layer/build.py`: `stage_layout` L344–380 (`--layout-params` JSON at
  L348; writes `layout/params.json`, merges `layout/stats.json`).
- `archival_structures/text_layer/presets.py`: `LayerPreset` L8–24, `SG_HAND_V2`/`SG_PRINT_V2`
  L53–80, `PRESETS` L82.
- `archival_structures/analysis/page_numbering.py` is **not** used here (15g uses it).

Data (read only; copy as 15fp did):

| Layer | Source | Inventories |
|---|---|---|
| hand tar | `data/text_layer_v2_dev/15d-a/hand_tar/NL-HaNA_1.01.02_text_layer_v2/` | 3349–3354 |
| hand local | `data/text_layer_v2_dev/15d-a/hand_local/NL-HaNA_1.01.02_text_layer_v2/` | 3771–3773, 3821–3823, 11182, 11281 |
| print (split, factor 1.19) | `data/text_layer_v2_dev/15fr-a/print/NL-HaNA_1.10.94_text_layer_v2/` | 455, 456 |
| 15fp layout (regression baseline) | `data/text_layer_v2_dev/15fp-a/{hand_tar,hand_local,print}/…/layout/` | all 16 |
| 15fp verdicts (dev set; don't re-sample these scans) | `reports/data/phase-07-sg-units-and-pilot/step-15fp/ea_verdicts.tsv` (`sample, case, scan, verdict, note`; scans like `3350_0625`) | 150 rows |

The line `column` field (`lines/<inv>.lines.tsv.gz`): print 455 has `'1'` 85,918, `'2'` 86,049,
`''` 1,548 (lines of tags that don't take part in the split); hand is `''` everywhere (measured).
pagexml-tools stays the **0.9.0 wheel** in `.venv`, even if 0.9.1 comes out meanwhile, so the
regression compares like with like (step 15i pins 0.9.1). Don't install the user's local copy:
at planning its `PageXMLPage.stats` raised `TypeError` on every page, and `line_split.py:1059`
calls it (F07-P.9, UA-007).

## Deliverables

| File | Change |
|---|---|
| `analysis/page_structure.py` | New `LayoutParams` fields; `inventory_line_width_rel`, `choose_gutter_rule`, `find_main_edge_x`, `split_regions_on_line_field`, `furniture_region_ids`; `find_separation_x(..., rule="gap", med_w_rel=None)`; merge with `exclude` and the interleave guard |
| `text_layer/layout.py` | `_line` copies `column` into metadata; the new per-page order; new table columns; `change` vocabulary |
| `text_layer/presets.py` | `LayerPreset.layout_params: Optional[Mapping] = None`; SG presets get the furniture patterns |
| `text_layer/build.py` | `stage_layout`: `LayoutParams(**{**(preset.layout_params or {}), **cli_json})` |
| `tests/test_analysis_page_structure.py`, `tests/test_text_layer_layout.py` | See *Tests* |
| `docs/text_layer.md` | Update *Layout tables (step 15fp)* → *(steps 15fp, 15fq)* |
| `docs/shared_library/changelog.md` | Add `## Unreleased` above `## v0.10.0 (2026-10-19)` (it doesn't exist), then `### Step 15fq: layout fixes`, with Edit |
| `reports/data/phase-07-sg-units-and-pilot/step-15fq/` | `acceptance.py`, `ea.py`, `ea_verdicts.tsv` (copy and adapt 15fp's scripts, which hardcode `ROOT`) |
| `reports/phase-07-sg-units-and-pilot.md` | Fill in the empty `### Step 15fq: …` section; findings `F07-15fq.n` |

## Behaviour to implement

New `LayoutParams` fields. Defaults are the new behaviour; `LEGACY_15FP` reproduces 15fp:

```python
gutter_rule: str = "auto"          # "gap" (15fp) | "main_edge" | "auto"
gap_share_min: float = 0.95        # auto: "gap" if n_scans_with_gap / n_openings_with_lines >= this
long_line_frac: float = 0.6        # a line is long if w/page_w >= long_line_frac * med_w_rel
min_long_lines: int = 5            # per side, else fall back to the gap rule
edge_quantile: float = 0.95
split_on_line_field: str = "column"   # "" = off
furniture_patterns: tuple = ()     # regexes (re.IGNORECASE, fullmatch on the stripped joined text)
furniture_max_lines: int = 2
furniture_max_chars: int = 30
catchword_band: float = 0.04       # 0 = off
interleave_guard: bool = True
interleave_width_frac: float = 0.5
merge: bool = True                 # False = no merging at all (the fallback)
LEGACY_15FP = LayoutParams(gutter_rule="gap", split_on_line_field="", furniture_patterns=(),
                           catchword_band=0.0, interleave_guard=False)
```

`from_dict` must turn JSON lists into tuples. `params.json` records every field.

**1. Gutter (hand).** In 3349–3353 the recto's margin column abuts the verso's text, so the gap
rule cuts the recto's margin notes off to the verso (F07-15fp.4). The new rule looks at the
verso's main text instead:
- `inventory_line_width_rel(scans) -> float`: the median of `w/page_w` over all text lines of the
  inventory's landscape scans (measured: 3349 0.277, 3350 0.242, 11182 0.316).
- `choose_gutter_rule(n_scans_with_gap, n_openings_with_lines, params) -> "gap"|"main_edge"`.
  Under `auto`, measured on the 15fp tables: **gap** for 3354 (0.984), 3771–3773, 3821–3823, 455,
  456 (1.0); **main_edge** for 3349 (0.352), 3350 (0.072), 3351 (0.874), 3352 (0.645), 3353
  (0.695), 11182 (0.742), 11281 (0.688).
- `find_main_edge_x(lines_xw, page_w, gutter_rel, med_w_rel, params) -> Optional[float]`: let
  `c0 = gutter_rel·page_w`, long = `w/page_w >= long_line_frac·med_w_rel`, centre `x + w/2`. Verso
  long = long with centre < c0; recto long = long with centre ≥ c0. If either has fewer than
  `min_long_lines`, return None. Else `se = min(np.quantile(verso right edges, edge_quantile),
  np.quantile(recto left edges, 1 - edge_quantile))` (numpy default `method="linear"`). If
  `|se − c0| > gutter_window_frac·page_w`, return None. Else return `se`.
- `find_separation_x(boxes, page_w, gutter_rel, params, *, rule="gap", lines_xw=None,
  med_w_rel=None)`: for `rule="main_edge"`, try `find_main_edge_x`; if it returns a value, return
  `(se, "main_edge")`, else fall back to the existing gap logic. For `rule="gap"` the behaviour is
  unchanged.
- Lines still go to a page by box centre; regions straddling the separation are split as before.

Planning measurement (scratch script, 15fp tables, same definitions): the lines whose page
changes against 15fp: 3349 343 (105 scans), 3350 1,615 (215), 3351 4, 3352 14, 3353 52,
11182 236, 11281 268; 0 in the `gap` inventories. On 15fp's 60 read page-split scans, all 14
"cut off a margin column" scans move exactly their margin notes ("1. Maart", "Vittinghoff",
"Hamilton" in 3349_0353), and 3350_0856 ("gutter missed by a whole page") gets its 40 verso lines
back. Five scans judged "gutter right" in 11182/11281 change by 1–3 lines ("R: v. Brab: 6"). That
judgement is doubtful: in 11182_0524 the verso text runs 0.19–0.50 and the recto text starts at
0.595, so by symmetry 0.50–0.595 is the recto's margin column. Re-read them (see *Error analysis*).

**2. Print column split.** Print regions are page-level and interleave the two columns
(F07-15fp.3). `split_regions_on_line_field(regions, field, *, id_fn) -> (regions, n_splits)`:
group each region's lines by `line.metadata.get(field, "")`. With one group the region is
returned unchanged, same object, same id. With more, there is one region per group: groups are
ordered by first occurrence, lines keep their order, coords are the hull, the id is `id_fn`, and
`metadata["column_split_from"] = original id`. Hand lines all have `''`, so this is a no-op there.

**3. Furniture out of merges.** `furniture_region_ids(regions, page_text_bottom, page_h, params)
-> set[id]`: a region is furniture if it has ≤ `furniture_max_lines` lines and ≤
`furniture_max_chars` characters (joined with spaces, stripped), and either its text fullmatches a
pattern, or (catch word) it has one line with one token of ≤ 15 characters whose box top ≥
`page_text_bottom − catchword_band·page_h`. Furniture regions take no part in overlap sets: they
are never merged, and they stay their own layout region. SG presets set:

```python
MONTHS = r"(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|sep|sept|oct|okt|nov|dec)"
SG_FURNITURE_PATTERNS = (
    r"\W*(fol\.?|pag\.?|p\.)?\s*\d{1,4}\s*\W*",                              # page number "( 93", "383."
    r"\W*(den\s+)?\d{1,2}\s*\.?\s*" + MONTHS + r"\w*\.?\W*(\d{4}\W*)?",      # "Den 26. Maart.", "1. Meij"
    r"\W*1[67]\d\d\W*",                                                      # running year "1717."
)
```

Planning measurement (pattern part only): it flags the furniture member of all 14 hand merge sets
15fp read as "furniture absorbed", and "1. Meij" in 3349_0662 (an interleaved margin note). Merges
with a flagged member: 3349 30 of 101, 3350 28/107, 11281 420/550, 455 654/1,784, 456 216/385.

**4. Interleave guard.** In `get_overlapping_region_sets`, a pair that overlaps is not linked
when one member S has every line shorter than `interleave_width_frac` × the median line width of
the other member L, and S's line y-range lies strictly inside L's (S's top line top > L's top line
top, and S's bottom line bottom < L's bottom line bottom). A nested region with full-width lines
still merges. Count the blocked pairs.

**Per page, in this order:** page split → `split_regions_on_line_field` → furniture ids → merge
(skipped if `merge=False`; else with exclusion and guard, repeated to a fixed point) → columns →
roles. **`change`** becomes `unchanged` or the `+`-join, in this order, of `merged`,
`split_at_gutter`, `split_at_column`. 15fp's `merged+split` is now `merged+split_at_gutter`.

**Table columns**, appended at the end (the 15fp columns keep their order):
- scans: `gutter_rule`, `n_column_splits`, `n_furniture`, `n_guard_blocked`. `split_method`
  gains `main_edge`.
- regions: `furniture` (`1`/`0`), `column_value` (the shared line-field value, or `''`).
- `layout/stats.json`: per inventory, add `gutter_rule`, `med_w_rel` and the new counts.

## Constraints

- Generic code stays free of text-layer imports, and the SG patterns live in `presets.py`, not
  in `analysis/`.
- Don't change `extract_v2.py`, `line_split.py`, `word_split.py`, `join.py`, `word_breaks.py`,
  or the extract/wordfix stages.
- Layers are copies: `cp -R` the three inputs into `data/text_layer_v2_dev/15fq-a/{hand_tar,
  hand_local,print}/` (main run) and `15fq-legacy/` (regression run with `--layout-params` =
  `LEGACY_15FP` as JSON). If the fallback runs, use `15fq-b/`.
- **Protected:** `15d-a/`, `15fr-a/`, `15fp-a/` and all of `shared-data/`. Hash them as 15fp did
  (sha256 of the SG source archives, plus size+mtime of every file) into
  `15fq-protected-{before,after}.txt` and compare after every run.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`.
  `rm -rf` is denied: use new directories.

## Tests

- `test_analysis_page_structure.py` (synthetic): `test_main_edge_moves_recto_margin_notes`
  (verso lines end at x=1000; recto notes at 1005–1200; recto text from 1300; gap 1210–1290: the
  gap rule puts the notes on the verso, main_edge on the recto); `test_main_edge_fallback_few_long_lines`;
  `test_main_edge_fallback_window`; `test_choose_gutter_rule`; `test_split_on_line_field` (lines
  `1`,`2`,`''` → 3 regions in order; all `''` → same object); `test_furniture_not_merged`
  (a "( 93" region overlapping a column stays separate); `test_catchword_band`;
  `test_interleave_guard_blocks_margin_item`; `test_nested_full_width_still_merges`;
  `test_merge_off`; `test_legacy_params_match_15fp_defaults` (`LEGACY_15FP` gives today's
  outputs on the existing synthetic fixtures).
- `test_text_layer_layout.py`: the fixture layer test still passes; preset `layout_params` reach
  `layout/params.json`, and `--layout-params` overrides them.
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_structure.py
  tests/test_text_layer_layout.py`, then the full suite (1023 collected at planning; take the new
  count from `--collect-only -q`).

## Acceptance criteria

All 16 inventories.
1. **Conservation and ids** (as 15fp): `layout/lines` has every input `(scan_id, line_id)` exactly
   once; `layout_region_id` is unique per inventory; every `unchanged` row has
   `layout_region_id == source_region_ids`, byte-identical.
2. **Regression:** `15fq-legacy` equals `15fp-a` on the 15fp columns of all three tables (pandas,
   decompressed, same row order), after mapping `merged+split` → `merged+split_at_gutter`.
3. **Gap inventories** (3354, 3771–3773, 3821–3823, 455, 456): `separation_x`, `split_method` and
   every line's `page` equal 15fp's.
4. **main_edge inventories:** the lines whose `page` differs from 15fp's, per inventory. The
   planning numbers above should reproduce exactly. If not, explain the difference before going on.
5. **Dev check:** on 15fp's 60 read page-split scans, list per verdict how many scans change and
   which lines move. Expected: 14/14 cut-offs and 3350_0856 change, plus 5 of the 44 "right".
6. **Print:** 0 non-empty layout regions whose lines carry both `column` `1` and `2`.
7. **Furniture:** 0 `merged` layout regions with a furniture member. Residual overlapping pairs
   (`regions_overlap(·,·,0.5)` on line hulls): report them; each must involve a furniture region
   or a guard-blocked pair.
8. Full AS suite green; protected hashes unchanged.

## Error analysis

Use `random.Random(16)`, and exclude every scan in 15fp's `ea_verdicts.tsv`. Read texts and boxes
from the tables; don't fetch images. If a stratum has fewer cases, take all of them.

- **60 merge sets**: 30 hand, 30 print. *Right* = reading order kept and nothing joined that a
  reader keeps apart. Count a catch word absorbed at the bottom of a column as right, but list
  it. *Wrong* = margin item interleaved, two columns joined, a sentence broken by the y-sort, or
  a page number or date absorbed. *Ambiguous* is not counted. **Bar: precision ≥ 0.90 per side.**
- **Page split:** 30 scans where `page` differs from 15fp (20 from 3349–3353, 10 from
  11182/11281), and 20 random main_edge openings with no change. Use 15fp's categories. Rule for
  margin items near the gutter: if the verso has its own left margin column, short lines between
  the verso's text edge and the recto's text belong to the recto. Re-read the five 11182/11281
  dev scans and say whether 15fp's verdict stands.
- **Furniture:** 30 regions flagged furniture: true furniture vs text wrongly excluded.
- **Print column split:** 20 print layout regions with `change` containing `split_at_column`:
  is each column-pure, in reading order? Classify the `''` parts.
- **Guard:** every guard-blocked pair, up to 30: was blocking right?
- Re-run 15fp's roles count (main char share, extra/page). Don't re-read roles.

## Fallback (decided by the user, 2026-10-20)

If merge precision is below 0.90 for a side, re-run that side with `merge=False` (through the
preset's `layout_params`) into `15fq-b/`, re-check criteria 1, 6 and 8, and record which layer is
final in `STATUS.md` under *Facts later steps need*. Don't tune thresholds. A miss is a negative
result, recorded as a finding.

## Do not touch

- `types.py`, `units.py` (15g/15h); siblings; `shared-data/`; republic-project.

## When done

1. Fill in the `### Step 15fq: …` section of `reports/phase-07-sg-units-and-pilot.md` (it exists
   as an empty skeleton): what changed, criteria 1–8 with numbers, the error analysis (counts,
   verbatim examples, conclusion), findings `F07-15fq.n` with status `draft`. Expected findings:
   the merge precision per side, the page-split result, furniture precision, the guard's effect.
   Run `make reports` and fix warnings.
2. Append the progress block (format in `plan/templates/STEP.md`) to `PROGRESS.md`.
3. Overwrite `STATUS.md`: the final layout layer path (`15fq-a` or `15fq-b`), and the
   `LayoutParams` used, under *Facts later steps need*. Next step: 15g. If the final layer or a
   column name differs from this brief, update briefs 15g, 15h, 15hj and 15i (rule 10).
