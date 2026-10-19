# Step 15fp: Page layout — page split, overlapping-region merge, main columns and extra

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** phase 06 (15d, 15fr layers).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you
> have to explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

Laypa's SG regions overlap a lot. An oddly shaped region often sits inside the region that holds
the main text column, and the two are siblings in the PageXML. REPUBLIC merges such regions
(`merge_overlapping_text_regions`) and organises each page into main text columns plus margin
elements (`extra`). The user's experience on the resolution books is that this removes most
PageXML oddities and does little harm. This step ports that into AS and adds a `layout` stage
to the v2 text layer. The stage splits each two-page scan into verso and recto, merges
overlapping regions, groups regions into columns and marks each column `main` or `extra`. 15g
(types), 15h (units, `doc_id` mapping) and 15i (pilot) build on its output. Units for hand become
layout regions, and the old → new `doc_id` mapping (R5) comes from this step's tables.

## Inputs

Measured at planning (2026-10-18, scratch scripts, not in the repo):

- **Region types.** In hand 3351, 3354, 3771 and 11182, and in print 456, every TextRegion has
  `custom="structure {type:Text;}"`. There is no layout label at all. Print 455 has Laypa types,
  but most typed regions are empty. Of the regions that carry at least one TextLine: `date` 6 of
  204, `header` 111 of 1,149, `page-number` 122 of 934. Laypa types are therefore **not** an
  input to this step. Copy `region_tag` through, and use it for nothing.
- **Overlap.** Counted over regions with ≥ 1 line, as bbox intersection ≥ 0.5 of the smaller box
  between siblings (no SG region is nested). The share of scans with at least one such pair:

  | Inventory | Scans with a pair | Note |
  |---|---|---|
  | hand 3351 | 78 / 219 | |
  | hand 3771 | 124 / 579 | |
  | print 456 | 320 / 730 | |
  | print 455 | 704 / 721 | `index`×`resolution` is 4,350 of the pairs; the index part of the volume is where the regions overlap most |
- **Original run under the current pins** (pagexml-tools 0.9.0). Republic's
  `merge_overlapping_text_regions` was run with every region with lines in one column. It
  raised nothing and kept every line:

  | Inventory | Scans | Regions before → after |
  |---|---|---|
  | hand 3351 | 199 | 4,146 → 4,017 |
  | hand 3771 | 118 | 2,509 → 2,456 |
  | print 456 | 118 | 461 → 396 |
  | print 455 | 120 | 1,604 → 645 |

  pagexml-tools `split_scan_pages_with_separation_point` also ran on the same scans without
  errors. **Don't use it as is, though.** Fed raw scans, it makes one column per empty noise
  region: 4,658 verso columns on 120 pages of 455.
- **Openings.** About 99% of SG scans are landscape two-page spreads, plus two portrait covers
  per volume. AS's `analysis.opening_detection.classify_inventory_structure` labels *all* SG
  scans `odd_shaped`/`mixed`, because the single-page group holds 2 of 219 scans, below its
  `min_share=0.15`. Don't use it here; the rule below is enough.
- **Gutter.** On landscape scans, AS's `analysis.page.get_separation_point` (the zero-height x
  nearest the centre, ±200 px) and the centre of the widest zero-coverage gap in 35–65% of the
  width disagree by more than 50 px on:
  - 127 of 199 scans in 3351;
  - 243 of 298 in 11182;
  - 25 of 297 in 456.

  The current `page_side` (line centre vs `page_w/2`) is therefore suspect in hand. Neither
  method was validated; this step validates one.

Data. All of it is read only and hashed before and after (see *Constraints*):

| Layer | Path under `data/text_layer_v2_dev/` |
|---|---|
| hand tar (3349–3354) | `15d-a/hand_tar/NL-HaNA_1.01.02_text_layer_v2/` |
| hand local (3771–3773, 3821–3823, 11182, 11281) | `15d-a/hand_local/NL-HaNA_1.01.02_text_layer_v2/` |
| print, split, factor 1.19 (455, 456) | `15fr-a/print/NL-HaNA_1.10.94_text_layer_v2/` |

Each layer has `scans/<inv>.scans.tsv.gz`, `regions_meta/<inv>.tsv.gz` and
`lines/<inv>.lines.tsv.gz`. Their headers are `SCAN_COLUMNS_V2`, `REGION_COLUMNS_V2` and
`LINE_COLUMNS_V2`, in `archival_structures/text_layer/extract_v2.py` L24–33. Boxes are
`x, y, w, h` in `Page@imageWidth/Height` space, and `baseline` is `"x,y x,y …"`. The `region_id`
of a region is `legacy_doc_id(scan_id, box)` = `f"{scan_id}-text_region-{x}-{y}-{w}-{h}"`
(`extract_v2.py`, `def legacy_doc_id`). In the print layer, split halves have new `line_id`s and
a `split_from`.

Code to port (republic-project, read only; MIT, same author; cite it in the module docstring):

- `republic/helper/pagexml_helper.py` (from `def merge_overlapping_sets` to the end of
  `def merge_overlapping_text_regions`): `merge_overlapping_sets` L151,
  `get_overlapping_text_regions` L178 and `merge_overlapping_text_regions` L222.
- `republic/parser/pagexml/republic_page_parser.py` `def split_column_regions` (L33). Port only
  its column-assignment criterion: a region joins the first column where
  `min(ov/tr_w, ov/col_w) > 0.5` and `max(ov/tr_w, ov/col_w) > 0.75`, with `ov` the horizontal
  pixel overlap. Otherwise it starts a new column.
- `republic/parser/pagexml/page_date_parser.py` `def merge_extra_regions_with_columns` (L258).
  This one is **not** ported: the column criterion above already covers it. Say so in the
  docstring.

pagexml-tools 0.9.0 calls you may use:

```python
from pagexml.helper.pagexml_helper import regions_overlap
#   regions_overlap(region1, region2, threshold: float = 0.5, debug: int = 0) -> bool
#   True if, for either region i, v_overlap/(height_i+1) > threshold AND h_overlap/(width_i+1) > threshold
from pagexml.model.coords import parse_derived_coords   # (document_list) -> Coords (convex hull)
import pagexml.model.physical_document_model as pdm
#   pdm.get_horizontal_overlap(doc1, doc2) -> int; pdm.PageXMLTextRegion(doc_id=, metadata=, coords=, lines=)
#   pdm.PageXMLColumn(doc_id=, metadata=, coords=, text_regions=); pdm.PageXMLPage(doc_id=, metadata=, coords=, columns=, extra=)
#   pdm.Coords(points) ; Coords.coords_from_box_params(x, y, w, h); pdm.Baseline(points)
#   pdm.PageXMLTextLine(doc_id=, metadata=, coords=, baseline=, text=)
```

Known pagexml-tools traps; work around them and don't fix them here (UA-005):
- `PageXMLPage.regions`, `.get_lines()` and `.stats` ignore `page.text_regions`, and `.stats`
  raises KeyError when the page has `text_regions` but no columns. Put every region in
  `columns` or `extra`, never in `page.text_regions`.
- `set_parentage` skips `extra`. Call `set_as_parent` on `extra` yourself.
- `parsers.column_parser.merge_columns` writes `doc_id`, not `id`. Don't use it.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/page_structure.py` (new) | Generic, on pdm objects: `get_overlapping_region_sets`, `merge_regions`, `merge_overlapping_text_regions`, `fit_gutter`, `find_separation_x`, `split_scan_into_pages`, `group_regions_into_columns`, `assign_column_roles`, `LayoutParams` |
| `archival_structures/text_layer/layout.py` (new) | The rows ↔ pdm adapter and `layout_inventory(inv, layer_root, params) -> dict`, which writes the `layout/` tables |
| `archival_structures/text_layer/build.py` | `--stage layout` (v2 presets only), modelled on `stage_wordfix` (`def stage_wordfix`): reads `run.root/{scans,regions_meta,lines}`, writes `run.root/layout/`, and records in the manifest. Without `--pagexml-dir`, like wordfix. The existing stages are unchanged |
| `tests/test_analysis_page_structure.py`, `tests/test_text_layer_layout.py` (new) | See *Tests* |
| `docs/text_layer.md` | A section *Layout tables (step 15fp)*: the schemas below |
| `docs/shared_library/changelog.md` | `### Step 15fp: page layout` under *Unreleased*, added with Edit. It is additive; no sibling import changes |
| `reports/phase-07-sg-units-and-pilot.md` | `### Step 15fp: …` section, findings `F07-15fp.n` |

## Behaviour to implement

`LayoutParams` is a dataclass with these defaults. Record it in `layout/params.json`.
- `overlap_threshold=0.5`
- `col_min_ratio=0.5`, `col_max_ratio=0.75`
- `main_min_char_frac=0.25`
- `min_gap_px=20`
- `gutter_window_frac=0.15`
- `portrait_ratio=1.0`, meaning a scan is an opening iff `page_w > portrait_ratio * page_h`

Per scan, in this order:

1. **Rebuild** pdm lines from the line rows: box coords, baseline, `text`. Rebuild each region
   with its lines in `line_idx` order. Its coords are `parse_derived_coords(lines)`, the hull
   of its line boxes, **not** the `regions_meta` box. Phase 06 found that 455's region polygons
   don't bound their lines (F06-15d.3). The `regions_meta` box is used only for the id of an
   unchanged region. Regions with 0 lines, or only empty-text lines, get role `empty` and take
   no further part.
   - The planning overlap numbers above used region boxes. In the report, give the number of
     scans with an overlapping pair both ways, region boxes and line hulls, per inventory.
2. **Page split.**
   - A portrait scan is one page, `page="single"`.
   - Otherwise, `fit_gutter` runs once per inventory over all landscape scans. For each scan it
     takes the coverage of line x-extents (`x … x+w`), finds the zero-coverage gaps of at least
     `min_gap_px` inside `[0.35, 0.65]·page_w`, and keeps the centre of the widest. It returns
     the median of `centre/page_w` (`gutter_rel`) and the number of scans with a gap.
   - `find_separation_x` then picks, per scan, the zero-coverage gap whose centre is nearest
     `gutter_rel·page_w`, within `±gutter_window_frac·page_w`. This is `method="gap"`. If there
     is none, it takes the x of minimum coverage in that window (`method="min_coverage"`).
   - Lines go to verso or recto by their box centre. A region whose lines fall on both sides is
     split into two regions, one per side, with lines kept in order (`change="split_at_gutter"`).
   - Coordinates stay in scan space. Don't translate the recto: R1 needs scan coordinates.
3. **Merge**, per page. Use `get_overlapping_region_sets`:
   - `regions_overlap(·,·,overlap_threshold)` on every pair;
   - transitive closure (port of `merge_overlapping_sets`), made **deterministic**: sets are
     ordered by their first member in input order, and members keep input order. The original
     iterates Python sets, so its line order is arbitrary.

   `merge_regions` does what the original does, with these changes:
   - lines are sorted by (baseline mean y, box left);
   - coords come from `parse_derived_coords(lines)`;
   - metadata is a union with first-member-wins, plus `metadata["merged_from"] = [member ids]`;
   - types are the union.

   The id is `legacy_doc_id(scan_id, box)` of the merged box. A region not in any merge keeps
   its `region_id` **byte-identical**. A merged hull can newly overlap a third region, so repeat
   the merge until no pair overlaps (a fixed point), and record the number of passes.
4. **Columns**, per page (`group_regions_into_columns`): regions are sorted by left. Each one
   joins the first column (as it is so far) that meets the republic criterion above with
   `col_min_ratio` and `col_max_ratio`; otherwise it starts a new column. Columns are numbered
   0… by their left edge. Within a column, regions are ordered by top.
5. **Roles** (`assign_column_roles`): a column is `main` if its number of text characters is at
   least `main_min_char_frac` × that of the page's largest column; otherwise `extra`. Build a
   `PageXMLPage` with `columns` = main and `extra` = the regions of the extra columns, and set
   the parentage, including `extra`.

### Layout tables (gzip TSV, written with `io.write_tsv_gz`)

- `layout/scans/<inv>.tsv.gz`: `scan_id, page_mode` (`opening`/`single`), `separation_x`,
  `split_method` (`gap`/`min_coverage`/`none`), `n_lines_crossing`, `n_regions_in`,
  `n_regions_out`, `n_merges`, `n_gutter_splits`.
  - `n_lines_crossing` counts lines whose box contains `separation_x`.
- `layout/regions/<inv>.tsv.gz`, one row per layout region: `scan_id, page, layout_region_id,
  x, y, w, h, layout_column, layout_role, n_lines, source_region_ids` (space-separated, in input
  order), and `change` (`unchanged`/`merged`/`split_at_gutter`/`merged+split`).
  - Empty source regions get a row with `layout_role=empty`, `page` from their centre, and
    `change=unchanged`.
- `layout/lines/<inv>.tsv.gz`, one row per input line: `scan_id, line_id, region_id` (input),
  `page`, `layout_region_id`, `layout_line_idx`, `layout_column`, `layout_role`.
- `layout/stats.json` (per inventory: the counts above summed, `gutter_rel`, and
  `n_scans_with_gap`) and `layout/params.json`.

The input tables are never rewritten.

## Constraints

- Generic code goes in `analysis/page_structure.py`, with no text-layer imports. The
  `text_layer/layout.py` adapter imports it, never the other way round.
- Don't change `extract_v2.py`, `line_split.py`, `word_split.py`, `presets.py`, or the existing
  stages and their outputs. A layout layer is a copy: `cp -R` each input layer to
  `data/text_layer_v2_dev/15fp-a/{hand_tar,hand_local,print}/` and run `--stage layout` there.
- **Protected outputs:** `data/text_layer_v2_dev/15d-a/` and `15fr-a/`, and all of
  `shared-data/`. Hash them (sha256 + mtime) into `15fp-protected-before.txt`; check them into
  `-after.txt` after every run. `republic-project` is read only. Don't run it from AS code or
  tests.
- Don't port the date machinery: `DateRegionClassifier`, `DateNameMapper`, `reclassify_lines`,
  `split_merged_marginalia_and_para_lines`, `split_page_column_text_regions`. It needs the neural
  `line_class`, `date`/`attendance` region types and REPUBLIC inventory metadata, and SG has none
  of them. Date headers are 15g's job.
- Every expectation about the data in this brief says whether it was measured; the planning
  numbers above were measured on the inventories and scan counts given.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs. Use
  `.venv/bin/python`.

## Tests

- `test_analysis_page_structure.py`, on synthetic regions built in the test:
  - `test_nested_region_merges` (an odd shape inside a column → one region; lines sorted by y;
    `merged_from`);
  - `test_side_by_side_not_merged` (a margin note beside the column stays separate);
  - `test_transitive_merge_deterministic` (A∩B, B∩C → one set; the same output over 5
    shuffles of the input);
  - `test_unmerged_id_unchanged`;
  - `test_gutter_split_region` (one region straddling the gutter → two, lines conserved);
  - `test_portrait_single`;
  - `test_wide_header_not_chaining_columns` (a header over two columns doesn't join them);
  - `test_roles` (a narrow 2-line margin column → `extra`);
  - `test_page_parentage` (the `extra` regions have the page as parent).
- `test_text_layer_layout.py`:
  - run `layout_inventory` on a 3-scan layer written in `tmp_path` by `extract_inventory_v2`
    from `tests/fixtures/text_layer/sg/9999` (preset `PRESETS["sg_1.01.02_v2"]`);
  - check that every input line appears exactly once in `layout/lines`, that the inputs'
    bytes are unchanged, and that the headers match the schemas;
  - `--stage layout` through `build.main([...])`.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_structure.py tests/test_text_layer_layout.py`, then the full suite.
  Phase 06 ended at 1010 passed; take the expected count from `--collect-only -q`.

## Acceptance criteria

All 16 inventories (3349–3354, 3771–3773, 3821–3823, 11182, 11281, 455, 456):

1. **Conservation:** `layout/lines` row count = the input line count, and every
   `(scan_id, line_id)` appears exactly once. Every `layout_region_id` is unique per inventory.
2. **Stability:** every `change=unchanged` row has `layout_region_id == source_region_ids`,
   byte-identical.
3. **Merges:** report regions in → out, `n_merges` and `n_gutter_splits` per inventory. No
   number is required (the planning numbers above are for comparison), but the 455 reduction
   must be explained in the error analysis. After the layout, no two `layout_region`s on a page
   satisfy `regions_overlap(·,·,0.5)` (0 pairs).
4. **Page split:** report `split_method` counts, `n_lines_crossing` (sum and the share of scans
   with > 0), and `gutter_rel` per inventory. Also report the number of lines whose `page`
   differs from the old `page_side` (left ↔ verso), per inventory.
5. **Roles:** the share of text characters in `main` regions, and the number of `extra` regions
   per page, per inventory.
6. Full AS suite green; protected hashes unchanged.

## Error analysis

Use a seeded random sample (`random.Random(15)`) over all 16 inventories. Take all cases if
there are fewer. Read the line texts and boxes from the tables. Thumbnails exist only for some
inventories; don't fetch images.

- **60 merge sets:** 30 hand, 30 print, 10 of them from 455's index pages. Each set is right or
  wrong. It is wrong if the merged region joins text that a reader would read as separate
  (a margin note interleaved with main text, or two columns), or if the y-sorted line order
  breaks a sentence. If it is ambiguous, record it as such and don't count it. **Bar:**
  precision ≥ 0.90 on the decided sets; below that, it is a negative result. Report it and
  don't tune the threshold until it passes.
- **30 scans where `page` ≠ old `page_side`**, and 30 with `split_method=min_coverage`. Was the
  split right? The test: each page keeps a column of running text, no sentence crosses pages,
  and margin notes stay on their own page. Use the categories `gutter right`, `cut off a margin
  column`, `cut through text`, `cover/single wrongly split`.
- **30 pages for roles:** for every `extra` region, is it a margin note, a header/date heading,
  a page number, a catch word, noise, or **main text wrongly put in `extra`**? Count the last
  category separately: it costs LotA text.

Write the counts, verbatim examples and conclusions in the report section, not in
`PROGRESS.md`.

## Do not touch

- `extract_v2.py`, `line_split.py`, `word_split.py`, `presets.py`, `join.py`, `word_breaks.py`;
  15g–15i's future modules (`types.py`, `units.py`).
- Siblings, `shared-data/`, `republic-project`.

## When done

1. Create `reports/phase-07-sg-units-and-pilot.md`, if it is missing, from
   `plan/templates/REPORT.md`; the planning session has already created it with a *Planning*
   section. Add your `### Step 15fp: …` section and the `F07-15fp.n` rows (status `draft`).
   Run `make reports` and fix any warnings.
2. Append the progress block (format in `plan/templates/STEP.md`, *When done*) to
   `plan/phase-07-sg-units-and-pilot/PROGRESS.md`.
3. Overwrite `STATUS.md`: 15fp done, the layout layer paths under *Facts later steps need*, and
   the next step (planning of 15g–15i, prompt 1).
