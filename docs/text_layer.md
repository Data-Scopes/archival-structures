# Text layer: lines, regions and word breaks from PageXML

{mod}`archival_structures.text_layer` builds a versioned, per-inventory text layer from
PageXML and is the shared source of text for the sibling projects (LotA, NLP). It is built
once by `as-build-text-layer` and read by everyone else.

## What the layer holds

Under `<out-dir>/<preset name>/` (for the default preset `voc_text_layer_v1/`), one file per
inventory in each of:

| Directory | Columns | Content |
|---|---|---|
| `lines/<inv>.lines.tsv.gz` | `scan_id region_id line_idx line_id line_text` | every text line, unjoined |
| `regions_meta/<inv>.tsv.gz` | `scan_id region_id region_box region_tag n_lines` | region geometry and Laypa tag (an intermediate) |
| `regions/<inv>.tsv.gz` | `scan_id region_id region_box region_tag region_text` | region text with line-final word breaks joined |

Further directories hold statistics (`extract_stats/`, `join_stats/`, `wbc/`), the optional
`join_events/` (guard firings, with `--write-events`), `status.tsv`, `progress.log` and
`manifest.json`.

Row order is PageXML **document order** (scans sorted by file name, regions depth-first,
lines in XML order). It is not a reading order; PageXML reading order is ignored. Files are
tab-separated UTF-8 with a header and `csv.QUOTE_NONE`; tabs, CR and LF inside text are replaced
by one space each (counts are in the stats JSONs). Read them with
{func}`archival_structures.text_layer.io.read_tsv_gz` (dict rows, no NaN coercion). If you use
pandas, pass `quoting=3, keep_default_na=False` so that text such as `"NA"` or a stray quote
survives.

## The three stages

1. **extract**: parse PageXML, write `lines/`, `regions_meta/`, `extract_stats/`.
2. **wbc** (word-break characters): count, per inventory, which line-final characters behave
   like word-break marks, pool the counts, and decide the set per inventory and a corpus default.
   Writes `wbc/`. The pooled sanity anchors (`¬` in, `:` and `.` out) must hold, or `join`
   refuses to run.
3. **join**: join each region's lines, merging a word split by a line-final break character,
   with guards against joining numbers and punctuation. Writes `regions/`, `join_stats/`.

A character is a word-break mark for an inventory if all three thresholds below hold, with
enough evidence (`min_end_attached`); inventories with too few lines take the corpus default.

| Threshold | Value | Reason (short) |
|---|---|---|
| `min_end_attached` | 30 | below this the two fractions are too noisy to decide |
| `min_join_vocab_frac` | 0.25 | genuine marks join into an attested word 0.5 of the time, abbreviation marks at most 0.04 |
| `min_end_rel_frac` | 5.0 | a wrap mark is strongly enriched at line end relative to its overall frequency |
| `min_inventory_lines` | 2000 | smaller inventories take the corpus default set |
| `corpus_default_min_share` | 0.5 | in the default if derived in at least half the inventories with evidence |

The full rule with reasons is written to `wbc/corpus_default.json`.

## Presets

A {class}`~archival_structures.text_layer.presets.LayerPreset` fixes the layer directory name,
the tag for regions without a Laypa type, the scan-id prefix and the throughput fit used by
`--estimate`. The v1 preset is `voc_v1` (`--preset voc_v1`, the default): GLOBALISE VOC
PageXML v2.1, tag `unclassified`, prefix `NL-HaNA_1.04.02`. A new archive gets a new preset.

## Building a layer

`as-build-text-layer` is a console script that installing archival-structures creates. It is
declared under `[project.scripts]` in `pyproject.toml` and lands in the environment's `bin/`
directory, next to `python`. Without an activated environment, call it by its full path, e.g.
`.venv/bin/as-build-text-layer`. The same driver also runs as
`python -m archival_structures.text_layer.build`.

The text layer is not in archival-structures 0.8.0 (the current PyPI release, which also pins
an older pagexml-tools and fuzzy-search). Until the next release, install from the checkout
into the environment you build with. The install pulls in `pagexml-tools>=0.9.0` and
`fuzzy-search>=3.0.0`:

```
python -m venv .venv && .venv/bin/python -m pip install -e /path/to/archival-structures
.venv/bin/as-build-text-layer --pagexml-dir D --out-dir O --estimate --workers 8   # predicted time, parses nothing
.venv/bin/as-build-text-layer --pagexml-dir D --out-dir O --stage all --workers 8
```

The archival-structures checkout's own `.venv` already has everything installed, so it can be
used directly.

`D` holds `<inv>.zip` files or `<inv>/` directories. `--stage` runs `extract`, `wbc`, `join`
or `all`. Each stage is resumable per inventory (rerun the same command; `--force` redoes
it). `--inventories a,b`, `--inventories-file F` and `--limit N` select inventories;
`--allow-partial`, `--allow-subset-wbc`, `--ignore-sanity`, `--wbc-stats-only` and `--write-events` are described in
`--help`. The driver's module docstring has the full handoff text for a run on another machine,
including expected runtime and output size. Run AS's `tests/test_text_layer_*.py` first.

**Never build into an existing shared layer with a subset of its inventories.** Since step 15hj,
`--stage wbc` exits with a message when the run's inventories are a strict subset of those with a
`lines/` table in the layer, unless `--allow-subset-wbc` is given. Rerunning a stage moves its old
`manifest.json` entry to `history.<stage>` (a list); a fresh build has no `history`. The `wbc`
stage (also part of `--stage all`) re-derives `wbc/corpus_default.json` and
`wbc/wbc_per_inventory.tsv` from the inventories of *this run* only, and overwrites them.
`join` then rewrites every region file whose word-break set changed. Build a sample into a
fresh `--out-dir`, and copy the layer's `wbc/` decision files in (as in *Checking a rebuild
against v1*) if the sample must use them.

## v2 (SG presets)

The presets `sg_1.01.02_v2` (States General hand volumes) and `sg_1.10.94_v2` (print volumes)
build a **v2 layer** (`format_version` 2): one row per scan, per region and per text line, with
geometry. For v2, `--stage all` runs extract (+ `wordfix` when the preset has it on) -> `layout` ->
`types` -> `units` -> `wbc` -> `join`; `wbc` and `join` read the unit tables (step 15hj), the
`layout`, `types` and `units` stages are also runnable alone (no `--pagexml-dir`). Rows are in document order (scans by file name, regions
depth-first, lines in XML order); reading-order indices are columns, not the row order.

```
as-build-text-layer --pagexml-dir <dir with NL-HaNA_1.01.02_<inv>-pagexml.tar.gz | .zip | <inv>/> \
    --out-dir <out> --preset sg_1.01.02_v2 --workers 8
```

Output under `<out>/NL-HaNA_1.01.02_text_layer_v2/` (or `..._1.10.94_...`): `scans/<inv>.scans.tsv.gz`,
`regions_meta/<inv>.tsv.gz`, `lines/<inv>.lines.tsv.gz`, `extract_stats/<inv>.json`,
`manifest.json`, `status.tsv`.

**scans** (`SCAN_COLUMNS_V2`): `scan_id archive_id inv_num inv_dir scan_num scan_variant
scan_id_ok member status page_w page_h n_regions n_regions_text n_lines n_lines_text n_words`.
`status` is the first that applies of `parse_error`, `partial_shot_dropped` (a `_deelopnameN`
scan: counted, no region or line rows), `no_regions`, `no_words` (regions, but no line with
text), `ok`. Counts are over recursive regions and all lines.

**regions** (`REGION_COLUMNS_V2`): `scan_id page_side region_id region_xml_id
parent_region_xml_id region_tag region_ro_index x y w h n_lines n_lines_text`. `region_id` is the
legacy `doc_id` `<scan_id>-text_region-<x>-<y>-<w>-<h>`. Regions without lines are kept.
`region_tag` is the Laypa `structure` type, else `untagged`. `page_side` is `left` if the box
centre is left of half the page width.

**lines** (`LINE_COLUMNS_V2`): `scan_id page_side column region_id line_idx line_id x y w h
baseline conf text_raw text edit_flags split_from`. `line_idx` is 0-based within the region and
counts empty lines. `baseline` is `x,y x,y ...`; `conf` is `%.6g`. `text` equals `text_raw`
after extract; `column`, `edit_flags` and `split_from` are empty until a `scan_hook` (step 15e)
or a repair (15f) sets them through `line.metadata` (the print presets set them, see below).

**Legacy text.** `legacy_region_text(line_texts)` joins the non-empty lines with the old
word-break set `{-, „}`; for every region with text it reproduces the `text` of the legacy
paragraph TSVs (100% on 14 inventories, step 15d).

### Merged-line splitter (print presets, step 15e)

In printed two-column volumes (NL-HaNA 1.10.94 inventories 455/456) the HTR often merges a line
of the left column and the line beside it into one. `text_layer.line_split` is a verbatim port
of republic-project's `long_line_splitter.py` (commit `47dfc010`, MIT; changes listed in the
module docstring) plus an AS adapter. A preset with `merged_line_split="on"` (`sg_1.10.94_v2`)
runs it as a `scan_hook` during extract:

- Lines of regions whose tag is in `preset.split_region_tags` (`index`, `resolution`,
  `untagged`, `Text`) take part; page-furniture tags are never split.
- Parameters are derived per inventory and page side from the lines themselves
  (`derive_split_params`): the text span is the 5th percentile of `x` to the 95th of `x + w`;
  the lines with a width of 0.35-0.6 of that span give the default line width and character
  count; a line is a merge candidate when it has at least that many characters and is at least
  1.19 times that wide (`merged_width_factor`). The plain median width is not used (in 456 most lines are merged).
- The bigram scorer (the notebook's PMI scorer) is fitted on the inventory's own lines, with no
  labels (`fit_split_model`). Order per page side: hyphenated splits, then bigram splits (a page
  that raises keeps its hyphenated result), then `page_split_columns`.
- A merged line is replaced by two lines, `<line_id>_L` and `<line_id>_R`, each with
  `split_from=<line_id>`, `edit_flags` `split:hyphen` or `split:bigram`, and `column` `1`/`2`;
  `text_raw` of a half is the corresponding part of the original (runs of spaces in the original
  count as one space). Every other participating line gets `column` `1` or `2` from the column
  split (`x` for extra lines is possible in the code but never occurs). Lines without text and
  lines of other tags keep an empty `column`. A split that would leave a half without text
  (degenerate) is dropped and the line stays whole.
- `split_model/<inv>.json` (parameters, finder sizes) and `split_stats/<inv>.json` (candidates
  before, split by hyphen and bigram, candidates after, page errors) are written next to the
  tables. See `reports/phase-06-text-layer-v2-sg.md` (step 15e) for the counts.

### Glued-word repair (stage `wordfix`, step 15f; off by default)

`text_layer.word_split` inserts spaces into long out-of-vocabulary tokens. A token qualifies when its
core (letters only, leading and trailing punctuation stripped; a trailing `-` or `„` stays outside the
core) has at least `min_token_len` letters and occurs fewer than `min_part_count` times in the
vocabulary (lower-cased cores over the layer's own `text_raw`, or `--word-vocab`). A Viterbi search
looks for 2 to `max_parts` (4) parts of at least `min_part_len` (2) letters, each occurring at least
`min_part_count` times, with score sum(log p) - `split_penalty` x (parts - 1); it must beat
`log(0.5/total)`. Casing is kept and removing the inserted spaces gives `text_raw` back.
`as-build-text-layer --preset P --out-dir D --stage wordfix` rewrites `lines/<inv>.lines.tsv.gz`
(`text` recomputed from `text_raw`; flag `wordsplit:<n>`, earlier `wordsplit:*` flags replaced, other
flags kept; idempotent) and writes `word_split/{vocab.tsv.gz, params.json, stats.json,
edits/<inv>.tsv.gz}`. `--stage all` runs it only when `preset.word_split == "on"`. Both SG
presets hold the dev-chosen parameters (8, 20, 0) and are off: real precision was 0.62 (print) and
0.16 (hand), see the phase 06 report, step 15f.

### Layout tables (steps 15fp, 15fq, 15fq2)

`as-build-text-layer --preset P --out-dir D --stage layout` (v2 presets; no `--pagexml-dir`;
not part of `--stage all`; `--layout-params` takes JSON overriding `LayoutParams`) reads
`scans/`, `regions_meta/` and `lines/` and writes `layout/`. The input tables are never
rewritten; run it on a copy of the layer. Per scan (`text_layer.layout`, on
`analysis.page_structure`):

1. Regions are rebuilt from their lines (`line_idx` order); their coords are the hull of the
   line boxes, not the `regions_meta` box. A region with no line with text gets role `empty`.
2. Page split. A scan is an opening iff `page_w > portrait_ratio · page_h`, else one page
   `single`. `gutter_rel` is fitted once per inventory: the median centre/`page_w` of the widest
   zero-coverage gap (≥ `min_gap_px`) of line x-extents in [0.35, 0.65]·`page_w`. The
   per-inventory rule (`gutter_rule`; `auto`: `gap` if at least `gap_share_min` of the openings
   with text have such a gap, else `main_edge`) sets how `separation_x` is found per scan.
   `main_edge`: from the long lines (width ≥ `long_line_frac` × the inventory's median line
   width) of both pages, the smaller of the verso's right edge (`edge_quantile`) and the recto's
   left edge; it needs `min_long_lines` per side and must lie within ±`gutter_window_frac·page_w`
   of the gutter, else the `gap` logic applies. `gap`: the centre of the gap nearest
   `gutter_rel·page_w` within that window, else the x of minimum coverage there
   (`min_coverage`). Lines go to `verso`/`recto` by box centre; a region with lines on both
   sides is split (`split_at_gutter`).
3. Per page: regions are split on the line field `split_on_line_field` (`column`; print: the two
   text columns; `split_at_column`), page furniture is flagged (regions of at most
   `furniture_max_lines` lines and `furniture_max_chars` characters that fullmatch
   `furniture_patterns`, or catch words in the bottom `catchword_band` of the page; the SG
   presets set page numbers, dates and running years), then regions that overlap
   (`regions_overlap`, `overlap_threshold`) are merged until no pair overlaps. Furniture takes
   no part in merges, and an overlapping pair is not linked by the interleave guard when one
   region has only lines narrower than `interleave_width_frac` × the other's median line width
   and lies strictly inside its line y-range. The column merge guard
   (`column_merge_guard`, step 15fq2; needs `split_on_line_field`) never lets two regions end up in
   one merge set when the union of their non-empty line values of that field has more than one
   element (a `''` line conflicts with nothing; checked per component, edges in region order).
   Skipped pairs are counted in the scans column `n_column_blocked`. `merge=False` skips merging. Merged lines are
   sorted by (baseline mean y, box left). An unmerged, unsplit region keeps its `region_id`; any
   other gets `legacy_doc_id(scan_id, hull box)`.
4. Columns: regions sorted by left join the first column where
   `min(ov/w_region, ov/w_column) > col_min_ratio` and the max `> col_max_ratio`. A column is
   `main` when its characters are ≥ `main_min_char_frac` × those of the page's largest column,
   else `extra`.

Defaults: `overlap_threshold=0.5`, `col_min_ratio=0.5`, `col_max_ratio=0.75`,
`main_min_char_frac=0.25`, `min_gap_px=20`, `gutter_window_frac=0.15`, `portrait_ratio=1.0`,
`gutter_rule=auto`, `gap_share_min=0.95`, `long_line_frac=0.6`, `min_long_lines=5`,
`edge_quantile=0.95`, `split_on_line_field=column`, `furniture_max_lines=2`,
`furniture_max_chars=30`, `catchword_band=0.04`, `interleave_guard=True`,
`interleave_width_frac=0.5`, `merge=True`; a preset's `layout_params` override these, and
`--layout-params` overrides both. `LEGACY_15FP` reproduces the 15fp stage. `sg_1.01.02_v2` sets
`merge=False` (step 15fq: the hand merges were 0.52 precise).

| Table | One row per | Columns |
|---|---|---|
| `layout/scans/<inv>.tsv.gz` | scan | `scan_id, page_mode` (`opening`/`single`), `separation_x` (empty for `single`), `split_method` (`gap`/`main_edge`/`min_coverage`/`none`; `none` for single pages and openings without text), `n_lines_crossing` (lines whose box contains `separation_x`), `n_regions_in`, `n_regions_out` (non-empty regions only), `n_merges` (output regions that are merges), `n_gutter_splits` (input regions split at the gutter), `gutter_rule` (`gap`/`main_edge`, per inventory), `n_column_splits`, `n_furniture`, `n_guard_blocked` (overlapping pairs the interleave guard kept apart) |
| `layout/regions/<inv>.tsv.gz` | layout region, and empty source region | `scan_id, page` (`verso`/`recto`/`single`), `layout_region_id, x, y, w, h` (hull of the lines; the `regions_meta` box for `empty`), `layout_column` (0… by left edge per page; empty for `empty`), `layout_role` (`main`/`extra`/`empty`), `n_lines, source_region_ids` (space-separated, input order), `change` (`unchanged` or the `+`-join of `merged`, `split_at_gutter`, `split_at_column`), `furniture` (`1`/`0`), `column_value` (the line-field value the region shares, else empty) |
| `layout/lines/<inv>.tsv.gz` | input line | `scan_id, line_id, region_id` (input), `page, layout_region_id, layout_line_idx, layout_column, layout_role` |

Rows of `layout/regions/` are per scan, per page, by column then top, with the empty regions
last. `layout/stats.json` holds per inventory the summed counts, `split_method` and
`layout_role` counts, `gutter_rel`, `n_scans_with_gap`, `n_landscape_scans` and
`max_merge_passes`; `layout/params.json` the `LayoutParams` used.

Known limits (phase 07 report, steps 15fp, 15fq): `min_coverage` sometimes misses the gutter by a
whole page (15fp; 15fq's `main_edge` removes most of these in 3350). In print, a layout region
can still hold lines of both columns (145 regions in 455 and 456): where a column piece overlaps
the other column's piece, for example on title pages, the merge joins them again. Hand merges
are off in the SG hand preset: merged regions interleaved margin items, columns and line
fragments (precision 0.52).

### Type tables (step 15g)

`--stage types` (v2 presets only, not part of `all`; `--types-params` JSON overrides the preset's
`TypeParams`) reads `scans/`, `lines/` and `layout/` and writes `types/`. The old `region_type`
stays retired. Code: `text_layer/types.py`.

`types/segments/<inv>.tsv.gz`, one row per line **segment**, in input line order. Every line
with non-blank `text` has at least one; empty lines have none.

| Column | Meaning |
|---|---|
| `scan_id, line_id, seg_idx` | the line and the segment's order in it |
| `char_start, char_end` | offsets into the line's `text`; `text[char_start:char_end]` is the stripped segment text, whitespace between segments is excluded |
| `x, y, w, h, geom_method` | `line` (the line box) or `char_proportion` (`x = line.x + round(w*char_start/len(text))`, same for `w`) |
| `page, layout_region_id` | from `layout/lines` |
| `line_type` | one of `main_text, page_number, running_header, date_header, catch_word, signature_mark, marginalia, index, other` |
| `type_method` | the rule: `rule:pn_line`, `rule:pn_line_offchain`, `rule:pn_suffix`, `rule:date_line`, `rule:date_prefix`, `rule:margin_prefix`, `rule:header`, `rule:catch_word`, `rule:signature`, `rule:index_page`, `rule:extra_role`, `rule:page_margin`, `rule:noise`, `rule:default` |
| `column_override` | print presets: `x` for page_number, running_header, date_header, catch_word and signature_mark segments (F06-15e.8), else empty |

`types/regions/<inv>.tsv.gz`: one row per non-empty layout region: `scan_id, page,
layout_region_id, region_type` (most segment characters, ties in the order of `TYPES`),
`type_method` (`char_majority`), `n_segments`, `type_counts` (`main_text:120;page_number:3`).
`types/stats.json` (per inventory: segments and characters per type, splits per rule, page-number
chain) and `types/params.json`.

Only three rules split a line: `rule:date_prefix` (a date at the start of the topmost line or of
a line starting in the margin), `rule:margin_prefix` (a line starting in the margin and running
into the text is cut at the space nearest to the main left edge, within 0.04 `page_w`; under the
default `margin_cut="month_only"` the cut is made only when the prefix is a month alone, typed
`date_header`, and any other line stays whole; `"all"` is step 15g's behaviour, where the other
prefixes became `marginalia`; `"off"` skips the rule) and `rule:pn_suffix` (a number at the
end of the topmost line that lies in the inventory's longest increasing chain of page numbers).
All other rules type the whole line. Page context (top band = 3 topmost text lines, bottom band =
2 lowest, main left edge and centre) is computed per page and, where a page has several main
columns, per column: a layout column, or in print the line's own `column` field, that holds
>= 25% of the main characters has its own reference.

Known limits (phase 07 report, step 15g): `rule:margin_prefix` cuts of non-month prefixes were mostly wrong (2 of 9
judged right; step 15g2 removed them), `marginalia` precision is about 0.5 (register numbers and main text in `extra`
regions), `date_header` also types date entries on the 3821-3823 register pages, and page numbers
read with extra OCR digits ("11323 )") stay `main_text`.

### Units and the doc_id mapping (step 15h)

`--stage units` (v2 presets only, not part of `all`; `--unit-params` JSON overrides the preset's
`UnitParams`; `--republic-dir` points at REPUBLIC's `paragraphs-<inv>.tsv.gz` for print) reads
`scans/`, `regions_meta/`, `lines/`, `layout/` and `types/` and writes `units/`. Code:
`text_layer/units.py`. No text is joined here (step 15hj).

A **unit** never crosses a layout region. Per layout region, from its segments in
(`layout_line_idx`, `seg_idx`) order:

- hand presets: runs of adjacent segments of one non-`main_text` type are units (furniture,
  `marginalia`, `index`, `other`; method `hand:run`); all `main_text` segments form one unit
  (`hand:region`), cut where a `date_header` or `running_header` segment lies between two main
  segments (`hand:region_cut_at_header`);
- print presets: the same runs (`print:run`); the `main_text` segments are cut into paragraphs. A
  new paragraph starts at main segment i > 0 if a cue holds: `indent` (`x_i - col_left >=
  indent_frac * med_h`), `short_prev` (previous segment ends left of `col_right - short_frac *
  col_width`), `gap` (vertical gap `> gap_factor * med_h`), `header` (a date/running header
  between; always on). `col_left`/`col_right` are the medians over the region's main segments.
  Methods: `print:region` (one paragraph), `print:region_start` (the first of several),
  `print:<cues>` (e.g. `print:indent+short_prev`). `UnitParams` fitted in step 15h:
  `indent_frac=0.5, short_frac=0.1, gap_factor=None` (`SG_PRINT_UNIT_PARAMS`).

**Unit ids (R5).** In each layout region the unit with the most `main_text` characters (none:
most characters; ties: first) takes `layout_region_id`, which for an `unchanged` region is the
legacy `doc_id`, byte-identical. Every other unit gets `legacy_doc_id(scan_id, bbox of its
segments)`; while the id is taken (by a unit, a layout region or a legacy region of the scan),
`w` is raised by 1 (`n_id_collisions` in `stats.json`).

**Reading order** `unit_order` (per scan, from 0): page (verso, recto/single), `layout_column`,
unit top y, position of the first segment.

| Table | Columns |
|---|---|
| `units/units/<inv>.tsv.gz` | `scan_id, page, unit_id, unit_order, unit_type, unit_method, layout_region_id, layout_column, x, y, w, h, n_segments, n_lines, first_line_id, last_line_id` (one `unit_type` per unit) |
| `units/segments/<inv>.tsv.gz` | `scan_id, unit_id, seg_order, line_id, seg_idx` |
| `units/doc_id_map/<inv>.tsv.gz` | `old_doc_id, unit_id, link, old_status, n_segments_shared, n_chars_shared`; one row per (legacy region, unit holding its segments); `link` is `same_id`, `part` or `none` |
| `units/republic_lines/<inv>.tsv.gz` (print) | `republic_line_id, scan_id, line_id, match` |
| `units/stats.json`, `units/params.json` | counts per type and method, `old_status`, crosswalk counts; the parameters |

`old_status` (repeated on each row of a legacy region): `identical` (one `same_id` row, the unit
holds exactly the region's segments), `trimmed` (a `same_id` row plus others, or the unit holds
fewer or more segments), `changed` (no `same_id` row), `empty` (no text lines; `unit_id=""`).

**REPUBLIC crosswalk.** Each distinct `line_start`/`line_end` id (`<scan_id>-line-x-y-w-h`) is
matched against the scan's non-blank v2 lines: `exact`; `near` (all four within 5 px, smallest
summed difference); `split_original` (the box contains two halves with one `split_from`; maps to
the first half); `split_half` (our split half with the best IoU >= 0.5, only lines with a
`split_from`; REPUBLIC splits merged print lines itself and its halves differ from ours by 10-30
px in width); else `unmatched`.

### Unit text and offsets (step 15hj)

`--stage wbc` (v2) feeds the segment texts of the `main_text` units to the v1 statistics and
decides per inventory with the preset's `word_break_rule` (default: v1's rule),
`wbc_sanity_in`/`wbc_sanity_out` (SG: `()` and `(":", ".")`) and `word_break_stat`
(`default` or `robust`). The parts file `wbc/parts/<inv>.tsv` has the v1 columns plus
`all_freq_robust` (`all_freq` minus occurrences in a run at line start or between two digits,
ignoring adjacent marks) and `end_rel_frac_robust`; the `robust` statistic puts these in place of
`all_freq` and `end_rel_frac` before the rule is applied (pooled counts too). Both decisions are
written to `wbc/wbc_per_inventory_{default,robust}.tsv` and `wbc/corpus_default_{default,robust}.json`;
`wbc_per_inventory.tsv` and `corpus_default.json` hold the one the preset names.

`--stage join` (v2) joins every unit's segments with `join_unit(texts, wbcs)`
(`text_layer/join_v2.py`), which returns exactly `join.join_region`'s text, plus per segment its span
and per boundary the kind (`plain`, `merge`, `g_*`). Segment text = `lines.text[char_start:char_end]`
of `types/segments`. Tables under `join/`:

- `units/<inv>.tsv.gz`: `scan_id, unit_id, unit_type, word_break_chars, n_breaks_merged,
  ends_with_break_char, text`.
- `segments/<inv>.tsv.gz`: `scan_id, unit_id, seg_order, line_id, seg_idx, char_start, char_end,
  line_offset, break_char_end, merged_with_next, boundary_kind`. `char_start`/`char_end` are unit
  text offsets; **a unit position `p` in `[char_start, char_end)` is line position
  `line_offset + (p - char_start)` in `lines.text`** (`line_offset = types.char_start` plus 1 when a
  leading break char was dropped by the both-ends fix). Between consecutive spans the unit text has
  `""` after a `merge` and one space otherwise. `break_char_end` is the segment's final break char
  (`""` if none); on a `merge` it is not in the span. `boundary_kind` is the boundary after the segment.
- `stats/<inv>.json`: boundary kinds (also by unit type), `wbc_set`, guards, offset violations.

Guards, the both-ends fix and the wbc set are those of v1. A WBC set that fails the sanity anchors
stops `join` as in v1 (`--ignore-sanity` overrides and is recorded in the manifest).

#### Pinned word-break sets (step 15hj2, UA-010)

`LayerPreset.wbc_pinned` (`None` = derive, as above) is a mapping `{"default": chars, "<inv>": chars}`.
The SG presets pin `-=„` everywhere and `-:=„` for 11281 (`SG_HAND_V2`, `SG_PRINT_V2`). Why: the
derived sets failed the pooled anchors (`:` in hand_local, `.` in print), `.` is right about half the
time as a break char (11/20 in 455, 4/10 in 3822), and the rule misses `„` in 3351 and 3354. With a
pin, `decide_v2` still computes and writes both derived variants (`*_default`/`*_robust` files), then
replaces the decision: `wbc_per_inventory.tsv` has `mode = pinned`, the pinned `wbc_set`, and
`char_status` entries suffixed `/pinned_in` or `/pinned_out` (e.g. `.:derived_in/pinned_out`).
`corpus_default.json` keeps every key; `default_set` is the pinned default, and it adds `pinned`,
`derived_default_set` and `pooled_rule_sanity_pass` (the old `sanity_pass`). `sanity_pass` is then
computed on the **pinned default set** (every `sanity_in` char in, no `sanity_out` char in); per-inventory
pins are explicit decisions and are not checked. The manifest `wbc` entry gets `pinned: true`.

**hand_tar exceptions (step 15i2, UA-011 b).** `SG_HAND_V2` drops `=` for 3349, 3350, 3351, 3353 and 3354
(pin `-„`), the hand_tar inventories whose derived set (15hj) lacks `=`. In 3349, 3350 and 3353 the `=`
merges the default pin added were right in only 11 of 20 (lower-case right-side share 0.74). 3352 derived
`=„` and keeps `-=„`; 3351 and 3354 had no `=` merges, so their output does not change.

## Published SG pilot (text_layer_v2)

Published 2026-10-21 (phase 07, step 15i, from the dev layer `15i-b`) for the sibling projects, `coverage: partial`:

| Path (under `/Users/Shared/claude_projects/data_scopes/shared-data/`) | Content |
|---|---|
| `NL-HaNA_1.01.02/text_layer_v2/` | hand, 14 inventories: 3349-3354, 3771-3773, 3821-3823, 11182, 11281 (preset `sg_1.01.02_v2`) |
| `NL-HaNA_1.10.94/text_layer_v2/` | print, 455 and 456 (preset `sg_1.10.94_v2`) |

Tables and keys are those of the sections *Layout tables*, *Type tables*, *Units and the doc_id mapping* and *Unit text and
offsets* above; `manifest.json` has a `publication` key with the producer, `as_version`, `git_head`, the audit summary (176 of 182
invariants; the 6 failures are the reported lower-case merge share below 0.95 in 3351, 3823, 11182, 11281, 455, 456), the counts per
inventory, the sources with sha256 (tar/zip) or path and file count (directories) and the known gaps. Known gaps: `stages.*.argv`
and the `member`/`source` columns name dev paths (`data/text_layer_v2_dev/15i-src/...`); glued words are not repaired (UA-003); the
build ran on a tree with uncommitted changes (`as_version` 0.9.0 as recorded). The full SG run will be `text_layer_v3`; a change is a
new version, never a rebuild into these directories. Catalogue: shared-data `README.md`; consumer note: LotA
`docs/from-archival-structures.md` section 6. The audit (`reports/data/phase-07-sg-units-and-pilot/step-15i/audit.py --layer <base>`)
expects `<base>/{hand,print}/*_text_layer_v2`, so audit the published copies through symlinks with that suffix.

## Provenance

`manifest.json` holds the layer-level keys `layer`, `row_order`, `columns`, `format`,
`untagged_region_tag` and, per stage under `stages`, `finished_utc`, `git_head` (of
archival-structures), `archival_structures` and `pagexml_tools` (version, path, and for a git
checkout HEAD and dirty files), `preset`, `python`, `platform`, `argv`, `params` and
stage-specific keys (`n_inventories`, `failed`, `default_set`, `sanity_pass`, ...).
`status.tsv` has one row per inventory and stage attempt; `progress.log` is the run log.

## Checking a rebuild against v1

To check that a rebuild from the GLOBALISE v2.1 PageXML reproduces the v1 layer on a sample,
with `<IN>` the v2.1 PageXML directory (holding `<inv>.zip` files or `<inv>/` directories),
`<O>` a fresh local output directory, `<DS>` the `data_scopes` directory that holds
`shared-data/`, and `<VENV>` an environment with archival-structures installed (see *Building a layer*):

```
INVS=1171A,1457B,7872A,9615A,1095,1394,2158,3172,3944,3972,7586,8817,8846,9253,9685,9994,10256,10466,2555,8834
<VENV>/bin/as-build-text-layer --pagexml-dir <IN> --out-dir <O> --stage extract --inventories $INVS --workers 8
mkdir -p <O>/voc_text_layer_v1/wbc
cp <DS>/shared-data/voc_text_layer_v1/wbc/wbc_per_inventory.tsv <DS>/shared-data/voc_text_layer_v1/wbc/corpus_default.json <O>/voc_text_layer_v1/wbc/
<VENV>/bin/as-build-text-layer --pagexml-dir <IN> --out-dir <O> --stage join --inventories $INVS --workers 8
<VENV>/bin/python -m archival_structures.text_layer.compare <O>/voc_text_layer_v1 <DS>/shared-data/voc_text_layer_v1 --inventories $INVS --out <O>/compare_v1.tsv
```

The v1 word-break decisions are reused (the `cp`), so the join runs with the same sets. Exit
code 0 means `lines/`, `regions_meta/` and `regions/` are byte-identical and the two stats JSONs
are equal (the `source` key is ignored). On a mismatch, `<O>/compare_v1.tsv` has one row per
inventory and kind: `order_only` marks files whose rows are the same but in another order,
`multiset_equal` False marks changed text. See
{func}`archival_structures.text_layer.compare.compare_layers`.

## Known pagexml-tools 0.9.0 limits (unchanged since 0.8.1)

- Nested regions fail to parse (`TypeError: unsupported operand type(s) for +=: 'set' and 'set'`
  in `get_all_text_regions`, `pagexml/model/pagexml_document_model.py` L447). Example:
  `tests/fixtures/text_layer/pagexml/9999A/NL-HaNA_1.04.02_9999A_0003.xml`. The scan is skipped, `Error parsing file` is printed and the error is recorded in `extract_stats/` (`parse_errors`) and `status.tsv` (`n_errors`).
- A region with neither `Coords` nor lines is dropped.
