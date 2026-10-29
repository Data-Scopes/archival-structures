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
into the environment you build with. The install pulls in `pagexml-tools>=0.9.1` and
`fuzzy-search>=3.0.0`:

```
python -m venv .venv && .venv/bin/python -m pip install -e /path/to/archival-structures
.venv/bin/as-build-text-layer --pagexml-dir D --out-dir O --estimate --workers 8   # predicted time, parses nothing
.venv/bin/as-build-text-layer --pagexml-dir D --out-dir O --stage all --workers 8
```

The archival-structures checkout's own `.venv` already has everything installed, so it can be
used directly.

`D` holds `<inv>.zip` files or `<inv>/` directories. `--stage` runs `extract`, `wbc`, `join`
or `all` (v2 presets also `layout`, `types`, `units`). Each stage is resumable per inventory (rerun the same command;
`--force` redoes it). Since step 15y this includes the v2 stages `layout`, `types` and `units`: an inventory is skipped when
its done row is newer than the latest row of the stage it reads (`build.V2_UPSTREAM`, `build.is_current`; `wbc` parts and
`join` too) and its outputs exist, and a stage redoes everything when its `params.json` differs from the new parameters. A
Since step 15yr a v2 stage skips ("blocked") an inventory whose upstream stage, at any depth of `V2_UPSTREAM`, has a last row
that is not `done` (`build.upstream_ok`): after a failed forced extract the later stages neither run on the old lines nor record
themselves `done`; the log says `N blocked`. The v2 build reuses the sizes recorded in `status.tsv` (`build.recorded_sizes`,
last `done` extract row, used when the source file's size equals the recorded `input_bytes`) and sizes only the other sources
(`[sizes] N from status.tsv, M measured`). The `seconds` of a v4 extract row time the whole call, mode pre-pass included
(`prepass_seconds` is in the stage result; rows written before 15yr leave the pre-pass out, about half of each call). A
wordfix run writes no status rows, so it needs `--force` on layout (no preset uses wordfix). v1 (VOC) is unchanged. `--inventories a,b`, `--inventories-file F` and `--limit N` select inventories;
`--allow-partial`, `--allow-subset-wbc`, `--ignore-sanity`, `--wbc-stats-only` and `--write-events` are described in
`--help`. The driver's module docstring has the full handoff text for a run on another machine,
including expected runtime and output size. Run AS's `tests/test_text_layer_*.py` first.

**Never build into an existing shared layer with a subset of its inventories** (the one exception is the forced extract of an in-place rebuild, *Rebuilding pinned volumes* below, always followed by `--stage all` over the whole layer). Since step 15hj,
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
part of `--stage all`, also runnable alone; `--layout-params` takes JSON overriding `LayoutParams`) reads
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
   other gets `legacy_doc_id(scan_id, hull box)`. Since step 15ir (`sort_split_lines=True`; `LEGACY_15FP`
   has it off) the lines of each piece of a column-split region are sorted by the same key (a Laypa
   page-level print region interleaves the columns in XML order); an unsplit region keeps its line
   order, so `unchanged` regions can still read out of order. `blocked` pairs are counted once, per
   final region pair (earlier a relation was counted again under each merged id).
4. Columns: regions sorted by left join the first column where
   `min(ov/w_region, ov/w_column) > col_min_ratio` and the max `> col_max_ratio`. A column is
   `main` when its characters are ≥ `main_min_char_frac` × those of the page's largest column,
   else `extra`.

Defaults: `overlap_threshold=0.5`, `col_min_ratio=0.5`, `col_max_ratio=0.75`,
`main_min_char_frac=0.25`, `min_gap_px=20`, `gutter_window_frac=0.15`, `portrait_ratio=1.0`,
`gutter_rule=auto`, `gap_share_min=0.95`, `long_line_frac=0.6`, `min_long_lines=5`,
`edge_quantile=0.95`, `split_on_line_field=column`, `furniture_max_lines=2`,
`furniture_max_chars=30`, `catchword_band=0.04`, `interleave_guard=True`,
`interleave_width_frac=0.5`, `merge=True`, `column_merge_guard=True`, `sort_split_lines=True`, `line_sort=row`, `row_tol_frac=0.2`, `sort_unchanged_lines=False`; `gutter_rule` is validated (`gap`, `main_edge`, `auto`); a preset's `layout_params` override these, and
`--layout-params` overrides both. `LEGACY_15FP` reproduces the 15fp stage.

Line order (step 15j): with `line_sort=row` the lines of merged regions and column-split pieces are
sorted by printed row, not by baseline y alone. Walking the lines by baseline y, a line joins the
current row when its baseline y minus the row's first baseline y is at most `row_tol_frac` x the median
box height of the region's lines; rows keep their order and each row is sorted by box x (planning:
baseline gaps below 15 px within a row, about 48 px between rows, h about 75 px). `line_sort=baseline`
is the old sort (`LEGACY_15FP`). `sort_unchanged_lines=True` (the print presets only) applies the same
sort to `unchanged` regions in `layout/`; ids, `change` and boxes stay, and `lines_resorted` flags
the regions whose line order changed. Hand keeps the XML order. `sg_1.01.02_v2` sets
`merge=False` (step 15fq: the hand merges were 0.52 precise).

| Table | One row per | Columns |
|---|---|---|
| `layout/scans/<inv>.tsv.gz` | scan | `scan_id, page_mode` (`opening`/`single`), `separation_x` (empty for `single`), `split_method` (`gap`/`main_edge`/`min_coverage`/`none`; `none` for single pages and openings without text), `n_lines_crossing` (lines whose box contains `separation_x`), `n_regions_in`, `n_regions_out` (non-empty regions only), `n_merges` (output regions that are merges), `n_gutter_splits` (input regions split at the gutter), `gutter_rule` (`gap`/`main_edge`, per inventory), `n_column_splits`, `n_furniture`, `n_guard_blocked` (overlapping pairs of final regions the interleave guard kept apart; since step 15ir the pairs of the last merge pass only), `n_column_blocked` (the same for the column merge guard), `n_regions_resorted` (regions with `lines_resorted=1`) |
| `layout/regions/<inv>.tsv.gz` | layout region, and empty source region | `scan_id, page` (`verso`/`recto`/`single`), `layout_region_id, x, y, w, h` (hull of the lines; the `regions_meta` box for `empty`), `layout_column` (0… by left edge per page; empty for `empty`), `layout_role` (`main`/`extra`/`empty`), `n_lines, source_region_ids` (space-separated, input order), `change` (`unchanged` or the `+`-join of `merged`, `split_at_gutter`, `split_at_column`), `furniture` (`1`/`0`), `column_value` (the line-field value the region shares, else empty), `lines_resorted` (`1` if the lines of an `unchanged` region were re-ordered by `sort_unchanged_lines`, else `0`) |
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

`--stage types` (v2 presets only, part of `--stage all` and runnable alone; `--types-params` JSON overrides the preset's
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
| `type_method` | the rule: `rule:pn_line`, `rule:pn_line_offchain` (a pn_line-shaped band line not confirmed by the page-number chain: since step 15ir every `rule:pn_line` segment that is not the in-chain page's candidate line is relabelled; the `line_type` stays `page_number`, except where step 15k retypes it to `other`, see below), `rule:pn_line_gap` (step 15k: an off-chain number strictly between the chain numbers of the nearest chain pages before and after; stays `page_number`), `rule:year_offchain` (step 15k: an off-chain year line, `line_type` `other`), `rule:pn_suffix`, `rule:date_line`, `rule:date_prefix`, `rule:margin_prefix`, `rule:header`, `rule:catch_word`, `rule:signature`, `rule:index_page`, `rule:extra_role`, `rule:page_margin`, `rule:noise`, `rule:default` |
| `column_override` | print presets: `x` for page_number, running_header, date_header, catch_word and signature_mark segments (F06-15e.8), else empty |

`types/regions/<inv>.tsv.gz`: one row per non-empty layout region: `scan_id, page,
layout_region_id, region_type` (most segment characters, ties in the order of `TYPES`),
`type_method` (`char_majority`), `n_segments`, `type_counts` (`main_text:120;page_number:3`).
`types/stats.json` (per inventory: segments and characters per type, splits per rule, page-number
chain) and `types/params.json`.

**Off-chain page numbers (step 15k, decision P6).** With `offchain_retype=True` (both SG presets) each
`rule:pn_line_offchain` segment is decided in this order: (1) its number (first `\d{1,4}` of the segment text) lies
strictly between the chain numbers of the nearest chain pages before and after: `rule:pn_line_gap`, stays
`page_number`; (2) the segment text fullmatches `year_line` and `offchain_years_other`: `other`,
`rule:year_offchain`; (3) its band (`top` = the `top_n` topmost lines of the page, else `bottom`) is in
`offchain_other_bands`: `other`, method stays `rule:pn_line_offchain`. The three params default to
`False`, `()`, `False` (output unchanged). SG presets: `offchain_years_other=True` (37 of 37 years read are not page
numbers); `offchain_other_bands=()` for hand and `("bottom",)` for print (bars: >= 16 of 20 read not page numbers; print
bottom 19, print top 3, hand top 6, hand bottom 12; the rest were misread page numbers, signatures or unclear).
Verdicts: `reports/data/phase-09-text-layer-v3-prep/step-15k/pn_verdicts.tsv`.

`TypeParams` validates `margin_cut` (`month_only`, `all`, `off`) and `top_n`, `bottom_n` (>= 1) and `offchain_other_bands` (a subset of `top`, `bottom`) and raises `ValueError`.
A line that fullmatches `date_line`, `pn_line` or `year_line` is never split by `rule:date_prefix` (step 15ir: "28. Junij 1650." stays one `date_header` segment).

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

`--stage units` (v2 presets only, part of `--stage all` and runnable alone; `--unit-params` JSON overrides the preset's
`UnitParams`; `--republic-dir` points at REPUBLIC's `paragraphs-<inv>.tsv.gz` for print; since step 15k it has no default: without it a print build logs a warning and writes no `republic_lines/`) reads
`scans/`, `regions_meta/`, `lines/`, `layout/` and `types/` and writes `units/`. Code:
`text_layer/units.py`. No text is joined here (step 15hj). Unit ids: the main part of a layout region keeps the
region id; any other unit takes the legacy id of its own box, bumping `w` while the id is taken. Since step 15ir a
unit whose own-box id is a legacy region id that is not a layout region id of the scan, and that holds every
non-blank line of that legacy region, takes that id (a legacy twin; `doc_id_map` status `same_id`, no longer
`changed`). `--stage units` writes `republic_dir` (the REPUBLIC directory used, or `null`) into `units/params.json`
and, in print mode, logs a warning when `paragraphs-<inv>.tsv.gz` is missing.

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
| `units/republic_lines/<inv>.tsv.gz` (print) | `republic_line_id, scan_id, line_id, match, role` |
| `units/stats.json`, `units/params.json` | counts per type and method, `old_status`, crosswalk counts; the parameters |

`old_status` (repeated on each row of a legacy region): `identical` (one `same_id` row, the unit
holds exactly the region's segments), `trimmed` (a `same_id` row plus others, or the unit holds
fewer or more segments), `changed` (no `same_id` row), `empty` (no text lines; `unit_id=""`).

**REPUBLIC crosswalk.** Each distinct `line_start`/`line_end` id (`<scan_id>-line-x-y-w-h`) is
matched against the scan's non-blank v2 lines: `exact`; `near` (all four within 5 px, smallest
summed difference); `split_original` (the box contains two halves with one `split_from`; maps to
the first half); `split_half` (our split half with the best IoU >= 0.5, only lines with a
`split_from`; REPUBLIC splits merged print lines itself and its halves differ from ours by 10-30
px in width); else `unmatched`. `role` (step 15k) is `start` (the id only occurs as `line_start`), `end` (only
as `line_end`) or `both`. REPUBLIC reads an unsplit merged line as part of the paragraph, so a paragraph that ends
there ends in our last half: for role `end`, `split_original` maps to the last half (to the first half otherwise;
513 ids, 455 135 and 456 378). `units/stats.json` has `republic_crosswalk_roles` (`match|role` counts).

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

`--stage join` (v2) joins every unit's segments with `join_unit(texts, wbcs, lead_marks=preset.join_lead_marks)`
(`text_layer/join_v2.py`), which returns exactly `join.join_region`'s text, plus per segment its span
and per boundary the kind (`plain`, `merge`, `g_*`). Segment text = `lines.text[char_start:char_end]`
of `types/segments`. Tables under `join/`:

- `units/<inv>.tsv.gz`: `scan_id, unit_id, unit_type, word_break_chars, n_breaks_merged,
  ends_with_break_char, text`.
- `segments/<inv>.tsv.gz`: `scan_id, unit_id, seg_order, line_id, seg_idx, char_start, char_end,
  line_offset, break_char_end, merged_with_next, boundary_kind`. `char_start`/`char_end` are unit
  text offsets; **a unit position `p` in `[char_start, char_end)` is line position
  `line_offset + (p - char_start)` in `lines.text`** (`line_offset = types.char_start` plus the number of
  leading characters dropped: the both-ends fix, a `lead_marks` character (step 15ir), or a second leading
  break char the library strips itself). Between consecutive spans the unit text has
  `""` after a `merge` and one space otherwise. `break_char_end` is the segment's final break char
  (`""` if none); on a `merge` it is not in the span. `boundary_kind` is the boundary after the segment.
- `stats/<inv>.json`: boundary kinds (also by unit type), `wbc_set`, guards, `lead_marks`, offset violations.
  A merged segment may lose up to two trailing break chars (`--`, `=-`, `-„`; the library drops both), so the
  span check compares `text[char_start:char_end]` with `seg_text[lead:len(seg_text) - n_trailing_removed]`
  and requires the removed tail to be break chars only (step 15ir).

`LayerPreset.join_lead_marks` (step 15ir, default `""`; `","` in `SG_HAND_V2` only): on a `merge` boundary, a
character of this set at the start of the next segment text and followed by a letter is dropped after the
both-ends fix. In 11182 the scribe repeats the break mark at the start of the next line and the HTR reads it as
`,` ("gero„" + ",ijeert" gave "gero,ijeert"). `--stage join` records it in `join/stats/<inv>.json` and re-joins
when it differs from the preset.

Step 15l extends this (v2 join only; `join.py` and v1 are untouched):

- `join_lead_marks` strips one or two marks and an optional single space before a letter: `", rant"` and
  `",, den"` now join as "fuerant" and "behouden" (before: "fue,rant" kept the comma and space; `",,"` was kept).
  Bar: 67 of 70 hand rows read were the repeated break mark (62 + 5; 1 real comma, 2 garbled), needed 63.
- `LayerPreset.join_lead_marks_lower` (default `None`; per inventory, keys an inventory or `"default"`, like
  `wbc_pinned`): a character of the pinned string at the start of the next text, followed by a lower-case
  letter, is dropped ("hou-" + "Jdende" gives "houdende"; "JAN" is kept). `SG_PRINT_V2` pins `{"455": "J"}`:
  30 of 30 455 rows read were the repeated hyphen read as `J` (bar 27). 456 is not pinned (2 of 32 `mark`, 23 real
  words such as "Junii", "Januarii"; 7 unclear).
- `LayerPreset.join_upper_guard` (default `False`): after the strips, a merge whose right side starts upper-case is
  refused; boundary kind `g_upper_right`, both lines kept as they were, the run closes like any other guard. Bar
  27 of 40 `wrong`: hand 24, print 20, so both presets keep it off. The code path is tested and ready.
- `join/stats/<inv>.json` has `lead_marks_lower` and `upper_guard`; `--stage join` re-joins when either
  differs from the preset. `guards` stays v1's `ALL_GUARDS`.

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

## Text layer v4 (phase 12)

The v4 presets (`sg_1.01.02_v4`, `sg_1.10.94_v4`) decide per inventory whether to build with the hand or the print rules of
the v3 presets, so a printed volume inside 1.01.02 (3771-3773, 3821-3823) gets the print rules. A `LayerPreset` with
`mode_presets` (`{"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}`) is a *mode preset*; `volume_mode` says how the mode is
decided. `presets.inventory_preset(preset, mode)` returns the preset with the six rule fields
(`presets.MODE_RULE_FIELDS`: `merged_line_split`, `split_region_tags`, `layout_params`, `types_params`, `unit_params`,
`join_lead_marks`) of the mode's v3 preset; identity and per-archive fields (names, `wbc_pinned`, `join_lead_marks_lower`)
stay those of the v4 preset. v1-v3 presets and their output do not change.

**The rule.** The char-weighted median line-box height of the volume (regions with at least 100 characters, from the
pre-pass line rows) is at most 87.75 px for `print`, above it for `hand`; no qualifying region gives `hand` (method
`default`). Cut and procedure: [`reports/phase-12-hand-print-box-height.md` § Step 15u](../reports/phase-12-hand-print-box-height.md#step-15u-box-height-test)
(60/60 on the test volumes). `sg_1.10.94_v4` uses method `fixed` (`print`) and records the box-height decision as
`box_height_mode`.

**Build.** Stage `extract` runs `modes.extract_inventory_v2_modes`: `line_split.prepass_rows` (the raw line rows, the same
rows the 15u values come from), `modes.decide_mode`, `modes.write_mode`, then the extract with the resolved preset (with
the merged-line split in print mode; its model is fitted on the same rows rounded as before). Later stages read the mode
with `modes.read_mode(layer_root, inv)`; `layout/`, `types/` and `units/` `params.json` and manifest entries are
`{"hand": {...}, "print": {...}}` (v4 layers only). The REPUBLIC crosswalk is computed for print-mode inventories only.

**Split gate (step 15wr).** The merged-line splitter assumes two text columns per page side. For a 1.01.02 volume in
`print` mode, `modes.column_start_share` measures the share of lines (at least 20 characters, with a box) that start in the
right 60% of the text span of their page side (pooled over sides with at least 50 such lines). The splitter runs only if
that share is at least 0.35 (`presets.SPLIT_GATE_V4`) or cannot be measured. A gated volume keeps the print layout, types
and unit rules, without `split_model/`, `split_stats/` and `split_from` lines. 1.10.94 has no gate.

**`mode/` tables.** `mode/<inv>.json`: `mode`, `method`, `value`, `margin`, `n_chars`, `n_regions`, `params`, `inv`,
`n_scans`, `n_parse_fail`, `n_scans_sided` (scans with >= 200 qualifying characters), `scan_minority_share` (their share on
the other side of the cut than the volume), and for `fixed` also `box_height_mode`, `box_height_method`. A volume with a split gate (1.01.02, `print`) also has
`split_gate`: `{share, n_rows, min_share, split}`.
`mode/scans/<inv>.tsv.gz` (`scan_id, scan_order, page_w, page_h, n_lines_text, value, n_chars, n_regions, side`) and
`mode/regions/<inv>.tsv.gz` (`region_id, scan_id, n_lines, n_chars, h_med, pitch_med, hrel`).

**Limits.** One mode per volume: a printed insert in a handwritten volume (or the reverse) is built with the volume's rules.
The values are raw pixels and assume one scan scale (F12-R.1); a volume from another scan campaign could shift.

**Drop-cap-aware print units (step 15x, UA-026 (b), P18).** A printed paragraph opens with a dropped initial (its own
region), and the next one or two lines sit about 1.1-1.7 line heights right of the column edge beside it, so the `indent`
cue cut the paragraph there. `UnitParams.drop_cap_lines` (default `None`, left out of `to_dict` and `units/params.json`
when `None`) drops a cut whose only cue is `indent` when it falls within that many main lines of the last kept cut.
`sg_1.01.02_v4` sets `drop_cap_lines=2` for print-mode volumes through `LayerPreset.mode_overrides` (applied by
`inventory_preset`); hand mode, `sg_1.10.94_v4` and v1-v3 are unchanged. On the 31 *gedrukte resoluties* it removes 22.9%
of the `main_text` unit starts (units per scan 61.12 to 52.49; 3771: 46.05 to 35.21) and 279 of 38,176 decision/*Dat*
unit starts (0.7%) are lost with them. Reads: 59 of 60 dropped cuts continue the paragraph; 19 of 20 pages are closer to
the printed paragraphs, none worse. A decision clause that follows a one- or two-line entry directly can be merged into
it, and a three-line chunk beside an initial stays cut (the window is two lines).

**Known issues (UA-026, F12-15v.4, F12-R.3, F12-R.4, UA-029).** The drop-cap rule was read only on the *gedrukte
resoluties*; in the single-column *nouvelles* and *plakkaten* it applies as well (UA-029). The 15x review read 40
dropped cuts per series there: on readable text 27 of 28 (*plakkaten*) and 30 of 36 (*nouvelles*) are right. In the
*nouvelles* the rule can merge a closing formula, signature or dateline into the paragraph before it, and the join can
then glue words across a break mark ("Getee|Waar meede, &c."; F12-R.13, F12-R.14). The merged-line splitter runs only where the volume's column-start share is at least
0.35 (`split_gate` in `mode/<inv>.json`): the two-column *gedrukte resoluties* (0.50) are split, the single-column
*nouvelles*, *plakkaten* and *Processtukken* (at most 0.23) are not. Before step 15wr they were split, which cut about a
million ordinary lines in two (`15w-a`).

**Mode pins (step 15y, P21, P24).** `volume_mode["pins"]` is a `{inventory: "hand" | "print"}` mapping, for volumes the rule gets
wrong and a reader has settled (`presets.MODE_PINS_V4`: 16 pins since step 15z, see *Pinned volumes* below; the first, 3463, was read handwritten 5/5 in 15w,
measured 86.5 px, so built `print` and split wrongly, F12-R.9). `modes.decide_mode(line_rows, preset, inv=None)` applies the pin of `inv`: `mode` is
the pin, `method` is `pinned`, and the measured result stays in `box_height_mode` / `box_height_method` (with `value` and
`margin`). The split gate then follows the pinned mode (a `hand` pin: no split, no `split_gate` key). `sg_1.10.94_v4` has no
pins. Files written per inventory under `mode/`: `mode/<inv>.json` (the decision above, `n_scans`, `n_parse_fail`,
`scan_minority_share`), `mode/scans/<inv>.tsv.gz` (per scan: box-height value, chars, regions, `side`), `mode/regions/<inv>.tsv.gz`
(per region: lines, chars, median height, pitch). A forced extract also removes the per-inventory files that a hand build does
not write (`split_model/<inv>.json`, `split_stats/<inv>.json`, `split_stats/<inv>.splits.json`), and the units stage removes
`units/republic_lines/<inv>.tsv.gz` when it writes no crosswalk.

**Pinned volumes (step 15z, P28).** 5 scans of each of 37 volumes outside the printed EAD series, or built the other way round, were
read from the text alone (`reports/data/phase-12-hand-print-box-height/step-15z/verdicts.tsv`, `volumes.tsv`). Rule, fixed before the
reads: pin to the other mode iff at least 4 of the 5 scans have the other class (2 of 2 readable if fewer than 5 are readable);
2 or 3 is a *mixed volume* and is left as built. Pins: to `hand` (5/5 handwritten, built `print` at 74.0-86.5 px): the *tweede serie*
index volumes 3433, 3436, 3439, 3442, 3445, 3463, 3466, 3469, 3472, 3475, 3489, the "Tafel der respecten" 3726, the secrete index 4792
and the systematic index of the commissieboeken 12297; to `print` (5/5 printed, built `hand` at 88.0-89.5 px): the silver-print *plakkaten*
12163 and 12165. Everything else read (20 volumes) stays as built, including the printed pamphlets, extracts and verbals; 4016
(3 printed, 2 handwritten) is the one mixed volume. Known issue: a `hand` pin on an index volume removes the wrong splits of the
merged-line splitter but does not cut at the entries (on the first read scan of the 13 volumes pinned `hand` in 15z, 5 are worse than the print build, 3 mixed, 4 same, 1 better; 3436,
3445, 3466, 3469 and 3472 lose 26-50% of their date-starting units; F12-15y.5, F12-15z.3); entries are merged, not cut in words. The cause is the hand unit rule (one unit per PageXML region), and the hand layout keeps a
region that spans two columns in XML order, so 1.4% of the units of these volumes interleave two columns (0.07% in their print
builds; F12-R.21, F12-R.22). Of the 16 pins, only 3463 is in the published v4 pilot (pinned there since 2026-10-27, UA-038). The unit cut at the entries is built in v5 (see *Text layer v5 (phase 13)*).

### Rebuilding pinned volumes (step 15y)

After adding a pin, rebuild only the pinned volumes of an existing layer, in place:

```
.venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --rebuild 1.01.02=pins.txt \
    --src 1.01.02=<dir with the tars of the pinned volumes and the rest> --out-dir <layer base> --work-dir <work dir>
```

`pins.txt` lists the inventories, one per line. The script runs a forced `--stage extract` of them, then `--stage all` over the
whole layer: layout, types, units, the wbc part and the join are redone for the listed volumes only (their status rows are stale),
the corpus word-break statistics are re-derived from every inventory, and every other per-inventory file is left untouched. Every
volume of the layer needs its source in `--src`: the build lists and sizes all of them (a `.tar.gz` is decompressed to be sized,
about 9 minutes for the 9,122 SG tars; F12-15z.11), and the wbc stage stops if a layer inventory has no source. Only the listed
volumes are parsed. Rerunning a `--rebuild` command after a crash repeats the forced extract of the listed volumes. Check the result with
`step-15v/compare.py` against the layer before (only the listed inventories and the pooled `wbc/` files may differ) and with
the audit.

### Running the full v4 build (step 15w)

`scripts/run_text_layer_v4_full.py` builds both archives (`<out-dir>/1.01.02/` and `<out-dir>/1.10.94/`) from PageXML on
an external drive and audits the result; its docstring lists what to stage. Run from the AS root, one step at a time:

```
.venv/bin/python scripts/run_text_layer_v4_full.py --check-only \
    --src 1.01.02=<1.01.02-dir> \
    --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml \
    --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 \
    --out-dir <drive>/layer
# <1.01.02-dir>: every NL-HaNA_1.01.02_<inv>-pagexml.tar.gz (hand and print mixed); <drive>: the output mount point
# the same with --limit 1 --out-dir <drive>/layer_smoke        (smoke test)
# the same without --check-only                                (the full run; rerun the command to resume)
```

No `--republic-dir` for 1.01.02 (the crosswalk matches 0.5% of its lines); the units stage then logs one REPUBLIC warning for
the print-mode inventories. The audit (`reports/data/phase-12-hand-print-box-height/step-15w/audit.py`) discovers its
inventories and checks the phase-09 invariants plus, for layers with `mode/`, the v4 ones.

## Text layer v5 (phase 13)

v5 of 1.01.02 is v4 plus a hand override. In v4 a `hand` unit is one PageXML region, so on index pages many entries
merged into one unit (F12-R.21, F12-R.22). v5 finds the index page sides and cuts their hand units at the entries.
Preset `sg_1.01.02_v5` (`presets.SG_1_01_02_V5`) differs from `sg_1.01.02_v4` only in `mode_overrides["hand"]`:
`layout_params` = `SG_HAND_LAYOUT_PARAMS` + `"index_sides": SG_INDEX_SIDES_V5` and `unit_params` =
`{"index_entry": SG_INDEX_ENTRY_V5B}`. Print volumes, the mode decision (`volume_mode`, pins) and every other stage are
v4's: all 106 print inventories of the dev build are equal to v4 in every per-inventory file. 1.10.94 has no v5.

**Rules in v5**
- *Index-side detector* (`LayoutParams.index_sides`, `SG_INDEX_SIDES_V5`; step 19c): a page side is an index side if
  `col_share >= 0.25` and `max(end_num, date_start) >= 0.15` (`shape_min` 0.15, no shape-only arm, date pattern
  `SG_INDEX_DATE_START`), then gaps of up to 8 sides between flagged sides are filled (`gap` 8, `min_run` 1). Bars: D1 on
  the GT volumes P 0.9711, R 0.9490; D2 54 of 60 read sides (on its bar). The result is the table `layout/pages/`
  (hand inventories only): columns `scan_id, page, n_lines, n_text8, n_text20, col_share, end_num, date_start, rule,
  index_side`; `rule` = the side rule (1/0), `index_side` = after the gap fill (`"1"` = flagged).
- *Index entry rule v2* (`UnitParams.index_entry`, `SG_INDEX_ENTRY_V5B`; step 19g): on flagged sides a unit is cut at
  the cues `outdent`, `date_start`, `end_ref_prev` (the previous line ends in a page number) and `short_prev`, a line of only
  page numbers and punctuation (`no_cut_pattern`) never starts an entry, and `index`-typed lines join the main lines
  (`main_types`). Unit methods: `hand:index_start` (first unit of a side) and `hand:index:<cue>+<cue>`. On flagged sides
  `index`-typed lines are therefore in `main_text` units, while `types/segments` keeps `line_type` `index`. Bar on 20 fresh
  index sides (read before any code ran): P 0.9530, R 0.8974, F1 0.9244 against 0.5009 for v4 hand units (bar P >= 0.90,
  R >= 0.85: passed).
- *Left out*: 19b's column split (B1 0.763, failed), 19d's rule E5 (entry bar failed), 19f's column reorder (B2 35/39 =
  0.897, failed; UA-043 (a): not shipped; `SG_HAND_COLUMN_ORDER_V5` exists but no preset uses it).

**Build.** `.venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --src 1.01.02=<dir> ...
--work-dir <dir> --out-dir <dir>` (`--version` default `v4`; `ARCHIVES_BY_VERSION`; the throughput constants stay v4's).
Dev build `data/text_layer_v2_dev/19e-a` (231 volumes: 125 hand, 106 print; 4 workers, 60 min).

**v4 to v5 (dev build).** In the 125 hand volumes `lines/`, `scans/`, `regions_meta/`, `mode/`, `extract_stats/`,
`layout/{scans,regions,lines}` and `types/` are equal to v4; new: `layout/pages/`; `units/` and `join/`, `wbc/parts` differ
(the units equal the 19g clones). 8,873 of 115,274 sides are flagged (363,998 of 4,655,693 lines). `main_text`
units on flagged sides double, 80,344 to 162,021; `index` units on flagged sides 2,789 to 0; units elsewhere are unchanged.
Interleaved units: 728 of 481,391 `main_text` units (0.15%) against 2,612 of 399,714 in v4 (0.65%): the entries are cut
before the columns can interleave.

**Known limits.** Entries are cut after an amount or a short cross-reference line (F13-19g.4); entry starts in lines typed
`marginalia` on fragmented sides are in no main unit (F13-19g.3, F13-R.12); a detector false positive cuts that side at entry
cues (a regiment roster with page numbers, 4831: 10 flagged sides; account pages; a garbled scan, 11930); one-column and
garbled indexes are missed (F13-19c.5); a `date_header` unit followed by its entry is one entry; gutter-merged lines remain.

**Published pilot.** 2026-10-29 (UA-046 (a)): `19e-a` copied unchanged (with `status.tsv` and `progress.log`, as v4) to
`shared-data/NL-HaNA_1.01.02/text_layer_v5/` (2.7 GB). The catalogue row lists the known issues, including the 54 false
positive sides of F13-R.13. None of the 23 volumes rebuilt after the `layout/pages` side-order fix (2026-10-29) is in it.

**Full run.** 2026-10-29 (UA-045, 23 volumes rebuilt after the side-order fix): 9,122 volumes done, 39.5 GiB, audit 0
judged failures; kept on the user's external drive. Review files in `data/text_layer_v5_full/`; check in phase-13
`PROGRESS.md` (*Full v5 run checked*). The manifests of `19e-a`, the published v5 pilot and the full run record archival-structures 0.14.1 (stale install
metadata in `.venv`, fixed in step 20r); their `git_head` identifies the code (F14-R.7).

Index entries of the front index are linked to folios and scans by a derived layer built from v5: see `docs/index_links.md`.

## Published SG pilot (text_layer_v2)

Published 2026-10-21 (phase 07, step 15i, from the dev layer `15i-b`) for the sibling projects, `coverage: partial`:

| Path (under `/Users/Shared/claude_projects/data_scopes/shared-data/`) | Content |
|---|---|
| `NL-HaNA_1.01.02/text_layer_v2/` | hand preset, 14 inventories: 3349-3354, 3771-3773, 3821-3823, 11182, 11281 (preset `sg_1.01.02_v2`). 3771-3773 and 3821-3823 are printed volumes built with the hand rules (found 2026-10-23; phase 10) |
| `NL-HaNA_1.10.94/text_layer_v2/` | print, 455 and 456 (preset `sg_1.10.94_v2`) |

**Known issues in `text_layer_v2` (found in the phase-07 review, fixed in the dev layer `15ir-a`, not in these
directories; the full run `text_layer_v3` will carry the fixes; UA-013 (a)):**

1. *F07-R.1, print reading order.* In column-split print regions the lines are in XML order, which interleaves
   the two columns: a unit can read y 2952, 3102, 1371, ... (`455_0010`). 455 has 546 layout regions whose
   non-blank lines jump back more than 50 px (416 `split_at_column`, 127 `unchanged`, 2 `merged+split_at_column`,
   1 `merged`); 335 of 28,079 units in 455 and 160 of 25,030 in 456 contain such a jump. Hand is not affected.
2. *F07-R.2, repeated break mark read as a comma.* In 11182 1,305 of 4,699 merges give "gero,ijeert" (3352 336,
   11281 170, 3772 47, 3822 20, 3353 16, 3350 13, 3823 10; print at most 3). The lower-case share of the right
   side is therefore understated there.
3. *F07-R.3, ids of legacy twins.* A unit with the box and all lines of a legacy region gets `w+1` instead of the
   legacy id, so its `doc_id_map` row says `changed` (455: 45 of 58 `n_id_collisions`, 456: all 32).
4. *F07-R.4, types.* A whole date line with a year ("28. Junij 1650.") is split by `rule:date_prefix` (19 lines:
   11281 10, 3772 5, 3771 2, 3353 1, 455 1), and `rule:pn_line` is relabelled `_offchain` only for the candidate
   line of an off-chain page, so 1,501 `rule:pn_line` segments that were not the in-chain candidate line (in the
   sample read in step 15ir 18 of 20 were bottom-of-page digits or other non-page-numbers) still say
   `rule:pn_line` (10,327 in the pilot; `_offchain` 1,792; the fix gives 8,826 and 3,293). Their `line_type`
   is `page_number` in both layers.
5. Smaller: `n_guard_blocked` / `n_column_blocked` count a blocked relation again under each merged id (455 1,322 /
   62; 456 673 / 113); `n_offset_violations` compared a prefix of the span only.

**Status of the known issues in the v3 code (phase 09, details in *Text layer v3*):** 1 (print reading order) is fixed by
the row-tolerant sort of step 15j (jump regions 130 to 0, same-row swaps 600 to 13 in 455); 2 (repeated break mark) by the
15ir fix, extended in 15l to `, ` / `,,` (hand) and the `J` mark in 455; 3 (legacy-twin ids) and 5 (counters) by the 15ir
fixes already in `15ir-a`; 4 (types) by 15ir and 15k (`rule:pn_line_gap`, `rule:year_offchain`, print-bottom numbers
`other`). Still open: the four lower-case merge shares below 0.95, glued words (UA-003).

Tables and keys are those of the sections *Layout tables*, *Type tables*, *Units and the doc_id mapping* and *Unit text and
offsets* above; `manifest.json` has a `publication` key with the producer, `as_version`, `git_head`, the audit summary (176 of 182
invariants; the 6 failures are the reported lower-case merge share below 0.95 in 3351, 3823, 11182, 11281, 455, 456), the counts per
inventory, the sources with sha256 (tar/zip) or path and file count (directories) and the known gaps. Known gaps: `stages.*.argv`
and the `member`/`source` columns name dev paths (`data/text_layer_v2_dev/15i-src/...`); glued words are not repaired (UA-003); the
build ran on a tree with uncommitted changes (`as_version` 0.9.0 as recorded). The full SG run will be `text_layer_v3`; a change is a
new version, never a rebuild into these directories. Catalogue: shared-data `README.md`; consumer note: LotA
`docs/from-archival-structures.md` section 6. The audit (`reports/data/phase-07-sg-units-and-pilot/step-15i/audit.py --layer <base>`)
expects `<base>/{hand,print}/*_text_layer_v2`, so audit the published copies through symlinks with that suffix.

## Text layer v3 (phase 09)

`text_layer_v3` is the full SG run with the phase-07 review fixes (15ir) and the phase-09 follow-ups
(15j-15l). Nothing is published yet (user decision P1): the dev layer `data/text_layer_v2_dev/15m-a`
(hand 14 inventories, print 455 and 456) is the reference; the pilot in `shared-data/*/text_layer_v2` stays as it is.

**Presets.** `sg_1.01.02_v3` and `sg_1.10.94_v3` (`SG_HAND_V3`, `SG_PRINT_V3`) equal the v2 presets in every field
but `name` (`NL-HaNA_1.01.02_text_layer_v3`, `NL-HaNA_1.10.94_text_layer_v3`) and `description`
(`test_v3_presets_equal_v2_but_name`). `format_version` stays 2: v3 is the v2 code path with the fixes below, and the
v2 presets stay for reproducing the pilot. The built layer `15m-a` equals the chained layer `15l-a` in every table
(`compare.py 15l-a 15m-a`: hand 237 and print 51 tables equal, 0 differ); only the layer directory name,
`manifest.json`, `progress.log`, `status.tsv` and the `seconds` values in `*/stats.json` differ.

**Output changes against `text_layer_v2` (dev layer `15ir-a` = the pilot code with the review fixes), by stage.**
Numbers are `15ir-a` to `15m-a`; the full table is in the phase report, *Step 15m*.

| Stage | Change | Step | Numbers |
|---|---|---|---|
| `layout` | Lines of merged regions, column-split pieces and (print) `unchanged` regions are sorted by printed row, not by baseline y alone (`line_sort=row`); `lines_resorted` marks regions whose order changed. Hand keeps the XML order | 15j | print same-row swap pairs 455 600 to 13, 456 1,382 to 14; regions with a > 50 px backward jump 130 to 0 (455 `unchanged`); 189 regions resorted (455 182, 456 7) |
| `types` | A page-number-shaped band line between the chain numbers of its neighbours stays `page_number`, method `rule:pn_line_gap` | 15k | 188 segments (hand 184, print 4) |
| `types` | Off-chain page-number-shaped segments in the print bottom band are `other` (method stays `rule:pn_line_offchain`); off-chain years are `other`, method `rule:year_offchain` | 15k | print 367 (366 in the bottom band, 1 in the top band); years hand 34, print 3. Hand top/bottom and print top stay `page_number` (bars not met: 6, 12 and 3 of 20 not page numbers) |
| `units` | `unit_type` follows the retyped segments; `units/republic_lines.role` and the `end` rule maps a split REPUBLIC line that ends a paragraph to the last half | 15k | units 455 28,005 to 27,956, 456 24,710 to 24,514 (hand 181,455 to 181,456); `split_original` ids with role `end` moved to the last half 513 (455 135, 456 378); `n_id_collisions` 0 and 0 |
| `units` | `claimable` is built per scan | 15k | 455 units stage 9.7 s to 4.2 s, output identical |
| `join` | `join_lead_marks` strips `, ` and `,,` as well as `,`: "fue,rant" becomes "fuerant" | 15l | hand `comma_sp` 65 + `comma2` 5 to 0; read 67 of 70 as the repeated break mark |
| `join` | `join_lead_marks_lower={"455": "J"}` in `SG_PRINT_V3`: a `J` before a lower-case letter after a hyphen is the repeated hyphen, dropped | 15l | 455 `J`+lower merges 2,175 to 0 (30 of 30 read), lower share 0.892 to 0.948; 456 unpinned (2 of 32) |
| `join` | **Not adopted:** the upper-case guard (`join_upper_guard`). It was wrong on 24 of 40 (hand) and 20 of 40 (print) merges read, bar 27, so it stays `False` in both presets and no layer carries the boundary kind `g_upper_right` (0 merges) | 15l | upper-case merges unchanged (hand 4,219 to 4,222, print 4,447 to 4,447) |

Merge counts for the other join effects: hand merges 215,277 and print 79,645 to 79,879 (the row sort changed which lines
meet; 455 38,965 to 38,981, 456 40,680 to 40,898).

**Schema additions.** `layout/regions/<inv>`: `lines_resorted`; `layout/scans/<inv>`: `n_regions_resorted`;
`units/republic_lines/<inv>`: `role` (`start`/`end`/`both`); `types/segments` `type_method` values `rule:pn_line_gap`
and `rule:year_offchain`; `line_type` `other` for the off-chain print-bottom numbers and for off-chain years;
`join/stats/<inv>.json`: `lead_marks_lower` and `upper_guard`. The boundary kind `g_upper_right` exists in the code
and is never written by the v3 presets (guard off). Readers of v2 tables that ignore unknown columns are unaffected;
a reader that counts `line_type` `page_number` sees fewer (print bottom 367, years 37).

**Print builds need `--republic-dir`.** Without it a print build logs a warning and writes no `units/republic_lines/`
(step 15k removed the default path). Point it at REPUBLIC's `paragraphs-<inv>.tsv.gz` (today only in LotA
`data/paragraphs/NL-HaNA_1.10.94/`; UA-015 asks for a `shared-data` dataset). Since 2026-10-28 `shared-data/NL-HaNA_1.10.94/
republic_segmentation_v1/` (republic-segmentation; see `docs/from-republic-segmentation.md`) has paragraphs of 455 and 456
with first and last line ids, as Parquet. `--republic-dir` does not read it yet: it is another segmentation (C07d.1, with
new `rs1-` ids where units changed) than the 2026-03 export the crosswalk was checked on, and its line ids are not yet
checked against the `<scan_id>-line-x-y-w-h` form. Like every REPUBLIC label, it is for scoring and crosswalks, never a
feature.

**Quality against the pilot code (`15ir-a` to `15m-a`).** The audit passes 178 of 182 invariants in both; the same four
lower-case merge shares stay below 0.95 (3351 0.9415, 3823 0.9414, 455 0.9477, 456 0.932; 455 was 0.892). The 116
labelled rows of LotA's 02d gold sheet predict the same share free 0.526 and bound 0.560: 8 rows change cells (a stray
`J` gone, a reading order restored) but no hand-read verdict changes.

**Full-run commands** (run from the AS root; stage the inputs first, UA-015; `--estimate` first, it prints the
PageXML size, the throughput fit for the presets is not set so it gives no time):

```
D=/Users/Shared/claude_projects/data_scopes/shared-data
R=<staged REPUBLIC paragraphs dir, with paragraphs-<inv>.tsv.gz for every print inventory>
mkdir -p data/text_layer_v3_full/hand data/text_layer_v3_full/print
.venv/bin/as-build-text-layer --pagexml-dir $D/NL-HaNA_1.01.02/pagexml --out-dir data/text_layer_v3_full/hand --preset sg_1.01.02_v3 --stage all --estimate --workers 8
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label v3-hand --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir $D/NL-HaNA_1.01.02/pagexml --out-dir data/text_layer_v3_full/hand --preset sg_1.01.02_v3 --stage all --workers {workers}
.venv/bin/as-build-text-layer --pagexml-dir $D/NL-HaNA_1.10.94/pagexml --out-dir data/text_layer_v3_full/print --preset sg_1.10.94_v3 --stage all --republic-dir $R --estimate --workers 8
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label v3-print --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir $D/NL-HaNA_1.10.94/pagexml --out-dir data/text_layer_v3_full/print --preset sg_1.10.94_v3 --stage all --republic-dir $R --workers {workers}
```

Both were smoke-tested with `--limit 1` on the staged shared-data sources (hand: 3349, print: 455) into
`data/text_layer_v2_dev/15m-smoke/`, exit 0. The shared-data hand directory holds 6 inventory tars; the other 8 pilot
inventories (3771-3773, 3821-3823, 11182, 11281) are in `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02` and have to be
staged under `shared-data/NL-HaNA_1.01.02/pagexml/` for the full hand run (UA-015). After the build run
`reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py --layer data/text_layer_v3_full/` on the base directory.

**Running on an external drive.** `scripts/run_text_layer_v3_full.py` (step 15n) wraps the commands above for inputs and
output that live on a drive: it links the PageXML of several `--hand-src` / `--print-src` dirs into one local input dir
(first source wins), checks the dirs, the subset files, the REPUBLIC coverage and the free space (the output is predicted
from the pilot ratios; it stops below 1.2 times that), runs `--estimate` and the build through `coreshare`, then the audit.
Its docstring is the manual: run it with `--check-only`, then with `--limit 1 --out-dir <other dir>`, then without; after an
interruption rerun the same command. The log is `<out-dir>/run_v3.log`. On the local pilot sources it reproduces `15m-a`
(`15n-a`).

## Published SG pilot (text_layer_v3)

Published 2026-10-23 (phase 09, step 15o) from the dev layer `data/text_layer_v2_dev/15n-a` (built from git head
`fa5cc63`, v0.14.0, by `scripts/run_text_layer_v3_full.py`; equal to `15m-a` up to path columns), `coverage: partial`:

| Path (under `/Users/Shared/claude_projects/data_scopes/shared-data/`) | Content |
|---|---|
| `NL-HaNA_1.01.02/text_layer_v3/` | hand preset, 14 inventories: 3349-3354, 3771-3773, 3821-3823, 11182, 11281 (preset `sg_1.01.02_v3`). 3771-3773 and 3821-3823 are printed volumes built with the hand rules (found 2026-10-23; phase 10) |
| `NL-HaNA_1.10.94/text_layer_v3/` | print, 455 and 456 (preset `sg_1.10.94_v3`) |

Without `progress.log` and `status.tsv`; `manifest.json` has a `publication` key (sources and sha256, audit 178/182,
counts, 116-row prediction, known gaps). Audit through symlinks:
`data/text_layer_v2_dev/15o-pub/{hand,print}`; result in `reports/data/phase-09-text-layer-v3-prep/step-15o/audit_published.json`.

**Known gaps.** `stages.*.argv`, `scans.member` and `extract_stats` `source` name dev paths
(`data/text_layer_v3_full_inputs/...`); the stage `version` field says 0.13.0 (stale install metadata); four merge shares
below 0.95 (3351, 3823, 455, 456; reported); glued words are not repaired (UA-003); print REPUBLIC paragraphs come from
LotA `data/paragraphs/`; pilot inventories only, a full SG run will be a new version. What changed since v2 is in
*Text layer v3 (phase 09)* below.

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

## Known pagexml-tools 0.9.1 limits

- Nested regions parse since 0.9.1 (they raised `TypeError` in `get_all_text_regions` on 0.8.1 and 0.9.0, which skipped
  the scan). Fixture `tests/fixtures/text_layer/pagexml/9999A/NL-HaNA_1.04.02_9999A_0003.xml` now gives regions `n1` and
  `n1a` (`extract_stats` counts `nested_text_regions` 1). Real data: 0 scans of the v5 full run (9,122 volumes) and of the
  dev build `19e-a` failed on nested regions (their only parse errors are 6 METS `doc.xml` files), and the step 20a
  regression build of 1.01.02 on 0.9.1 gives byte-identical tables. Re-checked by
  `reports/data/phase-14-index-links/step-20a/known_limits_check.py`.
- A region with neither `Coords` nor lines is dropped.
