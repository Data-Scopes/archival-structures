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
`--allow-partial`, `--ignore-sanity`, `--wbc-stats-only` and `--write-events` are described in
`--help`. The driver's module docstring has the full handoff text for a run on another machine,
including expected runtime and output size. Run AS's `tests/test_text_layer_*.py` first.

**Never build into an existing shared layer with a subset of its inventories.** The `wbc`
stage (also part of `--stage all`) re-derives `wbc/corpus_default.json` and
`wbc/wbc_per_inventory.tsv` from the inventories of *this run* only, and overwrites them.
`join` then rewrites every region file whose word-break set changed. Build a sample into a
fresh `--out-dir`, and copy the layer's `wbc/` decision files in (as in *Checking a rebuild
against v1*) if the sample must use them.

## v2 (SG presets)

The presets `sg_1.01.02_v2` (States General hand volumes) and `sg_1.10.94_v2` (print volumes)
build a **v2 layer** (`format_version` 2): one row per scan, per region and per text line, with
geometry. Only the `extract` stage exists for v2 (`--stage all` means extract; `wbc` and `join`
exit with status 2 until phase 07). Rows are in document order (scans by file name, regions
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

### Layout tables (step 15fp)

`as-build-text-layer --preset P --out-dir D --stage layout` (v2 presets; no `--pagexml-dir`;
not part of `--stage all`; `--layout-params` takes JSON overriding `LayoutParams`) reads
`scans/`, `regions_meta/` and `lines/` and writes `layout/`. The input tables are never
rewritten; run it on a copy of the layer. Per scan (`text_layer.layout`, on
`analysis.page_structure`):

1. Regions are rebuilt from their lines (`line_idx` order); their coords are the hull of the
   line boxes, not the `regions_meta` box. A region with no line with text gets role `empty`.
2. Page split. A scan is an opening iff `page_w > portrait_ratio · page_h`, else one page
   `single`. `gutter_rel` is fitted once per inventory: the median centre/`page_w` of the widest
   zero-coverage gap (≥ `min_gap_px`) of line x-extents in [0.35, 0.65]·`page_w`. Per scan,
   `separation_x` is the centre of the gap nearest `gutter_rel·page_w` within
   ±`gutter_window_frac·page_w` (`gap`), else the x of minimum coverage there (`min_coverage`).
   Lines go to `verso`/`recto` by box centre; a region with lines on both sides is split.
3. Per page, regions that overlap (`regions_overlap`, `overlap_threshold`) are merged until no
   pair overlaps. Merged lines are sorted by (baseline mean y, box left). An unmerged, unsplit
   region keeps its `region_id`; any other gets `legacy_doc_id(scan_id, hull box)`.
4. Columns: regions sorted by left join the first column where
   `min(ov/w_region, ov/w_column) > col_min_ratio` and the max `> col_max_ratio`. A column is
   `main` when its characters are ≥ `main_min_char_frac` × those of the page's largest column,
   else `extra`.

Defaults: `overlap_threshold=0.5`, `col_min_ratio=0.5`, `col_max_ratio=0.75`,
`main_min_char_frac=0.25`, `min_gap_px=20`, `gutter_window_frac=0.15`, `portrait_ratio=1.0`.

| Table | One row per | Columns |
|---|---|---|
| `layout/scans/<inv>.tsv.gz` | scan | `scan_id, page_mode` (`opening`/`single`), `separation_x` (empty for `single`), `split_method` (`gap`/`min_coverage`/`none`; `none` for single pages and openings without text), `n_lines_crossing` (lines whose box contains `separation_x`), `n_regions_in`, `n_regions_out` (non-empty regions only), `n_merges` (output regions that are merges), `n_gutter_splits` (input regions split at the gutter) |
| `layout/regions/<inv>.tsv.gz` | layout region, and empty source region | `scan_id, page` (`verso`/`recto`/`single`), `layout_region_id, x, y, w, h` (hull of the lines; the `regions_meta` box for `empty`), `layout_column` (0… by left edge per page; empty for `empty`), `layout_role` (`main`/`extra`/`empty`), `n_lines, source_region_ids` (space-separated, input order), `change` (`unchanged`/`merged`/`split_at_gutter`/`merged+split`) |
| `layout/lines/<inv>.tsv.gz` | input line | `scan_id, line_id, region_id` (input), `page, layout_region_id, layout_line_idx, layout_column, layout_role` |

Rows of `layout/regions/` are per scan, per page, by column then top, with the empty regions
last. `layout/stats.json` holds per inventory the summed counts, `split_method` and
`layout_role` counts, `gutter_rel`, `n_scans_with_gap`, `n_landscape_scans` and
`max_merge_passes`; `layout/params.json` the `LayoutParams` used.

Known limits (phase 07 report, step 15fp): in print (455, 456) most Laypa regions span both text
columns, so a layout region is a page-level block whose lines interleave the two columns; use
the line `column` field to separate them. In hand 3349–3353 and 3350 above all, the split can
cut off the recto's margin column (its notes go to the verso), and `min_coverage` sometimes
misses the gutter by a whole page.

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
