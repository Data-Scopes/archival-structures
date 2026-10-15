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
`--estimate`. The only preset is `voc_v1` (`--preset voc_v1`, the default): GLOBALISE VOC
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
