# Step 15d: v2 extract — lines with geometry, scans, legacy `doc_id`

> **Phase:** 06, Text layer v2. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md`. If you have to explore anyway, record it under *Brief defects*.

## Context

LotA needs per-line geometry for the States General (SG) hand and print volumes. Today's SG
TSVs keep one row per region and drop line boxes, page size and empty scans. This step adds a
**v2 extract** to `archival_structures.text_layer` that writes three tables per inventory:
- one row per scan, including scans without text;
- one row per region, carrying the legacy `doc_id`;
- one row per text line, with box, baseline and confidence.

It also adds two SG presets and lets the build CLI run them. Later steps plug into it:
- 15e splits merged print lines through a `scan_hook`;
- 15f repairs glued words in the `text` column.

v1 (`voc_v1`) must stay byte-identical.

## Inputs

All read only. Never write under `/Users/Shared/claude_projects/data_scopes/` (protected).

| What | Path | Notes |
|---|---|---|
| Hand PageXML | `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_{3349..3354}-pagexml.tar.gz` | members `<inv>/page/NL-HaNA_1.01.02_<inv>_NNNN.xml`, plus **AppleDouble `._*.xml` members (binary; skip them)** |
| Print PageXML | `…/shared-data/NL-HaNA_1.10.94/pagexml/NL-HaNA_1.10.94_{455,456}.zip` | 721 / 730 xml, plus `__MACOSX/` entries (skip) |
| AS-local hand/print | `data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_{3771,3772,3773,3821,3822,3823,11182,11281}/` | directories of `.xml` (3771–3823 are printed volumes) |
| Legacy TSVs | `…/shared-data/NL-HaNA_1.01.02/paragraphs/source/paragraphs-NL-HaNA_1.01.02_<inv>.tsv.gz` | header `archive_id inv_num scan_id doc_id region_type text`; QUOTE_NONE |
| Scan stats | `data/scan_stats-1.01.02/` | inspect its header; use it for the coverage check if it has the inventories |
| v1 reference | `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/output/voc_text_layer/voc_text_layer_v1/` and NLP `pagexml/` | for the v1 regression only |

Existing AS code you extend:
- `archival_structures/text_layer/io.py`:
  - `iter_pagexml(source)` (L146–160; read it): sorted by base file name, from a zip or `rglob("*.xml")`;
  - `discover_inventories(pagexml_dir)` (L114);
  - `sanitise(text, counter)` (L32);
  - `write_tsv_gz(path, header, rows)` (L58; deterministic, atomic);
  - `write_json_atomic` (L96).
- `text_layer/presets.py`: `@dataclass(frozen=True) class LayerPreset` with the fields `name`,
  `untagged_tag`, `scan_id_prefix`, `throughput_fit=None`, `output_bytes_per_xml_byte=None`,
  `description=""`; `VOC_V1`; `PRESETS = {"voc_v1": VOC_V1}`.
- `text_layer/extract.py`: `iter_text_regions(regions, depth=0)` (L31, depth-first over
  `region.text_regions`), `extract_inventory` (L63; the v1 path, don't change).
- `text_layer/join.py`: `library_join(texts, wbcs)` (L27) calls pagexml `make_text_region_text`.
- `text_layer/build.py`:
  - `STAGES = ("extract","wbc","join")`;
  - `stage_extract` (L243);
  - `main(argv=None)` (L386), with the flags `--pagexml-dir --out-dir --preset --stage
    --inventories --limit --workers --force …`.
- `archival_structures/model/scan_id.py`: `ScanId.try_parse(scan_id, digits=None, prefix=…)` gives
  a `ScanId` with `.inventory` (an `InventoryNumber`; check that `str()` gives `"3349"`) and
  `.number`, or `None`.
- pagexml-tools 0.9.0 (verified 2026-10-17 on `3349_0100`):

  | Value | How to get it |
  |---|---|
  | parse | `pagexml.parser.parse_pagexml_file(name, pagexml_data=text)` |
  | scan id | `scan.id` = `NL-HaNA_1.01.02_3349_0100.jpg`, so strip `.jpg` |
  | page size | `scan.coords.width/height` = Page@imageWidth/Height (5947×4127) |
  | region reading order | `region.metadata.get("reading_order", {}).get("index")` |
  | region type | `region.metadata.get("structure")` and `region.metadata.get("type")` |
  | line confidence | `line.conf` (float or None) |
  | baseline | `line.baseline.points`, a list of (x, y) |
  | boxes | `line.coords.box` and `region.coords.box` = dict `x,y,w,h` |
  | words | `line.words` |

## Deliverables

| File | Change |
|---|---|
| `text_layer/io.py` | `iter_pagexml(source, *, skip_resource_forks: bool = False)`: adds `.tar.gz`/`.tgz` sources (members sorted like zip members). When the flag is True, skip members whose path contains `__MACOSX/` or whose basename starts with `._`. The default keeps v1 behaviour exactly. New `discover_inventories_v2(pagexml_dir, pattern: str) -> dict[str, Path]`: a regex with a named group `inv`, matched with `fullmatch` against entry names (files and dirs); sorted with `inventory_sort_key` |
| `text_layer/presets.py` | New `LayerPreset` fields with defaults that leave `VOC_V1` unchanged: `format_version: int = 1`, `archive_id: Optional[str] = None`, `source_pattern: Optional[str] = None`, `skip_resource_forks: bool = False`, `merged_line_split: str = "off"`. New presets `SG_HAND_V2` (key `"sg_1.01.02_v2"`, name `"NL-HaNA_1.01.02_text_layer_v2"`) and `SG_PRINT_V2` (key `"sg_1.10.94_v2"`, name `"NL-HaNA_1.10.94_text_layer_v2"`), see *Behaviour* |
| `text_layer/extract_v2.py` (new) | `SCAN_COLUMNS_V2`, `REGION_COLUMNS_V2`, `LINE_COLUMNS_V2`; `legacy_doc_id(scan_id, box) -> str`; `legacy_region_text(line_texts) -> str`; `extract_scan_v2(scan, *, inv, inv_dir, member, preset) -> tuple[dict, list[dict], list[dict]]`; `extract_inventory_v2(inv, source, out_root, preset, *, scan_hook=None) -> dict` |
| `text_layer/build.py` | If `preset.format_version == 2`: discovery uses `discover_inventories_v2(dir, preset.source_pattern)`, and the extract stage calls `extract_inventory_v2`. The stages `wbc` and `join` exit with status 2 and the message `"stage <s> is not available for v2 presets yet (phase 07)"`. `--stage all` with a v2 preset runs extract only. The manifest records `format_version`. v1 code paths unchanged |
| `tests/fixtures/text_layer/sg/9999/` | 4 small synthetic PageXML files with the SG prefix `NL-HaNA_1.01.02_9999_000N.xml`; see *Tests* |
| `tests/test_text_layer_extract_v2.py` (new) | tests listed below |
| `docs/text_layer.md` | a `## v2 (SG presets)` section: the three schemas, presets, CLI usage, "document order; reading-order indices in columns" |
| `docs/shared_library/changelog.md` | `### Step 15d: text layer v2 extract` under *Unreleased* (create the heading if missing), added with Edit |
| `reports/data/phase-06-text-layer-v2-sg/step-15d/` | `legacy_check.py`, `ea.py` (the scripts behind the numbers) |
| `reports/phase-06-text-layer-v2-sg.md` | your `### Step 15d` section |

## Behaviour to implement

**Presets.**
- `SG_HAND_V2`:
  - `untagged_tag="untagged"`, `scan_id_prefix="NL-HaNA_1.01.02"`, `archive_id="NL-HaNA_1.01.02"`;
  - `format_version=2`, `skip_resource_forks=True`, `merged_line_split="off"`;
  - `source_pattern=r"NL-HaNA_1\.01\.02_(?P<inv>[0-9]+[A-Z]*)(?:-pagexml)?(?:\.zip|\.tar\.gz|\.tgz)?"`.
- `SG_PRINT_V2`: the same, but with `1.10.94` everywhere and `merged_line_split="on"`. The flag
  is used from 15e on.
- Register both in `PRESETS`.

**`legacy_doc_id(scan_id, box)`** = `f"{scan_id}-text_region-{box['x']}-{box['y']}-{box['w']}-{box['h']}"`.
**`legacy_region_text(line_texts)`** = `library_join([t for t in line_texts if t], ["-", "„"])`
(the text of the old TSVs; `""` if no line has text).

**`SCAN_COLUMNS_V2`** = `scan_id archive_id inv_num inv_dir scan_num scan_variant scan_id_ok member
status page_w page_h n_regions n_regions_text n_lines n_lines_text n_words`.
- `scan_id`: `scan.id` without `.jpg`. For a parse error, use the member basename without
  `.xml`.
- `inv_num`: `str(ScanId.try_parse(scan_id, digits=None, prefix=preset.scan_id_prefix).inventory)`,
  else `inv_dir`. `scan_id_ok` is `1` or `0`. `scan_num` is the number, or empty.
- `scan_variant`: `deelopnameN` if `scan_id` matches `_deelopname(\d+)$`, else empty.
- `status`, checked in this order:
  1. `parse_error`;
  2. `partial_shot_dropped` (a deelopname scan; it gets no region or line rows);
  3. `no_regions` (0 regions, walked recursively);
  4. `no_words` (regions, but no line with non-empty text);
  5. `ok`.
- `member`: the member path inside the archive, or the file name for a dir.
- The counts are over recursive regions and all lines. `n_words` is `len(line.words)`.

**`REGION_COLUMNS_V2`** = `scan_id page_side region_id region_xml_id parent_region_xml_id region_tag
region_ro_index x y w h n_lines n_lines_text`.
- `region_id` = `legacy_doc_id`.
- `region_tag` follows v1's rule: the `type` if `metadata.get("structure")`, else
  `preset.untagged_tag`.
- `region_ro_index` is the index, or empty.
- `page_side` is `left` if the region's centre x < `page_w / 2`, else `right`.
- **Keep regions with 0 lines** (the print has thousands).

**`LINE_COLUMNS_V2`** = `scan_id page_side column region_id line_idx line_id x y w h baseline conf
text_raw text edit_flags split_from`.
- `line_idx`: 0-based, in document order within the region, **including empty lines**.
- `line_id`: TextLine `@id`.
- `baseline`: `" ".join(f"{x},{y}")`, or empty.
- `conf`: `format(conf, ".6g")`, or empty.
- `text_raw`: `sanitise(line.text)` (`""` for None).
- `text` = `text_raw`.
- `column`, `edit_flags`, `split_from`: take them from `line.metadata.get(<name>, "")`. So 15e's
  `scan_hook` can set them; in 15d they are empty.
- `page_side`: by the line's centre x.

**Order and output.**
- Rows go in member order, then recursive region order (`iter_text_regions`), then line order.
  This is document order.
- `extract_inventory_v2` writes:
  - `scans/<inv>.scans.tsv.gz`;
  - `regions_meta/<inv>.tsv.gz`;
  - `lines/<inv>.lines.tsv.gz`;
  - `extract_stats/<inv>.json`, with counts per status, the regions/lines totals, the sanitise
    counts, `source` (path string) and `source_sha256` (of the archive file; `null` for a dir).
- `scan_hook(scan) -> scan` is applied after parsing and before rows are made. It is not
  applied to dropped or failed scans.
- Two runs give byte-identical files.

## Constraints

- Never `git commit`; never bump the version; `chmod g+w` everything you create.
- **Protected:** everything under `/Users/Shared/claude_projects/data_scopes/` and the v1
  reference. Before the first run, record sha256 and mtime of the 8 shared-data PageXML archives
  and of the 14 legacy TSVs you read. Check them again at the end and report "unchanged".
- Outputs go only to `data/text_layer_v2_dev/15d-<run>/` (a new dir per run; `rm -rf` is denied)
  or to pytest's `tmp_path`.
- Don't change `extract.py`, `join.py`, `word_breaks.py` or the v1 behaviour of `io.py`/`build.py`.
- Import pagexml lazily inside functions, as the other modules do.
- Agent shell: `nohup`/`&` fail, so run in the foreground. Install with
  `.venv/bin/python -m pip`, then `chmod g+w`.

## Tests

Fixture (synthetic, hand-written, <60 lines each):
- `_0001`: 2 regions, one with `custom="readingOrder {index:1;} structure {type:Text;}"`. 3
  lines with `Baseline`, `TextEquiv@conf` and `Word` elements; one line ends in `„`. Plus one
  region with no lines.
- `_0002`: no regions.
- `_0003`: a region whose lines have empty text (`no_words`).
- `NL-HaNA_1.01.02_9999_0004_deelopname1.xml`: a partial shot.

The tests build tar.gz and zip versions at run time in `tmp_path`, adding a `._` member and
`__MACOSX/` junk.

Tests in `tests/test_text_layer_extract_v2.py`:
- `test_iter_pagexml_targz_matches_dir`: same names, same order.
- `test_skip_resource_forks`: skipped with the flag on; with the default, the yield is unchanged.
- `test_discover_inventories_v2`: tar, zip and dir names; non-matching entries ignored.
- `test_legacy_doc_id` and `test_legacy_region_text`: the `„` join, no `-`/`„` at a region end,
  and empty lines ignored.
- `test_extract_scan_v2_rows`: columns, box, baseline string, `conf` format, RO index,
  `page_side`, the empty region kept.
- `test_scan_status_values`: covers all five statuses (make a broken XML in `tmp_path` for
  `parse_error`).
- `test_extract_inventory_v2_deterministic`: two runs are byte-identical.
- `test_scan_hook_metadata_propagates`.
- `test_presets_voc_unchanged`: the `VOC_V1` field values, and `format_version == 1`.
- `test_build_v2_cli_extract_and_refuses_join`.

Commands:
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_model_scan_id.py`
- the full suite with `--continue-on-collection-errors`. The baseline is 799 passed, 2 skipped and
  6 collection errors (matplotlib/ipywidgets); the new tests add to the passed count.

## Acceptance criteria

1. **Legacy regression** (`legacy_check.py <run dir>`) on the 14 inventories that have a TSV
   (3349–3354, 3771–3773, 3821–3823, 11182, 11281):
   - the regions with ≥1 non-empty line form exactly the TSV `doc_id` set;
   - `legacy_region_text` equals the TSV `text` for **100%**;
   - planning measured 100% on 3349, 3351, 3353 and 3771.
   Report per inventory: TSV rows, matched, text-equal.
2. **Uniqueness:** `region_id` is unique per inventory, and `line_id` unique per inventory.
   Report the duplicates if any.
3. **Coverage:** for each inventory, the scan rows (all statuses) equal the number of
   non-junk XML members. Report the status counts per inventory. If `data/scan_stats-1.01.02`
   covers an inventory, its scan set equals ours minus `partial_shot_dropped`; report the
   differences.
4. **v1 unchanged:** run from the AS root:
   ```
   NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
   S=data/text_layer_v2_dev/15d-v1check; mkdir -p $S
   ls $NLP/output/voc_text_layer/voc_text_layer_v1/lines/ | sed 's/\.lines\.tsv\.gz$//' | paste -sd, - > $S/inv22.txt
   .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $S/r22 --stage all --inventories $(cat $S/inv22.txt) --write-events --workers 8
   .venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py $S
   ```
   This must print `TOTAL 157 / 157` (phase 05 reproduced it the same way).
5. **The pilot extract runs** with the CLI and `--workers 8` into
   `data/text_layer_v2_dev/15d-a/`:
   - `hand_tar/`: `--preset sg_1.01.02_v2`, `--pagexml-dir` the shared-data hand dir;
   - `hand_local/`: the same preset, the AS-local dir;
   - `print/`: `--preset sg_1.10.94_v2`, the shared-data print dir.

   All 16 inventories finish with exit 0. Record the wall time.
6. AS tests green, as above.

The user decides when the step is done.

## Error analysis

Write it into the report.
- **Containment:** count lines whose box exceeds their region box by >5 px on any side, and
  baselines outside their line box by >5 px, per inventory. Read 10 of each (seed 15, or all if
  fewer) and classify them: region box derived from lines, rotated scan, other.
- **Empty structure:**
  - status counts per inventory;
  - print regions with 0 lines: count, region tags, and the median box size. Read 10 at random
    (seed 15) and say what they are.
- **Page side:** the share of lines per side per inventory. List the scans whose lines all sit
  on one side (single-page scans, covers) and read 10 of their first lines.
- **Expected findings:**
  - F06-15d.1 the legacy regression;
  - F06-15d.2 empty scans and regions per inventory;
  - F06-15d.3 containment;
  - F06-15d.4 v1 unchanged.

## Do not touch

`text_layer/extract.py`, `join.py`, `word_breaks.py`, `compare.py`; the sibling repos; anything
under `shared-data/`; `make_iventory_paragraphs.py`.

## When done

1. Add `### Step 15d: v2 extract` to `reports/phase-06-text-layer-v2-sg.md` (results, error
   analysis, findings `F06-15d.n` as `draft`), and add a *Reproduce* line. Run `make reports` and
   fix any warning.
2. Append the block below to `plan/phase-06-text-layer-v2-sg/PROGRESS.md`.
3. Overwrite `STATUS.md`. Put the run-dir paths, the wall time and any schema change under *Facts
   later steps need*.

```markdown
### Step 15d: v2 extract
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures>
- **Tests:** <command> → <result>
- **Acceptance:** 1 … 6 with numbers
- **Report:** [`reports/phase-06-text-layer-v2-sg.md` § Step 15d](../../reports/phase-06-text-layer-v2-sg.md#step-15d-v2-extract); findings F06-15d.1–n
- **Error analysis:** <two lines>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
