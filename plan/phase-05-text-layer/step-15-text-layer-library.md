# Step 15: text-layer extract, join, provenance and presets (C15 library)

> **Phase:** 05, Text layer. **Depends on:** 14.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Step 14 created `archival_structures/text_layer/` with `io.py` and `word_breaks.py`. This step
ports the other stages of NLP's producer as library functions: extract (PageXML → `lines/`,
`regions_meta/`, `extract_stats/`) and join (→ `regions/`, `join_stats/`, optional
`join_events/`). It also adds provenance helpers and a VOC preset. The resumable driver and CLI
are step 15c. **Port verbatim: the acceptance is byte identity.**

The reference is **not v1**. v1 was built from GLOBALISE PageXML v2.1, which exists only on an
external disk. NLP's local zips are a 2023 export with the same lines and texts, but the lines
come in a different order. The reference is NLP's own 22-inventory build from the local zips,
made by the same code. In planning (2026-10-14), NLP's module under AS's `.venv` reproduced it:
157/157 files identical.

## Inputs

- `<NLP>` = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`. Port from
  `<NLP>/src/startpage/voc_text_layer.py` (879 lines; ranges checked 2026-10-14):
  - L37–52 `LAYER_NAME`, `MIN_PAGEXML_TOOLS`, the three `*_COLUMNS`, `UNTAGGED_TAG`,
    `ROW_ORDER_STATEMENT`;
  - L106–131 `pagexml_tools_info`, `check_pagexml_tools_version`;
  - L273–363 `iter_text_regions`, `region_tag_of`, `extract_inventory` (the `_{inv}_` check is
    at L314);
  - L654–842 `_make_line`, `library_join`, `SPEC_GUARDS`/`ALL_GUARDS` with their comment,
    `classify_boundary`, `join_region`, `_event`, `join_vocabulary`, `EVENT_COLUMNS`,
    `join_inventory`;
  - L849–879 `THROUGHPUT_FIT` (with its comment), `OUTPUT_BYTES_PER_XML_BYTE`,
    `predict_seconds`, `predict_wall`.
  - Tests: `<NLP>/tests/test_voc_text_layer.py` L1–181 (the `DEFAULT`/`join` helpers and the 23
    join tests) and L233 `test_pagexml_tools_version_is_at_least_080`.
- From step 14 (`archival_structures.text_layer`): `io.sanitise`, `format_box`, `write_tsv_gz`,
  `read_tsv_gz`, `write_json_atomic`, `iter_pagexml`, `input_bytes`, `read_region_lines`,
  `discover_inventories`, `inventory_sort_key`; `word_breaks.first_non_wbc`, `joined_word`,
  `mid_line_vocabulary`, `inventory_word_break_stats`, `write_word_break_stats_part`,
  `decide_word_breaks`, `format_stat`, `read_word_break_sets`, `WORD_BREAK_STATS_COLUMNS`.
- `archival_structures.model.scan_id`: `ScanId.try_parse(scan_id, *, digits=4, prefix=None)`
  → `ScanId | None`, with field `.inventory` (`InventoryNumber`; `str()` gives the original).
- pagexml (AS `.venv` 0.8.1, lazy imports only): `from pagexml.parser import parse_pagexml_file`;
  `import pagexml.model.physical_document_model as pdm`; `from pagexml.model.coords import Coords`;
  `from pagexml.helper.pagexml_helper import make_text_region_text`.
- **Fixture:** copy `reports/data/phase-05-text-layer/fixture_9999A/*.xml` (4 files, made at
  planning) to `tests/fixtures/text_layer/pagexml/9999A/`. Don't edit them: file 0001 holds a
  literal tab.
- **References (read-only):** `<REF>` = `<NLP>/output/voc_text_layer/voc_text_layer_v1/` (22
  inventories, built with `--write-events`); `<V1>` = `<NLP>/data/voc_text_layer_v1/`; input
  zips `<NLP>/pagexml/<inv>.zip` (504 local).

## Deliverables

| File | Change |
|---|---|
| `text_layer/extract.py` | `LINES_COLUMNS`, `META_COLUMNS`, `DEFAULT_UNTAGGED_TAG = "unclassified"`, `ROW_ORDER_STATEMENT`, `iter_text_regions`, `region_tag_of(region, counts, untagged_tag=DEFAULT_UNTAGGED_TAG)`, `scan_id_matches(scan_id, inv, prefix=None) -> bool`, `extract_inventory(inv, source, out_root, *, untagged_tag=DEFAULT_UNTAGGED_TAG, scan_id_prefix=None)` |
| `text_layer/join.py` | `REGION_COLUMNS`, `EVENT_COLUMNS`, `SPEC_GUARDS`, `ALL_GUARDS`, `_make_line`, `library_join`, `classify_boundary`, `join_region`, `_event`, `join_vocabulary`, `join_inventory(inv, out_root, wbcs, write_events=False)` |
| `text_layer/provenance.py` | `package_info(dist_name, module_name=None)`, `pagexml_tools_info()`, `MIN_PAGEXML_TOOLS = (0, 8, 0)`, `check_min_version(dist_name, minimum)`, `check_pagexml_tools_version()` |
| `text_layer/presets.py` | `LayerPreset`, `VOC_V1`, `PRESETS`, `predict_seconds`, `predict_wall` |
| `tests/fixtures/text_layer/pagexml/9999A/*.xml` | the 4 fixture files |
| `tests/test_text_layer_join.py` | the 23 ported join tests |
| `tests/test_text_layer_extract.py` | fixture tests (below) |
| `tests/test_text_layer_provenance.py` | the ported version test + new |
| `tests/test_packaging_core_imports.py` | add the four modules to `names`, and to the step-14 "no pagexml in `sys.modules`" test |
| `docs/shared_library/changelog.md` | `### Step 15: text-layer library (C15)` under *Unreleased*, with Edit |
| `reports/phase-05-text-layer.md` | `### Step 15` section, findings `F05-15.n` |

## Behaviour to implement

- **extract.py:** NLP code verbatim, with two changes.
  - `region_tag_of` returns the `untagged_tag` argument.
  - The scan-id check is `scan_id_matches`:
    ```python
    def scan_id_matches(scan_id, inv, prefix=None):
        """True if scan_id parses as <institution>_<archive>_<inv>_<digits>[.ext] with this inventory."""
        parsed = ScanId.try_parse(scan_id, prefix=prefix)
        return parsed is not None and str(parsed.inventory) == inv
    ```
    `extract_inventory` records a mismatch when `not scan_id_matches(scan_id, inv, scan_id_prefix)`.
    The stats JSON keeps its keys and format.
- **join.py:** verbatim. Imports come from `io` and `word_breaks`. `join_inventory` reads the
  rows with `read_region_lines(out_root, inv)`.
- **provenance.py:**
  - `package_info(dist_name, module_name=None)` generalises `pagexml_tools_info`.
    - `module_name` defaults to `dist_name.replace("-", "_")`.
    - It returns `{"version": importlib.metadata.version(dist_name), "path": <package dir>}`.
    - When the package directory's parent has `.git`, it adds `git_head` and `git_dirty_files`
      (`git status --porcelain -- <package dir name>`, with the first 3 characters cut from
      each line, exactly as NLP does).
  - `pagexml_tools_info()` returns `package_info("pagexml-tools", "pagexml")`, which gives the
    same dict as NLP's function.
  - `check_min_version` is NLP's `check_pagexml_tools_version` with parameters.
- **presets.py:**
  ```python
  @dataclass(frozen=True)
  class LayerPreset:
      name: str                      # layer directory name under --out-dir
      untagged_tag: str
      scan_id_prefix: Optional[str]  # e.g. "NL-HaNA_1.04.02"; None = any NA-style prefix
      throughput_fit: Optional[Mapping[str, Mapping[str, float]]] = None
      output_bytes_per_xml_byte: Optional[float] = None
      description: str = ""
  VOC_V1 = LayerPreset(name="voc_text_layer_v1", untagged_tag="unclassified",
                       scan_id_prefix="NL-HaNA_1.04.02", throughput_fit=<NLP THROUGHPUT_FIT>,
                       output_bytes_per_xml_byte=0.0233, description=<one line: GLOBALISE VOC
                       PageXML v2.1; throughput fitted on one 10-core laptop, report/74 §6>)
  PRESETS = {"voc_v1": VOC_V1}
  def predict_seconds(xml_bytes, stage, fit): ...      # NLP's, with the fit passed in
  def predict_wall(per_inventory_xml_bytes, workers, fit, stages=("extract", "wbc", "join")): ...
  ```
  Keep NLP's `THROUGHPUT_FIT` comment in `presets.py`. No other module may hold VOC constants.
- All pagexml imports stay inside functions.

## Constraints

- Port, don't improve. Known pagexml-tools 0.8.1 behaviour, which the fixture pins down:
  - a file with nested `TextRegion`s fails to parse (`TypeError … 'set' and 'set'`, in
    `get_all_text_regions`, `pagexml_document_model.py` L447) and lands in `parse_errors`;
  - a region without `Coords` gets a box derived from its lines;
  - a region with neither `Coords` nor lines is dropped by the parser.
  Don't work around any of these.
- **Protected:** `<REF>` (sha256 of every file) and `<V1>` (size and mtime of every file under
  `wbc/`, plus `manifest.json` and `status.tsv`). Hash them before the first run and check after
  every run. Every output goes into the scratchpad. `extract_stats/*.json` hold an absolute
  `source` path: compare the JSONs without it, and never follow v1's `source` paths (external
  drive).
- Never `git commit`; `chmod g+w` new files. Don't edit NLP or LotA.

## Tests

- `tests/test_text_layer_join.py`: NLP L1–181 ported, with imports switched to
  `archival_structures.text_layer.join`. Keep the `DEFAULT = set("¬„")` helper. **23 tests.**
- `tests/test_text_layer_extract.py` (fixture). Copy `pagexml/9999A` to `tmp_path`, run
  `extract_inventory("9999A", <dir>, out, scan_id_prefix="NL-HaNA_1.04.02")` and
  `join_inventory(..., {"¬", "„"}, write_events=True)`, then assert exactly what planning
  measured with NLP's code:
  - **status:** `n_rows` 4, `n_lines` 6, `n_scans` 3, `n_errors` 1;
  - **regions_meta:**
    `r1 10-10-980-90 header 2`, `r2 10-200-980-100 paragraph 0`,
    `r3 10-400-980-150 unclassified 3`, `m1 10-10-290-90 marginalia 1`;
  - **lines:** `„schrijven met tab` (tab sanitised); r3's second line `line_text == ""`;
  - **extract_stats:**
    - `counts == {"lines_text_none": 1, "n_scans": 3, "table_regions": 0, "untagged_no_custom": 1, "zero_line_regions": 1}`;
    - `sanitised == {"tab": 1}`;
    - one parse error naming `_0003.xml` and `TypeError`;
    - `n_scan_id_mismatch == 1` (`_0004.xml -> NL-HaNA_1.04.02_9998_0004.jpg`);
    - `scans_without_text_regions == ["NL-HaNA_1.04.02_9999A_0002.jpg"]`;
  - **regions text:** r1 `Copia van een briefschrijven met tab`, r2 `""`, r3 `f 2500„ tesamen`,
    m1 `misfiled scan`;
  - **join_events:** 3 rows: `both_ends_fix_last_line`, `merge` (joined `briefschrijven`),
    `g_digit_left`;
  - **join_stats `join`:**
    `{"both_ends_fix": {"last_line": {"„": 1}}, "g_digit_left": {"„": 1}, "lines_empty_dropped": 1, "merge": {"¬": 1}, "merge_context": {"letter>lower": 1}, "regions": 4, "regions_empty_text": 1}`.

  Name the nested-region test `test_nested_regions_fail_in_pagexml_0_8_1`, and give it a comment
  that step 13 revisits it after the 0.8.2 release. Also:
  - `scan_id_matches` unit cases: suffix match, prefix mismatch, `.jpg` extension, garbage →
    False;
  - `region_tag_of` with a custom `untagged_tag`.
- `tests/test_text_layer_provenance.py`:
  - the ported version test;
  - `package_info("pagexml-tools", "pagexml")["version"] == importlib.metadata.version("pagexml-tools")`;
  - `package_info("archival-structures", "archival_structures")` has `git_head` (AS is a git
    checkout).
- A `presets` test: `predict_wall` on `[1e6] * 4` with 2 workers equals a hand computation.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_packaging_core_imports.py`,
  then the full suite (count in `STATUS.md`).

## Acceptance criteria

Scratch script `<scratchpad>/step15/check.py` (`.venv/bin/python -I`), using AS functions only:
1. **The 22, byte-identical to `<REF>`.** The inventories are `1120 1267 1274 1388 1529 1539 1547
   1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834`, in
   `inventory_sort_key` order.
   1. For each: `extract_inventory(inv, <NLP>/pagexml/<inv>.zip, OUT, scan_id_prefix="NL-HaNA_1.04.02")`,
      then `write_word_break_stats_part(inv, OUT)`.
   2. Concatenate the parts, read with the `pd.read_csv` options from step 14, write
      `wbc/wbc_stats.tsv` with `format_stat`, and call `decide_word_breaks` (`inventory_lines` =
      first `n_lines` per inventory, or 0). Set `corpus["allow_partial"] = False` and
      `corpus["n_inventories_missing_extract"] = 0`, then write both decision files.
   3. `join_inventory(inv, OUT, sets.get(inv, default), write_events=True)`, with
      `sets, default = read_word_break_sets(OUT)`.

   Compare with `filecmp.cmp(shallow=False)`: `lines/`, `regions_meta/`, `regions/`,
   `join_events/` and `wbc/parts/` (22 × 5), and the 3 `wbc/` files. `extract_stats/` and
   `join_stats/` are compared as JSON without `source` (44). **157/157.** Takes about 4 min in
   one process; a `ProcessPoolExecutor(8)` over inventories is fine.
2. **A/B against the original module** on 20 more local inventories: `1105 4365 4387 7597 7613
   8031 8402 8809 8889 9037 9121 9223 9541 9626A 9720 10115 10186 10359 10428 11013B`.
   - Load a copy of `<NLP>/src/startpage/voc_text_layer.py`, placed in the scratchpad, with
     `importlib.util.spec_from_file_location`; never import from `<NLP>/src`.
   - Run extract, the WBC part, and join with the **v1 sets** from
     `read_word_break_sets(<V1>)` (reads only), on both sides, into separate scratch roots.
   - Lines, regions_meta, regions, join_events and parts are byte-identical, and the stats
     JSONs are equal without `source`: **100 files plus 40 JSONs, all equal**.
3. **Scan-id check on all of v1:** for every inventory, collect the distinct `scan_id`s from
   `<V1>/regions_meta/<inv>.tsv.gz` (`read_tsv_gz`, one column) and from
   `scans_without_text_regions` in `<V1>/extract_stats/<inv>.json`. Then
   `scan_id_matches(id, inv, "NL-HaNA_1.04.02")` is False for **0 ids**. Print the number of ids
   and inventories (expect about 4.8 M scans; 6,914 inventories; several minutes).
4. Tests green; AS full suite green; protected hashes unchanged.

If a number doesn't reproduce, stop and ask. Don't adjust the port to match.

## Error analysis

The local input differs from v1's only in order. Measure that on the 42 rebuilt inventories (the
22 plus the 20), against `<V1>`, ignoring order:
- per inventory: are the `(scan_id, region_id, line_id, line_text)` multisets per region equal,
  are the region-key sets and `regions_meta` values equal, and does the region order differ;
- totals: regions whose line order differs, and region texts that differ. Planning measured on
  34 inventories: line multisets 34/34 equal; line order differed in 8,626 of 160,132 regions; 8,141 texts
  differed, all of them in regions whose line order differed.

Then read 10 regions whose line order differs (`random.Random(15)`) and classify them:
- one line moved (e.g. the last XML line placed second by v1);
- several lines permuted;
- column interleaving;
- other.

For each, say which order reads correctly, quoting 2–3 lines. Read 5 scans whose region order
differs, the same way. Expected findings: the reproduction (result), the A/B (result), the
scan-id check (data-property), and the input-order difference with its categories
(data-property). Say whether v2.1's order (v1) or the 2023 export's order reads better.

## Do not touch

- NLP and LotA files; `<REF>`, `<V1>`. `io.py` and `word_breaks.py` only for bug fixes, which go
  into the report.

## When done

1. Add `### Step 15: text-layer library` to `reports/phase-05-text-layer.md` (findings
   `F05-15.n`, `draft`); run `make reports` and fix any warning.
2. Append the block below to `plan/phase-05-text-layer/PROGRESS.md`.
3. Overwrite `STATUS.md`: 15 done, next 15c, and the AS suite count.

```markdown
### Step 15: text-layer library
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `text_layer.extract/join/provenance/presets` (new)
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers and runtimes>
- **Report:** [`reports/phase-05-text-layer.md` § Step 15](../../reports/phase-05-text-layer.md#step-15-text-layer-library);
  findings F05-15.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
