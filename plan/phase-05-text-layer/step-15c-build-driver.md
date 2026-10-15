# Step 15c: text-layer build driver, `as-build-text-layer`, layer comparison, docs

> **Phase:** 05, Text layer. **Depends on:** 15.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Steps 14–15 ported the text-layer stages as library functions in `archival_structures.text_layer`.
This step ports NLP's driver, `scripts/build_voc_text_layer.py`, into `text_layer/build.py`. The
driver is resumable per stage, appends to `status.tsv` and `progress.log`, writes the manifest,
and runs a process pool, longest inventory first. It is exposed as the console script
`as-build-text-layer`, with **the same flags** plus `--preset`. A comparison module
(`compare.py`) checks one layer against another. The user needs it to check a v2.1 sample
against v1 on the external disk, which no session may access. Step 15b later makes NLP's script
a wrapper around this driver.

## Inputs

- `<NLP>` = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`. Port
  `<NLP>/scripts/build_voc_text_layer.py` (427 lines; checked 2026-10-14):
  - L1–81 the handoff docstring;
  - L99–101 `STAGES`, `STATUS_COLUMNS`; L104 `now`;
  - L108–168 `class Run` (`update_manifest` L141);
  - L171 `select_inventories`; L190 `run_pool`; L230–240 `FORCE`, `recorded_done`, `is_done`;
  - L243 `stage_extract`; L260 `extracted`; L270–320 `stage_wbc`; L323 `_nonempty_lines`;
  - L330 `stage_join`; L365 `estimate`; L380–423 `main`.
- AS API (steps 14–15):
  - `io`: `discover_inventories`, `input_bytes`, `inventory_sort_key`, `write_json_atomic`,
    `write_plain_tsv`, `read_tsv_gz`;
  - `word_breaks`: `write_word_break_stats_part`, `WORD_BREAK_STATS_COLUMNS`, `format_stat`,
    `pooled_stats`, `decide_word_breaks`, `read_word_break_sets`;
  - `extract`: `extract_inventory(inv, source, out_root, *, untagged_tag, scan_id_prefix)`,
    `LINES_COLUMNS`, `META_COLUMNS`, `ROW_ORDER_STATEMENT`;
  - `join`: `join_inventory(inv, out_root, wbcs, write_events=False)`, `REGION_COLUMNS`,
    `ALL_GUARDS`;
  - `provenance`: `package_info`, `pagexml_tools_info`, `check_pagexml_tools_version`;
  - `presets`: `LayerPreset`, `PRESETS`, `VOC_V1`, `predict_seconds(xml_bytes, stage, fit)`,
    `predict_wall(xml_bytes_list, workers, fit, stages=…)`.
- References (read-only): `<REF>` = `<NLP>/output/voc_text_layer/voc_text_layer_v1/` (22
  inventories; its manifest's stages have the keys `argv failed finished_utc git_head
  n_inventories pagexml_tools params platform python`, plus the stage extras). `<V1>` =
  `<NLP>/data/voc_text_layer_v1/`. Input: `<NLP>/pagexml/<inv>.zip`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/build.py` | the driver (below); `main(argv=None) -> int`; `if __name__ == "__main__": sys.exit(main())` |
| `archival_structures/text_layer/compare.py` | `compare_layers`, `compare_run_files`, `summarise`, `main` (below) |
| `pyproject.toml` | `[project.scripts]` after `[project.optional-dependencies]`: `as-build-text-layer = "archival_structures.text_layer.build:main"` |
| `tests/test_text_layer_build.py`, `tests/test_text_layer_compare.py` | below |
| `tests/test_packaging_core_imports.py` | add `text_layer.build` and `text_layer.compare` to `names` and to the no-pagexml test |
| `docs/text_layer.md` | new guide (below); add `text_layer` after `stream_analysis` in the *Guides* toctree of `docs/index.md` |
| `docs/shared_library/changelog.md` | `### Step 15c: text-layer driver and CLI` under *Unreleased*, with Edit |
| `reports/phase-05-text-layer.md` | `### Step 15c` section, findings `F05-15c.n` |

## Behaviour to implement

**`build.py`**: NLP's script verbatim, with these changes only.
- Module docstring: NLP's handoff text adapted:
  - install `archival-structures` instead of copying NLP files;
  - commands `as-build-text-layer …` (or `python -m archival_structures.text_layer.build …`);
  - the unit-test step says "run AS's `tests/test_text_layer_*.py`" instead of NLP's test file;
  - the layer dir is `<out-dir>/<preset name>/`.
  Keep the expected-runtime, output-size, sanity-anchor and send-back paragraphs. Give the
  argparse description as the first paragraph.
- `--preset`, choices `sorted(PRESETS)`, default `"voc_v1"`. `Run(args, preset)` sets
  `root = out_dir / preset.name`.
- `FORCE` global → `Run.force` (from `args.force`), and `is_done(status, inv, stage, force)`.
- **Extract** passes `functools.partial(extract_inventory, untagged_tag=preset.untagged_tag,
  scan_id_prefix=preset.scan_id_prefix)` to `run_pool` (picklable; the args tuple stays
  `(inv, source, root)`).
- **WBC and join** call the AS names (`write_word_break_stats_part`, `decide_word_breaks`,
  `read_word_break_sets`, `join_inventory`). The `stage_wbc` file writing, columns, the
  `allow_partial` keys and the pooled print stay as they are.
- **`update_manifest`:**
  - `layer = preset.name` and `untagged_region_tag = preset.untagged_tag`; the other
    layer-level keys and the `format` string are verbatim;
  - per stage, `git_head` = `package_info("archival-structures", "archival_structures").get("git_head", "")`;
  - two new stage keys: `"archival_structures": package_info("archival-structures", "archival_structures")`
    and `"preset": args.preset`;
  - all other stage keys as NLP.
- `estimate` uses `preset.throughput_fit` and `preset.output_bytes_per_xml_byte`. If either is
  None it prints that the preset has no throughput fit and returns.
- `main(argv=None)` returns the int exit code.

**`compare.py`**:
```python
TSV_KINDS = {"lines": "lines/{inv}.lines.tsv.gz", "regions_meta": "regions_meta/{inv}.tsv.gz",
             "regions": "regions/{inv}.tsv.gz", "join_events": "join_events/{inv}.tsv.gz",
             "wbc_parts": "wbc/parts/{inv}.tsv"}
JSON_KINDS = {"extract_stats": "extract_stats/{inv}.json", "join_stats": "join_stats/{inv}.json"}
RUN_FILES = ("wbc/wbc_stats.tsv", "wbc/wbc_per_inventory.tsv", "wbc/corpus_default.json")
def compare_layers(left, right, inventories=None, *, kinds=("lines", "regions_meta", "regions"),
                   json_kinds=("extract_stats", "join_stats"), ignore_json_keys=("source",)) -> pd.DataFrame
def compare_run_files(left, right, files=RUN_FILES) -> pd.DataFrame
def summarise(frame) -> dict
def main(argv=None) -> int   # python -m archival_structures.text_layer.compare LEFT RIGHT [--inventories a,b | --inventories-file F] [--kinds ...] [--out TSV]
```
- `left` and `right` are layer roots (the dirs holding `lines/`). `inventories` defaults to the
  stems present under `lines/` on **both** sides, sorted by `inventory_sort_key`.
- One row per inventory and kind, with the columns `inventory_number`, `kind`, `exists_left`,
  `exists_right`, `byte_equal`, `rows_left`, `rows_right`, `content_equal` (decompressed text
  equal), `multiset_equal` (sorted data rows equal) and `order_only` (`multiset_equal and not
  content_equal`).
- JSON kinds: `json_equal` after dropping `ignore_json_keys`; the TSV columns are NA.
- `summarise` counts each boolean column per kind.
- `main` prints the summary and exits 0 iff every compared TSV is `byte_equal` and every JSON is
  equal; `--out` writes the frame (TSV).
- No pagexml import; read gzip with the stdlib.

**`docs/text_layer.md`** (MyST, ~80–120 lines):
- what the layer is: columns of `lines/`, `regions_meta/`, `regions/`; row order is document
  order, not reading order; `QUOTE_NONE`; read with `text_layer.io.read_tsv_gz`;
- the three stages and the word-break rule (the five thresholds in a table, with their reasons
  shortened);
- presets;
- how to build (handoff summary);
- provenance (manifest keys);
- **"Checking a rebuild against v1"**: the user's v2.1 recipe below;
- the known pagexml-tools 0.8.1 limits: nested regions fail to parse; a region with neither
  `Coords` nor lines is dropped.

**The v2.1 recipe** (the user runs it; check it locally first, criterion 4). `<IN>` = the
GLOBALISE v2.1 PageXML directory, `<O>` = a fresh local output dir, and `<INVS>` =
`1171A,1457B,7872A,9615A,1095,1394,2158,3172,3944,3972,7586,8817,8846,9253,9685,9994,10256,10466,2555,8834`:
```
as-build-text-layer --pagexml-dir <IN> --out-dir <O> --stage extract --inventories <INVS> --workers 8
mkdir -p <O>/voc_text_layer_v1/wbc
cp <NLP>/data/voc_text_layer_v1/wbc/wbc_per_inventory.tsv <NLP>/data/voc_text_layer_v1/wbc/corpus_default.json <O>/voc_text_layer_v1/wbc/
as-build-text-layer --pagexml-dir <IN> --out-dir <O> --stage join --inventories <INVS> --workers 8
python -m archival_structures.text_layer.compare <O>/voc_text_layer_v1 <NLP>/data/voc_text_layer_v1 --inventories <INVS> --out <O>/compare_v1.tsv
```
Expected: exit 0, i.e. lines, regions_meta and regions byte-identical, and the stats JSONs equal.

## Constraints

- Port, don't improve: same flags, help texts, log lines, status columns and skip rules.
- **Protected:** `<REF>` (sha256 of every file), and `<V1>` (size and mtime under `wbc/`, plus
  `manifest.json` and `status.tsv`). Check them after every run. `--out-dir` always points into
  the scratchpad. Never follow `source` paths in v1's JSONs (external drive).
- **Install:** run `.venv/bin/pip install --no-deps -e .` once, to register the console script.
  The editable install's metadata is stale (0.1.0) and has no entry points. Record the
  command in `PROGRESS.md`. Install nothing else.
- Never `git commit`; never bump the version; `chmod g+w` new files.

## Tests

`tests/test_text_layer_build.py` (fixture `tests/fixtures/text_layer/pagexml/9999A`, copied to
`tmp_path`; also a copy renamed `9999B` for multi-inventory cases):
- `--stage all` without `--ignore-sanity` exits non-zero at join. One tiny inventory → no
  eligible chars → empty default set → `sanity_pass` False.
- With `--ignore-sanity`, `regions/9999A.tsv.gz` exists and `status.tsv` has 3 `done` rows.
- A rerun adds no `extract` rows and logs "already done". `--force` adds them.
- `--stage wbc` on a fresh out dir exits non-zero ("extract is not done").
- `--estimate` returns 0 and creates no layer directory.
- `--limit 1` with two inventories processes only `9999A`.
- The manifest has the layer keys `layer row_order columns format untagged_region_tag stages`.
  Each stage has `finished_utc git_head archival_structures preset pagexml_tools python platform
  argv params`.

`tests/test_text_layer_compare.py` (tmp layers written with `io.write_tsv_gz`):
- identical → all `byte_equal`;
- rows swapped → `order_only` True;
- one text changed → `multiset_equal` False;
- missing file → `exists_right` False;
- JSONs differing only in `source` → equal;
- `main` exit codes 0 and 1.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_packaging_core_imports.py`,
then the full suite.

## Acceptance criteria

1. **CLI reproduces `<REF>`.** Run `.venv/bin/as-build-text-layer --pagexml-dir <NLP>/pagexml
   --out-dir <scratch>/r22 --stage all --inventories 1120,1267,1274,1388,1529,1539,1547,1557,2448,2548,2555,2775,3142,3891,7923,8023,8121,8237,8276,8284,8697,8834
   --write-events --workers 8`. Then:
   - `compare_layers(<scratch>/r22/voc_text_layer_v1, <REF>, kinds=tuple(TSV_KINDS))`: all 110
     byte-equal and all 44 JSONs equal;
   - `compare_run_files`: 3/3.
   - **157/157.** Record the wall time.
2. **Manifest is a superset.** Per stage, the `<REF>` keys ⊆ the new keys, and new − ref ==
   `{"archival_structures", "preset"}`. The layer-level values are equal to `<REF>`'s (`layer`,
   `row_order`, `columns`, `format`, `untagged_region_tag`).
3. **Resumable.** Rerunning the same command logs 0 to do in every stage and changes no mtime
   under `lines/` or `regions/`.
4. **The v2.1 recipe works locally.** Run the four commands with `<IN>` = `<NLP>/pagexml`,
   `--inventories 1120,2555,8834`, and the two `wbc` files copied from `<REF>` (not `<V1>`). Then
   compare with `<REF>`: exit 0.
5. `as-build-text-layer --help` lists NLP's 13 flags plus `--preset`. `--estimate --workers 8`
   on the 22 prints a prediction.
6. Tests green; AS full suite green; protected hashes unchanged.

If a number doesn't reproduce, stop and ask.

## Error analysis

- Diff the new manifest against `<REF>`'s, key by key, and explain every difference (expected:
  `git_head` now AS's, the two new keys, timestamps, argv and paths).
- Compare the `--estimate` prediction for the 22 on 8 workers with the measured wall time of
  criterion 1. The fit comes from one laptop; report the ratio, and don't refit.
- Read `progress.log` of the run: list every line that isn't a per-inventory progress line.
- Expected findings: the CLI reproduction (result), the manifest change (behaviour-change), the
  estimate accuracy (method).

## Do not touch

- NLP and LotA files; `<REF>`, `<V1>`. Steps 14–15 modules only for bug fixes, which go into the
  report.

## When done

1. Add `### Step 15c: text-layer driver and CLI` to `reports/phase-05-text-layer.md` (findings
   `F05-15c.n`, `draft`); run `make reports` and fix any warning.
2. Append the block below to `plan/phase-05-text-layer/PROGRESS.md`.
3. Overwrite `STATUS.md`:
   - 15c done;
   - next: **the user releases pagexml-tools 0.8.2**, then step 13;
   - the v2.1 recipe as an open item for the user (P3d);
   - the AS suite count.

```markdown
### Step 15c: text-layer driver and CLI
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `text_layer.build.main`, `as-build-text-layer`, `text_layer.compare.*` (new)
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers and wall times>
- **Report:** [`reports/phase-05-text-layer.md` § Step 15c](../../reports/phase-05-text-layer.md#step-15c-text-layer-driver-and-cli);
  findings F05-15c.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
