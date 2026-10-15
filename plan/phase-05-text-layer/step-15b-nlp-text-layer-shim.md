# Step 15b: NLP `voc_text_layer.py` becomes a shim, the build script a wrapper; reproduction

> **Phase:** 05, Text layer. **Depends on:** 13, 15c.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Steps 14–15c moved NLP's text-layer producer into `archival_structures.text_layer`, and step 13
put all environments on pagexml-tools 0.9.0 (the release was 0.9.0, not the planned 0.8.2; NLP's
dist-info metadata may still report 0.8.0, see `STATUS.md`). NLP's venv imports AS from the AS
repo (`/Users/marijnkoolen/Code/Huygens/archival-structures`), so the AS changes are live there.
Here:
- NLP's module keeps every public name but delegates to AS;
- NLP's build script keeps its path and flags but calls AS's driver;
- a verify script reruns the build on the 22 annotated inventories and proves its outputs are
  byte-identical to NLP's stored local build, and to a run of the original code.

`scripts/validate_voc_text_layer.py` stays NLP research code, unchanged. After this step the
user moves the v1 data to `shared-data/` (commands below).

## Inputs

`<NLP>` = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`. **Read
`<NLP>/CLAUDE.md` first** and `<NLP>/docs/from-archival-structures.md`: when a module moves, NLP's
copy becomes a thin re-export. Interpreter: `<NLP>/venv/bin/python`. Sessions run as
`claude-agent`: if an import fails only here, retry with `PYTHONNOUSERSITE=1` and report.

`<NLP>/src/startpage/voc_text_layer.py` (879 lines). Its importers:
- `scripts/build_voc_text_layer.py` (becomes a wrapper);
- `scripts/validate_voc_text_layer.py`, which uses `ALL_GUARDS, SPEC_GUARDS, LAYER_NAME,
  UNTAGGED_TAG, discover_inventories, input_bytes, join_region, library_join, load_wbc_sets,
  read_tsv_gz, write_json_atomic, write_plain_tsv`;
- `tests/test_voc_text_layer.py` (29 tests), which uses `SPEC_GUARDS,
  check_pagexml_tools_version, discover_inventories, format_box, inventory_sort_key,
  join_region, library_join, read_tsv_gz, sanitise, write_tsv_gz`.

Nothing in notebooks imports it.

AS names (steps 14–15c):
- `text_layer.io`: `SANITISE_PATTERN, sanitise, format_box, _assert_clean, write_tsv_gz,
  read_tsv_gz, write_text_atomic, write_json_atomic, write_plain_tsv, inventory_sort_key,
  discover_inventories, _is_pagexml_member, input_bytes, iter_pagexml, read_region_lines`;
- `text_layer.word_breaks`: `LETTERS, TRAILING_LETTERS, LEADING_LETTERS,
  DEFAULT_WORD_BREAK_RULE, SANITY_IN, SANITY_OUT, is_candidate_char, char_label,
  mid_line_vocabulary, ends_attached, strip_leading_marks, joined_word, first_non_wbc,
  word_break_stats, inventory_word_break_stats(inv, layer_root), WORD_BREAK_STATS_COLUMNS,
  format_stat, write_word_break_stats_part(inv, layer_root), pooled_stats, rule_passes,
  decide_word_breaks(stats, inventory_lines, rule=…), read_word_break_sets(layer_root)`;
- `text_layer.extract`: `LINES_COLUMNS, META_COLUMNS, ROW_ORDER_STATEMENT, iter_text_regions,
  region_tag_of, extract_inventory(inv, source, out_root, *, untagged_tag, scan_id_prefix)`;
- `text_layer.join`: `REGION_COLUMNS, EVENT_COLUMNS, SPEC_GUARDS, ALL_GUARDS, _make_line,
  library_join, classify_boundary, join_region, _event, join_vocabulary, join_inventory`;
- `text_layer.provenance`: `MIN_PAGEXML_TOOLS, pagexml_tools_info, check_pagexml_tools_version`;
- `text_layer.presets`: `VOC_V1` (`.name`, `.untagged_tag`, `.scan_id_prefix`,
  `.throughput_fit`, `.output_bytes_per_xml_byte`), `predict_seconds(xml_bytes, stage, fit)`,
  `predict_wall(xml_list, workers, fit, stages=…)`;
- `text_layer.build`: `main(argv=None) -> int` (`--preset` defaults to `voc_v1`);
- `text_layer.compare`: `compare_layers(left, right, inventories=None, *, kinds=…)`,
  `TSV_KINDS`, `compare_run_files`, `summarise`.

References:
- `<REF>` = `<NLP>/output/voc_text_layer/voc_text_layer_v1/` (22 inventories, built with
  `--write-events`; the stored local build);
- `<V1>` = `<NLP>/data/voc_text_layer_v1/`;
- input `<NLP>/pagexml/<inv>.zip`.

The 22: `1120,1267,1274,1388,1529,1539,1547,1557,2448,2548,2555,2775,3142,3891,7923,8023,8121,8237,8276,8284,8697,8834`.

## The shim (`src/startpage/voc_text_layer.py`)

- Docstring: "Thin shim over AS `archival_structures.text_layer` (AS phase 05, steps 14–15c).
  The VOC values are AS's `presets.VOC_V1`. Every name of the original module is kept."
- **Re-export under the old names**, everything in *AS names* above that the original module
  defined (all top-level names of the original; check with `grep -n "^def \|^[A-Z_]* = "` on the
  saved copy). Mapping:
  - `LAYER_NAME = VOC_V1.name`;
  - `UNTAGGED_TAG = VOC_V1.untagged_tag`;
  - `WBC_RULE = DEFAULT_WORD_BREAK_RULE`;
  - `WBC_STATS_COLUMNS = WORD_BREAK_STATS_COLUMNS`;
  - `_fmt = format_stat`;
  - `inventory_wbc_stats = inventory_word_break_stats`;
  - `wbc_stats_part = write_word_break_stats_part`;
  - `load_wbc_sets = read_word_break_sets`;
  - `decide_wbc(stats, inventory_lines, rule=WBC_RULE)` calls `decide_word_breaks`;
  - `THROUGHPUT_FIT = VOC_V1.throughput_fit`;
  - `OUTPUT_BYTES_PER_XML_BYTE = VOC_V1.output_bytes_per_xml_byte`;
  - `predict_seconds(xml_bytes, stage)` and `predict_wall(per_inventory_xml_bytes, workers,
    stages=("extract", "wbc", "join"))` pass `THROUGHPUT_FIT`;
  - `extract_inventory(inv, source, out_root)` calls AS with `untagged_tag=VOC_V1.untagged_tag,
    scan_id_prefix=VOC_V1.scan_id_prefix`;
  - everything else under the same name.
- No logic stays in NLP.

## The wrapper (`scripts/build_voc_text_layer.py`)

```python
"""Build the shared VOC text layer (thin wrapper, AS phase 05 step 15b).

The producer now lives in archival-structures: `archival_structures.text_layer.build`
(console script `as-build-text-layer`, preset `voc_v1`). Flags are unchanged. The handoff
instructions are in AS `docs/text_layer.md` and the `build` module docstring.
"""
import sys
from archival_structures.text_layer.build import main

if __name__ == "__main__":
    sys.exit(main())
```

Before editing, copy both originals to `<NLP>/output/as-phase-05-originals/src/voc_text_layer.py`
and `<NLP>/output/as-phase-05-originals/scripts/build_voc_text_layer.py`; `chmod g+w` them.

## `scripts/verify_as_text_layer.py` (new)

- argparse:
  - `--out-dir` (required; refuse a path inside `<NLP>/output/` or `<NLP>/data/`);
  - `--mode {current,originals,both}`, default `both`;
  - `--workers`, default 8.
- Each mode runs in a **subprocess with `cwd=<NLP>`** and writes to `<out-dir>/<mode>/`, with
  `--pagexml-dir pagexml --stage all --inventories <the 22> --write-events --workers N`.
  - `current` runs `scripts/build_voc_text_layer.py`.
  - `originals` runs a small runner (`python -c`):
    1. `sys.path.insert(0, "<NLP>/src")`; `import startpage`.
    2. Load the saved module copy with `importlib.util.spec_from_file_location` and register it
       as `sys.modules["startpage.voc_text_layer"]`.
    3. Load the saved script copy the same way, **set its `__file__` to the real
       `<NLP>/scripts/build_voc_text_layer.py` before `exec_module`**, because it derives
       `PROJECT_ROOT` from `__file__`.
    4. `sys.exit(mod.main([...]))`.
- **Checks** (PASS/FAIL each; exit 0 only if all pass):
  1. **stored:** `current/voc_text_layer_v1` vs `<REF>`: `compare_layers(kinds=tuple(TSV_KINDS))`
     all 110 byte-equal and 44 JSONs equal; `compare_run_files` 3/3. **157/157**.
  2. **A/B:** `current` vs `originals`: the same 157. Manifest: layer-level values equal. Per
     stage, the current keys minus the originals' keys == `{"archival_structures", "preset"}`.
  3. **decide:** the shim's `decide_wbc` on `<V1>/wbc/wbc_stats.tsv`. Use the inventory order
     and `inventory_lines` rule from AS step 14 criterion 1: all `<V1>/lines/` stems, sorted by
     `inventory_sort_key`, the first `n_lines` per inventory or 0, and `allow_partial`/
     `n_inventories_missing_extract` set to False/0. It writes `wbc_per_inventory.tsv` and
     `corpus_default.json` into `<out-dir>/decide/`, byte-identical to `<V1>/wbc/`: **2/2**.
  4. **names:** every name in the two importer lists above resolves on the shim.
  5. **protected:** unchanged (below).

## Protected outputs

Record before the first run and check after every run:
- `<REF>`: sha256 of every file;
- `<V1>`: size and mtime of every file, plus sha256 of `wbc/*`, `manifest.json` and
  `status.tsv`;
- `<NLP>/output/voc_text_layer_validation/` (if present): sha256.

Never write under `<NLP>/data/` or `<NLP>/output/voc_text_layer/`. `extract_stats` JSONs hold an
absolute `source` path; v1's point to an external drive, so never follow them.

## Deliverables

| File | Change |
|---|---|
| `<NLP>/src/startpage/voc_text_layer.py` | shim |
| `<NLP>/scripts/build_voc_text_layer.py` | wrapper |
| `<NLP>/output/as-phase-05-originals/{src,scripts}/…` | copies of the originals |
| `<NLP>/scripts/verify_as_text_layer.py` | new |
| AS `docs/shared_library/changelog.md` | a "NLP switched (step 15b)" bullet under `### Step 15`, with Edit, listing the shim and wrapper |
| AS `reports/phase-05-text-layer.md` | `### Step 15b` section, findings `F05-15b.n` |

## Constraints

- Never `git commit`; `chmod g+w` everything new. Don't edit AS code; if AS needs a change, stop
  and ask.
- Don't edit `validate_voc_text_layer.py`, `tests/test_voc_text_layer.py`, or any phase-03/04
  shim.
- 8 workers (NLP `CLAUDE.md`).

## Tests

- `<NLP>/venv/bin/python scripts/verify_as_text_layer.py --out-dir <scratchpad>/verify15b` →
  exit 0 (about 1–2 min per mode on 8 workers).
- From `<NLP>`: `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_voc_text_layer.py`
  → **29 passed through the shim**. Then `… tests/test_evaluation.py tests/test_semi_markov.py`
  → **21 passed** (in that order).
- `venv/bin/python scripts/validate_voc_text_layer.py --help` exits 0 (imports resolve).
- AS full suite green (count in `STATUS.md`).

## Acceptance criteria

1. Verify script exit 0: stored 157/157, A/B 157/157 plus manifest, decide 2/2, names
   resolve, protected unchanged.
2. `grep -c "^def " <NLP>/src/startpage/voc_text_layer.py` shows only thin wrappers
   (`decide_wbc`, `predict_seconds`, `predict_wall`, `extract_inventory`); the module imports
   `archival_structures.text_layer`.
3. NLP 29 + 21 passed; AS suite green.

If criterion 1 fails, stop and ask. Don't loosen a comparison.

## Error analysis

- From the A/B manifests, list every differing value per stage and classify it (timestamp,
  `git_head`, the new keys, other). "Other" must be empty, or explained.
- Read the `current` run's `progress.log` against the originals' log. The lines should be
  identical apart from timestamps and timings; report any line that isn't.
- Expected findings: the reproduction through the shim (result), the manifest change
  (behaviour-change, citing F05-15c), and any name that the importers needed beyond the
  lists (brief defect).

## Do not touch

- AS code. `<REF>`, `<V1>`. LotA.

## When done

1. Add `### Step 15b: NLP text-layer shim` to `reports/phase-05-text-layer.md` (findings
   `F05-15b.n`, `draft`); run `make reports`.
2. Append the block below to `plan/phase-05-text-layer/PROGRESS.md`.
3. Overwrite `STATUS.md`:
   - phase ready for review;
   - the NLP working-tree changes, listed for the user;
   - **the D5 data move for the user**, verbatim (`<DS>` = `/Users/Shared/claude_projects/data_scopes`):
     ```
     mv <DS>/nlp-start-page-detection/data/voc_text_layer_v1 <DS>/shared-data/voc_text_layer_v1
     ln -s ../../shared-data/voc_text_layer_v1 <DS>/nlp-start-page-detection/data/voc_text_layer_v1
     ln -sfn ../../shared-data/voc_text_layer_v1 <DS>/language-of-the-archive/data/voc_text_layer_v1
     ```
     and update `<DS>/shared-data/README.md` (v1 moved; producer = AS `text_layer`, preset
     `voc_v1`);
   - the user's v2.1 sample (from 15c), if it is still open.

```markdown
### Step 15b: NLP text-layer shim
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none in AS; NLP names unchanged
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers and runtimes>
- **Report:** [`reports/phase-05-text-layer.md` § Step 15b](../../reports/phase-05-text-layer.md#step-15b-nlp-text-layer-shim);
  findings F05-15b.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
