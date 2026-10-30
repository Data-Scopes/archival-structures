# Step 21d: the `index_links_v2` layer (builder, dev build, silver, run kit)

> **Phase:** 15, Index links v2. **Depends on:** 21b and 21c (done; their STATUS entries name `FOLIO_MAP_V2` and the
> selected `method`/`variant`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phase 14 shipped `NL-HaNA_1.01.02_index_links_v1` as a dev build and a full run, unpublished. 21b and 21c added folio
map v2, locator variants L5–L7 and line-level entry assembly A1–A3, and tested them once. This step builds the derived
layer **`NL-HaNA_1.01.02_index_links_v2`** from a v5 text layer with the components that 21c selected:
- a builder option;
- an entries table;
- a dev build of the 41 local front-index volumes;
- silver checks;
- updated docs;
- the UA for the full run on the user's drive.

**Build v2 whatever 21b/21c's bars said**: the layer records which bars failed, and the review decides on publication.
v1 must still rebuild byte-identically. The v5 text layer is not rebuilt.

## Inputs

- `scripts/build_index_links.py` (160 lines; read it whole).
  - argparse: `--layer` (required), `--out-dir` (required, must exist), `--inventories-file`, `--workers`
    (default 1), `--limit`, `--variant` (default `DEFAULT_LOCATOR_VARIANT`).
  - Output: `<out-dir>/NL-HaNA_1.01.02_index_links_v1/` (`LAYER_NAME`), holding:
    - `folio_map/<inv>.tsv.gz` (`MAP_COLUMNS`);
    - `links/<inv>.tsv.gz` (`LINK_COLUMNS`);
    - `meta/<inv>.json`;
    - `volumes.tsv` (`VOLUME_COLUMNS`: `inv mode n_anchors recto_share chain_len chain_first chain_last front_first
      front_last end n_entries n_locators n_linked`);
    - `manifest.json`.
  - `process()` skips an inventory whose 3 files exist. Its outcomes are `skipped | no_front_index | done`.
    `run_one()` records `failed`.
  - `ProcessPoolExecutor` runs when `--workers > 1`, and `progress.log` gets one line per inventory.
  - The tracked test `tests/test_analysis_index_links.py::test_builder_two_volumes_and_resume` (and
    `…failed_volume_is_recorded`) loads the script with importlib.
- `archival_structures/analysis/index_links.py` after 21c:
  - v1: `volume_links`, `extract_locators`, `SG_LOCATOR_PARAMS` (L1–L7);
  - new: `LineBox`, `Entry`, `read_side_lines(layer_root, inv, sides, *, unit_types=…)`,
    `side_reading_order(lines, cfg)`, `assemble_entries(lines, method, *, …, wbc, column_cfg)` and
    `entry_locators(entry, params, *, folio_range=None)`.
  - Read the 21c section of `STATUS.md` for the selected `method` (A0–A3) and `variant` (L2, L5–L7).
- `archival_structures/analysis/folio_map.py` after 21b: `FOLIO_MAP_V2`, and `FolioTarget.opening`.
  `table()` adds `opening_scan_id opening_page` when `params.opening`.
- `presets.SG_HAND_COLUMN_ORDER_V5` and `presets.SG_HAND_V2.wbc_pinned` (word-break chars per inventory).
- The dev layer: `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5`. The 41 front-index inventories
  are in `reports/data/phase-14-index-links/step-20e/dev_inventories.txt`.
- The v1 reference build: `data/index_links_dev/20r-a/NL-HaNA_1.01.02_index_links_v1/` (41 volumes, L2).
- The phase-14 silver script `reports/data/phase-14-index-links/step-20e/silver.py`. You may read it for the
  entry-word definition: words of ≥ 6 letters, matched on the target sides against the folio N+3 control.
- `docs/index_links.md`, the v1 schema and limits (72 lines): `## Rules`, `## Schema`, `## Numbers`, `## Full run`,
  `## Known limits`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_links.py` | `volume_links_v2(...)`, `ENTRY_COLUMNS`, `LINK_COLUMNS_V2`, `V2_CONFIG` |
| `scripts/build_index_links.py` | `--version {v1,v2}` (default `v1`); a v2 path; v1 output unchanged |
| `tests/test_analysis_index_links.py` | v2 builder and `volume_links_v2` tests |
| `docs/index_links.md` | a `# v2` part: rules, schema, numbers, bars, known limits; v1 text kept |
| `data/index_links_dev/21d-a/` | the v2 dev build, 41 volumes |
| `data/index_links_dev/21d-v1/` | the v1 rebuild with the new code, for the regression |
| `reports/data/phase-15-index-links-v2/step-21d/` | `regress.txt`, `silver.py`, `silver.tsv`, `compare_v1_v2.py`, `compare.tsv`, `sample_links.py`, `sample_links.txt`, protect lists |
| `docs/USER_ACTIONS.md` | the full-run UA (next free ID) |
| `docs/shared_library/changelog.md` | `### Step 21d: index_links_v2 layer` under *Unreleased* (Edit only) |
| `reports/phase-15-index-links-v2.md` | the `### Step 21d: …` section, with draft findings `F15-21d.n` |

## Behaviour to implement

**`V2_CONFIG`** (module constant) is a dict holding `map_params=FOLIO_MAP_V2`, `method=<21c's>`,
`variant=<21c's>` and `column_cfg=SG_HAND_COLUMN_ORDER_V5`. Next to it, a comment gives 21c's dev F1 and the test bars.

**`volume_links_v2(inputs, side_lines, units, texts, *, unit_segments=None, config=V2_CONFIG, wbc="-=„") -> Optional[dict]`.**
`unit_segments` maps `(scan_id, unit_id)` to `[(line_id, char_start, char_end)]` from `join/segments`. It is needed
only for A0, and the builder reads it only then.
- Front run, front sides and exclude set as in `volume_links`; the map is `build_folio_map(…, params=config["map_params"])`.
- The entries per front side depend on the method:
  - **A0**: the side's `main_text` units, in `unit_order`. `entry_id` = the unit_id, `kind` = `entry`, and the text
    comes from `texts`. `line_ids` come from `unit_segments`, and a locator's `line_id` is the segment whose
    `char_start ≤ loc.char_start < char_end` (as in 21c's A0);
  - **A1–A3**: `assemble_entries(side_lines[side], method, wbc=wbc, column_cfg=config["column_cfg"])`, with
    `entry_id` = `f"{scan_id}:{page}:e{n:03d}"` (n from 1 per side, headings included).
- Locators: `entry_locators(entry, variant_params, folio_range=(chain first, chain last) if L7 else None)`.
- Each locator is located with `map.locate(N)`.
- Returns `{"front", "map", "entries": [entry rows], "links": [link rows]}`.

`ENTRY_COLUMNS = scan_id page entry_id kind first_line_id n_lines line_ids text`. Here `line_ids` is `|`-joined.

`LINK_COLUMNS_V2 = scan_id page entry_id loc_idx number char_start char_end line_id front_scan_id front_page
back_scan_id back_page opening_scan_id opening_page method anchor_dist confidence`. Unlocated links get empty target
fields and `method` = `none`.

**Builder `--version v2`.**
- The layer name is `NL-HaNA_1.01.02_index_links_v2`, and the tables are `folio_map/` (with the opening columns),
  `entries/`, `links/` (`LINK_COLUMNS_V2`), `meta/`, `volumes.tsv` and `manifest.json`.
- `process()` also reads the side lines with `read_side_lines` (lines, units/units, units/segments) for the front sides
  only, and passes the inventory's `wbc`.
- An inventory is skipped when its 4 files exist.
- `volumes.tsv` adds `n_headings n_dup_dropped chain_cut` to the v1 columns. `n_dup_dropped` is the number of tied
  numbers G2 dropped, and `chain_cut` is 1 if G1 cut the chain. Both come from `FolioMap.n_dup_dropped` and
  `FolioMap.chain_cut`, which 21b added.
- The manifest adds `version`, `config` (map params as a dict, method, variant, column cfg) and `bars` (copy the 21b
  and 21c numbers from `STATUS.md`).
- `--version v1` writes exactly what the current builder writes.
- `--variant` stays v1-only. With `--version v2` it is an error, because v2 takes its variant from `V2_CONFIG`.

**Regression (`regress.txt`).** Build `--version v1` for the 41 inventories into `data/index_links_dev/21d-v1/`. Compare
it with `20r-a`: every `folio_map`, `links` and `meta` file (decompressed text; for meta, JSON without timing keys) and
`volumes.tsv`. **All equal is required.**

**Dev build.** Build `--version v2` into `data/index_links_dev/21d-a/` for the 41 inventories. With more than one
worker, launch it through coreshare. First check
`python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then run `python3 …/coreshare.py
run --project archival-structures --label 21d-a --want 3 --min 1 --wait -- .venv/bin/python scripts/build_index_links.py
--version v2 --layer <dev layer> --out-dir data/index_links_dev/21d-a --inventories-file <file> --workers '{workers}'`.
Run a `--limit 2` smoke test into a scratch dir first.

**Compare and silver.**
- `compare_v1_v2.py` writes `compare.tsv`, per volume: entries, locators and linked for v1 and v2, `n_headings`, the
  share of `high`/`medium`/`low`/`extrapolated` links, and chain changes. For the 7 volumes whose chain end changes in
  21b, the number of v1 `high` links between the last real anchor and the old end, and what v2 does with them
  (expected: none located).
- `silver.py` copies phase 14's definition (per link: does an entry word of ≥ 6 letters appear on the target sides,
  and on the N+3 control?). Report v1 and v2 on the same 41 volumes, overall and for the 13 `high`-stratum volumes
  (3438 3444 3455 3459 3493 3507 3522 3539 3547 3572 3587 3623 3630). These are the two-column ones, where
  attribution should matter most. Also report the share of v2 links whose matter is found on the opening side only.
  **Silver is reported, not a bar.**
- `sample_links.py`: 20 random v2 links (`random.Random(2104)`). For each, print the entry text, N, and the target
  sides' first lines.

**`docs/index_links.md`.** Add a v2 part:
- the rules: map G1–G4, locator variant, assembly method, each with a one-line rationale;
- the schema of all tables;
- the dev-build numbers;
- the bars from 21b/21c with intervals;
- the known limits: carry over the v1 ones that still hold, and add new ones from 21b/21c's error analyses.

Keep the v1 text; mark it *v1*.

**UA: full run** (append with Edit; follow the format at the top of `docs/USER_ACTIONS.md`). Title: `### UA-0NN Run
the index-links v2 build on the full v5 layer (<date>, from phase-15 step 21d)`.
- Action: Run, in the AS root, on the machine with the drive. Copy the 4 commands of the resolved UA-052
  (`docs/USER_ACTIONS_resolved.md`, grep `UA-052`) with three changes:
  - add `--version v2`;
  - the out dir is `<drive>/index_links_v2`;
  - the copy-back target is `data/index_links_full_v2/`.

  State 21d's measured dev time.
- Options: (a) run; (b) skip until the review. Recommended: (a). It costs minutes, and the counts feed the review's
  publication decision.
- Default if unanswered: the review judges v2 on the dev build only.
- Blocks: the publication decision only.
- Response: empty.

## Constraints

- Change nothing in `folio_map.py`, `page_structure.py`, `presets.py`, or 21c's functions. If one is wrong, stop and
  record it under *Open questions* rather than fix it here.
- Every 21a/b/c file is read only. Gold is not read in this step except through 21b/21c's reported numbers.
- `.gitignore` already un-ignores `scripts/build_index_links.py`. Add no new script under `scripts/`.
- **Protected outputs:** run `protect15.py` before and after, with `--skip data/index_links_dev/21d-a
  data/index_links_dev/21d-v1 reports/data/phase-15-index-links-v2/step-21d`. The command is
  `.venv/bin/python -I reports/data/phase-15-index-links-v2/planning/protect15.py
  reports/data/phase-15-index-links-v2/step-21d/protect_before.txt --skip …`. Then `diff` the two lists. `20e-a`,
  `20r-a` and `index_links_full` must be unchanged.
- The agent shell: no `rm -rf`, `nohup` or `&`; zsh (quote `'{workers}'`); no `sed -i`; `chmod -R g+w` the new build
  dirs; never `git commit`, `git stash`, `checkout` or `reset`.

## Tests

- `test_volume_links_v2_synthetic`: a two-column front side in the mini layer gives entries in column order and
  links with `entry_id`, `line_id` and opening columns.
- `test_builder_v2_tables_and_resume`: `--version v2` on the mini layer writes the 4 tables plus `volumes.tsv` and
  `manifest.json` with `version: v2`. A rerun skips.
- `test_builder_v1_unchanged_default`: no `--version` gives the v1 layer name and columns.
- `test_builder_v2_rejects_variant`.

Commands: the module tests, then the full suite (take the expected count from `pytest --collect-only -q`).

## Acceptance criteria

1. The tests pass and the full AS suite is green.
2. `regress.txt`: the v1 rebuild equals `20r-a` in every compared file (41 + 41 + 41 files and `volumes.tsv`).
3. The v2 dev build has 41 volumes, 0 failed, and 0 `no_front_index` among them. `manifest.json` records the version,
   config and bars. Every link's `entry_id` is in `entries/`. In the 7 chain-end volumes, no v2 link is located
   beyond the new chain end + 6 (by construction; checked).
4. `compare.tsv`, `silver.tsv` and `sample_links.txt` are written. `docs/index_links.md` has the v2 part. The UA is
   written.
5. Protected trees unchanged except the step's own dirs.

The user decides when the step is done.

## Error analysis

- Read the 20 sampled links. For each, is the entry whole, is N a folio number, and do the target sides hold the
  matter? Classify them as right, opening, next folio, wrong entry text, wrong number, wrong scan, or untestable.
- Read the 5 volumes whose link count changes most between v1 and v2, and say why (assembly, variant, map).
- Name the silver result honestly. If v2's word share is not above v1's in the high stratum, say so: the assembly
  did not help there.

## Do not touch

`folio_map.py`, `page_structure.py`, `presets.py`, `data/index_links_dev/20e-a/` and `20r-a/`,
`data/index_links_full/`, every text layer, 21a–21c's files, `planning/` (except running `protect15.py`),
`shared-data/`, the siblings, the external drive.

## When done

1. In the report, write `### Step 21d: the index_links_v2 layer` in `reports/phase-15-index-links-v2.md`, with draft
   findings `F15-21d.n`. Then run `make reports` and check it is clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 21d: the index_links_v2 layer
- **Session:** <model>, <date>. **Status:** done / partial / blocked
- **Files changed:** <paths>
- **Public API:** <names added; builder flag>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21d](../../reports/phase-15-index-links-v2.md#step-21d-the-index_links_v2-layer); findings F15-21d.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** UA-0NN (full run)
```

3. Overwrite the phase `STATUS.md`. The phase is ready for review. Include the UA id, the dev-build numbers, and the
   time per volume.
