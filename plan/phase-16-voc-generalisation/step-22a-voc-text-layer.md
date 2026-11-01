# Step 22a: The VOC text layer with the SG v5 rules unchanged

> **Phase:** 16, A second archive (1.04.02 VOC): what generalises. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phases 05–15 tuned the text layer on the States General (SG; 1.01.02, 1.10.94). Phase 16 runs the same generic
components on the VOC archive (NL-HaNA 1.04.02) **unchanged first**, and records what carries over. This step adds a
VOC preset that is the SG v5 preset with only the archive plumbing changed. It builds one layer over 568 VOC
inventories: the 22 with GLOBALISE document segmentation and 546 Generale Missiven (GM) inventories. It runs the
phase-13 audit unchanged and measures what the SG rules do on VOC. Steps 22b–22d read this layer. **No rule or
threshold is tuned here.**

## Inputs

All paths are relative to the AS root `/Users/Shared/claude_projects/data_scopes/archival-structures`, checked
2026-10-31.

- **PageXML (read-only, on the external drive):** `/Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1/<inv>.zip`
  (6,914 files). Each zip holds `<inv>/NL-HaNA_1.04.02_<inv>_<NNNN>.xml`. A hook denial or `Permission denied` under
  `/Volumes` is final: stop and ask.
- **Inventory list:** `reports/data/phase-16-voc-generalisation/planning/gm_split.tsv` (columns `inv layout split`; 547
  GM inventories). Drop `1171`: the drive has only `1171A.zip`/`1171B.zip`, and the gold names scans `1171_*`. The 22
  GLOBALISE inventories: 1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276
  8284 8697 8834. The two sets are disjoint, so the total is **546 + 22 = 568**. Write it, sorted with a (number,
  suffix) key (`1179B`, `1457A` carry suffixes), to `reports/data/phase-16-voc-generalisation/step-22a/inventories.txt`.
- **Code you call (unchanged):**
  - `archival_structures/text_layer/presets.py`: `LayerPreset` (dataclass, `presets.py:9-49`); `SG_1_01_02_V5`
    (`presets.py:240`, anchor `SG_1_01_02_V5 = dataclasses.replace(`); `_sg_source_pattern` (`presets.py:73`);
    `PRESETS` (`presets.py:252`); `inventory_preset(preset, mode)`.
  - `SG_1_01_02_V5` = `SG_1_01_02_V4` + hand `index_sides` + hand `index_entry`. It inherits from `SG_HAND_V2`:
    `format_version=2`, `archive_id="NL-HaNA_1.01.02"`, `source_pattern=_sg_source_pattern("1.01.02")`,
    `skip_resource_forks=True`, `wbc_pinned={"default": "-=„", "11281": "-:=„", "3349".."3354": "-„"}` and
    `join_lead_marks=","`. Its `volume_mode = {"method": "box_height", "params": BOX_HEIGHT_V4 (cut 87.75,
    min_region_chars 100), "split_gate": SPLIT_GATE_V4, "pins": MODE_PINS_V4}`. MODE_PINS_V4 holds 16 SG inventory
    numbers.
  - CLI `.venv/bin/as-build-text-layer` (`text_layer/build.py:874 main`): `--preset --pagexml-dir --out-dir
    --inventories-file --stage all --workers N`. With a v2 preset, `--stage all` runs extract → layout → types →
    units → wbc → join. Discovery is `io.discover_inventories_v2(pagexml_dir, preset.source_pattern)`: `fullmatch` on
    entry names, named group `inv`, dot-files ignored.
  - The audit: `.venv/bin/python reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer <base> --out
    <file>`. It discovers `<base>/*/NL-HaNA_*_text_layer_v*` and finds sources through the manifest's `--pagexml-dir`
    plus `PRESETS[preset].source_pattern`. It runs the v5 checks when the layer name ends in `_v5`. Read
    `n_fail_judged`.
  - Protected outputs: `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 22`. It writes `size=N
    MTIME PATH` lines for shared-data, LotA/NLP data and `data/text_layer_v2_dev/*`, and does not follow the VOC
    symlinks.
- **The planner ran this preset once** (2026-10-31) on 8023 and 2775: a runtime `dataclasses.replace` registered in
  `PRESETS`, `--stage all`, 1 worker, 18 s, `failures=0`. Measured there:
  - both volumes `hand` (box height 212 and 172 px against the SG cut 87.75);
  - every side `single` (portrait scans, about 2835 × 4121 px);
  - 2775 has 403 `page_number` segments, 363 of them `rule:pn_line_offchain`;
  - wbc reported `corpus default ['-', '=', '„'] (pooled rule ['„'])`; `¬` ends 16 lines (end_rel_frac 38.8);
  - output about 0.47 × the zipped input.

  These are planning numbers on 2 volumes, not expectations for 568.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/presets.py` | `VOC_SOURCE_PATTERN = r"(?P<inv>[0-9]+[A-Z]*)\.zip"`; `VOC_1_04_02_V5SG` (below); `PRESETS["voc_1.04.02_v5sg"]` |
| `tests/test_text_layer_voc_preset.py` (new) | the tests below |
| `docs/text_layer.md` | a short "VOC preset `voc_1.04.02_v5sg` (phase 16)" paragraph: what it is, that it is SG v5 unchanged, the layer location |
| `docs/shared_library/changelog.md` | `### Step 22a: VOC preset voc_1.04.02_v5sg` under `## Unreleased` (add the heading if the top section is a release), added with Edit; additive, no sibling import affected |
| `.gitignore` | nothing (no new script under `scripts/`) |
| `reports/data/phase-16-voc-generalisation/step-22a/` | `inventories.txt`, `stage_inputs.py`, `measure.py`, `audit.json`, the TSVs below, `protect_before.txt`/`protect_after.txt` |
| `reports/phase-16-voc-generalisation.md` | `### Step 22a: …` section and draft findings `F16-22a.n` |

## Behaviour to implement

**1. The preset.**

```python
VOC_SOURCE_PATTERN = r"(?P<inv>[0-9]+[A-Z]*)\.zip"
VOC_1_04_02_V5SG = dataclasses.replace(
    SG_1_01_02_V5, name="NL-HaNA_1.04.02_text_layer_v5", archive_id="NL-HaNA_1.04.02",
    scan_id_prefix="NL-HaNA_1.04.02", source_pattern=VOC_SOURCE_PATTERN, throughput_fit=None,
    wbc_pinned={"default": SG_1_01_02_V5.wbc_pinned["default"]},
    volume_mode={k: v for k, v in SG_1_01_02_V5.volume_mode.items() if k != "pins"},
    description="NL-HaNA 1.04.02 (VOC) text layer: the SG v5 rules unchanged (phase 16 step 22a)")
```

- Every **inventory-keyed** SG setting is dropped. VOC inventory numbers overlap SG ones, so a pin on `3349` would hit
  a different volume. That covers the `wbc_pinned` inventory keys and `volume_mode["pins"]`. Also check
  `join_lead_marks_lower`: if `SG_1_01_02_V5.join_lead_marks_lower` is not None, set it to None and say so in the
  report.
- Nothing else changes: `mode_presets`, `mode_overrides`, `split_gate`, layout/types/unit params, `join_lead_marks`.
- The preset key is `voc_1.04.02_v5sg`; the layer name ends in `_v5` so that the audit runs its v5 checks.

**2. Staging** (`step-22a/stage_inputs.py`): create `data/text_layer_voc_inputs/1.04.02/` with one symlink per
inventory in `inventories.txt`, `<inv>.zip → /Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1/<inv>.zip`. Stop if
any target is missing. `chmod g+w` the directories.

**3. The build.**
1. Check write access to `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/` (it was writable
   on 2026-10-31).
2. Create `…/data/text_layer_voc/22a-a/1.04.02/` and link it locally: `data/text_layer_voc_22a →
   /Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/22a-a`.
3. Run `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then launch through
   coreshare:

```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures \
  --label 22a --want 6 --min 2 --wait --expected-min 90 -- \
  .venv/bin/as-build-text-layer --preset voc_1.04.02_v5sg --pagexml-dir data/text_layer_voc_inputs/1.04.02 \
  --out-dir /Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/22a-a/1.04.02 \
  --stage all --workers '{workers}'
```

- **Run time.** Estimated 1–1.5 h wall, from the planner's 18 s per 22 MB zipped (13.5 GiB of GM zips + ~0.6 GiB).
  Run it with the Bash tool's `run_in_background`, not `nohup`/`&`, and check `progress.log` while it runs.
- **Resume.** Stages skip current inventories, so after an interruption you rerun the same command.
- **Expected output.** The layer root is `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/`.
- **Stdout noise.** In `print` mode the line splitter prints "some of content_lines have no ID" for VOC line ids
  (`line_split.py:617`). That is noise, not an error; count the volumes it happens in.

**4. The audit, unchanged.** Run `audit.py --layer data/text_layer_voc_22a --out
reports/data/phase-16-voc-generalisation/step-22a/audit.json`. Do not edit the audit:
- a judged failure that comes from an SG assumption is a **finding**;
- a judged failure that comes from a bug in the new preset is fixed, followed by a rebuild of the affected stage.

**5. Measurements** (`step-22a/measure.py`; read the layer with `text_layer.io.read_tsv_gz`, never whole files into
the chat). Write each table to `step-22a/`:
- `modes.tsv`: per inventory, `inv mode value n_chars scan_minority_share` from `mode/<inv>.json`. Report the
  box-height distribution (min/quartiles/max) for GM and GLOBALISE separately, and the count of `print` volumes.
- `sides.tsv`: per inventory, counts of `single`, `verso` and `recto` sides from `layout/pages/<inv>.tsv.gz` (or
  from `layout/lines` for volumes with no `pages` table). For the GM inventories, add `gm_layout` from `gm_split.tsv`.
  **Agreement:** a volume agrees when (`Enkel` and ≥ 90% `single`) or (`Dubbel` and ≥ 90% `verso`+`recto`). Report the
  agreement share and list the disagreeing volumes.
- `types.tsv`: per inventory, the `line_type` counts and the `page_number` `type_method` counts from
  `types/segments/<inv>.tsv.gz`. Pool them, and set them beside the SG hand reference. The reference is the
  per-inventory `methods` counts (keys `"<line_type>|<type_method>"`) in
  `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/types/stats.json`, pooled over that layer's
  hand volumes (`mode/<inv>.json` `"mode": "hand"`).
- `laypa_pn.tsv`: line-level agreement between the types `page_number` segments and lines in Laypa `page-number`
  regions. The Laypa tag is in `regions_meta/<inv>.tsv.gz`, column `region_tag`; lines join on `region_id`. Report
  P/R of "line has a `page_number` segment" against "line is in a `page-number` region". Laypa is **not gold**; call
  it agreement.
- `breaks.tsv`: line-final character counts of `lines/<inv>.lines.tsv.gz` `text` for `¬ - = „ :`, pooled and per
  inventory. Also give the count of `¬` still at a line end in `join/units` text (an unjoined break) per 1,000 lines.
- `index_flags.tsv`: per GLOBALISE inventory, flagged sides (`index_side == "1"`) against the scans whose gold
  `Type of non-document page` starts with "Table". The gold is in
  `reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv`, column `toc`; join on `scan_id` (no `.jpg` in
  either). Report counts only (TP/FP/FN at scan level, all 22). Step 22c owns the detector analysis, so draw no
  conclusion here.

## Constraints

- No change to `layout.py`, `types.py`, `units.py`, `modes.py`, `index_pages.py`, `page_structure.py` or the audit.
  No new preset field.
- Never `git commit`; never bump the version; `chmod g+w` everything you create (local and on the drive).
- **Drive:** read only `/Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1/`. Write only under
  `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/`.
- **Protected outputs:** run `protect.py reports/data/phase-16-voc-generalisation/step-22a/protect_before.txt --skip
  22` before the build, and `protect_after.txt` after it. Every difference must be explained (changes by other
  projects in `shared-data/` are expected; name them).
- Agent shell: no `rm -rf`, `nohup` or `&`; zsh (quote `'{workers}'`); no `sed -i`; never `git stash`/`checkout`/
  `reset`; a new output dir per run (`22a-b` if you must start over).
- Edit shared files (changelog, report, `STATUS.md`, `PROGRESS.md`) with Edit only.
- Do not open `planning/toc_peek_dev.tsv` or `planning/gm_proto_dev.tsv`.
- An error-analysis sample with fewer cases than asked: read all of them.

## Tests

`tests/test_text_layer_voc_preset.py`:
- `VOC_SOURCE_PATTERN` full-matches `1068.zip` (inv `1068`) and `1457A.zip` (inv `1457A`), and does not match
  `NL-HaNA_1.04.02_1068.zip`, `1068.tar.gz` or `1068`.
- `discover_inventories_v2(tmp_path, VOC_SOURCE_PATTERN)` on empty files `1068.zip`, `1457A.zip`, `._1068.zip` and
  `notes.txt` returns exactly `{"1068", "1457A"}`.
- `PRESETS["voc_1.04.02_v5sg"] is VOC_1_04_02_V5SG`. Fields:
  - `archive_id == "NL-HaNA_1.04.02"`, `format_version == 2`, `skip_resource_forks is True`;
  - `wbc_pinned == {"default": "-=„"}`, `"pins" not in volume_mode`, `volume_mode["params"]["cut"] == 87.75`;
  - `name.endswith("_v5")`.
- **Unchanged rules:** for `mode` in `hand` and `print`, every field in `MODE_RULE_FIELDS` of
  `inventory_preset(VOC_1_04_02_V5SG, mode)` equals the same field of `inventory_preset(SG_1_01_02_V5, mode)`.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_voc_preset.py`, then the full
  suite. Before this step (2026-10-31): 1,402 passed, 11 skipped (1,413 collected).

## Acceptance criteria

1. New tests and the full suite green.
2. The build covers all 568 inventories: `failures=0` at the end of `progress.log`, and one `mode/<inv>.json` and one
   `join/units/<inv>.tsv.gz` per inventory. Any failed inventory is listed with its error. Fewer than 568 is a
   result to report, not a reason to change rules.
3. Audit run unchanged, with `n_fail_judged` and every judged check reported. The text layer "runs unchanged" on VOC
   iff `n_fail_judged == 0`, as for SG. If it is > 0, classify each failing check as an SG assumption, a VOC data
   property or a preset bug. Fix preset bugs only.
4. The six measurement tables written, and summarised in the report with numbers.
5. Protect diff explained.

The user decides when the step is done.

## Error analysis

- **Print volumes:** read every `print` volume, up to 10. Is it printed? Open 2 scans' line texts from
  `lines/<inv>.lines.tsv.gz` and give `value`.
- **Sides:** read every volume that disagrees with the GM layout, up to 10. Give `page_w`/`page_h` of 3 scans from
  `scans/<inv>.scans.tsv.gz`, and say what the scan is (an opening photographed as one page, a fold-out, …).
- **Page numbers:** read 30 `page_number` segments (seed 20261031, over all volumes) and classify them as a folio
  number, a number in another role (year, amount, register number), or noise. Then read 20 lines in Laypa
  `page-number` regions with **no** `page_number` segment (same seed), and classify them the same way. Say whether
  the dominant `rule:pn_line_offchain` method marks real folio numbers.
- **Units:** read 10 sides (seed 20261031, GM volumes, sides with ≥ 10 lines): the units in order, from `units/units`
  and `join/units`. Classify each side: reading order right / interleaved / cut mid-sentence. Name the cause in VOC
  terms (marginalia, catch-words, columns).
- **Word breaks:** read 10 lines ending in `¬` and say whether the join kept or removed the mark.

Categories with counts and verbatim examples go into the report. Draft findings are expected for:
- the audit result;
- the hand/print outcome and box heights (the SG cut assumes one scan scale);
- side layout against the GM labels;
- the page-number types on VOC;
- word-break marks (the SG set lacks `¬`);
- whether the SG v5 index flags fire on the VOC tables of contents (counts only).

## Do not touch

`analysis/folio_map.py`, `analysis/index_links.py` (steps 22b–22d), every text layer under `data/text_layer_v2_dev/`
and `shared-data/`, the ground-truth files, the phase-13 audit script, NLP and LotA.

## When done

1. Add your `### Step 22a: The VOC text layer with the SG v5 rules unchanged` section to
   `reports/phase-16-voc-generalisation.md`, following `plan/templates/REPORT.md`. Add one row per finding to its
   *Findings* table (ID `F16-22a.n`, status `draft`). Don't edit other steps' sections or the *Summary*/*Review*
   sections. Run `make reports` from the AS root and fix any warning.
2. Append the `PROGRESS.md` block (template in `plan/templates/STEP.md`, *When done*).
3. Overwrite the phase `STATUS.md`. Record under *Facts later steps need*:
   - the layer root;
   - the inventory count built;
   - the `print` volumes, if any;
   - the passed/skipped test counts;
   - the sides agreement.
