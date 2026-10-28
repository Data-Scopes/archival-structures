# Step 19e: v5 preset, dev build, audit and run kit

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** 19c (detector), 19g (entry rule), 19f
> (only if UA-043 says ship it). **Re-planned** by the review of 19f/19g (Opus, 2026-10-28); replaces the stale brief.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Gate: start only when this holds (else stop and say which UA is open)

Run `grep -n -A1 '^### UA-\|Response:' docs/USER_ACTIONS.md`.
1. **UA-039** (blind check) has a **Response**. (a) and `reports/data/phase-13-register-index-volumes/step-19a/
   check_verdicts.json` exists: compare it with `step-19a/side_verdicts.tsv` on the 30 items (user `Index`/`Not index`
   against agent `index`/`not index`; `Unsure` and agent `unclear`/`unreadable input` left out); report agreement and
   every disagreement; agreement < 0.90 → stop, add a Decide UA (the gold and bar D2 are in doubt). (b) → the report
   says the side gold is unchecked. Empty → **do not start**.
2. **UA-043** (column reorder): Response (a) or empty → **branch A** (no reorder, the default). (b) → **branch B**
   (reorder in v5). (c) → stop (a fresh read is not part of this step).
3. **UA-044** (Parquet reader, optional silver check): (a) or (b) → do step 7 with that reader; (c) or empty → skip
   step 7 and say so.
Move every UA you acted on to `docs/USER_ACTIONS_resolved.md` (date, what was done), with Edit.

## Context

Phase 13 tested three hand-mode rules against bars fixed in advance. **Passed:** the index-side detector
(`LayoutParams.index_sides`, preset `SG_INDEX_SIDES_V5`, table `layout/pages/`; D1 P 0.9711 R 0.9490, D2 54/60) and
the index entry rule v2 (`UnitParams.index_entry`, preset `SG_INDEX_ENTRY_V5B`; fresh test P 0.9530, R 0.8974, F1
0.9244 against v4 hand 0.5009). **Failed:** 19b's column split (B1 0.763), 19d's E5 (entry bar), 19f's reorder (B2
35/39 = 0.897). Published layers are never rebuilt (D17), so v5 is a new layer version: `sg_1.01.02_v5` = `sg_1.01.02_v4`
plus a hand override. This step defines it, builds v5 of the 231 local 1.01.02 volumes, compares it with v4, audits
it, documents it, extends the run kit and writes the UAs for a full run and a pilot. 1.10.94 gets no v5.

## Inputs (checked 2026-10-28)

- `archival_structures/text_layer/presets.py`: `SG_HAND_LAYOUT_PARAMS` (anchor `SG_HAND_LAYOUT_PARAMS =`, L90; equals
  the hand mode preset's `layout_params`); `SG_INDEX_SIDES_V5`, `SG_HAND_COLUMN_ORDER_V5`, `SG_INDEX_NO_CUT`,
  `SG_INDEX_ENTRY_V5B` (L96–120); `SG_1_01_02_V4 = dataclasses.replace(SG_HAND_V3, …, mode_overrides={"print":
  {"unit_params": {**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}}})` (L228); `PRESETS = {…}` (L243);
  `inventory_preset(preset, mode)` (L248) copies the six `MODE_RULE_FIELDS` from `PRESETS[preset.mode_presets[mode]]`
  (hand → `sg_1.01.02_v3`, whose `unit_params` is `None`) and replaces **whole fields** from `mode_overrides[mode]`.
- `scripts/run_text_layer_v4_full.py`: `ARCHIVES = {"1.01.02": "sg_1.01.02_v4", "1.10.94": "sg_1.10.94_v4"}` (L113);
  `link_archive` (L208, source pattern from `PRESETS[ARCHIVES[archive]]`); `build_cmds` (L307; `--preset
  ARCHIVES[archive]`; coreshare label `f"v4-{archive}"` L319); `archive_pair` (L331); `parse_args` (L339; `--only`
  choices `sorted(ARCHIVES)`); `main` (L373; `PRESETS[ARCHIVES[archive]].name` at L394, L447). Tests:
  `tests/test_run_text_layer_v4_full.py` (18). `tests/test_text_layer_modes.py` (14; `test_v1_v3_presets_unchanged`
  L235, `test_v4_pins` L424). Full suite: 1,365 collected, 1,354 passed, 11 skipped (2026-10-28).
- Sources (231 = 14 + 182 + 35): `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02` (14 dirs),
  `data/text_layer_v4_staged/NL-HaNA_1.01.02` (182 tars), `data/text_layer_v4_staged_15z/NL-HaNA_1.01.02` (37 tars;
  **3463 and 9486 are also in the staged dir: never link a tar twice**).
- v4 references (read only; sub-path `1.01.02/NL-HaNA_1.01.02_text_layer_v4/`): `data/text_layer_v2_dev/15y-b` (196
  volumes) and `15z-b` (37; it wins for its volumes). Hand lists: `reports/data/phase-13-register-index-volumes/planning/
  hand_15y-b.txt` (111) and `hand_15z-b.txt` (14) = the 125 hand volumes. Phase-13 clones (same sub-path): `19g-a`/`19g-z`
  (v4 layout + `index_sides` + units with V5B = **branch A's expected hand units**), `19f-a`/`19f-z` (reorder layout,
  types and v4 hand units = **branch B's expected layout and types**).
- Scripts to copy and adapt: `reports/data/phase-12-hand-print-box-height/step-15w/audit.py` (`--layer <base> --out F`,
  `--self-test`; read `n_fail_judged`); `reports/data/phase-13-register-index-volumes/step-19f/interleave.py` (usage in
  its docstring); `step-19d/entry_eval.py` (start extraction `starts_from_units`, `predict_table`, `score`);
  `step-19g/entry_eval_v2.py` (`sides_select`, `sides_fresh`); `step-19g/units_stats.py`.
- Gold (spent; scoring only, no bar): 30 sides `step-19a/entry_sides.tsv` + `entry_gold.tsv`; 20 sides
  `step-19g/fresh_sides.tsv` + `fresh_gold.tsv` (side metadata in `review/fresh_entry_sides.tsv`).
- Silver (step 7 only): `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/
  lines/inv=<inv>/part-0.parquet`, columns `scan_id`, `page_id`, `unit_type`. `page_id` = `<scan_id>-page-<n>`: `n` =
  2·scan − 2 is the verso, 2·scan − 1 the recto (3265: scan 0047 page 93 recto, 0048 page 94 verso). 27 of the 125
  hand volumes are in it. Resolution pages only, never index pages. Scoring only, never a feature.
- `docs/text_layer.md`: `## Text layer v4 (phase 12)` (L466); its pin paragraph ends with the known issue "a `hand` pin
  on an index volume … does not cut at the entries" (L542–546); `## Published SG pilot (text_layer_v2)` (L586).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/presets.py` | `SG_1_01_02_V5`, `"sg_1.01.02_v5"` in `PRESETS` |
| `scripts/run_text_layer_v4_full.py` | `--version {v4,v5}` (default `v4`; v4 behaviour unchanged) |
| `tests/test_text_layer_modes.py`, `tests/test_run_text_layer_v4_full.py` | tests below |
| `reports/data/phase-13-register-index-volumes/step-19e/` | `user_check.py`, `compare_v4.py`, `audit.py` (copy), `metrics.py`, `silver.py` (if step 7), their JSON/TSV outputs, `protect_*.txt` |
| `docs/text_layer.md` | `## Text layer v5 (phase 13)` before *Published SG pilot (text_layer_v2)*; the v4 known issue points to it |
| `docs/shared_library/changelog.md` | `### Step 19e: text layer v5 preset and run kit` under *Unreleased* (Edit) |
| `docs/USER_ACTIONS.md` | two Decide UAs (full run; pilot), next free IDs (UA-045 onward on 2026-10-28) |
| `reports/phase-13-register-index-volumes.md` | `### Step 19e: …` section (the heading exists, empty), findings `F13-19e.n` |

## Behaviour to implement

```python
# Step 19e (phase 13): text layer v5 = v4 + a hand override. Bars: detector D1 P 0.9711 R 0.9490, D2 54/60 (19c);
# entry rule v2 on fresh gold P 0.9530 R 0.8974 F1 0.9244 vs v4 hand 0.5009 (19g). Not in v5: 19b column split
# (B1 0.763), 19d E5 (entry bar failed), 19f reorder (B2 0.897; UA-043 (a)).
SG_1_01_02_V5 = dataclasses.replace(
    SG_1_01_02_V4, name="NL-HaNA_1.01.02_text_layer_v5",
    description="NL-HaNA 1.01.02 text layer v5 (v4 + index page sides and index entry units in hand volumes)",
    mode_overrides={**SG_1_01_02_V4.mode_overrides,
                    "hand": {"layout_params": {**SG_HAND_LAYOUT_PARAMS, "index_sides": SG_INDEX_SIDES_V5},
                             "unit_params": {"index_entry": SG_INDEX_ENTRY_V5B}}})
```
**Branch B** only: add `"hand_column_split": SG_HAND_COLUMN_ORDER_V5` to the hand `layout_params`, say "column order"
in the description, and change the comment to "19f reorder shipped by UA-043 (b) although B2 failed (0.897)". Register
`"sg_1.01.02_v5": SG_1_01_02_V5` in `PRESETS`.

Run script: `ARCHIVES_BY_VERSION = {"v4": {"1.01.02": "sg_1.01.02_v4", "1.10.94": "sg_1.10.94_v4"}, "v5": {"1.01.02":
"sg_1.01.02_v5"}}`; keep `ARCHIVES` (= the v4 map; tests and scripts import it); `--version {v4,v5}` (default `v4`);
`archive_pair` and `--only` accept the union; after parsing, an archive not in the chosen map is an argparse error
(`ap.error`); every `ARCHIVES[archive]` use takes the chosen map; the coreshare label becomes `f"{version}-{archive}"`.
The throughput constants stay v4's (say so in the docstring).

## Procedure (from the AS root; zsh)

1. Gate. Check write access: `ls -ld data data/text_layer_v2_dev reports/data/phase-13-register-index-volumes`.
   Protect `_before`: `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
   reports/data/phase-13-register-index-volumes/step-19e/protect_before.txt --skip 19b 19c 19d 19e 19f 19g` and
   `find data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/15z-b data/text_layer_v2_dev/19f-a data/text_layer_v2_dev/19f-z
   data/text_layer_v2_dev/19g-a data/text_layer_v2_dev/19g-z data/text_layer_v4_full -type f | sort | xargs shasum -a 256 >
   reports/data/phase-13-register-index-volumes/step-19e/protect2_before.txt`.
2. Code, tests (below), suite green (counts with `pytest --collect-only -q`).
3. Sources: `mkdir -p data/text_layer_v5_19e_src15z/NL-HaNA_1.01.02 data/text_layer_v5_19e_inputs`; one absolute symlink
   per tar of `data/text_layer_v4_staged_15z/NL-HaNA_1.01.02` except 3463 and 9486 (35 links); `chmod -R g+w` both.
   `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then
   `.venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --src
   1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --src
   1.01.02=data/text_layer_v5_19e_src15z/NL-HaNA_1.01.02 --work-dir data/text_layer_v5_19e_inputs --out-dir
   data/text_layer_v2_dev/19e-a --check-only`: expect 231 inventories, no duplicate. Then the same without
   `--check-only`, with `--want 4 --min 2` (the script calls coreshare itself). It runs an hour or more: use the Bash
   tool's background mode (never `&` or `nohup`) and wait. Layer root: `data/text_layer_v2_dev/19e-a/1.01.02/
   NL-HaNA_1.01.02_text_layer_v5/`. `chmod -R g+w data/text_layer_v2_dev/19e-a`.
4. `compare_v4.py`: per inventory and per-inventory file, v5 against the v4 reference (`15z-b` for its 37 volumes, else
   `15y-b`): `.gz` decompressed, `.json` without the keys `seconds`, `elapsed*`, `source`. Expected:
   - **print** inventories (106): equal in every per-inventory file;
   - **hand** (125), branch A: equal in `lines/`, `scans/`, `regions_meta/`, `mode/`, `extract_stats/`,
     `layout/{scans,regions,lines}`, `types/`; new `layout/pages/`; `units/units` and `units/segments` **equal to
     `19g-a`/`19g-z`** (v4's differ only on flagged sides); `join/` and `wbc/` per-inventory files: list the differences;
   - hand, branch B: `layout/{regions,lines}` and `types/` equal to `19f-a`/`19f-z`; units: no reference (list);
   - layer-level files (`*/params.json`, `*/stats.json`, corpus wbc files, `manifest.json`, `status.tsv`): listed, not
     judged. Any other difference: explain it from the code or stop and ask.
5. Audit: copy `step-15w/audit.py` to `step-19e/audit.py`; `--self-test`, then `--layer data/text_layer_v2_dev/19e-a
   --out reports/data/phase-13-register-index-volumes/step-19e/audit_19e-a.json`. If it fails on the layer name or
   preset key (written for v4), make the smallest change in the copy and record it. Add v5 checks: every hand
   inventory has `layout/pages/<inv>.tsv.gz` and no print inventory has one; no duplicate `layout_region_id` within a
   scan; every `hand:index*` `unit_method` sits on a page side with `index_side == "1"`; no unit of type `index` on a
   flagged side.
6. `metrics.py` (one output prefix per part): (a) interleave (`step-19f/interleave.py`) over the 125 hand volumes of
   `19e-a` (expected A: 2,612 of 399,714; B: 843 of 399,429); (b) flagged sides and their lines per volume (index volumes,
   registers' front sections, others); (c) main_text units on flagged sides, v4 against v5 (expected A: 80,344 to 162,021),
   and `unit_method` counts; (d) end to end on the 50 spent gold sides with the v5 units (19d's `starts_from_units` and
   `score`; 30 from 19a, 20 from 19g, scored separately; no bar). Expected A: 19a sides P 0.8768 R 0.9329; 19g sides
   P 0.9530 R 0.8974; B (19g sides): P 0.9552 R 0.8951. A different number means the build differs from the clones:
   find out why before going on.
7. *Optional, per UA-044.* `silver.py`: for each hand volume of `19e-a` with a `republic_segmentation_v1` partition, count
   per page side (from `page_id`, rule above) the lines whose `unit_type` is `resolution_paragraph`, `session_opening`,
   `attendance_list` or `other_session_part`; a side with ≥ 5 is a silver negative. Report per volume: silver negatives,
   how many are flagged (`index_side == "1"`), and of those how many by `rule` (1) or by gap fill (0). Read 5 flagged
   silver negatives (`random.Random(20261129)`) in the text. No bar. With UA-044 (b) the reading half runs with
   `PYTHONNOUSERSITE=1 /Users/Shared/claude_projects/data_scopes/language-of-the-archive/venv/bin/python -I` and writes
   `step-19e/silver_sides.tsv`; the scoring half runs with `.venv/bin/python`. Never write under `shared-data/`.
8. Docs, changelog, UAs, report; `make reports` clean; protect `_after`; `diff` both lists.

## Tests

`tests/test_text_layer_modes.py`: `test_v5_preset`: `PRESETS["sg_1.01.02_v5"].name == "NL-HaNA_1.01.02_text_layer_v5"`;
`inventory_preset(v5, "print")` equals `inventory_preset(v4, "print")` in all `MODE_RULE_FIELDS`; `inventory_preset(v5,
"hand")` has `layout_params["index_sides"] == SG_INDEX_SIDES_V5`, `merge` False, `unit_params == {"index_entry":
SG_INDEX_ENTRY_V5B}`, and (branch A) no `hand_column_split`; `volume_mode` (pins, cut, split gate) equals v4's. The existing
v1–v4 tests stay green. `tests/test_run_text_layer_v4_full.py`: `--version v5` builds commands with `--preset
sg_1.01.02_v5` and label `v5-1.01.02`; `--version v5 --only 1.10.94` is an error; without `--version` every existing test
passes unchanged.

## Docs (`## Text layer v5 (phase 13)`)

What v5 changes and why (F12-R.21, F12-R.22: hand units on index pages merged many entries); the preset; the rules in
v5 with their parameters and bar results (D1, D2, entry bar v2) and those left out (B1, E5, B2; branch B: the reorder and
that its bar was overridden); `layout/pages/` (columns `scan_id, page, n_lines, n_text8, n_text20, col_share, end_num,
date_start, rule, index_side`; `rule` = the side rule, `index_side` = after gap fill 8); the `unit_method` values
`hand:index_start` and `hand:index:<cue>+<cue>`; on flagged sides `index`-typed lines are in `main_text` units while
`types/segments` keeps `line_type` `index`. **Known limits:** entries cut after an amount or a short cross-reference
line (F13-19g.4); entry starts in lines typed `marginalia` on fragmented sides are in no main unit (F13-19g.3, F13-R.12);
a detector false positive (a roster with page numbers, 4831; account pages) cuts that side at entry cues; one-column and
garbled indexes are missed (F13-19c.5); a `date_header` unit followed by its entry is one entry; gutter-merged lines.
The build command and `--version v5`; the v4 → v5 differences from `compare_v4.py`. Point the v4 known issue (L542–546)
to this section.

## UAs (append to `docs/USER_ACTIONS.md` with Edit; format of the file's existing entries)

1. **Full v5 run of 1.01.02** (Decide + Run): (a) run on the drive like UA-024: give the exact command with `--version
   v5`, the `--out-dir` and work dir on the drive, and the expected time = the v4 full run's time × the v5/v4 ratio of
   the dev build's stage seconds (from `status.tsv` of `19e-a` against `15y-b`); (b) not now. Recommended: (a) if the
   user wants v5 for all 9,124 volumes. Default: nothing runs. Blocks: a full v5 layer.
2. **Publish a v5 pilot in `shared-data/`** (Decide): (a) publish `19e-a` as `shared-data/NL-HaNA_1.01.02/text_layer_v5/`
   (a later session copies it, edits the README and the changelog line, as with UA-038); (b) wait for the full run;
   (c) not at all. Recommended: (a), so LotA can read index entries now. Default: nothing published. Blocks: LotA's
   use of v5.

## Acceptance criteria

1. Gate passed and recorded (UA-039 agreement or "unchecked"; branch A or B; silver yes/no). Suite green.
2. `--check-only`: 231 inventories; the build: every stage's last row `done` for 231 inventories.
3. `compare_v4.py`: as expected in step 4; branch A: hand `units/` equal to `19g-*` for all 125.
4. Audit: 0 judged failures; the v5 checks pass; reported shares listed.
5. Metrics (a)–(d) reported and equal to the expected numbers (or the difference explained); (d) has no bar.
6. Docs, changelog, two UAs; protected lists unchanged apart from the new dirs and other projects' `shared-data/`
   changes (name them).

The user decides when the step is done.

## Error analysis

Read, in full with their lines: 10 v5 units on flagged sides of volumes outside both gold sets (`random.Random(20261122)`
over their sorted ids): one entry each? Classify (one entry, two entries, part of an entry, heading, not an index).
5 units on flagged sides that the gap fill flagged (`rule == 0`, seed 20261123): an index page, or a resolution page cut
at entry cues (harm)? The 3 volumes with the largest relative change in main_text units: one flagged side each. Draft
findings: the end-to-end numbers, units on flagged sides, gap-fill harm, the silver result if run, compare surprises.

## Do not touch

The rule code of 19b–19g (report a defect, do not fix it), published layers, `data/text_layer_v4_full/`, every dev layer
except `19e-a`, the 19a–19g step data and the review files, `shared-data/`, the siblings. Never `git commit`, never `git
stash`/`checkout`/`reset`; `chmod g+w`; `.venv/bin/python`; zsh (quote `'{workers}'` before a redirect); no `rm -rf`,
`nohup`, `&`; shared files (changelog, `reports/index.md`) only with Edit. Never touch the external drive. Do not install
packages unless UA-044 says (a).

## When done

1. Report: `### Step 19e: v5 preset, dev build, audit and run kit` (gate, branch, what changed, compare, audit, metrics,
   silver, error analysis; findings `F13-19e.n` as `draft`); `make reports` clean.
2. Changelog (Edit): `presets.SG_1_01_02_V5`, `"sg_1.01.02_v5"`, the run script's `--version` and
   `ARCHIVES_BY_VERSION`. "Affected sibling imports: none (LotA reads published layers only; a v5 pilot is UA-0NN)."
3. `PROGRESS.md` block; overwrite `STATUS.md`: 19e done, next = phase review (prompt 3 of `plan/PROMPTS.md`), open UAs.
