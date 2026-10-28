# Step 15y: mode pins, in-place rebuild, resume debts, throughput model

> **Phase:** 12, Hand and print by line-box height (post-review addendum). **Depends on:** 15x (reviewed), the full v4
> run (UA-024, staged in `data/text_layer_v4_full/`, checked 2026-10-27). **Model:** Sonnet. **Read:**
> `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this file. **Read nothing else unless
> this brief names it.** Anchors checked 2026-10-27 (Opus planning). Record any exploration under *Brief defects*.

## Context

The full v4 run built 9,122 volumes of 1.01.02 with one mode each. Some handwritten index volumes sit below the
87.75 px cut and were built `print`. Two printed *plakkaten* sit just above it and were built `hand`. Decision P21
(UA-028 (a)) fixes these with **per-inventory pins** in the preset and a rebuild of only those volumes. This step builds
the mechanism and the in-place rebuild, and pins the one volume already read: 3463, read handwritten 5/5 in 15w
(`step-15w/verdicts.tsv`; F12-R.9: its 106 splits are wrong). The next step, 15z, reads the other 36 and adds their pins.
A correct in-place rebuild needs the stages to know when their output is stale, which is also **debt 1**: layout and
types have no done-skip, so the resume re-ran them for all 9,122 volumes (3.3 h). **Debt 2**: the run summary counts
old failed rows. Last, fit a **throughput model** on the full run's `status.tsv` to replace the dev-build constants.

## Inputs

- `text_layer/modes.py`: `decide_mode(line_rows, preset) -> dict` L26–42 (`box_height` copies the measured result
  `r`; `fixed` stores `r` as `box_height_mode`/`box_height_method` and sets `mode`, `method="fixed"`; other methods
  raise `ValueError`); `extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False)` L117–145:
  `decide_mode(rows, preset)` L123, resolve, split gate L127–133, then a split extract with
  `line_split._write_split_files` (`split_model/<inv>.json`, `split_stats/<inv>.json`, `split_stats/<inv>.splits.json`
  if `record_splits`) or an extract without split.
- `text_layer/presets.py`: `BOX_HEIGHT_V4` L183, `SPLIT_GATE_V4` L186, `SG_1_01_02_V4` L190–197 (`volume_mode={"method":
  "box_height", "params": BOX_HEIGHT_V4, "split_gate": SPLIT_GATE_V4}`); `predict_seconds(xml_bytes, stage, fit)` L225,
  `predict_wall(per_inventory_xml_bytes, workers, fit, stages=(...))` L230 (longest-first; `fit[stage] =
  {"intercept_s", "s_per_mb"}`, MB = bytes / 1e6).
- `text_layer/build.py`: `STATUS_COLUMNS` L113 (`inventory_number, stage, status, n_rows, n_lines, n_scans, n_errors,
  seconds, input_bytes, input_xml_bytes, finished_utc, error`); `Run.status()` L156 (latest row per `(inv, stage)`;
  appended, so file order is time order); `is_done(status, inv, stage, force)` L279; `stage_extract` L284 (done iff
  `is_done` and four outputs exist); `stage_layout` L371 (writes `layout/params.json` L390, loops all L394, no skip);
  `stage_types` L418 (same, loop L441); `stage_units` L463 (`params.json` = `{**params_doc, "republic_dir":
  republic_used}` L504, loop L508); `stage_wbc` L553 (parts skip L577–578); `stage_join` L701 (`current` L734–741);
  `main` L775 (v2 `--stage all`: extract, layout, types, units, wbc, join). `*/stats.json` are dicts keyed by inventory.
- `text_layer/units.py`: `unit_inventory(inv, layer_root, params, *, print_mode=False, republic_path=None)` L420;
  crosswalk `units/republic_lines/<inv>.tsv.gz` only for print with an existing path (L499–510).
- `scripts/run_text_layer_v4_full.py`: constants L90–107 (`ARCHIVE_XML_RATIO = 4.2`, `OUTPUT_PER_XML_BYTE`,
  `WORKER_SECONDS_PER_XML_BYTE`, `SLOW_FACTOR = 3`); `predict(archive, xml_sizes, want)` L207; `archive_summary` L228
  (counts **every** non-`done` row: "6 failed or unfinished rows" for a complete run); `build_cmds(archive, args,
  work_dir, out_dir, subset_file)` L244 → `(estimate_cmd, coreshare_cmd)`; `parse_args` L268; `main` L297. Tests
  `tests/test_run_text_layer_v4_full.py` (`test_predict_range` L110, `test_build_cmds_per_archive` L137).
- Audit `reports/data/phase-12-hand-print-box-height/step-15w/audit.py`: `METHODS` L46; `audit_v4` L267–313 (`print`
  iff `value <= cut` only for `box_height`; `split_model` iff print and gate not false); `self_test` L380.
- Tests `tests/test_text_layer_modes.py` (`_rows` L223, fixture `src` L228 = SG inventory `9999`, `test_decide_mode`
  L293, `_fixed` L306); `tests/test_text_layer_build.py` L54. Baseline 2026-10-27: **1,312 collected, 1,301 passed, 11 skipped**.
- Full run (read only): `data/text_layer_v4_full/{1.01.02,1.10.94}/NL-HaNA_*_text_layer_v4/` (`status.tsv`: 72,976
  rows for 1.01.02, last row `done` for 9,122 × 6 stages, layout and types twice; `mode/`); `data/text_layer_v4_full/run_v4.log`
  ("coreshare: granted N workers to archival-structures/v4-<archive>", the final "summary [1.01.02]: … output 39,427
  MiB", "summary [1.10.94]: … output 84 MiB"). The log names a drive: **never open, list or quote that path**.
- `data/text_layer_v2_dev/15x-a/` (v4 reference, protected; its `status.tsv`, `run_v4.log` time that build); sources
  `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02` (14), `data/text_layer_v4_staged/NL-HaNA_1.01.02` (182, with 3463).
  Comparer `reports/data/phase-12-hand-print-box-height/step-15v/compare.py base_a base_b --sub-map A:B,...
  [--explained-invs i,j] [--out F]` (decompressed bytes, JSON without `seconds`/`elapsed*`, skips `manifest.json`,
  `progress.log`, `status.tsv`).

## Deliverables

| File | Change |
|---|---|
| `text_layer/modes.py` | `decide_mode(line_rows, preset, inv=None)`: pins; `extract_inventory_v2_modes` passes `inv` and removes stale split files |
| `text_layer/presets.py` | `MODE_PINS_V4 = {"3463": "hand"}`; `SG_1_01_02_V4.volume_mode["pins"] = MODE_PINS_V4` |
| `text_layer/build.py` | done-skip for layout, types, units; staleness against the upstream stage for layout, types, units, wbc parts, join (v2 only) |
| `text_layer/units.py` | `unit_inventory` removes a stale `units/republic_lines/<inv>.tsv.gz` when it writes no crosswalk |
| `scripts/run_text_layer_v4_full.py` | `--rebuild ARCHIVE=F`; `archive_summary` by last row; the fitted throughput model; docstring |
| `step-15w/audit.py` | method `pinned`; self-test with a pinned build |
| tests | below |
| `reports/data/phase-12-hand-print-box-height/step-15y/` | `pins.txt`, `throughput.py`, `throughput.json`, `throughput.tsv`, `same_inv.py`, compare/audit outputs, protect lists, reads |
| `data/text_layer_v2_dev/15y-a/`, `15y-b/` | fresh 3463 build; in-place rebuild of a copy of `15x-a` |
| `docs/text_layer.md` | resumability (L83), the subset rule (L89), v4: pins, `mode/` tables, *Rebuilding pinned volumes* |
| `docs/shared_library/changelog.md` | add `## Unreleased` above `## v0.16.0`, then `### Step 15y: …` (Edit) |
| `reports/phase-12-hand-print-box-height.md` | `### Step 15y: …` after § Step 15x; findings `F12-15y.n` |

## Behaviour to implement

**Pins (P24).** `volume_mode` may hold `"pins": {inv: "hand"|"print"}`. In `decide_mode`, after the method branch: if
`inv` is in the pins, set `result["box_height_mode"], result["box_height_method"] = r["mode"], r["method"]` (the
measured result, also for `fixed`), then `result["mode"], result["method"] = pin, "pinned"`. A pin value outside
`{"hand", "print"}` raises `ValueError`. `inv=None` or no `pins` key: today's behaviour. The split gate then follows the
pinned mode, as it does now (a hand pin means no split, no `split_gate` key). `extract_inventory_v2_modes` calls
`decide_mode(rows, preset, inv=inv)`.

**Stale per-inventory files.** In `extract_inventory_v2_modes`, when the inventory is extracted **without** a split,
unlink `split_model/<inv>.json`, `split_stats/<inv>.json` and `split_stats/<inv>.splits.json` if they exist. With a split
and `record_splits=False`, unlink `split_stats/<inv>.splits.json` if it exists. In `unit_inventory`, when no crosswalk
is written, unlink `units/republic_lines/<inv>.tsv.gz` if it exists. A fresh build is unchanged.

**Staleness (P25, v2 only; v1/VOC paths byte-identical).** `Run.status()` adds `"_seq"` (0-based data-row index in
`status.tsv`) to each row it returns. New in `build.py`:

```python
# Step 15y: a v2 stage reads the output of the stage before it; its done row counts only if it is newer
V2_UPSTREAM = {"layout": "extract", "types": "layout", "units": "types", "wbc": "units", "join": "units"}

def is_current(status, inv, stage, force):
    """Done, not forced, and recorded after the latest row of the stage it reads (v2)."""
    if not is_done(status, inv, stage, force):
        return False
    up = status.get((inv, V2_UPSTREAM.get(stage)))
    return up is None or status[(inv, stage)]["_seq"] > up["_seq"]
```

- `stage_layout`, `stage_types`, `stage_units`: read the existing `<stage>/params.json` **before** writing it. If it
  exists and differs from the new document (compare `json.loads` of the file with `json.loads(json.dumps(new))`), log
  `[<stage>] parameters changed: redoing all` and skip nothing. Otherwise skip an inventory when `is_current(...)` and
  its outputs exist (layout: `layout/{lines,regions,scans}/<inv>.tsv.gz`; types: `types/{regions,segments}/<inv>.tsv.gz`;
  units: `units/{units,segments,doc_id_map}/<inv>.tsv.gz`). Log `[<stage>] N already done, M to do` as extract does.
  Skipped inventories keep their `stats.json` entry and get no new status row. Note: wordfix writes no status rows, so a
  wordfix run needs `--force` on layout (no preset uses wordfix); say so in the docstring.
- `stage_wbc` (v2): a part is skipped only if `is_current(status, inv, "wbc", force)` and it exists. `stage_join` (v2):
  `current` also requires `is_current(status, inv, "join", force)` in place of `is_done(...)`.

So after `--stage extract --force --inventories-file F`, a plain `--stage all` over the whole layer redoes layout, types,
units, the wbc part and the join of F only, re-decides wbc over every inventory, and leaves every other file untouched. A
resume after a crash redoes nothing that is done (debt 1).

**Run script.** (1) `--rebuild ARCHIVE=F` (repeatable like `--inventories-file`; `#` lines ignored by `read_subset`). It is
an error (exit 2, message) to give it together with `--inventories-file` for the same archive or with `--limit`, if an
inventory of F is not among the links, or if `<out-dir>/<archive>/<layer>/status.tsv` does not exist (`<layer>` =
`PRESETS[ARCHIVES[archive]].name`). For such an archive the build runs **two** coreshare commands: first `… --stage
extract --force --inventories-file <work-dir>/<archive>_rebuild.txt --workers {workers}` (the run script writes F's
inventories there, one per line, without `#` lines: the build's own `--inventories-file` does not skip comments), then
today's `--stage all` command without a subset; the log
line says `[<archive>] rebuild: N of M inventories (forced extract, then every stage over the layer)`. Prediction, space
check and estimate use F's inventories. `build_cmds(..., rebuild_file=None)` returns `(estimate, [cmds])`.
(2) `archive_summary`: keep the last row per `(inv, stage)` per `status.tsv`; done = inventories whose last rows are all
`done` and cover the most stages; bad = pairs whose last row is not `done`. (3) The throughput model below. Update the
docstring: an *In-place rebuild of pinned volumes* paragraph and the new prediction.

**Throughput model (P27).** `step-15y/throughput.py` reads the last `done` row per `(inv, stage)` of a `status.tsv`, MB
= `input_xml_bytes` / 1e6. 1.01.02: per stage an OLS fit `seconds = intercept_s + s_per_mb × MB` over all 9,122 (both
modes pooled; it carries the run's mode mix), plus per-mode fits for the report. 1.10.94 (2 inventories): `intercept_s =
0`, `s_per_mb = Σ seconds / Σ MB`. Planning check (measured): extract hand 0.0917 s/MB (R² 0.986), print 0.277 (0.886);
layout hand 0.0199; print byte share 0.120; XML/stored 4.2393. Write `throughput.json`
and `throughput.tsv` (archive, stage, mode, n, intercept_s, s_per_mb, r2, worker_h, measured_span_h, predicted_span_h).
In the run script replace `WORKER_SECONDS_PER_XML_BYTE` and `SLOW_FACTOR` with `THROUGHPUT_V4 = {archive: {stage: {...}}}`
(the pooled fits, rounded to 4 significant digits, with a comment naming `throughput.json`), `POOL_STAGES = ("extract",
"wbc", "join")`, `SERIAL_STAGES = ("layout", "types", "units")` (these run in one process). `predict(archive, xml_sizes,
want, min_workers)` → `(out_bytes, low_s, high_s)`: wall = Σ over POOL_STAGES of `predict_wall(sizes, w, fit,
stages=(s,))` + Σ over SERIAL_STAGES and inventories of `predict_seconds`, with `w = want` for `low_s` and `w =
min_workers` for `high_s`. Set `ARCHIVE_XML_RATIO` = Σ `input_xml_bytes` / Σ `input_bytes` of the 1.01.02 extract rows
(2 decimals) and `OUTPUT_PER_XML_BYTE` = summary output MiB × 2²⁰ / Σ `input_xml_bytes` per archive (4 decimals).
**Validation, no bar:** measured span per stage = max(`finished_utc`) − min(`finished_utc` − `seconds`) over its last
rows. Predict it with the worker count granted to that pass (from `run_v4.log`), in sample (full run) and out of sample
(`15x-a`). Report both, and the share of the run's wall time spent in SERIAL_STAGES. Then read residuals (below).

## Constraints

- Hand rules, print rules, the cut, the gate and `drop_cap_lines` unchanged; `MODE_RULE_FIELDS` unchanged; v1–v3 presets
  and output unchanged; `sg_1.10.94_v4` gets no pins. No parallel layout/types/units (deferred by the user, P29).
- Protected (hash before and after; from the AS root; `mkdir -p reports/data/phase-12-hand-print-box-height/step-15y` first):
  `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
  reports/data/phase-12-hand-print-box-height/step-15y/protect_before.txt --skip 15y` (and `_after`), plus
  `find data/hand_print_geom data/text_layer_v4_full reports/data/phase-10-hand-print-per-region reports/data/phase-12-hand-print-box-height/step-15u data/text_layer_v2_dev/15wr-a data/text_layer_v2_dev/15x-a -type f | sort | xargs shasum -a 256 > reports/data/phase-12-hand-print-box-height/step-15y/protect2_before.txt`
  (and `_after`). `shared-data/` and LotA files may change through other projects: report such a difference and go on.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`; no `rm -rf`, `nohup` or
  `&`; zsh; shared files (report, changelog, `STATUS.md`, `PROGRESS.md`) only with Edit. Never touch the external drive.
- Always pass `--work-dir data/text_layer_v4_15y_inputs` to the run script (never the default work dir).

## Tests

- modes: `test_decide_mode_pins` (pin `hand` on `_rows(80)` with `inv="9999"`: mode `hand`, method `pinned`,
  `box_height_mode` `print`, `box_height_method` `box_height`; another inv and `inv=None` unpinned; pin `print` on
  `_rows(120)`; a bad pin value raises); `test_v4_pins` (`sg_1.01.02_v4` pins `{"3463": "hand"}`, `sg_1.10.94_v4` none);
  `test_rebuild_pinned_in_place` (`monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", …)` with pins `{"9999": "print"}` and
  the gate made to split (`min_share` 0.0) → build all; then pins `{"9999": "hand"}`, `--stage extract --force
  --inventories 9999`, then `--stage all`: `split_model/9999.json` and `split_stats/9999.json` gone; mode `hand`/`pinned`;
  every file of 9999 equals a fresh hand-pinned build in another out dir (decompressed bytes; JSON without `seconds`)).
  `test_inventory_preset` and `test_v1_v3_presets_unchanged` must still pass (update only what the new key forces).
- build: `test_v2_rerun_skips_done_stages` (fixture v4 build, rerun `--stage all`: the logs say `[layout] 1 already done,
  0 to do` (and types, units), no new layout/types/units/wbc/join rows, file mtimes unchanged); `test_v2_params_change_redoes`
  (`--stage layout --layout-params '{"merge": true}'` redoes it); `test_v2_forced_extract_makes_later_stages_stale`;
  `test_units_removes_stale_crosswalk`. An existing test that relied on layout always re-running: adapt it, say which in the report.
- run script: `test_predict_model` (replaces `test_predict_range`: the low bound with many workers is the pooled
  throughput plus the serial sum; `high_s >= low_s`; output = Σ × ratio); `test_build_cmds_rebuild` (two commands,
  the first `--stage extract --force --inventories-file F`, the second without a subset); `test_rebuild_errors` (with
  `--inventories-file` for the same archive, with `--limit`, missing inventory, no `status.tsv`: exit 2);
  `test_archive_summary_last_row` (failed then done → 0 bad). `test_build_cmds_per_archive` keeps passing after
  the return shape changes (adapt its unpacking only).
- audit: `--self-test` also builds a pinned v4 layer (the preset replaced in `PRESETS` during the test) with 0 judged failures.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_modes.py tests/test_text_layer_build.py tests/test_run_text_layer_v4_full.py`, then the full suite (collect first; counts before/after).

## Steps

1. Code, tests, docs; protect `_before`.
2. `cp -R data/text_layer_v2_dev/15x-a data/text_layer_v2_dev/15y-b && chmod -R g+w data/text_layer_v2_dev/15y-b`;
   `printf '3463\n' > reports/data/phase-12-hand-print-box-height/step-15y/pins.txt`.
3. `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then
   `.venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --rebuild 1.01.02=reports/data/phase-12-hand-print-box-height/step-15y/pins.txt --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --work-dir data/text_layer_v4_15y_inputs --out-dir data/text_layer_v2_dev/15y-b --check-only`
   (expect `rebuild: 1 of 196`), then the same without `--check-only`.
4. Reference: `mkdir -p data/text_layer_v2_dev/15y-a/1.01.02 && chmod -R g+w data/text_layer_v2_dev/15y-a` and
   `.venv/bin/as-build-text-layer --preset sg_1.01.02_v4 --pagexml-dir data/text_layer_v4_staged/NL-HaNA_1.01.02 --out-dir data/text_layer_v2_dev/15y-a/1.01.02 --inventories 3463 --stage all --workers 1`.
5. `compare.py data/text_layer_v2_dev/15x-a data/text_layer_v2_dev/15y-b --sub-map 1.01.02:1.01.02,1.10.94:1.10.94 --explained-invs 3463 --out reports/data/phase-12-hand-print-box-height/step-15y/compare_15x-a.json`;
   `step-15y/same_inv.py` (every file whose name starts with `3463.` under the two layer roots: same set of relative
   paths, equal decompressed bytes, JSON without `seconds`/`elapsed*`) on `15y-a` vs `15y-b`.
6. Resume no-op: list `size mtime path` of every file under `15y-b/1.01.02/*/` except the root `status.tsv`,
   `manifest.json`, `progress.log`, `*/stats.json`, `*/params.json`, `wbc/*.json`, `wbc/*.tsv` (top level); run the step-3 command
   without `--rebuild`; list again; `diff` must be empty, and every stage logs `0 to do`. Note its wall time.
7. Summary of the staged full run: `archive_summary(Path("data/text_layer_v4_full"), "1.01.02")` → `(9122, 0)` (was 6).
8. Throughput fit and validation; constants into the run script; reads; report.

## Acceptance criteria

1. Tests pass; the full suite is green (give counts).
2. Compare `15x-a` vs `15y-b`: every per-inventory file of the other 195 1.01.02 inventories and all of 1.10.94 equal.
   For 3463 only, `split_model/3463.json` and `split_stats/3463.json` exist only in `15x-a`. The corpus files may differ
   only in the 3463 entries of `layout|types|units/stats.json` and in `wbc/` (pooled stats; `default_set` and `pinned`
   identical). Anything else: stop and report.
3. `same_inv.py`: 3463 in `15y-b` equals the fresh `15y-a` build in every file. `mode/3463.json`: `mode` `hand`, `method`
   `pinned`, `box_height_mode` `print`, `value` 86.5; no `split_from` line in `lines/3463`.
4. Resume no-op (step 6): empty diff; `0 to do` in layout, types, units, wbc and join.
5. Audit of `15y-b` (written by the run script): 0 judged failures; the reported lower-case shares listed (`15x-a`: 33; say
   whether 3463's share changed). Run summary `196 inventories done, 0 failed or unfinished rows`; step 7 gives `(9122, 0)`.
6. Throughput: `throughput.json`/`.tsv` written; in-sample and out-of-sample span errors reported per stage; the run script
   uses the fit; the step-3 `--check-only` log shows the new prediction line.
7. Protected lists unchanged (`diff` before/after), apart from differences that other projects caused.

## Error analysis

- **3463, print-built vs pinned hand.** Draw 5 scans (`random.Random(20261103).sample` over the sorted scan ids with ≥ 3
  `main_text` units in `15x-a`). For each, put the page's units from `15x-a` and `15y-b` side by side, full text, and give
  a verdict: `better` (closer to the entries on the page), `same`, `worse`, `mixed`. Expectation (not a bar): the hand
  build has no cut phrases (F12-R.9 read 8 of 12 sampled splits as wrong). Count the units per scan before and after.
- **Throughput residuals.** Per stage, the 5 inventories with the largest positive residual (seconds above the fit): give
  inventory, MB, scans, mode and a cause you can see (many scans, parse errors, split volume, very small page text).
  Say whether the pooled linear fit is enough for planning (it predicts sums, not single volumes).
Write categories, counts, verbatim examples and a conclusion into the report. Findings: the throughput model (stage fits,
serial share), the rebuild check, 3463's verdicts.

## Do not touch

`data/text_layer_v2_dev/{15m-a,15n-a,15o-pub,15i-src,15v-a,15w-a,15wr-a,15x-a}`, `data/text_layer_v4_full/`,
`data/hand_print_geom/`, `reports/data/phase-10-*`, `reports/data/phase-12-*/{step-15u,step-15wr,step-15x,review}/`,
the published layers, `shared-data/`, the siblings.

## When done

1. Report section `### Step 15y: mode pins, in-place rebuild, resume debts, throughput model` after § Step 15x:
   what changed, acceptance table, throughput tables (fits, in/out of sample, serial share), the 3463 reads, conclusion;
   rows `F12-15y.n` (`draft`) in *Findings*; `make reports` clean.
2. Changelog (Edit): `## Unreleased` above `## v0.16.0`, then `### Step 15y: mode pins and in-place rebuild`: the pins
   (`decide_mode(..., inv=None)`, method `pinned`; `sg_1.01.02_v4` pins 3463, a behaviour change; the published pilot keeps
   3463 `print` until the full run replaces it, P23); v2 stage skip and staleness; stale split and crosswalk files
   removed; v1 (VOC) unchanged. "Affected sibling imports: none (NLP's `scripts/build_voc_text_layer.py` calls
   `build.main` with the v1 preset)."
3. Append the block below to `PROGRESS.md`; overwrite `STATUS.md`: 15y done, next 15z (waits for UA-034), facts for 15z
   (pins file, the `--rebuild` command shape, reads of 3463).

```markdown
### Step 15y: mode pins, in-place rebuild, resume debts, throughput model
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `decide_mode(..., inv=None)`; `volume_mode["pins"]`; `MODE_PINS_V4`; v2 stage skip and staleness
- **Tests:** <command> → <result>
- **Acceptance:** <1–7 with numbers>
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15y](../../reports/phase-12-hand-print-box-height.md#step-15y-mode-pins-in-place-rebuild-resume-debts-throughput-model); findings F12-15y.1–n
- **Error analysis:** <two lines>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
