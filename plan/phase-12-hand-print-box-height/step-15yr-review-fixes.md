# Step 15yr: review fixes for 15y/15z (staleness, audit keys, stager, time model)

> **Phase:** 12, post-review addendum. **Depends on:** the 15y/15z review (2026-10-27). **Model:** Sonnet. **Read:**
> `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this file. **Read nothing else unless this
> brief names it.** Anchors checked 2026-10-27 (Opus review). Record any exploration under *Brief defects*.

## Context

The review found seven defects in the 15y/15z code. Fix them, test each one, re-measure the time model, and change nothing else.
1. **F12-R.19.** The v2 staleness rule (`build.is_current`) treats a *failed* upstream row as newer. After a forced extract fails
   (the new `mode/<inv>.json` is already written, `lines/` are old), a plain `--stage all` rebuilds layout→join with the new
   mode's rules on the old lines and records them `done`.
2. **F12-R.20.** The audit's `done_inventories` counts a stage as done if *any* row was `done`, not the last one.
3. **F12-15z.9.** The audit keys `modes`/`counts` and its invariant tags by inventory only, so 1.10.94's 455/456 overwrite
   1.01.02's 455/456.
4. **F12-15z.10.** `scripts/stage_text_layer_v4_full.py` copies but never deletes. Stale files stay in the staged copy.
5. **F12-R.17.** A v4 extract row's `seconds` leave out the mode pre-pass, which takes 47–49% of each call (measured on 9448, 3436
   and 12476).
6. **F12-15z.11.** Every run decompresses all tars twice to size them (`io.input_bytes`, about 9 min for 9,122 tars): once for a
   `--estimate` call that prints "no throughput fit; no prediction" for both v4 presets, and once at the start of `build.main`.
   The time model has neither scan, nor the audit (59 min).
7. The run-script docstring says the other volumes' sources "are not read", which is false (`docs/text_layer.md` was fixed in
   the review).

## Inputs (anchors in the current code)

- `archival_structures/text_layer/build.py`: `Run.status()` L156 (rows carry `_seq`); `recorded_done` L275; `is_done` L280;
  `V2_UPSTREAM` L286 (`{"layout": "extract", "types": "layout", "units": "types", "wbc": "units", "join": "units"}`);
  `is_current(status, inv, stage, force)` L289–294; `v2_todo(run, stage, inventories, params_doc, output_names)` L297–316
  (called by `stage_layout` L425, `stage_types` L474, `stage_units` L544); `stage_wbc` v2 todo L619–622; `stage_join` v2
  `current` L778–786; `main` L820: for non-local stages `inventories = select_inventories(args)` and `sizes = {inv:
  input_bytes(src) …}` L885–886, then `Run(args, preset)` L891. `STATUS_COLUMNS` L113: `inventory_number, stage, status,
  n_rows, n_lines, n_scans, n_errors, seconds, input_bytes, input_xml_bytes, finished_utc, error`.
- `archival_structures/text_layer/io.py`: `input_bytes(source) -> (stored_bytes, xml_bytes)` L141 (a `.tar.gz` is decompressed;
  a dir sums its files: `(xml, xml)`).
- `archival_structures/text_layer/modes.py`: `extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False)`
  L128–163 (`prepass_rows` L133, `return result` L163). No `import time` yet.
- `scripts/run_text_layer_v4_full.py`: docstring L1–83 (the *In-place rebuild* paragraph L53–64 ends "the others are not read.";
  *Predictions are rough* L79); `THROUGHPUT_V4` L114, `POOL_STAGES` L128, `SERIAL_STAGES` L129, `ARCHIVE_XML_RATIO` L132,
  `OUTPUT_PER_XML_BYTE` L134; `xml_bytes(path)` L177 (stat × ratio for a file); `predict(archive, xml_sizes, want, min_workers)`
  L234; `build_cmds(archive, args, work_dir, out_dir, subset_file, rebuild_file=None)` L282 → `(estimate, cmds)` (estimate built
  L291); `main` L349 (`predict` L421; the build loop L455–466 logs and runs `estimate`, then each command).
- `reports/data/phase-12-hand-print-box-height/step-15w/audit.py`: `done_inventories(root)` L64; `audit_layer(root, invs,
  sources, A)` L135 (`counts[inv]` L145, `tag = f"[{inv}]"` L155); `audit_v4(root, invs, A)` L267 (tag L278, `modes[inv]` L285);
  `audit_base(base, A)` L340 (`counts.update`, `modes.update`; `all_invs` feeds `lota_keys`, keep it bare); `main` L358
  (`"modes"`, `"counts"` in the JSON L374); `self_test()` L383 (swaps `PRESETS["sg_1.01.02_v4"]` without `try/finally`).
- `scripts/stage_text_layer_v4_full.py` (154 lines): `LAYER_DIRS` L35, `SKIP_DIRS` L36, `TAG_RE = r"\[([0-9]+[A-Z]?)\]"` L38,
  `Stager.file` L61, `Stager.tree(src_dir, layer)` L74, `failing_inventories(audit)` L83, `main` L98 (layers `src/<archive>/
  NL-HaNA_*_text_layer_v*` L131; per-inventory walk L142–148). It has no tests.
- Tests: `tests/test_text_layer_build.py` (fixture `v4_out`, helpers `v4_run`, `v4_rows`, `layer_mtimes`, `SG_FIXTURE`, `V4_LAYER`
  from step 15y, at the end of the file); `tests/test_text_layer_modes.py` (`src` fixture L228, `test_rebuild_pinned_in_place`);
  `tests/test_run_text_layer_v4_full.py` (loads the script with `importlib.util.spec_from_file_location`, L10–12; `test_build_cmds_rebuild`,
  `test_build_cmds_per_archive` unpack `build_cmds`). The review's case A is
  `reports/data/phase-12-hand-print-box-height/review/stale_cases_15yz.py::test_A_failed_forced_extract` (monkeypatches
  `archival_structures.text_layer.extract_v2.extract_inventory_v2` to raise). Baseline 2026-10-27: **1,322 collected, 1,311 passed, 11 skipped**.
- Measurements for the constants (from the review; do not open `run_v4.log`, it names a drive path): sizing scan before the first
  stage line of `--stage all`: 529, 564, 561 s (full run, resume, rebuild; these include the coreshare grant); `--estimate`, the same
  scan without coreshare: 532, 535, 546 s; audit 3,582 s (full run)
  and 3,538 s (rebuild) for 9,124 inventories; wbc decision (last part to decision line) 39 s and 47 s.
- Data (read only): `data/text_layer_v4_full/1.01.02/NL-HaNA_1.01.02_text_layer_v4/{status.tsv,mode/}` (full run, 16 volumes
  rebuilt after `2026-10-27T17:00:00Z`); `data/text_layer_v2_dev/15x-a/`, `15y-b/` (protected); sources
  `data/text_layer_v4_staged/NL-HaNA_1.01.02/` (182 tars), links `data/text_layer_v4_15y_inputs/1.01.02/` (196: 188 tars).

## Behaviour to implement

**A. Upstream gate (F12-R.19, `build.py`, v2 only).** New `upstream_ok(status, inv, stage)`: True if `V2_UPSTREAM.get(stage)` is
None, or there is no row for `(inv, upstream)`, or that row's `status == "done"`. In `v2_todo`, an inventory with `not
upstream_ok(...)` is **blocked**: not in `todo`, no status row, files untouched. Log `[<stage>] N blocked: the stage they read
is not done (e.g. [first 5])` when N > 0 (the `already done` count excludes them). Same in `stage_wbc` (v2: blocked inventories
leave the parts todo; their existing part still enters the decision) and `stage_join` (v2: not in `todo`). A blocked inventory
adds no failure (its upstream failure is already counted). v1 paths unchanged.

**B. Recorded sizes (F12-15z.11, `build.py`).** New `recorded_sizes(status_path) -> {inv: (stored, xml)}` from the **last**
`done` extract row per inventory with non-empty integer `input_bytes` and `input_xml_bytes`. In `main`, for a v2 preset and a
non-local stage: if `<out-dir>/<preset.name>/status.tsv` exists, reuse `rec[inv]` when the source is a file and
`Path(src).stat().st_size == rec[inv][0]`; otherwise call `input_bytes(src)`. After `run = Run(...)` log `[sizes] N from
status.tsv, M measured`. v1 paths unchanged; `--estimate` behaves as before.

**C. Whole-call extract time (F12-R.17, `modes.py`).** `start = time.monotonic()` before `prepass_rows`; after it,
`result["prepass_seconds"] = round(<pre-pass time>, 3)` and, just before `return`, `result["seconds"] = round(time.monotonic()
- start, 3)`. Status rows then time the whole call; `extract_stats/<inv>.json` is unchanged (it is written inside
`extract_inventory_v2`).

**D. Audit (F12-15z.9, F12-R.20; `step-15w/audit.py`).** `done_inventories`: keep the last row per `(inv, stage)` (file order);
done = every stage seen has a last row `done`. New `archive_of(root)`: group 1 of `re.fullmatch(r"NL-HaNA_(.+)_text_layer_v\d+",
root.name)`, else `root.parent.name`. `audit_layer` and `audit_v4` take `archive` and use `key = f"{archive}/{inv}"` for
`counts`/`modes` and `tag = f"[{key}]"`. `audit_base` passes it; `all_invs` stays bare. The JSON keeps `"modes"`/`"counts"`
(keys now `"<archive>/<inv>"`). Self-test: wrap the preset swap in `try/finally`. Add a case `two` (copy the built `v4` sub-layer
dir to `td/two/a/NL-HaNA_1.01.02_text_layer_v4` and `td/two/b/NL-HaNA_1.10.94_text_layer_v4`, audit `td/two`: `modes` has both
`1.01.02/9999` and `1.10.94/9999`, 0 judged) and a case `fail` (copy the `v4` sub-layer, append an extract `failed` row for 9999
with 12 tab-separated fields: `counts` is empty). Update the existing asserts to the new keys. Do not change
`after-15z-rebuild/check.py`: it reads the old `audit.json` format in `data/text_layer_v4_full`.

**E. Stager (F12-15z.10, the new tags).** `TAG_RE = re.compile(r"\[(?:([0-9.]+)/)?([0-9]+[A-Z]?)\]")`;
`failing_inventories` returns `[(archive or None, inv)]`. `--inventories` items are `INV` (every archive) or `ARCHIVE/INV`. A
layer's archive is `layer.parent.name`; its wanted set is the invs whose archive is None or equal. **Mirror deletions:** after
copying, schedule for deletion every destination file (name not starting with `.`) that has no source counterpart, (1) under each
mirrored `LAYER_DIRS` tree (skipping `SKIP_DIRS`), and (2) among the per-inventory files of the layer's wanted inventories
(`name.split(".", 1)[0] in wanted`, at any depth under the destination layer). Never anything else (not `ROOT_FILES`,
`LAYER_FILES`, `*/params.json`, other inventories' files). New `--max-deletions N` (default 100): if more are scheduled, print the
count and the first 20, delete nothing, and return exit code 1. `--dry-run` prints `would delete` and deletes nothing. The summary
line adds `deleted N` (`would delete N`). Update the docstring.

**F. Run script (F12-15z.11, F12-R.17, defect 7).** (1) Drop the `--estimate` call: `build_cmds(...)` returns the list of
coreshare commands only; `main` no longer builds or runs `estimate`. (2) Measure the pre-pass factor (G) and multiply the
`extract` entries of `THROUGHPUT_V4` (both archives; `intercept_s` and `s_per_mb`) by it, with a comment naming
`step-15yr/prepass_factor.json`. (3) Whole-layer terms, measured above: `SIZING_S_PER_STORED_GB` (mean of the three `--estimate` scans,
532/535/546 s, divided by Σ `input_bytes` / 1e9 of the full run's last done extract rows), `WBC_DECISION_S = 45`, `AUDIT_S_PER_INVENTORY` (mean of
3,582/3,538 s divided by 9,124). New `whole_layer_seconds(n_inventories, unsized_stored_bytes)`. In `main`, per archive:
`unsized` = stored bytes of the links that B would size again (use `build.recorded_sizes` on
`<out-dir>/<archive>/<layer>/status.tsv` if it exists, else all links; stored = `entry_bytes`), `n` = all links. Log
`predicted wall time <stages span> with W to M workers, plus about X min over the whole layer (sizing Y min, wbc decision, audit
of n inventories)`. (4) Docstring: replace "The listed inventories need their sources in `--src`; the others are not read." with
"Every inventory of the layer needs its source in `--src`: the build lists them, sizes those without a recorded extract row
(a `.tar.gz` is decompressed, about 9 minutes for 9,122 SG tars) and the wbc stage stops if one is missing; only the listed
inventories are parsed. Rerunning a `--rebuild` command after a crash repeats the forced extract."; rewrite *Predictions are
rough* for the new terms and say that `--estimate` is no longer called.

**G. Pre-pass factor** (`reports/data/phase-12-hand-print-box-height/step-15yr/prepass_factor.py`, `.json`). From the 182
staged tars, take the volumes of `15x-a` with `input_xml_bytes` ≤ 300e6 in its 1.01.02 `status.tsv` (last extract row) and
group them by `15x-a/…/mode/<inv>.json` as `hand`, `print_split` (`split_gate.split` true) and `print_gated`; draw 4 per group
with `random.Random(20261109).sample(sorted(group), 4)`. Run `extract_inventory_v2_modes(inv, tar, tmpdir, PRESETS["sg_1.01.02_v4"])`
one at a time into a `tempfile.TemporaryDirectory()` (after C, read `seconds` and `prepass_seconds` from the result). Factor per
group = Σ seconds / Σ (seconds − prepass_seconds). 1.01.02 factor = Σ_g w_g × f_g, where w_g is group g's share of
`input_xml_bytes` among the full run's last done extract rows (group from `mode/<inv>.json`; leave out the 16 rebuilt rows). The
1.10.94 factor = the `print_split` one. Write per-volume rows, group factors, weights, both factors.

## Tests (all must pass; count before and after)

- build: `test_v2_failed_extract_blocks_later_stages` (case A of `stale_cases_15yz.py` turned into a test: build with a `print`
  pin and the split gate opened, pin `hand`, make the extract raise, `--stage extract --force --inventories 9999` → 1, then
  `--stage all` → 1: no new layout/types/units/wbc/join rows, the log says `blocked`, every file of 9999 except `mode/9999.json`
  keeps its bytes; restore the extract, `--stage all` → 0, and 9999 equals a fresh hand-pinned build (decompressed, JSON without
  `seconds`, `source`)). `test_v2_recorded_sizes` (a `.tar.gz` source of the fixture named `NL-HaNA_1.01.02_9999-pagexml.tar.gz`,
  `tarfile` in `tmp_path`; build; rerun with `build.input_bytes` monkeypatched to count calls: 0 calls, `[sizes] 1 from status.tsv,
  0 measured`, no new rows; rewrite the tar with one more member: 1 call).
- modes: `test_extract_seconds_include_prepass` (`result["seconds"] >= result["prepass_seconds"] > 0`).
- run script: adapt `test_build_cmds_rebuild` and `test_build_cmds_per_archive` to the new return; `test_whole_layer_terms`
  (`whole_layer_seconds(0, 0) == WBC_DECISION_S`; linear in both arguments); `test_check_only_logs_whole_layer` (the `--check-only`
  log has `over the whole layer`).
- stager, new `tests/test_stage_text_layer_v4_full.py` (load the script as the run-script test does): copy and skip; mirror
  deletion in `split_model/`; a wanted inventory's stale `units/units/<inv>.tsv.gz` deleted, another inventory's kept; `--dry-run`
  deletes nothing; the cap (101 stale files → exit 1, nothing deleted); tags `[455]` and `[1.01.02/455]` parsed; an `ARCHIVE/INV`
  item copies from that archive's layer only.
- audit: `--self-test` passes with the `two` and `fail` cases.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_build.py tests/test_text_layer_modes.py
  tests/test_run_text_layer_v4_full.py tests/test_stage_text_layer_v4_full.py`, the audit `--self-test`, then the full suite.

## Steps

1. Protect `_before` (from the AS root; `mkdir -p reports/data/phase-12-hand-print-box-height/step-15yr` first):
   `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
   reports/data/phase-12-hand-print-box-height/step-15yr/protect_before.txt --skip 15yr` and
   `find data/hand_print_geom data/text_layer_v4_full data/text_layer_v2_dev/15x-a data/text_layer_v2_dev/15y-a data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/15z-a data/text_layer_v2_dev/15z-b -type f | sort | xargs shasum -a 256 > reports/data/phase-12-hand-print-box-height/step-15yr/protect2_before.txt`.
2. A–F and tests; suite green.
3. Audit on real data: `.venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15w/audit.py --layer
   data/text_layer_v2_dev/15y-b --out reports/data/phase-12-hand-print-box-height/step-15yr/audit_15y-b.json`.
4. Rebuild check on a clone: `cp -cR data/text_layer_v2_dev/15y-b data/text_layer_v2_dev/15yr-a && chmod -R g+w
   data/text_layer_v2_dev/15yr-a` (`-c` = APFS clone, no extra space; if `cp -c` fails, stop and report). Then, with `B=
   .venv/bin/as-build-text-layer --preset sg_1.01.02_v4 --pagexml-dir data/text_layer_v4_15y_inputs/1.01.02 --out-dir
   data/text_layer_v2_dev/15yr-a/1.01.02 --workers 1`: `$B --stage all` (note the time), `$B --stage extract --force --inventories
   3463`, `$B --stage all`. Compare 3463 with `step-15y/same_inv.py` (15yr-a vs 15y-b).
5. G, the constants, then `.venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --rebuild
   1.01.02=reports/data/phase-12-hand-print-box-height/step-15y/pins.txt --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02
   --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --work-dir data/text_layer_v4_15yr_inputs --out-dir
   data/text_layer_v2_dev/15yr-a --check-only` (`mkdir -p data/text_layer_v4_15yr_inputs` first): expect `rebuild: 1 of 196`, no
   `estimate:` line, and the new prediction line with a sizing term of 0 min (every tar has a recorded row).
6. Validation (`step-15yr/validate.py`, no bar): with the corrected fit, the predicted extract span of the full run (7 workers)
   and of `15x-a` (4 workers) against 2.836 h and 0.372 h; the UA-035 rebuild (16 volumes, 3 workers, the 9,122-volume layer)
   predicted with the code it ran (two sizing scans, no reuse) against the measured 79 min, and with the new code.
7. Protect `_after`; `diff` both lists.

## Acceptance criteria

1. Tests: every test above passes; full suite green (counts).
2. Audit of `15y-b`: 3,592 pass, 33 fail, 0 judged (as in 15y); `modes` and `counts` have 198 keys each, all of the form
   `<archive>/<inv>`, `modes["1.10.94/455"] == ["print", "fixed"]`, `modes["1.01.02/3463"] == ["hand", "pinned"]`.
3. Step 4: the first `--stage all` logs `0 to do` in all six stages, `[sizes] 188 from status.tsv, 8 measured`, adds no row, and
   takes under 10 s (it took 30 s before, F12-R.16); after the forced extract, `--stage all` redoes 3463 only (6 new rows of 3463
   in total), `same_inv.py` finds every file of 3463 equal to `15y-b`'s; the new 3463 extract row's `seconds` is at least 1.5 x
   its `15y-b` row's.
4. `prepass_factor.json` written: 12 volumes, three group factors and both archive factors; `THROUGHPUT_V4` extract scaled; the
   validation table reported (no bar).
5. Protected lists unchanged, apart from differences other projects caused (name them).

## Do not touch

`data/text_layer_v4_full/` (read only), `data/text_layer_v2_dev/{15x-a,15y-a,15y-b,15z-a,15z-b}` and every older dev layer,
`data/hand_print_geom/`, `reports/data/phase-12-*/{planning,review,step-15u,…,step-15z,after-15z-rebuild}/` except
`step-15w/audit.py`, `shared-data/`, the siblings, the published layers. Pins, cut, gate, unit rules and presets unchanged. Never
`git commit`; `chmod g+w` everything created; `.venv/bin/python`; no `rm -rf`, `nohup` or `&`; zsh; shared files (report,
changelog, `STATUS.md`, `PROGRESS.md`) only with Edit. Never touch the external drive.

## When done

1. Report: `### Step 15yr: review fixes for 15y/15z` after § *After 15z: check of the staged drive rebuild*: what changed, the
   acceptance table, the factor table, the validation table, case A before and after, conclusion; findings `F12-15yr.n` (`draft`);
   `make reports` clean.
2. Changelog (Edit, under *Unreleased*): `### Step 15yr: staleness gate, recorded sizes, extract timing`: `build.upstream_ok`
   and blocked inventories in v2 stages; `build.recorded_sizes` and size reuse in `build.main` (v2); v4 extract rows' `seconds`
   now time the whole call (`prepass_seconds` in the result); the run script no longer calls `--estimate` and `build_cmds` returns
   the command list; the audit keys `modes`/`counts`/tags by `<archive>/<inv>`; the stager mirrors deletions. "Affected sibling
   imports: none (NLP's `build_voc_text_layer.py` uses the v1 preset)."
3. `docs/text_layer.md`: the resumability paragraph (blocked inventories; recorded sizes); `seconds` of v4 extract rows.
4. `PROGRESS.md` block (template as in 15y, title above); overwrite `STATUS.md`: 15yr done, next = its review; facts: the new
   audit key format; `15yr-a`.
