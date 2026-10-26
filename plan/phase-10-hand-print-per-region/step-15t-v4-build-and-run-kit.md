# Step 15t: v4 dev build, full-run audit and run kit

> **VOID (2026-10-23).** Replaced by the re-plan after UA-019 (b) (one mode per volume): see the phase `PLAN.md`
> steps table (`step-15r-volume-mode-build.md`, `step-15s-v4-build-and-run-kit.md`). Don't run this brief.

> **Phase:** 10, Hand and print per region (text layer v4). **Depends on:** 15s.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file, then the docstring of `scripts/run_text_layer_v3_full.py` (L1–48) and its `tests/test_run_text_layer_v3_full.py`.
> **Read nothing else unless this brief names it.** Record any exploration under *Brief defects*.

## Context

15r and 15s made the build choose hand or print rules per region (v4 presets `sg_1.01.02_v4`, `sg_1.10.94_v4`).
This step builds the v4 dev layer from scratch on every local inventory (the 16 pilot ones plus whatever the user
staged for 15q), replaces the pilot-only audit with one that discovers its inventories (follow-up F09-15n.3),
and turns the v3 hand-off script into a v4 one without hand/print "sides": one source list per **archive**. It
ends with a user decision on the full run and on publishing a v4 pilot. Nothing is published here.

## Inputs

- Sources: `data/text_layer_v3_full_inputs/{hand,print}/` (symlinks to the 14 + 2 pilot sources); staged 1.01.02
  tars in `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/` (15q part B lists them
  in `STATUS.md`, or none).
- REPUBLIC: 1.01.02 printed volumes `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_paragraphs_v1/`
  (use it only if 15s's verdict in `STATUS.md` says the crosswalk works); 1.10.94
  `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/`.
- Reference layers: `data/text_layer_v2_dev/15s-a/{1.01.02,1.10.94}` (v4) and `15m-a/{hand,print}` (v3).
- v3 run script `scripts/run_text_layer_v3_full.py` (340 lines): `BUILD`, `AUDIT`, `SIDES = {"hand": "sg_1.01.02_v3",
  "print": "sg_1.10.94_v3"}` L66–67, pilot cost constants L69–79 (`ARCHIVE_XML_RATIO = 4.16`, `OUTPUT_PER_XML_BYTE`,
  `WORKER_SECONDS_PER_XML_BYTE` per side, `SLOW_FACTOR = 3`), `link_side(side, srcs, work_dir, log)` L129,
  `select` L161, `republic_missing` L167, `predict(side, xml_sizes, want)` L175, `side_summary` L196,
  `build_cmds(side, args, work_dir, out_dir, subset_file)` L212, `parse_args` L228, `main` L250. Tests: 8 in
  `tests/test_run_text_layer_v3_full.py` (link first-wins, relink, subset, republic warning, space check,
  check-only, refuses outside root, predict range).
- Pilot audit `reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py` (`--layer`, `--out`, `--self-test`):
  `audit_layer(root, invs, sources, A, wbc_chars=None)` L92, `raw_checks` L67, `lota_keys` L219 (LotA 02d gold),
  `main` L238 with the fixed `HAND`/`PRINT` lists and `src = data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02` (L248–261).

## Deliverables

| File | Change |
|---|---|
| `data/text_layer_v2_dev/15t-a/{1.01.02,1.10.94}/` | v4 from scratch, `--stage all`, via the new run script (below) |
| `reports/data/phase-10-hand-print-per-region/step-15t/audit.py` | copy of the 15m audit, generalised: sub-layers found under `--layer` by the glob `*/NL-HaNA_*_text_layer_v*`; inventories from each sub-layer's `status.tsv` (done rows); sources from `manifest.json` `stages.extract.argv` `--pagexml-dir` (resolve symlinks); the LotA-key invariant only for inventories present; the lower-case merge share stays **reported** (never pass/fail-blocking); new v4 invariants below; `--self-test` on the fixture |
| `scripts/run_text_layer_v4_full.py` (+ un-ignore it in `.gitignore` as 15n did for v3) | v3 script generalised: `--src ARCHIVE=DIR` (repeatable; ARCHIVE ∈ `1.01.02`, `1.10.94`; first wins), `--republic-dir ARCHIVE=DIR` (repeatable), `--inventories-file ARCHIVE=F`, `--only ARCHIVE`; `ARCHIVES = {"1.01.02": "sg_1.01.02_v4", "1.10.94": "sg_1.10.94_v4"}`; layer under `<out-dir>/<archive>/`; log `run_v4.log`; audit = the new one; cost constants per archive from `15t-a` (same method as v3, comment with the numbers); docstring rewritten (what to stage per archive, order of work, resume, logs, what the audit checks) |
| `tests/test_run_text_layer_v4_full.py` | the 8 v3 tests ported to archives, plus `test_two_archives_one_run` and `test_republic_per_archive` |
| `docs/text_layer.md` | v4 section: *Running the full v4 build* (commands with `<drive>` placeholders) |
| `docs/shared_library/changelog.md` | `### Step 15t: …` (script, audit), with Edit |
| `reports/phase-10-hand-print-per-region.md` | `### Step 15t: v4 build, audit, run kit`; findings `F10-15t.n` |
| `docs/USER_ACTIONS.md` | two UAs (below) |

New audit invariants (each with its expected value): every `regions_meta` row has `mode` ∈ {hand, print},
`mode_method` ∈ {density, page, inventory, default}, a numeric `mode_score`; `layout/regions.mode` equals its
source regions' mode (merged → all sources print); every line with a `split_from` value lies in a print region;
`units/units.mode` equals its layout region's mode; every unit with method `print:*` has mode print and `hand:*` hand;
`extract_stats` `modes` totals equal the `regions_meta` counts. Plus every phase-09 invariant that does not depend
on fixed lists.

## Steps

1. Port the script and its tests; `--check-only` on the local sources:
   `.venv/bin/python scripts/run_text_layer_v4_full.py --check-only --src 1.01.02=data/text_layer_v3_full_inputs/hand --src 1.01.02=<staged dir, if any> --src 1.10.94=data/text_layer_v3_full_inputs/print --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 [--republic-dir 1.01.02=…/print_paragraphs_v1] --out-dir data/text_layer_v2_dev/15t-a`.
   A staged dir of tars must be given as a dir that holds only them (make one of symlinks under
   `data/text_layer_v4_staged/`; never write into shared-data).
2. `--limit 1` smoke into `15t-smoke`, then the full local build into `15t-a`.
3. Compare `15t-a` with `15s-a` (15r's comparer, `--sub` `1.01.02,1.10.94` on both): equal on decompressed bytes
   except path columns (`scans.member`, `extract_stats` `source`, manifests). Staged inventories exist only in
   `15t-a`.
4. Audit `15t-a` with the new audit; then the new audit on `15m-a` too (it must run on a v3 layer; the v4
   invariants are skipped there), and compare its phase-09 invariants with `reports/data/phase-09-text-layer-v3-prep/step-15m/audit_15m-a.json`
   (178 pass, 4 fail): same results.
5. Size and time: per archive, XML bytes, output bytes, worker-seconds from `status.tsv`; the v4 prediction for the
   full 1.01.02 corpus (9,122 inventories; the user estimates about 400 GB of XML) as a range, with the caveats of
   F09-15n.1/15n.2.
6. UAs (format and next free ID in `docs/USER_ACTIONS.md`; the resolved file holds the highest used IDs too):
   - **Decide + Run: the full v4 run.** The commands from the new docstring with `<drive>` placeholders, the census
     numbers from 15p (printed share, mixed volumes), the predicted size/time; options (a) all 9,122 + 1.10.94,
     (b) a subset file (e.g. the volumes LotA aligns), (c) later. Default: no run. Blocks: the full layer.
   - **Decide: publish `15t-a` as a v4 pilot** (`shared-data/NL-HaNA_*/text_layer_v4/`, `coverage: partial`), so
     LotA's printed 1.01.02 volumes get the print rules now. Options (a) publish, (b) wait for the full run. Default:
     nothing published. Blocks: LotA's use of 3771–3823 as print.

## Acceptance criteria

1. New and ported tests pass; full suite green.
2. `15t-a` = `15s-a` up to path columns (step 3).
3. The new audit passes every invariant on `15t-a` except reported shares, each failure listed with a cause; on
   `15m-a` it reproduces 178/4 for the phase-09 invariants.
4. Prediction table and the two UAs written.
5. Protected hashes unchanged (`--skip 15t`): `15m-a`, `15n-a`, `15r-a`, `15s-a`, shared-data, LotA data.

## Error analysis

- Every audit failure: read 5 offending rows each (all if fewer).
- Staged volumes (if any): per volume, pages by label and the runs along scan order; read 5 pages per mixed volume
  at label changes (seed 19), with their unit texts: does the v4 output read right on both sides of a change?

## Constraints

- No library changes except bug fixes the audit forces (each one recorded, with a test). No `shared-data` or
  sibling writes. No external drive: the script is for the user to run there.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`; new dir per run.

## When done

Report section (comparison, audit table, prediction, staged-volume reads), findings `F10-15t.n`; `make reports`.
`PROGRESS.md` block; `STATUS.md`: phase 10 ready for review (prompt 3), the two UA IDs.
