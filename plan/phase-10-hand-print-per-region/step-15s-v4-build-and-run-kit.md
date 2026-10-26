# Step 15s: v4 dev build with the staged volumes, full-run audit and run kit

> **Phase:** 10, Hand and print per volume (text layer v4). **Depends on:** 15r.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, this file,
> then the docstring of `scripts/run_text_layer_v3_full.py` (L1–48) and its `tests/test_run_text_layer_v3_full.py`.
> **Read nothing else unless this brief names it.** Record any exploration under *Brief defects*.

## Context

15r made the build choose the hand or print rules per inventory, from the inventory's own PageXML (v4 presets
`sg_1.01.02_v4`, `sg_1.10.94_v4`; the decision and diagnostics under `mode/`). This step builds the v4 dev layer from
scratch on every local inventory: the 16 pilot ones plus the 72 staged 1.01.02 volumes (12 from UA-017, 60 from
UA-020). It replaces the pilot-only audit with one that discovers its inventories (follow-up F09-15n.3), and turns
the v3 hand-off script into a v4 one without hand/print "sides": one source list per **archive**. It ends with user
decisions on the full run and on publishing a v4 pilot. Nothing is published here.

## Inputs

- Sources: 1.01.02 pilot `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02/` (14); 1.10.94
  `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml/` (455, 456); staged 1.01.02 tars in
  `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/` (lists:
  `reports/data/phase-10-hand-print-per-region/step-15p/staging.tsv` and `.../planning-15q/silver_sample.tsv`; that dir
  also holds the pilot tars 3349–3354, which must not be linked twice).
- REPUBLIC: 1.01.02 printed volumes `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_paragraphs_v1/`
  (use only if 15r's verdict in `STATUS.md` says the crosswalk works); 1.10.94
  `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/`.
- Reference layers: `data/text_layer_v2_dev/15r-a/{1.01.02,1.10.94}` (v4) and `15m-a/{hand,print}` (v3).
- 15q's per-volume modes: `reports/data/phase-10-hand-print-per-region/step-15q/{dev,transfer,sample,staged}_volumes.tsv`.
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
  Its result on `15m-a`: `reports/data/phase-09-text-layer-v3-prep/step-15m/audit_15m-a.json` (178 pass, 4 fail).

## Deliverables

| File | Change |
|---|---|
| `data/text_layer_v2_dev/15s-a/{1.01.02,1.10.94}/` | v4 from scratch, `--stage all`, via the new run script |
| `reports/data/phase-10-hand-print-per-region/step-15s/audit.py` | copy of the 15m audit, generalised: sub-layers found under `--layer` by the glob `*/NL-HaNA_*_text_layer_v*`; inventories from each sub-layer's `status.tsv` (done rows); sources from `manifest.json` `stages.extract.argv` `--pagexml-dir` (resolve symlinks); the LotA-key invariant only for inventories present; the lower-case merge share stays **reported** (never pass/fail-blocking); the v4 invariants below; `--self-test` on the fixture |
| `scripts/run_text_layer_v4_full.py` (+ un-ignore it in `.gitignore` as 15n did for v3) | v3 script generalised: `--src ARCHIVE=DIR` (repeatable; ARCHIVE ∈ `1.01.02`, `1.10.94`; first wins), `--republic-dir ARCHIVE=DIR` (repeatable), `--inventories-file ARCHIVE=F`, `--only ARCHIVE`; `ARCHIVES = {"1.01.02": "sg_1.01.02_v4", "1.10.94": "sg_1.10.94_v4"}`; layer under `<out-dir>/<archive>/`; log `run_v4.log`; audit = the new one; cost constants per archive from `15s-a` (same method as v3, comment with the numbers; the pre-pass is part of extract); docstring rewritten (what to stage per archive, order of work, resume, logs, what the audit checks, that a volume's mode is decided from its PageXML and recorded under `mode/`) |
| `tests/test_run_text_layer_v4_full.py` | the 8 v3 tests ported to archives, plus `test_two_archives_one_run` and `test_republic_per_archive` |
| `docs/text_layer.md` | v4 section: *Running the full v4 build* (commands with `<drive>` placeholders) |
| `docs/shared_library/changelog.md` | `### Step 15s: …` (script, audit), with Edit |
| `reports/phase-10-hand-print-per-region.md` | fill `### Step 15s: v4 build, audit, run kit`; findings `F10-15s.n` |
| `docs/USER_ACTIONS.md` | two UAs (below) |

New v4 audit invariants (each with its expected value): every inventory in `status.tsv` has `mode/<inv>.json` with
`mode` ∈ {hand, print} and `method` ∈ {geometry, default, fixed}, and its `mode/scans` counts equal the JSON's;
`split_model/<inv>.json` exists iff the mode is print; no line with a `split_from` value in a hand inventory; unit
methods `print:*` only in print inventories, `hand:*` only in hand ones; `layout/params.json`, `types/params.json`,
`units/params.json` have keys `hand` and `print`. Plus every phase-09 invariant that does not depend on fixed lists.

## Steps

1. Port the script and its tests. Make a dir of symlinks `data/text_layer_v4_staged/NL-HaNA_1.01.02/` to the 72
   staged tars (never write into shared-data; skip 3349–3354). `--check-only` on all local sources:
   `.venv/bin/python scripts/run_text_layer_v4_full.py --check-only --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 [--republic-dir 1.01.02=…/print_paragraphs_v1] --out-dir data/text_layer_v2_dev/15s-a`.
2. `--limit 1` smoke into `15s-smoke`, then the full local build into `15s-a` (the script takes its worker count from
   coreshare as the v3 one does).
3. Compare `15s-a` with `15r-a` on the 16 shared inventories (15r's comparer, `--sub` `1.01.02,1.10.94` on both):
   equal on decompressed bytes except path columns (`scans.member`, `extract_stats` `source`, manifests) and the
   corpus-level files that depend on the inventory set (`wbc/corpus_default.json`, `*/stats.json`; list the keys).
4. Check every `mode/<inv>.json` against 15q's per-volume TSVs: same mode and share for all 88 volumes.
5. Audit `15s-a` with the new audit; then the new audit on `15m-a` (a v3 layer; the v4 invariants are skipped there)
   and compare its phase-09 invariants with `audit_15m-a.json` (178 pass, 4 fail): same results.
6. Size and time: per archive, XML bytes, output bytes, worker-seconds from `status.tsv` (extract incl. pre-pass);
   the v4 prediction for the full 1.01.02 corpus (9,122 inventories; the user estimates about 400 GB of XML) as a
   range, with the caveats of F09-15n.1/15n.2.
7. UAs (format and next free ID in `docs/USER_ACTIONS.md`; `docs/USER_ACTIONS_resolved.md` holds the highest IDs too):
   - **Decide + Run: the full v4 run.** The commands from the new docstring with `<drive>` placeholders, 15q's
     out-of-sample agreement, the census numbers from 15p, the predicted size/time; options (a) all 9,122 + 1.10.94,
     (b) a subset file (e.g. the volumes LotA aligns), (c) later. Default: no run. Blocks: the full layer.
   - **Decide: publish `15s-a` as a v4 pilot** (`shared-data/NL-HaNA_*/text_layer_v4/`, `coverage: partial`), so
     LotA's printed 1.01.02 volumes get the print rules now. Options (a) publish, (b) wait for the full run.
     Default: nothing published. Blocks: LotA's use of 3771–3823 as print.

## Acceptance criteria

1. New and ported tests pass; full suite green.
2. `15s-a` = `15r-a` on the shared inventories up to the listed paths and corpus files (step 3); step 4 agrees 88/88.
3. The new audit passes every invariant on `15s-a` except reported shares, each failure listed with a cause; on
   `15m-a` it reproduces 178/4 for the phase-09 invariants.
4. Prediction table and the two UAs written.
5. Protected hashes unchanged (`--skip 15s`): `15m-a`, `15n-a`, `15r-a`, shared-data, LotA data.

## Error analysis

- Every audit failure: read 5 offending rows each (all if fewer).
- Staged volumes: for each volume whose 15p label was `mixed` or whose scan labels are > 20% against its mode, 5
  scans at label changes or against the mode (seed 19), with their unit texts: does the v4 output read right with
  the volume's single mode? Count how many such scans are real minority material (printed inserts in a hand volume,
  handwritten slots in a print one), the limit of P2.
- Print-built staged volumes: 5 units each from two *plakkaten* and two *nouvelles* volumes (seed 19): do the print
  rules (split, units) work on material outside the *gedrukte resoluties*?

## Constraints

- No library changes except bug fixes the audit forces (each one recorded, with a test). No `shared-data` or
  sibling writes. No external drive: the script is for the user to run there.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`; new dir per run.

## When done

Report section (comparison, audit table, prediction, staged-volume reads), findings `F10-15s.n`; `make reports`.
`PROGRESS.md` block; `STATUS.md`: phase 10 ready for review (prompt 3), the two UA IDs.
