# Step 15w: v4 dev build with every local volume, discovering audit, run kit

> **Phase:** 12, Hand and print by line-box height. **Depends on:** 15v (its `STATUS.md` facts: `15v-a` is the v4
> reference layer, the crosswalk verdict, the test count).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, this file,
> then the docstring of `scripts/run_text_layer_v3_full.py` (L2–49) and `tests/test_run_text_layer_v3_full.py`.
> **Read nothing else unless this brief names it.** Anchors checked 2026-10-25. Record any exploration under
> *Brief defects*.

## Context

15v made the build choose the hand or print rules per inventory from the inventory's own line-box height (presets
`sg_1.01.02_v4`, `sg_1.10.94_v4`; the decision and diagnostics under `mode/`). This step builds the v4 dev layer
from scratch on **every local volume** (198), replaces the pilot audit (fixed inventory lists) with one that discovers
its inventories (follow-up F09-15n.3), and turns the v3 hand-off script into a v4 one with one source list per
**archive** instead of hand/print "sides". It reads the volumes the 15u review flagged (near the cut; `print` outside
the printed series), because the likely error at scale is a handwritten volume called `print` (F12-R.2). It ends with
two user decisions (the full run, a v4 pilot). Nothing is published here.

## Inputs

- Sources (all local; never the external drive):
  - 1.01.02 pilot (14): `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02/` (symlinks; 3349–3354 point at tars in
    shared-data, the rest at dirs);
  - 1.01.02 staged (182): `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/`, 188
    `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` (5.0 GB) **minus 3349–3354** (already in the pilot; never link twice) =
    12 UA-017 + 60 phase-10 sample + 110 phase-12 (85 test incl. 25 reserves, 25 diagnostic incl. 5 reserves);
  - 1.10.94: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml/` (455, 456 zips).
- REPUBLIC: 1.10.94 only, `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/`.
  **No `--republic-dir` for 1.01.02** (decided in the 15v review: the 1.01.02 crosswalk matches 0.5% of the REPUBLIC
  lines, F12-15v.5). The units stage then logs one REPUBLIC warning for the print-mode 1.01.02 inventories: expected.
- Known issue, not to fix here: the print unit rules cut printed 1.01.02 paragraphs after their drop-cap lines, but
  separate decisions and *Dat …* clauses correctly (F12-15v.4, F12-R.3). **UA-026 (a):** keep the print rules in
  `15w-a` and the run kit, and document the cut as a known issue in `docs/text_layer.md` (row below). The fix is a
  later step, 15x (UA-026 (b)); do not start it here.
- Reference layers: `data/text_layer_v2_dev/15v-a/{1.01.02,1.10.94}` (v4), `15m-a/{hand,print}` (v3).
- Reference box values (168 volumes; 15u definition, so the v4 pre-pass must reproduce them exactly):
  `reports/data/phase-12-hand-print-box-height/step-15u/fit_volumes.tsv` (76: `set` dev_eval/transfer/sample,
  column `value`), `.../step-15u/test_volumes.tsv` (60, `value`), `.../step-15u/diagnostic_volumes.tsv` (20, `value`),
  and the 12 UA-017 volumes in `reports/data/phase-10-hand-print-per-region/review/h_by_volume.tsv` (rows `set ==
  "staged"`, column `h_w`). The 30 reserves have no reference.
- Series and silver per 1.01.02 inventory: `reports/data/phase-10-hand-print-per-region/step-15p/inventories.tsv`
  (`usecols=["inv", "silver", "ancestor_3"]`; `silver` ∈ hand, print, empty). Never join it to 455/456 (another archive).
- v3 run script `scripts/run_text_layer_v3_full.py` (358 lines; `.gitignore` L217 `/scripts/*`, L223
  `!/scripts/run_text_layer_v3_full.py`): `BUILD` L64, `CORESHARE` L65, `AUDIT` L66, `SIDES = {"hand":
  "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}` L67, `SPACE_FACTOR` L68, cost constants L77–80 (`ARCHIVE_XML_RATIO =
  4.16`, `OUTPUT_PER_XML_BYTE` and `WORKER_SECONDS_PER_XML_BYTE` per side, `SLOW_FACTOR = 3`); `link_side(side, srcs,
  work_dir, log)` L129, `select(links, subset, limit)` L161, `republic_missing(invs, republic_dir)` L167,
  `predict(side, xml_sizes, want)` L175 → `(output bytes, low s, high s)`, `side_summary(out_dir, side)` L196,
  `build_cmds(side, args, work_dir, out_dir, subset_file)` L212 (coreshare prefix with `--want`/`--min`, `--workers
  {workers}`, L223–225), `parse_args` L228, `main` L250; audit call L340–347 (`.venv/bin/python -I AUDIT --layer
  <out-dir> --out <out-dir>/audit.json`, not fatal). 8 tests in `tests/test_run_text_layer_v3_full.py`:
  `test_link_first_wins_and_ignores`, `test_relink_removes_stale`, `test_subset_missing_reports_and_exits`,
  `test_republic_warning`, `test_space_check`, `test_check_only_builds_nothing`, `test_refuses_outside_root`,
  `test_predict_range`.
- Pilot audit `reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py` (298 lines; `--layer`, `--out`,
  `--self-test`): `HAND`/`PRINT` lists L22–23, `layer_root` L46, `raw_checks(root, inv_by_table)` L67,
  `audit_layer(root, invs, sources, A, wbc_chars=None)` L92, `lota_keys(roots, A, doc_maps)` L219, `main` L238 (sources
  L249–251 from 15i-src and shared-data), `self_test` L277. Its result on `15m-a`:
  `reports/data/phase-09-text-layer-v3-prep/step-15m/audit_15m-a.json` (182 invariants, 178 pass, 4 fail: the
  "merges with lower-case right side ≥ 0.95" share for 3351 0.9415, 3823 0.9414, 455 0.9477, 456 0.932).
- Comparer: `reports/data/phase-12-hand-print-box-height/step-15v/compare.py` (15v's; run from the AS root; flags
  `--sub-map A:B,…`, `--ignore-paths P,…`, `--explained-invs I,…` (differing files counted, not listed), `--out F`).

## Deliverables

| File | Change |
|---|---|
| `data/text_layer_v4_staged/NL-HaNA_1.01.02/` (new) | 182 symlinks to the staged tars (skip 3349–3354); never write into shared-data |
| `data/text_layer_v2_dev/15w-a/{1.01.02,1.10.94}/` | v4 from scratch, `--stage all`, via the new run script |
| `reports/data/phase-12-hand-print-box-height/step-15w/audit.py` | the 15m audit generalised (below), `--self-test` on the fixture |
| `scripts/run_text_layer_v4_full.py` (+ `!/scripts/run_text_layer_v4_full.py` in `.gitignore` after L223) | v3 script by archive (below) |
| `tests/test_run_text_layer_v4_full.py` | the 8 v3 tests ported to archives, plus `test_two_archives_one_run`, `test_republic_per_archive` |
| `docs/text_layer.md` | in the v4 section (15v): *Running the full v4 build* (commands with `<drive>` placeholders), and a *Known issue* paragraph: in print-mode 1.01.02 volumes the print unit rules cut a paragraph after its first one or two (drop-cap) lines (3771: 8 of 10 pages read worse than the hand rules, units per page 25.9 → 45.5; F12-15v.4) but separate *WAER op gedelibereert* decisions and *Dat …* clauses (F12-R.3); kept by UA-026 (a), fix planned as step 15x |
| `docs/shared_library/changelog.md` | `### Step 15w: v4 run kit and audit`, with Edit, after 15v's entry |
| `reports/phase-12-hand-print-box-height.md` | fill `### Step 15w: v4 dev build, audit, run kit`; findings `F12-15w.n` |
| `docs/USER_ACTIONS.md` | two UAs (step 8), next free IDs UA-024, UA-025 (check both UA files for the highest) |

**Run script.** `--src ARCHIVE=DIR` (repeatable; ARCHIVE ∈ `1.01.02`, `1.10.94`; first source wins, duplicate
logged), `--republic-dir ARCHIVE=DIR` (repeatable), `--inventories-file ARCHIVE=F`, `--only ARCHIVE`, `--limit`,
`--check-only`, `--want`/`--min`, `--out-dir`; `ARCHIVES = {"1.01.02": "sg_1.01.02_v4", "1.10.94": "sg_1.10.94_v4"}`;
the layer under `<out-dir>/<archive>/`; log `run_v4.log`; audit = the new one. Cost constants per archive measured
on `15w-a` (same method as the v3 comment: XML bytes, output bytes, worker-seconds from `status.tsv`, extract
including the pre-pass; keep a comment with the numbers). Rewrite the docstring: what to stage per archive, order of
work, resume, logs, what the audit checks, that each volume's mode is decided from its own PageXML line-box height
(cut 87.75 px, raw pixels) and recorded under `mode/`, and that 1.10.94 is fixed `print`.

**Audit.** Sub-layers found under `--layer` by the glob `*/NL-HaNA_*_text_layer_v*`; inventories from each
sub-layer's `status.tsv` (done rows); sources from `manifest.json` `stages.extract.argv` `--pagexml-dir` (resolve
symlinks); the LotA-key invariant only for inventories present; the lower-case merge share stays **reported**, never
pass/fail. v4 invariants (skipped for a sub-layer without `mode/`):
- every done inventory has `mode/<inv>.json` with `mode` ∈ {hand, print}, `method` ∈ {box_height, default, fixed};
  for `box_height`, `mode == "print"` iff `value <= params.cut`; `mode/scans/<inv>.tsv.gz` rows = `n_scans −
  n_parse_fail`; `mode/regions/<inv>.tsv.gz` exists;
- `split_model/<inv>.json` exists iff the mode is `print`; no non-empty `split_from` in a hand inventory's `lines/`;
- unit methods `print:*` only in print inventories, `hand:*` only in hand ones;
- `layout/params.json`, `types/params.json`, `units/params.json` have keys `hand` and `print`.
Plus every phase-09 invariant that does not depend on the fixed lists.

## Steps

1. Port script and tests; make the symlink dir. `--check-only` on all local sources, as written:
   `.venv/bin/python scripts/run_text_layer_v4_full.py --check-only --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir data/text_layer_v2_dev/15w-a`.
   Expect 196 + 2 inventories.
2. Check `coreshare.py status`; `--limit 1` smoke into `data/text_layer_v2_dev/15w-smoke`, then the full build into
   `15w-a` (the script asks coreshare for its workers). Disk: 146 GB free on 2026-10-25; `15m-a` (16 volumes) is 389 MB.
3. Compare `15w-a` with `15v-a` on the 16 shared inventories (`compare.py … --sub-map 1.01.02:1.01.02,1.10.94:1.10.94`):
   equal on decompressed bytes except path columns (`scans.member`, `extract_stats` `source`, manifests) and corpus-level
   files that depend on the inventory set (`wbc/` corpus files, `*/stats.json`, `*/params.json`; list the keys).
   `15v-a/1.01.02` was built **with** `print_paragraphs_v1`, `15w-a` without, so for 3771–3773 and 3821–3823 also
   expect: `units/republic_lines/<inv>.tsv.gz` only in `15v-a`, the `republic_crosswalk*` keys of `units/stats.json`
   and `units/params.json` `republic_dir` differ. Any other difference in their unit, segment or join tables: stop.
4. Every `mode/<inv>.json` `value` against the 168 reference values: equal for 168/168. Mode counts, and the 30
   reserves' values listed.
5. Audit `15w-a`; then audit `15m-a` (v4 invariants skipped) and compare its phase-09 invariants with
   `audit_15m-a.json`: 178 pass, 4 fail, the same four.
6. Size and time per archive (XML bytes, output bytes, worker-seconds per stage, pre-pass included); predict the full
   1.01.02 run (9,122 inventories; the user estimates about 400 GB of XML) as a range, with the caveats of the v3
   docstring (inventory mix, USB drive).
7. Error analysis (below).
8. UAs:
   - **Decide + Run: the full v4 run.** The commands from the new docstring with `<drive>` placeholders; the evidence
     (F12-15u.1: 60/60 in the five silver series; F12-15u.6: diagnostic tests hand recall only; F12-R.2: about 5 of
     783 silver volumes expected wrong; your audit reads; the known unit issue of printed 1.01.02 volumes, F12-R.3,
     kept by UA-026 (a) with the fix planned as step 15x, so a full run before 15x has the drop-cap cut in about 70
     printed volumes); predicted size and time. Options (a) all 9,122 + 1.10.94,
     (b) a subset file (e.g. the volumes LotA aligns), (c) later. Default: no run. Blocks: the full v4 layer.
   - **Decide: publish `15w-a` as a v4 pilot** (`shared-data/NL-HaNA_*/text_layer_v4/`, `coverage: partial`), so LotA's
     printed 1.01.02 volumes get the print rules now. Options (a) publish, (b) wait for the full run. Default: nothing
     published. Blocks: LotA's use of 3771–3823 as print.

## Tests

- The 8 v3 tests ported (`side` → `archive`), `test_two_archives_one_run` (two `--src` archives, one layer per
  archive, one log), `test_republic_per_archive` (a warning only for the archive whose print inventories lack paragraphs).
- The audit's `--self-test` passes; a v3 layer runs without the v4 invariants.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_run_text_layer_v4_full.py
  tests/test_run_text_layer_v3_full.py`, then the full suite (before 15w: 1,233 collected, 1,222 passed, 11 skipped).
- Edit shared files (report, changelog, `STATUS.md`, `PROGRESS.md`, `docs/USER_ACTIONS.md`) only with the Edit tool,
  never with `sed`, `cat >>` or a script.

## Acceptance criteria

1. New and ported tests pass; full suite green (collect first; give the count before and after).
2. `15w-a` = `15v-a` on the 16 shared inventories up to the listed paths and corpus files; step 4: 168/168 values
   equal. Expected modes (computed at planning from the reference values, cut 87.75): of the 168, 1.01.02 `print` =
   3771–3773, 3821–3823, the 60 silver-print volumes of the phase-10 sample (30) and the 15u test set (30), and the
   four staged 12169, 12184, 3463, 9486 (70 in all); 455/456 `fixed`/`print`; the other 96 `hand`. A difference:
   stop and report.
3. The new audit passes every v4 and phase-09 invariant on `15w-a` except the reported shares, each failure listed
   with a cause; on `15m-a` it reproduces 178/4 with the same four failures.
4. Prediction table and the two UAs written.
5. Protected data unchanged (`diff` before/after): `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py <out> --skip 15w`
   (covers `15m-a`, `15n-a`, `15o-pub`, `15v-a`, shared-data, LotA data) and
   `find data/hand_print_geom reports/data/phase-10-hand-print-per-region reports/data/phase-12-hand-print-box-height/step-15u -type f | sort | xargs shasum -a 256 > <out2>`
   (outputs in `step-15w/`).

## Error analysis (verdicts in `step-15w/verdicts.tsv`: `printed`, `handwritten`, `form`, `table/numbers`, `ocr_junk`, `unclear`)

Context for each read: `text_head` and `h_med` of the 2–3 largest regions of the scan (from `mode/regions`), the
scan's value and side, the volume's series. `unclear` when the text does not settle it.
- **Near the cut** (15u review): every volume with |margin| ≤ 10 px. Measured at planning on the 168 references:
  39 1.01.02 volumes (78.0–97.0 px; 35 silver print in nouvelles/plakkaten, plus 3463, 3492, 4617, 9486) and 456 (82.5),
  plus any reserve that lands there. 2 scans each (the sided scans nearest the volume value, seed 21); 5 scans each
  for 3463, 3492, 4617, 9486 and every reserve in the band.
- **`print` outside the printed series**: every 1.01.02 `print` volume whose `silver` is not `print` (planning: 3463,
  silver hand, 86.5 px, 15p `print` 0.840; 9486, no silver, 79.0 px, *Processtukken*; plus any reserve), 5 scans each.
  Is it printed, or a handwritten volume on the wrong side (the F12-R.2 error)?
- **Minority material**: every volume with `scan_minority_share` > 0.2 that 15u did not read (15u read the 19 test
  volumes in `step-15u/verdicts.tsv`; skip those), 5 minority scans each (seed 19), with unit texts: real minority
  material (printed insert, handwritten slip) or type-size noise? The limit of one mode per volume.
- **Print rules outside the gedrukte resoluties**: 5 units each from two *plakkaten* and two *nouvelles* volumes
  (seed 19): do the split and unit rules work there? Categories `ok`, `bad split`, `bad unit boundary`, `unclear`.
- Every audit failure: 5 offending rows each (all, if fewer).
Write categories with counts, verbatim examples, and a conclusion in the report.

## Constraints

- No library changes except bug fixes the audit forces (each recorded, with a test). No `shared-data` or sibling
  writes. No external drive: the script is for the user to run there.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`; a new dir per run;
  no `rm -rf`, `nohup` or `&`; zsh: write the `coreshare.py run …` prefix out.

## Do not touch

`data/text_layer_v2_dev/{15m-a,15n-a,15o-pub,15i-src,15v-a}`, `data/hand_print_geom/`, `reports/data/phase-10-*`,
`reports/data/phase-12-*/step-15u/`, `scripts/run_text_layer_v3_full.py` and its tests, the published layers.

## When done

1. Report section (comparison, mode check, audit table, prediction, reads with categories and examples, conclusion),
   findings `F12-15w.n` (`draft`); `make reports` without warnings.
2. Append the block below to `PROGRESS.md`; overwrite `STATUS.md`: phase 12 ready for review (prompt 3), the two UA IDs.

```markdown
### Step 15w: v4 dev build, audit, run kit
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <or "none">
- **Tests:** <command> → <result>
- **Acceptance:** <1–5, with numbers>
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15w](../../reports/phase-12-hand-print-box-height.md#step-15w-v4-dev-build-audit-run-kit); findings F12-15w.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <UA IDs>
```
