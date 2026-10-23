# Step 15n: hand-off script for the full `text_layer_v3` run

> Post-review addendum to phase 09 (2026-10-22, from UA-015/UA-016). Opus wrote this brief; a
> Sonnet session implements it. Read only the files named here.

## Context

The full `text_layer_v3` run (UA-015) cannot be staged on the local disk: there is not enough space for all
SG PageXML. The user will put the PageXML archives on an external drive and run the build **themselves**,
on this machine, reading from the drive and writing the layer to the drive. You write the script they run.

**Hard rule: never access an external drive.** Run nothing (`ls`, `df`, `stat`, …) on a mounted-volume path or on
any path not named in this brief. All tests and smoke runs use the local paths listed under *Inputs*. Drive paths
reach the script only as CLI arguments when the user runs it. Never write a drive path, real or made up, into code,
docs, tests or logs; use placeholders such as `<drive>/sg_hand`.

The build CLI already does what a hand-off needs:
- it reads one `--pagexml-dir`, with an entry per inventory (`.tar.gz`, `.zip` or a directory);
- it resumes, because every stage marks each inventory done in `status.tsv`;
- it has `--limit`, `--inventories-file` and `--estimate`.

The script adds four things:
1. Combining several source dirs into one input dir.
2. Checks before anything runs.
3. Launching through `coreshare`.
4. Logging, plus the audit afterwards.

## Inputs (all local)

- Build CLI: `.venv/bin/as-build-text-layer` (see `--help`). Presets: `sg_1.01.02_v3` (hand), `sg_1.10.94_v3`
  (print).
- Inventory discovery: `archival_structures/text_layer/io.py` `discover_inventories_v2(pagexml_dir, pattern)`
  (lines 191-206). It takes entries whose name `fullmatch`es the preset pattern
  `NL-HaNA_<archive>_(?P<inv>[0-9]+[A-Z]*)(?:-pagexml)?(?:\.zip|\.tar\.gz|\.tgz)?`
  (`presets.py` `_sg_source_pattern`, line 67), skips dot-files, and lets a file win over a dir. It follows
  symlinks (`is_file()`/`is_dir()`).
- Local pilot sources for tests:
  - hand: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml` (6 tars) **and**
    `data/PageXML/NL-HaNA/NL-HaNA_1.01.02` (8 flat dirs);
  - print: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml` (455, 456 zips);
  - REPUBLIC paragraphs: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94`
    (`paragraphs-455.tsv.gz`, `paragraphs-456.tsv.gz`).
- Reference layer: `data/text_layer_v2_dev/15m-a/{hand,print}` (built from exactly these sources with the v3
  presets).
- Pilot timings for the time estimate: `data/text_layer_v2_dev/15m-a/{hand,print}/*/status.tsv` (columns
  `inventory_number, stage, status, seconds, input_bytes, input_xml_bytes, ...`). Output size: hand 305 MiB, print
  84 MiB.
- `coreshare`: `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project
  archival-structures --label <l> --want N --min M --wait -- <cmd ... --workers {workers}>`. Run its `status`
  subcommand before any local multi-worker run.
- Compare tool: `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/compare.py BASE_A BASE_B
  [--sub hand,print] [--out F.json]`.
- Audit: `reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py --layer BASE --out F.json`. Read its
  invariants: some expected values may be pilot-specific.

## Deliverables

1. `scripts/run_text_layer_v3_full.py`. The module docstring is the user's manual. It lists:
   - what to put on the drive, and the accepted names (`NL-HaNA_1.01.02_<inv>-pagexml.tar.gz`, `NL-HaNA_1.10.94_<inv>.zip`, …);
   - the order to run things in: `--check-only`, then a `--limit 1` smoke run into a separate out dir, then the full run;
   - what to do after an interruption (rerun the same command);
   - where the logs are.
2. `tests/test_run_text_layer_v3_full.py`. It loads the script with `importlib` from its path.
3. A one-line fix in `archival_structures/text_layer/build.py`: the `--republic-dir` help (around line 761) still
   says "(default: LotA's data/paragraphs)". Step 15k removed that default, so change it to say that without the
   option, print builds have no crosswalk.
4. In `docs/text_layer.md`, section *Text layer v3 (phase 09)*, add a paragraph *Running on an external drive* naming
   the script. Keep the existing command block.

## Behaviour to implement

CLI (argparse, no prompts, every path explicit):

```
--hand-src DIR        repeatable; sources for sg_1.01.02_v3, in priority order
--print-src DIR       repeatable; sources for sg_1.10.94_v3
--republic-dir DIR    paragraphs-<inv>.tsv.gz for the print inventories (optional; warn if absent)
--out-dir DIR         layer base; the script writes <out-dir>/hand and <out-dir>/print
--work-dir DIR        local dir for the input links (default data/text_layer_v3_full_inputs)
--hand-inventories-file F / --print-inventories-file F   optional subsets, passed through
--only {hand,print}   run one side
--limit N             passed through (smoke test)
--check-only          do every check and the report below, build nothing
--want N (default 8) --min M (default 2)
--ignore-space        run although the free-space check fails
```

1. **Link the sources.** For each side, (re)create `<work-dir>/<side>/` holding one symlink per inventory entry
   found in the `--*-src` dirs. Use `discover_inventories_v2` with the preset's pattern; don't write a second
   regex. Pass the pattern from the preset object; find its attribute name in `presets.py`.
   - The first source that has an inventory wins. Log every inventory that is also in a later source, and say which
     copy was used.
   - Link to absolute paths. Remove stale links before relinking.
   - Never create anything under a `--*-src` dir or write into it.
2. **Checks** (all logged, all run before any build):
   - every `--*-src` dir and `--republic-dir` exists;
   - `--out-dir` exists and is writable: create a test file and remove it;
   - inventory counts per side;
   - every inventory in a subset file must be found, otherwise exit with the missing list;
   - for print, which inventories have no `paragraphs-<inv>.tsv.gz`: a warning, not an error;
   - input bytes per side: the sum of the entries' sizes (`du`-like for dirs);
   - predicted output size: input bytes × the pilot ratio (output size / input bytes of `15m-a`, per side), computed
     from the numbers in *Inputs* and hard-coded with a comment saying where they came from;
   - predicted wall time: pilot worker-seconds per input byte (from `15m-a` `status.tsv`, all stages), divided by
     `--want` workers. Label it a rough estimate;
   - free space on `--out-dir` (`shutil.disk_usage`): exit if it is below 1.2 × the predicted output, unless
     `--ignore-space`.
3. **Run** (not with `--check-only`), per side, hand first:
   1. call `as-build-text-layer --pagexml-dir <work-dir>/<side> --out-dir <out-dir>/<side> --preset … --stage all --estimate --workers <want>`;
   2. then the real build through `coreshare run … --label v3-<side> … --workers {workers}`, with `--republic-dir` for print.
   - Use `.venv/bin/as-build-text-layer` relative to the AS root. The script must refuse to start when cwd is not
     the AS root (check `pyproject.toml` with `name = "archival-structures"`, or how the repo identifies itself).
   - Stream the child output to the console and to `<out-dir>/run_v3.log` (append, with UTC timestamps for the
     script's own lines).
   - On a non-zero exit, stop, print the log path, and say that rerunning the same command resumes.
4. **After the build:**
   - run the audit on `<out-dir>` with `--out <out-dir>/audit.json`;
   - log its pass count. Don't fail the script on a pilot-specific invariant; say in the docstring which invariants
     are pilot-specific;
   - log a final summary: inventories done per side, failures, output size.

Code style: match `scripts/*.py` and `archival_structures/text_layer/build.py`. Keep it under ~350 lines.

## Constraints

- Never touch an external drive (see *Context*).
- Use `.venv/bin/python`. Run local multi-worker jobs only through `coreshare` (`status` first), and use the granted
  count.
- `chmod g+w` every file and dir you create (also the dev-layer dirs from the smoke runs).
- Don't change the build's behaviour or any preset. The `--republic-dir` help text is the only library change.
- Don't commit.

## Tests

All tests use `tmp_path` and tiny fake entries (empty files named like tars, empty dirs); no real PageXML.
- linking: two sources sharing one inventory, so the first wins and the duplicate is logged; dot-files and
  non-matching names are ignored; a re-link removes stale links;
- the subset check reports missing inventories;
- the republic coverage warning;
- the space check fails below 1.2× and passes with `--ignore-space`;
- `--check-only` builds nothing (mock `subprocess`).

Run `.venv/bin/python -m pytest tests/test_run_text_layer_v3_full.py -q` and then the whole suite: 1,205 tests passed
before this step.

## Acceptance criteria

1. `--check-only` on the local pilot sources:
   - `--hand-src` gets both hand sources, `--print-src` the print source, `--republic-dir` the LotA dir, and
     `--out-dir data/text_layer_v2_dev/15n-a` (create it first);
   - the result is hand 14 inventories, print 2, republic coverage 2/2, no duplicates, exit 0.
2. Smoke: the same with `--limit 1 --out-dir data/text_layer_v2_dev/15n-smoke` exits 0, and both sides have one
   inventory done.
3. Full local run into `data/text_layer_v2_dev/15n-a`:
   - `compare.py data/text_layer_v2_dev/15m-a data/text_layer_v2_dev/15n-a` gives every table equal;
   - the only allowed differences are source-path columns in `scans/`, because they now name the link. If anything
     else differs, stop and record it as an open question;
   - audit pass count equal to `15m-a` (178/182).
4. Report how close the predicted output size and time came to the actual ones.
5. Test suite green.

## Error analysis

- Compare predicted with actual output size and wall time on the pilot. Say how far off they might be on the full
  run (different inventories, USB read speed).
- For every `scans/` difference that compare reports, read 3 rows and say what differs.

## Do not touch

`join.py` and the other v1 paths; the presets; `shared-data/`; the phase report
`reports/phase-09-text-layer-v3-prep.md` (the reviewer adds the 15n section).

## When done

Append a `### Step 15n: hand-off script for the full text_layer_v3 run` block to the phase `PROGRESS.md` (template
in `plan/templates/STEP.md`, *When done*), with the acceptance numbers and findings listed there as F09-15n.1–n. Then
overwrite `STATUS.md`: 15n done, next action review.
