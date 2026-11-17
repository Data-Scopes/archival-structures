# Step 25b: Fresh test pages, v2 reader pre-labels, check page v2, test gold (UA)

> **Phase:** 19, Page elements v2. **Depends on:** 25a (`datasets/element_gold.py`, the dev v2 files).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to explore the code base
> anyway, do what you must and record it under *Brief defects*.

## Context

Phase 19 tests a margin split and an element reader (steps 25c, 25d) on **fresh** gold: 60 pages from volumes and scan
ranges phase 18 did not use, in the same 10 strata. As in phase 18, agents pre-label every line and the user corrects
the labels on a check page. The labels now follow gold format v2 (25a): type, placement, element id, item start, prose.
The step runs in parts, because reader agents can only be started by the orchestrating session and the gold needs the
user: **A** (you) stage, build, sample, images, dumps, reader prompts and batches, then stop; **B** (orchestrator) runs
the readers; **C** (a new session, you or another) merges, self-checks, builds the check page and hands over a UA;
**D** the user corrects and runs `summarise_v2.py`. The STATUS file says which part is next.

## Inputs

- `reports/data/phase-19-page-elements-v2/planning/volumes.tsv` (sha256
  `24a2b456cf460e74d3a817e36d2d1b0125a9574c9c774b86c6591faf8ace1a2a`; check first, stop if it differs). Columns
  `stratum archive inv split source n_files`; `split` = `test` (3 volumes in each of sg_hand, sg_print, voc,
  na_2.10.01, na_2.10.50, na_1.05.06) or `fresh_pages` (the single volumes of asdsaa_5023 `4`, asdsaa_5075 `2409`,
  asdsaa_89 `3.1`, asnda_0114.11 `1`). `planning/dev_run_inventories.txt`: 233 SG inventories (none is in volumes.tsv;
  assert it).
- Phase-18 step-24b scripts to copy and adapt (read them): `reports/data/phase-18-page-element-types/step-24b/`
  `stage_inputs.py` (49 lines), `build.sh` (22), `draw_pages.py` (152), `fetch_images.py` (83), `dump.py` (61). Phase-18
  sample `step-24b/sample_pages.tsv` (100 rows, columns `page_id stratum archive inv split scan_id layer_root
  n_lines_text page_w page_h image_kind image_ref image_file`), and step-24c scripts `reader_prompt.md` (133 lines),
  `make_batches.py`, `check_reads.py`, `merge_reads.py`, `build_element_page.py`, `element_page.js` (265),
  `element_page.css`, `summarise_elements.py` (read them when you adapt them).
- Sources (read only): 2.10.50 zips `/Volumes/T7_Data/Data/PageXML/Ministerie_van_Kolonien/2.10.50/scans/<inv>.zip`
  (members `Volumes/T7_Data/.../2.10.50/<inv>/page/NL-HaNA_2.10.50_<inv>_<nnnn>.xml`); 1.05.06 zips
  `/Volumes/T7_Data/Data/PageXML/NA/HTR results 1.05.06 PAGE/<inv>.zip` (members `NL-HaNA_1.05.06_<inv>_<nnnn>.xml`);
  2.10.01 zips `/Volumes/T7_Data/Data/PageXML/Ministerie_van_Kolonien/2.10.01/scans/<inv>.zip`. Skip members whose base
  name starts with `._`.
- Layers (read only): SG `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5`;
  VOC `data/text_layer_voc_23b/1.04.02/NL-HaNA_1.04.02_text_layer_v6`; the four single-volume strata
  `data/page_elements_layers/24b-a/<archive>/<archive>_text_layer_v5sg` (phase 18; same volumes, not rebuilt).
- 25a: `archival_structures/datasets/element_gold.py` (`GOLD_V2_COLS`, `read_gold`, `write_gold`, `type_error`,
  `validate_rows`, `element_table`, `to_annotations`, `ITEM_FAMILIES`); dev files
  `reports/data/phase-19-page-elements-v2/step-25a/dev/devA_v2.tsv`, `devB_v2.tsv` (check `dev/SHA256SUMS`).
- Thumbs (read only) under `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/thumbs/`
  (`thumb_path` in 24b's `draw_pages.py`); 1.05.06 thumbs are `NL-HaNA/NL-HaNA_1.05.06/<inv>/<scan_id>.tif`.

`D = reports/data/phase-19-page-elements-v2/step-25b/`. Every script resolves paths against the AS root and says
"run from the AS root" in its docstring.

## Part A (Sonnet)

1. **Stage** (`D/stage_inputs.py`): for na_2.10.50 and na_1.05.06 test volumes, extract the xml members flat into
   `data/page_elements_inputs_v2/<archive>/<archive>_<inv>/` (base names; assert count == `n_files`); for na_2.10.01,
   symlink `data/page_elements_inputs_v2/NL-HaNA_2.10.01/<inv>.zip` → the drive zip. Check write access to `data/` first.
2. **Build** (`D/build.sh`, copy of 24b's): presets `na_2.10.50_v5sg`, `na_1.05.06_v5sg`, `na_2.10.01_v5sg`;
   `--pagexml-dir data/page_elements_inputs_v2/<a> --out-dir data/page_elements_layers/25b-a/<a> --stage all`, through
   `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 25b --want 4 --min 1 --wait -- … --workers '{workers}'`
   (run `coreshare.py status` first). Expect 0 failures (planning built 2.10.50 `835` this way: 70 files, 0 failures).
   Log to `D/build_log.txt`.
3. **Draw** (`D/draw_pages.py`, adapted from 24b's): layer roots as above (25b-a for the three NA archives);
   eligibility unchanged (`status == "ok"`, `3 <= n_lines_text <= CAP` with `CAP = {"sg_print": 300}` else 150, an image
   source by 24b's `image_source`); `test` volumes: k = 2 per volume; `fresh_pages`: k = 6 from the whole volume after
   removing every scan in the phase-18 `sample_pages.tsv` for that inventory and every scan within ±3 positions of one
   in the scans-table order. Seed `random.Random(f"20261117-{stratum}-{inv}")`, `rng.sample(sorted(eligible ids), k)`;
   a shortfall stops the run (report it, do not top up). Page ids `T001`…`T060` sorted by stratum order (as 24b's
   `STRATA`), volume order in volumes.tsv, scan order. `image_file = data/page_elements_images_v2/<pid>.jpg|png`. Write
   `D/sample_pages.tsv` (24b's columns + `n_eligible`) and `D/SHA256SUMS`. Assert: no SG inventory in
   `dev_run_inventories.txt`; no (scan_id) in the phase-18 sample.
4. **Images** (`D/fetch_images.py`, adapted): same checks as 24b, into `data/page_elements_images_v2/`;
   `D/image_checks.tsv`; IIIF cache in `D/iiif_cache/`.
5. **Dumps** (`D/dump.py`, adapted): `D/dumps/<pid>.txt` and `<pid>.lines.tsv` exactly as 24b's format;
   `D/dump_stats.tsv`.
6. **Reader prompt v2** `D/reader_prompt_v2.md`: copy 24c's `reader_prompt.md` and change: paths (dumps in `D/dumps/`,
   images in `data/page_elements_images_v2/`, output `D/reads/<pid>.tsv`, check command `D/check_reads_v2.py`); output
   columns `k line_id type placement element item_start prose unclear note`; the field rules of format v2 (below); rubric
   additions, hard cases first: (i) a register, ledger or inventory with an amount column is one `table` element
   (description lines and amounts), its column heads `table:header`; (ii) item descriptions written as sentences keep
   type `table`/`list`, the element gets `prose = 1`; (iii) a note or insertion inside a table is its own element
   (`note` or `addition` with its placement) and the table lines after it keep the table's element id; (iv) form vs
   table: a ruled or pre-printed grid of repeated rows is `table`; `form` is a document with captions beside blanks filled
   once; (v) `letterhead`, `copymark` (definitions as 25a); (vi) insertions of omitted text are `addition` with their own
   placement. Remove the `start` field. Keep "Do not open" (add: no `data/page_elements_layers/`, no AS analysis outputs,
   no `reports/data/phase-19-page-elements-v2/` except `D/dumps/`, `D/reads/` and the prompt).
   **Field rules:** `element` = `e1`, `e2`, … numbered by first line in k order; every line belongs to exactly one
   element; a run of lines of one kind is one element; an interrupted element keeps its id. `item_start` = 1 on the first
   line of each item of a `list`, each row of a `table` (header rows too) and each entry of a `form`, else 0; 0 on all
   other types. `prose` = 1 on every line of a list/table/form element whose items are written as sentences, 0 on other
   list/table/form elements, empty on all other types.
7. **Dev item prompt** `D/dev_item_prompt.md` and inputs: for each of the 100 phase-18 pages (P001–P100, dumps in
   `reports/data/phase-18-page-element-types/step-24b/dumps/`, images `data/page_elements_images/<pid>.*`) write
   `D/dev_item_inputs/<pid>.tsv` with `k line_id type element` from the 25a dev files. The reader writes
   `D/dev_item_reads/<pid>.tsv` with `k line_id item_start` for every line: 1/0 on lines whose type family is list,
   table or form, by the item_start rule above; 0 on all others. It may not change types or elements.
8. **Checks and batches:** `D/check_reads_v2.py [--pages …] [--dev]` (from 24c's `check_reads.py`: per-line checks with
   `element_gold.type_error`; then build the page's v2 rows and run `element_gold.validate_rows` on them; for `--dev`,
   every k once, `item_start` in {0,1}, 0 outside list/table/form, the first line of each list/table/form element 1).
   `D/make_batches_v2.py`: test batches as 24c (`MAX_PAGES, TARGET_LINES, BIG, MAX_PAGES_BIG = 8, 600, 200, 3`, one
   stratum per batch) → `D/reader_batches.tsv`; dev item batches with `12, 1000, 300, 4` → `D/dev_item_batches.tsv`.
9. `find data/annotations -type f -exec stat -f '%z %N' {} + | sort -k2 > D/annotations_list_before.txt`.
10. Stop. In STATUS: "Part B: orchestrator runs readers per `D/reader_batches.tsv` and `D/dev_item_batches.tsv`".

## Part B (orchestrator, not a step session)

Reader agents (Sonnet), one per batch, each with the prompt file and its page list; each runs the check script on its
pages before finishing. The orchestrator re-runs both checks on all pages and records any re-reads in PROGRESS.

## Part C (Sonnet)

1. `D/merge_reads_v2.py` → `D/prelabels_v2.tsv` (`GOLD_V2_COLS` + `reader`), refusing on any check error; and the dev
   items: copy `devA_v2.tsv`/`devB_v2.tsv` with `item_start` from the reads into `D/dev_items/devA_v2.tsv`,
   `devB_v2.tsv`; `validate_rows` → 0 errors; `D/dev_items/SHA256SUMS`.
2. **Self-check** of 30 test pre-labels (`random.Random(20261117)` over lines, stratified 3 per stratum), each read
   against the image: `D/selfcheck.tsv` (`row page_id k field label verdict comment`), with the same for 10 dev item
   starts. Report the agreement; it misses convention errors (F18-R.12), so also list three convention questions you met.
3. **Check page v2** (`D/build_page_v2.py`, `D/page_v2.js`, `D/page_v2.css`, copied from 24c's current files, which
   already have the arrows left of the counter and the page-jump dropdown): `KEY = "as_phase19_25b_elements_v2"`;
   `IMG_REL = "../../../../data/page_elements_images_v2/"`; pages T001–T060; stored per line `type placement element
   item_start prose unclear note`. Keys: type letters, subtype digits, Shift+1..5 placement, `u` unclear, `k` note,
   Cmd/Ctrl+Z, Enter, ←/→ as now; **`b`** = the selected lines become a new element (id = max + 1; `prose` 0 for
   list/table/form, empty else); **`j`** = the selected lines join the element of the nearest unselected line above
   them in k order (they take its prose); **`i`** toggles `item_start`; **`p`** toggles `prose` on every line of the
   element(s) of the selection. Show each row's element id and a coloured left band per element, `▸` on item starts.
   A **Rulings** box at the top renders `D/rulings.md` (create it with a heading and "none yet"). Export
   `element_verdicts_v2.json` (`{page, exported, items: [{id, reviewed, page_note, lines: [{k, line_id, type, placement,
   element, item_start, prose, unclear, note}]}]}`). Instructions: the format-v2 field rules, the rubric of
   `reader_prompt_v2.md`, the keys. Check the JS with `node --check` on the extracted script.
4. `D/summarise_v2.py` (user runs it; from 24c's `summarise_elements.py`): refuse if `D/gold/test_gold.done` exists
   (exit 2); apply the export to `prelabels_v2.tsv`; renumber element ids per page by first appearance; run
   `validate_rows` (errors → print, exit 1, write nothing); write `D/gold/test_v2.tsv` (`GOLD_V2_COLS` + `pre_type
   pre_placement pre_element pre_item_start pre_prose changed_type changed_placement changed_element changed_item
   changed_prose reviewed page_note`; `changed_element` = the set of k in the line's element differs from the pre-label's),
   `D/gold/test_elements.tsv` (`element_table`), annotations via `to_annotations(rows of reviewed pages, D/gold/annotations)`,
   `D/gold/SHA256SUMS`, `D/gold/test_gold.done` (UTC time + sha256 of test_v2.tsv). Unreviewed pages are left out.
   Test it on a fake export in the scratchpad (`--gold-dir` option), never in `D/gold/`.
5. Add a UA to `docs/USER_ACTIONS.md` (next free id; format in `CLAUDE.md`): Run `open "<abs path>/D/page_v2.html"`;
   correct the 60 pages; Export into `D/`; then Run from the AS root `.venv/bin/python -I
   reports/data/phase-19-page-elements-v2/step-25b/summarise_v2.py`. Options (a) all 60 (b) as many as possible
   (recommended (a)); default: no test gold, 25e reports dev only; blocks 25e. Add: rulings made during the review go
   into `D/rulings.md` with the first page they apply to, and the page is rebuilt (same key).

## Constraints

- Readers and the check page show no AS calls (layers' types/layout, `listlike`, `page_grid`, detectors, region tags).
- Write nothing on the drive; a hook denial or `Permission denied` is final. `data/annotations/` is never written
  (before/after lists). Do not modify phase-18 files or `data/page_elements_layers/24b-a/`.
- Never `git commit`; `chmod g+w` every file and dir; no `rm -rf`, `nohup`, `&`, `$TMPDIR`; zsh (quote `'{workers}'`);
  BSD `sed -i ''`; a new output dir per run.

## Acceptance criteria

1. Part A: 9 builds' inventories with 0 failures; 60 pages (6 per stratum) with an image and a dump; all asserts pass.
2. Part C: `check_reads_v2.py` 0 errors on 60 test and 100 dev pages; `validate_rows` 0 errors on prelabels and dev items;
   page builds and `node --check` passes; `summarise_v2.py` passes its scratch test.
3. Part D (after the UA): `test_gold.done`; annotations before == after.

## Error analysis

Part A: per stratum, eligible scans and pages drawn; read 3 dumps per stratum against the image (is the dump complete,
are there two pages?). Part C: the self-check; per field the share of empty or default pre-labels; the 10 pages with the
most elements. Write it into the report.

## When done

Each part adds to the `### Step 25b: Fresh test gold v2` section of `reports/phase-19-page-elements-v2.md` (findings
`F19-25b.n`, draft), runs `make reports`, appends a PROGRESS block (fields as in `plan/templates/STEP.md`: session,
status, files, tests, acceptance, report link, error analysis, deviations, brief defects, open questions) and
overwrites STATUS (part done, next part).
