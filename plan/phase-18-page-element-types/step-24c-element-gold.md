# Step 24c: Run-labelling check page, agent pre-labels, gold freeze (UA-063)

> **Phase:** 18, Page element types v1. **Depends on:** 24a (vocabulary), 24b (`sample_pages.tsv`, dumps, images).
> **Model:** Sonnet for parts A, C, D; **part B is run by the orchestrating session** (step sessions cannot spawn reader
> agents; CONTEXT §5). **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file. **Read nothing else unless this brief names it.** Record any exploration under *Brief defects*.

## Context

Every non-empty line of the 100 sampled pages gets a **type** (a `generic:` tag), a **placement**, a **start** flag (an
element begins here) and an **unclear** flag. Sonnet reader agents pre-label all 100 pages from the dumps and images.
The 40 dev pages keep the agent labels as dev gold. The user corrects the 60 test pages on a check page (UA-063); those
corrections are the test gold. The readers never see AS's own calls (layer types, layout, `listlike`), because 24d
measures those calls against this gold.

## Inputs

- `reports/data/phase-18-page-element-types/step-24b/` (call it `B`): `sample_pages.tsv` (check its hash in
  `B/SHA256SUMS`; columns `page_id stratum archive inv split scan_id layer_root n_lines_text page_w page_h image_kind
  image_ref image_file`), `B/dumps/<page_id>.txt` and `.lines.tsv` (`k line_id region_id x y w h text`), images in
  `data/page_elements_images/`.
- Vocabulary (24a): `from archival_structures.datasets.vocabulary import GENERIC_TYPES, PLACEMENT_TYPES, is_known_tag,
  make_tag`; definitions of the types in `docs/vocabulary.md` § placement / new types (read that section).
- Annotation API (`archival_structures/datasets/annotations.py`): `ScanAnnotation(scan_id, opening=None, tags=[],
  pages={}, lines={line_id: [tags]}, regions=[])`, `Element(element_type: str, spans: List[ElementSpan])`,
  `ElementSpan(scan_id: str, line_ids: List[str])`, `scan_annotation_path(institute_id, archive_id, inventory_num_id,
  scan_id, annotations_dir=...) -> Path` (→ `<dir>/<institute>/<archive>/<inv>/annotations-<scan_id>.json`),
  `elements_path(institute_id, archive_id, inventory_num_id, annotations_dir=...)`, `save_scan_annotation(annotation,
  path)`, `save_elements(elements, path)`.
- Check-page helpers: `sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools"); import checkpage`:
  `checkpage.BASE_CSS`, `checkpage.HELPERS_JS` (JS `class CheckStore(key)` with `get(id)`, `set(id, patch)` (merges),
  `exportJSON(filename, page, items)`, `importJSON(file, idOf, done)`; `esc`, `naUrl`), `checkpage.scan_url(scan_id)`.
  Read the README section "checkpage" in `/Users/Shared/claude_projects/data_scopes/shared-tools/README.md` (l.42–86).
- A working zoom viewer to copy from: `reports/data/phase-17-voc-rules-v1/step-23a/build_folio_page.py` (188 lines;
  `initZoom`, wheel/pinch/drag, `+`/`−` buttons; how it injects CSS/JS and replaces `%KEY%`). Read it.

## Deliverables (`D = reports/data/phase-18-page-element-types/step-24c/`)

| File | Content |
|---|---|
| `D/reader_prompt.md`, `D/reader_batches.tsv` | part A: the prompt (rubric below) and batches of ≤ 8 pages |
| `D/check_reads.py`, `D/merge_reads.py` | validate reads; merge into `D/prelabels.tsv` |
| `D/reads/<page_id>.tsv` | part B: one file per page (`k line_id type placement start unclear note`) |
| `D/gold/dev_labels.tsv`, `D/gold/SHA256SUMS`, `D/gold/dev_gold.done` | dev gold (agent labels, frozen) |
| `D/build_element_page.py`, `D/element_page.html` | the user's page (60 test pages) |
| `D/summarise_elements.py` → `D/gold/test_labels.tsv`, `D/gold/test_gold.done`, `D/gold/annotations/` | part D |
| `docs/USER_ACTIONS.md` | UA-063 (Edit to append) |
| `D/annotations_list_{before,after}.txt` | `find data/annotations -type f | sort` + sizes, to show it is untouched |
| `reports/phase-18-page-element-types.md` | `### Step 24c: …` section, findings `F18-24c.n` |

## Behaviour to implement

**Part A (Sonnet): reader prompt and tooling.** `reader_prompt.md` is self-contained (the reader reads only the files
it names: its dumps and images). It gives the output format, the type list with definitions (copy from
`docs/vocabulary.md`), placements, and this **rubric, fixed before any read**:
1. Type = what the element does; placement = where it sits (`main` column(s), `margin` beside the main text, `top`/
   `bottom` band above/below it, `interlinear` between main lines). A marginal summary is `heading:summary` + margin; a
   marginal text that continues or corrects the main text is `addition` + margin; a marginal remark about it is `note`.
2. One label per line. A line holding two elements (a page number merged into the first text line): label the larger
   part, note `merged`.
3. `list`: items one per line or a few short lines (names, enclosures, index or TOC entries, attendance lists). `table`:
   aligned columns across lines (amounts, dates, ledger rows). `form`: pre-printed captions (`form:label`) and filled-in
   entries (`form:value`); a pre-printed grid of rows is `table`. A locator on a line of its own is `reference`.
4. Dates: one opening a sitting, entry or letter = `heading:date`; a page-top date repeating the current unit's date =
   `running_title:date`; place and date at the end of a document = `closing:dating`.
5. End of a document: formula (`closing:formula`), signatures (`closing:signature`). Letter address and salutation:
   `salutation`. Docket or receipt note on a letter's back: `endorsement`.
6. Numbers: `numbering:folio` for one number per leaf in a corner of a manuscript leaf; `numbering:page` for numbers on
   both sides or in print; `numbering:item` for paragraph or item numbers; otherwise `numbering:other`. Catchword at a
   page foot: `quire_marks:catchwords`; quire signature (A2, B): `quire_marks:signature`.
7. Garbage, bleed-through, text from the facing page edge: `noise`. A lemma or group heading in an index: `heading`.
8. `start = 1` on the first line of each element (a whole list or table is one element); the page's first line always
   has `start = 1`, even when it continues the previous page.
9. When the reader cannot decide, `unclear = 1` with its best guess, and a note. Anything that fits no type:
   `custom:<word>` and a note.
Output: `reads/<page_id>.tsv` with header `k line_id type placement start unclear note`, one row per dump line; `type`
without the `generic:` prefix (e.g. `heading:date`), `placement` without `placement:`.

`check_reads.py`: every page has a read; every k of the dump appears once; `is_known_tag("generic:" + type)` (or
`custom:`), placement in `PLACEMENT_TYPES`, start/unclear in {0,1}, first line start = 1. `merge_reads.py` →
`prelabels.tsv`: `page_id split stratum archive inv scan_id line_id k type placement start unclear note reader`.
`reader_batches.tsv`: `batch page_ids`, ≤ 8 pages per batch, pages of one stratum together where possible.

Stop after part A with `STATUS.md` state "waiting for reads (part B)".

**Part B (orchestrator).** Run one Sonnet reader agent (`model: "sonnet"`) per batch, in parallel, with
`reader_prompt.md` plus its page list. Readers may open only their dumps and images; they never open `layout/`,
`types/`, `regions_meta/`, `data/annotations/` or any other read. Run `check_reads.py`; re-run a failed page once with
the error in the prompt. Record in `PROGRESS.md` how many reruns were needed. No rubric change after the reads start; a
case the rubric does not cover goes in the note and the report.

**Part C (Sonnet).** 
1. `merge_reads.py`. **Self-check:** read 30 lines drawn with `random.Random(20261102)` from `prelabels.tsv` (dev and
   test), each against the dump and the image, and write your verdict (agree / type wrong / placement wrong / start
   wrong) into `D/selfcheck.tsv`; report the agreement.
2. Dev gold: the dev rows of `prelabels.tsv` → `gold/dev_labels.tsv` (same columns); sha256 into `gold/SHA256SUMS`;
   `gold/dev_gold.done` (UTC time + hash). Refuse to overwrite an existing `.done`.
3. `build_element_page.py` → `element_page.html`, the 60 test pages in `page_id` order, pre-labels embedded:
   - **Layout:** left, the page image with one SVG box per line (PageXML x/y/w/h scaled by image width / `page_w`),
     coloured by type family; right, the line list (k, text, type chip, placement chip, start bar, unclear mark). Header:
     page id, archive and inventory, scan id (NA: `checkpage.scan_url` link), `n reviewed / 60`, a page note field.
   - **Zoom** (copy 23a's viewer): image and SVG in one transformed wrapper so boxes follow; for `na_iiif`/`iiif3` swap
     to `{base}/full/2400,/0/default.jpg` when zoomed beyond 1.5×; thumbs have no higher resolution.
   - **Selection:** click a row or a box; Shift+click extends the range in k order; Cmd/Ctrl+click toggles; Esc clears.
   - **Keys** (shown in a legend): type letters `r` running_text, `l` list, `t` table, `f` form, `h` heading, `s`
     salutation, `c` closing, `e` endorsement, `g` running_title, `n` numbering, `q` quire_marks, `x` reference, `o`
     note, `a` addition, `z` noise; a digit within 1.5 s after a type letter picks its subtype (in the vocabulary's
     sorted subtype order, listed in the legend); a dropdown sets any other generic type (seal, stamp, graphic, damage,
     drop_capital, title_page, cover, music, custom); placement Shift+1…5 = main, margin, top, bottom, interlinear; `b`
     toggles start; `u` toggles unclear; `k` edits the note of the selected lines; Cmd/Ctrl+Z undoes the last change;
     Enter marks the page reviewed and goes to the next; ←/→ move without marking.
   - **Store:** `new CheckStore("as_phase18_24c_elements_v1")`; per page `{reviewed, page_note, lines: {k: {type,
     placement, start, unclear, note}}}`, only changed lines stored; the pre-label fills the rest. Export
     `store.exportJSON("element_verdicts.json", KEY, items)` with one item per page: `{id, reviewed, page_note, lines:
     [{k, line_id, type, placement, start, unclear, note}]}` (all lines, merged). Import restores a previous export.
   - **Instructions** at the top: the definitions, rubric points 1–9, and "change only what is wrong; press Enter when
     the page is right; new type ideas go in the page note".
   - Check: `node --check` on the extracted script (or `handprint.check_js_syntax(html)`), and open the page headless if
     possible; otherwise state that it was not opened.
4. Append **UA-063** to `docs/USER_ACTIONS.md` (Edit):
   - **Action:** Run (any directory): open `<abs path>/element_page.html` in a browser, correct the 60 pages (Enter per
     page), Export; put `element_verdicts.json` in `D/`; then Run from the AS root `.venv/bin/python -I
     reports/data/phase-18-page-element-types/step-24c/summarise_elements.py`.
   - **Options:** (a) all 60 pages; (b) as many as you can (unreviewed pages are left out of the test). Recommended (a).
   - **Default if unanswered:** no test gold; 24d reports dev results only and no bar is tested.
   - **Blocks:** the 24d test stage (bars L and M).

**Part D (`summarise_elements.py`, run by the user).** Reads `element_verdicts.json` and `prelabels.tsv`; writes
`gold/test_labels.tsv`: `page_id stratum archive inv scan_id line_id k type placement start unclear note pre_type
pre_placement pre_start changed reviewed page_note`, one row per line of the 60 pages (`changed` = any of type,
placement, start differs). Also writes, for dev and test, `gold/annotations/<institute>/<archive>/<inv>/annotations-
<scan_id>.json` (`lines[line_id] = ["generic:<type>", "placement:<placement>"]`) and `elements-<inv>.json` (an `Element`
per run from a `start = 1` line to the line before the next start, `element_type = "generic:<type>"` of its first line).
Appends the hash to `gold/SHA256SUMS`, writes `gold/test_gold.done`, refuses to overwrite it, `chmod g+w` only files it
owns (a `PermissionError` is printed, not fatal), and prints counts per stratum and the `changed` share.

## Constraints

- `data/annotations/` is never written; write `annotations_list_before.txt` before part C and `_after` after part D's
  first run (by the step session, after UA-063), and diff them.
- Readers and the page never show layer types, layout roles, `listlike` output or PageXML region tags.
- Never `git commit`; `chmod g+w` new files; no `rm -rf`, `nohup`, `&`, `$TMPDIR`.

## Tests

`summarise_elements.py --verdicts F --gold-dir DIR` options; run it once on a synthetic export (2 pages, one change,
one unreviewed page) into a scratch dir and check rows, `changed`, the element runs and the refusal to overwrite. Full
suite green.

## Acceptance criteria

1. Reads: 100 pages, every line labelled, `check_reads.py` clean. Self-check of 30 written up (agreement as a count).
2. `dev_gold.done` exists before the page is handed over; `data/annotations` lists identical.
3. Page: 60 test pages, JS syntax checked, the export round-trips through `summarise_elements.py` on the synthetic
   export. UA-063 entry present.
4. After UA-063 (a later session): `test_gold.done`, counts per stratum, `changed` and `unclear` shares, reviewed pages.

## Error analysis

From the reads: label counts per type and stratum; the notes (every `custom:` and every `unclear` line, verbatim, up to
40); the self-check disagreements. After UA-063: the `changed` share per type and stratum, the 20 most frequent (pre →
gold) changes, and every page note. Findings expected: how often the agent pre-labels were changed, which types the
user changed most, new type candidates from the notes.

## Do not touch

- `archival_structures/`, `data/annotations/`, the layers, `shared-tools/`, sibling repos.

## When done

1. Add `### Step 24c: Element gold` to `reports/phase-18-page-element-types.md` (findings `F18-24c.n`, draft); `make
   reports`. 2. Append a PROGRESS block. 3. Overwrite `STATUS.md` (state: waiting for UA-063, or done).
