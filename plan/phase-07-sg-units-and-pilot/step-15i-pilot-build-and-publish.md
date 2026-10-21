# Step 15i: Pilot build, audit, 116-row evidence, and (after the user agrees) publication

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15hj.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you
> have to explore the code base anyway, do what you must and record it under *Brief defects*.
>
> **Two sessions.** Part A (build, audit, evidence) ends with a user action. Part B (publish)
> runs in a new session, and only once that action's **Response** says to publish.

## Context

LotA's hand-off (R1–R6, *Acceptance*) asks for a versioned layer in `shared-data` covering at
least hand 3349, 3350, 3352, 3353 and print 455, 456. The layer needs a manifest with counts and
the audit invariants, and evidence on the user's 116 labelled rows (target: ≥ 80% free of the five
input artefacts, UA-001; baseline 56%). The user decided (2026-10-20) where it goes:
`shared-data/NL-HaNA_1.01.02/text_layer_v2/` and `shared-data/NL-HaNA_1.10.94/text_layer_v2/`,
`coverage: partial`. The full SG run will be `text_layer_v3`. This step builds the pilot from the
PageXML with the final code, audits it, maps the 116 rows, and then publishes.

## Inputs

PageXML sources (read only), as used for the dev layers (their manifests' `argv`):
- hand 3349–3354: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_{3349..3354}-pagexml.tar.gz`;
- hand 3771–3773, 3821–3823, 11182, 11281: `data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_<inv>/` (AS);
- print 455, 456: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml/NL-HaNA_1.10.94_45{5,6}.zip`.

**One hand layer, not two.** The dev layers split hand into `hand_tar/` and `hand_local/`, so the
WBC corpus default was decided over 6 and 8 inventories. The pilot must be one layer of 14. Make a
source dir of symlinks, `data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02/`, pointing to the 6 tar
files and the 8 directories. Check that `io.discover_inventories_v2` finds all 14 there before you
build.

Dev layers to compare against: the 15hj layer named in `STATUS.md` (`data/text_layer_v2_dev/15hj-b/`,
pinned WBC sets). For column lists, see `docs/text_layer.md` (sections from
15fp–15hj).

LotA (read only):
- gold sheet `…/language-of-the-archive/gold/02d-hand-print-differences-partial-extended-rescaled.tsv`:
  300 rows, 116 with `label`. Columns used: `row_no, unit_id, hand_tokens, print_tokens, label,
  second_label, notes, hand_filename, print_filename`. The `*_coords` columns are mangled
  (thousands separators) and the `*_info_file` columns hold external-drive paths: **don't use
  either**;
- `…/output/parallel_text/1.10.94-1.01.02/difference_units.tsv.gz` (`unit_id, para_id,
  hand_doc_id`, joined on `unit_id`);
- `…/data/paragraphs/NL-HaNA_1.10.94/paragraphs-45{5,6}.tsv.gz` (`para_id, line_start,
  line_end`), with 15h's `units/republic_lines/` crosswalk (`match` ∈ exact, near, split_original,
  split_half, unmatched; `split_half` was added in 15h: 456's ids are REPUBLIC's own half-lines);
- `…/data/scan_info/<archive>/<inv>/<scan_id>.jpg` (1,200 px wide thumbnails) and
  `<scan_id>-info.json` (IIIF info: `width`/`height` equal the layer's `page_w`/`page_h`; take the
  IIIF base from its `id`, or `@id`). Staged for 3349, 3350, 3352, 3353, 455 and 456 only.

Labels (measured at planning): spelling 29, abbreviation 22, print_ocr_error 21, segmentation 16,
number 9, hand_htr_error 7, editorial 5, misaligned 4, unclear 3. The 51 artefact rows are the
labels print_ocr_error, segmentation, hand_htr_error, misaligned and unclear. In their notes,
about 13 are missing whitespace in print (glue; repair is off, UA-003), 3–4 are line-end hyphens
(rows 133, 178, 196), 4 are hand furniture or reading order (rows 9, 50, 80, 134), 1 is a column
merge (row 250), 4 are misaligned, and the rest are HTR/OCR errors or have no note (planning
estimate, not measured on the new layer).

## Part A — deliverables and behaviour

| File | Change |
|---|---|
| `data/text_layer_v2_dev/15i-a/{hand,print}/` | The pilot, built with `--stage all --workers 8` (`sg_1.01.02_v2` from `15i-src`, `sg_1.10.94_v2` from the shared-data print dir) |
| `reports/data/phase-07-sg-units-and-pilot/step-15i/` | `audit.py`, `rows116.py`, `rows116.tsv`, `crops.py`, `crops/index.html` + JPEGs, `audit.json` |
| `reports/phase-07-sg-units-and-pilot.md` | Fill in `### Step 15i: …` (Part A), findings `F07-15i.n` |
| `docs/USER_ACTIONS.md` | A new UA: check the crops and the evidence, and agree to publish |

0. **pagexml-tools pin**, only if the user has released 0.9.1 with the `PageXMLPage.stats` fix
   (check `docs/USER_ACTIONS_resolved.md` for UA-007, and PyPI with `.venv/bin/python -m pip
   index versions pagexml-tools`). If so: `.venv/bin/python -m pip install 'pagexml-tools==0.9.1'`
   and `chmod -R g+w` what it installed. Set `pyproject.toml` to `"pagexml-tools (>=0.9.1,<1.0.0)"`
   and `provenance.MIN_PAGEXML_TOOLS = (0, 9, 1)`. Grep AS's tests for any 0.9.0 behaviour the
   fixes change (`text_regions`, `set_parentage`, `extra`). Then run the suite, and add a changelog
   entry. If not released, skip this, keep 0.9.0, and record that.
1. **Build** the two layers. Record wall times. *(Added by 15hj.)* `--stage all` now runs
   extract → (wordfix) → layout → types → units → wbc → join for v2, and `wbc` exits for a subset of
   the layer's inventories unless `--allow-subset-wbc`. The WBC sets are pinned by the presets (UA-010 (c), step 15hj2), so `--stage all` runs
   through `join` without `--ignore-sanity`. `word_break_stat` stays `default` (the robust statistic changed no
   decision). Join tables: `join/units` (7 columns), `join/segments` (11 columns, offsets
   `char_start/char_end/line_offset`), `join/stats/<inv>.json`; no segment text is empty, so a unit's
   `n_segments` equals its `join/segments` rows.
2. **Reproducibility:** per inventory, every table under `scans/ regions_meta/ lines/ layout/
   types/ units/` equals the 15hj-b layer's (decompressed bytes). With pinned sets, the `wbc_set` column and all of
   `join/` must equal `15hj-b`'s byte for byte. Only `wbc/wbc_stats.tsv`, `char_status` and the derived
   or pooled fields of `corpus_default.json` may differ (one layer of 14 instead of 6 + 8).
3. **Audit** (`audit.py` → `audit.json`, adapted from the SG audit §3; each line is an invariant
   with its expected value):
   - every XML member has a scan row (as in 15d), and every scan with text lines has ≥ 1 unit;
   - every `regions_meta` row is in `doc_id_map`; every `unchanged` layout region keeps its id
     (`same_id`);
   - `unit_id` is unique per inventory;
   - every file of a table has the same header; values lie in their sets (`page` ∈ {verso,
     recto, single}; `unit_type` ∈ the 9 types; `old_status` ∈ identical, trimmed, changed, empty; `link` ∈ same_id, part, none (`none` only on
     `empty` rows with `unit_id=""`); `change`, `layout_role`);
     no tab, CR or LF in text; valid UTF-8;
   - each unit's `n_segments` equals its rows in `join/segments`; the offset invariant of 15hj;
     every segment box lies inside its unit box (5 px);
   - merges with a lower-case right side: the share (target ≥ 0.95, reported);
   - LotA keys: all 300 02d `hand_doc_id`s are found. The LotA `gold/02-` and `02c-` region ids in
     pilot inventories: count them (planning: those files cover other inventories, so expect 0;
     the full run checks them);
   - nothing under `shared-data/` or LotA changed: protected hashes, as in 15fq.
4. **Manifest counts (R6)**, per inventory, into `audit.json`: scans; scans without text; lines;
   legacy regions; layout regions; units per type; split lines (from `split_stats/`); whitespace
   repairs (0, UA-003); `old_status` counts; `gutter_rule`; the WBC set; `word_break_stat`. Known
   gaps: scans without text and their reasons; 455's empty Laypa regions (F06-15d.2).
5. **116-row evidence** (`rows116.py` → `rows116.tsv`, one row per labelled row):
   - hand: the units of `hand_doc_id` (through `doc_id_map`) plus the next 2 units in
     `unit_order` (row 9's tokens cross units). Find `hand_tokens`, whitespace-normalised, in their
     texts: an exact substring, else the best `difflib.SequenceMatcher` window with ratio ≥ 0.8.
     Map the span through the offsets to segments, lines and boxes;
   - print: `para_id` → REPUBLIC `line_start`/`line_end` → crosswalk → v2 lines → their units.
     Find `print_tokens` the same way;
   - automatic flags: the hand span holds a non-`main_text` segment; the print span's lines lie in
     more than one `layout_column`; the print span holds a token of ≥ 18 letters (glue);
   - `predicted` ∈ {`fixed`, `persists_glue`, `persists_htr_ocr`, `needs_realignment`, `unclear`}
     for the 51 artefact rows, decided by reading each row's note against the new evidence. For
     the 65 other rows: `unchanged_free` if both token strings are still found unchanged, else
     `changed` (LotA re-labels those);
   - report the predicted share free, `(unchanged_free + fixed) / 116`, and an upper bound that
     also counts `needs_realignment`, against 0.80. The real check is LotA's, after it re-aligns
     on the new layer (hand-off §4).
6. **Crops for the user's row-level check** (hand-off *Acceptance* 2): rows 39, 60 and 241 (the
   user's examples) plus 30 random labelled rows (`random.Random(20)`). Per row, draw the hand and
   print line boxes of the token span on the thumbnail (scale = 1200 / `page_w`). Save a crop
   JPEG of at most 300 KB (30 MB in total), and write `crops/index.html` with the tokens, the row's
   note, and for each line a IIIF region link `<base>/<x>,<y>,<w>,<h>/full/0/default.jpg` for full
   resolution. **Don't fetch** the IIIF images yourself.
7. **Stop.** Add a UA to `docs/USER_ACTIONS.md` (format and next free ID in that file): **Run**
   `open reports/data/phase-07-sg-units-and-pilot/step-15i/crops/index.html` (AS root); check
   that the boxes cover the tokens; **Decide**: (a) publish the pilot as planned; (b) fix first
   (list); (c) don't publish. Default if unanswered: nothing is published. It blocks Part B and
   LotA's §4.

## Part B — publication (new session; only after UA-012 says (a))

> **Updated by step 15i2 (rule 10).** Publish `data/text_layer_v2_dev/15i-b/`, not `15i-a/`: `hand` was rebuilt
> without `=` for 3349, 3350, 3351, 3353, 3354 (UA-011 b); `print` is byte-equal to `15i-a/print`. Use
> `reports/data/phase-07-sg-units-and-pilot/step-15i2/audit.json` as the audit summary and `deltas.json` for the
> merge counts. The 116-row evidence is unchanged.

Read LotA's `CLAUDE.md` first (its rules apply to the file you edit there).
1. Check, and stop on any failure: neither `…/NL-HaNA_1.01.02/text_layer_v2/` nor
   `…/NL-HaNA_1.10.94/text_layer_v2/` exists; their parents are real directories, not symlinks
   (`test -L`); you have write access (`test -w`).
2. Copy with `cp -R` (not `ln`): `15i-b/hand/NL-HaNA_1.01.02_text_layer_v2/` →
   `shared-data/NL-HaNA_1.01.02/text_layer_v2/`, and the print layer likewise. Then `chmod -R g+w`.
   Leave out `progress.log` and `status.tsv`.
3. Add to the copied `manifest.json` a key `publication`: `{"producer": "archival-structures",
   "as_version", "git_head", "layer": "text_layer_v2", "format_version": 2, "coverage": "partial",
   "inventories": [...], "date": "<YYYY-MM-DD>", "audit": <audit.json summary>, "counts": <R6>,
   "known_gaps": [...], "schema": "docs/text_layer.md (AS) §§ layout, types, units, join",
   "built_from": [source paths + sha256 of the tar/zip files]}`.
   `built_from` uses the **resolved** source paths (follow the `15i-src` symlinks), with sha256 (measured 2026-10-21):
   hand_tar, under `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/`:
   `…_3349-pagexml.tar.gz` 46ee0a68445e437fa85cc96bfc9ba384dcf575ad611426d1867abd1605ca7b04, `…_3350-…`
   eee1cc91b1b41df580648f92a1d56b7834e292d682fe3e5d69be1f2b1768e527, `…_3351-…`
   9f700c6f7e11b58fa65c195e6e8afd5559169092088dca86489213c82fcac797, `…_3352-…`
   2d97790a381400f728de43f13736044122d9dcc9a2ba5e1623b7a5506ba4adb1, `…_3353-…`
   b5e8a0d0e25a656472e91e341ee72adf81eff110c72b17d2bb245bbfce761169, `…_3354-…`
   a8fe7c3757cb2a51f30431b0269e7488cc04fa4f1fee92ece126c16ed2dc2460; print, under
   `…/shared-data/NL-HaNA_1.10.94/pagexml/`: `NL-HaNA_1.10.94_455.zip`
   3f483416061fef5d6bb559c663b878ed720950752fae737c875d067930898584, `…_456.zip`
   e0ca7d9799376b05476cf1e4033af73975451dbcc348b50115ca58a6db3d39c6 (re-check the hashes in Part B). The 8 hand_local
   inventories (3771, 3772, 3773, 3821, 3822, 3823, 11182, 11281) are directories under
   `data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_<inv>` (no archive to hash; record the path and the file count).
   Put in `known_gaps`: `stages.*.argv` in `manifest.json` and the `member`/`source` fields in the copied tables name
   `data/text_layer_v2_dev/15i-src/…` (dev paths); the tables are not rewritten. Also: `join/` of 3351 and 3354 changed
   slightly with the `=` pin (3351 −3 merges).
4. `shared-data/README.md`: add one catalogue row per layer to the table at L68–77 (`path | what
   | source / version | staged | used by`), with *used by* = `LotA (planned)`, and one changelog
   line (L86–91). Use Edit.
5. LotA `docs/from-archival-structures.md`: append a section *SG text layer v2 (pilot)*: the paths,
   the tables and their keys, the `doc_id` rule and the mapping, coverage, the known limits
   (glued words not repaired; char-proportional boxes for split segments; print units never cross
   columns or pages). Say that LotA updates `tsv_source.py` (its 6-column check, L43, L169–179) for
   the new schema. Use Edit.
6. AS `docs/text_layer.md`: a section *Published SG pilot (text_layer_v2)*.
7. Re-run the audit on the published copies (paths changed only). Then fill in the Part B lines of
   the report section.

## Constraints

- Never write under `shared-data/` or LotA in Part A. In Part B, write only the paths in steps
  2–5, and never through a symlink.
- Don't change library code in this step, except for step 0's pin.
- Protected: every dev layer (including `15hj-b`, `15hj2-v1check`, `15i-a`, `15i-b`), Part A's `step-15i/` and 15i2's `step-15i2/` outputs, `shared-data/` (in Part A all of it), LotA `data/`, `gold/`,
  `output/`, NLP `output/voc_text_layer/`. Hash them before and after into
  `15i-protected-*.txt`.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`.

## Tests

No new library tests. `audit.py` and `rows116.py` each get a `--self-test` that runs on the fixture
layer built by the 15hj test (`tests/fixtures/text_layer/sg/9999`). Full AS suite green.

## Acceptance criteria

1. Both layers build with exit 0. Reproducibility as in A2, with every difference explained.
2. Every audit invariant holds, or its failure is listed with a cause. A failure blocks Part B
   unless the user accepts it in the UA.
3. `rows116.tsv` has 116 rows. Hand tokens are found for ≥ 110 of them, and print tokens for
   ≥ 105. List every miss with its reason. Report the predicted share free and the upper bound.
4. Crops exist for the 3 + 30 rows; `index.html` opens.
5. Part B: the published copies pass the audit; the catalogue, the changelog line and the LotA
   note are written.

## Error analysis

- Read all 51 artefact rows (A5): the user's note, the old tokens, and the new unit texts and
  types. Classify them, with a verbatim example per class.
- Read 20 random `unchanged_free` rows: are the new units really the same text?
- Read every token-not-found row.
- In the conclusion, say whether ≥ 80% is reachable on this layer, and what stands in the way
  (glue, HTR errors, re-alignment).

## When done

1. Fill in `### Step 15i: …` in the phase report (Part A; Part B adds a short paragraph), findings
   `F07-15i.n` (`draft`). Then `make reports`.
2. Append the progress block; overwrite `STATUS.md`: after Part A, "waiting for UA-NNN"; after
   Part B, "phase 07 ready for review (prompt 3)".
