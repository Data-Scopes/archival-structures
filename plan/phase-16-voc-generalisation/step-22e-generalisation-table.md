# Step 22e: The generalisation table, and the notes for NLP and the ground-truth store

> **Phase:** 16, A second archive (1.04.02 VOC): what generalises. **Depends on:** 22a, 22b, 22c, 22d (all done).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file, then the 22a–22d sections and *Findings* table of
> `reports/phase-16-voc-generalisation.md`. **Read nothing else unless this brief names it.** If you have to explore
> the code base anyway, do what you must and record it under *Brief defects*.

## Context

Steps 22a–22d ran the SG-tuned components on VOC unchanged, then with a fixed set of VOC options, each against a bar
set at planning. This step writes no code. It:
- compiles their results into one table, component × archive → verdict;
- writes the coordination note for NLP;
- records phase 16's use of the VOC gold in the ground-truth catalogue.

## Inputs

- The phase report: its 22a–22d sections and findings `F16-22a.*` to `F16-22d.*`. Use only numbers from there. A
  number you cannot find there is a gap: say so; do not recompute it.
- `ground_truth/README.md`: the rows for `NL-HaNA_1.04.02/globalise_document_segmentation/`,
  `NL-HaNA_1.04.02/globalise_digitized_indexes/` and `NL-HaNA_1.04.02/generale_missiven_overview/`, column *used by*
  (the last column).
- NLP: `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/CLAUDE.md` (read it first; its rules apply
  to files there), then `docs/from-archival-structures.md` in that repo (read its last section, to match its format).

## Deliverables

| File | Change |
|---|---|
| `reports/phase-16-voc-generalisation.md` | `### Step 22e: Generalisation table` with the table below and findings `F16-22e.n` |
| `reports/data/phase-16-voc-generalisation/step-22e/generalisation.tsv` | the same table, one row per component |
| `ground_truth/README.md` | add "AS (phase 16: …)" to *used by* of the three VOC rows (Edit) |
| NLP `docs/from-archival-structures.md` | one dated section (Edit; append only) |

## Behaviour to implement

**The table.** Columns:
- `component`;
- `module / parameter`;
- `SG evidence`: the SG bar and result, from the finding IDs named in the rows below;
- `VOC verdict`;
- `VOC evidence`: the numbers and finding IDs;
- `SG-specific part`: the rule, constant or assumption that did not carry over, or "none".

`VOC verdict` is one of:
- `unchanged`: the unchanged configuration passed the VOC bar;
- `reparameterised`: only a VOC-selected configuration passed;
- `fails on VOC`: no configuration passed;
- `runs, no bar`: it ran without error, and there was no gold to test it;
- `not run`: say why.

The verdicts for rows 7–9 come from the steps' own verdict lines: copy them, do not re-judge them.

Rows, at least:

| # | Component | SG evidence to cite |
|---|---|---|
| 1 | text-layer build (extract → join) | 22a audit; SG full v5 run, audit 0 judged |
| 2 | volume mode by line-box height (cut 87.75 px) | F12-15u.1, F12-R.5 |
| 3 | side layout (opening vs single page) | 22a sides agreement with the GM `Enkel`/`Dubbel` labels |
| 4 | line types, page-number types | 22a `types.tsv`, `laypa_pn.tsv` |
| 5 | word-break set (pinned `-=„`) | 22a `breaks.tsv` |
| 6 | units (hand: region-based) | 22a units read |
| 7 | folio map (v1, v2) | phase 14 F 33/35, phase 15 F 62/63; 22b bar F |
| 8 | index-side detector (v5 `index_sides`) | phase 13 D1 P 0.971 R 0.949; 22c bar D |
| 9 | index links (locators L2/L7 + map) | phase 15 L P 0.981, E 0.825 (failed); 22d bar E |
| 10 | front-index run (`front_index_run` defaults) | 22c `front_runs.tsv` |

Below the table, write at most 10 lines: which kinds of rules carried over (geometry? numbering?) and which were SG
conventions (index style, scan scale, break marks), each with a finding ID.

**The ground-truth catalogue.** In each of the three rows, append to *used by*:
- `globalise_document_segmentation`: `AS (phase 16: "Table of contents" scans as detector gold, document starts as
  link targets, scoring only)`;
- `globalise_digitized_indexes`: `AS (phase 16: locator silver)`;
- `generale_missiven_overview`: `AS (phase 16: folio ↔ scan gold, planning/gm_anchors.tsv)`.

Keep the other cells as they are.

**The NLP note.** One section, dated 2026-MM-DD (today), titled "AS phase 16 (VOC): what NLP may want to know". It
says, in at most 15 lines:
- **Layer.** AS built `NL-HaNA_1.04.02_text_layer_v5` (preset `voc_1.04.02_v5sg`, the SG v5 rules unchanged) for 567
  inventories (545 GM + 22 GLOBALISE; 1171 absent), on T7_Data. The path is in the AS report. It is a dev layer, not published.
- **AS's use of NLP's gold.** AS used the 22 GLOBALISE inventories' *Type of non-document page* labels (TOC scans) and
  `TANAP Boundaries` starts, for scoring only: no AS rule was fitted on starts, and no AS output is a start-detection
  feature. Name the AS dev/test split (by inventory, seed 20261031) and say it is not NLP's split.
- **The GM folio fields.** AS used them as folio ↔ scan gold. NLP's report/83 used the GM start scans for recall on
  21 inventories; the AS use does not touch NLP's numbers.
- **The folio-map result,** with the finding ID. This is relevant to NLP report/76's folio alignment.

Do not edit any other NLP file. Run no NLP code.

## Constraints

- No code; no change to the steps' sections or findings; no new numbers.
- Never `git commit`; `chmod g+w`; Edit for every shared file.

## Tests

None (no code). `make reports` must run without warnings.

## Acceptance criteria

1. Table rows 1–10, each with a verdict and finding IDs; the TSV equals the table.
2. The three catalogue cells edited, nothing else in `ground_truth/README.md`.
3. The NLP section appended.
4. `make reports` clean.

The user decides when the step is done.

## Error analysis

Check every verdict against the step's own numbers, and list any row where the step and the table disagree (there
should be none). For each `fails on VOC` row, read the step's error-analysis categories and name the largest one in
the `SG-specific part` column.

## Do not touch

All code, every layer, the gold files, other NLP files, LotA.

## When done

1. The `### Step 22e: Generalisation table` section, with findings `F16-22e.n` (status `draft`); `make reports`.
2. Append the `PROGRESS.md` block.
3. Update `STATUS.md`: all steps done; next is the phase review (Opus, `plan/PROMPTS.md` §3).
