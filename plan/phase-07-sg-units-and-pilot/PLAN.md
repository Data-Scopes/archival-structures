# Phase 07: SG units, types and the pilot layer (LotA hand-off R4–R6 and acceptance)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. **Status: seeded** 2026-10-17 by phase-06 planning. Plan it after
> phase 06 is done (prompt 1 in `plan/PROMPTS.md`).

## Goal

From phase 06's v2 lines, AS builds the **units** LotA aligns on, and publishes a pilot layer
for hand 3349–3354 and print 455/456 in `shared-data`. Each unit has:
- its text, joined with per-inventory word-break decisions;
- the character offsets of every line (R1);
- line and region **types** (R4);
- a mapping from the old `doc_id` to the new one (R5).

The phase also produces a manifest with audit invariants (R6), and the evidence the user needs
for the acceptance on the 116 labelled rows.

## Owns (provisional)

- `analysis/page_structure.py` and `text_layer/layout.py` (new, step 15fp), and the `layout` stage in
  `build.py`;
- `text_layer/` modules `types.py`, `units.py` (new);
- the v2 `wbc` and `join` stages in `build.py`;
- `presets.py`;
- a schema note for LotA in `docs/text_layer.md`;
- the new `shared-data/NL-HaNA_1.01.02/…_v2/` and `shared-data/NL-HaNA_1.10.94/…_v2/` directories
  (written once, after the user agrees).

## Sources

- `docs/from-language-of-the-archive.md` R1, R4–R6, §3–4.
- The SG audit §2 (schema advice) and §3 (validation checklist).
- Phase-06 `PLAN.md` (P1–P12) and its report.
- Phase-05 `REVIEW.md` rows 4, 6, 7.
- republic-project: `republic/helper/pagexml_helper.py` L151–289,
  `republic/parser/pagexml/republic_page_parser.py` `split_column_regions` (L33),
  `page_date_parser.py` L258–359 (read for 15fp; summarised in its brief).
- LotA `archival_language/corpus/tsv_source.py` (header check L43, L169–179; `parse_doc_id`
  L51–69).
- LotA `corpus/parallel_text.py` §7 (`find_session_headers` L506; it needs a weekday, so bare
  "21. April" headers are not found).

## Steps (provisional)

| Step | Title | Content |
|---|---|---|
| 15fp | Page layout ([brief](step-15fp-page-layout.md), written 2026-10-18) | Port of republic's overlapping-region merge, plus a page split at a fitted gutter, columns (republic's `split_column_regions` criterion) and `main`/`extra` roles, as the v2 `layout` stage writing side tables. It runs first: 15g's types and 15h's units and `doc_id` mapping build on the layout regions |
| 15g | Line and region types (R4) | Classes: `main_text`, `page_number`, `running_header`, `date_header`, `catch_word`, `signature_mark`, `marginalia`, `index`, `other`, each with a method tag. Print takes the Laypa tags and checks them. Hand uses rules on position, length and text (numerals, date patterns such as "21. April"). Test cases: the 02d rows 9, 19, 50, 52, 55, 80 and 134. The old `region_type` stays retired |
| 15h | Join, units, mapping | The v2 `wbc`/`join` stages: per-inventory word-break decisions; never across region or column ends; char offsets per line. Units: print = column runs split into paragraphs (indentation/gap, method-tagged), plus a REPUBLIC crosswalk (line ids, F06-P.4); hand = regions with header lines split off. The old → new `doc_id` mapping table. Phase-05 follow-ups: the preset carries the wbc rule and sanity anchors, a subset-run guard, and a manifest history |
| 15i | Pilot build, audit, acceptance evidence, publish | Build 3349–3354 and 455/456. Run the audit §3 invariants. Map the 116 labelled rows (`gold/02d-…-rescaled.tsv`, unit/`hand_doc_id` via `difference_units.tsv.gz`) to new lines, and report the artefact status per row. After the user agrees: write the `shared-data` layer, its catalogue row and the LotA note (`docs/from-archival-structures.md` in LotA) |

## Pre-planning: the republic layout port (2026-10-18, Opus, at the user's request)

The user asked to port republic's PageXML preprocessing before phase 07. The evidence is in
the report's *Planning* section (F07-P.1–P.8), and the brief is
[`step-15fp-page-layout.md`](step-15fp-page-layout.md).

- **What is ported:** the overlapping-region merge (`merge_overlapping_text_regions` and its
  helpers) and the column-assignment criterion of `split_column_regions`.
- **What is new:** a gutter fitted per inventory, because AS's `classify_inventory_structure`
  fails on SG (F07-P.5) and the separation methods disagree (F07-P.6).
- **What is not ported:** the date-header machinery. It needs the neural `line_class`, `date`
  region types and REPUBLIC inventory metadata (F07-P.8). Hand SG and print 456 have no region
  labels at all (F07-P.1).
- **pagexml-tools already has** `regions_overlap`, `parse_derived_coords`, the overlap ratios
  and a page split at a given point. It has no gutter detection, no column building that
  respects `extra`, and no line types. Its traps are listed in F07-P.7 and UA-005.

Consequences for the steps that follow (planning of 15g–15i, prompt 1):

- **15g** gets `page`, `layout_column`, `layout_role` and the position relative to the page
  and column, instead of Laypa tags. For print, "takes the Laypa tags" holds for 455 only, and
  even there most typed regions are empty (F07-P.2).
- **15h**:
  - hand units = layout regions;
  - the R5 mapping = `layout/regions.source_region_ids`;
  - `change=unchanged` keeps the `doc_id` byte-identical.

## Open questions for planning

- The `auto` rule for merged-line splitting in printed 1.01.02 volumes, from 15e's rates
  (F06-15e.4).
- Layer naming and versioning in `shared-data`: is the pilot `…_v2` with `coverage: partial`,
  and does the full external run become v3?
- The schema LotA must read (R5: "AS states the new schema"). Keep `doc_id` byte-identical and
  ship the mapping for the units that change.
- **From the phase-06 review (2026-10-18):**
  - the split print layer is `data/text_layer_v2_dev/15fr-a/print/` (factor 1.19, UA-002), once
    step 15fr has run;
  - the mapping and char offsets of split lines go through `split_from`. A half's `text_raw` is
    a piece of the HTR line, and in 87 lines the join lost a double space (F06-R.4);
  - 15g's types should override `column` for page numbers and date headings (F06-15e.8);
  - 15h also carries F05-R.4 (a `„`-robust word-break statistic, measured on a labelled sample
    before adoption).
- **Glued words: closed.** The user answered UA-003 (a) on 2026-10-18: repair stays off and
  there is no redesign step. The analysis below is kept for reference. Phase 06's unigram
  segmenter failed the bar (real precision 0.62 print, 0.16 hand), and the failure is partly in
  the planner's design:
  - the constant unsplit score `log(0.5/total)` lets 2-part splits of rare inflections win
    (`vermeynende` → `vermeynen de`) while blocking 3+ part splits (recall 0.27);
  - "candidate = count < `min_part_count`" makes every rare genuine form a candidate.

  If the user picks (b), a redesign could:
  - only consider tokens that are absent from the corpus (count 0–1) and ≥12 characters;
  - score the unsplit token with a character n-gram model, not a constant;
  - forbid a split whose only effect is to detach a frequent suffix or prefix (`de`, `en`,
    `voor`, `ge…`);
  - re-use 15f's evaluation and its 0.90 bar.

  Relevant to the ≥ 80% target: 21 of the 116 rows are `print_ocr_error`, many of them glue.
- **Acceptance target (UA-001, answered 2026-10-17):** ≥ 80% of the 116 labelled rows free of
  the five input artefacts (baseline 56%).

## Review checklist

As in `plan/templates/PHASE_PLAN.md`, plus: the audit invariants pass; there is no write through
an existing symlink; the v1 inputs are untouched; and the user signs off on the 116-row result.
