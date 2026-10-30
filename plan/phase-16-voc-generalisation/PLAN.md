# Phase 16: A second archive (1.04.02 VOC): what generalises

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **State:** seeded 2026-10-30 by the phase-15 planning session. Not planned; no briefs.

## Goal

Phases 05–15 tuned the text layer, page numbering, index pages and index links on the States General (1.01.02,
1.10.94). This phase runs the same generic components on the VOC archive (NL-HaNA 1.04.02) **unchanged first**. It
measures what carries over against ground truth already in AS, and records which rules turned out to be
SG-specific. Only after that may it fit VOC parameters. User, 2026-10-30: VOC first among the candidates (1.04.02, 2.10.01, 2.10.50).

## Owns

<To be settled at planning. Likely candidates:
- a VOC v2+-format preset in `text_layer/presets.py`;
- `analysis/page_numbering.py` / `folio_map.py` parameters only;
- `ground_truth/NL-HaNA_1.04.02/*/converted/`;
- `reports/data/phase-16-…/`.

Phase 15 owns `folio_map.py` and `index_links.py` while it is active.>

## Sources

- `plan/CONTEXT.md` §1–§3. Everything there is SG-measured: list each fact a VOC step would assume and check it.
- `ground_truth/README.md`, the rows for `NL-HaNA_1.04.02/…`:
  - `globalise_document_segmentation/` (22 inventories, 20,905 pages, 2,367 document starts; version 3);
  - `generale_missiven_overview/` (950 missives with start/end scan **and folio**; `converted/` = 547 YAMLs);
  - `globalise_digitized_indexes/` (OBP indexes, 227,526 documents; silver once aligned to scans).
- `shared-data/voc_text_layer_v1/` (6,914 inventories, NLP's v1 preset `voc_v1`; lines without the v2+ types/units
  stages).
- `docs/staging/generale_missiven_inventories.csv` (staging facts per GM inventory) and
  `docs/staging/voc_phase16_inventories.tsv` (the UA-054 list).
- NLP's `CLAUDE.md` and research plan (VOC start-page detection uses the same GT; coordinate, don't duplicate).

## Candidate questions (seed; to be turned into steps with bars)

1. **Text layer:** does a v5-style build (extract v2 → layout → types → units) run on VOC PageXML (`pagexml_v2.1`),
   and which audit invariants hold? What do the hand/print box-height rule, the line types and the page-number types
   do on VOC hands?
2. **Folio map, generic:** the Generale Missiven overview gives start/end **folio and scan** per missive. That is
   folio ↔ scan gold at no reading cost, for 547 inventories. Bar: phase 14/15's F definition. Does foliation on
   rectos hold for VOC? To check: does foliation restart within a volume, e.g. per bundle of letters? The
   Generale Missiven start/end folios will show it.
3. **Document starts:** the GLOBALISE segmentation (22 inventories) against AS's stream-analysis/evaluation stack
   (phase 03/04 code; NLP's territory). Only what NLP does not already measure.
4. **Index pages and links:** do VOC volumes carry front indexes? The index-side detector (phase 13) and
   `index_links` run unchanged; the digitised OBP indexes are silver. Which phase-13/14/15 rules are SG-specific?
5. **Report:** a generalisation table: component × archive → unchanged / reparameterised / SG-only.

## Open questions for planning

- UA-054: which VOC PageXML is staged (63 inventories proposed; disk was 98% full on 2026-10-30).
- Whether 2.10.50 (4 inventories of PageXML local in `data/PageXML/NL-HaNA/`) joins as a small third archive, or
  goes to a later phase. Ask the user.
- Coordination with NLP (its `CLAUDE.md`): VOC gold that NLP has spent.

## Hand-off

<Filled in at review.>
