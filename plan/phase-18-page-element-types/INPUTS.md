# Phase 18 planning inputs: page element types (user decisions, 2026-11-02)

Written by the phase-17 review session (Opus 5.5) after a discussion with the user. The phase-18 planning session
(`plan/PROMPTS.md` §1) reads this first and folds it into `PLAN.md`; this file then stays as the record.

## Origin

- republic-segmentation handed over `shared-tools/listlike.py` (running text against lists and tables, per line;
  `docs/from-republic-segmentation.md`, 2026-11-02). Its evidence is thin: 9/10 flagged and 10/10 unflagged windows
  right, one archive (REPUBLIC hand resolutions); the AUCs in the note are for resolution starts, not lists.
- The user wants element types beyond running text and lists/tables (headings, marginal notes, ...), tested on a wide
  range of archival material.

## Decisions (user, 2026-11-02)

1. **Phase 18 is this phase** (option 1 of three; folio map v3 and VOC index links v2 come later).
2. **`listlike` becomes an AS module.** In the end every shared-tools module moves into AS (AS is the backbone
   package); `listlike` is the first. Keep it stdlib-only so non-AS callers can import it; port the self-test to
   pytest; changelog entry in `docs/shared_library/changelog.md`. The shared-tools copy stays frozen until then.
   Code points found on reading it (fix or test in the port): `amount_end` counts any line ending in a digit
   (dates, years); `numbering` is case-insensitive, so it matches "item"/"idem" in prose and an initial ("C. van");
   `edge_rep` with the 2 % default probably does not separate left-aligned prose from lists; `gw` defaults to the
   group span, so one long margin line shifts every right-edge ratio (AS adapters pass `gx0`/`gw`); output is per
   line only, so a side-level or run-level aggregate is needed.
3. **Placement is a separate axis** from type. `marginalia` becomes a placement, not a type. The axis needs its own
   name (`position:` is taken by start/continuation/end): e.g. `placement:` with `main`, `margin`, `top`, `bottom`,
   `interlinear`. Legacy `generic:marginalia` tags read as "placement margin, type unknown".
4. **Types: the `generic:` vocabulary (`docs/vocabulary.md`, `datasets/vocabulary.py`) plus all proposed additions:**

   | Group | Types (additions in bold) |
   |---|---|
   | Body | `running_text`, `list`, `table`, `form` (label/value) |
   | Unit openers | `heading` (subtypes **title**, **date**, **summary**) |
   | Document frame | **`salutation`** (address, salutation); `closing` (subtypes **formula**, **dating**, **signature**); **`endorsement`** (dockets, receipt notes on letter backs) |
   | Wayfinding | `running_title` (subtype **date**: a page-top date that repeats the current unit's date); `numbering` (page, **folio**, **item**, other); `quire_marks` (signature, catchwords); **`reference`** (locators, cross-references: "fol. 12", "vide") |
   | Annotation | **`note`** (a remark on the main text, incl. archivists' notes); **`addition`** (text inserted into the main text) |
   | Non-text | `seal`, `stamp`, `graphic`, `damage`, `drop_capital`, `digitization_artefact` |
   | Residual | **`noise`** (bleed-through, facing-page text, HTR garbage), `custom` |

   `heading` (once, opens a unit) and `running_title` (repeats per page) are distinct; the text layer's `date_header`
   mixes them. Mapping of existing vocabularies, to be fixed in the plan: types stage (`text_layer/types.py`)
   `main_text` → `running_text`, `page_number` → `numbering:page`, `running_header` → `running_title`,
   `date_header` → `running_title:date` or `heading:date` (by rule), `catch_word` → `quire_marks:catchwords`,
   `signature_mark` → `quire_marks:signature`, `marginalia` → placement margin, `index` → `list`, `other` → `custom`;
   REPUBLIC types (scoring only) map likewise (e.g. `attendance_list` → `list`).
   **New types:** the user notes candidates in the check page's note field or in the chat; the vocabulary grows from
   those (it is extensible by design).
5. **Gold unit: whole pages.** The user labels runs of lines on a page with a type and a placement; every line of a
   gold page is labelled. Store as `ScanAnnotation` (`lines` keyed by PageXML line id, `regions` for zones) so
   `datasets.annotations` reads it. Needs a run-labelling mode in the check page (`shared-tools/checkpage.py` labels
   one item at a time).
6. **The user corrects pre-labels** rather than labelling from scratch. Planning must keep the systems under test out of
   the pre-labels, or the test measures agreement with the pre-labels: pre-label with an agent reading, not with
   `listlike` or the types-stage rules that phase 18 measures, and record per line whether the user changed the label.

## Archives (all in scope) and data

| Archive | PageXML | Images |
|---|---|---|
| NL-HaNA 1.01.02 (SG, hand and print) | text layer v5 (local), PageXML local | NA IIIF |
| NL-HaNA 1.04.02 (VOC) | text layer v6 on T7_Data (`data/text_layer_voc_23b/`) | NA IIIF |
| NL-HaNA 1.05.06 | `data/PageXML/NL-HaNA/NL-HaNA_1.05.06.tar.gz`; also T7 `Data/PageXML/NA/HTR results 1.05.06 PAGE*` | NA IIIF (1.05.06: none; T7 `…/thumbs/NL-HaNA/` 600 px) |
| NL-HaNA 2.10.50 | `data/PageXML/NL-HaNA/NL-HaNA_2.10.50.tar.gz` | NA IIIF |
| NL-HaNA 2.10.01 | T7 `Data/PageXML/Ministerie_van_Kolonien/2.10.01/scans/<inv>.zip` (4,635 inventories), `paragraphs-2025-05/`, `scan_stats/` (read only) | NA IIIF |
| NL-AsdSAA 5023, 5075, 89 | `data/PageXML/NL-AsdSAA/*.tar.gz` (no images in them) | T7 `claude-projects-external/archival-structures/data/thumbs/NL-AsdSAA/`: 89 Memorix IIIF `info.json`; 5023/5075 600 px PNG (UA-062) |
| NL-AsnDA 0114.11 | `data/PageXML/NL-AsnDA/NL-AsnDA_0114.11.tar.gz`, `data/pages/NL-AsnDA_0114.11/` | T7 `…/thumbs/NL-AsnDA/`: 300 px PNG (UA-062) |

Existing labels in the vocabulary (dev and sanity checks only, not test): `data/annotations/NL-AsnDA/` 123 region tags
(48 `marginalia`, 44 `closing`, 28 `table`, 2 `seal`, 1 `running_text`); `data/annotations/NL-HaNA/NL-HaNA_2.10.50/`
scan tags (2,101 `table`, 350 `cover`, 252 `title_page`, 87 `table:multipage_table`).

## Proposed scope (for the planning session to fix)

- the vocabulary in code and docs (placement axis, new types, the mappings above);
- `listlike` ported, with AS adapters (text layers and raw PageXML) and a side/run aggregate;
- the run-labelling check page;
- whole-page gold: roughly 6 pages per archive, about 55 pages; dev and test split by volume;
- measurement of what exists (types-stage rules mapped to `generic:`, `listlike`) per type and per archive, with
  error analysis. A bar only where an existing detector makes a claim: `listlike` lists/tables against running text,
  perhaps the types-stage small elements outside SG. Rare types get error analysis, no bar.
- Detectors for the new types: phase 19, from phase 18's error analysis.
