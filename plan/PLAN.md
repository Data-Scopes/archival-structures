# AS plan: phase index

Protocol: `plan/README.md`. Prompts: `plan/PROMPTS.md`. The shared-library program comes from
`docs/shared_library/upstream_inventory.md`: §4 has the step order and §6 the user's decisions.
Its changes are recorded in `docs/shared_library/changelog.md`. What each phase found (results,
error analysis, findings with IDs) is written up in `reports/phase-NN-slug.md`, indexed in
`reports/index.md`, and built to HTML with `make reports` (from phase-02 step 03d on).

| Phase | Title | Inventory steps | Owns (modules) | Status | Release |
|---|---|---|---|---|---|
| 01 | Shared-library foundation | 0–3 (packaging, C1 `<lb/>` text, C2 node ids) | packaging, `parsers/ead_text.py`, `parsers/ead_tree.py`, `parsers/ead_views.py` | done before `plan/` existed; see the changelog | v0.5.0 |
| 02 | [EAD for LotA](phase-02-ead-for-lota/PLAN.md) | 3b (LotA switches to AS node ids), 3d (`reports/` + `make reports`), 3c (LotA output baselines), 4 (C5), 5 (C3), 6 (C4) | `parsers/ead_digital.py`, `parsers/ead_scope_notes.py` (new), `parsers/ead_fields.py`, `ead_views` `archive_id` arg, `ead_text` private hook; LotA `ead/`, `model/node.py`, `corpus/coverage.py`, `facets/title_norm.py`, `cli/` (3 modules), `output/baselines/` | done; reviewed 2026-10-14 (accept with follow-ups; [REVIEW](phase-02-ead-for-lota/REVIEW.md)) | pending (v0.6.0) |
| 03 | [Evaluation for NLP](phase-03-evaluation-for-nlp/PLAN.md) | 7 (C6; NLP shim as 7b), 8 (C7), 9 (C8) | `stream_analysis/evaluation/` (new), `stream_analysis/corruption/metrics.py`; NLP `src/startpage/evaluation.py`, split and baseline code, two verify scripts | done; reviewed 2026-10-14 (accept with follow-ups; [REVIEW](phase-03-evaluation-for-nlp/REVIEW.md)) | pending (v0.7.0) |
| 04 | [Decoders and page numbering](phase-04-decoders-and-page-numbering/PLAN.md) | 10 (C9; NLP shims as 10b), 11 (C13), 12 (C11; NLP shim as 12b) | `stream_analysis/decoding/`, `model/scan_id.py`, `analysis/page_numbering.py`, `datasets/globalise_index.py` (all new), core `scipy` dep; NLP decoder and alignment modules + CRF helpers in 3 scripts, 2 verify scripts; LotA `ead/adapters.py` | done; reviewed 2026-10-14 (accept with follow-ups; [REVIEW](phase-04-decoders-and-page-numbering/REVIEW.md)) | pending (v0.8.0) |
| 05 | [Text layer (v1 producer into AS)](phase-05-text-layer/PLAN.md) | 14 (C14), 15 (C15; split into 15 library, 15c driver, NLP shim 15b), 13 (pins after the user's pagexml-tools release, which became 0.9.0), 15r (review fixes) | `text_layer/` (new), its tests and fixture, `pyproject.toml` scripts + pagexml/fuzzy-search pins, `docs/text_layer.md`; NLP `voc_text_layer.py`, `build_voc_text_layer.py`, a verify script | done; reviewed 2026-10-15 (accept with follow-ups; [REVIEW](phase-05-text-layer/REVIEW.md)); 15r done | v0.9.0 |
| 06 | [Text layer v2: SG line geometry and layout repair](phase-06-text-layer-v2-sg/PLAN.md) | "later" row of §4 + LotA hand-off R1–R3: 15d (v2 extract, lines with geometry, empty scans, legacy `doc_id`), 15e (port of republic's merged-line splitter), 15f (glued-word repair; off, negative result), 15fr (review fixes) | `text_layer/extract_v2.py`, `line_split.py`, `word_split.py` (new); additive `io.py`, `presets.py`, `build.py` v2 paths; `nltk` core dep | done; reviewed 2026-10-18 (accept with follow-ups; [REVIEW](phase-06-text-layer-v2-sg/REVIEW.md)); 15fr (factor 1.19, UA-002) to run before release | pending (v0.10.0) |
| 07 | [SG units, types and the pilot layer](phase-07-sg-units-and-pilot/PLAN.md) | LotA hand-off R4–R6 + acceptance: 15fp (page layout: port of republic's region merge, page split, main columns + `extra`), 15g (line/region types), 15h (v2 join, units with char offsets, `doc_id` mapping, phase-05 wbc follow-ups), 15i (pilot build, audit, 116-row evidence, `shared-data` publication) | `analysis/page_structure.py`, `text_layer/layout.py`, `types.py`, `units.py` (new); v2 `layout`/`wbc`/`join` stages | 15fp briefed 2026-10-18; 15g–15i seeded | — |
| 08 | Region quality and duplicates | 16 (C16), 17 (C17, then the `page_duplication` operator) | — | not planned | — |
| 09 | Optional | 18 (C10, C12, C18 docs) | — | not planned | — |

Phases 02 and 03 are independent and can run in parallel. Phase 04 needs 03 (it uses the C6
metric conventions). Phase 05 is released in v0.9.0. Phase 06 needs 05 and is reviewed (its
step 15fr runs before the v0.10.0 release). Phase 07 needs 06 and is the next to plan.

**Renumbering (2026-10-14, phase-05 planning, decision P1):** v2 and the SG preset were split out
of phase 05 into a new phase 06. The old 06 and 07 became 07 and 08. Phase-04 documents that
say "phase 07" (separator sheets, the remaining scan-id sites) now mean phase 08.

**Renumbering (2026-10-17, phase-06 planning, decision P1):** LotA's input-regeneration hand-off
(`docs/from-language-of-the-archive.md`) made v2 + SG too big for one phase. It became phases 06
and 07. The old 07 (region quality) and 08 (optional) became 08 and 09. Phase 07 needs 06.

## Preconditions still open from phase 01

The user is checking these. Once a session has confirmed one, it removes it from this list.

- `poetry lock` in AS.
- LotA: install AS without `--no-deps`.
- NLP: install AS and itself editable into `venv/`.
