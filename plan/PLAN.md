# AS plan: phase index

Protocol: `plan/README.md`. Prompts: `plan/PROMPTS.md`. The shared-library program comes from
`docs/shared_library/upstream_inventory.md`: §4 has the step order and §6 the user's decisions.
Its changes are recorded in `docs/shared_library/changelog.md`. What each phase found (results,
error analysis, findings with IDs) is written up in `reports/phase-NN-slug.md`, indexed in
`reports/index.md`, and built to HTML with `make reports` (from phase-02 step 03d on).
Standing context for planning (current pipeline, accepted deviations, recurring brief defects) is in
[`CONTEXT.md`](CONTEXT.md); planning sessions read it after this file.

| Phase | Title | Inventory steps | Owns (modules) | Status | Release |
|---|---|---|---|---|---|
| 01 | Shared-library foundation | 0–3 (packaging, C1 `<lb/>` text, C2 node ids) | packaging, `parsers/ead_text.py`, `parsers/ead_tree.py`, `parsers/ead_views.py` | done before `plan/` existed; see the changelog | v0.5.0 |
| 02 | [EAD for LotA](phase-02-ead-for-lota/PLAN.md) | 3b (LotA switches to AS node ids), 3d (`reports/` + `make reports`), 3c (LotA output baselines), 4 (C5), 5 (C3), 6 (C4) | `parsers/ead_digital.py`, `parsers/ead_scope_notes.py` (new), `parsers/ead_fields.py`, `ead_views` `archive_id` arg, `ead_text` private hook; LotA `ead/`, `model/node.py`, `corpus/coverage.py`, `facets/title_norm.py`, `cli/` (3 modules), `output/baselines/` | done; reviewed 2026-10-14 (accept with follow-ups; [REVIEW](phase-02-ead-for-lota/REVIEW.md)) | pending (v0.6.0) |
| 03 | [Evaluation for NLP](phase-03-evaluation-for-nlp/PLAN.md) | 7 (C6; NLP shim as 7b), 8 (C7), 9 (C8) | `stream_analysis/evaluation/` (new), `stream_analysis/corruption/metrics.py`; NLP `src/startpage/evaluation.py`, split and baseline code, two verify scripts | done; reviewed 2026-10-14 (accept with follow-ups; [REVIEW](phase-03-evaluation-for-nlp/REVIEW.md)) | pending (v0.7.0) |
| 04 | [Decoders and page numbering](phase-04-decoders-and-page-numbering/PLAN.md) | 10 (C9; NLP shims as 10b), 11 (C13), 12 (C11; NLP shim as 12b) | `stream_analysis/decoding/`, `model/scan_id.py`, `analysis/page_numbering.py`, `datasets/globalise_index.py` (all new), core `scipy` dep; NLP decoder and alignment modules + CRF helpers in 3 scripts, 2 verify scripts; LotA `ead/adapters.py` | done; reviewed 2026-10-14 (accept with follow-ups; [REVIEW](phase-04-decoders-and-page-numbering/REVIEW.md)) | pending (v0.8.0) |
| 05 | [Text layer (v1 producer into AS)](phase-05-text-layer/PLAN.md) | 14 (C14), 15 (C15; split into 15 library, 15c driver, NLP shim 15b), 13 (pins after the user's pagexml-tools release, which became 0.9.0), 15r (review fixes) | `text_layer/` (new), its tests and fixture, `pyproject.toml` scripts + pagexml/fuzzy-search pins, `docs/text_layer.md`; NLP `voc_text_layer.py`, `build_voc_text_layer.py`, a verify script | done; reviewed 2026-10-15 (accept with follow-ups; [REVIEW](phase-05-text-layer/REVIEW.md)); 15r done | v0.9.0 |
| 06 | [Text layer v2: SG line geometry and layout repair](phase-06-text-layer-v2-sg/PLAN.md) | "later" row of §4 + LotA hand-off R1–R3: 15d (v2 extract, lines with geometry, empty scans, legacy `doc_id`), 15e (port of republic's merged-line splitter), 15f (glued-word repair; off, negative result), 15fr (review fixes) | `text_layer/extract_v2.py`, `line_split.py`, `word_split.py` (new); additive `io.py`, `presets.py`, `build.py` v2 paths; `nltk` core dep | done; reviewed 2026-10-18 (accept with follow-ups; [REVIEW](phase-06-text-layer-v2-sg/REVIEW.md)); 15fr done | v0.10.0 |
| 07 | [SG units, types and the pilot layer](phase-07-sg-units-and-pilot/PLAN.md) | LotA hand-off R4–R6 + acceptance: 15fp (page layout), 15fq/15fq2 (layout fixes, column merge guard), 15g/15g2 (line segments and nine types), 15h (units, `doc_id_map`, REPUBLIC crosswalk), 15hj/15hj2 (v2 word breaks and join with offsets, pinned WBC sets), 15i/15i2 (pilot `text_layer_v2` published in `shared-data`), 15ir (review fixes) | `analysis/page_structure.py`, `text_layer/layout.py`, `types.py`, `units.py`, `join_v2.py` (new); v2 `layout`/`types`/`units`/`wbc`/`join` stages and v2 `--stage all`; preset fields; `shared-data/NL-HaNA_1.01.02/text_layer_v2/`, `NL-HaNA_1.10.94/text_layer_v2/` | done; reviewed 2026-10-21 (accept with follow-ups; [REVIEW](phase-07-sg-units-and-pilot/REVIEW.md)); 15ir done and reviewed (accept); UA-013 (a) | v0.11.0 (15fp–15i2); 15ir: v0.12.0 recommended |
| 08 | [Region quality and duplicates](phase-08-region-quality-and-duplicates/PLAN.md) | 16 (C16), 17 (C17), 17b (`page_duplication` operator) | `analysis/region_quality.py`, `analysis/duplicate_pages.py` (new), `tests/fixtures/region_quality/`; additive `stream_analysis/corruption/` (operator, `observed_start_labels`, `CASE_NAMES`) and two `docs/challenges/` pages; LotA `corpus/quality.py`, `corpus/integrity.py`, their tests | done; reviewed 2026-10-21 (accept with follow-ups; [REVIEW](phase-08-region-quality-and-duplicates/REVIEW.md)); doc fixes applied in review | v0.13.0 (prepared; user tags) |
| 09 | Text layer v3 preparation | phase-07 follow-ups ([`CONTEXT.md`](CONTEXT.md) §4) | `text_layer/` | not planned | — |
| 10 | Optional | 18 (C10, C12, C18 docs); a `text_extent` step for NLP (UA-014 (a): region-type filter for `top`/`bot`, margin-gap columns, both additive; NLP re-checks report/84) | `analysis/text_extent.py` (additive) | not planned | — |

Phases 02 and 03 are independent and can run in parallel. Phase 04 needs 03 (it uses the C6
metric conventions). Phase 05 is released in v0.9.0. Phase 06 needs 05 and is reviewed (its
step 15fr ran; released in v0.10.0). Phase 07 needs 06 and is reviewed; 15ir runs after the user's
commit and the v0.11.0 release. Phase 08 is done and reviewed (2026-10-21; v0.13.0 recommended).
Phase 09 touches only `text_layer/` and is next to plan.

**Renumbering (2026-10-21, phase-08 planning, decision P1):** the phase-07 text-layer follow-ups
left phase 08 for a new phase 09 (*Text layer v3 preparation*). The optional phase became 10.

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
