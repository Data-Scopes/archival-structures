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
| 05 | [Text layer (v1 producer into AS)](phase-05-text-layer/PLAN.md) | 14 (C14), 15 (C15; split into 15 library, 15c driver, NLP shim 15b), 13 (pins after the user's pagexml-tools release, which became 0.9.0), 15r (review fixes) | `text_layer/` (new), its tests and fixture, `pyproject.toml` scripts + pagexml/fuzzy-search pins, `docs/text_layer.md`; NLP `voc_text_layer.py`, `build_voc_text_layer.py`, a verify script | done; reviewed 2026-10-15 (accept with follow-ups; [REVIEW](phase-05-text-layer/REVIEW.md)); 15r to run before release | pending (v0.9.0) |
| 06 | Text layer v2 and SG preset | "later" row of §4: per-line geometry, page size, empty-scan rows (D4); the SG preset that retires `make_iventory_paragraphs.py` (audit `docs/audits/sg_paragraph_tsv_audit.md`); region-end word-break statistic | — | not planned | — |
| 07 | Region quality and duplicates | 16 (C16), 17 (C17, then the `page_duplication` operator) | — | not planned | — |
| 08 | Optional | 18 (C10, C12, C18 docs) | — | not planned | — |

Phases 02 and 03 are independent and can run in parallel. Phase 04 needs 03 (it uses the C6
metric conventions). Phase 05 is reviewed (its
step 15r runs before the v0.9.0 release). Phase 06 needs 05 and starts from its hand-off.

**Renumbering (2026-10-14, phase-05 planning, decision P1):** v2 and the SG preset were split out
of phase 05 into a new phase 06. The old 06 and 07 became 07 and 08. Phase-04 documents that
say "phase 07" (separator sheets, the remaining scan-id sites) now mean phase 08.

## Preconditions still open from phase 01

The user is checking these. Once a session has confirmed one, it removes it from this list.

- `poetry lock` in AS.
- LotA: install AS without `--no-deps`.
- NLP: install AS and itself editable into `venv/`.
