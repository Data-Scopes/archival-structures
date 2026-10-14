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
| 03 | [Evaluation for NLP](phase-03-evaluation-for-nlp/PLAN.md) | 7 (C6), 8 (C7), 9 (C8) | `stream_analysis/evaluation/` (new), `stream_analysis/corruption/metrics.py`; NLP `src/startpage/evaluation.py`, split and baseline code | seeded; needs planning | — |
| 04 | Decoders and page numbering | 10 (C9), 11 (C13), 12 (C11) | — | not planned | — |
| 05 | Text layer | 13 (pagexml-tools release), 14 (C14), 15 (C15); v2 line geometry and SG preset (D4) | — | not planned | — |
| 06 | Region quality and duplicates | 16 (C16), 17 (C17, then the `page_duplication` operator) | — | not planned | — |
| 07 | Optional | 18 (C10, C12, C18 docs) | — | not planned | — |

Phases 02 and 03 are independent and can run in parallel. Phase 04 needs 03 (it uses the C6
metric conventions). Phase 05 needs inventory step 13, which is the user's pagexml-tools
release.

## Preconditions still open from phase 01

The user is checking these. Once a session has confirmed one, it removes it from this list.

- `poetry lock` in AS.
- LotA: install AS without `--no-deps`.
- NLP: install AS and itself editable into `venv/`.
