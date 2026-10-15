# Phase 05 status

> Overwrite at the end of every session. **40 lines max**; history in `PROGRESS.md`.
- **Updated:** 2026-10-15, by step 15r (Sonnet)
- **State:** 15r done; ready for release ([REVIEW](REVIEW.md))
- **Released in:** v0.9.0

## Steps
Done: 14, 15, 15c, 13, 15b, 15r. Review done.

## Next action
1. The user reviews and stages the working tree, then releases v0.9.0.
2. Plan phase 06 (v2 + SG preset).

## Open for the user
- F05-R.3: `pip install --no-deps -e /Users/marijnkoolen/Code/Huygens/archival-structures` with
  NLP's and LotA's interpreters. Their metadata says 0.1.0 / 0.3.0, and NLP-built manifests
  record that version.
- F05-13.3: report nested-`TextRegion` parsing upstream (pagexml-tools).
- `poetry lock` (pins: pagexml-tools >=0.9.0, fuzzy-search >=3.0.0; scipy).
- NLP working tree to review/stage: `src/startpage/voc_text_layer.py`,
  `scripts/build_voc_text_layer.py`, `scripts/verify_as_text_layer.py`,
  `output/as-phase-05-originals/`.
- Optional: `.venv/bin/python -m pip install -e '.[notebook]'` (matplotlib, ipywidgets) so
  the 6 uncollectable AS test modules run (not a code defect).

## Facts later steps need
- v1 lives in `<DS>/shared-data/voc_text_layer_v1`; NLP and LotA `data/voc_text_layer_v1` are
  symlinks. Never build into it with a subset (F05-R.5; now in `docs/text_layer.md`).
- Reference for byte identity: NLP `output/voc_text_layer/voc_text_layer_v1/` (22 inventories,
  160 files). CLI, library and NLP wrapper reproduce it 157/157 (about 35 s, 8 workers).
- `MIN_PAGEXML_TOOLS` is (0, 9, 0) since 15r.
- Environments on 2026-10-15: AS `.venv` pandas 3.0.6, NLP pandas 2.3.3, LotA pandas 3.0.6. All
  three report pagexml-tools 0.9.0.
- Suites after 15r: AS 799 passed, 2 skipped (torch), 0 failed, 6 collection errors (matplotlib
  x4, ipywidgets x2; run with `--continue-on-collection-errors`). NLP 29 + 21, LotA 327.
- Agent shell: `nohup`/`&` fail; `rm -rf` is denied; `.venv/bin/python -m pip`, then `chmod g+w`.
