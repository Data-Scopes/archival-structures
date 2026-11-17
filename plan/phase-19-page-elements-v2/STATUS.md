# Phase 19 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-11-17, by phase planning (Opus 5.5)
- **State:** planned; no step started
- **Released in:** not yet

## Steps

| Step | State | Note |
|---|---|---|
| 25a Gold format v2, dev conversion, scan sizes | to do | first |
| 25b Fresh test gold v2 (parts A–D, UA) | to do | after 25a; part B by the orchestrator; part D by the user |
| 25c `page_grid`, `margin_split`, dev grid M | to do | after 25a; parallel with 25b |
| 25d `element_reader`, dev grid E, items | to do | after 25c; item report after 25b part C |
| 25e Measure on test (L2, M2) | to do | after 25b `test_gold.done`, 25c, 25d |

## Next action

- 25a (Sonnet): `plan/phase-19-page-elements-v2/step-25a-gold-format-v2.md`.

## Blockers and open questions for the user

- None for 25a. UA-074 (b) done 2026-11-17: 13 old SG dev runs are symlinks to the drive; the sample draw reads `planning/dev_run_inventories.txt`.

## Facts later steps need

- `R = reports/data/phase-19-page-elements-v2/`. `R/planning/volumes.tsv` sha256 24a2b456cf460e74d3a817e36d2d1b0125a9574c9c774b86c6591faf8ace1a2a
  (22 rows: 18 test volumes, 4 single volumes); `R/planning/dev_run_inventories.txt` (233 SG inventories).
- Phase-18 gold (dev-A = `test_labels.tsv`, dev-B = `dev_labels.tsv`) under `reports/data/phase-18-page-element-types/step-24c/gold/`,
  hashes in its `SHA256SUMS`; page → layer in `…/step-24b/sample_pages.tsv`.
- Probes with expected dev-A numbers: `R/planning/probe_margin.py` (M cells), `R/planning/probe_elements.py` (E cells).
- Suite at planning: 1,471 collected; 1,460 passed, 11 skipped after 24r (provenance test needs the `GIT_CONFIG_*` override).
