# Phase 06 status

> Overwrite every session. **40 lines max**; history in `PROGRESS.md`.

- **Updated:** 2026-10-18, by step 15fr (Sonnet)
- **State:** 15fr done; ready for release
- **Released in:** v0.10.0

## Steps

| Step | State | Note |
|---|---|---|
| 15d | done | v2 extract; re-run byte-identical |
| 15e | done | A/B 2,856/2,856 re-run; split layer byte-identical |
| 15f | done | negative result; presets `word_split="off"` (UA-003: leave off) |
| 15fr | done | factor 1.19 (456: +202 splits, 455: +42), attribution, changelog; report addendum written |

## Next action
The user releases v0.10.0 (`make bump-minor`, `make release`), then plans phase 07.

## Blockers and open questions for the user
- none.

## Facts later steps need

- The new split print layer is `data/text_layer_v2_dev/15fr-a/print/NL-HaNA_1.10.94_text_layer_v2/`
  (factor 1.19). It **replaces `15e-a`** as phase 07's input. 15e-a, 15d-a and 15f-a stay as baselines.
- Scans 0222 and 0223 of 455 are a French insert in narrow sub-columns; the splitter handles them poorly
  (18 of the 42 new 455 splits are in 0222). Phase 07 may exclude or flag them.
- Suite: 1010 passed, 0 collection errors. Presets still have `word_split="off"`.
- Dev outputs live under `data/text_layer_v2_dev/<step>-<run>/` (gitignored). Nothing goes to `shared-data/`.
