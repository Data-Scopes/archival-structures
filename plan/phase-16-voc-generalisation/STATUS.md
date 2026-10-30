# Phase 16 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-30, by the phase-15 planning session (Opus 5.5)
- **State:** seeded
- **Released in:** not yet

## Steps

None yet (seeded; see `PLAN.md`, *Candidate questions*).

## Next action

After phase 15 is reviewed: an Opus planning session with `plan/PROMPTS.md` §1 (for a phase that has a directory
and a seeded `PLAN.md`).

## Blockers and open questions for the user

- UA-054: stage VOC PageXML (63 inventories, list in `docs/staging/voc_phase16_inventories.txt`). Blocks the VOC
  text-layer build only.

## Facts later steps need

- The VOC v1 text layer (`shared-data/voc_text_layer_v1/`, 6,914 inventories) has no types/units stages, so the
  page-number types that `folio_map` reads need a v2+-format build from PageXML.
- VOC inventory ids can carry a letter suffix (`1457A`); sort with a (number, suffix) key.
