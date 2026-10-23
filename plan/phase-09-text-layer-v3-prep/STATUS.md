# Phase 09 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-23, by the 15o review (Opus 5.5)
- **State:** done (phase reviewed: accept with follow-ups, [REVIEW](REVIEW.md); 15n and 15o reviewed: accept)
- **Released in:** v0.14.1

## Steps

| Step | State | Note |
|---|---|---|
| 15j-15m | done | see `PROGRESS.md` |
| 15n | done, reviewed | `scripts/run_text_layer_v3_full.py`; `15n-a` = `15m-a` but source paths |
| 15o | done, reviewed | v3 pilot published to shared-data from `15n-a` (hand 305 MB, print 84 MB); audit 178/182 |

## Next action

The user commits and releases v0.14.1. Then phase 10 planning (Opus, prompt 1 of `plan/PROMPTS.md`), which also
decides the full-run scope (UA-015 response: option (c), per-page/per-region hand/print parameters, first).

## Blockers and open questions for the user

- UA-007: pagexml-tools 0.9.1 is not on PyPI yet.

## Facts later steps need

- Published: `shared-data/NL-HaNA_1.01.02/text_layer_v3/`, `shared-data/NL-HaNA_1.10.94/text_layer_v3/` (2026-10-23,
  AS 0.14.0 at `fa5cc63`, from `15n-a`), `coverage: partial`; `publication` in each manifest; published audit
  `reports/data/phase-09-text-layer-v3-prep/step-15o/audit_published.json`. v2 stays until no consumer is listed.
- A full SG run is a **new version** (v4), not added into v3: the run-level word-break corpus default would change.
- Reference layer: `data/text_layer_v2_dev/15m-a/{hand,print}`; `15n-a`/`15n-smoke`/`15o-pub` (symlinks) are local.
- The step-15m audit is pilot-specific (F09-15n.3). 11 tests skip while `data/thumbs` is off the disk (F09-15n.4).
