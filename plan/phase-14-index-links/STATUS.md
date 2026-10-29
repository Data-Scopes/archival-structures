# Phase 14 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-29, by the UA-050/052/053 session (Opus 5.5)
- **State:** reviewed: accept with follow-ups ([REVIEW](REVIEW.md)); step 20r done; all user actions resolved; closed
- **Released in:** v0.19.0

## Steps

| Step | State | Note |
|---|---|---|
| 20a | done, reviewed | pin 0.9.1; regression build equal (4,591 files); workarounds gone |
| 20b | done, reviewed | NLP shims; verify 15/15 PASS (re-run) |
| 20c | done, reviewed | gold 68 rows (60 read + 8 extra test reserves, sound: F14-R.1); confirm page UA-050 |
| 20d | done, reviewed | map frozen; LOO 0.9848; bar F failed 33/35 (negative result) |
| 20e | done, reviewed | L2 kept (F14-R.2); bar L failed (P 61/66), bar E passed (33/35); dev build 41 volumes |
| 20r | done | [`step-20r-review-fixes.md`](step-20r-review-fixes.md): builder failure records + `progress.log`, `make update_install` (0.14.1 metadata), docs limits |

## Next action
User releases v0.19.0 (not yet tagged; the full run used the same working tree as `20r-a`). Then the next phase
(index-links v2, `plan/CONTEXT.md` §4). 20r: suite 1,381 passed / 11 skipped; rebuild byte-identical; .venv metadata now 0.18.0 (a v0.19.0 release needs `make update_install` again to record it).

## Release v0.19.0 contents (after 20r)
pagexml-tools `>=0.9.1` and `MIN_PAGEXML_TOOLS` (0, 9, 1); `assign_column_roles` without workarounds; nested regions
parse; v5 `layout/pages` side order follows the scans table (post-phase-13 fix); new `analysis.folio_map` and
`analysis.index_links`; `scripts/build_index_links.py`; `docs/index_links.md`. NLP needs no AS release for 20b (its shims
import phase-11 code, in AS since v0.16.0) but requires pagexml-tools 0.9.1 in its venv (done, UA-048).

## Blockers and open questions for the user
- UA-050 (confirm page): resolved 2026-10-29; 18/20 folio, 17/20 matter, all 11 test links confirmed (F14-20c.8).
- UA-052 (full run): done 2026-10-29; 412 front-index volumes, 902,031 links (F14-20e.9/.10), tables in `data/index_links_full/`.
- UA-053 (publish `index_links_v1`?): (a), not published; a v2 after the next phase.
- None open.

## Facts later steps need
- 20r: do not change `folio_map.py`/`index_links.py` behaviour; regression = rebuild of `data/index_links_dev/20e-a/`
  byte-identical (41 + 41 files and `volumes.tsv`); AS suite 1,380 passed, 11 skipped before 20r.
- `.gitignore` now un-ignores `scripts/build_index_links.py` (review fix, F14-R.6).
- Reviewer scripts and outputs: `reports/data/phase-14-index-links/review/`.
- Next phase (recommended): index-links v2 on fresh gold from the 29 untouched local front-index volumes, or the 371
  drive-only ones (`plan/CONTEXT.md` §4).
