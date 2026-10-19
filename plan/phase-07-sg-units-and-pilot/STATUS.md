# Phase 07 status

> Overwrite this file at the end of every session. **40 lines max**; history belongs in
> `PROGRESS.md`.

- **Updated:** 2026-10-19, by step 15fp (Opus)
- **State:** 15fp done (its validation is negative); 15g–15i still provisional (not yet planned)
- **Released in:** not yet

## Steps

| Step | State | Note |
|---|---|---|
| 15fp | done | layout stage; merge precision 0.82 hand / 0.13 print (bar 0.90); F07-15fp.1–11 |
| 15g–15i | provisional | planned next (prompt 1), using the 15fp tables and UA-006 |

## Next action

1. The user answers UA-006 (how 15g–15i use the layout). If unanswered, planning works from (b).
2. Plan 15g–15i (Opus, prompt 1). Read the report's step 15fp section first.

## Blockers and open questions for the user

- UA-006: add a fix step 15fq, or plan on the layout as is, or plan on the input regions. It
  blocks the scope of the 15g–15i plan.
- UA-005 was answered (a): upstream fixes now, after 15fp. Add F07-15fp.11 (`PageXMLPage` raises
  on an empty page) to them. AS pins the release in a later step.

## Facts later steps need

- Layout layers (regenerate them with the report's *Reproduce* commands):
  - `data/text_layer_v2_dev/15fp-a/{hand_tar,hand_local}/NL-HaNA_1.01.02_text_layer_v2/layout/`;
  - `15fp-a/print/NL-HaNA_1.10.94_text_layer_v2/layout/`.

  The inputs next to them are byte-identical to 15d-a / 15fr-a.
- Print: layout regions are page-level and interleave both text columns (F07-15fp.3). Units
  must split on the line `column` field (1/2), not on regions.
- Hand 3349–3353: lines near the gutter may sit on the wrong page. The recto's margin notes
  sometimes land on the verso (F07-15fp.4). In 3350, the openings 0851, 0856, 0859 and 0886 put
  the verso text on the recto (F07-15fp.5).
- Hand merges often absorb page furniture (page numbers, running dates) as the first or last
  line of a main region (14 of 30 sampled). 15g's type rules must expect that.
- `extra` holds register sub-columns in 3821–3823 (keyword, date, page; F07-15fp.8).
- No layout labels in hand SG or print 456 (F07-P.1). Laypa's `index` tag in 455 is not the
  index part, which is scans 0003–0005 (F07-15fp.10).
