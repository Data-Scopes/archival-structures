# Phase 19 planning inputs: page elements v2 (user decisions, 2026-11-17)

Written by the orchestrating session (Opus 5.5) that ran the phase-18 test stage and review. The user chose the phase
(UA-073 a) and answered four scope questions in the same session. This file stays as the record.

## Origin

Phase 18 failed both bars on 60 test pages (`reports/phase-18-page-element-types.md`):

- **L** (`listlike` C0R1 separates list/table/form lines from running text): P 0.950, R 0.575. It works on a line
  window, so it misses wrapped table rows, register cells, digit-initial ledger rows and amounts followed by `—`/`„`
  (F18-24d.12, F18-R.4, F18-R.5).
- **M** (the layer's `marginalia` call finds margin lines): P 0.259, R 0.872. The call is layout role `extra` plus a
  page-margin rule, so second table columns and register cells in mid-page count as margin (F18-24d.10, F18-R.2).
- The line gold cannot express an interrupted element (`start` runs, F18-R.9); `prose` was collected on 16 of 60 pages
  (F18-R.8); form vs table drifted between registers (F18-R.3).

## Decisions (user, 2026-11-17)

1. **Phase 19 = page elements v2** (UA-073 a). Deed boundaries in NL-AsdSAA 5075 (UA-065) stay the next candidate;
   reading order / layout (coordination #10, #15) later.
2. **Scope: three pieces.** (a) a margin/table-column split: tell second table columns and register cells apart from
   margin notes before a line is called margin (bar M again); (b) an element reader: group lines into elements and
   rows or items by column alignment and indent, and call each element list, table, form or running text (bar L
   again); (c) annotation format v2: element ids instead of `start` runs, an item/row-start flag, element properties
   (e.g. cells written as prose), a fixed form-vs-table rule, the `letterhead`/`copymark` candidates. **Not in scope:**
   mode detection outside SG (F18-R.11).
3. **Fresh test gold: 60 random pages**, 6 per stratum in the 10 strata of phase 18, drawn from volumes phase 18 did
   not use. Agent pre-labels, corrected by the user on a check page (as UA-063).
4. **Item/row starts are in the gold and in the reader, reported without a bar** this phase.
5. **Analysis modules first.** The detectors live under `archival_structures/analysis/` and read the existing layers;
   no layer is rebuilt for them. If the bars pass, a later phase puts them into the text layer as a new layer version.

## Consequences fixed at planning (orchestrator)

- The phase-18 test pages (60, user-corrected) become phase-19 **dev**: their errors were read in detail, so the new
  rules are designed on them and they cannot test them. The phase-18 dev pages (40, agent-read, other conventions) are a
  second dev set, reported but not used for selection.
- Rulings that come up during the user's correction go onto the check page as fixed keys or tokens, with the page from
  which they apply (CONTEXT §5, F18-R.8/.9), not only into the chat.
