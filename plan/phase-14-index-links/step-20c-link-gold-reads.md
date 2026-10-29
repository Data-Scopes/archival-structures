# Step 20c: gold reads (locators and link targets)

> **Phase:** 14, Index → resolution links. **Depends on:** nothing (runs in parallel with 20a and 20b).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Handwritten States General registers (archive 1.01.02) open with an index whose entries end in **folio numbers** that
point to the resolutions later in the same volume. Steps 20d (folio map) and 20e (locator extraction and links) are
judged against gold that this step reads **before any phase-14 rule exists**. You read text, not images. For each
sampled entry you write down its locators, find the folio its first locator names by reading the printed folio numbers
in the volume, and judge whether that folio holds the entry's matter. You change no code in `archival_structures/`.
You also build a confirm page so the user can check 20 of your links on the scans.

**How the volumes are numbered** (measured at planning on 41 volumes): the resolutions are **foliated**: a number is
written at the top of the right-hand page (recto) only, so folio N's front is the recto of one scan and its back is the
left-hand page (verso) of the **next** scan. The front index itself is paginated (both sides numbered), and the folio
numbering starts again after it. A scan is one opening: `verso` = left page, `recto` = right page, `single` = a whole
scan that is one page.

## Inputs

- Read list (planner, 2026-10-29): `reports/data/phase-14-index-links/planning/reads_entries.tsv`: `read_id, split, role,
  inv, scan_id, page, unit_id`; 72 rows = 12 volumes × (5 `read` + 1 `reserve`). Volumes: dev 3254, 3471, 3394, 3308;
  test 3256, 3456, 3401, 3611, 3320, 3712, 3590, 3352. An entry = one `main_text` unit on a front-index side.
- **Do not open** `front_index_volumes.tsv`, `folio_proto.tsv`, `folio_proto.py` or `draw_reads.py` in that directory:
  they hold folio offsets that would bias your search.
- Layer (read only): `L = data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/`. Read with
  `from archival_structures.text_layer.io import read_tsv_gz` (yields dicts of strings). Tables per inventory:
  - `join/units/<inv>.tsv.gz`: `scan_id unit_id unit_type word_break_chars n_breaks_merged ends_with_break_char text`;
  - `join/segments/<inv>.tsv.gz`: `scan_id unit_id seg_order line_id seg_idx char_start char_end line_offset …`;
  - `units/units/<inv>.tsv.gz`: `scan_id page unit_id unit_order unit_type unit_method layout_region_id layout_column x y
    w h n_segments n_lines first_line_id last_line_id`;
  - `lines/<inv>.lines.tsv.gz`: `scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw text
    edit_flags split_from`;
  - `layout/lines/<inv>.tsv.gz`: `scan_id line_id region_id page layout_region_id layout_line_idx layout_column
    layout_role` (join to `lines` on `(scan_id, line_id)`; `page` ∈ verso/recto/single);
  - `scans/<inv>.scans.tsv.gz`: `scan_id … scan_num …` — **the scan order of the volume is the row order of this table**
    (never parse a number out of the scan id).
- Check page: `/Users/Shared/claude_projects/data_scopes/shared-tools/checkpage.py`; read its README section *checkpage*
  (`/Users/Shared/claude_projects/data_scopes/shared-tools/README.md` L42–86) first. Use `checkpage.BASE_CSS`,
  `checkpage.HELPERS_JS` (`scanLine`, `idLine`, `CheckStore`), `checkpage.scan_url(scan_id)`.

## Deliverables

All under `reports/data/phase-14-index-links/step-20c/` unless named otherwise.

| File | Change |
|---|---|
| `show_entry.py READ_ID` | prints the entry (below); no split, no prediction |
| `show_scan.py INV (POS \| SCAN_ID) [--full verso\|recto\|single]` | prints one scan (below) |
| `entry_gold.tsv` | one row per read entry (fields below) |
| `build_check_page.py`, `check_page.html`, `check_items.json` | confirm page of 20 links |
| `summary.py` | the report's counts from `entry_gold.tsv` |
| `docs/USER_ACTIONS.md` | one new UA for the confirm page (next free ID; grep the highest `### UA-` in both UA files) |
| `reports/phase-14-index-links.md` | `### Step 20c: …` section, draft findings `F14-20c.n` |

## Behaviour to implement

**`show_entry.py`.** Header: `read_id`, `inv`, `scan_id`, `page`, and the scan's position (1-based row in the scans
table) and the number of scans. Then the entry text (`join/units` text). Then the side's lines (non-empty stripped
`text`), sorted by `(layout_column, layout_line_idx)` of `layout/lines`, numbered `k`, with `>` before lines of this
unit (its `line_id`s in `join/segments/<inv>.tsv.gz`: `scan_id unit_id seg_order line_id …`), so you see the heading above
the entry.

**`show_scan.py`.** For the scan at 1-based position `POS` (or by id): for each of its sides, the side name, the number
of lines, the first 4 and last 3 lines (by `(y, x)`), and every line whose stripped text is at most 8 characters and
holds a 1–4 digit number, with its `y` and whether it is in the top or bottom fifth of the side. With `--full SIDE`,
all lines of that side by `(layout_column, layout_line_idx)`. Nothing else.

**Reading procedure, per `read` row in `read_id` order.**
1. `show_entry.py`. `entry_verdict`: `entry` (a reference entry: a subject, name or summary with locators), `heading`
   (a lemma heading, page number or other non-entry line that the unit rule made a unit), `not entry` (running text,
   not an index), `unreadable input` (more than half garbled). For `not entry` / `unreadable input` use the volume's
   `reserve` row instead (once; `used_for` = the replaced `read_id`). `heading` rows are kept (no reserve): they are
   valid cases with no locator.
2. `locators`: every number in the entry text that refers to a folio, in text order, `;`-separated, as integers
   (`252: 568: 581:` → `252;568;581`). Not locators: amounts (`f 150`, `30000 guldens`), counts (`6 maanden`, `40 man`),
   ordinals (`6e`, `2e.`), dates (`17. April`), years. A number after `vide`/`siet` (a cross-reference) **is** a locator
   if it is a folio number; note `vide` in `note`. `loc_other`: dates used as locators or other references, free text.
3. If `locators` is non-empty: `target_locator` = its first number N. Find the scan whose **recto carries the printed
   folio N**: open a scan near the middle with `show_scan.py`, read its printed number, jump by the difference, and
   repeat until you are on N; when N itself is not printed, use the nearest printed numbers on both sides and count
   scans (one folio per scan). Record `target_scan_id`, `folio_evidence` = `printed` (N read on that recto), `neighbours`
   (counted from printed numbers within 3 scans), or `unclear`. `scans_opened` = how many scans you opened.
4. Read folio N fully: `show_scan.py INV <target> --full recto` and the **next** scan's `--full verso`. `content`:
   `recto`, `verso`, `both` (the entry's matter — its subject, name, place, or summary words — is there), `adjacent`
   (not on folio N but on folio N−1 or N+1: give `adjacent_side` as `scan_id:page`), `absent` (not on folio N±1),
   `unclear`. Quote the matching words in `evidence` (≤ 80 chars).
5. Write the row to `entry_gold.tsv` at once (append), so a long session loses nothing.

`entry_gold.tsv` columns: `read_id, split, inv, scan_id, page, unit_id, used_for, entry_verdict, locators, loc_other,
target_locator, target_scan_id, folio_evidence, scans_opened, content, adjacent_side, evidence, note`.

**Hard cases.** A unit holding several sub-entries (`… 198. Misse Uts: … 258.`): all numbers are locators, the target
is the first. A number split by a line break or a word-break mark: one number. A number the HTR garbled (`1o7`): not a
locator unless you are sure; say so in `note`. An entry whose locator is above the highest folio of the volume:
`target_scan_id` = `none`, `content` empty, `note` `beyond volume`.

**Confirm page.** Draw 20 rows with `random.Random(20261130).sample` from the rows with `folio_evidence` ≠ `unclear`
and a `target_scan_id`, sorted by `read_id`. One item per row: inventory number; the entry text; the entry's scan as a
viewer link (`scanLine`) and side in words; the locator N; **your** target: the scan link of folio N's recto and of the
next scan (its verso). Two questions: "Is the number on the right-hand page of the first linked scan folio N (or does
counting from nearby numbers give N)?" — Yes / No / Unsure; "Is the entry's matter on that page or on the left-hand page
of the second scan?" — Yes / No / Unsure; a note field. Say at the top that this page shows the agent's answer (it is
a confirmation, not a blind test), and where to save the export:
`reports/data/phase-14-index-links/step-20c/check_verdicts.json`. `CheckStore` key `as_phase14_20c_links_v1`. Check the
item count (20), the links, and the JS syntax (`osascript -l JavaScript` with `new Function(src)`, as `checkpage._selftest`).

**UA** (append with Edit; format of `docs/USER_ACTIONS.md`): `### UA-0NN Confirm 20 index links on the scans
(<date>, from phase-14 step 20c)`. Action: Run `open reports/data/phase-14-index-links/step-20c/check_page.html` (AS
root), answer, export to the named file. Options (a) check (about 30 min); (b) skip: the links are published as
unchecked. Recommended (a). Default if unanswered: the review states the gold is unchecked. Blocks: the publication
decision for `index_links_v1` only (20d and 20e run on the agent gold). Response: empty.

## Constraints

- No change under `archival_structures/`, `tests/`, `docs/` (except `USER_ACTIONS.md`), any layer or the planning files.
- Read every entry and every target yourself; no script decides a label. The tools only print.
- Never `git commit`; `chmod g+w` everything you create; `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`. Shared files
  (report, `STATUS.md`, `PROGRESS.md`, `USER_ACTIONS.md`) with Edit only: 20a runs at the same time.

## Tests

None (no library code). Run `.venv/bin/python -I reports/data/phase-14-index-links/step-20c/summary.py` and paste its
output into the report.

## Acceptance criteria

1. `entry_gold.tsv`: 60 rows (one per `read` row or its reserve), each `read_id` once; verdicts in the four values.
2. At least 30 test and 15 dev rows with a `target_scan_id` that is not `none` and `folio_evidence` ≠ `unclear`. If
   fewer, stop and report (do not draw new entries).
3. Confirm page: 20 items, links on every item, JS syntax checked; UA written.

The user decides when the step is done.

## Error analysis

This step *is* the reading. Report per split and per volume: verdict counts; entries with 0, 1, ≥ 2 locators; the
number of locators; non-locator numbers seen (by kind: amount, count, ordinal, date, year, garbled) with counts;
`folio_evidence` counts and the mean `scans_opened`; `content` counts (how often the index points to the right folio:
a fact about the source); volumes whose numbering surprised you (no restart after the index, skipped or repeated
numbers, two numbering runs) with the scans; 5 verbatim hard cases with your decision. Draft findings: the locator
styles, the share of entries without locators, the index accuracy (`recto`+`verso`+`both` of rows with a target).

## Do not touch

`archival_structures/`, `tests/`, every layer under `data/`, `reports/data/phase-14-index-links/planning/`, other
phases' report data, `shared-data/`, the siblings, the external drive.

## When done

1. Report: `### Step 20c: gold reads (locators and link targets)` in `reports/phase-14-index-links.md` (draft findings
   `F14-20c.n`); `make reports` clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 20c: gold reads (locators and link targets)
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none
- **Tests:** none (summary.py output in the report)
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-14-index-links.md` § Step 20c](../../reports/phase-14-index-links.md#step-20c-gold-reads-locators-and-link-targets); findings F14-20c.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** UA-0NN (confirm page)
```

3. Overwrite the phase `STATUS.md`: 20c done; the UA id; facts for 20d/20e (gold counts per split, reserves used, any
   volume whose numbering surprised you).
