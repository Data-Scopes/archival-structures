# Step 21a: fresh gold reads (index sides, entries, locators, link targets)

> **Phase:** 15, Index links v2. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Handwritten States General registers (archive 1.01.02) open with an index whose entries end in **folio numbers**
(locators) that point to resolutions later in the same volume. Phase 14 built links from them and spent its gold. Steps
21b–21d choose and test new rules, so they need **fresh gold that is read before any phase-15 rule exists**. You read
text, not images. You read **whole index sides**, line by line: which entry each line belongs to, and the locators on it.
For a few entries per side, you also find the folio that the first locator names and judge whether the matter is
there. You change no code in `archival_structures/`. You build a confirm page so the user can check 20 of your links.

This is a long read: 29 sides (1,263 layout lines) and about 90 target lookups. **It may take more than one session.**
Write every row to its file as soon as you decide it. A new session continues from the files and adds a `PROGRESS.md`
block. Read **all dev sides first**, then the test sides. Step 21c can start on the dev gold while you read the test sides.

**How the volumes are numbered** (measured in phase 14 on 41 volumes). The resolutions are **foliated**: a number is
written at the top of the right-hand page (recto) only. So folio N's front is the recto of one scan, and its back is
the left-hand page (verso) of the **next** scan. The left-hand page of the **same** scan faces folio N; that page is the
*opening* side. The front index itself is paginated, and the folio numbering usually starts again after it. A scan is
one opening: `verso` is the left page, `recto` the right page, and `single` a whole scan that is one page. The index is
right for most entries. But the matter often runs on to the next folio, and printed numbers are garbled or missing for
about a quarter of the folios.

## Inputs

- Read list (planner, 2026-10-30, `random.Random(20261030)`): `reports/data/phase-15-index-links-v2/planning/reads_sides.tsv`
  has the columns `read_id split stratum role inv scan_id page n_layout_lines front_pos n_front`. It holds 58 rows:
  29 volumes × (1 `read` + 1 `reserve`).
  - Dev (read S01–S13): 3126 3307 3315 3368 3421 3428 3444 3455 3487 3493 3539 3587 3623.
  - Test (read S27–S42): 3265 3274 3280 3283 3290 3303 3349 3350 3353 3438 3459 3507 3522 3547 3572 3630.
  - Reserves are S14–S26 (dev) and S43–S58 (test).
- **Open no other file in `reports/data/phase-15-index-links-v2/planning/`**, and nothing under
  `reports/data/phase-14-index-links/` or `data/index_links_dev/`. They hold folio offsets and link predictions that
  would bias your search.
- Layer (read only): `L = data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/`. Read it with
  `from archival_structures.text_layer.io import read_tsv_gz` (it yields dicts of strings). The tables per inventory:
  - `lines/<inv>.lines.tsv.gz`: `scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw
    text edit_flags split_from`;
  - `layout/lines/<inv>.tsv.gz`: `scan_id line_id region_id page layout_region_id layout_line_idx layout_column
    layout_role`. `page` is one of verso/recto/single and gives each line its side;
  - `units/units/<inv>.tsv.gz`: `scan_id page unit_id unit_order unit_type unit_method layout_region_id layout_column
    x y w h n_segments n_lines first_line_id last_line_id`;
  - `units/segments/<inv>.tsv.gz`: `scan_id unit_id seg_order line_id seg_idx`;
  - `types/segments/<inv>.tsv.gz`: `scan_id line_id seg_idx char_start char_end … page layout_region_id line_type …`;
  - `scans/<inv>.scans.tsv.gz`: `scan_id … scan_num …`. **The scan order of the volume is the row order of this
    table.** Never parse a number out of the scan id.
- Check page: `/Users/Shared/claude_projects/data_scopes/shared-tools/checkpage.py`. Read the *checkpage* section of
  `/Users/Shared/claude_projects/data_scopes/shared-tools/README.md` first (grep `checkpage`). Use `checkpage.BASE_CSS`,
  `checkpage.HELPERS_JS` (`scanLine`, `idLine`, `CheckStore`) and `checkpage.scan_url(scan_id)`. The phase-14 page
  `reports/data/phase-14-index-links/step-20c/build_check_page.py` is a working example; you may read **that one file**.

## Deliverables

All files go under `reports/data/phase-15-index-links-v2/step-21a/` unless named otherwise.

| File | Change |
|---|---|
| `show_side.py READ_ID` | prints one side for reading (below); no prediction |
| `show_scan.py INV (POS \| SCAN_ID) [--full verso\|recto\|single]` | prints one scan (below) |
| `side_verdicts.tsv` | one row per read side: `read_id split inv scan_id page side_verdict used_for note` |
| `line_gold.tsv` | one row per printed line of a judged side (fields below) |
| `draw_targets.py`, `targets.tsv` | the target sample per side (rule below); written before any target is read |
| `target_gold.tsv` | one row per target (fields below) |
| `build_check_page.py`, `check_page.html`, `check_items.json` | confirm page of 20 test links |
| `summary.py` | the report's counts from the four TSVs |
| `hashes.txt` | `shasum -a 256` of `line_gold.tsv`, `embedded_entries.tsv`, `target_gold.tsv`, `side_verdicts.tsv`, `targets.tsv` when complete |
| `docs/USER_ACTIONS.md` | one new UA for the confirm page (next free ID: grep the highest `### UA-` in both UA files) |
| `reports/phase-15-index-links-v2.md` | the `### Step 21a: …` section, with draft findings `F15-21a.n` |

## Behaviour to implement

**`show_side.py READ_ID`.** The header gives `read_id`, `inv`, `scan_id`, `page`, the scan's 1-based position in the
scans table, and the number of scans. Then come all lines of that side (`layout/lines.page` = the side), each with a
non-empty stripped `text`. Order them by unit, then segment: (`unit_order` of the line's unit, `seg_order`). A line in
no unit goes last, ordered by `y`. For each line print `k` (1-based), `x`, `y`, `w`, the unit's `unit_type` (4 letters)
and the text. The x values let you see the two columns of a two-column index. **This order is not the reading order**:
two-column sides often alternate between the columns. You decide the reading order yourself.

**`show_scan.py`.** Use the phase-14 behaviour. For the scan at 1-based position `POS` (or given by id), print for each
side: the side name, the number of lines, the first 4 and last 3 lines (by `(y, x)`), and every line whose stripped
text has at most 8 characters and holds a 1–4 digit number, with its `y` and whether it is in the top or bottom fifth
of the side. With `--full SIDE`, print all lines of that side by `(y, x)`.

**Side verdict** (per `read` row, in `read_id` order: dev S01–S13 first, then test S27–S42). Choose one of:
- `index`: a page-reference index;
- `dated index`: entries end in dates, not folio numbers;
- `not index`;
- `unreadable input`: more than half the lines garbled.

For anything other than `index`, read the volume's `reserve` row once instead (`used_for` = the replaced read id), and
record both sides in `side_verdicts.tsv`. Give a `dated index` side's lines no labels.

**Line labels** (`line_gold.tsv`, one row per `k` of an `index` side; write the whole side at once). The columns are
`read_id inv scan_id page k line_id col label entry locators note`.
- `col`: `L`, `R` or `S` (a line spanning both columns, e.g. a centred heading). Use `L` on a one-column side.
- `label`, one of:
  - `S`: the line starts an entry;
  - `C`: the line continues an entry;
  - `H`: a lemma heading or sub-heading with no locator of its own ("Groningen en Ommelanden.", "Commissien.");
  - `F`: furniture (a page number or running head);
  - `X`: garbled.
- `entry`: for `S`/`C`, the id of the entry the line belongs to. Ids are integers `1, 2, …`, numbered in **your reading
  order** of the side: the left column top to bottom, then the right column. A `C` line names the entry it continues,
  even when the printed order jumps between columns. `H`/`F`/`X` lines have an empty `entry`.
- `locators`: every number on that line that refers to a folio, in text order, `;`-separated
  (`252: 568: 581:` → `252;568;581`).
  - A line can end entry A and start entry B (`… 580. Misse van den Broek …`). Label it `S` with entry B, and write
    A's number as `580@A`, where A is entry A's id.
  - Not locators: amounts (`f 150`, `30000 guldens`), counts (`6 maanden`, `40 man`), ordinals (`6e`, `2e.`), dates
    (`17. April`), years.
  - A number after `vide`/`siet` **is** a locator if it is a folio number; note `vide`.
  - A number split by a line break counts once, on the line where its first digit is.
  - A number the HTR garbled (`1o7`) is not a locator unless you are sure; say so in `note`.
  - A number glued to a word (`Cunes94`) is a locator only if you are sure it is one (the phase-14 user check found
    `Cunes94` to be an HTR artefact).

**Hard cases.** Judge entries by meaning, not by layout:
- a hanging-indent continuation is `C`;
- a short first line ("Voor") followed by the rest of the entry is `S` + `C`, one entry;
- a lemma heading with its own locator is `S`;
- a line that holds text of both columns (the HTR merged them) is labelled for the column its first word belongs to,
  with `note` `merged columns`.
- **Embedded entries** (added 2026-10-30 by the orchestrator, after dev session 1): an entry of the other column whose
  text lies wholly inside a merged line (it has no line of its own) cannot get an `S` line. Record it in
  `embedded_entries.tsv` (`read_id inv k line_id text locators note`): `k`/`line_id` of the merged line, the entry's
  words as printed, and its folio locators in text order (`;`-separated, same rules as above). Do **not** put these
  locators in `line_gold.tsv`. An entry that starts inside a merged line but has later lines of its own keeps its `S`
  on its first own-column line (note `S placed on own-column line`), as before. Embedded entries are never targets.

**Target sample** (`draw_targets.py`; run it **after** the side's lines are written and **before** you open any target
scan). Per judged side, the candidates are the entries with at least one locator, in entry-id order. Draw 2 per dev
side and 4 per test side (all candidates if there are fewer) with `random.Random(f"21a-{read_id}").sample`. Write
`targets.tsv`: `target_id read_id split inv entry first_line_id target_locator`. `target_locator` is the entry's first
locator in text order. Number the `target_id`s `T001…` in `read_id` order.

**Target reading** (per row of `targets.tsv`, procedure as in phase 14). The columns of `target_gold.tsv` are
`target_id read_id split inv entry target_locator target_scan_id folio_evidence scans_opened content content_sides
evidence note`.
1. Find the scan whose **recto carries the printed folio N**. Open a scan near the middle with `show_scan.py`, read
   its printed number, jump by the difference, and repeat until you reach N. If N itself is not printed, use the
   nearest printed numbers on both sides and count scans (one folio per scan).
   - Record `target_scan_id` and `folio_evidence`: `printed` (N read on that recto), `neighbours` (counted from printed
     numbers within 3 scans) or `unclear`. Also record `scans_opened`.
   - An N above the volume's last folio gives `target_scan_id` = `none` and the note `beyond volume`.
2. Read the opening side (`--full verso` of the target scan), folio N's recto (`--full recto`), the next scan's verso,
   and the next scan's recto. Then record:
   - `content_sides`: the `;`-joined subset of `opening;front;back;next` on which the entry's matter (its subject,
     name, place or summary words) appears;
   - `content`: `leaf` (front or back), `opening only`, `next only`, `absent`, or `unclear`;
   - `evidence`: the matching words, ≤ 80 characters.
3. Append the row at once.

**Confirm page.** Draw 20 rows with `random.Random(20261031).sample` from the **test** targets that have a
`target_scan_id` other than `none` and `folio_evidence` ≠ `unclear`. Sort them by `target_id`. Build the items like the
phase-14 page: the inventory, the entry text (its lines joined in `k` order), the entry's scan as a viewer link with the
side in words, the locator N, and **your** target (the scan link of folio N's recto and the next scan's link). Ask
three questions, each answered Yes / No / Unsure:
1. "Is the number on the right-hand page of the first linked scan folio N (or does counting from nearby numbers
   give N)?"
2. "Is the entry's matter on that page or on the left-hand page of the second scan?"
3. "Do the entry's words belong together (one entry, not two columns mixed)?"

Add a note field. State at the top that this page shows the agent's answers (it is a confirmation, not a blind test),
and give the export path `reports/data/phase-15-index-links-v2/step-21a/check_verdicts.json`. Use the `CheckStore` key
`as_phase15_21a_links_v1`. Check the item count (20), the links, and the JS syntax (`osascript -l JavaScript` with
`new Function(src)`, as `checkpage._selftest` does).

**UA** (append with Edit; follow the format at the top of `docs/USER_ACTIONS.md`). Title: `### UA-0NN Confirm 20
index-link v2 gold targets on the scans (<date>, from phase-15 step 21a)`.
- Action: Run `open reports/data/phase-15-index-links-v2/step-21a/check_page.html` (AS root), answer, and export to
  the named file.
- Options: (a) check (about 30 min); (b) skip. Recommended: (a).
- Default if unanswered: the review states that the gold is unchecked.
- Blocks: the publication decision for `index_links_v2` only. 21b–21d run on the agent gold.
- Response: empty.

## Constraints

- No change under `archival_structures/`, `tests/`, `docs/` (except `USER_ACTIONS.md`), any layer, or `planning/`.
- Read every line and every target yourself. No script decides a label; the tools only print and draw.
- Never `git commit`; `chmod g+w` everything you create; use `.venv/bin/python`; the shell is zsh (`echo ==` fails);
  no `rm -rf`, `nohup` or `&`.
- Edit shared files (report, `STATUS.md`, `PROGRESS.md`, `USER_ACTIONS.md`) with Edit only, because 21c may run while
  you read the test sides.
- **Once `hashes.txt` is written, the gold files are frozen.** A later correction goes into a new file
  `corrections.tsv` with a reason, and does not change the original rows.

## Tests

There is no library code. Run `.venv/bin/python -I reports/data/phase-15-index-links-v2/step-21a/summary.py` and
paste its output into the report.

## Acceptance criteria

1. `side_verdicts.tsv` holds a verdict for every `read` row (and each reserve used). `line_gold.tsv` holds every
   printed line of each judged `index` side, with the labels in the five values. Every `S`/`C` line has an `entry`,
   every entry id has exactly one `S` line, and every `@` reference names an entry on the same side.
2. Dev: at least 11 judged `index` sides. Test: at least 14. If fewer, stop and report (do not draw new sides).
3. Targets: every row of `targets.tsv` is read. At least **50 test** targets and 20 dev targets have a
   `target_scan_id` other than `none` and `folio_evidence` ≠ `unclear`. If fewer, report and continue: the bars use
   what there is, and the report says so.
4. The confirm page has 20 items, links on every item and checked JS syntax. The UA is written. `hashes.txt` is written.

The user decides when the step is done.

## Error analysis

This step *is* the reading. Report per split and per stratum (`high`/`low`):
- the side verdicts, and the reserves used;
- the number of lines, entries, `H` lines and locators;
- entries with 0, 1, ≥ 2 locators;
- mid-line entry ends (`@`);
- two-column sides (any `R` line), and lines with `merged columns`;
- the non-locator numbers seen, by kind (amount, count, ordinal, date, year, garbled, glued), with counts;
- `folio_evidence` counts and the mean `scans_opened`;
- `content` and `content_sides` counts. How often is the matter on the leaf, the opening, the next folio? This is a
  fact about the source;
- volumes whose numbering surprised you (no restart, skipped or repeated numbers), with the scans;
- 6 verbatim hard cases with your decision.

Draft findings: the share of two-column sides, the share of entries whose lines are not consecutive in `show_side`
order, the locator styles, and the index accuracy.

## Do not touch

`archival_structures/`, `tests/`, every layer under `data/`, `reports/data/phase-15-index-links-v2/planning/`, other
phases' report data (except reading the one phase-14 page builder), `shared-data/`, the siblings, the external drive.

## When done

1. In the report, write `### Step 21a: fresh gold reads (index sides, entries, locators, link targets)` in
   `reports/phase-15-index-links-v2.md`, with draft findings `F15-21a.n`. Then run `make reports` and check it is clean.
2. Append to the phase `PROGRESS.md` (one block per session):

```markdown
### Step 21a: fresh gold reads (session <n>)
- **Session:** <model>, <date>. **Status:** done / partial (dev done, test in progress) / blocked
- **Files changed:** <paths>
- **Public API:** none
- **Tests:** none (summary.py output in the report)
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21a](../../reports/phase-15-index-links-v2.md#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets); findings F15-21a.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** UA-0NN (confirm page)
```

3. Overwrite the phase `STATUS.md` with:
   - 21a's state (`dev gold done` lets 21c start its dev selection; `done` means test is complete and hashed);
   - the UA id;
   - for 21b–21d: the gold counts per split, the reserves used, and any volume whose numbering surprised you.
