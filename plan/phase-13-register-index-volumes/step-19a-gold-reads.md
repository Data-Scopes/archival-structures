# Step 19a: gold reads (index sides and entry starts)

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** nothing (runs in parallel with 19b).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phase 13 adds three rules for handwritten 1.01.02 volumes: a column split for hand regions (19b), a detector of **index
page sides** (19c) and an entry-start unit rule on those sides (19d). The detector and the entry rule are judged
against gold labels that this step reads **before any of the rules exists**. You read text, not images (as step 15z
did: its text reads agreed with the user's image check on 175 of 176 scans). The planner drew both read lists. You
change no code in `archival_structures/`. You also build a blind check page so that the user can check 30 of your side
labels against the scans.

## Inputs

- Read lists (planner, 2026-10-28, `reports/data/phase-13-register-index-volumes/planning/`):
  - `reads_sides.tsv`: `read_id, stratum, inv, layer, scan_id, page, n_lines`; 60 rows. Strata: `S1` index volumes,
    `S2` scans ≤ 0100 of *tweede serie* half-year registers, `S3h`/`S3a` other hand volumes (hard / any). **Do not
    look at `stratum` before a side's verdict is written** (read the side by `read_id` with the tool below).
  - `reads_entries.tsv`: `read_id, split, group, role, inv, layer, scan_id, page, n_lines, split_volumes`; 38 rows =
    30 `read` + 8 `reserve` (one per `(split, group)`).
- **Do not open** `side_features.tsv`, `gt_split.tsv`, `detector_grid.py` or `interleave_v4_hand.tsv` in the same
  directory: they hold feature values and GT labels that would bias your reads.
- Layers (read only): `data/text_layer_v2_dev/<layer>/1.01.02/NL-HaNA_1.01.02_text_layer_v4/` with `<layer>` from the
  `layer` column (`15y-b` or `15z-b`).
  - `lines/<inv>.lines.tsv.gz`: `scan_id, page_side, column, region_id, line_idx, line_id, x, y, w, h, baseline,
    conf, text_raw, text, edit_flags, split_from` (line order = XML order).
  - `layout/lines/<inv>.tsv.gz`: `scan_id, line_id, region_id, page, layout_region_id, layout_line_idx,
    layout_column, layout_role`. A side is `(scan_id, page)` with `page` ∈ `verso` (left page of an opening), `recto`
    (right), `single`. Join the two tables on `(scan_id, line_id)`.
  - Read with `from archival_structures.text_layer.io import read_tsv_gz` (yields dicts of strings).
- EAD titles and dates for the check page: `data/metadata/eads/NL-HaNA_1.01.02.xml`;
  `from archival_structures.parsers.ead_tree import parse_ead` (`parse_ead(Path) -> EADFindingAid`) and
  `from archival_structures.parsers.ead_views import unit_of_description_frame` (`(finding_aid) -> DataFrame` with
  `inventory_number, title, unitdate`; strip whitespace in `title`).
- Check-page module: `/Users/Shared/claude_projects/data_scopes/shared-tools/checkpage.py`; read its README section
  *checkpage* in `/Users/Shared/claude_projects/data_scopes/shared-tools/README.md` (L42–85) first. Use
  `checkpage.BASE_CSS`, `checkpage.HELPERS_JS` (`scanLine`, `idLine`, `CheckStore`), `checkpage.scan_url(scan_id)`.

## Deliverables

| File | Change |
|---|---|
| `reports/data/phase-13-register-index-volumes/step-19a/show_side.py` | `show_side.py LIST READ_ID`: prints one side (below); no prediction, no stratum |
| `…/step-19a/side_verdicts.tsv` | `read_id, verdict, kind, note` (60 rows) |
| `…/step-19a/entry_gold.tsv` | `read_id, scan_id, page, line_id, k, label, note` (one row per listed line of every entry side read) |
| `…/step-19a/entry_sides.tsv` | `read_id, split, group, role, side_verdict, n_S, n_C, n_H, n_unclear, used_for` |
| `…/step-19a/build_check_page.py`, `check_page.html`, `check_items.json` | blind check page of 30 side labels |
| `…/step-19a/summary.py` | the counts of the report section from the three TSVs |
| `docs/USER_ACTIONS.md` | one new entry (next free ID; UA-038 was the last on 2026-10-28) for the check |
| `reports/phase-13-register-index-volumes.md` | `### Step 19a: …` section and draft findings `F13-19a.n` |

## Behaviour to implement

**`show_side.py`.** Arguments: the list (`sides` or `entries`) and a `read_id`. Load the side's lines (lines with
non-empty stripped `text`). Geometric listing: `lo`/`hi` from the side's lines with `len(text) >= 20` if there are at
least 3, else from all its lines; `lo` = the sorted `x` at index `len // 20`, `hi` = `max(x + w)`. A line is column 2
if `x >= lo + 0.4 * (hi - lo)`, else column 1. Print the lines sorted by `(column, y, x)`, numbered `k = 1..n`, as
`k  col  x  y  w | text`. Header: `read_id`, `inv`, `scan_id`, `page`, number of lines. Nothing else (no stratum, no
unit or type information). If the listing order is wrong for a page (a column break inside an entry), judge by content.

**Side verdicts** (`reads_sides.tsv`, all 60):
- `index`: the side's main content is a list of entries that each point to a resolution or document, by date and/or a
  page/folio reference, usually under lemma headings (place, person, subject). Registers, repertoria, *tafels* and
  *respecten* count. Example: `Reqt:. P: Roos cums: ten eijnde … 41.`, `17. April. Ry. voor den Considl de lon …`.
- `not index`: running text (resolutions, minutes, letters, reports), lists without a reference per entry (names,
  accounts), title pages, near-empty pages. Minutes in a half-page column (`Ontfangen een missive van …`) are not index.
- A side that is partly index and partly running text: the majority of its lines decides; say so in `note`.
- `unclear`: you cannot decide from the text. `unreadable input`: more than half the lines are garbled HTR.
- `kind`: a short free description (`register index`, `chronological index`, `alphabetical repertory`, `minuten`,
  `resolution text`, `title page`, `list`, …).

**Entry gold** (`reads_entries.tsv`): read the 30 `read` rows. First give the side a `side_verdict` as above. If it
is `not index` or `unreadable input`, record it, label no lines, and read the reserve of the same `(split, group)`
instead (`used_for` = the replaced `read_id`); a reserve is used only this way, and only once. For an `index` side,
label **every listed line**:
- `S`: the first line of an entry. A date-only line followed by the entry text: the date line is `S`, the text `C`. A
  sub-entry with its own date or its own page/folio reference is a new entry (`Advis R: V: State daarop. 95.` → `S`).
- `C`: a continuation line of the entry above it.
- `H`: a line in no entry: lemma or section heading, page number, running header, catchword, a year line.
- `?`: you cannot decide. It is left out of precision and recall in 19d.
- A line that holds text of **both columns** (an HTR merge across the gutter): label it by its first part and write
  `merged` in `note` (phase decision: gutter merges are counted, not repaired).
- Write the labels to `entry_gold.tsv` side by side as you go (append per side), so a long session loses nothing.

**Check page** (blind, for the user): draw 30 of the 60 side reads with `random.Random(20261116)`: 10 from `S1`, 10
from `S2`, 10 from `S3h` + `S3a` together (`sample` on each stratum's rows sorted by `read_id`). One item per side:
inventory number, EAD title and date, the scan id as a Nationaal Archief link (`scanLine`), the page side in words
(`verso = left page of the opening`, `recto = right page`, `single = the whole scan`), and the first 3 listed lines of
the side (to find it on the image). The question: "Is this page side an index (entries pointing to resolutions by date
or page/folio number, under headings)?" Buttons **Index / Not index / Unsure** and a note field. The instructions at the
top give the two definitions above with one example each, say to look at the named page side only, and say where to
save the export: `reports/data/phase-13-register-index-volumes/step-19a/check_verdicts.json`. `CheckStore` key
`as_phase13_19a_sides_v1`. **No agent verdict on the page**; grep the HTML for `index"`/`not index` values from
`side_verdicts.tsv` before handing it over. Check the item count (30), that every item has its IDs and link, and the JS
syntax (`osascript -l JavaScript` with `new Function(src)`, as in `checkpage._selftest`).

**UA entry** (append to `docs/USER_ACTIONS.md` with Edit, format of that file):
`### UA-0NN Blind check of 30 index-side labels (2026-MM-DD, from phase-13 step 19a)`.
- **Action:** Run: `open reports/data/phase-13-register-index-volumes/step-19a/check_page.html` (from the AS root);
  judge the 30 sides and export to the named file.
- **Options:** (a) do the check (about 30 min); (b) skip it: 19e then states that the gold is unchecked. Recommended:
  (a).
- **Default if unanswered:** step 19e does not start. **Blocks:** step 19e only (19b–19d run on the agent labels).
- **Response:** (empty)

## Constraints

- No change under `archival_structures/`, `tests/`, `docs/` (except `USER_ACTIONS.md`), the layers or the planning
  files. Never `git commit`; `chmod g+w` every file and directory you create; `.venv/bin/python`; zsh; no `rm -rf`,
  `nohup` or `&`. Shared files (report, `STATUS.md`, `PROGRESS.md`, `USER_ACTIONS.md`) only with Edit; step 19b runs
  at the same time.
- Read every side yourself; do not label lines with a rule or a script.
- If a listed layer file is missing, stop and say which.

## Tests

None (no library code). Run `.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19a/summary.py`
and paste its output into the report.

## Acceptance criteria

1. `side_verdicts.tsv`: 60 rows, `read_id` 1–60 once each, verdict in the four values. Report the counts per stratum.
   If more than 10 of 60 are `unclear` or `unreadable input`, say so first in the report (step 19c then stops on its
   bar D2).
2. `entry_sides.tsv`: every `read` row and every used reserve; at least 28 `index` sides in total, at least 9 in `dev`
   and 18 in `test`. If fewer, stop and report (do not draw new sides).
3. `entry_gold.tsv`: one row per listed line of every `index` entry side, labels in `S/C/H/?`; per side `n_S ≥ 1`.
4. Check page: 30 items (10/10/10), blind, links and copy buttons on every item, JS syntax checked; UA entry written.

The user decides when the step is done.

## Error analysis

This step *is* the reading. In the report give: verdict counts per stratum; `kind` counts; for the entry sides per
`group` (G1 GT register index sections, G2 late register front sections, G3 index volumes, G4 other index books) the
lines, `S`, `C`, `H`, `?` and the mean entry length in lines; how entries start (count over all `S` lines: starts with
a date, the line before ends in a number, starts further left than the line above, none of these: estimate each by
reading, not by regex); the number of `merged` lines; and 5 verbatim examples of hard cases with your decision. Draft
findings: the index styles seen (with counts), the share of `?`, gutter merges per side.

## Do not touch

`archival_structures/`, `tests/`, every layer under `data/`, `reports/data/phase-13-register-index-volumes/planning/`,
other phases' report data, `shared-data/`, the siblings. Never touch the external drive.

## When done

1. Report: add `### Step 19a: gold reads (index sides and entry starts)` to
   `reports/phase-13-register-index-volumes.md` (what was read and how, the tables above, hard cases, findings
   `F13-19a.n` as `draft` rows in the *Findings* table); `make reports` clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 19a: gold reads (index sides and entry starts)
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none
- **Tests:** none (summary.py output in the report)
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-13-register-index-volumes.md` § Step 19a](../../reports/phase-13-register-index-volumes.md#step-19a-gold-reads-index-sides-and-entry-starts); findings F13-19a.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** UA-0NN (blind check)
```

3. Overwrite the phase `STATUS.md`: 19a done; the UA id; facts for 19c/19d (the gold file names and counts, reserves
   used).
