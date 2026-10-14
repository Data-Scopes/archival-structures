# Step 03c: LotA output baselines after AS step 2

> **Phase:** 02, EAD for LotA. **Depends on:** 03d (reports tooling). Must be done before 05 and 06.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

AS step 2 (2026-10-14) changed how EAD text is extracted, and with it LotA's scope-note and
title text. LotA's outputs were never regenerated after that change. Steps 05 and 06 will
each diff a regenerated output against a baseline, so the baseline has to come from the
post-step-2 code, **before** either step's own change. LotA has no git history, so the
current, pre-step-2 output files are the only record of the old state. Keep them (user
decision, 2026-10-14). This step:

1. preserves the pre-step-2 outputs;
2. regenerates post-step-2 baselines into a separate directory;
3. adds a small reusable TSV diff tool;
4. checks that the pre/post differences are exactly the ones step 2 predicted.

The canonical `output/scope-notes/` and `output/titles/` are **not** touched; 05 and 06
overwrite them.

## Inputs

LotA root: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive`. **Read LotA
`CLAUDE.md` first.** Interpreter: `venv/bin/python`. Checked on 2026-10-14.

**Current outputs (pre-step-2):**
- `output/scope-notes/1.01.02-notes.tsv.gz`: 2,176 rows, 2026-10-03.
- `output/scope-notes/1.04.02-notes.tsv.gz`: 1,471 rows, 2026-10-03.
- `output/titles/<archive_id>-normalised-titles.tsv.gz`: 9 files, 2026-10-08.
- `output/titles/normalisation-census.tsv` and `output/titles/token-kinds-by-level.tsv`.

`output/titles/` also holds other title_vocab and gold files. Leave them alone.

**CLIs.** Both parse the full EAD through `ArchiveTree.load`, and both create `--out` if it is
missing.
- `archival_language/cli/parse_scopecontent.py`. Options: `--archive` (repeatable; the default
  is `1.01.02` and `1.04.02`), `--tags` (default `scopecontent`), `--limit`, `--out` (default
  `output/scope-notes`). It writes `<out>/<archive_id>-notes.tsv.gz`.
  - Columns: `node_id, archive_id, source_tag, element_index, shape, shape_rule, text,
    segments, references, n_references, date_strings, n_date_strings, char_len, citation`.
  - List columns are joined with `" ||| "`. A reference is serialised as `kind:raw:inv:folio`.
  - The row key is `(node_id, source_tag, element_index)`.
  - Expected to take seconds.
- `archival_language/cli/normalise_titles.py`. Options: `--archive` (the default is all 9
  registered archives), `--limit` (smoke mode; also skips the census), `--out` (default
  `output/titles`).
  - It writes `<out>/<archive_id>-normalised-titles.tsv.gz`, `<out>/normalisation-census.tsv`
    and `<out>/token-kinds-by-level.tsv`.
  - Columns: `node_id, level, depth, original, text, prose, n_tokens, date_text, date_source,
    date_span_start, date_span_end, date_spans, date_locator, lb_adjusted,
    spelling_normalised, ead_sha256`.
  - The row key is `node_id`.
  - It raises `AssertionError` if a title invariant fails.
  - Expected to take seconds to tens of seconds.

**Step 2's predictions** (from LotA `STATUS.md`, 2026-10-14, measured on SG and VOC, 45,872
titles together):
- `lb_adjusted`: True for 23 titles before, 0 after.
- `original` changes for 12 titles (the `<lb/>` glue fixes, e.g. `eerstedeel` → `eerste deel`).
- `text`/`prose` change for 11 titles (spurious spaces before punctuation disappear).
- `date_span` shifts by one character for 2 titles.
- `date_locator` is unchanged (0 regex fallbacks before and after).

For scope notes: `text` changes only where whitespace sat at a child element's edge
(previously glued, e.g. `derdenSeptember` → `derden September`), or where `<lb/>` stood before
punctuation (`Eems . Met` → `Eems. Met`). `char_len`, `segments`, `references` and
`date_strings` may change only on rows whose `text` changed. Nothing was predicted for the 7
other archives.

## Deliverables

| File | Change |
|---|---|
| LotA `output/baselines/pre-as-step-2/{scope-notes,titles}/` | `cp -p` copies of the current files listed above |
| LotA `output/baselines/post-as-step-2/{scope-notes,titles}/` | regenerated with the current code |
| LotA `output/baselines/README.md` | what each directory is: date, code state, exact command, row counts |
| LotA `archival_language/cli/diff_outputs.py` | new keyed TSV diff tool (spec below) |
| LotA `tests/test_diff_outputs.py` | 3 synthetic tests |
| AS `reports/phase-02-ead-for-lota.md` | `### Step 03c: …` section: the diff summary tables, the error analysis and draft findings `F02-03c.n`; then `make reports`. There is no separate LotA report: the AS phase report is the canonical one. Long changed-row listings go to `reports/data/phase-02-ead-for-lota/03c-*.tsv` |
| LotA `STATUS.md` | append a dated section |

## Behaviour to implement

**Before running anything**, check that you can write to `output/` (`test -w`). If you can't,
stop and ask the user.

**`diff_outputs.py`**
`venv/bin/python -m archival_language.cli.diff_outputs OLD NEW --key COL[,COL...] [--ignore-cols COL,...] [--out CHANGED.tsv] [--sample N] [--seed S]`.
- Read both files with `pandas.read_csv(sep="\t", dtype=str, keep_default_na=False)`; `.gz`
  is handled automatically.
- Report:
  - row counts;
  - keys only in OLD and keys only in NEW (count plus up to 10 examples);
  - columns only in one file;
  - for each shared column, the number of rows whose value differs;
  - the number of rows with any difference.
- `--out` writes one row per (key, column) difference, with columns `key..., column, old,
  new`.
- `--sample N` prints N seeded random changed rows, showing old and new for each changed
  column.
- Exit code 0 always: a diff is information, not failure.
- Write a `main(argv)` and a function
  `diff_frames(old: pd.DataFrame, new: pd.DataFrame, key: list[str], ignore: list[str] = ()) -> DiffResult`
  (a dataclass of the counts above plus a long-form changed-cells DataFrame), so steps 05 and
  06 can import it.
- Error on duplicate keys in either file: `ValueError` naming the first duplicate.

## Constraints

- Don't modify any LotA code other than the new `diff_outputs.py` and its test.
- Don't overwrite `output/scope-notes/` or `output/titles/`.
- Never `git commit`; `chmod -R g+w` everything you create (including `output/baselines/`).

## Tests

`tests/test_diff_outputs.py`:
- `test_identical_frames_report_no_changes`;
- `test_changed_cell_and_added_key_are_reported` (one changed cell, one key only in NEW);
- `test_duplicate_key_raises`.

Commands, from the LotA root:
- `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_diff_outputs.py`;
- the full suite with `--ignore=tests/test_derive_voc.py`. The baseline is 320 passed, or more
  if step 03b ran first; check the phase `STATUS.md`.

## Acceptance criteria

1. The pre copies are byte-identical to the canonical files (`cmp`). The post directories hold
   2 scope-note files and 9 title files plus the census and token-kinds files. Wall time for
   each CLI run is recorded in the README and `PROGRESS.md`.
2. Scope notes, pre vs post: identical keys (2,176 + 1,471). Every changed `text` falls into
   one of the two predicted categories. Count each category per archive.
3. Titles, pre vs post, SG and VOC: the counts reproduce 23 / 12 / 11 / 2 / 0 (lb_adjusted /
   original / text-or-prose / date_span / date_locator). If a count differs, **stop and ask
   the user** (README rule 6) and record the difference.
4. Titles, other 7 archives: report every changed column with counts. There is no prediction,
   so nothing is asserted.
5. The census file: report every changed cell.
6. The LotA suite is green.

The user decides when the step is done.

## Error analysis

- **Scope notes.** Read every changed row, up to 30 per archive, old `text` next to new.
  Classify each as child-edge space, `<lb/>`-before-punctuation, or *other*. Read every
  *other* row in full, look up its element in the EAD (by `citation`), and say what caused it.
  Possible causes: a LotA code change between 2026-10-03 and 2026-10-14 other than step 2, or
  an EAD file change (compare `ead_sha256` in the title files).
- **Titles.** Read every changed SG/VOC row (about 50). For the other 7 archives, read up to 30
  changed rows each, plus a seeded random sample of 10 unchanged rows across all archives.
- Put the tables in your section of the phase report and give the totals in `PROGRESS.md`.
  Expected findings include: whether step 2's predictions reproduced, the category counts, and
  any *other* change with its cause.

## Do not touch

- AS code (nothing in AS changes in this step), `stream_analysis/`, NLP.
- LotA `ead/`, `facets/`, `model/` (steps 03b, 05, 06).

## When done

1. Write your `### Step 03c: …` section in `reports/phase-02-ead-for-lota.md`. Follow
   `plan/templates/REPORT.md`: what changed, results, error analysis (what was read and how it
   was sampled, categories with counts, verbatim examples, a conclusion), and open issues. Add
   one *Findings* row per finding (`F02-03c.n`, status `draft`); a negative result is a
   finding too. Don't edit other sections. Run `make reports` from the AS root; it must
   print no warnings.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: this step done; record the baseline paths and the
   `diff_outputs` usage under *Facts later steps need*.

```markdown
### Step 03c: LotA output baselines after AS step 2
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none in AS; LotA `archival_language.cli.diff_outputs.diff_frames`
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers and wall times>
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 03c; findings F02-03c.1–n
- **Error analysis:** <two lines at most; the details are in the report>
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
