# Step 22r: Review fixes for phase 16

> **Phase:** 16, A second archive (1.04.02 VOC): what generalises. **Depends on:** 22a–22e (done) and the phase
> review (Opus 5.5, 2026-11-01), which found the defect below.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file, then the *Step 22d* and *Step 22e* sections and the F16-22d / F16-22e rows of
> `reports/phase-16-voc-generalisation.md`. **Read nothing else unless this brief names it.**

## Context

`toc_gold.tsv` (`inv row scan_id toc start split`) has **one row per GLOBALISE CSV row, not per scan**. 698 scans
have two or more rows (up to 4), and 691 of them disagree on `start`. Example: 1120 rows 536 (start 1) and 537 (start 0) are both
`NL-HaNA_1.04.02_1120_0535`. `toc` never disagrees.

`reports/data/phase-16-voc-generalisation/step-22d/eval_links.py`, `load_gold()` (line 76), keeps the **last** row per
scan:

```python
g[r["inv"]][r["scan_id"]] = (int(r["toc"]), int(r["start"]))
```

So a scan whose start row is followed by a non-start row loses its start. That deflates E, coverage and chance in
every cell, on dev and test.

The reviewer ran a fixed copy (taking the max over a scan's rows) in a scratch directory. The results:
- **Dev:** C3 is still selected (F1 0.5106, E 689/1,275 = 0.540); C2 is second (F1 0.4948).
- **Test:**

  | Cell | E | Wilson | Chance | Coverage |
  |---|---|---|---|---|
  | U1 | 142/411 = 0.345 | 0.301–0.393 | 77 | 130/478 |
  | U2 | 90/163 = 0.552 | 0.476–0.626 | 41 | 103/478 |
  | C3 | 279/433 = 0.644 | 0.598–0.688 | 103 | 275/478 |

- **2775 (C3):** 41/143, coverage 33/52, chance 22.
- **Silver:** unchanged.

The verdict stays `fails on VOC`. Your rerun must reproduce these numbers. If it does not, stop and report.

## Deliverables

1. **The fix.** In `eval_links.py` `load_gold()`, a scan's (toc, start) is the max over its rows:
   ```python
   old = g[r["inv"]].get(r["scan_id"], (0, 0))
   g[r["inv"]][r["scan_id"]] = (max(old[0], int(r["toc"])), max(old[1], int(r["start"])))
   ```
   Add a one-line comment saying why.
2. **Rerun in place,** stages `dev` then `test`. Run them with `PYTHONDONTWRITEBYTECODE=1` from the AS root:
   `.venv/bin/python reports/data/phase-16-voc-generalisation/step-22d/eval_links.py --stage dev`, then `--stage test`.
   - The test stage asserts that `test_done.json` does not exist. So first move the old outputs to
     `reports/data/phase-16-voc-generalisation/step-22d/superseded_22r/` with `mv`, keeping their names. Move all of
     these: `select.json`, `test_done.json`, `dev_grid.tsv`, `dev_links.tsv`, `links_eval.tsv`, `silver.tsv`,
     `end_to_end.tsv`. This is a scorer fix, not a second look at test: the selection rule and cells are unchanged.
   - Do **not** rerun the `errors` stage. `errors_dump.txt` and `cases_read.tsv` stay as read. Instead, list which of
     the 60 read non-hits (`cases_read.tsv`) are hits under the fixed gold. Write that list to
     `reports/data/phase-16-voc-generalisation/step-22d/cases_flipped_22r.tsv` and give the count per category.
3. **Report, Step 22d section.**
   - Replace the dev-grid table, the Bar E table, the 2775 sentence, the per-volume list and the coverage numbers with
     the rerun's numbers.
   - Add one paragraph headed **Review fix (22r)**: what the defect was, the old and new C3 test E (274/433 → 279/433),
     and that the selection and verdict are unchanged.
   - Update the error-analysis text only where the flipped cases change a count. Add a sentence saying so.
   - Update findings F16-22d.1, .3, .4, .5 and .8 where their numbers change. Keep status `draft`; the review sets the
     final statuses.
4. **Report, Step 22e section.**
   - Update row 9 of the table and F16-22e.1 (the "E 0.336-0.633" range) to the new numbers.
   - Update `reports/data/phase-16-voc-generalisation/step-22e/generalisation.tsv` row 9 to match.
   - Change nothing else in 22e.
5. **Clean up.** Delete the directory `reports/data/phase-16-voc-generalisation/step-22c/__pycache__`: delete its one
   `.pyc` file with `rm`, then `rmdir` the directory.

## Constraints

- Never `git commit`. `chmod g+w` every file and directory you create.
- Do not touch library code, the layer, the gold files, or any report section other than 22d and 22e.
- Do not edit the F16-P, 22a, 22b or 22c rows. The reviewer revises those.
- Use Edit for the report and `STATUS.md`.
- T7_Data rules:
  - read only, except under `/Volumes/T7_Data/Data/claude-projects-external`;
  - a hook denial or `Permission denied` there is final: stop and report.
- The script is single-process, so no coreshare is needed.

## Acceptance criteria

1. The rerun reproduces the reviewer's numbers in *Context*.
2. `select.json` (new) still selects C3, and it is time-stamped before the new `test_done.json`.
3. The 22d and 22e numbers in the report, and `generalisation.tsv`, agree with the new `test_done.json` and
   `dev_grid.tsv`.
4. `make reports` is clean. `.venv/bin/python -m pytest -q -p no:cacheprovider` passes: 1,413 passed, 11 skipped.
5. No `__pycache__` is left under `reports/data/phase-16-voc-generalisation/`.

## When done

- Append a `PROGRESS.md` block.
- In `STATUS.md`, add a row for 22r (done) and set the next action to "phase review finalisation (Opus)".
- Return a report of at most 25 lines: the numbers, the flipped cases, the files changed, and any deviation.
