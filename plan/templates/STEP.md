# Step SS: <title>

> **Phase:** NN, <phase title>. **Depends on:** <step ids, or "nothing">.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

<3–5 sentences: what this step does, and why now.>

## Inputs

<Exact paths in AS and the sibling, with line ranges as checked on <date> **and** a grep-able
anchor for each (a function name or a unique string), because line numbers drift. Signatures to
call, copied in verbatim. Fixture data to develop against, including the input for every
before/after comparison the acceptance asks for, and any input that is known to be empty. The
baseline directory to diff against, as currently named in `STATUS.md`. Any sibling `CLAUDE.md`
to read first.>

```python
# signatures you will call
```

If an input is not available locally, stop and tell the user which files to stage.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/...` | `def f(...) -> T`: <one line> |
| `tests/test_....py` | <tests> |
| `<sibling>/...` | <import switch, deletion, shim> |
| `docs/shared_library/changelog.md` | `### Step SS: …` under *Unreleased*, added with Edit |
| `reports/phase-NN-<slug>.md` | `### Step SS: <title>` section and draft findings `FNN-SS.n` (see *When done*) |

## Behaviour to implement

<The exact rules, edge cases and data shapes. Where porting, say what to keep verbatim and what
to change, and why.>

## Constraints

- <What not to build, and where it already exists.>
- <Interfaces that must not change.>
- Never `git commit`; never bump the version; `chmod g+w` new files.
- **Protected outputs** (stored results a step reproduces, caches): name them here. Hash them
  (sha256 and mtime) before the first run and check after every run. Run any sibling script
  with an explicit output path into the scratchpad, never with its default. When a script's
  default output path is a module constant that another script patches, resolve it at call
  time. (Phase 03: F03-09.7, a stored CSV overwritten through a default bound at definition
  time.) When an A/B run loads a saved copy of an original module, set its `__file__` to the
  real module path if it derives paths from `__file__` (phase 04, step 12b).
- An error-analysis sample ("read 30 differing cases") says what to do if there are fewer:
  "all of them, if any".
- When steps run in parallel, shared files (changelog, report, `STATUS.md`, `PROGRESS.md`) are
  edited only with Edit, never by a scripted read-modify-write, so a concurrent edit isn't lost.
- Agent shell (phase 05): install with `.venv/bin/python -m pip` (there is no `.venv/bin/pip`)
  and `chmod g+w` what it installs. `nohup`/`&` fail, so run long jobs in the foreground.
  `rm -rf` is denied, so give every scratch run its own new directory.
- A pin change greps AS's tests for the upstream behaviour it changes. (Phase 05: a test pinned
  pagexml-tools 0.8.1's `vertical_distance` bug.)
- A port brief says that the planner **ran** the original once under the current pins, not
  only imported it. (Phase 06: republic's splitter imported cleanly but raised on every page
  under pagexml-tools ≥ 0.8.)
- Every expectation about the data in a brief ("near 0", "about 900 px") says whether it was
  measured (give the number and the inventories) or not. (Phase 06: hand candidate rates were
  expected near 0 and came out at 40–87%.)

## Tests

- `tests/test_x.py::test_y`: <what it pins down>
- Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/<files>`; sibling
  `<command>`.
- Expected test counts come from `pytest --collect-only -q`, not from reading the test files.

## Acceptance criteria

1. <Concrete, with numbers. For each file comparison, say how it is compared: bytes,
   decompressed text, or JSON without named keys. Run every acceptance query once at planning
   time on the full data, not on a sample (phase 05: 98 irregular scan ids).>
2. Full AS suite green, and the sibling suite green.

The user decides when the step is done.

## Error analysis

<Which individual outputs to read (how many, and which kind), and what to look for. For
example: diff old and new sibling output, and read every changed row up to 30, then a random
sample of 30. Name the categories to classify cases into, if they are known in advance, and the
findings the step is expected to produce (e.g. "the new Aanwinsten count"). A hand-read
precision bar comes with the rule for judging ambiguous cases, e.g. a historical compound
written as one word (phase 06, step 15f).>

Write the error analysis into the phase report (see *When done*), not into `PROGRESS.md`.

## Do not touch

- <Modules owned by another active phase; files outside this step.>

## When done

1. Add your `### Step SS: <title>` section to `reports/phase-NN-<slug>.md`, following
   `plan/templates/REPORT.md`. Create the file from the template if it doesn't exist. Add one
   row per finding to its *Findings* table (ID `FNN-SS.n`, status `draft`). Don't edit other
   steps' sections or the *Summary*/*Review* sections. Then run `make reports` from the AS root
   and fix any warning it prints.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: current step done, next step, open questions.

```markdown
### Step SS: <title>
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-NN-<slug>.md` § Step SS](../../reports/phase-NN-<slug>.md#step-ss-title);
  findings FNN-SS.1–n
- **Error analysis:** <two lines at most; the details are in the report>
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
