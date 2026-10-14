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

## Tests

- `tests/test_x.py::test_y`: <what it pins down>
- Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/<files>`; sibling
  `<command>`.
- Expected test counts come from `pytest --collect-only -q`, not from reading the test files.

## Acceptance criteria

1. <Concrete, with numbers.>
2. Full AS suite green, and the sibling suite green.

The user decides when the step is done.

## Error analysis

<Which individual outputs to read (how many, and which kind), and what to look for. For
example: diff old and new sibling output, and read every changed row up to 30, then a random
sample of 30. Name the categories to classify cases into, if they are known in advance, and the
findings the step is expected to produce (e.g. "the new Aanwinsten count").>

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
