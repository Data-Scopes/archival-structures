# How `plan/` works

AS work runs as **phases** of a few **steps**. Each step and each phase review runs in a **new
session**, so no session has to read a long history. The files are set up so that each kind of
session reads only what it needs.

## Layout

```
plan/
  README.md          this file: the protocol, and the rules every session follows
  PLAN.md            phase index: one row per phase, with status. Short
  CONTEXT.md         standing context for planning and review: the pipeline as it is now,
                     accepted deviations, recurring brief defects. Updated by each phase review
  PROMPTS.md         template prompts for planning, implementation and review sessions
  templates/         skeletons for the per-phase files
  phase-NN-slug/
    PLAN.md          the phase plan: goal, rationale, steps, decisions, review checklist (Opus)
    STATUS.md        current state in under 40 lines; overwritten, not appended
    PROGRESS.md      append-only log, one block per session
    step-SS-slug.md  one self-contained brief per step (Sonnet)
    REVIEW.md        written by the phase review (the code review)

reports/             at the AS root, next to plan/: the written results, for reading
  index.md           phase list plus a findings index, one row per finding (shared, append-only)
  phase-NN-slug.md   one report per phase, from templates/REPORT.md
  data/phase-NN-slug/  tables too long for the report (TSV), linked from it
  _tools/            the pandoc Lua filter and CSS used by `make reports`
  _build/            HTML output of `make reports` (gitignored)
```

`plan/` records how the work went; `reports/` records what was found. A reader who wants a
result, a number or an error analysis goes to `reports/` and never needs the plan files.
`REVIEW.md` reviews the code; the report's *Review* section reviews the findings.

**Finding IDs.** Every finding gets an ID `F<phase>-<step>.<n>`, e.g. `F02-05.2`. A finding
made during phase planning is `F<phase>-P.<n>`, and one the review adds is `F<phase>-R.<n>`. The step is part of the ID so that steps running in
parallel never collide. IDs are stable: a wrong finding is marked `withdrawn`, never deleted or
renumbered. `grep -rn F02-05.2 reports/ plan/` finds every mention.

Steps keep the global numbers from `docs/shared_library/upstream_inventory.md` §4 (step 4 is
`step-04-…`), so the changelog, the inventory and the plan use the same numbers. A step the
inventory doesn't have gets a letter suffix, e.g. `step-03b-…` or `step-06r-review-fixes`.

## Sessions and models

| Session | Model | Reads | Writes |
|---|---|---|---|
| **Phase planning** | Opus | `plan/PLAN.md`, `plan/CONTEXT.md`, the phase `PLAN.md` and `STATUS.md`, the source sections the phase `PLAN.md` names, and the current code (via an Explore subagent) | step briefs, phase `PLAN.md`, `STATUS.md`, a `PROGRESS.md` block, and the report skeleton `reports/phase-NN-slug.md` (from `templates/REPORT.md`, status *draft*) |
| **Step implementation** | Sonnet | the phase `STATUS.md` and its own step brief; **nothing else unless the brief names it** | code and tests, **its section of the phase report** (results, error analysis, draft findings), a `PROGRESS.md` block, `STATUS.md`, the changelog entry |
| **Phase review** | Opus | the phase `PLAN.md`, `STATUS.md` and `PROGRESS.md`, the phase report, the diff, and the code | `REVIEW.md`, the report's *Summary*, *Review* and *Follow-ups* sections, finding statuses, the phase's rows in `reports/index.md`, a review-fixes brief if needed, `plan/PLAN.md`, `plan/CONTEXT.md` (accepted deviations, follow-ups), `STATUS.md` |
| **Status check** (optional) | Haiku or Sonnet | `plan/PLAN.md` and the phase `STATUS.md` files | nothing |

Implementation steps run on Sonnet, so **a step brief must be complete**. It gives exact file
paths, the signatures to call (copied in), the API to build, the tests, numeric acceptance
criteria, and what not to touch. If a Sonnet session has to explore the code base to find
something out, the brief has a defect. The session records it under *Brief defects*, and the
review fixes the template.

## Size budgets

These keep fresh sessions cheap. Split a file before it goes over its budget.

| File | Budget | When it gets too long |
|---|---|---|
| `plan/PLAN.md` | ~80 lines | move detail into the phase `PLAN.md` |
| `plan/CONTEXT.md` | ~150 lines | drop what no later phase needs; history is in `PROGRESS.md` |
| phase `PLAN.md` | ~200 lines | the phase is too big; split it |
| `STATUS.md` | 40 lines | overwrite it; history belongs in `PROGRESS.md` |
| `PROGRESS.md` block | ~40 lines per session | put results tables and the error analysis in the phase report and link to its section |
| step section of a report | ~120 lines | move long tables to `reports/data/phase-NN-slug/*.tsv` and link them |
| step brief | ~250 lines | split the step |
| phase | 2–5 steps | open a new phase |

## Rules for every session

1. Project rules in `CLAUDE.md` apply. Never `git commit`, never run the release targets,
   `chmod g+w` everything created, use `.venv/bin/python`, never access external drives.
2. **When a step edits a sibling (LotA or NLP), read that sibling's `CLAUDE.md` first.** Its
   rules apply to the files there. Run the sibling's tests with the sibling's own interpreter.
3. A change to an AS public signature or behaviour gets a `### Step N: …` entry under
   *Unreleased* in `docs/shared_library/changelog.md`, listing the affected sibling imports.
   **Add the entry with Edit, never by rewriting the file.** Two phases may run at the same time.
4. Sessions never bump the version. The user releases after a phase review, or after a single
   step if a sibling needs to pin to it. `STATUS.md` records which release a phase went into.
5. Before stopping, update the phase `STATUS.md` (overwrite) and append a `PROGRESS.md` block.
   The next session starts from those two files, not from the conversation.
6. **Stop and ask the user** when:
   - an input is missing locally (name the files to stage);
   - write access is missing;
   - an acceptance number doesn't reproduce;
   - a decision in the brief turns out to be wrong;
   - finishing the step would mean changing a public API the brief didn't name.

   Record the question in `STATUS.md` under *Open questions*.
7. Every step includes error analysis, as `CLAUDE.md` requires: read individual changed
   outputs, failures and successes, not just the counts.
8. **Write it up in the phase report.** Before stopping, a step session adds its
   `### Step SS: …` section to `reports/phase-NN-slug.md`, following `plan/templates/REPORT.md`.
   If the file doesn't exist, it creates it from the template. The section holds what changed,
   the results against the acceptance criteria, the error analysis (what was read and how it
   was sampled, categories with counts, verbatim examples, a conclusion) and the findings. Each
   finding gets a row in the report's *Findings* table with status `draft`. A negative result
   is a finding too. The `PROGRESS.md` block summarises in a few lines and links to the
   section. A step that edits a sibling writes a sibling report (e.g. LotA `report/NN-…`) only
   when its brief says so; the AS phase report is the canonical one.
9. Only the phase review touches the report's *Summary*, *Review* and *Follow-ups* sections,
   changes a finding's status, sets the report to *final*, and appends the phase's findings to
   `reports/index.md`. After any report change, run `make reports` and check that it gives no
   warnings.
10. **Propagate mid-phase changes.** A session that changes a baseline, a shared constant or
    regex, or an acceptance number (for example in a follow-up the user asks for) updates every
    step brief in the phase that names it and has not run yet. It records the change in
    `STATUS.md` under *Facts later steps need*. (Phase 02: the `post-quote-rule/` baseline
    reached briefs 05 and 06 only through `STATUS.md`, F02-R.8.)

## Parallel phases

Two phases may be active at the same time when they touch disjoint modules. They share one
working tree, so:

- each phase `PLAN.md` lists the modules it owns;
- a step never edits a module that another active phase owns;
- the changelog and `reports/index.md` are the only shared files. Both are edited by appending
  with Edit, as in rule 3. Each phase has its own report file.
