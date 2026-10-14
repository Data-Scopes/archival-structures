# Phase NN: <title>

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).

## Goal

<2–4 sentences: what is true of AS and the sibling(s) when this phase ends.>

## Owns

<The AS modules and sibling files this phase may edit. Another active phase must not touch
them (`plan/README.md`, *Parallel phases*).>

## Sources

<What the planning session reads, with exact line ranges. For example, inventory §3 C5 at
`upstream_inventory.md` L258–268. Prefer `Read` with offset/limit over whole files.>

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| SS | | | | `step-SS-slug.md` |

## Why this order

<The sequencing, and which step de-risks which.>

## Decisions that bind this phase

<Project decisions (D1–D9 in inventory §6) and phase decisions, each with the rule a step must
follow and enough rationale for a reviewer to tell a considered choice from an accident.>

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|

## Hazards

<Hazards a step could actually hit. The planning session copies each one into the brief of
every step it affects.>

## Open questions for planning

<What the planning session must settle or check before writing briefs. Remove each item once
it is answered.>

## Review checklist

- [ ] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer rather than taken from the log.
- [ ] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`; sibling tests with the
      sibling's interpreter.
- [ ] Error analysis read individual cases, as listed in each brief, and is written up in the
      phase report (`reports/phase-NN-<slug>.md`), one section per step.
- [ ] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [ ] Every public API change is in `docs/shared_library/changelog.md`, with the affected
      sibling imports.
- [ ] Ported code came with its tests; the sibling switched its import only after those tests
      passed here.
- [ ] No archive-specific knowledge hardcoded in AS core: it is a parameter with a named
      default.
- [ ] Sibling copies deleted, or turned into shims, as planned.
- [ ] Leakage checks (`CLAUDE.md`) where a metric or split is involved.
- [ ] `Brief defects` entries answered: templates or briefs fixed.
- [ ] <phase-specific checks>

## Hand-off

<Filled in at review: what later phases can now assume, and the release this went into.>
