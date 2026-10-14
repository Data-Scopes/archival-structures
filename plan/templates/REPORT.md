# Phase NN report: <title>

> **Status:** draft (step sessions add sections) / final (set by the phase review).
> **Phase plan:** [`plan/phase-NN-<slug>/PLAN.md`](../plan/phase-NN-<slug>/PLAN.md).
> **Code state:** AS <version or "unreleased after vX.Y.Z">; sibling(s) <name, date>.
> **Last updated:** <date>, by <session>.

<!-- Copy this file to reports/phase-NN-<slug>.md. Write GitHub-flavoured Markdown: pipe
tables, no raw HTML, and relative links to other reports as `phase-NN-<slug>.md#anchor`.
`make reports` turns the links into .html. -->

## Summary

<At most 10 lines, written or rewritten by the phase review: what the phase set out to do,
what changed for the siblings, and the 3–5 findings a reader should know.>

## Findings

One row per finding. The ID is `F<phase>-<step>.<n>`, e.g. `F02-05.2`. Planning findings use
`F<phase>-P.<n>` (in a `### Planning` section before the steps), and the review's own findings
use `F<phase>-R.<n>`. IDs never change and are never reused. A finding that turns out
to be wrong is marked `withdrawn`, not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| FNN-SS.1 | <e.g. "7 of 9 LotA archives have an `<eadid>` that differs from the LotA archive id"> | data-property | ead, node-id | draft | [Step SS](#step-ss-title) |

- **Kind:** `result` (an acceptance number), `behaviour-change` (output differs, as intended),
  `bug-fixed`, `data-property` (a fact about the archives or the corpus), `negative-result`,
  `open-issue` (found, not fixed), `method` (a lesson about how to do the work).
- **Tags:** lower-case keywords, comma-separated: the archive (`sg`, `voc`, …), the component
  (`scope-notes`, `node-id`, `titles`, …) and the sibling (`lota`, `nlp`).
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Step SS: <title>

**What changed.** <2–5 lines: AS API, sibling code, outputs. Link the brief.>

**Results.**

| Criterion | Expected | Measured |
|---|---|---|

**Error analysis.**

- *What was read:* <n cases, how they were chosen (all / first 30 / seeded random sample,
  seed), and from which output file>.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|

- *Conclusion:* <what the failures, or the changes, had in common; what that says about the
  method>.

**Findings:** FNN-SS.1, FNN-SS.2.

**Open issues.** <or "none">

<!-- Tables longer than ~30 rows go into reports/data/phase-NN-<slug>/<file>.tsv and are
linked from here. -->

## Review

<Written by the phase review: which step findings were confirmed or revised, the reviewer's own
error analysis (cases read, verdict) and findings `FNN-R.n`. A link to `REVIEW.md` for the code
review.>

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|

## Reproduce

<The commands that regenerate every number in this report, with interpreters and paths.>
