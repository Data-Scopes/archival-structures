# Phase NN review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** <model>, <date>
- **Verdict:** accept / accept with follow-ups / rework

## Checklist

<The phase `PLAN.md` review checklist, item by item, pass or fail with evidence: a command and
its output, a number, a file:line.>

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|

## Findings

Most serious first. **Defect** means must fix (it goes into a review-fixes brief), **debt**
means recorded and deferred, and **observation** means no action.

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|

## Error analysis read by the reviewer

<The individual cases the reviewer read, and whether the steps' own error analysis holds up.>

## Brief quality

<Each `Brief defects` entry, and what was changed in the templates or later briefs to avoid it.>

## Updates applied

- [ ] Phase report `reports/phase-NN-<slug>.md`: *Summary*, *Review* and *Follow-ups*
      written; every finding is `confirmed`, `revised` or `withdrawn`; status set to *final*
- [ ] `reports/index.md`: the phase's findings appended (with Edit), and its phase row updated
- [ ] `make reports` runs without warnings; links checked in `reports/_build/`
- [ ] Review-fixes brief written (`step-SSr-review-fixes.md`) or not needed
- [ ] `docs/shared_library/changelog.md` complete for this phase
- [ ] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [ ] phase `STATUS.md` overwritten
- [ ] sibling `STATUS.md` notes, if the sibling's own conventions ask for them
- [ ] Release recommendation for the user: version and what it contains
