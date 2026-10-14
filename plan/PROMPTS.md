# Template prompts

Copy a prompt, fill in the `<…>` parts, and paste it as the **first message of a new session**.
Choose the model when you start the session: `claude --model opus` or `claude --model sonnet`,
or `/model` inside a session. `CLAUDE.md` loads automatically, so the prompts don't repeat it.

---

## 1. Phase planning (Opus, once per phase)

Use this when the phase `STATUS.md` says *seeded*. For a phase that has no directory yet,
change the first task to "create `plan/phase-NN-<slug>/` from `plan/templates/` and seed its
`PLAN.md` from `upstream_inventory.md`".

```
Plan phase <NN> (<title>) of AS. Don't implement anything.

Read, in this order: plan/README.md, plan/PLAN.md, plan/phase-<NN>-<slug>/PLAN.md and STATUS.md,
then only the source sections that PLAN.md lists under "Sources" (use offset/limit).

Then:
1. Check the current code against the sources. Line numbers date from 2026-10-13, and earlier
   steps have changed some files. Delegate the reading of sibling code to an Explore subagent
   and ask it for current paths, line ranges, signatures, test names and counts, not file dumps.
2. Settle each item under "Open questions for planning". Ask me about anything that is a real
   decision rather than a fact you can check.
3. Write one brief per step from plan/templates/STEP.md. A Sonnet session must be able to
   implement it without exploring the code base: copy in the signatures, the exact behaviour,
   the tests, numeric acceptance criteria, and the error analysis to do. Keep each brief under
   ~250 lines.
   Each brief says which findings and error analysis go into the phase report.
4. Update the phase PLAN.md (steps table, decisions, hazards), overwrite STATUS.md, and append a
   planning block to PROGRESS.md.
5. Create reports/phase-<NN>-<slug>.md from plan/templates/REPORT.md (status draft), with
   empty step sections in step order.

<Optional: phase-specific instructions, e.g. "write briefs for steps 7 and 8 only".>
```

## 2. Step implementation (Sonnet, once per step)

```
Implement step <SS> of phase <NN>.

Read plan/README.md (only the "Rules for every session" section), then
plan/phase-<NN>-<slug>/STATUS.md, then plan/phase-<NN>-<slug>/step-<SS>-<slug>.md. Follow the
brief. Don't read the phase PLAN.md or docs/shared_library/upstream_inventory.md. If the brief
lacks something you need, find it, and record it under "Brief defects".

Stop and ask me if an acceptance number doesn't reproduce, an input is missing, or the brief
looks wrong. Don't work around it.

When done, write your section of reports/phase-<NN>-<slug>.md (results, error analysis,
draft findings; see plan/templates/REPORT.md), unless the brief says the step has none, and run
`make reports`. Then append the
PROGRESS.md block from the brief and overwrite STATUS.md.

<Optional: "Do only the AS side; leave the sibling switch for later.">
```

Review fixes use the same prompt, with `step-<SS>r-review-fixes.md` as the brief.

## 3. Phase review (Opus, once per phase)

```
Review phase <NN> (<title>) of AS. All its steps are marked done in
plan/phase-<NN>-<slug>/STATUS.md.

Read plan/README.md, then the phase PLAN.md, STATUS.md and PROGRESS.md, then
reports/phase-<NN>-<slug>.md, then the code as it is now (git diff and the files the PROGRESS
blocks list). Review the code, not the log.

1. Work through the PLAN.md review checklist with evidence. Re-run every acceptance check and
   both test suites (AS and the sibling) yourself.
2. Do your own error analysis. Read individual changed outputs and failures, and check whether
   each step's error analysis in the report holds up. Check for leakage where metrics or
   splits are involved.
3. Check docs/shared_library/changelog.md against the actual API changes.
4. Write plan/phase-<NN>-<slug>/REVIEW.md from plan/templates/REVIEW.md.
5. Finalise the phase report:
   - write its Summary, Review (your own error analysis, findings F<NN>-R.n) and Follow-ups;
   - set every finding to confirmed, revised or withdrawn;
   - set the status to final;
   - append the phase's findings to reports/index.md with Edit;
   - run `make reports`.

   List findings that would help someone reusing AS as candidates for docs/findings.md, but
   don't edit that file unless I say so.
6. If there are defects, write step-<SS>r-review-fixes.md for a Sonnet session. Don't fix them
   yourself unless a fix is a one-liner.
7. Fill in the PLAN.md hand-off, update the row in plan/PLAN.md, overwrite STATUS.md, and tell
   me what to release (the version and its contents) and which phase to plan next.

<Optional: "Pay particular attention to …".>
```

## 4. Status check (Haiku or Sonnet, any time)

```
Read plan/PLAN.md and the STATUS.md of every phase that is not done. (For results rather than
status, read reports/index.md instead.) Tell me, in under 15 lines:
what is in progress, what is blocked and why, which questions are waiting for me, and the next
session to run (with the prompt number from plan/PROMPTS.md). Don't read anything else.
```
