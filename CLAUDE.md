# Project rules: archival-structures (AS)

Identifying and operationalising hierarchical and referential structure in digitised archives
(proposal: `literature/OEC-2026-Administrative_Devices-Full_Proposal.pdf`). Since 2026-10-13 AS
has two extra roles:

- **the shared library** that the sibling projects depend on. The siblings are
  `/Users/Shared/claude_projects/data_scopes/language-of-the-archive` (LotA) and
  `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (NLP). Generic
  evaluation, splits, decoders, EAD parsing, text-layer and quality tooling live here, and the
  siblings keep only archive-specific research code. See `docs/shared_library/`;
- **the central ground-truth store** under `ground_truth/` (see its `README.md`), which the
  siblings read from.

This file is loaded automatically. Do not restate it back to the user.

## Repo hygiene

- **Never `git commit`.** The user reviews and stages everything themselves.
- Releases are made by the user with `make bump-patch|bump-minor|bump-major` and
  `make release[-push|-github]` (`scripts/release.py`). Sessions may run `make version` and
  `make check-tags`, never the release targets.
- `chmod g+w` every file and directory you create; the user needs write access to them too.
- Check write access to a directory **before** running code that writes there. If it is
  missing, ask the user to update permissions. Do not use a workaround.
- **Never access external drives**, not even with read-only commands, and keep their paths out
  of subagent prompts. If an input is not available locally, say exactly which files to stage
  and stop.
- Source data shared between projects lives in /Users/Shared/claude_projects/data_scopes/shared-data/ 
  (rules in its README); stage it by symlinking it under data/.

## Environment

- Use `.venv/bin/python` (also what `docs/Makefile` uses). The older `venv/` is stale:
  pagexml-tools 0.7.0 and no scikit-learn, so several test modules fail to import there.

## Shared library

- The siblings import from `archival_structures`. A change to a public function's signature or
  behaviour must either stay backwards compatible or be listed, with the affected sibling
  imports, in `docs/shared_library/changelog.md`.
- Upstreamed code comes with its tests, and the tests pass here before the sibling switches
  its import.

## Ground truth (`ground_truth/`)

- Layout: `ground_truth/<archive-id>/<dataset>/source/` holds the dataset exactly as received,
  never edited. `converted/` holds the `InventoryGroundTruth` YAMLs derived from it.
- Every dataset has a row in `ground_truth/README.md`: source and version/DOI, date staged,
  provenance method, coverage (`complete`/`partial`), and which projects use it.
- Converted layers carry a `provenance` mapping (see `docs/ground_truth_format.md`). A
  `partial` layer supports recall measurement, not precision.
- The archive-institution prefix is always hyphenated: `NL-HaNA`, `NL-AsdSAA`, `NL-AsnDA`,
  never `NL_HaNA`.

## Standing methodology

- **Every avenue includes error analysis**: read individual failures and successes, not just
  aggregate metrics.
- **A negative result is a result.** Do not tune a feature or threshold until it looks
  significant.
- Before believing a good number, check for leakage. These checks come from NLP's
  `CLAUDE.md`:
  - models fit on the corpus whose training split does not nest in the evaluation split;
  - features computed from ground truth, such as distance to the previous start;
  - normalisation statistics fitted using labels.
- **Opus plans and reviews, Sonnet implements.** Work runs as phases of steps under `plan/`
  (protocol in `plan/README.md`, prompts in `plan/PROMPTS.md`). Each step and each review is a
  new session. Phase planning and phase review use Opus. Implementation steps use Sonnet, so
  every step brief must be complete enough to implement without exploring the code base. This
  includes subagents: an implementing subagent gets `model: "sonnet"` and a self-contained
  prompt.
- "Section" is ambiguous across levels. Always qualify it (e.g. *volume section*,
  *within-document section*) or name its type; see the glossary in `docs/definitions/`.
- Check pages for the user: build them with the shared module
  `/Users/Shared/claude_projects/data_scopes/shared-tools/checkpage.py`
  (scan IDs as Nationaal Archief viewer links, per-value copy buttons, page-unique
  localStorage key). Its README section "checkpage" is the checklist.
  
## When something is left for the user

Never leave a user action as prose in the chat ("you may want to…", "if it
matters…", "worth checking…"). Every item that needs the user — a decision,
a command, a file edit, a confirmation — takes one of three forms:

- **Run** — a copy-pasteable command, with the directory it runs in.
- **Edit** — the file path, the location (section heading or line), and the
  exact text to add or replace.
- **Decide** — an entry in `docs/USER_ACTIONS.md` (format below); give its
  ID in the chat.

Each item also says **what happens if the user does nothing** and **what it
blocks** ("nothing" is a valid answer). End any message that leaves items
open with a short **For you** list of the items and their IDs.

### `docs/USER_ACTIONS.md`

The user answers here, not in the chat. One entry per item:

    ### UA-NNN <short title> (YYYY-MM-DD, from <step or session>)
    - **Action:** what to do, in Run / Edit / Decide form
    - **Options:** (a) … (b) … — recommended: …
    - **Default if unanswered:** …
    - **Blocks:** …
    - **Response:**

- At the start of a session, run `grep -n -A1 '^### UA-\|Response:'
  docs/USER_ACTIONS.md` rather than reading the whole file.
- Act on every item whose **Response** is filled in and touches your task.
  Record the outcome where it belongs (plan, step file, `PROGRESS.md`). Then
  move the entry to `docs/USER_ACTIONS_resolved.md` with the date and what
  was done.
- Never fill in a **Response** yourself. Number IDs sequentially and never
  reuse one.
- Keep `docs/USER_ACTIONS.md` to open items only, so it stays cheap to read.

## Compute

- Local: MPS or CPU. Select the device at runtime; never hardcode `cuda`.
  Parallelise batch jobs, but all projects share one budget of 8 workers. Check
  `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`
  first, and launch every job with more than one worker through
  `coreshare.py run --project <project> --label <step> --want N --min M --wait -- <cmd --workers {workers}>`.
  Use the granted count; never hardcode a worker count. Subagent briefs say the same.
- **GPU work runs on the user's external VMs (2× A10, 22 CPU cores), which
  the user launches themselves.** Never request or assume SSH access.
  VMs are reserved for long/overnight jobs.
- Therefore every GPU script must be **handoff-ready**: `argparse` with
  explicit paths, a docstring listing exactly which files to copy over,
  resumable from checkpoints, progress logged to a file, no interactive
  prompts, and a `--limit N` smoke-test mode that runs locally first.
- Batch independent GPU steps into one overnight run.
- Optimise code to use both A10 GPUs (CUDA:0 and CUDA:1) where it speeds 
  things up.

## Token discipline

- Never read large data files into context. Use pandas with explicit `usecols` and print
  `head()`/shape only.
- Prefer targeted `grep` and `Read` with `offset`/`limit` on long files.
- Delegate broad multi-file searches to a subagent so only the conclusion returns.
