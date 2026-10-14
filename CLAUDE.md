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

## Token discipline

- Never read large data files into context. Use pandas with explicit `usecols` and print
  `head()`/shape only.
- Prefer targeted `grep` and `Read` with `offset`/`limit` on long files.
- Delegate broad multi-file searches to a subagent so only the conclusion returns.
