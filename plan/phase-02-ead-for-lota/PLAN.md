# Phase 02: EAD for LotA

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> not read this file. Seeded 2026-10-14 from `docs/shared_library/upstream_inventory.md`;
> planned 2026-10-14 (Opus, against the code as of that day).

## Goal

LotA takes its generic EAD logic from AS: node ids (C2), digitisation evidence (C5), the
scope-note parser with cross-archive references (C3), and structural `<unitdate>` excision from
titles (C4). LotA's copies are deleted, and its regenerated outputs differ from the
post-step-2 baseline only in the ways each step predicts.

## Owns

- **AS:**
  - `parsers/ead_digital.py`;
  - `parsers/ead_scope_notes.py` (new);
  - `parsers/ead_fields.py`;
  - `parsers/ead_views.py` (only the `archive_id` argument, step 03b);
  - `parsers/ead_text.py` (only a private marker hook, step 06);
  - the tests for all of these;
  - until step 03d is done: the `reports`/`reports-open` Makefile targets, the
    `reports/_build/` line in `.gitignore`, `reports/_tools/` and `reports/index.md`. After
    03d these are shared, and `reports/index.md` is append-only;
  - `reports/phase-02-ead-for-lota.md` and `reports/data/phase-02-ead-for-lota/`.
- **LotA:**
  - `archival_language/ead/`: `hierarchy.py`, `digitisation.py`, `scopecontent.py`,
    `scopecontent_survey.py`, `__init__.py`;
  - `model/node.py`, `corpus/coverage.py`;
  - `facets/title_norm.py`;
  - `cli/parse_scopecontent.py`, `cli/normalise_titles.py`, `cli/diff_outputs.py` (new);
  - their tests, plus `tests/test_title_vocab.py` (one keyword);
  - `output/scope-notes/`, `output/titles/{*-normalised-titles.tsv.gz, normalisation-census.tsv, token-kinds-by-level.tsv}`
    and `output/baselines/` (new).

Phase 03 owns `stream_analysis/` and NLP; there is no overlap.

## Sources

The planning session read these sections, and no others.

- `upstream_inventory.md`:
  - C2 at L221–231;
  - C3 at L233–244;
  - C4 at L246–256;
  - C5 at L258–268;
  - §4 steps 3–6 at L447–450;
  - §6 decisions D6 and D7 at L518–528.
- `changelog.md` steps 2 and 3 (L9–51).
- LotA `CLAUDE.md`, and LotA `STATUS.md` L1294–1323 (the step-2 entry).
- The current code: AS read directly; LotA through two Explore subagents.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 03b | LotA switches to AS node ids | — | `frame()` rows/ids identical on all 9 LotA archives; AS 582+2, LotA 320 green | [step-03b](step-03b-lota-node-ids.md) |
| 03d | `reports/` directory and `make reports` (user, 2026-10-14) | — | `make reports` builds with no warnings; links rewritten; phase-02 report holds planning findings F02-P.1–6 and the 03b section | [step-03d](step-03d-reports-tooling.md) |
| 03c | LotA output baselines after AS step 2 | 03d | pre copies kept; post baselines regenerated; titles diff reproduces 23/12/11/2/0; every scope-note text change explained | [step-03c](step-03c-lota-output-baselines.md) |
| 04 | C5 `DigitisationSummary.has_evidence` | — | +5 AS tests; LotA digitisation/coverage frames `equals` before/after | [step-04](step-04-digitisation-has-evidence.md) |
| 05 | C3 scope-note parser into `parsers/ead_scope_notes.py`, with `target_archive` | 03b, 03c | +16 AS tests; scope-notes diff vs post baseline only in `references`/`n_references` on cross-archive rows; Aanwinsten count reported | [step-05](step-05-scope-notes.md) |
| 06 | C4 `split_unittitle_dates`; retire `lb_adjusted` | 03c | +11 AS tests; titles diff vs post baseline: 0 changed cells, only `lb_adjusted` dropped, all 9 archives | [step-06](step-06-unittitle-dates.md) |

## Why this order

- **03b** finishes inventory step 3, whose LotA side was left undone. Step 05's `ScopeNote.node_id`
  comes from `EADComponent.node_id`, which equals LotA's id only through 03b.
- **03d** was added after planning, at the user's request (2026-10-14). Every later step writes
  its section of the phase report and runs `make reports`, so it runs next, before 03c. It
  backfills 03b's report section from `PROGRESS.md`.
- **03c** exists because the step-2 regeneration never happened (hazard 1). It changes no code,
  and can run in parallel with 04.
- **04** is the smallest AS change, and a good first test of the brief format on Sonnet.
- **05 and 06** touch different LotA modules and could run in either order. 05 comes first
  because D7 fixes a wrong join that is live today.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| D6 | Node id `archive:1.2.3`; `EADComponent` compares and hashes by identity | Join on `node_id`, never on `id()` or `citation` | Done in step 3 |
| D7 | Cross-archive scope-note references carry the target archive code | `ScopeReference.target_archive: str \| None = None`, last field; a reference into another archive must never join to the current archive's inventory | The VOC "Aanwinsten, 1.11.01.01, inv.nr. 687" notes join to VOC 687 today |
| P1 | LotA's `lb_adjusted` is retired (user, 2026-10-14) | Step 06 removes the field, the column, the census column `n_lb_adjusted` and `test_lb_adjusted_flag` | Always False after C1 |
| P2 | `is_digitised` keeps its handle-inclusive meaning | Add `has_evidence`; its docstring says a handle is not evidence | Backwards compatibility |
| P3 | NA rules are a named default, not hardcoded | `NA_SHAPE_RULES`, `NA_REFERENCE_PATTERNS` (`ReferencePatterns` dataclass) passed as default parameters | AS is generic |
| P4 | Baselines (user, 2026-10-14) | Step 03c: `output/baselines/pre-as-step-2/` (copies) and `post-as-step-2/` (regenerated); canonical outputs are only overwritten by 05/06 | LotA has no git; the old files are the only record |
| P5 | TSV serialisation of `target_archive` (user, 2026-10-14) | `kind:raw:inv:folio` plus `:<target>` **only when set** | All other references stay byte-identical |
| P6 | `target_archive` is None when no code is written **or** the code equals the current `archive_id` | in `_inventory_references` | "None = this archive" is the join rule |
| P7 | No per-repository default map in `ead_survey.py` (planning) | Step 04 does not build it | Mechanisms are per-archive config in LotA; no second consumer yet |
| P8 | `classify_shape`, `ShapeRule` and `descendant_tags` move to AS with the parser; the LotA survey re-imports them and keeps a `SHAPE_RULES` alias | step 05 | the parser needs them; the inventory says so |
| P9 | AS `ead_views` frames take an optional `archive_id` | `None` = `default_archive_id` | LotA archive ids need not equal `<eadid>` |
| P10 | `split_unittitle_dates(el) -> (prose, spans)`, one span or `None` per **direct** `<unitdate>` child, indexing `element_text(el)` | LotA's regex date fallback is deleted as unreachable | Matches LotA's direct-children rule |
| P11 | A written report per phase (user, 2026-10-14) | Each step writes its section of `reports/phase-02-ead-for-lota.md` (template `plan/templates/REPORT.md`) with draft findings `F02-<step>.n`. The AS report replaces the LotA `report/` file that 03c used to ask for. `make reports` converts it with pandoc | Findings easy to look up; `plan/` is process, `reports/` is results |

## Hazards

1. **Baselines.** Resolved by 03c (P4). Steps 05 and 06 diff against `post-as-step-2/`, never
   against the canonical files from before the step.
2. Inventory line numbers are stale. The briefs carry line numbers re-checked on 2026-10-14;
   by step 05/06, 03b may have shifted lines in `hierarchy.py` only.
3. C3: LotA's per-node `parse_scope_notes(node)` stays as a wrapper. It overrides `citation` with
   `dataclasses.replace`, because `ArchiveNode` does not carry the EAD path.
4. C3: in planning, the old inventory regex was found to also swallow an archive code as an
   inventory token (`inv.nr. 12, 1.11.01.01, inv.nr. 687` → `12, 1.11.01.01`). The new regex
   stops the list there, so `n_references` can drop on such rows. The 83 count needs
   re-measuring. Notes without an `inv.nr.` cue (`zie Aanwinsten, 1.11.01.01 (1893 29r)`) stay
   unextracted; step 05 counts them.
5. C3: `_segments` maps `<lb/>` to a space without the punctuation rule. It is ported verbatim
   (outputs must not change); a follow-up for the review.
6. C4: lxml rejects control characters in element text. The span markers therefore live in a
   private `ead_text` walk, not in a copied tree (prototype checked: invariant holds on 22,197 SG
   + 14,505 VOC titles).
7. AS real-data tests use AS's own `data/metadata/eads/` copies, which may differ from LotA's
   `eads/`. Never assert LotA counts in AS tests.
8. LotA `ArchiveNode`'s docstring claims equality on `node_id` alone; it actually compares all
   fields except `component`. 03b fixes the docstring, not the behaviour.
9. LotA `tests/test_derive_voc.py` fails to import (`fuzzy_search.similarity`). It is unrelated;
   every step runs LotA tests with `--ignore` on it.
10. Step 06 regenerates only `normalise_titles` outputs. The title_vocab and gold files in
    `output/titles/` are not regenerated; they do not read `lb_adjusted`.

## Settled at planning (2026-10-14)

- **Outputs regenerated after step 2?** No. Scope notes date from 2026-10-03 and titles from
  2026-10-08; LotA `STATUS.md` says they were not regenerated. This became step 03c.
- **Per-repository default map (step 04)?** No (P7).
- **`classify_shape` moves with the parser?** Yes (P8).
- **Do `hierarchy.py`/`model/node.py` match changelog L49–51?** Yes. `_node_id` at L38–39,
  `id()` keys at L58/72/100/103/113, citation join at L136–138. `model/node.py` builds the
  `parent_id`/`ancestor_ids` strings by hand (L51–68).
- **Commands and timing.**
  - `venv/bin/python -m archival_language.cli.parse_scopecontent [--out DIR]` (SG+VOC, seconds).
  - `venv/bin/python -m archival_language.cli.normalise_titles [--out DIR]` (9 archives,
    seconds to tens of seconds).
  - Measured baselines: AS suite 582 passed in 97 s; LotA `test_hierarchy` +
    `test_digitisation` 22 passed in 23 s; LotA full suite 320 (per LotA `STATUS.md`).

## Review checklist

All items pass; the evidence is in [`REVIEW.md`](REVIEW.md) (2026-10-14).

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, re-run by the reviewer.
- [x] AS suite (`.venv/bin/python -m pytest -q -p no:cacheprovider`) and LotA suite green.
- [x] Output diffs (03c, 05, 06) read row by row by the reviewer for at least 30 changed rows
      each, including all cross-archive notes up to 30. 03c has 26 changed rows in all, all
      read. 06 has 0 changed cells, so 36 cuts were read instead.
- [x] `changelog.md` has steps 3b, 4, 5, 6, with LotA imports listed.
- [x] LotA copies deleted: `_node_id`, both `_has_evidence`, `scopecontent.py` internals,
      `_structural_date_spans`/`_structural_text_and_prose`, `lb_adjusted`.
- [x] No NA-specific regex hardcoded in AS core (P3).
- [x] `is_digitised` semantics unchanged (P2).
- [x] `output/baselines/README.md` exists, and both baselines are intact.
- [x] `reports/phase-02-ead-for-lota.md`: one section per step (03b, 03c, 04, 05, 06) with an
      error analysis that read individual cases; every finding `confirmed`, `revised` or
      `withdrawn`; *Summary*, *Review* and *Follow-ups* written; status *final*.
- [x] Phase findings appended to `reports/index.md`; `make reports` clean.
- [x] `Brief defects` entries answered.

## Hand-off

Reviewed 2026-10-14 (Opus): **accept with follow-ups**, no review-fixes step.

- **For the siblings.** LotA imports these from AS:
  - `ead_views` frames with `archive_id`;
  - `ead_digital.DigitisationSummary.has_evidence`;
  - `parsers.ead_scope_notes` (`ScopeNote`, `ScopeReference.target_archive`, `NA_SHAPE_RULES`,
    `classify_shape`, …);
  - `ead_fields.split_unittitle_dates`.

  It needs AS **v0.6.0** (the user's next release). Before the release, LotA runs on the
  editable AS working tree. NLP is untouched by this phase.
- **Outputs.** LotA `output/scope-notes/` and the 9 `output/titles/*-normalised-titles.tsv.gz`
  files, plus the census and token kinds, are current with the code (F02-R.1). The baselines
  stay in `output/baselines/`.
- **Deferred.** These are listed in the report's *Follow-ups* and `REVIEW.md` debts 1–4:
  - P6 under prefixed archive ids (F02-R.6);
  - `assert` invariants in `split_unittitle_dates`;
  - orphaned separators in title `prose` (F02-R.4, a user decision);
  - the dead `date` token kind and `n_tokens_date`;
  - BHIC's empty frame and cue-less Aanwinsten notes.
- **Protocol changes from this review.** These are `plan/README.md` rule 10 (propagate
  mid-phase changes into briefs not yet run), the `STEP.md` *Inputs* and *Tests* lines, and
  `PROMPTS.md` prompt 2 (no report section when the brief says none).
