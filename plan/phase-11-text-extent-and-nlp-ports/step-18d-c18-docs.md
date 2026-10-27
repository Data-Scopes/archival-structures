# Step 18d: C18 docs, leakage and splits, granularity hypothesis

> **Phase:** 11, Text extent options and NLP ports. **Depends on:** 18a, 18b, 18c (done).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Inventory item C18 lists review items that are documentation, not code: NLP's leakage and split rules as AS standing
methodology in `docs/challenges/`, and the *granularity rule* from the cross-project review. The user decided
(2026-10-13) that the granularity rule is **a hypothesis to test**, not a finding, and that **Framing B (pairwise) is
the default** framing. Steps 18a–18c added `text_extent` options, NLP's DP/TextTiling segmenters and the
separator-sheet rule; this step documents them where a reader looks. Docs only: no code changes.

## Inputs

- **Read first:** the phase report's sections *Step 18a*, *Step 18b*, *Step 18c* in
  `reports/phase-11-text-extent-and-nlp-ports.md` (their findings and the exact API names), and the *Public API* lines
  of their blocks in the phase `PROGRESS.md`. Use the names as built; if they differ from this brief, the code wins.
- NLP's leakage rules (NLP `CLAUDE.md` L116–132, verbatim):
  1. Any model fit on this corpus (continuation classifier, cross-encoder, metric-learned encoder) whose own training
     split does not nest inside the downstream split. This is exactly how report/20's F1 0.822 evaporated to 0.635.
     Assert the nesting in code.
  2. Features computed over physically adjacent pages at a chunk edge — a known, bounded residual of the chunk splits.
     Keep it bounded.
  3. Any "pages since last start" / "distance to previous start" feature computed from ground truth. Label leakage.
     The legitimate versions are positional (page index in volume) or derived from an unsupervised segmentation.
  4. Normalisation statistics fitted across train+test *using labels*. Per-inventory centring is fine (it uses no
     labels); anything label-dependent is not.
- NLP's split protocol (NLP `CLAUDE.md` L33–62, summarised): the primary split is document-atomic long chunks
  (`long_chunk`) evaluated on `dev`; the secondary is `inventory_held_out`; `test` is touched at most once per phase
  and never to choose variants; at least 3 seeds plus a bootstrap CI over inventories; trivial baselines as the floor
  (the majority baseline beat several real models on Pk/WindowDiff); with AS's metrics NLP passes
  `k_convention="pages"`.
- AS APIs that implement these (exist, checked 2026-10-27), all in `archival_structures.stream_analysis.evaluation`:
  `splits.check_nested_split(component_split, downstream_split, *, component_train=("train",),
  downstream_train=("train",))` ("Leakage rule 1"), `splits.check_no_document_crosses_partitions`,
  `splits.check_context_not_scored`, `splits.check_no_duplicate_page_ids`, `splits.check_split_coverage`,
  `splits.build_long_sequence_split`, `splits.build_inventory_held_out`; `aggregate.evaluate_all`,
  `aggregate.bootstrap_ci`, `aggregate.per_inventory`, `aggregate.start_rate_calibration`;
  `baselines.baseline_suite`, `baselines.baseline_floors`, `baselines.majority`; `segmentation.default_k(true_labels,
  convention="gaps")` (`"pages"` = NLP). AS `CLAUDE.md` repeats three leakage checks (rules 1, 3, 4) as standing
  methodology.
- The granularity rule (`docs/cross-project-review.md` §1 item 3, L81–91): NLP's negative results (DINOv2, ink density,
  gap geometry hurt or failed for document starts; DP and TextTiling score below the majority class) against AS's
  DINOv2 success at section and sub-sequence boundaries suggest "vision and layout find section-level units, and text
  finds document-level ones". User decision 9 (§0 L41–44): a hypothesis to test; candidates NL-AsdSAA 5075 (some
  deed-level ground truth), NL-HaNA 2.10.50 and 2.10.01; NL-AsnDA is hard (low image quality, hard ground truth).
  Decision 10 (L45–48): Framing B default; which framing suits which archive is itself a research question; the user's
  hunch is that early modern archives have sub-page starts and modern ones top-of-page starts.
- Evidence from this project for the hypothesis section (cite, don't re-measure): NLP report/84 via AS
  (`docs/from-nlp-start-page-detection.md` §1): `early_end` lift 2.1 but recall 0.315, about half of VOC starts are
  mid-page; step 18a's reproduction and findings; step 18b's DP/TextTiling reading. Use "section" only qualified
  (*volume section*, *within-document section*), as `CLAUDE.md` requires; the cross-project "section" here is a
  *volume section* (a run of pages coarser than a document, e.g. an SG session block).
- Docs files (Sphinx with MyST; `make docs` is currently broken for an unrelated reason, UA-033, so you can't build):
  - `docs/challenges/index.md` (37 lines): catalog table L19–23 (`| Challenge | Status | Page |`) and a hidden
    `{toctree}` L28–37 listing page stems;
  - `docs/challenges/document_segmentation.md` (126 lines): `### Framing A is the default assumption elsewhere in this
    doc` L71–78 (wording predates decision 10), `## Why the ordering assumption matters` L108, `## Open questions` L116;
  - `docs/stream_analysis.md`: `### Subsequence detection (…overview.subsequence_detection)` L81–101, a bullet list of
    functions;
  - `docs/findings.md`: **don't edit** (the phase review lists candidates; the user decides).

## Deliverables

| File | Change |
|---|---|
| `docs/challenges/leakage_and_splits.md` | new page (below) |
| `docs/challenges/index.md` | catalog row (status *Methodology*) and toctree entry `leakage_and_splits` |
| `docs/challenges/document_segmentation.md` | new section `## The granularity hypothesis`; the Framing A heading/paragraph reworded to match decision 10; a link to the new page |
| `docs/stream_analysis.md` | bullets for the 18b functions and converters in *Subsequence detection*, with the start-index convention and the section-level caveat |
| `reports/phase-11-text-extent-and-nlp-ports.md` | fill `### Step 18d: …` (what changed; no metrics; findings only if the writing exposed a contradiction) |

No changelog entry (docs only).

## Behaviour to implement

**`leakage_and_splits.md`** (~80–120 lines), sections:
1. *Why*: a good number is checked for leakage before it is believed (one paragraph; NLP's 0.822 → 0.635 example).
2. *Leakage channels*: the four rules, generalised from VOC to any page stream, each with "how to check" naming the AS
   API where one exists (`check_nested_split` for 1; context pages not scored for 2; for 3 the legitimate alternatives,
   incl. a start list from `segment_dp` (step 18b) as an unsupervised source, with the warning that its two
   constants were chosen from corpus label statistics, so re-derive them from train rows before a headline number; for 4
   per-inventory, label-free statistics such as `text_extent`'s stable margins). Add a fifth, AS-specific line from step
   18a if the report supports it: thresholds or region types chosen after reading the errors are post hoc (NLP's body
   variant was) and need a fresh test.
3. *Splits and protocol*: document-atomic chunks, inventory-held-out, test discipline, seeds and bootstrap over
   inventories, trivial-baseline floors, and the `k` convention, each with its AS function.
4. *Granularity of the target*: one paragraph pointing to the hypothesis section in `document_segmentation.md` (a
   method validated on volume sections is not evidence for documents).
Write in plain, short sentences; no marketing words; MyST-compatible Markdown (no raw HTML). Cross-link with relative
links (`document_segmentation.md#the-granularity-hypothesis`, `../stream_analysis.md`).

**`## The granularity hypothesis`** in `document_segmentation.md` (~25–40 lines): the statement; status
"hypothesis, not a finding (user decision, 2026-10-13)"; the evidence for it and its limits (VOC letter bundles only,
labels at document level only for VOC); what would test it (the candidate archives); what in AS applies to which level
today (DINOv2 subsequence detection and `segment_dp`/TextTiling: volume sections; text-based start classifiers:
documents; `text_extent` `early_end`: a weak document-end signal). Then reword the Framing A heading/paragraph so it no
longer calls Framing A *the default*: say the later sections use Framing A's vocabulary for convenience, while Framing B
is the project default (decision 10).

**`stream_analysis.md`**: add bullets for `segment_dp`, `textiling_depth_scores`, `calibrate_length_penalty_weight`,
`predicted_start_rate`, `starts_to_boundaries`, `boundaries_to_starts`: one line each; one sentence that they return
start indices (incl. 0), unlike `detect_boundaries_*`; one sentence that NLP found both below the majority baseline for
VOC document starts (use for volume sections), linking the new hypothesis section.

## Constraints

- Docs only; no code, test or changelog edits; don't edit `docs/findings.md` or other steps' report sections.
- Every API name written must exist: check each with one `.venv/bin/python -c "from <module> import <name>"` command
  and quote the command in your report section.
- Never `git commit`; `chmod g+w` the new file.

## Tests

None (docs). Check: the import command above exits 0; `grep -n leakage_and_splits docs/challenges/index.md` shows the
row and the toctree line; `make reports` is clean.

## Acceptance criteria

1. The new page exists with the four sections; the index has its row and toctree entry.
2. The hypothesis section exists and states the status as a hypothesis; the Framing A paragraph no longer contradicts
   decision 10.
3. Every AS name in the three edited/new docs imports (one command, exit 0).
4. `make reports` clean.

## Error analysis

Not applicable as metrics. Instead, read the three edited pages once end to end and list in the report any statement
in AS docs you found that now contradicts the hypothesis status or Framing B default (grep `docs/` for
"granularity rule" and "Framing A"); fix those in the files you own, list others as findings `F11-18d.n` (open-issue).

## Do not touch

- Code, tests, `docs/findings.md`, `docs/shared_library/`, NLP and LotA.

## When done

1. Fill your `### Step 18d: …` section in `reports/phase-11-text-extent-and-nlp-ports.md` and run `make reports`.
2. Replace the line `<!-- 18d block -->` in the phase `PROGRESS.md` with the block below.
3. Overwrite the phase `STATUS.md` (all four steps' rows kept; state *in review* next; next action: phase review).

```markdown
### Step 18d: C18 docs, leakage and splits, granularity hypothesis
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Checks:** <import command> → <result>; `make reports` → <result>
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18d](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18d-c18-docs-leakage-and-splits-granularity-hypothesis)
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
