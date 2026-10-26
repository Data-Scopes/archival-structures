# Step 15p: Hand/print census of SG 1.01.02

> **Phase:** 10, Hand and print per region (text layer v4). **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file. **Read nothing else unless this brief names it.** In particular, don't read the phase `PLAN.md`; what you
> need from it is copied in here. If you have to explore the code base anyway, record it under *Brief defects*.

## Context

The SG text layer treats archive 1.01.02 as handwritten and 1.10.94 as printed. That is wrong: 1.01.02 holds printed
volumes (3771–3773 and 3821–3823 in the pilot are printed resolutions), and the user expects "any mixture at any
level": runs of printed pages among handwritten ones, printed forms with handwritten slots, and things nobody has
catalogued. This step makes **no code change**. It measures one feature, text density per PageXML region, over all
9,122 inventories, from local TSVs, and answers: is the archive separable into hand and print by density, how much
is printed, which volumes are mixed, and what fits neither class. The rule is fixed below **before** you look at
the corpus; you do not tune it.

## Inputs

- **Paragraph TSVs** (read only): `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/paragraphs/source/paragraphs-NL-HaNA_1.01.02_<inv>.tsv.gz`,
  9,122 files, 2.1 GB, 20.6 M rows. Read with
  `pd.read_csv(f, sep="\t", quoting=3, dtype=str, keep_default_na=False, usecols=["scan_id", "doc_id", "text"])`.
  One row = one PageXML text region. `doc_id` = `<scan_id>-text_region-<x>-<y>-<w>-<h>` (the box; checked
  against the `15m-a` `regions_meta.region_id`, identical). `text` has **no line breaks**. **Never use the
  `region_type` column** (audit D1: wrong on two-column pages). **Never use `inv_num`**: 20 files hold rows of a
  suffixed inventory under the bare number (audit D3); take the inventory from `scan_id`:
  `scan_id[len("NL-HaNA_1.01.02_"):].rsplit("_", 1)[0]`.
- **EAD** for silver labels, tested at planning (1.3 s):
  ```python
  from pathlib import Path
  from archival_structures.parsers.ead_tree import parse_ead
  from archival_structures.parsers.ead_views import to_frame
  df = to_frame(parse_ead(Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/ead/source/NL-HaNA_1.01.02.xml")), "file")
  anc = [c for c in df.columns if c.startswith("ancestor_")]          # ancestor_1 .. ancestor_7
  path = df[anc].fillna("").agg(" > ".join, axis=1)                  # columns: inventory_number, title, ...
  ```
  Silver groups (counts measured at planning; "with TSV" = a TSV file exists):
  `print`: path contains `Gedrukte registers van de ordinaris` (105, 3760–3864), `Gedrukte nouvelles` (78; 64 with
  TSV), `Gedrukte plakkaten` (76); `hand`: path contains `Registers van ordinaris net-resoluties` (562; 550 with
  TSV, 3096–3681). `Geschreven indices` (3865–3868) are handwritten indices to the printed resolutions: count them
  as `hand`. Everything else: no silver label.
- **Dev inventories** (thresholds are fitted on these only): hand 3349, 3350, 3351, 3352, 3353, 3354, 11182, 11281;
  print 3771, 3772, 3773, 3821, 3822, 3823. Planning measured (TSV, regions with ≥ 20 chars): hand 45,971 regions,
  density percentiles 1/5/25/50/75/95/99 = 0.07/0.12/0.20/0.25/0.30/0.44/0.56; print 47,034 regions,
  0.30/0.42/0.63/0.76/0.87/0.93/0.98. Per volume, hand medians 0.19–0.33 (3350: 5.4% of regions > 0.55), print
  0.75–0.78.
- **Dev layer** (resolution check only): `data/text_layer_v2_dev/15m-a/hand/NL-HaNA_1.01.02_text_layer_v3/scans/<inv>.scans.tsv.gz`
  (columns include `scan_id, page_w, page_h, n_lines_text`) and `…/15m-a/print/NL-HaNA_1.10.94_text_layer_v3/scans/45{5,6}.scans.tsv.gz`.

## The rule (fixed; phase decision P3)

- Region: `n_chars = len(text)`, `density = 1000 * n_chars / (w * h)`. Regions with `n_chars < 20` are `short`
  (no label; they count nowhere).
- `t_hand` = 95th percentile of the hand dev regions' density, `t_print` = 5th percentile of the print dev regions'
  (both over regions with ≥ 20 chars, `numpy.percentile`, default interpolation). If `t_hand > t_print`, swap them, so
  that `lo = min`, `hi = max`. Region label: `hand` if density ≤ `lo`, `print` if ≥ `hi`, else `uncertain`. Planning
  expects `lo` ≈ 0.42 and `hi` ≈ 0.44 (a narrow overlap band; **re-measure**, don't copy).
- Scan (the TSV has no page side, so the census works per scan): `print_share` = print chars / (print + hand chars)
  over labelled regions. `print` if ≥ 0.9, `hand` if ≤ 0.1, else `mixed`; fewer than 200 labelled chars → `uncertain`.
- Volume (inventory): the same 0.9 / 0.1 rule over its `hand` and `print` scans (count scans, not chars); fewer than
  5 such scans → `uncertain`.
- **Stop rule:** on the silver volumes **minus the dev inventories**, if fewer than 90% of silver `print` volumes get
  `print`, or fewer than 90% of silver `hand` volumes get `hand`, stop after the error analysis and report
  (negative result). Do not change the rule.

## Deliverables

| File | Change |
|---|---|
| `reports/data/phase-10-hand-print-per-region/step-15p/census.py` | argparse: `--tsv-dir`, `--out-dir` (default `data/hand_print_census/15p-a`), `--workers N`, `--limit N` (first N files, for a smoke run), `--fit-only` |
| `data/hand_print_census/15p-a/` (git-ignored, not in reports) | `regions/<inv>.tsv.gz` (`scan_id, x, y, w, h, n_chars, density, label`), `scans.tsv.gz` (`inv, scan_id, n_regions, n_chars, hand_chars, print_chars, uncertain_chars, short_regions, print_share, label`) |
| `reports/data/phase-10-hand-print-per-region/step-15p/` | `thresholds.json` (lo, hi, both percentile lists, n regions per class, the dev inventory lists); `inventories.tsv` (one row per inventory: chars, scans per label, `print_scan_share`, label, silver label, median region `w`, `h`); `agreement.tsv` (silver × predicted counts); `density_hist.tsv` (corpus region density, bins of 0.02 up to 2.0 plus overflow, char-weighted and region counts); `resolution.tsv` (dev inventories: median `page_w`, `page_h`); `samples/*.tsv` (the error-analysis reads); `staging.tsv` |
| `reports/phase-10-hand-print-per-region.md` | `### Step 15p: hand/print census` section; findings `F10-15p.n` |
| `docs/USER_ACTIONS.md` | one new UA (staging), next free ID (the file's last ID is UA-016 in the resolved file; check both files) |

Run the full census through coreshare from the AS root (one worker per file, `concurrent.futures.ProcessPoolExecutor`):
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15p-census --want 6 --min 2 --wait -- .venv/bin/python -I reports/data/phase-10-hand-print-per-region/step-15p/census.py --workers {workers}
```
Smoke first with `--limit 20` (no coreshare needed, `--workers 1`). Expect minutes, not hours (2.1 GB of gz).
`mkdir -p data/hand_print_census/15p-a` and `chmod -R g+w` it; a re-run goes to a new dir (`15p-b`), never over it.

## Analysis (in this order)

1. `--fit-only`: thresholds from the 14 dev TSVs → `thresholds.json`. Compare with the planning percentiles above;
   a difference > 0.01 in any of them is a brief defect to record, not a stop.
2. Full census. Totals: regions, chars, scans; scans and chars per label; volumes per label.
3. Silver agreement (dev inventories excluded) → `agreement.tsv`, with the stop rule. Also the dev inventories'
   own labels (expected 8 hand, 6 print; report any other).
4. Corpus shape: from `density_hist.tsv`, is the char-weighted histogram bimodal, and where are its modes? What
   share of chars falls in `uncertain`? Which volumes are `mixed` (count, their `print_scan_share` histogram in
   0.1 bins), and are mixed scans clustered in runs (for each mixed volume, the number of label changes along scan
   order vs scans)? Group the volume labels by EAD `ancestor_2`/`ancestor_3` (top 15 series by chars).
5. **Odd material** (P2: report what fits neither class): scans with ≥ 200 labelled chars whose region densities
   are bimodal within the scan (both ≥ 20% hand chars and ≥ 20% print chars: "mixed scans"); regions with density
   > 1.5 or < 0.05 and ≥ 200 chars; inventories whose median region `w` or `h` lies outside the corpus 1st–99th
   percentile (a resolution or layout outlier). Counts, then read samples (below).
6. Resolution: `resolution.tsv` for the 14 dev inventories plus 455/456; say whether `page_w` varies by more than
   10% among them.
7. **Staging list** → `staging.tsv` (`inv, why, label, print_scan_share, chars`): up to 12 inventories, chosen by
   `random.Random(15)` within groups: 5 `mixed` volumes (spread: one from each `print_scan_share` quintile that has
   any), 3 silver contradictions (if any), 2 odd-material volumes, 2 `print` volumes outside the silver groups (if
   any). None of the 16 local ones.

## Error analysis (read text; write verdicts into `samples/*_verdicts.tsv`)

You read region text with its box size and density (no images are available locally; IIIF ids are not derivable
from scan ids). Verdicts: `printed`, `handwritten`, `form` (printed frame with handwritten fill), `table/numbers`,
`ocr_junk`, `unclear`. Signs of print OCR: regular spelling of the period's printed resolutions, `-` line-end
hyphens, typesetting artefacts ("ſ" read as "f", "Rentmeelter"); signs of HTR on hand: `„`/`=` break marks,
abbreviations with superscripts lost, irregular spelling. **When text alone cannot decide, say `unclear`**; do not
guess from the density.
- 3350: all regions with density > 0.55 if ≤ 30, else 30 (`random.Random(15)`); are they printed inserts?
- 20 `uncertain` regions from non-dev inventories (seed 15, ≥ 100 chars).
- 20 mixed scans (seed 15): their two densest and two sparsest regions each.
- Every silver contradiction volume (up to 30): its 3 most-chars scans, top region of each.
- 20 odd-material cases (seed 15) across the three kinds in analysis 5.
Write a category table with counts and one verbatim example each (shortened) per sample.

## Constraints

- No library code changes; no writes under `shared-data/` or any sibling. Never touch an external drive: if a
  file is missing locally, stop and name it.
- Never `git commit`; never bump the version; `chmod g+w` everything you create; `.venv/bin/python -I`.
- Don't read whole TSVs into context: print `head()`/shapes only.

## Tests

No library tests. `census.py --self-test` runs the region/scan/volume rules on a 6-row hand-made frame (one region
of each label, one short, one scan per scan label) and asserts the labels.

## Acceptance criteria

1. `thresholds.json` exists; `lo`/`hi` fitted from the 14 dev inventories only.
2. All 9,122 TSV files processed (or each failure listed with its error); totals reported.
3. `agreement.tsv` and the stop-rule verdict; the 14 dev volumes' labels.
4. The five samples read, with verdict tables; analyses 4–6 answered with numbers.
5. `staging.tsv` and the staging UA: **Run** (by the user, from their drive) copying
   `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` for each listed inventory into
   `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/`; default if unanswered: 15q
   validates on the 16 local inventories only; blocks: 15q's staged-volume validation. Free disk on 2026-10-23 was
   44 GB; the six local hand tars are 6–62 MB each.

## When done

1. Report section `### Step 15p: hand/print census` (template `plan/templates/REPORT.md`): totals, thresholds,
   agreement, corpus shape, odd material, the verdict tables, conclusion: is density enough, and what the
   `uncertain`/`mixed` material is. Findings `F10-15p.n` (`draft`), at least: the printed share of 1.01.02 (chars and
   volumes), the number of mixed volumes, the silver agreement, the odd-material classes. `make reports`, no warnings.
2. Append to `PROGRESS.md`; overwrite `STATUS.md` (15p done or stopped; the staging UA ID; next: 15q).
