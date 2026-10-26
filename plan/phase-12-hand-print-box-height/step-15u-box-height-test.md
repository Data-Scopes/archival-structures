# Step 15u: Box-height volume mode: fit on 76 volumes, one test on a fresh sample

> **Phase:** 12, Hand and print by line-box height. **Depends on:** UA-023 (the 110 tars staged and readable).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file. **Read nothing else unless this brief names it.** Don't read the phase `PLAN.md`. If you have to explore the
> code base anyway, do what you must and record it under *Brief defects*.

## Context

Phase 10 tried to give every SG inventory a `hand`/`print` mode from its own PageXML. Density (15p) and the ratio
line-box height / baseline pitch (`hrel`, 15q) both failed their pre-registered tests. The phase-10 review found
(F10-R.1, F10-R.3) that the **absolute** line-box height separates all 76 labelled volumes seen so far: print
65.0–86.0 px, hand 89.5–173.0 px. That was a post hoc look, so it is not a result. This step turns it into one
rule, fits its cut on those 76 volumes, freezes it, and tests it **once** on 60 fresh random silver volumes with a
stop rule fixed in advance. Nothing is tuned after the freeze. A negative result is a result: if the stop rule
fires, report it and stop.

## The rule (decisions P1–P5: copy exactly)

- **Region features** are those of `hand_print.region_features` (15q): per region, over its lines with non-empty
  `text_raw`: `n_lines`, `n_chars`, `h_med` (median `h`), `pitch_med`, `hrel`; a region without a feature (< 3
  lines, or < 2 positive baseline pitches) has `h_med = hrel = None`.
- **Qualifying region:** `hrel is not None` **and** `n_chars >= min_region_chars` (100).
- **Volume value** = char-weighted median of `h_med` over the qualifying regions of the whole inventory: sort by
  `(h_med, n_chars)`, `c` = cumulative `n_chars`, value = `h_med` at index `bisect_left(c, c[-1] / 2)` (the same rule
  as `scan_value`, applied to `h_med`). None if no qualifying region.
- **Mode:** `print` if value ≤ `cut`, else `hand`, `method = "box_height"`; value None → `hand`, `method = "default"`.
  `margin` = value − cut.
- **Cut (`fit_box_height_cut`):** if `max(print_values) < min(hand_values)`: their midpoint. Otherwise: candidates =
  midpoints of consecutive distinct values of the pooled sorted list; balanced error of a cut = (share of print
  values > cut + share of hand values ≤ cut) / 2; take the candidate with the lowest balanced error, ties → the
  lowest cut. Round to 2 decimals.
- **Fit set (76 volumes; never a 15u test or diagnostic volume):** dev hand 3349–3354, 11182, 11281 and dev print
  3771–3773, 3821–3823; 455 and 456 (`print`); the 60 volumes of
  `reports/data/phase-10-hand-print-per-region/planning-15q/silver_sample.tsv` (columns `inv, silver, stratum,
  stratum_size, label, print_scan_share, chars`; class = `silver`).
- **Scan diagnostics** (no rule): per scan, the same weighted median over its qualifying regions; a scan with ≥ 200
  qualifying chars has side `print` (≤ cut) or `hand`; `scan_minority_share` = share of sided scans whose side differs
  from the volume mode.

Planning reference (measured 2026-10-23 with this definition, `reports/data/phase-10-hand-print-per-region/review/h_by_volume.tsv`,
columns `set, inv, silver, mode, share, h_w`; rows with `set` ∈ `dev_eval, transfer, sample` are the fit set,
`h_w` the volume value): max print 86.0 (12135), min hand 89.5 (3492), so **cut = 87.75**, 76/76 on the fit set;
455 = 75.0, 456 = 82.5.

## Inputs

- Region caches (gzip TSV, header `region_id, scan_id, scan_order, n_lines, n_chars, h_med, pitch_med, hrel,
  text_head`; empty cell = None): `data/hand_print_geom/15q-a/1.01.02/regions/<inv>.tsv.gz` (dev and the 60) and
  `data/hand_print_geom/15q-a/1.10.94/regions/{455,456}.tsv.gz`. **Read only.**
- 15q harness to copy from: `reports/data/phase-10-hand-print-per-region/step-15q/validate.py`: `tsv_rows`,
  `write_tsv`, `mkdir` (L47–65), `_prepass_one` (L68–104), `cmd_prepass` (L107–121), `load_regions` (L125–132),
  `wilson` (L201), the stop-rule part of `cmd_evaluate` (L211–275). `_prepass_one` reads with
  ```python
  for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):   # text_layer.io
      try: scan = parse_pagexml_file(member, pagexml_data=data)                              # pagexml.parser
      except Exception: n_fail += 1; continue
      s_row, r_rows, l_rows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
  ```
  with `preset = PRESETS["sg_1.01.02_v3"]`; `s_row` has keys `scan_id`, `page_w`, `page_h`, `n_lines_text`.
  `discover_inventories_v2(pagexml_dir, pattern) -> {inv: Path}` (`text_layer/io.py` L191), pattern =
  `preset.source_pattern`.
- Test and diagnostic volumes: `reports/data/phase-12-hand-print-box-height/planning/test_sample.tsv` (columns
  `inv, set, stratum, silver, role, stratum_size, chars, label15p`; `set` = `test` (60 `sample` + 25 reserves) or
  `diagnostic` (20 + 5); `role` = `sample` or `reserve1`…`reserve5`). Tars:
  `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/` (read only). If a tar raises
  `PermissionError`, stop: UA-023 is not done.
- Existing module `archival_structures/analysis/hand_print.py` (15q, unused): `region_features` L117, `scan_value`
  L137 (`-> (value, chars)`, the weighted-median rule above, on `hrel`), `VolumeModeParams` L44,
  `volume_mode_from_features(feats, params)` L173 (the 15q rule; needed only for the comparison in *Error analysis*).
- Series names: `reports/data/phase-10-hand-print-per-region/step-15p/inventories.tsv` (use `usecols=["inv",
  "ancestor_3", "label"]`).
- Fit-set scan widths (resolution check): `data/text_layer_v2_dev/15m-a/hand/NL-HaNA_1.01.02_text_layer_v3/scans/<inv>.scans.tsv.gz`
  and `.../15m-a/print/NL-HaNA_1.10.94_text_layer_v3/scans/{455,456}.scans.tsv.gz` (column `page_w`).

## Library API (additive, in `archival_structures/analysis/hand_print.py`)

```python
BOX_HEIGHT_METHODS = ("box_height", "default", "fixed")      # MODE_METHODS stays unchanged

@dataclass(frozen=True)
class BoxHeightParams:
    cut: float                      # required: no archive-neutral default
    min_region_chars: int = 100
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, d: dict) -> "BoxHeightParams": ...   # ValueError on unknown keys or cut <= 0

def volume_box_height(features: Sequence[dict], min_region_chars: int = 100) -> tuple[Optional[float], int, int]
    # (weighted median h_med over qualifying regions or None, their chars, their count)
def scan_box_heights(features: Sequence[dict], min_region_chars: int = 100) -> list[dict]
    # [{"scan_id", "value", "n_chars", "n_regions"}] in first-seen scan order
def fit_box_height_cut(hand_values: Sequence[float], print_values: Sequence[float]) -> float
def box_height_mode(features: Sequence[dict], params: BoxHeightParams) -> dict
    # {"mode", "method", "value", "margin", "n_chars", "n_regions", "params": params.to_dict()}
```
Pure Python plus numpy, no `text_layer` import, deterministic under shuffled input. Add a paragraph to the module
docstring: the box-height rule (phase 12, step 15u, F10-R.3), raw pixels, cut is a parameter; keep the existing
*Status: negative result* paragraph for `hrel`.

## Harness (`reports/data/phase-12-hand-print-box-height/step-15u/validate.py`, new)

New cache `data/hand_print_geom/15u-a/1.01.02/{regions,pre,scans}/` (new dir; `scans/<inv>.tsv.gz`: `scan_id,
scan_order, page_w, page_h, n_lines_text`). Subcommands:

1. `fit`: the 76 fit volumes from the 15q caches → `fit_volumes.tsv` (`set, inv, cls, value, n_chars, n_regions`),
   `cut.json` (`cut`, `max_print` + inv, `min_hand` + inv, `separable`, `n_print`, `n_hand`, `fit_agreement`,
   `params`). If `cut.json` exists, assert the recomputed dict equals it and never overwrite.
2. `prepass --set {test|diagnostic} --workers {workers}`: **refuses to run if `cut.json` is missing.** Takes the
   `sample` rows of the set; for a sample whose tar is not found, the next unused reserve of the same stratum, in
   order (`substitutions.tsv`: `stratum, missing, replacement`). Writes the caches (copy `_prepass_one`, add the
   scans table). Launch through coreshare:
   `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15u-prepass --want 6 --min 2 --wait -- .venv/bin/python reports/data/phase-12-hand-print-box-height/step-15u/validate.py prepass --set test --workers {workers}`
   (then `--set diagnostic`).
3. `evaluate --set {test|diagnostic}`: per volume `box_height_mode` with the frozen cut → `<set>_volumes.tsv` (`inv,
   stratum, silver, stratum_size, role, mode, method, value, margin, n_chars, n_regions, n_scans, n_parse_fail,
   scan_minority_share, median_page_w, median_page_h, label15p, agree`) and `<set>_scans.tsv.gz` (`inv, scan_id,
   scan_order, value, n_chars, n_regions, side, page_w, page_h`). `test` also writes `stop_rule.json`.

## Order of work (the order is part of the test)

- **Part A:** library + tests (synthetic only).
- **Part B (fit):** `fit`; check acceptance 2 **before** anything else. Then **freeze**: `cut.json` written, its
  sha256 recorded in a `PROGRESS.md` block "Step 15u, part B frozen", before `data/hand_print_geom/15u-a/` exists.
- **Part C (stop rule, P5):** prepass and evaluate `test`. Agreement per stratum = volumes whose mode equals `silver`
  / stratum sample size (parse failure or `default` = wrong). Weighted agreement for silver `print` = Σ
  (stratum_size × agreement) / Σ stratum_size over *gedrukte_resoluties* (99), *nouvelles* (64), *plakkaten* (76);
  for silver `hand` over *eerste_serie* (123), *tweede_serie* (421). Wilson 95% per stratum. **Pass iff both ≥ 0.90.**
- **Part D (diagnostic, P6):** prepass and evaluate `diagnostic`; no pass/fail.
- Never change the rule, the cut, a feature or a threshold after part B. If part C fails: finish the error analysis,
  write the report, set `STATUS.md` to "stop rule fired; phase review next", and stop.

## Tests (`tests/test_analysis_hand_print.py`, add; synthetic)

- `test_volume_box_height`: features (h_med, hrel, chars) (60, 1.4, 300), (140, 1.7, 100) → 60; filters: n_chars 99
  excluded, `hrel None` excluded; none qualifying → `(None, 0, 0)`; ties at half: `bisect_left`.
- `test_fit_box_height_cut`: separable → midpoint (e.g. print [65, 86], hand [89.5, 150] → 87.75); overlapping →
  lowest balanced error; equal errors → lowest cut; rounding to 2 decimals.
- `test_box_height_mode`: value = cut → `print`; above → `hand`; no qualifying region → `hand`/`default`, margin None.
- `test_box_height_params`: round trip; unknown key and `cut <= 0` → `ValueError`; missing `cut` → `TypeError`.
- `test_scan_box_heights` (order, values); `test_box_height_deterministic` (shuffled features, same result).
- The 7 existing tests stay unchanged and green.
Command: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_hand_print.py`, then the full suite.

## Acceptance criteria

1. New tests pass; full suite green (before this step: 1,220 collected, 1,209 passed, 11 skipped; collect first).
2. `fit`: all 76 values equal `h_w` of `h_by_volume.tsv` (|diff| ≤ 0.001, same inv); max print 86.0 (12135), min
   hand 89.5 (3492); `cut` = 87.75; fit agreement 76/76. Otherwise stop and report: the planning number does not reproduce.
3. `cut.json` sha256 in `PROGRESS.md` before the first prepass; the mtime of `cut.json` precedes every file under
   `data/hand_print_geom/15u-a/`.
4. Test set: 60 volumes evaluated (after substitutions, each listed with its reason); parse failures counted.
5. `stop_rule.json`: per-stratum n, agree, agreement, Wilson 95%; both weighted agreements; pass/fail.
6. Diagnostic table (20 volumes: series, mode, value, margin).
7. Protected data unchanged, before and after the step, compared with `diff`:
   `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py <out> --skip 15u` and
   `find data/hand_print_geom/15q-a reports/data/phase-10-hand-print-per-region -type f | sort | xargs shasum -a 256 > <out2>`
   (outputs in `step-15u/`).

## Error analysis (text only; verdicts `printed`, `handwritten`, `form`, `table/numbers`, `ocr_junk`, `unclear`)

- Every test volume whose mode disagrees with its silver label (all, if any): value, margin, scan-value histogram
  (5 px bins), median `page_w`, and 5 scans nearest its value (seed 21) with the 2–3 largest regions' `text_head` and
  `h_med`. Why did it fail: a mislabelled volume, a real mixed volume, scan resolution, or the feature?
- Every test or diagnostic volume with |margin| ≤ 5 px (all, if any): 3 scans each, same reads.
- Diagnostic set: series (`ancestor_3`) of each volume; every volume with mode `print` (5 scans each) and the 3 `hand`
  volumes with the lowest values (3 scans each). Is the mode right?
- Minority scans: every volume with `scan_minority_share` > 0.2: 5 minority scans (seed 21). Real minority material
  (a printed insert, a handwritten slip) or noise?
- Resolution: median `page_w` per class for the fit set (dev and 455/456 from the `15m-a` scans tables) and for the
  test set; list test volumes whose median `page_w` lies outside the fit set's range.
- Comparison only (no decision): the 15q `hrel` mode of each test volume (`volume_mode_from_features` with
  `VolumeModeParams(1.529, 1.534)`); count the volumes where the two rules disagree and which one matches silver.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/hand_print.py` | API above (additive) |
| `tests/test_analysis_hand_print.py` | tests above |
| `docs/shared_library/changelog.md` | `### Step 15u: hand_print box-height rule` under *Unreleased* (after the 15q entry), with Edit; affected sibling imports: none |
| `reports/data/phase-12-hand-print-box-height/step-15u/` | `validate.py`, `fit_volumes.tsv`, `cut.json`, `substitutions.tsv`, `test_*`, `diagnostic_*`, `stop_rule.json`, `verdicts.tsv`, protect outputs |
| `reports/phase-12-hand-print-box-height.md` | fill the empty section `### Step 15u: box-height test`; findings `F12-15u.n` (`draft`) |

## Constraints

- `hand_print.py` holds no SG knowledge; the cut is a required parameter. Don't touch `text_layer/`.
- Silver labels of the test set are used only in part C, for evaluation. No fitting on any 15u volume.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`; no external drive;
  no `rm -rf`, `nohup` or `&`; zsh: write the `coreshare.py run …` prefix out, never in a variable.

## Do not touch

`text_layer/`, `data/hand_print_geom/15q-a/`, `reports/data/phase-10-hand-print-per-region/`, the phase-10 report.

## When done

1. Report section (fit table, stop-rule table, diagnostic table, error analysis, conclusion), findings `F12-15u.n`;
   `make reports` (no warnings).
2. Append the block below to `PROGRESS.md`; overwrite `STATUS.md`. If the stop rule passed: next = an Opus session
   writes the 15v/15w briefs; under *Facts later steps need*: the `BoxHeightParams` dict, `cut.json` sha256, the
   per-volume modes (paths), the minority-share list. If it failed: next = the phase review (prompt 3).

```markdown
### Step 15u: box-height test
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added>
- **Tests:** <command> → <result>
- **Acceptance:** <1–7, with numbers>
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15u](../../reports/phase-12-hand-print-box-height.md#step-15u-box-height-test); findings F12-15u.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
