# Step 15q: Volume-mode classifier (line geometry) and its one test

> **Phase:** 10, Hand and print per volume (text layer v4). **Depends on:** 15p; UA-020 (60 tars staged) and UA-021
> (their group read permission) for part C.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, this file.
> **Read nothing else unless this brief names it.** If you have to explore, record it under *Brief defects*.

## Context

Step 15p showed that text density cannot tell hand from print (F10-15p.1). The re-plan (report section *Planning*,
paragraph *Re-plan of 15q*, F10-P.6) measured line geometry on the dev volumes: one size-independent feature, `hrel`
(line-box height / baseline pitch), separates the dev **volumes** but not single regions. The user chose one mode per
inventory, decided from its own PageXML (UA-019 (b)). This step puts that rule in the library, reproduces the
re-plan's numbers, and runs **one** out-of-sample test with a stop rule fixed in advance. Nothing is tuned after seeing
the test volumes. A negative result is a result: if the stop rule fires, report it and stop.

## The rule (decisions P3, P10, P11: copy it exactly)

- **Lines of a region:** the v2 line rows (`region_id, scan_id, h, baseline, text_raw`) whose `text_raw` is non-empty.
- **Region feature** (needs ≥ 3 such lines): `h_med` = median of `h` over them (skip `h == ""`; none left → no feature).
  Baseline `y` of a line = mean of the y values of its points; `baseline` is the string `"x,y x,y …"` of the lines
  table (empty or one point → no y). Sort the ys, take consecutive differences, keep those > 0: the pitches. Need ≥ 2
  pitches, else no feature. `pitch_med` = median of the pitches. `hrel = h_med / pitch_med`. `n_chars` = sum of
  `len(text_raw)` over the region's non-empty lines.
- **Scan value:** over the scan's featured regions, the char-weighted median of `hrel`: sort the regions by `hrel`,
  `c` = cumulative `n_chars`, value = `hrel` at index `bisect_left(c, c[-1] / 2)`. The scan is **labelled** if
  `c[-1] ≥ min_scan_chars` (200). Labelled scan: `print` if value ≤ `t_lo`, `hand` if value ≥ `t_hi`, else
  `uncertain`. An unlabelled scan has label `unlabelled`.
- **Thresholds** (`fit_thresholds`): `t_print` = 0.95 quantile of the print dev scans' values, `t_hand` = 0.05
  quantile of the hand dev scans' values (labelled scans only; linear interpolation, as `numpy.quantile`'s default);
  `(t_lo, t_hi) = sorted((t_print, t_hand))`, each rounded to 3 decimals. Dev = the 14 1.01.02 inventories: hand
  3349–3354, 11182, 11281; print 3771–3773, 3821–3823. **Not** 455/456 and never a silver volume.
- **Volume:** `print_scan_share` = print / (print + hand) labelled scans (`uncertain` excluded). Mode `print` if the
  share ≥ `print_share` (0.5), else `hand`, `method = "geometry"`; no print or hand scans → `hand`, `method =
  "default"`.

Re-plan reference numbers (from the `15m-a` lines tables, script
`reports/data/phase-10-hand-print-per-region/planning-15q/geom_features.py`): thresholds 1.529 / 1.534. Share of
`print` among labelled scans **including** `uncertain` in the denominator: 11182 0.000, 11281 0.132, 3349 0.050,
3350 0.024, 3351 0.185, 3352 0.014, 3353 0.011, 3354 0.157; 3771 0.946, 3772 0.930, 3773 0.962, 3821 0.979, 3822 0.910,
3823 0.929 (9,048 labelled scans).

## Library API (`archival_structures/analysis/hand_print.py`, new)

```python
SCAN_LABELS = ("hand", "print", "uncertain", "unlabelled")
MODES = ("hand", "print")
MODE_METHODS = ("geometry", "default", "fixed")

@dataclass(frozen=True)
class VolumeModeParams:
    t_lo: float                     # required: no archive-neutral default
    t_hi: float
    min_lines: int = 3
    min_scan_chars: int = 200
    print_share: float = 0.5
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, d: dict) -> "VolumeModeParams": ...  # ValueError on unknown keys or t_lo > t_hi

def region_geometry(lines: Sequence[dict], min_lines: int = 3) -> Optional[dict]
    # lines of one region (any order); returns {"n_lines", "n_chars", "h_med", "pitch_med", "hrel"} or None
def region_features(line_rows: Iterable[dict], min_lines: int = 3) -> list[dict]
    # groups by region_id (first-seen order); each dict adds "region_id", "scan_id"; regions without a feature
    # are included with hrel=None (n_lines, n_chars filled) so callers can count them
def scan_value(features: Sequence[dict]) -> tuple[Optional[float], int]   # (weighted median or None, featured chars)
def scan_label(value: Optional[float], n_chars: int, params: VolumeModeParams) -> str
def fit_thresholds(hand_values: Sequence[float], print_values: Sequence[float]) -> tuple[float, float]
def volume_mode(line_rows: Iterable[dict], params: VolumeModeParams) -> dict
    # {"mode", "method", "print_scan_share" (None if no hand/print scans), "n_scans", "n_scans_labelled",
    #  "n_scans_print", "n_scans_hand", "n_scans_uncertain", "params": params.to_dict(),
    #  "scans": [{"scan_id", "n_regions_feat", "n_chars", "value", "label"} in first-seen order],
    #  "regions": region_features(...) output}
```
Pure Python plus `numpy` (no pandas, no import of `text_layer`: the module stays independent). Deterministic:
the same rows in another order give the same mode, share and labels. Module docstring: cite steps 15p/15q, F10-P.6,
decisions P2/P3, and say that region and scan labels are diagnostics, not rules.

## Validation script (`reports/data/phase-10-hand-print-per-region/step-15q/validate.py`)

Subcommands; geometry caches go to `data/hand_print_geom/15q-a/` (new dir; never overwrite another run).

1. `prepass --archive {1.01.02|1.10.94} --src DIR [--inventories a,b,…] --workers {workers}`: discover sources with
   `archival_structures.text_layer.io.discover_inventories_v2(DIR, preset.source_pattern)` (preset `sg_1.01.02_v3`
   or `sg_1.10.94_v3`), and per inventory run the pre-pass exactly as `fit_split_model` does
   (`archival_structures/text_layer/line_split.py` L1396–1410; copy it):
   ```python
   for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):
       try: scan = parse_pagexml_file(member, pagexml_data=data)      # from pagexml.parser
       except Exception: n_parse_fail += 1; continue
       s_row, r_rows, l_rows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
   ```
   (`extract_scan_v2` from `archival_structures.text_layer.extract_v2`; it never splits: splitting is a hook.)
   Write `regions/<inv>.tsv.gz` (`region_features` output plus `scan_id`) and `pre/<inv>.json` (scan count, parse
   failures). Use a process pool over inventories. Launch through coreshare:
   `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15q-prepass --want 6 --min 2 --wait -- .venv/bin/python reports/data/phase-10-hand-print-per-region/step-15q/validate.py prepass … --workers {workers}`.
2. `fit`: dev scan values from the caches → `thresholds.json` (unrounded and rounded, counts) and `dev_scans.tsv.gz`,
   `dev_volumes.tsv`.
3. `evaluate --set {dev|transfer|sample|staged}`: per volume `volume_mode` from the caches with the frozen
   thresholds → `<set>_volumes.tsv` (inv, mode, method, share, scan counts, silver/stratum where known);
   `sample` also writes `stop_rule.json`.

Sources: dev 1.01.02 = `data/text_layer_v3_full_inputs/hand/` (14); transfer = `data/text_layer_v3_full_inputs/print/`
(455, 456); sample (60) and staged (12, UA-017) =
`/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/` (read only; pass `--inventories`
from `planning-15q/silver_sample.tsv` and `step-15p/staging.tsv`, both under `reports/data/phase-10-hand-print-per-region/`).
If a tar raises `PermissionError`, stop: UA-021 is not done (rule 6 of `plan/README.md`).

## Order of work (the order is part of the test)

- **Part A (dev):** prepass dev, `fit`. Check acceptance 2–3 **before** any other set is processed. Then **freeze**:
  write `thresholds.json` and record its sha256 in `PROGRESS.md` before part B.
- **Part B (transfer, P11):** evaluate 455/456. If either is not `print`, the 1.10.94 v4 preset will use
  `{"method": "fixed", "mode": "print"}` (15r applies it); write a finding.
- **Part C (stop rule, P10):** prepass and evaluate the 60 sample volumes. Agreement per stratum = volumes whose mode
  equals their silver label / stratum sample size (a parse failure or `default` counts as wrong). Weighted agreement
  for silver `print` = Σ (stratum_size × agreement) / Σ stratum_size over *gedrukte_resoluties* (99), *nouvelles* (64),
  *plakkaten* (76); for silver `hand` over *eerste_serie* (123), *tweede_serie* (421). `stratum_size` is a column of
  `silver_sample.tsv`. Wilson 95% interval per stratum. **Pass iff both weighted agreements ≥ 0.90.** If it fails:
  finish the error analysis, write the report, set `STATUS.md` to "stop rule fired; phase review next", and stop.
  Never change a threshold, feature or rule after part C has started.
- **Part D (diagnostic):** the 12 UA-017 volumes: mode, share, 15p label (`step-15p/inventories.tsv` column `label`),
  and for 15p `mixed` volumes the run of scan labels along scan order (count label changes).

## Tests (`tests/test_analysis_hand_print.py`, synthetic only)

- `test_region_geometry`: 3 lines at baseline y 100/150/200, h 80 → pitch 50, hrel 1.6; two lines → None; an empty
  baseline skipped; equal ys give no pitch; `h == ""` skipped.
- `test_scan_value_weighted_median`: regions (hrel, chars) (1.2, 100), (1.6, 300) → 1.6; (1.2, 300), (1.6, 100) →
  1.2; ties at half: `bisect_left` behaviour.
- `test_scan_label_bounds`: = t_lo → print, = t_hi → hand, between → uncertain, < 200 chars → unlabelled.
- `test_fit_thresholds_swap_and_round`: crossing and non-crossing inputs; rounding to 3 decimals.
- `test_volume_mode`: share exactly 0.5 → print; all scans uncertain/unlabelled → hand/`default`; counts add up.
- `test_params_round_trip_and_validation`; `test_deterministic` (shuffled rows → same result).
Command: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_hand_print.py`, then the full suite.

## Acceptance criteria

1. New tests pass; full suite green (baseline before this step: 1,213 collected, 1,202 passed, 11 skipped; collect first).
2. On 3349, the pre-pass rows equal `data/text_layer_v2_dev/15m-a/hand/NL-HaNA_1.01.02_text_layer_v3/lines/3349.lines.tsv.gz`
   on `(region_id, line_idx, h, baseline, text_raw)` for 100% of rows (list any difference and stop).
3. `fit` gives t_lo, t_hi = 1.529, 1.534 (±0.001), 9,048 ± 10 labelled dev scans, and the per-volume shares above
   (same denominator) within ±0.005; all 14 dev volumes get their class. Otherwise stop and report: the re-plan
   number does not reproduce.
4. Part B reported, P11 applied or not, with the numbers.
5. Part C: `stop_rule.json` with both weighted agreements, per-stratum counts and intervals, pass/fail.
6. Part D table. Protected data unchanged: hash `data/text_layer_v2_dev/15m-a`, `15n-a`, `data/hand_print_census/15p-a`
   and `shared-data/` before and after with
   `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py <out> --skip 15q` (equal outputs).

## Error analysis (text only; verdicts as in 15p: `printed`, `handwritten`, `form`, `table/numbers`, `ocr_junk`, `unclear`)

- Every sample volume whose mode disagrees with its silver label: its share, the histogram of its scan values
  (0.05 bins), and 5 scans nearest its median value (seed 16) with their region texts and `hrel`. Why did it fail?
- The 10 *plakkaten* (where density failed): all shares, and 3 scans each of the lowest-share two.
- Dev: every labelled scan whose label differs from its volume's class, up to 30 (seed 16): read it. Is it a real
  printed or handwritten page in the "wrong" volume, a short or tabular page, or a feature failure?
- Near the cut: every evaluated volume with share in 0.3–0.7 (list), and the volumes whose scan values have > 30%
  within ±0.05 of the cut (segmentation-model hazard).
- Part D: for each 15p `mixed` volume, 5 scans at label changes (seed 16): is the change real?

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/hand_print.py` (new) | API above |
| `tests/test_analysis_hand_print.py` (new) | tests above |
| `docs/shared_library/changelog.md` | `### Step 15q: hand_print volume-mode classifier` under *Unreleased* (add the heading if none after the last release), with Edit |
| `reports/data/phase-10-hand-print-per-region/step-15q/` | `validate.py`, `thresholds.json`, `dev_*.tsv*`, `transfer_volumes.tsv`, `sample_volumes.tsv`, `stop_rule.json`, `staged_volumes.tsv`, verdict files |
| `reports/phase-10-hand-print-per-region.md` | fill the empty section `### Step 15q: volume-mode classifier`; findings `F10-15q.n` (`draft`) |

## Constraints

- `hand_print.py` holds no SG knowledge; thresholds are required parameters. Don't touch `text_layer/` (15r does).
- Silver labels are used only in parts C and D, only for evaluation. No tuning after part A's freeze.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`; no external drive.

## When done

1. Report section (results table, stop-rule table, error analysis, conclusion), findings `F10-15q.n`; `make reports`
   (no warnings).
2. `PROGRESS.md` block; overwrite `STATUS.md`. If the stop rule passed: next 15r; under *Facts later steps need*:
   t_lo/t_hi, the `VolumeModeParams` dict, the P11 outcome, the per-volume modes of the 16 dev and 72 staged
   volumes (path to the TSVs). If it failed: next is the phase review (prompt 3).
