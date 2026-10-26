# Step 15q: Hand/print region classifier in the library

> **VOID (2026-10-23).** The density rule failed 15p's stop rule. Don't run this brief; it is rewritten after UA-019
> (see the phase `STATUS.md`).

> **Phase:** 10, Hand and print per region (text layer v4). **Depends on:** 15p (its `thresholds.json` and
> report section); the staging UA from 15p for part B.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, this file,
> then the report section `### Step 15p` in `reports/phase-10-hand-print-per-region.md` (for its numbers only).
> **Read nothing else unless this brief names it.** If you have to explore, record it under *Brief defects*.

## Context

Step 15p measured one feature, text density per PageXML region, on all SG 1.01.02 inventories and fixed a rule
(below). This step puts that rule in the library, with the fallback that gives every region a mode, so the text-layer
stages (15r, 15s) can choose hand or print rules per region. It validates the library rule on the 16 local
inventories' PageXML-derived tables, and on the volumes the user staged from 15p's list. **If 15p stopped on its
stop rule, don't start this step: the phase needs re-planning.**

## The rule (phase decisions P2, P3, P5; copy the thresholds from 15p, don't refit)

- Region: `n_chars` = length of the region text as the legacy TSVs built it, which is what 15p measured:
  `archival_structures.text_layer.extract_v2.legacy_region_text(line_texts)` (non-empty lines joined by
  `text_layer.join.library_join(texts, ["-", "„"])`, so word breaks are joined, not spaced). Check on 3349 that it
  reproduces the TSV `len(text)` (acceptance 2). `density = 1000 * n_chars / (w * h)`
  (0 if `w * h == 0`). `n_chars < min_chars` (20) → `short`; else `hand` if density ≤ `t_lo`, `print` if ≥ `t_hi`,
  else `uncertain`. `t_lo`, `t_hi` = `lo`, `hi` of `reports/data/phase-10-hand-print-per-region/step-15p/thresholds.json`,
  rounded to 3 decimals.
- Page (`(scan_id, page_side)`): `print_share` = print chars / (print + hand chars). `print` ≥ 0.9, `hand` ≤ 0.1,
  else `mixed`; < 200 labelled chars → `uncertain`.
- **Mode** (what the build uses; only `hand` or `print`): a `hand`/`print` region keeps its label
  (`mode_method="density"`); a `short` or `uncertain` region takes its page's majority (`print_share ≥ 0.5` →
  `print`, `mode_method="page"`); a page with no labelled chars takes the inventory's majority over all labelled
  chars (`"inventory"`); an inventory with none → `hand` (`"default"`).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/hand_print.py` (new) | the API below, module docstring citing step 15p and decisions P2/P3/P5 |
| `tests/test_analysis_hand_print.py` (new) | tests below |
| `docs/shared_library/changelog.md` | `### Step 15q: hand_print classifier` under *Unreleased* (add the heading if the file has none after the last release), with Edit |
| `reports/data/phase-10-hand-print-per-region/step-15q/` | `validate.py`, `dev_volumes.tsv`, `dev_pages.tsv.gz`, `staged_*.tsv`, verdict files |
| `reports/phase-10-hand-print-per-region.md` | `### Step 15q: region classifier`; findings `F10-15q.n` |

```python
# archival_structures/analysis/hand_print.py
LABELS = ("hand", "print", "uncertain", "short")
PAGE_LABELS = ("hand", "print", "mixed", "uncertain")
MODES = ("hand", "print")
MODE_METHODS = ("density", "page", "inventory", "default")

@dataclass(frozen=True)
class HandPrintParams:
    t_lo: float                 # required: no archive-neutral default exists
    t_hi: float
    min_chars: int = 20
    page_print: float = 0.9
    page_hand: float = 0.1
    min_page_chars: int = 200
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, d: dict) -> "HandPrintParams": ...   # rejects unknown keys and t_lo > t_hi (ValueError)

def region_density(n_chars: int, w: float, h: float) -> float
def classify_region(n_chars: int, w: float, h: float, params: HandPrintParams) -> tuple[str, float]  # (label, density)
def page_label(labelled: Iterable[tuple[str, int]], params: HandPrintParams) -> str               # (label, n_chars) pairs
def resolve_modes(regions: Sequence[dict], params: HandPrintParams) -> dict[str, dict]
    # regions: dicts with region_id, page_key (any hashable, e.g. (scan_id, page_side)), n_chars, w, h.
    # returns {region_id: {"label", "density", "mode", "mode_method", "page_label"}}; deterministic;
    # the inventory majority is computed over all given regions (call it once per inventory).
def classify_inventory_tables(region_rows: Sequence[dict], line_rows: Sequence[dict], params,
                              join: Callable[[list[str]], str] = " ".join) -> dict[str, dict]
    # v2 extract rows: regions_meta (region_id, scan_id, page_side, w, h) and lines (region_id, line_idx, text_raw);
    # joins line texts per region in line_idx order with `join`, then resolve_modes with page_key=(scan_id, page_side).
```
`text_raw` may be empty; treat a missing text as `""`. No pandas in the module (it runs inside the extract worker),
and no import of `text_layer` (the analysis module stays independent): callers in the text layer and `validate.py`
pass `join=legacy_region_text`. `w`/`h` may be `""` for regions without coords: such a region is `short`.

## Validation

Part A (local, always): `validate.py --layer data/text_layer_v2_dev/15m-a` reads, per inventory, `regions_meta/<inv>.tsv.gz`
(`region_id, scan_id, page_side, w, h`) and `lines/<inv>.lines.tsv.gz` (`region_id, line_idx, text_raw`) of
`hand/NL-HaNA_1.01.02_text_layer_v3` (14 inventories) and `print/NL-HaNA_1.10.94_text_layer_v3` (455, 456; their
lines are post-split halves, so `n_chars` slightly undercounts the joining spaces: report the difference against an
unsplit count if cheap, else note it). Outputs: `dev_volumes.tsv` (per inventory: chars and pages per page label,
chars per resolved mode, `mode_method` counts) and `dev_pages.tsv.gz` (per page). Expected: hand 3349–3354, 11182,
11281 resolve ≥ 0.95 of chars to `hand`; 3771–3773, 3821–3823, 455, 456 ≥ 0.95 to `print`.

Part B (only if 15p's staging UA has a **Response** saying the files are staged): for each staged inventory in
`/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/` (exclude the six pilot tars
3349–3354), extract with the existing v3 hand preset into a new scratch layer (extract only; no split happens
with the hand preset):
```
.venv/bin/as-build-text-layer --preset sg_1.01.02_v3 --pagexml-dir <dir of symlinks to the staged tars> \
  --out-dir data/text_layer_v2_dev/15q-staged --stage extract --workers {workers}
```
(through `coreshare.py run --project archival-structures --label 15q-staged --want 4 --min 1 --wait -- …`). Then run
`validate.py` on it: per volume page labels, and for each `mixed` volume the runs of page labels along scan order.
Compare each volume with 15p's TSV-based label (`reports/data/phase-10-hand-print-per-region/step-15p/inventories.tsv`).
If the UA is unanswered: skip part B, say so in the report and `STATUS.md`; the phase continues.

## Tests (`tests/test_analysis_hand_print.py`, synthetic only)

- `test_density_and_labels`: boundaries (= t_lo → hand, = t_hi → print, between → uncertain, < min_chars → short, zero area).
- `test_page_label`: 0.9/0.1 bounds, `mixed`, < 200 chars → `uncertain`.
- `test_resolve_modes_fallbacks`: an uncertain region on a print page → print/`page`; a short region on a page
  without labelled chars → inventory majority/`inventory`; an inventory with only short regions → hand/`default`.
- `test_params_round_trip_and_validation`: `from_dict(to_dict())`; unknown key and `t_lo > t_hi` raise.
- `test_classify_inventory_tables_joins_lines_in_order`: line_idx order and the given `join` decide `n_chars`;
  a region with `w=""` is `short`.
- `test_deterministic`: same input order-shuffled → same output.
Command: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_hand_print.py`, then the full suite.

## Acceptance criteria

1. New tests pass; full suite green (baseline: 1,202 passed, 11 skipped; collect the count first).
2. On 3349, library `n_chars` per region equals the 15p TSV `len(text)` for ≥ 99% of regions (join on `region_id`
   = TSV `doc_id`); list the kinds of difference.
3. Part A: the 16 dev volumes as expected above, each with its share. Any volume below 0.95: stop and report.
4. Part B done or explicitly skipped; staged volumes compared with 15p's labels.

## Error analysis

- Every dev page whose label differs from its volume's class (up to 30, seed 16; all if fewer): read the region
  texts, verdict as in 15p (`printed`, `handwritten`, `form`, `table/numbers`, `ocr_junk`, `unclear`).
- Part B: per mixed staged volume, 5 pages at label changes (seed 16): is the change real?
- `mode_method` counts per volume: what share of chars is decided by fallback rather than density? Read 10
  `page`-fallback regions (seed 16).

## Constraints

- `hand_print.py` holds no SG knowledge beyond the parameter values the presets pass in; thresholds are required
  parameters. Don't touch `text_layer/` (15r does).
- Protected: `data/text_layer_v2_dev/15m-a`, `15n-a`, `data/hand_print_census/15p-a`, `shared-data/`: hash before and
  after with `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py <out> --skip 15q`
  (format `size=N mtime path`); the two outputs must be equal.
- Never `git commit`; never bump the version; `chmod g+w` new files; `.venv/bin/python`; no external drive.

## When done

1. Report section (results, the error-analysis tables, conclusion), findings `F10-15q.n` (`draft`); `make reports`.
2. `PROGRESS.md` block; overwrite `STATUS.md` (next: 15r; under *Facts later steps need*: the exact `t_lo`/`t_hi`
   and the dev fallback shares).
