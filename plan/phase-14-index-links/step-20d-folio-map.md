# Step 20d: folio map

> **Phase:** 14, Index → resolution links. **Depends on:** 20c (its test gold) for the bar; development can start
> before. **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Index entries in handwritten 1.01.02 registers end in folio numbers. To follow one, AS must know which scan carries
folio N. The text layer types printed page numbers (`line_type` `page_number`) but stores no per-side number. This step
adds a generic **folio map** (`analysis/folio_map.py`): anchors from the page-number segments, a monotone main chain,
interpolation for the gaps; plus the volume helpers in a new `analysis/index_links.py` (front-index run, reading a
volume's inputs) that step 20e extends. The rules are fixed (no tuning). The map is tested once on 20c's test gold.

**Numbering facts** (planning, 41 volumes): resolutions are foliated (21,432 of 21,437 chain anchors on rectos); the
front index is paginated and folio numbering restarts after it; folio N = recto of scan S plus verso of the next scan;
3712's numbers do not restart (offset −336). Index sides carry locator-only lines typed `page_number`
(`rule:pn_line_offchain`); they must not become anchors.

## Inputs

- Layer (read only): `L = data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/`. Per inventory:
  - `scans/<inv>.scans.tsv.gz` (`scan_id archive_id inv_num inv_dir scan_num scan_variant scan_id_ok member status …`):
    **the volume's scan order is this table's row order**; scans with `status` `no_regions` are real scans (blank leaves)
    and count for positions (3254: 625 scans, 618 in `layout/pages`);
  - `layout/pages/<inv>.tsv.gz` (`scan_id page n_lines n_text8 n_text20 col_share end_num date_start rule index_side`;
    hand volumes only; sides in scan order, verso before recto; `index_side` `"1"` = flagged);
  - `types/segments/<inv>.tsv.gz` (`scan_id line_id seg_idx char_start char_end x y w h geom_method page
    layout_region_id line_type type_method column_override`);
  - `lines/<inv>.lines.tsv.gz` (`… line_id … text …`); segment text = `text[int(char_start):int(char_end)]` of the line
    with the same `(scan_id, line_id)`.
- Read with `from archival_structures.text_layer.io import read_tsv_gz` (dicts of strings).
- Reuse, do not copy (`archival_structures/analysis/page_numbering.py`):
  ```python
  CONFIDENCE_BANDS = (3, 10)
  def longest_increasing_chain(values) -> list[int]          # indices of a longest strictly increasing subsequence
  def interpolate_folio_to_scan(folio, run, extrap=False, max_scan=None, *, edge=6) -> tuple[float, float, bool]
      # run: (n,2) float array [position, number], numbers increasing; -> (position float, distance to nearest anchor
      # position, inside the number range); with extrap and >= 3 anchors the median slope of the `edge` nearest anchors
      # extrapolates, clamped to [1, max_scan]
  def confidence_band(dist, inside, bands=CONFIDENCE_BANDS) -> str   # "high" / "medium" / "low"; not inside -> "low"
  ```
  Do **not** use `numbering_mode`: on SG openings recto-only foliation has a slope of 1.0 scans per number and is
  labelled `pagination`.
- Gold (from 20c): `reports/data/phase-14-index-links/step-20c/entry_gold.tsv`, columns `read_id, split, inv, scan_id,
  page, unit_id, used_for, entry_verdict, locators, loc_other, target_locator, target_scan_id, folio_evidence,
  scans_opened, content, adjacent_side, evidence, note`. Dev volumes 3254, 3471, 3394, 3308; test 3256, 3456, 3401,
  3611, 3320, 3712, 3590, 3352. Facts 20c left in `STATUS.md` apply.
- Planning reference: `reports/data/phase-14-index-links/planning/front_index_volumes.tsv` (`inv n_sides
  n_front_flagged front_first front_last n_after n_index_units`; 41 rows; `front_first`/`front_last` are 0-based
  indices into the `layout/pages` rows) and `folio_proto.py` (its positions count `layout/pages` scans, not the scans
  table: do not compare offsets with it).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/folio_map.py` (new) | API below |
| `archival_structures/analysis/index_links.py` (new) | `front_index_run`, `VolumeInputs`, `read_volume_inputs` |
| `tests/test_analysis_folio_map.py`, `tests/test_analysis_index_links.py` (new) | below |
| `reports/data/phase-14-index-links/step-20d/eval_folio.py` | builds the 41 maps, LOO, bar F; writes `volumes.tsv`, `loo.tsv`, `gold_eval.tsv` |
| `docs/shared_library/changelog.md` | `### Step 20d: folio map` under *Unreleased* (Edit); affected sibling imports: none |
| `reports/phase-14-index-links.md` | `### Step 20d: folio map` section, draft findings `F14-20d.n` |

## Behaviour to implement

```python
# archival_structures/analysis/folio_map.py
NUMBER_PATTERN = r"^\D{0,3}(\d{1,4})\D{0,3}$"

@dataclass(frozen=True)
class FolioMapParams:
    number_pattern: str = NUMBER_PATTERN
    foliation_recto_share: float = 0.9     # anchors on rectos at or above this share -> foliation
    min_chain: int = 3                     # fewer chain anchors -> mode "none"
    extrapolate: int = 6                   # numbers beyond the chain ends that are still located
    edge: int = 6                          # passed to interpolate_folio_to_scan
    bands: tuple[int, int] = CONFIDENCE_BANDS

@dataclass(frozen=True)
class FolioTarget:
    number: int
    front: tuple[str, str]                 # (scan_id, page): foliation: the recto (or "single") of the scan at position
    back: Optional[tuple[str, str]]        # foliation: (next scan_id, "verso") if that side exists; pagination: None
    position: int                          # 1-based position (scan position in foliation, side position in pagination)
    method: str                            # "printed" | "interpolated" | "extrapolated"
    anchor_dist: float
    confidence: str

@dataclass
class FolioMap:
    mode: str                              # "foliation" | "pagination" | "none"
    chain: np.ndarray                      # (n, 2) float [position, number]; empty (0, 2) for "none"
    n_anchors: int
    recto_share: float                     # nan without anchors
    def locate(self, number: int) -> Optional[FolioTarget]: ...
    def table(self) -> list[dict]: ...     # one dict per number N in [max(1, min-extrapolate), max+extrapolate] that
                                           # locates: number, front_scan_id, front_page, back_scan_id, back_page
                                           # ("" if None), position, method, anchor_dist (round 2), confidence

def build_folio_map(scan_ids: Sequence[str], sides: Sequence[tuple[str, str]],
                    numbers: Mapping[tuple[str, str], Sequence[str]], *,
                    exclude: Collection[tuple[str, str]] = (), params: FolioMapParams = FolioMapParams()) -> FolioMap
```

- `scan_ids`: every scan of the volume in order (scan position = 1-based index). `sides`: the volume's sides in order
  (`page` ∈ `verso`, `recto`, `single`); side position = 1-based index in `sides`. `numbers[side]`: the side's
  page-number segment texts in segment order.
- **Anchors:** for each side not in `exclude`, the first text that matches `number_pattern` (after `.strip()`) gives
  `int(group(1))`; one anchor per side at most. `recto_share` = recto anchors / anchors. ≥ `foliation_recto_share` →
  `foliation`: keep recto anchors only, position = scan position of the side's scan. Else `pagination`: all anchors,
  position = side position. Sort anchors by position.
- **Chain:** `longest_increasing_chain` over the anchor numbers in position order; `chain` = those `[position,
  number]`. Fewer than `min_chain` → mode `none` (and `locate` returns None).
- **`locate(N)`:** N equal to a chain number → that position, `printed`, dist 0, `confidence_band(0, True)`. Else None
  if `N < first − extrapolate` or `N > last + extrapolate` (chain numbers) or N < 1. Else `s, dist, inside =
  interpolate_folio_to_scan(N, chain, extrap=True, max_scan=n_positions, edge=edge)`; `position = floor(s + 0.5)`
  clamped to `[1, n_positions]`; method `interpolated` if `inside` else `extrapolated`; `confidence_band(dist, inside,
  bands)`. `n_positions` = number of scans (foliation) or sides (pagination).
- **Targets:** foliation: `front` = the scan's `recto` side if in `sides`, else its `single`, else its first side in
  `sides`; if the scan has no side in `sides` (a blank scan), `front = (scan_id, "recto")` anyway. `back` = `(next
  scan_id, "verso")` if that side is in `sides`, else None. Pagination: `front = sides[position − 1]`, `back = None`.

```python
# archival_structures/analysis/index_links.py
def front_index_run(flags: Sequence[bool], *, min_flagged: int = 10, max_first: int = 10, gap: int = 20,
                    min_after: int = 300, max_share: float = 0.6) -> Optional[tuple[int, int, int]]
    # flags in side order. None if no sides or sum(flags)/n > max_share. end = index of the first side of the first run
    # of >= gap consecutive unflagged sides (None -> return None). idx = flagged indices < end; None unless
    # len(idx) >= min_flagged, idx[0] < max_first and n - end >= min_after. -> (idx[0], idx[-1], end), 0-based.

@dataclass
class VolumeInputs:
    inv: str
    scan_ids: list[str]                          # scans table order
    sides: list[tuple[str, str]]                 # layout/pages order
    flags: list[bool]                            # index_side == "1", aligned with sides
    numbers: dict[tuple[str, str], list[str]]    # page_number segment texts per side, segment-table order

def read_volume_inputs(layer_root: Path, inv: str) -> VolumeInputs   # FileNotFoundError if layout/pages is missing
```

`eval_folio.py` (from the AS root): for each of the 41 volumes, `read_volume_inputs`, `front_index_run(flags)`,
`exclude` = the sides at indices ≤ `front_last` plus every flagged side, `build_folio_map`. Writes `volumes.tsv`
(`inv mode n_anchors recto_share chain_len chain_first chain_last front_first front_last end`). **LOO:** for each chain
anchor that is not the first or last, locate its number on the chain without it (`interpolate_folio_to_scan` on the
reduced chain, same rounding) and compare positions → `loo.tsv` (`inv number position predicted`) and the pooled exact
share. **Bar F:** on `entry_gold.tsv` rows with `target_scan_id` not in `{"", "none"}` and `folio_evidence` in
`{printed, neighbours}`: `locate(int(target_locator)).front[0] == target_scan_id` → `gold_eval.tsv` (`read_id split inv
target_locator target_scan_id predicted_scan method confidence hit`), share per split and by method.

## Constraints

- Map rules exactly as above; no parameter search. Run the test-split bar **once**, after the dev rows were read.
- `front_index_run` must reproduce the planning table: 41 volumes, the same `front_first`/`front_last` for each
  (planning `end` = the 0-based end index; `n_after = n − end`).
- No archive-specific constants outside the params defaults; no gold or REPUBLIC input to the map.
- No edits to `page_numbering.py`, `text_layer/`, any layer. Never `git commit`; `chmod g+w`; zsh; no `rm -rf`,
  `nohup`, `&`; Edit for shared files. Single process (41 volumes take about 2 minutes).

## Tests

`tests/test_analysis_folio_map.py` (synthetic volumes, no layer files): printed hit; interpolation across a gap of 3
unnumbered scans; a blank scan inside the numbered run shifts positions (scan positions, not side positions); an
out-of-order anchor dropped by the chain; verso anchors dropped in foliation; pagination mode (recto share 0.5);
`exclude` ignored; extrapolation 6 beyond the end located, 7 → None; `min_chain` → `none`; round half up (`s = 4.5` → 5);
`back` = next scan's verso, None when the next scan has no verso; `table()` row count and keys.
`tests/test_analysis_index_links.py`: `front_index_run` on synthetic flags (each condition fails once; a gap of 19 does
not end the run); `read_volume_inputs` on a 2-scan synthetic layer written with `write_tsv_gz` in `tmp_path`.
Commands: the two files, then the full suite `.venv/bin/python -m pytest -q -p no:cacheprovider` (before this step: 1,358
passed, 11 skipped, if 20a did not change the count; take the current number from `STATUS.md`).

## Acceptance criteria

1. `front_index_run` reproduces the planning table (41 volumes; equal `front_first`/`front_last`).
2. LOO pooled exact share ≥ 0.97 (floor). Planning measured it under this definition with
   `reports/data/phase-14-index-links/planning/loo_check.py`: 0.9858 (21,030 / 21,334; lowest 3611 0.925, 3265 0.928,
   3712 0.946). Your number must reproduce it within 0.002; below 0.97 → stop and report.
3. **Bar F** (pre-registered): test hit share ≥ 0.95 (rows as defined). Report numerator and denominator, dev share,
   and the share per method. Fail → negative result: report, no change to the rules, STATUS says 20e waits for the user.
4. New tests and the full suite green.

The user decides when the step is done.

## Error analysis

Read every miss of bar F (dev and test) and every LOO miss in the dev volumes and 3712 (all of them, if fewer than 30;
else 30 at `random.Random(20261131)`): show the chain anchors around the position (their segment text and
`type_method`) and the scans near it with `show_scan.py` from `reports/data/phase-14-index-links/step-20c/`. Classify:
`chain error` (a wrong number in the chain), `skip/duplicate` (numbering skips or repeats a folio), `blank scans`,
`restart` (a second numbering run), `beyond chain`, `gold error`, `other`. Also read 3712's numbering and say what it is.
Draft findings: LOO share, bar F result, the miss categories with counts, anchors per recto (coverage).

## Do not touch

`page_numbering.py`, `text_layer/`, every layer, `reports/data/phase-14-index-links/planning/` and `step-20c/` (read
only), the siblings, `shared-data/`, the external drive.

## When done

1. Report: `### Step 20d: folio map` in `reports/phase-14-index-links.md` (draft findings `F14-20d.n`); `make reports`
   clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 20d: folio map
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `analysis.folio_map` (`FolioMapParams`, `FolioTarget`, `FolioMap`, `build_folio_map`, `NUMBER_PATTERN`); `analysis.index_links` (`front_index_run`, `VolumeInputs`, `read_volume_inputs`)
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-14-index-links.md` § Step 20d](../../reports/phase-14-index-links.md#step-20d-folio-map); findings F14-20d.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```

3. Overwrite the phase `STATUS.md`: 20d state; for 20e: the bar F result, the map is frozen.
