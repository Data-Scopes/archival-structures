# Step 22b: The folio map on VOC: unchanged, then VOC options (bar F on the Generale Missiven gold)

> **Phase:** 16, A second archive (1.04.02 VOC): what generalises. **Depends on:** 22a (the layer).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

The folio map (`analysis/folio_map.py`, phases 14–15) turns the printed page numbers of a volume into "folio N → scan".
It was built for SG hand volumes. There each scan is an opening (verso + recto), folio numbers sit on rectos, and
the map passed bar F on SG gold (62/63 in phase 15). The VOC Generale Missiven (GM) overview gives, per missive, the
begin/end **folio and scan**: 1,841 folio ↔ scan anchors in 545 inventories, read by people, at no reading cost.

This step:
1. measures the map **unchanged** (map v1 and map v2) on that gold;
2. tries a fixed grid of two VOC options on dev;
3. tests the selected cell once;
4. answers two archive questions: does VOC foliation sit on rectos, and does it restart within a volume?

## Inputs

Paths are relative to the AS root, checked 2026-10-31.

- **Layer (from 22a):** `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/`. The local link points to
  the drive; a hook denial is final. Take the exact root and the list of `print` volumes from `STATUS.md`. Tables:
  - `scans/<inv>.scans.tsv.gz` (`scan_id`, …);
  - `layout/pages/<inv>.tsv.gz` (`scan_id page … index_side`), written for `hand` volumes only;
  - `layout/lines/<inv>.tsv.gz` (`scan_id line_id region_id page …`);
  - `lines/<inv>.lines.tsv.gz` (`scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw
    text edit_flags split_from`);
  - `types/segments/<inv>.tsv.gz` (`scan_id line_id seg_idx char_start char_end … page layout_region_id line_type
    type_method …`);
  - `regions_meta/<inv>.tsv.gz` (`scan_id page_side region_id region_xml_id parent_region_xml_id region_tag …`).
  Scan ids have no `.jpg`, e.g. `NL-HaNA_1.04.02_1068_0021`.
- **Gold (frozen at planning; never re-derive it):** `reports/data/phase-16-voc-generalisation/planning/gm_anchors.tsv`
  (sha256 starts `a2b875d9b06e9972`). Columns `inv gm_id kind folio scan layout split`:
  - one row per distinct (inv, folio, scan) from the begin/end of each missive; `scan` is the scan number;
  - 1171 excluded;
  - dev: 937 anchors in 272 inventories (308 `Dubbel`, 629 `Enkel`); test: 904 in 273 (310 `Dubbel`, 594 `Enkel`);
  - split by inventory, stratified by layout, seed 20261031 (`planning/gm_split.tsv`).

  The gold scan id is `f"NL-HaNA_1.04.02_{inv}_{scan:04d}"`. A gold scan id that is not in the volume's scans
  table is **unreadable input**: count it, and keep it out of the denominator.
- **Code (current):**

```python
# archival_structures/analysis/folio_map.py
NUMBER_PATTERN = r"^\D{0,3}(\d{1,4})\D{0,3}$"
@dataclass(frozen=True)
class FolioMapParams:
    number_pattern: str = NUMBER_PATTERN; foliation_recto_share: float = 0.9; min_chain: int = 3
    extrapolate: int = 6; edge: int = 6; bands: tuple = CONFIDENCE_BANDS
    jump_k: Optional[float] = None; jump_c: float = 10.0      # G1
    dup_window: Optional[int] = None                           # G2
    gap_slack: Optional[float] = None                          # G3
    opening: bool = False                                      # G4
FOLIO_MAP_V2 = FolioMapParams(jump_k=1.0, jump_c=10.0, dup_window=2, gap_slack=2.0, opening=True)
def build_folio_map(scan_ids, sides, numbers, *, exclude=(), params=FolioMapParams()) -> FolioMap
# FolioMap.locate(N) -> Optional[FolioTarget(number, front: (scan_id, page), back, position, method, anchor_dist,
#                                            confidence, opening)]
```

  `build_folio_map` (anchor `def build_folio_map(`) does the following:
  - **Anchors:** the first `numbers[side]` text matching `number_pattern` (after `.strip()`) gives one anchor per
    side; sides in `exclude` are skipped.
  - **Mode:** `share` = recto anchors / anchors. `foliation` iff share ≥ 0.9: the points are recto anchors at **scan**
    positions. Otherwise `pagination`: all anchors at **side** positions.
  - **Chain:** G2 (if on), then `longest_increasing_chain`, then G1 (if on); fewer than `min_chain` points → `none`.
  - **Targets** (`FolioMap._target`): foliation front = (scan, `recto`, or `single` if the scan has only that side);
    back = (next scan, `verso`) if that side exists. Pagination front = `sides[position − 1]`, back None.

  On VOC single-page scans every side is `single`, so the share is 0, so the volume is always `pagination`.
  **Correction after 22a (Opus, 2026-11-01):** not every VOC volume is single-page. 22a's
  `reports/data/phase-16-voc-generalisation/step-22a/sides.tsv` (`inv single verso recto gm_layout agrees`) shows
  that the side rule splits 143 of 202 `Dubbel` volumes and 3 `Enkel` ones (1603, 2770, 3366) into verso/recto
  openings, as in SG; there the unchanged map can already be `foliation`. 57 `Dubbel` volumes (all of 7528–7599,
  1122, 1241, …) are portrait single-page scans. So report every share below also per **side layout**:
  `openings` if (verso + recto) ≥ 0.5 × all sides in `sides.tsv`, else `single`; keep the per-GM-label shares as
  well. The GM label is not a scan layout.

```python
# archival_structures/analysis/index_links.py
def front_index_run(flags, *, min_flagged=10, max_first=10, gap=20, min_after=300, max_share=0.6)
    -> Optional[tuple]   # (first, last, end), 0-based side indices
def read_volume_inputs(layer_root: Path, inv: str) -> VolumeInputs   # scan_ids, sides, flags, numbers
# numbers[(scan_id, page)] = the page_number segment texts (types/segments line_type == "page_number"), sliced from
# lines.text by char_start:char_end, in segment-table order. FileNotFoundError without layout/pages.
# archival_structures/analysis/page_numbering.py
def interpolate_folio_to_scan(folio, run, extrap=False, max_scan=None, *, edge=6)  # run (n,2) [position, number]
def numbering_restarts(anchors, keep, min_len=3, max_rounds=4)   # extra monotone runs off the main chain
def main_chain(anchors) -> (chain, keep)                       # anchors (n,2) [position, number]
```

  LOO, as in phase 15 (`reports/data/phase-15-index-links-v2/step-21b/eval_map.py`, anchor `def loo(chain)`): for each
  interior chain point, interpolate its number on the chain without it; a hit is `floor(s + 0.5) == position`. Copy
  `loo` and `wilson` from there.
- **Planning prototype** (dev only; do not open its output before your dev run). Anchors from v1 Laypa `page-number`
  regions, scan positions, LIS, interpolation (`planning/gm_split_and_proto.py`): dev leaf share 0.647 (606/937),
  strict 0.505. The planner read 8 misses:
  - the same number on two consecutive `Dubbel` scans (2970 '76' at 87 and 88; 2171 '388' at 398 and 399);
  - an HTR misread (3559 '1302' for 1102);
  - a prefix the pattern refuses ('sur 311');
  - no number on 1–4 scans around the gold (1752, 2392).

  `numbering_mode` slopes: 167 of 170 dev `Enkel` volumes come out about 2 scans per number; 95 of 101 `Dubbel`
  about 1.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/folio_map.py` | `FolioMapParams.single_foliation: bool = False`, `single_slope: float = 1.5` (below); default behaviour unchanged |
| `archival_structures/analysis/index_links.py` | `read_volume_inputs(layer_root, inv, *, number_tags: Collection[str] = ())` (below); default unchanged |
| `tests/test_analysis_folio_map.py`, `tests/test_analysis_index_links.py` | the new tests below |
| `docs/index_links.md` | a short "VOC options (phase 16)" paragraph for both parameters |
| `docs/shared_library/changelog.md` | `### Step 22b: …` under `## Unreleased`, added with Edit; additive, no sibling import affected |
| `reports/data/phase-16-voc-generalisation/step-22b/` | `eval_gm.py`, `volumes.tsv`, `dev_grid.tsv`, `select.json`, `test_done.json`, `gold_eval.tsv`, `loo.tsv`, `restarts.tsv`, `misses_read.tsv` |
| `reports/phase-16-voc-generalisation.md` | `### Step 22b: …` section, draft findings `F16-22b.n` |

## Behaviour to implement

**1. `single_foliation` (S).** In `build_folio_map`, after the anchors are collected:
1. If `params.single_foliation` and `n_anchors > 0`, and the share of anchors on `single` sides is ≥
   `params.foliation_recto_share`, take `pts = sorted((scan position, number))` over the `single` anchors.
2. Run `longest_increasing_chain` on the numbers. If that chain has ≥ `min_chain` points and the median of
   Δscan/Δnumber over its consecutive points is ≥ `params.single_slope`, the volume is `foliation` with those
   `pts`. G2, the LIS and G1 then run as usual on the scan positions.
3. Else, nothing changes.

In `_target`, foliation branch: when `single_foliation` is on and `(next scan, "single")` is in `sides`, back = that
side (today back needs a `verso`). `recto_share` keeps its definition. With `single_foliation=False` every output is
identical to today's.

**2. `number_tags` (N).** In `read_volume_inputs`: for every line whose `region_id` has a `region_tag` in
`number_tags` (`regions_meta`), append the line's full `text` to `numbers[(scan_id, page)]`. The page comes from
`layout/lines`; use `lines`-table order, and skip a text already in that side's list. These go after the
`page_number` segment texts. With `number_tags=()` the function reads exactly what it reads today; `regions_meta` is
then not read.

**3. The evaluation** (`step-22b/eval_gm.py`, single process). For every inventory in `gm_anchors.tsv`:
1. `v = read_volume_inputs(LAYER, inv, number_tags=…)`. A volume without `layout/pages` is a `print` volume: every
   anchor there is a miss with method `no_pages`.
2. `run = front_index_run(v.flags)`; `excl = {sides with index ≤ run[1]} if run else set()`, plus every flagged side.
   That is SG's exclusion, unchanged.
3. `fmap = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=P)`.
4. For each anchor: `t = fmap.locate(folio)`. If `t` is None, it is a miss (method `none`). Else `pos` = index of
   `t.front[0]` in `v.scan_ids`; **leaf** = `{scan_ids[pos], scan_ids[pos + 1]}` (the second only if it exists).
   - **leaf hit:** gold scan id ∈ leaf (folio N's recto and verso, in both layouts);
   - **strict hit:** gold == `t.front[0]`.

**Grid (fixed at planning; 8 cells):**
- M ∈ {`V0` = `FolioMapParams()`, `V2` = `FOLIO_MAP_V2`};
- S ∈ {off, on} (`single_foliation`);
- N ∈ {`T` = `()`, `TL` = `("page-number",)`}.

The two **unchanged cells** are (V0, off, T) and (V2, off, T). Build S=on as `dataclasses.replace(M,
single_foliation=True)`.

**4. Dev run, selection, test.**
1. Run all 8 cells on **dev** → `dev_grid.tsv`, with leaf and strict shares overall, per GM layout and per side
   layout.
2. **Selection:** max dev leaf share. Ties within 0.002 go to the cell with fewer changes from (V2, off, T), counting
   S and N; then to V2 over V0.
3. Write `select.json` (cell, dev numbers, time) **before** any test number exists.
4. Then run on **test**, **once**: the selected cell and the two unchanged cells. Write `test_done.json` and
   `gold_eval.tsv` (`inv split layout kind folio gold_scan cell pred_scan method confidence leaf strict`).
5. **Bar F (VOC)** = test leaf share ≥ 0.95. Give numerator, denominator and the Wilson interval for each of the three
   test cells, strict share beside them, and shares per layout and per `method`.
6. **Verdict for the generalisation table:**
   - `unchanged` if (V2, off, T) passes;
   - `reparameterised` if only the selected cell passes;
   - `fails on VOC` if none passes.

**5. Archive questions** (all 545 volumes, no gold needed; selected cell):
- **Rectos.** For `openings` volumes (side layout, above), the distribution of `recto_share` from (V2, off, T). For
  `single` volumes, the share of consecutive chain points one number apart that are exactly 2 scans apart.
- **Restarts.** Take `numbering_restarts` on the volume's anchors (all numbered sides, scan positions, in scan order,
  keep mask from `main_chain`). A volume has a candidate restart if it has an extra run with ≥ 10 anchors →
  `restarts.tsv`. Then list the 11 GM volume parts where the folio decreases along scans: 1269, 1279, 1341, 1363,
  1408, 1614, 1696, 2394, 2550, 2808, 3987. Classify each with the layer's numbers around the decrease as a numbering
  restart, a nested or duplicate registration, a gold error, or unclear.
- **LOO** over each volume's chain (selected cell, dev volumes) → `loo.tsv` and the pooled exact share (SG: 0.9858).

## Constraints

- The library defaults stay byte-for-byte: `FolioMapParams()`, `FOLIO_MAP_V2` and `read_volume_inputs(root, inv)`
  behave as now. Check it with the existing tests and with one SG volume: `read_volume_inputs` on
  `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5`, inv 3444, before and after your edit
  (equal objects).
- No parameter search beyond the 8 cells. The test split runs once, after `select.json`; a failed bar is a negative
  result, with no change afterwards.
- No gold in library code; the map never sees `gm_anchors.tsv`. Do not open `planning/gm_proto_dev.tsv` or
  `planning/toc_peek_dev.tsv`.
- Never `git commit`; `chmod g+w`; zsh; no `rm -rf`, `nohup`, `&`, `sed -i`; Edit for shared files. If you use more
  than one worker, launch through `coreshare.py run` (status first). One process should do: 545 volumes × 8 cells
  read the layer once per N.
- Step 22c may run at the same time: edit only your own report section and your own `STATUS.md` lines.

## Tests

- `test_analysis_folio_map.py`:
  - `single_foliation` on a synthetic volume of 12 `single` sides numbered 1..6 on every other scan →
    `foliation`, `locate(4).back == (next scan, "single")`;
  - the same volume with the option off → `pagination` (today's result);
  - single sides numbered on every scan (slope 1) with the option on → `pagination`;
  - a mixed `verso`/`recto` volume gives the same map with the option on and off.
- `test_analysis_index_links.py`: `read_volume_inputs(..., number_tags=("page-number",))` on a synthetic 2-scan layer
  written with `write_tsv_gz` in `tmp_path` (add a `regions_meta` and a `layout/lines` table). A Laypa line text is
  appended after the segment text, a duplicate is skipped, and without `number_tags` the old result comes back.
- Commands: the two files, then the full suite (before 22a: 1,402 passed, 11 skipped; take the current number from
  `STATUS.md`).

## Acceptance criteria

1. New tests and the full suite green; the SG check object is unchanged.
2. `dev_grid.tsv` has 8 rows over 937 dev anchors (minus unreadable, counted); `select.json` is time-stamped before
   `test_done.json`.
3. Bar F (VOC) reported for the 3 test cells, with numerator/denominator and Wilson intervals, and the verdict.
4. The rectos, restarts and LOO numbers, and all 11 decreasing-folio parts classified.

The user decides when the step is done.

## Error analysis

Read 40 dev misses of the selected cell (seed 20261031) and every test miss of the selected cell, up to 30. For each,
look at the layer's page-number texts (segment texts and Laypa `page-number` line texts) on the gold scan ± 3.
Classify it:
- gold error (the printed number on the gold scan contradicts the gold folio);
- HTR misread;
- no number near the gold (gap);
- a tied or duplicated number;
- restart or second numbering;
- extrapolation beyond the chain;
- a side/layout error;
- the front matter excluded wrongly;
- unclear.

Write `misses_read.tsv` (`read_id inv folio gold_scan pred_scan category note`).

**User check.** If you classify ≥ 5 misses as gold errors, build a check page for the user with the shared module
`/Users/Shared/claude_projects/data_scopes/shared-tools/checkpage.py`; follow the checklist in its README section
"checkpage":
- up to 20 of those misses;
- Nationaal Archief viewer links for the gold scan and the predicted scan;
- the question "which scan carries folio N?".

Record it as a Decide UA in `docs/USER_ACTIONS.md` (next free id; format in `CLAUDE.md`) that **blocks nothing**. The
bar stands as measured; the check only says how much of the miss rate is gold noise.

Draft findings are expected for:
- bar F unchanged (V0, V2) and selected;
- what fails, by category with counts;
- rectos in both layouts;
- restarts;
- whether Laypa `page-number` regions help (N);
- LOO.

## Do not touch

The text layer and every layer under `data/`, `presets.py`, `layout.py`, `types.py`, `page_structure.py`,
`index_pages.py`, the ground-truth files, `volume_links`/`extract_locators` (22d), NLP and LotA.

## When done

1. Add `### Step 22b: The folio map on VOC` to `reports/phase-16-voc-generalisation.md` (template
   `plan/templates/REPORT.md`), with findings `F16-22b.n` (status `draft`). Run `make reports` and fix any warning.
2. Append the `PROGRESS.md` block (template in `plan/templates/STEP.md`).
3. Update `STATUS.md` with Edit. Record under *Facts later steps need*:
   - the selected cell (M, S, N), **frozen**: 22d uses it;
   - the bar result;
   - the test counts.
