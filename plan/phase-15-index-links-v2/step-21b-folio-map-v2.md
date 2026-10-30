# Step 21b: folio map v2 (jump guard, tied numbers, gap-aware confidence, opening side)

> **Phase:** 15, Index links v2. **Depends on:** 21a, but only for the bar-F run. Everything else can start at once.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

`analysis/folio_map.py` (phase 14) maps a printed folio number to its scan from the page-number segments of a v5
text layer. Its bar F failed by one row (33/35). The phase-14 review found three map defects, and this step fixes them
with rules that were fixed at planning. **There are no variants and no tuning.**

1. **Year- or inventory-number-like chain ends.** In 6 of the 41 dev volumes the chain ends on a number such as 1659
   or 3522, three or four scans before the end of the volume. Every number between the last real anchor and that
   value is then interpolated onto the last scans with `high` confidence (94 links).
2. **Duplicated printed numbers.** The HTR reads the same number on two nearby rectos (3590: `23` on 0070 and 0072,
   the second a misread of 25). The chain keeps the later one, so folio 23 lands two scans off.
3. **Confidence sees only the scan distance**, not whether the bracketing anchors are consistent.

A fourth change adds the **opening** side: the verso of folio N's own scan, which faces folio N.

All changes are additive. With default parameters the map behaves exactly as in v1. The new constant `FOLIO_MAP_V2`
switches them on.

## Inputs

- `archival_structures/analysis/folio_map.py` (130 lines, as checked 2026-10-30; read it whole). Current API:

```python
NUMBER_PATTERN = r"^\D{0,3}(\d{1,4})\D{0,3}$"
@dataclass(frozen=True)
class FolioMapParams:
    number_pattern: str = NUMBER_PATTERN; foliation_recto_share: float = 0.9; min_chain: int = 3
    extrapolate: int = 6; edge: int = 6; bands: tuple = CONFIDENCE_BANDS          # (3, 10)
@dataclass(frozen=True)
class FolioTarget:
    number: int; front: tuple; back: Optional[tuple]; position: int; method: str; anchor_dist: float; confidence: str
@dataclass
class FolioMap:
    mode: str; chain: np.ndarray; n_anchors: int; recto_share: float; scan_ids: tuple = (); sides: tuple = ()
    params: FolioMapParams = FolioMapParams()
    def locate(self, number: int) -> Optional[FolioTarget]: ...
    def table(self) -> list: ...   # columns number front_scan_id front_page back_scan_id back_page position method anchor_dist confidence
def build_folio_map(scan_ids, sides, numbers, *, exclude=(), params=FolioMapParams()) -> FolioMap
```

  How v1 works, in order (from the code):
  1. One anchor per non-excluded side: the first `numbers[side]` text matching the pattern.
  2. `share` = the share of anchors on a recto; the mode is foliation iff `share >= 0.9`.
  3. In foliation the points are the recto anchors as `(scan_pos, n)`; in pagination they are all anchors as
     `(side_pos, n)`. The points are then sorted.
  4. `longest_increasing_chain` (strict LIS; for an equal value the later one wins).
  5. Mode `none` if the chain has fewer than `min_chain` points.
  6. `locate` works like this:
     - an exact chain hit gives `printed` / `high`;
     - a number outside `[first − 6, last + 6]` gives `None`;
     - otherwise it calls `interpolate_folio_to_scan(number, chain, extrap=True, max_scan=n_positions, edge=6)`, rounds
       half up and clamps;
     - the method is `interpolated` inside the chain and `extrapolated` outside, and the confidence is
       `confidence_band(dist, inside, bands)`, where dist is the distance to the nearest anchor.
- From `archival_structures/analysis/page_numbering.py`, used as they are:
  `longest_increasing_chain(values) -> list[int]` (indices), `interpolate_folio_to_scan(folio, run, extrap=False,
  max_scan=None, *, edge=6) -> (s, dist, inside)`, and `confidence_band(dist, inside, bands=(3, 10)) -> str`
  (`low` if not inside, else `high` if dist ≤ 3, `medium` if dist ≤ 10, else `low`).
- **Reference implementation** (planner prototype, 2026-10-30):
  `reports/data/phase-15-index-links-v2/planning/map_v2_proto.py`, functions `chain_v2` and `locate`. Your library code
  must give the same numbers (acceptance 3); you may read this file. Do **not** open any other file in `planning/` (you run `protect15.py`, below, without reading it).
- Volume inputs: `from archival_structures.analysis.index_links import front_index_run, read_volume_inputs`.
  `read_volume_inputs(layer_root, inv)` returns `.scan_ids, .sides, .flags, .numbers`, and
  `front_index_run(flags)` returns `(first, last, end)` or None. The map's exclude set is the one `volume_links` uses:
  `{s for i, s in enumerate(v.sides) if i <= last or v.flags[i]}`.
- Layer: `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5` (read only). The 41 front-index volumes
  are the `inv` column of `reports/data/phase-14-index-links/planning/front_index_volumes.tsv`; read only that column.
- Gold (after 21a): `reports/data/phase-15-index-links-v2/step-21a/target_gold.tsv` (`target_id read_id split inv entry
  target_locator target_scan_id folio_evidence scans_opened content content_sides evidence note`). Read the `split`
  column, and use the **test** rows only for the one bar run.
- Tests: `tests/test_analysis_folio_map.py` has 13 synthetic tests with the helper `_vol`; no fixture files.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/folio_map.py` | new `FolioMapParams` fields, `FolioTarget.opening`, `FolioMap.n_dup_dropped`/`chain_cut`, `FOLIO_MAP_V2`; G1–G4 below |
| `tests/test_analysis_folio_map.py` | the new tests below; the 13 existing tests unchanged and green |
| `reports/data/phase-15-index-links-v2/step-21b/eval_map.py` | LOO, chain table, v1/v2 link comparison, bar F (below) |
| `…/step-21b/` outputs | `volumes.tsv`, `loo.tsv`, `dev_eval.tsv`, `test_eval.tsv`, `test_done.json`, `eval_map.txt` (stdout) |
| `docs/shared_library/changelog.md` | `### Step 21b: folio map v2` under *Unreleased* (Edit only; see Constraints) |
| `reports/phase-15-index-links-v2.md` | the `### Step 21b: …` section, with draft findings `F15-21b.n` |

## Behaviour to implement

New `FolioMapParams` fields. They go **after** the existing fields, and their defaults reproduce v1:

```python
jump_k: Optional[float] = None    # G1 off when None
jump_c: float = 10.0
dup_window: Optional[int] = None  # G2 off when None
gap_slack: Optional[float] = None # G3 off when None
opening: bool = False             # G4
FOLIO_MAP_V2 = FolioMapParams(jump_k=1.0, jump_c=10.0, dup_window=2, gap_slack=2.0, opening=True)
```

Write a comment above `FOLIO_MAP_V2` that cites phase 15 planning and the reason for each rule (F14-R.3, F14-20d.3,
F14-R.4).

- **Mode decision: unchanged** (the share over all anchors). Do not put a bound before it. The planner tried one, and
  it turned 3712 into foliation with an 8-anchor chain. 3712's numbers continue from the previous volume (348…684 on
  533 sides, on rectos only), so **no count bound** (scan count, side count) may drop anchors either.
- **G2, tied numbers** (before the LIS, when `dup_window` is set). Among the sorted points, take every number that
  occurs at two or more positions where the smallest distance between consecutive occurrences is ≤ `dup_window`.
  Drop **all** points with that number. Both copies are suspect: in 3590 the earlier one is right, and in 3254 the
  case is unclear.
- **G1, jumps** (after the LIS, when `jump_k` is set):
  - compute the consecutive chain differences `dpos`, `dn`;
  - cut the chain after every pair with `dn > jump_k * dpos + jump_c`;
  - keep the longest piece (on a tie, the earliest);
  - then apply the `min_chain` rule to that piece.
- **G3, confidence** (when `gap_slack` is set):
  - `printed` stays `high` and `extrapolated` stays `low`;
  - for an `interpolated` number, take the bracketing anchors: `j = np.searchsorted(chain[:, 1], number)`, then
    anchors `j−1` and `j`. Let `off = |dn − dpos|` between those two anchors;
  - the confidence is `high` iff `anchor_dist ≤ bands[0]` and `off ≤ gap_slack`, `medium` iff
    `anchor_dist ≤ bands[1]` and `off ≤ 2 * gap_slack`, else `low`;
  - leave `confidence_band` itself unchanged.
- **G4, opening:**
  - `FolioTarget` gets a last field `opening: Optional[tuple] = None`;
  - in foliation with `params.opening`, it is `(front_scan_id, "verso")` if that side is in `sides`, else None;
  - in pagination it is always None;
  - `table()` adds the columns `opening_scan_id opening_page` **only** when `params.opening` is true, so the v1 table
    is byte-identical.
- **Diagnostics:** `FolioMap` gets two last fields, `n_dup_dropped: int = 0` (the numbers G2 dropped) and
  `chain_cut: bool = False` (G1 cut the chain). Set them only when the rule is on; 21d writes them to its volume
  table.
- **Docstrings:** state each rule and its rationale in a sentence or two (ports and rules carry their rationale).

**`eval_map.py`** (writes into `step-21b/` only; run it from the AS root). Over the 41 volumes, with v1
(`FolioMapParams()`) and v2 (`FOLIO_MAP_V2`):
1. `volumes.tsv`: `inv mode_v1 mode_v2 chain_v1 chain_v2 last_v1 last_v2 n_scans dup_numbers_dropped loo_v1 loo_v2`.
2. `loo.tsv`: per volume and version, the LOO count, defined as in phase 14. For `i` in `1 … len(chain) − 2`,
   `interpolate_folio_to_scan(chain[i, 1], np.delete(chain, i, axis=0))` (no extrapolation), round half up, and compare
   with `chain[i, 0]`. Do this for every mode except `none`. Print the pooled ratios.
3. **Dev check** (`dev_eval.tsv`): the 21a **dev** targets with `target_scan_id` ≠ `none` and `folio_evidence` ≠
   `unclear`. Columns: `target_id inv N gold_pos v1_pos v1_method v1_conf v2_pos v2_method v2_conf v2_hit`. Positions
   are 1-based rows of the scans table, and `v2_hit` means `v2_pos == gold_pos`.
4. **Bar F, test, once** (`test_eval.tsv`, same columns): the 21a **test** targets under the same filter. Before
   scoring, write `test_done.json` (`{"run_utc": …, "params": asdict(FOLIO_MAP_V2)}`). If that file exists, refuse to
   run the test part. Print `hits/n`, the Wilson 95% interval, and v1's `hits/n` on the same rows (reported only).
   Also report the calibration: hits/n per `v2_conf` value. Report the opening coverage: of the rows whose `content`
   is not `unclear`, how many have the matter on the leaf (`front` or `back` in `content_sides`), how many only on
   `opening`, and how many only on `next`. These two are reported, not bars.

If 21a's test gold is not complete (see `STATUS.md`: hashed), do 1–3, stop with status `partial`, and say so.

## Constraints

- Change no other module. `index_links.py`, the builder and `page_numbering.py` stay as they are.
- `build_folio_map` and `locate` keep their signatures. With default params every v1 output is identical: the 13 old
  tests pass unchanged, and acceptance 2 compares the v1 tables.
- Pattern and thresholds are parameters with named defaults; nothing archive-specific is hardcoded.
- **Changelog:** run `make version`. If the latest tag is `v0.19.0`, add `## Unreleased` above `## v0.19.0` (once) and
  your entry under it. If the latest tag is still `v0.18.0`, the user has not released v0.19.0 yet: stop before
  editing the changelog, and record it under *Open questions* (the user should release first).
- **Protected outputs:** every dev layer, `data/index_links_dev/`, `data/index_links_full/`, the phase-14 and phase-15
  report data (21a's gold included), `shared-data/` and the sibling outputs. Before your first run and after your last
  run, list them with `.venv/bin/python -I reports/data/phase-15-index-links-v2/planning/protect15.py
  reports/data/phase-15-index-links-v2/step-21b/protect_before.txt --skip
  reports/data/phase-15-index-links-v2/step-21b` (and the same with `protect_after.txt`). It takes about 8 s. Then
  `diff` the two lists. 21a's files differ only if 21a is still reading the test sides; name such differences.
- The agent shell: no `rm -rf`, `nohup` or `&`; zsh (`echo ==` fails); no `sed -i`; `chmod g+w` new files; never
  `git commit`, `git stash`, `checkout` or `reset`.

## Tests

Add to `tests/test_analysis_folio_map.py` (synthetic, using `_vol` or small explicit inputs):
- `test_v2_jump_cuts_year_end`: a foliated chain `1…40` plus a final anchor `1659` three scans later. With v2, the
  chain ends at 40, and `locate(1000)` is None. With v1 the chain ends at 1659.
- `test_v2_jump_keeps_longest_piece`: two pieces split by a jump; the longer one is kept, and on a tie the earlier.
- `test_v2_tied_numbers_dropped`: the anchors `21@68, 22@69, 23@70, 23@72, 26@73`, with no anchor at 71 (the 3590
  case). With v1, the chain keeps `23@72` and `locate(23)` is printed at 72. With v2, both 23s are dropped and
  `locate(23)` is interpolated at 70.
- `test_v2_tie_outside_window_kept`: the same number 6 positions apart is kept (LIS behaviour as in v1).
- `test_v2_confidence_sees_gap`: the anchors `8@8, 9@9, 10@10, 21@12, 22@13`. The bracket `10@10, 21@12` has
  `dn = 11 ≤ 1·2 + 10`, so G1 does not cut it, and `off = 9`. Number 15 is `high` under v1 and `low` under v2.
- `test_v2_opening_side`: foliation gives `opening == (front_scan, "verso")` when that side exists, and None when it
  does not or in pagination. `table()` has the two extra columns only with `opening=True`.
- `test_v2_continuing_numbers_not_bounded`: numbers starting at 348 on a 30-scan volume keep their chain (the 3712
  case).

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_folio_map.py`, then the full suite.
Before 21b the suite collected 1,392 tests (1,381 passed, 11 skipped). Take the expected count from
`pytest --collect-only -q`.

## Acceptance criteria

1. The new tests pass. The full AS suite is green: 1,381 + new passed, 11 skipped.
2. **v1 unchanged:** for each of the 41 volumes, `FolioMap.table()` with default params equals the
   `folio_map/<inv>.tsv.gz` of `data/index_links_dev/20r-a/NL-HaNA_1.01.02_index_links_v1/` row for row. Compare the
   decompressed rows as strings; `anchor_dist` is written with 2 decimals.
3. **Planner numbers reproduce** (measured on `19e-a`, 2026-10-30):
   - LOO v1 = 21,010/21,334 (0.9848), as in phase 14; LOO v2 = 20,952/21,176 (0.9894);
   - 166 tied numbers dropped in 29 of the 41 volumes;
   - the chain changes its last number in exactly 7 volumes:
     - 3126: 1591 → 356;
     - 3265: 1659 → 822;
     - 3290: 1674 → 696;
     - 3307: 1683 → 642;
     - 3320: 1689 → 987;
     - 3471: 485 → 457;
     - 3522: 3522 → 550;
   - 3712 stays `pagination`, with a chain of 203 (v1 205) ending at 684;
   - no volume changes mode.

   A difference means your code and the reference disagree. Find out which is wrong before going on, and report it.
4. Bar F (test, once): v2 predicted front scan = gold scan for **≥ 0.95** of the filtered test targets. Pass or fail
   is reported with `hits/n` and the Wilson interval. **A fail is a negative result. Do not change a rule after the
   test run.**
5. Protected trees unchanged (diff of `protect_before.txt` and `protect_after.txt`).

The user decides when the step is done.

## Error analysis

- Read every test and dev miss of v2: print the page numbers on the 3 scans either side of the gold and the
  predicted scan with `step-21a/show_scan.py`. Classify each miss as one of:
  - garbled or untyped printed number;
  - duplicate (G2 dropped the right one);
  - jump guard (G1 cut real anchors);
  - interpolation across a gap;
  - leaf vs opening (the gold is the opening scan);
  - gold error;
  - other.
- Read every row where v1 and v2 disagree, among the test and dev targets.
- For the 7 changed volumes, check that the dropped chain-end anchors are not folio numbers: print the 4 last
  anchors before and after, and quote them.
- Pick 10 links whose confidence moved from `high` to `low` or `medium` under G3 (seeded `random.Random(21)` over the
  20r-a links of the 41 volumes, located again with v2). For each, judge whether the bracketing anchors are really
  inconsistent.

Write the error analysis into the report.

## Do not touch

`index_links.py`, `scripts/build_index_links.py`, `page_numbering.py`, every layer and `data/index_links_*`, 21a's
files (read only), `planning/` (except reading `map_v2_proto.py`), `shared-data/`, the siblings, the external drive.

## When done

1. In the report, write `### Step 21b: folio map v2` in `reports/phase-15-index-links-v2.md`, with draft findings
   `F15-21b.n`. Then run `make reports` and check it is clean.
2. Append the standard block (template in `plan/templates/STEP.md`, *When done*; the block format is copied below) to
   the phase `PROGRESS.md`:

```markdown
### Step 21b: folio map v2
- **Session:** <model>, <date>. **Status:** done / partial / blocked
- **Files changed:** <paths>
- **Public API:** <FolioMapParams fields, FolioTarget.opening, FolioMap.n_dup_dropped/chain_cut, FOLIO_MAP_V2>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21b](../../reports/phase-15-index-links-v2.md#step-21b-folio-map-v2); findings F15-21b.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```

3. Overwrite the phase `STATUS.md`. Include 21b's state, bar F, and, for 21c/21d, the exact name `FOLIO_MAP_V2` and
   any change in the planner numbers.
