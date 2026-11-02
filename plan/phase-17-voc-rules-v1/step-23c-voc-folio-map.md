# Step 23c: VOC folio map (candidate choice, tied numbers, chain cleaning, pieces, extrapolation)

> **Phase:** 17, VOC rules v1. **Depends on:** dev stage: nothing; test stage: 23a's `gold/folio_gold.done` (the
> user's crop reads, UA-061). May start in parallel with 23a, 23b and 23d.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

`analysis/folio_map.py` maps a folio number N to a scan: the printed page numbers form a longest increasing chain,
and other numbers are interpolated. On VOC it puts the Generale Missiven (GM) gold scan in the leaf (front scan or
the next one) for only 0.654 of the phase-16 test anchors, against bar 0.95 (F16-22b.2).

The read misses (F16-22b.4, F16-R.3, F16-R.8):
- tied or repeated numbers on consecutive scans (16 of 70), including layered sheets;
- several numbers on a side, where the map takes the first;
- restarts and second numberings;
- gaps;
- noisy chains (leave-one-out 0.884 against SG's 0.986);
- volume-start extrapolation.

This step adds five options, all off by default, selects a cell on all phase-16 GM anchors (now dev), and tests it
**once** on the fresh anchors the user reads in 23a.

## Inputs

Paths relative to the AS root, checked 2026-11-01.
- `archival_structures/analysis/folio_map.py` (201 lines):
  - `NUMBER_PATTERN = r"^\D{0,3}(\d{1,4})\D{0,3}$"`;
  - `FolioMapParams` l.25–49 (frozen): `number_pattern=NUMBER_PATTERN, foliation_recto_share=0.9, min_chain=3,
    extrapolate=6, edge=6, bands=CONFIDENCE_BANDS, jump_k=None, jump_c=10.0, dup_window=None, gap_slack=None,
    opening=False, single_foliation=False, single_slope=1.5`;
  - `FOLIO_MAP_V2` l.55;
  - `FolioTarget(number, front, back, position, method, anchor_dist, confidence, opening=None)`;
  - `FolioMap(mode, chain, n_anchors, recto_share, scan_ids=(), sides=(), params=FolioMapParams(), n_dup_dropped=0,
    chain_cut=False)`. `locate(number)` (l.104–124) returns:
    - None when the mode is `none`, N < 1, or N is outside [first − extrapolate, last + extrapolate];
    - `printed` on an exact chain hit;
    - otherwise `interpolate_folio_to_scan(…, extrap=True, edge=p.edge)`, as `interpolated` (inside) or
      `extrapolated`.
  - `build_folio_map(scan_ids, sides, numbers, *, exclude=(), params=FolioMapParams()) -> FolioMap` (l.146–201):
    1. Anchors = the **first** text per side matching `number_pattern`.
    2. foliation iff recto share ≥ 0.9 (else the S branch, else pagination). Points are (scan pos, n) of recto
       anchors in foliation, (side pos, n) of all anchors in pagination.
    3. G2 (`dup_window`), then `longest_increasing_chain` (`analysis/page_numbering.py`, strict), then G1
       (`jump_k`).
    4. The mode is `none` if the chain has fewer than `min_chain` points.
- **Harness to copy:** `reports/data/phase-16-voc-generalisation/step-22b/eval_gm.py`: `Vols` (l.86–103: the
  `read_volume_inputs` cache, with exclude = sides up to the SG `front_index_run` end or flagged), `evaluate`
  (l.111–140: leaf = gold ∈ {front scan, next scan}; strict = gold == front), `loo` (l.64), `wilson` (l.54),
  `side_layouts` (l.78), and `stage_context` (l.362) for the miss dumps. Copy, don't import.
- **Planning prototype:** `reports/data/phase-17-voc-rules-v1/planning/map_proto.py` (with `cache_gm.py`). Its
  options are the ones below.
  - Its `base` reproduces 22b: dev 647/929, test 591/904.
  - Results on the two phase-16 splits (uncorrected gold), dev / test leaf hits out of 929 / 904:

| Option | dev | test |
|---|---|---|
| base | 647 | 591 |
| pick | 663 | 613 |
| ties first | 656 | 592 |
| ties last | 646 | 595 |
| clean 3 | 646 | 595 |
| extrap 30 | 648 | 591 |
| pieces | 648 | 591 |
| all (ties first) | 679 | 625 |
| all (ties last) | 669 | 619 |

  So every option moves the measure, the best by 2 points. Expect the test to fail 0.95 by far.
- **Dev layer:** `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5` (protected). Number tags
  `("page-number",)` (the 22b "TL" choice) in every cell.
- **Dev gold** (P2: all phase-16 GM anchors are dev now): `reports/data/phase-16-voc-generalisation/planning/gm_anchors.tsv`
  (`inv gm_id kind folio scan layout split`; 1,841 rows; one row per distinct (inv, folio, scan); `scan` is a
  number, so the id is `NL-HaNA_1.04.02_<inv>_<scan:04d>`).
  - Apply the 7 user corrections from `reports/data/phase-16-voc-generalisation/step-22b/check_result.tsv`
    (`id split inv folio gold_scan pred_scan verdict correct_scan user_note`): for each row with a non-empty
    `correct_scan`, replace that anchor's scan.
  - Write the copy to `step-23c/dev_anchors.tsv`. **Never edit the frozen file.**
  - Drop anchors whose scan is not in the layer (8 at phase 16). Expect about 1,833 scored.
- **Test layer:** `data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5` (built by 23a; 24 sample volumes).
- **Test gold** (open only in the test stage): `reports/data/phase-17-voc-rules-v1/step-23a/gold/folio_gold.tsv`.
  - Columns: `item_id inv scan_id scan_num side_layout status number left_number beneath_number note`.
  - **Row grain: one row per crop.**
  - Anchors: (inv, `number`, scan_id) for `status == number`, plus (inv, `left_number`, scan_id) where it is
    filled.
  - `beneath_number` is never an anchor.
  - Two crops can carry the same number; score both.
- SG check volume: `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5`, inventory 3444.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/folio_map.py` | new `FolioMapParams` fields (below), `FolioMap.pieces: tuple = ()`, the build and locate rules below; defaults leave every output byte-equal |
| `tests/test_analysis_folio_map.py` | `test_new_fields_default_equal`, `test_pick_by_chain`, `test_tie_keep_first_last`, `test_clean_tol_drops_outlier`, `test_pieces_fallback`, `test_pieces_main_first` (synthetic inputs) |
| `reports/data/phase-17-voc-rules-v1/step-23c/` | `eval_map.py` (stages `sg`, `dev`, `test`, `context`), `dev_anchors.tsv`, `sg_check.json`, `dev_grid.tsv`, `select.json`, `test_done.json`, `test_eval.tsv`, `misses_read.tsv` |
| `docs/index_links.md` | paragraph *VOC folio-map options (phase 17)* after the 22b paragraph (anchor `single_foliation`) |
| `docs/shared_library/changelog.md` | `## Unreleased` above `## v0.21.0` if absent; `### Step 23c: VOC folio-map options`; "no sibling imports affected" |
| `reports/phase-17-voc-rules-v1.md` | `### Step 23c: …`, findings `F17-23c.n` |

## Behaviour to implement

New `FolioMapParams` fields, appended after `single_slope`:
```python
pick_by_chain: bool = False          # 23c: per side, the candidate number closest to a first-pass chain
tie_keep: Optional[str] = None       # 23c: None | "first" | "last" among equal numbers <= tie_window positions apart
tie_window: int = 3
clean_tol: Optional[float] = None    # 23c: drop interior chain points off the line through their neighbours
pieces: bool = False                 # 23c: secondary chains outside the main chain's span; locate falls back
piece_min: int = 10
piece_slope: tuple = (0.5, 3.0)      # allowed median positions-per-number of a piece
```

`build_folio_map`, in order. With all new fields at their defaults, the code path and the output must be today's.
1. **Candidates.** For each side, collect every text matching `number_pattern`, in order. The first-pass anchor is
   the first candidate, as today. The recto share and the mode decision (incl. the S branch) use first-pass
   anchors only.
2. **`_chain(pts)`**, applied to the sorted points:
   1. G2 (existing);
   2. `tie_keep`: a point (pos, n) is kept iff it is the min ("first") or max ("last") position among the points
      with the same n within `tie_window` positions of it. The group is computed per point and is not transitive,
      as in the prototype. Group by n with a dict;
   3. the LIS;
   4. `clean_tol`: if the chain has ≥ 5 points, up to 3 rounds. For each interior point b with neighbours a, c, let
      `step = (c.pos − a.pos)/(c.n − a.n)` and `exp = a.pos + (b.n − a.n)·step`. Flag b if
      `|b.pos − exp| > max(clean_tol, step)`. Drop the flagged points together; stop when none is flagged or fewer
      than 5 points remain;
   5. G1 (existing).
3. **`pick_by_chain`:** if the first-pass chain has ≥ `min_chain` points, give each side with ≥ 2 candidates the
   candidate minimising `|n − np.interp(pos, chain[:,0], chain[:,1])|`. On a tie, take the earlier candidate.
   `pos` is the side's position in the mode's units. Rebuild the points with the same branch rule, and run
   `_chain` again.
4. **`pieces`** (only when the mode is not `none`):
   1. `left` = the points (after `tie_keep`) that are not in the main chain and whose position lies outside
      [chain[0,0], chain[-1,0]].
   2. Up to 3 rounds: stop if `len(left) < piece_min`. Take the LIS of `left`, and stop if it is shorter than
      `piece_min`. If its median Δpos/Δn lies within `piece_slope`, append
      `FolioMap(mode, sub, len(sub), share, scan_ids, sides, params)` to the pieces. Remove the LIS points from
      `left`.
   3. Store the pieces on the map as a tuple.
- **`locate`:** if `self.pieces` is not empty and the main result is None or `extrapolated`, try each piece in
  order. Return the first piece result that is not None and not `extrapolated`, as
  `dataclasses.replace(u, method="piece_" + u.method)`. Otherwise return the main result.

**Grid (fixed):** `pick_by_chain` {F, T} × `tie_keep` {None, "first", "last"} × `clean_tol` {None, 3.0} × `pieces`
{F, T} × `extrapolate` {6, 30} = 48 cells. All other fields are `FolioMapParams()` defaults, and the number tags
are `("page-number",)`. The all-default cell is **BASE** (= the 22b selected cell (V0, off, TL)).

**Stages of `eval_map.py`** (run from the AS root with `-I`; each writes into `step-23c/` only):
- `sg`: on 3444, using `read_volume_inputs` (no tags) and the exclude rule of `Vols`:
  - compare `locate(n)` for n = 0..1,499 of the current module against a `git show HEAD:archival_structures/analysis/folio_map.py`
    copy loaded with importlib from the scratchpad, under `FolioMapParams()` and `FOLIO_MAP_V2` (each module's own
    class);
  - also compare the chain arrays;
  - write `sg_check.json`. Any difference stops the step.
- `dev`:
  - write `dev_anchors.tsv`, then score the 48 cells: leaf, strict, per side layout (`openings` if verso+recto ≥
    0.5 of the sides in `reports/data/phase-16-voc-generalisation/step-22a/sides.tsv`), per method, plus LOO of
    BASE and of the best cell;
  - write `dev_grid.tsv` and `select.json`: the max dev leaf; within 0.002 of the max, fewer non-default options,
    then `extrapolate` 6. Record the cell, the numbers and a UTC time.
- `test`:
  - **Run once.** It asserts that `select.json` exists, that `test_done.json` does not, and that
    `step-23a/gold/folio_gold.done` exists.
  - If the gold is missing but `step-23a/folio_verdicts.json` exists, first run
    `.venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/summarise_folio.py`.
  - If neither exists, stop after `dev` and record "waiting on UA-061" in `STATUS.md`.
  - Score BASE and SEL on the fresh anchors: leaf (the bar), strict, Wilson intervals, strata (side layout, anchor
    kind main/left, method), and an exact two-sided McNemar test of SEL against BASE on the leaf hits:
    `p = min(1, 2·Σ_{k ≤ min(b,c)} C(b+c, k) / 2^(b+c))`.
  - Write `test_eval.tsv` (one row per anchor and cell) and `test_done.json`.
- `context`: dump the page-number texts on gold ± 3 scans and around the predicted scan for the misses to read.

If SEL passes the bar, add `VOC_FOLIO_MAP_V1 = FolioMapParams(<SEL fields>)` with a comment naming the tags
`("page-number",)`, and a test that pins its fields. Otherwise add no constant.

## Constraints

- Additive only: no change to existing fields, defaults, `FOLIO_MAP_V2`, `page_numbering.py` or the
  `index_links.py` call sites.
- **Blindness:** never open `step-23a/dumps/` or `step-23a/reads/`. Open `step-23a/gold/` only in the `test` stage.
  Run nothing on the 23a layer before the `test` stage.
- No rule may shift the front scan to fit GM's begin convention. GM end anchors often sit on the scan after the
  numbered recto (single-page printed ends: 110 at +1, 257 at 0), and 33 single-page begin anchors sit 2 scans
  before. The fresh gold is read where the number is written.
- Protected: run `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py` before the `dev` stage and after the
  `test` stage. Expect no difference apart from 23a's paths appearing (name them).
- Never `git commit`; never bump the version; `chmod g+w` new files. Agent shell: no `rm -rf`, `nohup`, `&` or
  `$TMPDIR`; BSD `sed -i ''`; never `git stash`/`checkout`/`reset`. Single process (no coreshare needed).
- Parallel sessions: edit only your own `STATUS.md` row; shared files with Edit only.

## Tests

- The six tests above.
  - `test_new_fields_default_equal`: `build_folio_map` with the new fields passed explicitly at their defaults gives
    the same `mode`, chain and `locate` as the implicit defaults, on a synthetic 30-side input with ties and two
    candidates per side.
  - `test_pieces_fallback`: 170 `single` sides. Numbers 50..90 on sides 1..81 (every other side) form the main
    chain. A second numbering 1..20 sits on sides 120..158 (every other side). It cannot extend the LIS, so it
    becomes a piece. With `pieces=True`, `locate(5)` returns `piece_printed` at side 128. With `pieces=False`, it
    returns None (5 < 50 − 6).
  - `test_pieces_main_first`: on the same input, `locate(60)` returns the main chain's `printed` result in both
    settings.
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_folio_map.py`, then the full suite (1,424
  collected at planning, plus the new tests of parallel steps).

## Acceptance criteria

1. The new tests and the full suite are green, and `sg_check.json` shows 0 differences.
2. `dev_grid.tsv` has 48 rows over the same anchor count. BASE reproduces 22b on the uncorrected rows: report
   BASE's hits on the phase-16 dev and test splits **before** the corrections (expected 647/929 and 591/904); a
   mismatch stops the step.
3. `select.json` is time-stamped before `test_done.json`.
4. **Bar F (VOC):** SEL's leaf share ≥ 0.95 on the fresh anchors (Wilson reported). Also report BASE, the McNemar
   b, c and p, and the strict share. A miss is a negative result; no retuning.

## Error analysis

- **Test:** every SEL miss, up to 30 (seeded 20261101 if more), with the context dump and the user's note.
  Categories: tied or duplicated number; restart or second numbering; gap; several numbers on a side; side or layout
  offset; extrapolation; HTR misread; layered sheet (the crop has a `beneath_number`); unclear.
- Every BASE/SEL discordant anchor, up to 20: which option made the difference.
- **Dev:** 15 anchors SEL fixes against BASE and 15 it breaks (seeded): which option, and was the fix real.
- Expected findings: the selected options and their dev gain; bar F on fresh gold; whether the fresh gold confirms
  the GM gap (BASE fresh against BASE GM); the miss classes on image gold.

## Do not touch

`index_links.py`, `index_pages.py`, `presets.py`, the layers, and the 23a, 23b, 23d, 23e step dirs (except reading
`step-23a/gold/` in `test`).

## When done

1. Add `### Step 23c: VOC folio map` to `reports/phase-17-voc-rules-v1.md` (results, error analysis, findings
   `F17-23c.n`, status `draft`), then run `make reports`.
2. Append the `PROGRESS.md` block (template `plan/templates/STEP.md`).
3. Overwrite only the 23c row of `STATUS.md`. Under *Facts later steps need*, add the SEL fields (23e reads them).
   If only `dev` ran, mark the row `dev done, test waiting on UA-061` and still record SEL.
