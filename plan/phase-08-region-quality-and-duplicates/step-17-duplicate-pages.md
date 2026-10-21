# Step 17: Duplicate-page detector (C17)

> **Phase:** 08, Region quality and duplicates. **Depends on:** nothing (16 may run before or
> after). **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, this file, then LotA's `CLAUDE.md` (you edit LotA in Part B). **Read nothing else
> unless this brief names it.** If you have to explore anyway, record it under *Brief defects*.

## Context

LotA's corpus-integrity pass finds re-shot pages with a hand-rolled MinHash/LSH over character
5-gram shingles. It confirms candidates by exact Jaccard and always adds pairs from adjacent
scans. It found 13/13 of the documented SG re-shoots. The algorithm is generic, so AS takes it as
`analysis/duplicate_pages.py`, over plain `(page_id, text, position)` triples. LotA keeps its page
splitter (`page_units`), its manifest and its CLI, and calls AS. This is a pure port: a full SG
run must give a byte-identical candidate table.

## Inputs

`L=/Users/Shared/claude_projects/data_scopes/language-of-the-archive`, with `venv/bin/python`
(3.14; it imports AS from the AS working tree). Checked 2026-10-21; no uncommitted changes:
- `L/archival_language/corpus/integrity.py` (554 lines):
  - constants L58–72; `_shingles` L403, `_page_shingles` L409;
  - `_minhash_coefficients` L418, `_COEFF_A, _COEFF_B` L425, `_minhash_sketch` L428;
  - `_lsh_candidates` L442, `duplicate_candidates` L459–554;
  - also `page_units` L308 and `inventory_manifest` L374 (it calls `duplicate_candidates` at
    L388);
  - imports include `bisect, random, statistics, zlib, numpy as np, pandas as pd`.
- Other callers: `cli/corpus_integrity.py` L34–40 (imports `duplicate_candidates` and
  privates `_build_pages, _malformed_count_for_inventory, _manifest_from_parts`, which stay), L107.
  `tests/test_corpus_integrity.py` (16 tests) imports `DEFAULT_DUPLICATE_THRESHOLD`,
  `duplicate_candidates` and `page_units`. Nothing outside `integrity.py` uses `_shingles`,
  `_minhash_*`, `_lsh_candidates` or the private constants.
- `PageUnit` (LotA `model/page.py`) has `.page_id` (`f"{scan_id}-page-{n}"`), `.scan_number`
  (int or None) and `.region_ids` (in source row order).
- Stored outputs (5 Oct, all 9,122 SG inventories; 3,994.5 s; peak RSS ~4.9 GiB):
  - `L/output/integrity/1.01.02-duplicate-candidates.tsv`: 48,473 rows, 4,228 inventories,
    14,994 adjacent; 9 rows have no `scan_number_b`, so that column is float;
  - `L/output/integrity/1.01.02-manifest.tsv`: 9,122 rows.
- Input: `L/data/paragraphs/NL-HaNA_1.01.02`, a symlink to `shared-data/…/paragraphs/source`
  (9,122 local `.tsv.gz` files).
- Planning scripts in `reports/data/phase-08-region-quality-and-duplicates/planning/` (they
  write nothing; see their `README.md`):
  - `dup_check.py INV…`, run from `L` with LotA's venv: compares fresh `duplicate_candidates`
    rows with the stored table. At planning, 7/7 were identical: 3779 4, 3767 5, 3822 6, 638 2,681,
    1576 87, 2588 5 and 4000 1 rows;
  - `reshoots.py TSV`, run with AS `.venv`: at planning, 13/13 found, 23 rows, all adjacent.

The LotA code to port, verbatim:

```python
_SHINGLE_LENGTH = 5; _MIN_SHINGLES_FOR_COMPARISON = 100; _NUM_MINHASHES = 128; _LSH_BANDS = 16
_LSH_ROWS = _NUM_MINHASHES // _LSH_BANDS; _MINHASH_PRIME = (1 << 31) - 1; _MINHASH_SEED = 2026_10_04
DEFAULT_DUPLICATE_THRESHOLD = 0.6
def _shingles(text, k=_SHINGLE_LENGTH):
    if len(text) < k: return {text} if text else set()
    return {text[i : i + k] for i in range(len(text) - k + 1)}
def _minhash_coefficients(n, *, seed):
    rng = random.Random(seed)
    a = np.array([rng.randrange(1, _MINHASH_PRIME) for _ in range(n)], dtype=np.uint64)
    b = np.array([rng.randrange(0, _MINHASH_PRIME) for _ in range(n)], dtype=np.uint64)
    return a, b
def _minhash_sketch(shingles):
    if not shingles: return tuple(int(_MINHASH_PRIME) for _ in range(_NUM_MINHASHES))
    base = np.array([zlib.crc32(s.encode("utf-8")) % _MINHASH_PRIME for s in shingles], dtype=np.uint64)
    permuted = (base[:, None] * _COEFF_A[None, :] + _COEFF_B[None, :]) % np.uint64(_MINHASH_PRIME)
    return tuple(int(v) for v in permuted.min(axis=0))
def _lsh_candidates(sketches):          # dict page_id -> sketch
    candidates = set()
    for band in range(_LSH_BANDS):
        start, end = band * _LSH_ROWS, (band + 1) * _LSH_ROWS
        buckets = {}
        for page_id, sketch in sketches.items(): buckets.setdefault(sketch[start:end], []).append(page_id)
        for ids in buckets.values():
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    a, b = ids[i], ids[j]; candidates.add((a, b) if a < b else (b, a))
    return candidates
# duplicate_candidates(source, inv, *, adapter, threshold) body
    regions = list(source.iter_regions(inv)); pages = page_units(regions, adapter=adapter)
    regions_by_id = {r.region_id: r for r in regions}
    page_shingle_sets = {}; n_excluded = 0
    for page in pages:   # _page_shingles: "".join(regions_by_id[rid].text for rid in page.region_ids if rid in regions_by_id)
        shingles = _page_shingles(page, regions_by_id)
        if len(shingles) < _MIN_SHINGLES_FOR_COMPARISON: n_excluded += 1; continue
        page_shingle_sets[page.page_id] = shingles
    sketches = {pid: _minhash_sketch(s) for pid, s in page_shingle_sets.items()}
    pairs = _lsh_candidates(sketches)
    pages_by_id = {p.page_id: p for p in pages if p.page_id in page_shingle_sets}
    by_scan_number = {}
    for page_id, page in pages_by_id.items():
        if page.scan_number is not None: by_scan_number.setdefault(page.scan_number, []).append(page_id)
    adjacent_pairs = set()
    for scan_number, page_ids in by_scan_number.items():
        for next_page_id in by_scan_number.get(scan_number + 1, []):
            for page_id in page_ids:
                adjacent_pairs.add((page_id, next_page_id) if page_id < next_page_id else (next_page_id, page_id))
    all_pairs = pairs | adjacent_pairs
    rows = []
    for a, b in sorted(all_pairs):
        sa, sb = page_shingle_sets[a], page_shingle_sets[b]
        inter = len(sa & sb); union = len(sa | sb); jaccard_exact = inter / union if union else 0.0
        jaccard_estimated = sum(x == y for x, y in zip(sketches[a], sketches[b])) / len(sketches[a])
        if jaccard_exact < threshold: continue
        rows.append({"archive_id": adapter.archive_id, "inventory_number": str(inv), "page_id_a": a,
            "page_id_b": b, "scan_number_a": pages_by_id[a].scan_number, "scan_number_b": pages_by_id[b].scan_number,
            "n_shingles_a": len(sa), "n_shingles_b": len(sb), "jaccard_estimated": jaccard_estimated,
            "jaccard_exact": jaccard_exact, "adjacent": (a, b) in adjacent_pairs})
    frame = pd.DataFrame(rows, columns=[the 11 keys above, in that order])
    frame.attrs["n_excluded_near_empty"] = n_excluded; frame.attrs["n_pages_compared"] = len(page_shingle_sets)
    return frame
```

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/duplicate_pages.py` (new) | the API below; the module docstring credits LotA step 2.3 |
| `tests/test_analysis_duplicate_pages.py` (new) | tests below |
| `reports/data/phase-08-region-quality-and-duplicates/step-17/` | `coeff_pin.txt`, `ea_sample.py`, `ea_verdicts.tsv`, `protect_*.txt` |
| LotA `archival_language/corpus/integrity.py` | wrapper (Part B) |
| LotA `docs/from-archival-structures.md` | a paragraph for 17 in `## 7. Region quality and duplicates in AS (phase 08)`; create the section if step 16 hasn't |
| `docs/shared_library/changelog.md` | `### Step 17: duplicate-page detector (C17)` under `## Unreleased` (add the heading above `## v0.12.0` if it is missing), with Edit |
| `reports/phase-08-region-quality-and-duplicates.md` | `### Step 17` section, findings `F08-17.n` |

## Behaviour to implement

Part A, in AS:

```python
DEFAULT_SHINGLE_LENGTH = 5; DEFAULT_MIN_SHINGLES = 100; DEFAULT_NUM_HASHES = 128; DEFAULT_BANDS = 16
MINHASH_PRIME = (1 << 31) - 1; DEFAULT_MINHASH_SEED = 20261004; DEFAULT_DUPLICATE_THRESHOLD = 0.6
CANDIDATE_COLUMNS = ("page_id_a", "page_id_b", "position_a", "position_b", "n_shingles_a",
                     "n_shingles_b", "jaccard_estimated", "jaccard_exact", "adjacent")
def shingles(text: str, k: int = DEFAULT_SHINGLE_LENGTH) -> set[str]
def minhash_coefficients(n: int, *, seed: int) -> tuple[np.ndarray, np.ndarray]   # functools.lru_cache; return read-only arrays
def minhash_sketch(shingle_set: Collection[str], *, num_hashes: int = DEFAULT_NUM_HASHES,
                   seed: int = DEFAULT_MINHASH_SEED) -> tuple[int, ...]
def lsh_candidates(sketches: Mapping[str, tuple[int, ...]], *, bands: int = DEFAULT_BANDS) -> set[tuple[str, str]]
def duplicate_page_candidates(pages: Iterable[tuple[str, str, int | None]], *,
        threshold: float = DEFAULT_DUPLICATE_THRESHOLD, adjacency: bool = True,
        shingle_length: int = DEFAULT_SHINGLE_LENGTH, min_shingles: int = DEFAULT_MIN_SHINGLES,
        num_hashes: int = DEFAULT_NUM_HASHES, bands: int = DEFAULT_BANDS,
        seed: int = DEFAULT_MINHASH_SEED) -> pd.DataFrame
```

- The algorithm is LotA's, step for step, with the page text given instead of built:
  - the iteration orders are the same: input order for pages, sorted pairs for rows;
  - the rows are a list of dicts, turned into `pd.DataFrame(rows, columns=CANDIDATE_COLUMNS)`, so
    that `position_*` infers dtypes as LotA's `scan_number_*` did;
  - `attrs` are `n_excluded_near_empty` and `n_pages_compared`.
- **Adjacency is by position** (decision P5): every kept page at position `p` pairs with every
  kept page at `p + 1`, which includes cross pairs such as p1–p2. Pages whose position is `None`
  are never adjacent. With `adjacency=False`, the adjacent pairs are not added as candidates, but
  the `adjacent` column still reports position adjacency. **Never adjacency by sequence order:**
  all 23 rows of the 13 re-shoots are same-side pairs across scans N and N+1, which are not
  neighbours in sequence.
- `ValueError` on:
  - a repeated `page_id`;
  - `num_hashes % bands != 0`;
  - `threshold` outside [0, 1];
  - `shingle_length < 1`.
- No archive literals (`1.01.02`, `NL-HaNA`, `main_text`, `margin`) and no `reading_order` in the
  source.

Part B, in LotA (after Part A's tests pass). `duplicate_candidates(source, inv, *, adapter,
threshold=DEFAULT_DUPLICATE_THRESHOLD)` keeps its signature:
1. Build `pages = page_units(regions, adapter=adapter)` and `regions_by_id` as now.
2. Call AS with `[(p.page_id, "".join(regions_by_id[rid].text for rid in p.region_ids if rid in
   regions_by_id), p.scan_number) for p in pages]` and `threshold=threshold`.
3. Rename `position_a`/`_b` to `scan_number_a`/`_b`. Insert `inventory_number = str(inv)` and
   then `archive_id = adapter.archive_id` at position 0, which restores the 11-column order.
4. Carry the `attrs` over.
5. `DEFAULT_DUPLICATE_THRESHOLD` is imported from AS.
6. Delete the LotA constants, functions and imports that are no longer used (`zlib`, perhaps
   `random` and `numpy`; check with `make lint`). `page_units`, the manifest code and the CLI
   are unchanged.

## Constraints

- **LotA `resolve_path` refuses paths outside its allowed roots** (the AS scratchpad is refused).
  The full run writes to the **new** dir `L/output/integrity/as-step17/`, which must not exist
  before. Never use the CLI default `--out`.
- **Protected outputs:** `L/output/integrity/*` (except `as-step17/`), `L/output/quality/*`,
  `L/gold/*`. Record `size=N mtime path` before the first run into `protect_before.txt` (adapt
  `reports/data/phase-07-sg-units-and-pilot/step-15i2/protect.py`; skip `as-step17/`) and compare
  after each run.
- The full run takes about 67 min and ~5 GiB. Run it with the Bash tool's `run_in_background:
  true`; no `&`, no `nohup`. Wait for its notification. Nothing heavy alongside it.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; no `rm -rf`.

## Tests

`tests/test_analysis_duplicate_pages.py`. Helpers copied from LotA: `_perturb(text, *, fraction,
seed)` replaces `max(1, int(len*fraction))` chars, at `rng.sample` positions, with
`rng.choice("xqz#@")`. `_LONG_TEXT = "Den negenentwintighsten December is ter Vergaderinge gelesen
een Memorie van wegen den Heer Resident tot Hamburgh aengaende de Commertie der Ingesetenen van
dese Landen."`
1. `test_reshoot_found_with_perturbed_text`: S_0010 (position 10), S_0011 (position 11,
   `_perturb(…, 0.05, seed=1)`), and the unrelated S_0099 "Een geheel andere, onverwante tekst
   over de Oost-Indische Compagnie en haer schepen." The pair is found; S_0099 never is.
2. `test_near_empty_pages_excluded`: two "hi" pages give an empty frame with
   `CANDIDATE_COLUMNS` and `attrs["n_excluded_near_empty"] == 2`.
3. `test_same_side_pairs_across_adjacent_positions`: four pages at positions 10, 10, 11, 11,
   where 11-p1 repeats 10-p1 and 11-p2 repeats 10-p2. Both same-side pairs are found with
   `adjacent=True`; the cross pairs are compared but rejected.
4. `test_non_adjacent_identical_pages_found_by_lsh`: positions 1 and 5 give `adjacent=False`;
   positions `None` give `adjacent=False`.
5. `test_adjacency_false_skips_forced_pairs`: two adjacent pages with Jaccard between 0.6 and
   0.7, built so that their sketches share no band, are found with `adjacency=True` and not with
   `False`. Build the texts by trial with a fixed seed, and assert the band-free condition.
6. `test_output_sorted_and_normalised` (`a < b`, rows sorted).
7. `test_jaccard_estimated_is_share_of_equal_minhashes`.
8. `test_coefficients_pinned`: the first 3 values of `a` and `b` for (128, 20261004), and the
   full sketch of `shingles(_LONG_TEXT)`. Take the values from **LotA's** functions and record
   them in `coeff_pin.txt`.
9. `test_validation_errors`.
10. `test_input_not_mutated_and_no_archive_literals`.
11. `test_default_threshold` (`== 0.6`).

Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_duplicate_pages.py`,
then the full suite. LotA (from `L`): `make test` (368 before your change, or 369 if step 16 ran
first; it must not drop) and `make lint`.

## Acceptance criteria

1. AS tests green; the full AS suite green.
2. After Part B, `planning/dup_check.py` on `3779 3767 3822 638 1576 2588 4000` plus the 13
   re-shoot inventories `3777 3780 3781 3788 3860 3773 3803 3816 3826 3838` reports
   `same True` for all 17.
3. Full run, from `L`: `venv/bin/python -m archival_language.cli.corpus_integrity --archive
   1.01.02 --workers 8 --out $L/output/integrity/as-step17`. Then:
   - `cmp` against the stored candidate table gives **identical bytes** (48,473 rows);
   - `cmp` of `1.01.02-manifest.tsv` gives identical bytes.

   If either differs, stop comparing bytes. Diff the rows (sort by `inventory_number`,
   `page_id_a`, `page_id_b`). Then run the **original** module, `git show
   HEAD:archival_language/corpus/integrity.py` loaded under another name in a scratch script
   (from `L`, writing nothing), on the differing inventories. That separates input drift from a
   port error. A port error is a defect: fix it. Input drift is a finding; report it and ask
   the user.
4. `planning/reshoots.py` on the new table: `found 13/13, rows 23`.
5. LotA `make test` green (no count drop); `make lint` clean. Protected outputs unchanged.

The user decides when the step is done.

## Error analysis

The port is exact, so read **what the detector reports** on real data, using the new table:
- **sample A:** 30 non-adjacent rows outside inventories 638–643, seed 20261021;
- **sample B:** 20 adjacent rows that are not among the 23 documented rows, seed 20261021;
- **sample C:** the 9 rows with no `scan_number_b`.

For each row, print the first 150 chars of both page texts. Get them from LotA's source and
`page_units`, in a read-only script (`ea_sample.py`). Classify each row as:
`re-shoot`, `form / template page`, `repeated formula or heading`, `index or list page`,
`blank-ish page` or `other` (name it). Record the verdicts in `ea_verdicts.tsv`.

Report per-sample counts. The precision of `adjacent=True` in sample B bears on LotA's open
question (filter on `adjacent`?). Say so, but don't decide it.

Findings to expect:
- F08-17.1: the port is byte-identical;
- F08-17.2: same-side adjacency (P5);
- F08-17.3–.4: the sample categories.

## Do not touch

`text_layer/`, `stream_analysis/`, `analysis/region_quality.py` (step 16), LotA `quality.py`, and
LotA files other than those listed.

## When done

Follow `plan/templates/STEP.md` *When done*: the report section `### Step 17: Duplicate-page
detector (C17)` and its findings, then `make reports`; a `PROGRESS.md` block; overwrite
`STATUS.md`.
