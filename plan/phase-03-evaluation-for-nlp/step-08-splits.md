# Step 08: C7 generic splits, held-out builder, public validators, `check_nested_split`

> **Phase:** 03, Evaluation for NLP. **Depends on:** 07 (the package exists). It may run in
> parallel with 07b: no shared files except the changelog and the report, both edited with Edit.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP's document-atomic long-chunk split and its inventory-held-out split are generic, apart
from a thin VOC layer. Every NLP number depends on the cached split CSVs, so the move must
rebuild them **byte-identical** (P2: keep the 10-slot striding and the greedy rebalance as they
are). The step also makes NLP's private validators public and adds
`check_nested_split`, which asserts NLP's leakage rule 1 in code: *a component model's training
split must nest inside the downstream split*. No split code has tests today.

## Inputs

NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`). **Read
`<NLP>/CLAUDE.md` first.** Interpreter `<NLP>/venv/bin/python` (AS resolves to the live tree;
`startpage` needs `<NLP>/src` on `sys.path`). Files to read (checked 2026-10-14):

- `src/startpage/long_subsequence_chunk_split.py` (541 lines, git-tracked).
  - L5 `from .chunk_split import load_top_of_page_scannames`; L7–8 `TRAIN_FRAC = 0.7`,
    `DEV_FRAC = 0.15`.
  - L14–142: dataclasses `Page(page_id, is_doc_start, is_doc_end)`, `Document`, `Chunk`,
    `SubSequence`, `LongSequenceSplit` (with `by_split`, `to_long_dataframe`).
  - L149 `parse_documents`, L177 `partition_file_into_chunks`, L231
    `assign_splits_and_pad_context`, L298 `build_archival_dataset_splits` (its `test_ratio` is
    unused), L337 `build_subsequences`, L373 `validate_subsequences`.
  - `print` at L434, L514, L515, L527.
  - L449 `build_long_sequence_chunk_split(full_sequences, train_frac=TRAIN_FRAC,
    dev_frac=DEV_FRAC, num_chunks_per_file=12, context_window=3, include_context=False)`.
    L477–499 derive `Page`s with a VOC rule: end id = start scan number − 1, zero-padded to 4
    digits, plus the last page. L501–538 are generic: build the splits, `split_by_key`,
    sub-sequences, validation, `LongSequenceSplit`.
- `src/startpage/splits.py` (312 lines, git-tracked).
  - Constants: L53 `DEFAULT_HELD_OUT`, L55 `LONG_CHUNK_DEFAULTS`, L63 `COLUMNS`, L139
    `KNOWN_SPLIT_COVERAGE_GAPS`, L224 `BUILDERS`.
  - Private functions: L92 `_check_duplicate_page_ids(df)`, L112
    `_assert_no_document_split_across_splits(df, full_sequences)` (calls
    `load_top_of_page_scannames()` inside), L150 `_validate_split(df, full_sequences, kind,
    build_params)`, L182 `_select_held_out(all_inventories, seed, n=5)`, L189
    `_build_long_chunk`, L212 `_build_inventory_held_out(full_sequences, seed, params)`.
  - L231 `load_or_build_split(kind, seed=0, force=False, **params)`: caches
    `SPLITS_DIR/<kind>_seed<seed>.{csv,json}`. The cache key is `(kind, seed)` only. It
    validates, writes `to_csv(index=False)`, and chmods.
  - L292 `iter_sequences` stays in NLP.
- `src/startpage/chunk_split.py` (legacy, 146 lines) **stays in NLP unchanged**. It holds
  `load_top_of_page_scannames(position_csv_path=...)` (L41; reads
  `output/start_page_position.csv`, returns `{inv: {scanname}}` for `top_of_page` rows) and
  `build_chunk_split` (used by `kind="chunk"`).

The seven cached splits in `<NLP>/output/splits/` (untracked in git; never overwrite them):

| file | call | rows |
|---|---|---|
| `long_chunk_seed0` | `load_or_build_split("long_chunk")` (K=12, W=3) | 22,201 |
| `long_chunk_seed1` | `load_or_build_split("long_chunk", seed=1, context_window=1)`; the seed slot only holds W=1 | 21,337 |
| `inventory_held_out_seed0..3` | `load_or_build_split("inventory_held_out", seed=s)` | 20,905 each |
| `chunk_seed42` | `load_or_build_split("chunk", seed=42)` (legacy) | 20,905 |

**Checked during planning (2026-10-14):** rebuilding all seven with the *current* NLP code,
using `splits.SPLITS_DIR = <scratch>` and `force=True`, gave byte-identical CSVs and identical
manifests apart from `git_head`, and left the cache untouched (sha256 and mtime). The VOC
end-id rule and the adjacency rule ("the page physically before a top-of-page start, plus the
last page") give the same 1,135 end pages in all 22 inventories.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/stream_analysis/evaluation/splits.py` | new (API below) |
| `archival_structures/stream_analysis/evaluation/__init__.py` | also re-export `splits`' public names |
| `tests/test_stream_analysis_evaluation_splits.py` | new |
| `<NLP>/src/startpage/long_subsequence_chunk_split.py` | shim: re-export the AS names; keep `TRAIN_FRAC`, `DEV_FRAC`, and the wrapper `build_long_sequence_chunk_split` with the same signature, built on AS |
| `<NLP>/src/startpage/splits.py` | keeps the cache, manifest, VOC constants, `BUILDERS`, `iter_sequences`; private helpers call AS |
| `<NLP>/scripts/verify_as_splits.py` | new; the byte-identity check |
| `docs/shared_library/changelog.md` | `### Step 8: chunk splits and validators (C7)` under *Unreleased*, with Edit; list NLP's changed imports |
| `reports/phase-03-evaluation-for-nlp.md` | `### Step 08` section, draft findings `F03-08.n` |

## Behaviour to implement

**AS `splits.py`.** Port L14–448 of `long_subsequence_chunk_split.py` **verbatim**: the
dataclasses, `parse_documents`, `partition_file_into_chunks`,
`assign_splits_and_pad_context`, `build_archival_dataset_splits` (keep `test_ratio`; say
"unused" in the docstring) and `build_subsequences`/`validate_subsequences`. Only two things
change: `print` becomes `logger = logging.getLogger(__name__)` calls (`info` for stats,
`warning` for the duplicate NOTE), and the docstrings document the P2 edge behaviour. Chunks
get a 10-slot pattern `train×4, dev, train×4, test`, so before rebalancing only a file with ≥10
chunks has a test chunk. The greedy swap then retypes the chunk before a split change into the
globally deficient partition, which can overshoot (see the pinned test). It skips files with
fewer than 3 chunks, which therefore stay all-train.
Iteration order is always the insertion
order of the input mappings; document it. New:

```python
def pages_from_starts(page_ids: Sequence[str], start_ids: Collection[str]) -> list[Page]:
    """is_doc_start = id in start_ids; is_doc_end = next page is a start, or last page."""

def build_long_sequence_split(archival_files: Mapping[str, list[Page]], *, num_chunks_per_file=12,
        context_window=3, train_frac=0.7, dev_frac=0.15, include_context=False):
    """NLP L501-538 verbatim (prints -> logging). Returns split_by_key {(file_id, page_id): split}
    if not include_context, else LongSequenceSplit."""

def build_inventory_held_out(sequences: Mapping[str, Sequence[str]], held_out: Collection[str] | None = None,
        seed: int = 0, n_held_out: int = 5) -> tuple[pd.DataFrame, list[str]]:
    """held_out None -> set(random.Random(seed).sample(sorted(sequences), n_held_out)).
    Unknown ids in held_out -> ValueError. Rows in sequences order: inventory_number, page_id,
    split ('test' if held out else 'train'). Returns (frame, sorted(held_out))."""

def core_rows(split_df) -> pd.DataFrame          # drops role == "context" if the column exists
def eval_scan_ids(split_df, split: str, sep: str = "/") -> list[str]   # core rows of `split`, f"{inv}{sep}{page_id}"

class SplitValidationError(AssertionError): ...   # subclass of AssertionError: NLP's callers caught asserts

def check_no_duplicate_page_ids(split_df) -> int
    # NLP L96-109: core rows; a (inventory_number, page_id) in >1 split raises; returns the
    # number of same-split duplicates (logged as a warning)
def check_no_document_crosses_partitions(split_df, sequences: Mapping[str, Sequence[str]],
        top_of_page_starts: Mapping[str, Collection[str]]) -> None   # NLP L116-129, set passed in
def check_context_not_scored(split_df) -> None    # raise if any role == "context" row has eval_mask == 1
def check_split_coverage(split_df, inventories: Iterable[str], splits=("train", "dev", "test"),
        known_gaps: Mapping[str, Collection[str]] | None = None) -> list[tuple[str, str]]
    # NLP's _validate_split coverage loop: unknown gap raises; returns the known gaps hit
def check_nested_split(component_split, downstream_split, *, component_train=("train",),
        downstream_train=("train",)) -> dict[str, int]
```

`check_nested_split` takes two split frames (`inventory_number, page_id, split`, optional
`role`) and uses core rows only. A violation is a page in one of the component's
`component_train` partitions whose downstream split is **not** in `downstream_train` (that is,
a page the component was trained on and the downstream model is evaluated on). On any
violation it raises `SplitValidationError` with the count and the first 10 keys. Otherwise it
returns `{"checked": n_component_train, "absent_downstream": n}`; pages absent from the
downstream frame are counted, not errors. The docstring quotes the rule and says that context
pages are not checked.

All validators raise `SplitValidationError`, never bare `assert`. They contain no VOC constants.

**NLP `long_subsequence_chunk_split.py`.** It re-exports `Page, Document, Chunk, SubSequence,
LongSequenceSplit, parse_documents, partition_file_into_chunks,
assign_splits_and_pad_context, build_archival_dataset_splits, build_subsequences,
validate_subsequences` from AS. `build_long_sequence_chunk_split` keeps its signature and does
`top = load_top_of_page_scannames()`, then
`archival_files = {inv: pages_from_starts([s for s, _, _ in seq], top[inv]) for inv, seq in
full_sequences.items()}` (`top[inv]` keeps NLP's KeyError for an inventory without top-of-page
starts), then returns `build_long_sequence_split(...)` with the same parameters. The VOC
end-id grammar is deleted (adjacency was shown to be identical; the byte check proves it).

**NLP `splits.py`.**
- The private names stay, as one-line delegations:
  - `_check_duplicate_page_ids` → `check_no_duplicate_page_ids`;
  - `_assert_no_document_split_across_splits(df, full_sequences)` →
    `check_no_document_crosses_partitions(df, _page_ids(full_sequences),
    load_top_of_page_scannames())`. Define `_page_ids(full_sequences)` locally as
    `{inv: [s for s, _, _ in seq] for inv, seq in full_sequences.items()}`;
  - `_validate_split` keeps its structure but calls `check_split_coverage` (with
    `KNOWN_SPLIT_COVERAGE_GAPS[gap_key]`), the two checks above and `check_context_not_scored`.
- `_select_held_out(all_inventories, seed, n=5)` keeps the VOC seed-0 rule
  (`set(DEFAULT_HELD_OUT) & set(all_inventories)`). For seed ≥ 1 it returns `None`, so that AS
  samples.
- `_build_inventory_held_out` uses an explicit override if one is given. Otherwise it takes
  `_select_held_out(...)`, calls `build_inventory_held_out(_page_ids(full_sequences),
  held_out, seed)`, and returns `(df, {"held_out_inventories": sorted_list})`.
- NLP's `print` NOTEs that remain in `splits.py` may stay.

## Constraints

- **Never write into `<NLP>/output/splits/`.** All rebuilds go through
  `splits.SPLITS_DIR = <scratch>` with `force=True`.
- Don't change chunking, striding, rebalancing, padding, row order, the CSV columns or
  `to_csv` options. Don't add a "corrected" strategy (P2: opt-in only, and not in this step).
- No VOC constants (held-out set, coverage gaps, scan-name grammar) in AS.
- Don't edit `chunk_split.py`, `iter_sequences`, or any of NLP's 22 importers of
  `load_or_build_split`.
- Never `git commit`; `chmod g+w` new files.

## Tests

`tests/test_stream_analysis_evaluation_splits.py` (synthetic; no NLP data):

- documents tile exactly: the core ranges of all chunks cover every page of every file once;
- an oversized document (≥ the target size) becomes its own chunk;
- `pages_from_starts`: starts and ends on a 7-page toy file, including a file whose first page
  is not a start;
- context stays in bounds (`0 ≤ context_start ≤ core_start`,
  `context_end ≤ last page`), and in `to_long_dataframe()` every context row has
  `eval_mask == 0`; `check_context_not_scored` passes on it and raises on a doctored row;
- P2 pinned with values computed with NLP's code on 2026-10-14. File `A` has 10 documents of
  10 pages (doc start on page 0, end on page 9 of each). File `B` has 2 documents of 5 pages.
  - `build_archival_dataset_splits({"A": A}, num_chunks_per_file=10)` gives splits
    `[train, train, train, test, dev, train, train, train, train, test]` and distribution
    `{train: 70.0, dev: 10.0, test: 20.0}`. The swap turned index 3 into test, and dev stays
    under target: this is the documented oddity.
  - `{"A": A, "B": B}` (K=10) gives B `[train, train]` and `{train: 72.73, dev: 9.09, test:
    18.18}`.
  - `{"B": B}` with K=2 and the default `context_window=2` gives `(split, context_start,
    core_start, core_end, context_end)` = `(train, 0, 0, 4, 6)` and `(train, 3, 5, 9, 9)`;
- `build_inventory_held_out`:
  - held-out inventories are whole (each inventory in exactly one split);
  - seed 1 equals `random.Random(1).sample(sorted(keys), 5)`;
  - an unknown id raises;
- `check_no_duplicate_page_ids`: a key in two splits raises `SplitValidationError` (which is
  also an `AssertionError`); a same-split duplicate returns 1;
- `check_no_document_crosses_partitions` raises when a document straddles train/dev;
- `check_split_coverage`: an unknown gap raises; a listed gap is returned;
- `check_nested_split`:
  - nested passes with the right counts;
  - one component-train page in downstream `dev` raises;
  - context rows are ignored;
- logging, not print: `capsys` captures nothing from `build_long_sequence_split`.

Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_evaluation_splits.py`,
then the full suite. NLP has no split tests; the verify script is the NLP check.

`<NLP>/scripts/verify_as_splits.py` (argparse `--out DIR`, required; exit 0 only on success):
record sha256 and mtime of every file in `output/splits/`; set `splits.SPLITS_DIR = out`; for
the seven calls in the table, call `load_or_build_split(kind, seed=…, force=True, **params)`
(redirect stdout), then compare the CSV bytes with the cached file and the manifests as JSON
without `git_head`. Finally assert that the cache's sha256 and mtimes are unchanged. Print one
line per file.

## Acceptance criteria

1. Run `verify_as_splits.py` **before** editing NLP (with step 07 merged, AS `splits.py` may
   already exist): 7/7 identical. Run it again after: 7/7 identical. Cache untouched both times.
2. The new AS split tests pass. Full AS suite green; the total is the post-07 count plus your
   tests.
3. `grep -n "8834\|8023\|_[0-9]\{4\}\|start_page_position" archival_structures/stream_analysis/evaluation/splits.py`
   finds nothing.
4. NLP smoke check: `venv/bin/python -c "import sys; sys.path.insert(0,'src'); from startpage.splits import load_or_build_split as f; print(f('long_chunk').shape)"`
   prints `(22201, 7)` and reads from the cache.

## Error analysis

- Coverage: from the rebuilt `long_chunk_seed0` and `long_chunk_seed1`, list per inventory the
  number of core pages in train/dev/test, and the inventories with a missing partition. Expect
  only 8023 with no test pages. Explain from the chunk counts why (P2 striding) (finding).
- Chunk-size distribution at K=12: the number of oversized single-document chunks, and the
  minimum, median and maximum core pages per chunk. Read the 5 largest chunks (inventory, doc
  count, pages).
- Nesting, measured on real splits: `check_nested_split(long_chunk_seed0,
  inventory_held_out_seed0)` and the reverse. Report the counts. These are expected to
  **violate** (the two splits are not nested). Say what that means for any component trained
  on one split and evaluated on the other (finding; leakage rule 1).
- Record that the VOC end-id rule was dropped, with the planning-time equality (1,135 ends in
  both rules); the byte check confirms it.

## Do not touch

- `evaluation/segmentation.py`, `aggregate.py` (07), NLP `evaluation.py` (07b),
  `run_trivial_baselines.py` (09), `parsers/`, LotA.

## When done

1. Fill in `### Step 08: …` in `reports/phase-03-evaluation-for-nlp.md`, with findings
   `F03-08.n` (draft). Run `make reports`; no warnings.
2. Append the `plan/templates/STEP.md` *When done* block to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`. Under *Facts later steps need*, record the final names of
   `core_rows` and `eval_scan_ids` (step 09 uses them).
