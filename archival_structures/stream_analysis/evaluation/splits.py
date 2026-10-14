"""Document-atomic long-chunk splits, inventory-held-out splits and split validators (C7).

Generic code moved from NLP's ``long_subsequence_chunk_split.py`` / ``splits.py``. The
chunking, 10-slot striding and greedy rebalance are ported unchanged so that NLP's cached
splits are rebuilt byte-identically (P2): their edge behaviour is documented, not fixed.

Iteration order is always the insertion order of the input mappings (files, then pages).
No dataset-specific constants live here; callers pass start sets and held-out sets in.
"""

from __future__ import annotations

import logging
import random
from collections import Counter, defaultdict
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = [
    "Page", "Document", "Chunk", "SubSequence", "LongSequenceSplit",
    "parse_documents", "partition_file_into_chunks", "assign_splits_and_pad_context",
    "build_archival_dataset_splits", "build_subsequences", "validate_subsequences",
    "pages_from_starts", "build_long_sequence_split", "build_inventory_held_out",
    "core_rows", "eval_scan_ids", "SplitValidationError",
    "check_no_duplicate_page_ids", "check_no_document_crosses_partitions",
    "check_context_not_scored", "check_split_coverage", "check_nested_split",
]

TRAIN_FRAC = 0.7
DEV_FRAC = 0.15

# =====================================================================
# Data Structures
# =====================================================================

@dataclass
class Page:
    page_id: str
    is_doc_start: bool
    is_doc_end: bool

@dataclass
class Document:
    doc_id: str
    pages: List[Page]
    start_idx: int  # 0-based index within the archival file
    end_idx: int    # 0-based inclusive index

    @property
    def page_count(self) -> int:
        return len(self.pages)

@dataclass
class Chunk:
    chunk_id: str
    file_id: str
    documents: List[Document]
    split: Optional[str] = None  # 'train', 'dev', or 'test'
    
    # Read-only context padding metadata
    context_start_idx: int = 0
    context_end_idx: int = 0

    @property
    def core_start_idx(self) -> int:
        return self.documents[0].start_idx

    @property
    def core_end_idx(self) -> int:
        return self.documents[-1].end_idx

    @property
    def core_page_indices(self) -> List[int]:
        return list(range(self.core_start_idx, self.core_end_idx + 1))

    @property
    def page_count(self) -> int:
        return sum(d.page_count for d in self.documents)


@dataclass
class SubSequence:
    """
    One chunk, materialized as the contiguous page sequence a sequence
    model actually consumes: core pages plus the read-only context pages
    on either side (report/45's W parameter), in physical order, with a
    per-page eval mask.

    `eval_mask[i] == 1` means page_ids[i] is a CORE page of this chunk --
    it contributes to the loss and to evaluation. `0` means it is a
    read-only context page: its features/representation may be used by
    the model (that is the whole point -- a page at a chunk edge would
    otherwise have no left or right context), but its target is masked
    out, so it contributes no loss and no metric.

    The mask is only meaningful PER SUBSEQUENCE, not per page globally:
    chunks tile a file's pages, so every page is core in exactly one
    subsequence and context in up to two neighbouring ones. Aggregating
    predictions across subsequences therefore means keeping only the
    core rows, or every page near a cut would be predicted more than
    once.
    """
    chunk_id: str
    file_id: str
    split: str
    page_ids: List[str]
    eval_mask: List[int]
    context_start_idx: int
    core_start_idx: int
    core_end_idx: int
    context_end_idx: int

    @property
    def n_core(self) -> int:
        return sum(self.eval_mask)

    @property
    def n_context(self) -> int:
        return len(self.eval_mask) - self.n_core

    def core_page_ids(self) -> List[str]:
        return [p for p, m in zip(self.page_ids, self.eval_mask) if m]


@dataclass
class LongSequenceSplit:
    """
    Returned by build_long_sequence_chunk_split(include_context=True).

    `split_by_key` is byte-for-byte what the function returns with
    include_context=False (one split label per (file_id, page_id)), so
    page-level callers such as train.run_training() keep working
    unchanged; `subsequences` is the extra structure sequence models need.
    """
    split_by_key: Dict[Tuple[str, str], str]
    subsequences: List["SubSequence"]
    distribution: Dict[str, float]
    context_window: int
    num_chunks_per_file: int

    def by_split(self, split: str) -> List["SubSequence"]:
        return [s for s in self.subsequences if s.split == split]

    def to_long_dataframe(self):
        """
        One row per (subsequence, page) -- the format sequence-model
        training and the on-disk dump both use. Imported lazily so this
        module keeps no hard pandas dependency for page-level callers.
        """
        import pandas as pd

        rows = []
        for sub in self.subsequences:
            for position, (page_id, mask) in enumerate(zip(sub.page_ids, sub.eval_mask)):
                rows.append({
                    "inventory_number": sub.file_id,
                    "page_id": page_id,
                    "chunk_id": sub.chunk_id,
                    "split": sub.split,
                    "role": "core" if mask else "context",
                    "eval_mask": mask,
                    "position_in_subsequence": position,
                })
        return pd.DataFrame(rows)


# =====================================================================
# Core Algorithm
# =====================================================================

def parse_documents(pages: List[Page]) -> List[Document]:
    """Group pages into atomic document units based on start/end annotations."""
    docs: List[Document] = []
    current_pages: List[Page] = []
    start_idx = 0

    for idx, page in enumerate(pages):
        if not current_pages:
            start_idx = idx
        current_pages.append(page)

        # Finalize document if explicitly marked as end or if next page is a start
        is_last_page = (idx == len(pages) - 1)
        next_is_start = (not is_last_page) and pages[idx + 1].is_doc_start

        if page.is_doc_end or next_is_start or is_last_page:
            doc_id = f"doc_{len(docs):04d}"
            docs.append(Document(
                doc_id=doc_id,
                pages=current_pages,
                start_idx=start_idx,
                end_idx=idx
            ))
            current_pages = []

    return docs


def partition_file_into_chunks(
    file_id: str, 
    documents: List[Document], 
    num_chunks: int
) -> List[Chunk]:
    """
    Slices an archival file into dynamic continuous multi-document chunks.
    Respects document boundaries and handles oversized documents gracefully.
    """
    total_pages = sum(d.page_count for d in documents)
    target_pages = total_pages / max(1, num_chunks)

    chunks: List[Chunk] = []
    curr_docs = []
    curr_page_count = 0

    for doc in documents:
        # Case A: Oversized document (exceeds target size on its own)
        if doc.page_count >= target_pages:
            # First flush any accumulated documents
            if curr_docs:
                chunk_id = f"{file_id}_chunk_{len(chunks):03d}"
                chunks.append(Chunk(chunk_id=chunk_id, file_id=file_id, documents=curr_docs))
                curr_docs = []
                curr_page_count = 0
            
            # Place oversized document in its own dedicated chunk
            chunk_id = f"{file_id}_chunk_{len(chunks):03d}"
            chunks.append(Chunk(chunk_id=chunk_id, file_id=file_id, documents=[doc]))
            continue

        # Case B: Check if adding 'doc' exceeds target size
        if curr_docs and (curr_page_count + doc.page_count > target_pages):
            diff_without = abs(curr_page_count - target_pages)
            diff_with = abs((curr_page_count + doc.page_count) - target_pages)

            if diff_without <= diff_with:
                # Close current chunk before adding doc
                chunk_id = f"{file_id}_chunk_{len(chunks):03d}"
                chunks.append(Chunk(chunk_id=chunk_id, file_id=file_id, documents=curr_docs))
                curr_docs = [doc]
                curr_page_count = doc.page_count
                continue

        curr_docs.append(doc)
        curr_page_count += doc.page_count

    if curr_docs:
        chunk_id = f"{file_id}_chunk_{len(chunks):03d}"
        chunks.append(Chunk(chunk_id=chunk_id, file_id=file_id, documents=curr_docs))

    return chunks


def assign_splits_and_pad_context(
    all_chunks: List[Chunk], 
    total_file_pages: Dict[str, int],
    context_window: int = 2,
    train_ratio: float = 0.70,
    dev_ratio: float = 0.15
) -> List[Chunk]:
    """
    Assigns chunks to train/dev/test using interleaved depth striding,
    performs page-count balancing, and attaches read-only context boundaries.

    Edge behaviour (P2, kept as-is so cached splits stay reproducible): chunks of each file
    cycle through the 10-slot pattern train x4, dev, train x4, test, so before rebalancing
    only a file with >= 10 chunks has a test chunk. The greedy swap then retypes the chunk
    before a split change into the globally most deficient partition, which can overshoot
    the target and leave dev under target. Files with fewer than 3 chunks are skipped by the
    swap and stay all-train. Iteration follows the insertion order of ``all_chunks``.
    """
    # Group chunks by archival file
    chunks_by_file: Dict[str, List[Chunk]] = {}
    for c in all_chunks:
        chunks_by_file.setdefault(c.file_id, []).append(c)

    # Step 1: Interleaved Depth Striding Assignment per File
    # Pattern designed for 70/15/15 ratio: 0,1,2,3 -> Train, 4 -> Dev, 5,6,7,8 -> Train, 9 -> Test
    strided_pattern = ['train', 'train', 'train', 'train', 'dev', 'train', 'train', 'train', 'train', 'test']

    for file_id, f_chunks in chunks_by_file.items():
        for idx, chunk in enumerate(f_chunks):
            pattern_idx = idx % len(strided_pattern)
            chunk.split = strided_pattern[pattern_idx]

    # Step 2: Global Equalization Swap (Greedy adjustment to match exact ratios)
    total_global_pages = sum(c.page_count for c in all_chunks)
    test_ratio = 1.0 - train_ratio - dev_ratio
    target_pages = {
        'train': total_global_pages * train_ratio,
        'dev': total_global_pages * dev_ratio,
        'test': total_global_pages * test_ratio
    }

    def get_current_totals():
        totals = {'train': 0, 'dev': 0, 'test': 0}
        for c in all_chunks:
            totals[c.split] += c.page_count
        return totals

    current_totals = get_current_totals()

    # Rebalance if dev or test are deficient by swapping adjacent chunks within files
    for file_id, f_chunks in chunks_by_file.items():
        if len(f_chunks) < 3:
            continue
        for i in range(len(f_chunks) - 1):
            c1, c2 = f_chunks[i], f_chunks[i + 1]
            if c1.split != c2.split:
                surplus_split = max(current_totals, key=lambda s: current_totals[s] - target_pages[s])
                deficit_split = min(current_totals, key=lambda s: current_totals[s] - target_pages[s])

                if c1.split == surplus_split and c2.split != deficit_split:
                    # Try swapping split tag if it improves global equilibrium
                    c1.split = deficit_split
                    current_totals = get_current_totals()

    # Step 3: Compute Context Window Padding Metadata
    for file_id, f_chunks in chunks_by_file.items():
        max_page_idx = total_file_pages[file_id] - 1
        for chunk in f_chunks:
            chunk.context_start_idx = max(0, chunk.core_start_idx - context_window)
            chunk.context_end_idx = min(max_page_idx, chunk.core_end_idx + context_window)

    return all_chunks


def build_archival_dataset_splits(
    archival_files: Dict[str, List[Page]], 
    num_chunks_per_file: int = 10,
    context_window: int = 2,
    train_ratio: float = 0.7,
    dev_ratio: float = 0.15,
    test_ratio: float = 0.15
) -> Tuple[List[Chunk], Dict[str, float]]:
    """
    Main entry point: Slices multi-file archival dataset into balanced Train/Dev/Test chunks.

    ``test_ratio`` is unused (the test share is ``1 - train_ratio - dev_ratio``). Files are
    processed in the insertion order of ``archival_files``. See
    ``assign_splits_and_pad_context`` for the striding and rebalance edge behaviour.
    """
    all_chunks: List[Chunk] = []
    file_page_counts = {}

    for file_id, pages in archival_files.items():
        file_page_counts[file_id] = len(pages)
        docs = parse_documents(pages)
        file_chunks = partition_file_into_chunks(file_id, docs, num_chunks_per_file)
        all_chunks.extend(file_chunks)

    # Assign splits and context padding
    processed_chunks = assign_splits_and_pad_context(
        all_chunks, 
        total_file_pages=file_page_counts,
        context_window=context_window,
        train_ratio=train_ratio, 
        dev_ratio=dev_ratio
    )

    # Calculate actual percentage metrics
    total_pages = sum(file_page_counts.values())
    split_counts = {'train': 0, 'dev': 0, 'test': 0}
    for c in processed_chunks:
        split_counts[c.split] += c.page_count

    actual_ratios = {s: round(count / total_pages * 100, 2) for s, count in split_counts.items()}
    return processed_chunks, actual_ratios


def build_subsequences(chunks: List[Chunk],
                       archival_files: Dict[str, List[Page]]) -> List[SubSequence]:
    """
    Materialize each chunk's context-padded page sequence.

    assign_splits_and_pad_context() computes context_start_idx /
    context_end_idx but the page ids behind those indices live in
    `archival_files`, so this needs both. Index spaces match by
    construction: Document.start_idx/end_idx, and therefore every
    Chunk's core and context indices, are positions in that file's own
    page list.
    """
    subsequences: List[SubSequence] = []
    for chunk in chunks:
        pages = archival_files[chunk.file_id]
        core_start, core_end = chunk.core_start_idx, chunk.core_end_idx
        ctx_start, ctx_end = chunk.context_start_idx, chunk.context_end_idx

        page_ids = [pages[i].page_id for i in range(ctx_start, ctx_end + 1)]
        eval_mask = [1 if core_start <= i <= core_end else 0
                     for i in range(ctx_start, ctx_end + 1)]

        subsequences.append(SubSequence(
            chunk_id=chunk.chunk_id,
            file_id=chunk.file_id,
            split=chunk.split,
            page_ids=page_ids,
            eval_mask=eval_mask,
            context_start_idx=ctx_start,
            core_start_idx=core_start,
            core_end_idx=core_end,
            context_end_idx=ctx_end,
        ))
    return subsequences


def validate_subsequences(subsequences: List[SubSequence],
                          archival_files: Dict[str, List[Page]]) -> Dict[str, int]:
    """
    Asserts the invariants a sequence model relies on, and returns a few
    summary counts for logging. Cheap enough to run every time.

    1. Core ranges tile each file exactly once -- no page is core in two
       subsequences, none is core in none (so keeping only core rows
       yields each page exactly once).
    2. Context never runs outside the file.
    3. Every subsequence is contiguous and non-empty, and has at least
       one core page.
    """
    by_file: Dict[str, List[SubSequence]] = defaultdict(list)
    for sub in subsequences:
        by_file[sub.file_id].append(sub)

    total_core = 0
    total_context_slots = 0
    for file_id, subs in by_file.items():
        n_pages = len(archival_files[file_id])
        covered: Set[int] = set()
        for sub in subs:
            assert sub.page_ids, f"{sub.chunk_id}: empty subsequence"
            assert sub.n_core > 0, f"{sub.chunk_id}: no core pages"
            assert sub.context_start_idx >= 0, f"{sub.chunk_id}: context before file start"
            assert sub.context_end_idx < n_pages, f"{sub.chunk_id}: context past file end"
            assert len(sub.page_ids) == sub.context_end_idx - sub.context_start_idx + 1, \
                f"{sub.chunk_id}: page_ids length does not match index range"
            core_range = set(range(sub.core_start_idx, sub.core_end_idx + 1))
            overlap = covered & core_range
            assert not overlap, f"{sub.chunk_id}: core pages already covered: {sorted(overlap)[:5]}"
            covered |= core_range
            total_core += sub.n_core
            total_context_slots += sub.n_context
        missing = set(range(n_pages)) - covered
        assert not missing, f"{file_id}: {len(missing)} pages are core in no subsequence"

    # Index space is unique by construction, page_id space is not: at
    # least one scanname can occur twice in a physical sequence (an
    # annotation artefact that survives dedup in the VOC data: one copy
    # labelled start_page, the other not). Sequence models
    # index by position and are unaffected, but any page_id-keyed join
    # silently collapses the pair, so surface it rather than hide it --
    # and treat it as an error if the copies ever land in different
    # splits, which would be real leakage.
    splits_per_page: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    core_count: Counter = Counter()
    for sub in subsequences:
        for page_id in sub.core_page_ids():
            splits_per_page[(sub.file_id, page_id)].add(sub.split)
            core_count[(sub.file_id, page_id)] += 1

    duplicated = {key: n for key, n in core_count.items() if n > 1}
    cross_split = {key: sorted(splits_per_page[key]) for key in duplicated
                   if len(splits_per_page[key]) > 1}
    if cross_split:
        raise AssertionError(
            f"duplicate page_ids assigned to more than one split (leakage): {cross_split}")
    if duplicated:
        logger.warning(
            "NOTE: %d page_id(s) occur more than once in their inventory's sequence and "
            "are therefore core more than once (same split each, no leakage): %s",
            len(duplicated), [f"{inv}/{page}" for inv, page in list(duplicated)[:3]])

    return {
        "n_subsequences": len(subsequences),
        "n_files": len(by_file),
        "total_core_pages": total_core,
        "n_unique_core_page_ids": len(core_count),
        "n_duplicate_page_ids": len(duplicated),
        "total_context_slots": total_context_slots,
    }


# =====================================================================
# Builders
# =====================================================================

def pages_from_starts(page_ids: Sequence[str], start_ids: Collection[str]) -> List[Page]:
    """Build ``Page`` objects from an ordered page-id list and the set of start ids.

    ``is_doc_start`` = id in ``start_ids``; ``is_doc_end`` = the next page is a start, or the
    page is the last one. A file whose first page is not a start is allowed: the leading pages
    form a document of their own in ``parse_documents``.
    """
    pages = []
    n = len(page_ids)
    for i, pid in enumerate(page_ids):
        is_end = i == n - 1 or page_ids[i + 1] in start_ids
        pages.append(Page(page_id=pid, is_doc_start=pid in start_ids, is_doc_end=is_end))
    return pages


def build_long_sequence_split(archival_files: Mapping[str, List[Page]], *,
                              num_chunks_per_file: int = 12,
                              context_window: int = 3,
                              train_frac: float = TRAIN_FRAC,
                              dev_frac: float = DEV_FRAC,
                              include_context: bool = False):
    """Chunk every file at document boundaries and assign whole chunks to train/dev/test.

    Returns ``{(file_id, page_id): split}`` if ``include_context`` is false, else a
    ``LongSequenceSplit`` carrying that mapping plus one context-padded ``SubSequence`` per
    chunk (``num_chunks_per_file`` = K, ``context_window`` = W). Iteration order follows
    ``archival_files``. Statistics are logged at INFO level, not printed.
    """
    chunks, distribution = build_archival_dataset_splits(
        archival_files=archival_files,
        num_chunks_per_file=num_chunks_per_file,
        context_window=context_window,
        train_ratio=train_frac,
        dev_ratio=dev_frac,
    )

    logger.info("Total Chunks Generated: %d", len(chunks))
    logger.info("Split Distribution (%% pages): %s", distribution)
    split_by_key: Dict[Tuple[str, str], str] = {}
    for chunk in chunks:
        for doc in chunk.documents:
            for page in doc.pages:
                split_by_key[(chunk.file_id, page.page_id)] = chunk.split

    if not include_context:
        return split_by_key

    subsequences = build_subsequences(chunks, archival_files)
    stats = validate_subsequences(subsequences, archival_files)
    logger.info("Sub-sequences: %d across %d inventories (K=%d, W=%d); %d core pages, "
                "%d read-only context slots",
                stats["n_subsequences"], stats["n_files"], num_chunks_per_file,
                context_window, stats["total_core_pages"], stats["total_context_slots"])

    return LongSequenceSplit(
        split_by_key=split_by_key,
        subsequences=subsequences,
        distribution=distribution,
        context_window=context_window,
        num_chunks_per_file=num_chunks_per_file,
    )


def build_inventory_held_out(sequences: Mapping[str, Sequence[str]],
                             held_out: Collection[str] | None = None,
                             seed: int = 0, n_held_out: int = 5
                             ) -> Tuple[pd.DataFrame, List[str]]:
    """Whole-inventory split: held-out inventories are ``test``, the rest ``train``.

    ``held_out=None`` samples ``set(random.Random(seed).sample(sorted(sequences), n_held_out))``.
    Unknown ids in ``held_out`` raise ``ValueError``. Rows follow the order of ``sequences``
    (columns ``inventory_number, page_id, split``). Returns ``(frame, sorted(held_out))``.
    """
    if held_out is None:
        held = set(random.Random(seed).sample(sorted(sequences), n_held_out))
    else:
        held = set(held_out)
        unknown = held - set(sequences)
        if unknown:
            raise ValueError(f"held-out ids not in sequences: {sorted(unknown)}")
    rows = []
    for inv, page_ids in sequences.items():
        split = "test" if inv in held else "train"
        for pid in page_ids:
            rows.append({"inventory_number": inv, "page_id": pid, "split": split})
    df = pd.DataFrame(rows, columns=["inventory_number", "page_id", "split"])
    return df, sorted(held)


# =====================================================================
# Split-frame helpers and validators
# =====================================================================

class SplitValidationError(AssertionError):
    """A split violates an invariant. Subclasses AssertionError: NLP callers caught asserts."""


def core_rows(split_df: pd.DataFrame) -> pd.DataFrame:
    """Rows that are scored: drops ``role == "context"`` rows if the column exists."""
    if "role" not in split_df.columns:
        return split_df
    return split_df[split_df["role"] != "context"]


def eval_scan_ids(split_df: pd.DataFrame, split: str, sep: str = "/") -> List[str]:
    """Core rows of ``split`` as ``f"{inventory_number}{sep}{page_id}"``, in frame order."""
    core = core_rows(split_df)
    core = core[core["split"] == split]
    return [f"{i}{sep}{p}" for i, p in zip(core["inventory_number"], core["page_id"])]


def check_no_duplicate_page_ids(split_df: pd.DataFrame) -> int:
    """A (inventory_number, page_id) core key in more than one split raises (leakage).

    Returns the number of keys duplicated within one split (logged as a warning, not an error).
    """
    core = core_rows(split_df)
    by_key = core.groupby(["inventory_number", "page_id"])["split"]
    bad = by_key.nunique()
    bad = bad[bad > 1]
    if len(bad):
        raise SplitValidationError(
            f"duplicate page_ids assigned to more than one split (leakage): "
            f"{bad.index.tolist()[:5]}")
    sizes = by_key.size()
    duplicated = sizes[sizes > 1]
    if len(duplicated):
        logger.warning("NOTE: %d page_id(s) occur more than once in their inventory's "
                       "sequence (same split each, no leakage), e.g. %s",
                       len(duplicated), duplicated.index.tolist()[:3])
    return int(len(duplicated))


def check_no_document_crosses_partitions(split_df: pd.DataFrame,
                                         sequences: Mapping[str, Sequence[str]],
                                         top_of_page_starts: Mapping[str, Collection[str]]
                                         ) -> None:
    """No document (start-delimited) has pages in two splits. Inventories without an entry in
    ``top_of_page_starts`` are treated as having no starts (one document)."""
    core = core_rows(split_df)
    split_by_key = {(r.inventory_number, r.page_id): r.split
                    for r in core.itertuples(index=False)}
    for inv, page_ids in sequences.items():
        starts = top_of_page_starts.get(inv, set())
        pages = [Page(page_id=p, is_doc_start=(p in starts), is_doc_end=False)
                 for p in page_ids]
        for doc in parse_documents(pages):
            splits_in_doc = {split_by_key.get((inv, p.page_id)) for p in doc.pages}
            splits_in_doc.discard(None)
            if len(splits_in_doc) > 1:
                raise SplitValidationError(
                    f"document {inv}/{doc.doc_id} straddles splits: {splits_in_doc}")


def check_context_not_scored(split_df: pd.DataFrame) -> None:
    """Raise if any ``role == "context"`` row has ``eval_mask == 1``."""
    if "role" not in split_df.columns or "eval_mask" not in split_df.columns:
        return
    bad = (split_df["role"] == "context") & (split_df["eval_mask"] == 1)
    if bad.any():
        raise SplitValidationError(
            f"context rows must never carry eval_mask=1 ({int(bad.sum())} rows)")


def check_split_coverage(split_df: pd.DataFrame, inventories: Iterable[str],
                         splits: Sequence[str] = ("train", "dev", "test"),
                         known_gaps: Mapping[str, Collection[str]] | None = None
                         ) -> List[Tuple[str, str]]:
    """Every inventory has pages in every split, except the ``known_gaps``
    ({inventory: {split, ...}}). An unlisted gap raises; listed gaps that occur are returned
    as sorted ``(inventory, split)`` pairs."""
    known_gaps = known_gaps or {}
    all_inv = set(inventories)
    hit: List[Tuple[str, str]] = []
    for split in splits:
        present = set(split_df.loc[split_df["split"] == split, "inventory_number"])
        missing = all_inv - present
        unexpected = {i for i in missing if split not in known_gaps.get(i, set())}
        if unexpected:
            raise SplitValidationError(f"inventories missing from {split}: {sorted(unexpected)}")
        for inv in sorted(missing - unexpected):
            logger.warning("NOTE: known split-coverage gap: %s absent from %s", inv, split)
            hit.append((inv, split))
    return hit


def check_nested_split(component_split: pd.DataFrame, downstream_split: pd.DataFrame, *,
                       component_train: Collection[str] = ("train",),
                       downstream_train: Collection[str] = ("train",)) -> Dict[str, int]:
    """Leakage rule 1: a component model's training split must nest inside the downstream
    split.

    A violation is a core page in one of the component's ``component_train`` partitions whose
    downstream split is not in ``downstream_train`` (the component was trained on a page the
    downstream model is evaluated on). Raises ``SplitValidationError`` with the count and the
    first 10 keys. Otherwise returns ``{"checked": n_component_train_keys,
    "absent_downstream": n}``; pages absent from the downstream frame are counted, not
    errors. Only core rows are compared; context pages are not checked.
    """
    comp = core_rows(component_split)
    comp = comp[comp["split"].isin(set(component_train))]
    comp_keys = set(zip(comp["inventory_number"], comp["page_id"]))

    down = core_rows(downstream_split)
    down_splits: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for inv, pid, sp in zip(down["inventory_number"], down["page_id"], down["split"]):
        down_splits[(inv, pid)].add(sp)

    train_ok = set(downstream_train)
    absent = 0
    violations = []
    for key in sorted(comp_keys):
        sp = down_splits.get(key)
        if sp is None:
            absent += 1
        elif not sp <= train_ok:
            violations.append(key)
    if violations:
        raise SplitValidationError(
            f"{len(violations)} page(s) in component {sorted(component_train)} are not in "
            f"downstream {sorted(train_ok)}; first 10: {violations[:10]}")
    return {"checked": len(comp_keys), "absent_downstream": absent}
