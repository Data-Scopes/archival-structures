"""Tests for evaluation.splits (C7): chunk splits, held-out builder, validators."""

import logging
import random

import pandas as pd
import pytest

from archival_structures.stream_analysis.evaluation import splits as S
from archival_structures.stream_analysis.evaluation.splits import (
    Page,
    SplitValidationError,
    build_archival_dataset_splits,
    build_inventory_held_out,
    build_long_sequence_split,
    check_context_not_scored,
    check_nested_split,
    check_no_document_crosses_partitions,
    check_no_duplicate_page_ids,
    check_split_coverage,
    core_rows,
    eval_scan_ids,
    pages_from_starts,
)


def _doc_file(n_docs, size, prefix="p"):
    """A file of n_docs documents of `size` pages; start on page 0, end on page size-1."""
    pages = []
    for d in range(n_docs):
        for j in range(size):
            pages.append(Page(f"{prefix}{d * size + j}", j == 0, j == size - 1))
    return pages


A = _doc_file(10, 10)
B = _doc_file(2, 5)


def test_package_reexports():
    from archival_structures.stream_analysis import evaluation as ev
    for name in ("check_nested_split", "core_rows", "eval_scan_ids", "build_inventory_held_out"):
        assert hasattr(ev, name) and name in ev.__all__


def test_documents_tile_exactly():
    files = {"A": A, "B": B, "C": _doc_file(7, 3)}
    chunks, _ = build_archival_dataset_splits(files, num_chunks_per_file=4)
    for fid, pages in files.items():
        covered = [i for c in chunks if c.file_id == fid for i in c.core_page_indices]
        assert covered == list(range(len(pages)))


def test_oversized_document_is_own_chunk():
    pages = _doc_file(1, 30) + _doc_file(4, 2, prefix="q")
    chunks, _ = build_archival_dataset_splits({"X": pages}, num_chunks_per_file=4)
    big = [c for c in chunks if c.page_count == 30]
    assert len(big) == 1 and len(big[0].documents) == 1


def test_pages_from_starts():
    ids = [f"s{i}" for i in range(7)]
    pages = pages_from_starts(ids, {"s0", "s3", "s5"})
    assert [p.is_doc_start for p in pages] == [1, 0, 0, 1, 0, 1, 0]
    assert [p.is_doc_end for p in pages] == [0, 0, 1, 0, 1, 0, 1]
    pages = pages_from_starts(ids, {"s2", "s4"})  # first page is not a start
    assert [p.is_doc_start for p in pages] == [0, 0, 1, 0, 1, 0, 0]
    assert [p.is_doc_end for p in pages] == [0, 1, 0, 1, 0, 0, 1]
    assert [d.page_count for d in S.parse_documents(pages)] == [2, 2, 3]


def test_context_bounds_and_mask():
    files = {"A": A, "B": B}
    res = build_long_sequence_split(files, num_chunks_per_file=10, context_window=3,
                                    include_context=True)
    n = {k: len(v) for k, v in files.items()}
    for sub in res.subsequences:
        assert 0 <= sub.context_start_idx <= sub.core_start_idx
        assert sub.core_end_idx <= sub.context_end_idx <= n[sub.file_id] - 1
    df = res.to_long_dataframe()
    assert (df.loc[df["role"] == "context", "eval_mask"] == 0).all()
    assert (df["role"] == "context").any()
    check_context_not_scored(df)
    bad = df.copy()
    bad.loc[bad.index[bad["role"] == "context"][0], "eval_mask"] = 1
    with pytest.raises(SplitValidationError):
        check_context_not_scored(bad)
    assert len(core_rows(df)) == sum(n.values())


def test_p2_striding_pinned():
    chunks, dist = build_archival_dataset_splits({"A": A}, num_chunks_per_file=10)
    assert [c.split for c in chunks] == ["train", "train", "train", "test", "dev", "train",
                                         "train", "train", "train", "test"]
    assert dist == {"train": 70.0, "dev": 10.0, "test": 20.0}


def test_p2_small_file_stays_train():
    chunks, dist = build_archival_dataset_splits({"A": A, "B": B}, num_chunks_per_file=10)
    assert [c.split for c in chunks if c.file_id == "B"] == ["train", "train"]
    assert dist == {"train": 72.73, "dev": 9.09, "test": 18.18}


def test_p2_context_pinned():
    chunks, _ = build_archival_dataset_splits({"B": B}, num_chunks_per_file=2)
    got = [(c.split, c.context_start_idx, c.core_start_idx, c.core_end_idx, c.context_end_idx)
           for c in chunks]
    assert got == [("train", 0, 0, 4, 6), ("train", 3, 5, 9, 9)]


def test_no_print_from_builder(capsys):
    build_long_sequence_split({"A": A}, num_chunks_per_file=10, include_context=True)
    out = capsys.readouterr()
    assert out.out == "" and out.err == ""


def test_builder_logs_info(caplog):
    with caplog.at_level(logging.INFO, logger=S.__name__):
        build_long_sequence_split({"A": A}, num_chunks_per_file=10)
    assert any("Total Chunks" in r.message for r in caplog.records)


SEQ = {str(i): [f"{i}_{j}" for j in range(3)] for i in range(10)}


def test_held_out_whole_inventories():
    df, held = build_inventory_held_out(SEQ, held_out={"1", "4"})
    assert held == ["1", "4"]
    per = df.groupby("inventory_number")["split"].nunique()
    assert (per == 1).all()
    assert set(df.loc[df["split"] == "test", "inventory_number"]) == {"1", "4"}
    assert list(df["inventory_number"].unique()) == list(SEQ)  # sequences order


def test_held_out_seed_sampling():
    df, held = build_inventory_held_out(SEQ, seed=1, n_held_out=5)
    assert held == sorted(random.Random(1).sample(sorted(SEQ), 5))
    assert set(df.loc[df["split"] == "test", "inventory_number"]) == set(held)


def test_held_out_unknown_raises():
    with pytest.raises(ValueError):
        build_inventory_held_out(SEQ, held_out={"nope"})


def _df(rows, cols=("inventory_number", "page_id", "split")):
    return pd.DataFrame(rows, columns=list(cols))


def test_duplicate_page_ids():
    df = _df([("1", "a", "train"), ("1", "a", "dev")])
    with pytest.raises(SplitValidationError) as ei:
        check_no_duplicate_page_ids(df)
    assert isinstance(ei.value, AssertionError)
    df = _df([("1", "a", "train"), ("1", "a", "train"), ("1", "b", "dev")])
    assert check_no_duplicate_page_ids(df) == 1


def test_document_crosses_partitions():
    seqs = {"1": ["a", "b", "c", "d"]}
    starts = {"1": {"a", "c"}}
    ok = _df([("1", "a", "train"), ("1", "b", "train"), ("1", "c", "dev"), ("1", "d", "dev")])
    check_no_document_crosses_partitions(ok, seqs, starts)
    bad = _df([("1", "a", "train"), ("1", "b", "dev"), ("1", "c", "dev"), ("1", "d", "dev")])
    with pytest.raises(SplitValidationError):
        check_no_document_crosses_partitions(bad, seqs, starts)


def test_split_coverage():
    df = _df([("1", "a", "train"), ("1", "b", "dev"), ("1", "c", "test"),
              ("2", "d", "train"), ("2", "e", "dev")])
    with pytest.raises(SplitValidationError):
        check_split_coverage(df, ["1", "2"])
    with pytest.raises(SplitValidationError):
        check_split_coverage(df, ["1", "2"], known_gaps={"2": {"dev"}})
    assert check_split_coverage(df, ["1", "2"], known_gaps={"2": {"test"}}) == [("2", "test")]


def test_nested_split():
    comp = _df([("1", "a", "train"), ("1", "b", "train"), ("1", "c", "dev")])
    down = _df([("1", "a", "train"), ("1", "b", "train"), ("1", "c", "dev")])
    assert check_nested_split(comp, down) == {"checked": 2, "absent_downstream": 0}
    down2 = _df([("1", "a", "train")])
    assert check_nested_split(comp, down2) == {"checked": 2, "absent_downstream": 1}
    down3 = _df([("1", "a", "train"), ("1", "b", "dev")])
    with pytest.raises(SplitValidationError, match="1 page"):
        check_nested_split(comp, down3)


def test_nested_split_ignores_context():
    cols = ("inventory_number", "page_id", "split", "role")
    comp = _df([("1", "a", "train", "core"), ("1", "x", "train", "context")], cols)
    down = _df([("1", "a", "train", "core"), ("1", "x", "dev", "core")], cols)
    assert check_nested_split(comp, down) == {"checked": 1, "absent_downstream": 0}
    down_ctx = _df([("1", "a", "train", "core"), ("1", "x", "dev", "context")], cols)
    assert check_nested_split(comp, down_ctx)["checked"] == 1


def test_eval_scan_ids():
    cols = ("inventory_number", "page_id", "split", "role")
    df = _df([("1", "a", "dev", "core"), ("1", "b", "dev", "context"), ("2", "c", "dev", "core"),
              ("2", "d", "train", "core")], cols)
    assert eval_scan_ids(df, "dev") == ["1/a", "2/c"]
    assert eval_scan_ids(df, "dev", sep="_") == ["1_a", "2_c"]
