import numpy as np
import pandas as pd
import pytest

from archival_structures.analysis.page_numbering import (
    AlignmentConfig, align_all, align_documents, anchors_array, confidence_band,
    interpolate_folio_to_scan, longest_increasing_chain, main_chain, numbering_mode,
    numbering_restarts, page_number_anchors)


def A(pairs):
    return np.array(pairs, float)


def test_lis_strict_and_later_duplicate_wins():
    assert longest_increasing_chain([1, 2, 2, 3]) == [0, 2, 3]
    assert longest_increasing_chain([5, 5, 5]) == [2]


def test_lis_with_noise():
    vals = list(range(1, 51))
    for i, v in zip(range(3, 50, 6), [900, 0, 700, 2, 800, 1, 600, 3]):
        vals[i] = v
    idx = longest_increasing_chain(vals)
    assert all(vals[a] < vals[b] for a, b in zip(idx, idx[1:]))
    outliers = {3, 9, 15, 21, 27, 33, 39, 45}
    assert len(idx) >= 42 and not outliers & set(idx)


def test_lis_empty():
    assert longest_increasing_chain([]) == []


def _two_runs():
    first = [(i + 1, i + 1) for i in range(20)]
    second = [(21 + i, i + 1) for i in range(15)]
    return A(first + second)


def test_numbering_restarts_finds_second_run():
    a = _two_runs()
    chain, keep = main_chain(a)
    runs = numbering_restarts(a, keep)
    assert len(chain) == 20 and len(runs) == 1 and len(runs[0]) == 15
    assert runs[0][0, 0] == 21


def test_numbering_restarts_ignores_short_runs():
    a = A([(i + 1, i + 1) for i in range(20)] + [(21, 1), (22, 2)])
    _, keep = main_chain(a)
    assert numbering_restarts(a, keep) == []


def test_numbering_mode_foliation_pagination_spread_unknown():
    mk = lambda step: A([(1 + step * i, 1 + i) for i in range(5)])
    assert numbering_mode(mk(2)) == ("foliation", 2.0)
    assert numbering_mode(mk(1)) == ("pagination", 1.0)
    assert numbering_mode(mk(0.5)) == ("spread", 0.5)
    mode, slope = numbering_mode(A([(1, 1), (3, 2)]))
    assert mode == "unknown" and np.isnan(slope)


def test_interpolate_inside_clamped_and_extrapolated():
    run = A([(10 + 2 * i, 1 + i) for i in range(10)])  # folio f -> scan 8 + 2f
    s, d, inside = interpolate_folio_to_scan(5, run)
    assert (s, d, inside) == (18.0, 0.0, True)
    s, d, inside = interpolate_folio_to_scan(15, run)  # clamped to last anchor
    assert s == 28.0 and not inside
    s, d, inside = interpolate_folio_to_scan(15, run, extrap=True)
    assert s == 38.0 and not inside
    s, _, _ = interpolate_folio_to_scan(15, run, extrap=True, max_scan=33)
    assert s == 33.0
    s, _, _ = interpolate_folio_to_scan(-20, run, extrap=True)
    assert s == 1.0


def test_confidence_band_thresholds():
    assert confidence_band(3, True) == "high"
    assert confidence_band(4, True) == "medium"
    assert confidence_band(10, True) == "medium"
    assert confidence_band(11, True) == "low"
    assert confidence_band(0, False) == "low"


def _docs(starts, ends):
    return pd.DataFrame({"folio_start": starts, "folio_end": ends})


def test_align_documents_foliation():
    scans = list(range(1, 41, 2))  # anchors every other scan
    anchors = A([(s, (s + 1) // 2) for s in scans])
    docs = _docs([1.0, 5.0], [4.0, 9.0])
    res = align_documents(docs, anchors, AlignmentConfig(mode=True))
    assert list(res.columns) == ["start_scan", "start_scan_alt", "end_scan", "numbering_mode",
                                 "method", "nearest_anchor_dist", "confidence"]
    assert list(res.start_scan) == [1, 9]
    assert list(res.start_scan_alt) == [2, 10]
    assert list(res.end_scan) == [8, 18]
    assert set(res.numbering_mode) == {"foliation"} and set(res.method) == {"chain"}


def test_align_documents_no_anchors_and_no_folio():
    docs = _docs([1.0, np.nan], [2.0, np.nan])
    r = align_documents(docs, None)
    assert set(r.method) == {"no_anchors"} and set(r.confidence) == {"none"}
    r = align_documents(docs, A([(1, 1), (2, 2), (3, 3)]))
    assert list(r.method) == ["chain", "no_folio"]
    assert r.numbering_mode.iloc[0] == "unused"


def test_dup_first_keeps_first_scan():
    anchors = A([(1, 1), (2, 2), (3, 2), (4, 3), (5, 4)])
    docs = _docs([2.0], [np.nan])
    assert align_documents(docs, anchors).start_scan.iloc[0] == 3  # later duplicate
    assert align_documents(docs, anchors, AlignmentConfig(dup_first=True)).start_scan.iloc[0] == 2


def test_bankers_rounding_is_kept():
    anchors = A([(10, 1), (15, 2), (20, 3)])  # folio 1.5 -> 12.5 -> 12
    res = align_documents(_docs([1.5, 2.5], [np.nan, np.nan]), anchors)
    assert list(res.start_scan) == [12, 18]  # 17.5 -> 18 (even)


def test_extrapolated_suffix_without_extrap():
    anchors = A([(10, 1), (15, 2), (20, 3)])
    res = align_documents(_docs([7.0], [np.nan]), anchors)
    assert res.method.iloc[0] == "chain+extrapolated" and res.start_scan.iloc[0] == 20


def test_align_all_requires_unique_index():
    df = pd.DataFrame({"inventory_number": ["a", "a"], "folio_start": [1.0, 2.0],
                       "folio_end": [1.0, 2.0]}, index=[0, 0])
    with pytest.raises(ValueError):
        align_all(df, {})


def test_align_all_groups_and_joins():
    df = pd.DataFrame({"inventory_number": ["a", "b", "a"], "folio_start": [1.0, 1.0, 3.0],
                       "folio_end": [2.0, 2.0, 3.0]})
    anchors = {"a": A([(1, 1), (2, 2), (3, 3)])}
    out = align_all(df, anchors, max_scans={"a": 3})
    assert list(out.index) == [0, 1, 2] and "n_sections" not in out.columns
    assert list(out.method) == ["chain", "no_anchors", "chain"]
    assert out.start_scan.iloc[2] == 3


def _regions(rows):
    return pd.DataFrame(rows, columns=["scan_id", "region_tag", "region_text"])


def test_page_number_anchors_last_region_wins():
    r = _regions([("NL-HaNA_1.04.02_1000_0003.jpg", "page-number", "7"), ("NL-HaNA_1.04.02_1000_0003.jpg", "page-number", "8"),
                  ("NL-HaNA_1.04.02_1000_0002.jpg", "page-number", " 5 "), ("NL-HaNA_1.04.02_1000_0002.jpg", "paragraph", "99")])
    out = page_number_anchors(r)
    assert list(out.columns) == ["scan_id", "scan_no", "page_number"]
    assert list(out.scan_no) == [2, 3] and list(out.page_number) == [5, 8]
    assert list(out.index) == [0, 1]


def test_page_number_anchors_non_digit_last_region_drops_page():
    r = _regions([("NL-HaNA_1.04.02_1000_0001.jpg", "page-number", "7"), ("NL-HaNA_1.04.02_1000_0001.jpg", "page-number", "7a")])
    assert page_number_anchors(r).empty


def test_page_number_anchors_empty_and_nan_text_skipped():
    r = _regions([("NL-HaNA_1.04.02_1000_0001.jpg", "page-number", "7"), ("NL-HaNA_1.04.02_1000_0001.jpg", "page-number", np.nan),
                  ("NL-HaNA_1.04.02_1000_0001.jpg", "page-number", "  ")])
    out = page_number_anchors(r)
    assert list(out.page_number) == [7]


def test_page_number_anchors_custom_tag_and_pattern():
    r = _regions([("NL-HaNA_1.04.02_1000_0001.jpg", "folio", "3"),
                  ("NL-HaNA_1.04.02_1000_0002.jpg", "folio", "123"),
                  ("NL-HaNA_1.04.02_1000_0002.jpg", "page-number", "9")])
    assert list(page_number_anchors(r, tag="folio").scan_no) == [1, 2]
    out = page_number_anchors(r, tag="folio", pattern=r"^\d$")
    assert list(out.scan_no) == [1] and list(out.page_number) == [3]


def test_anchors_array_sorted():
    df = pd.DataFrame({"scan_no": [3, 1], "page_number": [2, 1]})
    arr = anchors_array(df)
    assert arr.dtype == float and arr.tolist() == [[1, 1], [3, 2]]
