"""Tests for archival_structures.analysis.text_extent."""
import unittest

import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.text_extent import (
    classify_corpus_text_extents,
    classify_text_extent,
    compute_corpus_text_extents,
    compute_page_text_extent,
    estimate_stable_margins,
    full_text_page_fraction,
)


def _make_scan(doc_id: str, page_h: int, page_w: int, line_specs):
    """Build a PageXMLScan with lines at given (left, top, right, bottom) coords."""
    lines = [
        pdm.PageXMLTextLine(
            doc_id=f'{doc_id}-l{i}',
            coords=pdm.Coords([(left, top), (right, top), (right, bot), (left, bot)]),
        )
        for i, (left, top, right, bot) in enumerate(line_specs)
    ]
    region = pdm.PageXMLTextRegion(
        doc_id=f'{doc_id}-r',
        coords=pdm.Coords.coords_from_box_params(0, 0, page_w, page_h),
        lines=lines,
    )
    return pdm.PageXMLScan(
        doc_id=doc_id,
        coords=pdm.Coords.coords_from_box_params(0, 0, page_w, page_h),
        text_regions=[region],
    )


class TestComputePageTextExtent(unittest.TestCase):

    def test_basic_margins(self):
        # Page 1000×500; single line from (50,100) to (450,130)
        scan = _make_scan('p', page_h=1000, page_w=500,
                          line_specs=[(50, 100, 450, 130)])
        result = compute_page_text_extent(scan)
        self.assertIsNotNone(result)
        self.assertEqual(result['page_id'], 'p')
        self.assertEqual(result['n_lines'], 1)
        self.assertAlmostEqual(result['top'], 100 / 1000)
        self.assertAlmostEqual(result['bot'], (1000 - 130) / 1000)
        self.assertAlmostEqual(result['left'], 50 / 500)
        self.assertAlmostEqual(result['right'], (500 - 450) / 500)
        # mean_width: single line spans 400 / 500 = 0.80
        self.assertAlmostEqual(result['mean_width'], 400 / 500)
        # equal_frac: only one line, no pairs → 0.0
        self.assertAlmostEqual(result['equal_frac'], 0.0)

    def test_multiple_lines_uses_extremes(self):
        # Two lines: line A near top, line B near bottom
        scan = _make_scan('p2', page_h=1000, page_w=500,
                          line_specs=[(50, 80, 450, 110), (60, 850, 440, 880)])
        result = compute_page_text_extent(scan)
        self.assertAlmostEqual(result['top'], 80 / 1000)
        self.assertAlmostEqual(result['bot'], (1000 - 880) / 1000)
        self.assertAlmostEqual(result['left'], 50 / 500)
        self.assertAlmostEqual(result['right'], (500 - 450) / 500)
        # mean_width: (400 + 380) / 2 / 500 = 0.780
        self.assertAlmostEqual(result['mean_width'], (400 + 380) / 2 / 500)

    def test_equal_frac_identical_consecutive_lines(self):
        # Two lines with identical horizontal span → equal_frac = 1.0
        scan = _make_scan('p_eq', page_h=1000, page_w=500,
                          line_specs=[(50, 100, 450, 130), (50, 200, 450, 230)])
        result = compute_page_text_extent(scan)
        self.assertAlmostEqual(result['equal_frac'], 1.0)

    def test_equal_frac_different_consecutive_lines(self):
        # Two lines with clearly different right extents → equal_frac = 0.0
        scan = _make_scan('p_ne', page_h=1000, page_w=500,
                          line_specs=[(50, 100, 450, 130), (50, 200, 200, 230)])
        result = compute_page_text_extent(scan)
        self.assertAlmostEqual(result['equal_frac'], 0.0)

    def test_equal_frac_within_tolerance(self):
        # Two lines that differ by 5px on a 500px page (1%) — within 2% tolerance
        scan = _make_scan('p_tol', page_h=1000, page_w=500,
                          line_specs=[(50, 100, 450, 130), (52, 200, 452, 230)])
        result = compute_page_text_extent(scan)
        self.assertAlmostEqual(result['equal_frac'], 1.0)

    def test_empty_page_returns_none(self):
        scan = pdm.PageXMLScan(
            doc_id='empty',
            coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
            text_regions=[],
        )
        self.assertIsNone(compute_page_text_extent(scan))

    def test_region_without_lines_returns_none(self):
        region = pdm.PageXMLTextRegion(
            doc_id='r',
            coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
            lines=[],
        )
        scan = pdm.PageXMLScan(
            doc_id='s',
            coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
            text_regions=[region],
        )
        self.assertIsNone(compute_page_text_extent(scan))


class TestComputeCorpusTextExtents(unittest.TestCase):

    def test_shape_and_index(self):
        pages = [
            _make_scan('p0', 1000, 500, [(50, 100, 450, 130)]),
            _make_scan('p1', 1000, 500, [(50, 80, 450, 900)]),
        ]
        df = compute_corpus_text_extents(pages)
        self.assertEqual(list(df.index), ['p0', 'p1'])
        for col in ('top', 'bot', 'left', 'right', 'n_lines', 'mean_width', 'equal_frac'):
            self.assertIn(col, df.columns)

    def test_empty_pages_excluded(self):
        pages = [
            _make_scan('p0', 1000, 500, [(50, 100, 450, 130)]),
            pdm.PageXMLScan(
                doc_id='empty',
                coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
                text_regions=[],
            ),
        ]
        df = compute_corpus_text_extents(pages)
        self.assertEqual(len(df), 1)
        self.assertIn('p0', df.index)
        self.assertNotIn('empty', df.index)

    def test_all_empty_returns_empty_dataframe(self):
        pages = [
            pdm.PageXMLScan(
                doc_id='e',
                coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
                text_regions=[],
            )
        ]
        df = compute_corpus_text_extents(pages)
        self.assertTrue(df.empty)


class TestEstimateStableMargins(unittest.TestCase):

    def test_stable_margins_low_percentile(self):
        # 10 pages; most have top=0.05 but one outlier has top=0.30
        rows = [{'page_id': f'p{i}', 'n_lines': 10, 'top': 0.05, 'bot': 0.05,
                 'left': 0.05, 'right': 0.05} for i in range(9)]
        rows.append({'page_id': 'outlier', 'n_lines': 2, 'top': 0.30, 'bot': 0.40,
                     'left': 0.30, 'right': 0.30})
        df = pd.DataFrame(rows).set_index('page_id')
        stable = estimate_stable_margins(df, percentile=10)
        # 10th-percentile of [0.05]*9 + [0.30] ≈ 0.05
        self.assertAlmostEqual(stable['top'], 0.05, places=3)
        self.assertAlmostEqual(stable['bot'], 0.05, places=3)


class TestClassifyTextExtent(unittest.TestCase):

    def _stable(self):
        return {'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05}

    def test_full_text(self):
        row = {'top': 0.06, 'bot': 0.06}
        self.assertEqual(classify_text_extent(row, self._stable()), 'full_text')

    def test_late_start(self):
        row = {'top': 0.15, 'bot': 0.06}
        self.assertEqual(classify_text_extent(row, self._stable()), 'late_start')

    def test_early_end(self):
        row = {'top': 0.06, 'bot': 0.20}
        self.assertEqual(classify_text_extent(row, self._stable()), 'early_end')

    def test_short(self):
        row = {'top': 0.30, 'bot': 0.40}
        self.assertEqual(classify_text_extent(row, self._stable()), 'short')

    def test_exact_tolerance_boundary_is_full_text(self):
        row = {'top': 0.05 + 0.05, 'bot': 0.05 + 0.05}
        self.assertEqual(classify_text_extent(row, self._stable()), 'full_text')

    def test_just_over_tolerance_is_late_start(self):
        row = {'top': 0.05 + 0.05 + 0.001, 'bot': 0.06}
        self.assertEqual(classify_text_extent(row, self._stable()), 'late_start')


class TestClassifyCorpusTextExtents(unittest.TestCase):

    def _make_df(self, rows):
        return pd.DataFrame(rows).set_index('page_id')

    def test_returns_series_with_correct_labels(self):
        rows = [
            {'page_id': 'full', 'n_lines': 50, 'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05},
            {'page_id': 'late', 'n_lines': 10, 'top': 0.30, 'bot': 0.05, 'left': 0.05, 'right': 0.05},
            {'page_id': 'short', 'n_lines': 5, 'top': 0.30, 'bot': 0.40, 'left': 0.05, 'right': 0.05},
        ]
        df = self._make_df(rows)
        stable = {'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05}
        classes = classify_corpus_text_extents(df, stable=stable)
        self.assertEqual(classes['full'], 'full_text')
        self.assertEqual(classes['late'], 'late_start')
        self.assertEqual(classes['short'], 'short')

    def test_infers_stable_if_not_given(self):
        rows = [
            {'page_id': f'p{i}', 'n_lines': 50, 'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05}
            for i in range(10)
        ]
        df = self._make_df(rows)
        classes = classify_corpus_text_extents(df)
        self.assertTrue((classes == 'full_text').all())


class TestFullTextPageFraction(unittest.TestCase):

    def _make_df(self, rows):
        return pd.DataFrame(rows).set_index('page_id')

    def test_all_full_text(self):
        rows = [
            {'page_id': f'p{i}', 'n_lines': 50, 'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05}
            for i in range(10)
        ]
        df = self._make_df(rows)
        stable = {'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05}
        self.assertAlmostEqual(full_text_page_fraction(df, stable=stable), 1.0)

    def test_half_full_text(self):
        rows = [
            {'page_id': 'p0', 'n_lines': 50, 'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05},
            {'page_id': 'p1', 'n_lines': 5, 'top': 0.40, 'bot': 0.40, 'left': 0.05, 'right': 0.05},
        ]
        df = self._make_df(rows)
        stable = {'top': 0.05, 'bot': 0.05, 'left': 0.05, 'right': 0.05}
        self.assertAlmostEqual(full_text_page_fraction(df, stable=stable), 0.5)

    def test_empty_dataframe_returns_zero(self):
        df = pd.DataFrame(columns=['page_id', 'n_lines', 'top', 'bot', 'left', 'right']).set_index('page_id')
        self.assertEqual(full_text_page_fraction(df), 0.0)


if __name__ == '__main__':
    unittest.main()
