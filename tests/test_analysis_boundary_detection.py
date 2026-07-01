import unittest

import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.boundary_detection import (
    is_empty_page, build_page_sequence, empty_page_neighbor_clusters, empty_page_transitions,
)


def make_line(line_id: str, text: str = 'This is a line with more than twenty characters of content.',
              x: int = 10, y: int = 10, w: int = 400, h: int = 20) -> pdm.PageXMLTextLine:
    return pdm.PageXMLTextLine(
        doc_id=line_id,
        coords=pdm.Coords.coords_from_box_params(x, y, w, h),
        text=text,
    )


def make_scan(scan_id: str, lines=None) -> pdm.PageXMLScan:
    region = pdm.PageXMLTextRegion(
        doc_id=f'{scan_id}-r1',
        coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
        lines=lines or [],
    )
    return pdm.PageXMLScan(
        doc_id=scan_id,
        coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
        text_regions=[region],
    )


class TestIsEmptyPage(unittest.TestCase):

    def test_zero_lines_is_empty(self):
        scan = make_scan('s1', lines=[])
        self.assertTrue(is_empty_page(scan))

    def test_all_empty_text_is_empty(self):
        lines = [make_line(f'l{i}', text='') for i in range(3)]
        scan = make_scan('s1', lines=lines)
        self.assertTrue(is_empty_page(scan))

    def test_real_text_is_not_empty(self):
        lines = [make_line('l1', text='This is a real sentence with plenty of content.')]
        scan = make_scan('s1', lines=lines)
        self.assertFalse(is_empty_page(scan))

    def test_noise_chars_below_threshold(self):
        # 3 lines each with 3 chars = 9 total < 20 → empty
        lines = [make_line(f'l{i}', text='abc') for i in range(3)]
        scan = make_scan('s1', lines=lines)
        self.assertTrue(is_empty_page(scan, max_total_text=20))

    def test_noise_chars_above_threshold(self):
        # 3 lines each with 10 chars = 30 total > 20 → not empty
        lines = [make_line(f'l{i}', text='hello wrld') for i in range(3)]
        scan = make_scan('s1', lines=lines)
        self.assertFalse(is_empty_page(scan, max_total_text=20))

    def test_custom_threshold(self):
        lines = [make_line('l1', text='Hi')]
        scan = make_scan('s1', lines=lines)
        # 2 chars < 5 → empty
        self.assertTrue(is_empty_page(scan, max_total_text=5))
        # 2 chars >= 1 → not empty
        self.assertFalse(is_empty_page(scan, max_total_text=1))


class TestBuildPageSequence(unittest.TestCase):

    def _make_pages(self):
        long_text = 'This is a line with more than twenty characters of content.'
        pages = [make_scan(f's{i}', lines=[make_line(f's{i}-l1', text=long_text)]) for i in range(4)]
        pages.append(make_scan('s4', lines=[]))  # empty page
        return pages

    def test_shape_and_columns(self):
        pages = self._make_pages()
        clusters = pd.Series([0, 0, 1, 1], index=['s0', 's1', 's2', 's3'], name='cluster')
        df = build_page_sequence(pages, clusters)
        self.assertEqual(len(pages), len(df))
        for col in ['page_id', 'cluster', 'is_empty', 'position']:
            self.assertIn(col, df.columns)

    def test_is_empty_column(self):
        pages = self._make_pages()
        clusters = pd.Series([], dtype=int, name='cluster')
        df = build_page_sequence(pages, clusters)
        # s0..s3 have text → not empty; s4 has no lines → empty
        self.assertFalse(df.loc[df['page_id'] == 's0', 'is_empty'].iloc[0])
        self.assertTrue(df.loc[df['page_id'] == 's4', 'is_empty'].iloc[0])

    def test_position_is_zero_based_sequential(self):
        pages = self._make_pages()
        clusters = pd.Series([], dtype=int, name='cluster')
        df = build_page_sequence(pages, clusters)
        self.assertEqual(list(range(len(pages))), list(df['position']))

    def test_missing_cluster_gets_na(self):
        pages = self._make_pages()
        clusters = pd.Series([0], index=['s0'], name='cluster')
        df = build_page_sequence(pages, clusters)
        missing = df.loc[df['page_id'] == 's1', 'cluster'].iloc[0]
        self.assertTrue(pd.isna(missing))


class TestEmptyPageNeighborClusters(unittest.TestCase):

    def _make_sequence_df(self):
        # sequence: cluster 0, 0, empty, 1, 1
        rows = [
            {'page_id': 'p0', 'cluster': 0, 'is_empty': False, 'position': 0},
            {'page_id': 'p1', 'cluster': 0, 'is_empty': False, 'position': 1},
            {'page_id': 'p2', 'cluster': pd.NA, 'is_empty': True, 'position': 2},
            {'page_id': 'p3', 'cluster': 1, 'is_empty': False, 'position': 3},
            {'page_id': 'p4', 'cluster': 1, 'is_empty': False, 'position': 4},
        ]
        return pd.DataFrame(rows)

    def test_window_2_finds_correct_neighbors(self):
        df = self._make_sequence_df()
        result = empty_page_neighbor_clusters(df, window=2)
        self.assertFalse(result.empty)
        # p1 (cluster 0) is at rel_position -1 from empty page at position 2
        # p3 (cluster 1) is at rel_position +1
        rel_positions = set(result['rel_position'])
        self.assertIn(-1, rel_positions)
        self.assertIn(1, rel_positions)

    def test_window_0_returns_no_neighbors(self):
        df = self._make_sequence_df()
        result = empty_page_neighbor_clusters(df, window=0)
        self.assertTrue(result.empty)

    def test_far_page_excluded_from_small_window(self):
        df = self._make_sequence_df()
        result = empty_page_neighbor_clusters(df, window=1)
        # p0 (position 0) is at distance 2 from the empty page (position 2), so it
        # should not appear at rel_position=-2 in a window=1 result
        self.assertFalse((result['rel_position'] == -2).any())


class TestEmptyPageTransitions(unittest.TestCase):

    def _make_sequence_df(self):
        rows = [
            {'page_id': 'p0', 'cluster': 0, 'is_empty': False, 'position': 0},
            {'page_id': 'p1', 'cluster': 0, 'is_empty': False, 'position': 1},
            {'page_id': 'p2', 'cluster': pd.NA, 'is_empty': True, 'position': 2},
            {'page_id': 'p3', 'cluster': 1, 'is_empty': False, 'position': 3},
            {'page_id': 'p4', 'cluster': 1, 'is_empty': False, 'position': 4},
        ]
        return pd.DataFrame(rows)

    def test_before_is_cluster_before_empty(self):
        df = self._make_sequence_df()
        before, after = empty_page_transitions(df)
        # The closest non-empty page before position 2 is p1 (cluster 0)
        self.assertIn(0, before.index)

    def test_after_is_cluster_after_empty(self):
        df = self._make_sequence_df()
        before, after = empty_page_transitions(df)
        # The closest non-empty page after position 2 is p3 (cluster 1)
        self.assertIn(1, after.index)

    def test_multiple_empty_pages(self):
        rows = [
            {'page_id': 'p0', 'cluster': 0, 'is_empty': False, 'position': 0},
            {'page_id': 'p1', 'cluster': pd.NA, 'is_empty': True, 'position': 1},
            {'page_id': 'p2', 'cluster': 1, 'is_empty': False, 'position': 2},
            {'page_id': 'p3', 'cluster': pd.NA, 'is_empty': True, 'position': 3},
            {'page_id': 'p4', 'cluster': 2, 'is_empty': False, 'position': 4},
        ]
        df = pd.DataFrame(rows)
        before, after = empty_page_transitions(df)
        # 2 empty pages → 2 before entries: clusters 0 and 1
        self.assertEqual(2, before.sum())
        self.assertEqual(2, after.sum())


if __name__ == '__main__':
    unittest.main()
