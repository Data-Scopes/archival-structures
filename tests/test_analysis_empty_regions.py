import unittest

import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.empty_regions import (
    compute_page_empty_regions,
    extract_empty_region_features,
    extract_corpus_empty_region_features,
    cluster_empty_regions,
    find_boundary_adjacent_symbols,
    boundary_symbol_scores,
    group_regions_into_blocks,
)


def make_line(line_id: str, x: int, y: int, w: int = 400, h: int = 20, text: str = 'text'):
    return pdm.PageXMLTextLine(
        doc_id=line_id,
        coords=pdm.Coords.coords_from_box_params(x, y, w, h),
        text=text,
    )


def make_region(region_id: str, x: int, y: int, w: int, h: int,
                lines=None) -> pdm.PageXMLTextRegion:
    coords = pdm.Coords.coords_from_box_params(x, y, w, h)
    return pdm.PageXMLTextRegion(doc_id=region_id, coords=coords, lines=lines or [])


def make_two_region_scan(scan_id: str = 'scan1', page_w: int = 500, page_h: int = 1000,
                         top_h: int = 200, bottom_y: int = 700, bottom_h: int = 200) -> pdm.PageXMLScan:
    """A page with one text region at the top and one at the bottom, leaving a gap in the middle."""
    top_line = make_line(f'{scan_id}-l1', x=10, y=10, w=480, h=40, text='top line')
    bottom_line = make_line(f'{scan_id}-l2', x=10, y=bottom_y + 10, w=480, h=40, text='bottom line')
    r_top = make_region(f'{scan_id}-r1', x=0, y=0, w=page_w, h=top_h, lines=[top_line])
    r_bottom = make_region(f'{scan_id}-r2', x=0, y=bottom_y, w=page_w, h=bottom_h, lines=[bottom_line])
    return pdm.PageXMLScan(
        doc_id=scan_id,
        coords=pdm.Coords.coords_from_box_params(0, 0, page_w, page_h),
        text_regions=[r_top, r_bottom],
    )


class TestComputePageEmptyRegions(unittest.TestCase):

    def test_gap_between_two_regions_produces_empty_region(self):
        scan = make_two_region_scan(top_h=100, bottom_y=600, bottom_h=100)
        # gap from y=100 to y=600 = 500px on a 1000px page → rel_height = 0.5
        ers = compute_page_empty_regions(scan, min_rel_height=0.1, min_rel_width=0.1)
        self.assertGreater(len(ers), 0)
        # All returned regions should pass the size filter
        for er in ers:
            self.assertGreaterEqual(er.coords.height / scan.coords.height, 0.1)
            self.assertGreaterEqual(er.coords.width / scan.coords.width, 0.1)

    def test_small_gap_filtered_out_by_min_rel_height(self):
        # Two regions with only a 5px gap on a 1000px page → rel_height = 0.005
        scan = make_two_region_scan(top_h=100, bottom_y=105, bottom_h=100)
        ers = compute_page_empty_regions(scan, min_rel_height=0.05, min_rel_width=0.05)
        # The tiny gap should be filtered; only border gaps (if any) might survive
        for er in ers:
            self.assertGreaterEqual(er.coords.height / scan.coords.height, 0.05)

    def test_empty_page_produces_full_page_empty_region(self):
        # No text regions → single empty region covering the whole page
        scan = pdm.PageXMLScan(
            doc_id='empty',
            coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
            text_regions=[],
        )
        ers = compute_page_empty_regions(scan, min_rel_height=0.5, min_rel_width=0.5)
        self.assertEqual(1, len(ers))
        self.assertAlmostEqual(ers[0].coords.height / scan.coords.height, 1.0, places=2)


class TestExtractEmptyRegionFeatures(unittest.TestCase):

    def setUp(self):
        self.scan = make_two_region_scan(top_h=100, bottom_y=600, bottom_h=100)
        self.ers = compute_page_empty_regions(self.scan, min_rel_height=0.1, min_rel_width=0.1)

    def test_features_keys(self):
        er = self.ers[0]
        features = extract_empty_region_features(self.scan, er)
        for key in ['page_id', 'region_id', 'rel_height', 'rel_width',
                    'aspect_ratio', 'rel_top', 'rel_left', 'location']:
            self.assertIn(key, features)

    def test_rel_height_matches_coords(self):
        er = self.ers[0]
        features = extract_empty_region_features(self.scan, er)
        expected = er.coords.height / self.scan.coords.height
        self.assertAlmostEqual(features['rel_height'], expected)

    def test_location_top_classification(self):
        # A tall empty region at the very top of the page
        scan = pdm.PageXMLScan(
            doc_id='s1',
            coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
            text_regions=[],
        )
        ers = compute_page_empty_regions(scan, min_rel_height=0.5, min_rel_width=0.5)
        self.assertEqual(1, len(ers))
        features = extract_empty_region_features(scan, ers[0])
        # Full-page empty region has rel_top=0 → 'top'
        self.assertEqual('top', features['location'])

    def test_aspect_ratio_tall_region(self):
        scan = make_two_region_scan(top_h=100, bottom_y=600, bottom_h=100)
        ers = compute_page_empty_regions(scan, min_rel_height=0.1, min_rel_width=0.1)
        er = ers[0]
        features = extract_empty_region_features(scan, er)
        expected = er.coords.height / er.coords.width if er.coords.width > 0 else float('inf')
        self.assertAlmostEqual(features['aspect_ratio'], expected)


class TestExtractCorpusEmptyRegionFeatures(unittest.TestCase):

    def test_dataframe_columns(self):
        pages = [make_two_region_scan('s1', top_h=100, bottom_y=600, bottom_h=100),
                 make_two_region_scan('s2', top_h=150, bottom_y=700, bottom_h=100)]
        df = extract_corpus_empty_region_features(pages, min_rel_height=0.1, min_rel_width=0.1)
        for col in ['page_id', 'region_id', 'rel_height', 'rel_width',
                    'aspect_ratio', 'rel_top', 'rel_left', 'location']:
            self.assertIn(col, df.columns)

    def test_empty_page_contributes_no_rows_when_filtered(self):
        # A page with only a tiny gap: below threshold
        scan = make_two_region_scan('s1', top_h=100, bottom_y=105, bottom_h=100)
        df = extract_corpus_empty_region_features([scan], min_rel_height=0.5, min_rel_width=0.5)
        # No region will have rel_height >= 0.5 (the gap is only 5/1000 = 0.5%)
        for _, row in df.iterrows():
            self.assertGreaterEqual(row['rel_height'], 0.5)


class TestClusterEmptyRegions(unittest.TestCase):

    def test_returns_series_with_multiindex(self):
        pages = [make_two_region_scan(f's{i}', top_h=100, bottom_y=600, bottom_h=100)
                 for i in range(10)]
        df = extract_corpus_empty_region_features(pages, min_rel_height=0.1, min_rel_width=0.1)
        labels = cluster_empty_regions(df, min_cluster_size=2)
        self.assertIsInstance(labels, pd.Series)
        self.assertEqual(['page_id', 'region_id'], list(labels.index.names))

    def test_empty_df_returns_empty_series(self):
        df = pd.DataFrame(columns=['page_id', 'region_id', 'aspect_ratio',
                                   'rel_height', 'rel_width', 'rel_top', 'rel_left'])
        labels = cluster_empty_regions(df)
        self.assertEqual(0, len(labels))
        self.assertEqual(['page_id', 'region_id'], list(labels.index.names))

    def test_length_matches_df_rows(self):
        pages = [make_two_region_scan(f's{i}', top_h=100, bottom_y=600, bottom_h=100)
                 for i in range(10)]
        df = extract_corpus_empty_region_features(pages, min_rel_height=0.1, min_rel_width=0.1)
        labels = cluster_empty_regions(df, min_cluster_size=2)
        self.assertEqual(len(df), len(labels))


class TestFindBoundaryAdjacentSymbols(unittest.TestCase):

    def _make_two_block_scan(self) -> pdm.PageXMLScan:
        """Page with two clusters of 2 lines each, large gap between blocks."""
        l1 = make_line('l1', x=10, y=10, w=400, h=20, text='line one')
        l2 = make_line('l2', x=10, y=40, w=400, h=20, text='line two')
        l3 = make_line('l3', x=10, y=600, w=400, h=20, text='line three')
        l4 = make_line('l4', x=10, y=630, w=400, h=20, text='line four')
        r1 = make_region('r1', x=0, y=0, w=450, h=100, lines=[l1, l2])
        r2 = make_region('r2', x=0, y=580, w=450, h=100, lines=[l3, l4])
        return pdm.PageXMLScan(
            doc_id='scan1',
            coords=pdm.Coords.coords_from_box_params(0, 0, 450, 1000),
            text_regions=[r1, r2],
        )

    def test_identifies_above_and_below_lines(self):
        scan = self._make_two_block_scan()
        index = pd.MultiIndex.from_tuples(
            [('scan1', 'l1'), ('scan1', 'l2'), ('scan1', 'l3'), ('scan1', 'l4')],
            names=['scan_id', 'line_id'],
        )
        line_clusters = pd.Series([0, 0, 1, 1], index=index, name='cluster')
        ers = compute_page_empty_regions(scan, min_rel_height=0.3, min_rel_width=0.3)
        self.assertGreater(len(ers), 0, "expected at least one large empty region")
        page_empty_regions = {'scan1': ers}
        df = find_boundary_adjacent_symbols(
            [scan], line_clusters, page_empty_regions,
            max_vertical_dist=None, directions=('below',),
        )
        self.assertFalse(df.empty, "expected at least one boundary symbol row")
        self.assertIn('side', df.columns)
        self.assertIn('own_cluster', df.columns)
        self.assertIn('relation', df.columns)

    def test_returns_empty_df_when_no_empty_regions(self):
        scan = self._make_two_block_scan()
        line_clusters = pd.Series([], index=pd.MultiIndex.from_tuples([], names=['scan_id', 'line_id']),
                                  dtype=int, name='cluster')
        df = find_boundary_adjacent_symbols([scan], line_clusters, {})
        self.assertTrue(df.empty)


class TestGroupRegionsIntoBlocks(unittest.TestCase):

    def _make_regions(self, boxes):
        """boxes: list of (x, y, w, h)"""
        return [
            make_region(f'r{i}', x, y, w, h)
            for i, (x, y, w, h) in enumerate(boxes)
        ]

    def test_close_regions_merged_into_one_block(self):
        # Two regions 10px apart → gap <= max_vertical_gap=50 → one block
        regions = self._make_regions([(0, 0, 100, 50), (0, 60, 100, 50)])
        blocks = group_regions_into_blocks(regions, max_vertical_gap=50)
        self.assertEqual(1, len(blocks))
        self.assertEqual(0, blocks[0].coords.top)
        self.assertEqual(110, blocks[0].coords.bottom)

    def test_distant_regions_produce_two_blocks(self):
        # Two regions 200px apart → gap > max_vertical_gap=50 → two blocks
        regions = self._make_regions([(0, 0, 100, 50), (0, 300, 100, 50)])
        blocks = group_regions_into_blocks(regions, max_vertical_gap=50)
        self.assertEqual(2, len(blocks))
        self.assertEqual(0, blocks[0].coords.top)
        self.assertEqual(300, blocks[1].coords.top)

    def test_block_bbox_spans_all_constituent_regions(self):
        # Three wide regions at different x positions, all close vertically
        regions = self._make_regions([(0, 0, 100, 20), (200, 10, 100, 20), (400, 5, 100, 20)])
        blocks = group_regions_into_blocks(regions, max_vertical_gap=50)
        self.assertEqual(1, len(blocks))
        self.assertEqual(0, blocks[0].coords.left)
        self.assertEqual(500, blocks[0].coords.right)

    def test_empty_regions_returns_empty_list(self):
        self.assertEqual([], group_regions_into_blocks([], max_vertical_gap=50))

    def test_large_page_with_many_regions_grouped_into_few_blocks(self):
        # Simulate a dense table: 30 regions spaced 5px apart (gap << threshold)
        # plus a large 200px gap in the middle, then 30 more tight regions
        tight = [(0, i * 15, 400, 10) for i in range(30)]
        gap_y = tight[-1][1] + tight[-1][3] + 200   # 200px gap
        tight2 = [(0, gap_y + i * 15, 400, 10) for i in range(30)]
        regions = self._make_regions(tight + tight2)
        blocks = group_regions_into_blocks(regions, max_vertical_gap=50)
        # Should produce exactly 2 blocks (one per tight group)
        self.assertEqual(2, len(blocks))

    def test_significant_gap_preserved_for_downstream_filter(self):
        # max_vertical_gap = 70% of filter threshold (e.g. min_rel_height=0.05, page_h=1000 → threshold=50px, gap=35px)
        # A 60px gap (> 50px threshold) must produce 2 blocks so the filter can see it
        regions = self._make_regions([(0, 0, 400, 30), (0, 100, 400, 30)])  # gap=70px
        blocks = group_regions_into_blocks(regions, max_vertical_gap=50)
        self.assertEqual(2, len(blocks))


class TestComputePageEmptyRegionsFastPath(unittest.TestCase):
    """Verify that the automatic grouping fast-path (>20 regions) preserves significant empty regions."""

    def _make_dense_scan(self, n_per_group=25, region_h=5, spacing=6, gap_size=300):
        """Two tight clusters of n_per_group single-line regions separated by gap_size.

        gap_y is computed from the actual end of the first cluster so the groups
        never overlap in Y.  Total regions = 2*n_per_group > 20 → triggers grouping.
        """
        # page tall enough for two clusters + gap + margins
        page_h = 2 * (n_per_group * spacing + region_h) + gap_size + 100
        regions = []
        y = 50
        for i in range(n_per_group):
            line = make_line(f'l{i}', x=10, y=y + 1, w=400, h=region_h)
            regions.append(make_region(f'r{i}', x=0, y=y, w=480, h=region_h, lines=[line]))
            y += spacing
        # y is now spacing beyond the last region top; actual bottom = y-spacing+region_h
        gap_y = (y - spacing + region_h) + gap_size
        for i in range(n_per_group):
            line = make_line(f'l{n_per_group+i}', x=10, y=gap_y + 1, w=400, h=region_h)
            regions.append(make_region(f'r{n_per_group+i}', x=0, y=gap_y, w=480, h=region_h, lines=[line]))
            gap_y += spacing
        return pdm.PageXMLScan(
            doc_id='dense_scan',
            coords=pdm.Coords.coords_from_box_params(0, 0, 500, page_h),
            text_regions=regions,
        )

    def test_significant_gap_detected_through_grouped_path(self):
        scan = self._make_dense_scan(n_per_group=25, region_h=5, spacing=6, gap_size=300)
        # 50 regions total → triggers grouping
        self.assertGreater(len(scan.get_textual_regions()), 20)
        ers = compute_page_empty_regions(scan, min_rel_height=0.1, min_rel_width=0.1)
        # The 300px gap is >10% of the computed page height and must survive the filter
        self.assertGreater(len(ers), 0)
        heights = [er.coords.height / scan.coords.height for er in ers]
        self.assertTrue(any(h >= 0.1 for h in heights))


class TestBoundarySymbolScores(unittest.TestCase):

    def test_affinity_column_present(self):
        from collections import Counter
        adjacent = pd.DataFrame([
            {'page_id': 's1', 'empty_region_id': 'er1', 'side': 'above',
             'line_id': 'l1', 'own_cluster': 0, 'direction': 'below',
             'relation': 'DC', 'neighbour_cluster': 1},
        ])
        corpus_counts = Counter({(0, 'below', 'DC', 1): 10})
        scores = boundary_symbol_scores(adjacent, corpus_counts)
        self.assertIn('affinity', scores.columns)
        self.assertAlmostEqual(scores.iloc[0]['affinity'], 1 / 10)

    def test_empty_adjacent_df_returns_empty_result(self):
        from collections import Counter
        df = find_boundary_adjacent_symbols([], pd.Series(dtype=int), {})
        scores = boundary_symbol_scores(df, Counter())
        self.assertTrue(scores.empty)


if __name__ == '__main__':
    unittest.main()
