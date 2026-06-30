from unittest import TestCase

import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.page_layout_clustering import build_grid_pattern, cluster_page_layouts


def make_line(doc_id: str, x: int, y: int, w: int = 200, h: int = 20):
    return pdm.PageXMLTextLine(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(x, y, w, h))


def make_one_block_scan(doc_id: str):
    """A page with a single wide column of lines."""
    lines = [make_line(f'{doc_id}-l{i}', x=50, y=50 + i * 30, w=400) for i in range(10)]
    region = pdm.PageXMLTextRegion(doc_id='r1', coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500), lines=lines)
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500),
                           text_regions=[region])


def make_two_block_scan(doc_id: str):
    """A page with two narrower columns of lines, side by side."""
    lines = [make_line(f'{doc_id}-l{i}a', x=50, y=50 + i * 30, w=150) for i in range(10)]
    lines += [make_line(f'{doc_id}-l{i}b', x=300, y=50 + i * 30, w=150) for i in range(10)]
    region = pdm.PageXMLTextRegion(doc_id='r1', coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500), lines=lines)
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500),
                           text_regions=[region])


class TestPageLayoutClustering(TestCase):

    def test_build_grid_pattern_has_one_row_per_scan(self):
        scans = [make_one_block_scan(f'a{i}') for i in range(3)]
        grid_pattern = build_grid_pattern(scans, unit_size=50, pattern_size=3)
        self.assertEqual(3, grid_pattern.num_docs)
        self.assertEqual((3, grid_pattern.vocab_size), grid_pattern.tfidf.shape)

    def test_cluster_page_layouts_separates_distinct_layouts(self):
        one_block_scans = [make_one_block_scan(f'a{i}') for i in range(8)]
        two_block_scans = [make_two_block_scan(f'b{i}') for i in range(8)]

        clusters, grid_pattern = cluster_page_layouts(one_block_scans + two_block_scans,
                                                       unit_size=50, pattern_size=3, min_cluster_size=3)

        one_block_clusters = {clusters[scan.id] for scan in one_block_scans}
        two_block_clusters = {clusters[scan.id] for scan in two_block_scans}
        self.assertEqual(1, len(one_block_clusters))
        self.assertEqual(1, len(two_block_clusters))
        self.assertNotEqual(one_block_clusters, two_block_clusters)
        self.assertNotIn(-1, one_block_clusters)
        self.assertNotIn(-1, two_block_clusters)

    def test_cluster_page_layouts_indexes_by_scan_id(self):
        scans = [make_one_block_scan(f'a{i}') for i in range(5)]
        clusters, _ = cluster_page_layouts(scans, unit_size=50, pattern_size=3, min_cluster_size=3)
        self.assertEqual({scan.id for scan in scans}, set(clusters.index))
