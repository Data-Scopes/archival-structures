from unittest import TestCase

import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.relational_layout_clustering import (
    build_relational_pattern, cluster_relational_layouts,
)


def make_line(doc_id: str, x: int, y: int, w: int = 400, h: int = 20):
    # `text` must be non-None: `horizontally_group_lines` (pagexml-tools), which
    # `get_neighbouring_line_pairs`/`LineNeighbourHood` depend on, filters out lines with no text.
    return pdm.PageXMLTextLine(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(x, y, w, h),
                               text=doc_id)


def make_stacked_scan(doc_id: str, n: int = 6, gap: int = 10, w: int = 400, h: int = 20):
    lines = [make_line(f'{doc_id}-l{i}', x=50, y=50 + i * (h + gap), w=w, h=h) for i in range(n)]
    region = pdm.PageXMLTextRegion(doc_id=f'{doc_id}-r1', lines=lines,
                                   coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000))
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
                           text_regions=[region])


def uniform_line_clusters(scan: pdm.PageXMLScan, label=0) -> pd.Series:
    lines = scan.get_lines()
    index = pd.MultiIndex.from_tuples([(scan.id, line.id) for line in lines], names=['scan_id', 'line_id'])
    return pd.Series([label] * len(lines), index=index, name='cluster')


def alternating_line_clusters(scan: pdm.PageXMLScan) -> pd.Series:
    lines = scan.get_lines()
    index = pd.MultiIndex.from_tuples([(scan.id, line.id) for line in lines], names=['scan_id', 'line_id'])
    return pd.Series([i % 2 for i in range(len(lines))], index=index, name='cluster')


class TestRelationalLayoutClustering(TestCase):

    def test_build_relational_pattern_has_one_row_per_scan(self):
        scans = [make_stacked_scan(f'a{i}', n=6, gap=10) for i in range(3)]
        line_clusters = pd.concat([uniform_line_clusters(scan) for scan in scans])
        pattern = build_relational_pattern(scans, line_clusters, directions=('below',))
        self.assertEqual(3, pattern.num_docs)
        self.assertEqual((3, pattern.vocab_size), pattern.tfidf.shape)

    def test_cluster_relational_layouts_separates_distinct_neighbourhoods(self):
        uniform_scans = [make_stacked_scan(f'u{i}', n=8, gap=10) for i in range(6)]
        alternating_scans = [make_stacked_scan(f'alt{i}', n=8, gap=10) for i in range(6)]
        all_scans = uniform_scans + alternating_scans

        line_clusters = pd.concat(
            [uniform_line_clusters(scan) for scan in uniform_scans]
            + [alternating_line_clusters(scan) for scan in alternating_scans]
        )

        clusters, relational_pattern = cluster_relational_layouts(
            all_scans, line_clusters, directions=('below',), min_cluster_size=3)

        uniform_clusters = {clusters[scan.id] for scan in uniform_scans}
        alternating_clusters = {clusters[scan.id] for scan in alternating_scans}
        self.assertEqual(1, len(uniform_clusters))
        self.assertEqual(1, len(alternating_clusters))
        self.assertNotEqual(uniform_clusters, alternating_clusters)
        self.assertNotIn(-1, uniform_clusters)
        self.assertNotIn(-1, alternating_clusters)

    def test_cluster_relational_layouts_indexes_by_scan_id(self):
        scans = [make_stacked_scan(f'a{i}', n=6, gap=10) for i in range(5)]
        line_clusters = pd.concat([uniform_line_clusters(scan) for scan in scans])
        clusters, _ = cluster_relational_layouts(scans, line_clusters, directions=('below',), min_cluster_size=3)
        self.assertEqual({scan.id for scan in scans}, set(clusters.index))
