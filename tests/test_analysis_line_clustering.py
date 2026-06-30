from unittest import TestCase

import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.line_clustering import (
    cluster_lines, cluster_lines_by_peaks, extract_corpus_line_features, extract_line_features,
)


def make_line(doc_id: str, x: int, w: int, y: int = 0, h: int = 30):
    return pdm.PageXMLTextLine(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(x, y, w, h))


def make_scan(doc_id: str, width: int, height: int, lines):
    region = pdm.PageXMLTextRegion(doc_id='r1', coords=pdm.Coords.coords_from_box_params(0, 0, width, height),
                                   lines=lines)
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, width, height),
                           text_regions=[region])


class TestLineClustering(TestCase):

    def test_extract_line_features_normalises_by_scan_size(self):
        lines = [make_line('l1', x=100, w=200, y=0, h=20)]
        scan = make_scan('scan1', width=1000, height=2000, lines=lines)
        df = extract_line_features(scan)
        self.assertEqual(1, len(df))
        row = df.iloc[0]
        self.assertEqual('scan1', row['scan_id'])
        self.assertEqual('l1', row['line_id'])
        self.assertAlmostEqual(0.1, row['left_norm'])
        self.assertAlmostEqual(0.2, row['width_norm'])
        self.assertAlmostEqual(0.01, row['height_norm'])

    def test_extract_corpus_line_features_concatenates_scans(self):
        scan1 = make_scan('scan1', 1000, 1000, [make_line('l1', 100, 200)])
        scan2 = make_scan('scan2', 1000, 1000, [make_line('l2', 300, 200), make_line('l3', 310, 210)])
        df = extract_corpus_line_features([scan1, scan2])
        self.assertEqual(3, len(df))
        self.assertEqual({'scan1', 'scan2'}, set(df['scan_id']))

    def test_cluster_lines_groups_by_indentation(self):
        # two well-separated indentation bands, repeated enough times to clear min_cluster_size
        lines = []
        for i in range(15):
            lines.append(make_line(f'left-{i}', x=100 + i, w=400, y=i * 25))
            lines.append(make_line(f'right-{i}', x=700 + i, w=150, y=i * 25))
        scan = make_scan('scan1', width=1000, height=2000, lines=lines)
        df = extract_line_features(scan)

        clusters = cluster_lines(df, min_cluster_size=10)
        df['cluster'] = clusters

        left_clusters = set(df[df['line_id'].str.startswith('left')]['cluster'])
        right_clusters = set(df[df['line_id'].str.startswith('right')]['cluster'])
        # each band should be internally consistent and distinct from the other band
        self.assertEqual(1, len(left_clusters))
        self.assertEqual(1, len(right_clusters))
        self.assertNotEqual(left_clusters, right_clusters)
        self.assertNotIn(-1, left_clusters)
        self.assertNotIn(-1, right_clusters)

    def test_cluster_lines_by_peaks_groups_by_indentation(self):
        lines = []
        for i in range(15):
            lines.append(make_line(f'left-{i}', x=100, w=400, y=i * 25))
            lines.append(make_line(f'right-{i}', x=700, w=150, y=i * 25))
        scan = make_scan('scan1', width=1000, height=2000, lines=lines)
        df = extract_line_features(scan)

        clusters = cluster_lines_by_peaks(df, cluster_col='left', group_cols='scan_id',
                                          width_bin_size=50, min_prominence=2)
        df['cluster'] = clusters

        left_clusters = set(df[df['line_id'].str.startswith('left')]['cluster'])
        right_clusters = set(df[df['line_id'].str.startswith('right')]['cluster'])
        self.assertEqual(1, len(left_clusters))
        self.assertEqual(1, len(right_clusters))
        self.assertNotEqual(left_clusters, right_clusters)
