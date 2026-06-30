import unittest

import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis.relational_patterns import (
    RelationalPattern, corpus_adjacent_line_vertical_gaps, derive_max_vertical_dist,
)


def make_line(doc_id: str, x: int, y: int, w: int = 400, h: int = 20):
    # `text` must be non-None: `horizontally_group_lines` (pagexml-tools), which
    # `get_neighbouring_line_pairs`/`LineNeighbourHood` depend on, filters out lines with no text.
    return pdm.PageXMLTextLine(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(x, y, w, h),
                               text=doc_id)


def make_stacked_scan(doc_id: str, n: int = 6, gap: int = 10, w: int = 400, h: int = 20):
    """A page with `n` full-width lines stacked vertically, `gap` pixels apart."""
    lines = [make_line(f'{doc_id}-l{i}', x=50, y=50 + i * (h + gap), w=w, h=h) for i in range(n)]
    region = pdm.PageXMLTextRegion(doc_id=f'{doc_id}-r1', lines=lines,
                                   coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000))
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, 500, 1000),
                           text_regions=[region])


def uniform_line_clusters(scan: pdm.PageXMLScan, label=0) -> pd.Series:
    """Every line in `scan` gets the same cluster label."""
    index = pd.MultiIndex.from_tuples([(scan.id, line.id) for line in scan.get_lines()],
                                      names=['scan_id', 'line_id'])
    return pd.Series([label] * len(index), index=index, name='cluster')


def alternating_line_clusters(scan: pdm.PageXMLScan) -> pd.Series:
    """Lines alternate between cluster labels 0 and 1, in stacking order."""
    lines = scan.get_lines()
    index = pd.MultiIndex.from_tuples([(scan.id, line.id) for line in lines],
                                      names=['scan_id', 'line_id'])
    labels = [i % 2 for i in range(len(lines))]
    return pd.Series(labels, index=index, name='cluster')


class TestCorpusAdjacentLineVerticalGaps(unittest.TestCase):

    def test_gaps_match_known_spacing(self):
        scan = make_stacked_scan('a1', n=4, gap=15, h=20)
        gaps = corpus_adjacent_line_vertical_gaps([scan])
        self.assertEqual(3, len(gaps))
        self.assertTrue(all(gap == 15 for gap in gaps))

    def test_single_line_region_has_no_gaps(self):
        line = make_line('s-l0', x=50, y=50)
        region = pdm.PageXMLTextRegion(doc_id='r1', lines=[line])
        scan = pdm.PageXMLScan(doc_id='s', coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500),
                               text_regions=[region])
        self.assertEqual([], corpus_adjacent_line_vertical_gaps([scan]))


class TestDeriveMaxVerticalDist(unittest.TestCase):

    def test_percentile_of_known_gap_distribution(self):
        scan = make_stacked_scan('a1', n=11, gap=10, h=20)
        # 10 consecutive gaps, all exactly 10px apart -> any percentile is 10
        threshold = derive_max_vertical_dist([scan], percentile=95.0)
        self.assertEqual(10, threshold)

    def test_raises_when_no_gaps_available(self):
        line = make_line('s-l0', x=50, y=50)
        region = pdm.PageXMLTextRegion(doc_id='r1', lines=[line])
        scan = pdm.PageXMLScan(doc_id='s', coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500),
                               text_regions=[region])
        with self.assertRaises(ValueError):
            derive_max_vertical_dist([scan])


class TestRelationalPattern(unittest.TestCase):

    def test_tfidf_shape(self):
        scans = [make_stacked_scan(f'a{i}', n=6, gap=10) for i in range(3)]
        line_clusters = pd.concat([uniform_line_clusters(scan) for scan in scans])
        pattern = RelationalPattern(scans, line_clusters, directions=('below',))
        self.assertEqual(3, pattern.num_docs)
        self.assertEqual((3, pattern.vocab_size), pattern.tfidf.shape)

    def test_uniform_clusters_only_emit_same_cluster_symbols(self):
        scan = make_stacked_scan('a1', n=6, gap=10)
        line_clusters = uniform_line_clusters(scan, label=0)
        pattern = RelationalPattern([scan], line_clusters, directions=('below',))
        for own_cluster, direction, relation, neighbour_cluster in pattern.vocab:
            self.assertEqual(own_cluster, neighbour_cluster)

    def test_alternating_clusters_emit_cross_cluster_symbols(self):
        scan = make_stacked_scan('a1', n=6, gap=10)
        line_clusters = alternating_line_clusters(scan)
        pattern = RelationalPattern([scan], line_clusters, directions=('below',))
        self.assertTrue(any(own_cluster != neighbour_cluster
                            for own_cluster, direction, relation, neighbour_cluster in pattern.vocab))

    def test_uniform_and_alternating_layouts_have_distinguishable_fingerprints(self):
        uniform_scan = make_stacked_scan('u1', n=6, gap=10)
        alternating_scan = make_stacked_scan('alt1', n=6, gap=10)
        line_clusters = pd.concat([
            uniform_line_clusters(uniform_scan, label=0),
            alternating_line_clusters(alternating_scan),
        ])
        pattern = RelationalPattern([uniform_scan, alternating_scan], line_clusters, directions=('below',))
        uniform_vec = pattern.doc_pfidf('u1')
        alternating_vec = pattern.doc_pfidf('alt1')
        self.assertFalse((uniform_vec == alternating_vec).all())

    def test_missing_line_cluster_gets_noise_label(self):
        scan = make_stacked_scan('a1', n=3, gap=10)
        empty_clusters = pd.Series([], index=pd.MultiIndex.from_tuples([], names=['scan_id', 'line_id']),
                                   dtype=int, name='cluster')
        pattern = RelationalPattern([scan], empty_clusters, directions=('below',))
        for own_cluster, direction, relation, neighbour_cluster in pattern.vocab:
            self.assertEqual(-1, own_cluster)
            self.assertEqual(-1, neighbour_cluster)

    def test_max_diff_changes_relation_classification(self):
        # two lines with a 30px gap
        line1 = make_line('s-l0', x=50, y=50, h=20)
        line2 = make_line('s-l1', x=50, y=100, h=20)
        region = pdm.PageXMLTextRegion(doc_id='r1', lines=[line1, line2])
        scan = pdm.PageXMLScan(doc_id='s', coords=pdm.Coords.coords_from_box_params(0, 0, 500, 500),
                               text_regions=[region])
        line_clusters = uniform_line_clusters(scan, label=0)

        pattern_strict = RelationalPattern([scan], line_clusters, directions=('below',), max_diff=0)
        pattern_lenient = RelationalPattern([scan], line_clusters, directions=('below',), max_diff=30)

        strict_relations = {relation for _, _, relation, _ in pattern_strict.vocab}
        lenient_relations = {relation for _, _, relation, _ in pattern_lenient.vocab}
        self.assertEqual({'DC'}, strict_relations)
        self.assertEqual({'EC'}, lenient_relations)


if __name__ == '__main__':
    unittest.main()
