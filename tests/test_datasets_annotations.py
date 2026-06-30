import tempfile
from pathlib import Path
from unittest import TestCase

import pagexml.model.physical_document_model as pdm

from archival_structures.datasets.annotations import (
    Element, ElementSpan, OpeningLabel, ScanAnnotation,
    elements_path, load_elements, load_line_labels, load_opening_labels,
    load_page_layout_labels, load_scan_annotation, new_scan_annotation,
    save_elements, save_scan_annotation, scan_annotation_path,
)


def make_scan_with_lines(doc_id: str, line_ids):
    lines = [pdm.PageXMLTextLine(doc_id=lid, coords=pdm.Coords.coords_from_box_params(0, 0, 10, 10))
             for lid in line_ids]
    region = pdm.PageXMLTextRegion(doc_id='r1', coords=pdm.Coords.coords_from_box_params(0, 0, 10, 10),
                                   lines=lines)
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, 10, 10),
                           text_regions=[region])


class TestAnnotations(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.annotations_dir = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_new_scan_annotation_has_one_entry_per_line(self):
        scan = make_scan_with_lines('scan1', ['l1', 'l2', 'l3'])
        annotation = new_scan_annotation(scan)
        self.assertEqual('scan1', annotation.scan_id)
        self.assertEqual({'l1': None, 'l2': None, 'l3': None}, annotation.lines)

    def test_scan_annotation_round_trip(self):
        annotation = ScanAnnotation(scan_id='scan1', opening=OpeningLabel(is_opening=True, separation_x=100.5),
                                    page_layout='two_column_body', lines={'l1': 'body', 'l2': None})
        path = scan_annotation_path('archive', 'inv', 'inv_1', 'scan1', annotations_dir=self.annotations_dir)
        save_scan_annotation(annotation, path)
        loaded = load_scan_annotation(path)
        self.assertEqual(annotation, loaded)

    def test_elements_round_trip(self):
        elements = [Element(element_type='closing', spans=[
            ElementSpan(scan_id='scanA', line_ids=['l1', 'l2']),
            ElementSpan(scan_id='scanB', line_ids=['l3']),
        ])]
        path = elements_path('archive', 'inv', 'inv_1', annotations_dir=self.annotations_dir)
        save_elements(elements, path)
        loaded = load_elements(path)
        self.assertEqual(elements, loaded)

    def test_dataframe_loaders_collect_across_files(self):
        ann1 = ScanAnnotation(scan_id='scan1', opening=OpeningLabel(is_opening=True, separation_x=50),
                              page_layout='layout_a', lines={'l1': 'body', 'l2': 'closing'})
        ann2 = ScanAnnotation(scan_id='scan2', opening=OpeningLabel(is_opening=False),
                              page_layout='layout_b', lines={'l3': None})
        save_scan_annotation(ann1, scan_annotation_path('archive', 'inv', 'inv_1', 'scan1',
                                                         annotations_dir=self.annotations_dir))
        save_scan_annotation(ann2, scan_annotation_path('archive', 'inv', 'inv_1', 'scan2',
                                                         annotations_dir=self.annotations_dir))

        opening_df = load_opening_labels(self.annotations_dir)
        self.assertEqual({'scan1', 'scan2'}, set(opening_df['scan_id']))
        self.assertTrue(opening_df.set_index('scan_id').loc['scan1', 'is_opening'])
        self.assertFalse(opening_df.set_index('scan_id').loc['scan2', 'is_opening'])

        layout_df = load_page_layout_labels(self.annotations_dir)
        self.assertEqual({'layout_a', 'layout_b'}, set(layout_df['page_layout']))

        line_df = load_line_labels(self.annotations_dir)
        # only labelled lines (not None) should appear
        self.assertEqual(2, len(line_df))
        self.assertNotIn('l3', set(line_df['line_id']))
