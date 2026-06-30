import json
import tempfile
from pathlib import Path
from unittest import TestCase

import pagexml.model.physical_document_model as pdm

from archival_structures.datasets.annotations import (
    Element, ElementSpan, OpeningLabel, ScanAnnotation,
    elements_path, import_bulk_image_labels, load_elements, load_line_labels,
    load_opening_labels, load_page_layout_labels, load_scan_annotation,
    migrate_legacy_region_annotations, new_scan_annotation, parse_thumb_path,
    save_elements, save_scan_annotation, scan_annotation_path,
)


MINIMAL_PAGEXML = """<PcGts xmlns="http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15">
  <Metadata><Created>2020-01-01T00:00:00</Created><LastChange>2020-01-01T00:00:00</LastChange></Metadata>
  <Page imageFilename="{image_filename}" imageWidth="{width}" imageHeight="{height}">
    <TextRegion id="r1">
      <Coords points="0,0 0,{height} {width},{height} {width},0"/>
      {lines}
    </TextRegion>
  </Page>
</PcGts>"""

LINE_TEMPLATE = """<TextLine id="{line_id}">
  <Coords points="{x},{y} {x},{y2} {x2},{y2} {x2},{y}"/>
  <Baseline points="{x},{y2} {x2},{y2}"/>
</TextLine>"""


def write_pagexml(path: Path, image_filename: str, width: int, height: int, line_boxes):
    """Write a minimal valid PageXML file with one TextRegion containing rectangular lines
    at the given `(line_id, x, y, w, h)` boxes."""
    lines_xml = "\n".join(
        LINE_TEMPLATE.format(line_id=lid, x=x, y=y, x2=x + w, y2=y + h)
        for lid, x, y, w, h in line_boxes
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(MINIMAL_PAGEXML.format(image_filename=image_filename, width=width, height=height,
                                            lines=lines_xml))


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


class TestParseThumbPath(TestCase):

    def test_bare_inventory_number_gets_archive_prefix(self):
        result = parse_thumb_path('../data/thumbs/NL-HaNA/NL-HaNA_2.10.50/148/NL-HaNA_2.10.50_148_0188.jpg')
        self.assertEqual(('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_148',
                          'NL-HaNA_2.10.50_148_0188.jpg'), result)

    def test_full_inventory_id_is_left_as_is(self):
        result = parse_thumb_path('data/thumbs/NL-HaNA/NL-HaNA_2.10.50/NL-HaNA_2.10.50_1/scan1.jpg')
        self.assertEqual(('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1', 'scan1.jpg'), result)


class TestImportBulkImageLabels(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.annotations_dir = Path(self.tmpdir.name)
        self.labels_path = Path(self.tmpdir.name) / 'labels.json'

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_labels(self, labels: dict):
        with open(self.labels_path, 'w') as fh:
            json.dump(labels, fh)

    def test_opening_tag_presence_sets_is_opening_true(self):
        self._write_labels({
            'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/NL-HaNA_2.10.50_1_0001.jpg': ['book_opening', 'table'],
        })
        counts = import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)
        self.assertEqual(1, counts['opening_set'])
        self.assertEqual(1, counts['page_layout_set'])

        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0001.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertTrue(annotation.opening.is_opening)
        self.assertIsNone(annotation.opening.separation_x)
        self.assertEqual('table', annotation.page_layout)

    def test_opening_tag_absence_sets_is_opening_false(self):
        self._write_labels({
            'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/NL-HaNA_2.10.50_1_0002.jpg': ['book_cover'],
        })
        import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0002.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertFalse(annotation.opening.is_opening)
        self.assertEqual('book_cover', annotation.page_layout)

    def test_does_not_overwrite_existing_opening(self):
        scan_id = 'NL-HaNA_2.10.50_1_0003.jpg'
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1', scan_id,
                                    annotations_dir=self.annotations_dir)
        save_scan_annotation(ScanAnnotation(scan_id=scan_id, opening=OpeningLabel(is_opening=False)), path)

        self._write_labels({f'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/{scan_id}': ['book_opening']})
        counts = import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)

        self.assertEqual(0, counts['opening_set'])
        annotation = load_scan_annotation(path)
        self.assertFalse(annotation.opening.is_opening)  # untouched, not flipped to True

    def test_ambiguous_page_layout_tags_are_skipped_not_guessed(self):
        self._write_labels({
            'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/NL-HaNA_2.10.50_1_0004.jpg': ['table', 'title_page'],
        })
        counts = import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)
        self.assertEqual(1, counts['skipped_ambiguous'])
        self.assertEqual(0, counts['page_layout_set'])

        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0004.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertIsNone(annotation.page_layout)


class TestMigrateLegacyRegionAnnotations(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.annotations_dir = Path(self.tmpdir.name) / 'annotations'
        self.pagexml_dir = Path(self.tmpdir.name) / 'pagexml'

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_legacy_file(self, scan_filename: str, regions: list):
        path = scan_annotation_path('archive', 'inv', 'inv_1', scan_filename,
                                    annotations_dir=self.annotations_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as fh:
            json.dump(regions, fh)
        return path

    def test_migrates_overlapping_lines_and_skips_low_overlap(self):
        write_pagexml(self.pagexml_dir / 'archive' / 'inv' / 'inv_1' / 'scan1.xml', 'scan1.jpg',
                      1000, 1000, [
                          ('l1', 0, 0, 500, 100),    # fully inside the marginalium region
                          ('l2', 0, 100, 500, 100),  # fully inside the marginalium region
                          ('l3', 900, 900, 100, 100),  # outside the region entirely
                      ])
        path = self._write_legacy_file('scan1.png', [
            {'thumb_box': {'x': 0, 'y': 0, 'w': 10, 'h': 10, 'label': 'marginalium'},
             'orig_box': {'x': 0, 'y': 0, 'w': 500, 'h': 200, 'label': 'marginalium'}},
        ])

        counts = migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                                    pagexml_dir=self.pagexml_dir)

        self.assertEqual(1, counts['files_seen'])
        self.assertEqual(1, counts['files_migrated'])
        self.assertEqual(2, counts['lines_labelled'])

        annotation = load_scan_annotation(path)
        self.assertEqual('marginalium', annotation.lines['l1'])
        self.assertEqual('marginalium', annotation.lines['l2'])
        self.assertIsNone(annotation.lines['l3'])

    def test_preserves_original_as_legacy_backup(self):
        write_pagexml(self.pagexml_dir / 'archive' / 'inv' / 'inv_1' / 'scan1.xml', 'scan1.jpg',
                      1000, 1000, [('l1', 0, 0, 500, 100)])
        regions = [{'thumb_box': {'x': 0, 'y': 0, 'w': 10, 'h': 10, 'label': 'closing'},
                   'orig_box': {'x': 0, 'y': 0, 'w': 500, 'h': 100, 'label': 'closing'}}]
        path = self._write_legacy_file('scan1.png', regions)

        migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                          pagexml_dir=self.pagexml_dir)

        backup_path = path.with_name('legacy-annotations-scan1.png.json')
        self.assertTrue(backup_path.exists())
        with open(backup_path) as fh:
            self.assertEqual(regions, json.load(fh))
        # the migrated file should no longer be the old list-shaped format
        with open(path) as fh:
            self.assertIsInstance(json.load(fh), dict)

    def test_skips_scan_with_no_matching_pagexml(self):
        self._write_legacy_file('scan1.png', [
            {'thumb_box': {'x': 0, 'y': 0, 'w': 10, 'h': 10, 'label': 'closing'},
             'orig_box': {'x': 0, 'y': 0, 'w': 500, 'h': 100, 'label': 'closing'}},
        ])
        counts = migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                                    pagexml_dir=self.pagexml_dir)
        self.assertEqual(1, counts['skipped_no_pagexml'])
        self.assertEqual(0, counts['files_migrated'])

    def test_already_migrated_files_are_left_alone(self):
        path = scan_annotation_path('archive', 'inv', 'inv_1', 'scan1.jpg',
                                    annotations_dir=self.annotations_dir)
        save_scan_annotation(ScanAnnotation(scan_id='scan1.jpg', lines={'l1': 'body'}), path)

        counts = migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                                    pagexml_dir=self.pagexml_dir)

        self.assertEqual(0, counts['files_seen'])
        annotation = load_scan_annotation(path)
        self.assertEqual({'l1': 'body'}, annotation.lines)
