import json
import tempfile
from pathlib import Path
from unittest import TestCase

import pagexml.model.physical_document_model as pdm

from archival_structures.datasets.annotations import (
    Element, ElementSpan, OpeningLabel, RegionTag, ScanAnnotation,
    elements_path, import_bulk_image_labels, load_elements, load_line_tags,
    load_opening_labels, load_page_tags, load_region_tags, load_scan_annotation,
    load_scan_tags, migrate_legacy_region_annotations, new_scan_annotation,
    parse_thumb_path, save_elements, save_scan_annotation, scan_annotation_path,
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
        self.assertEqual({'l1': [], 'l2': [], 'l3': []}, annotation.lines)

    def test_scan_annotation_round_trip(self):
        annotation = ScanAnnotation(
            scan_id='scan1', opening=OpeningLabel(is_opening=True, separation_x=100.5),
            tags=['carrier:opening'],
            pages={'verso': ['generic:running_text'], 'recto': ['generic:running_text', 'doctype:deed']},
            lines={'l1': ['generic:closing'], 'l2': []},
            regions=[RegionTag(tags=['generic:table'], box={'x': 1.0, 'y': 2.0, 'w': 3.0, 'h': 4.0})],
        )
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
        ann1 = ScanAnnotation(
            scan_id='scan1', opening=OpeningLabel(is_opening=True, separation_x=50),
            tags=['generic:table'], pages={'verso': ['generic:running_text']},
            lines={'l1': ['generic:closing'], 'l2': ['generic:closing', 'doctype:deed:closing']},
            regions=[RegionTag(tags=['generic:marginalia'], element_id='r1')],
        )
        ann2 = ScanAnnotation(scan_id='scan2', opening=OpeningLabel(is_opening=False),
                              tags=['generic:cover'], lines={'l3': []})
        save_scan_annotation(ann1, scan_annotation_path('archive', 'inv', 'inv_1', 'scan1',
                                                         annotations_dir=self.annotations_dir))
        save_scan_annotation(ann2, scan_annotation_path('archive', 'inv', 'inv_1', 'scan2',
                                                         annotations_dir=self.annotations_dir))

        opening_df = load_opening_labels(self.annotations_dir)
        self.assertEqual({'scan1', 'scan2'}, set(opening_df['scan_id']))
        self.assertTrue(opening_df.set_index('scan_id').loc['scan1', 'is_opening'])
        self.assertFalse(opening_df.set_index('scan_id').loc['scan2', 'is_opening'])

        scan_tags_df = load_scan_tags(self.annotations_dir)
        self.assertEqual({'generic:table', 'generic:cover'}, set(scan_tags_df['tag']))

        page_tags_df = load_page_tags(self.annotations_dir)
        self.assertEqual(1, len(page_tags_df))
        self.assertEqual('verso', page_tags_df.iloc[0]['side'])

        line_tags_df = load_line_tags(self.annotations_dir)
        # 'l3' has no tags, so it shouldn't contribute any rows
        self.assertEqual(3, len(line_tags_df))
        self.assertNotIn('l3', set(line_tags_df['line_id']))

        region_tags_df = load_region_tags(self.annotations_dir)
        self.assertEqual(1, len(region_tags_df))
        self.assertEqual('generic:marginalia', region_tags_df.iloc[0]['tag'])
        self.assertEqual('r1', region_tags_df.iloc[0]['element_id'])


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
        self.assertEqual(1, counts['tags_added'])

        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0001.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertTrue(annotation.opening.is_opening)
        self.assertIsNone(annotation.opening.separation_x)
        self.assertEqual(['generic:table'], annotation.tags)

    def test_opening_tag_absence_sets_is_opening_false(self):
        self._write_labels({
            'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/NL-HaNA_2.10.50_1_0002.jpg': ['book_cover'],
        })
        import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0002.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertFalse(annotation.opening.is_opening)
        self.assertEqual(['generic:cover'], annotation.tags)

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

    def test_multiple_mapped_labels_all_get_added_as_tags(self):
        # unlike the old single page_layout string, multiple co-occurring tags are fine now
        self._write_labels({
            'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/NL-HaNA_2.10.50_1_0004.jpg': ['table', 'title_page'],
        })
        counts = import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)
        self.assertEqual(2, counts['tags_added'])

        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0004.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertEqual({'generic:table', 'generic:title_page'}, set(annotation.tags))

    def test_does_not_add_duplicate_tags(self):
        scan_id = 'NL-HaNA_2.10.50_1_0005.jpg'
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1', scan_id,
                                    annotations_dir=self.annotations_dir)
        save_scan_annotation(ScanAnnotation(scan_id=scan_id, tags=['generic:table']), path)

        self._write_labels({f'data/thumbs/NL-HaNA/NL-HaNA_2.10.50/1/{scan_id}': ['table']})
        counts = import_bulk_image_labels(self.labels_path, annotations_dir=self.annotations_dir)

        self.assertEqual(0, counts['tags_added'])
        annotation = load_scan_annotation(path)
        self.assertEqual(['generic:table'], annotation.tags)


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

    def test_migrates_regions_with_mapped_tags(self):
        write_pagexml(self.pagexml_dir / 'archive' / 'inv' / 'inv_1' / 'scan1.xml', 'scan1.jpg',
                      1000, 1000, [('l1', 0, 0, 500, 100)])
        path = self._write_legacy_file('scan1.png', [
            {'thumb_box': {'x': 0, 'y': 0, 'w': 10, 'h': 10, 'label': 'marginalium'},
             'orig_box': {'x': 0, 'y': 0, 'w': 500, 'h': 200, 'label': 'marginalium'}},
            {'thumb_box': {'x': 0, 'y': 0, 'w': 10, 'h': 10, 'label': 'closing'},
             'orig_box': {'x': 0, 'y': 200, 'w': 500, 'h': 100, 'label': 'closing'}},
        ])

        counts = migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                                    pagexml_dir=self.pagexml_dir)

        self.assertEqual(1, counts['files_seen'])
        self.assertEqual(1, counts['files_migrated'])
        self.assertEqual(2, counts['regions_migrated'])

        annotation = load_scan_annotation(path)
        self.assertEqual(2, len(annotation.regions))
        tags = sorted(r.tags[0] for r in annotation.regions)
        self.assertEqual(['generic:closing', 'generic:marginalia'], tags)
        # the original PageXML-derived line skeleton is still preserved
        self.assertEqual({'l1': []}, annotation.lines)

    def test_unmapped_label_preserved_under_legacy_doctype(self):
        write_pagexml(self.pagexml_dir / 'archive' / 'inv' / 'inv_1' / 'scan1.xml', 'scan1.jpg',
                      1000, 1000, [('l1', 0, 0, 500, 100)])
        path = self._write_legacy_file('scan1.png', [
            {'thumb_box': {'x': 0, 'y': 0, 'w': 10, 'h': 10, 'label': 'other'},
             'orig_box': {'x': 0, 'y': 0, 'w': 500, 'h': 100, 'label': 'other'}},
        ])

        migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                          pagexml_dir=self.pagexml_dir)

        annotation = load_scan_annotation(path)
        self.assertEqual(['doctype:legacy:other'], annotation.regions[0].tags)

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
        save_scan_annotation(ScanAnnotation(scan_id='scan1.jpg', lines={'l1': ['generic:running_text']}), path)

        counts = migrate_legacy_region_annotations(annotations_dir=self.annotations_dir,
                                                    pagexml_dir=self.pagexml_dir)

        self.assertEqual(0, counts['files_seen'])
        annotation = load_scan_annotation(path)
        self.assertEqual({'l1': ['generic:running_text']}, annotation.lines)
