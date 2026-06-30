import tempfile
from pathlib import Path
from unittest import TestCase

from PIL import Image

from archival_structures.datasets.annotations import (
    OpeningLabel, ScanAnnotation, load_scan_annotation, save_scan_annotation, scan_annotation_path,
)
from archival_structures.datasets.bulk_tagging import (
    annotate_image_grid_with_tags, build_tag_from_inputs, subtype_options_for_type,
    type_options_for_namespace, used_doctype_types,
)


class TestBuildTagFromInputs(TestCase):

    def test_namespace_and_type_only(self):
        self.assertEqual('generic:table', build_tag_from_inputs('generic', 'table'))

    def test_with_subtype(self):
        self.assertEqual('generic:form:label', build_tag_from_inputs('generic', 'form', 'label'))

    def test_with_number(self):
        self.assertEqual('generic:running_text#2',
                         build_tag_from_inputs('generic', 'running_text', '', '2'))

    def test_with_subtype_and_number(self):
        self.assertEqual('generic:marginalia:note#1',
                         build_tag_from_inputs('generic', 'marginalia', 'note', '1'))

    def test_missing_type_returns_none(self):
        self.assertIsNone(build_tag_from_inputs('generic', ''))
        self.assertIsNone(build_tag_from_inputs('generic', '   '))

    def test_missing_namespace_returns_none(self):
        self.assertIsNone(build_tag_from_inputs('', 'table'))

    def test_non_digit_number_returns_none(self):
        self.assertIsNone(build_tag_from_inputs('generic', 'table', '', 'abc'))
        self.assertIsNone(build_tag_from_inputs('generic', 'table', '', '-1'))

    def test_uppercase_type_returns_none(self):
        self.assertIsNone(build_tag_from_inputs('generic', 'Table'))

    def test_doctype_with_arbitrary_type_is_valid(self):
        # doctype: is deliberately uncontrolled, any well-formed type is accepted
        self.assertEqual('doctype:deed', build_tag_from_inputs('doctype', 'deed'))


class TestTypeOptionsForNamespace(TestCase):

    def test_generic_returns_known_types(self):
        options = type_options_for_namespace('generic')
        self.assertIn('table', options)
        self.assertIn('running_text', options)

    def test_doctype_returns_given_suggestions(self):
        self.assertEqual(['deed', 'minutes'], type_options_for_namespace('doctype', ['minutes', 'deed']))

    def test_doctype_with_no_suggestions_returns_empty(self):
        self.assertEqual([], type_options_for_namespace('doctype'))


class TestSubtypeOptionsForType(TestCase):

    def test_known_subtypes(self):
        self.assertEqual(['label', 'value'], subtype_options_for_type('generic', 'form'))

    def test_type_with_no_subtypes_returns_empty(self):
        self.assertEqual([], subtype_options_for_type('generic', 'table'))

    def test_doctype_namespace_returns_empty(self):
        self.assertEqual([], subtype_options_for_type('doctype', 'deed'))

    def test_empty_type_returns_empty(self):
        self.assertEqual([], subtype_options_for_type('generic', ''))


class TestUsedDoctypeTypes(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.annotations_dir = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_collects_distinct_doctype_types_across_files(self):
        ann1 = ScanAnnotation(scan_id='scan1', tags=['doctype:deed', 'generic:table'])
        ann2 = ScanAnnotation(scan_id='scan2', tags=['doctype:minutes', 'doctype:deed'])
        save_scan_annotation(ann1, scan_annotation_path('archive', 'inv', 'inv_1', 'scan1',
                                                         annotations_dir=self.annotations_dir))
        save_scan_annotation(ann2, scan_annotation_path('archive', 'inv', 'inv_1', 'scan2',
                                                         annotations_dir=self.annotations_dir))
        self.assertEqual(['deed', 'minutes'], used_doctype_types(self.annotations_dir))


def make_thumbnails(base_dir: Path, n: int = 3):
    """Write `n` tiny real JPEGs under a NL-HaNA-style thumbnail path, return their paths."""
    thumb_dir = base_dir / 'thumbs' / 'NL-HaNA' / 'NL-HaNA_2.10.50' / '1'
    thumb_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(n):
        path = thumb_dir / f'NL-HaNA_2.10.50_1_000{i}.jpg'
        Image.new('RGB', (40, 40), (210, 210, 210)).save(path)
        paths.append(str(path))
    return paths


class TestAnnotateImageGridWithTags(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmpdir.name)
        self.annotations_dir = self.base_dir / 'annotations'
        self.image_paths = make_thumbnails(self.base_dir, n=3)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_raises_on_empty_image_ids(self):
        with self.assertRaises(ValueError):
            annotate_image_grid_with_tags([], annotations_dir=self.annotations_dir)

    def test_builds_without_error(self):
        box = annotate_image_grid_with_tags(self.image_paths, rows=1, cols=2,
                                            annotations_dir=self.annotations_dir)
        self.assertIsNotNone(box)

    def _find_widgets(self, box):
        """Pull out the controls this test needs to drive, by their constructor order in
        annotate_image_grid_with_tags's returned VBox: [nav, tag_controls, preview, actions,
        selection, status, out]."""
        nav, tag_controls, preview, actions, selection, status, out = box.children
        namespace_dd, type_combo, subtype_combo, number_text = tag_controls.children
        add_btn, remove_btn = actions.children
        return namespace_dd, type_combo, subtype_combo, number_text, add_btn, remove_btn, status

    def test_add_to_selected_writes_tag_to_scan_annotation(self):
        box = annotate_image_grid_with_tags(self.image_paths, rows=1, cols=3,
                                            annotations_dir=self.annotations_dir)
        namespace_dd, type_combo, subtype_combo, number_text, add_btn, remove_btn, status = \
            self._find_widgets(box)

        namespace_dd.value = 'generic'
        type_combo.value = 'table'
        add_btn.click()

        self.assertIn('Added', status.value)
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0000.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertEqual(['generic:table'], annotation.tags)

    def test_remove_from_selected_removes_tag(self):
        box = annotate_image_grid_with_tags(self.image_paths, rows=1, cols=3,
                                            annotations_dir=self.annotations_dir)
        namespace_dd, type_combo, subtype_combo, number_text, add_btn, remove_btn, status = \
            self._find_widgets(box)

        namespace_dd.value = 'generic'
        type_combo.value = 'table'
        add_btn.click()
        remove_btn.click()

        self.assertIn('Removed', status.value)
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0000.jpg', annotations_dir=self.annotations_dir)
        annotation = load_scan_annotation(path)
        self.assertEqual([], annotation.tags)

    def test_add_does_not_overwrite_existing_unrelated_fields(self):
        # an image with an existing ScanAnnotation (e.g. opening already labelled) should
        # keep that field after a tag is added via the grid
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0000.jpg', annotations_dir=self.annotations_dir)
        save_scan_annotation(ScanAnnotation(scan_id='NL-HaNA_2.10.50_1_0000.jpg',
                                            opening=OpeningLabel(is_opening=True, separation_x=42.0)),
                             path)

        box = annotate_image_grid_with_tags(self.image_paths, rows=1, cols=3,
                                            annotations_dir=self.annotations_dir)
        namespace_dd, type_combo, subtype_combo, number_text, add_btn, remove_btn, status = \
            self._find_widgets(box)
        namespace_dd.value = 'generic'
        type_combo.value = 'table'
        add_btn.click()

        annotation = load_scan_annotation(path)
        self.assertEqual(['generic:table'], annotation.tags)
        self.assertTrue(annotation.opening.is_opening)
        self.assertEqual(42.0, annotation.opening.separation_x)

    def test_add_with_invalid_tag_shows_error_and_writes_nothing(self):
        box = annotate_image_grid_with_tags(self.image_paths, rows=1, cols=3,
                                            annotations_dir=self.annotations_dir)
        namespace_dd, type_combo, subtype_combo, number_text, add_btn, remove_btn, status = \
            self._find_widgets(box)
        # no type entered -- invalid tag
        add_btn.click()

        self.assertIn('valid namespace', status.value)
        path = scan_annotation_path('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1',
                                    'NL-HaNA_2.10.50_1_0000.jpg', annotations_dir=self.annotations_dir)
        self.assertFalse(path.exists())

    def test_doctype_suggestions_grow_after_use(self):
        box = annotate_image_grid_with_tags(self.image_paths, rows=1, cols=3,
                                            annotations_dir=self.annotations_dir)
        namespace_dd, type_combo, subtype_combo, number_text, add_btn, remove_btn, status = \
            self._find_widgets(box)
        namespace_dd.value = 'doctype'
        self.assertEqual((), type_combo.options)

        type_combo.value = 'deed'
        add_btn.click()

        # re-selecting doctype should now offer 'deed' as a suggestion
        namespace_dd.value = 'generic'
        namespace_dd.value = 'doctype'
        self.assertIn('deed', type_combo.options)
