from unittest import TestCase

from archival_structures.datasets.vocabulary import (
    CARRIER_TYPES, DOCTYPE, GENERIC_TYPES, POSITION_TYPES, Tag, is_known_tag, make_tag,
)


class TestTagParsing(TestCase):

    def test_parses_namespace_and_type(self):
        tag = Tag.parse('generic:table')
        self.assertEqual(Tag(namespace='generic', type='table'), tag)

    def test_parses_subtype(self):
        tag = Tag.parse('generic:form:label')
        self.assertEqual(Tag(namespace='generic', type='form', subtype='label'), tag)

    def test_parses_number(self):
        tag = Tag.parse('generic:running_text#2')
        self.assertEqual(Tag(namespace='generic', type='running_text', number=2), tag)

    def test_parses_subtype_and_number_together(self):
        tag = Tag.parse('generic:marginalia:note#1')
        self.assertEqual(Tag(namespace='generic', type='marginalia', subtype='note', number=1), tag)

    def test_doctype_tag_with_subtype(self):
        tag = Tag.parse('doctype:deed:loan_table')
        self.assertEqual(Tag(namespace='doctype', type='deed', subtype='loan_table'), tag)

    def test_rejects_malformed_tag(self):
        for bad in ['no-colon-here', 'generic:', ':table', 'generic:Table', 'generic table']:
            with self.subTest(bad):
                with self.assertRaises(ValueError):
                    Tag.parse(bad)

    def test_str_round_trips(self):
        for s in ['generic:table', 'generic:form:label', 'generic:running_text#2',
                 'generic:marginalia:note#1', 'doctype:deed:loan_table']:
            with self.subTest(s):
                self.assertEqual(s, str(Tag.parse(s)))


class TestMakeTag(TestCase):

    def test_builds_expected_string(self):
        self.assertEqual('generic:table', make_tag('generic', 'table'))
        self.assertEqual('generic:form:label', make_tag('generic', 'form', 'label'))
        self.assertEqual('generic:running_text#1', make_tag('generic', 'running_text', number=1))
        self.assertEqual('generic:marginalia:note#1',
                         make_tag('generic', 'marginalia', 'note', 1))


class TestIsKnownTag(TestCase):

    def test_known_generic_type_is_known(self):
        self.assertTrue(is_known_tag('generic:table'))

    def test_unknown_generic_type_is_not_known(self):
        self.assertFalse(is_known_tag('generic:nonsense'))

    def test_unknown_namespace_is_not_known(self):
        self.assertFalse(is_known_tag('bogus:table'))

    def test_malformed_tag_is_not_known(self):
        self.assertFalse(is_known_tag('not-a-tag'))

    def test_doctype_tag_is_always_known_if_well_formed(self):
        # doctype: is deliberately uncontrolled -- any well-formed tag counts
        self.assertTrue(is_known_tag('doctype:anything_at_all'))
        self.assertTrue(is_known_tag('doctype:deed:loan_table'))


class TestVocabularyTables(TestCase):

    def test_every_generic_subtype_round_trips_as_known(self):
        for type_, subtypes in GENERIC_TYPES.items():
            with self.subTest(type_):
                self.assertTrue(is_known_tag(make_tag('generic', type_)))
                for subtype in (subtypes or []):
                    with self.subTest(f"{type_}:{subtype}"):
                        self.assertTrue(is_known_tag(make_tag('generic', type_, subtype)))

    def test_every_carrier_type_is_known(self):
        for type_ in CARRIER_TYPES:
            self.assertTrue(is_known_tag(make_tag('carrier', type_)))

    def test_every_position_type_is_known(self):
        for type_ in POSITION_TYPES:
            self.assertTrue(is_known_tag(make_tag('position', type_)))

    def test_doctype_constant_matches_namespace_string(self):
        self.assertEqual('doctype', DOCTYPE)


_OLD_GENERIC = {
    'running_text': set(), 'table': set(), 'list': set(), 'form': {'label', 'value'},
    'marginalia': {'note', 'commentary', 'correction', 'variants'}, 'closing': set(), 'heading': set(),
    'title_page': set(), 'cover': set(), 'running_title': set(), 'seal': set(), 'stamp': {'postal', 'curatorial'},
    'damage': {'corrosion', 'hole', 'mold', 'peeled', 'soaked'},
    'graphic': {'illustration', 'ornamentation', 'figure'}, 'numbering': {'page', 'other'},
    'quire_marks': {'signature', 'catchwords'}, 'digitization_artefact': {'ruler', 'test_card'},
    'drop_capital': {'historiated', 'floriate', 'flourish', 'voided', 'parted'}, 'music': set(), 'custom': None,
}


class TestVocabularyV2(TestCase):

    def test_placement_types_known(self):
        from archival_structures.datasets.vocabulary import PLACEMENT, PLACEMENT_TYPES, NAMESPACES
        self.assertIn(PLACEMENT, NAMESPACES)
        self.assertEqual({'main', 'margin', 'top', 'bottom', 'interlinear'}, set(PLACEMENT_TYPES))
        for t in PLACEMENT_TYPES:
            self.assertTrue(is_known_tag(f'placement:{t}'))
        self.assertTrue(is_known_tag('placement:interlinear'))

    def test_placement_round_trips(self):
        tag = Tag.parse('placement:margin')
        self.assertEqual(('placement', 'margin'), (tag.namespace, tag.type))
        self.assertEqual('placement:margin', str(tag))

    def test_new_types_and_subtypes_known(self):
        for tag in ('generic:heading:summary', 'generic:heading:title', 'generic:heading:date',
                    'generic:numbering:folio', 'generic:numbering:item', 'generic:closing:signature',
                    'generic:closing:formula', 'generic:closing:dating', 'generic:running_title:date',
                    'generic:noise', 'generic:salutation', 'generic:endorsement', 'generic:reference',
                    'generic:note', 'generic:addition'):
            with self.subTest(tag):
                self.assertTrue(is_known_tag(tag))
                parsed = Tag.parse(tag)
                if parsed.subtype:
                    self.assertIn(parsed.subtype, GENERIC_TYPES[parsed.type])

    def test_every_old_tag_still_known(self):
        for type_, subtypes in _OLD_GENERIC.items():
            self.assertIn(type_, GENERIC_TYPES)
            self.assertTrue(is_known_tag(f'generic:{type_}'))
            for sub in (subtypes or ()):
                self.assertIn(sub, GENERIC_TYPES[type_])
                self.assertTrue(is_known_tag(f'generic:{type_}:{sub}'))
        for t in ('page', 'opening', 'foldout', 'cut'):
            self.assertTrue(is_known_tag(f'carrier:{t}'))
        for t in ('start', 'continuation', 'end'):
            self.assertTrue(is_known_tag(f'position:{t}'))

    def test_legacy_placement(self):
        from archival_structures.datasets.vocabulary import legacy_placement
        self.assertEqual('margin', legacy_placement('generic:marginalia:note'))
        self.assertEqual('margin', legacy_placement('generic:marginalia'))
        self.assertIsNone(legacy_placement('generic:heading'))
        self.assertIsNone(legacy_placement('bad'))
        self.assertIsNone(legacy_placement('doctype:marginalia'))

    def test_unknown_placement_type_not_known(self):
        self.assertFalse(is_known_tag('placement:nowhere'))
