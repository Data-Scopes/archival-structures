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
