from unittest import TestCase

from archival_structures.model.inventory_number import (
    InventoryNumber,
    InvalidInventoryNumberError,
)


def inv(s):
    return InventoryNumber(s)


class TestInventoryNumberOrdering(TestCase):

    def test_simple_numeric_order(self):
        self.assertLess(inv('42'), inv('43'))
        self.assertLess(inv('9'), inv('10'))

    def test_extension_after_base(self):
        self.assertLess(inv('42'), inv('42A'))
        self.assertLess(inv('42A'), inv('43'))

    def test_dotted_equivalent_to_undotted(self):
        self.assertEqual(inv('42A'), inv('42.A'))

    def test_nested_extension_order(self):
        self.assertLess(inv('42.A.1'), inv('42.A.2'))
        self.assertLess(inv('42.A.2'), inv('42.B'))
        self.assertLess(inv('42.A'), inv('42.A.1'))

    def test_letter_order(self):
        self.assertLess(inv('42A'), inv('42B'))

    def test_letter_case_insensitive_order(self):
        self.assertEqual(inv('42a'), inv('42A'))

    def test_sorting_a_list(self):
        numbers = ['43', '42A', '42', '42.B', '42.A.2', '42.A.1']
        ordered = sorted(inv(n) for n in numbers)
        self.assertEqual(
            [str(n) for n in ordered],
            ['42', '42A', '42.A.1', '42.A.2', '42.B', '43'],
        )

    def test_equality_and_hash(self):
        self.assertEqual(inv('42.A.1'), inv('42A1'))
        self.assertEqual(hash(inv('42.A.1')), hash(inv('42A1')))

    def test_str_and_repr_preserve_original(self):
        n = inv('42.A.1')
        self.assertEqual(str(n), '42.A.1')
        self.assertEqual(repr(n), '42.A.1')

    def test_invalid_empty_string_raises(self):
        with self.assertRaises(InvalidInventoryNumberError):
            inv('')

    def test_invalid_empty_part_raises(self):
        with self.assertRaises(InvalidInventoryNumberError):
            inv('42..A')

    def test_invalid_characters_raise(self):
        with self.assertRaises(InvalidInventoryNumberError):
            inv('42-A')
