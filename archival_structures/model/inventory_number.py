"""Parsing and sorting of archival inventory numbers.

Inventory numbers consist of a primary running number followed by zero or
more extension parts (e.g. ``42``, ``42A``, ``42.A``, ``42.A.1``). Extension
parts alternate freely between letters and digits, and the separating period
is optional whenever the part type changes (digit -> letter or letter ->
digit), but required between two parts of the same type (e.g. ``42.1.2``).
"""

import re
from functools import total_ordering
from typing import List, Tuple, Union

PartKey = Tuple[int, Union[int, str]]

_TOKEN_RE = re.compile(r'\d+|[A-Za-z]+')


class InvalidInventoryNumberError(ValueError):
    """Raised when a string cannot be parsed as an inventory number."""


def _tokenize(inventory_number: str) -> List[str]:
    parts = []
    for chunk in inventory_number.split('.'):
        if chunk == '':
            raise InvalidInventoryNumberError(
                f"empty part in inventory number {inventory_number!r}"
            )
        tokens = _TOKEN_RE.findall(chunk)
        if not tokens or sum(len(token) for token in tokens) != len(chunk):
            raise InvalidInventoryNumberError(
                f"invalid part {chunk!r} in inventory number {inventory_number!r}"
            )
        parts.extend(tokens)
    return parts


def _part_key(part: str) -> PartKey:
    if part.isdigit():
        return (0, int(part))
    return (1, part.lower())


@total_ordering
class InventoryNumber:
    """An archival inventory number that sorts in natural archival order.

    Examples:
        >>> sorted([InventoryNumber('43'), InventoryNumber('42A'),
        ...         InventoryNumber('42')])
        [42, 42A, 43]
        >>> InventoryNumber('42.A.1') < InventoryNumber('42.B')
        True
    """

    __slots__ = ('original', '_key')

    def __init__(self, inventory_number: str):
        if not isinstance(inventory_number, str) or not inventory_number.strip():
            raise InvalidInventoryNumberError(
                f"inventory number must be a non-empty string, got {inventory_number!r}"
            )
        self.original = inventory_number
        parts = _tokenize(inventory_number.strip())
        self._key: Tuple[PartKey, ...] = tuple(_part_key(part) for part in parts)

    @property
    def key(self) -> Tuple[PartKey, ...]:
        """The tuple this inventory number sorts/compares by: one `(0, int)` or `(1, str)`
        pair per part (digit parts sort numerically before letter parts, case-insensitively)."""
        return self._key

    def __str__(self) -> str:
        return self.original

    def __repr__(self) -> str:
        return self.original

    def __hash__(self) -> int:
        return hash(self._key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InventoryNumber):
            return NotImplemented
        return self._key == other._key

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, InventoryNumber):
            return NotImplemented
        return self._key < other._key
