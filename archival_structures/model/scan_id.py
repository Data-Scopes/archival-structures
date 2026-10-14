"""Scan-id grammar ``<institution>_<archive>_<inventory>_<NNNN>[.ext]``.

Example: ``NL-HaNA_1.04.02_10119A_0002.jpg``. Archive-specific values are parameters with
named defaults; nothing here knows a particular archive.
"""

import dataclasses
import re
from dataclasses import dataclass
from typing import Optional, Tuple

from archival_structures.model.inventory_number import (
    InvalidInventoryNumberError,
    InventoryNumber,
)

NA_INSTITUTION = "NL-HaNA"
DEFAULT_DIGITS = 4

_EXT_RE = re.compile(r'^\.[A-Za-z][A-Za-z0-9]*$')
_NUM_RE = re.compile(r'^[0-9]+$')


class InvalidScanIdError(ValueError):
    """Raised when a string is not a valid scan id."""


@dataclass(frozen=True, order=True)
class ScanId:
    institution: str
    archive: str
    inventory: InventoryNumber
    number: int
    ext: str = ""

    @property
    def prefix(self) -> str:
        return f"{self.institution}_{self.archive}"

    @classmethod
    def from_prefix(cls, prefix: str, inventory, number: int, ext: str = "") -> "ScanId":
        institution, sep, archive = prefix.partition("_")
        if not sep or not institution or not archive:
            raise InvalidScanIdError(f"invalid scan-id prefix: {prefix!r}")
        if not isinstance(inventory, InventoryNumber):
            try:
                inventory = InventoryNumber(inventory)
            except InvalidInventoryNumberError as exc:
                raise InvalidScanIdError(f"invalid inventory in prefix {prefix!r}: {exc}") from exc
        return cls(institution, archive, inventory, number, ext)

    def format(self, *, digits: int = DEFAULT_DIGITS, ext: Optional[str] = None) -> str:
        e = self.ext if ext is None else ext
        return f"{self.prefix}_{self.inventory}_{self.number:0{digits}d}{e}"

    def __str__(self) -> str:
        return self.format()

    def with_number(self, number: int) -> "ScanId":
        return dataclasses.replace(self, number=number)

    @classmethod
    def parse(cls, scan_id: str, *, digits: Optional[int] = DEFAULT_DIGITS,
              prefix: Optional[str] = None) -> "ScanId":
        def bad(why: str) -> InvalidScanIdError:
            return InvalidScanIdError(f"invalid scan id {scan_id!r}: {why}")

        if not isinstance(scan_id, str) or not scan_id:
            raise bad("not a non-empty string")
        head, sep, last = scan_id.rpartition("_")
        if not sep:
            raise bad("no '_' separator")
        num, dot, rest = last.partition(".")
        ext = "." + rest if dot else ""
        if ext and not _EXT_RE.match(ext):
            raise bad("bad extension")
        if not _NUM_RE.match(num):
            raise bad("scan number is not ASCII digits")
        if digits is not None and len(num) != digits:
            raise bad(f"scan number does not have {digits} digits")
        pre, sep2, inv = head.rpartition("_")
        if not sep2 or not inv:
            raise bad("no inventory part")
        try:
            inventory = InventoryNumber(inv)
        except InvalidInventoryNumberError as exc:
            raise bad(f"bad inventory number: {exc}") from exc
        institution, sep3, archive = pre.partition("_")
        if not sep3 or not institution or not archive or "_" in archive:
            raise bad("bad institution/archive part")
        if prefix is not None and pre != prefix:
            raise bad(f"prefix is not {prefix!r}")
        return cls(institution, archive, inventory, int(num), ext)

    @classmethod
    def try_parse(cls, scan_id: str, **kwargs) -> "Optional[ScanId]":
        try:
            return cls.parse(scan_id, **kwargs)
        except InvalidScanIdError:
            return None


def scan_number(scan_id: str, *, digits: Optional[int] = None) -> int:
    """Scan number of ``scan_id`` (any digit count by default)."""
    return ScanId.parse(scan_id, digits=digits).number


def parse_doc_id(doc_id: str, *, marker: str = "-text_region-"
                 ) -> Optional[Tuple[str, Tuple[int, int, int, int]]]:
    """Split a pagexml-tools region doc id into ``(scan_id, (x, y, w, h))``, or None."""
    i = doc_id.find(marker)
    if i <= 0:
        return None
    parts = doc_id[i + len(marker):].split("-")
    if len(parts) != 4:
        return None
    try:
        x, y, w, h = (int(p) for p in parts)
    except ValueError:
        return None
    return doc_id[:i], (x, y, w, h)
