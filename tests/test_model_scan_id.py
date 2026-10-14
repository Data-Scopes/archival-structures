import pytest

from archival_structures.model.inventory_number import InventoryNumber
from archival_structures.model.scan_id import (
    InvalidScanIdError, ScanId, parse_doc_id, scan_number,
)

SG = "NL-HaNA_1.01.02_3809_0415"
VOC = "NL-HaNA_1.04.02_9015A_0415"


@pytest.mark.parametrize("s", [SG, VOC])
def test_roundtrip_sg_and_voc(s):
    p = ScanId.parse(s)
    assert str(p) == s
    assert p.number == 415


@pytest.mark.parametrize("ext", [".jpg", ".JPG"])
def test_roundtrip_keeps_extension(ext):
    s = VOC + ext
    p = ScanId.parse(s)
    assert p.ext == ext and str(p) == s


def test_format_can_drop_extension():
    p = ScanId.parse(VOC + ".jpg")
    assert p.format(ext="") == VOC
    assert p.format() == VOC + ".jpg"


def test_from_prefix_formats_staged_name():
    p = ScanId.from_prefix("NL-HaNA_1.04.02", "10000", 1)
    assert str(p) == "NL-HaNA_1.04.02_10000_0001"
    assert p.prefix == "NL-HaNA_1.04.02"


def test_prefix_mismatch_raises_and_try_parse_returns_none():
    with pytest.raises(InvalidScanIdError):
        ScanId.parse(SG, prefix="NL-HaNA_1.04.02")
    assert ScanId.try_parse(SG, prefix="NL-HaNA_1.04.02") is None
    assert ScanId.try_parse(SG, prefix="NL-HaNA_1.01.02") is not None


@pytest.mark.parametrize("inv", ["4848A", "5745A", "9626A", "10119C", "1171B", "1234II"])
def test_suffixed_inventories(inv):
    s = f"NL-HaNA_1.04.02_{inv}_0001"
    p = ScanId.parse(s)
    assert p.inventory == InventoryNumber(inv)
    assert str(p) == s


def test_dotted_inventory_equals_undotted():
    a = ScanId.parse("NL-HaNA_1.04.02_42.A_0001")
    b = ScanId.parse("NL-HaNA_1.04.02_42A_0001")
    assert a.inventory == b.inventory


def test_slash_inventory_rejected():
    with pytest.raises(InvalidScanIdError):
        ScanId.parse("NL-HaNA_1.04.02_5745A/B_0001")


def test_digit_count():
    s = "NL-HaNA_1.04.02_10000_10000"
    with pytest.raises(InvalidScanIdError):
        ScanId.parse(s)
    assert ScanId.parse(s, digits=None).number == 10000
    assert str(ScanId.parse(s, digits=None)) == s
    for bad in ["NL-HaNA_1.04.02_10_00a1", "NL-HaNA_1.04.02_10_０００１"]:
        with pytest.raises(InvalidScanIdError):
            ScanId.parse(bad, digits=None)


@pytest.mark.parametrize("s", ["", "foo", "NL-HaNA_0001", "NL-HaNA_1.04.02__0001",
                               "_1.04.02_1_0001", "NL-HaNA_1.04.02_2555_0001.", None, 5])
def test_malformed(s):
    with pytest.raises(InvalidScanIdError):
        ScanId.parse(s)
    assert ScanId.try_parse(s) is None


def test_ordering_natural():
    ids = [ScanId.parse(f"NL-HaNA_1.04.02_{i}_0001") for i in ["10", "9626A", "9626", "9"]]
    assert [str(x.inventory) for x in sorted(ids)] == ["9", "10", "9626", "9626A"]
    a, b = ScanId.parse(VOC), ScanId.parse(VOC).with_number(2)
    assert b < a and sorted([a, b]) == [b, a]


def test_scan_number():
    assert scan_number("NL-HaNA_1.04.02_2555_0029.jpg") == 29
    assert scan_number("NL-HaNA_1.04.02_2555_10029") == 10029


def test_with_number():
    p = ScanId.parse(VOC + ".jpg").with_number(7)
    assert str(p) == "NL-HaNA_1.04.02_9015A_0007.jpg"


def test_parse_doc_id():
    assert parse_doc_id(SG + "-text_region-1-2-3-4") == (SG, (1, 2, 3, 4))
    assert parse_doc_id("-text_region-1-2-3-4") is None
    assert parse_doc_id(SG + "-text_region-1-2-3") is None
    assert parse_doc_id(SG + "-text_region-1-2-3-4-5") is None
    assert parse_doc_id(SG + "-text_region-1-2-x-4") is None
    assert parse_doc_id(SG + "-text_region--1-2-3-4") is None
    assert parse_doc_id(SG) is None
