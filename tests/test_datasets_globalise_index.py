import numpy as np

from archival_structures.datasets.globalise_index import (
    GLOBALISE_INDEX_COLUMNS, load_globalise_index)


def _write(tmp_path):
    cols = list(GLOBALISE_INDEX_COLUMNS) + ["EXTRA"]
    rows = [
        ["1", "1.2.3", "t1", "1000", "s", "10", "12", "1650", "Batavia", "letter", "x"],
        ["2", "4.5", "t2", "1000", "s", "abc", "15", "1651", "Batavia", "letter", "x"],
        ["3", "", "t3", "1001", "s", "1", "2", "n.a.", "Ceylon", "report", "x"],
        ["4", "7.8", "t4", "1002", "s", "3", "4", "1700", "Ceylon", "report", "x"],
        ["5", "9", "t5", "1002", "s", "5", "6", "1701", "Ceylon", "report", "x"],
    ]
    p = tmp_path / "index.csv"
    p.write_text("\n".join(";".join(r) if False else ",".join(r) for r in [cols] + rows),
                 encoding="utf-8-sig")
    return p


def test_load_all(tmp_path):
    df = load_globalise_index(_write(tmp_path))
    assert list(df.columns) == list(GLOBALISE_INDEX_COLUMNS.values())
    assert len(df) == 5 and "EXTRA" not in df.columns
    assert df.folio_start.iloc[0] == 10 and np.isnan(df.folio_start.iloc[1])
    assert np.isnan(df.year_earliest.iloc[2])
    assert df.tanap_id.iloc[0] == "123" and df.tanap_id.iloc[1] == "45"
    assert df.inventory_number.dtype == object and df.inventory_number.iloc[0] == "1000"
    assert df.globalise_id.iloc[0] == "1"


def test_inventory_filter_resets_index(tmp_path):
    df = load_globalise_index(_write(tmp_path), inventories=["1002"])
    assert list(df.index) == [0, 1] and list(df.globalise_id) == ["4", "5"]


def test_column_map_override(tmp_path):
    cols = {"INVENTORY NUMBER": "inventory_number", "FOLIONUMBER (START OF DOCUMENT)": "folio_start"}
    df = load_globalise_index(_write(tmp_path), columns=cols)
    assert list(df.columns) == ["inventory_number", "folio_start"]
    assert df.folio_start.dtype.kind == "f"
