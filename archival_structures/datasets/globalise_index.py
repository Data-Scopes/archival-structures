"""Loader for the GLOBALISE digitised indexes of the VOC (CSV, UTF-8 with BOM).

The path is always passed in; the data stays where it is.
"""
import pandas as pd

GLOBALISE_INDEX_COLUMNS = {
    "ID": "globalise_id", "ID (TANAP)": "tanap_id", "ID (DIGITIZED TYPOSCRIPTS)": "typoscript_id",
    "INVENTORY NUMBER": "inventory_number", "SECTION": "section",
    "FOLIONUMBER (START OF DOCUMENT)": "folio_start", "FOLIONUMBER (END OF DOCUMENT)": "folio_end",
    "YEAR (EARLIEST)": "year_earliest", "SETTLEMENT": "settlement",
    "DOCUMENT TYPE (GLOBALISE)": "globalise_doc_type",
}


def load_globalise_index(path, inventories=None, columns=GLOBALISE_INDEX_COLUMNS):
    """Read the index with ``columns`` (source name -> short name), all as str, then coerce.

    ``folio_start``, ``folio_end`` and ``year_earliest`` (when present) become numeric (NaN when
    not a number); ``tanap_id`` (when present) loses its dots; ``inventories`` filters on
    ``inventory_number``. Index is reset.
    """
    df = pd.read_csv(path, usecols=list(columns), dtype=str).rename(columns=columns)
    if inventories is not None:
        df = df[df["inventory_number"].isin(inventories)]
    for c in ("folio_start", "folio_end", "year_earliest"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "tanap_id" in df.columns:
        df["tanap_id"] = df["tanap_id"].str.replace(".", "", regex=False)
    return df.reset_index(drop=True)
