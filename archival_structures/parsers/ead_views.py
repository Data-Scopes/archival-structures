"""Flat table views over the component tree (Step 6 of `docs/ead_parser/plan.md`).

Steps 3-5 built a lossless tree and the interpretation to hang off it (inventory numbers, dates,
digitisation links). This module is where that tree becomes the flat, one-row-per-thing tables
most callers actually want -- `pandas.DataFrame`s, like the legacy
{func}`archival_structures.parsers.ead_parser.get_inventory_info`, but built by *selecting*
components from a tree that already exists rather than accumulating rows during a level-specific
walk.

Two ways to select which components become rows, because Step 2 found that "the file level" is
not a safe universal answer:

`to_frame(finding_aid, level=...)`
    Rows are every component at a given, named level (`"file"`, `"item"`, or any of the other 25
    values the corpus contains). Simple, but wrong for the two-plus repositories (Step 2) that
    have no `file` components at all.

`unit_of_description_frame(finding_aid)`
    Rows are components with a **resolved inventory number** (Step 4's
    `identify_inventory_number`), excluding any component whose ancestor already has one. This
    answers Step 2/4's open question ("which component is the unit of description?")
    *functionally* rather than by level name: Brabants Historisch Informatie Centrum's rows come
    from `otherlevel:Inschrijving`, Collectie Overijssel Zwolle's from `otherlevel:Adres`, and
    Erfgoedcentrum Achterhoek en Liemers' `Dossier` (not its `item` children, which correctly
    resolve to no inventory number of their own -- Step 4's finding).

`legacy_compatible_frame(finding_aid)`
    Reproduces `get_inventory_info`'s exact column order (`series`, `subseries_1..N`,
    `filegroup`, `inventory_range`, `file`, `unitdate`, `inventory_num`, `mets_file`), for
    side-by-side comparison and as a migration aid. This is the **one place in the rewrite**
    that is allowed to branch on a component's level being literally `"series"`/`"subseries"`/
    `"otherlevel:filegrp"` -- its entire purpose is reproducing a column layout defined in those
    terms, which is a narrow, deliberate exception to decision D8, not a reversion of it.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd

from archival_structures.parsers.ead_digital import parse_dao, summarise_digitisation
from archival_structures.parsers.ead_fields import identify_inventory_number, parse_unitdate
from archival_structures.parsers.ead_tree import EADComponent, EADFindingAid


def _ancestor_titles(component: EADComponent) -> List[Optional[str]]:
    return [a.title for a in component.ancestors()]


def _row_from_component(component: EADComponent, max_ancestry: int, finding_aid: EADFindingAid) -> dict:
    ancestors = _ancestor_titles(component)
    ancestors = ancestors + [None] * (max_ancestry - len(ancestors))
    inventory = identify_inventory_number(component)
    date = parse_unitdate(component)
    digital = summarise_digitisation(component)
    row = {
        "path": " > ".join(t for t in _ancestor_titles(component) if t),
        "level": component.level,
        "title": component.title,
        "inventory_number": str(inventory.inventory_number) if inventory else None,
        "inventory_number_rule": inventory.rule,
        "unitdate": date.text if date else None,
        "year_begin": date.year_begin if date else None,
        "year_end": date.year_end if date else None,
        "digitisation_mechanisms": ";".join(digital.mechanisms),
        "n_scans_known": digital.n_scans_known,
        "citation": component.cite(finding_aid.path),
    }
    for depth, title in enumerate(ancestors, start=1):
        row[f"ancestor_{depth}"] = title
    return row


def _ancestor_columns(max_ancestry: int) -> List[str]:
    return [f"ancestor_{i}" for i in range(1, max_ancestry + 1)]


def _frame_from_components(components: List[EADComponent], finding_aid: EADFindingAid) -> pd.DataFrame:
    max_ancestry = max((len(c.ancestors()) for c in components), default=0)
    rows = [_row_from_component(c, max_ancestry, finding_aid) for c in components]
    columns = (["path"] + _ancestor_columns(max_ancestry) +
               ["level", "title", "inventory_number", "inventory_number_rule", "unitdate",
                "year_begin", "year_end", "digitisation_mechanisms", "n_scans_known",
                "citation"])
    return pd.DataFrame(rows, columns=columns)


def to_frame(finding_aid: EADFindingAid, level: str) -> pd.DataFrame:
    """One row per component at `level` (a normalised level string -- `"file"`, `"item"`, an
    `"otherlevel:<value>"`, or `"(none)"`), with dynamically-sized ancestor-title columns.

    Does not assume `level` is the finding aid's unit of description; that is
    `unit_of_description_frame`'s job. `to_frame` is for when the caller already knows which
    level they want (e.g. Step 5's `level="asset"`-shaped question, or comparing one named level
    across finding aids that do share a convention).
    """
    components = [c for c in finding_aid.walk() if c.level == level]
    return _frame_from_components(components, finding_aid)


def unit_of_description_frame(finding_aid: EADFindingAid) -> pd.DataFrame:
    """One row per component that is *the* thing being described -- identified functionally, by
    carrying a resolved inventory number, not by its `@level` name.

    A component qualifies if `identify_inventory_number` resolves it (§ Step 4) **and** none of
    its *descendants* also resolves -- so a `Dossier` numbered `28` contributes one row even
    though it sits under a `series` that carries no separate number of its own, and if a
    resolved component contains a more specific resolved descendant, the descendant wins (the
    deepest resolvable node in any chain is preferred, never the shallowest): a `file` inside a
    resolved `series` must be the row, not the series.

    That last case is not observed anywhere in the 40-finding-aid corpus (0 of 7,079 `series`/
    `subseries` components resolve at all -- Amsterdam's series-level `<unitid label=
    "Rubriekscode">` is correctly excluded by name, and every other repository's series-level
    identifier is either similarly labelled or shape-excluded, e.g. a `series_code`-shaped value
    with a trailing dot), but an earlier version of this function excluded by resolved
    *ancestor* instead of resolved *descendant* -- the wrong direction, silently correct only
    because no real finding aid currently has a resolved series with a resolved child. Fixed
    before that stopped being true rather than after.
    """
    resolved = [c for c in finding_aid.walk() if identify_inventory_number(c)]
    resolved_ids = {id(c) for c in resolved}

    def has_resolved_descendant(component: EADComponent) -> bool:
        return any(id(descendant) in resolved_ids
                  for descendant in component.walk() if descendant is not component)

    rows_components = [c for c in resolved if not has_resolved_descendant(c)]
    return _frame_from_components(rows_components, finding_aid)


# ---------------------------------------------------------------------------------------------
# Legacy-compatible view
# ---------------------------------------------------------------------------------------------

#: `get_inventory_info`'s column order, reproduced exactly.
LEGACY_COLUMNS = [
    "series", "subseries_1", "subseries_2", "filegroup", "inventory_range", "file",
    "unitdate", "inventory_num", "mets_file",
]


def legacy_compatible_frame(finding_aid: EADFindingAid, max_subseries_depth: int = 2) -> pd.DataFrame:
    """Reproduces `ead_parser.get_inventory_info`'s exact column layout and row selection --
    one row per `file`-level component with a resolved inventory number -- for direct,
    column-for-column comparison against the legacy parser's output.

    This is the one function in the rewrite allowed to test `component.level` against a literal
    string for control flow (`"series"`, `"subseries"`, `"otherlevel:filegrp"`): its entire job
    is reproducing a column layout defined in exactly those terms. Everywhere else in this
    rewrite, decision D8 holds.
    """
    rows = []
    for component in finding_aid.walk():
        if component.level != "file":
            continue
        inventory = identify_inventory_number(component)
        if not inventory:
            continue  # legacy only emitted files with an inventory-number-shaped unitid
        ancestors = component.ancestors()
        series = next((a.title for a in ancestors if a.level == "series"), None)
        subseries = [a.title for a in ancestors if a.level == "subseries"]
        subseries = subseries[:max_subseries_depth]
        subseries += [None] * (max_subseries_depth - len(subseries))
        filegroup = next((a for a in reversed(ancestors) if a.level == "otherlevel:filegrp"),
                         None)
        date = parse_unitdate(component)
        mets_refs = parse_dao(component)
        mets_file = next((d.href for d in mets_refs if d.role == "METS"), None)

        rows.append([
            series, *subseries,
            filegroup.title if filegroup else None,
            filegroup.unitid_values()[0] if filegroup and filegroup.unitid_values() else None,
            component.title,
            date.text if date else None,
            str(inventory.inventory_number),
            mets_file,
        ])
    return pd.DataFrame(rows, columns=LEGACY_COLUMNS)
