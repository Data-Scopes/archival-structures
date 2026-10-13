"""Digitisation links: `<dao>`, `<daogrp>`, Memorix `<assets>`, and handle `<unitid>`s
(Step 5 of `docs/ead_parser/plan.md`).

Step 2 found four distinct digitisation-linking mechanisms in the corpus, and they are close to
mutually exclusive by repository -- no finding aid mixes more than one:

| Mechanism | Corpus total | Repositories |
| --- | ---: | --- |
| `<unitid type="handle">` | 179,890 | most repositories -- the majority mechanism |
| Memorix `<nexus:assets>`/`<nexus:asset>` | 42,133 + 239,458 (`assets`/`asset` elements) | Stadsarchief Amsterdam only |
| `<dao>` | 27,661 | Nationaal Archief only |
| `<daogrp>` | 14,263 | 7 `opendata.archieven.nl`-family repositories, 95% (13,603) at BHIC alone |

That table corrects an unverified attribution this plan carried since Step 2 ("`<dao>` and
`<daogrp>`... Nationaal Archief: 27,661 and 14,263 respectively"): checking per repository
(this step) shows `<daogrp>` never occurs in a Nationaal Archief finding aid at all -- it is a
different exporter's construct that happens to look adjacent to `<dao>` in the EAD schema.

`<daogrp>` also turned out to be structurally different from a single `<dao>` reference: it is
an XLink *extended link group* -- a `<daodesc>` free-text note plus one `thumb`/`reference`
`<daoloc>` pair *per scan* -- so it is a small, per-component digitisation manifest in its own
right, not a single href. No truncation ceiling was found in it (max 8 scans observed, median
1), unlike Memorix's `<nexus:assets>`, which Step 0 found is capped at 10 regardless of an
inventory number's real page count.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from archival_structures.parsers.ead_harvest import local_name

XLINK_NS = "http://www.w3.org/1999/xlink"


def _xlink(element, attribute: str) -> Optional[str]:
    return element.get(f"{{{XLINK_NS}}}{attribute}") or element.get(attribute)


# ---------------------------------------------------------------------------------------------
# <dao> -- Nationaal Archief
# ---------------------------------------------------------------------------------------------

@dataclass
class DaoReference:
    """One `<dao>` element -- a single digital-object reference (Nationaal Archief: always a
    METS manifest URL for the whole inventory number, never a per-page link)."""

    href: Optional[str]
    role: Optional[str]
    linktype: Optional[str]
    text: Optional[str]
    attributes: Dict[str, str]


def parse_dao(component) -> List[DaoReference]:
    """`component`'s `<dao>` elements (`did_extra['dao']` -- Step 3 retains them inside `<did>`)."""
    results = []
    for element in component.did_extra.get("dao", []):
        results.append(DaoReference(
            href=_xlink(element, "href"),
            role=element.get("role"),
            linktype=_xlink(element, "type"),
            text=(element.text or "").strip() or None,
            attributes=dict(element.attrib),
        ))
    return results


# ---------------------------------------------------------------------------------------------
# <daogrp> -- opendata.archieven.nl-family exports
# ---------------------------------------------------------------------------------------------

@dataclass
class DaoGroupScan:
    """One `thumb`/`reference` `<daoloc>` pair inside a `<daogrp>` -- one digitised scan."""

    thumbnail_url: Optional[str]
    reference_url: Optional[str]


@dataclass
class DaoGroup:
    """One `<daogrp>` -- an XLink extended-link group of scan thumbnail/reference pairs, plus
    free-text notes (`<daodesc><p>`) that are not guaranteed to correspond 1:1 with the scans
    (Gelders Archief's notes reference other components: `"hoort bij no. 11, scan 1-0015"`)."""

    notes: List[str]
    scans: List[DaoGroupScan]
    attributes: Dict[str, str]

    @property
    def n_scans(self) -> int:
        return len(self.scans)


def parse_daogrp(component) -> List[DaoGroup]:
    """`component`'s `<daogrp>` elements (`other_children['daogrp']` -- a sibling of `<did>`,
    not inside it)."""
    results = []
    for element in component.other_children.get("daogrp", []):
        notes = []
        scans: List[DaoGroupScan] = []
        pending_thumb: Optional[str] = None
        for child in element:
            if not isinstance(child.tag, str):
                continue
            name = local_name(child.tag)
            if name == "daodesc":
                notes.extend(
                    (p.text or "").strip() for p in child
                    if isinstance(p.tag, str) and local_name(p.tag) == "p" and p.text
                )
            elif name == "daoloc":
                role = _xlink(child, "role")
                label = _xlink(child, "label")
                href = _xlink(child, "href")
                if role == "thumbnail" or label == "thumb":
                    pending_thumb = href
                elif label == "reference":
                    scans.append(DaoGroupScan(thumbnail_url=pending_thumb, reference_url=href))
                    pending_thumb = None
        results.append(DaoGroup(notes=[n for n in notes if n], scans=scans,
                                attributes=dict(element.attrib)))
    return results


# ---------------------------------------------------------------------------------------------
# Memorix <nexus:assets> -- Stadsarchief Amsterdam
# ---------------------------------------------------------------------------------------------

#: Step 0 found every `<nexus:assets>` in the corpus capped at exactly 10 `<nexus:asset>`
#: children, with no count/pagination attribute to recover the rest -- a preview, not a page
#: sequence. Reused here rather than re-measured, since re-measuring would need Step 0's evidence
#: to already have found and cross-checked (it did: `docs/ead_parser/reports/step-00-baseline.md`).
ASSET_TRUNCATION_CEILING = 10


@dataclass
class Asset:
    """One `<nexus:asset>` -- one digitised scan's thumbnail-service identifiers."""

    order: Optional[int]
    name: Optional[str]
    iiif_url: Optional[str]


@dataclass
class AssetList:
    """`component`'s `<nexus:assets>`, sorted by `asset_order`, with the truncation this step
    inherits from Step 0's finding made explicit rather than left for a caller to discover by
    counting to exactly 10."""

    assets: List[Asset]
    truncated: bool
    allow_downloads: Optional[bool]

    def __len__(self) -> int:
        return len(self.assets)


def parse_assets(component) -> Optional[AssetList]:
    """`component`'s Memorix `<nexus:assets>` (`did_extra['assets']`), or `None` if it has none."""
    elements = component.did_extra.get("assets")
    if not elements:
        return None
    element = elements[0]
    assets = []
    for child in element:
        if not isinstance(child.tag, str) or local_name(child.tag) != "asset":
            continue
        name = iiif_url = None
        for grandchild in child:
            if not isinstance(grandchild.tag, str):
                continue
            gname = local_name(grandchild.tag)
            if gname == "name":
                name = (grandchild.text or "").strip() or None
            elif gname == "iiif":
                iiif_url = (grandchild.text or "").strip() or None
        order_text = child.get("asset_order")
        order = int(order_text) if order_text is not None and order_text.isdigit() else None
        assets.append(Asset(order=order, name=name, iiif_url=iiif_url))
    assets.sort(key=lambda a: (a.order is None, a.order))
    allow = element.get("allowDownloads")
    return AssetList(
        assets=assets,
        truncated=len(assets) >= ASSET_TRUNCATION_CEILING,
        allow_downloads={"true": True, "false": False}.get(allow) if allow else None,
    )


# ---------------------------------------------------------------------------------------------
# <unitid type="handle"> -- the majority mechanism
# ---------------------------------------------------------------------------------------------

def parse_handle_urls(component) -> List[str]:
    """`component`'s `<unitid type="handle">` URLs -- 179,890 corpus-wide, used by most
    repositories (including Het Utrechts Archief, which has neither `<dao>`, `<daogrp>` nor
    Memorix assets). Typically one per component, but not assumed to be."""
    return [u.value for u in component.unitids if u.type == "handle" and u.value]


# ---------------------------------------------------------------------------------------------
# Unified summary
# ---------------------------------------------------------------------------------------------

@dataclass
class DigitisationSummary:
    """Every digitisation link `component` has, whichever of the four mechanisms it uses (a
    component uses at most one in every finding aid observed, but this does not assume that)."""

    dao: List[DaoReference] = field(default_factory=list)
    daogrp: List[DaoGroup] = field(default_factory=list)
    assets: Optional[AssetList] = None
    handle_urls: List[str] = field(default_factory=list)

    @property
    def mechanisms(self) -> List[str]:
        """Which of the four mechanisms are actually present, by name."""
        present = []
        if self.dao:
            present.append("dao")
        if self.daogrp:
            present.append("daogrp")
        if self.assets:
            present.append("assets")
        if self.handle_urls:
            present.append("handle")
        return present

    @property
    def is_digitised(self) -> bool:
        return bool(self.mechanisms)

    @property
    def n_scans_known(self) -> Optional[int]:
        """The number of scans this component is known to have, when countable at all.

        `None` when the only mechanism present doesn't say (a bare `<dao>`/handle URL to a
        whole-object manifest, not a per-page list) -- distinct from `0`, which means a listed
        mechanism with no scans in it.
        """
        if self.daogrp:
            return sum(group.n_scans for group in self.daogrp)
        if self.assets is not None:
            return len(self.assets)
        return None


def summarise_digitisation(component) -> DigitisationSummary:
    """`component`'s digitisation links, across all four mechanisms."""
    return DigitisationSummary(
        dao=parse_dao(component),
        daogrp=parse_daogrp(component),
        assets=parse_assets(component),
        handle_urls=parse_handle_urls(component),
    )
