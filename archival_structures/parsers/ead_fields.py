"""Interpreting `<did>` fields: inventory-number identification, dates, physical description
(Step 4 of `docs/ead_parser/plan.md`).

Step 3 built a lossless tree that retains every `<did>` child but interprets only `<unitid>` and
`<unittitle>`. This module interprets the rest, and does the one piece of interpretation the
whole rewrite exists to get right: given a component's `<unitid>` list, which one (if any) is
*the* inventory number.

There is no shared convention to key off. Step 1/2 measured three disjoint, mutually exclusive
ones across the corpus's 13 repositories:

- Stadsarchief Amsterdam **labels** it: `<unitid label="Inventarisnummer" identifier="...">`.
- The Nationaal Archief marks it by *absence* of a `@type`, plus an `@identifier` attribute.
- Het Utrechts Archief (and every `opendata.archieven.nl` finding aid) marks it not at all --
  it is the one `<unitid>` left over once its siblings' `type="guid"`, `type="handle"`,
  `type="series_code"` etc. are excluded.

The legacy parser's `re.match(r"\\d+", unit['unitid'])` -- "the first `<unitid>` in the list
whose text starts with a digit" -- coped with the first two conventions by accident (Amsterdam
and Nationaal Archief happen not to put a digit-leading `<unitid>` before the real one) and
cannot cope with the third at all. Worse, it is a live hazard on the wider corpus: a `type="guid"`
`<unitid>` is a 32-character hex string, and a hex string starting with `0`-`9` is not rare --
**34,480 of 118,946 `file` components corpus-wide (29%) have a GUID-typed `<unitid>` listed
before their real identifier and starting with a digit**, which the legacy rule would silently
select instead (`docs/ead_parser/reports/step-04-inventory-numbers.md`, § Data analysis).

`identify_inventory_number` replaces the single regex with an explicit, ranked set of rules,
each one grounded in a measured convention rather than a guess, and reports *which* rule fired
(or that none did) so disagreements are auditable rather than silent.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from archival_structures.model.inventory_number import (
    InvalidInventoryNumberError,
    InventoryNumber,
)
from archival_structures.parsers.ead_harvest import local_name
from archival_structures.parsers.ead_start_end_year import get_begin_end_year
from archival_structures.parsers.ead_text import (
    _SPAN_END,
    _SPAN_START,
    _raw_marked_text,
    element_text,
)

if TYPE_CHECKING:
    from archival_structures.parsers.ead_tree import EADComponent, UnitID

#: Non-identifying `<unitid @type>` values, i.e. values that are never the inventory number.
#: guid and urn:uuid identify the record itself (Memorix/De Ree exports); handle is an
#: alternate reference; blank is a literal placeholder (three hyphens); series_code is a
#: dotted hierarchy position (e.g. 2.1.1.) shared by every component in a branch, not one file.
#:
#: type=obsolete is deliberately not in this set, though Step 1 originally listed it as
#: non-identifying based on one repository's usage (Archief Eemland: values like St90-832,
#: genuinely superseded). Step 6 found type=obsolete means something entirely different at the
#: Nationaal Archief and Erfgoedcentrum Achterhoek en Liemers: a perfectly ordinary, current,
#: plain-digit inventory number (e.g. 3714, 9125, 20, 316 -- 97%+ of both repositories'
#: obsolete-typed values parse cleanly as an InventoryNumber). Excluding it by type was
#: actively wrong for those two repositories (181+ real inventory numbers in one Nationaal
#: Archief finding aid alone resolved to no match because of it -- found by Step 6's row-level
#: diff against the legacy parser, which happened to reproduce these correctly by accident) and
#: was never actually necessary for Eemland's case either: a hyphenated value already fails to
#: parse as an InventoryNumber (no token for a hyphen), so the shape rule excludes it on its
#: own. Every other type in this set was checked the same way (per repository, does its value
#: actually parse, and if so at what level) before being kept.
NON_IDENTIFYING_TYPES = {"guid", "urn:uuid", "handle", "blank", "series_code"}

#: `<unitid @label>` values that are a different kind of code, not the inventory number --
#: Stadsarchief Amsterdam's "Rubriekscode" also carries an `@identifier` attribute (3,013 in the
#: corpus) and would otherwise satisfy the `identifier:untyped` rule below.
NON_IDENTIFYING_LABEL_RE = re.compile(r"rubriekscode", re.IGNORECASE)

#: The one `@label` value Step 1/2 found that positively marks the inventory number.
INVENTORY_LABEL_RE = re.compile(r"inventarisnummer", re.IGNORECASE)

#: A bare 32-character hex string is a GUID whatever its `@type` says (defensive: the corpus's
#: `type="guid"` unitids are always this shape, but the shape is the actual hazard, not the type
#: attribute, so it is checked directly rather than trusted to always be labelled).
_HEX32_RE = re.compile(r"^[0-9A-Fa-f]{32}$")


def _looks_like_url(value: str) -> bool:
    return "://" in value


def _looks_like_guid(value: str) -> bool:
    return bool(_HEX32_RE.match(value))


def _excluded_as_identifier(unitid: "UnitID") -> bool:
    """True if `unitid` is positively known *not* to be the inventory number."""
    if not unitid.value:
        return True
    if unitid.type in NON_IDENTIFYING_TYPES:
        return True
    if unitid.label and NON_IDENTIFYING_LABEL_RE.search(unitid.label):
        return True
    if _looks_like_url(unitid.value) or _looks_like_guid(unitid.value):
        return True
    return False


@dataclass
class InventoryNumberResult:
    """The outcome of `identify_inventory_number` for one component."""

    unitid: Optional["UnitID"]
    inventory_number: Optional[InventoryNumber]
    #: which rule produced this result -- ``label:inventarisnummer``, ``type:ABS``,
    #: ``identifier:untyped``, ``shape:only-candidate``, ``shape:ambiguous``, or ``none`` (no
    #: unitid could be identified as the inventory number at all).
    rule: str
    #: for ``shape:ambiguous``: every candidate the shape heuristic could not choose between.
    candidates: List["UnitID"] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.inventory_number is not None


def identify_inventory_number(component: "EADComponent") -> InventoryNumberResult:
    """The `<unitid>` of `component` (if any) that is its inventory number, and by which rule.

    Rules are tried in order of how directly they were measured, not how common they are, so
    that a labelled or `@identifier`-marked unitid is never overridden by a same-looking one
    the shape heuristic would also have accepted:

    1. `@label` matching "Inventarisnummer" (Stadsarchief Amsterdam).
    2. `@type="ABS"` (the legacy parser's rule; measured to fire 0 times in the current 40-finding-aid
       corpus, kept because it is a documented Nationaal Archief convention that may appear
       elsewhere).
    3. No `@type`, an `@identifier` attribute present, and no exclusionary `@label` (Nationaal
       Archief).
    4. Shape: among unitids not positively excluded (§ `NON_IDENTIFYING_TYPES` /
       `NON_IDENTIFYING_LABEL_RE` / URL- or GUID-shaped values), the one whose text parses as an
       `InventoryNumber`. Exactly one such candidate → accepted (Het Utrechts Archief and every
       `opendata.archieven.nl` finding aid rely on this). More than one → reported as ambiguous
       rather than guessed at.
    """
    unitids = component.unitids

    for unitid in unitids:
        if unitid.label and INVENTORY_LABEL_RE.search(unitid.label) and unitid.value:
            try:
                return InventoryNumberResult(
                    unitid, InventoryNumber(unitid.value), "label:inventarisnummer")
            except InvalidInventoryNumberError:
                return InventoryNumberResult(unitid, None, "label:inventarisnummer:unparseable")

    remaining = [u for u in unitids if not _excluded_as_identifier(u)]

    for unitid in remaining:
        if unitid.type == "ABS":
            try:
                return InventoryNumberResult(unitid, InventoryNumber(unitid.value), "type:ABS")
            except InvalidInventoryNumberError:
                continue

    for unitid in remaining:
        if unitid.type is None and unitid.identifier:
            try:
                return InventoryNumberResult(
                    unitid, InventoryNumber(unitid.value), "identifier:untyped")
            except InvalidInventoryNumberError:
                continue

    shape_candidates = []
    for unitid in remaining:
        try:
            shape_candidates.append((unitid, InventoryNumber(unitid.value)))
        except InvalidInventoryNumberError:
            continue

    if len(shape_candidates) == 1:
        unitid, number = shape_candidates[0]
        return InventoryNumberResult(unitid, number, "shape:only-candidate")
    if len(shape_candidates) > 1:
        # deterministic tie-break (shortest text) so the result is reproducible, but the
        # ambiguity itself -- not this tie-break -- is what gets reported and audited
        unitid, number = min(shape_candidates, key=lambda pair: len(pair[0].value))
        return InventoryNumberResult(
            unitid, number, "shape:ambiguous", candidates=[u for u, _ in shape_candidates])

    return InventoryNumberResult(None, None, "none")


def legacy_style_identify(component: "EADComponent") -> Optional[str]:
    """What `archival_structures.parsers.ead_parser`'s `unit_has_inv_num_unitid` /
    `get_inv_num_unit` actually select: the *first* `<unitid>` in document order whose text
    starts with a digit -- regardless of `@type`. Used only to measure how often the legacy rule
    and `identify_inventory_number` disagree (see the Step 4 report); not used by the new parser.
    """
    for unitid in component.unitids:
        if unitid.value and re.match(r"\d", unitid.value):
            return unitid.value
    return None


# ---------------------------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------------------------

@dataclass
class UnitDate:
    """A parsed `<unitdate>`, wherever in the component it was found."""

    text: Optional[str]
    normal: Optional[str]
    calendar: Optional[str]
    era: Optional[str]
    year_begin: Optional[str]
    year_end: Optional[str]
    #: `"did"` for an ordinary `<did><unitdate>`, `"unittitle"` for one embedded inside
    #: `<unittitle>` (Step 0 found the legacy parser's text/tail concatenation for this case
    #: buggy -- see the Step 4 report's regression example).
    source: str


def _build_unitdate(element, source: str) -> UnitDate:
    normal = element.get("normal")
    year_begin, year_end = get_begin_end_year(normal or "")
    return UnitDate(
        text=(element.text or "").strip() or None,
        normal=normal,
        calendar=element.get("calendar"),
        era=element.get("era"),
        year_begin=year_begin or None,
        year_end=year_end or None,
        source=source,
    )


def parse_unitdate(component: "EADComponent") -> Optional[UnitDate]:
    """`component`'s date, from an ordinary `<did><unitdate>` if present, else from a
    `<unitdate>` embedded inside its `<unittitle>` (the construct at
    `NL-HaNA_1.01.02` file `7474`:
    `<unittitle><unitdate>1600-1601</unitdate></unittitle>`, with no separate `<did><unitdate>` at all). `None` if there is no date either way.
    """
    dates = component.did_extra.get("unitdate")
    if dates:
        return _build_unitdate(dates[0], "did")
    if component.unittitle_element is not None:
        for child in component.unittitle_element:
            if isinstance(child.tag, str) and local_name(child.tag) == "unitdate":
                return _build_unitdate(child, "unittitle")
    return None


def _collapse_with_map(raw: str) -> Tuple[str, List[int]]:
    """`" ".join(raw.split())` plus, for every non-space raw index, its index in the result
    (`-1` for a space)."""
    out: List[str] = []
    index_map = [-1] * len(raw)
    pending_space = False
    for i, ch in enumerate(raw):
        if ch.isspace():
            pending_space = True
            continue
        if pending_space and out:
            out.append(" ")
        pending_space = False
        out.append(ch)
        index_map[i] = len(out) - 1
    return "".join(out), index_map


def split_unittitle_dates(unittitle_element) -> Tuple[str, List[Optional[Tuple[int, int]]]]:
    """Cut the structurally marked dates out of a `<unittitle>`: `(prose, spans)`.

    With `text = element_text(unittitle_element)`, `spans` has one entry per *direct*
    `<unitdate>` child, in document order: `(s, e)` with `text[s:e] == element_text(child)`, or
    `None` for a child with no non-space text. `prose` is `text` with each non-`None` span
    replaced by one space and whitespace collapsed (so two punctuation marks around a cut date
    never glue). With no such child, `spans == []` and `prose == text`.

    The spans come from one walk of the tree with the children bracketed by markers, then the
    same whitespace collapse that builds `element_text`; the date is never searched for in the
    text, so repeated or oddly spaced date strings cannot be confused.
    """
    children = [
        child
        for child in unittitle_element
        if isinstance(child.tag, str) and local_name(child.tag) == "unitdate"
    ]
    marked = _raw_marked_text(unittitle_element, children)
    raw_chars: List[str] = []
    raw_spans: List[Tuple[int, int]] = []  # in document order of the *start* marker
    open_at: List[int] = []  # stack of indices into raw_spans
    for ch in marked:
        if ch == _SPAN_START:
            open_at.append(len(raw_spans))
            raw_spans.append((len(raw_chars), -1))
        elif ch == _SPAN_END:
            idx = open_at.pop()
            raw_spans[idx] = (raw_spans[idx][0], len(raw_chars))
        else:
            raw_chars.append(ch)
    collapsed, index_map = _collapse_with_map("".join(raw_chars))
    assert collapsed == element_text(unittitle_element)
    assert len(raw_spans) == len(children)

    spans: List[Optional[Tuple[int, int]]] = []
    for a, b in raw_spans:
        mapped = [index_map[i] for i in range(a, b) if index_map[i] != -1]
        spans.append((mapped[0], mapped[-1] + 1) if mapped else None)
    prose = collapsed
    for span in sorted((s for s in spans if s is not None), reverse=True):
        prose = prose[: span[0]] + " " + prose[span[1] :]
    return " ".join(prose.split()), spans


# ---------------------------------------------------------------------------------------------
# Physical description / location
# ---------------------------------------------------------------------------------------------

@dataclass
class PhysDesc:
    extent: Optional[str]
    physfacet: Optional[str]
    raw_text: Optional[str]


def parse_physdesc(component: "EADComponent") -> Optional[PhysDesc]:
    """`component`'s `<physdesc>`: its own text as `extent` if it has no children (the common
    case, e.g. `<physdesc>1 stuk</physdesc>`), else its `<extent>`/`<physfacet>` children."""
    elements = component.did_extra.get("physdesc")
    if not elements:
        return None
    element = elements[0]
    extent = physfacet = None
    children = [c for c in element if isinstance(c.tag, str)]
    if not children:
        extent = (element.text or "").strip() or None
    else:
        for child in children:
            name = local_name(child.tag)
            text = (child.text or "").strip() or None
            if name == "extent":
                extent = text
            elif name == "physfacet":
                physfacet = text
    raw_text = element_text(element) or None
    return PhysDesc(extent=extent, physfacet=physfacet, raw_text=raw_text)


@dataclass
class PhysLoc:
    text: Optional[str]
    attributes: Dict[str, str]


def parse_physloc(component: "EADComponent") -> Optional[PhysLoc]:
    """`component`'s `<physloc>`: usually empty text with a `depot` attribute (Stadsarchief
    Amsterdam), sometimes free text (Nationaal Archief)."""
    elements = component.did_extra.get("physloc")
    if not elements:
        return None
    element = elements[0]
    return PhysLoc(text=(element.text or "").strip() or None, attributes=dict(element.attrib))


# ---------------------------------------------------------------------------------------------
# Controlled access (genre/form)
# ---------------------------------------------------------------------------------------------

@dataclass
class GenreForm:
    text: Optional[str]
    attributes: Dict[str, str]


def parse_controlaccess(component: "EADComponent") -> List[GenreForm]:
    """`component`'s `<controlaccess><genreform>` entries (the legacy parser's `parse_access`),
    from `other_children` -- `<controlaccess>` is a sibling of `<did>`, not inside it."""
    results = []
    for element in component.other_children.get("controlaccess", []):
        for child in element:
            if isinstance(child.tag, str) and local_name(child.tag) == "genreform":
                results.append(GenreForm(
                    text=(child.text or "").strip() or None, attributes=dict(child.attrib)))
    return results
