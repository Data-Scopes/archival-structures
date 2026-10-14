"""Parsing ``<scopecontent>`` (and, by request, ``<odd>``) elements into typed, picklable
``ScopeNote``s: shape, segments, references (inventory, folio, external), raw date strings.

Moved here from LotA's ``archival_language.ead.scopecontent`` and ``scopecontent_survey`` (phase 02,
step 5). **Extract, do not interpret:** no date parsing beyond keeping the matched substring, no
place matching, no spelling normalisation. The parser never drops an element: every element in
``component.other_children[tag]`` gets exactly one ``ScopeNote``.

The Dutch / Nationaal Archief rules are parameters with ``NA_...`` defaults: ``NA_SHAPE_RULES``
for the shape taxonomy and ``NA_REFERENCE_PATTERNS`` for the reference and date regexes. No
NA-specific regex is referenced outside those constants.

**Cross-archive references.** A note such as ``Zie Aanwinsten, 1.11.01.01, inv.nr. 687`` points
into another archive. ``ScopeReference.target_archive`` holds that archive code; ``None`` means
the current archive. A reference with a target must never be joined to the current archive's
inventory.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Pattern, Sequence, Tuple

import pandas as pd
from lxml import etree

from archival_structures.model.inventory_number import (
    InvalidInventoryNumberError,
    InventoryNumber,
)
from archival_structures.parsers.ead_text import _LB_MARK, _NO_SPACE_BEFORE, element_text
from archival_structures.parsers.ead_tree import EADComponent, EADFindingAid

LB_TAG = "lb"


def _local_tag(tag: object) -> Optional[str]:
    if not isinstance(tag, str):
        return None
    return tag.split("}")[-1]


def _direct_children(element: etree._Element) -> List[etree._Element]:
    """``element``'s direct element children, skipping comments/PIs (non-``str`` tags)."""
    return [c for c in element if _local_tag(c.tag) is not None]


def descendant_tags(element: etree._Element) -> Tuple[str, ...]:
    """Sorted, de-duplicated local tag names of every descendant (any depth)."""
    tags = {t for d in element.iterdescendants() if (t := _local_tag(d.tag)) is not None}
    return tuple(sorted(tags))


# -- shape classification ------------------------------------------------------------------------


@dataclass(frozen=True)
class ShapeRule:
    """One row of the shape taxonomy. Exactly one of ``requires_empty``, ``requires_tag`` or
    ``pattern`` is set; ``classify_shape`` does not know or care which shape a rule names.
    """

    name: str
    description: str
    pattern: Optional[Pattern[str]] = None
    requires_tag: Optional[str] = None
    requires_empty: bool = False


# The taxonomy. Order matters: the first matching rule wins.
NA_SHAPE_RULES: Tuple[ShapeRule, ...] = (
    ShapeRule(
        "empty",
        "Zero-length text (S7). The one SG element with no content at all.",
        requires_empty=True,
    ),
    ShapeRule(
        "list_markup",
        "Carries a genuine <list>/<item> descendant (S6), rather than prose.",
        requires_tag="list",
    ),
    ShapeRule(
        "exhibitum_date",
        "An exhibitum ('received in session') date stamp (S1): 'Exh. St.-Gen. 19 december "
        "1742'. Matched anywhere in the text because it is often prefixed by a short physical "
        "note ('Kopie.Exh. ...').",
        pattern=re.compile(r"Exh\."),
    ),
    ShapeRule(
        "former_reference",
        "Provenance / former shelfmark (S2): 'Was Loketkas Admiraliteit, inv.nr. 12561.13.2.'",
        pattern=re.compile(r"^Was\b"),
    ),
    ShapeRule(
        "cross_reference",
        "A pointer to another inventory number (S3), Dutch 'zie' ('see') or 'vgl.' "
        "('compare'): 'zie inv. nr. 9919.', 'Zie Aanwinsten, 1.11.01.01 inv.nr. 1519 ...'. "
        "This is the dominant VOC shape.",
        pattern=re.compile(r"(?i)^(zie|vgl\.)\b"),
    ),
    ShapeRule(
        "content_enumeration",
        "A colon-introduced enumeration of what the unit holds (S4): 'Dit deel bevat: ...', "
        "'Het betreft hier: ...'.",
        pattern=re.compile(r"(?i)\b(bevat|betreft)\b.*:"),
    ),
    ShapeRule(
        "short_content_flag",
        "A short, uncolonned content flag (S5): 'Bevat index', 'Bevatten ook kopie-...'.",
        pattern=re.compile(r"(?i)^Bevat(ten)?\b"),
    ),
    ShapeRule(
        "index_note",
        "Mentions an index without fitting short_content_flag's prefix: 'De index mankeert', "
        "'Voorin een alfabetische index'.",
        pattern=re.compile(r"(?i)\bindex\b"),
    ),
    ShapeRule(
        "physical_form_note",
        "The whole element is a bare copy/physical-form note: 'Kopie.', 'Kopieën.'",
        pattern=re.compile(r"(?i)^Kopie(ën|en)?\.?$"),
    ),
    ShapeRule(
        "duplicate_note",
        "Notes a parallel/duplicate copy elsewhere: 'Met gelijkluidende ...', "
        "'Twee gelijkluidende kopieën ...'.",
        pattern=re.compile(r"(?i)^(Met |Twee )?gelijkluidende"),
    ),
    ShapeRule(
        "continuation_note",
        "'Hierbij tot ... ook ...' — a following-items-included note, common in SG liassen.",
        pattern=re.compile(r"(?i)^Hierbij\b"),
    ),
    ShapeRule(
        "gap_note",
        "A date-range exception or gap, either parenthetical ('(m.u.v. februari)') or using "
        "'ontbreekt'/'ontbreken'/'ontbrekende' ('De jaren 1729 en 1737 ontbreken'). Dutch's "
        "open/closed-syllable spelling rule means the stem is spelled 'ontbreek' in the "
        "singular but 'ontbrek' in the plural/infinitive — both alternatives are needed, a "
        "single 'ontbreek' substring check misses every plural form.",
        pattern=re.compile(r"(?i)(^\(.*\)$)|ontbr(eek|eken)"),
    ),
    ShapeRule(
        "annotation_note",
        "A note about a physical annotation or mis-marking: 'Op de rug abusievelijk gemerkt "
        "als 1688'.",
        pattern=re.compile(r"(?i)gemerkt"),
    ),
    ShapeRule(
        "language_note",
        "States the language of the document: 'Grotendeels in het Frans'.",
        pattern=re.compile(r"(?i)in het (Frans|Latijn|Duits|Engels)"),
    ),
)


def classify_shape(
    text: str, tags: Tuple[str, ...], rules: Sequence[ShapeRule] = NA_SHAPE_RULES
) -> Tuple[str, str]:
    """``(shape, shape_rule)`` for one element's text and descendant-tag set.

    One generic loop over ``rules`` (first match wins); nothing here names a shape. Returns
    ``("unclassified", "unclassified")`` when no rule matches.
    """
    for rule in rules:
        if rule.requires_empty:
            if len(text) == 0:
                return rule.name, rule.name
        elif rule.requires_tag is not None:
            if rule.requires_tag in tags:
                return rule.name, rule.name
        elif rule.pattern is not None and rule.pattern.search(text):
            return rule.name, rule.name
    return "unclassified", "unclassified"


# -- reference and date patterns -----------------------------------------------------------------

_MONTH = (
    r"(?:jan(?:uari)?|feb(?:ruari)?|mrt|maart|apr(?:il)?|mei|jun(?:i)?|jul(?:i)?|aug(?:ustus)?"
    r"|sep(?:t(?:ember)?)?|okt(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?"
)

# A single inventory-number token: digits, an optional letter suffix, optionally repeated with
# dots ("9412A", "12561.13.2"). A trailing sentence period is never consumed because the dot
# group requires a following digit.
_INV_NUMBER_TOKEN = r"\d+[A-Za-z]*(?:\.\d+[A-Za-z]*)*"

# A token may itself carry a bare dash-range suffix ("8480-8482"). That whole token is handed to
# ``InventoryNumber`` as one piece and deliberately NOT split into two references: it fails to
# parse and surfaces as ``kind="unparsed"``.
_INV_NUMBER_OR_RANGE = rf"{_INV_NUMBER_TOKEN}(?:-{_INV_NUMBER_TOKEN})?"

_INV_SEPARATOR = r"(?:\s*,\s*|\s+en\s+|\s+and\s+)"

_NA_ARCHIVE_CODE = r"\d\.\d{2}(?:\.\d{2,3}){1,3}"  # 1.04.02, 1.11.01.01, 2.21.281.08
_NA_CUE = r"(?:inv(?:entaris)?\.?\s*nrs?\.?|inventarisnummer)"
_NA_ARCHIVE_PREFIX = rf"(?:\b(?P<archive>{_NA_ARCHIVE_CODE})\.?\s*,?\s*)?"
_NA_NEXT_IS_NEW_REF = rf"(?!{_NA_ARCHIVE_CODE}\.?\s*,?\s*{_NA_CUE})"


@dataclass(frozen=True)
class ReferencePatterns:
    """The regexes that extract references and raw date strings from a note's text."""

    inventory: Pattern[str]  # must have group "numbers"; may have group "archive"
    inventory_token: Pattern[str]
    folio: Pattern[str]
    date_range: Pattern[str]
    date_full: Pattern[str]
    external_tags: Tuple[str, ...] = ("extref", "bibref", "title")


NA_REFERENCE_PATTERNS = ReferencePatterns(
    # "zie inv. nr. 9919.", "inv.nrs. 8622 en 8623", "inventarisnummer 1519", optionally with an
    # archive code in front of the cue ("Zie Aanwinsten, 1.11.01.01, inv.nr. 687"). The number
    # list stops before an archive code that starts a new reference.
    inventory=re.compile(
        rf"{_NA_ARCHIVE_PREFIX}{_NA_CUE}\s*"
        rf"(?P<numbers>{_INV_NUMBER_OR_RANGE}"
        rf"(?:{_INV_SEPARATOR}{_NA_NEXT_IS_NEW_REF}{_INV_NUMBER_OR_RANGE})*)",
        re.IGNORECASE,
    ),
    inventory_token=re.compile(_INV_NUMBER_OR_RANGE),
    # "f. 214-240", "f. 103-132, 205" — kept as one raw string; never expanded.
    folio=re.compile(r"\bf\.\s*\d+(?:\s*[,-]\s*\d+)*"),
    # "1692 jan. 6 - mrt. 3" — year, then an abbreviated month/day range (VOC list-item style).
    date_range=re.compile(
        rf"\b\d{{3,4}}\s+{_MONTH}\s*\d{{1,2}}\s*-\s*{_MONTH}\s*\d{{1,2}}\b", re.IGNORECASE
    ),
    # "19 december 1742" — day, full/abbreviated month, year.
    date_full=re.compile(rf"\b\d{{1,2}}\s+{_MONTH}\s+\d{{3,4}}\b", re.IGNORECASE),
    external_tags=("extref", "bibref", "title"),
)


@dataclass(frozen=True)
class ScopeReference:
    """One pointer found in a scope note's text or markup.

    ``raw`` always holds the matched substring, so a wrong parse is diagnosable without
    re-running the regex. ``target_archive`` is the archive code the pointer names, or ``None``
    for the current archive.
    """

    kind: str  # "inventory" | "folio" | "external" | "unparsed"
    raw: str
    inventory_number: Optional[InventoryNumber]
    folio: Optional[str]
    target_archive: Optional[str] = None


def _inventory_references(
    text: str, archive_id: str, patterns: ReferencePatterns
) -> List[ScopeReference]:
    """Every inventory pointer in ``text``, one per number.

    "N en M" and "N, M" are two references; "N-M" is handed to ``InventoryNumber`` as a single
    token and comes back as ``kind="unparsed"`` (this module never expands a range). A match
    that names an archive other than ``archive_id`` puts that code on all its references.
    """
    references: List[ScopeReference] = []
    for match in patterns.inventory.finditer(text):
        target = match.groupdict().get("archive")
        if target is not None and target == archive_id:
            target = None
        for token_match in patterns.inventory_token.finditer(match.group("numbers")):
            token = token_match.group()
            try:
                inv = InventoryNumber(token)
                references.append(ScopeReference("inventory", token, inv, None, target))
            except InvalidInventoryNumberError:
                references.append(ScopeReference("unparsed", token, None, None, target))
    return references


def _folio_references(text: str, patterns: ReferencePatterns) -> List[ScopeReference]:
    return [
        ScopeReference("folio", match.group(), None, match.group())
        for match in patterns.folio.finditer(text)
    ]


def _external_references(
    element: etree._Element, patterns: ReferencePatterns
) -> List[ScopeReference]:
    """Descendants named in ``patterns.external_tags`` that carry an ``href``.

    Never ``<ref>``: VOC's ``<ref linktype="simple">`` elements carry no ``href``/``target``;
    the pointer is in the text, extracted by ``_inventory_references`` instead.
    """
    references: List[ScopeReference] = []
    for descendant in element.iterdescendants():
        if _local_tag(descendant.tag) not in patterns.external_tags:
            continue
        href = descendant.attrib.get("href")
        if href:
            references.append(ScopeReference("external", href, None, None))
    return references


def _extract_references(
    element: etree._Element, text: str, archive_id: str, patterns: ReferencePatterns
) -> Tuple[ScopeReference, ...]:
    return tuple(
        _inventory_references(text, archive_id, patterns)
        + _folio_references(text, patterns)
        + _external_references(element, patterns)
    )


# -- date strings --------------------------------------------------------------------------------


def _extract_date_strings(
    element: etree._Element, text: str, patterns: ReferencePatterns
) -> Tuple[str, ...]:
    """Raw date-like substrings only: range matches, full-date matches, then the text and
    ``@normal`` of every ``<date>`` descendant; de-duplicated keeping the first occurrence.
    """
    found: List[str] = []
    for match in patterns.date_range.finditer(text):
        found.append(match.group())
    for match in patterns.date_full.finditer(text):
        found.append(match.group())
    for descendant in element.iterdescendants():
        if _local_tag(descendant.tag) != "date":
            continue
        date_text = element_text(descendant)
        if date_text:
            found.append(date_text)
        normal = descendant.attrib.get("normal")
        if normal:
            found.append(normal)

    seen = set()
    deduped: List[str] = []
    for value in found:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return tuple(deduped)


# -- segmentation --------------------------------------------------------------------------------


def _segments(element: etree._Element) -> Tuple[str, ...]:
    """One segment per contiguous prose run (sibling ``<p>``s are joined), plus one segment per
    ``<item>`` inside a ``<list>``. ``<list>`` is a hard break at any depth.

    A ``<lb/>`` contributes a space, except directly before punctuation (``Eems<lb/>. Met`` is
    ``Eems. Met``), as in ``element_text``.

    Falls back to the element's whole text as a single segment when there is no ``<p>``/``<list>``
    structure, and to no segments for an empty element.
    """
    segments: List[str] = []
    buffer_parts: List[str] = []

    def _flush() -> None:
        # Same rule as ``element_text``: a line break before punctuation contributes no space.
        raw = _NO_SPACE_BEFORE.sub("", "".join(buffer_parts)).replace(_LB_MARK, " ")
        joined = " ".join(raw.split())
        if joined:
            segments.append(joined)
        buffer_parts.clear()

    def _walk(el: etree._Element) -> None:
        if el.text:
            buffer_parts.append(el.text)
        for child in el:
            tag = _local_tag(child.tag)
            if tag is None:
                pass
            elif tag == "list":
                _flush()
                for item in _direct_children(child):
                    if _local_tag(item.tag) == "item":
                        item_text = element_text(item)
                        if item_text:
                            segments.append(item_text)
            elif tag == LB_TAG:
                buffer_parts.append(_LB_MARK)
            else:
                _walk(child)
            if child.tail:
                buffer_parts.append(child.tail)

    _walk(element)
    _flush()

    if segments:
        return tuple(segments)
    whole = element_text(element)
    return (whole,) if whole else ()


# -- ScopeNote and the walk ----------------------------------------------------------------------


@dataclass(frozen=True)
class ScopeNote:
    """One ``<scopecontent>``/``<odd>`` element, parsed. Holds no lxml element, so it is
    picklable and serialisable.
    """

    node_id: str
    archive_id: str
    source_tag: str  # "scopecontent" | "odd"
    element_index: int  # 0-based within that node's elements of source_tag
    shape: str  # a taxonomy name, or "unclassified"
    shape_rule: str  # which rule assigned the shape (provenance)
    text: str  # whitespace-normalised full text; ALWAYS populated
    segments: Tuple[str, ...]  # <p> / <item> segmentation, in document order
    references: Tuple[ScopeReference, ...]
    date_strings: Tuple[str, ...]  # RAW substrings only
    char_len: int
    citation: str


def parse_scope_notes(
    component: EADComponent,
    archive_id: str,
    *,
    source_path: Optional[Path] = None,
    tags: Tuple[str, ...] = ("scopecontent",),
    shape_rules: Sequence[ShapeRule] = NA_SHAPE_RULES,
    patterns: ReferencePatterns = NA_REFERENCE_PATTERNS,
) -> List[ScopeNote]:
    """Every ``tags`` element retained on ``component.other_children``, parsed.

    Never drops an element: every element present gets exactly one ``ScopeNote``, including the
    empty one and anything ``classify_shape`` cannot name.
    """
    node_id = component.node_id(archive_id)
    citation = component.cite(source_path)
    notes: List[ScopeNote] = []
    for source_tag in tags:
        for element_index, element in enumerate(component.other_children.get(source_tag, [])):
            text = element_text(element)
            shape, shape_rule = classify_shape(text, descendant_tags(element), shape_rules)
            notes.append(
                ScopeNote(
                    node_id=node_id,
                    archive_id=archive_id,
                    source_tag=source_tag,
                    element_index=element_index,
                    shape=shape,
                    shape_rule=shape_rule,
                    text=text,
                    segments=_segments(element),
                    references=_extract_references(element, text, archive_id, patterns),
                    date_strings=_extract_date_strings(element, text, patterns),
                    char_len=len(text),
                    citation=citation,
                )
            )
    return notes


def iter_scope_notes(
    finding_aid: EADFindingAid,
    archive_id: Optional[str] = None,
    *,
    tags: Tuple[str, ...] = ("scopecontent",),
    shape_rules: Sequence[ShapeRule] = NA_SHAPE_RULES,
    patterns: ReferencePatterns = NA_REFERENCE_PATTERNS,
) -> Iterator[ScopeNote]:
    """``ScopeNote``s for every component of ``finding_aid``, in pre-order."""
    archive_id = finding_aid.default_archive_id if archive_id is None else archive_id
    for component in finding_aid.walk():
        yield from parse_scope_notes(
            component,
            archive_id,
            source_path=finding_aid.path,
            tags=tags,
            shape_rules=shape_rules,
            patterns=patterns,
        )


def scope_notes_frame(
    finding_aid: EADFindingAid,
    archive_id: Optional[str] = None,
    *,
    tags: Tuple[str, ...] = ("scopecontent",),
    shape_rules: Sequence[ShapeRule] = NA_SHAPE_RULES,
    patterns: ReferencePatterns = NA_REFERENCE_PATTERNS,
) -> pd.DataFrame:
    """``ScopeNote``s for every component of ``finding_aid`` as a DataFrame, one row per note."""
    return pd.DataFrame(
        [
            vars(n)
            for n in iter_scope_notes(
                finding_aid, archive_id, tags=tags, shape_rules=shape_rules, patterns=patterns
            )
        ]
    )
