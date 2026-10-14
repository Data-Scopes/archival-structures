"""The lossless EAD component tree (Step 3 of `docs/ead_parser/plan.md`, decision D2).

`archival_structures.parsers.ead_parser` walks `<dsc>` with one `parse_<level>` function per
expected level, each enumerating the child tags it accepts and raising or silently dropping
anything else. Step 0 and Step 2 measured what that costs: 21.5% of components in the Step 2
corpus (44,951 of 208,926), because the walk assumes components are rooted under `<series>`
(false for `NL-AsdSAA_395`, `NL-HaNA_1.05.06`), assumes `@level` is one of a known few values
(false for 22+ free-text `otherlevel` values and 2,593 components with no `@level` at all --
Step 2, decision D8), assumes bare `<c>` in no namespace (false for 27 of 40 finding aids --
Step 1), and never recurs below `file` level (losing every `item`/`subfile`/`otherlevel`
component nested there).

This module replaces the level-specific walk with one that has no level-specific logic at all:
`build_component` treats a `<c>` (or `<c01>`..`<c12>`) as a component whatever its `@level` is or
isn't, and recurses into every component child regardless of what the parent's level was. A
component's identity in the tree is its position (`index_path`), not its `<unitid>` -- so an
unlabelled, untitled component (2,593 of them -- Step 2) is still a real node, not a gap.

Nothing under a `<did>` is discarded either: `<unitid>` and `<unittitle>` are parsed now (needed
to identify a component and title it), and every other `<did>` child -- `<unitdate>`,
`<physdesc>`, `<dao>`, `<assets>`, ... -- is retained verbatim, uninterpreted, in
`EADComponent.did_extra`, a `{tag: [elements]}` map. Interpreting those is Step 4/5's job; Step
3's job is that nothing is silently lost while they wait. The same applies to a component's
non-`<did>` children (`<odd>`, `<scopecontent>`, `<controlaccess>`, ...), kept in
`EADComponent.other_children`.

Acceptance criterion (measured against `docs/ead_parser/reports/step-02-conformance.md`'s
harness): 100% component coverage at every `@level`, corpus-wide, up from the legacy parser's
21.5%. `tree_parser_components` is the `ead_survey.coverage` adapter that proves it.
"""

from __future__ import annotations

import copy
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from lxml import etree

from archival_structures.parsers.ead_harvest import (
    agency_code_from_filename,
    component_depth_name,
    extract_ead_identity,
    find_ead_root,
    is_component,
    local_name,
    normalise_level,
)
from archival_structures.parsers.ead_text import element_text

logger = logging.getLogger(__name__)


def make_node_id(archive_id: str, index_path: Tuple[int, ...]) -> str:
    """`f"{archive_id}:{'.'.join(map(str, index_path))}"`; `archive_id` must not contain ':'."""
    if ":" in archive_id:
        raise ValueError(f"archive_id must not contain ':': {archive_id!r}")
    return f"{archive_id}:{'.'.join(map(str, index_path))}"


@dataclass
class UnitID:
    """One `<unitid>` element, attributes and all.

    Step 1/2 found no shared convention for which `<unitid>` marks the inventory number: a
    component may carry several -- a GUID, a handle URL, a series code, a bare number -- and
    which one identifies it is repository-specific (Step 4's job). All are kept, in document
    order, rather than the legacy parser's single guess.
    """

    value: Optional[str]
    type: Optional[str] = None
    label: Optional[str] = None
    identifier: Optional[str] = None
    audience: Optional[str] = None
    attributes: Dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:  # pragma: no cover -- cosmetic
        bits = [repr(self.value)]
        for name in ("type", "label", "identifier"):
            value = getattr(self, name)
            if value:
                bits.append(f"{name}={value!r}")
        return f"UnitID({', '.join(bits)})"


@dataclass(eq=False)
class EADComponent:
    """One EAD component (`<c>` or `<c01>`..`<c12>`), whatever its `@level` is or isn't.

    Three different notions of "the same component", deliberately kept apart:

    - **identity** -- the same Python object. `==` and `hash()` are identity-based
      (`eq=False`), so components work as set members and dict keys. Two parses of the same
      file give components that are *not* `==`.
    - **node id** -- the same *position* in one finding aid: `node_id(archive_id)` is
      `"<archive_id>:<index_path joined by '.'>"`, e.g. `"1.01.02:0.3.12"`. Stable across
      re-parses of the same input; not comparable across different exports of an archive.
    - **persistent id** -- the same *archival unit* across exports: `persistent_id`, the value
      of `<unitid type="urn:uuid">` where the source provides one, else `None`.

    Deliberately has no level-specific fields or methods: nothing here branches on `level`
    being `"series"` versus `"file"` versus an unrecognised `otherlevel` value, because Step 2
    found 27 distinct level values in a 13-repository corpus and no reason to expect that list
    is complete. A component's *kind* is data (`level`), never control flow.
    """

    tag: str
    raw_level: Optional[str]
    otherlevel: Optional[str]
    unitids: List[UnitID]
    title: Optional[str]
    #: the raw `<unittitle>` element, kept alongside its extracted `title` text so Step 4 can
    #: recover an embedded `<unitdate>` (`<unittitle><unitdate>1600-1601</unitdate></unittitle>`
    #: -- Step 0 found the legacy parser's handling of exactly this buggy). `None` if the
    #: component had no `<unittitle>`.
    unittitle_element: Optional[etree._Element]
    #: `<did>` children other than `<unitid>`/`<unittitle>` (`<unitdate>`, `<physdesc>`,
    #: `<dao>`, Memorix `<assets>`, ...), kept verbatim by local tag name for Step 4/5 to parse.
    did_extra: Dict[str, List[etree._Element]]
    #: non-`<did>` children of the component itself (`<odd>`, `<scopecontent>`,
    #: `<controlaccess>`, ...), kept verbatim the same way.
    other_children: Dict[str, List[etree._Element]]
    children: List["EADComponent"]
    index_path: Tuple[int, ...]
    level_path: Tuple[str, ...]
    sourceline: Optional[int]
    attributes: Dict[str, str]
    parent: Optional["EADComponent"] = field(default=None, repr=False, compare=False)
    #: True if this component's `<did>` was missing entirely (EAD 2002 requires one; a handful
    #: of malformed real-world documents omit it) -- reported rather than silently defaulted.
    missing_did: bool = False

    @property
    def level(self) -> str:
        """This component's level, normalised exactly as `ead_survey` reports it.

        `"(none)"` for an absent `@level` (2,593 in the Step 2 corpus), `"otherlevel:<value>"`
        for a free-text `otherlevel`, the bare `@level` value otherwise. Comparable 1:1 against
        `ead_survey.ComponentRef.level`, which is what makes `tree_parser_components` a valid
        `ead_survey.coverage` adapter.
        """
        return normalise_level(self.raw_level, self.otherlevel)

    @property
    def depth(self) -> int:
        return len(self.index_path)

    @property
    def persistent_id(self) -> Optional[str]:
        """The value of the first non-empty `<unitid type="urn:uuid">`, else None."""
        for unitid in self.unitids:
            if unitid.value and (unitid.type or "").strip().lower() == "urn:uuid":
                return unitid.value.strip()
        return None

    def node_id(self, archive_id: str) -> str:
        """Position-based id `"<archive_id>:<i>.<j>..."` (see the class docstring)."""
        return make_node_id(archive_id, self.index_path)

    def parent_id(self, archive_id: str) -> Optional[str]:
        """`node_id` of the parent, or None for a root component."""
        if len(self.index_path) <= 1:
            return None
        return make_node_id(archive_id, self.index_path[:-1])

    def ancestor_ids(self, archive_id: str) -> List[str]:
        """`node_id`s of the ancestors, root first, not including this component."""
        return [make_node_id(archive_id, self.index_path[:n])
                for n in range(1, len(self.index_path))]

    def unitid_values(self) -> List[str]:
        """Non-empty `<unitid>` text values, in document order."""
        return [u.value for u in self.unitids if u.value]

    def ancestors(self) -> List["EADComponent"]:
        """This component's ancestors, root first, not including itself."""
        chain = []
        node = self.parent
        while node is not None:
            chain.append(node)
            node = node.parent
        return list(reversed(chain))

    def walk(self) -> Iterator["EADComponent"]:
        """This component and every descendant, pre-order."""
        yield self
        for child in self.children:
            yield from child.walk()

    def cite(self, path: Optional[Path] = None) -> str:
        """A `file:line` citation, matching `ead_survey.ComponentRef.cite`."""
        name = path.name if path else "?"
        return f"{name}:{self.sourceline}" if self.sourceline else name

    def __repr__(self) -> str:  # pragma: no cover -- cosmetic; avoids dumping the whole subtree
        unitid = self.unitid_values()
        shown = unitid[0] if unitid else None
        return (f"EADComponent(tag={self.tag!r}, level={self.level!r}, unitid={shown!r}, "
                f"title={(self.title or '')[:40]!r}, children={len(self.children)})")


@dataclass
class EADFindingAid:
    """One parsed EAD document: identity, profile, and its forest of root components.

    `roots` is a list, not a single tree, because Step 1 found finding aids (`NL-AsdSAA_395`,
    `NL-HaNA_1.05.06`) with `<file>`-level components directly under `<dsc>`, siblings of
    `<series>` rather than descendants of one -- the construct
    {func}`~archival_structures.parsers.ead_parser.get_series`'s single-series-loop silently
    dropped entirely (Step 0, finding E1).
    """

    path: Path
    agency_code: Optional[str]
    eadid: Optional[str]
    title: Optional[str]
    namespace: Optional[str]
    component_style: str
    wrapper: Optional[str]
    roots: List[EADComponent] = field(default_factory=list)
    #: non-component children directly under `<dsc>` (e.g. `<head>` on a `type="combined"` dsc)
    dsc_extra: Dict[str, List[etree._Element]] = field(default_factory=dict)
    parse_error: Optional[str] = None

    def walk(self) -> Iterator[EADComponent]:
        """Every component in the document, pre-order, roots first."""
        for root in self.roots:
            yield from root.walk()

    @property
    def default_archive_id(self) -> str:
        """`eadid` (e.g. "1.01.02"), falling back to the file stem; ':' replaced by '_'."""
        return (self.eadid or self.path.stem).replace(":", "_")

    def iter_node_ids(self, archive_id: Optional[str] = None) -> Iterator[Tuple[str, EADComponent]]:
        """`(node_id, component)` for every component, pre-order."""
        archive_id = self.default_archive_id if archive_id is None else archive_id
        for component in self.walk():
            yield component.node_id(archive_id), component

    def components_at(self, level: str) -> List[EADComponent]:
        return [c for c in self.walk() if c.level == level]

    @property
    def n_components(self) -> int:
        return sum(1 for _ in self.walk())

    @property
    def max_depth(self) -> int:
        return max((c.depth for c in self.walk()), default=0)


def _find_did(element) -> Optional[etree._Element]:
    for child in element:
        if isinstance(child.tag, str) and local_name(child.tag) == "did":
            return child
    return None


def _text_of(element) -> Optional[str]:
    """An element's full text content, whitespace-normalised (handles e.g. an embedded
    `<unitdate>` inside `<unittitle>` -- Step 0 found the legacy parser's bespoke handling of
    this buggy; a raw walk sidesteps that class of bug rather than repeating it; `<lb/>` becomes a space,
    see `ead_text.element_text`).
    """
    if element is None:
        return None
    return element_text(element) or None


def _build_unitid(element) -> UnitID:
    text = (element.text or "").strip() or None
    return UnitID(
        value=text,
        type=element.get("type"),
        label=element.get("label"),
        identifier=element.get("identifier"),
        audience=element.get("audience"),
        attributes=dict(element.attrib),
    )


def _element_level(element) -> str:
    """`normalise_level` applied directly to an lxml element's `@level`/`@otherlevel`."""
    raw_level = element.get("level")
    otherlevel = element.get("otherlevel") if raw_level == "otherlevel" else None
    return normalise_level(raw_level, otherlevel)


def _parse_did_element(did, copy_retained: bool = False):
    """The fields carried by one already-located `<did>` element (or `None`):
    `(unitids, title, unittitle_element, did_extra)`.

    Split out from `_extract_did_fields` so the streaming path (`ead_stream`) can call it on a
    `<did>` it captured (and, if needed, already deep-copied) on that `<did>`'s own `end` event
    -- before the component-level "drop already-finished siblings" memory sweep (Step 7) could
    ever remove it as a preceding sibling of a later nested component. Whatever is copy-safety
    the caller already arranged for `did` itself, `copy_retained` here only governs `did`'s own
    children (`did_extra`/`unittitle_element`); pass `False` when `did` is already an
    independent, detached copy, so its children don't get copied a second time.
    """
    unitids: List[UnitID] = []
    title: Optional[str] = None
    did_extra: Dict[str, List[etree._Element]] = defaultdict(list)
    unittitle_element = None
    if did is not None:
        for did_child in did:
            if not isinstance(did_child.tag, str):
                continue
            name = local_name(did_child.tag)
            if name == "unitid":
                unitids.append(_build_unitid(did_child))
            elif name == "unittitle":
                title = _text_of(did_child)
                unittitle_element = copy.deepcopy(did_child) if copy_retained else did_child
            else:
                kept = copy.deepcopy(did_child) if copy_retained else did_child
                did_extra[name].append(kept)
    return unitids, title, unittitle_element, dict(did_extra)


def _extract_did_fields(element, copy_retained: bool = False):
    """The `<did>`-derived fields of a component element: `(unitids, title,
    unittitle_element, did_extra, missing_did)`.

    Used by the in-memory path (`build_component`), where the whole DOM stays alive for the
    lifetime of the returned tree, so a direct element reference (`copy_retained=False`) is
    cheap and correct. The streaming path does not call this directly -- see
    `_parse_did_element` and `ead_stream`'s module docstring for why a component's `<did>` has
    to be captured on its *own* `end` event there, not read back off the component element
    after the fact.
    """
    did = _find_did(element)
    unitids, title, unittitle_element, did_extra = _parse_did_element(
        did, copy_retained=copy_retained)
    return unitids, title, unittitle_element, did_extra, (did is None)


def _extract_other_children(element, copy_retained: bool = False):
    """Non-`<did>`, non-component children of a component element (`<odd>`, `<scopecontent>`,
    ...), by local tag name. See `_extract_did_fields` for why `copy_retained` exists.
    """
    other_children: Dict[str, List[etree._Element]] = defaultdict(list)
    for child in element:
        if not isinstance(child.tag, str):
            continue
        name = local_name(child.tag)
        if name == "did" or is_component(child.tag):
            continue
        kept = copy.deepcopy(child) if copy_retained else child
        other_children[name].append(kept)
    return dict(other_children)


def build_component(element, index_path: Tuple[int, ...],
                     level_path: Tuple[str, ...]) -> EADComponent:
    """Build one `EADComponent` from an EAD `<c>`/`<c01>`..`<c12>` element, recursively.

    No level-specific logic: whatever `@level` says (or doesn't), the same code walks `<did>`
    for `<unitid>`/`<unittitle>` (everything else in `<did>` retained in `did_extra`), walks the
    remaining children for nested components (via `is_component`, which accepts both component
    spellings) and retains everything else in `other_children`.
    """
    tag = local_name(element.tag)
    raw_level = element.get("level")
    otherlevel = element.get("otherlevel") if raw_level == "otherlevel" else None

    unitids, title, unittitle_element, did_extra, missing_did = _extract_did_fields(element)
    other_children = _extract_other_children(element)

    children: List[EADComponent] = []
    child_index = 0
    for child in element:
        if isinstance(child.tag, str) and is_component(child.tag):
            children.append(build_component(
                child, index_path + (child_index,), level_path + (_element_level(child),),
            ))
            child_index += 1

    component = EADComponent(
        tag=tag, raw_level=raw_level, otherlevel=otherlevel, unitids=unitids, title=title,
        unittitle_element=unittitle_element,
        did_extra=did_extra, other_children=other_children, children=children,
        index_path=index_path, level_path=level_path, sourceline=element.sourceline,
        attributes=dict(element.attrib), missing_did=missing_did,
    )
    for child in children:
        child.parent = component
    return component



def _finding_aid_from_document(document, path: Path) -> EADFindingAid:
    """Build an `EADFindingAid` from an already-parsed document root, whatever its origin
    (a file, per `parse_ead`, or an in-memory string/response body, per `parse_ead_string`).
    `path` is used only for naming (Amsterdam's filename-based agency-code fallback, and
    citations) -- it does not have to point at a real file.
    """
    # not always the document root: opendata.archieven.nl wraps <ead> in <opendata> whose
    # namespace differs from the <ead> inside it (Step 2, finding E3 / decision D9)
    root = find_ead_root(document)
    wrapped = root is not None and root is not document
    if root is None:
        return EADFindingAid(path=path, agency_code=None, eadid=None, title=None,
                             namespace=None, component_style="unknown", wrapper=None,
                             parse_error="no <ead> element found")

    namespace = root.tag[1:].partition("}")[0] if root.tag.startswith("{") else None
    agency_code, eadid, title = extract_ead_identity(root)
    agency_code = agency_code_from_filename(agency_code, path.name)

    dsc = next((e for e in root.iter() if isinstance(e.tag, str) and local_name(e.tag) == "dsc"),
              None)
    if dsc is None:
        return EADFindingAid(path=path, agency_code=agency_code, eadid=eadid, title=title,
                             namespace=namespace, component_style="unknown",
                             wrapper=local_name(document.tag) if wrapped else None,
                             parse_error="no <dsc> element")

    roots: List[EADComponent] = []
    dsc_extra: Dict[str, List[etree._Element]] = defaultdict(list)
    numbered = 0
    index = 0
    for child in dsc:
        if not isinstance(child.tag, str):
            continue
        if is_component(child.tag):
            if component_depth_name(child.tag) is not None:
                numbered += 1
            roots.append(build_component(child, (index,), (_element_level(child),)))
            index += 1
        else:
            dsc_extra[local_name(child.tag)].append(child)

    # component style is a document-wide property; checking only dsc's direct children would
    # miss a document (none observed in the corpus, but cheap to rule out) whose roots are
    # recursive <c> while descendants further down use <c01>..<c12>.
    if not numbered:
        numbered = sum(1 for root_component in roots for component in root_component.walk()
                       if component_depth_name(component.tag) is not None)

    return EADFindingAid(
        path=path, agency_code=agency_code, eadid=eadid, title=title, namespace=namespace,
        component_style="numbered" if numbered else "recursive",
        wrapper=local_name(document.tag) if wrapped else None,
        roots=roots, dsc_extra=dict(dsc_extra),
    )


def parse_ead(path: Path) -> EADFindingAid:
    """Parse the EAD document at `path` into a lossless `EADFindingAid`.

    Never raises on malformed input or an unsupported profile: a failure is recorded in
    `EADFindingAid.parse_error` and the finding aid comes back with no roots, so one bad
    document cannot stop a corpus run -- the same contract as `ead_survey.survey_ead`.
    """
    path = Path(path)
    try:
        tree = etree.parse(str(path))
    except (etree.XMLSyntaxError, OSError) as error:
        logger.error("%s: %s", path.name, error)
        return EADFindingAid(path=path, agency_code=None, eadid=None, title=None,
                             namespace=None, component_style="unknown", wrapper=None,
                             parse_error=str(error))
    return _finding_aid_from_document(tree.getroot(), path)


def parse_ead_string(xml, source_name: str = "<string>") -> EADFindingAid:
    """Parse an EAD document already in memory (`bytes` or `str`) rather than on disk --
    e.g. an OAI-PMH `GetRecord` response body, fetched and held in memory by a caller such as
    `download_thumbnails.py` rather than written to a file first (Step 8's migration off
    `archival_structures.parsers.read.EADReader`, which parsed the same kind of in-memory OAI
    response via BeautifulSoup).

    `source_name` stands in for `parse_ead`'s `path` -- used only for naming (Amsterdam's
    filename-based agency-code fallback does not apply here, since there is no real filename)
    and citations, and does not need to point at a real file. Same non-raising contract as
    `parse_ead`.
    """
    try:
        document = etree.fromstring(
            xml.encode("utf-8") if isinstance(xml, str) else xml)
    except etree.XMLSyntaxError as error:
        logger.error("%s: %s", source_name, error)
        return EADFindingAid(path=Path(source_name), agency_code=None, eadid=None, title=None,
                             namespace=None, component_style="unknown", wrapper=None,
                             parse_error=str(error))
    return _finding_aid_from_document(document, Path(source_name))


# ---------------------------------------------------------------------------------------------
# ead_survey.coverage adapter
# ---------------------------------------------------------------------------------------------

def tree_parser_components(path: Path):
    """`ead_survey.ParserAdapter` for this module: every `(level, unitid)` pair the tree holds.

    One pair per non-empty `<unitid>` value of every component (a component with several
    `<unitid>`s -- the norm, not the exception, per Step 2 -- can legitimately be matched by
    any of them, mirroring how `ead_survey.coverage` matches its source side), and one
    `(level, None)` pair for a component with no usable `<unitid>` at all, so that a genuinely
    unlabelled component (2,593 in the Step 2 corpus) still counts as reached rather than as a
    silent gap on either side of the comparison.
    """
    finding_aid = parse_ead(path)
    for component in finding_aid.walk():
        values = component.unitid_values()
        if values:
            for value in values:
                yield (component.level, value)
        else:
            yield (component.level, None)


def extra_census(finding_aid: EADFindingAid) -> Tuple[Counter, Counter]:
    """`(did_extra_counts, other_children_counts)`: how many times each retained-but-unparsed
    tag occurs across the whole document, for the "what Step 4/5 still has to interpret" report.
    """
    did_extra: Counter = Counter()
    other: Counter = Counter()
    for component in finding_aid.walk():
        for name, elements in component.did_extra.items():
            did_extra[name] += len(elements)
        for name, elements in component.other_children.items():
            other[name] += len(elements)
    return did_extra, other
