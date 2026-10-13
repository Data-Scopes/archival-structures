"""Bounded-memory parsing of large EAD documents (Step 7 of `docs/ead_parser/plan.md`).

{func}`archival_structures.parsers.ead_tree.parse_ead` builds the whole document as one `lxml`
DOM tree before walking it. Step 3 found this genuinely fine for the largest file in this
project's own corpus (`NL-AsdSAA_5075`, 106 MB: 0.8s, 615 MB peak RSS) -- but the user's own
practice includes EAD files larger than anything in this corpus, and "large EAD files are not
uncommon" outside it. `parse_ead_streaming` builds the identical `EADFindingAid` structure using
`lxml.etree.iterparse`, clearing each component's underlying subtree the moment its
`EADComponent` has been built from it, so peak memory is bounded by the largest single *root*
component's subtree in flight plus the (small) set of still-open ancestors, not by the whole
document.

## Why this bounds memory, and what it does not bound

`iterparse`'s `end` events fire in document order, depth-first: a component's entire subtree,
however large, is fully materialised in memory by the time its own `end` event fires (that is
what "the element is closed" means). So clearing happens *after* the subtree that produced one
`EADComponent` has already been converted and no longer needs to exist as raw XML -- this frees
the memory a finished branch used before the next sibling branch is even parsed. What it does
**not** do is bound memory to less than one root component's subtree: a single `<series>`
containing a million `<file>`s still needs all million in memory at once while that series is
open, because `<series>`'s own `end` event (and therefore its clearing) cannot fire until every
descendant's has. Most large EAD documents in this corpus are large because they have *many*
root-level components, not one gigantic one (Step 1/2's structural surveys), which is exactly
the shape this streaming path helps with.

## The subtlety that made the first version of this module wrong

The standard `iterparse` memory-bounding idiom -- clear a finished element, then delete its
already-finished preceding siblings from its parent -- is normally used when nothing but the
target elements themselves matters. It does not compose safely with also needing a *sibling* of
those target elements: a component's own `<did>`, or its `<odd>`/`<scopecontent>`/etc., sit
*before* that component's nested `<c>` children in document order, so the moment the first
nested child finishes and the sibling-sweep runs, `<did>` would be deleted -- before the parent
component's own `end` event ever gets a chance to read it. An earlier draft of this module did
exactly that, and would have silently produced empty `did_extra`/`other_children` for every
component with nested component children (which is most non-leaf components in the corpus).

Fixed by never waiting for a component's own `end` event to read its `<did>`/other children at
all: each is captured (deep-copied) on **its own** `end` event, the moment *it* closes, which
happens before any of its later siblings can be swept. `<dsc>`'s own non-component children
(e.g. `<head>`) are captured the same way. By the time a component (or `<dsc>`) is itself ready
to be assembled, everything it needs has already been pulled out of the live tree into
independent copies, and the live element is free to be cleared or swept without losing anything.

## `strict`

A malformed document raises immediately in `strict=True` (the default). `strict=False` collects
a `ParseReport` instead: an XML syntax error partway through the document, or a component found
missing its `<did>`, is recorded rather than raised, and whatever tree could be built before the
problem is still returned.
"""

from __future__ import annotations

import copy
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from lxml import etree

from archival_structures.parsers.ead_harvest import (
    agency_code_from_filename,
    component_depth_name,
    extract_ead_identity,
    is_component,
    local_name,
    normalise_level,
)
from archival_structures.parsers.ead_tree import EADComponent, EADFindingAid, _parse_did_element

logger = logging.getLogger(__name__)

#: sentinel key for the per-parent dicts below: components/children whose parent is `<dsc>`
#: itself (the finding aid's roots), which is never used as a real `id(element)` value
_ROOTS_KEY = 0


@dataclass
class ParseDiagnostic:
    """One thing recorded during a `strict=False` parse, instead of being raised."""

    kind: str
    message: str
    sourceline: Optional[int] = None

    def __str__(self) -> str:
        where = f" (line {self.sourceline})" if self.sourceline else ""
        return f"{self.kind}{where}: {self.message}"


@dataclass
class ParseReport:
    """Diagnostics collected during a `strict=False` streaming parse. Never raised; a caller
    decides what an empty vs. non-empty report means for their use of the resulting tree."""

    diagnostics: List[ParseDiagnostic] = field(default_factory=list)

    def add(self, kind: str, message: str, sourceline: Optional[int] = None) -> None:
        self.diagnostics.append(ParseDiagnostic(kind, message, sourceline))

    @property
    def ok(self) -> bool:
        return not self.diagnostics

    def __len__(self) -> int:
        return len(self.diagnostics)

    def __iter__(self):
        return iter(self.diagnostics)


def _empty_finding_aid(path: Path, message: str, agency_code=None, eadid=None, title=None,
                       namespace=None, wrapper=None) -> EADFindingAid:
    return EADFindingAid(path=path, agency_code=agency_code, eadid=eadid, title=title,
                         namespace=namespace, component_style="unknown", wrapper=wrapper,
                         parse_error=message)


def parse_ead_streaming(path, strict: bool = True) -> Tuple[EADFindingAid, ParseReport]:
    """Parse the EAD document at `path` with bounded memory, via `iterparse` + clearing.

    Returns `(finding_aid, report)`. `finding_aid` is structurally identical to what
    `ead_tree.parse_ead` would build from the same bytes -- same components, same fields, same
    ancestry -- built by a different route for memory reasons only (see
    `tests/test_parsers_ead_stream.py`'s equivalence tests). `report` is empty on a clean parse;
    see the module docstring's `strict` section for what ends up in it otherwise.
    """
    path = Path(path)
    report = ParseReport()

    #: id(parent element) -> already-built children, in document order. `_ROOTS_KEY` collects
    #: <dsc>'s own direct component children (the finding aid's `roots`).
    children_by_parent: Dict[int, List[EADComponent]] = defaultdict(list)
    #: id(element) -> (index_path, level_path), assigned on `start` from the parent's already-
    #: known context (parents start before their children), so it is available when the child
    #: itself needs it on `end`.
    context_by_element: Dict[int, Tuple[Tuple[int, ...], Tuple[str, ...]]] = {_ROOTS_KEY: ((), ())}
    #: id(parent element) -> next child index to assign (component children only)
    next_index: Dict[int, int] = defaultdict(int)
    #: id(parent element) -> that parent's <did> element, captured on the <did>'s own `end`
    #: event -- see the module docstring for why this cannot wait for the parent's `end`.
    pending_did: Dict[int, Optional[etree._Element]] = {}
    #: id(parent element) -> {tag: [elements]}, captured the same way and for the same reason;
    #: also used directly for <dsc>'s own non-component children (key `_ROOTS_KEY`).
    pending_other: Dict[int, Dict[str, List[etree._Element]]] = defaultdict(lambda: defaultdict(list))

    dsc_element = None
    document_element = None  # the outermost element, whatever it is (may not be <ead>)
    ead_element = None  # the actual <ead>, once its start event fires
    numbered_count = 0

    def parent_key(element) -> int:
        # identity against `dsc_element`, not a tag-name check: EAD allows a <dsc> nested
        # inside a component (Step 3's finding E1 -- a free-text container in every case
        # observed, never a real hierarchy), and a tag-name check cannot tell that one apart
        # from the genuine top-level <dsc>, which would misfile the nested one's own children
        # into the finding aid's actual dsc_extra. Safe to compare by identity here because
        # `dsc_element` is assigned on the *first* <dsc>-named element's `start` event (below),
        # and `start` fires top-down -- the outer <dsc> is necessarily seen, and recorded,
        # before any of its descendants (including a nested <dsc>) fire their own start or end.
        parent = element.getparent()
        if parent is None or parent is dsc_element:
            return _ROOTS_KEY
        return id(parent)

    def sweep(element) -> None:
        """Clear a finished element and drop its already-finished preceding siblings, freeing
        memory. Safe here because anything worth keeping from `element` or an earlier sibling
        was already captured on that element's own `end` event, before this ever runs."""
        element.clear()
        while element.getprevious() is not None:
            parent = element.getparent()
            if parent is None:
                break
            del parent[0]

    try:
        context = etree.iterparse(str(path), events=("start", "end"), huge_tree=True)
        for event, element in context:
            if not isinstance(element.tag, str):
                continue
            name = local_name(element.tag)

            if event == "start":
                if document_element is None:
                    document_element = element
                if ead_element is None and name == "ead":
                    ead_element = element
                if dsc_element is None and name == "dsc":
                    dsc_element = element
                if is_component(element.tag):
                    parent_id = parent_key(element)
                    parent_index_path, parent_level_path = context_by_element.get(
                        parent_id, ((), ()))
                    index = next_index[parent_id]
                    next_index[parent_id] = index + 1
                    raw_level = element.get("level")
                    otherlevel = element.get("otherlevel") if raw_level == "otherlevel" else None
                    level = normalise_level(raw_level, otherlevel)
                    context_by_element[id(element)] = (
                        parent_index_path + (index,), parent_level_path + (level,),
                    )
                continue

            # event == "end" from here on
            if element is dsc_element:
                continue  # handled after the loop, once everything under it has been captured

            if is_component(element.tag):
                if component_depth_name(element.tag) is not None:
                    numbered_count += 1

                did_element = pending_did.pop(id(element), None)
                unitids, title, unittitle_element, did_extra = _parse_did_element(
                    did_element, copy_retained=False)  # already an independent copy, or None
                missing_did = did_element is None
                if missing_did:
                    report.add("missing_did", f"<{name}> has no <did>", element.sourceline)
                other_children = {k: v for k, v in pending_other.pop(id(element), {}).items()}

                raw_level = element.get("level")
                otherlevel = element.get("otherlevel") if raw_level == "otherlevel" else None
                index_path, level_path = context_by_element.pop(id(element), (
                    (), (normalise_level(raw_level, otherlevel),)))

                children = children_by_parent.pop(id(element), [])
                component = EADComponent(
                    tag=name, raw_level=raw_level, otherlevel=otherlevel,
                    unitids=unitids, title=title, unittitle_element=unittitle_element,
                    did_extra=did_extra, other_children=other_children, children=children,
                    index_path=index_path, level_path=level_path,
                    sourceline=element.sourceline, attributes=dict(element.attrib),
                    missing_did=missing_did,
                )
                for child in children:
                    child.parent = component

                children_by_parent[parent_key(element)].append(component)
                # this element will never again be looked up as a *parent* key (all of its own
                # children's `start` events have already fired, by definition, before its own
                # `end` did) -- drop it now, before `sweep()` frees the element and CPython is
                # free to hand this exact id() to a brand-new, unrelated element later in the
                # document. Left in place, a coincidental id() reuse would make that unrelated
                # element inherit this one's leftover child count instead of starting at 0 --
                # the bug an earlier draft of this module had, found via the corpus-wide
                # equivalence check against `ead_tree.parse_ead` (`step-07` report).
                next_index.pop(id(element), None)
                sweep(element)
                continue

            # not a component, not <dsc>: either a <did> or some other descriptive child of a
            # component/<dsc> -- captured (deep-copied) now, on its own `end`, before it could
            # ever be swept away as a preceding sibling of a later nested component (see the
            # module docstring). Anything not a direct child of a component or <dsc> (i.e. a
            # descendant of a <did>/<odd>/etc. already being captured whole) is ignored here;
            # its content is already inside the deep copy its own direct-child ancestor takes.
            parent = element.getparent()
            if parent is None or not (parent is dsc_element or is_component(parent.tag)):
                continue
            pid = parent_key(element)
            if name == "did":
                if pid not in pending_did:  # first <did> wins, matching _find_did's semantics
                    pending_did[pid] = copy.deepcopy(element)
            else:
                pending_other[pid][name].append(copy.deepcopy(element))

    except etree.XMLSyntaxError as error:
        if strict:
            raise
        report.add("xml_syntax_error", str(error))
        return _empty_finding_aid(path, str(error)), report
    except OSError as error:
        if strict:
            raise
        report.add("os_error", str(error))
        return _empty_finding_aid(path, str(error)), report

    if ead_element is None:
        message = "no <ead> element found"
        wrapper = local_name(document_element.tag) if document_element is not None else None
        if strict:
            raise ValueError(message)
        report.add("no_ead_element", message)
        return _empty_finding_aid(path, message, wrapper=wrapper), report

    wrapped = document_element is not None and document_element is not ead_element
    namespace = ead_element.tag[1:].partition("}")[0] if ead_element.tag.startswith("{") else None
    agency_code, eadid, title = extract_ead_identity(ead_element)
    agency_code = agency_code_from_filename(agency_code, path.name)

    if dsc_element is None:
        message = "no <dsc> element"
        wrapper = local_name(document_element.tag) if wrapped else None
        if strict:
            raise ValueError(message)
        report.add("no_dsc_element", message)
        return _empty_finding_aid(path, message, agency_code, eadid, title, namespace, wrapper), report

    dsc_extra = dict(pending_other.pop(_ROOTS_KEY, {}))

    finding_aid = EADFindingAid(
        path=path, agency_code=agency_code, eadid=eadid, title=title, namespace=namespace,
        component_style="numbered" if numbered_count else "recursive",
        wrapper=local_name(document_element.tag) if wrapped else None,
        roots=children_by_parent.get(_ROOTS_KEY, []), dsc_extra=dsc_extra,
    )
    return finding_aid, report
