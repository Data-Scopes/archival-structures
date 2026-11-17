"""A multi-level, extensible tag vocabulary for annotating scans, pages, and regions.

Every tag is a string of the form `namespace:type(:subtype)?(#number)?` (the grammar SegmOnto
<https://segmonto.github.io> uses for its own `Type(:subtype)?(#N)?` zone/line vocabulary,
reused here unchanged). The same grammar applies at every level of granularity -- a whole scan,
one page (verso/recto) within a two-page opening, or one region/line within a page -- the only
difference between levels is what a tag list is attached to, not the tags themselves.

There are five namespaces:

- `carrier:` -- what the *scan* physically is (page/opening/foldout/cut). Scan-level only;
  informed by standard codicological terminology (cf. Muzerelle's *Vocabulaire codicologique*,
  online as Codicologia <http://codicologia.irht.cnrs.fr/>).
- `generic:` -- universal content type, comparable across any archive or inventory number
  (running text, table, list, form, marginalia, closing, ...). Valid at any level. Mostly
  SegmOnto's own Zone typology (renamed to drop the `Zone` suffix, since here the same types
  apply at scan/page/region granularity, not just to PAGE-XML-style regions), plus `list` and
  `form` -- two types SegmOnto doesn't have, since it comes out of manuscript/print layout
  analysis rather than administrative/record-keeping documents. `form`'s `label`/`value`
  subtypes borrow the question/answer (fixed-prompt vs. variable-response) distinction from
  form-understanding research (e.g. the FUNSD task).
- `position:` -- where a tagged scan/page/region falls in a document (start/continuation/end).
  Informed by diplomatics' classical tripartite document structure (protocol/text/eschatocol,
  i.e. opening formula / body / closing formula) -- used here at the structural level only, not
  its full medieval-charter terminology (*intitulatio*, *corroboratio*, ...), which doesn't fit
  early-modern administrative material.
- `placement:` -- where an element sits on the page, independent of what it is (`main`, `margin`,
  `top`, `bottom`, `interlinear`). A marginal note is `placement:margin` plus its type, e.g.
  `generic:heading:summary, placement:margin`. `generic:marginalia` is kept for old annotations and
  means "placement margin, type not given" (see `legacy_placement`).
- `doctype:` -- archive- or inventory-specific document/block type (e.g. `doctype:deed`,
  `doctype:deed:loan_table`, `doctype:minutes`). Deliberately uncontrolled: unlike the other
  four namespaces, there is no suggested type list, because this is exactly the layer meant to
  be discovered iteratively per archive rather than standardised up front. A scan's directory
  path already identifies its archive/inventory number, so a `doctype:` tag doesn't need to
  repeat that -- `doctype:deed` means "a deed, in whichever archive this annotation lives under".

`CARRIER_TYPES`/`GENERIC_TYPES`/`POSITION_TYPES` are suggested-but-not-enforced type vocabularies
(with their own suggested subtypes), mirroring SegmOnto's "controlled type, open subtype list"
design -- including its `CustomZone` escape hatch, ported here as `generic:custom`, for anything
that doesn't fit yet. `is_known_tag` checks against these lists for documentation/autocomplete
purposes; it does not gate what can be stored -- nothing in this module or in
`archival_structures.datasets.annotations` rejects an unrecognised tag, since the whole point is
that the vocabulary grows as new content types are encountered.
"""
import re
from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional

CARRIER = 'carrier'
GENERIC = 'generic'
POSITION = 'position'
PLACEMENT = 'placement'
DOCTYPE = 'doctype'
NAMESPACES = (CARRIER, GENERIC, POSITION, PLACEMENT, DOCTYPE)

CARRIER_TYPES: Dict[str, FrozenSet[str]] = {
    'page': frozenset(),
    'opening': frozenset(),
    'foldout': frozenset(),
    'cut': frozenset(),
}

GENERIC_TYPES: Dict[str, Optional[FrozenSet[str]]] = {
    'running_text': frozenset(),
    'table': frozenset(),
    'list': frozenset(),
    'form': frozenset({'label', 'value'}),
    'marginalia': frozenset({'note', 'commentary', 'correction', 'variants'}),
    'closing': frozenset({'formula', 'dating', 'signature'}),
    'heading': frozenset({'title', 'date', 'summary'}),
    'title_page': frozenset(),
    'cover': frozenset(),
    'running_title': frozenset({'date'}),
    'seal': frozenset(),
    'stamp': frozenset({'postal', 'curatorial'}),
    'damage': frozenset({'corrosion', 'hole', 'mold', 'peeled', 'soaked'}),
    'graphic': frozenset({'illustration', 'ornamentation', 'figure'}),
    'numbering': frozenset({'page', 'folio', 'item', 'other'}),
    'quire_marks': frozenset({'signature', 'catchwords'}),
    'digitization_artefact': frozenset({'ruler', 'test_card'}),
    'drop_capital': frozenset({'historiated', 'floriate', 'flourish', 'voided', 'parted'}),
    'music': frozenset(),
    'salutation': frozenset(),
    'endorsement': frozenset(),
    'reference': frozenset(),
    'note': frozenset(),
    'addition': frozenset(),
    'noise': frozenset(),
    'custom': None,  # any subtype, per SegmOnto's CustomZone
}

POSITION_TYPES: Dict[str, FrozenSet[str]] = {
    'start': frozenset(),
    'continuation': frozenset(),
    'end': frozenset(),
}

PLACEMENT_TYPES: Dict[str, FrozenSet[str]] = {
    'main': frozenset(),
    'margin': frozenset(),
    'top': frozenset(),
    'bottom': frozenset(),
    'interlinear': frozenset(),
}

# generic types kept for old annotations that imply a placement (type not given)
LEGACY_PLACEMENT_TYPES: Dict[str, str] = {'marginalia': 'margin'}

_NAMESPACE_TYPES: Dict[str, Dict[str, Optional[FrozenSet[str]]]] = {
    CARRIER: CARRIER_TYPES,
    GENERIC: GENERIC_TYPES,
    POSITION: POSITION_TYPES,
    PLACEMENT: PLACEMENT_TYPES,
}

_TAG_PATTERN = re.compile(
    r"^(?P<namespace>[a-z][a-z0-9_]*):(?P<type>[a-z][a-z0-9_]*)"
    r"(?::(?P<subtype>[a-z][a-z0-9_]*))?(?:#(?P<number>\d+))?$"
)


@dataclass(frozen=True)
class Tag:
    """A parsed `namespace:type(:subtype)?(#number)?` tag."""

    namespace: str
    type: str
    subtype: Optional[str] = None
    number: Optional[int] = None

    def __str__(self) -> str:
        tag = f"{self.namespace}:{self.type}"
        if self.subtype:
            tag += f":{self.subtype}"
        if self.number is not None:
            tag += f"#{self.number}"
        return tag

    @staticmethod
    def parse(tag: str) -> "Tag":
        """Parse a tag string into its parts. Raises `ValueError` if it doesn't match
        `namespace:type(:subtype)?(#number)?`."""
        match = _TAG_PATTERN.match(tag)
        if not match:
            raise ValueError(f"not a valid tag: {tag!r} (expected 'namespace:type[:subtype][#N]')")
        number = match.group('number')
        return Tag(namespace=match.group('namespace'), type=match.group('type'),
                   subtype=match.group('subtype'), number=int(number) if number else None)


def make_tag(namespace: str, type: str, subtype: Optional[str] = None,
            number: Optional[int] = None) -> str:
    """Build a tag string from its parts (the inverse of `Tag.parse`)."""
    return str(Tag(namespace=namespace, type=type, subtype=subtype, number=number))


def is_known_tag(tag: str) -> bool:
    """Whether `tag` is well-formed and, for the four controlled namespaces (`carrier:`/
    `generic:`/`position:`/`placement:`), uses one of their suggested types. `doctype:` tags are always
    considered known if well-formed -- that namespace is deliberately uncontrolled (see the
    module docstring). This is for documentation/autocomplete purposes; nothing in this
    package rejects a tag for failing this check."""
    try:
        parsed = Tag.parse(tag)
    except ValueError:
        return False
    if parsed.namespace == DOCTYPE:
        return True
    types = _NAMESPACE_TYPES.get(parsed.namespace)
    return types is not None and parsed.type in types


def legacy_placement(tag: str) -> Optional[str]:
    """The placement a legacy `generic:` tag implies (`generic:marginalia[:x]` -> `'margin'`), else
    `None` (also for malformed tags and other namespaces)."""
    try:
        parsed = Tag.parse(tag)
    except ValueError:
        return None
    if parsed.namespace != GENERIC:
        return None
    return LEGACY_PLACEMENT_TYPES.get(parsed.type)
