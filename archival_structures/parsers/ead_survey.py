"""Surveying EAD structure, and measuring how much of it a parser actually reaches.

Step 2 of the EAD parser rewrite (see `docs/ead_parser/plan.md`) needs an instrument built
*before* the parser changes, so that later steps can be held to a number rather than an
impression. This module is that instrument, and it is deliberately independent of
{mod}`archival_structures.parsers.ead_parser`: it must be able to say the parser is wrong.

Two things it does.

**Survey** (`survey_ead`) reports what an EAD document contains: its namespace and component
spelling, every element tag and attribute name under `<dsc>`, every `@level`/`@otherlevel` value,
every root-to-leaf ancestry path of component levels, the children and `<unitid>` shapes seen
inside a `<did>`, and how digitisation is linked. `aggregate_surveys` rolls several up into a
corpus-wide vocabulary, and `unique_to_one_repository` names the constructs that occur in exactly
one repository -- the over-fitting risks.

**Coverage** (`coverage`) reports what a parser reached: for each `@level`, how many of the
components in the source turn up in the parser's output. This generalises the `recall` figure of
Step 0's report past `file` level, which is the only way failures like the 7,035 unreached `item`
components become visible. A parser is adapted to it by a callable returning `(level, unitid)`
pairs; `legacy_parser_components` is that adapter for the current parser.

Unlike the rest of the package this module parses with `lxml` rather than
`xml.etree.ElementTree`, for two reasons: `lxml` elements carry `.sourceline`, so a survey can
cite `file:line` for the construct it is reporting (which is what makes an error analysis
checkable), and its iterative parsing handles the 40--106 MB finding aids in the corpus more
comfortably. `lxml` is already a hard dependency of the project.

EAD spellings this module accepts, via
{func}`~archival_structures.parsers.ead_harvest.is_component` and
{func}`~archival_structures.parsers.ead_harvest.local_name`: components as recursive `<c>` or as
depth-numbered `<c01>`..`<c12>`, with or without the EAD 2002 default namespace. Step 1 found all
four combinations in the corpus.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from lxml import etree

from archival_structures.parsers.ead_harvest import (
    AGENCY_REPOSITORIES,
    component_depth_name,
    find_ead_root,
    is_component,
    local_name,
    normalise_level,
)

logger = logging.getLogger(__name__)

#: Element names that carry a link to a digitised object, per Step 1's finding that there are
#: three mechanisms in the corpus rather than one.
DIGITAL_LINK_TAGS = {"dao", "daogrp", "assets", "asset"}


@dataclass
class ComponentRef:
    """One component element, enough to name it in a report and match it against parser output."""

    level: str
    #: index path within `<dsc>`, e.g. `(0, 2, 5)` -- a stable identity independent of `<unitid>`
    index_path: Tuple[int, ...]
    #: ancestry of `@level` values from the `<dsc>` child down to and including this component
    level_path: Tuple[str, ...]
    unitids: Tuple[str, ...]
    title: Optional[str]
    sourceline: Optional[int]
    tag: str

    @property
    def depth(self) -> int:
        return len(self.index_path)

    @property
    def primary_unitid(self) -> Optional[str]:
        return self.unitids[0] if self.unitids else None

    @property
    def display_unitid(self) -> Optional[str]:
        """The `<unitid>` most likely to be the one a person would quote.

        Components routinely carry several: a GUID, a handle URL, a series code and the
        inventory number. For a report, the short non-URL, non-GUID one is the useful one --
        Het Utrechts Archief lists its GUID first, so `primary_unitid` alone would print 32 hex
        characters where `1` was meant.
        """
        candidates = [u for u in self.unitids
                      if "://" not in u and not (len(u) == 32 and _is_hex(u))]
        return min(candidates, key=len) if candidates else self.primary_unitid

    def cite(self, path: Path) -> str:
        """A `file:line` citation for this component."""
        return f"{path.name}:{self.sourceline}" if self.sourceline else path.name


@dataclass
class EADSurvey:
    """What one EAD document contains, structurally."""

    path: Path
    agency_code: Optional[str]
    eadid: Optional[str]
    repository: str
    namespace: Optional[str]
    component_style: str
    components: List[ComponentRef] = field(default_factory=list)
    element_tags: Counter = field(default_factory=Counter)
    attributes_by_tag: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    levels: Counter = field(default_factory=Counter)
    ancestry_paths: Counter = field(default_factory=Counter)
    did_children: Counter = field(default_factory=Counter)
    unitid_shapes: Counter = field(default_factory=Counter)
    digital_links: Counter = field(default_factory=Counter)
    dsc_child_levels: Counter = field(default_factory=Counter)
    #: name of the envelope element the `<ead>` was found inside, if it was wrapped
    wrapper: Optional[str] = None
    parse_error: Optional[str] = None

    @property
    def max_depth(self) -> int:
        return max((c.depth for c in self.components), default=0)

    @property
    def n_components(self) -> int:
        return len(self.components)

    def components_at(self, level: str) -> List[ComponentRef]:
        return [c for c in self.components if c.level == level]

    def leaf_paths(self) -> Counter:
        """Ancestry paths of components that have no component children."""
        parents = {c.index_path[:-1] for c in self.components if c.index_path}
        return Counter(c.level_path for c in self.components if c.index_path not in parents)


def _is_hex(text: str) -> bool:
    return all(character in "0123456789abcdefABCDEF" for character in text)


def _did_of(element):
    for child in element:
        if isinstance(child.tag, str) and local_name(child.tag) == "did":
            return child
    return None


def _unitid_shape(unitid) -> str:
    """A compact description of one `<unitid>`'s identifying attributes."""
    parts = []
    for name in ("type", "label"):
        if unitid.get(name):
            parts.append(f"{name}={unitid.get(name)}")
    if unitid.get("identifier"):
        parts.append("@identifier")
    if unitid.get("audience"):
        parts.append(f"audience={unitid.get('audience')}")
    return ", ".join(parts) if parts else "(bare)"


def _text_of(element) -> Optional[str]:
    if element is None:
        return None
    text = " ".join("".join(element.itertext()).split())
    return text or None


def survey_ead(path: Path, repository: Optional[str] = None) -> EADSurvey:
    """Survey the structure of the EAD document at `path`.

    Never raises on malformed input: a parse failure is recorded in `EADSurvey.parse_error` and
    the survey comes back otherwise empty, so one bad file cannot stop a corpus run.
    """
    path = Path(path)
    agency_code = eadid = namespace = None
    try:
        tree = etree.parse(str(path))
    except (etree.XMLSyntaxError, OSError) as error:
        logger.error("%s: %s", path.name, error)
        return EADSurvey(path=path, agency_code=None, eadid=None,
                         repository=repository or "unknown", namespace=None,
                         component_style="unknown", parse_error=str(error))

    document = tree.getroot()
    # not always the document root: see find_ead_root on the opendata.archieven.nl envelope
    root = find_ead_root(document)
    wrapped = root is not None and root is not document
    if root is None:
        root = document
    if root.tag.startswith("{"):
        namespace = root.tag[1:].partition("}")[0]
    for element in root.iter():
        if isinstance(element.tag, str) and local_name(element.tag) == "eadid":
            agency_code = element.get("mainagencycode")
            eadid = (element.text or "").strip() or None
            break
    if agency_code is None and path.name.startswith("NL-"):
        agency_code = path.name.split("_", 1)[0]

    survey = EADSurvey(
        path=path,
        agency_code=agency_code,
        eadid=eadid,
        repository=repository or AGENCY_REPOSITORIES.get(agency_code, agency_code or "unknown"),
        namespace=namespace,
        component_style="recursive",
        wrapper=local_name(document.tag) if wrapped else None,
    )

    dsc = next((e for e in root.iter()
                if isinstance(e.tag, str) and local_name(e.tag) == "dsc"), None)
    if dsc is None:
        survey.parse_error = "no <dsc> element"
        return survey

    numbered = 0

    def walk(parent, index_path: Tuple[int, ...], level_path: Tuple[str, ...]) -> None:
        nonlocal numbered
        component_index = 0
        for child in parent:
            if not isinstance(child.tag, str):  # comments, processing instructions
                continue
            if not is_component(child.tag):
                _record_non_component(survey, child)
                continue
            if component_depth_name(child.tag) is not None:
                numbered += 1
            level = normalise_level(child.get("level"), child.get("otherlevel"))
            child_index_path = index_path + (component_index,)
            child_level_path = level_path + (level,)
            component_index += 1

            survey.levels[level] += 1
            survey.element_tags[local_name(child.tag)] += 1
            for name in child.keys():
                survey.attributes_by_tag[local_name(child.tag)][name] += 1
            if len(index_path) == 0:
                survey.dsc_child_levels[level] += 1

            did = _did_of(child)
            unitids: List[str] = []
            title = None
            if did is not None:
                for did_child in did:
                    if not isinstance(did_child.tag, str):
                        continue
                    name = local_name(did_child.tag)
                    survey.did_children[name] += 1
                    if name == "unitid":
                        survey.unitid_shapes[_unitid_shape(did_child)] += 1
                        if did_child.text and did_child.text.strip():
                            unitids.append(did_child.text.strip())
                    elif name == "unittitle":
                        title = _text_of(did_child)

            survey.components.append(ComponentRef(
                level=level, index_path=child_index_path, level_path=child_level_path,
                unitids=tuple(unitids), title=title,
                sourceline=child.sourceline, tag=local_name(child.tag),
            ))
            survey.ancestry_paths[child_level_path] += 1
            walk(child, child_index_path, child_level_path)

    walk(dsc, (), ())
    survey.component_style = "numbered" if numbered else "recursive"
    return survey


def _record_non_component(survey: EADSurvey, element) -> None:
    """Record a non-component element (and its descendants) in the tag/attribute vocabulary."""
    for node in element.iter():
        if not isinstance(node.tag, str):
            continue
        name = local_name(node.tag)
        survey.element_tags[name] += 1
        for attribute in node.keys():
            survey.attributes_by_tag[name][attribute] += 1
        # counted here and nowhere else, so a <dao> inside a <did> is not counted twice
        if name in DIGITAL_LINK_TAGS:
            survey.digital_links[f"<{name}>"] += 1
        elif name == "unitid" and node.get("type") == "handle":
            survey.digital_links['<unitid type="handle">'] += 1


def survey_corpus(corpus_dir: Path, pattern: str = "*.xml") -> List[EADSurvey]:
    """Survey every EAD document in `corpus_dir`."""
    surveys = []
    for path in sorted(Path(corpus_dir).glob(pattern)):
        logger.info("surveying %s", path.name)
        surveys.append(survey_ead(path))
    return surveys


# ---------------------------------------------------------------------------------------------
# Corpus-level aggregation
# ---------------------------------------------------------------------------------------------

@dataclass
class CorpusSurvey:
    """The structural vocabulary of a whole corpus, and where each part of it comes from."""

    surveys: List[EADSurvey]

    @property
    def repositories(self) -> List[str]:
        return sorted({s.repository for s in self.surveys})

    def totals(self, attribute: str) -> Counter:
        """Sum one `Counter` attribute across every survey."""
        total: Counter = Counter()
        for survey in self.surveys:
            total.update(getattr(survey, attribute))
        return total

    def repositories_by(self, attribute: str) -> Dict[str, List[str]]:
        """For each key of a `Counter` attribute, the repositories it occurs in."""
        found: Dict[str, set] = defaultdict(set)
        for survey in self.surveys:
            for key in getattr(survey, attribute):
                found[str(key)].add(survey.repository)
        return {key: sorted(repos) for key, repos in found.items()}

    def unique_to_one_repository(self, attribute: str) -> Dict[str, str]:
        """Keys of a `Counter` attribute that occur in exactly one repository."""
        return {key: repos[0]
                for key, repos in sorted(self.repositories_by(attribute).items())
                if len(repos) == 1}

    def profiles(self) -> Counter:
        """(namespace declared?, component spelling, wrapped?) combinations in the corpus."""
        return Counter(
            (("namespaced" if s.namespace else "no namespace"),
             s.component_style,
             f"<{s.wrapper}> envelope" if s.wrapper else "bare <ead>")
            for s in self.surveys if s.parse_error is None
        )


# ---------------------------------------------------------------------------------------------
# Coverage: how much of the source a parser reaches
# ---------------------------------------------------------------------------------------------

#: A parser adapter: given an EAD path, yield the `(level, unitid)` pairs it produced.
ParserAdapter = Callable[[Path], Iterable[Tuple[str, Optional[str]]]]


@dataclass
class LevelCoverage:
    level: str
    n_source: int
    n_parsed: int
    n_matched: int
    missing_examples: List[ComponentRef] = field(default_factory=list)

    @property
    def recall(self) -> float:
        return self.n_matched / self.n_source if self.n_source else 1.0


@dataclass
class CoverageReport:
    """Per-level coverage of one EAD document by one parser."""

    survey: EADSurvey
    by_level: Dict[str, LevelCoverage]
    error: Optional[str] = None

    @property
    def n_source(self) -> int:
        return sum(c.n_source for c in self.by_level.values())

    @property
    def n_matched(self) -> int:
        return sum(c.n_matched for c in self.by_level.values())

    @property
    def recall(self) -> float:
        return self.n_matched / self.n_source if self.n_source else 1.0


def coverage(
    survey: EADSurvey,
    adapter: ParserAdapter,
    max_examples: int = 3,
) -> CoverageReport:
    """Measure what fraction of `survey`'s components a parser reaches, per `@level`.

    Components are matched on `(level, unitid)`. A component whose `<unitid>` list is empty
    cannot be matched by identity, so it counts towards `n_source` and can only be matched
    positionally -- which is the honest answer: a parser that emits an untitled, unidentified
    component gives the caller no way to tell which one it was.

    A parser that raises is not an error here: the exception is recorded and every level reports
    zero coverage, because that is what the caller gets.
    """
    parsed: Counter = Counter()
    error: Optional[str] = None
    try:
        for level, unitid in adapter(survey.path):
            parsed[(level, unitid)] += 1
    except Exception as exception:  # noqa: BLE001 -- measuring a parser includes its crashes
        error = f"{type(exception).__name__}: {exception}"

    by_level: Dict[str, LevelCoverage] = {}
    for level in sorted(survey.levels):
        components = survey.components_at(level)
        available = Counter(parsed)
        matched = 0
        missing: List[ComponentRef] = []
        for component in components:
            # a component carries several <unitid>s (guid, handle, series code, inventory
            # number); a parser may legitimately report any of them, so match on any
            keys = [(level, unitid) for unitid in component.unitids] or [(level, None)]
            key = next((k for k in keys if available.get(k, 0) > 0), None)
            if key is not None:
                available[key] -= 1
                matched += 1
            elif len(missing) < max_examples:
                missing.append(component)
        by_level[level] = LevelCoverage(
            level=level,
            n_source=len(components),
            n_parsed=sum(count for (parsed_level, _), count in parsed.items()
                         if parsed_level == level),
            n_matched=matched,
            missing_examples=missing,
        )
    return CoverageReport(survey=survey, by_level=by_level, error=error)


def legacy_parser_components(path: Path) -> List[Tuple[str, Optional[str]]]:
    """`ParserAdapter` for the current {mod}`~archival_structures.parsers.ead_parser`.

    `get_inventory_info` returns one row per inventory-number-shaped file, so the only level it
    can report is `file`. Its `print` progress output is suppressed.
    """
    import contextlib
    import io

    from archival_structures.parsers import ead_parser

    with contextlib.redirect_stdout(io.StringIO()):
        frame = ead_parser.get_inventory_info(ead_file=str(path))
    return [("file", None if value is None else str(value))
            for value in frame["inventory_num"]]


# ---------------------------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------------------------

def format_survey(survey: EADSurvey, max_rows: int = 12) -> str:
    """A short human-readable summary of one survey."""
    if survey.parse_error:
        return f"{survey.path.name}: could not be surveyed -- {survey.parse_error}"
    lines = [
        f"{survey.path.name}  ({survey.repository})",
        f"  eadid={survey.eadid}  namespace={survey.namespace or 'none'}  "
        f"components={survey.component_style}  max_depth={survey.max_depth}"
        + (f"  wrapped in <{survey.wrapper}>" if survey.wrapper else ""),
        f"  levels: {dict(survey.levels.most_common())}",
        f"  <dsc> children: {dict(survey.dsc_child_levels.most_common())}",
    ]
    if survey.digital_links:
        lines.append(f"  digital links: {dict(survey.digital_links.most_common())}")
    lines.append("  ancestry paths to leaf components:")
    for path, count in survey.leaf_paths().most_common(max_rows):
        lines.append(f"    {' > '.join(path)} = {count}")
    return "\n".join(lines)


def format_coverage(report: CoverageReport, path_for_citation: Optional[Path] = None) -> str:
    """A human-readable per-level coverage table, with citations for missing components."""
    cite_path = path_for_citation or report.survey.path
    lines = [f"{report.survey.path.name}  ({report.survey.repository})"]
    if report.error:
        lines.append(f"  parser raised {report.error}")
    lines.append(f"  {'level':<22} {'source':>8} {'parsed':>8} {'matched':>8} {'recall':>8}")
    for level, level_coverage in sorted(report.by_level.items()):
        lines.append(f"  {level:<22} {level_coverage.n_source:>8} {level_coverage.n_parsed:>8} "
                     f"{level_coverage.n_matched:>8} {level_coverage.recall:>7.1%}")
        for component in level_coverage.missing_examples:
            unitid = component.display_unitid or "(no unitid)"
            title = (component.title or "(no title)")[:54]
            lines.append(f"      missing {unitid:<14} {component.cite(cite_path):<32} {title}")
    lines.append(f"  {'TOTAL':<22} {report.n_source:>8} {'':>8} {report.n_matched:>8} "
                 f"{report.recall:>7.1%}")
    return "\n".join(lines)


def format_corpus_vocabulary(corpus: CorpusSurvey, min_count: int = 1) -> str:
    """The corpus-wide structural vocabulary, and what is unique to one repository."""
    lines = [f"{len(corpus.surveys)} finding aid(s), {len(corpus.repositories)} repositories", ""]
    lines.append("profiles (namespace x component spelling):")
    for profile, count in corpus.profiles().most_common():
        lines.append(f"  {profile[0]:<14} {profile[1]:<10} {profile[2]:<20} {count}")
    lines.append("")
    lines.append("component levels:")
    repos_by_level = corpus.repositories_by("levels")
    for level, count in corpus.totals("levels").most_common():
        lines.append(f"  {level:<24} {count:>8}   in {len(repos_by_level[level])} repositor"
                     f"{'y' if len(repos_by_level[level]) == 1 else 'ies'}")
    lines.append("")
    lines.append("<did> children:")
    for name, count in corpus.totals("did_children").most_common():
        if count >= min_count:
            lines.append(f"  {name:<24} {count:>8}")
    lines.append("")
    lines.append("<unitid> shapes:")
    for shape, count in corpus.totals("unitid_shapes").most_common():
        lines.append(f"  {shape:<44} {count:>8}")
    lines.append("")
    lines.append("digitisation links:")
    for name, count in corpus.totals("digital_links").most_common():
        lines.append(f"  {name:<28} {count:>8}")
    lines.append("")
    for attribute, label in [("levels", "component levels"),
                             ("did_children", "<did> children"),
                             ("unitid_shapes", "<unitid> shapes")]:
        unique = corpus.unique_to_one_repository(attribute)
        lines.append(f"{label} unique to one repository:")
        for key, repository in unique.items():
            lines.append(f"  {key:<44} {repository}")
        lines.append("")
    return "\n".join(lines)
