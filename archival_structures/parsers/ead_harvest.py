"""Building the EAD test corpus: harvesting where that is allowed, ingesting where it is not.

Step 1 of the EAD parser rewrite (see `docs/ead_parser/plan.md`) needs finding aids from more
than the two exporters the corpus started with, because with two it is impossible to tell a
general EAD pattern from one institution's house style. Publishers fall into two groups.

**Harvestable** -- a documented machine interface that serves a self-identifying client:

`service.archief.nl` (Nationaal Archief)
    OAI-PMH with an `oai_ead` metadata prefix, already used by `download_thumbnails.py`.
    `GetRecord` returns the EAD inside an OAI envelope.

`archief.amsterdam` (Stadsarchief Amsterdam)
    A per-archive EAD export at a predictable path. No API, but no gatekeeping either.

**Not harvestable** -- `opendata.archieven.nl`, the De Ree open-data portal that aggregates EAD
from a few dozen Dutch regional and municipal archives. Its metadata is CC0/CC-BY, but the site
is fronted by a filter that answers **403 to every request whose `User-Agent` is not a
browser's** (tested: absent, `curl/*`, `Python-urllib/*`, and two honest bot strings naming this
project; only a Chrome-like string gets through), and its `robots.txt` disallows `/download/` --
the path the EAD files are actually served from. Both the dataset listing and the sitemap are
covered, so there is no automated route at all, and this module does not pretend otherwise: it
does not fetch from that host. Instead `download_guide` prints what a person needs to collect
the files in their own browser -- which is the client the portal is built for -- and
`ingest_directory` takes whatever they downloaded and files it into the corpus.

Harvest requests are serialised with a delay (see `HarvestSession`), and finding aids already on
disk are skipped unless overwriting is asked for, so a re-run is cheap.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

USER_AGENT = (
    "archival-structures/0.3 EAD corpus harvester "
    "(+https://github.com/Data-Scopes/archival-structures)"
)

OPENDATA_BASE = "https://opendata.archieven.nl"
NA_OAI_URL = "https://service.archief.nl/gaf/oai/!open_oai.OAIHandler"
SAA_EAD_URL = "https://archief.amsterdam/archives/xml/{archive}.ead.xml"

#: `adtid` values for organisations on opendata.archieven.nl that publish EAD. Needed because a
#: download URL is `/download/<adtid>/EAD/<guid>`: filtering a listing by organisation fixes the
#: `adtid`, so the GUIDs visible on that page are enough to build every download link on it.
#: The portal's own organisation listing is itself capped at 15 rows, so this is a curated
#: starting set, not a complete registry.
OPENDATA_ORGANISATIONS: Dict[int, str] = {
    5: "Groninger Archieven",
    26: "Erfgoedcentrum Achterhoek en Liemers",
    28: "Archief Eemland",
    37: "Gelders Archief",
    39: "Het Utrechts Archief",
    45: "Collectie Overijssel, locatie Deventer",
    59: "Haags Gemeentearchief",
    137: "Gemeentearchief Zaanstad",
    141: "Collectie Overijssel, locatie Zwolle",
    235: "Brabants Historisch Informatie Centrum (BHIC)",
    236: "Noord-Hollands Archief",
}

#: EAD 2002's namespace, which exporters disagree about declaring: the Nationaal Archief and
#: Stadsarchief Amsterdam exports declare no default namespace, so their element tags are bare
#: names, while the opendata.archieven.nl exports do declare it, so theirs arrive from
#: ElementTree as `{urn:isbn:1-931666-22-9}did` and friends. Any name matching must therefore go
#: through `local_name`.
EAD_NS = "urn:isbn:1-931666-22-9"

#: Repository names for the agency codes in the corpus, so that a manifest rebuilt from disk
#: (`describe_corpus`) names institutions rather than codes. Stadsarchief Amsterdam's exports
#: carry no `mainagencycode` at all, so their code is recovered from the filename prefix that
#: `save_ead` wrote.
AGENCY_REPOSITORIES: Dict[str, str] = {
    # harvestable directly
    "NL-HaNA": "Nationaal Archief",
    "NL-AsdSAA": "Stadsarchief Amsterdam",
    # collected from opendata.archieven.nl; codes as the finding aids themselves declare them
    "NL-AhGldA": "Gelders Archief",
    "NL-AmfAE": "Archief Eemland",
    "NL-DtcSARA": "Erfgoedcentrum Achterhoek en Liemers",
    "NL-DvCO": "Collectie Overijssel, locatie Deventer",
    "NL-GnGRA": "Groninger Archieven",
    "NL-HaHGA": "Haags Gemeentearchief",
    "NL-HaNIMH": "NIMH / Nederlands Instituut voor Militaire Historie",
    "NL-HlmNHA": "Noord-Hollands Archief",
    "NL-HtBHIC": "Brabants Historisch Informatie Centrum (BHIC)",
    "NL-UtHUA": "Het Utrechts Archief",
    "NL-ZdGAZ": "Gemeentearchief Zaanstad",
    "NL-ZlCO": "Collectie Overijssel, locatie Zwolle",
    # seen in this project's other data, not (yet) in the EAD corpus
    "NL-AsnDA": "Drents Archief",
    # International corpus (decision D19). The UK's National Council on Archives numbering
    # convention gives <eadid mainagencycode> a bare repository *number* (e.g. "143"), not an
    # alphabetic code; extract_ead_identity combines it with <eadid countrycode="GB"> into
    # "GB-143" before it ever reaches this map, so the bare-number collision risk noted here in
    # earlier revisions no longer applies -- every key below is already country-qualified.
    "GB-143": "Birmingham Archives and Heritage Service (GB)",
    "GB-3624": "Oxford House Archive (GB)",
    # EAD3 (Yale, via GitHub -- see docs/ead_parser/reports/step-01-corpus.md's addendum).
    # <maintenanceagency><agencycode> is already self-qualifying, one per Yale library/unit.
    "US-CtY": "Yale University, Manuscripts and Archives",
    "US-CtY-BR": "Yale University, Beinecke Rare Book & Manuscript Library",
    "US-CtY-D": "Yale University, Divinity Library",
    "US-CtY-Mus": "Yale University, Irving S. Gilmore Music Library",
    "US-CtY-P": "Yale University, Peabody Museum of Natural History",
}


def local_name(tag: str) -> str:
    """The local part of an `ElementTree` tag, dropping any `{namespace}` prefix."""
    return tag.rpartition("}")[2]


#: EAD 2002 allows a component to be written either as a recursive `<c>` or as a depth-numbered
#: `<c01>`...`<c12>`, and the two are equivalent. The Nationaal Archief and Stadsarchief Amsterdam
#: exports use `<c>`; the opendata.archieven.nl (De Ree) exports use the numbered form, where
#: nesting depth is carried by the element name instead of by the tree. Anything that looks for
#: components has to accept both, which is what `is_component` is for.
COMPONENT_RE = re.compile(r"^c(0[1-9]|1[0-2])?$")


def is_component(tag: str) -> bool:
    """True if `tag` is an EAD component element: `<c>` or a numbered `<c01>`..`<c12>`."""
    return bool(COMPONENT_RE.match(local_name(tag)))


def find_ead_root(root):
    """The `<ead>` element of a parsed document, which is not always the document root.

    `opendata.archieven.nl` serves its EAD wrapped in an `<opendata><header/><data>` envelope
    (16 of the 40 finding aids in the corpus), and the envelope is *not* in the EAD namespace
    while the `<ead>` inside it is. Anything reading the root tag directly therefore reports the
    envelope's namespace rather than the EAD's, and anything looking for `<ead>` by bare name
    misses it. Works for both `ElementTree` and `lxml` elements; returns `None` if there is no
    `<ead>` anywhere.
    """
    if local_name(root.tag) == "ead":
        return root
    for element in root.iter():
        if isinstance(element.tag, str) and local_name(element.tag) == "ead":
            return element
    return None


def component_depth_name(tag: str) -> Optional[str]:
    """The numbered-component suffix of `tag` (`"01"`..`"12"`), or `None` for a plain `<c>`."""
    name = local_name(tag)
    return name[1:] if len(name) == 3 else None


def normalise_level(raw_level: Optional[str], otherlevel: Optional[str]) -> str:
    """A component's `@level`/`@otherlevel` pair as one comparable string.

    `@level` is optional in EAD 2002 (absent on 2,593 components in the corpus -- Step 2), so
    `None` becomes the literal `"(none)"` rather than being conflated with any real level value.
    `@level="otherlevel"` carries its real meaning in `@otherlevel`, which Step 2 found to be a
    free-text vocabulary of 22+ Dutch terms (`Adres`, `Dossier`, `Hinderwetvergunning`, ...) --
    not a controlled set to enumerate, so it is folded in verbatim as `"otherlevel:<value>"`.
    Used identically by `ead_survey` (measuring) and `ead_tree` (building), so a coverage number
    computed from one is comparable to output produced by the other.
    """
    if raw_level is None:
        return "(none)"
    if raw_level == "otherlevel":
        return f"otherlevel:{otherlevel}"
    return raw_level


@dataclass
class HarvestedEAD:
    """One finding aid in the corpus and what we know about it without parsing it properly."""

    repository: str
    agency_code: Optional[str]
    eadid: Optional[str]
    title: Optional[str]
    source: str
    url: str
    path: Path
    n_bytes: int
    namespace: Optional[str]
    component_style: str = "recursive"
    level_counts: Dict[str, int] = field(default_factory=dict)

    @property
    def n_components(self) -> int:
        return sum(self.level_counts.values())


class HarvestSession:
    """A polite, serialised HTTP session: one request at a time, `delay` seconds apart."""

    def __init__(self, delay: float = 1.0, timeout: float = 180.0, retries: int = 2):
        self.delay = delay
        self.timeout = timeout
        self.retries = retries
        self._last_request = 0.0

    def get(self, url: str) -> bytes:
        for attempt in range(self.retries + 1):
            wait = self.delay - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    self._last_request = time.monotonic()
                    return response.read()
            except (urllib.error.URLError, TimeoutError) as error:
                self._last_request = time.monotonic()
                if attempt == self.retries:
                    raise
                backoff = self.delay * (2 ** attempt)
                logger.warning("%s on %s -- retrying in %.1fs", error, url, backoff)
                time.sleep(backoff)
        raise AssertionError("unreachable")


# ---------------------------------------------------------------------------------------------
# Describing and filing a finding aid
# ---------------------------------------------------------------------------------------------

def extract_ead_identity(root) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """`(agency_code, eadid, title)` from an EAD document's identity block, across every profile
    the corpus has been checked against so far:

    - EAD 2002, agency code already self-qualifying (Dutch practice):
      `<eadid mainagencycode="NL-HaNA">1.23.45</eadid>`.
    - EAD 2002, agency code a bare repository *number* with a separate `@countrycode` (the UK's
      National Council on Archives numbering convention -- `<eadid mainagencycode="143"
      countrycode="GB">gb143-ms2141</eadid>`): combined into `"GB-143"`, so a bare number that
      happens to repeat across two countries (a real, if not yet observed, risk -- see
      `AGENCY_REPOSITORIES`) does not silently collide.
    - EAD3 (`<control><recordid>...</recordid>...<maintenanceagency><agencycode>US-CtY
      </agencycode>`) -- structurally a different header entirely (`<control>` not
      `<eadheader>`, `<recordid>` not `<eadid>`), but already self-qualifying like the Dutch
      case once found in the right place.

    Works on either `xml.etree.ElementTree` or `lxml.etree` elements (both expose `.iter()`/
    `.get()`/`.text`), so `ead_harvest.describe_ead` and `ead_tree.parse_ead` share one
    implementation rather than two that could drift.
    """
    eadid_element = title = None
    agency_code = eadid = title_text = None
    countrycode = None
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        name = local_name(element.tag)
        if name in ("eadid", "recordid") and eadid_element is None:
            eadid_element = element
            agency_code = element.get("mainagencycode")
            countrycode = element.get("countrycode")
            eadid = (element.text or "").strip() or None
        elif name == "agencycode" and not agency_code:
            # EAD3's <maintenanceagency><agencycode> -- already self-qualifying, takes priority
            # over a bare <recordid> attribute (recordid does not carry mainagencycode at all)
            agency_code = (element.text or "").strip() or None
        elif name == "titleproper" and title is None:
            title = element
            title_text = " ".join("".join(element.itertext()).split()) or None
        if eadid_element is not None and title is not None and agency_code:
            break

    if agency_code and agency_code.isdigit() and countrycode:
        agency_code = f"{countrycode}-{agency_code}"

    return agency_code, eadid, title_text


def agency_code_from_filename(agency_code: Optional[str], filename: str) -> Optional[str]:
    """Falls back to a `"NL-XXX_..."`-shaped filename's own prefix when `extract_ead_identity`
    found no agency code at all -- Stadsarchief Amsterdam's exports carry no `mainagencycode`
    anywhere in the document (Step 1), so the corpus filenames this project's own harvester
    writes (`save_ead`/`ingest_directory`) are the only place that information survives. Used
    identically by `ead_tree.parse_ead` and `ead_stream.parse_ead_streaming` so the two parse
    paths cannot drift on this the way an earlier draft of `ead_stream` briefly did.
    """
    if agency_code is None and filename.startswith("NL-"):
        return filename.split("_", 1)[0]
    return agency_code


def describe_ead(xml_bytes: bytes) -> dict:
    """Agency code, eadid, title, namespace and per-`@level` component counts of an EAD.

    Deliberately independent of `ead_parser`: this is the instrument that says what the corpus
    contains, so it must not depend on the code under test. Namespace-agnostic via `local_name`.
    """
    document = ET.fromstring(xml_bytes)
    root = find_ead_root(document) or document
    namespace = root.tag[1:].partition("}")[0] if root.tag.startswith("{") else None

    agency_code, eadid, title = extract_ead_identity(root)

    levels: Dict[str, int] = {}
    numbered = 0
    for element in root.iter():
        if not is_component(element.tag):
            continue
        if component_depth_name(element.tag) is not None:
            numbered += 1
        level = element.get("level") or "(none)"
        if level == "otherlevel":
            level = f"otherlevel:{element.get('otherlevel')}"
        levels[level] = levels.get(level, 0) + 1

    return {
        "agency_code": agency_code,
        "eadid": eadid,
        "title": title,
        "namespace": namespace,
        "component_style": "numbered" if numbered else "recursive",
        "level_counts": levels,
    }


def _safe_stem(*parts: Optional[str]) -> str:
    joined = "_".join(part for part in parts if part)
    return re.sub(r"[^A-Za-z0-9._-]+", "-", joined).strip("-_")


def save_ead(
    xml_bytes: bytes,
    out_dir: Path,
    repository: str,
    source: str,
    url: str,
    fallback_stem: str,
) -> HarvestedEAD:
    """Write an EAD into `out_dir`, named `<agency-code>_<eadid>.ead.xml` from its own `<eadid>`.

    Falls back to `fallback_stem` unless the document names *both* an agency code and an eadid:
    Stadsarchief Amsterdam's exports carry an `<eadid identifier="...">` with no
    `mainagencycode`, and naming those files after the bare archive number alone would drop the
    repository from the corpus's filenames, which is the one thing the name has to convey.
    """
    described = describe_ead(xml_bytes)
    stem = (_safe_stem(described["agency_code"], described["eadid"])
            if described["agency_code"] and described["eadid"] else fallback_stem)
    path = out_dir / f"{stem}.ead.xml"
    path.write_bytes(xml_bytes)
    return HarvestedEAD(
        repository=repository,
        agency_code=described["agency_code"],
        eadid=described["eadid"],
        title=described["title"],
        source=source,
        url=url,
        path=path,
        n_bytes=len(xml_bytes),
        namespace=described["namespace"],
        component_style=described["component_style"],
        level_counts=described["level_counts"],
    )


# ---------------------------------------------------------------------------------------------
# opendata.archieven.nl: manual collection in a browser, then ingest
# ---------------------------------------------------------------------------------------------

def opendata_listing_url(adtid: Optional[int] = None) -> str:
    """A dataset listing URL restricted to EAD, optionally to one organisation.

    Meant to be opened in a browser (see this module's docstring). The portal caps the listing
    at 15 rows whatever `list[limit]` says, and its `limitstart` paging is session state rather
    than a URL parameter, so one organisation at a time is the reliable way to page through it.
    """
    params = {
        "filter[search]": "",
        "filter[conditions][type]": "oai_ead",
        "list[fullordering]": "dt DESC",
        "list[limit]": "15",
        "submitter": "filter",
    }
    if adtid is not None:
        params["filter[conditions][adtid]"] = str(adtid)
    return f"{OPENDATA_BASE}/nl/datasets?" + urllib.parse.urlencode(params)


def opendata_download_url(adtid: int, guid: str) -> str:
    """The EAD download URL for one dataset, as its page's schema.org `DataDownload` gives it."""
    return f"{OPENDATA_BASE}/download/{adtid}/EAD/{guid}"


#: A snippet to paste into the browser's JavaScript console while a filtered dataset listing is
#: open. Every row on an organisation-filtered listing shares that organisation's `adtid`, so the
#: GUIDs in the row links are enough to build the download URLs, without visiting each dataset
#: page. It only prints them; the person decides what to fetch.
OPENDATA_CONSOLE_SNIPPET = """\
// Paste into the browser console on an organisation-filtered EAD dataset listing.
// Prints one EAD download URL per listed finding aid.
(() => {
  const adtid = new URLSearchParams(location.search).get('filter[conditions][adtid]');
  if (!adtid) return console.warn('No adtid in the URL -- filter the listing by organisation first.');
  const guids = [...new Set([...document.querySelectorAll('a[href*="guid="]')]
    .map(a => new URL(a.href, location.origin).searchParams.get('guid'))
    .filter(Boolean))];
  console.log(guids.map(g => `${location.origin}/download/${adtid}/EAD/${g}`).join('\\n'));
})();
"""


def download_guide(adtids: Iterable[int]) -> str:
    """Instructions for collecting EAD from `opendata.archieven.nl` by hand, in a browser."""
    lines = [
        "opendata.archieven.nl cannot be harvested from a script: it answers 403 to every",
        "non-browser User-Agent, and its robots.txt disallows /download/, where the EAD files",
        "are served. Collect them in a browser instead -- the client the portal is built for.",
        "",
        "For each organisation below:",
        "  1. open the listing URL (already filtered to EAD, newest first, 15 rows);",
        "  2. either click through each dataset and use its 'Download XML (EAD)' button, or",
        "     paste the console snippet at the end of this guide to print the direct URLs;",
        "  3. save the files anywhere -- then file them into the corpus with:",
        "       python scripts/harvest_eads.py data/metadata/eads --ingest ~/Downloads",
        "",
    ]
    for adtid in adtids:
        lines.append(f"{OPENDATA_ORGANISATIONS.get(adtid, f'adtid={adtid}')}  (adtid={adtid})")
        lines.append(f"  {opendata_listing_url(adtid)}")
        lines.append("")
    lines.append("Console snippet:")
    lines.append("")
    lines.extend(f"  {line}" for line in OPENDATA_CONSOLE_SNIPPET.splitlines())
    return "\n".join(lines)


def ingest_directory(
    source_dir: Path,
    out_dir: Path,
    repository: Optional[str] = None,
    source: str = "opendata.archieven.nl (manual browser download)",
    pattern: str = "*.xml",
    overwrite: bool = False,
) -> List[HarvestedEAD]:
    """File manually downloaded EAD documents from `source_dir` into the corpus at `out_dir`.

    Each file is identified from its own `<eadid>` (`mainagencycode` plus the eadid text) and
    renamed accordingly, so browser-assigned names like `<GUID>_oai_ead.xml` become
    `NL-UtHUA_842-3.ead.xml`. `repository` defaults to the EAD's own `mainagencycode`. Anything
    that is not parseable EAD is reported and skipped rather than filed.
    """
    ingested: List[HarvestedEAD] = []
    candidates = sorted(p for p in source_dir.glob(pattern) if p.is_file())
    if not candidates:
        logger.warning("no files matching %r in %s", pattern, source_dir)
        return ingested

    for path in candidates:
        try:
            xml_bytes = path.read_bytes()
            described = describe_ead(xml_bytes)
        except (ET.ParseError, OSError) as error:
            logger.info("%s is not parseable XML (%s) -- skipping", path.name, error)
            continue
        if not described["level_counts"] and described["eadid"] is None:
            logger.info("%s has no <eadid> and no components -- not EAD, skipping", path.name)
            continue

        stem = (_safe_stem(described["agency_code"], described["eadid"])
                if described["agency_code"] and described["eadid"] else path.stem)
        target = out_dir / f"{stem}.ead.xml"
        if target.exists() and not overwrite and target.resolve() != path.resolve():
            logger.info("%s already in corpus -- skipping %s", target.name, path.name)
            continue

        if target.resolve() != path.resolve():
            target.write_bytes(xml_bytes)
        agency_code = described["agency_code"]
        ingested.append(HarvestedEAD(
            repository=(repository
                        or AGENCY_REPOSITORIES.get(agency_code, agency_code)
                        or "unknown"),
            agency_code=described["agency_code"],
            eadid=described["eadid"],
            title=described["title"],
            source=source,
            url="",
            path=target,
            n_bytes=len(xml_bytes),
            namespace=described["namespace"],
            component_style=described["component_style"],
            level_counts=described["level_counts"],
        ))
        logger.info("%s -> %s (%d bytes, %d %s components, namespace=%s)",
                    path.name, target.name, len(xml_bytes), ingested[-1].n_components,
                    described["component_style"], described["namespace"] or "none")
    return ingested


# ---------------------------------------------------------------------------------------------
# Harvestable sources
# ---------------------------------------------------------------------------------------------

def harvest_nationaal_archief(
    session: HarvestSession, out_dir: Path, archive_numbers: Iterable[str],
    overwrite: bool = False,
) -> List[HarvestedEAD]:
    """Harvest Nationaal Archief finding aids by archive number via OAI-PMH `GetRecord`."""
    harvested: List[HarvestedEAD] = []
    for number in archive_numbers:
        url = f"{NA_OAI_URL}?verb=GetRecord&metadataPrefix=oai_ead&identifier={number}"
        target = out_dir / f"NL-HaNA_{number}.ead.xml"
        if target.exists() and not overwrite:
            logger.info("NL-HaNA %s already in corpus -- skipping", number)
            continue
        try:
            envelope = ET.fromstring(session.get(url))
            ead = next((e for e in envelope.iter() if local_name(e.tag) == "ead"), None)
            if ead is None:
                error = next((e for e in envelope.iter() if local_name(e.tag) == "error"), None)
                logger.error("NL-HaNA %s: no <ead> in OAI response (%s)", number,
                             (error.text or error.get("code")) if error is not None else "?")
                continue
            result = save_ead(
                ET.tostring(ead, encoding="utf-8"), out_dir,
                repository="Nationaal Archief", source="service.archief.nl OAI-PMH",
                url=url, fallback_stem=f"NL-HaNA_{number}",
            )
        except Exception as error:  # noqa: BLE001 -- one bad archive must not stop the run
            logger.error("NL-HaNA %s failed: %s", number, error)
            continue
        logger.info("NL-HaNA %s -> %s (%d bytes, %d components)",
                    number, result.path.name, result.n_bytes, result.n_components)
        harvested.append(result)
    return harvested


def harvest_stadsarchief_amsterdam(
    session: HarvestSession, out_dir: Path, archive_numbers: Iterable[str],
    overwrite: bool = False,
) -> List[HarvestedEAD]:
    """Harvest Stadsarchief Amsterdam finding aids by archive number."""
    harvested: List[HarvestedEAD] = []
    for number in archive_numbers:
        url = SAA_EAD_URL.format(archive=number)
        target = out_dir / f"NL-AsdSAA_{number}.ead.xml"
        if target.exists() and not overwrite:
            logger.info("NL-AsdSAA %s already in corpus -- skipping", number)
            continue
        try:
            result = save_ead(
                session.get(url), out_dir, repository="Stadsarchief Amsterdam",
                source="archief.amsterdam", url=url, fallback_stem=f"NL-AsdSAA_{number}",
            )
        except Exception as error:  # noqa: BLE001
            logger.error("NL-AsdSAA %s failed: %s", number, error)
            continue
        logger.info("NL-AsdSAA %s -> %s (%d bytes, %d components)",
                    number, result.path.name, result.n_bytes, result.n_components)
        harvested.append(result)
    return harvested


# ---------------------------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------------------------

MANIFEST_COLUMNS = [
    "file", "repository", "agency_code", "eadid", "title", "source", "url", "bytes",
    "namespace", "component_style", "n_components", "series", "subseries", "filegrp",
    "file_level", "item", "subfile", "other_levels",
]

KNOWN_LEVELS = {
    "series": "series",
    "subseries": "subseries",
    "otherlevel:filegrp": "filegrp",
    "file": "file_level",
    "item": "item",
    "otherlevel:subfile": "subfile",
}


def manifest_rows(harvested: Iterable[HarvestedEAD]) -> List[List[str]]:
    """One manifest row per finding aid, in `MANIFEST_COLUMNS` order."""
    rows = []
    for ead in harvested:
        counts = {column: 0 for column in KNOWN_LEVELS.values()}
        others = []
        for level, count in sorted(ead.level_counts.items()):
            if level in KNOWN_LEVELS:
                counts[KNOWN_LEVELS[level]] = count
            else:
                others.append(f"{level}={count}")
        rows.append([
            ead.path.name, ead.repository, ead.agency_code or "", ead.eadid or "",
            (ead.title or "").replace("\t", " "),
            ead.source, ead.url, str(ead.n_bytes), ead.namespace or "",
            ead.component_style, str(ead.n_components),
            str(counts["series"]), str(counts["subseries"]), str(counts["filegrp"]),
            str(counts["file_level"]), str(counts["item"]), str(counts["subfile"]),
            ";".join(others),
        ])
    return rows


def write_manifest(harvested: Iterable[HarvestedEAD], path: Path, append: bool = False) -> None:
    """Write (or extend) a TSV manifest of the corpus at `path`."""
    rows = manifest_rows(harvested)
    existing: List[str] = []
    if append and path.exists():
        existing = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
        existing = existing[1:] if existing else []
        known = {line.split("\t", 1)[0] for line in existing}
        rows = [row for row in rows if row[0] not in known]
    lines = ["\t".join(MANIFEST_COLUMNS)] + existing + ["\t".join(row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("wrote manifest with %d row(s) to %s", len(lines) - 1, path)


def describe_corpus(corpus_dir: Path, pattern: str = "*.xml") -> List[HarvestedEAD]:
    """Describe every EAD already in `corpus_dir`, for rebuilding a manifest from disk."""
    described: List[HarvestedEAD] = []
    for path in sorted(corpus_dir.glob(pattern)):
        try:
            xml_bytes = path.read_bytes()
            info = describe_ead(xml_bytes)
        except (ET.ParseError, OSError) as error:
            logger.info("%s: %s -- skipping", path.name, error)
            continue
        # fall back to the filename prefix for exports with no mainagencycode (see
        # AGENCY_REPOSITORIES), so a rebuilt manifest still attributes every finding aid
        agency_code = info["agency_code"] or (
            path.name.split("_", 1)[0] if path.name.startswith("NL-") else None
        )
        described.append(HarvestedEAD(
            repository=AGENCY_REPOSITORIES.get(agency_code, agency_code or "unknown"),
            agency_code=agency_code, eadid=info["eadid"], title=info["title"],
            source="(already in corpus)", url="", path=path, n_bytes=len(xml_bytes),
            namespace=info["namespace"], component_style=info["component_style"],
            level_counts=info["level_counts"],
        ))
    return described
