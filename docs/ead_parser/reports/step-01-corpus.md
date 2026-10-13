# Step 1 -- Widening the corpus

**Date:** 2026-09-27 · **Code added:**
{mod}`archival_structures.parsers.ead_harvest`, `scripts/harvest_eads.py`,
`tests/test_parsers_ead_harvest.py` (15 tests).

## Summary

The corpus grew from 10 finding aids over 2 repositories to **14 over 3**, which is short of the
≥5 repositories the plan asked for -- and the reason is the interesting part. Machine access to
Dutch EAD is not uniformly available, and the single finding aid obtained from a third
repository turned out to be written in a **different EAD 2002 spelling** that the current parser
cannot read at all, more completely than anything Step 0 found.

Two findings dominate:

1. `opendata.archieven.nl`, the registry that aggregates EAD from a few dozen Dutch regional and
   municipal archives, **cannot be harvested by a script** (§ Access). Cross-repository breadth
   therefore depends on manual collection in a browser, which the tooling now supports rather
   than pretends around.
2. EAD 2002 permits components to be written either as recursive `<c>` or as depth-numbered
   `<c01>`…`<c12>`, and permits the EAD namespace to be declared or omitted. The corpus now
   contains **both** spellings, and the parser assumes one of them in a way that fails to zero
   (§ E1).

## Data analysis

### Corpus composition

| Finding aid | Repository | Components | Namespace | Component style |
| --- | --- | ---: | --- | --- |
| `NL-AsdSAA_30312` | Stadsarchief Amsterdam | 149 | none | recursive |
| `NL-AsdSAA_395` | Stadsarchief Amsterdam | 5,103 | none | recursive |
| `NL-AsdSAA_5023` *(new)* | Stadsarchief Amsterdam | 18 | none | recursive |
| `NL-AsdSAA_5074` | Stadsarchief Amsterdam | 1,768 | none | recursive |
| `NL-AsdSAA_5075` | Stadsarchief Amsterdam | 34,291 | none | recursive |
| `NL-AsdSAA_5076.NWD` | Stadsarchief Amsterdam | 245 | none | recursive |
| `NL-AsdSAA_834` | Stadsarchief Amsterdam | 3,385 | none | recursive |
| `NL-AsdSAA_89` *(new)* | Stadsarchief Amsterdam | 187 | none | recursive |
| `NL-HaNA_1.01.02` | Nationaal Archief | 28,571 | none | recursive |
| `NL-HaNA_1.04.02` | Nationaal Archief | 17,299 | none | recursive |
| `NL-HaNA_1.05.06` | Nationaal Archief | 558 | none | recursive |
| `NL-HaNA_2.10.36.22` | Nationaal Archief | 82 | none | recursive |
| `NL-HaNA_2.10.50` *(new)* | Nationaal Archief | 929 | none | recursive |
| `NL-UtHUA_842-3` *(new)* | **Het Utrechts Archief** | 305 | `urn:isbn:1-931666-22-9` | **numbered** |

The manifest at `data/metadata/eads/corpus.tsv` carries this plus per-level counts, rebuildable
at any time with `python scripts/harvest_eads.py data/metadata/eads --describe-only`.
(`data/` is gitignored, so the corpus itself is local; this report is the committed record.)

### Access: which publishers can be harvested

| Publisher | Route | Honest User-Agent | Verdict |
| --- | --- | --- | --- |
| Nationaal Archief | OAI-PMH, `oai_ead` prefix | 200 | harvestable |
| Stadsarchief Amsterdam | per-archive EAD export | 200 | harvestable |
| `opendata.archieven.nl` | HTML portal + `/download/<adtid>/EAD/<guid>` | **403** | manual only |

The `opendata.archieven.nl` result is worth stating precisely, because it determines how the
rest of Step 1 can proceed. Tested against its dataset listing:

| `User-Agent` | Status |
| --- | ---: |
| *(absent)* | 403 |
| `curl/8.7.1` | 403 |
| `Python-urllib/3.11` | 403 |
| `archival-structures/0.3 EAD corpus harvester (+github…)` | 403 |
| `ead-corpus-harvester/1.0 (+mailto:…)` | 403 |
| `Mozilla/5.0 … Chrome/120.0 Safari/537.36` | **200** |

Only a browser-like string is served, and the same filter covers `/sitemap.xml` and every other
path tried, so there is no machine-readable index either. Independently, its `robots.txt` has:

```text
User-agent: *
Disallow: /download/
```

…which is exactly where the EAD files are served from. So the portal is closed to scripts on two
independent counts, and {mod}`~archival_structures.parsers.ead_harvest` does not fetch from it.
Instead it offers `--download-guide`, which prints the per-organisation listing URLs and a
browser-console snippet that turns a filtered listing into direct download links, and
`--ingest`, which files whatever was downloaded into the corpus -- renaming each file from its
own `<eadid>`, so a browser-assigned `609C5B9D…_oai_ead.xml` becomes `NL-UtHUA_842-3.ead.xml`.

The registry itself is the useful discovery, and it is recorded in
{data}`~archival_structures.parsers.ead_harvest.OPENDATA_ORGANISATIONS`: 11 organisations with
their `adtid` values, enough to build any download URL once a dataset GUID is known, because a
download URL is just `/download/<adtid>/EAD/<guid>`. Organisations seen publishing EAD there
include Gelders Archief, Het Utrechts Archief, Noord-Hollands Archief, Zeeuws Archief,
Groninger Archieven, BHIC, Haags Gemeentearchief, Westfries Archief, Streekarchief
Voorne-Putten, Collectie Overijssel and NIMH.

### `@level` vocabulary per repository

| Level | Nationaal Archief | Stadsarchief Amsterdam | Het Utrechts Archief |
| --- | ---: | ---: | ---: |
| `series` | 25 | 325 | 4 |
| `subseries` | 631 | 621 | 30 |
| `otherlevel="filegrp"` | 1,445 | -- | 28 |
| `file` | 36,081 | 9,909 | 243 |
| `item` | 7,035 | -- | -- |
| `otherlevel="subfile"` | 2,222 | -- | -- |

`filegrp` appearing at Utrecht as well as the Nationaal Archief is mildly reassuring -- it is a
shared convention, not one house's. `item` and `subfile` remain Nationaal-Archief-only, so the
plan's intent to support them is still, on this evidence, support for one exporter's practice.

### Features unique to a single repository

These are the over-fitting risks the plan asked this step to name. Element tags under `<dsc>`
that occur in exactly one repository:

| Repository | Unique elements | What depends on it |
| --- | --- | --- |
| Het Utrechts Archief | `c01`…`c07` | the entire component walk (§ E1) |
| Nationaal Archief | `dao`, `item`, and 26 others (`bioghist`, `custodhist`, `otherfindaid`, `phystech`, …) | `mets_file` extraction; `item`-level description |
| Stadsarchief Amsterdam | `assets`, `asset`, `iiif`, `name`, `accessrestrict` | all asset/page extraction |

So **every** digitisation-linking mechanism in the corpus is repository-specific, and there are
now three of them, not two:

```text
Nationaal Archief       <dao role="METS" href="https://service.archief.nl/gaf/api/mets/v1/…"/>
Stadsarchief Amsterdam  <nexus:assets><nexus:asset><nexus:iiif>…</nexus:iiif></nexus:asset>…
Het Utrechts Archief    <unitid audience="internal" type="handle">
                            https://hetutrechtsarchief.nl/collectie/609C5B9D68E64642E0534701000A17FD
                        </unitid>
```

Utrecht has no `<dao>` and no assets: its link to the digitised object is a handle URL inside a
`<unitid>`, on every one of its 243 `file` components and all 28 of its `filegrp`s -- and on none
of its series or subseries. Step 5 has a third mechanism to model, and the generalisation
"digitisation lives in `<dao>` or in assets" is already wrong.

### `<unitid>` shapes per repository

Step 4 plans repository-aware inventory-number identification; this is the evidence it has to
work from. `(type, label, has @identifier)` → count:

| Repository | `<unitid>` variants |
| --- | --- |
| Nationaal Archief | `(handle)` 36,081 · `(—, —, @identifier)` 35,561 · `(urn:uuid)` 17,299 · `(—)` 1,560 · `(series_code)` 648 · `(blank)` 405 · `(obsolete)` 265 |
| Stadsarchief Amsterdam | `(—, Inventarisnummer, @identifier)` 9,909 · `(—, Rubriekscode, @identifier)` 946 |
| Het Utrechts Archief | `(guid)` 305 · `(—)` 271 · `(handle)` 271 · `(series_code)` 34 |

Three disjoint conventions for "which of these is the inventory number":

- Amsterdam labels it (`label="Inventarisnummer"`) -- unambiguous;
- the Nationaal Archief gives it an `@identifier` attribute and no `type`;
- Utrecht gives it *neither* -- the inventory number is the bare, untyped, unlabelled
  `<unitid>1</unitid>`, distinguished from its siblings only by their having a `type`.

Step 4's fallback shape heuristic therefore has real work to do at Utrecht, and `type="guid"`
is a new value the current parser's `unitid` type handling does not know
(`{'ABS', 'handle', 'blank', 'obsolete', 'urn:uuid'}`).

## Error analysis

### E1 -- The numbered/namespaced profile fails before parsing starts

`NL-UtHUA_842-3` has 305 components, 243 of them files. The current parser returns none of them,
and unlike the Step 0 failures this one is loud:

```python
>>> ead_parser.read_ead(ead_file='data/metadata/eads/NL-UtHUA_842-3.ead.xml')
None
>>> ead_parser.get_inventory_info(ead_file='data/metadata/eads/NL-UtHUA_842-3.ead.xml')
TypeError: 'NoneType' object is not iterable
```

Two independent causes, either of which alone is fatal.

**Cause 1 -- the default namespace.**
{func}`~archival_structures.parsers.read.read_xml` is called with `root_name='ead'` and compares
it to `root.tag`. Here the root tag is `{urn:isbn:1-931666-22-9}ead`, so the comparison fails,
the fallback `root.find('.//ead')` finds nothing, and `read_ead` returns `None`. Every tag
comparison downstream (`child.tag == 'did'`, `child.tag == 'c'`, …) would fail the same way.

**Cause 2 -- depth-numbered components.** Even with the namespace stripped, there are no `<c>`
elements at all. The hierarchy is carried by element *names*:

```xml
<c01 level="series">
  <did id="MF1003654">
    <unitid type="guid">609C5B9D68E44642E0534701000A17FD</unitid>
    <unitid type="series_code">1.</unitid>
    <unittitle>Archief van de pastoor</unittitle>
    <unitdate calendar="gregorian" era="ce" normal="1567/1989"/>
  </did>
  <c02 level="subseries">
    <did id="MF1003655">
      <unitid type="series_code">1.1.</unitid>
      <unittitle>Stukken van algemene aard</unittitle>
    </did>
    <c03 level="file">
      <did id="MF1003656">
        <unitid type="guid">609C5B9D68E64642E0534701000A17FD</unitid>
        <unitid>1</unitid>
        <unitid audience="internal" type="handle">https://hetutrechtsarchief.nl/collectie/…</unitid>
        <unittitle>Ingekomen en minuten van uitgaande stukken, 1839-1972, z.j.</unittitle>
      </did>
    </c03>
  </c02>
</c01>
```

Note that `@level` is still present and still says `series`/`subseries`/`file`, so the level
information the parser wants is right there -- only the element name it looks for is different.
Depth in this finding aid reaches `c07`, i.e. 7 levels, consistent with the depths Step 0 found
elsewhere.

Both spellings are valid EAD 2002 and mean the same thing. The fix is small but it has to be in
the right place: a single `is_component(tag)` predicate plus namespace-agnostic name matching,
applied everywhere the walk asks "is this a component?". Step 3 now has
{func}`~archival_structures.parsers.ead_harvest.is_component` and
{func}`~archival_structures.parsers.ead_harvest.local_name` to build on, and
`tests/test_parsers_ead_harvest.py::TestEADProfileFixtures` pins both profiles as hand-written
fixtures so the tree builder is tested against them without needing a harvested file on disk.

`is_component` is deliberately narrow: `c13` is past EAD 2002's `c12` limit, and `controlaccess`,
`custodhist` and `corpname` all begin with a `c` and must not be mistaken for components. Those
are tested.

### E2 -- A naming bug in the harvester, found by running it

First run of `--source stadsarchief-amsterdam` filed its output as `89.ead.xml` and
`5023.ead.xml`, dropping the repository from the corpus filenames. Cause: Amsterdam's
`<eadid identifier="…">89</eadid>` carries no `mainagencycode`, so composing the name from
`(agency_code, eadid)` yielded just the archive number. `save_ead` now falls back to the
caller's stem unless the document supplies *both* parts, and `ingest_directory` does the same.

Worth recording because it is the same class of mistake as the parser's own: assuming a field
that two exporters supply is a field all exporters supply.

### E3 -- What the step did not achieve

The plan asked for ~20--30 finding aids over ≥5 repositories. Delivered: 14 over 3. The gap is
entirely the `opendata.archieven.nl` access constraint, and it is not a coding problem. Options,
none of which block Step 2:

- collect from the portal in a browser (`--download-guide` prints what is needed, `--ingest`
  files the results);
- ask De Ree or the individual archives for a sanctioned harvesting route;
- look for other repositories with documented machine interfaces -- Het Utrechts Archief
  answers on `/api/` with a JSON error rather than a 404, so it has an API worth investigating.

One finding aid from one new repository was enough to invalidate an assumption held by every
function in the parser, so breadth is paying off faster than volume would. Step 2's structural
survey should run over whatever the corpus holds at that time, and be re-runnable as it grows.

## Conclusions carried into the plan

1. Component recognition must accept `<c>` and `<c01>`…`<c12>`, and must match on local names,
   not on `element.tag` -- both before Step 3 can claim losslessness.
2. `read_ead`'s `root_name='ead'` check is broken for namespaced EAD and must go through
   `local_name` too.
3. There are three digitisation-linking mechanisms in the corpus, not two; Step 5's scope grows
   to include handle `<unitid>`s.
4. Inventory-number identification has three disjoint conventions, one of which (Utrecht) offers
   no positive marker at all -- Step 4's fallback heuristic is load-bearing, not a safety net.
5. `type="guid"` joins the known `<unitid>` types.
6. Corpus breadth is limited by publisher access policy, not by tooling; the manifest and
   `--describe-only` make it cheap to re-measure as the corpus grows.

## Addendum -- the corpus goes international (2026-09-27, decision D19)

The user added two UK finding aids to the corpus directory and asked, explicitly, that the
corpus widen to more countries and archival systems on purpose: *"I think it's good to expand
the set with more EADs from archival institutions that use different archival systems and
practices, so from a range of countries."* This resolves the scope question the main report left
open (D14) and reframes everything above as describing where the corpus *started*, not a
boundary it stays inside. The corpus is now **47 finding aids across 20 repositories in 3
countries** (Netherlands, United Kingdom, United States).

### The two UK finding aids

`GB-143_ms2141.ead.xml` (Birmingham Archives and Heritage Service, 224 components) and
`GB-3624_oh.ead.xml` (Oxford House Archive, 853 components) -- renamed to lead with a country
code, matching the `NL-*` convention, from filenames the user assigned when downloading both by
hand in a browser. (`download_sample_eads.py`, a `Sickle`-based OAI-PMH script targeting Archives
Hub UK found alongside them in the corpus directory, was a separate attempt that hit the same
User-Agent blockade described below and was not the source of these two files -- corrected here;
an earlier draft of this report wrongly assumed it was, and drew a false inference from that, in
the access-findings section below.) Both parse to **100% tree coverage**, confirming decision D10's level-agnostic
design generalises outside the Netherlands without any code change. Both introduce a level not
seen in any Dutch finding aid, `subfonds` (14 occurrences across the two), and use
slash-hierarchical reference numbers (`"MS 2141/A/1/1"`, `"OH/1/2/1/1"`) that no current
inventory-number rule resolves -- a new open question, same category as D13 (status.md).

### A real naming bug, and a real fix: `extract_ead_identity`

Registering the UK files by their own `<eadid mainagencycode>` produced bare numbers (`"143"`,
`"3624"`) with no country information -- the UK's National Council on Archives numbering
convention gives a repository a *number*, not a self-qualifying code like Dutch practice's
`NL-HaNA`. `status.md` had already flagged this as a collision risk (D19's open question:
"another country's finding aid could reuse the same bare number"); fixed properly rather than
left as a documented risk, since the risk was created by the same feature it needed. A new
shared function, {func}`~archival_structures.parsers.ead_harvest.extract_ead_identity`, combines
a purely-numeric `mainagencycode` with its sibling `@countrycode` (`"143"` + `"GB"` → `"GB-143"`)
before it reaches `AGENCY_REPOSITORIES` or a filename. `ead_harvest.describe_ead` and
`ead_tree.parse_ead` (which had two separate, drifting implementations of this extraction) now
share the one function.

### EAD3, finally -- and it needed the same fix again, differently

Yale University publishes its entire finding-aid corpus as EAD3 on GitHub
(`YaleArchivesSpace/Archives-at-Yale-EAD3`) -- plain HTTPS, no bot protection, no API to guess
at. Five sample files were fetched across five Yale libraries/units (Manuscripts and Archives,
Beinecke, Divinity, Music, Peabody Museum), sizes from 8.5 KB to 799 KB. This is the corpus's
first genuine EAD3 document (not just a different EAD 2002 dialect): the header is
`<control>`/`<recordid>`, not `<eadheader>`/`<eadid>`, in the namespace
`http://ead3.archivists.org/schema/`.

The result was better than expected. `<archdesc>`/`<dsc>`/`<c level="...">`/`<did>`/`<unitid>`/
`<unittitle>` are **identical in EAD3** to EAD 2002 -- the tree builder needed zero changes and
reached **100% coverage on all five files** (2 to 1,447 components each) the first time it was
tried. Only document *identity* extraction needed EAD3 awareness, because `extract_ead_identity`
was looking for `<eadid>` by name: extended to also recognise `<recordid>`, and to recognise
EAD3's `<maintenanceagency><agencycode>` (already self-qualifying, e.g. `"US-CtY-BR"` for
Beinecke) as a second, higher-priority source of the agency code, since `<recordid>` itself
carries no `mainagencycode` attribute at all.

One further, real difference in archival *practice*, not just schema: none of the five Yale
`file`-level components sampled carry a `<unitid>` at all. US institutional practice via
ArchivesSpace commonly identifies material by `<container>` (box/folder, with a barcode-like
`containerid`) rather than by a single per-item inventory number -- a fourth identification
paradigm alongside Amsterdam's label, the Nationaal Archief's untyped `@identifier`, and
Utrecht's mark-by-absence, and one `identify_inventory_number` currently has no rule for at all
(it correctly returns `"none"`, not a wrong guess, but a `<container>`-based identification rule
is unexplored territory, not merely an unresolved edge case within the existing rule set).

### Access, at international scale, is the same wall found domestically -- one floor higher

Before finding Yale's GitHub mirror, direct harvesting was attempted against Archives Hub UK,
Library of Congress, SNAC, Calames, Kalliope, Riksarkivet, PARES, and several US university
ArchivesSpace instances (Yale, Duke, UNC, Cornell, MIT, Washington State, Washington University
in St Louis). Findings, briefly:

- **Library of Congress's `findingaids.loc.gov`** -- returns a Cloudflare JavaScript-challenge
  page (`"Just a moment..."`) to every request, regardless of `User-Agent`. This is a strictly
  harder block than `opendata.archieven.nl`'s UA-sniffing (D6): that one let an honest
  browser-shaped string through; this one requires executing JavaScript, which no plain HTTP
  client can do. (An earlier draft of this report claimed the two UK finding aids came from this
  endpoint via `download_sample_eads.py`, and read the fact that they are UK records as evidence
  LoC's OAI-PMH aggregates records from contributing institutions worldwide. Both the premise and
  the inference were wrong: the user downloaded both files by hand in a browser after
  `download_sample_eads.py` hit this same blockade, the same as everything else in this list.
  Whether LoC's OAI-PMH aggregates non-US records at all is, correctly, unknown.)
- **Archives Hub UK, Duke University** -- the same Cloudflare/Anubis-style challenge.
- **SNAC, Kalliope** -- reachable, but returned an HTML landing page rather than the OAI-PMH
  response expected at the guessed path; the correct endpoint was not found in the time
  budgeted for this search.
- **Calames, Riksarkivet, PARES, UNC, Cornell, MIT, Washington State, Washington University** --
  connection failures, 403s, or 404s on every guessed URL.

Blind URL-guessing against aggregator portals has a low, worsening hit rate; **institution-run
GitHub mirrors (Yale's being the one found) are a materially better source** when they exist --
ordinary HTTPS, no bot defence, and version-controlled. Worth checking for before further
portal-guessing on any future country.

### Conclusions carried into the plan

7. The corpus is deliberately international (D19); further harvesting should seek national and
   structural diversity over volume, the same intent D3 had for repositories within one country.
8. `extract_ead_identity` is the one place agency-code/eadid/title extraction happens, across
   EAD 2002 (self-qualifying or bare-numeric-plus-country) and EAD3; new profiles extend this
   function, not a third parallel implementation.
9. EAD3 needs no changes to the component tree itself (D10 generalises); only identity
   extraction and, likely, Step 5's digitisation-link mechanisms (unchecked so far -- EAD3 has
   its own `<dao>`-equivalent conventions, not yet surveyed) are schema-specific.
10. A `<container>`-based (box/folder) identification paradigm exists and is currently
    unsupported; noted as an open question rather than guessed at.
11. GitHub-hosted institutional EAD mirrors are worth checking before attempting a portal's own
    (increasingly bot-protected) API.
