# Plan: testing and rewriting the EAD parser

## Why

{mod}`archival_structures.parsers.ead_parser` was written against a handful of Nationaal
Archief (`NL-HaNA`) finding aids and generalised from there. Running it over the ten EADs
currently in `data/metadata/eads` (see
[Step 0 -- baseline and structural survey](reports/step-00-baseline.md)) shows it is not
wrong so much as *narrow*: it recovers 94--100% of file-level components in seven of the nine
finding aids it can process, but it

- returns **zero rows** for `NL-HaNA_1.05.06` (542 files), because that finding aid has no
  `<c level="series">` under `<dsc>` and {func}`~archival_structures.parsers.ead_parser.get_series`
  only yields series children;
- silently drops the 287 `<c level="file">` elements that sit directly under `<dsc>` in
  `NL-AsdSAA_395`, for the same reason;
- discards **every** `level="item"` and `otherlevel="subfile"` component -- 7,024 items in
  `NL-HaNA_1.01.02` alone -- because
  {func}`~archival_structures.parsers.ead_parser.parse_file` does not recurse into a file's
  `<c>` children;
- truncates the series hierarchy to `max_subseries_depth=2`, while `NL-AsdSAA_834` nests
  subseries **eight** levels deep and `NL-HaNA_1.04.02` six;
- extracts no digitised-object information beyond a single `dao[@role='METS']` href, so the
  1,172--1,910 `<nexus:asset>` page descriptions per Amsterdam finding aid are invisible;
- fails loudly on unfamiliar structure (`raise ValueError(f'unexpected ... child')`), which is
  a defensible design choice, but combined with the silent drops above it gives a misleading
  signal: the parser is quiet exactly where it loses the most data.

The failure mode that matters is the silent one. A rewrite is justified, but only on top of a
measured account of what the current parser gets right, so that the rewrite can be held to it.

## Target data model

The rewrite parses an EAD into a **lossless tree of typed component nodes**, and derives flat
tables from that tree rather than building them during the walk. This is the decision that
unblocks the `item`/`subfile` levels and the deep subseries nesting: neither can be expressed
in a fixed-column flattening, but both are ordinary nodes in a tree.

```text
EADFindingAid
  repository, eadid, title, date_range, source_profile   # NL-HaNA | NL-AsdSAA/nexus
  root: [EADComponent, ...]

EADComponent
  level            # series | subseries | filegrp | file | item | subfile | otherlevel:<x>
  unitids          # [UnitID(value, type, label, identifier)] -- all of them, untyped
  title, dates     # unittitle (with embedded unitdate handled), unitdate + @normal
  physdesc, physloc, scopecontent, access
  digital          # [DigitalObject]  -- dao hrefs, roles, METS references
  assets           # [Asset]          -- per-scan/page descriptions (see below)
  children: [EADComponent, ...]
  parent / ancestors                  # full path, no depth cap
```

Derived views, each a `pandas.DataFrame`:

| view | one row per | replaces |
| --- | --- | --- |
| `to_frame(level='file')` | inventory number [^unit] | `get_inventory_info` |
| `to_frame(level='item')` | described document within an inventory number | -- (new) |
| `to_frame(level='asset')` | digitised scan/page | -- (new) |

[^unit]: Step 2 complicates this: two repositories in the corpus have no `file` components at
    all and describe at `otherlevel="Dossier"` or `otherlevel="Hinderwetvergunning"`. The unit
    of description probably has to be identified by carrying an inventory number rather than by
    its level name. Open question in [status.md](status.md), to settle in Step 6.

Ancestry is carried as a `path` column (a `>`-joined list of titles) plus dynamically named
`level_1..level_n` columns sized to the actual depth of the finding aid, so no hierarchy is
lost to a `max_subseries_depth` argument. A `max_depth` argument may still be offered for
callers that want the old fixed shape, but it truncates a *view*, not the parse.

### Assets and document-level descriptions

Two different things get conflated under "digitised document", and the model keeps them apart:

- **`item` / `subfile` components** are *intellectual* subdivisions of an inventory number --
  individual deeds, letters, registrations -- described in the EAD with their own `<did>`.
  They are `EADComponent`s like any other.
- **`<nexus:asset>` elements** (Memorix namespace `urn:memorix.io`, Stadsarchief Amsterdam
  exports) are *physical/digital* surrogates: one per scan, with `asset_order`, a `name`
  (e.g. `A22796000001.jp2`) and a IIIF `info.json` URL. They attach to a `file` component.

  They do **not** give the full page sequence. Step 0 established that the exports truncate
  every asset list at ten: no `<nexus:assets>` in the corpus holds more than 10 `<nexus:asset>`
  children and no `asset_order` exceeds `9`, with no count or pagination attribute to recover
  the rest. `NL-AsdSAA_30312` inventory 1 -- a notarial register covering 1730--1733 -- lists
  `A22796000001.jp2` … `A22796000010.jp2` and stops. So an asset list tells us *that* an
  inventory number is digitised, gives the first ten scan identifiers, and seeds a IIIF lookup;
  the model records this explicitly with an `assets_truncated` flag rather than letting callers
  mistake ten for all.

Note the two IIIF URL shapes already present in the corpus --
`.../resources/iiif/3/<uuid>/info.json` (`NL-AsdSAA_30312`) and
`.../resources/records/media/<record-uuid>/iiif/3/<numeric-id>/info.json`
(`NL-AsdSAA_5075`) -- so asset parsing must not assume one pattern.

`archival_structures.parsers.read.EADReader` already contains a BeautifulSoup-based sketch of
asset extraction (`_get_file_assets`). It is untested and built on a different element API; the
rewrite reimplements it on the `ElementTree` path and `EADReader` is retired in Step 8.

## Steps

Each step produces working code, tests, and a report in `reports/` containing **both** a data
analysis (what the corpus looks like on this dimension) and an error analysis (worked examples
of what the parser gets right and what it still gets wrong, quoted from real EADs). Reports are
cumulative evidence, not progress notes -- the progress notes live in [status.md](status.md).

### Step 0 -- Baseline and structural survey *(complete)*

Measure the current parser against every EAD in the corpus; profile the structural variation
it has to cope with. → [reports/step-00-baseline.md](reports/step-00-baseline.md)

### Step 1 -- Widen the corpus *(complete, partially)*

Harvest EADs from repositories beyond `NL-HaNA` and `NL-AsdSAA`, so the rewrite is not tuned to
two exporters. → [reports/step-01-corpus.md](reports/step-01-corpus.md)

Delivered: 14 finding aids over 3 repositories (target was ~20--30 over ≥5), plus
{mod}`archival_structures.parsers.ead_harvest` and `scripts/harvest_eads.py` with three modes --
harvest (Nationaal Archief OAI-PMH, Stadsarchief Amsterdam), `--download-guide` and `--ingest`
for publishers that do not serve scripts, and `--describe-only` to rebuild the `corpus.tsv`
manifest from disk.

The shortfall is publisher access policy, not tooling: `opendata.archieven.nl`, which aggregates
EAD from dozens of Dutch regional archives, 403s every non-browser `User-Agent` and disallows
`/download/` in `robots.txt` (see decision D6). Further breadth comes from collecting in a
browser and ingesting, or from a sanctioned route if one can be arranged.

It paid off anyway. One finding aid from one new repository (Het Utrechts Archief) established
that EAD 2002's **two component spellings** -- recursive `<c level="...">` and depth-numbered
`<c01>`…`<c12>` -- and its **optional default namespace** both occur in practice, and that the
current parser handles neither: `read_ead` returns `None` and `get_inventory_info` raises
`TypeError`, reaching none of the file's 305 components. It also turned up a **third**
digitisation-linking mechanism (a `<unitid type="handle">` URL, rather than `<dao>` or inline
assets) and a **third** convention for marking the inventory number (no positive marker at all --
it is the one untyped, unlabelled `<unitid>`). Steps 3--5 absorb all of this.

### Step 2 -- Structural conformance harness *(complete)*

Before changing the parser, build the instrument that says whether a change helped.
→ [reports/step-02-conformance.md](reports/step-02-conformance.md)

Delivered: {mod}`archival_structures.parsers.ead_survey`, `scripts/survey_eads.py` and 29 tests.
**First reading: the current parser reaches 44,951 of 208,926 components — 21.5% — raising
`TypeError` on 27 of 40 finding aids and scoring 0% in 11 of 13 repositories.** Outside `file`
level it reaches nothing. Step 0's 94--100% figures were measured on the two repositories the
parser was written against, at the one level it emits; both restrictions are now lifted.

The harness immediately paid for its independence by catching an error in the Step 1 report:
16 finding aids arrive inside an `<opendata>` envelope whose namespace differs from the `<ead>`
inside it, so Step 1's namespace column was wrong for them (decision D9). It also established
that `otherlevel` is a free-text vocabulary of 22 values (D8), that 2,593 components carry no
`@level` at all, that nesting reaches 11, and that **two repositories have no `file` components
whatsoever** -- they describe at `Dossier` and `Hinderwetvergunning` -- which is a problem for
Step 6's view design, not just for the parser.

- `archival_structures/parsers/ead_survey.py`: given an EAD, report every element tag under
  `<dsc>`, every `@level`/`@otherlevel` value, every attribute name per element, and every
  root-to-`file` ancestry path with counts. (The ad-hoc version of this used for Step 0 becomes
  a supported module.) It must be namespace-agnostic and accept both component spellings from
  the start -- via {func}`~archival_structures.parsers.ead_harvest.local_name` and
  {func}`~archival_structures.parsers.ead_harvest.is_component` -- or it will under-report the
  Utrecht-style finding aids exactly as the parser does.
- `tests/test_parsers_ead_survey.py` over small hand-written EAD fragments.
- A **coverage metric** used by every later step: for a given EAD, the fraction of `<c>`
  elements in the source that appear in the parser's output, broken down by level. This is the
  number Step 0 reports as `recall`, generalised past `file`.
- **Report:** the full structural vocabulary of the corpus; a ranked list of constructs the
  current parser does not handle, each with a file/line citation.

### Step 3 -- The component tree *(complete)*

Implement `EADFindingAid` / `EADComponent` / `UnitID` and a builder that walks `<dsc>` into a
tree, with no level-specific logic -- a `<c>` is a component whatever its `@level`.
→ [reports/step-03-component-tree.md](reports/step-03-component-tree.md)

**Acceptance criterion met: 100% component coverage, all 40 finding aids, all 27 levels
(243,217/243,217), up from the legacy parser's 21.5%.** Delivered:
{mod}`archival_structures.parsers.ead_tree` and 25 tests.

Unknown levels are retained with their literal `@level`/`@otherlevel` value (`EADComponent.level`
is a plain string, never a branch target -- D10); unmodelled `<did>` children land in
`did_extra`, unmodelled non-`<did>` children in `other_children`, both counted and available for
Step 4/5. `<c>` elements directly under `<dsc>` are roots (`NL-HaNA_1.05.06`'s 535 of them, where
the legacy parser found none), and the `<ead>` root is located via
{func}`~archival_structures.parsers.ead_harvest.find_ead_root`, not assumed to be the document
root.

Two findings beyond the acceptance criterion: `NL-AsdSAA_5075` (106 MB), excluded by every prior
step as too large to parse, parses in 0.8s with `lxml` and reaches 100% coverage like everything
else -- the exclusion described `ElementTree`, not the file (Step 7 is now about headroom beyond
106 MB, not about this file specifically). And a genuinely dangerous near-miss: both `ead_survey`
and `ead_tree` independently locate `<dsc>` as "the first one in document order," and one finding
aid has 5,231 *nested* `<dsc>` elements neither descends into -- they happened to agree because
those nested ones hold no components (verified by hand), but the agreement itself proved nothing
(decision D11, sharpening D9: independent code can still share a blind spot about the format).

### Step 4 -- `<did>` field extraction *(complete)*

Port and harden the field-level parsing: multiple `<unitid>`s with `type`/`label`/`identifier`
attributes, `<unittitle>` with an embedded `<unitdate>`, `<unitdate normal>` ranges, `<physdesc>`,
`<physloc>`, `<controlaccess>`. → [reports/step-04-inventory-numbers.md](reports/step-04-inventory-numbers.md)

Delivered: {mod}`archival_structures.parsers.ead_fields`
(`identify_inventory_number`/`parse_unitdate`/`parse_physdesc`/`parse_physloc`/`parse_controlaccess`),
34 new tests across two files. **Inventory-number identification is now four ranked, audited
rules (decision D12) rather than a regex over raw text, replacing a rule measured to misidentify
29% of `file` components corpus-wide** (a `type="guid"` `<unitid>` starting with a digit, listed
before the real identifier -- exactly the hazard Step 1/2's three-convention survey predicted).
At the actual unit-of-description level, 11 of 13 repositories resolve 98.8-100%; the two
exceptions are a source-data limitation (no human-legible identifier exists at all -- BHIC) and
a genuine, deliberately-unfixed rule gap (hyphen-separated identifiers, decision D13, since `-`
already means something else at Amsterdam/Nationaal Archief filegroup level).

Reused {class}`archival_structures.model.inventory_number.InventoryNumber` as planned, and
{func}`~archival_structures.parsers.ead_start_end_year.get_begin_end_year` (which had no test
coverage and crashed on real corpus data -- a three-part `normal` date list -- the first time it
was exercised beyond its original single hand-picked example; fixed and now tested). Two UK
finding aids found in the corpus directory outside the established harvest workflow were checked
(100% tree coverage, 0% inventory-number resolution) and explicitly excluded from this step's
figures pending a scope decision (D14).

### Step 5 -- Digital objects and assets *(complete)*

Extract all four digitisation-linking mechanisms the corpus uses, namespace-agnostically (the
prefix is bound as `ns0` by `ElementTree`, so matching must be on the namespace URI).
→ [reports/step-05-digital-objects.md](reports/step-05-digital-objects.md)

Delivered: {mod}`archival_structures.parsers.ead_digital`
(`parse_dao`/`parse_daogrp`/`parse_assets`/`parse_handle_urls`/`summarise_digitisation`), 15
tests. The sampled IIIF check ran (5 random Amsterdam files, 3 `info.json` fetches, all HTTP 200
with valid image dimensions): the truncated 10-asset preview is live and real, just incomplete
in count, confirming Step 0's finding rather than an artefact of the one file it was measured on.
Digitisation coverage had to be measured at each repository's real unit-of-description level
(decision D16) -- a `file`-level-only query reports Brabants Historisch Informatie Centrum as 0%
digitised, because it has no `file` components at all, when it is in fact 100% covered by a
handle URL at its actual unit, `otherlevel:Inschrijving`.

- `<dao>` href/role/`xlink` attributes -- METS references (Nationaal Archief only: 27,661,
  exclusively; confirmed by Step 5, correcting an unverified attribution carried in this plan
  since Step 2 -- `<daogrp>` is not a Nationaal Archief construct at all, see next bullet);
- `<daogrp>` -- an XLink extended-link *group*, not a single reference: a `<daodesc>` note plus
  one `thumb`/`reference` `<daoloc>` pair per scan. Used by 7 `opendata.archieven.nl`-family
  repositories (14,263 total, 95% of it -- 13,603 -- at Brabants Historisch Informatie Centrum
  alone), never by the Nationaal Archief or Stadsarchief Amsterdam;
- `<nexus:asset>` sequences in `urn:memorix.io` (Stadsarchief Amsterdam);
- `<unitid type="handle">` URLs -- 179,890 corpus-wide, the **majority** mechanism, used by
  most repositories including Het Utrechts Archief, which has neither of the others.

- `Asset(order, name, iiif_url, identifier)`, sorted by `asset_order`, exposed both on the
  component and as the `level='asset'` frame, with `assets_truncated` set whenever a list hits
  the ten-asset ceiling.
- The two repositories are disjoint on this: all four `NL-HaNA` finding aids use `<dao>` and no
  assets; all five measurable `NL-AsdSAA` ones use assets and no `<dao>`. So there is no
  inventory number on which a METS-vs-assets page-order cross-check is even possible. What can
  be checked is coarser and still worth having: does *digitised according to assets* agree with
  *digitised according to the archive's IIIF service*, sampled over inventory numbers?
- Establish how to get the real page sequence for an Amsterdam inventory number -- IIIF
  Presentation manifest from the record UUID is the obvious candidate -- and record the
  finding, even though fetching it is out of scope for the parser.
- **Report:** assets per finding aid and per inventory number (distribution, not just totals),
  with the truncation ceiling shown; the IIIF URL shapes encountered; inventory numbers with
  zero assets (not digitised, or dropped by the export?); the sampled IIIF check.

### Step 6 -- Flat views *(complete)*

Implement `to_frame(level=...)`, a functional (not level-name-based) unit-of-description view,
and a compatibility shim reproducing `get_inventory_info`'s exact column order.
→ [reports/step-06-flat-views.md](reports/step-06-flat-views.md)

**Acceptance criterion met, on the corrected count of eight finding aids** (Step 0's own table
lists eight at ≥94% legacy recall, not seven as an earlier draft of this plan said): every one
of 69 legacy-only rows across those eight is individually explained (a database UUID or a
malformed multi-part reference the legacy parser's prefix-matching regex truncated into
something that merely looked like a number), and none is a real inventory number the new parser
fails to reproduce. 336 rows the legacy parser never produced at all are also recovered (287
from Step 0's dsc-rooted-file construct, 49 from components its structural walk never reaches).

Delivered: {mod}`archival_structures.parsers.ead_views`
(`to_frame`/`unit_of_description_frame`/`legacy_compatible_frame`), 13 tests.
`unit_of_description_frame` answers Step 2/4's open question -- functionally, by which component
carries a resolved inventory number (preferring the deepest in any chain), never by level name
(decision D17). Building the row-diff surfaced and fixed two real bugs: a latent, never-yet-
triggered direction error in the row-selection logic itself (fixed and regression-tested even
though the real corpus cannot trigger it), and an already-triggered one in Step 4's rule set --
`type="obsolete"` was excluding 181 genuinely correct Nationaal Archief inventory numbers,
because the exclusion had been generalised from one repository's data without checking the
others (decision D18, the fourth instance of the D9/D11/D15/D16 pattern).

### Step 7 -- Scale and robustness *(complete)*

Add an `iterparse`-based streaming path with element clearing so large finding aids parse in
bounded memory, and check it produces an identical tree to the in-memory path.
→ [reports/step-07-streaming.md](reports/step-07-streaming.md)

Delivered: {mod}`archival_structures.parsers.ead_stream` (`parse_ead_streaming`, `ParseReport`,
`strict` flag as specified), verified to build a tree structurally identical to
{func}`~archival_structures.parsers.ead_tree.parse_ead`'s on **all 47 finding aids** in the
corpus (a standing test, `TestCorpusEquivalence`, not a one-off check), 16 tests total.

**Measured, not assumed: `parse_ead_streaming` is slower and uses more peak memory than
`parse_ead` on every finding aid in the corpus, up to 106 MB** -- confirmed by ablation to be
`iterparse`'s inherent per-element Python-callback overhead (~2x a plain full-DOM parse before
any of this module's own logic runs), not a flaw in this implementation. `parse_ead` stays the
recommended default; `parse_ead_streaming` is documented as opt-in for a file already known or
suspected to be large enough to risk it, not a blanket replacement (decision D22) -- the benefit
is for documents too large to build as one DOM *at all*, a size nothing in the current corpus
reaches, so it cannot be demonstrated on it, only reasoned from the user's own experience that
such files exist elsewhere.

Two real, if narrow, bugs were found only by comparing against the whole corpus rather than
hand-written fixtures: the textbook `iterparse` memory-freeing idiom (clear a finished element,
drop its already-finished preceding siblings) silently deletes a component's own `<did>` once
that component has nested component children (D21), and `id()` reuse after clearing an element
let a later, unrelated element inherit a stale sibling-index counter (found via `NL-AsdSAA_5074`
and 13 other finding aids showing index-label mismatches with no corresponding content
difference). Both fixed; both now guarded by dedicated fixtures as well as the corpus-wide check.
`_parse_did_element`/`_extract_did_fields`/`_extract_other_children` are shared between both
parse paths (decision D20) so they cannot silently drift apart on what a `<did>` field is.

### Step 8 -- Migration and retirement *(complete, partially)*

Update callers, retire {class}`archival_structures.parsers.read.EADReader` and the
BeautifulSoup dependency it implies, and refresh [ead_mets_parsing.md](../ead_mets_parsing.md).
→ [reports/step-08-migration.md](reports/step-08-migration.md)

**`EADReader` is removed.** Its one real, currently-functional caller
(`download_thumbnails.py`'s `get_archive_file_info`) is migrated onto
{func}`~archival_structures.parsers.ead_tree.parse_ead_string` (added this step -- `ead_tree`
only had a file-path entry point before a real in-memory caller needed the other one) +
{func}`~archival_structures.parsers.ead_fields.identify_inventory_number` +
{func}`~archival_structures.parsers.ead_digital.parse_dao`, verified importable. Two unrelated
dead-code paths (an unreachable method referencing an undefined variable; an unused
`EADReader()` instantiation) were found and removed while migrating the file, not ported forward.

The plan's own caller list needed correcting: `make_iventory_paragraphs.py` and
`archival_structures/datasets/` were checked directly and found to reference the legacy EAD API
**not at all** -- never real callers.

Four notebooks reference `ead_parser`. Two already use a stale import path
(`archival_structures.ead_parser`, missing `.parsers.`) predating this rewrite, with no currently
-working behaviour to migrate. The other two are substantial, execution-dependent analyses --
one references a file path in a sibling repository not present in this checkout -- that cannot be
verified by running them here; migrating their cells without being able to see that the output is
still correct would risk a notebook that looks migrated and silently isn't. Left deliberately
unmigrated (decision D23), which the plan's own report ask explicitly allows for.

## Test corpus

| Finding aid | Repository | Size | Notable structure |
| --- | --- | --- | --- |
| `NL-AsdSAA_30312` | Stadsarchief Amsterdam | 496 KB | series > subseries > file; 1,172 assets |
| `NL-AsdSAA_395` | Stadsarchief Amsterdam | 5.7 MB | 287 files directly under `<dsc>` |
| `NL-AsdSAA_5074` | Stadsarchief Amsterdam | 2.5 MB | subseries nested 3 deep |
| `NL-AsdSAA_5075` | Stadsarchief Amsterdam | 106 MB | scale; second IIIF URL shape |
| `NL-AsdSAA_5076.NWD` | Stadsarchief Amsterdam | 812 KB | 1,910 assets |
| `NL-AsdSAA_834` | Stadsarchief Amsterdam | 4.6 MB | subseries nested **8** deep |
| `NL-HaNA_1.01.02` | Nationaal Archief | 18 MB | 7,024 `item`, 2,222 `otherlevel="subfile"`, `series` nested inside `subseries` |
| `NL-HaNA_1.04.02` | Nationaal Archief | 18 MB | 16,548 files; `filegrp` nested in `filegrp` |
| `NL-HaNA_1.05.06` | Nationaal Archief | 912 KB | **no series at all**; `item` under `file` |
| `NL-HaNA_2.10.36.22` | Nationaal Archief | 60 KB | smallest; `dao` per file |

Step 1 extended this with 30 more Dutch finding aids across 11 further repositories (manual
collection from `opendata.archieven.nl`, decision D6); the live inventory is
`data/metadata/eads/corpus.tsv`, rebuildable with
`python scripts/harvest_eads.py data/metadata/eads --describe-only`.

**The corpus is now deliberately international (decision D19), not Dutch-only.** Two UK finding
aids are registered: `birmingham_archives_and_heritage_serivce-gb143-ms2141` (Birmingham
Archives and Heritage Service, 224 components, `subfonds` > `series` > `subseries` > `file` >
`item`) and `oxford_house_archives-gb3624-oh` (Oxford House Archive, 853 components). Both use
slash-hierarchical reference numbers (`"MS 2141/A/1/1"`, `"OH/1/2/1/1"`) that no current
inventory-number rule resolves (open question in [status.md](status.md), same category as D13).
Further international harvesting hit the same access friction D6 found domestically, one step
worse: see [reports/step-01-corpus.md](reports/step-01-corpus.md)'s addendum for what was tried
and a prioritised list of further candidates for manual collection.

## Out of scope

- METS parsing itself ({mod}`archival_structures.parsers.mets_parser`) beyond the
  cross-check in Step 5.
- Fetching IIIF manifests or images; the parser records URLs, it does not dereference them.
- EAD3. Everything in the corpus is EAD 2002 so far -- in either component spelling, with or
  without the default namespace, plus the Memorix nexus extension. No EAD3 has turned up yet in
  any country harvested; support for it stays a separate decision if one does.
