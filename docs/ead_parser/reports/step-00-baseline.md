# Step 0 -- Baseline and structural survey

> **Note (2026-10-13).** `NL-HaNA_1.01.02` numbers and line citations in this report were computed on the 2025-03-18 export of that finding aid (7,024 `item`s), now kept as `data/metadata/eads/superseded/NL-HaNA_1.01.02-2025-03-18.xml`. The current export, `data/metadata/eads/NL-HaNA_1.01.02.xml` (version 2026-09-09), has 7,026 `item`s and about 28.6k extra internal `<unitid>`s, so its numbers differ.

**Date:** 2026-09-27 · **Corpus:** the 10 EAD files in `data/metadata/eads`
(`NL-HaNA` ×4, `NL-AsdSAA` ×6) · **Code under test:**
`archival_structures/parsers/ead_parser.py` at commit `a84fce2`, unmodified.

## Method

For each finding aid: count `<c>` elements in the source by `@level`, then run
{func}`~archival_structures.parsers.ead_parser.get_inventory_info` and compare. `recall` is
output rows ÷ source `<c level="file">` elements -- a generous measure, since it ignores
whether each row's *content* is right, which Step 4 takes up.

`NL-AsdSAA_5075` (106 MB) was excluded throughout: it exhausts memory on the
`ElementTree`-parses-everything path. That exclusion is itself a finding, and Step 7's problem.

## Data analysis: what the parser is up against

### Coverage of the current parser

| Finding aid | `file` in source | `item` in source | rows out | recall |
| --- | ---: | ---: | ---: | ---: |
| `NL-AsdSAA_30312` | 119 | 0 | 119 | 100.0% |
| `NL-AsdSAA_395` | 4,847 | 0 | 4,560 | **94.1%** |
| `NL-AsdSAA_5074` | 1,641 | 0 | 1,641 | 100.0% |
| `NL-AsdSAA_5075` | -- | -- | -- | *not run (106 MB)* |
| `NL-AsdSAA_5076.NWD` | 209 | 0 | 209 | 100.0% |
| `NL-AsdSAA_834` | 2,926 | 0 | 2,926 | 100.0% |
| `NL-HaNA_1.04.02` | 16,548 | 0 | 16,501 | 99.7% |
| `NL-HaNA_1.05.06` | 542 | 11 | **0** | **0.0%** |
| `NL-HaNA_2.10.36.22` | 77 | 0 | 76 | 98.7% |
| `NL-HaNA_1.01.02` | 18,054 | 7,024 | 17,899 | 99.1% |

No run raised an exception. Every loss above is silent.

### Hierarchy shapes

Ancestry paths from `<dsc>` to each `file`/`item`, most frequent first:

```text
NL-HaNA_1.05.06      file                                    530     <- no series at all
                     filegrp > file                           12
                     filegrp > file > item                     8
                     file > item                               3

NL-AsdSAA_395        series > file                          4560
                     file                                    287     <- file directly under dsc

NL-AsdSAA_834        series > subseries×4 > file            1400
                     series > subseries×5 > file             609
                     series > subseries×7 > file              58     <- 8 levels deep
                     series > file                             29

NL-HaNA_1.04.02      series > subseries×3 > filegrp > file  7953
                     series > subseries×2 > filegrp > file  3131
                     series > subseries×3 > filegrp > filegrp > file
                                                              55     <- nested filegrp

NL-HaNA_1.01.02      series > subseries×3 > filegrp > file > item
                                                            6379
                     series > subseries×3 > series > filegrp > file > item
                                                             496     <- series inside subseries
```

Component-level census across the corpus (excluding `5075`):

| level | count | handled by current parser |
| --- | ---: | --- |
| `file` | 44,963 | yes |
| `item` | 7,035 | **no** |
| `otherlevel="subfile"` | 2,222 | **no** |
| `subseries` | 1,238 | only the first 2 levels |
| `otherlevel="filegrp"` | 1,388 | yes (one level kept) |
| `series` | 314 | yes |

### Digital objects

The two repositories describe digitisation in entirely different ways, and neither is
currently extracted in full.

| Finding aid | files | with `<dao>` | with `<nexus:assets>` | assets total | median | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `NL-AsdSAA_30312` | 119 | 0 | 119 | 1,172 | 10 | 10 |
| `NL-AsdSAA_395` | 4,847 | 0 | 4,847 | 8,306 | 0 | 10 |
| `NL-AsdSAA_5074` | 1,641 | 0 | 1,641 | 3,807 | 0 | 10 |
| `NL-AsdSAA_5076.NWD` | 209 | 0 | 209 | 1,910 | 10 | 10 |
| `NL-AsdSAA_834` | 2,926 | 0 | 2,926 | 5,397 | 0 | 10 |
| `NL-HaNA_1.04.02` | 16,548 | 16,363 | 0 | 0 | -- | -- |
| `NL-HaNA_1.05.06` | 542 | 533 | 0 | 0 | -- | -- |
| `NL-HaNA_2.10.36.22` | 77 | 76 | 0 | 0 | -- | -- |
| `NL-HaNA_1.01.02` | 18,054 | 9,832 | 0 | 0 | -- | -- |

:::{admonition} The Amsterdam asset lists are truncated at 10
:class: warning

**No `<nexus:assets>` element in the entire corpus contains more than 10 `<nexus:asset>`
children, and no `asset_order` attribute anywhere exceeds `9`.** The distribution is bimodal
in a way that only truncation explains -- `NL-AsdSAA_834`: 2,357 files with 0 assets, 51 files
with 1--9, and 511 files with exactly 10.

`NL-AsdSAA_30312` inventory number 1 (a notarial protocol register covering 1730--1733, so
certainly hundreds of scans) lists exactly:

```text
A22796000001.jp2 … A22796000010.jp2
```

Sequential filenames, stopping dead at `000010`. The `<nexus:assets>` element carries only
`allowAssets`/`allowDownloads`/`allowReservations`/`allowScanning` attributes -- there is no
total count and no pagination token to recover the rest from.

This overturns the assumption that inline assets give the page sequence for an inventory
number. They give a **preview of the first ten scans**, which is enough to detect *that* an
inventory number is digitised and to seed a IIIF lookup, but not to enumerate its pages. The
full sequence has to come from the IIIF service or a separate manifest call. The plan's
Step 5 has been amended accordingly, and "do inline assets agree with METS?" is no longer a
meaningful cross-check -- the question is whether asset *presence* agrees with METS presence.
:::

Two IIIF URL shapes appear, so no single pattern can be assumed:

```text
NL-AsdSAA_30312  https://stadsarchiefamsterdam.memorix.io/resources/iiif/3/<uuid>/info.json
NL-AsdSAA_5075   https://stadsarchiefamsterdam.memorix.io/resources/records/media/<record-uuid>/iiif/3/<numeric-id>/info.json
```

## Error analysis

### E1 -- `NL-HaNA_1.05.06`: 542 files, 0 rows

{func}`~archival_structures.parsers.ead_parser.get_files_info` iterates
{func}`~archival_structures.parsers.ead_parser.get_series`, which yields only
`<c level="series">` children of `<dsc>`. This finding aid has none -- its `<dsc>` children are
530 `file`s and 5 `filegrp`s. The loop body never executes and an empty DataFrame comes back
with no warning.

The very first thing dropped:

```xml
<c level="file">
  <did>
    <unitid identifier="39276968">1</unitid>
    <unitid audience="internal" type="handle">http://hdl.handle.net/10648/667e3e76-…</unitid>
    <unittitle>Octrooi van de Staten-Generaal voor Gerrit Jacob Witsen, Jonas Witsen c.s.
               om te handelen op Nieuw-Nederland.</unittitle>
    <unitdate calendar="gregorian" normal="1614" era="ce">11 oktober 1614 (kopie 20e eeuw).</unitdate>
    <physdesc>1 stuk</physdesc>
    <dao audience="internal" role="METS" href="https://service.archief.nl/gaf/api/mets/v1/70d6d70d-…" />
  </did>
</c>
```

A fully described, digitised inventory number with a METS reference. The same cause accounts
for the 287 missing rows in `NL-AsdSAA_395` (94.1% recall), where files also sit directly
under `<dsc>`.

**Fix:** Step 3 -- treat every `<c>` under `<dsc>` as a root, whatever its level.

### E2 -- `item` and `subfile` components are never reached

{func}`~archival_structures.parsers.ead_parser.parse_file` handles a `<c>` child of a file by
recording `file_info['file']['level']` and nothing else -- it does not recurse. Every component
below file level is lost: 7,035 `item`s and 2,222 `subfile`s.

`NL-HaNA_1.01.02` file 7474 shows why this matters, and why it is not a tidy problem:

```xml
<c level="file">
  <did>
    <unitid identifier="39104160">7474</unitid>
    <unittitle><unitdate calendar="gregorian" era="ce">1600-1601</unitdate></unittitle>
  </did>
  <c level="item">
    <did><unittitle>Claes Cornelis van Saenredam - Octrooi voor apparaat om bij wind
                    hout te zagen</unittitle></did>
    <odd><p>Met twee tekeningen</p></odd>
  </c>
  <c otherlevel="subfile" level="otherlevel">
    <did><unittitle>Pouwels Corn. timmerman te Kromenie - Octrooi windmolen met 12 vleugels,
         <unitdate calendar="gregorian" era="ce">1600 december</unitdate></unittitle></did>
    <odd><p>Met twee tekeningen</p></odd>
  </c>
  …
</c>
```

The file itself has no title -- only a date range. All the descriptive content (individual
patents, each with its own applicant, subject and date) lives in the children. The current
parser emits one row for 7474 with an empty title and discards the substance.

Note that `item` and `subfile` appear here as **siblings inside one file, describing the same
kind of thing**. This is the evidence for the "`item` vs `subfile`" open question in
[status.md](../status.md): at least in this finding aid, the distinction looks like exporter
inconsistency rather than semantics. Step 2 should confirm across the corpus.

**Fix:** Step 3 (tree recursion) + Step 6 (`to_frame(level='item')`).

### E3 -- Subseries hierarchy truncated to 2 levels

`max_subseries_depth=2` is a default that the corpus contradicts. `NL-AsdSAA_834` inventory
number 3375 sits 7 subseries deep:

```text
series     2           HEINEKEN'S BIERBROUWMAATSCHAPPIJ (HBM) TE AMSTERDAM, 1873-1973 (1997)
subseries  2.2         Stukken betreffende bijzondere onderwerpen
subseries  2.2.5       Bedrijfsmiddelen
subseries  2.2.5.3     Personeel
subseries  2.2.5.3.4   Arbeidsvoorwaarden
subseries  2.2.5.3.4.1 Algemeen
subseries  VB          Correspondentie met bijlage van HBM betreffende de CAO
file       3375        (no title)
```

What the parser returns:

```text
series           HEINEKEN'S BIERBROUWMAATSCHAPPIJ (HBM) TE AMSTERDAM, 1873-1973 (1997)
subseries_1      Stukken betreffende bijzondere onderwerpen
subseries_2      Bedrijfsmiddelen
filegroup        None
file             None
unitdate         1925-1927
inventory_num    3375
```

The row counts as recall, but `Personeel > Arbeidsvoorwaarden > Algemeen > Correspondentie …
betreffende de CAO` -- everything that says what this document *is* -- is gone, and the file's
own title is empty, so the row carries no subject information at all.

Two further details this example exposes:

- the deepest subseries has `unitid` `VB`, not a dotted number, so the dotted-decimal pattern
  cannot be relied on to detect hierarchy depth;
- `<unittitle/>` is empty on this file, as it is on most files in the Amsterdam exports --
  the "Empty `<unittitle/>`" open question in [status.md](../status.md).

**Fix:** Step 3 (no depth cap in the parse) + Step 6 (dynamic ancestry columns).

### E4 -- Digitisation information is thrown away

`get_inventory_info`'s `mets_file` column takes `dao[@role='METS']/@href` from the *first*
`<dao>` and nothing else -- no handle `<unitid>`, no non-METS `<dao>`, and, on the
`ElementTree` path, no `<nexus:asset>` at all. For the six Amsterdam finding aids that means
the `mets_file` column is `None` on every row, and 20,592 asset descriptions (in the five that
could be measured) are invisible.

`archival_structures.parsers.read.EADReader._get_file_assets` does extract them -- but on a
BeautifulSoup `Tag` API, unreachable from `read_ead`'s `ET.Element` output, with no tests and
one scratch-notebook caller.

**Fix:** Step 5.

### E5 -- Silent drops vs loud raises

The module docstring states the design intent: *"unrecognised child tags generally raise
`ValueError` so a new EAD structural variant surfaces as an error rather than silently losing
data."* That is a good principle, applied to only half the problem. Unrecognised **tags** raise;
unrecognised **levels**, and levels the walk simply never visits, disappear without a word.
E1--E4 are all in the second category, and together they account for every byte of data loss
measured here -- while no run in this survey produced a single error.

**Fix:** Step 3/Step 4 (`ParseReport` with a `strict` flag) -- the diagnostics channel has to
cover structure, not just tags.

## Conclusions carried into the plan

1. The tree walk must be level-agnostic and rooted at `<dsc>`'s children, not at `series`
   (E1, E2, E3).
2. Depth must not be a parameter of the parse, only of a view (E3).
3. Coverage has to be measured per level, not just for `file`, or E2 stays invisible (E5).
4. Inline `<nexus:asset>` lists are a 10-scan preview, not a page sequence -- the model must
   record them as such, and `assets_truncated` is a flag the output needs to carry.
5. `NL-AsdSAA_5075` needs a streaming path before it can be surveyed at all (Step 7).
