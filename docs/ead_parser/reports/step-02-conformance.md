# Step 2 -- Structural conformance harness

**Date:** 2026-09-27 · **Corpus:** 40 finding aids, 13 repositories (Step 1, extended by
manual collection) · **Code added:** {mod}`archival_structures.parsers.ead_survey`,
`scripts/survey_eads.py`, `tests/test_parsers_ead_survey.py` (29 tests).

## Summary

The instrument now exists, and its first reading is worse than Step 0 suggested. Measuring
coverage *per `@level`* rather than only at `file` level, across a corpus that is no longer two
exporters:

> **The current parser reaches 44,951 of 208,926 components — 21.5%.
> It raises `TypeError` on 27 of 40 finding aids, and scores 0% in 11 of 13 repositories.
> Outside `file` level it reaches nothing at all.**

Step 0's 94–100% recall figures were not wrong; they were measured on the two repositories the
parser was written against, at the one level it emits. Both restrictions are now lifted.

Three findings reshape the rewrite:

1. `@level="otherlevel"` carries a **free-text, uncontrolled vocabulary** — 22 distinct values
   in Dutch, from `Adres` to `Prentbriefkaart`. Any design that enumerates levels is unbuildable
   (§ E1).
2. **`file` is not the universal unit of description.** Two repositories have no `file`
   components whatsoever; their inventory numbers live at `Dossier` or `Hinderwetvergunning`
   (§ E2). The plan's `to_frame(level='file')` needs rethinking.
3. A **fourth document profile**: 16 of 40 finding aids arrive wrapped in an `<opendata>`
   envelope whose namespace differs from the `<ead>` inside it (§ E3). This one was found by the
   harness catching an error in *my own* Step 1 report.

## Data analysis

### Document profiles

| Namespace | Component spelling | Envelope | Finding aids |
| --- | --- | --- | ---: |
| `urn:isbn:1-931666-22-9` | numbered `<c01>`…`<c12>` | `<opendata>` | 16 |
| *(none)* | recursive `<c>` | bare `<ead>` | 13 |
| `urn:isbn:1-931666-22-9` | numbered `<c01>`…`<c12>` | bare `<ead>` | 11 |

The 13 bare/recursive documents are exactly the Nationaal Archief and Stadsarchief Amsterdam
finding aids the parser was written against. **27 of 40 — every other repository — use the
namespaced, depth-numbered profile it cannot read.**

### Component levels across the corpus

27 distinct `@level` values, 208,926 components:

| Level | Count | Repositories |
| --- | ---: | ---: |
| `file` | 118,305 | 12 |
| `otherlevel:Adres` | 35,729 | 4 |
| `otherlevel:Inschrijving` | 26,131 | 1 |
| `item` | 25,564 | 2 |
| `otherlevel:Dossier` | 8,620 | 1 |
| `otherlevel:Persoon op afbeelding` | 5,788 | 1 |
| `subseries` | 5,573 | 11 |
| `otherlevel:Rechtspersoon in vergunning` | 2,953 | 1 |
| **`(no @level at all)`** | **2,593** | **3** |
| `otherlevel:filegrp` | 2,590 | 11 |
| `otherlevel:subfile` | 2,222 | 1 |
| `otherlevel:Hinderwetvergunning` | 2,112 | 1 |
| `series` | 1,506 | 12 |
| `otherlevel:Regest` | 820 | 2 |
| `otherlevel:Procesdossier` | 525 | 1 |
| `otherlevel:Tekening` | 499 | 2 |
| `otherlevel:Kaart` | 463 | 2 |
| `otherlevel:Lijst` | 450 | 1 |
| `otherlevel:Prent` | 310 | 2 |
| `otherlevel:Dia` | 258 | 1 |
| `otherlevel:Film` | 88 | 2 |
| `otherlevel:Geluid` | 50 | 1 |
| `otherlevel:Filmfragment` | 38 | 1 |
| `otherlevel:Bodemonderzoek` | 15 | 1 |
| `otherlevel:Perceel` | 9 | 1 |
| `otherlevel:Prentbriefkaart` | 4 | 1 |
| `otherlevel:Affiche` | 2 | 1 |

Only five levels (`series`, `subseries`, `filegrp`, `file`, and arguably `item`) are shared
across many repositories. The remaining 22 are local vocabulary.

### `<unitid>` shapes and digitisation links, corpus-wide

| `<unitid>` shape | Count |
| --- | ---: |
| `type=handle, audience=internal` | 179,890 |
| `type=guid` | 150,632 |
| *(bare — no type, label or identifier)* | 145,369 |
| `label=Inventarisnummer, @identifier` | 42,133 |
| `@identifier` | 35,561 |
| `type=urn:uuid, audience=internal` | 17,299 |
| `type=series_code` | 4,058 |
| `label=Rubriekscode, @identifier` | 3,013 |
| `type=obsolete` / `type=blank` / `type=obsolete, audience=internal` | 1,272 |

| Digitisation link | Count | Note |
| --- | ---: | --- |
| `<asset>` | 239,458 | Memorix, Amsterdam only (truncated at 10 per file — Step 0) |
| `<unitid type="handle">` | 179,890 | the majority mechanism, across most repositories |
| `<assets>` | 42,133 | Memorix container |
| `<dao>` | 27,661 | Nationaal Archief only |
| **`<daogrp>`** | **14,263** | **new — a fourth element, not in Step 1's list of three** |

`<dao>` is not the general mechanism; the handle `<unitid>` is. Step 5's scope grows again, to
four elements.

### Coverage by level

| Level | Source | Reached | Recall |
| --- | ---: | ---: | ---: |
| `file` | 86,081 | 44,951 | **52.2%** |
| every other level (26 of them) | 122,845 | 0 | **0.0%** |
| **TOTAL** | **208,926** | **44,951** | **21.5%** |

### Coverage by repository

| Repository | Source | Reached | Recall |
| --- | ---: | ---: | ---: |
| Noord-Hollands Archief | 56,361 | 0 | 0.0% |
| Nationaal Archief | 47,439 | 35,330 | 74.5% |
| Erfgoedcentrum Achterhoek en Liemers | 42,109 | 0 | 0.0% |
| Brabants Historisch Informatie Centrum | 28,008 | 0 | 0.0% |
| Stadsarchief Amsterdam | 10,855 | 9,621 | 88.6% |
| Archief Eemland | 7,977 | 0 | 0.0% |
| Collectie Overijssel, Zwolle | 5,245 | 0 | 0.0% |
| Het Utrechts Archief | 5,188 | 0 | 0.0% |
| Groninger Archieven | 3,706 | 0 | 0.0% |
| Gemeentearchief Zaanstad | 812 | 0 | 0.0% |
| Haags Gemeentearchief | 669 | 0 | 0.0% |
| Collectie Overijssel, Deventer | 397 | 0 | 0.0% |
| Gelders Archief | 160 | 0 | 0.0% |

`NL-AsdSAA_5075` (106 MB) is excluded throughout: it still cannot be parsed in memory by the
legacy path (Step 7).

## Error analysis: constructs the parser does not handle, ranked

Ranked by components lost. Every citation is `file:line` and checkable.

### E1 -- `otherlevel` is free text, not an enumeration (86,000+ components)

{func}`~archival_structures.parsers.ead_parser.parse_series` and its siblings test
`child.attrib['otherlevel'] == 'filegrp'` and raise `ValueError` on anything else. The corpus has
22 distinct `otherlevel` values and only one of them is `filegrp`:

```text
NL-ZlCO_0718.2.ead.xml:161   <c02 level="otherlevel" otherlevel="Hinderwetvergunning">
                             path: series > Hinderwetvergunning
                             unitids: ('221C32A4DC7047B8A74C1D590F263159', '886')
                             title:   "Zwolle, Aalvangersweg 7"

NL-ZlCO_0718.2.ead.xml:187   <c03 level="otherlevel" otherlevel="Adres">
                             path: series > Hinderwetvergunning > Adres
                             title:   "Zwolle, Aalvangersweg 7"

NL-DtcSARA_1661.ead.xml:129  <c02 level="otherlevel" otherlevel="Dossier">
                             path: series > Dossier
                             unitids: ('57AB2F88398FB387E06301C91F0A89B0', '28')
                             title:   "Verkiezingen - Processen-verbaal vernietiging
                                       verkiezingsstukken"
```

These are not exotic: `Adres` alone accounts for 35,729 components across four repositories.
A whitelist cannot be completed, so **the tree builder must accept any `otherlevel` value and
record it verbatim** — which is what D4 already decided and what this step quantifies.

### E2 -- `file` is not the universal unit of description (50,000+ components)

`NL-DtcSARA_1661` (Erfgoedcentrum Achterhoek en Liemers, 40,128 components) and
`NL-ZlCO_0718.2` (Collectie Overijssel Zwolle, 4,505) contain **zero** `file` components:

```text
NL-DtcSARA_1661   series > Dossier > Adres            item: 18,505   file: 0
NL-ZlCO_0718.2    series > Hinderwetvergunning > Adres              file: 0
```

The inventory-number-bearing component in `NL-DtcSARA_1661` is `Dossier` — `unitid` `28`, with
a GUID sibling. In `NL-ZlCO_0718.2` it is `Hinderwetvergunning` — `unitid` `886`.

This breaks an assumption the *plan* makes, not just the parser: `to_frame(level='file')` would
return nothing for these repositories. The unit of description has to be identified
functionally — "the component that carries an inventory number" — rather than by its level name.
Step 6 needs to account for this; noted as a new open question.

### E3 -- The `<opendata>` envelope (16 finding aids, 5 repositories)

Found by the harness disagreeing with my own Step 1 report, which listed these finding aids as
having no namespace. They do have one; the envelope does not:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<opendata>                                          <!-- no namespace -->
  <header><type>EAD</type>
    <rights>Public Domain Dedication</rights>
    <dataProvider>Gelders Archief</dataProvider>
  </header>
  <data>
    <ead audience="external" xmlns="urn:isbn:1-931666-22-9">     <!-- namespaced -->
```
*(`NL-AhGldA_2388.ead.xml:1`)*

Anything reading the document root's tag reports the wrong namespace, and anything looking for a
bare `<ead>` root misses the document entirely.
{func}`~archival_structures.parsers.ead_harvest.find_ead_root` now locates the `<ead>` wherever
it sits, and both the survey and `describe_ead` go through it. The Step 1 report's namespace
column was wrong for these 16 files; this report's profile table supersedes it.

The lesson is about the instrument, not the parser: a harness that shares an assumption with the
code under test cannot detect that assumption. `ead_survey` is deliberately independent of
`ead_parser` and uses a different XML library, which is why the disagreement surfaced at all.

### E4 -- Components with no `@level` at all (2,593 components, 3 repositories)

```text
NL-AmfAE_0001.01.ead.xml:35655  <c03>  path: series > subseries > (none)   2,589 of these
NL-HaHGA_0843-01.ead.xml:3380   <c01>  path: (none)
NL-UtHUA_850.ead.xml:3155       <c04>  path: series > subseries > subseries > (none)
```

All carry a GUID `unitid` and an empty `<unittitle>`. `@level` is optional in EAD 2002, and
`parse_series`'s `child.attrib['level']` would raise `KeyError` on every one. The depth-numbered
element name (`c03`) still gives the hierarchy, so these are recoverable — the level is simply
unknown, and must be representable as unknown rather than as a crash.

### E5 -- Nesting reaches 11 levels

| Depth | Finding aid | Repository |
| ---: | --- | --- |
| 11 | `NL-HlmNHA_2232` | Noord-Hollands Archief |
| 9 | `NL-AsdSAA_834` | Stadsarchief Amsterdam |
| 8 | `NL-HaNA_1.01.02`, `NL-HaNA_1.04.02`, `NL-AmfAE_0001.01` | — |

Step 0 found 8; the wider corpus reaches 11, within EAD 2002's `c12` limit but far past the
`max_subseries_depth=2` default. Confirms D2 with a larger margin.

## What the harness provides to later steps

- `survey_ead(path)` → tags, attributes, levels, ancestry paths, `<did>` and `<unitid>` shapes,
  digitisation links, per-component `file:line` citations. Never raises: a malformed document
  records a `parse_error` and surveys as empty.
- `CorpusSurvey.unique_to_one_repository(...)` → the over-fitting risks, computed rather than
  eyeballed.
- `coverage(survey, adapter)` → per-level recall for any parser, with cited examples of what it
  missed. A parser that *raises* is measured as zero rather than propagating, because that is
  what a caller gets. Matching accepts any of a component's `<unitid>`s, since repositories
  disagree about which one is canonical.
- `scripts/survey_eads.py` → all of the above from the command line.

**The number to beat: 21.5%.** Step 3's acceptance criterion (100% component coverage at every
level) is measured by exactly this harness, and `tests/test_parsers_ead_survey.py` pins the
harness itself against hand-written fragments for all four document profiles.

## Conclusions carried into the plan

1. No level enumeration anywhere: `otherlevel` is free text with 22 values and counting (E1).
2. `@level` is optional — an unlevelled component is normal, not an error (E4).
3. The unit of description is not always `file`; identify it by carrying an inventory number,
   not by name (E2). This changes Step 6's view design.
4. `find_ead_root` must be used everywhere, including in a fixed `read_ead` (E3).
5. Four digitisation-linking elements, not three: `<daogrp>` joins `<dao>`, Memorix assets and
   handle `<unitid>`s.
6. Depth reaches 11.
