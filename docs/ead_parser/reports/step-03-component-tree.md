# Step 3 -- The component tree

> **Note (2026-10-13).** `NL-HaNA_1.01.02` numbers and line citations in this report were computed on the 2025-03-18 export of that finding aid (7,024 `item`s), now kept as `data/metadata/eads/superseded/NL-HaNA_1.01.02-2025-03-18.xml`. The current export, `data/metadata/eads/NL-HaNA_1.01.02.xml` (version 2026-09-09), has 7,026 `item`s and about 28.6k extra internal `<unitid>`s, so its numbers differ.

**Date:** 2026-09-27 · **Corpus:** 40 finding aids, 13 repositories (unchanged from Step 2) ·
**Code added:** {mod}`archival_structures.parsers.ead_tree` (`EADFindingAid`, `EADComponent`,
`UnitID`, `parse_ead`), `tests/test_parsers_ead_tree.py` (25 tests).

## Summary

> **The tree reaches 243,217 of 243,217 components — 100.0% — across all 40 finding aids,
> up from the legacy parser's 21.5% (Step 2). Zero exceptions. Zero partial coverage.**

That total includes `NL-AsdSAA_5075` (106 MB), which every prior step excluded on the
assumption that it could not be parsed in memory. It parses in 0.8s with 615 MB peak RSS and
reaches 100% coverage like everything else (§ Unexpected finding). The number to beat, set by
Step 2 at 208,926 components across 39 finding aids, is beaten with room to spare once the
fortieth is added back in.

No level-specific logic was needed to get here. `build_component` does not know what `"series"`
or `"Hinderwetvergunning"` mean; it treats every `<c>`/`<c01>`..`<c12>` the same way regardless
of `@level`, which is precisely why it doesn't need to know about the 22 free-text `otherlevel`
values or the 2,593 level-less components that made a level-enumerating design unbuildable
(Step 2, decision D8).

## Data analysis

### Coverage, before and after

| | Finding aids | Components | Reached | Recall |
| --- | ---: | ---: | ---: | ---: |
| Legacy parser (Step 2, 39 finding aids) | 39 | 208,926 | 44,951 | 21.5% |
| Tree (this step, 39 finding aids) | 39 | 208,926 | 208,926 | **100.0%** |
| Tree (this step, all 40 finding aids) | 40 | 243,217 | 243,217 | **100.0%** |

By level -- every one of the 27 distinct `@level` values Step 2 catalogued, now fully reached:

| Level | Source | Reached | Recall |
| --- | ---: | ---: | ---: |
| `file` | 118,305 | 118,305 | 100.0% |
| `otherlevel:Adres` | 35,729 | 35,729 | 100.0% |
| `otherlevel:Inschrijving` | 26,131 | 26,131 | 100.0% |
| `item` | 25,564 | 25,564 | 100.0% |
| `otherlevel:Dossier` | 8,620 | 8,620 | 100.0% |
| `otherlevel:Persoon op afbeelding` | 5,788 | 5,788 | 100.0% |
| `subseries` | 5,573 | 5,573 | 100.0% |
| `otherlevel:Rechtspersoon in vergunning` | 2,953 | 2,953 | 100.0% |
| `(none)` | 2,593 | 2,593 | 100.0% |
| `otherlevel:filegrp` | 2,590 | 2,590 | 100.0% |
| `otherlevel:subfile` | 2,222 | 2,222 | 100.0% |
| `otherlevel:Hinderwetvergunning` | 2,112 | 2,112 | 100.0% |
| `series` | 1,506 | 1,506 | 100.0% |
| *(15 more levels, 2--820 components each)* | 2,976 | 2,976 | 100.0% |
| **TOTAL** | **243,217** | **243,217** | **100.0%** |

(Full 27-row table reproducible with `coverage()` over `ead_tree.tree_parser_components`; the
legacy-parser comparison uses `ead_parser.legacy_parser_components`, both against the same
`ead_survey` harness, so the two rows above are directly comparable.)

Matching in `ead_survey.coverage` accepts any of a component's `<unitid>` values, and
`tree_parser_components` emits one `(level, unitid)` pair per `<unitid>` a component has (plus
`(level, None)` for the 2,593 with none) -- so a component with no usable identifier still
counts as reached rather than falling through a matching gap on either side.

### Unexpected finding: `NL-AsdSAA_5075` was never actually untouchable

Steps 0, 1 and 2 each excluded this 106 MB finding aid, citing memory concerns with the
`ElementTree`-based legacy parser. `ead_tree` uses `lxml` (as `ead_survey` already did, for
`.sourceline`), and:

```text
parsed in 0.8s, peak RSS 615 MB, n_components=34,291
coverage: 1.0s, recall=100.0%
```

Four steps of "excluded, too large" turn out to describe a property of `ElementTree`, not of the
file. This does not make Step 7 (scale and robustness) redundant -- `lxml.etree.parse` still
loads the whole document into memory, and a repository publishing something larger than 106 MB
is not implausible given the spread already in the corpus (43 MB, 37 MB, 25 MB) -- but it does
mean Step 7 is validating headroom, not fixing a known failure, and the corpus no longer has a
standing exception to remember when reporting totals.

### Extra-tag census: what Step 4/5 still have to interpret

Nothing under a `<did>` or a component is discarded; what Step 3 doesn't parse is retained
verbatim and counted, corpus-wide (including `NL-AsdSAA_5075`):

| `<did>` child (→ `did_extra`) | Count | Handled in |
| --- | ---: | --- |
| `unitdate` | 195,828 | Step 4 |
| `physloc` | 42,279 | Step 4 |
| `assets` (Memorix) | 42,133 | Step 5 |
| `dao` | 27,661 | Step 5 |
| `physdesc` | 17,774 | Step 4 |
| `accessrestrict` | 23 | Step 4 |
| `langmaterial` | 9 | Step 4 |
| `materialspec` | 1 | Step 4 |

| Other component child (→ `other_children`) | Count | Note |
| --- | ---: | --- |
| `odd` | 364,137 | free text |
| `controlaccess` | 25,614 | `genreform` etc. |
| `daogrp` | 14,263 | Step 5 -- the fourth digitisation-linking element (Step 2) |
| `scopecontent` | 8,627 | free text |
| `accessrestrict` | 6,360 | can also sit outside `<did>` |
| **`dsc`** | **5,562** | **see below -- not a hierarchy** |
| `phystech`, `separatedmaterial`, `altformavail`, `custodhist`, `relatedmaterial`, `acqinfo`, `processinfo`, `otherfindaid`, `bibliography`, `bioghist`, `arrangement`, `originalsloc` | 372 combined | free text / metadata |

Every count here is retrievable from the tree today (`component.did_extra['unitdate']`, etc.);
none of it is lost, all of it is deferred.

## Error analysis

### E1 -- A nested `<dsc>` inside a component is not a hierarchy, and the harness could have missed it the same way twice

`other_children['dsc']` (5,562 occurrences, concentrated in `NL-AmfAE_0001.01.ead.xml` at
5,231) was unexpected enough to check by hand before trusting the 100% figure. Both
`ead_survey.survey_ead` and `ead_tree.parse_ead` locate `<dsc>` with
`next(e for e in root.iter() if local_name(e.tag) == 'dsc')` -- the *first* one in document
order -- and neither descends into a `<dsc>` nested inside a component. If such a nested `<dsc>`
contained real components, **both instruments would silently agree on the wrong number**,
which is exactly the failure mode decision D9 exists to catch, and D9's independence (different
XML library, no shared import) does not help when the blind spot is identical by construction
rather than by coincidence.

It was checked directly rather than assumed:

```python
>>> [e for e in root.iter() if local_name(e.tag) == 'dsc']
# 5,232 <dsc> elements in NL-AmfAE_0001.01.ead.xml -- 5,231 of them nested inside a <c03>
```

```xml
<!-- NL-AmfAE_0001.01.ead.xml:35659, inside a level-less <c03> -->
<dsc type="othertype">
  <p>1 akte van attestatie betreffende het gewelddadig optreden van de schout
     tegen het St Aagtenklooster, (1559)</p>
</dsc>
```

Every nested `<dsc>` in the corpus (5,562 across 6 finding aids) contains only `<p>` --
this exporter convention uses `<dsc type="othertype">` as a free-text scope-note container, not
as a further breakdown into components. Confirmed exhaustively:
`components_under_nested_dsc == 0` for every nested `<dsc>` in every finding aid. The 100%
coverage figure holds, but it holds because this was checked, not because the two instruments
agreeing was taken as proof. `other_children['dsc']` keeps the raw elements (and their `<p>`
text) available regardless, so nothing is lost if a future finding aid uses this construct
differently.

### E2 -- The three previously-dropped constructs, now recovered

Worked examples of exactly what Step 0/1 found the legacy parser losing, now reached:

**Files directly under `<dsc>` (Step 0, finding E1).** `NL-HaNA_1.05.06` has 535 root
components (530 `file`, 5 `otherlevel:filegrp`) where the legacy parser found zero, because
`get_series` only yields `<c level="series">` children of `<dsc>`:

```python
>>> finding_aid = parse_ead('NL-HaNA_1.05.06.xml')
>>> len(finding_aid.roots)
535
>>> [r.level for r in finding_aid.roots[:3]]
['file', 'file', 'file']
```

**Components nested below `file` level (Step 0, finding E2).** `NL-HaNA_1.01.02` file `7474` --
titled only by its date range, `1600-1601`, with the substantive content entirely in its
children -- now yields all 49:

```python
>>> file_7474.children[:2]
item             : "Claes Cornelis van Saenredam - Octrooi voor apparaat om bij wind hout te
                    zagen"                                    (superseded/NL-HaNA_1.01.02-2025-03-18.xml:115790)
otherlevel:subfile: "Pouwels Corn. timmerman te Kromenie - Octrooi windmolen met 12 vleugels,
                    1600 december"                             (superseded/NL-HaNA_1.01.02-2025-03-18.xml:115798)
```

**The numbered/namespaced profile (Step 1, finding E1).** `NL-UtHUA_842-3` -- where
`ead_parser.read_ead` returns `None` and `get_inventory_info` raises `TypeError` -- parses
completely:

```python
>>> finding_aid = parse_ead('NL-UtHUA_842-3.ead.xml')
>>> finding_aid.namespace, finding_aid.component_style, finding_aid.n_components
('urn:isbn:1-931666-22-9', 'numbered', 305)
```

Also recovered, without special-casing: the `<opendata>` envelope (Step 2, finding E3, via
`find_ead_root`), and level-less depth-numbered components (Step 2, finding E4 -- e.g.
`NL-AmfAE_0001.01.ead.xml:35655`, a `<c03>` with a GUID `<unitid>` and no `@level`, which is now
just an ordinary component with `level == "(none)"`).

## What the tree provides to later steps

- `EADComponent.level` is a plain string (`"(none)"`, `"otherlevel:<value>"`, or the bare
  `@level`), never a branch target -- consistent with decision D8.
- `EADComponent.unitids` keeps every `<unitid>` with its attributes, undecided about which one
  is "the" inventory number -- Step 4's job, informed by Step 1/2's three-repository survey of
  disjoint conventions.
- `EADComponent.did_extra` / `other_children` keep every unparsed element, ready for Step 4
  (`unitdate`, `physdesc`, `physloc`) and Step 5 (`dao`, `daogrp`, Memorix `assets`).
- `.parent` / `.ancestors()` / `.level_path` give Step 6's views their ancestry without
  re-walking the tree.
- `tree_parser_components` is a working `ead_survey.coverage` adapter, so every later step's
  claim about coverage is checked the same way this one was, not asserted.

## Conclusions carried into the plan

1. Step 3's acceptance criterion is met: 100% coverage, all 40 finding aids, all 27 levels.
2. `NL-AsdSAA_5075` is no longer a standing exclusion; corpus totals in future reports should
   include it, and Step 7 is about headroom beyond 106 MB, not about this specific file.
3. A harness and a parser sharing a structural assumption (here: "the first `<dsc>` is the only
   one") can agree with each other while both being wrong. It was checked by hand this time;
   Step 4 onward should keep spot-checking rather than trusting agreement alone as proof.
4. `<dsc type="othertype">` nested in a component is a free-text container in this corpus, not a
   further hierarchy -- captured in `other_children['dsc']`, worth a one-line note if Step 4/5
   surfaces scope-note text, since this is where some of it is hiding.
