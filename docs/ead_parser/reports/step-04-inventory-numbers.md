# Step 4 -- `<did>` field extraction and inventory-number identification

> **Note (2026-10-13).** `NL-HaNA_1.01.02` numbers and line citations in this report were computed on the 2025-03-18 export of that finding aid (7,024 `item`s), now kept as `data/metadata/eads/superseded/NL-HaNA_1.01.02-2025-03-18.xml`. The current export, `data/metadata/eads/NL-HaNA_1.01.02.xml` (version 2026-09-09), has 7,026 `item`s and about 28.6k extra internal `<unitid>`s, so its numbers differ.

**Date:** 2026-09-27 · **Corpus:** 40 finding aids, 13 repositories (unchanged from Step 2/3;
see § Note on two out-of-scope files for two additional UK finding aids found in the corpus
directory but not included in the figures below) · **Code added:**
{mod}`archival_structures.parsers.ead_fields` (`identify_inventory_number`, `parse_unitdate`,
`parse_physdesc`, `parse_physloc`, `parse_controlaccess`), a small addition to
{mod}`archival_structures.parsers.ead_tree` (`EADComponent.unittitle_element`, needed to recover
a date embedded inside `<unittitle>`), a bugfix to
{mod}`archival_structures.parsers.ead_start_end_year`, `tests/test_parsers_ead_fields.py` (25
tests), `tests/test_parsers_ead_start_end_year.py` (9 tests, none existed before).

## Summary

> **The legacy parser's identification rule and the new one disagree on 125,171 of 233,952
> components with a `<unitid>` -- 53.5% -- almost entirely (119,531 cases) because the legacy
> rule picks a GUID, UUID or URL instead of the real identifier. At the actual unit-of-description
> level, the new rule resolves 98.8-100% correctly in 11 of 13 repositories.**

The legacy parser's `re.match(r"\d+", unit['unitid'])` -- "the first `<unitid>` in the list that
starts with a digit" -- was never really an inventory-number rule; it was a coincidence that
happened to work on the two exporters it was written against. On the wider corpus it is actively
dangerous: **29% of all `file` components (34,480 of 118,946) carry a `type="guid"` `<unitid>`
that starts with a digit and is listed before the real identifier**, because a 32-character hex
string beginning with `0`-`9` is unremarkable. `identify_inventory_number` replaces it with four
explicit rules, each grounded in a measured convention, that report which one fired.

## Data analysis

### The GUID hazard, quantified

```python
>>> component.unitid_values()
['67A31B2F3A94451C852E5545242E6C1B', '2', 'https://permalink.geldersarchief.nl/67A31B2F…']
>>> legacy_style_identify(component)          # first digit-leading unitid, ignoring @type
'67A31B2F3A94451C852E5545242E6C1B'            # <- a GUID, not an inventory number
>>> identify_inventory_number(component).inventory_number
2
```

| | Count |
| --- | ---: |
| `file` components corpus-wide | 118,946 |
| ...with a `type="guid"` `<unitid>` starting with a digit, listed first | 34,480 (29.0%) |
| Components with ≥1 `<unitid>`, corpus-wide | 233,952 |
| Legacy vs. new: disagree | 125,171 (53.5%) |
| ...of which the legacy pick was specifically a GUID/UUID/URL value | 119,531 |

The legacy rule was accidentally safe on the two-repository corpus it was designed for --
Amsterdam and the Nationaal Archief don't put a GUID-typed `<unitid>` before the real one. Nine
of the eleven repositories added since (Step 1) do, via the `opendata.archieven.nl`/De Ree
export convention.

### Rule-firing frequency, corpus-wide

| Rule | Count | Share |
| --- | ---: | ---: |
| `none` (no rule matched) | 104,737 | 44.8% |
| `shape:only-candidate` | 51,541 | 22.0% |
| `label:inventarisnummer` | 42,133 | 18.0% |
| `identifier:untyped` | 35,541 | 15.2% |
| `type:ABS` | 0 | 0.0% |
| `shape:ambiguous` | 0 | 0.0% |

Two results worth being explicit about: **`type="ABS"` -- the legacy parser's own rule -- never
fires once in the 40-finding-aid corpus.** It is kept (a documented Nationaal Archief convention
that may appear in finding aids not yet in the corpus) but is currently dead code, proven so by
measurement rather than by inspection. And the tie-break logic for `shape:ambiguous` is
exercised only by the test suite's synthetic fixture -- the real corpus never produces a genuine
tie between two equally-valid shape candidates.

The raw 44.8% `none` rate is not the number that matters, and reporting it alone would be
misleading -- see below.

### Resolution at the actual unit-of-description level

`identify_inventory_number` runs over *every* component with a `<unitid>`, including `series`
and `subseries` nodes that structurally have no reason to carry an independent inventory number
(their `<unitid>` is a hierarchy-position code, e.g. `series_code="2.1.1."`). Reporting `none`
for those is correct, not a gap. What matters is resolution at each repository's actual unit of
description -- `file` for most, but not all (Step 2's open question):

| Repository | Unit level | n | Resolved | Rate |
| --- | --- | ---: | ---: | ---: |
| Stadsarchief Amsterdam | `file` | 42,133 | 42,133 | **100.0%** |
| Collectie Overijssel, Deventer | `file` | 378 | 378 | **100.0%** |
| Gemeentearchief Zaanstad | `file` | 690 | 690 | **100.0%** |
| Gelders Archief | `file` | 7 | 7 | **100.0%** |
| Noord-Hollands Archief | `file` | 25,967 | 25,896 | 99.7% |
| Het Utrechts Archief | `file` | 4,053 | 4,020 | 99.2% |
| Haags Gemeentearchief | `file` | 577 | 574 | 99.5% |
| Nationaal Archief | `file` | 36,081 | 35,663 | 98.8% |
| Groninger Archieven | `file` | 3,483 | 3,441 | 98.8% |
| Erfgoedcentrum Achterhoek en Liemers | `otherlevel:Dossier` | 8,620 | ~8,596 † | ~99.7% † |
| Archief Eemland | `file` | 3,843 | 2,631 | **68.5%** |
| Collectie Overijssel, Zwolle | `otherlevel:Kaart` (map collection) | 451 | 386 | 85.6% |
| Brabants Historisch Informatie Centrum | `otherlevel:Inschrijving` | 26,131 | 0 | **0.0%** |

† Erfgoedcentrum's `Dossier` level resolves cleanly (`28`, `29`, `30`, ...); its `item`
sub-level (18,529 components) uses relative position markers (`-1`, `-2`, `-3`, ...) rather than
independent identifiers, and correctly returns `none` for those -- they are sequence positions
within a `Dossier`, not inventory numbers of their own, the same relationship as `Prent` under
`file` for Gelders Archief.

**11 of 13 repositories resolve at 98.8-100%.** The two real exceptions were investigated by
hand rather than left as a headline number:

**Brabants Historisch Informatie Centrum -- 0%, and correctly so.** Every `otherlevel:Inschrijving`
component's only `<unitid>`s are a GUID and a proxy URL:

```python
>>> component.unitid_values()
['25E7AD8FD8064FA2803EBAF97C773DF2',
 'https://proxy.archieven.nl/235/25E7AD8FD8064FA2803EBAF97C773DF2']
```

Its parent `subseries` is the same. `NL-HtBHIC_SVO_1595_1649` ("Staten van Oorlog") is a
register of entries with no human-legible reference number assigned to individual entries at
all in this export -- a property of the source, not a gap in the rules. `none` is the honest
answer; inventing one would be worse.

**Archief Eemland -- 68.5%, and this one *is* a rule gap.** The unresolved 31.5% are
hyphen-separated numeric codes:

```python
>>> component.unitid_values()
['79EFF2F86E61405FB0779D64E4050EC3', '3818-059', 'https://proxy.archieven.nl/28/79EFF2F8…']
```

`InventoryNumber`'s tokenizer (`\d+|[A-Za-z]+`) has no token for `-`, so `"3818-059"` raises
`InvalidInventoryNumberError` and falls through to `none`. The same construct affects Collectie
Overijssel Zwolle's map collection (`"10-1"`, `"10 - 10"`, 65 of 451 `Kaart` components, 14.4%).

This is a real, cross-repository gap, and **it is not fixed here.** `-` is not a free signal:
Stadsarchief Amsterdam and the Nationaal Archief use it as a *range* separator between two whole
inventory numbers at filegroup level (`"44-45"`, `"1166-1167"` -- two files, not one compound
identifier), while Archief Eemland's `"3818-059"` is a single compound identifier. Teaching
`InventoryNumber` to accept `-` would risk conflating these two meanings, and `InventoryNumber`
is shared project infrastructure used beyond this parser -- changing its tokenizer is not a call
to make inside this rewrite without knowing what else depends on its current behaviour. Recorded
as an open question in [status.md](../status.md) rather than resolved by guessing.

### Embedded dates recovered

| Where the date was found | Count |
| --- | ---: |
| Ordinary `<did><unitdate>` | 195,828 |
| Embedded inside `<unittitle>` (no separate `<did><unitdate>` at all) | **36,699** |
| No date at all | 10,690 |

36,699 components -- including `NL-HaNA_1.01.02` file `7474`, Step 0's original example -- have
*no* date anywhere except inside their title. `EADComponent.unittitle_element` (added this step)
retains the raw `<unittitle>` element specifically so `parse_unitdate` can recover this, rather
than the date only existing as unstructured text inside `title`.

## Error analysis

### E1 -- A crash in already-existing, previously untested code, found by using it

`archival_structures.parsers.ead_start_end_year.get_begin_end_year` -- reused per the plan
rather than reimplemented -- raised `ValueError: too many values to unpack (expected 2)` the
first time `parse_unitdate` was run over the full corpus:

```python
>>> get_begin_end_year("1610/1625/1643")
ValueError: too many values to unpack (expected 2)
```

Source: `superseded/NL-HaNA_1.01.02-2025-03-18.xml:248426`, `<unitdate normal="1610/1625/1643">1610, 1625 en
1643</unitdate>` -- three discrete years, not a range, and `date_begin, date_end =
date.split("/")` assumed exactly two parts. No test file for this module existed before today.
Fixed to take the first and last part (`parts[0][:4], parts[-1][:4]`), which is exact for the
ordinary two-part case and a reasonable span for a longer list; `test_parsers_ead_start_end_year.py`
(9 tests, all new) now covers it, including the exact failing input.

This is the same lesson as Step 3's D11, in a smaller key: a function's contract can look solid
against the handful of inputs it was written for and still not be, and the wider corpus keeps
finding these rather than the code review that would have needed to imagine `"1610, 1625 en
1643"` in advance.

### E2 -- `Rubriekscode` almost satisfied the wrong rule

Amsterdam's `Rubriekscode` `<unitid>` (a classification code, not a file identifier) carries
both a `label` and an `@identifier`:

```xml
<unitid identifier="r1" label="Rubriekscode">1</unitid>
```

This exactly matches rule 3's surface condition ("untyped, has `@identifier`") that correctly
identifies Nationaal Archief files. Without the explicit `NON_IDENTIFYING_LABEL_RE` exclusion,
3,013 `Rubriekscode` components would have been misidentified as inventory numbers. Caught by
writing `test_rubriekscode_is_not_mistaken_for_the_label_rule` before trusting the rule against
real data, not by the real data itself -- worth naming, since it is exactly the kind of mistake
that would have shipped quietly otherwise.

## What Step 4 provides to later steps

- `identify_inventory_number(component) -> InventoryNumberResult` -- `.inventory_number` (an
  `InventoryNumber`, sortable), `.unitid` (which one), `.rule` (audit trail), `.candidates` (for
  `shape:ambiguous`). Never raises; `"none"` is a valid, honest outcome.
- `parse_unitdate`, `parse_physdesc`, `parse_physloc`, `parse_controlaccess` -- structured access
  to what Step 3 retained verbatim in `did_extra`/`other_children`.
- The per-repository "unit of description" table above is direct input to Step 6's open question
  (which level is "the file" when a repository has none).

## Note on two out-of-scope files

Two UK finding aids appeared in `data/metadata/eads/` during this step --
`birmingham_archives_and_heritage_serivce-gb143-ms2141.xml` and
`oxford_house_archives-gb3624-oh.xml` -- fetched by a separate script
(`download_sample_eads.py`, using the `Sickle` OAI-PMH client against Archives Hub UK) that is
not part of the Step 1 harvest workflow and not recorded in `corpus.tsv`. They are **excluded
from every figure above**: the plan, the corpus table, and every decision through D11 are scoped
to Dutch archival practice, and folding in a different national tradition's conventions without
that being a deliberate, discussed scope change would silently redefine what "the corpus" means.

They were, however, free to check structurally, and the result is worth recording rather than
ignoring:

- `ead_tree` reaches **100% component coverage** on both (224 and 853 components respectively) --
  confirming decision D10's level-agnostic design generalises past the corpus it was built from.
- Both introduce a level not seen in any Dutch finding aid: `subfonds` (18 components combined).
- `identify_inventory_number` resolves **0%** of either -- UK reference numbers are
  slash-hierarchical (`"MS 2141/A/1/1"`, `"OH/1/1/1"`), a fourth convention distinct from all
  three the rules were built from. This is the correct outcome given the current rules (`none`,
  not a wrong guess), but it is not support for the convention.

Whether to bring UK (or other non-Dutch) EAD into the corpus's scope is a decision for whoever
resumes this work, not one made here by quietly expanding what "13 repositories" means.
