# Step 6 -- Flat views

> **Note (2026-10-13).** `NL-HaNA_1.01.02` numbers and line citations in this report were computed on the 2025-03-18 export of that finding aid (7,024 `item`s), now kept as `data/metadata/eads/superseded/NL-HaNA_1.01.02-2025-03-18.xml`. The current export, `data/metadata/eads/NL-HaNA_1.01.02.xml` (version 2026-09-09), has 7,026 `item`s and about 28.6k extra internal `<unitid>`s, so its numbers differ.

**Date:** 2026-09-27 · **Corpus:** 40 finding aids, 13 repositories · **Code added:**
{mod}`archival_structures.parsers.ead_views` (`to_frame`, `unit_of_description_frame`,
`legacy_compatible_frame`), `tests/test_parsers_ead_views.py` (13 tests).

## Summary

> **`legacy_compatible_frame` reproduces every genuinely valid inventory number the legacy
> parser ever produced, across the 8 finding aids where Step 0 measured ≥94% legacy recall.
> Every one of the 69 rows it does not reproduce is a case where the legacy value was already
> wrong -- a database UUID or a malformed multi-part reference, not a real inventory number.**
> It additionally recovers 336 rows the legacy parser never produced at all (287 from Step 0's
> dsc-rooted-file construct, 49 from components its structural walk never reaches).

(The plan's Step 6 text said "the seven finding aids" -- Step 0's own table lists eight at
≥94% recall; corrected here rather than force-fitting the comparison to the wrong count.)

Step 2/4's open question -- "which component is the unit of description, when it isn't
`file`?" -- is answered functionally rather than by level name: `unit_of_description_frame`
selects components with a **resolved inventory number** (Step 4), preferring the deepest such
component in any chain. Finding this required fixing a real, if never-yet-triggered, bug in the
row-selection logic itself (§ E1), and finding *that* bug's fix uncovered a second, real,
already-triggered bug in Step 4's rule set (§ E2) that had been silently suppressing 235 correct
Nationaal Archief inventory numbers.

## Data analysis

### The row-level diff, by finding aid

| Finding aid | Legacy rows | New rows | Missing (legacy-only) | Extra (new-only) |
| --- | ---: | ---: | ---: | ---: |
| `NL-AsdSAA_30312` | 119 | 119 | 0 | 0 |
| `NL-AsdSAA_395` | 4,560 | 4,847 | 0 | **287** |
| `NL-AsdSAA_5074` | 1,641 | 1,641 | 0 | 0 |
| `NL-AsdSAA_5076.NWD` | 209 | 209 | 0 | 0 |
| `NL-AsdSAA_834` | 2,926 | 2,926 | 0 | 0 |
| `NL-HaNA_1.04.02` | 16,501 | 16,447 | **54** | 0 |
| `NL-HaNA_2.10.36.22` | 76 | 76 | 0 | 0 |
| `NL-HaNA_1.01.02` | 17,899 | 17,936 | **15** | **49** |
| **Total** | **42,492** | **42,759** | **69** | **336** |

Five of eight finding aids match exactly. The three that don't are explained below, individually
where the plan asks for it (`NL-HaNA_1.04.02`'s 54 missing rows are homogeneous enough to explain
as one class; `NL-HaNA_1.01.02`'s 15 are shown individually).

### Missing: `NL-AsdSAA_395` and the file-under-`<dsc>` construct -- fully covered

Zero missing here confirms the easy direction of the acceptance criterion. The 287 *extra* rows
are Step 0's original finding, now closed: files sitting directly under `<dsc>` as siblings of
`<c level="series">`, which `ead_parser.get_series`'s series-only loop cannot reach at all. The
new parser roots every `<dsc>` child, whatever its level (Step 3), so these were never lost to
begin with.

### Missing, explained individually: `NL-HaNA_1.04.02` (54 rows)

Every one of the 54 has the identical shape:

```python
>>> component.unitid_values()
['---', 'http://hdl.handle.net/10648/00d906d4-48bf-451b-aee2-9d7a3052125c',
 '00d906d4-48bf-451b-aee2-9d7a3052125c']
```

Legacy's `re.match(r"\d+", unitid)` only requires the string to **start** with a digit -- not
that the whole string be digits -- so a bare, untyped, dashed record UUID beginning with `0`-`9`
(roughly half of them, by chance) passes the test and gets returned as `inventory_num`. It is a
database identifier, not an inventory number, on every one of the 54: none of them has an
`InventoryNumber`-parseable shape at all (the internal hyphens have no token in
`InventoryNumber`'s tokenizer, so they fail the shape rule cleanly, correctly, on their own
merits -- not because of any `type`-based exclusion). `identify_inventory_number` correctly
returns `"none"` for all 54; reproducing legacy's value here would mean deliberately
reintroducing the exact hazard Step 4 quantified at 29% of `file` components corpus-wide.

### Missing, listed individually: `NL-HaNA_1.01.02` (15 rows)

| Legacy value | Why it is not a valid inventory number |
| --- | --- |
| `12548.17 (1)` .. `12548.17 (5)` (5 rows) | parenthetical sub-item suffix -- five *different* components share the prefix `12548.17`, each annotated `(1)`..`(5)`; there is no single inventory number here, five are needed |
| `12548.154 ` | trailing space |
| `12548.290A ` | trailing space (compare the *valid* `12548.290A`, without a trailing space, which a **different** component in the same finding aid carries correctly -- see next section) |
| `12548.52. 34` | embedded space after an extra `.` -- two numbers run together |
| `12584.107,109` | comma-separated pair -- two inventory numbers, not one |
| `12584.253-254` | hyphen-separated range -- two inventory numbers, not one (the general hyphen-ambiguity question from Step 4's D13, here in a plain-number context rather than Archief Eemland's compound-identifier one) |
| `12686*`, `12687*` | trailing asterisk (an archival convention marking something about the item -- restricted access, a note, a cross-reference -- not part of the number) |

Legacy's prefix-matching regex silently truncates every one of these to a leading digit run and
calls the result the inventory number, discarding the parenthetical, comma, or asterisk that
made the value not a clean single reference to begin with. `identify_inventory_number` declines
rather than guessing which truncation (if any) would have been the "right" one -- correctly, in
the report's judgement: a comma-joined `"12584.107,109"` names two files, and picking either
number silently would misrepresent which one a row is actually about.

### Extra: `NL-HaNA_1.01.02` (49 rows) -- components the legacy walk never reaches

Step 0 already measured this finding aid at 99.1% legacy recall (17,899 of 18,054 `file`
components) -- meaning roughly 155 components were unreachable by the legacy structural walk for
reasons unconnected to identification. The 49 "extra" rows here are exactly that population,
confirmed directly against the genuine legacy output (not re-derived from the identification
rule in isolation, which does not know whether the legacy walk ever reaches a component at all --
an earlier draft of this analysis made exactly that mistake and had to be corrected: see § E3).
Every one of the 49 has a perfectly ordinary, valid inventory number
(`12579.42`, `12579.100`, ...); the legacy parser simply never produces a row for them.

## Error analysis

### E1 -- The row-selection algorithm preferred the wrong end of a resolved chain

`unit_of_description_frame`'s first version excluded a resolved component if any **ancestor**
also resolved. For the ordinary case (a resolved `Dossier` under an unresolved `series`) this is
right. But if a `series` *also* happened to resolve (a bare, untyped, letter/digit unitid with no
excluding label -- not observed anywhere in the real corpus, 0 of 7,079 `series`/`subseries`
components resolve at all, but not something the code should get right only by chance), the
algorithm would keep the *shallow* node (the series) and drop the *specific* one (the file) --
backwards from what "unit of description" should mean. Fixed to exclude a resolved component if
any **descendant** resolves instead, so the deepest resolvable node in a chain always wins;
`tests/test_parsers_ead_views.py::test_a_resolved_series_with_a_resolved_child_yields_the_child_not_the_series`
exercises the corrected direction directly, since the real corpus cannot exercise it at all.

### E2 -- Step 4's `type="obsolete"` exclusion was repository-specific and wrong for two repositories

Building the row-diff surfaced 181 unexplained missing rows in `NL-HaNA_1.01.02` before
investigation (reduced to 15, above, once this was fixed). All 181 traced to one cause: their
resolved-looking value (`3714`, `4835`, ...) sits on a `<unitid type="obsolete">`, and Step 4's
`NON_IDENTIFYING_TYPES` excluded `"obsolete"` wholesale, based on one repository's usage
(`NL-AmfAE`: `"St90-832"`, genuinely superseded and hyphen-shaped). Checked properly this time,
type by type, repository by repository (§ table below): the Nationaal Archief and Erfgoedcentrum
Achterhoek en Liemers use `type="obsolete"` for perfectly ordinary, **current** plain-digit
inventory numbers -- 97%+ of both repositories' `obsolete`-typed values parse cleanly.

| Repository | `obsolete`-typed values | Shape-parseable | Example |
| --- | ---: | ---: | --- |
| Archief Eemland | 341 | 0 (0%) | `"St90-832"` |
| Erfgoedcentrum Achterhoek en Liemers | 260 | 251 (96.5%) | `"20"`, `"316"` |
| Nationaal Archief | 264 | 257 (97.3%) | `"423"`, `"9125"` |
| Gemeentearchief Zaanstad | 1 | 0 (0%) | `"AWAR/12/18"` |

The exclusion was not just wrong for two repositories, it was never *necessary* for the one it
was built from either: `"St90-832"` already fails to parse as an `InventoryNumber` (no token for
`-`), so the shape rule alone excludes it with no help from a type check. `"obsolete"` is removed
from `NON_IDENTIFYING_TYPES`; every other entry in that set (`guid`, `urn:uuid`, `handle`,
`blank`, `series_code`) was checked the same way, per repository, before being kept -- `guid`
values are consistently, universally 32-hex and already caught by the independent shape-based
GUID check regardless of the type exclusion; `urn:uuid`/`handle`/`blank` are self-excluding by
shape (dashes/URLs) in every repository that uses them; `series_code`'s rare parseable values
(`"1.1"`, bare `"1"`) occur exclusively at `series`/`subseries` level, where excluding them is
correct, not incidental.

This is the same lesson as D9/D11/D15/D16 for the fourth time: a claim ("`type=X` is never the
inventory number") generalised from one repository's data was wrong for others, and the fix was
found only by checking every repository's actual values, not by re-inspecting the reasoning.

### E3 -- A verification script's own bug, caught before it reached the report

The first pass at explaining `NL-HaNA_1.01.02`'s 49 "extra" rows used
`legacy_style_identify(component)` (Step 4's standalone re-simulation of the legacy heuristic) to
ask "what would legacy have picked here?", found plausible-looking values, and almost reported
them as a mismatch needing explanation. They were not: `legacy_style_identify` answers what the
identification rule *would* return *if the legacy structural walk ever reached this component*,
which it does not for these 49 (Step 0's recall gap). Checked directly against the real
`ead_parser.get_inventory_info` output (`'12579.42' in set(legacy['inventory_num'])` →
`False`), confirming these rows are simply new-only, exactly as expected, not a discrepancy at
all. Caught before it became a false claim in this report -- the same discipline D9/D11 ask for,
applied to this step's own tooling rather than to the parser under test.

## What Step 6 provides to later steps

- `to_frame(finding_aid, level=...)` -- for when the caller already knows which level they want.
- `unit_of_description_frame(finding_aid)` -- the functional "what is *the* thing being
  described" answer, correct for the 11-of-13-repository common case and the BHIC/Zwolle/
  Erfgoedcentrum exceptions alike, without a level-name special case anywhere.
- `legacy_compatible_frame(finding_aid)` -- a checked migration aid: Step 8 can point existing
  callers at it as a drop-in column-for-column replacement with a known, itemised list of the
  (all legacy-was-wrong) cases where output differs.

## Conclusions carried into the plan

1. Step 6's acceptance criterion is met on the corrected count of eight finding aids: every
   legacy-only row is individually explained and none represents a real inventory number the new
   parser fails to reproduce.
2. `unit_of_description_frame` resolves the Step 2/4 open question; it is exercised by a test the
   real corpus cannot exercise (E1), which stays in place as a guard for a future finding aid.
3. Step 4's `NON_IDENTIFYING_TYPES` no longer includes `"obsolete"`; this materially improves
   real output for two repositories (181 previously-suppressed, genuinely correct Nationaal
   Archief inventory numbers alone).
4. Hyphen ambiguity (Step 4's D13) recurs here in a new shape (`"12584.253-254"`, a range between
   two plain numbers) -- another data point for that open question, not a new one.
