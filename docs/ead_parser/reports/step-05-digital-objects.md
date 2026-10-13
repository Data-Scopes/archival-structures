# Step 5 -- Digital objects and assets

**Date:** 2026-09-27 · **Corpus:** 40 finding aids, 13 repositories · **Code added:**
{mod}`archival_structures.parsers.ead_digital` (`parse_dao`, `parse_daogrp`, `parse_assets`,
`parse_handle_urls`, `summarise_digitisation`), `tests/test_parsers_ead_digital.py` (15 tests).

## Summary

Four digitisation-linking mechanisms, corrected from Step 2's tally and, this time, checked
per repository before anything was written down:

| Mechanism | Count | Repositories |
| --- | ---: | --- |
| `<unitid type="handle">` | 179,890 | most -- the majority mechanism |
| Memorix `<nexus:assets>` | 42,133 lists / 239,458 scans | Stadsarchief Amsterdam only |
| `<dao>` | 27,661 | Nationaal Archief only |
| `<daogrp>` | 14,263 | 7 `opendata.archieven.nl`-family repositories |

**Correction to a claim this plan carried since Step 2:** `<daogrp>` was written up as a
Nationaal Archief construct ("`<dao>` and `<daogrp>`... Nationaal Archief: 27,661 and 14,263
respectively"). Checked properly this step: **the Nationaal Archief has zero `<daogrp>`
elements.** All 14,263 belong to the `opendata.archieven.nl` export family -- 95% of them
(13,603) at Brabants Historisch Informatie Centrum alone. `plan.md` is corrected; this is the
same class of mistake Step 3's D11 named (a plausible-sounding attribution that was never
actually checked against the bytes), now caught in the step meant to check it.

`<daogrp>` also turned out structurally different from what "a `<dao>`-like element" suggests:
it is an XLink *extended link group* -- a free-text `<daodesc>` note plus one `thumb`/`reference`
`<daoloc>` pair **per scan** -- making it a small per-component digitisation manifest, not a
single href.

## Data analysis

### Digitisation coverage at each repository's actual unit-of-description level

Following Step 4's method (a `file`-level-only query would have reported Brabants Historisch
Informatie Centrum -- which has no `file` components at all -- as having *zero* digitisation
links, which is wrong):

| Repository | Unit level | Mechanism(s) | No digital link |
| --- | --- | --- | ---: |
| Gelders Archief | `otherlevel:Prent` | `daogrp` + `handle` | 0.0% |
| Archief Eemland | `file` | `daogrp` + `handle` | 0.0% |
| Stadsarchief Amsterdam | `file` | `assets` | **42.9%** (18,068/42,133) |
| Erfgoedcentrum Achterhoek en Liemers | `item` | `handle` | 0.0% |
| Collectie Overijssel, Deventer | `file` | `handle` | 0.0% |
| Groninger Archieven | `file` | `handle` (+1 `daogrp`) | 0.0% |
| Haags Gemeentearchief | `file` | `handle` | 0.2% |
| Nationaal Archief | `file` | `dao` + `handle` | 0.0% |
| Noord-Hollands Archief | `file` | `handle` | 0.0% |
| Brabants Historisch Informatie Centrum | `otherlevel:Inschrijving` | `daogrp` + `handle` | 0.0% |
| Het Utrechts Archief | `file` | `handle` | 0.0% |
| Gemeentearchief Zaanstad | `file` | `handle` (+`daogrp`) | 0.0% |
| Collectie Overijssel, Zwolle | `otherlevel:Adres` | `handle` | 0.0% |

**Stadsarchief Amsterdam is the only repository with a substantial not-digitised fraction**
(42.9%) -- and it is measurable precisely because Amsterdam's `<nexus:assets>` is a per-file
presence/absence signal (§ next section), unlike a handle URL, which every Nationaal Archief and
most other repositories' files carry regardless of whether the item behind it is actually
digitised (a handle can resolve to a catalogue record with no images at all -- the corpus cannot
tell from the EAD alone, which is a real limit on what "0.0% no digital link" means for those
rows: it is 0.0% *no reference*, not necessarily 0.0% *not digitised*).

**Nationaal Archief: every file has a handle; only 76.6% also have a `<dao>`.** 27,661 of
36,081 files carry `<dao role="METS">`; the other 8,420 have a handle URL only. Whether the
8,420 are genuinely undigitised or simply lack a METS manifest for another reason is not
answerable from the EAD alone.

### Memorix assets: the truncation confirmed at full scale

Step 0 found every `<nexus:assets>` list in one finding aid capped at 10. Across the full
corpus:

| Assets listed | Files |
| --- | ---: |
| 1-9 | 235 |
| **10** | **23,830** |
| **Total with any assets** | **24,065 (99.0% truncated)** |

Confirms Step 0's finding was not an artefact of the one finding aid it was measured on.
`AssetList.truncated` (this step) makes the flag explicit on every parsed component rather than
requiring a caller to notice the number 10 is suspicious.

### `<daogrp>`: almost always one scan, with a structural exception

| Scans per `<daogrp>` | Count |
| --- | ---: |
| 1 | 14,104 (98.9%) |
| 3 | 1 |
| 8 | 1 |

The two multi-scan exceptions are both at `file` level in `NL-AhGldA_2388.ead.xml` -- a
complete per-page manifest for the file as a whole. The overwhelming 1-scan majority sits one
level down, at `otherlevel:Prent` (individual prints extracted from a file) and
`otherlevel:Inschrijving` (BHIC's register entries): each such sub-component points at exactly
the one image that is *it*, which is the expected shape, not a truncation.

### The sampled IIIF check

The plan asks whether "digitised according to assets" agrees with "digitised according to the
archive's IIIF service." A cross-check on page *counts* is not possible (Amsterdam and the
Nationaal Archief use disjoint mechanisms -- Step 1 already established this), so the check
actually available is narrower and was run as such: do the asset entries the EAD lists resolve
to real images at all? Five Amsterdam files were sampled at random (seed 42); three of their
first `<nexus:iiif>` URLs were fetched (one request per second, this project's harvester's own
politeness convention):

```text
1275281b-…  -> HTTP 200  3414 x 4344 px
1b49d64c-…  -> HTTP 200  2702 x 4093 px
9e09f81f-…  -> HTTP 200  2281 x 3324 px
```

All three resolve. The ten-asset preview is real and live, not stale metadata -- it is simply
incomplete, exactly as Step 0 characterised it. Recovering the *actual* page count (a IIIF
Presentation manifest fetched by record UUID, most likely) remains explicitly out of scope for
the parser, per the plan.

## Error analysis

### E1 -- A previous report's attribution, corrected

Covered in the Summary: `plan.md`'s Step 5 section attributed `<daogrp>` to the Nationaal
Archief, unverified, since Step 2. The actual per-repository count (this step) shows zero
`<daogrp>` in any Nationaal Archief finding aid. `plan.md` has been corrected in place, with a
note explaining what changed and why, rather than silently rewritten.

### E2 -- A `file`-level query would have hidden BHIC's digitisation entirely

Querying `summarise_digitisation` only at `c.level == 'file'` (the obvious first attempt) reports
Brabants Historisch Informatie Centrum -- 26,131 components, its whole presence in the corpus --
as having **zero** digital links, because it has zero `file` components at all (Step 2's finding,
carried forward). Its real unit of description, `otherlevel:Inschrijving`, is 100% covered by
`handle` and 52.1% additionally by a single-scan `daogrp`. Same fix as Step 4: query at the
component that actually carries the description, identified by where inventory numbers resolve
(Step 4's per-repository table), not by a level name assumed to be universal.

## What Step 5 provides to later steps

- `summarise_digitisation(component) -> DigitisationSummary` -- `.mechanisms` (which of the
  four are present), `.is_digitised`, `.n_scans_known` (a real count when one exists --
  `daogrp`/`assets` -- `None`, not `0`, when only a whole-object reference is present).
- `AssetList.truncated` makes Step 0's finding a checkable property on every parsed component,
  not something a caller has to remember.
- The per-repository unit-of-description + mechanism table is direct input to Step 6's flat
  `level='asset'` view.

## Conclusions carried into the plan

1. `<daogrp>` is not a Nationaal Archief construct; `plan.md` corrected.
2. Digitisation coverage must be measured at each repository's real unit of description, not at
   `file`, or a majority of the corpus's `<daogrp>` usage (and all of BHIC) is invisible.
3. Amsterdam is the one repository where "no digital link" is a meaningful, large (42.9%)
   fraction; elsewhere a handle URL's near-universal presence means "0% no link" measures
   *reference existence*, not necessarily *digitisation*, and the report says so rather than
   implying otherwise.
4. The ten-asset truncation is confirmed at full corpus scale (99.0% of Amsterdam files with any
   assets hit exactly 10) and is live, not stale (sampled IIIF check).
