# NL-AsdSAA 5075 (Amsterdam notarial archive): Golden Agents survey (2026-11-11)

What is available for deed-level work on archive 5075, checked in a chat session on 2026-11-11 (nothing staged yet).
Per-inventory list: `NL-AsdSAA_5075_golden_agents_inventories.tsv` (the 254 PageXML folders that have ground truth).

## Sources

| What | Where | Size |
|---|---|---|
| PageXML (Transkribus, PAGE 2013, exported Dec 2020–Apr 2021) | `/Volumes/T7_Data/Data/PageXML/Golden_Agents/inventories/` | 1,364 folders, 605,971 pages, 41 GB |
| EAD | `../language-of-the-archive/eads/NL-AsdSAA_5075.ead.xml` | 106 MB, 32,224 file-level `<c>`, 1578–1930 |
| Deed begin/end marks | `/Volumes/T7_Data/Data/claude-projects-external/golden-agents-notarial-coordinates/` | 2.2 GB in all; `data/records.csv` 74 MB |

The ground truth is *Golden Agents – Coördinaten Personen/Locaties Notarieel Archief Amsterdam* v1.0, doi:10.5281/zenodo.7387918,
CC BY 4.0 (van Wissen, Reinders & van den Heuvel 2022), from the VeleHanden project Alle Amsterdamse Akten, exports up to
autumn 2022. `records.csv` has one row per deed: deed URI, begin `x,y` and begin scan name, end `x,y` and end scan name. The
rest of the folder (persons, locations, Turtle, raw Memorix exports, mappings) is not needed for deed boundaries.

## How they join

- **Folder names** are `<inventory>_<scan series>` (e.g. `10021_NOTA00961`), files `<series><6-digit scan>.xml`. 5 folders don't
  parse (`1157_29824`, `A16098`, `NOTI01181`, `VHBI00002`, `test_LA_bloecke`); `KLAD0190` and `KLAB04762` each have 2–3 folders,
  one of them empty.
- **EAD → series:** each inventory's `nexus:assets` lists its first scans (`KLAB05453000001.jp2`), so the EAD maps scan
  series to inventory numbers (33,638 series). 1,351 of the 1,364 folders agree with the EAD; 3 have a suffix mismatch
  (`1292_1645` vs 1292, `481A_I` vs 481A, `8711` vs 8711A); 10 series aren't in the EAD. The EAD lists only a sample of
  each inventory's scans, so it can't give page counts.
- **Ground truth → series:** scan name minus its last 6 digits. 99.98% of deeds map to an EAD inventory: 3,272
  inventories, mostly 1700–1790 (median 1754).
- **Overlap:** only 254 of the 1,364 PageXML folders (with 94,131 pages) have deeds, and only 254 of the 3,275 series with
  ground truth have PageXML here.

## Ground truth quality

- 577,347 rows, 23,138 deed ids repeated across exports: 46,291 rows are exact duplicates, the rest repeat an id with a
  different span (later corrections, presumably). Deduplicate before use and decide which span wins.
- Begin and end are on the same scan series in all but 1 deed. Pages spanned (end − begin): 0 for 7%, 1 for 28%, 2 for
  45%, ≥ 5 for 4%, max 266.
- Coordinates are in the PageXML's full image pixels (no rescaling): begin marks fall at the top left of the deed (median
  x/W 0.13, y/H 0.05). Scans are mostly openings (landscape, two pages), and 25%+ of begin marks are on the right-hand page
  (x/W > 0.36). A sample of 400 begin scans: 399 have a PageXML file. The first line under a begin mark is usually the
  date line or a folio number, not a deed opener ("Op heden…"), so the mark is the deed's top left corner, header included.
- **Coverage per folder** (pages inside some deed span ÷ pages in the folder), using the status column in the TSV:

| status | folders | meaning |
|---|---|---|
| usable | 217 | 90–100% of pages inside deeds: 43,034 deeds, 83,289 pages, 1613–1787 |
| check | 13 | 70–90% inside deeds (2378–2414, 2411A–2412B and others): unindexed pages or a gap |
| partial ground truth | 5 | 11898, 11899, 11901, 11902, 11915: 1–2 deeds each |
| PageXML incomplete | 4 | 1279, 3603, 481A_I, 562A: deeds span more pages than the folder has |
| file names are page ids | 15 | files are named by Transkribus page id (`26574330.xml`), with `imageFilename` the same, but `TranskribusMetadata` has a `pageNr`; if that equals the 6-digit scan number (unverified), these 5,838 deeds are usable too |

For the 217 usable folders, the ground truth is complete enough to measure precision as well as recall at the deed level.

## Relation to what AS already has

- `data/PageXML/NL-AsdSAA/NL-AsdSAA_5075.tar.gz` holds one inventory, 2409 (474 pages, file names `0215_A27338000215.xml`,
  a different export). Golden Agents has the same inventory as `2409_A27338` with 71 deeds, 93% coverage.
- `text_layer/presets.py` has `asdsaa_5075_v5sg` (step 24a), keyed on `NL-AsdSAA_5075_<inv>` folder names. The Golden Agents
  folders are named differently, so they need a source pattern or a staging step with links.
- The PageXML has no region types; regions and reading order are Transkribus defaults.
