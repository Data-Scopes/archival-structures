# Index links (`NL-HaNA_1.01.02_index_links_v1`)

Derived layer (phase 14): for every entry of a hand volume's front index, the folio numbers (locators) it cites and the
scan sides where each folio is. Built from a v5 text layer by `scripts/build_index_links.py`; the v5 layer is not changed.
Code: `archival_structures.analysis.folio_map` (folio -> scan sides) and `analysis.index_links` (front-index run,
locators, `volume_links`). Report: `reports/phase-14-index-links.md` (steps 20d, 20e).

## Rules

- **Front index** (`front_index_run`): >= 10 flagged `index_side` sides, the first within the first 10 sides, a run of 20
  unflagged sides ends it, >= 300 sides after it, <= 60% of sides flagged. 41 of the local volumes qualify.
- **Entry** = a `main_text` unit on a flagged side up to the last flagged side of the run (any `unit_method`).
- **Folio map** (frozen in 20d): anchors are the first `page_number` segment of a side that parses as a number
  (`^\D{0,3}(\d{1,4})\D{0,3}$`), outside the front index; the longest increasing chain is interpolated. `foliation`
  (recto share >= 0.9): folio N = the recto of scan S (`front`) plus the verso of the next scan (`back`); `pagination`:
  `front` is one side. A number outside the chain range +-6 is not located.
- **Locators** (`extract_locators`, default `DEFAULT_LOCATOR_VARIANT` = `L2`): numbers of 1 to 4 digits in runs separated by
  `[\s.,:;]+`; a run counts when what follows it is empty or starts with an upper-case letter. L1 keeps only the
  trailing run; L3 drops numbers followed by an ordinal, a month or a quantity word, or preceded by an amount sign; L4
  also drops numbers outside the volume's chain range +-6. The word lists are `SG_MONTHS` / `SG_LOCATOR_PARAMS`.

## Schema

`links/<inv>.tsv.gz` (one row per locator): `scan_id page unit_id loc_idx number char_start char_end front_scan_id
front_page back_scan_id back_page method anchor_dist confidence`. `char_*` index the unit text (`join/units`).
`method` is `printed`, `interpolated`, `extrapolated` or `none` (not located: the target columns are empty).
`folio_map/<inv>.tsv.gz`: `number front_scan_id front_page back_scan_id back_page position method anchor_dist
confidence`. `meta/<inv>.json`: the map summary (builder-internal, used to rebuild `volumes.tsv`).
`volumes.tsv`: `inv mode n_anchors recto_share chain_len chain_first chain_last front_first front_last end n_entries
n_locators n_linked`. `manifest.json`: layer name, source layer, `package_info`, variant and all parameters, counts.
Inventories without a front index get no files. The builder is resumable (an inventory with all three files is skipped).

**Not published** (UA-053, 2026-10-29): v1 stays a local and drive build; a v2 follows an index-links v2 phase.

## Numbers (dev build `data/index_links_dev/20e-a/`, 41 volumes, L2)

72,558 entries, 102,588 locators (54,905 entries with at least one), 101,282 located (printed 73,984, interpolated
26,259, extrapolated 1,039; none 1,306). 0.47 s per volume single-process. Bars and error analysis: report, step 20e.

Bars (test gold, read + extra rows; report step 20d/20e): F (folio N on the right scan) 33/35 = 0.943, **failed**
(bar 0.95); L (locators, L2) P 61/66 = 0.924, **failed** (bar 0.95), R 61/61; E (gold locator extracted and on the right
scan) 33/35 = 0.943, passed (bar 0.90). About 0.87 of the emitted links point to the right scan on the test rows
(0.924 × 0.943, estimate). L2 stays the default: the dev rule chose it before the test, and L3's better test precision
comes from a mistaken test run (F14-R.2). Silver per link: 70.7% of located links have an entry word on the target
sides, 21.8% on folio + 3 (F14-R.4).

## Full run (v5 full layer, UA-052, 2026-10-29, L2)

On the drive; `volumes.tsv` and `manifest.json` in `data/index_links_full/`. 8,856 inventories, 412 with a front index
(408 foliated, 4 paginated), none failed; 629,912 entries, 914,994 locators, 902,031 linked (0.986); 247 s. The 41 dev
volumes are identical to the dev build (report § *After the review*, F14-20e.9).

## Known limits

- A link is a folio number, not a verified location: several locators of an entry are all linked, and the entry's words
  are only on some of them (silver word share 0.455 against 0.100 for the control folio +3).
- Units that are first-line fragments of multi-line entries have their locators in other units; units that merge two
  index columns pair words with the wrong number.
- Dated chronological indexes (3712) have day numbers as false locators under L2 (L3 drops those followed by a month).
- `amount_before` has no word boundary: "Denhof 354." loses 354 under L3 ("f " is read as an amount sign).
- 6 foliated volumes end their chain with a year or inventory-number-like anchor above the scan count (3126, 3265, 3290,
  3307, 3320, 3522; in the full run about 22 of 412 by an estimated rule, F14-20e.10), which makes L4's range useless there and puts the last folios on interpolated, doubtful scans.
- Printed numbers garbled by the HTR or duplicated on two consecutive rectos mislocate a folio by one scan (bar F misses).
- Year-like end anchors (the 6 volumes above): numbers between the last real anchor and the year are placed on the
  last scans with confidence `high` (94 links in the dev build, 90 `high`), including years cited in entries (3265:
  1650, 1653); their folio-map tables run to the year (1,597–3,528 rows). `confidence` looks only at the scan distance
  to an anchor (F14-R.3).
- `extrapolated` links are mostly noise: 939 of 1,039 are single-digit numbers, often digits glued to a word by the HTR
  (`U1`, `Thesing7.`); treat `extrapolated` and `low` as unverified (F14-R.5).
- A link is the leaf (recto of scan S, verso of S+1). Some entries point at the opening instead: the matter is on the
  verso of scan S, facing folio N's recto (about 2% of testable links; 1 of 52 gold targets), and matter often runs on
  to folio N+1 (F14-R.4).
