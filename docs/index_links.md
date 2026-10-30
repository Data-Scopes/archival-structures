# Index links (`NL-HaNA_1.01.02_index_links_v1`, `_v2`)

The first part (down to *Known limits*) describes **v1** (phase 14). The v2 layer (phase 15, step 21d) is described in
[*v2*](#v2-nl-hana_10102_index_links_v2) at the end.

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

# v2: `NL-HaNA_1.01.02_index_links_v2`

Phase 15 (step 21d). Built by `scripts/build_index_links.py --version v2` from a v5 text layer with
`archival_structures.analysis.index_links.V2_CONFIG` and `volume_links_v2`. v1 is unchanged (`--version v1`, the default;
the 21d rebuild equals the phase-14 build file for file). **Not published**: the layer is built whatever the bars said; the
phase review decides on publication. Report: `reports/phase-15-index-links-v2.md` (steps 21a-21d).

## Rules (v2)

- **Front index**: as v1 (`front_index_run`).
- **Folio map** `FOLIO_MAP_V2` (21b): the v1 map plus four rules.
  G1 jump guard: a chain that jumps (year-like anchors) is cut, so years no longer extend it (`chain_cut`).
  G2 tied numbers: two anchors with the same number within 2 scans are dropped (`n_dup_dropped`), because a duplicate
  printed number mislocates the folio by one scan.
  G3 gap-aware confidence: `high`/`medium` also need a small number-versus-scan offset between the neighbouring anchors,
  because the v1 confidence saw only the scan distance.
  G4 opening side: `opening_scan_id`/`opening_page` hold the verso of the target scan, the page facing folio N's recto,
  because an entry's matter can start on the facing page (1 of 89 usable 21a targets; silver: 1.3% of links).
- **Locator variant L7** (21c): L6 plus the chain range (+-6). L6 = L5 plus the L3 exclusions (ordinal, month, quantity,
  amount), with a left word boundary: a number glued to a letter is dropped, and the amount sign (`SG_AMOUNT_BEFORE_V2`,
  not preceded by a letter, so `Hof 252.` keeps 252) is searched in the whole text before the number. L5 = L2 with a run
  before a capital kept only if one of `.,:;` separates them (`14 Aug` is no longer a locator). It cuts
  locator false positives (dev, A0: 16 to 7) at the cost of one extra miss; it was selected on dev by the fixed rule (max
  link F1).
- **Entry assembly A0** (21c): the entry is a `main_text` unit (as v1), in `unit_order`. A1-A3 (line-level assembly, with
  column reorder and a heading rule) are implemented (`assemble_entries`, selectable through `config["method"]`) but were not
  selected: they win locator recall on dev (.985-.990 against .950) and lose attribution (A1 .637 against .859). With A0 the
  builder does not read the side lines (only `join/units` and `join/segments`); `n_headings` is 0 for every volume.

## Schema (v2)

- `entries/<inv>.tsv.gz` (`ENTRY_COLUMNS`): `scan_id page entry_id kind first_line_id n_lines line_ids text`. A0:
  `entry_id` = the unit id, `kind` = `entry`, `line_ids` the `|`-joined lines of the unit (from `join/segments`).
  A1-A3: `entry_id` = `<scan_id>:<page>:eNNN`, `kind` `entry` or `heading`.
- `links/<inv>.tsv.gz` (`LINK_COLUMNS_V2`): `scan_id page entry_id loc_idx number char_start char_end line_id
  front_scan_id front_page back_scan_id back_page opening_scan_id opening_page method anchor_dist confidence`.
  `char_*` index the entry text; `line_id` is the line that holds the locator; a not located link (`method` `none`) has
  empty target fields. Every `(scan_id, entry_id)` of a link is in `entries/`.
- `folio_map/<inv>.tsv.gz`: the v1 columns plus `opening_scan_id opening_page` after `back_page`.
- `volumes.tsv`: the v1 columns plus `n_headings n_dup_dropped chain_cut`. `meta/<inv>.json` (builder-internal).
- `manifest.json`: v1 keys plus `version: "v2"`, `config` (map params, method, variant, locator params, column cfg) and
  `bars` (the 21b/21c bar numbers, with their results). The builder is resumable (an inventory with all 4 files is
  skipped) and records a failing volume like v1. `--variant` is an error with `--version v2`.

## Numbers (v2 dev build `data/index_links_dev/21d-a/`, 41 volumes)

72,558 entries (identical to v1: same units), 100,019 locators (v1 102,588: -2,569, all from L7), 99,997 located (printed
72,789, interpolated 26,374, extrapolated 834; none 22; v1: none 1,306). Confidence of located links: high 0.980, medium
0.009, low 0.011 (v1 0.983, 0.007, 0.011). 0.68 s per volume single-process (27.9 s for 41 volumes; v1 0.49 s), 10.2 s through coreshare
(3 workers wanted). 7 volumes change a chain end (G1: the year-like ends of 3126, 3265, 3290, 3307, 3320, 3471, 3522); of the 92 `high`
v1 links between the new and the old end, v2 locates 21 (all `extrapolated`/`low`, within +6 of the new end) and drops the
rest at the locator step. Silver (reported, not a bar): share of entry words found on the target sides 0.464 (v1 0.455), control 0.101;
in the 13 two-column `high`-stratum volumes 0.368 (v1 0.357), control 0.075; 1.3% of v2 links have their matter on the
opening side only.

## Bars (v2 against fresh gold, 21a; Wilson 95%)

| Bar | v2 | v1 | Result |
|---|---|---|---|
| F folio N on the right scan >= 0.95 (21b) | 62/63 = 0.984 [0.915, 0.997] | 62/63 | pass |
| L precision >= 0.95 (21c) | 404/412 = 0.981 [0.962, 0.990] | 407/421 = 0.967 | pass |
| L recall >= 0.90 (21c) | 404/472 = 0.856 [0.821, 0.885] | 407/472 = 0.862 | **fail** |
| A attribution >= 0.90 (21c) | 355/398 = 0.892 [0.858, 0.919] | 358/401 = 0.893 | **fail** (just) |
| E end to end >= 0.90 (21c) | 52/63 = 0.825 [0.714, 0.900] | 52/63 = 0.825 | **fail** |

On the 63 usable test targets the F bar does not separate v2 from v1 (the targets hold none of the failure modes G1-G3 fix).
Method A0 + L7 gives no gain on attribution or end to end over v1.

## Known limits (v2)

Carried over from v1 and still true:
- A link is a folio number, not a verified location; several locators of an entry are all linked. Units that are
  first-line fragments of multi-line entries have their locators in other units, and units that merge two index columns
  pair words with the wrong number (A0 attribution 0.892: 41 predicted entry starts at a non-start line, 11 merged entries on dev).
- Printed numbers garbled by the HTR mislocate a folio by one scan (sample: inv 3280 N=334 lands on the scan that prints 335).
- A link is the leaf (recto of scan S, verso of S+1); the opening column is new, but whether the matter is on the
  opening is not decided per link.
- `extrapolated` and `low` links are unverified.

New from 21b/21c:
- **Locator recall 0.856**: 53 of 67 test misses are lines typed `marginalia` (right columns outside `main_text` units),
  which A0 never sees. The line-level assemblies see them but attribute worse; a hybrid is a follow-up (F15-21c.6).
- Numbers outside the chain range +-6 are dropped as locators (L7), so volumes whose chain covers only part of the folios
  lose those locators (3712, paginated, chain 348-684: 237 of 299 locators, none of them located in v1 either).
- Year-like end anchors no longer reach the map (G1), but the cut chain may then end before the last real folios.
- The test gold has none of the failure modes G1-G3 fix, so their benefit is only shown by the dev volume tables
  (166 tied numbers dropped in 29 volumes, 7 chain ends changed), not by a bar.
- v2 links do not say which of an entry's several locators the matter belongs to (silver: 71% of links have an entry word on
  the target sides against 22% on the control).
