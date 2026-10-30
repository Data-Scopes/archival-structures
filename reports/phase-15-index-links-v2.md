# Phase 15 report: Index links v2 (map guards, locators, entries from lines)

> **Status:** final (reviewed 2026-10-30).
> **Phase plan:** [`plan/phase-15-index-links-v2/PLAN.md`](../plan/phase-15-index-links-v2/PLAN.md).
> **Code state:** unreleased after v0.19.0 (recommended release: v0.20.0); no sibling changes.
> **Last updated:** 2026-10-30, by the phase-15 review (Opus 5.5).

## Summary

Phase 15 built a second version of the index → resolution links for the States General front indexes. It read fresh
side-level gold, added four map rules, three locator variants and three line-level entry assemblies, selected on dev
and tested once.

- **Gold (21a).** 29 front-index sides from 29 untouched volumes were read line by line: 1,251 lines, 595 entries,
  851 locators and 21 entries embedded in merged lines. There are 90 link targets, 63 of them usable on test.
  - All 29 sides are two-column.
  - The matter of a first locator is on folio N's leaf in 82 of 89 targets (F15-21a.4). It is on the recto in half of
    them and on the back in two thirds (F15-21a.6).
- **Map v2 (21b).** Bar F passes: 62/63 = 0.984 [0.915, 0.997].
  - v1 scores the same, because the random targets hold none of the failure modes that G1–G3 fix (F15-R.4).
  - The rules fix 7 year-like chain ends and drop 166 tied numbers. Of the 92 v1 `high` links interpolated onto the
    last scans, v2 drops 71 and keeps 21 as `low` (F15-21b.2, F15-21d.2).
- **Locators and entries (21c).** The fixed rule selected **A0 + L7**: v1's units with the stricter locator rule.
  - Line-level assembly finds more locators (dev recall 0.985 against 0.950). It loses more on attribution, mainly
    because lemma headings are attached to the next entry (F15-21c.1, .7).
  - On test, locator precision passes (0.981). Recall (0.856), attribution (0.892) and end to end (0.825) fail. They
    are no better than v1 (F15-21c.5).
- **The layer (21d).** `index_links_v2` is built for 41 dev volumes. The v1 rebuild is byte-identical. Silver is
  slightly up: 0.464 against 0.455, and 0.368 against 0.357 in the two-column volumes (F15-21d.3).

**Verdict.** v2 is v1 with fewer false locators, a map that no longer trusts years and duplicates, and an opening
column. Its entries and attribution are v1's. Three of the five bars fail, so v2 is not ready for publication. Most
missing locators are right-column lines typed `marginalia` (F15-21c.6). The next index-links step is a hybrid assembly
or a stricter heading rule, tested on new gold (*Follow-ups*).

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F15-P.1 | Many front indexes are two-column. In 13 of the 29 untouched front-index volumes, ≥ 25% of the v1 entry units carry no L2 locator (pool range 0.11–0.47). In 3444 the hand layout interleaves both columns line by line, and in 3459 the right column is typed `marginalia`, so v1 has no entry for it | data-property | sg, index-links, layout | confirmed | [Planning](#planning) |
| F15-P.2 | A count bound on folio anchors is wrong. 3712's numbering continues from the previous volume (348–684 on 533 sides, rectos only), so a scan- or side-count bound drops real anchors and, applied before the mode decision, turns it into foliation with an 8-anchor chain. A jump test on the chain (Δn > Δpos + 10) is used instead | method | sg, folio-map | confirmed | [Planning](#planning) |
| F15-P.3 | The map v2 prototype (jump cut, tied numbers dropped, bracket-aware confidence) reproduces the phase-14 LOO exactly for v1 (21,010/21,334) and gives 20,952/21,176 = 0.9894 for v2. It fixes the 7 implausible chain ends of the 41 volumes. Of the 119 v1 `high` links above the scan count, the 54 left `high` are all 3712's | result | sg, folio-map | confirmed | [Planning](#planning) |
| F15-P.4 | L3's `amount_before` has no left word boundary, so `Hof 252.` (*Hof*, court, is a frequent index word) loses 252, like `Denhof 354.` (F14-R.2). Fixed in L6/L7 by `SG_AMOUNT_BEFORE_V2` (21c) | open-issue | sg, index-links, locators | revised | [Planning](#planning) |
| F15-P.5 | In the full v1 run, 32 of 412 front-index volumes end their chain above the volume's scan count. These are years, inventory-like or garbled numbers, plus a few anchors just above the count. Phase 14 estimated 22 by a ratio rule | data-property | sg, folio-map | confirmed | [Planning](#planning) |
| F15-21a.1 | All 29 judged index sides are two-column (dev 13/13, test 16/16): 595 of 1,251 lines (47.6%) are right-column lines, and 42 lines (3.4%) merge text of both columns. The pool is later registers, so this is not a corpus-wide share. | data-property | sg, index-links, layout | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21a.2 | 21 of 595 entries (3.5%; 21 of 290 entries with >= 2 lines = 7.2%) have lines that are not consecutive in `show_side` (unit) order; in 20 of them another entry's line sits in the gap (dev 10/260, test 11/335). | data-property | sg, index-links, entries | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21a.3 | Locator styles (851 locators): number plus period 513 (60.3%), comma-separated 200 (23.5%), colon 108 (12.7%), line end without punctuation 27 (3.2%), other 3. The colon style sits on low-stratum sides (105 of 108). 117 entries (19.7%) have >= 2 locators, 39 (6.6%) none, and 19 locators end an entry mid-line (`@`). | data-property | sg, index-links, locators | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21a.4 | Index accuracy: the matter named by the first locator is on the front or back of folio N in 82 of 89 usable targets (0.921; dev 23/26 = 0.885, test 59/63 = 0.937), absent in 4 and unclear in 3. One reader; UA-055 asks the user to confirm 20. | result | sg, index-links, gold | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21a.5 | 21 entries (3.4% of 616) lie wholly inside a merged line and have no line of their own (dev 13, test 8; 7 of the dev ones on one side, 3487). Their 25 locators are not in `line_gold`, so line-based assembly cannot see them; they are neutral for links. | data-property | sg, index-links, entries | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21a.6 | Where the matter sits (89 usable targets): front 45 (50.6%), back 61 (68.5%), next folio recto 22 (24.7%), opening 1; in 39 (43.8%) only on the back or next, not on the front. A check on folio N's recto alone would find half of the matter. | data-property | sg, index-links, folio-map | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21a.7 | Folio numbering is less regular than the index points assume: the scan-folio offset changes inside 7 of the 29 volumes (by 1 to 6, e.g. 3459 46 to 49, 3522 44, 50, 49); folio numbers often stand at the end of a long first recto line; 3572 has a locator (1182) beyond its last folio (about 694) and two scans printing the same number. | data-property | sg, folio-map | confirmed | [Step 21a](#step-21a-fresh-gold-reads-index-sides-entries-locators-link-targets) |
| F15-21b.1 | Folio map v2 (jump guard, tied-number drop, gap-aware confidence, opening side) is implemented as opt-in params; with defaults the v1 tables of all 41 volumes are identical to 20r-a (36,349 rows), and the planner numbers reproduce exactly (LOO 20,952/21,176 vs 21,010/21,334, 166 ties in 29 volumes, 7 chain ends) | result | sg, folio-map | confirmed | [Step 21b](#step-21b-folio-map-v2) |
| F15-21b.2 | The 6 chain ends the jump guard removes are years or the inventory number (e.g. 3522 printed on 0601, `1659`, `1674`); the 7th (3471, `485` after `457`) is a non-folio number too. After the cut the volumes end at the last plausible number | data-property | sg, folio-map | confirmed | [Step 21b](#step-21b-folio-map-v2) |
| F15-21b.3 | In a seeded sample of 10 links that G3 moves from `high` to `medium`/`low`, 8 sit in brackets that need more numbers than scans (e.g. 3280 `331@380 .. 335@381`), so v1's `high` was wrong; 1 is a G2 distance effect and 1 (3712) a false alarm where the chain skips unanchored sides | result | sg, folio-map | confirmed | [Step 21b](#step-21b-folio-map-v2) |
| F15-21b.4 | Bar F passes: v2 62/63 = 0.984 (Wilson 95% [0.915, 0.997]) on the filtered test targets; v1 gets the same 62/63, and dev is 26/26 for both. The targets hold none of the failure modes that G1-G3 fix, so they do not separate v2 from v1; all 63 are `high` | result | sg, folio-map | confirmed | [Step 21b](#step-21b-folio-map-v2) |
| F15-21b.5 | The only miss (3274 T034, folio 129 -> 0205 instead of 0209) is a run of misread printed numbers (`128`/`129` for 125/126) that still form an increasing chain; no map rule can detect it | negative-result | sg, folio-map | confirmed | [Step 21b](#step-21b-folio-map-v2) |
| F15-21c.1 | On dev, the v5 `main_text` unit (A0) beats line-level entry assembly (A1-A3) on link F1 (.830 against .626, .614, .529) although A1-A3 find more locators (R .985 against .950): the end-number line rule is worse than v5 units at deciding where an entry starts. | negative-result | sg, index-links, entries | confirmed | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21c.2 | The heading rule (A3) takes 158 headings against 47 gold, because digit-free short first lines are common in wrapped entries. | negative-result | sg, index-links, entries | confirmed | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21c.3 | L7 (L6 plus range) is best on every method; it cuts A0 locator FP from 16 to 7 with one extra FN (dev). | result | sg, index-links, locators | confirmed | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21c.4 | The phase-13 column reorder moved 7/13 dev sides but did not improve attribution (A2 below A1). | negative-result | sg, index-links, layout | confirmed | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21c.5 | Test, once (A0 + L7): locator P 0.981 passes; locator R 0.856, attribution 0.892 and E 0.825 (52/63) fail their bars. v1 (A0 + L2) gets R 0.862, A 0.893, E 0.825: no gain from 21c's selection on attribution or E. All four test numbers and v1's reproduce with the reviewer's independent scoring (F15-R.1). | negative-result | sg, index-links | revised | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21c.6 | 53 of 67 test locator misses are on lines typed `marginalia` (right columns outside `main_text` units); A1-A3 would recover them (dev recall .985 against .950) but lose on attribution. A hybrid (v1 units plus marginalia lines) is a follow-up candidate. | data-property | sg, index-links, layout | confirmed | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21c.7 | Post-hoc dev analysis (follow-up candidate, not tested): 67 of A1's 139 dev attribution errors have a gold `H` line as predicted first line; with them right A1's dev link F1 would be 0.798 (now 0.626). A3's heading rule finds 42 of 47 `H` lines but also takes 80 `S` and 30 `C` lines as headings. A heading rule that needs a trailing `.` or a following number-ended entry is untested. | open-issue | sg, index-links, entries | confirmed | [Step 21c](#step-21c-locators-v2-and-entry-assembly) |
| F15-21d.1 | `index_links_v2` builds for the 41 dev volumes with A0 + L7 and the v2 map: 41 done, 0 failed, 72,558 entries, 100,019 locators, 99,997 located (v1: 102,588 and 101,282); the v1 rebuild equals `20r-a` in 41 + 41 + 41 files and `volumes.tsv`. | result | index-links, builder | confirmed | [Step 21d](#step-21d-the-index_links_v2-layer) |
| F15-21d.2 | All of v2's change over v1 on the dev volumes comes from L7 (entries identical; 2,569 fewer locators, none new) and the map (G2 moves 31-35 links in 3630 and 3587 from `printed` to `interpolated`, mostly onto a neighbouring scan); 21 of v2's links in the 7 chain-end volumes are `extrapolated`/`low` within +6 of the new end, the 71 other `high` v1 links there are no longer locators. | behaviour-change | folio-map, locators | confirmed | [Step 21d](#step-21d-the-index_links_v2-layer) |
| F15-21d.3 | Silver (reported): entry-word share on the target sides 0.464 for v2 against 0.455 for v1 (control 0.101); in the 13 two-column `high`-stratum volumes 0.368 against 0.357 (control 0.075). Small gain, and it comes from dropped false locators, not from better assembly (A0 has v1's entries). | result | silver | confirmed | [Step 21d](#step-21d-the-index_links_v2-layer) |
| F15-21d.4 | 1.3% of v2 links (1,240 of 92,852) have an entry word on the opening side (verso of the target scan) and none on the target sides; 1.4% of words are found on the opening side only. The opening column is a small but real addition. | data-property | opening | confirmed | [Step 21d](#step-21d-the-index_links_v2-layer) |
| F15-21d.5 | Of 20 random v2 links, 17 are right (1 of them probable: OCR `252` for 232; review: 3283 N=593 is right too, F15-R.3), 1 is on the wrong scan (3280 N=334, one scan late behind the misread anchor `331` for 334, marked `medium`), 2 are untestable from the first lines. No opening or next-folio cases among the 20. | result | sample | revised | [Step 21d](#step-21d-the-index_links_v2-layer) |
| F15-21d.6 | 3712 (paginated, chain 348-684) loses 237 of 299 locators to the range filter L7; v1 located none of them either (62 linked in both). The range filter turns unlocatable links into non-links in volumes whose chain covers only part of the folios. | data-property | locators, range | confirmed | [Step 21d](#step-21d-the-index_links_v2-layer) |
| F15-R.1 | Every logged number reproduces from the reviewer's own runs: bar F 26/26 dev and 62/63 test (v1 the same); the 21c test bars P 404/412, R 404/472, A 355/398, E 52/63 and v1's 407/421, 407/472, 358/401, 52/63 with independent gold parsing and scoring; a fresh v1 rebuild equals `20r-a` in 123 files and `volumes.tsv`, a fresh v2 rebuild equals `21d-a` in 164 files; AS suite 1,402 passed, 11 skipped | result | sg, index-links, review | confirmed | [Review](#review) |
| F15-R.2 | One label per line cannot hold an index entry that lies wholly inside a line where the HTR merged both columns (21 entries, 25 locators, 3.4% of entries). The gold format was amended during 21a, before any test side was read, with `embedded_entries.tsv`: those locators count as gold locators and are neutral for links and attribution. Without it each of them, if predicted, would count as a false positive against the 0.95 precision bar | method | sg, index-links, gold | confirmed | [Review](#review) |
| F15-R.3 | 21d's sample misclassified 3283 N=593: scan 0620's recto prints the number split by the HTR (`Waerop 597` + `3` at the top right) between 592 (0619), `591` for 594 (0621) and 595 (0622), so the map's scan is right; 21d's sample is 17 right, 1 wrong scan, 2 untestable (F15-21d.5 revised) | result | sg, index-links, review | confirmed | [Review](#review) |
| F15-R.4 | On random targets v1's map is already about 0.99 (88/89 dev + test, the same rows as v2); the phase-14 bar F fail (33/35) was a small sample with two garbled numbers. G1-G3 act on tails that random targets rarely hit (7 chain ends, 166 tied numbers, 92 false `high` links in 41 volumes), so their benefit is shown by construction and the volume tables, not by a bar | result | sg, folio-map | confirmed | [Review](#review) |
| F15-R.5 | Reviewer, post hoc, for information only (after the test lock; nothing was chosen on it): A2 + L7 on the test sides gives locator P 0.983 and R 0.966 (+0.11 over A0) but attribution 0.787 (−0.10). The dev pattern of F15-21c.1 holds on test: line assembly recovers the `marginalia` column, and heading attachment costs more than it gains | negative-result | sg, index-links, entries | confirmed | [Review](#review) |
| F15-R.6 | Leakage checks pass. Selection (`select.json`, 09:57:24 UTC) read dev rows only (the eval filters by split on read). The test gold was hashed at 10:18 UTC, and both test runs started at 10:22 UTC, once each. No library code or builder reads gold. The test sides were read while 21b/21c code existed (the plan allowed it), by sessions barred from the code and its outputs | method | sg, index-links, leakage | confirmed | [Review](#review) |
| F15-R.7 | `docs/index_links.md` described L5 as built on L3 and L6 as an extended amount list. The code and brief say L5 = L2 + punctuation before a capital, and L6 = L5 + the L3 exclusions with a left word boundary and the amount sign searched in the whole prefix. Fixed in review | bug-fixed | docs, index-links | confirmed | [Review](#review) |

## Steps

### Planning

*Opus 5.5, 2026-10-29/30. Scripts and outputs: `reports/data/phase-15-index-links-v2/planning/`.*

**Pool** (`probe_pool.py`, `pool_volumes.tsv`).
- These are the 29 front-index volumes of `19e-a` that no phase-14 gold touched: 2,472 front-index sides, 51,808 v1
  entries (units), 71,296 L2 locators. A side holds 26–55 lines and 13–36 entries.
- The share of entry units without a locator is 0.11–0.47, and ≥ 0.25 in 13 volumes (3438–3630, the later
  registers).
- Two peeks (`peek_side.py`; both sides are excluded from the draw) show why: 3444 0020v interleaves two columns line
  by line, and 3459 0015v has its right column typed `marginalia` (F15-P.1).

**Map v2** (`map_v2_proto.py`, `map_v2_proto.tsv`).
- The prototype reproduces v1's LOO exactly.
- v2's rules:
  - G2 drops numbers printed twice within 2 positions;
  - G1 cuts the chain at jumps and keeps the longest piece;
  - G3 gives `high` only to brackets with |Δn − Δpos| ≤ 2.
- Chain ends change in 3126, 3265, 3290, 3307, 3320 (years), 3522 (its inventory number) and 3471 (misread 485).
- The 20r-a links located again: 98,908 stay `high`; 282 go from `high` to `low`, 247 to `medium` and 70 to none.
- On the spent 20c gold (information only) v2 gets 50/51 against v1's 49/51.
- A count bound was tried first and rejected (F15-P.2, F15-P.3).
- Full run: 32 of 412 chain ends exceed the scan count (F15-P.5).

**Assembly** (`assembly_proto.py`, spent volumes 3590, 3611, 3254, 3456 only).
- The phase-13 reorder (`_reorder_lines`) runs per side on duck-typed lines, and fires on 48/85, 72/114, 22/48 and
  64/85 front sides.
- A line rule that closes an entry after a line ending in a number leaves 96–99% of entries ending in a number.
- Mixed cases remain (`Venlo. | ,ten over Infractien van | 1 Hof. 115.`), and the gold will measure them.

**Draw** (`draw_sides.py`, `reads_sides.tsv`, seed 20261030).
- The volumes are split dev 13 / test 16, stratified by the share above (high: 6/7, low: 7/9), with 1 read + 1
  reserve side per volume.
- Excluded: sides read in phase 13 (50 sides), the planner's 2 sides, and sides with < 15 lines.
- The reading load is 579 dev and 684 test lines.

### Step 21a: fresh gold reads (index sides, entries, locators, link targets)

**Status: done (2026-10-30, 7 sessions); the gold is hashed and frozen.** Brief:
[`step-21a-gold-reads-v2.md`](../plan/phase-15-index-links-v2/step-21a-gold-reads-v2.md). Files:
`reports/data/phase-15-index-links-v2/step-21a/`.

**What changed.** No library code. One reader (Sonnet 5.5) read 29 whole index sides line by line (13 dev, 16 test; all
`index`, no reserve used) into `line_gold.tsv` (entry membership, labels, locators), 21 fully embedded entries into
`embedded_entries.tsv`, and the link target of 90 drawn entries (26 dev, 64 test) into `target_gold.tsv`. The 20 confirm
items are in `check_page.html` (UA-055). Pre-freeze edits are in `pre_freeze_edits.tsv` (9 cells: 8 `folio_evidence`
`neighbours` to `printed`, found when a number printed at the end of a long first recto line had been missed; T083
`printed` to `unclear`). `hashes.txt` holds the sha256 of the five gold files.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Verdicts, labels, entries | every `read` row, five labels, one `S` per entry, `@` on the same side | 29 `index`; `validate_gold.py` OK (labels, entry ids, `@`, embedded rows, `target_gold` against `targets.tsv`) |
| 2. Judged sides | dev >= 11, test >= 14 | dev 13, test 16 |
| 3. Targets | every row read; >= 50 test and 20 dev usable | 90 of 90 read; usable test 63, dev 26 (usable = scan not `none` and evidence not `unclear`) |
| 4. Confirm page, UA, hashes | 20 items, links, JS checked | 20 items (test, seed 20261031, pool 63), 3 scan links each, `osascript` syntax ok; UA-055; `hashes.txt` written |

Counts (`summary.py`, run with `-I`; strata from `reads_sides.tsv`; the counts of non-locator numbers come from the
`note` column, so they are lower bounds):

```text
                                                              dev-high       dev-low       dev-all     test-high      test-low      test-all       all-all
== side verdicts ==
Counter({('test', 'index'): 16, ('dev', 'index'): 13})
reserves used: none
judged index sides: 29  sides per group: {'dev-high': 6, 'dev-low': 7, 'dev-all': 13, 'test-high': 7, 'test-low': 9, 'test-all': 16, 'all-all': 29}

== size (index sides, line_gold.tsv) ==
sides                                                            6             7            13             7             9            16            29
layout lines                                                   250           322           572           317           362           679          1251
entries (S lines)                                               93           167           260           124           211           335           595
H lines                                                         36            11            47            31            40            71           118
F lines                                                          8             8            16            11            16            27            43
X lines                                                          4             2             6             0             0             0             6
locators (line_gold, @ counted once)                           122           267           389           122           340           462           851
entries with 0 locators                                          9            12            21            15             3            18            39
entries with 1 locator                                          73           123           196           101           142           243           439
entries with >=2 locators                                       11            32            43             8            66            74           117
mid-line entry ends (@ tokens)                                  12             0            12             3             4             7            19
two-column sides (any R line)                                    6             7            13             7             9            16            29
lines with 'merged columns'                                      6            15            21            11            10            21            42
embedded entries (embedded_entries.tsv)                          2            11            13             4             4             8            21
embedded locators                                                2            13            15             6             4            10            25
embedded share of all entries                                0.021         0.062         0.048         0.031         0.019         0.023         0.034
entries with a gap in k (strict)                                 5             5            10             8             3            11            21
  of which another entry sits in the gap                         5             4             9             8             3            11            20
entries with >=2 lines                                          60            84           144            66            80           146           290

== locator styles (character after each gold locator in the line text) ==
n (line end)                                                     7             6            13            11             3            14            27
n + other                                                        0             3             3             0             0             0             3
n,                                                              26            75           101             5            94            99           200
n.                                                              89           137           226           103           184           287           513
n:                                                               0            46            46             3            59            62           108

== non-locator numbers recorded in line notes (lines mentioning the kind; lower bound) ==
amount                                                           8             2            10             2            13            15            25
count                                                            4             2             6             2             1             3             9
ordinal                                                          1             0             1             0             0             0             1
date                                                             0             0             0             0             0             0             0
year                                                             1             0             1             1             0             1             2
garbled                                                          6             6            12             6             3             9            21
glued                                                            2             2             4             0             0             0             4

== targets (target_gold.tsv) ==
targets                                                         12            14            26            28            36            64            90
usable (scan != none, evidence != unclear)                      12            14            26            27            36            63            89
folio_evidence printed                                          12            12            24            27            30            57            81
folio_evidence neighbours                                        0             2             2             0             6             6             8
folio_evidence unclear                                           0             0             0             1             0             1             1
target_scan_id none                                              0             0             0             1             0             1             1
mean scans_opened                                             3.67          3.43          3.54          2.86          3.31          3.11          3.23
content leaf                                                    10            13            23            25            34            59            82
content opening only                                             0             0             0             0             0             0             0
content next only                                                0             0             0             0             0             0             0
content absent                                                   1             0             1             2             1             3             4
content unclear                                                  1             1             2             1             1             2             4
content_sides (exact combination; usable targets with a scan):
  (none)                                                         1             1             2             2             1             3             5
  back                                                           2             4             6             9             7            16            22
  front                                                          2             4             6             4            12            16            22
  back;next                                                      4             2             6             5             6            11            17
  front;back                                                     3             3             6             5             6            11            17
  opening;front                                                  0             0             0             0             1             1             1
  front;back;next                                                0             0             0             2             3             5             5
content_sides (any mention):
  mentions opening                                               0             0             0             0             1             1             1
  mentions front                                                 5             7            12            11            22            33            45
  mentions back                                                  9             9            18            21            22            43            61
  mentions next                                                  4             2             6             7             9            16            22
index accuracy, over usable targets (matter found at the named folio):
  leaf (front or back of folio N)                      10/12=0.833   13/14=0.929   23/26=0.885   25/27=0.926   34/36=0.944   59/63=0.937   82/89=0.921
  matter at front                                       5/12=0.417    7/14=0.500   12/26=0.462   11/27=0.407   22/36=0.611   33/63=0.524   45/89=0.506
  matter at back                                        9/12=0.750    9/14=0.643   18/26=0.692   21/27=0.778   22/36=0.611   43/63=0.683   61/89=0.685
  matter on next folio recto                            4/12=0.333    2/14=0.143    6/26=0.231    7/27=0.259    9/36=0.250   16/63=0.254   22/89=0.247
  matter only on back/next (not on front)               6/12=0.500    6/14=0.429   12/26=0.462   14/27=0.519   13/36=0.361   27/63=0.429   39/89=0.438
  absent                                                1/12=0.083    0/14=0.000    1/26=0.038    2/27=0.074    1/36=0.028    3/63=0.048    4/89=0.045

locator-position check: targets.tsv rows 90 = target_gold rows 90
```

**Error analysis.**

- *What was read:* every line of the 29 sides (1,251 non-empty lines; the planner counted 1,263 layout lines), no
  sample; all 90 targets (2 per dev side, 4 per test side, `random.Random("21a-<read_id>")`). Hard cases are in the
  `note` column of `line_gold.tsv` (322 lines have a note) and of `target_gold.tsv`.
- *Side verdicts:* 29 of 29 `index` (dev 13, test 16); no `dated index`, `not index` or `unreadable input`; no
  reserve used. Every side is two-column (`R` lines 595 of 1,251, `S` lines 11). The stratum `high`/`low` is the
  planner's, from `reads_sides.tsv`.
- *Size per split and stratum:* in the block above (lines, entries, `H`, `F`, `X`, locators, entries with 0/1/>=2
  locators, `@` ends, merged lines). Dev 572 lines, 260 entries, 389 locators; test 679 lines, 335 entries, 462
  locators. `X` lines (garbled) occur only on dev (6). 39 entries have no locator (Nihil, `vide` without folio, a
  number lost by the HTR).
- *Non-locator numbers, by the notes:* amounts 25 lines (`f 50`, `500m gls`, `6190824 gls`), counts 9 (`5000`
  rations, `18 Schepen`), garbled 21 (`5499`, `Renger:1.`), glued 4 (`nselt40`, `Livor110.`), years 2 (`1715`,
  `1731`), ordinals 1 (`1e.`), dates 0.
- *Targets:* `folio_evidence` printed 81, neighbours 8, unclear 1 (dev 24/2/0; test 57/6/1); mean `scans_opened` 3.23
  (dev 3.54, test 3.11; scans read individually, bulk number listings not counted). `content` leaf 82, absent 4
  (T025, T028, T075, T089), unclear 4 (T007, T013, T037, T083). `content_sides` (usable 89): back 22, front 22,
  front;back 17, back;next 17, front;back;next 5, opening;front 1, none 5. The matter is on the front in 45 of 89
  (50.6%), on the back in 61 (68.5%), on the next folio's recto in 22 (24.7%), on the opening in 1. In 39 (43.8%) it
  is on the back or next but not on the front. This is a fact about the source: the matter often starts at the foot
  of the front, or only on the back, so the index folio N names the start of the resolution, not always its text.
- *Volume numbering surprises (scans are 1-based positions in the scans table):*
  - 3572 has 738 scans but its last printed folio is about 694; scans 183 and 185 both print `142` (T084); locator
    `1182` of T083 is beyond the volume, probably a garbled `182` (flagged in `line_gold`). T083 got no scan.
  - Offsets (scan minus folio) change inside a volume: 3349 38-39 near scans 59-104, 35 by scan 243; 3265 46 (scan
    61) then 47 (scan 785); 3283 29 (scan 372) then 28; 3459 46 at scan 100, 49 from about 200 (scan 281 prints `252`
    for 232); 3522 44 at scan 100, 50 at 122-514, 49 at 247-300; 3303 33 and 34 (scan 67); 3572 41-42. That is 7 of
    the 29 volumes. Each target therefore needs local calibration, which is also why 8 `neighbours` rows remain.
  - Folio numbers sit at the end of a long first recto line in 3349, 3350, 3265, 3290, 3303, 3280 and 3274 (e.g. 3265
    scan 166: `ende gearresteert sijn de depesches 121`). `show_scan.py` did not list them (now fixed); a parser
    that only looks at standalone short lines misses them (8 rows were first written `neighbours`).
  - The HTR splits or garbles printed numbers: `'50' + '22'` for 502 (3283 scan 530), `'489' / '19.'` for 419 (3630
    scan 474), `3s1` for 311 (3315 scan 351).
- *Six verbatim hard cases:*

  | Side, line | Printed text | Decision |
  |---|---|---|
  | S02 k11 | `den Consul van nselt40 528. Johan Hassenborgh 449.` | `S` entry 10, locator 528. `nselt40` is glued, not a locator. `Johan Hassenborgh 449.` is an entry of the other column wholly inside the merged line, so it goes to `embedded_entries.tsv` (locator 449), not to `line_gold` |
  | S35 k7 | `indepetitie van 8649: vernieuwingder capitulatie` | `H` (left lemma `inde petitie van <amount> tot den aanbouw van Schepen`; 8649 is an amount); the right entry 15 starts inside, so its `S` is on the own-column line k8 |
  | S42 k36 | `tien. Resol: daarop. 419. Oo` | `S` entry 10, locators `419@9`: 419 ends entry 9 mid-line, entry 10 starts at the garbled `Oo` |
  | S08 k3 | `gedrag van den voorn: Argat273.` | `C` of entry 1, locator 273 although glued: it is an entry end and ascending (273 < 325 < 372) |
  | S09 k26 | `Bleiswijk 1. Aantzig.` | `S` entry 16 with no locator: `1.` is a garbled `V.` (vide) |
  | S38 k39 | `de Bodens, 200. guldt. 10.` | `C` entry 23, locator 10 only; `200` is an amount, and `10` after `guldt.` is uncertain |

- *Conclusion:* the index sides of these later registers are all two-column, and the `show_side` line order is a good
  but not perfect unit order (3.5% of entries are split by another entry). Locator syntax is mostly `n.`, but `n:`
  and `n,` are common enough that a rule fitted to one volume fails on another. The index points to the right folio
  in 92% of usable targets, but the matter is on the front in only half of them. The 4 `absent` targets are in four
  volumes, each searched in the neighbouring scans; they may be index errors or HTR garble, which one reader cannot
  separate. The reader is one agent. UA-055 asks the user to confirm 20 test targets; until then the gold is
  unchecked.

**Findings:** F15-21a.1 to F15-21a.7.

**Open issues.** UA-055 (confirm page). The pool is later registers, so the shares above are not corpus-wide. The
tool gaps (line-end folio numbers in `show_scan.py`, entries embedded in merged lines, no `content` value for a `none`
target) are listed under *Brief defects* in the `PROGRESS.md` blocks.

### Step 21b: folio map v2

**Status: done.** Rules, tests, LOO, regression, dev check and bar F are complete. Brief: [`step-21b-folio-map-v2.md`](../plan/phase-15-index-links-v2/step-21b-folio-map-v2.md).

**What changed.** `analysis/folio_map.py` gets four opt-in rules, all off by default: G1 jump guard (`jump_k`, `jump_c`),
G2 tied numbers (`dup_window`), G3 gap-aware confidence (`gap_slack`), G4 opening side (`opening`, `FolioTarget.opening`,
two extra table columns only when on). `FolioMap` gets `n_dup_dropped` and `chain_cut`. `FOLIO_MAP_V2 =
FolioMapParams(jump_k=1.0, jump_c=10.0, dup_window=2, gap_slack=2.0, opening=True)`. Changelog entry under *Unreleased*.
Data in `reports/data/phase-15-index-links-v2/step-21b/` (`volumes.tsv`, `loo.tsv`, `eval_map.txt`, `error_analysis.{py,txt}`).

**Results against the acceptance criteria (41 front-index volumes, 19e-a).**

| # | Criterion | Result |
|---|---|---|
| 1 | tests | 20 in `test_analysis_folio_map.py` (13 old + 7 new); full suite 1,388 passed, 11 skipped (1,399 collected) |
| 2 | v1 tables unchanged | `FolioMap.table()` with default params equals 20r-a `folio_map/<inv>.tsv.gz` in 41/41 volumes (36,349 rows) |
| 3 | planner numbers | LOO v1 21,010/21,334 (0.9848), v2 20,952/21,176 (0.9894); 166 tied numbers in 29 volumes; 7 chain ends change (3126 1591→356, 3265 1659→822, 3290 1674→696, 3307 1683→642, 3320 1689→987, 3471 485→457, 3522 3522→550); 3712 stays pagination, chain 205→203, ends at 684; no mode change. All reproduce exactly |
| 4 | bar F (test, once) | **PASS**: v2 62/63 = 0.984, Wilson [0.915, 0.997]; v1 62/63 on the same rows |
| 5 | protected trees | no differences except new or changed files of the parallel sessions 21a and 21c (`step-21a/`, `step-21c/`) |

**Error analysis (without gold).**

*Chain ends of the 7 changed volumes* (number@scan position; `error_analysis.txt`). In 6 of 7 volumes the dropped anchor
is not a folio number: it is the last or second-to-last non-blank scan, 3-4 scans from the end, directly after the
last real anchor, with a value 600-1000 above it: 3126 `356@365 → 1591@366`, 3265 `822@869 → 1659@870`, 3290
`696@760 → 1674@762`, 3307 `642@677 → 1683@678`, 3320 `987@1027 → 1689@1028`, 3522 `550@600 → 3522@601`. These
are years (1591, 1659, 1674, 1683, 1689 are plausible dates) or the inventory number (3522, the volume's own number). 3290 also has
`99` on 0763 after it. The seventh, 3471, `457@512 → 485@515`: 485 is 28 above 457 over 3 scans, so not a folio either
(garble of a smaller number, or a page of a different numbering), and 3 scans of nothing follow. In all seven the v2
chain ends at the last plausible number, and the final 3-5 scans carry no printed number.

*Ten seeded G3 links* (`random.Random(21)` over 507 links that were `high` in 20r-a and are `medium`/`low` interpolated
under v2; the pool also contains changes caused by G1/G2). Judged by the bracketing anchors and the printed numbers
between them:
- 8 genuinely inconsistent: the bracket needs more numbers than it has scans, so the interpolated scan is a guess. 3280
  `331@380 .. 335@381` (334 placed on 381), 3401 `83@118 .. 88@119` (85), 3290 `135@205 .. 141@207` (140), 3283 `1@54 ..
  11@55` (2), 3611 `10@77 .. 17@78` (16 and 12), 3265 `520@573 .. 531@578` (523 and 528; the scans between hold `5`,
  nothing, nothing, nothing). Either leaves are missing in the scans or an anchor is misread; v1's `high` was wrong.
- 1 moved by distance, not G3: 3265 number 394, bracket `391@437 .. 400@447`, `off` is 1; G2 dropped the tied `395`
  (0439, 0441), so the nearest anchor is 3.3 away: `medium` is the G2 effect.
- 1 false alarm: 3712 number 623, bracket `622@439 .. 640@462`. `623.` is printed on side 440, but it is not in the chain
  (the sides 623-639 are not anchors in either version; the chain jumps), so G3 reads dn=18, dpos=23 and says `low`
  while the position is probably right. The rule is conservative where the chain skips flagged or garbled sides.

**Dev check (21a dev targets T001-T026, `dev_eval.tsv`).** The filter (target scan not `none`, evidence not `unclear`)
keeps 26 of 26 rows (24 `printed`, 2 `neighbours`). **v2 26/26 (Wilson 95% [0.871, 1.000]), v1 26/26.** All 26 are
`high` under v2 (calibration 26/26; no `medium`/`low` rows to calibrate on). 21 are exact chain hits (`printed`) at
the gold scan; 5 are interpolated at the gold scan (T001, T002 in 3126; T005, T006 in 3315; T011 in 3428).
- **Misses:** none, so nothing to classify (garbled number / duplicate / jump guard / gap / leaf vs opening / gold error).
- **v1 vs v2 disagreements:** none; every row has the same position, method and confidence in v1 and v2. The dev
  volumes do not contain the failure modes (year-like chain ends, tied numbers, inconsistent brackets) at the target
  numbers, so the dev check cannot separate v1 from v2. It is a sanity check, not evidence for v2 over v1.
- **Opening coverage (reported, not a bar).** Of the 24 rows with `content` not `unclear`: 23 are `leaf` with the matter
  on `front` or `back` (6 front only, 6 back only, 5 front;back, 6 back;next), 0 only on `opening`, 0 only on `next`;
  1 is `absent` with empty `content_sides` (the script's `other` bucket). The parser matches the real format
  (`content_sides` is `;`-joined; tokens opening/front/back/next); no fix was needed.

**Bar F (test, run once, 2026-10-30 10:22 UTC by the machine clock; `test_done.json`).** Gold re-read fresh after
the freeze (64 test rows; T083 is `none`/`unclear`; the filter leaves 63). **v2 62/63 = 0.9841, Wilson 95% [0.915,
0.997]: PASS (>= 0.95). v1 on the same rows: 62/63.** (The lower Wilson bound is below 0.95, so the pass is on the point
estimate, as the criterion states.) 47 rows are `printed`, 16 `interpolated`; all 63 are `high` under v2, so calibration
is 62/63 for `high` and there are no `medium`/`low` rows.
- **The one miss, T034 (v1 misses it identically):** 3274, entry 12, folio 129; gold scan 0209, v2 0205. Class:
  **garbled printed number**. Chain anchors: `124@203, 128@204, 129@205` then `436.` on 0206, nothing on 0207-0209, `130@210`.
  Gold note: 129 is not printed on 0209, 130 is on 0210 and the offset is 80, so 0204/0205 read `128`/`129` for what
  are folios 125/126 (HTR misread), and the unnumbered 0207-0209 carry 127-129. The misreads still form an increasing
  chain, so no rule (G1 jump, G2 tie) can see them; the interpolation across 0205→0210 would also not be flagged by G3
  (`dn` 1, `dpos` 5, but the number is a printed chain hit). No rule was changed.
- **v1 vs v2 disagreements on test:** none. As on dev, position, method and confidence are identical in all 63 rows.
  The test targets contain none of the G1/G2/G3 failure modes, so bar F does not separate v2 from v1.
- **Opening coverage** (59 `leaf`, 0 opening-only, 0 next-only; 3 `absent` with empty `content_sides`). Of the 62 rows
  with `content` not `unclear`, 59 have the matter on the leaf (`front` and/or `back`; 1 of them also on `opening`),
  none only on the opening, none only on the next scan, 3 absent (the script's `other` bucket). Detail: 15 front, 16
  back, 11 front;back, 11 back;next, 5 front;back;next, 1 opening;front.
- **Dev + test together:** v2 88/89, v1 88/89.

**Protected trees.** `protect_before.txt` vs `protect_final.txt` differ only in new or changed files of 21a
(`step-21a/*`) and 21c (`step-21c/*`), which run in parallel. Nothing outside `step-21a/` and `step-21c/`, and no layer,
`data/index_links_*` or sibling output, changed. Full suite: 1,397 passed, 11 skipped (1,388 at my first run; the rest
came from 21c's parallel work).

**Findings.** F15-21b.1 to F15-21b.5 in the table above.

### Step 21c: locators v2 and entry assembly

**Status: done (2026-10-30); bars L-recall, A and E failed (negative result).** `select.json` 10:57, `test_done.json` 11:22
(the test ran once; no rule changed after it).

**What changed.** `archival_structures/analysis/index_links.py` gained locator variants L5-L7 (`sub_entry_punct`,
`left_boundary`, `SG_AMOUNT_BEFORE_V2`), `LineBox`, `Entry`, `read_side_lines`, `side_reading_order` (the phase-13 19f
`_reorder_lines`, run per side on duck-typed lines, no F13-R.10 guards), `assemble_entries` (A1 line rule, A2 with column
reorder, A3 with heading rule) and `entry_locators`. v1 names and L1-L4 are unchanged. Tests: 19 pass in
`tests/test_analysis_index_links.py` (10 old unchanged, 9 new); full suite 1397 passed, 11 skipped (1408 collected).
Scripts: `reports/data/phase-15-index-links-v2/step-21c/{eval_links.py,spent_check.py}`.

**Dev grid** (13 index sides; 386-400 gold locators incl. 15 embedded; 389 gold links; `dev_grid.tsv`).

| method | variant | loc P | loc R | link F1 | attribution |
|---|---|---|---|---|---|
| A0 | L2 / L5 / L6 / L7 | .960 / .963 / .972 / .982 | .955 / .955 / .950 / .950 | .824 / .825 / .826 / **.830** | .860 / .860 / .859 / .859 |
| A1 | L2 / L5 / L6 / L7 | .964 / .966 / .973 / .983 | .990 / .990 / .985 / .985 | .621 / .622 / .623 / .626 | .636 / .636 / .637 / .637 |
| A2 | L2 / L5 / L6 / L7 | .964 / .966 / .973 / .983 | .990 / .990 / .985 / .985 | .608 / .609 / .610 / .614 | .623 / .623 / .624 / .624 |
| A3 | L2 / L5 / L6 / L7 | .964 / .966 / .973 / .983 | .990 / .990 / .985 / .985 | .525 / .525 / .526 / .529 | .538 / .538 / .538 / .538 |

**Selection (fixed rule, max dev link F1): A0 + L7, dev link F1 0.8303** (`select.json`). The line-level assemblies
(A1-A3) win on locator recall (.985-.990 against .950-.955, because they also see the right columns typed `marginalia`)
but lose clearly on attribution. Variants: L7 > L6 > L5 > L2 on precision (FP 16 -> 7 on A0), recall within 1 locator.

**Error analysis (dev).**
- *Reorder rate:* A2/A3 reorder 7 of 13 sides. A2 moves link F1 by -0.013 against A1; A1 -> A2 changes 5 links, mostly
  worse (S09, S13). No sign yet that the column reorder helps entry attribution.
- *Headings:* A3 creates 158 heading entries against 47 gold `H` lines (A1/A2: 0). The rule takes the digit-free first
  line (up to 4 words) of ordinary wrapped entries (e.g. "opgebragte Turken, en"); 151 of its attribution errors start
  at a non-S line. The cost the brief documents is large, not marginal.
- *A0 attribution errors (52 links):* 41 predicted entry starts at a non-S line (the v1 unit is cut mid-entry; many on S10
  three-column merge, 25 links, and S02, S13), 11 two entries merged into one unit (S03, S07, S09: a sub-entry or an
  entry end followed by a capital).
- *A1 attribution errors (139 links):* 67 heading attached to the next entry (the gold counts the heading as no entry:
  S04 "Statien." + "Livor110." join the first entry; S01), 45 entry starts at a non-S line, 27 entries merged.
- *A0 locator errors:* 20 FN, 7 FP. FN: right-column or marginal lines outside `main_text` units (S03, S09, S10), glued
  digits ("Argat273.") and a bare number line. FP: amounts "f 7922: 7: 0." (S08), "Renger:1. 913" and a date-like "1 14" (S13),
  "Bleiswijk 1. Aantzig." (S09).
- *Gold conventions:* no H/F/X line carries a locator in the dev gold (0 of 389). 5 gold entries have their `S` line
  placed on an own-column line (S03 x3, S10, S11); removing them moves link F1 by +0.002 (A0 .8303 -> .8327, A1 +0.002,
  A2 +0.002, A3 -0.000); it changes no ranking. 15 embedded locators (13 rows) are neutral for links and attribution
  (`n_neutral` 14-15 per row).
- *Spent volumes 3590 and 3611* (10 seeded entries, `random.Random(2103)`, `spent_check.txt`): 6 whole, 2 merged entries
  (624/627/92 in one unit; "1." and "516" in one), 1 fragment starting with a number ("267. Misss. van Hertell,"), 1
  digit-free fragment ("Consul Guerin.").

**Test (16 index sides, once; A0 + L7 against the v1 baseline A0 + L2).**

| Bar | A0 + L7 (selection) | v1 (A0 + L2) | Result |
|---|---|---|---|
| L precision >= 0.95 | 404/412 = 0.981 [0.962, 0.990] | 407/421 = 0.967 [0.945, 0.980] | pass |
| L recall >= 0.90 | 404/472 = 0.856 [0.821, 0.885] | 407/472 = 0.862 [0.828, 0.890] | **fail** |
| A attribution >= 0.90 | 355/398 = 0.892 [0.858, 0.919] | 358/401 = 0.893 [0.859, 0.919] | **fail** (just) |
| E end to end >= 0.90 | 52/63 = 0.825 [0.714, 0.900] | 52/63 = 0.825 | **fail** |

Link F1 0.818 against 0.816. Embedded locators: 10 gold, 6 neutral. Files: `test_sides.tsv`, `test_errors.tsv`,
`test_targets.tsv`, `eval_links.txt`. The selection is no better than v1 on test; L7 gains 6 locator false positives and
loses 3 true ones (the range filter drops 1780, 1443 and 1182, which are gold).

**Test error analysis.**
- *Locator FN (67, all read):* 53 on lines typed `marginalia` (right columns, outside any `main_text` unit; e.g. S35
  "van Vriesland 442.", S29 "ten, 80, 86."), 3 dropped by the L7 range filter (years or high numbers: 1780, 1443, 1182),
  7 on merged lines where the run is followed by lowercase or sits on another line than gold (S29 k23: 231...419 then
  "wijck, 262."; S29 262), 3 on S39 k19 (350, 459, 490, predicted on k20), 1 `19.` before lowercase "memorie".
- *Locator FP (8):* 4 the same line-assignment cases as above (S29 262; S39 350, 459, 490 on k20 instead of k19), 1 bare count
  "2. Kisten" (S38), 1 "491: 4" (S30), 1 "99: Cumss" (S42), 1 S36 "332." on an entry continued from the previous page.
- *Attribution errors (43 links in 29 first-line groups, all read):* 22 cut mid-entry (the v1 unit starts at a non-S line;
  S37 5 groups, S42 3, S39 3), 12 heading attached (S32-S35 lemma headings: "Declaratien", "Petitien", "Consenten
  Constantinopolen."), 9 two entries merged into one unit (S29, S30, S38, S41, S42).
- *E misses (11 of 63):* locator 5 (S29 80, 207, 231; S35 424, 717: all marginalia or merged-line locators), entry 5 (S34
  236, S35 442, S37 316, S39 464, S42 136), map 1 (T034: number 129 on S28 maps to scan 0205, gold 0209; a neighbours case).
  The v1 baseline gets the same 52/63.

**Post-hoc dev analysis (not a rule run on test; dev gold only).** A1's 139 attribution errors on dev: 67 (48%) are
entries whose predicted first line is a gold `H` line. If those links were right, A1's dev link F1 would be 0.798 instead
of 0.626, close to A0's 0.830. A3's 158 headings on dev are, by the gold label of their line: 42 `H`, 80 `S`, 30 `C`, 5 `F`,
1 `X` (47 gold `H` lines): the rule finds 42 of 47 headings but also takes 80 entry starts and 30 continuation lines.

**Findings.** F15-21c.1–7 are in the phase *Findings* table (moved there by the review).

### Step 21d: the index_links_v2 layer

**Status: done (2026-10-30), ready for review.** v2 was built although three of the 21c bars failed; the review decides on
publication (nothing is published). Session: Sonnet 5.5.

**What changed.**
- `archival_structures/analysis/index_links.py`: `volume_links_v2`, `V2_CONFIG` (`FOLIO_MAP_V2`, A0, L7,
  `SG_HAND_COLUMN_ORDER_V5`), `ENTRY_COLUMNS`, `LINK_COLUMNS_V2`. `volume_links_v2` supports A0 (units + `join/segments`) and
  A1-A3 (`assemble_entries`) through `config`. Under A0 the side lines are not read (`read_side_lines` is called only for
  A1-A3); `n_headings` is 0 everywhere.
- `scripts/build_index_links.py`: `--version {v1,v2}` (default v1), `process_v2` (4 files per inventory, skipped when all
  exist), `volumes.tsv` with `n_headings n_dup_dropped chain_cut`, manifest with `version`, `config`, `bars`;
  `--variant` with v2 is an error.
- Tests: `tests/test_analysis_index_links.py` 24 pass (19 + 5 new: `test_volume_links_v2_synthetic` (config with A2, entries in
  column order, opening columns), `test_volume_links_v2_default_a0`, `test_builder_v2_tables_and_resume`,
  `test_builder_v1_unchanged_default`, `test_builder_v2_rejects_variant`); full suite 1402 passed, 11 skipped (1413 collected).
- Docs: `docs/index_links.md` (v2 part), changelog entry, UA-056 (full run).
- Data: `data/index_links_dev/21d-a/` (v2), `21d-v1/` (v1 rebuild); scripts and outputs in
  `reports/data/phase-15-index-links-v2/step-21d/` (`regress.py/.txt`, `compare_v1_v2.py`, `compare.tsv`, `chain_ends.tsv`,
  `silver.py/.tsv`, `sample_links.py/.txt`, `protect_{before,after}.txt`).

**Acceptance.**

| # | Criterion | Result |
|---|---|---|
| 1 | tests green | 24 in the module, 1402 passed / 11 skipped in the suite |
| 2 | v1 rebuild equals `20r-a` | **all equal**: folio_map 41, links 41, meta 41 (no timing keys), `volumes.tsv` (`regress.txt`) |
| 3 | v2 dev build | 41 done, 0 failed, 0 `no_front_index`; every link's `(scan_id, entry_id)` is in `entries/` (0 missing; 0 empty `line_id`); no located v2 link beyond chain +-6 (0 of 99,997), including the 7 chain-end volumes; manifest has version, config, bars |
| 4 | compare, silver, sample, docs, UA | written; UA-056 |
| 5 | protected trees | `protect_before.txt` and `protect_after.txt` identical (177,181 lines) |

**Dev build (`21d-a`).** 72,558 entries (same as v1), 100,019 locators, 99,997 located: printed 72,789, interpolated 26,374,
extrapolated 834, none 22 (v1: 73,984 / 26,259 / 1,039 / none 1,306). Confidence of located links: high 0.9795, medium
0.0093, low 0.0111 (v1: 0.9825, 0.0071, 0.0105); `extrapolated` 0.0083 (v1 0.0103). Time: 27.9 s single-process (0.68 s per
volume; v1 20.0 s, 0.49 s), 10.2 s through coreshare (3 wanted). G2 dropped tied numbers in 29 of 41 volumes (0 to 35 per volume), G1 cut the chain in 7 (3126, 3265, 3290, 3307, 3320,
3471, 3522: the chain-end volumes below).

**Compare and silver (reported, not bars).**

| set | version | links with control | target words | control words | links with a target word | opening-only links |
|---|---|---|---|---|---|---|
| all 41 | v1 | 94,056 | 0.4548 (118,464/260,497) | 0.1000 | 0.7065 | - |
| all 41 | v2 | 92,852 | 0.4637 (118,363/255,232) | 0.1008 | 0.7146 | 1,240 (1.34%) |
| 13 high stratum | v1 | 22,088 | 0.3566 | 0.0743 | 0.6385 | - |
| 13 high stratum | v2 | 21,418 | 0.3682 | 0.0747 | 0.6517 | 339 (1.58%) |

v2's word share is above v1's, overall (+0.009) and in the high stratum (+0.012). The assembly did not contribute (A0 has
v1's entries); the gain is the L7 locators (fewer false numbers) and the map. In the high stratum the share stays at 0.37:
the two-column attribution problem is not touched by A0 (F15-21c.6, F15-21c.7).

**Error analysis.**

*20 random v2 links* (`random.Random(2104)`, `sample_links.txt`; judged from the entry text, N and the first four lines of the target
sides by y; a folio number printed at the top or end of a long first line counts as seen).

| class | n | links |
|---|---|---|
| right (N printed on the target side, matter plausible) | 15 | 2, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 16, 17, 18, 19 |
| right, probable (page prints `252` for N=232, OCR of 232) | 1 | 11 |
| wrong scan | 2 | 1 (3280 N=334 `interpolated`/`medium`: 0381 prints 335, so 334 is on 0380), 3 (3283 N=593 `interpolated`/`high`: 0620 prints 597) |
| untestable (no number in the first lines) | 2 | 15 (3274 N=728), 20 (3254 N=265 inside an unsorted list 277, 284, 265) |
| opening, next folio, wrong entry text, wrong number | 0 | - |

Entries in 5 (a merged unit with garbled text), 7, 10 and 18 are long lists of folio numbers of one lemma (the entry is whole;
every number of the list is a link). The two wrong scans are interpolated links in volumes with drifting folio offsets
(3280, 3283; F15-21a.7).

*The 5 volumes with the largest change in locators (v1 to v2; entries unchanged in all 41).*
- **3623** (-294 locators, 3,412 to 3,118; targets unchanged): 75 outside the chain range, 60 with a letter before the
  number (`Corp1.`, `2Aardenburg`), 159 dropped by the exclusion lists/ L5 (e.g. `Gerechte van Venlo, rak: 115: 139`, where the
  numbers are followed by a capital without punctuation). L6/L7 trade some true locators for fewer false ones (A0 dev: FP 16 to 7, FN +1).
- **3712** (-237, 299 to 62): all 237 outside the chain 348-684 (paginated volume, 205 anchors from page 348 onwards);
  v1 located none of them (62 linked in both). Day numbers (`2.`) and `331 .`-like numbers are among them.
- **3611** (-181, 2,664 to 2,483): 65 range, 58 left boundary (`Aron1.` 868), 58 other (`f16500..`, `f 218.`).
- **3630** (-133): 66 range (years and `3746`, beyond the chain 2-901), 56 other, 11 left boundary; and 31 shared
  locators change target: G2 drops 8 tied numbers, so 10 `printed` links become `interpolated` on the same scan and 9
  onto a neighbour (698: 0754 to 0753; 28: 0084 to 0083).
- **3587** (-125): 35 range (`2165`), 72 other, 18 left boundary; 35 changed targets (G2 dropped 19 tied numbers):
  313 moves from 0361 to 0359 (a printed 313 that G2 judged tied).

Reading: the locator count changes come from L7 (range for years and large numbers, left boundary for glued digits, amounts
and ordinals for the rest). The map changes are few (G2) and mostly move a link by one scan.

*Chain-end volumes* (`chain_ends.tsv`): 3126, 3265, 3290, 3307, 3320, 3471, 3522. Between the new and the old chain end v1 had 92 `high`
links; v2 locates 21 of them (all `extrapolated`/`low`, number <= new end + 6: 3126 4, 3265 1, 3290 13, 3320 1, 3471 1, 3522 1) and
the other 71 are no longer locators (L7 range). The brief expected none located; the 21 are within the +-6 slack the map allows
and carry `low` confidence. G1 removes the year-like ends as intended.

*v2 against 21c's bars.* The builder uses the same code as 21c's evaluation, so the dev numbers repeat 21c's: the bars L recall
(0.856), A (0.892) and E (0.825) failed on the test and nothing in this step changes them.

**Findings.** F15-21d.1-6 (table above). A negative result stands: v2 (A0 + L7 + map v2) is v1 with fewer false locators, a
more careful map and an opening column; its entries and attribution are v1's.

**Deviations / defects.** See PROGRESS.md.

## Review

*Opus 5.5, 2026-10-30. Code review in [`plan/phase-15-index-links-v2/REVIEW.md`](../plan/phase-15-index-links-v2/REVIEW.md);
reviewer scripts in the session scratchpad (re-runs only, nothing written into the step dirs).*

**Re-runs (F15-R.1).** I re-ran every acceptance number myself rather than taking it from the logs:
- bar F, with my own map/locate loop;
- the 21c test bars, with independent gold parsing and scoring;
- the v1 and v2 builds, into the scratchpad;
- the AS suite.

All match.

**Gold (21a).**
- I read two random sides against their gold, S11 (3539) and S05 (3421), 64 lines in all. Labels, entry ids and
  locators are right, and so are the conventions the sessions settled (`S` on the own-column line, `1e.` not a
  locator, `S` for an entry continued from the previous page).
- The format gap and its fix are F15-R.2.
- The 9 pre-freeze edits (`pre_freeze_edits.tsv`) are labels only. They change `folio_evidence`, and T083's change
  removes it from the bar population. Every hash checks.
- The gold is one reader's. UA-055 (the user confirms 20 links) is open.

**Map (21b).**
- The code (`folio_map.py`) does what the brief says. G2 drops every copy of a tied number before the LIS. G1 keeps
  the longest piece, and the earliest on a tie. G3 recomputes the confidence only for `interpolated` targets. G4 and
  the two diagnostics are trailing fields with defaults.
- I read the test miss: T034 in 3274, folio 129, gold scan 0209, predicted 0205. The run `128`/`129` misread for 125/126
  is still increasing, so no rule can see it. 21b's classification holds.
- In 21d's sample I also read 3280 N=334. Scan 0380 prints `331` at the end of its first line where 334 is meant, and
  v2 is one scan late. G3 marks the link `medium`, which is the intended behaviour.
- F15-R.4 puts bar F in context.

**Locators and entries (21c).**
- On dev I printed the A1 entries of S01 and S04.
  - A1 attaches `Oraignen.` to `Congratulatie …` and `Savoyen.` to an entry carrying 21 locators. Every link of
    such an entry then counts as wrong.
  - A3's heading rule takes wrapped first lines such as `Missiven vanden Groot`.
- The low A1–A3 scores therefore come from the rules as specified, not from a bug, and the fixed selection of A0 + L7
  stands.
- 21c's post-hoc dev analysis (F15-21c.7) and my post-hoc test number (F15-R.5) both point to a narrower heading rule
  or a hybrid with v1 units as the next attempt. They need new gold, because the 21a test gold is now spent for
  selection.

**Layer (21d).**
- `volume_links_v2` and `--version v2` match the brief, and v1's output path is unchanged (both rebuilds byte-equal).
- The brief expected none of the 92 dropped `high` links to stay located. 21 stay, as `extrapolated`/`low` within the
  ±6 slack. That is how `locate` works; the brief's expectation was wrong.
- The docs had one wrong description, fixed (F15-R.7).
- F15-21d.5 is revised (F15-R.3).

**Leakage (F15-R.6)** passes.

**Publication.** The phase-14 decision (UA-053 a) was to publish a v2 only once it meets its bars on fresh gold. v2
fails three, so the recommendation is not to publish (UA-057).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F15-21c.6, F15-21c.7, F15-R.5 | Entry assembly v3, on new gold (the 21a test gold is spent for selection). Candidates: v1 units plus the `marginalia` lines of the side (a hybrid), or A1 with a heading rule that needs a trailing `.` and a following number-ended entry. Fresh gold from the drive-only front-index volumes | `plan/CONTEXT.md` §4 (index links) |
| UA-055 | User check of 20 test links on the scans (21a gold) | `docs/USER_ACTIONS.md` |
| UA-056 | Full run of `index_links_v2` (412 front-index volumes) | `docs/USER_ACTIONS.md` |
| UA-057 | Publication of `index_links_v2` (recommended: no) | `docs/USER_ACTIONS.md` |
| F15-21a.7, F15-R.3 | Folio numbers that the HTR splits or glues to a text line (`Waerop 597` + `3`) are invisible to the map's anchors; a page-number re-read on the top line would add anchors. Not planned | `plan/CONTEXT.md` §4 |
| F15-21d.6 | L7's range filter drops locators in volumes whose chain covers only part of the folios (3712). Harmless for links (none were located), but `n_locators` is not comparable across versions there | `docs/index_links.md` (known limits) |

## Reproduce

- Planning, from the AS root, each `.venv/bin/python -I reports/data/phase-15-index-links-v2/planning/<script>.py`:
  `probe_pool` (writes `pool_volumes.tsv`), `map_v2_proto` (`map_v2_proto.tsv`), `assembly_proto INV [K]` (spent
  volumes only), `draw_sides` (`reads_sides.tsv`). `peek_side.py` appends to `planner_seen.tsv`; do not rerun it.
