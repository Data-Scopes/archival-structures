# Phase 10 report: Hand and print per volume (text layer v4)

> **Status:** final (reviewed 2026-10-23).
> **Phase plan:** [`plan/phase-10-hand-print-per-region/PLAN.md`](../plan/phase-10-hand-print-per-region/PLAN.md).
> **Code state:** unreleased after AS v0.14.1 (`analysis/hand_print.py`, unused); no sibling code.
> **Last updated:** 2026-10-23, by the phase review (Opus).

## Summary

Phase 10 set out to stop assuming that an SG archive is either handwritten or printed: each inventory would get a
hand or print mode from its own PageXML, and the text layer v4 would build it with the matching v3 rules. Both
pre-registered tests failed, so no v4 layer was built (15r and 15s did not run) and nothing changed for the siblings.
Text density per region (15p) gets 72% of silver print and 85% of silver hand volumes right (F10-15p.1). Line-box
height over baseline pitch (`hrel`, 15q) reproduces on dev but scores 0.768 / 0.893 on a random silver sample
against a 0.90 bar (F10-15q.1), and labels the printed 1.10.94 volumes `hand` (F10-15q.4). The ratio mixes a
scribe's line spacing with the segmentation model's box convention (F10-R.1). Exploratory and post hoc: absolute
line-box height separates all 76 labelled volumes (F10-R.3), a lead that needs a fresh sample (F10-R.5, UA-022).
The v2/v3 pilots still build the printed 3771–3773 and 3821–3823 with the hand rules (F10-P.1, documented).

## Findings

One row per finding. The ID is `F10-<step>.<n>`. Planning findings are `F10-P.<n>`, the review's
are `F10-R.<n>`. IDs never change and are never reused. A finding that turns out to be wrong is
marked `withdrawn`, not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F10-P.1 | 6 of the 14 "hand" pilot inventories are printed volumes (3771–3773, 3821–3823; EAD *Gedrukte registers van de ordinaris resoluties*): about 122 lines per page at 66 px against 34–48 at 126–163 px for hand, like print 455/456 (123 at 75–77 px). The v2 and v3 pilots built them with the hand rules (no split, no cues, no crosswalk); both catalogue rows and the v3 manifest now say so | data-property | sg, text-layer, hand-print | confirmed | [Planning](#planning) |
| F10-P.2 | Nothing in the 1.01.02 PageXML marks print: the metadata names only the creator `Laypa`, every region is typed `Text`, and lines carry no column attribute; only 455 (a print-trained Laypa model) has typed regions | data-property | sg, pagexml, hand-print | confirmed | [Planning](#planning) |
| F10-P.3 | Text density per region box (chars per 1000 px², from the paragraph TSVs) separates the dev volumes (hand medians 0.19–0.33, print 0.75–0.78) but not every region: hand p95 0.44 and print p05 0.42 overlap, and 3350 has 5.4% of its regions above 0.55 | data-property | sg, hand-print | confirmed | [Planning](#planning) |
| F10-P.4 | The EAD gives silver volume labels: print 245 volumes with TSVs (*Gedrukte registers* 105, *nouvelles* 64, *plakkaten* 76), hand 550 (*Registers van ordinaris net-resoluties*); "gedrukt" also matches the handwritten *Geschreven indices* 3865–3868 | data-property | sg, ead, hand-print | confirmed | [Planning](#planning) |
| F10-P.5 | REPUBLIC paragraphs exist for the printed 1.01.02 volumes 3760–3864 (`print_paragraphs_v1`), with box-based line ids like 455's; 0 of 9,768 3771 ids equal a layer `line_id` (XML UUIDs), so only the geometric crosswalk classes can match | data-property | sg, republic, crosswalk | confirmed | [Planning](#planning) |
| F10-15p.1 | The stop rule fired: on the 787 silver volumes outside the dev set, 172 of 239 silver `print` volumes (72.0%) get `print` (67 get `mixed`) and 464 of 548 silver `hand` volumes (84.7%) get `hand` (65 `mixed`, 19 `print`) | negative-result | sg, hand-print, density | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.2 | Density alone cannot define print: dense neat hand reaches 0.45-0.8 (3350: 352 regions above 0.55, 29 of 30 read are handwritten, none printed; all 30 sampled top regions of 10 of the 19 silver-hand volumes labelled `print` are handwritten index/summary text), and the char-weighted histogram has no valley at `lo`/`hi` (0.42-0.44 holds 1.2% of regions) | negative-result | sg, hand-print, density | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.3 | Printed *plakkaten* are the other failure: 67 of 76 silver `print` plakkaten volumes (12xxx) are `mixed` with `print_scan_share` 0.61-0.90, yet 29 of 30 sampled top regions (10 volumes) are printed (large type or OCR noise lowers their density) | negative-result | sg, hand-print, density | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.4 | By the fixed rule 15.6% of the 5.62 G chars (15.96% of labelled chars) are in `print` regions and 11.0% of chars in 223 `print` volumes (of 9,191 inventory keys; 8,358 `hand`, 525 `mixed`, 85 `uncertain`); both numbers are upper bounds on true print, because `print` volumes outside the silver groups are mostly dense hand (7 of 8 read) | data-property | sg, hand-print | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.5 | 525 volumes (5.7%) are `mixed`; their print share is spread (235 of them 0.1-0.2, 52 at 0.8-0.9), and they are interleaved rather than runs: median 36 label changes against 51.7 expected under shuffling, only 105 of 525 below half of that | data-property | sg, hand-print | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.6 | Odd material exists and is mostly not hand: 933 regions of 200 or more chars above density 1.5 in 101 inventories, with real printed English newspaper text in 7461, 7462A, 7462B and 8611 (*Liassen Buitenland*, labelled `hand` or `mixed`), and 4,045 regions below 0.05 in 308 inventories (pension and officer tables, `table/numbers`) | data-property | sg, hand-print | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.7 | Dev-layer page geometry separates the dev volumes where density does not fully: median `page_w` 5,411-6,723 px for hand against 4,621-4,752 px for print (ratio 1.45), 52-96 against 241-244 text lines per scan | data-property | sg, hand-print, resolution | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-15p.8 | The TSV census yields 9,191 inventory keys from 9,122 files: 33 keys (66 scans) come from malformed scan ids such as `11411_0198` (all `uncertain`), the rest from suffixed inventories stored in bare-number files (audit D3) | data-property | sg, paragraphs | confirmed | [Step 15p](#step-15p-handprint-census) |
| F10-P.6 | Of nine size-independent line-geometry features on the dev volumes, only line-box height / baseline pitch (`hrel`) separates hand from print (region AUC 0.91, volume medians 1.62–1.81 against 1.37–1.46), but with a page cut fitted on dev, 3 of 8 hand volumes have 13–19% `print` pages, so they are `mixed` under the 0.9/0.1 volume rule even in sample; 455/456 sit at 1.52/1.57 (**revised in review:** under 15q's definition 455/456 are 1.63/1.86 by region median and 1.72/1.80 by scan median, F10-R.2) | negative-result | sg, hand-print, geometry | revised | [Planning](#planning) |
| F10-15q.1 | The stop rule fired: on the 60-volume silver sample the stratum-weighted agreement is 0.768 for silver `hand` (eerste serie 10/10, tweede serie 14/20) and 0.893 for silver `print` (gedrukte resoluties 10/10, plakkaten 10/10, nouvelles 6/10); both must be >= 0.90 | negative-result | sg, hand-print, geometry | confirmed | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-15q.2 | `hrel` fails on 18th-century hands with wide line spacing (**revised in review:** not "tight" hands; 3590's pitch is 137 px against 80–94 in dev, F10-R.1): 3 silver-`hand` tweede-serie volumes get `print` with shares 0.93-0.97 (3572, 3590, 3630; scan medians 1.40, 1.17, 1.28) although every scan read is handwritten text; 3 more (3303, 3459, 3525) sit at shares 0.51, 0.68, 0.88 | negative-result | sg, hand-print, geometry | revised | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-15q.3 | The dev reproduction holds (thresholds 1.529/1.534, 9,048 labelled scans, 14 of 14 dev volumes in their class), but the re-plan's shares used the unrounded `t_lo` (1.52875): with the brief's rounded 1.529, 3354 reads 0.1627 against 0.157 (+0.0057) and 3772 0.9317 against 0.930, because one scan at 1.528811 flips | method | sg, hand-print | confirmed | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-15q.4 | Transfer fails (P11 applies): 455 and 456 get `hand` (print scan share 0.187, 0.097; scan medians 1.72, 1.80), although both are printed, so a cut fitted on one archive's print does not transfer (**revised in review:** the cause is taller line boxes, 76–82 px against 67–69, at the same 47–48 px pitch, not wide leading; F10-R.1) | negative-result | sg, hand-print, geometry, transfer | revised | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-15q.5 | The volume share is not bimodal outside dev: 27 of 88 evaluated volumes have a print share in 0.3-0.7 (19 of the 20 silver-`print` nouvelles and plakkaten sample volumes lie between 0.39 and 0.68, one at 0.735, so even the correct plakkaten volumes sit only 0.05-0.18 above the 0.5 rule), and 8 volumes have more than 30% of their scan values within 0.05 of the cut | data-property | sg, hand-print, geometry | confirmed | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-15q.6 | Four of ten silver-`print` *nouvelles* volumes (12109, 12120, 12131, 12135; shares 0.39-0.48) get `hand`; the text read is dispatches to the States General with HTR-like noise and a broad `hrel` histogram (1.3-2.3), so either the silver label or the feature is wrong (**revised in review:** density, 64/64 `print` in 15p, and box height, 83–86 px, both agree with the silver label, so the feature is most likely wrong; F10-R.4) | open-issue | sg, hand-print, silver | revised | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-15q.7 | Scan labels within a volume flicker rather than form runs: 3712 has 106 label changes in 266 labelled scans, 12169 has 252 in 647; of the 12 staged UA-017 volumes, 11930 (15p `hand`) gets `print` with share 1.0 and 4617 (15p `print`) gets `hand` at 0.475, the other ten agree in direction with 15p's label or are `mixed` there | data-property | sg, hand-print, geometry | confirmed | [Step 15q](#step-15q-volume-mode-classifier) |
| F10-R.1 | `hrel` = box height / pitch fails for two independent reasons, both opposite to the step's account: handwritten 3590 has hand-sized boxes (157 px) but wide line spacing (pitch 137 against 80–94 in the dev hand volumes), and printed 455/456 have the dev print pitch (47–48 px against 48.5 in 3771) but taller boxes (76–82 against 67–69); the ratio tracks a scribe's spacing and the segmentation model's box convention, not script | negative-result | sg, hand-print, geometry | confirmed | [Review](#review) |
| F10-R.2 | Column interleave does not explain the transfer failure: on 40 random scans each of 455, 456, 3771, 3349 and 3590, 1–7% of regions hold two columns and column-wise `hrel` equals region-wise within 0.03; F10-P.6's 1.52/1.57 for 455/456 does not reproduce (1.63/1.86 region median) | method | sg, hand-print, geometry | confirmed | [Review](#review) |
| F10-R.3 | Exploratory and post hoc (spent sample): the char-weighted median line-box height per volume separates all 76 labelled volumes, print 65–86 px (38, incl. 455/456 and the four failing *nouvelles*), hand 89.5–173 px (38); the margin is thin (12135 at 86.0, 3492 at 89.5) and pixel size depends on scan resolution (F10-15p.7) | data-property | sg, hand-print, geometry | confirmed | [Review](#review) |
| F10-R.4 | The *nouvelles* silver label is probably right: 15p density called all 64 volumes `print`, and the four `hrel` failures have print-sized boxes (83–86 px) | data-property | sg, hand-print, silver | confirmed | [Review](#review) |
| F10-R.5 | The 60-volume silver sample is spent for any geometry rule (15q and the review have seen its geometry): a new test needs a new random sample that excludes it, the 12 UA-017 volumes and dev | method | sg, hand-print, silver | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`, `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Planned 2026-10-23 (Opus) from UA-015's response (option (c)) and the user's answers: phase 10 is the hand/print
work only; the decision unit is the region, with `uncertain` and `mixed` kept as outcomes ("any mixture at any
level"). Facts behind the decisions are F10-P.1–P.5; measurements were taken on the paragraph TSVs of the 14 local
1.01.02 inventories, the `15m-a` dev layer, the SG EAD (`ead_views.to_frame(..., "file")`) and `print_paragraphs_v1`.

**Re-plan of 15q (Opus, 2026-10-23, UA-018 (a)).** Before writing a brief, the session measured line geometry on the
dev layer `15m-a` (`lines` tables; regions with ≥ 3 text lines; script
[`planning-15q/geom_features.py`](data/phase-10-hand-print-per-region/planning-15q/geom_features.py)). Nine features
per region: pitch / `page_w` (a size feature, for comparison only), chars per pitch length, `hrel` = median line-box
`h` / median baseline pitch, left and right edge spread, pitch CV, baseline residual and slope spread, median `conf`.
On the 14 1.01.02 dev volumes (regions ≥ 100 chars), the per-region AUC is 0.50–0.71 for all size-independent features
except `hrel` (0.913; 3350 against print 0.944). A standardised logistic regression on seven of them, cross-validated
by leaving volumes out, reaches 0.885, so it adds nothing. Region labels with a P3-style percentile band (1.49–1.69)
leave 10–68% of chars `uncertain`. Per page (char-weighted median `hrel`, pages ≥ 200 chars), the dev band collapses to
a cut at 1.53, which labels these shares of pages `print`: 11182 0.00, 11281 0.13, 3349 0.05, 3350 0.02, 3351 0.19,
3352 0.01, 3353 0.01, 3354 0.16; 3771–3773 0.95/0.93/0.96, 3821–3823 0.98/0.91/0.93. Under the P3 volume rule, 11281,
3351 and 3354 are `mixed` even in sample. In 455/456 (column-wise pitch) the region medians are 1.52 and 1.57, so
the line-box convention of the other Laypa model shifts the feature (F10-P.6). The silver data were not used. How to
go on is UA-019; a random silver sample for any test is drawn in
[`planning-15q/silver_sample.tsv`](data/phase-10-hand-print-per-region/planning-15q/silver_sample.tsv) (UA-020).
The user chose UA-019 (b): one mode per inventory, by the majority of its scans' `hrel` labels. Region and scan
labels are kept as diagnostics. The rule, its thresholds procedure and the stop rule (weighted agreement ≥ 0.90 per
silver class on the 60-volume sample) were fixed on 2026-10-23, before any silver geometry was computed (phase
`PLAN.md` P2, P3, P10, P11). The remaining steps are 15q (classifier and test), 15r (volume-mode build) and 15s (v4
dev build, audit, run kit).

### Step 15p: hand/print census

**What changed.** No library change. `census.py` labels every PageXML text region of the 9,122 paragraph TSVs by
density (`1000 * chars / (w * h)`) with the fixed rule of the brief, and `analyze.py` and `samples.py` produce the
tables and samples (all in `reports/data/phase-10-hand-print-per-region/step-15p/`; per-region and per-scan outputs
in git-ignored `data/hand_print_census/15p-a/`). Brief: [step-15p](../plan/phase-10-hand-print-per-region/step-15p-hand-print-census.md).
The rule was not changed after seeing the corpus; the stop rule fired, so the step stops after the error analysis.

**Thresholds** (fitted on the 14 dev inventories only; `thresholds.json`): `t_hand` = hand p95 = 0.4411, `t_print` =
print p05 = 0.4219; `t_hand > t_print`, so swapped: **lo = 0.4219, hi = 0.4411**. Dev regions (>= 20 chars): hand
45,971, print 47,034, percentiles identical to the planning numbers within 0.001. At these thresholds 93.5% of hand
dev regions are `hand`, 5.0% `print`, 1.5% `uncertain`; 94.0% of print dev regions are `print` (5.0% `hand`, 1.0% `uncertain`).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| Files processed | 9,122 | 9,122, 0 failures; 20,601,168 regions, 5.62 G chars, 2,394,169 scans, 9,191 inventory keys (F10-15p.8) |
| Region/scan labels (chars) | n/a | `hand` 82.4%, `print` 15.7%, `uncertain` 1.0%, short 1.0%; scans: `hand` 2,104,847, `print` 133,009, `mixed` 83,937, `uncertain` 72,376 |
| Volume labels | n/a | `hand` 8,358, `mixed` 525, `print` 223, `uncertain` 85 (chars 4.52 G / 0.49 G / 0.62 G / 0.6 M) |
| Stop rule: silver `print` -> `print` (dev excluded) | >= 90% | **72.0%** (172 of 239; 67 `mixed`) |
| Stop rule: silver `hand` -> `hand` (dev excluded) | >= 90% | **84.7%** (464 of 548; 65 `mixed`, 19 `print`) |
| Dev volumes | 8 hand, 6 print | 7 hand, 1 `mixed` (3350, print share 0.17); 6 `print` (share 0.98-1.00) |
| Agreement incl. dev | n/a | print 178 of 245 (72.7%), hand 469 of 554 (84.7%); `agreement.tsv` |

**Corpus shape.** The char-weighted density histogram (`density_hist.tsv`) has modes at 0.18 (629 M chars) and 0.86
(71 M) and a shoulder at 0.60 (39 M); the region-count histogram has modes at 0.16 and 0.84. It is bimodal, but there
is no valley at the thresholds: bins 0.40-0.42, 0.42-0.44, 0.44-0.46 hold 138 k, 115 k, 97 k regions. The `lo`/`hi`
band is narrow, the hand tail beyond it is not. `uncertain` regions hold 1.0% of chars. Mixed volumes: 525, with
`print_scan_share` in 0.1-bins 0.1-0.2: 235, 0.2-0.3: 111, 0.3-0.4: 36, 0.4-0.5: 18, 0.5-0.6: 12, 0.6-0.7: 22, 0.7-0.8: 39,
0.8-0.9: 52. Label changes along scan order (hand/print scans only; `mixed_runs.tsv`): median 36 against 51.7 expected if
the same scans were shuffled; 105 of 525 volumes have fewer than half the expected changes, so run-like structure
is the exception. Series (`series_labels.tsv`, top 15 by chars): *Gedrukte resoluties* 105 `print`, *Gedrukte
nouvelles* 64 `print`, but *tweede serie registers van ordinaris net-resoluties* 371 `hand`, 37 `mixed`, 19 `print` and
*eerste serie registers* 94 `hand`, 29 `mixed`; the large hand series (*Eerste/Geresumeerde Minuten*, *ingekomen
brieven*) are 89-97% `hand`.

**Odd material.** Mixed scans (>= 200 labelled chars, >= 20% hand and >= 20% print chars): 55,543 scans in 3,871
inventories (2.3% of scans). Regions with >= 200 chars: 933 above density 1.5 (101 inventories), 4,045 below 0.05 (308
inventories). Layout outliers (median region `w` or `h` outside the corpus 1st-99th percentile, w [334, 1904], h [120,
1136]): 277 inventories (181 `hand`, 52 `print`, 18 `mixed`, 26 `uncertain`). Tables: `mixed_scans.tsv.gz`,
`odd_regions.tsv` (top 5,000 by chars), `layout_outliers.tsv`.

**Resolution** (`resolution.tsv`, dev layer, medians per inventory): `page_w` hand 5,411-6,723, print 4,621-4,752 and
455/456 4,631/4,675: it varies by 45% among the 16 (more than 10%), but the variation follows the hand/print split
(and the lines per scan, 52-96 against 241-244). Density is a per-pixel measure, so scan resolution matters: the
hand volumes are scanned wider, which would lower their density relative to print, not raise it.

**Error analysis.**

- *What was read:* 238 regions from five seeded (15) samples; text only (no images locally), snippets of 220 chars
  in `samples/s*.tsv`, verdicts in `samples/s*_verdicts.tsv`. Verdicts are from the text; where it could not decide
  they are `unclear`.
  1. 3350 regions with density > 0.55 (352 of them; 30 sampled).
  2. 20 `uncertain` regions (>= 100 chars) from 20 random non-dev inventories.
  3. 20 mixed scans (non-dev), their two densest and two sparsest regions (78 regions; 2 scans had fewer).
  4. Silver contradictions: 30 volumes (10 each of silver print -> `mixed`, hand -> `print`, hand -> `mixed`), top region of
     their 3 largest scans (90 regions).
  5. 20 odd cases: 7 density > 1.5, 7 density < 0.05, 6 layout outliers.
- *Categories:*

  | Sample | Verdicts (counts) | Example (verbatim, shortened) |
  |---|---|---|
  | 1. 3350 > 0.55 | handwritten 29, ocr_junk 1 | "Is ter vergaderinge gelesen de reqte. van Johanna huijsen, wede. wijlen Gabriel Granatier; ..." (0.56, 1324x758) |
  | 2. `uncertain` | handwritten 15, ocr_junk 5 | "Ontfangen een missive van den Raed van State gecommitteert tot het generale gouvernement der Spaensche Neder. landen" (0.44) |
  | 3. mixed scans | handwritten 53, printed 8, unclear 11, table/numbers 4, ocr_junk 2 | printed: 1708, "Waar op gedelibereerd zynde, is goed gevonden en verltaan, mids deere te contenteeren" (0.55; `l` for long s); hand: "Is gehoort het rapport van de Heere van Satdlo, haer Ho: Mo: gedep:" (0.42) |
  | 4a. silver print -> `mixed` (rows 1-30) | printed 29, ocr_junk 1 | "De inles is in de eerste Classe 5, in de tweede 10, in de derde 14. in de vierde 16" (12213, 1.04) |
  | 4b. silver hand -> `print` (31-60) | handwritten 30 | "18 Jan: Prov: nogmaal versogt sig te verklaren 5 maij. pasport voor Lynslager." (3448, 0.73, 2187x3384) |
  | 4c. silver hand -> `mixed` (61-90) | handwritten 29, unclear 1 | "Is Mr. Guille Boureel, Raedt en Advocaet van oostindische compe. (alhier by haere ho:Mo: bescreuen) ter vergaderinge binnen gestaen" (3180, 0.74) |
  | 5. odd | printed 9, table/numbers 6, handwritten 4, ocr_junk 1 | printed English: "M. Madison said that the gentlemen who were in favor of the amendment, seemed to be drawn from a kind of theory" (7462B); table: "Thomas Albregt van Rohr., 70, 102: 5. e, 182, 3. Jotans anduen Hamn ..." (8147, 0.03, 2278x2717) |

  Further read (not in the brief): top regions of the two largest scans in 8 of the 26 `print` volumes outside the silver
  groups (47, 3702, 4800, 9200, 9252, 9478, 8828A, 12478): 7 handwritten (dense index entries "Rapport en Res:", ships'
  and army journals), 1 unclear (12478, French treaty text).
- *Conclusion:* the failures are not noise at the threshold. (a) Handwriting with a dense, regular, abbreviated style
  (index volumes inside the net-resoluties, 3350's resolution bodies, 17th-century 3177-3181 registers) has a density of
  0.45-0.8, indistinguishable from print; its text carries the usual HTR signs (break marks, "‛", `Ho:Mog:`, lost
  superscripts). (b) Printed plakkaten set in large type or in a type the OCR misreads are sparse or fragmented and
  fall between the labels, so their volumes read `mixed`; the text is clearly printed ("Bersoon" for "Persoon", "Classe",
  stuyvers). (c) The brief's `mixed` scan label (print share 0.1-0.9) mostly marks a handwritten page with a dense body
  plus sparse headings and margin notes: in sample 3, 53 of 78 regions are handwritten and only 8 printed, all from
  the plakkaten volumes 12230 and the 1708 scan. (d) Real printed insertions do exist outside the catalogued groups:
  English newspaper pages in the *Liassen Buitenland* (7461-7463, 8611: 9 of 20 odd cases) and a Latin treaty (12500A),
  and they sit at density > 1.5. Text alone could not settle 11 mixed-scan regions (11089B, 12308, 8893: 16th-century
  German/Dutch and plakkaat-like passages) or 1 silver-hand region (3177, French treaty text). Density, a single
  per-region number, separates neither by itself; the text-layer features that do differ between the dev volumes
  (lines per scan, page width, F10-P.1) are a different kind of signal and are for 15q to test, not tuned here.

**Staging.** `staging.tsv`: 12 inventories (5 mixed over the print-share quintiles, 3 silver contradictions, 2 odd, 2
`print` outside silver); user action UA-017.

**Findings:** F10-15p.1 - F10-15p.8.

**Open issues.** The census rule is a negative result as a classifier; 15q must be re-planned (see `STATUS.md`).
The 12 staged volumes are still the right validation material, since they are the contradictory and mixed cases.

**Review note (Opus, 2026-10-23).** Re-computed the silver crosstab from `inventories.tsv`; it matches. Per series, the
*gedrukte resoluties* (99/99) and *nouvelles* (64/64) are all `print`; the print failures are all *plakkaten* (9 of 76
`print`, median print char share 0.84) and the hand failures are mostly in the *tweede serie* net-resoluties (55 of 421).
No leakage found. How the phase continues is UA-018.

### Step 15q: volume-mode classifier

**What changed.** New library module `archival_structures/analysis/hand_print.py` (the `hrel` volume-mode rule, thresholds as
required parameters, 7 synthetic tests in `tests/test_analysis_hand_print.py`) and the validation script
`reports/data/phase-10-hand-print-per-region/step-15q/validate.py` (`prepass`, `fit`, `evaluate`). Brief:
[`step-15q-volume-mode-classifier.md`](../plan/phase-10-hand-print-per-region/step-15q-volume-mode-classifier.md). Changelog entry
under *Unreleased*. Nothing in `text_layer/` changed. The order of work was kept: dev pre-pass and fit, acceptance 2-3 checked, then
`thresholds.json` frozen (sha256 `72cc1c042c010cd925c5897184d3e5f8b3d82736d6785d723e382209c22049f3`, recorded in `PROGRESS.md`) before the
transfer, sample and staged sets were read. UA-021 was in place (all 78 tars readable); no `PermissionError`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests | new tests pass; suite green (1,213 collected before) | 7 new pass; full suite 1,220 collected, 1,209 passed, 11 skipped |
| 2. 3349 pre-pass rows = `15m-a` lines table on `(region_id, line_idx, h, baseline, text_raw)` | 100% | 92,152 of 92,152 rows identical |
| 3. Fit | t_lo, t_hi 1.529 / 1.534 (+-0.001); 9,048 +-10 labelled scans; shares within +-0.005; 14 classes | unrounded 1.52875 / 1.53447, rounded 1.529 / 1.534; 9,048 labelled (5,967 hand, 3,081 print); 13 of 14 shares within 0.002; **3354: 0.1627 against 0.157 (+0.0057)**, see *Brief defects*; 14 of 14 volumes in their class (hand shares 0.000-0.187, print 0.917-0.981 among print+hand) |
| 4. Part B (P11) | report | 455: `hand`, share 0.187 (714 labelled scans, median 1.72); 456: `hand`, share 0.097 (723, median 1.80). P11 applies: the 1.10.94 v4 preset takes `{"method": "fixed", "mode": "print"}` |
| 5. Part C (stop rule) | both weighted agreements >= 0.90 | **fails**: hand 0.768, print 0.893 (table below) |
| 6. Part D, protection | table; protected hashes equal | table below; `protect_before.txt` and `protect_after.txt` identical (99,445 lines) |

Stop rule (`stop_rule.json`; Wilson 95% interval of the stratum agreement, on the sample size shown):

| Stratum | Silver | Stratum size | Sample | Agree | Agreement | Wilson 95% |
|---|---|---|---|---|---|---|
| eerste_serie | hand | 123 | 10 | 10 | 1.00 | 0.72-1.00 |
| tweede_serie | hand | 421 | 20 | 14 | 0.70 | 0.48-0.85 |
| gedrukte_resoluties | print | 99 | 10 | 10 | 1.00 | 0.72-1.00 |
| nouvelles | print | 64 | 10 | 6 | 0.60 | 0.31-0.83 |
| plakkaten | print | 76 | 10 | 10 | 1.00 | 0.72-1.00 |
| **silver hand, weighted** | | 544 | 30 | | **0.768** | |
| **silver print, weighted** | | 239 | 30 | | **0.893** | |

The hand figure is dominated by the tweede serie (421 of 544 volumes), where 6 of 20 sampled volumes fail; the interval for that
stratum alone (0.48-0.85) excludes 0.90. The print figure misses by 0.007 and rests entirely on the 4 failing nouvelles volumes
(weight 64): with 10 per stratum, one more nouvelles volume right would give 0.92, so the print side is within sampling noise of the
bar, the hand side is not. Volumes and scans: `sample_volumes.tsv`, `sample_scans.tsv.gz`; dev: `dev_volumes.tsv`, `dev_scans.tsv.gz`,
`dev_eval_*`; transfer: `transfer_*`.

Part D (12 UA-017 volumes, `staged_volumes.tsv`; changes = label changes along scan order among labelled scans):

| Inv | 15p label | Mode | Share | Labelled scans | Changes |
|---|---|---|---|---|---|
| 3712 | mixed | hand | 0.331 | 266 | 106 |
| 11117 | mixed | hand | 0.019 | 267 | 10 |
| 46 | mixed | hand | 0.023 | 522 | 18 |
| 4623 | mixed | hand | 0.314 | 51 | 22 |
| 12169 | mixed | print | 0.617 | 647 | 252 |
| 12184 | mixed | print | 0.519 | 217 | 99 |
| 3463 | print | print | 0.840 | 156 | 43 |
| 3166 | mixed | hand | 0.000 | 356 | 0 |
| 11930 | hand | print | 1.000 | 503 | 0 |
| 9469 | hand | hand | 0.125 | 8 | 2 |
| 9486 | print | print | 0.820 | 50 | 18 |
| 4617 | print | hand | 0.475 | 61 | 27 |

**Error analysis.**

- *What was read:* (a) all 6 silver-`hand` volumes with mode `print` (3303, 3459, 3525, 3572, 3590, 3630): value histogram (0.05 bins)
  and 5 scans nearest the volume median (seed 16, from the 25 nearest) with the 2-3 largest regions' text; (b) all 4 silver-`print`
  nouvelles volumes with mode `hand` (12109, 12120, 12131, 12135), same method; (c) the two lowest-share plakkaten (12197, 12162), 3
  scans each; (d) 455 and 456, 4 scans each; (e) 30 of the 457 dev scans whose label differs from their volume's class (seed 16); (f)
  5 scans at label changes in each 15p `mixed` staged volume (seed 16). Helper scripts `error_analysis.py` and `error_dev_partd.py`;
  verdicts in `verdicts.tsv`. Verdicts follow the 15p scheme and rest on the OCR text only; no scan images were opened.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | Silver-hand volume, text handwritten, geometry `print` (3572, 3590, 3630) | 3 volumes (15 scans read) | 3590 (median 1.17): "tatien als Leedemaaten van de gerefor„ / meerde Religie hadden overgegeeven" |
  | Silver-hand volume, handwritten, values straddle the cut (3303, 3459) | 2 volumes (10 scans) | 3303, median 1.529 (cut 1.529-1.534): "Is ter Vergaderinge gelesen de requeste / van Mr. Willem Veth" |
  | Silver-hand volume, cursive with OCR junk, mostly unlabelled (3525) | 1 volume (5 scans) | "28. ougussis pmyt, passent maryuy de Bonnr gest" (84 labelled, 50 unlabelled scans) |
  | Silver-print nouvelles read as hand (12109, 12120, 12131, 12135) | 4 volumes (20 scans) | 12109: "Hoog Mogende Heeren, / Ik heb voorleeden Pottdas niet geschreeven"; broad `hrel` 1.3-2.3; verdict `unclear` |
  | Transfer: printed volumes read as hand (455, 456) | 2 volumes (8 scans) | 455: "landt, en het selve aengeschreven soodanige ordre / te stellen" (median 1.72, 55 lines) |
  | Plakkaten at the lowest shares (12197, 12162) | 2 volumes (6 scans) | 12197: "Orsque noire Armee sut arrivée le 28 Join au Camp de / Hasselt" (French relation; printed or copied, `unclear`) |
  | Dev: hand-volume scans labelled `print`, index pages and name lists (3349, 3350, 3354) | 5 of 30 read | 3349: "Voogt 330. / Verdure 346, 475. / Vlugt 354." |
  | Dev: hand-volume scans labelled `print`, running text (3351) | 1 of 30 | "10 Juaij. 1703. heeft deselve aen president sijn pteeck na het / leger bekent gemaakt" |
  | Dev: hand-volume scans labelled `print` in 11281 (fair-copy hand, French and Dutch) | 8 of 30 | "Le Magistrat en avoit usé a l'esgard de / nos huissiers detenus" |
  | Dev: print-volume scans labelled `hand`, printed running text with looser leading (values 1.53-1.73) | 15 of 30 | 3772: "Ntfangen een Missive van den Resident / Bilderbeeck" |
  | Dev: print-volume scan labelled `hand`, OCR junk (3823, value 2.08) | 1 of 30 | "De Bewaarders van het agge- / gbregurpl" |
  | Part D label changes (3712, 11117, 46, 4623, 12169, 12184: 5 each) | 30 scans read | 3712 at scan 0110, `hand` to `print` (1.564 to 1.458): "4 Fabr: Missive Magistraat / van Groningen"; the text is the same kind of hand on both sides, so the changes are mostly noise around the cut, not boundaries |

  The dev rows count the 30 sampled scans (14 from hand volumes, 16 from print volumes) by the verdict on each scan's largest region;
  the totals 300 and 157 of the mismatching scans are in `dev_eval_scans.tsv.gz`.
- *Conclusion:* the failure is in the feature, not the threshold. (1) `hrel` is a property of leading, not of print: neat, tight
  handwriting (3590 at median 1.17) is more compact than the loose print of the dev print volumes, so no single cut separates them
  across volumes; the dev fit hid this because the dev hand volumes are loose-leaded. (2) The cut does not transfer across print
  typesetting: 455 and 456 (wide leading, 55-75 lines per page) sit at 1.72/1.80, above the dev hand median, and P11's fixed
  `print` fallback is necessary for 1.10.94. (3) At the dev cut the two classes overlap substantially, so scan labels are dense
  with noise around the cut (mixed volumes alternate every few scans, e.g. 3712 with 106 changes in 266 scans), which makes the
  volume share a continuous quantity (27 of 88 volumes in 0.3-0.7) and the 0.5 rule fragile. (4) The nouvelles failure cannot be
  settled from the text alone: the text reads like dispatches, which are manuscripts, while the silver label says print.
  This is a possible silver-label error and a reason for a visual check before the phase review judges the stop rule.
  (5) The dev "wrong" scans in hand volumes are mostly index pages and lists (many short lines, hence tall rows
  relative to the box), a known weakness of a pitch ratio. Negative result: nothing was tuned after the freeze.

**Brief defects.**

- Acceptance 3 expects shares "within +-0.005" using the rounded thresholds, but the re-plan numbers were computed with the unrounded
  `t_lo`. With the brief's rounded thresholds 3354 is off by 0.0057 (one scan at 1.528811). Everything else reproduces (checked:
  with the unrounded cut all 14 shares are within 0.0004). Judged immaterial, since the rounded rule is the one frozen as specified;
  reported rather than silently accepted.
- The brief's `evaluate --set dev` and `fit` both write `dev_*` files; `evaluate --set dev` writes `dev_eval_*` instead so `fit`'s
  `dev_scans.tsv.gz` is kept. The brief gives no source for scan order (taken from the sorted PageXML file names). The brief's
  `volume_mode` takes line rows; the script needs the same logic from cached region features, so `volume_mode_from_features` was added
  to the module (additive).
- F10-P.6 says 455/456 sit at 1.52/1.57; the scan-level weighted medians measured here are 1.72/1.80 (different aggregate), which
  matters for how the planning finding reads.

**Findings:** F10-15q.1 - F10-15q.7.

**Open issues.** The stop rule fired: the phase goes to review. Silver nouvelles labels need a visual check (see `STATUS.md`).

**Review note (Opus, 2026-10-23).** Numbers re-computed and the rule re-implemented independently (see *Review*). The cases read hold up,
but conclusion items (1) and (2) name the wrong cause (F10-R.1), and the *nouvelles* check cannot change the verdict: the hand side
fails alone (tweede serie upper bound 0.85), and relabelling silver after the test would be post hoc. F10-R.4 makes the label likely right.

### Step 15r: volume-mode build

Not run: the 15q stop rule fired (decision P10). Brief kept: `step-15r-volume-mode-build.md`.

### Step 15s: v4 build, audit, run kit

Not run (depends on 15r). Brief kept: `step-15s-v4-build-and-run-kit.md`.

## Review

Code review: [`REVIEW.md`](../plan/phase-10-hand-print-per-region/REVIEW.md). Verdict: accept as a negative result.

**Re-checked.** 15p's crosstab (its own review). For 15q: the freeze order (`thresholds.json` at 13:52, read-only,
hash as logged, before `transfer_*` 13:53 and `sample_*` 14:00), dev-only thresholds, acceptance 2 (92,152 rows) and 3
(14/14, 9,048 scans), and the stop rule from the volume tables (0.7678 / 0.8929; 0 parse failures). An independent
re-implementation of the P3 rule ([`review/indep_hrel.py`](data/phase-10-hand-print-per-region/review/indep_hrel.py))
on 3303, 12109 and 3126, run on the tars, gives identical scan counts and shares. AS suite 1,209 passed, LotA 368.

**Own error analysis.**

- *Why `hrel` fails* (F10-R.1): median box height and pitch per volume (regions ≥ 100 chars):

  | Volume | Class | h (px) | pitch (px) | `hrel` |
  |---|---|---|---|---|
  | 3349 / 11182 | hand, dev | 130.5 / 167.0 | 80.3 / 93.8 | 1.66 / 1.81 |
  | 3771 / 3822 | print, dev | 67.0 / 69.0 | 48.5 / 48.6 | 1.38 / 1.45 |
  | 3590 / 3572 | hand, read `print` | 157.0 / 151.5 | 137.2 / 111.2 | 1.17 / 1.40 |
  | 12109 / 12197 | print (*nouvelles* / *plakkaten*) | 86.0 / 82.0 | 54.8 / 54.8 | 1.58 / 1.53 |
  | 455 / 456 | print, 1.10.94, read `hand` | 76.0 / 82.0 | 48.3 / 46.6 | 1.60 / 1.85 |

  The handwritten failures keep hand-sized boxes but are spaced far wider; the 1.10.94 print keeps the dev pitch but
  has taller boxes (another Laypa model, the hazard in the phase plan). Box size alone sorts every row of this table.
- *Column interleave* (F10-R.2; [`review/interleave.py`](data/phase-10-hand-print-per-region/review/interleave.py),
  40 scans each, seed 16): two-column regions 17 of 237 (455), 2 of 108 (456), 29 of 548 (3771), 5 of 270 (3349), 7 of
  152 (3590); column-wise medians differ by ≤ 0.03. Rejected as a cause.
- *Box height across the 88 evaluated volumes* (F10-R.3;
  [`review/h_by_volume.tsv`](data/phase-10-hand-print-per-region/review/h_by_volume.tsv)): print 65–86 px, hand
  89.5–173 px, no overlap among the 76 labelled. This is post hoc on data 15q has already used. It is a candidate rule
  for a new pre-registered test (UA-022), not a result. Staged volumes: 11930 138.5 px (hand-sized, though `hrel`
  says `print` 1.0), 3463 86.5, 4617 97.
- *Nouvelles* (F10-R.4): density and box height both say print; F10-15q.6 revised. A visual check is useful only
  if a later test relies on these labels.

**Findings confirmed or revised.** P.1–P.5 and 15p.1–8 confirmed. P.6 revised (455/456 numbers). 15q.1, .3, .5, .7
confirmed; .2 and .4 revised (cause); .4 also loses the wrong `voc` tag; .6 revised (label likely right).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F10-15q.1, F10-R.3 | Decide how hand/print continues: a pre-registered box-height test on a fresh sample, EAD series modes, or close | UA-022 |
| F10-R.5 | Any new test needs a new random silver sample (excluding the 60, the 12 UA-017 volumes and dev) | UA-022 (a) |
| F10-P.1 | The published v2/v3 pilots build 3771–3773, 3821–3823 with the hand rules; documented as a known gap | `docs/text_layer.md`, v3 manifest |
| F10-15q.4 | 1.10.94 needs a fixed `print` mode in any v4 preset (P11) | `plan/CONTEXT.md` §4 |
| REVIEW #5, #6 | `hand_print.volume_mode` `n_scans` omits scans without lines; one weak test assertion | `REVIEW.md` (debt) |
| phase 09 | The full SG text-layer run waits for UA-022 | `plan/CONTEXT.md` §4 |

## Reproduce

Step 15p (from the AS root; `R=reports/data/phase-10-hand-print-per-region/step-15p`):

```
.venv/bin/python -I $R/census.py --self-test
.venv/bin/python -I $R/census.py --fit-only
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15p-census --want 6 --min 2 --wait -- .venv/bin/python -I $R/census.py --workers {workers}
.venv/bin/python -I $R/analyze.py
.venv/bin/python -I $R/samples.py
```

`staging.tsv` was drawn with `random.Random(15)` in an inline script (selection rules in the brief); the verdict
files were written by hand from the sample text.

Step 15q (`Q=reports/data/phase-10-hand-print-per-region/step-15q`; sources and `--inventories` lists in the brief):

```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15q-prepass --want 6 --min 2 --wait -- .venv/bin/python $Q/validate.py prepass --archive 1.01.02 --src data/text_layer_v3_full_inputs/hand --workers {workers}
.venv/bin/python $Q/validate.py fit
.venv/bin/python $Q/validate.py evaluate --set {dev|transfer|sample|staged}
```

Review (`V=reports/data/phase-10-hand-print-per-region/review`):

```
.venv/bin/python $V/indep_hrel.py /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_3303-pagexml.tar.gz 3303
.venv/bin/python $V/interleave.py data/text_layer_v3_full_inputs/print/NL-HaNA_1.10.94_455.zip 455 sg_1.10.94_v3 40
.venv/bin/python -I $V/h_by_volume.py
```
