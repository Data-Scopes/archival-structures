# Step 22d: Index links on VOC tables of contents (bar E against GLOBALISE document starts)

> **Phase:** 16, A second archive (1.04.02 VOC): what generalises. **Depends on:** 22a (the layer), 22b (the frozen
> map cell). Not on 22c: this step uses the gold TOC sides.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

`analysis/index_links.py` (phases 14–15) links each folio number in a front index to the scan that carries the folio.
It does this with locator rules L1–L7, written for SG index entries that **end** in a page number, and the folio map.
VOC tables of contents (TOCs, *Register der brieven*) put the folio numbers in short lines of their own or as a range
at the **start** of an entry ("288 a 291. Copie missive…").

This step measures links on the 21 VOC TOCs (GLOBALISE gold), independently of the detector, by taking the TOC
sides from the gold:
1. index links v1 and v2 **unchanged**;
2. one VOC locator variant, L8, fixed at planning;
3. dev selection, then the test once.

A link is right when the folio leaf it points to holds a gold document start. The user approved that use on
2026-10-31: **scoring only**, with no rule fitted on starts and no start-detection feature. NLP owns start detection
on these volumes.

## Inputs

Paths are relative to the AS root, checked 2026-10-31.

- **Layer (from 22a):** `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/`; the exact root is in
  `STATUS.md`. Tables:
  - `scans/`, `layout/pages/`, `layout/lines/`, `lines/`, `types/segments/`, `regions_meta/`;
  - `units/units/<inv>.tsv.gz` (`scan_id page unit_id unit_order unit_type unit_method layout_region_id layout_column
    x y w h n_segments n_lines first_line_id last_line_id`);
  - `join/units/<inv>.tsv.gz` (`scan_id unit_id unit_type word_break_chars n_breaks_merged ends_with_break_char
    text`).
- **The frozen map cell from 22b** (`STATUS.md`, *Facts later steps need*): M (`FolioMapParams()` or `FOLIO_MAP_V2`),
  S (`single_foliation`) and N (`number_tags` for `read_volume_inputs`). Call it `SEL`.
- **Gold (frozen):** `reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv` (sha256 starts
  `095e7f3198ac3542`): `inv row scan_id toc start split`. `start = 1` when *TANAP Boundaries* is `START` or
  `END/START`. Dev and test are the 22c split:
  - dev 1267 1274 1388 1547 1557 2548 2555 3142 7923 8121 8697;
  - test 1120 1529 1539 2448 2775 3891 8023 8237 8276 8284 8834.

  **2775 is planner-seen** (its TOC text was read at planning): it is left out of the test bar and reported on its
  own. 8023 has no TOC.
- **Silver:** the OBP digitised indexes,
  `ground_truth/NL-HaNA_1.04.02/globalise_digitized_indexes/source/GLOBALISE - Digitized Indexes of the Dutch East India
  Company OBP (1602-1799).csv`. Read them with `archival_structures.datasets.globalise_index.load_globalise_index(path,
  inventories=[...])`, which returns `inventory_number folio_start folio_end …` (folios numeric). The 22 have
  4–375 rows each.
- **Code (current):**

```python
# archival_structures/analysis/index_links.py
@dataclass(frozen=True)
class LocatorParams:  # variant; separators=r"[\s.,:;]+"; ordinal_after=""; month_after="";
    # quantity_after=""; amount_before=""; range_slack=6; sub_entry_punct=False; left_boundary=False
SG_LOCATOR_PARAMS: dict  # "L1".."L7"; v1 default "L2", v2 "L7"
def extract_locators(text, params, *, folio_range=None) -> list[Locator(number, char_start, char_end)]
    # anchor `def extract_locators(`: runs of numbers; L2+ keep a run at the text end or before a capital; L3/L4/L6/L7
    # exclusions (ordinal, month, quantity after; amount before; L6+ left word boundary); L4/L7 keep numbers within
    # folio_range ± range_slack. Raises ValueError for an unknown variant.
def volume_links(inputs, units, texts, params, map_params=FolioMapParams()) -> Optional[dict]
    # front_index_run(inputs.flags) or None; entries = main_text units on flagged sides up to the last flagged one;
    # excl = sides with index <= last or flagged; folio_range = chain (first, last) number
def read_volume_inputs(layer_root, inv, *, number_tags=()) -> VolumeInputs   # number_tags added by 22b
# archival_structures/analysis/folio_map.py: build_folio_map, FolioMapParams, FOLIO_MAP_V2 (see 22b)
# scripts/build_index_links.py reads units = units/units rows, texts = {(scan_id, unit_id): text} from join/units
```

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_links.py` | `LocatorParams` gains `lead_pattern: str = ""`, `standalone_pattern: str = ""`; variant `"L8"` in `extract_locators`; `VOC_LOCATOR_PARAMS = {"L8": …}` (below). L1–L7 unchanged |
| `tests/test_analysis_index_links.py` | L8 tests (below) |
| `docs/index_links.md` | an "L8 (VOC, phase 16)" paragraph |
| `docs/shared_library/changelog.md` | `### Step 22d: …` under `## Unreleased` (Edit); additive |
| `reports/data/phase-16-voc-generalisation/step-22d/` | `eval_links.py`, `dev_grid.tsv`, `select.json`, `test_done.json`, `links_eval.tsv`, `silver.tsv`, `end_to_end.tsv`, `cases_read.tsv` |
| `reports/phase-16-voc-generalisation.md` | `### Step 22d: …` section, draft findings `F16-22d.n` |

## Behaviour to implement

**1. L8** (in `extract_locators`, `params.variant == "L8"`; `text` is **one line**):
1. Let `core = text.strip(" \t„\"'.:,;-—–")`. If `params.standalone_pattern` full-matches `core` (IGNORECASE),
   the locator is group 1, with offsets mapped back into `text`.
2. Else, if `params.lead_pattern` matches at the start of `text` (`re.match`, IGNORECASE), the locator is group 1.
3. Else, the line goes to the L7 rules. Implement this by calling the existing code path with the L7 variant and the
   same exclusion fields.
4. In all three cases, keep a number only if it lies within `folio_range` ± `range_slack` (when `folio_range` is
   given).

```python
_VOC_PREFIX = r"(?:(?:fo|fol|folio|pag|pag:s|p)\.?\s*)?"
_VOC_RANGE = r"\s*(?:a|tot|en|—|–|-)\s*\d{1,4}"
VOC_LOCATOR_PARAMS = {"L8": LocatorParams(
    variant="L8", ordinal_after=SG_LOCATOR_PARAMS["L7"].ordinal_after, month_after=SG_LOCATOR_PARAMS["L7"].month_after,
    quantity_after=SG_LOCATOR_PARAMS["L7"].quantity_after, amount_before=SG_AMOUNT_BEFORE_V2, sub_entry_punct=True,
    left_boundary=True,
    standalone_pattern=_VOC_PREFIX + r"(\d{1,4})(?:\s*(?:verso|v)\.?)?(?:" + _VOC_RANGE + r"(?:\s*(?:verso|v)\.?)?)?",
    lead_pattern=r"\W{0,3}" + _VOC_PREFIX + r"(\d{1,4})\s*(?:verso\s*)?" + _VOC_RANGE[3:] + r"\b")}
```

(`_VOC_RANGE[3:]` drops the leading `\s*`, because `\s*` comes before it already.) A bare `f` is not a prefix: in VOC
text `f`/`ƒ` marks guilders. Checked at planning against these lines (SA = standalone, LE = lead): "„ 1492:" SA 1492; "288 a 291. Copie…" LE 288 (L7 alone gives 291, the range end); "1999 en 2000 Originele…" LE 1999 (L7: none); "ƒ 4800:—" none. The patterns were written from dev TOC text at planning ("„ 1492:", "158.", "288 a 291.
Copie…", "1999 en 2000 Originele…", "fo. 17 verso a 78", "1960 tot 1962").

**2. Oracle TOC sides.**
- The TOC sides of a volume are its `layout/pages` sides on gold `toc = 1` scans.
- The **front run** is the TOC sides from the first one up to the first gap of more than 20 sides without a TOC
  side.
- TOC sides after that gap (2555: scans 621–625, 745–749) are **later TOCs**. They are entries too, and their links
  are reported apart.
- `excl` = all TOC sides, plus every side with index ≤ the last front-run side.

**3. Cells** (map built with `read_volume_inputs(LAYER, inv, number_tags=…)` and `build_folio_map(…,
exclude=excl, params=…)`; `folio_range` = the chain's first and last number):

| Cell | Locators | Entries | Map | Meaning |
|---|---|---|---|---|
| U1 | `SG_LOCATOR_PARAMS["L2"]` | `main_text` units on TOC sides, `join/units` text | `FolioMapParams()`, N=`()` | index links v1 as is |
| U2 | `SG_LOCATOR_PARAMS["L7"]` | same | `FOLIO_MAP_V2`, N=`()` | index links v2 as is |
| C1 | L7 | same | `SEL` | v2 locators, VOC map |
| C2 | `VOC_LOCATOR_PARAMS["L8"]` | every line of the TOC sides (`lines` text) | `FOLIO_MAP_V2`, N=`()` | VOC locators, SG map |
| C3 | L8 | lines | `SEL` | VOC locators and map |

If `SEL` equals the U2 map (22b selected the unchanged cell), drop C1 and C3 as duplicates.

**4. Scoring, per link** (one locator):
1. `t = fmap.locate(N)`. Unlocated (`None`) counts as a miss.
2. `pos` = index of `t.front[0]` in `scan_ids`; leaf = `{scan_ids[pos], scan_ids[pos + 1]}`.
3. **E hit:** the leaf contains a gold `start = 1` scan. **E** = hits / all locators.

Also report:
- **coverage**: the share of gold start scans after the front run's last scan that lie in some link's leaf;
- **chance**: E with every leaf moved +3 positions;
- the located share.

**Silver** (report only): per volume, the precision and recall of the set of locator numbers against the OBP
`folio_start` set.

**5. Dev, selection, test.**
1. Run all cells on dev → `dev_grid.tsv`.
2. **Selection:** max dev F1 of (E, coverage). Ties within 0.005 go to U2, then U1, then the fewest changes.
3. Write `select.json`.
4. **Test once:** the selected cell, U1 and U2, on the test volumes without 2775 (8023 gives no locators). Report
   2775 on its own → `test_done.json` and `links_eval.tsv` (`inv split cell scan_id page source_id loc_idx number
   front_scan_id method leaf_hit chance_hit toc_part`).
5. **Bar E (VOC):** test E ≥ 0.90, with numerator/denominator and a Wilson interval, for each of the three cells.
6. **Verdict:** `unchanged` if U2 passes, `reparameterised` if only a VOC cell passes, `fails on VOC` otherwise.

**6. End to end, unchanged, reported only:** `volume_links(read_volume_inputs(LAYER, inv), units, texts,
SG_LOCATOR_PARAMS["L2"])` with the layer's own flags, for the 22 → `end_to_end.tsv` (`inv front links`). It shows
what the shipped pipeline does on VOC without gold sides.

## Constraints

- L1–L7 outputs unchanged: the existing tests pass, and on one SG volume (19e-a 3444) `volume_links` with L2 and with
  `volume_links_v2` give the same links before and after your edit. Compare the dicts' `links` lists.
- No gold or silver in library code. Test once, after `select.json`; a failed bar is a negative result.
- Do not open 2775's TOC text before the test run, and do not open `planning/toc_peek_dev.tsv` or
  `planning/gm_proto_dev.tsv`.
- Never `git commit`; `chmod g+w`; zsh; no `rm -rf`, `nohup`, `&`, `sed -i`; Edit for shared files. Single process.

## Tests

`tests/test_analysis_index_links.py`. With L8 and `folio_range=None`:
- `"„ 1492:"` → [1492], with offsets pointing at "1492";
- `"288 a 291. Copie missive"` → [288];
- `"fo. 17 verso a 78"` → [17];
- `"1999 en 2000 Originele missive"` → [1999];
- `"ƒ 4800:—"` → [];
- `"61. 10. —"` → [] (not standalone, no lead range; the L7 path gives [], checked at planning);
- `"Missive van den 4 Junij aan … 135"` → [135] (the L7 path).

With `folio_range=(100, 300)`, `"„ 1492:"` → []. An unknown variant still raises. The full suite stays green.

## Acceptance criteria

1. New tests and the full suite green; the SG check is unchanged.
2. `dev_grid.tsv` has one row per cell; `select.json` is time-stamped before `test_done.json`.
3. Bar E (VOC) for U1, U2 and the selected cell, with counts and Wilson intervals; coverage, chance, silver, 2775;
   the verdict.
4. `end_to_end.tsv` for the 22.

The user decides when the step is done.

## Error analysis

Read 30 non-hits of the selected cell on dev (seed 20261031) and every test non-hit, up to 30. Give the locator line,
the located scan, and the nearest gold start. Classify:
- not a folio (amount, date, count);
- a right folio on the wrong scan (map error);
- a right folio, but the document starts on the leaf's neighbour (a verso or a run-on);
- the TOC lists a subdocument the GLOBALISE gold has no start for;
- HTR misread;
- unclear.

Read 15 lines on dev TOC sides where L8 found **no** locator but the OBP index has a folio on that page's range, and
classify why. Write `cases_read.tsv`.

Draft findings are expected for:
- E for U1, U2 and the selected cell;
- coverage;
- the chance level;
- what the SG locators pick up on VOC TOCs (by category);
- silver;
- end to end.

## Do not touch

`folio_map.py` (frozen after 22b), the layer, presets, `layout.py`, `index_pages.py`, `volume_links_v2`'s defaults,
the ground-truth files, NLP and LotA.

## When done

1. Add `### Step 22d: Index links on VOC tables of contents` to `reports/phase-16-voc-generalisation.md` (template
   `plan/templates/REPORT.md`), with findings `F16-22d.n` (status `draft`). Run `make reports` and fix any warning.
2. Append the `PROGRESS.md` block.
3. Update `STATUS.md`: the bar result and the selected cell.
