# Step 15g: Line and region types (R4), with in-line segments

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15fq.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you
> have to explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

LotA's requirement R4: page numbers, running headers, date headers and marginalia must not be
`main_text`. The hand date headers ("21. April", "29. Augti.") must keep their own type, because
LotA's session-date logic uses them. Its `find_session_headers` needs a weekday, so a bare
"21. April" isn't found today. This step adds a v2 `types` stage. It assigns one type and a
method tag to every line **segment**, and a type to every layout region. The old `region_type`
stays retired. A segment is needed because, in the hand minutes, the HTR often merged the page's
date header, its first text line and its page number into **one** TextLine (planning,
F07-P.10). 15h builds units from the segments.

## Inputs

Layer: the final 15fq layer named in `STATUS.md` (`data/text_layer_v2_dev/15fq-a/` unless the
fallback ran, which makes it `15fq-b/`). It has three sub-layers: `hand_tar/` and `hand_local/`
(`NL-HaNA_1.01.02_text_layer_v2/`) and `print/` (`NL-HaNA_1.10.94_text_layer_v2/`). Tables used:
- `scans/<inv>.scans.tsv.gz`: `scan_id, …, page_w, page_h, …`;
- `regions_meta/<inv>.tsv.gz`: `scan_id, page_side, region_id, region_xml_id,
  parent_region_xml_id, region_tag, region_ro_index, x, y, w, h, n_lines, n_lines_text`;
- `lines/<inv>.lines.tsv.gz`: `scan_id, page_side, column, region_id, line_idx, line_id, x, y, w,
  h, baseline, conf, text_raw, text, edit_flags, split_from`;
- `layout/lines/<inv>.tsv.gz`: `scan_id, line_id, region_id, page, layout_region_id,
  layout_line_idx, layout_column, layout_role`;
- `layout/regions/<inv>.tsv.gz`: `scan_id, page, layout_region_id, x, y, w, h, layout_column,
  layout_role, n_lines, source_region_ids, change, furniture, column_value` (the last two from 15fq);
- `layout/stats.json`: per inventory, `med_w_rel` (median `w/page_w` of text lines; from 15fq).

Read tables with `archival_structures.text_layer.io.read_tsv_gz(path)` (yields dicts) and write
them with `write_tsv_gz(path, header, rows) -> n` (deterministic, atomic). Reuse:

```python
from archival_structures.analysis.page_numbering import longest_increasing_chain
#   longest_increasing_chain(values) -> list of indices of a longest strictly increasing subsequence
```

Model the new stage on `build.stage_layout` (`text_layer/build.py` L344–380 at planning; 15fq
edits it): v2 presets only, no `--pagexml-dir`, inventories from `lines/`, manifest via
`run.update_manifest("types", extra)`, `types/params.json` and `types/stats.json` written by the
stage. Add `"types"` to the `--stage` choices. It is not part of `all`.

Measured at planning (15fp tables; 15fq does not change line text or `page` in these counts'
sense):
- The topmost line of a page ends in a number (` 909.`) on 662 of 1,913 pages in 3349, 688/1,767
  in 3350, 685/2,043 in 3352, 648/1,826 in 3353 and 478/1,466 in 11182. On rectos, 96–97% of
  those numbers lie in the inventory's longest increasing chain (3349: 635 of 657; 3352: 652 of
  677). On versos, only 4–8 have such a number, none in the chain.
- The topmost line starts with a date (`28. Junij …`) on 274 pages (3349), 142 (3350), 422 (3352)
  and 497 (3353). It has both a date and a number on 235, 120, 344 and 416 pages.
- A separate date-only line is among a page's top 3 lines on 465 pages (3349), 984 (3771) and
  1,140 (455). A separate number-only line is there on 128 (3349), 2,184 (3771) and 1,199 (455).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/types.py` (new) | `TypeParams`, `TYPES`, `segment_page(...)`, `type_inventory(inv, layer_root, params) -> dict`, and the column lists `TYPE_SEGMENT_COLUMNS`, `TYPE_REGION_COLUMNS` |
| `text_layer/presets.py` | `LayerPreset.types_params: Optional[Mapping] = None`; the SG presets get the patterns below |
| `text_layer/build.py` | `--stage types` (v2 only), `--types-params` JSON (overrides the preset) |
| `tests/test_text_layer_types.py` (new) | See *Tests* |
| `docs/text_layer.md` | Section *Type tables (step 15g)*: classes, methods, schemas |
| `docs/shared_library/changelog.md` | `### Step 15g: line and region types` under *Unreleased*, with Edit (additive) |
| `reports/data/phase-07-sg-units-and-pilot/step-15g/` | `acceptance.py`, `ea.py`, `ea_verdicts.tsv` |
| `reports/phase-07-sg-units-and-pilot.md` | Fill in `### Step 15g: …`; findings `F07-15g.n` |

## Behaviour to implement

`TYPES = ("main_text", "page_number", "running_header", "date_header", "catch_word",
"signature_mark", "marginalia", "index", "other")`. Every line with non-blank `text` gets at
least one segment. Empty lines get none. `TypeParams` (frozen dataclass, `to_dict`, JSON lists →
tuples) holds every threshold below and the patterns. Core defaults are empty patterns; the SG
presets supply:

```python
MONTHS = r"(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)"
WEEKDAY = r"((lun|maen|maan|mart|dins|merc|woens|jov|donder|vene|vry|vrid|sabb|sat|zat|domin|solis|sond|zond)\w*\.?,?\s+)?"
SG_TYPE_PATTERNS = {
  "pn_line":     r"\W*(fol\.?|pag\.?|p\.)?\s*\(?\s*\d{1,4}\s*[.)]?\W*",            # fullmatch
  "pn_suffix":   r"\s\(?\s*(\d{1,4})\s*[.)]?\s*$",                                  # search
  "date_line":   r"\W*" + WEEKDAY + r"(den\s+)?\d{1,2}\s*[.:]?\s*" + MONTHS + r"\w*\s*[.:,]?\W*(1[67]\d\d\W*)?",  # fullmatch
  "date_prefix": r"\W*(den\s+)?\d{1,2}\s*[.:]?\s*" + MONTHS + r"\w*\s*[.:]?(?=\s+\S)",   # match at start
  "month_only":  r"\W*\d{0,2}\s*[.:]?\s*" + MONTHS + r"\w*\s*[.:]?\W*",             # fullmatch
  "year_line":   r"\W*1[67]\d\d\W*",                                                # fullmatch
  "signature":   r"[A-Z]{1,3}\s*\d{0,2}|\d\s*[A-Z]{1,3}",                           # fullmatch
}
```

All matching is `re.IGNORECASE` except `signature`, on the stripped `text`.

**Page context** (per scan and layout `page`): the text lines in y order; *top band* = the 3
topmost, *bottom band* = the 2 bottom-most; *main column* = the `layout_column` with the most
characters among `main` regions; *main left* = the median left edge of the long lines (`w/page_w ≥
0.6·med_w_rel`) in that column; *main centre* = the median line centre there.

**In-line segments** (only these three ever split a line; all others type the whole line):
1. `rule:date_prefix`: the topmost line of a page, or any line starting left of *main left* by
   more than 0.02·`page_w`, matches `date_prefix` at its start → segment `date_header` + rest.
2. `rule:margin_prefix`: otherwise, a line starting left of *main left* by more than
   0.02·`page_w` and ending right of it is cut at the first space whose char-proportional x
   (`x + w·i/len(text)`) is ≥ *main left* − 0.005·`page_w`. The prefix becomes `date_header` if it
   fullmatches `month_only`, else `marginalia`. (Row 19 below: "Augt." at x 0.478, text from 0.517.)
3. `rule:pn_suffix`: on the topmost line of a page, a `pn_suffix` match whose number is in the
   chain (see below) → segment `page_number` at the end.

Segment boundaries exclude the separating whitespace, so `text[char_start:char_end]` is the
stripped segment text. The geometry of a split segment is char-proportional: `x = line.x +
round(line.w·char_start/len(text))`, `w = round(line.w·(char_end−char_start)/len(text))`, `y`/`h`
from the line, `geom_method="char_proportion"`. An unsplit line keeps its box (`geom_method="line"`).

**Page-number chain**, per inventory: one candidate per page, in (scan number, verso before
recto/single) order. It is a `pn_line` line in the top or bottom band, else a `pn_suffix` on the
topmost line. Run `longest_increasing_chain` on the numbers. `pn_line` candidates are typed
`page_number` whether or not they are in the chain (`rule:pn_line` or `rule:pn_line_offchain`).
`pn_suffix` splits happen only for numbers in the chain.

**Whole-line rules**, first match wins, on lines (or the remaining main segment of a split line):
1. `page_number` (`rule:pn_line*`) as above;
2. `date_header` (`rule:date_line`): fullmatch `date_line`, anywhere on the page;
3. `running_header` (`rule:header`): in the top band and fullmatches `year_line`, or has ≥ 4
   letters, ≥ 80% of them uppercase, and ≤ 6 tokens;
4. `catch_word` (`rule:catch_word`): the bottom-most line, one token of ≤ 15 characters, centre
   right of *main centre*;
5. `signature_mark` (`rule:signature`): in the bottom band and fullmatches `signature`;
6. `index` (`rule:index_page`, an experiment): every remaining line on a page where ≥ 50% of the
   text lines are short (`w/page_w < 0.5·med_w_rel`) and ≥ 30% end in `\d{1,4}\.?` (registers,
   F07-15fp.8). It can be switched off (`index_rule=False`);
7. `marginalia`: a line in an `extra` region (`rule:extra_role`), or one whose right edge ≤ *main
   left* + 0.01·`page_w` (`rule:page_margin`);
8. `other` (`rule:noise`): fewer than 2 letters and fewer than 2 digits;
9. `main_text` (`rule:default`).

**Print column override** (F06-15e.8): for print presets (`merged_line_split == "on"`), segments
typed `page_number`, `running_header`, `date_header`, `catch_word` or `signature_mark` get
`column_override="x"`; all others get `""`.

**Laypa check** (455 only): a confusion table of `regions_meta.region_tag` of each line's input
region × the line's type. The rules win; the table is reported, not used.

### Tables

- `types/segments/<inv>.tsv.gz`, one row per segment, in input line order: `scan_id, line_id,
  seg_idx, char_start, char_end, x, y, w, h, geom_method, page, layout_region_id, line_type,
  type_method, column_override`.
- `types/regions/<inv>.tsv.gz`, one row per non-empty layout region: `scan_id, page,
  layout_region_id, region_type` (the type with the most segment characters, ties → order of
  `TYPES`), `type_method` (`char_majority`), `n_segments`, `type_counts` (`main_text:12;page_number:1`).
- `types/stats.json` (per inventory: segments and characters per type, splits per rule, chain
  length and candidates) and `types/params.json`.

## Constraints

- Run on copies: `cp -R` the final 15fq layer to `data/text_layer_v2_dev/15g-a/` and run
  `--stage types` there. Protected: `15d-a/`, `15fr-a/`, `15fp-a/`, the 15fq layers and all of
  `shared-data/`, hashed before and after (15fq's method), into `15g-protected-*.txt`.
- Don't change the extract, layout or line-split code, or their tables.
- LotA's gold sheet (read only):
  `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/gold/02d-hand-print-differences-partial-extended-rescaled.tsv`.
  You only need the row numbers below, which are already resolved.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`.

## Tests

`tests/test_text_layer_types.py`, on synthetic rows:
- `test_date_prefix_and_pn_suffix_split` ("28. Junij verthoonen, als het sullen requireren. 909."
  on a recto top line, chain containing 909 → three segments, offsets exact);
- `test_pn_suffix_not_in_chain_not_split`; `test_margin_prefix_month_only` ("Augt. haer Ho:Mog:"
  starting in the margin);
- `test_date_line_with_weekday` ("Lunæ den 3. Januarii"); `test_page_margin_marginalia`;
  `test_catch_word`; `test_print_column_override`;
- `test_every_text_line_has_segments_and_offsets_cover_text`;
- `test_build_stage_types` on the fixture layer (`tests/fixtures/text_layer/sg/9999`, preset
  `sg_1.01.02_v2`, extract then layout then types, through `build.main([...])`).

Run the new file, then the full suite (take counts from `--collect-only -q`).

## Acceptance criteria

1. **Coverage:** every line with non-blank `text` in all 16 inventories has ≥ 1 segment. The
   segments of a line don't overlap, and their stripped texts, joined with single spaces, equal
   `" ".join(text.split())`.
2. **R4 test rows** (02d sheet; the hand regions were resolved at planning through LotA's
   `difference_units.tsv.gz`). Line texts are verbatim from the 15fp layer:

   | Row | Scan, region (input `region_id` suffix) | Line → expected segments |
   |---|---|---|
   | 50 | 3349_0945, `2801-181-1715-168` | "28. Junij verthoonen, als het sullen requireren. 909." → date_header / main_text / page_number |
   | 52 | 3353_0881, `2560-173-1926-298` | "24. Decemb: van Overijssel hebben de voorz: 847." → date / main / page_number |
   | 55 | 3353_0857, `2485-170-2102-362` | "20: Decemb: uijtgedeelt volgens ende incon, 823." → date / main / page_number; "Nieuwe jaren" (x 0.509–0.583) → marginalia |
   | 134 | 3349_0601, `3304-126-2351-1016` | "21. April Is ter vergaderinge gelesen de reqte. 565." → date / main / page_number; "Diesbagh" → marginalia |
   | 19 | 3350_0284, `3313-109-2136-1136` | "Augt. haer Ho:Mog: geliefden den gem: 247." → date_header ("Augt.") / main / page_number |
   | 9 | 3350_0287, several regions | "29. Augti." → date_header; "de Ruijter" (`3312-2061-242-137`) and "betalinge der" / "Luijck" (`3301-3189-265-386`) → marginalia |

   A page number that is not in the chain fails the row: report why. Row 80 is a reading-order
   case, and it is 15h's test.
3. **Audit §3.5:** in 3771_0100 the resolution text is `main_text` and "( 152 )" is
   `page_number`. Report the `main_text` share of characters per inventory (11182 should hold the
   majority; LotA's old labels gave 98.1% of tokens).
4. **Per inventory:** segments and characters per type; splits per rule; chain length and the
   rectos with a page number. 455: the Laypa confusion table.
5. Full AS suite green; protected hashes unchanged.

## Error analysis

`random.Random(17)` over all 16 inventories; take all cases if a stratum is smaller.
- **Precision per type:** 20 segments each of `page_number`, `date_header`, `running_header`,
  `catch_word`, `signature_mark`, `marginalia`, `index` and `other`. Right or wrong; ambiguous isn't
  counted. **Bar: ≥ 0.90 for `page_number` and `date_header`.** Report the others without a bar.
- **Splits:** 30 split lines (10 per rule). Is the boundary at the right token?
- **Recall:** 40 `main_text` lines from top and bottom bands, 20 hand and 20 print. Count the
  furniture typed `main_text`, with verbatim examples.
- **Index experiment:** 20 pages typed `index` and 10 register pages from 3821–3823 that it
  missed (if any). If page precision < 0.80, set `index_rule=False` in the SG presets, re-run, and
  report a negative result.

## Do not touch

- `extract_v2.py`, `line_split.py`, `layout.py`, `page_structure.py` (15fq's),
  `units.py` (15h); siblings; `shared-data/`.

## When done

1. Fill in `### Step 15g: …` in the phase report: criteria with numbers, the error analysis,
   findings `F07-15g.n` (`draft`). Expected: the precision per type, the in-line split rates and
   whether they hold, and the fate of the index experiment. Then `make reports`.
2. Append the progress block to `PROGRESS.md`; overwrite `STATUS.md` (the types layer path under
   *Facts later steps need*; next step 15h). If a column name or path differs from this brief,
   update briefs 15h, 15hj and 15i.
