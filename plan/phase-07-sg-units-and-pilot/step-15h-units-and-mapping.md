# Step 15h: Units, reading order, the old → new `doc_id` mapping, and the REPUBLIC crosswalk

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15g.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you
> have to explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

LotA aligns hand and print on **units**. R1 asks for the unit ("paragraph") of every line. R5 asks
to keep `doc_id` byte-identical where a region is unchanged, and to give changed units new ids
with an old → new mapping table. This step adds a v2 `units` stage on top of 15fq's layout and
15g's segments. A hand unit is a layout region with its furniture pulled out. A print unit is a
paragraph inside one page column. The stage also writes a reading order, the mapping, and a
crosswalk from REPUBLIC's print line ids, which 15i needs to map the 116 labelled rows. The
unit text and its character offsets are built by step 15hj, which uses this step's tables.

## Inputs

Layer: the 15g layer named in `STATUS.md` (planned: `data/text_layer_v2_dev/15g-c/{hand_tar,
hand_local,print}/`). Tables, headers as of the 15fq and 15g briefs:
- `lines/<inv>.lines.tsv.gz`: `scan_id, page_side, column, region_id, line_idx, line_id, x, y, w,
  h, baseline, conf, text_raw, text, edit_flags, split_from`;
- `regions_meta/<inv>.tsv.gz`: `scan_id, page_side, region_id, …, x, y, w, h, n_lines,
  n_lines_text`. `region_id` **is** the legacy `doc_id`, `f"{scan_id}-text_region-{x}-{y}-{w}-{h}"`;
- `layout/lines/<inv>.tsv.gz`: `scan_id, line_id, region_id, page, layout_region_id,
  layout_line_idx, layout_column, layout_role`;
- `layout/regions/<inv>.tsv.gz`: `scan_id, page, layout_region_id, x, y, w, h, layout_column,
  layout_role, n_lines, source_region_ids, change, furniture, column_value`;
- `types/segments/<inv>.tsv.gz`: `scan_id, line_id, seg_idx, char_start, char_end, x, y, w, h,
  geom_method, page, layout_region_id, line_type, type_method, column_override`.

`io.read_tsv_gz` / `io.write_tsv_gz`; `extract_v2.legacy_doc_id(scan_id, box)` with `box` a dict
`{"x","y","w","h"}`. Model the stage on `stage_types` (15g) / `stage_layout` (`build.py`).

LotA (read only; check that each exists, and stop and name the file if one doesn't):
- REPUBLIC paragraphs: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/paragraphs-455.tsv.gz`
  and `…-456.tsv.gz`. Columns `session_date, resolution_id, para_id, line_start, line_end, text`;
  16,483 and 19,235 rows. Read them with `quoting=csv.QUOTE_NONE, dtype=str`. Line ids look like
  `NL-HaNA_1.10.94_455_0008-line-2677-1993-555-102` (`<scan_id>-line-x-y-w-h`).
- `…/language-of-the-archive/output/parallel_text/1.10.94-1.01.02/difference_units.tsv.gz`
  (318,887 rows; read only `unit_id, para_id, hand_doc_id`) and the gold sheet
  `…/gold/02d-hand-print-differences-partial-extended-rescaled.tsv` (300 rows; join on `unit_id`).

Measured at planning: of 1,603 REPUBLIC `line_start`/`line_end` ids in 455 (a sample), 1,504
match a PageXML line box exactly and 22 within 5 px (F06-P.4; pre-split boxes). REPUBLIC
paragraphs cross columns and pages, and split words across paragraphs (`gel-` | `sen en …`).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/units.py` (new) | `UnitParams`, `build_units_page(...)`, `unit_inventory(inv, layer_root, params) -> dict`, `republic_crosswalk(...)`, the column lists |
| `text_layer/presets.py` | `LayerPreset.unit_params: Optional[Mapping] = None` |
| `text_layer/build.py` | `--stage units` (v2), `--unit-params` |
| `tests/test_text_layer_units.py` (new) | See *Tests* |
| `docs/text_layer.md` | Section *Units and the doc_id mapping (step 15h)*: schemas, id rule, reading order |
| `docs/shared_library/changelog.md` | `### Step 15h: units and doc_id mapping` under *Unreleased*, with Edit |
| `reports/data/phase-07-sg-units-and-pilot/step-15h/` | `acceptance.py`, `ea.py`, `fit_paragraphs.py`, `ea_verdicts.tsv` |
| `reports/phase-07-sg-units-and-pilot.md` | Fill in `### Step 15h: …`; findings `F07-15h.n` |

## Behaviour to implement

Work per layout region (`layout_role` ≠ `empty`), with its segments in (`layout_line_idx`,
`seg_idx`) order. **Units never cross a layout region.** *Furniture types* are `page_number`,
`running_header`, `date_header`, `catch_word` and `signature_mark`.

**Hand presets** (`merged_line_split == "off"`):
- Every furniture segment is its own unit. Exception: consecutive segments of the same furniture
  type form one unit.
- `marginalia`, `index` and `other` segments form units of maximal consecutive runs of one type.
- The `main_text` segments form one unit, which may skip over pulled-out segments. It is cut only
  where a `date_header` or `running_header` segment lies between two of its segments: a new
  session or resolution starts there. Method `hand:region` or `hand:region_cut_at_header`.

**Print presets** (`merged_line_split == "on"`). After 15fq2, a print layout region is one column
of one page (0 mixed regions, F07-15fq2). Known residue (F07-15fq2.3): line-end fragments
("den.", "sen.", "doen.") whose source `column` label is the other column's sit in their own
one-line layout region. They become one-line units here. Don't special-case them, because
units never cross a layout region. Count them in the error analysis: one-line print `main_text`
units of at most 6 characters. Furniture and other non-main segments become units as for hand. `main_text`
segments are cut into paragraphs. A new paragraph starts at main segment *i > 0* if any cue holds;
the method lists the cues that fired, e.g. `print:indent+short_prev`:
- `indent`: `x_i − col_left ≥ indent_frac · med_h`, where `col_left` is the median left edge of
  the region's main segments and `med_h` the median line height;
- `short_prev`: the previous main segment ends left of `col_right − short_frac · col_width`
  (`col_right` is the median right edge; `col_width = col_right − col_left`);
- `gap`: `y_i − (y_{i−1} + h_{i−1}) > gap_factor · med_h`;
- `header`: a `date_header` or `running_header` segment lies between *i−1* and *i*.

`UnitParams` (frozen, `to_dict`, `from_dict`) holds `indent_frac`, `short_frac`, `gap_factor` (each
may be `None` = off). Fit them on a dev split against REPUBLIC paragraphs (below) from this grid,
fixed in advance: `indent_frac ∈ {0.5, 1.0, 1.5}`, `short_frac ∈ {0.1, 0.2, None}`,
`gap_factor ∈ {1.5, 2.0, None}`. Pick the highest dev boundary F1 (ties → the earlier grid
point), write it into `SG_PRINT_V2.unit_params`, and report the test split once. Don't refit
after seeing test.

**Unit ids (R5).** In each layout region, the unit with the most `main_text` characters (ties →
first; if there is none, the unit with the most characters) **inherits** `layout_region_id`. For
an `unchanged` layout region this is the legacy `doc_id`, byte-identical. Every other unit gets
`legacy_doc_id(scan_id, box)`, where `box` is the integer bbox of its segments. If that id is
already taken in the inventory, add 1 to `w` until it is free. Count `n_id_collisions` (expect 0).

**Reading order.** `unit_order` per scan: pages in the order verso, recto (or single); then
`layout_column` ascending; then unit top y; then the first segment's position. Within a unit,
segments keep region order.

**Mapping.** For every legacy region in `regions_meta` (including empty ones), one row per unit
that holds a segment of one of its lines. `link=same_id` if `unit_id == old_doc_id`, else `part`.
`old_status`, repeated on each of the region's rows:
- `identical`: one row, `same_id`, and the unit holds exactly that region's segments;
- `trimmed`: a `same_id` row exists, plus other rows, or the unit holds fewer or more segments;
- `changed`: no `same_id` row;
- `empty`: no text lines (`unit_id=""`).

**REPUBLIC crosswalk** (print): for each distinct `line_start`/`line_end` id, parse the scan and
box. Match against the v2 line boxes of that scan: `exact` (all four equal), else `near` (each
within 5 px; nearest by summed difference), else `unmatched`. A split half has its own box, so a
pre-split REPUBLIC box may be unmatched; count those by checking whether the box contains the
boxes of two halves with the same `split_from` (`match=split_original`, and map to the first half).

**Paragraph reference.** Evaluation pairs are consecutive `main_text` segments *(i−1, i)* in one
print layout region, on scans with at least one mapped `line_start`. A pair is a reference
boundary if segment *i*'s line is a mapped `line_start` (and is its line's first segment).
Dev = 455 scans with an even scan number; test = 455 odd plus all of 456. Report precision,
recall and F1, and the number of `line_start`s that could not be mapped.

### Tables (`units/…`, gzip TSV)

- `units/units/<inv>.tsv.gz`: `scan_id, page, unit_id, unit_order, unit_type, unit_method,
  layout_region_id, layout_column, x, y, w, h, n_segments, n_lines, first_line_id, last_line_id`.
  `unit_type` is the type of its segments: one type per unit.
- `units/segments/<inv>.tsv.gz`: `scan_id, unit_id, seg_order, line_id, seg_idx`.
- `units/doc_id_map/<inv>.tsv.gz`: `old_doc_id, unit_id, link, old_status, n_segments_shared,
  n_chars_shared`.
- `units/republic_lines/<inv>.tsv.gz` (print): `republic_line_id, scan_id, line_id, match`.
- `units/stats.json`, `units/params.json`.

## Constraints

- Run on a copy: `cp -R` the 15g layer to `data/text_layer_v2_dev/15h-a/`. Protected: every
  earlier dev layer (`15d-a`, `15fr-a`, `15fp-a`, `15fq-*` incl. `15fq-c`, `15fq2-noguard`, `15g-a`, `15g-b`, `15g-c`, `15g2-all`), all of `shared-data/` and
  LotA's `data/`, `gold/` and `output/`. Hash them (15fq's method) into `15h-protected-*.txt`.
- Don't change the extract, layout, types or line-split code or their tables.
- No text joining here: that is 15hj.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`.

## Tests

`tests/test_text_layer_units.py` (synthetic rows):
- `test_hand_furniture_pulled_out_main_keeps_id` (region with a top line split into date / main /
  page number → 3 units; the main unit inherits the region id);
- `test_hand_main_cut_at_date_header`; `test_marginalia_inside_region_does_not_cut_main`;
- `test_print_paragraph_cues` (indent, short_prev, gap, header, each alone);
- `test_new_id_collision_bumps_w`; `test_reading_order` (verso before recto, column, y);
- `test_doc_id_map_status` (identical, trimmed, changed, empty);
- `test_republic_crosswalk_exact_near_split_original`;
- `test_build_stage_units` on the fixture layer through `build.main` (extract → layout → types →
  units).

Run the new file, then the full suite.

## Acceptance criteria

All 16 inventories unless stated.
1. **Coverage:** every segment of `types/segments` is in exactly one unit; `unit_id` is unique per
   inventory; every unit has one `unit_type`.
2. **R5:** for every `unchanged` layout region, its legacy id appears in `doc_id_map` with
   `link=same_id`, byte-identical. Every `regions_meta` row appears in `doc_id_map`. Report
   `old_status` counts per inventory.
3. **Stable keys:** all 300 `hand_doc_id`s of the 02d sheet (through `difference_units` on
   `unit_id`) appear as `old_doc_id`. Report their `old_status` counts.
4. **Row 80** (3353_0754, recto): the unit holding "Noch wert alhier gebragt het extrs:" (input
   region suffix `3604-3785-1254-132`, y 0.917) is not adjacent in `unit_order` to the unit holding
   "in te huuren." (region `3617-1633-1232-221`, y 0.423).
5. **Crosswalk:** match counts per print inventory (exact / near / split_original / unmatched).
6. **Paragraphs:** the dev grid table, the chosen point, and test P/R/F1 once. No bar: REPUBLIC is
   a reference, not gold. Also report the units per type and per inventory, and the median main
   unit length in lines.
7. Full AS suite green; protected hashes unchanged.

## Error analysis

`random.Random(18)`; take all cases if a stratum is smaller.
- **Hand units:** 30 regions whose `old_status` is `trimmed`: is each pulled-out unit really
  furniture or a margin note, and does the main unit read as one text?
- **Print paragraphs:** 20 false-positive and 20 false-negative boundaries on test. For each, say
  whether REPUBLIC or the cue is right (REPUBLIC also errs).
- **Ids:** 20 `changed` legacy regions: does the mapping let a reader find the region's text again?
- **Crosswalk:** every `unmatched` REPUBLIC line, up to 20: why?

## Do not touch

- `extract_v2.py`, `line_split.py`, `layout.py`, `page_structure.py`, `types.py` (except
  importing `TYPES`), `join.py`; siblings; `shared-data/`.

## When done

1. Fill in `### Step 15h: …` in the phase report: criteria with numbers, the error analysis,
   findings `F07-15h.n` (`draft`). Expected: the `old_status` distribution (how many LotA keys
   survive), the paragraph result against REPUBLIC, the crosswalk rate. Then `make reports`.
2. Append the progress block to `PROGRESS.md`; overwrite `STATUS.md` (the units layer path and
   the chosen `UnitParams` under *Facts later steps need*; next step 15hj). If a path or column
   differs from this brief, update briefs 15hj and 15i.
