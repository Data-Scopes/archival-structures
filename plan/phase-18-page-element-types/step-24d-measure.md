# Step 24d: Measure (listlike grid on dev; bars L and M on test; type and placement tables)

> **Phase:** 18, Page element types v1. **Depends on:** 24a (library), 24b (pages), 24c (`gold/dev_gold.done` for the dev
> stage; `gold/test_gold.done` for the test stage). **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every
> session* only), the phase `STATUS.md`, then this file. **Read nothing else unless this brief names it.**

## Context

The gold of 24c labels every non-empty line of 100 pages (60 test, 40 dev; 10 strata) with a page-element type and a
placement. This step measures what AS already has against it: `listlike` (lists and tables against running text; six
fixed cells, one selected on dev), the text layer's margin call, and the layer's line types mapped to the vocabulary.
Two bars, fixed on 2026-11-02 before any read, are tested once; everything else is reported and read, for phase 19.

## Inputs

- `S = reports/data/phase-18-page-element-types/`:
  - `S/step-24b/sample_pages.tsv` (hash in `S/step-24b/SHA256SUMS`): `page_id stratum archive inv split scan_id
    layer_root n_lines_text page_w page_h image_kind image_ref image_file`;
  - `S/step-24c/gold/dev_labels.tsv` (dev; columns `page_id split stratum archive inv scan_id line_id k type placement
    start unclear note reader`), `S/step-24c/gold/test_labels.tsv` (test; `page_id stratum archive inv scan_id line_id k
    type placement start unclear note pre_type pre_placement pre_start changed reviewed page_note`), hashes in
    `S/step-24c/gold/SHA256SUMS`, sentinels `dev_gold.done`, `test_gold.done`. **Row grain: one row per line; key
    (archive, scan_id, line_id)**. `type` has no `generic:` prefix (`heading:date`); its family is the part before `:`.
  - `S/step-24c/prelabels.tsv` (agent labels for all 100 pages).
- Library (24a):
```python
from archival_structures.analysis.page_elements import read_layer_lines, layer_call, listlike_records, type_family, DATE_COMPATIBLE
from archival_structures.analysis.listlike import window_features, list_flag, LISTLIKE_CELLS  # {cell: (cfg or None, rule)}
```
  `read_layer_lines(layer_root, inv, scan_ids={scan_id})` gives the page's non-empty lines with `line_type`,
  `layout_role`, `region_tag`; `layer_call(line)` gives `{"type", "placement", "region_type", "region_placement"}`
  (`type` None for `marginalia`; `placement` "margin" iff `line_type == "marginalia"`).
- Layers: `layer_root` per page (SG on the drive, VOC via `data/text_layer_voc_23b`, others under
  `data/page_elements_layers/24b-a/`). If the drive is unmounted: stop.

## Deliverables (`D = S/step-24d/`)

| File | Content |
|---|---|
| `D/eval_elements.py` | `--stage dev` and `--stage test` |
| `D/dev_lines.tsv`, `D/dev_grid.tsv`, `D/select.json` | dev stage |
| `D/test_lines.tsv`, `D/test_eval.tsv`, `D/test_done.json` | test stage (once) |
| `D/tables/*.tsv` | reported tables (below) |
| `D/cases_*.tsv` | error-analysis reads |
| `reports/phase-18-page-element-types.md` | `### Step 24d: …` section, findings `F18-24d.n` |

## Behaviour to implement

**Joining.** For each page, `read_layer_lines` for its scan, and join to the gold rows on (scan_id, line_id). Assert a
1:1 match (gold lines = the page's non-empty layer lines; 24b's dumps came from the same table). Per line compute:
`listlike` flags for each of the six cells (features over the page's lines; groups as `listlike_records` makes them),
`layer_call`, and the gold. Write `dev_lines.tsv` / `test_lines.tsv` (one row per line, all of these).

**Bar L (lists and tables).** Lines with `unclear == 0` and gold family `running_text` (negative) or `list`, `table`,
`form` (positive). Flag = the cell's `list_flag`. P = TP / flagged, R = TP / positives. **Bar: P ≥ 0.90 and R ≥ 0.80.**

**Bar M (margin).** Lines with `unclear == 0` and gold placement `margin` (positive) or `main` (negative). Call =
`layer_call(line)["placement"] == "margin"`. **Bar: P ≥ 0.90 and R ≥ 0.80.**

**Intervals.** Wilson 95% for P and R, and a **page-cluster bootstrap**: 2,000 resamples of pages with replacement
(`random.Random(20261102)`), recompute P and R, percentiles 2.5 / 97.5. The bar verdict uses the point estimates; the
report gives both intervals.

**Dev stage** (`--stage dev`, needs `dev_gold.done`; never opens `test_labels.tsv`): all six cells' L numbers on dev →
`dev_grid.tsv` (`cell P R F1 TP FP FN n_pos n_neg`). Select the max F1; cells within 0.01 of the max are ties, broken by
the order C0R0, C0R2, C0R1, C1R0, C1R2, C1R1. Write `select.json` (`cell`, its dev numbers, time, `dev_labels.tsv`
hash). Also M on dev, and every reported table on dev (for reading only).

**Test stage** (`--stage test`): refuse if `select.json` is missing, `test_gold.done` is missing, or `test_done.json`
exists. Exclude pages with `reviewed == 0` (report how many). Compute L for the selected cell **and** for C0R0 (the tool
as handed over), M, intervals; write `test_eval.tsv` and `test_done.json` (time, hashes of `test_labels.tsv` and
`select.json`). Runs once. A failed bar is a negative result: no retuning, no second cell tested as the bar.

**Reported tables** (`D/tables/`, on test; the dev versions with a `dev_` prefix):
- `bars_by_stratum.tsv`: L and M per stratum (counts, P, R).
- `layer_types.tsv`: gold family × layer call family (`type_family(layer_call["type"])`, `margin` for marginalia), per
  stratum and pooled. A `date_header` call counts as right when the gold is `heading:date` or `running_title:date`
  (`DATE_COMPATIBLE`). Per family the layer calls (running_text, numbering, running_title, quire_marks, list, custom):
  P and R.
- `listlike_by_type.tsv`: flag rate of the selected cell and of C0R0 per gold family (shows what else it flags).
- `prelabel_agreement.tsv`: agent pre-label against the user's gold, per gold family and per stratum: type-family
  agreement, placement agreement, start agreement, `changed` share.
- `region_tags_voc.tsv`: VOC pages only: `region_type` / `region_placement` against gold (the provider's Laypa tags).
- `gold_counts.tsv`: lines per gold type (with subtype) and placement, per stratum; `unclear` share.

## Constraints

- The test stage runs once; `select.json` is written before `test_done.json` (check the times).
- No library change. If a library function misbehaves, stop and record it (Rule 6).
- Never `git commit`; `chmod g+w` new files; no `rm -rf`, `nohup`, `&`, `$TMPDIR`.

## Tests

`eval_elements.py --selftest`: a synthetic two-page fixture (gold, flags, calls) with hand-computed P/R, the tie rule
(two cells within 0.01), the bootstrap determinism (same seed, same interval) and the refusal rules. Full suite green.

## Acceptance criteria

1. Dev: `dev_grid.tsv` with six cells; `select.json`.
2. Test (after UA-063): bars L and M with P, R, both intervals, verdicts; the tables above; `test_done.json` later than
   `select.json`; 1:1 join on every page.
3. Error analysis written up; findings drafted. Full suite green.

## Error analysis

Read lines with their page context (the two lines above and below, and the image when the text is not enough):
- **L:** all false positives and all false negatives of the selected cell on test, up to 30 each (if more, a
  `random.Random(20261102)` sample). Categories, fixed now: FP — short prose lines (salutation, closing), headings,
  numbering or references, noise, a gold error; FN — name or item lists written as prose lines, tables of dates or
  without amounts, lists with long items, forms, a gold error. Add a category only with a verbatim example.
- **M:** 20 false positives and 20 false negatives (all, if fewer). Categories: table columns or second columns called
  margin (`layout_role == "extra"` or `rule:extra_role`), indented main lines, margin lines missed beside a narrow main
  column, top/bottom furniture, a gold error. Name the `type_method` of each call.
- **Layer types:** the five largest off-diagonal cells of `layer_types.tsv`: 10 lines each.
- **Pre-labels:** the 20 most frequent (pre → gold) changes, 3 examples each.
Write counts and verbatim examples in the report, with a conclusion per bar: what phase 19 should build first.

**Note tokens in the test gold** (user rulings during UA-063; the dev gold has none of them): `colhead` (a table column
heading, type `table`), `cont` (a table line after an interrupting note, `start = 1`), `prose` (a table line, e.g. a
probate-inventory item description, written as full sentences). Match them as words in `note`. Report, without a bar,
the selected cell's recall on `colhead` lines and (only if UA-072 chose (b); otherwise `prose` marks only examples on
P045–P048) on `prose` lines separately, and how many bar-L false negatives carry
each token; read them under the FN categories above.

## Do not touch

- `archival_structures/`, the gold files, the layers, `data/annotations/`, `shared-tools/`, sibling repos.

## When done

1. Add `### Step 24d: Measure` to `reports/phase-18-page-element-types.md` (findings `F18-24d.n`, draft); `make
   reports`. 2. Append a PROGRESS block. 3. Overwrite `STATUS.md` (dev done / test done).
