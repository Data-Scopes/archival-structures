# Step 25a: Gold format v2, dev conversion, scan sizes

> **Phase:** 19, Page elements v2. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phase 18 built line gold for page element types (type, placement, a `start` flag) on 100 pages. Its element model
could not express an element interrupted by another (a note inside a table), and the "cells are prose" property was a
free-text note on 16 pages only. Phase 19 tests two new detectors on fresh gold and develops them on the phase-18
pages. This step fixes the **gold format v2** in code (a module that reads, writes, validates and converts it), adds
three vocabulary entries, makes `Element` carry ids and item starts (additive), converts the phase-18 gold into two v2
dev sets, and adds a scan-size reader the detectors need.

## Inputs

- `archival_structures/datasets/vocabulary.py` (183 lines). `GENERIC_TYPES: Dict[str, Optional[FrozenSet[str]]]`
  at L64–91 (anchor `GENERIC_TYPES: Dict`), currently `'table': frozenset()`. Also `PLACEMENT_TYPES` (main, margin,
  top, bottom, interlinear), `def make_tag(namespace, type, subtype=None, number=None) -> str`,
  `def is_known_tag(tag: str) -> bool` (checks the type, **not** the subtype). Tests: `tests/test_datasets_vocabulary.py` (23).
- `archival_structures/datasets/annotations.py` (407 lines). Imports at L20–28 (`asdict, dataclass, field`; `Dict,
  List, Optional`). `ElementSpan` (L102, anchor `class ElementSpan`): `scan_id: str`, `line_ids: List[str]`.
  `Element` (L110, anchor `class Element`):
  ```python
  element_type: str
  spans: List[ElementSpan]
  def to_dict(self) -> dict:
      return {'element_type': self.element_type, 'spans': [asdict(s) for s in self.spans]}
  @staticmethod
  def from_dict(data: dict) -> "Element":
      return Element(element_type=data['element_type'], spans=[ElementSpan(**s) for s in data['spans']])
  ```
  Also `ScanAnnotation(scan_id, opening=None, tags=[], pages={}, lines={}, regions=[])` with `lines: Dict[str,
  List[str]]` (line id → tags); `scan_annotation_path(institute_id, archive_id, inventory_num_id, scan_id,
  annotations_dir=ANNOTATIONS_DIR) -> Path`; `elements_path(institute_id, archive_id, inventory_num_id,
  annotations_dir=ANNOTATIONS_DIR) -> Path`; `save_scan_annotation(annotation, path)`; `save_elements(elements, path)`;
  `ANNOTATIONS_DIR` (= `data/annotations`, the user's store: **never write there**). Tests:
  `tests/test_datasets_annotations.py` (16).
- `archival_structures/analysis/page_elements.py` (103 lines): `read_layer_lines(layer_root, inv, scan_ids=None) -> list`
  and the private `_table(layer_root, kind, inv)` path helper (L25). Text-layer tables are gzipped TSV read with
  `archival_structures.text_layer.io.read_tsv_gz(path)` (yields dict rows of strings). The scans table is
  `<layer_root>/scans/<inv>.scans.tsv.gz` with columns including `scan_id`, `status`, `page_w`, `page_h` (pixels).
  Tests: `tests/test_analysis_page_elements.py` (6; synthetic layer built by a `build()` helper in that file).
- Phase-18 gold (read only; check `reports/data/phase-18-page-element-types/step-24c/gold/SHA256SUMS` first with
  `shasum -a 256 -c`, from that directory; stop if it fails):
  - `gold/test_labels.tsv` (dev-A here): 4,334 rows, 60 pages P001–P060, columns `page_id stratum archive inv scan_id
    line_id k type placement start unclear note pre_type pre_placement pre_start changed reviewed page_note`.
  - `gold/dev_labels.tsv` (dev-B here): 3,167 rows, 40 pages P061–P100, columns `page_id split stratum archive inv
    scan_id line_id k type placement start unclear note reader`.
  - Both are tab-separated, `quoting=csv.QUOTE_NONE, escapechar="\\"`; read with `csv.DictReader(f, delimiter="\t",
    quoting=csv.QUOTE_NONE, escapechar="\\")` and `csv.field_size_limit(10**9)`. `type` has no `generic:` prefix
    (`running_text`, `heading:date`, `custom:letterhead`, …). `k` is the line's position in the page dump (1-based).
- `docs/vocabulary.md` (110 lines; `## The five namespaces` L33–86), `docs/page_elements.md` (no `##` sections yet),
  `docs/shared_library/changelog.md` (`## Unreleased` at L7 as of planning; if it is gone, add it above the newest
  release heading).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/datasets/vocabulary.py` | `'table': frozenset({'header'})`; new `'letterhead': frozenset()`, `'copymark': frozenset()` |
| `archival_structures/datasets/annotations.py` | `Element` gains `element_id: Optional[str] = None`, `properties: Dict[str, str] = field(default_factory=dict)`, `item_line_ids: List[str] = field(default_factory=list)`; `to_dict` omits each when at its default; `from_dict` reads them when present |
| `archival_structures/datasets/element_gold.py` (new, stdlib + AS imports) | format v2: constants, `read_gold`, `write_gold`, `type_error`, `validate_rows`, `element_table`, `from_v1_rows`, `to_annotations` |
| `archival_structures/analysis/page_elements.py` | `def read_scan_sizes(layer_root, inv) -> dict` |
| `tests/test_datasets_element_gold.py` (new), `tests/test_datasets_annotations.py`, `tests/test_datasets_vocabulary.py`, `tests/test_analysis_page_elements.py` | tests below |
| `reports/data/phase-19-page-elements-v2/step-25a/convert_dev.py` | writes `dev/devA_v2.tsv`, `dev/devB_v2.tsv`, `dev/devA_elements.tsv`, `dev/devB_elements.tsv`, `dev/SHA256SUMS` |
| `docs/vocabulary.md`, `docs/page_elements.md` | the three entries and the form/table rule; a `## Gold format v2` section |
| `docs/shared_library/changelog.md` | `### Step 25a: Gold format v2` under `## Unreleased` (Edit only): additive, "no sibling imports affected" |
| `reports/phase-19-page-elements-v2.md` | `### Step 25a: …` section, findings `F19-25a.n` |

## Behaviour to implement

**Vocabulary.** `table:header` = a column-head row of a table. `letterhead` = a printed or written sender heading at the
top of a letter or form (institution, address). `copymark` = a label or formula saying the text is a copy or collated
("Copie", "Copia", "Accordeert"). Document in `docs/vocabulary.md` next to the existing generic types, with the
**form vs table rule**: a ruled or pre-printed grid of repeated rows (registers, ledgers, inventories with an amount
column) is `table`, its column heads `table:header`; `form` is a document with captions beside blanks filled once per
document (`form:label` caption, `form:value` entry). Item descriptions written as sentences in an amount table stay
`table` (their element gets `prose = 1`); an insertion of omitted text is `addition` with its own placement.

**`Element` (additive).** Old JSON must stay byte-identical: `Element('generic:table', [span]).to_dict()` returns
exactly `{'element_type', 'spans'}`; keys `element_id`, `properties`, `item_line_ids` appear only when not default.
`from_dict` accepts old dicts (missing keys → defaults).

**`element_gold.py`.**
```python
GOLD_V2_COLS = ["page_id", "stratum", "archive", "inv", "scan_id", "line_id", "k", "type", "placement",
                "element", "item_start", "prose", "unclear", "note"]
ELEMENT_COLS = ["page_id", "element", "type", "prose", "n_lines", "first_k"]
ITEM_FAMILIES = frozenset({"list", "table", "form"})
def read_gold(path) -> list[dict]          # DictReader as above; values are strings
def write_gold(rows, path, cols=GOLD_V2_COLS) -> None   # header + rows, QUOTE_NONE, escapechar "\\", lineterminator "\n"; chmod g+w
def type_error(t: str) -> str | None       # None if ok
def validate_rows(rows) -> list[str]       # "<page_id> k<k>: <message>" per error, [] if valid
def element_table(rows) -> list[dict]      # ELEMENT_COLS, one row per (page_id, element), pages in input order, elements e1, e2, …
def from_v1_rows(rows) -> tuple[list[dict], dict]   # (v2 rows in GOLD_V2_COLS, stats)
def to_annotations(rows, ann_dir) -> list[Path]     # written files
```
- `type_error`: error if the type starts with `generic:`, equals `marginalia` or starts with `marginalia:`; if
  `is_known_tag("generic:" + t)` is false; if a subtype is given and the type is not `custom` and the subtype is not in
  `GENERIC_TYPES[type]`. `custom:<anything>` is valid.
- `validate_rows` per page (rows grouped by `page_id`, sorted by int `k`): `k` = 1…n without gaps; `type_error`;
  `placement` in `PLACEMENT_TYPES`; `unclear` in {0,1}; a non-empty `note` when `unclear == "1"` or the type is
  `custom…`; `element` matches `e[1-9][0-9]*`, and the ids appear in first-appearance order `e1, e2, …` without gaps;
  all lines of one element share the **family** (type before the first `:`); `item_start` in {"0","1",""} and
  `prose` in {"0","1",""}; both `""` or "0" on lines outside `ITEM_FAMILIES`; `prose` constant within an element;
  if any `item_start` of an `ITEM_FAMILIES` element is not `""`, none is `""` and its first line has "1".
- `element_table`: `type` = the family; `prose` = the element's prose value; `n_lines`; `first_k` = smallest k.
- `from_v1_rows`, per page in k order: a new element starts where `start == "1"`, on the first line, or where the family
  differs from the previous line's family; ids `e1…` in that order. Type mapping: `custom:letterhead` → `letterhead`,
  `custom:copymark` → `copymark`; family `table` and `re.search("column head", note, re.I)` → `table:header`; all other
  types unchanged. `item_start = ""`. `prose`: on a page where some line's note matches `\bprose\b` (case-insensitive),
  each `ITEM_FAMILIES` element gets "1" if any of its lines matches, else "0"; on other pages and for other families
  `""`. Columns `stratum archive inv scan_id line_id k placement unclear note` are copied. `stats` = {`pages`, `rows`,
  `elements`, `forced_splits` (elements opened by a family change without `start = 1`), `header_lines`,
  `letterhead_lines`, `copymark_lines`, `prose_pages`}.
- `to_annotations(rows, ann_dir)`: refuse (ValueError) if `Path(ann_dir).resolve()` is `ANNOTATIONS_DIR.resolve()` or
  inside it. Institute = `archive.split("_")[0]`. Per scan: `ScanAnnotation(scan_id, lines={line_id: ["generic:" + type,
  "placement:" + placement]})` saved at `scan_annotation_path(institute, archive, inv, scan_id, ann_dir)`. Per inventory:
  one `Element` per (page_id, element) with `element_type = "generic:" + family`, one `ElementSpan(scan_id, [line ids in
  k order])`, `element_id = f"{page_id}-{element}"`, `properties = {"prose": v}` when v != "", `item_line_ids` = lines
  with `item_start == "1"`, saved with `save_elements` at `elements_path(institute, archive, inv, ann_dir)`. `chmod g+w`.

**`read_scan_sizes(layer_root, inv)`** → `{scan_id: (page_w, page_h)}` as ints from the scans table (all statuses).

**`convert_dev.py`** (docstring: run from the AS root as `.venv/bin/python -I reports/data/phase-19-page-elements-v2/step-25a/convert_dev.py`):
check the phase-18 `SHA256SUMS`, convert `test_labels.tsv` → `dev/devA_v2.tsv` and `dev_labels.tsv` → `dev/devB_v2.tsv`,
run `validate_rows` on each (stop on any error and print them), write the element tables and `dev/SHA256SUMS`, print
the stats. Refuse to overwrite an existing `dev/` file (exit 2). It never writes annotations.

## Constraints

- Additive only: no existing signature or JSON output changes; `marginalia` stays a known legacy tag.
- `element_gold.py` imports only stdlib, `datasets.vocabulary` and `datasets.annotations`.
- Never write under `data/annotations/` (tests use `tmp_path`). Never `git commit`; never bump the version; `chmod g+w`
  new files and dirs. No `rm -rf`, `nohup`, `&` or `$TMPDIR`; zsh; BSD `sed -i ''`.
- If git reports dubious ownership, run the suite with `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory
  GIT_CONFIG_VALUE_0=/Users/Shared/claude_projects/data_scopes/archival-structures` (one provenance test needs git).

## Tests

- vocabulary: `table:header`, `letterhead`, `copymark` known; `generic:table` still known; if an existing test pins
  `GENERIC_TYPES['table'] == frozenset()`, update it and say so under *Deviations*.
- annotations: old-style `Element.to_dict()` has exactly two keys; a round trip with all three new fields; `from_dict` of
  an old dict.
- element_gold (≥ 12): `type_error` cases (prefix, marginalia, unknown subtype, custom ok, `table:header` ok);
  `validate_rows` catches each rule on small hand-built pages (gap in k, id order, mixed family, item_start on running
  text, prose not constant, first item line 0); `from_v1_rows` on a hand-built v1 page: forced split counted, column
  head → `table:header`, letterhead mapping, prose page vs non-prose page; `element_table`; `to_annotations` writes into
  `tmp_path` and refuses `ANNOTATIONS_DIR`; `write_gold`/`read_gold` round trip with a tab-free note containing `\`.
- page_elements: `read_scan_sizes` on the synthetic layer of the existing test file.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_datasets_element_gold.py tests/test_datasets_annotations.py tests/test_datasets_vocabulary.py tests/test_analysis_page_elements.py`, then the full suite.

## Acceptance criteria

1. `convert_dev.py` prints, measured at planning with the same rules (F19-P.3): dev-A 60 pages, 4,334 rows, 802
   elements, 156 forced splits, 12 header lines, 19 letterhead, 3 copymark, 16 prose pages; dev-B 40 pages, 3,167 rows,
   677 elements, 186 forced splits, 0 header, 19 letterhead, 3 copymark, 0 prose pages. `validate_rows` → 0 errors on
   both. If a number differs, stop and report it with the cause.
2. Old `Element` JSON byte-identical (test). Suite: 1,471 collected at planning (1,460 passed, 11 skipped after 24r)
   plus your new tests, all passing.
3. `make reports` clean.

## Error analysis

- Read the 20 longest elements of dev-A after conversion (by `n_lines`) and 20 random forced splits
  (`random.Random(20261117)`): is each a real element boundary? Categories: a type change inside one element
  (e.g. a `note` line inside a `table` run, the case P3 now gives its own element), an element the user would join,
  a conversion error.
- List per family the dev-A element counts and the share of elements with ≥ 2 lines.

Write it into the report, not into `PROGRESS.md`.

## Do not touch

- `data/annotations/`, `shared-tools/`, every text-layer module, `reports/data/phase-18-page-element-types/` (read only).

## When done

1. Create `reports/phase-19-page-elements-v2.md` from `plan/templates/REPORT.md` if it does not exist (the planning
   session created it with a *Planning* section: keep that). Add your `### Step 25a: Gold format v2` section and
   findings rows `F19-25a.n` (status `draft`). Run `make reports` and fix any warning.
2. Append the block below to `plan/phase-19-page-elements-v2/PROGRESS.md`.
3. Overwrite `plan/phase-19-page-elements-v2/STATUS.md`: 25a done, next 25b part A and 25c (they can run in parallel).

```markdown
### Step 25a: Gold format v2, dev conversion, scan sizes
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-19-page-elements-v2.md` § Step 25a](../../reports/phase-19-page-elements-v2.md#step-25a-gold-format-v2); findings F19-25a.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
