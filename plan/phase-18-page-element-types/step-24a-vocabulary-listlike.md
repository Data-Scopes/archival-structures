# Step 24a: Vocabulary v2, `listlike` port, `page_elements` reader, archive presets

> **Phase:** 18, Page element types v1. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you have to explore the code
> base anyway, do what you must and record it under *Brief defects*.

## Context

Phase 18 measures page element types (running text, lists, headings, marginal notes, …) across 10 strata in 9 archives.
This step builds the library pieces the later steps use:
1. the tag vocabulary gains a **placement** axis and new types (user decisions, 2026-11-02);
2. `shared-tools/listlike.py` (running text against lists and tables, per line) becomes an AS module, verbatim by
   default, with the phase's variants as named constants;
3. a reader that turns a text-layer inventory into one record per line with the layer's own calls, mapped to the
   vocabulary;
4. seven presets so the text-layer CLI can build the new archives with the SG v5 rules unchanged.
Nothing here changes an existing default.

## Inputs (checked 2026-11-02)

- `archival_structures/datasets/vocabulary.py` (148 lines; read it whole). Anchors: `NAMESPACES = (CARRIER, GENERIC,
  POSITION, DOCTYPE)` (l.50), `GENERIC_TYPES` (l.59–80), `POSITION_TYPES` (l.82–86), `_NAMESPACE_TYPES` (l.88–92),
  `def is_known_tag` (l.135). Subtypes are a `frozenset` per type (`frozenset()` = none suggested, `None` = any).
- `docs/vocabulary.md` (94 lines; read it whole).
- `/Users/Shared/claude_projects/data_scopes/shared-tools/listlike.py` (186 lines; read it whole). Public:
  `window_features(lines, window=3, cfg=None)`, `list_score(features, rule=None)`, `list_flag(features, rule=None)`,
  `DEFAULT_CFG`, `DEFAULT_RULE`, `selftest()`. Its self-test prints four mean scores (prose 0.14, numbered list 1.14,
  table of amounts 2.14, name list 2.57) and `listlike selftest ok`.
- `archival_structures/text_layer/presets.py` (304 lines). Anchors: `VOC_SOURCE_PATTERN` and `VOC_1_04_02_V5SG`
  (l.249–256), `PRESETS = {` (l.266). `LayerPreset` fields used here: `name`, `archive_id`, `scan_id_prefix`,
  `source_pattern` (regex with named group `inv`, fullmatched on entry names in `--pagexml-dir`).
- `archival_structures/text_layer/io.py`: `read_tsv_gz(path)` (rows as dicts; use it, not pandas, in library code).
- Text-layer tables (schema `docs/text_layer.md`), per inventory `<inv>`:
  - `lines/<inv>.lines.tsv.gz`: `scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw text
    edit_flags split_from`;
  - `layout/lines/<inv>.tsv.gz`: `scan_id line_id region_id page layout_region_id layout_line_idx layout_column
    layout_role` (`layout_role` ∈ main, extra, empty);
  - `types/segments/<inv>.tsv.gz`: `scan_id line_id seg_idx char_start char_end x y w h geom_method page
    layout_region_id line_type type_method column_override`; `line_type` ∈ main_text, page_number, running_header,
    date_header, catch_word, signature_mark, marginalia, index, other;
  - `regions_meta/<inv>.tsv.gz`: `scan_id page_side region_id region_xml_id parent_region_xml_id region_tag
    region_ro_index x y w h n_lines n_lines_text`.
  Measured at planning (SG 12212 and 10115 on the drive v5 layer, VOC 2361 v6): (scan_id, line_id) is unique in
  `lines/`; every line has a `layout/lines` row; lines with empty `text` have no `types/segments` row (383, 88, 512) and
  every non-empty line has one; 0–10 lines per volume have more than one segment. **Line ids repeat across scans in
  AsdSAA** (`tr_1_tl_1`): always key on (scan_id, line_id).
- Fixture layer for tests: none on disk is small enough; build a tiny one in `tmp_path` (below).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/datasets/vocabulary.py` | `PLACEMENT`, `PLACEMENT_TYPES`, new generic types and subtypes, `legacy_placement` |
| `archival_structures/analysis/listlike.py` | new: verbatim port + `LISTLIKE_CFG_C1`, `RULE_R1`, `RULE_R2`, `LISTLIKE_CELLS`, `group_shares` |
| `archival_structures/analysis/page_elements.py` | new: `LINE_TYPE_TO_GENERIC`, `REGION_TAG_TO_GENERIC`, `read_layer_lines`, `layer_call`, `listlike_records`, `type_family` |
| `archival_structures/text_layer/presets.py` | 7 archive presets and their `PRESETS` keys |
| `tests/test_datasets_vocabulary.py` | new tests (below) |
| `tests/test_analysis_listlike.py`, `tests/test_analysis_page_elements.py`, `tests/test_text_layer_presets_archives.py` | new |
| `docs/vocabulary.md`, new `docs/page_elements.md`, `docs/index.md` | placement axis and new types; module doc; toctree entry `page_elements` after `vocabulary` |
| `docs/shared_library/changelog.md` | add `## Unreleased` above `## v0.22.0 (2026-11-02)`, then `### Step 24a: …` (Edit only) |
| `reports/phase-18-page-element-types.md` | `### Step 24a: …` section, findings `F18-24a.n` |

## Behaviour to implement

**1. Vocabulary** (additive; every tag known today stays known):
- `PLACEMENT = 'placement'`; `NAMESPACES = (CARRIER, GENERIC, POSITION, PLACEMENT, DOCTYPE)`;
  `PLACEMENT_TYPES = {'main', 'margin', 'top', 'bottom', 'interlinear'}` each `frozenset()`; add to `_NAMESPACE_TYPES`.
- `GENERIC_TYPES` changes: `heading` → `{'title', 'date', 'summary'}`; `closing` → `{'formula', 'dating', 'signature'}`;
  `running_title` → `{'date'}`; `numbering` → `{'page', 'folio', 'item', 'other'}`; new keys `salutation`,
  `endorsement`, `reference`, `note`, `addition`, `noise` (each `frozenset()`). `marginalia` stays as it is.
- `LEGACY_PLACEMENT_TYPES = {'marginalia': 'margin'}` and `def legacy_placement(tag: str) -> Optional[str]`: for a
  `generic:` tag whose type is in that dict, the placement it implies (`'margin'`), else `None` (also for malformed tags).
- Update the module docstring and `docs/vocabulary.md`: a fifth namespace section `placement:` (where an element sits,
  independent of what it is; a marginal note is `placement:margin` plus its type), the new types with one-line
  definitions (below), and that `generic:marginalia` is kept for old annotations and means "placement margin, type not
  given". Keep the worked example; add one line with a marginal summary: `generic:heading:summary, placement:margin`.

  Definitions (use these words in the docs, and 24c shows them to the user):
  - `heading`: opens a unit once (title, a date line opening a sitting or entry, a summary of what follows);
  - `running_title`: repeats on page after page (a volume title, `:date` the current unit's date at the page top);
  - `salutation`: address and salutation of a letter or petition;
  - `closing` `:formula` / `:dating` / `:signature`: the end of a document (closing formula, place and date, signatures);
  - `endorsement`: dockets and receipt notes, usually on the back of a letter;
  - `numbering` `:page` / `:folio` / `:item` / `:other`: page numbers, folio numbers (one per leaf), item or paragraph
    numbers;
  - `reference`: a locator or cross-reference ("fol. 12", "vide", "zie pag. 4");
  - `note`: a remark on the main text (including archivists' notes); `addition`: text inserted into the main text;
  - `noise`: bleed-through, text from the facing page, HTR garbage.

**2. `analysis/listlike.py`.** Copy the shared-tools file **verbatim** below a new module docstring that says: ported
from shared-tools `listlike.py` (republic-segmentation 11c3, 2026-11-01) in AS phase 18 step 24a; the shared-tools copy
stays frozen; defaults are unchanged. Keep `selftest()` (it prints four lines and asserts); keep a `__main__` block that runs it
only when `--selftest` is given, as in the original. Then add, after `DEFAULT_RULE`:
```python
# Phase-18 candidates (step 24d grid); none is a default.
_AMOUNT_WORDS = (r"\bgl\.?|\bguldens?|\bst\.?|\bstuyvers?|\bstuivers?|ƒ|£|\bponden?|\bschell\.?|\bblancken|\bcrs?\.?"
                 r"|\blivres?|\bsols?|\bdeniers?|\bécus?|\becus?")
AMOUNT_END_LEADERS = (r"(?:" + _AMOUNT_WORDS + r")[\s.,:;]*$"
                      r"|(?:\.{2,}|-{2,}|„|" + _AMOUNT_WORDS + r"|\bfol?\.?|\bpag\.?|\bp\.)\s*[\d„\-—=]+[\s.,:;„\-—]*$")
LISTLIKE_CFG_C1 = {"amount_end": AMOUNT_END_LEADERS}   # a line-final number counts only after an amount word, a leader or a locator
RULE_R1 = {"conditions": DEFAULT_RULE["conditions"] + [("closed_share", "<=", 0.4)], "min": 2}
RULE_R2 = {"conditions": DEFAULT_RULE["conditions"] + [("closed_share", "<=", 0.4)], "min": 3}
LISTLIKE_CELLS = {"C0R0": (None, DEFAULT_RULE), "C0R1": (None, RULE_R1), "C0R2": (None, RULE_R2),
                  "C1R0": (LISTLIKE_CFG_C1, DEFAULT_RULE), "C1R1": (LISTLIKE_CFG_C1, RULE_R1),
                  "C1R2": (LISTLIKE_CFG_C1, RULE_R2)}

def group_shares(flags, keys):
    """{key: share of True} for parallel lists `flags` (bools) and `keys` (hashables), e.g. per side or per region."""
```
Keep the module stdlib-only (no pandas, no AS imports).

**3. `analysis/page_elements.py`** (stdlib + `text_layer.io.read_tsv_gz` only):
```python
LINE_TYPE_TO_GENERIC = {"main_text": "generic:running_text", "page_number": "generic:numbering:page",
    "running_header": "generic:running_title", "date_header": "generic:running_title:date",
    "catch_word": "generic:quire_marks:catchwords", "signature_mark": "generic:quire_marks:signature",
    "marginalia": None, "index": "generic:list", "other": "generic:custom"}
# PageXML region tags (Laypa, VOC) -> generic type, for a reported baseline only
REGION_TAG_TO_GENERIC = {"paragraph": "generic:running_text", "header": "generic:heading",
    "catch-word": "generic:quire_marks:catchwords", "page-number": "generic:numbering:page",
    "signature-mark": "generic:quire_marks:signature", "marginalia": None, "untagged": None}
REGION_TAG_PLACEMENT = {"marginalia": "margin"}
DATE_COMPATIBLE = frozenset({"heading", "running_title"})  # a date_header call is right for either family when the gold subtype is date

def read_layer_lines(layer_root, inv, scan_ids=None) -> list:
    """One dict per non-empty line of `lines/<inv>.lines.tsv.gz`, in that file's order, optionally only for `scan_ids`
    (a set). Keys: scan_id, line_id, region_id, page_side, x, y, w, h (ints), text, layout_region_id, layout_column,
    layout_role, page, line_type, type_method, n_segments, region_tag. line_type/type_method come from the segment with
    the most characters (char_end - char_start; tie: lowest seg_idx). Joins on (scan_id, line_id) and, for region_tag,
    (scan_id, region_id). Raises ValueError on a duplicate (scan_id, line_id), on a line without a layout row, or on a
    non-empty line without a types row."""

def layer_call(line) -> dict:
    """{"type": LINE_TYPE_TO_GENERIC[line_type] (None for marginalia), "placement": "margin" if line_type ==
    "marginalia" else "main", "region_type": REGION_TAG_TO_GENERIC.get(region_tag), "region_placement":
    REGION_TAG_PLACEMENT.get(region_tag, "main" if region_tag not in (None, "", "untagged") else None)}"""

def listlike_records(lines) -> list:
    """listlike input: {"text", "x0": x, "x1": x + w, "group": (scan_id, layout_region_id)} per line, same order."""

def type_family(tag) -> str:
    """'generic:heading:date' -> 'heading'; None -> None."""
```

**4. Presets** (after `VOC_1_04_02_V6`; comment: phase 18 step 24a, archive plumbing only, the SG v5 rules as in
`VOC_1_04_02_V5SG`). A helper `def _archive_v5sg(archive_id, source_pattern, scan_id_prefix)` returning
`dataclasses.replace(VOC_1_04_02_V5SG, name=f"{archive_id}_text_layer_v5sg", archive_id=archive_id,
scan_id_prefix=scan_id_prefix, source_pattern=source_pattern, description=f"{archive_id}: the SG v5 rules unchanged
(phase 18 step 24a)")`. Register:

| key | archive_id | source_pattern | scan_id_prefix |
|---|---|---|---|
| `na_1.05.06_v5sg` | `NL-HaNA_1.05.06` | `NL-HaNA_1\.05\.06_(?P<inv>[0-9]+)` | `NL-HaNA_1.05.06` |
| `na_2.10.50_v5sg` | `NL-HaNA_2.10.50` | `NL-HaNA_2\.10\.50_(?P<inv>[0-9]+)` | `NL-HaNA_2.10.50` |
| `na_2.10.01_v5sg` | `NL-HaNA_2.10.01` | `VOC_SOURCE_PATTERN` | `NL-HaNA_2.10.01` |
| `asdsaa_5023_v5sg` | `NL-AsdSAA_5023` | `NL-AsdSAA_5023_(?P<inv>[0-9][0-9.]*)` | `None` |
| `asdsaa_5075_v5sg` | `NL-AsdSAA_5075` | `NL-AsdSAA_5075_(?P<inv>[0-9][0-9.]*)` | `None` |
| `asdsaa_89_v5sg` | `NL-AsdSAA_89` | `NL-AsdSAA_89_(?P<inv>[0-9][0-9.]*)` | `None` |
| `asnda_0114.11_v5sg` | `NL-AsnDA_0114.11` | `NL-AsnDA_0114\.11_(?P<inv>[0-9][0-9.]*)` | `None` |

The planner built one inventory of each archive with exactly these settings (presets registered at run time; 0
failures; 8–25 s each; F18-P.1), including AsdSAA 89 inventory `3.1`. Every inventory got mode `hand` with method
`default` (the box-height pre-pass reported `n_chars 0`); record that, don't fix it.

## Constraints

- Additive only. No existing preset, default, function signature or table changes. `shared-tools/` is not edited.
- Never `git commit`; never bump the version; `chmod g+w` new files.
- No data is written in this step except under `tmp_path` in tests.

## Tests

- `tests/test_datasets_vocabulary.py` (+ ≥ 6): every placement type known; `placement:margin` round-trips through
  `Tag.parse`; each new generic type and each new subtype known (`generic:heading:summary`, `generic:numbering:folio`,
  `generic:closing:signature`, `generic:noise`, …); every tag the old tables knew is still known (iterate a frozen copy
  of the old tables written into the test); `legacy_placement('generic:marginalia:note') == 'margin'`,
  `legacy_placement('generic:heading') is None`, `legacy_placement('bad') is None`.
- `tests/test_analysis_listlike.py` (≥ 6): the self-test's four documents give mean scores 0.14 / 1.14 / 2.14 / 2.57
  (rounded to 2); `selftest()` runs; **equivalence**: load the shared-tools file with `importlib.util` from its absolute
  path (skip the test if absent) and assert `window_features` equal (NaN-aware) on the four self-test documents plus a
  fixture of ≥ 12 lines in two groups with French amounts (`livres`), Dutch amounts (`gl.`), roman numbering, `Item`,
  dot leaders and a line ending in a year; same `list_score`/`list_flag` under all six `LISTLIKE_CELLS` rules with
  `cfg=None`; C1 (`re.search` with `re.I` on the stripped text, as `window_features` does) does not count
  "den 3 Maart 1700" or "Anno 1650" as an amount end but counts "Voor hout .... 120", "Voor hout gl. 120", "fol. 12",
  "120 livres" and "Item betaelt 25 gl." (the planner checked all seven);
  `group_shares`.
- `tests/test_analysis_page_elements.py` (≥ 6): write a two-scan fixture layer into `tmp_path` (the four tables, gzip
  TSV with the columns above; repeated line ids across the two scans; one empty line; one two-segment line where the
  second segment is longer; a region tagged `marginalia`); `read_layer_lines` returns the non-empty lines in order with
  the longer segment's type; ValueError cases; `layer_call` for `marginalia`, `date_header`, `index`; `listlike_records`
  groups; `type_family`.
- `tests/test_text_layer_presets_archives.py` (≥ 4): the seven keys exist; each differs from `VOC_1_04_02_V5SG` only in
  `name`, `archive_id`, `scan_id_prefix`, `source_pattern`, `description` (compare `dataclasses.asdict` minus those);
  each pattern fullmatches its staged entry name (`NL-HaNA_1.05.06_44`, `NL-HaNA_2.10.50_85`, `1090.zip`,
  `NL-AsdSAA_5023_4`, `NL-AsdSAA_5075_2409`, `NL-AsdSAA_89_3.1` → inv `3.1`, `NL-AsnDA_0114.11_1`) and not
  `._NL-AsdSAA_5023_4`.
- Bulk-tagging tests (`tests/test_datasets_bulk_tagging.py`) read the vocabulary tables. If one pins the exact list of
  namespaces or generic types, update that expectation to the new tables and list it under *Deviations*.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_datasets_vocabulary.py
  tests/test_datasets_bulk_tagging.py tests/test_analysis_listlike.py tests/test_analysis_page_elements.py
  tests/test_text_layer_presets_archives.py`, then the full suite. At planning the suite collected 1,443 tests.

## Acceptance criteria

1. All listed tests pass; full suite green (1,443 + new tests, nothing else changes).
2. `listlike` equivalence holds on every fixture (features compared key by key; NaN equals NaN).
3. `is_known_tag` is True for every tag of the old tables (test 1) and for `placement:interlinear`.
4. `.venv/bin/python -I -c "from archival_structures.text_layer.presets import PRESETS; print(sorted(k for k in PRESETS
   if k.endswith('_v5sg')))"` lists the seven keys and `voc_1.04.02_v5sg`.

## Error analysis

Small for a library step: list in the report every vocabulary test that changed expectation, and run `listlike` C0R0
and C1R0 on the fixture to show (verbatim lines) which lines C1 stops flagging. Findings: the vocabulary diff (types
and subtypes added), and whether anything outside the new modules needed a change.

## Do not touch

- Any text-layer stage module, `analysis/index_*.py`, `analysis/folio_map.py`; `data/`; `shared-tools/`; sibling repos.

## When done

1. Add `### Step 24a: Vocabulary v2, listlike port, page_elements reader, archive presets` to
   `reports/phase-18-page-element-types.md` (it exists, from planning), following `plan/templates/REPORT.md`; findings
   rows `F18-24a.n`, status `draft`. Edit only your section and your rows. Run `make reports`; fix any warning.
2. Append the PROGRESS block (template in `plan/templates/STEP.md` § *When done*) to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: 24a done, next 24b, open questions.
