# Step 11: scan-id helpers (`model/scan_id.py`); LotA adapters delegate

> **Phase:** 04, Decoders and page numbering. **Depends on:** nothing. May run in parallel
> with 10/10b; they share only the changelog and the report (append with Edit).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

The Nationaal Archief scan-id grammar `NL-HaNA_<archive>_<inventory>_<NNNN>[.jpg]` is parsed
in at least four places across LotA and NLP. This step adds one parser to AS next to
`InventoryNumber`, and switches LotA's adapters to it. **User decision (2026-10-14):** only
the LotA adapters switch now. NLP's `globalise_alignment.scan_no` switches in step 12b. NLP's
`voc_text_layer`, `separator_sheets` and `layout_features` helpers move with their own phases
(05, 07). Don't touch them.

## Inputs

AS: `archival_structures/model/inventory_number.py` (91 lines). `model/` has no
`__init__.py` (implicit namespace package); keep it that way.

```python
class InvalidInventoryNumberError(ValueError): ...
class InventoryNumber:            # @total_ordering, __slots__ = ('original', '_key')
    def __init__(self, inventory_number: str)   # raises InvalidInventoryNumberError
    key: tuple                                    # ((0, 4848), (1, 'a')) for "4848A"
    # __str__/__repr__ return the original string; __eq__/__hash__/__lt__ use key.
    # "42A" == "42.A"; letters compare case-insensitively; "/", "-", " " are rejected.
```

LotA root `/Users/Shared/claude_projects/data_scopes/language-of-the-archive` (`<LotA>`).
**Read `<LotA>/CLAUDE.md` first.** Interpreter `<LotA>/venv/bin/python` (3.11). AS is
installed there editable (`--no-deps`).

`<LotA>/archival_language/ead/adapters.py` (236 lines). `_BaseAdapter` L44–115 sets
`self._ead_id = archive_config.ead_id` (L53, e.g. `"NL-HaNA_1.01.02"`, `"NL-HaNA_1.04.02"`).
L55–70 verbatim:

```python
    def scan_id(self, inv: InventoryNumber, scan_number: int) -> str:
        return f"{self._ead_id}_{inv}_{scan_number:04d}"

    def parse_scan_id(self, scan_id: str) -> tuple[InventoryNumber, int] | None:
        prefix = f"{self._ead_id}_"
        if not scan_id.startswith(prefix):
            return None
        rest = scan_id[len(prefix) :]
        inv_part, _, scan_part = rest.rpartition("_")
        if not inv_part or not scan_part.isdigit() or len(scan_part) != 4:
            return None
        try:
            inv = InventoryNumber(inv_part)
        except InvalidInventoryNumberError:
            return None
        return inv, int(scan_part)
```

`DescriptionOnlyAdapter` (L172–228) raises for every method, including these two; leave it
alone. `<LotA>/tests/test_adapters.py` has 11 tests. They all load the real
`config/archives.toml`, and they pass today (1.6 s). The full LotA suite on 2026-10-14:
`--ignore=tests/test_derive_voc.py` gives **316 passed**. `test_derive_voc.py` fails at
collection with `No module named 'fuzzy_search.similarity'` (an environment issue, F03-R.3).
That file monkeypatches `parse_scan_id`; it can't run here, so say so in the report.

Data for the checks (read-only):
- AS `ground_truth/NL-HaNA_1.04.02/globalise_document_segmentation/source/`: 22
  `<inv> - Document Segmentation.csv` files (`;`-separated, UTF-8 with BOM, read with
  `encoding="utf-8-sig", dtype=str`). The column `Scan File_Name` holds ids without an
  extension, e.g. `NL-HaNA_1.04.02_1120_0001`. 21,623 rows; 20,905 distinct names.
- NLP `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1/regions_meta/<inv>.tsv.gz`:
  6,914 files (1.8 GB in total). The first column `scan_id` has an extension, e.g.
  `NL-HaNA_1.04.02_10119A_0002.jpg`. **54** inventories have a letter suffix
  (`A, B, C, D, E, I, II`). Read only the `scan_id` column (`usecols=["scan_id"]`,
  `sep="\t"`, `quoting=csv.QUOTE_NONE`), one file at a time. Never follow paths recorded in
  that layer's `manifest.json`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/model/scan_id.py` | new (API below) |
| `tests/test_model_scan_id.py` | new |
| `<LotA>/archival_language/ead/adapters.py` | `_BaseAdapter.scan_id` / `parse_scan_id` delegate to AS (same signatures, same results) |
| `docs/shared_library/changelog.md` | `### Step 11: scan-id helpers (C13)` under `## Unreleased` (create the heading above `## v0.7.0` if it's missing; another step may have created it). Use Edit. List the LotA import |
| `reports/phase-04-decoders-and-page-numbering.md` | `### Step 11` section, findings `F04-11.n` |

## Behaviour to implement (`model/scan_id.py`)

```python
NA_INSTITUTION = "NL-HaNA"
DEFAULT_DIGITS = 4

class InvalidScanIdError(ValueError): ...

@dataclass(frozen=True, order=True)
class ScanId:
    institution: str              # "NL-HaNA"
    archive: str                  # "1.04.02"
    inventory: InventoryNumber
    number: int
    ext: str = ""                 # "" or ".jpg" (leading dot kept, case kept)

    @property
    def prefix(self) -> str       # f"{institution}_{archive}", i.e. LotA's ead_id
    @classmethod
    def from_prefix(cls, prefix: str, inventory: InventoryNumber | str, number: int,
                    ext: str = "") -> "ScanId"   # splits prefix at its FIRST "_"
    def format(self, *, digits: int = DEFAULT_DIGITS, ext: str | None = None) -> str
        # f"{prefix}_{inventory}_{number:0{digits}d}{self.ext if ext is None else ext}"
    def __str__(self) -> str      # self.format()
    def with_number(self, number: int) -> "ScanId"   # dataclasses.replace
    @classmethod
    def parse(cls, scan_id: str, *, digits: int | None = DEFAULT_DIGITS,
              prefix: str | None = None) -> "ScanId"   # raises InvalidScanIdError
    @classmethod
    def try_parse(cls, scan_id: str, **kwargs) -> "ScanId | None"

def scan_number(scan_id: str, *, digits: int | None = None) -> int   # ScanId.parse(...).number
def parse_doc_id(doc_id: str, *, marker: str = "-text_region-") -> tuple[str, tuple[int, int, int, int]] | None
```

`parse` algorithm (follow it literally; every failure raises `InvalidScanIdError` with the
input in the message):
1. `scan_id` must be a non-empty `str`.
2. `head, sep, last = scan_id.rpartition("_")`; `sep` must be non-empty.
3. `num, dot, rest = last.partition(".")`; `ext = "." + rest if dot else ""`. A non-empty
   `ext` must match `^\.[A-Za-z][A-Za-z0-9]*$`.
4. `num` must match `[0-9]+` (ASCII only); if `digits` is not None, `len(num) == digits`.
5. `pre, sep2, inv = head.rpartition("_")`; `sep2` and `inv` non-empty;
   `InventoryNumber(inv)`; convert its error to `InvalidScanIdError`.
6. `institution, sep3, archive = pre.partition("_")`: both non-empty, and `archive` has no
   `"_"`.
7. If `prefix` is given, `pre == prefix`, otherwise the parse fails.

Consequences that the tests pin down: `str(ScanId.parse(s)) == s` for every canonical 4-digit
id, with or without `.jpg`. Number 10000 formats as `..._10000` (5 digits), which
`parse(digits=4)` rejects and `parse(digits=None)` accepts. LotA has the same asymmetry
today. `"5745A/B"` is not a valid inventory in a scan id. `order=True` sorts by
institution, archive, inventory (natural order), then number.

`parse_doc_id`: the pagexml-tools region doc id `{scan_id}-text_region-{x}-{y}-{w}-{h}`.
Behave exactly like LotA's: `i = doc_id.find(marker)`; `None` if `i <= 0`; the coords part
split on `"-"` must give exactly 4 parts that all `int()`, otherwise `None`. So negative
coordinates give `None`. Returns `(scan_id_string, (x, y, w, h))`. The scan id is not parsed.

**LotA adapters** (keep behaviour identical, including `None` instead of raising):

```python
    def scan_id(self, inv, scan_number):
        return ScanId.from_prefix(self._ead_id, inv, scan_number).format()

    def parse_scan_id(self, scan_id):
        parsed = ScanId.try_parse(scan_id, prefix=self._ead_id)
        if parsed is None or parsed.ext:      # LotA never accepted an extension here
            return None
        return parsed.inventory, parsed.number
```

Keep the `InventoryNumber` returned as LotA's code expects. (LotA imports `InventoryNumber`
from AS already; check the import at the top of `adapters.py`.) Remove the now-unused
`InvalidInventoryNumberError` import only if nothing else in the file uses it.

## Constraints

- Archive-specific values are parameters with a named default (`NA_INSTITUTION`,
  `DEFAULT_DIGITS`); no VOC or SG archive code in `scan_id.py`.
- Don't change `InventoryNumber`.
- Never `git commit`; `chmod g+w` new files. Read-only on NLP and on `ground_truth/`.
- Don't switch any other LotA call site (`cli/document_units.py`, `cli/page_split_*`,
  `corpus/tsv_source.parse_doc_id`, `corpus/derive_voc._normalise_scan_id`). List them as
  follow-ups in the report; planning counted 6 such sites.

## Tests (`tests/test_model_scan_id.py`, synthetic, no config)

- `test_roundtrip_sg_and_voc` (`NL-HaNA_1.01.02_3809_0415`, `NL-HaNA_1.04.02_9015A_0415`);
- `test_roundtrip_keeps_extension` (`.jpg`, `.JPG`) and `test_format_can_drop_extension`;
- `test_from_prefix_formats_staged_name` (`("NL-HaNA_1.04.02", "10000", 1)` gives
  `NL-HaNA_1.04.02_10000_0001`);
- `test_prefix_mismatch_raises_and_try_parse_returns_none`;
- `test_suffixed_inventories` (`4848A`, `5745A`, `9626A`, `10119C`, `1171B`, plus the roman
  suffix `II`, e.g. `1234II`); `test_dotted_inventory_equals_undotted` (`42.A` vs `42A`);
- `test_slash_inventory_rejected` (`NL-HaNA_1.04.02_5745A/B_0001`);
- `test_digit_count` (5 digits: raises at 4, parses at None; `00a1` raises; Unicode digits
  such as `"０００１"` raise);
- `test_malformed` (`""`, `"foo"`, `"NL-HaNA_0001"`, `"NL-HaNA_1.04.02__0001"`,
  `"_1.04.02_1_0001"`, `"NL-HaNA_1.04.02_2555_0001."`, non-str);
- `test_ordering_natural` (inventory 9 before 10, `9626` before `9626A`; number order);
- `test_scan_number` (`NL-HaNA_1.04.02_2555_0029.jpg` → 29);
- `test_with_number`;
- `test_parse_doc_id` (valid; marker at 0; 3 or 5 parts; non-int; negative coordinate → None).
Packaging: `test_core_modules_import_without_extras` iterates `model/` automatically.

Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_model_scan_id.py tests/test_model_inventory_number.py tests/test_packaging_core_imports.py`,
then the full suite. LotA: `<LotA>/venv/bin/python -m pytest -q -p no:cacheprovider tests/test_adapters.py`,
then `... --ignore=tests/test_derive_voc.py`.

## Acceptance criteria

1. New AS tests pass; full AS suite green (717 before phase 04, plus whatever step 10 added;
   see `STATUS.md`).
2. LotA `tests/test_adapters.py`: **11 passed**, file unchanged. LotA suite with
   `--ignore=tests/test_derive_voc.py`: **316 passed**.
3. **Ground truth (scratch script):** every `Scan File_Name` in the 22 GT CSVs parses with
   `prefix="NL-HaNA_1.04.02"`, and its inventory equals the file's inventory. Expect
   **21,623/21,623 rows (20,905 distinct names)**: planning counted 21,623 rows, 718 of them
   repeated names (a scan with several rows), and 0 names failing the regex
   `NL-HaNA_1\.04\.02_<inv>_\d{4}`. Report the exact counts, and stop and ask if any row fails.
4. **Text layer (scratch script):** for all 54 suffixed inventories plus 200 others chosen with
   `random.Random(11).sample(sorted(numeric_names), 200)`, every distinct `scan_id` parses with
   `prefix="NL-HaNA_1.04.02"` and its inventory equals the file name. Report parsed/total
   ids and failures per inventory. `scan_number(s)` equals NLP's
   `int(str(s).rsplit("_", 1)[1].split(".")[0])` for every parsed id.

The user decides when the step is done.

## Error analysis

- Read **every** failure from criteria 3–4, up to 30, then a seeded sample of 30. Classify
  them into: partial shot / `deelopname`-style suffix after the number, wrong inventory in the
  id, 5-digit number, unexpected extension, other. Quote each category's ids verbatim. Zero
  failures is a valid result: say so and give the counts.
- Read the 54 suffixed inventories' first and last scan ids, and say whether any suffixed
  inventory's scans carry the *unsuffixed* inventory number (a known risk for a joined
  layer).
- Expected findings: GT parse count (result), text-layer parse counts and any non-conforming
  ids (data-property), the LotA behaviour kept (`.jpg` → `None`) (method).

## Do not touch

- NLP code. `stream_analysis/` (step 10). `analysis/`, `datasets/` (step 12).

## When done

1. Add `### Step 11: scan-id helpers` to `reports/phase-04-decoders-and-page-numbering.md`
   (findings `F04-11.n`, status `draft`); run `make reports` and fix any warning it prints.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`. Under *Facts later steps need*, put the final
   `scan_number` / `ScanId.parse` signatures, because step 12 calls them. Add the LotA working-tree
   change to *Blockers and open questions for the user*.

```markdown
### Step 11: scan-id helpers
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added>
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 11](../../reports/phase-04-decoders-and-page-numbering.md#step-11-scan-id-helpers);
  findings F04-11.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
