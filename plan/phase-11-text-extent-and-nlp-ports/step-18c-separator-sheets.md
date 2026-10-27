# Step 18c: C12, separator-sheet rule

> **Phase:** 11, Text extent options and NLP ports. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Some VOC volumes interleave near-blank *separator sheets* whose only text is a document number ("No: 7.", "No. 63").
Ground truth treats them inconsistently across inventories (the sheet is the start, or the next page is), so NLP built a
label-free detector and a convention-aware rule that moves a start prediction within the unit {sheet, next page}. This
step ports it into AS as `analysis/separator_sheets.py` (inventory item C12), with the regex and the length cap as
parameters (VOC values as named defaults) and the next-page id computed with AS's scan-id helper (C13). It adds tests and
shows AS = NLP on NLP's page texts. NLP is not edited.

**Parallel run:** steps 18a and 18b run at the same time in the same working tree. Edit shared files only with Edit,
only at your own placeholder (below), and in `STATUS.md` only your own row of the *Steps* table. Don't overwrite
`STATUS.md`.

## Inputs

- NLP root `NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (read only; never write there,
  never `pip install` into its venv). Source `$NLP/src/startpage/separator_sheets.py` (64 lines, checked 2026-10-27).
  Read it whole: module docstring L1–18 (the rationale, incl. the 2555 / 1274 / 3891 / 2775 conventions), `_SHEET_RX`
  and `MAX_SHEET_CHARS = 12` (L25–26), `is_separator_sheet(text) -> bool`, `detect_sheets(page_texts: pd.DataFrame) ->
  set` (columns `inventory_number, page_id, text` → `{(str(inv), page_id)}`), `_scan_no(page_id)`, and
  `apply_rule(df, sheets, convention, threshold=0.5) -> np.ndarray` (df: `inventory_number, page_id, pred_prob`, any
  order; `convention`: `"none" | "sheet" | "next" | {inventory_number: convention}`; returns a bool array aligned with
  df rows). NLP's next page id is `pid.rsplit("_", 1)[0] + f"_{_scan_no(pid) + 1:04d}"`.
- AS C13 helper `archival_structures/model/scan_id.py` (signatures verbatim):
  ```python
  class InvalidScanIdError(ValueError)
  @dataclass(frozen=True, order=True)
  class ScanId:            # institution, archive, inventory: InventoryNumber, number: int, ext: str = ""
      def format(self, *, digits: int = DEFAULT_DIGITS, ext: Optional[str] = None) -> str
      def with_number(self, number: int) -> "ScanId"
      @classmethod
      def parse(cls, scan_id: str, *, digits: Optional[int] = DEFAULT_DIGITS, prefix: Optional[str] = None) -> "ScanId"
      @classmethod
      def try_parse(cls, scan_id: str, **kwargs) -> "Optional[ScanId]"
  ```
  Checked 2026-10-27: on all 20,912 NLP scannames, `ScanId.try_parse(s, digits=None)` succeeds and
  `x.format(digits=len(s.rsplit('_', 1)[1])) == s` for every one.
- Data for the equivalence check: `$NLP/output/early_end/page_text.csv.gz`, columns `inventory_number, scanname, text`,
  20,912 rows (whole-page text of the 22 VOC 1.04.02 inventories). Planning measured NLP's `is_separator_sheet` hits:
  **169** (2555: 140, 2775: 13, 1274: 8, 3142: 4, 3891: 3, others 1). (NLP's docstring says 158 hits: that count was
  on another text field, the first text of the right page of transition pairs; don't expect 158.)
- Command pattern (checked; imports the AS working tree and NLP's `startpage`):
  `cd $NLP && PYTHONNOUSERSITE=1 PYTHONPATH=/Users/marijnkoolen/Code/Huygens/archival-structures venv/bin/python <script>`.
- Existing AS module style: `archival_structures/analysis/text_extent.py` (module docstring, type hints, numpy/pandas).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/separator_sheets.py` | new module (below) |
| `tests/test_analysis_separator_sheets.py` | new tests |
| `reports/data/phase-11-text-extent-and-nlp-ports/step-18c/compare_nlp.py` | equivalence script; `compare.tsv`, `sheets.tsv`, `run.log` in the same dir |
| `docs/shared_library/changelog.md` | replace the line `<!-- 18c -->` under `## Unreleased` with `### Step 18c: …` |
| `reports/phase-11-text-extent-and-nlp-ports.md` | fill `### Step 18c: …`; findings `F11-18c.n` |

## Behaviour to implement

```python
VOC_SHEET_PATTERN: re.Pattern = re.compile(<NLP's _SHEET_RX pattern string, verbatim>, re.I)
VOC_MAX_SHEET_CHARS: int = 12
SHEET_CONVENTIONS = ("none", "sheet", "next")

def is_separator_sheet(text, *, pattern: re.Pattern = VOC_SHEET_PATTERN, max_chars: int = VOC_MAX_SHEET_CHARS) -> bool
def detect_sheets(page_texts: pd.DataFrame, *, pattern=VOC_SHEET_PATTERN, max_chars=VOC_MAX_SHEET_CHARS,
                  inventory_col: str = "inventory_number", page_col: str = "page_id", text_col: str = "text"
                  ) -> set[tuple[str, str]]
def next_scan_id(page_id: str) -> Optional[str]
def apply_rule(df: pd.DataFrame, sheets: set, convention, threshold: float = 0.5, *,
               next_page_id: Callable[[str], Optional[str]] = next_scan_id,
               inventory_col: str = "inventory_number", page_col: str = "page_id", prob_col: str = "pred_prob"
               ) -> np.ndarray
```

- `is_separator_sheet`: NLP's logic (non-str → False; strip; `0 < len <= max_chars` and `pattern.match`).
- `detect_sheets`: NLP's set comprehension with the column names as parameters; inventory as `str`.
- `next_scan_id`: `x = ScanId.try_parse(page_id, digits=None)`; `None` if it fails; else
  `x.with_number(x.number + 1).format(digits=len(<the digits of page_id's last '_' part, before any extension>))`.
  Keep any extension. For NLP's ids this equals NLP's next id (verified above); document that ids which do not parse
  get no next page (the unit is skipped), where NLP would raise or build a wrong id.
- `apply_rule`: NLP's logic verbatim with the column names and `next_page_id` as parameters; an unknown convention
  string raises `ValueError` (NLP treated anything but "sheet" as "next": **this is a deliberate change**, record it
  in the changelog; the equivalence check only uses valid conventions).
- Module docstring: NLP's rationale (L1–18), generalised ("VOC 1.04.02" named as where it was found), plus "Ported from
  NLP `src/startpage/separator_sheets.py` (AS phase 11, step 18c)". Say that labels are never read: the convention
  per inventory is an input, decided elsewhere (NLP infers it in its own evaluation scripts).
- No `__init__` export needed (`archival_structures.analysis` has no lazy registry for it); confirm by
  `.venv/bin/python -c "from archival_structures.analysis.separator_sheets import apply_rule"`.

## Constraints

- No VOC pattern inside function bodies; only as the named default.
- Never write under `$NLP`; never `git commit`; never bump the version; `chmod g+w` every new file and directory.
- Agent shell: no `rm -rf`, `nohup` or `&`; zsh. Single process.
- Don't touch `text_extent.py`, `subsequence_detection.py`, `docs/`, `text_layer/`, `analysis/hand_print.py`.

## Tests (`tests/test_analysis_separator_sheets.py`)

- `is_separator_sheet`: positives `"No: 7."`, `"No. 63"`, `"N=o 6."`, `"Nr 12"`, `"No 4.\n"`; negatives `""`,
  `None`, `"No. 63 den 4 Maart"` (too long), `"Nota 7"`, `"7"`; a custom pattern (e.g. `r"^Folio \d+$"`) and
  `max_chars`.
- `detect_sheets`: custom column names; inventory cast to `str`.
- `next_scan_id`: `NL-HaNA_1.04.02_2555_0664` → `…_0665`; `…_0999` → `…_1000`; 5-digit ids keep 5 digits; with
  `.jpg` extension kept; a suffixed inventory (`NL-HaNA_1.04.02_4848A_0012`); unparsable → `None`.
- `apply_rule`: for each convention on a 4-page toy inventory with a sheet at page 2: `"none"` = plain threshold;
  `"sheet"` puts the unit's start on the sheet and clears the next page; `"next"` the reverse; the unit is a start iff
  `max(prob) >= threshold`; a dict convention per inventory with a missing inventory → `"none"`; a sheet whose next page
  is absent is left alone; row order of `df` shuffled gives the same per-row result; unknown convention → `ValueError`.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_separator_sheets.py`, then the
  full suite `.venv/bin/python -m pytest -q -p no:cacheprovider` (baseline 2026-10-27: 1241 passed, 11 skipped).
  Counts from `pytest --collect-only -q`.

## Equivalence script `compare_nlp.py`

argparse `--out-dir` (required). Load `page_text.csv.gz`, rename `scanname` → `page_id`. Import NLP's module under an
alias and AS's. Checks:
1. `is_separator_sheet` agrees on all 20,912 texts; 169 hits; `detect_sheets` sets equal;
2. `apply_rule` arrays equal for conventions `"none"`, `"sheet"`, `"next"` and a dict (seeded random choice per
   inventory, `numpy.random.default_rng(18)`), at thresholds 0.3, 0.5, 0.7, on random `pred_prob`
   (`default_rng(18).random(20912)`), with the rows shuffled once (`df.sample(frac=1, random_state=18)`);
3. `next_scan_id(pid)` equals NLP's next id for all 20,912 page ids.
Write `compare.tsv` (check, n compared, mismatches), `sheets.tsv` (the 169 rows: inventory, page_id, text) and
`run.log`.

## Acceptance criteria

1. All three checks: 0 mismatches; 169 sheets.
2. New tests pass; full AS suite green.

If a number does not reproduce, stop and record it under *Open questions* in your `PROGRESS.md` block.

## Error analysis

- Read all 169 detected texts in `sheets.tsv` and classify: clear sheet number / number with a letter suffix / other
  (e.g. a page number or folio that matches the pattern). Count each; give verbatim examples.
- Find near misses: texts of ≤ 20 characters that contain `No`, `Nr` or `N°` (case-insensitive) and a digit but are
  not detected. Read all of them (or the first 40 by inventory, page order, if more) and say whether each is a missed
  sheet (pattern too strict) or correctly rejected. This gives a recall/precision reading of the VOC pattern on these
  22 inventories, not a corpus rate; say so.

## Do not touch

- Other steps' sections, placeholders and `STATUS.md` rows; NLP and LotA files.

## When done

1. Fill your `### Step 18c: …` section in `reports/phase-11-text-extent-and-nlp-ports.md` (template
   `plan/templates/REPORT.md`) and add rows `F11-18c.n` (status `draft`) to its *Findings* table with Edit. Run
   `make reports` from the AS root and fix any warning.
2. Replace the line `<!-- 18c block -->` in the phase `PROGRESS.md` with the block below.
3. Edit only your row (`| 18c |`) of the *Steps* table in the phase `STATUS.md`.

```markdown
### Step 18c: C12, separator-sheet rule
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18c](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18c-c12-separator-sheet-rule); findings F11-18c.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
