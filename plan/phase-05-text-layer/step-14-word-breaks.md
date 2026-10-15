# Step 14: `text_layer.io` and the per-inventory word-break detector (C14)

> **Phase:** 05, Text layer. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP built the shared VOC text layer (`voc_text_layer_v1`, 6,914 inventories) with
`src/startpage/voc_text_layer.py`. This phase moves that producer into a new AS core subpackage
`archival_structures/text_layer/`. This step creates the package, its I/O module, and the word-break
character (WBC) detector. The detector looks at each line-final punctuation or symbol character
and asks how often it is attached to a word end, how strongly it is enriched at line end, and
whether removing it and joining the two fragments gives a word attested mid-line in the same
inventory. It then decides per inventory whether the character is a word break, with a corpus
default for small inventories. Step 15 adds extract and join on top. **Port verbatim: the
acceptance is byte identity with v1's stored files.** Planning (2026-10-14) checked the NLP
code on the same inputs: 2/2 decision files and 8/8 stats parts were byte-identical.

## Inputs

- `<NLP>` = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`. Source to port,
  **read these line ranges** (checked 2026-10-14) in `<NLP>/src/startpage/voc_text_layer.py`
  (879 lines; anchors in brackets):
  - L26–35 imports; L54–57 regexes (`SANITISE_PATTERN`, `LETTERS`, `TRAILING_LETTERS`,
    `LEADING_LETTERS`); L60–99 the rule comment, `WBC_RULE`, `SANITY_IN`, `SANITY_OUT`;
  - L134–137 `inventory_sort_key`, L140–232 `sanitise` … `discover_inventories`, L235–266
    `_is_pagexml_member`, `input_bytes`, `iter_pagexml`, L366–378 `read_region_lines`;
  - L385–509 `is_candidate_char` … `inventory_wbc_stats`; L512–647 `WBC_STATS_COLUMNS`, `_fmt`,
    `wbc_stats_part`, `pooled_stats`, `rule_passes`, `decide_wbc`, `load_wbc_sets`.
  - Tests to port: `<NLP>/tests/test_voc_text_layer.py`, the 5 tests at L184
    `test_sanitisation_counts_each_replacement`, L191 `test_written_fields_reject_tabs_and_newlines`,
    L201 `test_tsv_gz_is_deterministic_and_round_trips`, L215 `test_region_box_format`, L221
    `test_inventory_numbers_with_letter_suffix_stay_str`.
- v1 layer (read-only): `<V1>` = `<NLP>/data/voc_text_layer_v1/`: `lines/<inv>.lines.tsv.gz`,
  `regions_meta/<inv>.tsv.gz`, `wbc/wbc_stats.tsv` (109,984 rows, 6,913 inventories),
  `wbc/wbc_per_inventory.tsv` (6,914 rows), `wbc/corpus_default.json`, `wbc/parts/<inv>.tsv`.
- AS: `archival_structures/model/inventory_number.py`: `InventoryNumber(s)` (raises
  `InvalidInventoryNumberError`), `.key` → tuple sort key, `str()` → the original string.
- The pagexml dependency (AS `.venv`: pagexml-tools 0.8.1):
  `from pagexml.analysis.text_stats import LineCharAnalyser`; `LineCharAnalyser()`, then
  `.analyse_line_chars(lines)` (may raise `ZeroDivisionError` after filling `.freq`), then
  `.freq["all"|"start"|"end"]` (Counters). Import it **inside** the function, as NLP does.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/__init__.py` | Docstring only (purpose; module list `io`, `word_breaks`, and `extract`, `join`, `provenance`, `presets`, `build`, `compare` from steps 15/15c). No imports |
| `archival_structures/text_layer/io.py` | Ported I/O (below) |
| `archival_structures/text_layer/word_breaks.py` | Ported detector (below) |
| `tests/test_text_layer_io.py` | 5 ported tests + new sort/discovery tests |
| `tests/test_text_layer_word_breaks.py` | New synthetic tests (below) |
| `tests/test_packaging_core_imports.py` | Add `archival_structures.text_layer.io` and `.word_breaks` to `names` in `test_core_modules_import_without_extras`; add a test that importing both leaves `"pagexml" not in sys.modules` (use `run_blocked`) |
| `docs/shared_library/changelog.md` | `## Unreleased` above `## v0.8.0` if missing, then `### Step 14: text-layer I/O and word-break detector (C14)`, added with Edit |
| `reports/phase-05-text-layer.md` | `### Step 14` section, findings `F05-14.n` |

## Behaviour to implement

**`io.py`**: port verbatim, same names and behaviour: `SANITISE_PATTERN`, `sanitise`,
`format_box`, `_assert_clean`, `write_tsv_gz`, `read_tsv_gz`, `write_text_atomic`,
`write_json_atomic`, `write_plain_tsv`, `discover_inventories`, `_is_pagexml_member`,
`input_bytes`, `iter_pagexml`, and `read_region_lines(layer_root, inv)` (NLP's
`read_region_lines(out_root, inv)`, renamed parameter only). One change:

```python
def inventory_sort_key(inv: str):
    """Natural order of inventory numbers via InventoryNumber ('9626' < '9626A' < '9627').
    Names that are not inventory numbers sort last, by string."""
    try:
        return (0, InventoryNumber(inv).key)
    except InvalidInventoryNumberError:
        return (1, inv)
```

`discover_inventories` sorts with this key. It must give NLP's order on every real name. NLP's
key is `(int(m.group(1)), m.group(2))` for `re.match(r"(\d+)(.*)", inv)`, else `(10**9, inv)`.

**`word_breaks.py`**: port verbatim, with these AS names:

| NLP | AS |
|---|---|
| `LETTERS`, `TRAILING_LETTERS`, `LEADING_LETTERS` | same |
| `WBC_RULE` (with its comment block) | `DEFAULT_WORD_BREAK_RULE` (identical dict) |
| `SANITY_IN`, `SANITY_OUT` | same |
| `is_candidate_char`, `char_label`, `mid_line_vocabulary`, `ends_attached`, `strip_leading_marks`, `joined_word`, `first_non_wbc` | same |
| `inventory_wbc_stats(inv, out_root)` | `word_break_stats(regions, inventory_number)`: `regions` is an iterable of per-region line-text sequences (materialise it with `[list(t) for t in regions]`; it is walked twice). Body = NLP L440–509 with `for _, texts in regions` → `for texts in regions`. Returns `(rows, info)` as NLP does |
| — | `inventory_word_break_stats(inv, layer_root)` = `word_break_stats([texts for _, texts in read_region_lines(layer_root, inv)], inv)` |
| `WBC_STATS_COLUMNS` | `WORD_BREAK_STATS_COLUMNS` |
| `_fmt` | `format_stat` |
| `wbc_stats_part(inv, out_root)` | `write_word_break_stats_part(inv, layer_root)`, writes `<layer_root>/wbc/parts/<inv>.tsv` |
| `pooled_stats`, `rule_passes` | same (`rule=DEFAULT_WORD_BREAK_RULE`) |
| `decide_wbc(stats, inventory_lines, rule)` | `decide_word_breaks(stats, inventory_lines, rule=DEFAULT_WORD_BREAK_RULE, *, sanity_in=SANITY_IN, sanity_out=SANITY_OUT)`. The corpus dict stores the sanity lists given |
| `load_wbc_sets(out_root)` | `read_word_break_sets(layer_root)` |

Module docstring: the method in 5–8 lines (as in *Context*), that the thresholds were calibrated
on 22 VOC inventories (NLP report/74) and are parameters, and that statistics are measured at
line ends inside regions. Region ends are not measured: that is a v2 extension, and this step
doesn't add it.

## Constraints

- Port, don't improve. Keep every quirk: `.6g` floats, `sorted` orders, `strip` sets, the
  `ZeroDivisionError` catch, `differs_from_default` as Python `True`/`False` strings.
- No pagexml or pandas import at module level (pandas is imported inside `pooled_stats`, as in
  NLP; `decide_word_breaks` calls it).
- **Protected:** `<V1>` is read-only. Before the first run, record size and mtime of `<V1>/wbc/*`
  and `<V1>/manifest.json`; check them after every run. All outputs go to the scratchpad.
- Never `git commit`; never bump the version; `chmod g+w` new files and directories. Don't edit
  NLP or LotA.

## Tests

`tests/test_text_layer_io.py`:
- the 5 ported tests, with imports switched to `archival_structures.text_layer.io`;
- `inventory_sort_key`: `["9627", "10", "9626A", "9626", "x"]` sorts to
  `["10", "9626", "9626A", "9627", "x"]`;
- `read_region_lines` on a tmp layer (2 regions, one with 0 lines) returns both, in skeleton
  order, and raises `AssertionError` when `n_lines` disagrees.

`tests/test_text_layer_word_breaks.py` (synthetic, no real data):
- **`¬` detected:** one inventory of ≥ 2,000 lines whose regions have line pairs
  `"…woord¬"` + `"vervolg …"`, where the joined word also occurs mid-line ≥ 2 times. The
  per-inventory status of `¬` is `derived_in`.
- **`.` and `:` rejected** in the same data: end-attached ≥ 30, but the joined words are not in
  the vocabulary, so `derived_out`.
- **Small inventory** (< 2,000 lines) → `mode == "whole_default"`, `wbc_set` = the default.
- **Thin evidence:** a char with `n_end_attached < 30` takes its default membership
  (`status == "default"`).
- `corpus["sanity_pass"]` is True for the synthetic `¬` corpus and False when `¬` never occurs.
- `word_break_stats([])` and an inventory of empty lines give `([], info)` without raising.
- `read_word_break_sets` round-trips files written as the build writes them (columns
  `inventory_number, n_lines, mode, wbc_set, differs_from_default, char_status` via
  `write_plain_tsv`, and `corpus_default.json` via `write_json_atomic`).

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_io.py tests/test_text_layer_word_breaks.py tests/test_packaging_core_imports.py`,
then the full suite (890 before this step). Counts come from `pytest --collect-only -q`.

## Acceptance criteria

Write a scratch script, `<scratchpad>/step14/check.py`, run with `.venv/bin/python -I`. It
prints each number below.

1. **Decision reproduces v1.** Read `<V1>/wbc/wbc_stats.tsv` with
   `pd.read_csv(sep="\t", quoting=3, dtype={"inventory_number": str, "char": str}, keep_default_na=False, na_values=[])`.
   Build the inventory order the build script uses: every `<inv>` with a file in
   `<V1>/lines/`, sorted by `inventory_sort_key` (6,914). Set `inventory_lines[inv]` = the
   first `n_lines` of that inventory in the stats, or 0 if it has no row (one inventory). Call
   `decide_word_breaks`, then set `corpus["allow_partial"] = False` and
   `corpus["n_inventories_missing_extract"] = 0`. Write `corpus_default.json` with
   `write_json_atomic` and `wbc_per_inventory.tsv` with `write_plain_tsv` (columns as above),
   then `filecmp.cmp(shallow=False)` against `<V1>/wbc/`. **2/2 identical**. Also print the
   default set (`['¬', '„']`), the number with `differs_from_default` (**1,461**) and with
   `mode == "whole_default"` (**172**).
2. **Stats rewrite:** `write_plain_tsv` of the frame (formatted with `format_stat`) equals
   `<V1>/wbc/wbc_stats.tsv` byte for byte.
3. **Stats parts reproduce from v1 lines:** for 52 inventories, compute
   `inventory_word_break_stats(inv, <V1>)` (reads only), and write the part to the scratchpad
   with `write_plain_tsv(WORD_BREAK_STATS_COLUMNS, [[format_stat(r[c]) …]])`. Compare it with
   `<V1>/wbc/parts/<inv>.tsv`: **52/52 identical** (about 0.4 s each). The inventories are the 22
   annotated ones `1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923
   8023 8121 8237 8276 8284 8697 8834`, 28 seeded ones `1084 1851 2045 2118 2156 2181 2243
   2404 2503 2608 2957 3252 3275 3710 3848 4345 8147 8449 8503 8715 8866 9120 9130 9294 9478
   9724 10582 10926`, and `9626A`, `11013B`.
4. **Sort order:** `sorted(names, key=inventory_sort_key)` equals NLP's key order on all 6,914
   names (`<V1>/lines/` stems).
5. AS full suite green; protected sizes and mtimes unchanged.

If any number doesn't reproduce, stop and ask. Don't adjust the port to match.

## Error analysis

Use `<V1>/wbc/wbc_per_inventory.tsv` and `<V1>/wbc/wbc_stats.tsv` (pandas, explicit `usecols`):
1. Tabulate `wbc_set` over the 6,914 inventories (top 10 with counts). For the 1,461 that
   deviate, give the categories with counts: adds a char (which), drops `¬`, drops `„`, other.
2. **Near-threshold cases:** for (inventory, char) rows with `n_end_attached ≥ 30` and
   `n_lines ≥ 2000`, count those within ±10% of the `min_join_vocab_frac` (0.25) or
   `min_end_rel_frac` (5.0) threshold. That shows how many decisions a small threshold change
   would flip. Report counts only; **don't tune**.
3. Read 10 deviating inventories (seeded, `random.Random(14)`; all of them if fewer), with each
   char's `n_end_attached`, `join_vocab_frac`, `end_rel_frac` and status. Classify each as
   plausible (e.g. `=` really used as a break) or suspect (e.g. driven by ledger notation), and
   quote one line pair per inventory from `<V1>/lines/<inv>.lines.tsv.gz` (`zcat | grep -m`,
   never a full read).
Expected findings: the reproduction (result), the deviation profile (data-property), and the
threshold sensitivity (method).

## Do not touch

- NLP and LotA files; `<V1>`. AS modules outside `text_layer/` except the packaging test lines.

## When done

1. Add `### Step 14: text-layer I/O and word-break detector` to `reports/phase-05-text-layer.md`
   (findings `F05-14.n`, status `draft`); run `make reports` and fix any warning.
2. Append the block below to `plan/phase-05-text-layer/PROGRESS.md`.
3. Overwrite `STATUS.md`: 14 done, next 15, plus the AS suite count under *Facts later steps need*.

```markdown
### Step 14: text-layer I/O and word-break detector
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `archival_structures.text_layer.io.*`, `.word_breaks.*` (new)
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-05-text-layer.md` § Step 14](../../reports/phase-05-text-layer.md#step-14-text-layer-io-and-word-break-detector);
  findings F05-14.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
