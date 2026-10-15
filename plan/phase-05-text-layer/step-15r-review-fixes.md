# Step 15r: phase 05 review fixes

> **Model:** Sonnet. **Written:** 2026-10-15 by the phase-05 review (Opus).
> **Read:** this brief and `plan/phase-05-text-layer/STATUS.md`. Nothing else is needed.
> **Budget:** one short session. No AS public signature changes apart from the constant in §3.

## Why

The phase review (`plan/phase-05-text-layer/REVIEW.md`) found the text layer correct.
Before the release, though, it found three small defects and one documentation gap:

1. **pandas 3 breaks two AS tests outside the text layer** (F05-R.2). The user's `.venv` has
   pandas 3.0.6 since 2026-10-15, and `pyproject.toml` allows `pandas (>=2.3.3,<4.0.0)`. In
   pandas 3 a `pd.Series` of strings that holds `None` stores it as NaN (the `str` dtype), so
   `segment_into_elements` treats NaN as a label. `read_csv(dtype=str)` now gives `StringDtype`,
   not `object`.
2. **The run-time pagexml-tools check is still 0.8.0** while the pin is `>=0.9.0` (F05-R.6).
   The NLP and LotA environments now report 0.9.0 in their metadata, so raising it is safe.
3. **Docs:** the v1 layer moved to `shared-data/` (D5, done by the user 2026-10-15). A `wbc`
   run on a subset of inventories into an existing layer overwrites that layer's run-level
   word-break decision (F05-R.5). The docs don't warn about this.

## Preconditions (check first; stop and ask the user if one fails)

> **Update 2026-10-15 (later):** the user has restored the `.venv`, so the first precondition
> holds (pytest works, `[vision]` imports). The full suite now gives 796 passed, 2 failed (the
> two pandas-3 tests this step fixes) and 7 uncollectable modules. The 7 need matplotlib,
> ipywidgets or torch, which are not installed; they are not part of this step. Run
> `pytest --continue-on-collection-errors`.

- `.venv/bin/python -m pytest --version` works. On 2026-10-15 it failed with
  `ModuleNotFoundError: No module named 'py'`: pytest 9.1.1's `py.py` shim is missing from
  `.venv/lib/python3.11/site-packages`. **Do not reinstall anything yourself.** Ask the user to run
  `! .venv/bin/python -m pip install --force-reinstall --no-deps pytest==9.1.1` and
  `! .venv/bin/python -m pip install -e '.[vision]'`. The second restores hdbscan, opencv,
  Pillow and scikit-image; 13 test modules can't be collected without them.
- `chmod g+w` everything you create. Check write access to `archival_structures/`, `tests/`,
  `docs/` and `plan/phase-05-text-layer/` before editing.
- Never `git commit`. Never access external drives.

## Changes

### 1. `archival_structures/analysis/sequence_patterns.py` (`segment_into_elements`, ~L150–197)

The module already has `import pandas as pd` (L22). In the loop, replace

```python
        label = event.cluster
        if label is None or label == -1:
```

with

```python
        label = event.cluster
        if _is_unlabelled(label):
```

and add this module-level helper just above `def segment_into_elements`:

```python
def _is_unlabelled(label) -> bool:
    """No cluster label: None, a pandas missing value (NaN/pd.NA; pandas 3 stores a None in a
    str Series as NaN) or HDBSCAN noise -1."""
    if label is None or label is pd.NA:
        return True
    if isinstance(label, float) and label != label:  # NaN (np.float64 is a float subclass)
        return True
    return label == -1
```

In the docstring, change "Lines with no label (`None` or HDBSCAN noise `-1`)" to
"Lines with no label (`None`, NaN/`pd.NA`, or HDBSCAN noise `-1`)". Leave the test
`tests/test_analysis_sequence_patterns.py::TestSegmentIntoElements::test_unlabelled_lines_break_a_run`
unchanged; it must now pass. Add one test to the same class that builds `line_clusters` as
`pd.Series([...], dtype=object)` holding `float("nan")` for the middle line, and asserts 2
elements (same set-up as the existing test).

### 2. `tests/test_datasets_globalise_index.py::test_load_all` (L29)

Replace `assert df.inventory_number.dtype == object and df.inventory_number.iloc[0] == "1000"`
with

```python
    assert pd.api.types.is_string_dtype(df.inventory_number)  # object (pandas 2) or str (pandas 3)
    assert df.inventory_number.iloc[0] == "1000"
```

Add `import pandas as pd` if the file doesn't import it. Don't change `load_globalise_index`.

### 3. `archival_structures/text_layer/provenance.py` L6 and its uses

- `MIN_PAGEXML_TOOLS = (0, 9, 0)`.
- `archival_structures/text_layer/build.py` L19: "asserts pagexml-tools >= 0.8.0" becomes
  "asserts pagexml-tools >= 0.9.0".
- `tests/test_text_layer_provenance.py` L11: rename `test_pagexml_tools_version_is_at_least_080`
  to `test_check_pagexml_tools_version_uses_minimum`. Its body asserts
  `prov.MIN_PAGEXML_TOOLS == (0, 9, 0)` and `prov.check_pagexml_tools_version()`.

### 4. Docs (`docs/text_layer.md`, `archival_structures/text_layer/build.py` docstring)

- `docs/text_layer.md`, section *Checking a rebuild against v1*: replace the two
  `<NLP>/data/voc_text_layer_v1` paths with `<DS>/shared-data/voc_text_layer_v1`. Define `<DS>`
  in the sentence that lists the placeholders ("`<DS>` the `data_scopes` directory that holds
  `shared-data/`"), and drop `<NLP>` from that sentence if nothing else uses it. NLP's
  `data/voc_text_layer_v1` is now a symlink to the same place.
- `docs/text_layer.md`, end of *Building a layer*: add this paragraph.

  > **Never build into an existing shared layer with a subset of its inventories.** The `wbc`
  > stage (also part of `--stage all`) re-derives `wbc/corpus_default.json` and
  > `wbc/wbc_per_inventory.tsv` from the inventories of *this run* only, and overwrites them.
  > `join` then rewrites every region file whose word-break set changed. Build a sample into a
  > fresh `--out-dir`, and copy the layer's `wbc/` decision files in (as in *Checking a rebuild
  > against v1*) if the sample must use them.

- `build.py` module docstring, the paragraph that ends "the local {¬, „} from the 22 is
  provisional." Append: "It overwrites wbc/corpus_default.json and wbc/wbc_per_inventory.tsv,
  so never point --out-dir at an existing layer with a subset of its inventories."

### 5. Changelog (`docs/shared_library/changelog.md`, with Edit only)

- In the *Unreleased* → *Step 13* section, replace the bullet that begins
  "`text_layer.provenance.MIN_PAGEXML_TOOLS` (the driver's run-time check) is still `(0, 8, 0)`."
  with: "`text_layer.provenance.MIN_PAGEXML_TOOLS` is `(0, 9, 0)` (step 15r), matching the pin."
- Add a section `### Step 15r: phase 05 review fixes` after the *Step 15c* section, under
  *Unreleased*. It records: `segment_into_elements` now also treats NaN/`pd.NA` labels as
  unlabelled (pandas 3; behaviour under pandas 2 unchanged); `MIN_PAGEXML_TOOLS` is 0.9.0, so
  `as-build-text-layer` and NLP's `build_voc_text_layer.py` refuse to run when the
  pagexml-tools metadata is older than 0.9.0. Affected sibling imports: NLP
  `startpage.voc_text_layer.MIN_PAGEXML_TOOLS` and `check_pagexml_tools_version` (re-exported
  by the shim).

## Verify

Run from the AS root. `<SP>` is your session scratchpad; use a new empty directory for each
build.

1. `.venv/bin/python -m pytest -q -p no:cacheprovider tests`: 0 failed, 0 errors. Expect at
   least 976 passed (step 13's count, plus your new test); skips are allowed only for torch.
2. `.venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/pagexml --out-dir <SP>/r22 --stage all --inventories 1120,1267,1274,1388,1529,1539,1547,1557,2448,2548,2555,2775,3142,3891,7923,8023,8121,8237,8276,8284,8697,8834 --write-events --workers 8`
   exits 0. Then write the same 22 numbers, comma-separated, to `<SP>/inv22.txt`, and run
   `.venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py <SP>`. It must
   print `TOTAL 157 / 157`. (`check.py` expects `<SP>/r22/` and `<SP>/inv22.txt`.)
3. In NLP (`cd /Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`):
   `PYTHONNOUSERSITE=1 venv/bin/python scripts/verify_as_text_layer.py --out-dir <SP>/nlpv`
   ends `ALL PASS` with exit 0. Then `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider tests/test_voc_text_layer.py`
   gives 29 passed, and `... tests/test_evaluation.py tests/test_semi_markov.py` gives 21 passed.
   Don't edit any NLP file.
4. Error analysis (short). For change 1, run the new and the old test under the installed
   pandas, and report which labels each line gets. For change 3, print `importlib.metadata.version("pagexml-tools")` under
   all three interpreters (AS `.venv`, NLP `venv`, LotA `venv`, siblings with
   `PYTHONNOUSERSITE=1`). All three must be >= 0.9.0; if one isn't, stop and ask.

## Write-up

- The report `reports/phase-05-text-layer.md` is final. Append a subsection
  `### Addendum: step 15r (review fixes)` at the end of *Steps*, after Step 15b. It gives what
  changed, the verify results (numbers), and the short error analysis. Don't change finding
  statuses. A new observation goes into the addendum's text as a note for the user, not as a
  new finding row. Run `make reports` and check that it gives no warnings.
- Append a `PROGRESS.md` block and overwrite `STATUS.md` (state "15r done; ready for release").
- **Brief defects:** list anything here that you had to find out yourself.
