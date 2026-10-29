# Step 20a notes

- Status: done after UA-049 (a): regression build 4,591 equal, 0 differ; audit 0 judged, 4,196 pass; 7 extract tests updated.
- pagexml-tools 0.9.1 installed in `.venv`; pin and `MIN_PAGEXML_TOOLS` (0, 9, 1). `poetry.lock` untouched (still 0.9.0).
- Workaround 1 (`set_as_parent(extra)` loop): dropped, `test_page_parentage` passes.
- Workaround 2 (`page_coords` fallback): dropped, `test_empty_page_builds` and the 44 tests of the file pass.
- Part 2.4: with 0.9.1 and unchanged code, 7 `tests/test_text_layer_extract.py` tests fail: test_status, test_regions_meta,
  test_lines, test_extract_stats, test_regions_text, test_join_stats, test_nested_regions_fail_in_pagexml. Cause: nested
  regions of fixture 9999A_0003 now parse (n1, n1a) instead of raising TypeError, so the scan yields rows (regions 6 not 4,
  7 line rows not 6, n_scans 4 not 3, no parse_error). This changes extract output for any real scan with nested regions.
  Tests not edited (brief). Needs a decision, UA-049.
- known_limits_check.py: nested regions parse on 0.9.1; region without Coords and lines still dropped.
- protect diff: shared-data/README.md (my edit) and two LotA `output/test-staged-fixture-2.5b` fixtures (mtime only, same size;
  rewritten by the LotA test suite run in part 6).
- Siblings: both venvs 0.9.1; NLP 66 passed; LotA 380 passed, 1 skipped, 1 xfailed (brief expected 381 passed).
