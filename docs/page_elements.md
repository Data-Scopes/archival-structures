# Page element types

Phase 18 (step 24a) library pieces for measuring page element types (running text, lists, headings, marginal notes, ...).

- {mod}`archival_structures.analysis.listlike` -- running text against list- and table-like lines, per line. A verbatim
  port of shared-tools `listlike.py` (defaults unchanged) plus the phase-18 candidates `LISTLIKE_CFG_C1` (a line-final
  number counts only after an amount word, a leader or a locator), `RULE_R1`/`RULE_R2` (add `closed_share <= 0.4`;
  `min` 2 or 3), the grid `LISTLIKE_CELLS` (`C0R0` ... `C1R2`) and `group_shares`. Stdlib only.
- {mod}`archival_structures.analysis.page_elements` -- `read_layer_lines` reads one text-layer inventory into one
  record per non-empty line (key: `(scan_id, line_id)`, since line ids repeat across scans in AsdSAA) with the layer's
  line type and the PageXML region tag; `layer_call` maps them to vocabulary tags
  (`LINE_TYPE_TO_GENERIC`, `REGION_TAG_TO_GENERIC`); `listlike_records` builds the `listlike` input; `type_family`.
- Seven archive presets (`na_1.05.06_v5sg`, `na_2.10.50_v5sg`, `na_2.10.01_v5sg`, `asdsaa_5023_v5sg`,
  `asdsaa_5075_v5sg`, `asdsaa_89_v5sg`, `asnda_0114.11_v5sg`) in {mod}`archival_structures.text_layer.presets`: the SG v5
  rules unchanged, archive plumbing only.
- The vocabulary gains the `placement:` namespace and new generic types: see {doc}`vocabulary`.

```{eval-rst}
.. automodule:: archival_structures.analysis.listlike
   :members: AMOUNT_END_LEADERS, LISTLIKE_CFG_C1, RULE_R1, RULE_R2, LISTLIKE_CELLS, group_shares
.. automodule:: archival_structures.analysis.page_elements
   :members:
```
