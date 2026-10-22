# Open user actions (AS)

Open items only. Answer under **Response**. Resolved items move to
`docs/USER_ACTIONS_resolved.md`.

### UA-007 `PageXMLPage.stats` still raises before pagexml-tools 0.9.1 (2026-10-20, from phase-07 planning)
- **Action:** **Edit** `/Users/marijnkoolen/Code/Huygens/pagexml-tools/pagexml/model/pagexml_document_model.py`,
  `PageXMLPage.stats` (L1176–1196 at commit `16a51d0`). Replace
  ```python
              "text_regions": len(self.num_text_regions),
              "table_regions": len(self.num_table_regions)
  ```
  with
  ```python
              "text_regions": self.num_text_regions,
              "table_regions": self.num_table_regions
  ```
  and, in the `if self.extra:` branch, replace `sum(tr for tr in self.extra if isinstance(tr,
  PageXMLTextRegion))` with `sum(1 for tr in self.extra if isinstance(tr, PageXMLTextRegion))`
  (the same for `PageXMLTableRegion`). Then release 0.9.1 and fill in **Response**.
  Checked at `16a51d0` (2026-10-20): `stats` raises `TypeError: object of type 'int' has no
  len()` on a page with columns, with `text_regions` or with `extra`, because `num_text_regions`
  is already an int (L392–393). AS's print splitter calls `page.stats['lines']`
  (`text_layer/line_split.py:1059`), so the print extract would crash under this copy. The other
  six fixes of UA-005 are in place (F07-P.9).
- **Options:** (a) fix and release 0.9.1; (b) fix it later.
  Recommended: (a).
- **Default if unanswered:** AS stays on the 0.9.0 wheel with its workarounds; step 15i skips
  the pin.
- **Blocks:** only the pin in step 15i (step 0). Steps 15fq–15hj run on 0.9.0 either way.
- **Response:** a, fixed locally on `bug_fixes` branch but waiting for release 0.9.1.

### UA-015 Stage the inputs for the full `text_layer_v3` run (2026-10-22, from step 15m)
- **Action:** **Run**, then **Decide**. The full run (commands: `docs/text_layer.md`, *Text layer v3 (phase 09)*) reads
  three inputs; today they are only partly staged:
  1. Hand PageXML: `shared-data/NL-HaNA_1.01.02/pagexml/` holds 6 tars (3349-3354, layout `<inv>/page/*.xml`). The
     other 8 pilot inventories (3771-3773, 3821-3823, 11182, 11281) are only in AS `data/PageXML/NL-HaNA/NL-HaNA_1.01.02/`
     as flat directories (`15i-src` holds symlinks to them; don't copy those). Pack each as a tar with the same
     layout. **Run**, from the AS root:
     `for i in 3771 3772 3773 3821 3822 3823 11182 11281; do tar -czf /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_${i}-pagexml.tar.gz -C data/PageXML/NL-HaNA/NL-HaNA_1.01.02 -s ",^NL-HaNA_1.01.02_${i},${i}/page," NL-HaNA_1.01.02_${i}; done`
     (checked by the phase-09 review on 11182: a build from the tar gives the same tables as `15m-a` apart from the
     archive member path in `scans/`). Then update the shared-data README row for the dataset.
  2. Print PageXML: `shared-data/NL-HaNA_1.10.94/pagexml/` holds `NL-HaNA_1.10.94_455.zip` and `_456.zip`. Stage every
     further print inventory you want in `text_layer_v3`.
  3. REPUBLIC paragraphs `paragraphs-<inv>.tsv.gz` for every print inventory, currently only in LotA
     `data/paragraphs/NL-HaNA_1.10.94/` (455, 456). Suggested: a `shared-data` dataset
     `NL-HaNA_1.10.94/republic_paragraphs/` (per the shared-data README protocol), so the build does not read LotA's
     data directory; then pass it as `--republic-dir`.
- **Options:** (a) all SG inventories (needs the full 1.01.02 and 1.10.94 PageXML and REPUBLIC paragraphs for each print
  inventory); (b) a named subset that you list here (e.g. the years LotA aligns next); (c) only the 16 pilot inventories
  (item 1 alone; the result equals dev layer `15m-a`, so it is a publication step rather than a new build); (d) later.
  Recommended: (b), with the list in **Response**.
- **Default if unanswered:** no full run; `text_layer_v3` is not built or published (the pilot `text_layer_v2` in
  shared-data stays the only published layer; dev layer `15m-a` is available locally).
- **Blocks:** only the `text_layer_v3` build and its publication (user decision P1).
- **Response:**
