# Open user actions (AS)

Open items only. Answer under **Response**. Resolved items move to
`docs/USER_ACTIONS_resolved.md`.

### UA-031 Start phase 11 while the P21 step waits for the full run's files? (2026-10-26, from the UA-030 follow-up session)
- **Action:** **Decide.**
- **Context:** phase 12's last step (P21 pins, as step 15y in phase 12) needs the full v4 run's `mode/` and
  `status.tsv` staged locally. Phase 11 (optional: NLP `text_extent` options, UA-014 (a); C10, C12, C18 docs) touches
  modules disjoint from phase 12 and can start now. No other phase is planned.
- **Options:** (a) plan phase 11 now (Opus planning session), with 15y run once the files are staged; (b) do 15y first
  and plan nothing else until it is reviewed; (c) a different next phase (name it, e.g. the phase-09 leftovers in
  `plan/CONTEXT.md` §4). Recommended: (a), because phase 11 is not blocked and NLP has waited for `text_extent` since
  2026-10-21.
- **Default if unanswered:** (b): no new phase; 15y is planned once the full run's files are staged.
- **Blocks:** planning of any phase after 12.
- **Response:**

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

