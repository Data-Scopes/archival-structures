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

### UA-013 What happens to the published `text_layer_v2` after the review fixes (2026-10-21, from the phase-07 review)
- **Action:** **Decide.** The review found four defects in the published pilot
  (`plan/phase-07-sg-units-and-pilot/REVIEW.md`, rows 1–4):
  - print line order scrambled in 335 units of 455 (rows 153, 185 and 275 of the 116 are affected);
  - a stray continuation comma inside 1,8k hand words, mostly in 11182, 3352 and 11281 (none of the
    116 rows);
  - 77 print legacy ids lost needlessly;
  - wrong page-number method tags.

  Step 15ir fixes them into the dev layer `data/text_layer_v2_dev/15ir-a/`. It does not touch
  `shared-data`.
- **Options:**
  - (a) Keep `text_layer_v2` as published. 15ir adds a *Known issues* note to LotA §6 and to
    `docs/text_layer.md`, and the fixes go into the full SG run (`text_layer_v3`, D17).
  - (b) After 15ir, publish `15ir-a` as a new pilot version, `text_layer_v2_1` next to `v2`. This
    means a new catalogue row and a LotA note, and LotA re-aligns on it.
  - (c) As (b), but it replaces `v2` before LotA has used it. That needs the shared-data README's
    in-place rule waived.

  Recommended: (a), because the defects touch 3 of the 116 rows and the ≥ 80% target is out of
  reach for other reasons (glue, HTR/OCR).
- **Default if unanswered:** (a).
- **Blocks:** nothing in 15ir. Under (b) or (c), a publication step after 15ir.
- **Response:**
