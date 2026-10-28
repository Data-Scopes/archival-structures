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

### UA-045 Run the full v5 text layer build of 1.01.02 on the external drive (2026-10-28, from step 19e)
- **Action:** Decide, then Run (in the AS root, on the machine that has the drive; `<1.01.02-dir>` = the drive directory
  with all 9,122 `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz`, `<drive>` = the output mount point; both `<drive>/layer_v5` and
  `<drive>/layer_v5_inputs` must exist; no `--republic-dir`; rerun the last command to resume):
  ```
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --check-only --src 1.01.02=<1.01.02-dir> --work-dir <drive>/layer_v5_inputs --out-dir <drive>/layer_v5
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --limit 1 --src 1.01.02=<1.01.02-dir> --work-dir <drive>/layer_v5_inputs --out-dir <drive>/layer_v5_smoke
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --src 1.01.02=<1.01.02-dir> --work-dir <drive>/layer_v5_inputs --out-dir <drive>/layer_v5
  ```
- **Options:** (a) run it; (b) not now. Recommended: (a) if you want v5 for all 9,124 volumes.
- **Evidence:** dev build `19e-a` (231 volumes, 4 workers): 60 min wall, 0 failed rows, audit 0 judged failures. Stage seconds
  v5/v4 on the same 231 volumes: layout 0.94, types 0.96, units 0.99, join 0.90, wbc 0.88 (extract is the same code; its
  1.51 here is machine load), so the time is the v4 full run's: 11.5 h wall (10.5 h of work plus a 1 h crash gap),
  predicted 3.5 to 7 h for the stages by `--check-only` (it prints the range); the USB drive may be slower. Output about 39 GiB.
- **Default if unanswered:** nothing runs; v4 stays the only full layer.
- **Blocks:** a full v5 layer (index entries in the hand volumes outside the 125 local ones).
- **Reviewer note (2026-10-28, final phase-13 review):** still recommend (a); the run does not need to wait. (1) The run
  script's final audit was the v4 audit and did not check the v5 invariants; the review fixed that (it now runs
  `step-19e/audit.py`, same checks plus the v5 ones, no extra time), so read `n_fail_judged` in `<drive>/layer_v5/audit.json`
  as usual. (2) Create `<drive>/layer_v5_smoke` too before the second command: the script refuses an `--out-dir` that does
  not exist. (3) Expect detector false positives outside the index and register volumes: in the dev build about 1 side in
  1,000 there (rosters, accounts, letters), re-cut at entry cues, no text lost (F13-R.13); a guard is a later rule, not part
  of this run. The commands were checked to parse (`--check-only` on the local sources: 231 inventories).
- **Response:**

### UA-046 Publish the v5 pilot in `shared-data/` (2026-10-28, from step 19e)
- **Action:** Decide. `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/` (231 volumes: the 125 hand volumes
  with index sides and entry units, 106 print volumes equal to v4) would become `shared-data/NL-HaNA_1.01.02/text_layer_v5/`.
- **Options:** (a) publish it (a later session copies it, edits the shared-data README and the changelog line, as with
  UA-038); (b) wait for the full run (UA-045); (c) not at all. Recommended: (a), so LotA can read index entries now.
- **Default if unanswered:** nothing is published.
- **Blocks:** LotA's use of v5.
- **Reviewer note (2026-10-28, final phase-13 review):** still recommend (a). Every 19e number reproduces and the print
  volumes equal v4. Known false positives in the pilot: 54 flagged sides that are not index pages in 18 of the 125 hand
  volumes (most in 8706 accounts, 4831 rosters, 3199/3315 accounts), 0.18% of the hand units, re-cut but no text lost;
  the README line should name them with the other known limits (`docs/text_layer.md`, *Text layer v5*).
- **Response:**
