# Open user actions (AS)

Open items only. Answer under **Response**. Resolved items move to
`docs/USER_ACTIONS_resolved.md`.

### UA-054 Stage VOC (1.04.02) PageXML for phase 16 (2026-10-30, from phase-15 planning)
- **Action:** Decide when to stage, then Run on the machine with the drive. The list is
  `docs/staging/voc_phase16_inventories.txt` (63 inventories; groups in the `.tsv` beside it):
  - 22 inventories with GLOBALISE document segmentation;
  - 21 Generale Missiven inventories already in NLP's v1 text layer;
  - 20 random other Generale Missiven inventories (seed 20261030).

  The source is the drive directory that NLP's VOC text layer was built from (`pagexml_v2.1`, recorded in
  `shared-data/voc_text_layer_v1/manifest.json`, `stages.extract.argv`). It holds `<inv>.zip` or `<inv>/` per
  inventory, the two forms `archival_structures.text_layer.io.discover_inventories` reads. Set `SRC` to that directory.
  - First check the size. **The shared volume had 19 GiB free on 2026-10-30.** In the AS root, run:
    `SRC=<drive>/PageXML/Globalise/pagexml_v2.1; for i in $(cat docs/staging/voc_phase16_inventories.txt); do du -sk "$SRC/$i.zip" "$SRC/$i" 2>/dev/null; done | awk '{s+=$1} END {print s/1048576 " GiB"}'`
  - Then copy. In the AS root, run:
    `D=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.04.02/pagexml_v2.1/source; mkdir -p "$D" && for i in $(cat docs/staging/voc_phase16_inventories.txt); do if [ -f "$SRC/$i.zip" ]; then cp "$SRC/$i.zip" "$D/"; elif [ -d "$SRC/$i" ]; then cp -R "$SRC/$i" "$D/"; else echo "missing $i"; fi; done; chmod -R g+w "$D"`

  The phase-16 planning session links the directory under `data/` and adds the shared-data catalogue row. Report any
  `missing` lines in the Response.
- **Options:**
  - (a) stage all 63 now, while phase 15 runs;
  - (b) stage only the 22 GLOBALISE inventories now: `head -22` of the list;
  - (c) wait: the phase-16 planner picks its own set first.

  Recommended: (a), if the size check shows it fits with room to spare. Both ground-truth sets need it, and a random
  group keeps phase 16 from testing only on selected volumes.
- **Default if unanswered:** phase 16 is planned without VOC PageXML, and its first build step waits for this item.
- **Blocks:** phase 16's VOC text-layer build. Nothing in phase 15.
- **Response:** a

### UA-055 Confirm 20 index-link v2 gold targets on the scans (2026-10-30, from phase-15 step 21a)
- **Action:** Run, in the AS root: `open reports/data/phase-15-index-links-v2/step-21a/check_page.html`. Answer the
  three Yes / No / Unsure questions on each of the 20 items (about 30 min), press Export, and save the file as
  `reports/data/phase-15-index-links-v2/step-21a/check_verdicts.json`. The page shows the agent's answers (a
  confirmation, not a blind test). The 20 items are drawn at random (seed 20261031) from the 63 usable test targets.
- **Options:** (a) check (about 30 min); (b) skip. Recommended: (a).
- **Default if unanswered:** the phase-15 review states that the 21a gold is unchecked.
- **Blocks:** the publication decision for `index_links_v2` only. 21b–21d run on the agent gold.
- **Response:**

### UA-056 Run the index-links v2 build on the full v5 layer (2026-10-30, from phase-15 step 21d)
- **Action:** Run, in the AS root, on the machine with the drive. `<drive>` = the directory holding `text_layer_v5/`
  (UA-045). Create the output directory, smoke-test, run in full (rerun the third command to resume), then copy back:
  `mkdir -p <drive>/index_links_v2`
  `.venv/bin/python scripts/build_index_links.py --version v2 --layer <drive>/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5 --out-dir <drive>/index_links_v2 --limit 3`
  `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label index-links-v2-full --want 4 --min 1 --wait -- .venv/bin/python scripts/build_index_links.py --version v2 --layer <drive>/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5 --out-dir <drive>/index_links_v2 --workers '{workers}'`
  `mkdir -p data/index_links_full_v2 && cp <drive>/index_links_v2/NL-HaNA_1.01.02_index_links_v2/volumes.tsv <drive>/index_links_v2/NL-HaNA_1.01.02_index_links_v2/manifest.json data/index_links_full_v2/ && chmod -R g+w data/index_links_full_v2`
  Measured cost on the dev build: 27.9 s for 41 volumes single-process (0.68 s per front-index volume; v1 0.49 s); 10.2 s
  through coreshare with 3 workers wanted. The v2 builder also reads `join/segments/<inv>.tsv.gz` (the dev layer has it; the full layer comes from the same pipeline, and a volume without it is recorded as `failed`, not fatal).
- **Options:** (a) run; (b) skip. Recommended: (a) only if you want v2 on the drive. The review is done (UA-057
  recommends no publication), so the counts are no longer needed for that decision.
- **Default if unanswered:** v2 exists as the 41-volume dev build only.
- **Blocks:** options (b) of UA-057 only.
- **Response:**

### UA-057 Publish `index_links_v2`? (2026-10-30, from the phase-15 review)
- **Action:** Decide. v2 passes bars F (map, 62/63) and L precision (0.981). It fails L recall (0.856), attribution
  (0.892) and end to end (0.825), and on those three it is no better than v1. Its gains over v1 are 2,569 fewer
  locators, most of them false; no trust in year-like chain ends or duplicated numbers; and an opening column. Report:
  `reports/phase-15-index-links-v2.md` (*Summary*, *Review*).
- **Options:**
  - (a) do not publish. The dev build stays in `data/index_links_dev/21d-a/` (and on the drive after UA-056). A later
    index-links phase (entry assembly v3, new gold) publishes once it meets the bars. This is the rule you set in UA-053.
  - (b) publish the full-run v2 (after UA-056) in `shared-data/NL-HaNA_1.01.02/index_links_v2/` as
    `coverage: partial`, with the failed bars and known limits in the catalogue row.
  - (c) publish the 41-volume dev build that way.

  Recommended: (a). Three bars fail, and attribution is v1's.
- **Default if unanswered:** (a). Nothing is published.
- **Blocks:** publication only. Nothing in AS or the siblings.
- **Response:**
