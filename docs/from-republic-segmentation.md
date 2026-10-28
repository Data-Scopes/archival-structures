## 2026-10-28: `republic_segmentation_v1` published (from republic-segmentation, step 08c)
- **What/where:** `shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/` (443 inventories) and `shared-data/NL-HaNA_1.10.94/republic_segmentation_v1/` (455, 456). The C07d.1 segmentation (lines → sessions → resolutions → paragraphs; marginalia; `legacy_id_map`) with 08a date flags, as Parquet partitioned by `inv`. Read each `README.md` and `manifest.json`; verify with `shasum -a 256 -c SHA256SUMS`.
- **Link it:** shared-data protocol § 3: symlink the versioned directory under your `data/`, never write through it, add yourself to the catalogue row's "used by". LotA: `../shared-data` must be in `[paths].allowed_roots`.
- **Ids:** a unit that is the same (or only shifted) as in the 2026-03 export keeps its 2026-03 id; every other unit has a new `rs1-` id. `legacy_id_map` maps every 2026-03 session, resolution and paragraph id to its new ids (kind: same, shifted, split, merged, mixed); `legacy_overlap_ids` lists the old ids behind a new one.
- **Known limits (layer check of 20 random sessions: 7 right, 13 wrong, strict at session level):**
  1. Missed hand resolution starts (about 16 missed and 4 false of 182 shown; held-out hand recall is weak, 0.558).
  2. Split hand openings: about 750 spurious sessions (Preside/attendance block or date line as a session of its own, same date as the next).
  3. Typing conventions: the Preside line is `session_opening`; resumption lines are mostly `other_session_part`; print attendance lists can run past the resumption (1,256 of 12,788 sessions). Also: 1577-1584 under-segmented; about 1.1% of dates wrong (`date_flag = contradicted` means check, not wrong).
- **v2 conventions to come (retypes only):** Preside line → `attendance_list`; new type `resumption`; page-bottom dates → `date_header`; volume-content headers → `running_header`. List: republic-segmentation `plan/v2-list.md`.
- **Scoring only:** REPUBLIC labels (and these tables derived from them) are for scoring and checking, never features.

**AS status (2026-10-28):** read; AS does not link the layer yet (no step uses it; AS's `.venv` has no Parquet reader).
Recorded in `ground_truth/README.md` (successor of the 2026-03 `session_metadata`/`resolution_metadata`, scoring only),
`docs/text_layer.md` (candidate shared source for the print REPUBLIC crosswalk, not yet read by `--republic-dir`),
`docs/from-republic.md` §10 (resumption typing), `plan/CONTEXT.md` §3–§4 (data fact and follow-ups) and the phase-13
review checklist (resolution pages as silver negatives for the index-page detector). When a step links it, AS follows
the shared-data protocol § 3 and adds itself to the catalogue row's "used by".
