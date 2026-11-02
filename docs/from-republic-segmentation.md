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

## 2026-10-29: `republic_segmentation_v2` published (from republic-segmentation, step 09c)
- **What/where:** `shared-data/NL-HaNA_1.01.02/republic_segmentation_v2/` (443 inventories) and `shared-data/NL-HaNA_1.10.94/republic_segmentation_v2/` (455, 456): the `09.2` segmentation, same tables as v1 plus `v1_id_map`. v1 stays where it is. AS uses the layer for scoring only (phase 14).
- **Link it:** shared-data protocol § 3 (symlink under your `data/`, read-only, add yourself to the catalogue row's "used by"); LotA: `../shared-data` in `[paths].allowed_roots`.
- **What changed for a v1 user:** sessions 122,069 → 121,341 (1,538 v1 sessions merged into a neighbour by V4 and the Eodem rule, 101 Nihil-actum sessions added); new `lines.unit_type` `resumption` (193,190 lines); the Preside line is `attendance_list`; page-bottom dates are `date_header`, volume headers `running_header`; "Eodem die" lines are `other_session_part`; dates re-flagged (`date_flag`).
- **Ids:** 2026-03-looking ids keep their meaning; every other v2 id is `rs2-<kind>-<inv>-<n>`. No `rs1-` id appears in v2, and `rs2-...-00001` need not be `rs1-...-00001`.
- **Moving from v1:** join your v1 ids on `v1_id_map.v1_id`; `kind` is `same`, `shifted`, `split`, `merged`, `mixed` or `dropped`; `v2_ids` lists the v2 units. v1 sessions: 118,265 same, 1,116 shifted, 101 split, 1,538 merged, 1,049 mixed, 0 dropped.
- **Known limits (random check 10 of 20 right, v1 7 of 20, not paired):** hand resolution starts are still the weak row; 3 of 20 A6 merges joined real separate sittings (4561, 4601, 4681); 24 slogan-led 1795+ print sessions (3862-3864) keep a false resolution start below the date line; held-out print primary F1 in P2 is below v1's (V6).
- **Scoring only:** REPUBLIC labels (and tables derived from them) are for scoring and checking, never features.

## 2026-10-30: `republic_segmentation_v3` published (from republic-segmentation, step 10e)
- **What/where:** `shared-data/NL-HaNA_1.01.02/republic_segmentation_v3/` (443 inventories) and `shared-data/NL-HaNA_1.10.94/republic_segmentation_v3/` (455, 456): the `10` segmentation, same tables as v2 plus `v2_id_map` (instead of `v1_id_map`). v1 and v2 stay where they are. AS uses the layer for scoring only (phase 14).
- **Link it:** shared-data protocol § 3 (symlink under your `data/`, read-only, add yourself to the catalogue row's "used by"); LotA: `../shared-data` in `[paths].allowed_roots`.
- **What changed for a v2 user:** sessions 121,341 → 120,745 (596 cuts removed: dates of inserted extracts, W5n/W9c); resolutions 772,714 → 790,846 (hand +3.9%, print equal); 10,263 hand lines typed `catch_word`; margin attendance lines (3,122) moved to the next sitting; no new type.
- **Ids:** 2026-03-looking ids keep their meaning; every other v3 id is `rs3-<kind>-<inv>-<n>`. No `rs1-` or `rs2-` id appears in v3, and `rs3-...-00001` need not be `rs2-...-00001`.
- **Moving from v2:** join your v2 ids on `v2_id_map.v2_id`; `kind` is `same`, `shifted`, `split`, `merged`, `mixed` or `dropped`; `v3_ids` lists the v3 units. v2 sessions: 110,871 same, 6,377 shifted, 1,073 merged, 3,020 mixed; resolutions: 740,039 same, 19,476 split (new starts), 5,023 merged, 1,488 dropped. A v1 user chains v1 → v2 (`v1_id_map` of v2) → v3.
- **Known limits:** hand resolution starts (A7 sealed half F1 0.891, H1 0.864, 17 of 132 gold starts missed); held-out hand manual session recall is 11 lines lower than v2 (10 inserted extracts that the manual gold lists as sittings, 1 real sitting removed in error: 4845); 3 of 20 A6 merges joined real separate sittings (4561, 4601, 4681).
- **Scoring only:** REPUBLIC labels (and tables derived from them) are for scoring and checking, never features.

## 2026-11-02: `shared-tools/listlike.py`, running text vs lists and tables (from republic-segmentation, step 11c3)
- **What/where:** `/Users/Shared/claude_projects/data_scopes/shared-tools/listlike.py` (stdlib only; README section "listlike"). For each line it gives window features (±3 lines in the same column or group): capital-initial share, closed previous line, length regularity, left-edge repetition, numbering and amount shares, words per line, short-token share, capitalised words per word, and caller flags (such as attendance). `list_score` applies a fixed rule with thresholds you pass in; nothing is fitted. The default patterns cover early-modern Dutch and French. Self-test: `python3 listlike.py --selftest`.
- **Why it might move here:** the user (2026-11-01) wants telling running text from list- and table-like elements to become reusable, and tested on a wider range of archival material than the REPUBLIC resolutions. The candidate home is archival-structures.
- **What the REPUBLIC reads showed** (hand resolutions 1576–1796; republic-segmentation `output/step11c3/cueset.md`):
  - **As a detector, the fixed list score is sound.** Of 10 windows it flagged, 9 were a list, a table or a name list; the false flag was a letter with interleaved columns. Of 10 it did not flag, all 10 were running text.
  - **The strongest single features** (AUC for a resolution start against other lines) are left-edge repetition (0.86, reversed), closed previous line (0.84), capital-initial share (0.89 for the line itself, 0.72 for the window) and length spread (0.72).
  - **Limits:**
    - It misses single-line table headings ("Lijste van besettinge") and short name lists ("Collonnel heer van Duijvenvoorde."). With only ±3 lines, a short list surrounded by prose looks like prose.
    - Used as a veto on resolution-start candidates, it blocked only 3.6% of them. Most false candidates were not lists, but continuations, HTR noise and formula clauses.
  - **Not tested:** other archives, print, other languages, tables with ruled columns, and region-level (rather than line-level) input.
- **Status in republic-segmentation:** the start model that used it was not kept (UA-149 a). `listlike.py` stays in shared-tools, unchanged, and no republic-segmentation step depends on it.
