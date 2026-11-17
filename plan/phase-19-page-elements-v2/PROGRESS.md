# Phase 19 progress log

Append-only; one block per session.

### Planning (2026-11-17, Opus 5.5)
- **Inputs:** user decisions in [INPUTS](INPUTS.md) (UA-073 a; scope, test gold, item starts, analysis modules first).
- **Probes** (`reports/data/phase-19-page-elements-v2/planning/`, findings F19-P.1–P.5 in the report):
  - sample frame: 2.10.50 has only the four phase-18 volumes locally but 853 zips on the drive; 1.05.06 has 27 drive
    zips with thumbs and ≥ 40 scans; AsdSAA/AsnDA stay single volumes (fresh pages, ±3 scans from phase-18 pages);
  - `draw_volumes.py` → `volumes.tsv` (sha256 24a2b456…1a2a, seed 20261117), `dev_run_inventories.txt` (233);
  - smoke build of 2.10.50 `835` from the drive zip (extracted flat): 70 files, 0 failures;
  - margin cells on dev-A (94 positives): M0 82/234 (TP/FP), M1 71/130, M2 54/9, M3 47/0, M4 78/132, M5 47/2; best F1 M2
    (P .857, R .574): bar M2 likely fails on recall; most true margins are in 5023 openings (per-side bands needed);
  - element cells on dev-A: E0 P .950 R .575; E1 .912/.619; E2 .913/.754; E4 .877/.815; E5 .759/.946; plain table
    groups cost 354 FPs;
  - v1 → v2 conversion: dev-A 802 elements (156 forced splits), dev-B 677 (186).
- **Written:** PLAN.md, five briefs (25a–25e), STATUS.md, the report skeleton with a *Planning* section, the row in
  `plan/PLAN.md`. Answered UA-073 (moved to resolved).

### Housekeeping and release prep (2026-11-17, Opus 5.5, same session as planning)
- **UA-074 (b):** 13 full SG dev builds moved to the drive by `planning/ua074_move.py` (sha256 per file before and
  after the copy, all 51,520 files identical; local folder removed only after the match; symlinks at the old paths).
  Protect lists `ua074-protected-{before,after}.txt` on the drive differ by exactly the moved runs. Local
  `data/text_layer_v2_dev` 39 GB → 5.5 GB. Old phase scripts that read these runs need the drive mounted.
- **Release v0.23.0 docs:** changelog `## Unreleased` → `## v0.23.0 (2026-11-17)`; phase-18 STATUS/PLAN "Released in:
  v0.23.0"; `plan/PLAN.md` row 18; report headers. The user bumps, commits and tags. 25a opens a new `## Unreleased`.
