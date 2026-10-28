"""Step 19e step 7 (UA-044 a): the index-side detector against the REPUBLIC segmentation as silver negatives.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/silver.py

Reads (read only) shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/lines/inv=<inv>/part-0.parquet (columns scan_id,
page_id, unit_type) for the hand volumes of 19e-a that have a partition. page_id = <scan_id>-page-<n>: n = 2*scan - 2 verso,
2*scan - 1 recto. A side with >= 5 lines of unit_type resolution_paragraph, session_opening, attendance_list or
other_session_part is a silver negative (a resolution page). Scoring only, never a feature; no bar.
Writes silver_sides.tsv (one row per silver negative side: inv, scan_id, page, n_res_lines, index_side, rule) and
silver.json (per volume and total; seeded sample of 5 flagged negatives with their text -> silver_reads.txt)."""
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
OUT = A / "step-19e"
V5 = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
SILVER = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/lines")
RES = {"resolution_paragraph", "session_opening", "attendance_list", "other_session_part"}


def main():
    hand = sorted((p.stem for p in (V5 / "mode").glob("*.json")
                   if json.loads(p.read_text())["mode"] == "hand"), key=int)
    vols, sides_out, parity_bad = {}, [], 0
    for inv in hand:
        part = SILVER / f"inv={inv}" / "part-0.parquet"
        if not part.is_file():
            continue
        t = pq.read_table(part, columns=["scan_id", "page_id", "unit_type"]).to_pydict()
        cnt = Counter()
        for scan, pid, ut in zip(t["scan_id"], t["page_id"], t["unit_type"]):
            if ut in RES:
                cnt[(scan, pid)] += 1
        pages = {(r["scan_id"], r["page"]): r for r in read_tsv_gz(V5 / "layout/pages" / f"{inv}.tsv.gz")}
        v = {"silver_negatives": 0, "flagged": 0, "by_rule": 0, "by_gapfill": 0, "unmatched": 0}
        for (scan, pid), n in sorted(cnt.items()):
            if n < 5:
                continue
            m = re.search(r"-page-(\d+)$", pid)
            k = int(m.group(1))
            snum = int(scan.rsplit("_", 1)[1])
            page = "verso" if k == 2 * snum - 2 else "recto" if k == 2 * snum - 1 else None
            if page is None:
                parity_bad += 1
                v["unmatched"] += 1
                continue
            row = pages.get((scan, page))
            if row is None:
                v["unmatched"] += 1
                continue
            v["silver_negatives"] += 1
            if row["index_side"] == "1":
                v["flagged"] += 1
                v["by_rule" if row["rule"] == "1" else "by_gapfill"] += 1
                sides_out.append((inv, scan, page, n, row["index_side"], row["rule"]))
        vols[inv] = v
    with open(OUT / "silver_sides.tsv", "w") as f:
        f.write("inv\tscan_id\tpage\tn_res_lines\tindex_side\trule\n")
        for r in sides_out:
            f.write("\t".join(map(str, r)) + "\n")
    tot = Counter()
    for v in vols.values():
        tot.update(v)
    rnd = random.Random(20261129)
    sample = rnd.sample(sides_out, min(5, len(sides_out)))
    with open(OUT / "silver_reads.txt", "w") as f:
        for inv, scan, page, n, _, rule in sample:
            f.write(f"=== {inv} {scan} {page} silver_res_lines={n} rule={rule}\n")
            for r in read_tsv_gz(V5 / "lines" / f"{inv}.lines.tsv.gz"):
                pass
            lines = [r for r in read_tsv_gz(V5 / "layout/lines" / f"{inv}.tsv.gz") if r["scan_id"] == scan and r["page"] == page]
            txt = {r["line_id"]: r["text"] for r in read_tsv_gz(V5 / "lines" / f"{inv}.lines.tsv.gz") if r["scan_id"] == scan}
            for r in lines:
                f.write(f"{r.get('layout_column', '')}\t{txt.get(r['line_id'], '')}\n")
    res = {"n_volumes_with_silver": len(vols), "n_hand": len(hand), "total": dict(tot), "parity_unmatched": parity_bad,
           "per_volume": vols}
    (OUT / "silver.json").write_text(json.dumps(res, indent=1))
    print(len(vols), dict(tot))
    for inv, v in vols.items():
        print(inv, v)


if __name__ == "__main__":
    main()
