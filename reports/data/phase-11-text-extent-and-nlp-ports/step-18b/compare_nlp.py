"""Step 18b equivalence check: AS ports vs NLP originals, plus error-analysis tables.

Run with NLP's interpreter and the AS working tree on PYTHONPATH:

    cd /Users/Shared/claude_projects/data_scopes/nlp-start-page-detection && \
    PYTHONNOUSERSITE=1 PYTHONPATH=<AS root> venv/bin/python <this file> --out-dir <dir>

Reads (read only): NLP embeddings_paragraph_only.npz, output/unsupervised_changepoints.csv,
output/early_end/page_table.csv, output/early_end/page_text.csv.gz.
Writes compare.tsv, run.log, weight_sensitivity.tsv, segment_lengths.tsv, dp_starts_2555.tsv.
Single process; ~1 minute plus two calibrations (~25 s each).
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
EXPECTED_WEIGHT = 0.0293559853524414


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args()
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(message)s",
                        handlers=[logging.FileHandler(out / "run.log", mode="w"), logging.StreamHandler()])
    log = logging.getLogger("compare")

    from startpage.config import EMBEDDINGS_DIR
    from startpage.sequence_features import build_full_page_sequences
    import startpage.unsupervised_segmentation as N
    import archival_structures.stream_analysis.overview.subsequence_detection as A

    z = np.load(EMBEDDINGS_DIR / "embeddings_paragraph_only.npz", allow_pickle=True)
    sims = N.build_similarity_sequences(z["text_embeddings"], z["scan_ids"].tolist(), build_full_page_sequences())
    log.info("%d inventories, %d pages", len(sims), sum(len(s) + 1 for s in sims.values()))

    rows = []
    w_n = N.calibrate_length_penalty_weight(sims, target_mean_segment_length=4.0)
    w_a = A.calibrate_length_penalty_weight(sims, target_mean_segment_length=4.0)
    log.info("weights NLP=%r AS=%r", w_n, w_a)
    rows.append(("calibrated weight", 1, int(not (w_n == w_a == EXPECTED_WEIGHT))))
    w = w_a

    mism = 0
    for inv, s in sims.items():
        for ww in (w, 1.0):
            if N.segment_dp(s, 4.0, ww) != A.segment_dp(s, 4.0, ww):
                mism += 1
    rows.append(("segment_dp starts (weights w, 1.0)", 2 * len(sims), mism))

    mism = 0
    for inv, s in sims.items():
        dn, bn = N.textiling_depth_scores(s, 3)
        da, ba = A.textiling_depth_scores(s, 3)
        if not (np.array_equal(dn, da) and bn == ba):
            mism += 1
    rows.append(("textiling depth + starts", len(sims), mism))

    cp = pd.read_csv(NLP / "output/unsupervised_changepoints.csv",
                     usecols=["inventory_number", "page_id", "position", "method", "is_change_point"])
    mism = 0
    for (inv, method), g in cp.groupby(["inventory_number", "method"]):
        s = sims[str(inv)] if str(inv) in sims else sims[inv]
        starts = set(A.segment_dp(s, 4.0, w) if method == "dp" else A.textiling_depth_scores(s, 3)[1])
        pred = g["position"].isin(starts)
        mism += int((pred != g["is_change_point"].astype(bool)).sum())
    rows.append(("changepoints.csv is_change_point", len(cp), mism))

    mism = 0
    for inv, s in sims.items():
        st = A.segment_dp(s, 4.0, w)
        if A.predicted_start_rate(st, len(s) + 1) != N.predicted_start_rate(st, len(s) + 1):
            mism += 1
    rows.append(("predicted_start_rate", len(sims), mism))

    pd.DataFrame(rows, columns=["check", "compared", "mismatches"]).to_csv(out / "compare.tsv", sep="\t", index=False)
    for r in rows:
        log.info("%s: compared %s, mismatches %s", *r)

    # ---- weight sensitivity: mean segment length at 25 candidates
    sens = []
    for c in np.geomspace(0.02, 2.0, 25):
        lengths = []
        for s in sims.values():
            e = A.segment_dp(s, 4.0, c) + [len(s) + 1]
            lengths.extend(b - a for a, b in zip(e, e[1:]))
        sens.append((c, float(np.mean(lengths)), float(np.median(lengths)), len(lengths)))
    pd.DataFrame(sens, columns=["weight", "mean_len", "median_len", "n_segments"]).to_csv(
        out / "weight_sensitivity.tsv", sep="\t", index=False)

    # ---- error analysis: segment-length quantiles vs labelled
    pt = pd.read_csv(NLP / "output/early_end/page_table.csv", usecols=["inventory_number", "scanname", "start"])
    pt["page_id"] = pt["scanname"].str.replace(r"\.(jpg|tif|xml)$", "", regex=True)
    sizes = {inv: len(s) + 1 for inv, s in sims.items()}
    big = max(sizes, key=sizes.get)
    small = min(sizes, key=sizes.get)
    picks = [big, small] + [k for k in sims if str(k) == "2555" and k not in (big, small)]
    qs = [0.1, 0.25, 0.5, 0.75, 0.9]
    qrows = []
    ids = cp[cp.method == "dp"].drop_duplicates(["inventory_number", "position"])
    for inv in picks:
        s = sims[inv]
        n = len(s) + 1
        g = ids[ids.inventory_number.astype(str) == str(inv)].sort_values("position")
        pid = g.page_id.tolist()
        lab = pt[pt.inventory_number.astype(str) == str(inv)].set_index("page_id")["start"]
        lab_pos = [i for i, p in enumerate(pid) if bool(lab.get(p, False))]
        for name, st in (("dp", A.segment_dp(s, 4.0, w)), ("textiling", A.textiling_depth_scores(s, 3)[1]),
                         ("labelled", lab_pos)):
            st = sorted(set(st) | {0})
            ln = np.diff(st + [n])
            qrows.append(dict(inventory=inv, n_pages=n, source=name, n_starts=len(st),
                              **{f"q{int(q*100)}": float(np.quantile(ln, q)) for q in qs}, mean=float(ln.mean())))
    pd.DataFrame(qrows).to_csv(out / "segment_lengths.tsv", sep="\t", index=False)

    # ---- 10 DP starts in 2555 with page text
    inv = next(k for k in sims if str(k) == "2555")
    s = sims[inv]
    st = [p for p in A.segment_dp(s, 4.0, w) if p > 0]
    rng = np.random.RandomState(18)
    pick = sorted(rng.choice(st, size=min(10, len(st)), replace=False).tolist())
    g = ids[ids.inventory_number.astype(str) == str(inv)].set_index("position")["page_id"]
    txt = pd.read_csv(NLP / "output/early_end/page_text.csv.gz", usecols=["inventory_number", "scanname", "text"])
    txt = txt[txt.inventory_number.astype(str) == str(inv)]
    txt["page_id"] = txt["scanname"].str.replace(r"\.(jpg|tif|xml)$", "", regex=True)
    txt = txt.set_index("page_id")["text"]
    lab = pt[pt.inventory_number.astype(str) == str(inv)].set_index("page_id")["start"]
    recs = []
    for p in pick:
        pid = g[p]
        t = str(txt.get(pid, ""))[:300].replace("\n", " | ")
        recs.append(dict(position=p, page_id=pid, labelled_start=bool(lab.get(pid, False)), text=t))
    pd.DataFrame(recs).to_csv(out / "dp_starts_2555.tsv", sep="\t", index=False)
    log.info("done; inventory 2555 has %d DP starts", len(st))


if __name__ == "__main__":
    main()
