"""Equivalence check AS separator_sheets vs NLP startpage.separator_sheets.

Run: cd $NLP && PYTHONNOUSERSITE=1 PYTHONPATH=<AS root> venv/bin/python <this> --out-dir <dir>
Input: $NLP/output/early_end/page_text.csv.gz (read only).
"""
import argparse
import csv
import os

import numpy as np
import pandas as pd

from archival_structures.analysis import separator_sheets as AS
from startpage import separator_sheets as NL

NLP = "/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--page-text", default=f"{NLP}/output/early_end/page_text.csv.gz")
    a = ap.parse_args()
    df = pd.read_csv(a.page_text, usecols=["inventory_number", "scanname", "text"], dtype={"inventory_number": str})
    df = df.rename(columns={"scanname": "page_id"})
    n = len(df)
    rows = []
    log = open(os.path.join(a.out_dir, "run.log"), "w")

    def rec(check, cnt, mism):
        rows.append((check, cnt, mism))
        print(check, cnt, mism, file=log, flush=True)

    t_as = [AS.is_separator_sheet(t) for t in df["text"]]
    t_nl = [NL.is_separator_sheet(t) for t in df["text"]]
    rec("is_separator_sheet", n, sum(x != y for x, y in zip(t_as, t_nl)))
    s_as, s_nl = AS.detect_sheets(df), NL.detect_sheets(df)
    rec("detect_sheets", len(s_nl), len(s_as ^ s_nl))
    print("n sheets", len(s_as), file=log)
    rng = np.random.default_rng(18)
    probs = np.random.default_rng(18).random(n)
    df["pred_prob"] = probs
    df = df.sample(frac=1, random_state=18)
    invs = sorted(df["inventory_number"].unique())
    choice = rng.choice(["none", "sheet", "next"], size=len(invs))
    cdict = dict(zip(invs, choice))
    for name, conv in [("none", "none"), ("sheet", "sheet"), ("next", "next"), ("dict", cdict)]:
        for th in (0.3, 0.5, 0.7):
            x = AS.apply_rule(df, s_as, conv, th)
            y = NL.apply_rule(df, s_nl, conv, th)
            rec(f"apply_rule[{name},{th}]", n, int((x != y).sum()))
    mism = sum(AS.next_scan_id(p) != p.rsplit("_", 1)[0] + f"_{NL._scan_no(p) + 1:04d}" for p in df["page_id"])
    rec("next_scan_id", n, mism)
    with open(os.path.join(a.out_dir, "compare.tsv"), "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["check", "n_compared", "mismatches"])
        w.writerows(rows)
    sh = df.loc[[(str(i), p) in s_as for i, p in zip(df["inventory_number"], df["page_id"])], ["inventory_number", "page_id", "text"]]
    sh = sh.sort_values(["inventory_number", "page_id"])
    sh["text"] = sh["text"].str.strip().str.replace("\n", "\\n")
    sh.to_csv(os.path.join(a.out_dir, "sheets.tsv"), sep="\t", index=False)


main()
