"""Shared helpers for the step-20e scripts (run from the AS root)."""
import csv
from collections import Counter
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
P = Path("reports/data/phase-14-index-links")
OUT = P / "step-20e"


def gold():
    return list(csv.DictReader(open(P / "step-20c/entry_gold.tsv"), delimiter="\t"))


def unit_texts(inv):
    return {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(L / "join" / "units" / f"{inv}.tsv.gz")}


def gold_numbers(g):
    return [int(x) for x in g["locators"].split(";") if x.strip()]


def prf(tp, n_pred, n_gold):
    p = tp / n_pred if n_pred else float("nan")
    r = tp / n_gold if n_gold else float("nan")
    f = 2 * p * r / (p + r) if p == p and r == r and p + r else 0.0
    return p, r, f


def compare(pred, gold):
    pc, gc = Counter(pred), Counter(gold)
    tp = sum((pc & gc).values())
    return tp, sum(pc.values()), sum(gc.values())
