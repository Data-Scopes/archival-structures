"""Gold metrics of the AS region-quality filter on the fixture. Run: .venv/bin/python metrics.py
Prints the acceptance table (before = exemption threshold inf, after = defaults) and writes
predictions.tsv next to this script (for the error analysis)."""
import math
from pathlib import Path

import pandas as pd

from archival_structures.analysis.region_quality import (
    QualityThresholds, is_kept, quality_frame)

FIX = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "region_quality"
HERE = Path(__file__).resolve().parent
KEEP = {"usable", "fragment"}


def load():
    gold = pd.read_csv(FIX / "gold_rows.tsv", sep="\t", quoting=3, keep_default_na=False)
    lex = frozenset(l.strip() for l in (FIX / "lexicon_subset.txt").read_text(encoding="utf-8").splitlines() if l.strip())
    return gold, lex


def predict(gold, lex, **kw):
    df = quality_frame(gold, lexicon=lex, id_col=None, with_features=False, thresholds=QualityThresholds(**kw))
    return df


def metrics(df):
    g_keep = df.gold_label.isin(KEEP)
    p_keep = df.quality_class.map(is_kept)
    tp = int((g_keep & p_keep).sum())
    return {"n": len(df), "keep_P": tp / int(p_keep.sum()), "keep_R": tp / int(g_keep.sum()),
            "keepdrop_acc": float((g_keep == p_keep).mean()), "3way_acc": float((df.gold_label == df.quality_class).mean())}


def table():
    gold, lex = load()
    out = {}
    for view, kw in (("before", {"digit_vs_punct_fraction_for_numeric_exempt": math.inf}), ("after", {})):
        df = predict(gold, lex, **kw)
        ho = df[df.split == "held_out"]
        out[view] = {"sheet02_heldout": metrics(ho[ho.sheet == "02"]), "combined_heldout": metrics(ho)}
    return out


if __name__ == "__main__":
    t = table()
    for view, d in t.items():
        for k, m in d.items():
            print(view, k, {a: round(b, 3) if isinstance(b, float) else b for a, b in m.items()})
    gold, lex = load()
    pred = predict(gold, lex)
    pred.to_csv(HERE / "predictions.tsv", sep="\t", index=False, quoting=3)
