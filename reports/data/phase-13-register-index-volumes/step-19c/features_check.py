"""Step 19c, acceptance 2: the feature columns of every layout/pages row of the 19c clones against
planning/side_features.tsv (join inv, scan_id, page): 115,274 rows present and equal.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/features_check.py
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import load_pages  # noqa: E402

PLAN = Path(__file__).resolve().parents[1] / "planning"
FEATS = ["n_lines", "n_text8", "n_text20", "col_share", "end_num", "date_start"]

if __name__ == "__main__":
    mine = load_pages()
    ref = pd.read_csv(PLAN / "side_features.tsv", sep="\t", dtype=str, keep_default_na=False)
    print("layout/pages rows:", len(mine), " side_features rows:", len(ref))
    m = ref.merge(mine, on=["inv", "scan_id", "page"], how="outer", suffixes=("_ref", ""), indicator=True)
    print(m["_merge"].value_counts().to_dict())
    both = m[m["_merge"] == "both"]
    bad = 0
    for c in FEATS:
        a, b = both[c + "_ref"], both[c]
        if c in ("col_share", "end_num", "date_start", "n_lines", "n_text8", "n_text20"):
            na = pd.to_numeric(a.replace("", None))
            nb = pd.to_numeric(b.replace("", None))
            diff = ~((na == nb) | (na.isna() & nb.isna()) | ((na - nb).abs() < 1e-4))
        n = int(diff.sum())
        bad += n
        print(f"  {c}: {n} differing rows")
    ok = len(both) == len(ref) == len(mine) and bad == 0
    print("EQUAL" if ok else "DIFFERENT")
    sys.exit(0 if ok else 1)
