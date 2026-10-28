"""Phase-13 review: post-hoc entry variants on the burned 19a gold (diagnostic only, never a bar).

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/guard_estimate.py

Reads ``entry_lines_{dev,test}.tsv`` (written by ``entry_interleave_vs_cues.py``; per gold line the start method of
E3/E4/E5 on the v4 hand layout (``E3o``, ``E4o``, ``E5``) and on the reviewer's geometric re-split (``E3rs``,
``E4rs``)). ``guard=1`` removes every start on a line whose stripped text is only digits and punctuation (a page
reference list that ran over), which equals a rule that never cuts before such a line.
"""
from pathlib import Path

import pandas as pd

R = Path("reports/data/phase-13-register-index-volumes/review")
D = {s: pd.read_csv(R / f"entry_lines_{s}.tsv", sep="\t", dtype=str, keep_default_na=False) for s in ("dev", "test")}
D["pooled"] = pd.concat([D["dev"], D["test"]], ignore_index=True)
for s, d in D.items():
    no = d.text.str.strip().str.fullmatch(r"[\d\s.,:;]+")
    for k in ("E3o", "E4o", "E3rs", "E4rs", "E3reo", "E4reo", "E5"):
        st = ~d[k].isin(["-", "nounit"])
        for guard in (False, True):
            ok = st & (~no if guard else True)
            tp = int(((d.label == "S") & ok).sum())
            fp = int(((d.label == "C") & ok).sum())
            fn = int((d.label == "S").sum()) - tp
            p, r = tp / (tp + fp), tp / (tp + fn)
            print(f"{s:6s} {k:5s} guard={int(guard)} P {p:.4f} R {r:.4f} F1 {2 * p * r / (p + r):.4f} "
                  f"tp {tp} fp {fp} fn {fn}")
