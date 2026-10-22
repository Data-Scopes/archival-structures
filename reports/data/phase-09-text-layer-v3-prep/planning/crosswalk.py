"""REPUBLIC crosswalk (units/republic_lines) by match class and role (phase 09; step 15k).

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/crosswalk.py \
        --layer data/text_layer_v2_dev/15ir-a [--republic-dir DIR] [--sample-out S.tsv --seed 61]

Role of a REPUBLIC line id: `start` (only in `line_start`), `end` (only in `line_end`), `both`.
Uses the table's own `role` column when it has one (step 15k), else derives it from the paragraphs
files. The sample: 20 `split_original` rows with role `end` (Random(seed)), each with the REPUBLIC
paragraph's last 40 chars and the text of every AS half of the split line, so a reader can see
which half the paragraph ends in.
"""
import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import PRINT_INV, REPUBLIC_DIR, layer_root, read  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True)
    ap.add_argument("--republic-dir", default=str(REPUBLIC_DIR))
    ap.add_argument("--json", default="")
    ap.add_argument("--sample-out", default="")
    ap.add_argument("--seed", type=int, default=61)
    a = ap.parse_args(argv)
    root = layer_root(a.layer, "print")
    res, sample = {}, []
    for inv in PRINT_INV:
        P = read(Path(a.republic_dir) / f"paragraphs-{inv}.tsv.gz", ["line_start", "line_end", "text"])
        st, en = set(P.line_start), set(P.line_end)
        end_text = dict(zip(P.line_end, P.text))
        X = read(root / "units" / "republic_lines" / f"{inv}.tsv.gz")
        if "role" not in X.columns:
            X["role"] = ["both" if i in st and i in en else "start" if i in st else "end"
                         for i in X.republic_line_id]
        c = Counter(zip(X.match, X.role))
        res[inv] = {f"{m}|{r}": n for (m, r), n in sorted(c.items())}
        print(inv, res[inv])
        if a.sample_out:
            L = read(root / "lines" / f"{inv}.lines.tsv.gz", ["scan_id", "line_id", "text", "split_from"])
            text = dict(zip(L.line_id, L.text))
            split_of = dict(zip(L.line_id, L.split_from))
            halves = {}
            for lid, sf in zip(L.line_id, L.split_from):
                if sf:
                    halves.setdefault(sf, []).append(lid)
            rows = X[(X.match == "split_original") & (X.role == "end")].to_dict("records")
            random.Random(a.seed + PRINT_INV.index(inv)).shuffle(rows)
            for r in rows[:10]:
                hs = halves.get(split_of.get(r["line_id"], ""), [])
                sample.append((inv, r["republic_line_id"], r["line_id"],
                               str(hs.index(r["line_id"]) if r["line_id"] in hs else -1),
                               end_text.get(r["republic_line_id"], "")[-40:],
                               " || ".join(text[h][:60] for h in hs)))
    if a.json:
        Path(a.json).write_text(json.dumps(res, indent=1), encoding="utf-8")
    if a.sample_out:
        with open(a.sample_out, "w", encoding="utf-8") as fh:
            fh.write("inv\trepublic_line_id\tline_id\thalf_idx\tparagraph_end\thalves_text\tverdict\n")
            for r in sample:
                fh.write("\t".join(r) + "\t\n")


if __name__ == "__main__":
    main()
