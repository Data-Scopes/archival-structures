"""Draw the step-15z read list (phase-12 planning of 15y/15z, 2026-10-27; decision P28).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/planning/draw_reads_15z.py

The 37 volumes: every 1.01.02 volume of the full v4 run (`data/text_layer_v4_full`, staged 2026-10-27) built `print`
outside the printed EAD series (gedrukte resoluties, nouvelles, plakkaten: 35), plus the two silver-print plakkaten built
`hand` (12163, 12165; the user, 2026-10-27). Per volume 5 scans, drawn without replacement with probability
proportional to `n_chars` among the sided scans (`side` non-empty in `mode/scans/<inv>.tsv.gz`, i.e. >= 200 qualifying
characters), so the reads stand for the volume's text, not its page count; all sided scans if there are 5 or fewer.
The rows are shuffled (seed below) so that a reader goes through the volumes in mixed order. Writes
`reads_15z.tsv` next to this file: read_id, inv, scan_id, scan_order, n_chars.
"""
import random
from pathlib import Path

import pandas as pd

SEED = 20261104
MODE = Path("data/text_layer_v4_full/1.01.02/NL-HaNA_1.01.02_text_layer_v4/mode")
VOLUMES = ("2586 2600 3433 3436 3439 3442 3445 3463 3466 3469 3472 3475 3489 3726 4016 4792 7462 8080 8645 8828 "
           "9250 9252 9253 9264 9275 9396 9448 9449 9486 9487 9634 12297 12476 12477 12478 12163 12165").split()
N = 5


def weighted_sample(rng, items, weights, k):
    items, weights, out = list(items), list(weights), []
    while items and len(out) < k:
        i = rng.choices(range(len(items)), weights=weights)[0]
        out.append(items.pop(i))
        weights.pop(i)
    return out


def main():
    rng = random.Random(SEED)
    rows = []
    for inv in VOLUMES:
        sc = pd.read_csv(MODE / "scans" / f"{inv}.tsv.gz", sep="\t", usecols=["scan_id", "scan_order", "n_chars", "side"],
                         dtype={"scan_id": str, "side": str}, keep_default_na=False)
        sided = sc[sc["side"] != ""].sort_values("scan_id")
        picked = weighted_sample(rng, sided["scan_id"], sided["n_chars"], N)
        for sid in sorted(picked):
            r = sided[sided["scan_id"] == sid].iloc[0]
            rows.append([inv, sid, int(r["scan_order"]), int(r["n_chars"])])
    rng.shuffle(rows)
    out = pd.DataFrame(rows, columns=["inv", "scan_id", "scan_order", "n_chars"])
    out.insert(0, "read_id", range(1, len(out) + 1))
    path = Path(__file__).with_name("reads_15z.tsv")
    out.to_csv(path, sep="\t", index=False)
    print(f"{len(VOLUMES)} volumes, {len(out)} reads -> {path}")
    print(out.groupby("inv").size().value_counts().to_string())


if __name__ == "__main__":
    main()
