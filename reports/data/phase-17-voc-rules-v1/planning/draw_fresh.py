"""Phase 17 planning: draw the fresh VOC test volumes (seed 20261101).

Fresh = an OBP inventory (GLOBALISE digitised indexes) whose PageXML zip is on T7_Data and that is
not among the 567 phase-16 inventories (`data/text_layer_voc_inputs/1.04.02/`). The first 24 of
the seeded order are the sample; ranks 25-40 are reserves (used only under the rule in step 23a).
Writes `fresh_volumes.tsv`. Reads no PageXML content: only file names, sizes and OBP row counts.
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/draw_fresh.py
"""
import csv, os, random, re
from pathlib import Path

SEED = 20261101
N_SAMPLE, N_RESERVE = 24, 16
OBP = Path("ground_truth/NL-HaNA_1.04.02/globalise_digitized_indexes/source/"
           "GLOBALISE - Digitized Indexes of the Dutch East India Company OBP (1602-1799).csv")
ZIPS = Path("/Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1")
BUILT = Path("data/text_layer_voc_inputs/1.04.02")
OUT = Path(__file__).parent / "fresh_volumes.tsv"


def key(inv):
    m = re.match(r"(\d+)(.*)", inv)
    return int(m.group(1)), m.group(2)


def main():
    obp_rows = {}
    with OBP.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            inv = (row["INVENTORY NUMBER"] or "").strip()
            if inv:
                obp_rows[inv] = obp_rows.get(inv, 0) + 1
    sizes = {p.name[:-4]: p.stat().st_size for p in ZIPS.iterdir() if p.name.endswith(".zip")}
    built = {p.name[:-4] for p in BUILT.iterdir() if p.name.endswith(".zip")}
    fresh = sorted((set(obp_rows) & set(sizes)) - built, key=key)
    order = random.Random(SEED).sample(fresh, N_SAMPLE + N_RESERVE)
    with OUT.open("w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["rank", "inv", "role", "zip_mb", "obp_rows"])
        for i, inv in enumerate(order, 1):
            w.writerow([i, inv, "sample" if i <= N_SAMPLE else "reserve",
                        round(sizes[inv] / 1e6, 1), obp_rows[inv]])
    os.chmod(OUT, 0o664)
    print(f"fresh pool {len(fresh)}; wrote {OUT}")


if __name__ == "__main__":
    main()
