"""Step 15i A2: per-inventory byte comparison of the pilot layer against 15hj-b (decompressed)."""
import gzip
import json
import sys
from pathlib import Path

NEW = Path("data/text_layer_v2_dev/15i-a")
OLD = Path("data/text_layer_v2_dev/15hj-b")
TABLES = ["scans", "regions_meta", "lines", "layout/scans", "layout/regions", "layout/lines", "types/segments",
          "types/regions", "units/units", "units/segments", "units/doc_id_map", "units/republic_lines",
          "join/units", "join/segments"]
JSONS = ["join/stats", "extract_stats", "split_stats"]
TAR = {"3349", "3350", "3351", "3352", "3353", "3354"}


def root(base, kind, sub):
    return next((base / sub).glob("*_text_layer_v2"))


def read(p):
    return gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()


def main():
    invs = [("hand", i) for i in "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()] + \
           [("print", "455"), ("print", "456")]
    res = {"equal": 0, "differ": [], "missing": []}
    for kind, inv in invs:
        n = root(NEW, kind, kind)
        o = root(OLD, kind, "print" if kind == "print" else ("hand_tar" if inv in TAR else "hand_local"))
        for t in TABLES:
            pn = n / t / (f"{inv}.tsv.gz" if t != "scans" else f"{inv}.scans.tsv.gz")
            if t == "lines":
                pn = n / "lines" / f"{inv}.lines.tsv.gz"
            po = o / pn.relative_to(n)
            if not po.exists() and not pn.exists():
                continue
            if not po.exists() or not pn.exists():
                res["missing"].append(f"{inv}:{t}")
            elif read(pn) == read(po):
                res["equal"] += 1
            else:
                res["differ"].append(f"{inv}:{t}")
        for t in JSONS:
            pn = n / t / f"{inv}.json"
            po = o / t / f"{inv}.json"
            if pn.exists() or po.exists():
                if pn.exists() and po.exists() and read(pn) == read(po):
                    res["equal"] += 1
                else:
                    res["differ"].append(f"{inv}:{t}.json")
        # wbc: set column of the per-inventory table
    def wbc(r):
        out = {}
        for line in (r / "wbc" / "wbc_per_inventory.tsv").read_text().splitlines()[1:]:
            c = line.split("\t")
            out[c[0]] = c
        return out
    cols = (root(NEW, "", "hand") / "wbc" / "wbc_per_inventory.tsv").read_text().splitlines()[0].split("\t")
    res["wbc_columns"] = cols
    wn = {**wbc(root(NEW, "", "hand")), **wbc(root(NEW, "", "print"))}
    wo = {**wbc(root(OLD, "", "hand_tar")), **wbc(root(OLD, "", "hand_local")), **wbc(root(OLD, "", "print"))}
    res["wbc_diff"] = {}
    for inv in wo:
        if wn.get(inv) != wo[inv]:
            res["wbc_diff"][inv] = {"new": dict(zip(cols, wn.get(inv, []))), "old": dict(zip(cols, wo[inv]))}
    json.dump(res, open(Path(__file__).parent / "repro.json", "w"), indent=1)
    print(res["equal"], "equal;", len(res["differ"]), "differ:", res["differ"], "missing:", res["missing"])
    print("wbc differing invs:", list(res["wbc_diff"]))


main()
