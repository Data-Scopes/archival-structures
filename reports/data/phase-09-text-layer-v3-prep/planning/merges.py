"""Right sides of `merge` boundaries in join/, per inventory, with seeded samples (phase 09; step 15l).

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/merges.py \
        --layer data/text_layer_v2_dev/15ir-a [--json F.json] [--sample-out S.tsv --sample 40 --seed 51]

For every boundary with `boundary_kind == "merge"`, the first character of the next segment in the
unit text (`text[char_start]` of segment i+1) is classed: `J+lower` (`J` then a lower-case letter),
`upper`, `digit`, `lower`, `comma_sp` (", "), `comma2` (",,") or `other`. Also counts every boundary
kind (so `g_upper_right` shows up once the guard is on).
The sample (`--sample-out`): `--sample` rows per class and sub-layer for `upper` (Random(seed),
Random(seed+1) for hand, print), 30 `J+lower` rows of 455 (Random(seed+2)) plus all `J+lower` rows of
456, and every `comma_sp` / `comma2` row; context is 25 chars either side of the boundary.
"""
import argparse
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import inventories, layer_root, read  # noqa: E402


def klass(t, cs):
    ch = t[cs:cs + 1]
    if ch == "J" and t[cs + 1:cs + 2].islower():
        return "J+lower"
    if ch.isupper():
        return "upper"
    if ch.isdigit():
        return "digit"
    if ch.islower():
        return "lower"
    if t[cs:cs + 2] == ", ":
        return "comma_sp"
    if t[cs:cs + 2] == ",,":
        return "comma2"
    return "other"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True)
    ap.add_argument("--json", default="")
    ap.add_argument("--sample-out", default="")
    ap.add_argument("--sample", type=int, default=40)
    ap.add_argument("--seed", type=int, default=51)
    a = ap.parse_args(argv)
    res = {}
    ex = defaultdict(list)
    for sub in ("hand", "print"):
        root = layer_root(a.layer, sub)
        for inv in inventories(sub):
            U = read(root / "join" / "units" / f"{inv}.tsv.gz", ["scan_id", "unit_id", "text"])
            txt = dict(zip(zip(U.scan_id, U.unit_id), U.text))
            S = read(root / "join" / "segments" / f"{inv}.tsv.gz",
                     ["scan_id", "unit_id", "seg_order", "char_start", "char_end", "boundary_kind"])
            S["seg_order"] = S.seg_order.astype(int)
            S = S.sort_values(["scan_id", "unit_id", "seg_order"], kind="stable")
            c, kinds = Counter(), Counter(k for k in S.boundary_kind if k)
            prev = None
            for r in S.itertuples():
                if (prev is not None and prev.boundary_kind == "merge"
                        and (prev.scan_id, prev.unit_id) == (r.scan_id, r.unit_id)):
                    t = txt[(r.scan_id, r.unit_id)]
                    cs, pe = int(r.char_start), int(prev.char_end)
                    k = klass(t, cs)
                    c[k] += 1
                    if k in ("upper", "J+lower", "comma_sp", "comma2"):
                        ex[(sub, k, inv if k == "J+lower" else "")].append(
                            (sub, inv, r.scan_id, r.unit_id, k, t[max(0, pe - 25):pe] + "|" + t[pe:cs + 25]))
                prev = r
            n = sum(c.values())
            res[f"{sub}:{inv}"] = {"merges": n, "right_side": dict(sorted(c.items())),
                                   "boundary_kinds": dict(sorted(kinds.items()))}
            print(sub, inv, "merges", n, dict(sorted(c.items())), "kinds", dict(sorted(kinds.items())))
    if a.json:
        Path(a.json).write_text(json.dumps(res, indent=1), encoding="utf-8")
    if a.sample_out:
        rows = []
        for i, sub in enumerate(("hand", "print")):
            items = list(ex[(sub, "upper", "")])
            random.Random(a.seed + i).shuffle(items)
            rows += items[:a.sample]
        items = list(ex[("print", "J+lower", "455")])
        random.Random(a.seed + 2).shuffle(items)
        rows += items[:30] + ex[("print", "J+lower", "456")]
        for sub in ("hand", "print"):
            rows += ex[(sub, "comma_sp", "")] + ex[(sub, "comma2", "")]
        with open(a.sample_out, "w", encoding="utf-8") as fh:
            fh.write("sub\tinv\tscan_id\tunit_id\tclass\tcontext\tverdict\n")
            for r in rows:
                fh.write("\t".join(r) + "\t\n")


if __name__ == "__main__":
    main()
