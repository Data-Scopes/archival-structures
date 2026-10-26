"""Step 15w error analysis: print what to read for a volume's scans (text heads of the largest regions, h_med).

    .venv/bin/python -I read_scans.py INV [INV ...] --n 2 --seed 21 [--pick near|minority] [--units K]
`near`: the sided scans whose value is nearest the volume's value (ties broken by seed); `minority`: random
sided scans on the other side of the cut than the volume. `--units K`: instead K random units (seed) with text.
The layer base is data/text_layer_v2_dev/15w-a; the archive is 1.10.94 for 455/456, else 1.01.02.
"""
import argparse
import json
import random
from pathlib import Path

import pandas as pd

L = Path("data/text_layer_v2_dev/15w-a")


def root_of(inv):
    a = "1.10.94" if inv in ("455", "456") else "1.01.02"
    return next((L / a).glob("NL-HaNA_*_text_layer_v4"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inv", nargs="+")
    ap.add_argument("--n", type=int, default=2)
    ap.add_argument("--seed", type=int, default=21)
    ap.add_argument("--pick", default="near")
    ap.add_argument("--units", type=int, default=0)
    ap.add_argument("--head", type=int, default=140)
    a = ap.parse_args()
    for inv in a.inv:
        r = root_of(inv)
        m = json.loads((r / "mode" / f"{inv}.json").read_text())
        cut = m["params"]["cut"]
        print(f"=== {inv} mode={m['mode']} value={m['value']} margin={m['margin']} minority={m['scan_minority_share']:.3f} "
              f"n_scans={m['n_scans']} sided={m['n_scans_sided']}")
        lines = pd.read_csv(r / "lines" / f"{inv}.lines.tsv.gz", sep="\t", dtype=str, usecols=["scan_id", "region_id", "text"],
                            keep_default_na=False)
        rng = random.Random(a.seed)
        if a.units:
            u = pd.read_csv(r / "units" / "units" / f"{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
            j = pd.read_csv(r / "join" / "units" / f"{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False,
                            usecols=["scan_id", "unit_id", "text"])
            u = u.merge(j, on=["scan_id", "unit_id"])
            u = u[(u.unit_type == "main_text") & (u.text.str.len() > 40)]
            idx = sorted(rng.sample(range(len(u)), min(a.units, len(u))))
            for i in idx:
                x = u.iloc[i]
                print(f"--- unit {x.unit_id[-40:]} ({x.unit_method}, {x.n_lines} lines)\n    {x.text[:400]!r}")
            continue
        sc = pd.read_csv(r / "mode" / "scans" / f"{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
        sc = sc[sc.side != ""].copy()
        sc["value"] = sc.value.astype(float)
        if a.pick == "near":
            sc["d"] = (sc.value - m["value"]).abs()
            sc["tie"] = [rng.random() for _ in range(len(sc))]
            pick = sc.sort_values(["d", "tie"]).head(a.n)
        else:
            mino = sc[sc.side != m["mode"]]
            print(f"    minority scans: {len(mino)} of {len(sc)}")
            pick = mino.sample(min(a.n, len(mino)), random_state=a.seed)
        regs = pd.read_csv(r / "mode" / "regions" / f"{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
        for _, s in pick.iterrows():
            print(f"--- scan {s.scan_id[-12:]} value={s.value} side={s.side} chars={s.n_chars}")
            rr = regs[regs.scan_id == s.scan_id].copy()
            rr["n_chars"] = rr.n_chars.astype(int)
            for _, g in rr.sort_values("n_chars", ascending=False).head(3).iterrows():
                t = " ".join(lines[lines.region_id == g.region_id].text.tolist())
                print(f"    h_med={g.h_med} chars={g.n_chars}: {t[:a.head]!r}")


if __name__ == "__main__":
    main()
