"""Step 15u error-analysis helper (reads caches, writes nothing but stdout / analysis_*.txt / hrel_comparison.tsv).
  reads  : scan-level text_head samples (margin <= 5, lowest-valued diagnostic, minority scans)
  numbers: resolution, hrel comparison
"""
import csv, gzip, json, random, statistics, sys
from collections import Counter
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate import (CACHE, CACHE_Q, OUT, P10, ARCH, load_regions, fit_set, tsv_rows, write_tsv)  # noqa
from archival_structures.analysis.hand_print import VolumeModeParams, volume_mode_from_features  # noqa

cut = json.load(open(OUT / "cut.json"))["cut"]
tv = pd.read_csv(OUT / "test_volumes.tsv", sep="\t", dtype={"inv": str})
dv = pd.read_csv(OUT / "diagnostic_volumes.tsv", sep="\t", dtype={"inv": str})
ts = pd.read_csv(OUT / "test_scans.tsv.gz", sep="\t", dtype={"inv": str, "scan_id": str})
ds = pd.read_csv(OUT / "diagnostic_scans.tsv.gz", sep="\t", dtype={"inv": str, "scan_id": str})
T = 70


def regions_of(inv, sid):
    rs = [r for r in load_regions(CACHE, ARCH, inv) if r["scan_id"] == sid and r["h_med"] is not None]
    rs.sort(key=lambda r: -r["n_chars"])
    return rs[:3]


def show(inv, sid, k=2):
    for r in regions_of(inv, sid)[:k]:
        print(f"      h={r['h_med']:.0f} n={r['n_chars']} | {r['text_head'][:T]}")


def sided(scans, inv):
    s = scans[(scans.inv == inv) & (scans.side.notna())]
    return s


def hist(scans, inv):
    s = scans[(scans.inv == inv) & scans.value.notna() & (scans.n_chars >= 200)]
    c = Counter(int(v // 5) * 5 for v in s.value)
    return " ".join(f"{b}:{n}" for b, n in sorted(c.items()))


def nearest(scans, inv, value, k):
    s = sided(scans, inv).copy()
    rng = random.Random(21)
    s["d"] = (s.value - value).abs()
    s["r"] = [rng.random() for _ in range(len(s))]
    return s.sort_values(["d", "r"]).head(k)


mode = sys.argv[1]
if mode == "margin":
    for df, sc in ((tv, ts), (dv, ds)):
        for _, v in df[df.margin.abs() <= 5].iterrows():
            print(f"== {v.inv} {v.stratum} silver={v.silver} value={v.value} margin={v.margin} pw={v.median_page_w} minority={v.scan_minority_share}")
            print("   hist", hist(sc, v.inv))
            for _, s in nearest(sc, v.inv, v.value, 3).iterrows():
                print(f"   scan {s.scan_id} v={s.value} n={s.n_chars} side={s.side}")
                show(v.inv, s.scan_id)
elif mode == "diag":
    print(dv.merge(pd.read_csv(OUT.parent.parent / "phase-10-hand-print-per-region/step-15p/inventories.tsv", sep="\t", dtype={"inv": str}, usecols=["inv", "ancestor_3"]), on="inv")[["inv", "ancestor_3", "mode", "value", "margin"]].to_string())
    for _, v in dv.sort_values("value").head(3).iterrows():
        print(f"== {v.inv} value={v.value} hist", hist(ds, v.inv))
        for _, s in nearest(ds, v.inv, v.value, 3).iterrows():
            print(f"   scan {s.scan_id} v={s.value} n={s.n_chars} side={s.side}")
            show(v.inv, s.scan_id)
    print("diag minority>0.2:", dv[dv.scan_minority_share > 0.2].inv.tolist())
elif mode == "minority":
    for df, sc in ((tv, ts), (dv, ds)):
        for _, v in df[df.scan_minority_share > 0.2].iterrows():
            print(f"== {v.inv} {v.stratum} mode={v["mode"]} value={v.value} minority={v.scan_minority_share}")
            print("   hist", hist(sc, v.inv))
            s = sided(sc, v.inv)
            m = s[s.side != v["mode"]]
            rng = random.Random(21)
            idx = sorted(rng.sample(range(len(m)), min(5, len(m))))
            for i in idx:
                r = m.iloc[i]
                print(f"   scan {r.scan_id} v={r.value} n={r.n_chars} side={r.side}")
                show(v.inv, r.scan_id)
elif mode == "numbers":
    # resolution
    rows = []
    for st, arch, inv, cls in fit_set():
        if arch == ARCH:
            p = Path(f"data/text_layer_v2_dev/15m-a/hand/NL-HaNA_1.01.02_text_layer_v3/scans/{inv}.scans.tsv.gz")
            if not p.exists():
                continue
        else:
            p = Path(f"data/text_layer_v2_dev/15m-a/print/NL-HaNA_1.10.94_text_layer_v3/scans/{inv}.scans.tsv.gz")
        df = pd.read_csv(p, sep="\t", usecols=["page_w"])
        rows.append((st, inv, cls, df.page_w.median()))
    print("fit scans tables found:", len(rows), "of", len(fit_set()))
    for st, inv, cls, w in rows:
        if st == "dev_eval" or st == "transfer":
            print("  ", st, inv, cls, w)
    # fit set: dev+transfer from 15m-a; sample set from 15q pre? no page_w there
    fw = pd.DataFrame(rows, columns=["set", "inv", "cls", "w"])
    print(fw.groupby("cls").w.agg(["count", "min", "median", "max"]))
    lo, hi = fw.w.min(), fw.w.max()
    print("fit range", lo, hi)
    print(tv.groupby("silver").median_page_w.agg(["count", "min", "median", "max"]))
    print("test outside fit range:", tv[(tv.median_page_w < lo) | (tv.median_page_w > hi)][["inv", "silver", "median_page_w"]].to_string())
    # hrel comparison
    p = VolumeModeParams(1.529, 1.534)
    out = []
    for _, v in tv.iterrows():
        vm = volume_mode_from_features(load_regions(CACHE, ARCH, v.inv), p)
        out.append({"inv": v.inv, "stratum": v.stratum, "silver": v.silver, "box_mode": v["mode"], "hrel_mode": vm["mode"],
                    "hrel_method": vm["method"], "print_scan_share": vm["print_scan_share"]})
    o = pd.DataFrame(out)
    o.to_csv(OUT / "hrel_comparison.tsv", sep="\t", index=False)
    import os; os.chmod(OUT / "hrel_comparison.tsv", 0o664)
    print("rules disagree:", (o.box_mode != o.hrel_mode).sum(), "hrel matches silver:", (o.hrel_mode == o.silver).sum(), "box matches:", (o.box_mode == o.silver).sum())
    print(o[o.box_mode != o.hrel_mode].to_string())
