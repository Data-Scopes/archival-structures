"""Step 15p: hand/print census of NL-HaNA 1.01.02 by text density per PageXML region.

Reads the paragraph TSVs (read only), one row per text region; writes per-inventory region
labels, a per-scan table, and the report tables. The rule is fixed (phase 10, decision P3);
see plan/phase-10-hand-print-per-region/step-15p-hand-print-census.md.

Inputs (already local, nothing to copy):
  /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/paragraphs/source/
      paragraphs-NL-HaNA_1.01.02_<inv>.tsv.gz   (9,122 files)

Usage (from the AS root):
  .venv/bin/python -I reports/data/phase-10-hand-print-per-region/step-15p/census.py --self-test
  .venv/bin/python -I .../census.py --fit-only
  .venv/bin/python -I .../census.py --limit 20 --workers 1 --out-dir /tmp/x   # smoke
  coreshare.py run ... -- .venv/bin/python -I .../census.py --workers {workers}

Resumable: inventories whose regions/<inv>.tsv.gz exist are not recomputed only if --resume is
given; by default a non-empty out-dir is refused (a re-run goes to a new dir).
"""
import argparse
import concurrent.futures as cf
import gzip
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DEFAULT_TSV = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/paragraphs/source")
PREFIX = "NL-HaNA_1.01.02_"
DEV_HAND = ["3349", "3350", "3351", "3352", "3353", "3354", "11182", "11281"]
DEV_PRINT = ["3771", "3772", "3773", "3821", "3822", "3823"]
MIN_CHARS = 20
BIN = 0.02
NBINS = 100  # 0..2.0, plus overflow bin
BOX = re.compile(r"-text_region-(-?\d+)-(-?\d+)-(\d+)-(\d+)$")


def inv_of(scan_id):
    return scan_id[len(PREFIX):].rsplit("_", 1)[0]


def load(path):
    d = pd.read_csv(path, sep="\t", quoting=3, dtype=str, keep_default_na=False,
                    usecols=["scan_id", "doc_id", "text"])
    box = d["doc_id"].str.extract(BOX).astype("float")
    box.columns = ["x", "y", "w", "h"]
    out = pd.DataFrame({"scan_id": d["scan_id"], "x": box["x"], "y": box["y"], "w": box["w"],
                        "h": box["h"], "n_chars": d["text"].str.len()})
    area = out["w"] * out["h"]
    out["density"] = np.where(area > 0, 1000.0 * out["n_chars"] / area.where(area > 0, 1), np.nan)
    return out


def label_regions(df, lo, hi):
    lab = np.full(len(df), "uncertain", dtype=object)
    dens = df["density"].to_numpy()
    ok = ~np.isnan(dens)
    lab[ok & (dens <= lo)] = "hand"
    lab[ok & (dens >= hi)] = "print"
    lab[df["n_chars"].to_numpy() < MIN_CHARS] = "short"
    return lab


def scan_label(print_share, labelled):
    if labelled < 200:
        return "uncertain"
    if print_share >= 0.9:
        return "print"
    if print_share <= 0.1:
        return "hand"
    return "mixed"


def scan_table(df):
    """df has scan_id, n_chars, label (region labels). One row per scan."""
    g = df.assign(
        hand_chars=np.where(df["label"] == "hand", df["n_chars"], 0),
        print_chars=np.where(df["label"] == "print", df["n_chars"], 0),
        uncertain_chars=np.where(df["label"] == "uncertain", df["n_chars"], 0),
        short=(df["label"] == "short").astype(int),
        one=1,
    ).groupby("scan_id", sort=True).agg(
        n_regions=("one", "sum"), n_chars=("n_chars", "sum"), hand_chars=("hand_chars", "sum"),
        print_chars=("print_chars", "sum"), uncertain_chars=("uncertain_chars", "sum"),
        short_regions=("short", "sum")).reset_index()
    lab = g["hand_chars"] + g["print_chars"]
    g["print_share"] = np.where(lab > 0, g["print_chars"] / lab.where(lab > 0, 1), np.nan)
    g["label"] = [scan_label(s, int(n)) for s, n in zip(g["print_share"], lab)]
    g.insert(0, "inv", g["scan_id"].map(inv_of))
    return g


def volume_label(labels):
    """labels: iterable of scan labels of one inventory -> (share, label)."""
    labels = list(labels)
    nh, npr = labels.count("hand"), labels.count("print")
    if nh + npr < 5:
        return (npr / (nh + npr) if nh + npr else np.nan), "uncertain"
    share = npr / (nh + npr)
    return share, ("print" if share >= 0.9 else "hand" if share <= 0.1 else "mixed")


def fit_thresholds(tsv_dir):
    dens = {}
    for cls, invs in (("hand", DEV_HAND), ("print", DEV_PRINT)):
        parts = []
        for inv in invs:
            d = load(tsv_dir / f"paragraphs-{PREFIX}{inv}.tsv.gz")
            d = d[(d["n_chars"] >= MIN_CHARS) & d["density"].notna()]
            parts.append(d["density"].to_numpy())
        dens[cls] = np.concatenate(parts)
    pct = [1, 5, 25, 50, 75, 95, 99]
    t_hand = float(np.percentile(dens["hand"], 95))
    t_print = float(np.percentile(dens["print"], 5))
    lo, hi = min(t_hand, t_print), max(t_hand, t_print)
    return {
        "lo": lo, "hi": hi, "t_hand_p95": t_hand, "t_print_p05": t_print, "swapped": t_hand > t_print,
        "percentiles": pct,
        "hand_dev_percentiles": [float(np.percentile(dens["hand"], p)) for p in pct],
        "print_dev_percentiles": [float(np.percentile(dens["print"], p)) for p in pct],
        "n_regions_hand_dev": int(len(dens["hand"])), "n_regions_print_dev": int(len(dens["print"])),
        "n_hand_dev_labelled": {k: int(v) for k, v in zip(
            ["hand", "uncertain", "print"],
            [(dens["hand"] <= lo).sum(), ((dens["hand"] > lo) & (dens["hand"] < hi)).sum(), (dens["hand"] >= hi).sum()])},
        "n_print_dev_labelled": {k: int(v) for k, v in zip(
            ["hand", "uncertain", "print"],
            [(dens["print"] <= lo).sum(), ((dens["print"] > lo) & (dens["print"] < hi)).sum(), (dens["print"] >= hi).sum()])},
        "dev_hand_inventories": DEV_HAND, "dev_print_inventories": DEV_PRINT,
        "min_chars": MIN_CHARS, "density": "1000 * n_chars / (w * h)",
    }


def process(args):
    path, out_dir, lo, hi = args
    try:
        df = load(path)
        df["label"] = label_regions(df, lo, hi)
        df["inv"] = df["scan_id"].map(inv_of)
        scans, stats, odd = [], [], []
        hist_n = np.zeros(NBINS + 1)
        hist_c = np.zeros(NBINS + 1)
        for inv, g in df.groupby("inv", sort=False):
            g[["scan_id", "x", "y", "w", "h", "n_chars", "density", "label"]].to_csv(
                Path(out_dir) / "regions" / f"{inv}.tsv.gz", sep="\t", index=False, float_format="%.5g")
            scans.append(scan_table(g))
            stats.append({"inv": inv, "n_regions": len(g), "n_chars": int(g["n_chars"].sum()),
                          "median_w": float(g["w"].median()), "median_h": float(g["h"].median()),
                          "nan_density": int(g["density"].isna().sum())})
        lab = df[(df["n_chars"] >= MIN_CHARS) & df["density"].notna()]
        b = np.minimum((lab["density"].to_numpy() / BIN).astype(int), NBINS)
        np.add.at(hist_n, b, 1)
        np.add.at(hist_c, b, lab["n_chars"].to_numpy())
        o = df[(df["n_chars"] >= 200) & ((df["density"] > 1.5) | (df["density"] < 0.05))]
        return Path(path).name, pd.concat(scans), stats, hist_n, hist_c, o, None
    except Exception as e:  # recorded, not fatal
        return Path(path).name, None, None, None, None, None, repr(e)


def self_test():
    lo, hi = 0.42, 0.44
    # density = 1000*n/(w*h); box 1000x1000 -> density = n/1000
    rows = [("S_A", 300), ("S_A", 10), ("S_B", 800), ("S_C", 300), ("S_C", 800), ("S_D", 430)]
    df = pd.DataFrame(rows, columns=["scan_id", "n_chars"])
    df["w"] = 1000.0
    df["h"] = 1000.0
    df["density"] = 1000.0 * df["n_chars"] / (df["w"] * df["h"])
    df["label"] = label_regions(df, lo, hi)
    assert list(df["label"]) == ["hand", "short", "print", "hand", "print", "uncertain"], list(df["label"])
    df2 = df.copy()
    df2["scan_id"] = [PREFIX + "1_" + s for s in df["scan_id"]]
    sc = scan_table(df2).set_index("scan_id")
    got = {k.split("_")[-1]: v for k, v in sc["label"].items()}
    assert got == {"A": "hand", "B": "print", "C": "mixed", "D": "uncertain"}, got
    assert volume_label(["hand"] * 10)[1] == "hand"
    assert volume_label(["print"] * 9 + ["hand"])[1] == "print"
    assert volume_label(["print"] * 5 + ["hand"] * 5)[1] == "mixed"
    assert volume_label(["print"] * 4)[1] == "uncertain"
    print("self-test ok")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tsv-dir", type=Path, default=DEFAULT_TSV)
    ap.add_argument("--out-dir", type=Path, default=Path("data/hand_print_census/15p-a"))
    ap.add_argument("--report-dir", type=Path, default=HERE)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--fit-only", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    thr_path = a.report_dir / "thresholds.json"
    if a.fit_only or not thr_path.exists():
        thr = fit_thresholds(a.tsv_dir)
        thr_path.write_text(json.dumps(thr, indent=1))
        print(f"thresholds: lo={thr['lo']:.4f} hi={thr['hi']:.4f} -> {thr_path}")
        if a.fit_only:
            return
    thr = json.loads(thr_path.read_text())
    lo, hi = thr["lo"], thr["hi"]
    out = a.out_dir
    if (out / "scans.tsv.gz").exists():
        sys.exit(f"{out} already holds a census; use a new --out-dir")
    (out / "regions").mkdir(parents=True, exist_ok=True)
    files = sorted(a.tsv_dir.glob("paragraphs-*.tsv.gz"))
    if a.limit:
        files = files[: a.limit]
    print(f"{len(files)} files, workers={a.workers}, lo={lo:.4f} hi={hi:.4f}", flush=True)
    scans, stats, odd, fails = [], [], [], []
    hist_n = np.zeros(NBINS + 1)
    hist_c = np.zeros(NBINS + 1)
    jobs = [(f, out, lo, hi) for f in files]
    with cf.ProcessPoolExecutor(max_workers=a.workers) as ex:
        for i, (name, sc, st, hn, hc, o, err) in enumerate(ex.map(process, jobs, chunksize=8), 1):
            if err:
                fails.append((name, err))
                continue
            scans.append(sc)
            stats += st
            odd.append(o)
            hist_n += hn
            hist_c += hc
            if i % 500 == 0:
                print(f"{i}/{len(files)}", flush=True)
    scans = pd.concat(scans, ignore_index=True)
    scans.to_csv(out / "scans.tsv.gz", sep="\t", index=False, float_format="%.5g")
    pd.DataFrame(stats).to_csv(out / "inv_stats.tsv", sep="\t", index=False)
    pd.concat(odd, ignore_index=True).to_csv(out / "odd_regions.tsv.gz", sep="\t", index=False, float_format="%.5g")
    edges = [round(i * BIN, 2) for i in range(NBINS + 1)]
    pd.DataFrame({"bin_lo": edges, "bin_hi": edges[1:] + [None], "n_regions": hist_n.astype(int),
                  "n_chars": hist_c.astype(int)}).to_csv(
        a.report_dir / "density_hist.tsv", sep="\t", index=False)
    pd.DataFrame(fails, columns=["file", "error"]).to_csv(out / "failures.tsv", sep="\t", index=False)
    print(f"done: {len(files) - len(fails)} ok, {len(fails)} failed; scans={len(scans)}")


if __name__ == "__main__":
    main()
