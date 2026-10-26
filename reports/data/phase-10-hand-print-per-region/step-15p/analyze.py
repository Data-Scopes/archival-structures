"""Step 15p analysis: volume labels, silver agreement, corpus shape, odd material, resolution.

Reads data/hand_print_census/15p-a (census.py output) and the SG EAD; writes the report tables
next to this file. Run from the AS root: .venv/bin/python -I <this file> [--census-dir DIR]
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from archival_structures.parsers.ead_tree import parse_ead
from archival_structures.parsers.ead_views import to_frame

HERE = Path(__file__).resolve().parent
EAD = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/ead/source/NL-HaNA_1.01.02.xml")
PRINT_PAT = ["Gedrukte registers van de ordinaris", "Gedrukte nouvelles", "Gedrukte plakkaten"]
HAND_PAT = ["Registers van ordinaris net-resoluties", "Geschreven indices"]
DEV = ["3349", "3350", "3351", "3352", "3353", "3354", "11182", "11281",
       "3771", "3772", "3773", "3821", "3822", "3823"]


def volume_label(labels):
    labels = list(labels)
    nh, npr = labels.count("hand"), labels.count("print")
    if nh + npr < 5:
        return (npr / (nh + npr) if nh + npr else np.nan), "uncertain"
    s = npr / (nh + npr)
    return s, ("print" if s >= 0.9 else "hand" if s <= 0.1 else "mixed")


def silver_table():
    df = to_frame(parse_ead(EAD), "file")
    anc = [c for c in df.columns if c.startswith("ancestor_")]
    df["path"] = df[anc].fillna("").agg(" > ".join, axis=1)
    df = df[df["inventory_number"].notna()].copy()

    def lab(p):
        pr = any(k in p for k in PRINT_PAT)
        ha = any(k in p for k in HAND_PAT)
        if pr and ha:
            # Geschreven indices sit under "Gedrukte registers": handwritten (brief)
            return "hand" if "Geschreven indices" in p else "conflict"
        return "print" if pr else "hand" if ha else ""
    df["silver"] = df["path"].map(lab)
    df = df.drop_duplicates("inventory_number")
    return df.set_index("inventory_number")[["silver", "ancestor_2", "ancestor_3", "title"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--census-dir", type=Path, default=Path("data/hand_print_census/15p-a"))
    a = ap.parse_args()
    c = a.census_dir
    scans = pd.read_csv(c / "scans.tsv.gz", sep="\t", dtype={"inv": str})
    st = pd.read_csv(c / "inv_stats.tsv", sep="\t", dtype={"inv": str})
    thr = json.loads((HERE / "thresholds.json").read_text())
    ead = silver_table()

    # ---- inventories.tsv
    rows = []
    for inv, g in scans.groupby("inv"):
        sh, lab = volume_label(g["label"])
        vc = g["label"].value_counts()
        rows.append({"inv": inv, "n_scans": len(g), "chars": int(g["n_chars"].sum()),
                     "hand_chars": int(g["hand_chars"].sum()), "print_chars": int(g["print_chars"].sum()),
                     "uncertain_chars": int(g["uncertain_chars"].sum()),
                     **{f"scans_{k}": int(vc.get(k, 0)) for k in ["hand", "print", "mixed", "uncertain"]},
                     "print_scan_share": sh, "label": lab})
    inv = pd.DataFrame(rows).merge(st[["inv", "median_w", "median_h"]], on="inv", how="left")
    inv["silver"] = inv["inv"].map(ead["silver"]).fillna("")
    inv["ancestor_2"] = inv["inv"].map(ead["ancestor_2"]).fillna("")
    inv["ancestor_3"] = inv["inv"].map(ead["ancestor_3"]).fillna("")
    inv["dev"] = inv["inv"].isin(DEV)
    inv["silver_missing_in_ead"] = ~inv["inv"].isin(ead.index)
    inv.to_csv(HERE / "inventories.tsv", sep="\t", index=False, float_format="%.5g")
    print("inventories", len(inv), "not in EAD:", int(inv["silver_missing_in_ead"].sum()),
          "conflict:", int((inv["silver"] == "conflict").sum()))

    # ---- totals
    print("\nTOTALS regions", int(st["n_regions"].sum()), "chars", int(st["n_chars"].sum()), "scans", len(scans))
    print(scans["label"].value_counts().to_string())
    ch = {k: int(scans[k].sum()) for k in ["hand_chars", "print_chars", "uncertain_chars", "n_chars"]}
    ch["short_chars_etc"] = ch["n_chars"] - ch["hand_chars"] - ch["print_chars"] - ch["uncertain_chars"]
    print("chars", ch, {k: round(v / ch["n_chars"], 4) for k, v in ch.items()})
    print("volumes by label\n", inv["label"].value_counts().to_string())
    print("chars by volume label\n", inv.groupby("label")["chars"].sum().to_string())
    lab = ch["hand_chars"] + ch["print_chars"]
    print("print share of labelled chars", ch["print_chars"] / lab)

    # ---- agreement
    sil = inv[inv["silver"].isin(["hand", "print"])]
    for name, d in [("all silver", sil), ("silver minus dev", sil[~sil["dev"]])]:
        x = pd.crosstab(d["silver"], d["label"]).reindex(columns=["hand", "print", "mixed", "uncertain"], fill_value=0)
        print("\n", name, "\n", x.to_string())
        if name == "all silver":
            x.assign(set="all").reset_index()
            allx = x.reset_index().assign(set="all")
        else:
            nodev = x.reset_index().assign(set="minus_dev")
    pd.concat([allx, nodev]).to_csv(HERE / "agreement.tsv", sep="\t", index=False)
    nd = sil[~sil["dev"]]
    pr = (nd[nd["silver"] == "print"]["label"] == "print").mean()
    hd = (nd[nd["silver"] == "hand"]["label"] == "hand").mean()
    print(f"STOP RULE: silver print->print {pr:.4f} (n={int((nd['silver']=='print').sum())}); "
          f"silver hand->hand {hd:.4f} (n={int((nd['silver']=='hand').sum())}); stop={pr < .9 or hd < .9}")
    print("dev labels\n", inv[inv["dev"]][["inv", "label", "print_scan_share", "scans_hand", "scans_print", "scans_mixed", "scans_uncertain"]].to_string())
    print("silver-labelled invs without TSV:", len(set(ead[ead.silver.isin(['hand','print'])].index) - set(inv.inv)))

    # ---- corpus shape
    h = pd.read_csv(HERE / "density_hist.tsv", sep="\t")
    for col in ["n_regions", "n_chars"]:
        s = h[col].rolling(3, center=True, min_periods=1).mean().to_numpy()
        peaks = [(round(float(h.bin_lo[i]), 2), int(h[col][i])) for i in range(1, len(s) - 1)
                 if s[i] > s[i - 1] and s[i] >= s[i + 1] and s[i] > 0.03 * s.max()]
        print(f"\nmodes ({col}, smoothed):", peaks)
    cw = h["n_chars"] / h["n_chars"].sum()
    print("char-weighted share below lo / between / above hi:",
          cw[h.bin_hi.fillna(99) <= thr["lo"] + 0.02].sum(), "(approx, bin granularity 0.02)")
    mixed = inv[inv["label"] == "mixed"]
    print("\nmixed volumes", len(mixed))
    print(pd.cut(mixed["print_scan_share"], np.arange(0, 1.0001, 0.1), include_lowest=True).value_counts().sort_index().to_string())
    runs = []
    for i in mixed["inv"]:
        g = scans[scans["inv"] == i].sort_values("scan_id")
        l = g[g["label"].isin(["hand", "print"])]["label"].tolist()
        ch_ = sum(1 for p, q in zip(l, l[1:]) if p != q)
        n = len(l)
        p = l.count("print") / n
        runs.append({"inv": i, "n_scans_hp": n, "print_share": p, "changes": ch_,
                     "expected_if_shuffled": 2 * p * (1 - p) * (n - 1)})
    runs = pd.DataFrame(runs)
    runs.to_csv(HERE / "mixed_runs.tsv", sep="\t", index=False, float_format="%.4g")
    if len(runs):
        print("changes vs expected-if-shuffled: median changes", runs.changes.median(),
              "median expected", runs.expected_if_shuffled.median(), "volumes with changes < 0.5*expected:",
              int((runs.changes < 0.5 * runs.expected_if_shuffled).sum()), "of", len(runs))
    inv["series"] = inv["ancestor_2"].str[:40] + " / " + inv["ancestor_3"].str[:50]
    top = inv.groupby("series")["chars"].sum().sort_values(ascending=False).head(15).index
    ser = pd.crosstab(inv[inv.series.isin(top)]["series"], inv["label"]).reindex(columns=["hand", "print", "mixed", "uncertain"], fill_value=0)
    ser["chars"] = inv.groupby("series")["chars"].sum()
    ser = ser.sort_values("chars", ascending=False)
    ser.to_csv(HERE / "series_labels.tsv", sep="\t")
    print("\n", ser.to_string())

    # ---- odd material
    labc = scans["hand_chars"] + scans["print_chars"]
    ms = scans[(labc >= 200) & (scans["hand_chars"] >= 0.2 * labc) & (scans["print_chars"] >= 0.2 * labc)]
    print("\nmixed scans (>=200 labelled chars, >=20% each):", len(ms), "in", ms.inv.nunique(), "inventories")
    ms.to_csv(HERE / "mixed_scans.tsv.gz", sep="\t", index=False, float_format="%.5g")
    odd = pd.read_csv(c / "odd_regions.tsv.gz", sep="\t")
    print("odd regions >=200 chars: density>1.5:", int((odd.density > 1.5).sum()), " <0.05:", int((odd.density < 0.05).sum()))
    odd["inv"] = odd["scan_id"].str[len("NL-HaNA_1.01.02_"):].str.rsplit("_", n=1).str[0]
    print("  in inventories:", odd.groupby(odd.density > 1.5).inv.nunique().to_dict())
    lo_w, hi_w = np.percentile(inv.median_w.dropna(), [1, 99])
    lo_h, hi_h = np.percentile(inv.median_h.dropna(), [1, 99])
    out = inv[(inv.median_w < lo_w) | (inv.median_w > hi_w) | (inv.median_h < lo_h) | (inv.median_h > hi_h)]
    print(f"layout outliers: w 1-99% [{lo_w:.0f},{hi_w:.0f}] h [{lo_h:.0f},{hi_h:.0f}] ->", len(out), "inventories;",
          out.label.value_counts().to_dict())
    out[["inv", "median_w", "median_h", "label", "silver", "chars"]].to_csv(HERE / "layout_outliers.tsv", sep="\t", index=False)
    odd.to_csv(HERE / "odd_regions.tsv", sep="\t", index=False, float_format="%.5g") if len(odd) < 5000 else \
        odd.sort_values("n_chars", ascending=False).head(5000).to_csv(HERE / "odd_regions.tsv", sep="\t", index=False, float_format="%.5g")

    # ---- resolution
    res = []
    base = Path("data/text_layer_v2_dev/15m-a")
    for i in DEV:
        p = base / "hand" / "NL-HaNA_1.01.02_text_layer_v3" / "scans" / f"{i}.scans.tsv.gz"
        res.append((f"1.01.02/{i}", p))
    for i in ["455", "456"]:
        res.append((f"1.10.94/{i}", base / "print" / "NL-HaNA_1.10.94_text_layer_v3" / "scans" / f"{i}.scans.tsv.gz"))
    rr = []
    for name, p in res:
        d = pd.read_csv(p, sep="\t", usecols=["page_w", "page_h", "n_lines_text"])
        rr.append({"inv": name, "median_page_w": d.page_w.median(), "median_page_h": d.page_h.median(),
                   "median_lines": d.n_lines_text.median()})
    rr = pd.DataFrame(rr)
    rr.to_csv(HERE / "resolution.tsv", sep="\t", index=False)
    print("\n", rr.to_string())
    print("page_w max/min:", rr.median_page_w.max() / rr.median_page_w.min())


if __name__ == "__main__":
    main()
