"""Step 15q validation of the volume-mode classifier (phase 10). Subcommands:

  prepass   --archive {1.01.02|1.10.94} --src DIR [--inventories a,b,..] --workers N [--dump-lines INV,..]
            parse each inventory's PageXML (the fit_split_model pre-pass: iter_pagexml + parse_pagexml_file +
            extract_scan_v2) and cache region features in data/hand_print_geom/15q-a/<archive>/regions/<inv>.tsv.gz
            and pre/<inv>.json. Launch through coreshare (see the 15q brief).
  fit       dev scan values -> thresholds.json, dev_scans.tsv.gz, dev_volumes.tsv
  evaluate  --set {dev|transfer|sample|staged}  -> <set>_volumes.tsv, <set>_scans.tsv.gz (sample: stop_rule.json)

Inputs (repo-relative): data/text_layer_v3_full_inputs/{hand,print}/ (dev, transfer); the shared-data tars
(sample, staged; read only). Silver labels are used only by `evaluate` for the sample and staged sets.
"""
import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from archival_structures.analysis.hand_print import (  # noqa: E402
    VolumeModeParams, fit_thresholds, region_features, scan_label, scan_value, volume_mode_from_features)

REPO = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
PLAN = OUT.parent / "planning-15q"
P15 = OUT.parent / "step-15p"
CACHE = REPO / "data" / "hand_print_geom" / "15q-a"
SHARED = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml")
DEV_HAND = ["3349", "3350", "3351", "3352", "3353", "3354", "11182", "11281"]
DEV_PRINT = ["3771", "3772", "3773", "3821", "3822", "3823"]
TRANSFER = ["455", "456"]
REGION_COLS = ["region_id", "scan_id", "scan_order", "n_lines", "n_chars", "h_med", "pitch_med", "hrel", "text_head"]
# re-plan reference: share of print among labelled scans incl. uncertain in the denominator
REF_SHARE = {"11182": 0.000, "11281": 0.132, "3349": 0.050, "3350": 0.024, "3351": 0.185, "3352": 0.014,
             "3353": 0.011, "3354": 0.157, "3771": 0.946, "3772": 0.930, "3773": 0.962, "3821": 0.979,
             "3822": 0.910, "3823": 0.929}


def tsv_rows(path):
    with gzip.open(path, "rt", newline="") if str(path).endswith(".gz") else open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_tsv(path, cols, rows):
    op = gzip.open(path, "wt", newline="") if str(path).endswith(".gz") else open(path, "w", newline="")
    with op as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(cols)
        for r in rows:
            w.writerow([("" if r.get(c) is None else r.get(c)) for c in cols])
    os.chmod(path, 0o664)


def mkdir(p):
    Path(p).mkdir(parents=True, exist_ok=True)
    os.chmod(p, 0o775)


# ---------------------------------------------------------------- prepass
def _prepass_one(args):
    archive, inv, source, dump = args
    from pagexml.parser import parse_pagexml_file
    from archival_structures.text_layer.extract_v2 import extract_scan_v2
    from archival_structures.text_layer.io import iter_pagexml
    from archival_structures.text_layer.presets import PRESETS
    preset = PRESETS[f"sg_{archive}_v3"]
    rows, n_scans, n_fail, order = [], 0, 0, {}
    for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):
        n_scans += 1
        try:
            scan = parse_pagexml_file(member, pagexml_data=data)
        except Exception:
            n_fail += 1
            continue
        s_row, r_rows, l_rows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
        rows += l_rows
    feats = region_features(rows)
    texts = {}
    for r in rows:
        if r["text_raw"]:
            texts.setdefault(r["region_id"], []).append(r["text_raw"])
    for f in feats:
        order.setdefault(f["scan_id"], len(order))
        f["scan_order"] = order[f["scan_id"]]
        f["text_head"] = " / ".join(texts.get(f["region_id"], []))[:240]
    base = CACHE / archive
    for sub in ("regions", "pre", "lines"):
        mkdir(base / sub)
    write_tsv(base / "regions" / f"{inv}.tsv.gz", REGION_COLS, feats)
    with open(base / "pre" / f"{inv}.json", "w") as f:
        json.dump({"inv": inv, "source": str(source), "n_scans": n_scans, "n_parse_fail": n_fail,
                   "n_regions": len(feats), "n_line_rows": len(rows)}, f, indent=1)
    os.chmod(base / "pre" / f"{inv}.json", 0o664)
    if dump:
        write_tsv(base / "lines" / f"{inv}.lines5.tsv.gz", ["region_id", "line_idx", "h", "baseline", "text_raw"], rows)
    return inv, n_scans, n_fail, len(feats)


def cmd_prepass(a):
    from archival_structures.text_layer.io import discover_inventories_v2
    from archival_structures.text_layer.presets import PRESETS
    preset = PRESETS[f"sg_{a.archive}_v3"]
    found = discover_inventories_v2(a.src, preset.source_pattern)
    wanted = a.inventories.split(",") if a.inventories else list(found)
    missing = [i for i in wanted if i not in found]
    if missing:
        sys.exit(f"missing sources for {missing}")
    dump = set(a.dump_lines.split(",")) if a.dump_lines else set()
    jobs = [(a.archive, i, found[i], i in dump) for i in wanted]
    mkdir(CACHE)
    with ProcessPoolExecutor(max_workers=max(1, a.workers)) as ex:
        for inv, n, nf, nr in ex.map(_prepass_one, jobs):
            print(f"{a.archive} {inv}: {n} scans, {nf} parse failures, {nr} regions", flush=True)


# ---------------------------------------------------------------- helpers on caches
def load_regions(archive, inv):
    rows = tsv_rows(CACHE / archive / "regions" / f"{inv}.tsv.gz")
    for r in rows:
        for c in ("h_med", "pitch_med", "hrel"):
            r[c] = float(r[c]) if r[c] != "" else None
        for c in ("n_lines", "n_chars", "scan_order"):
            r[c] = int(r[c])
    return rows


def scan_values(feats, min_chars=200):
    by = {}
    for f in feats:
        by.setdefault(f["scan_id"], []).append(f)
    out = []
    for sid, fs in by.items():
        v, n = scan_value(fs)
        out.append({"scan_id": sid, "scan_order": fs[0]["scan_order"], "value": v, "n_chars": n,
                    "labelled": v is not None and n >= min_chars})
    return out


def load_params():
    t = json.load(open(OUT / "thresholds.json"))
    return VolumeModeParams(t["t_lo"], t["t_hi"]), t


# ---------------------------------------------------------------- fit
def cmd_fit(a):
    rows, vals = [], {"hand": [], "print": []}
    for cls, invs in (("hand", DEV_HAND), ("print", DEV_PRINT)):
        for inv in invs:
            for s in scan_values(load_regions("1.01.02", inv)):
                rows.append({"inv": inv, "cls": cls, **s})
                if s["labelled"]:
                    vals[cls].append(s["value"])
    t_print = float(np.quantile(vals["print"], 0.95))
    t_hand = float(np.quantile(vals["hand"], 0.05))
    lo, hi = fit_thresholds(vals["hand"], vals["print"])
    res = {"t_print_unrounded": t_print, "t_hand_unrounded": t_hand, "t_lo": lo, "t_hi": hi,
           "n_labelled_hand": len(vals["hand"]), "n_labelled_print": len(vals["print"]),
           "n_labelled": len(vals["hand"]) + len(vals["print"]), "min_scan_chars": 200, "min_lines": 3,
           "print_share": 0.5, "dev_hand": DEV_HAND, "dev_print": DEV_PRINT}
    if (OUT / "thresholds.json").exists():   # frozen after part A: verify, never overwrite
        assert json.load(open(OUT / "thresholds.json")) == res, "fit no longer reproduces the frozen thresholds.json"
    else:
        with open(OUT / "thresholds.json", "w") as f:
            json.dump(res, f, indent=1)
        os.chmod(OUT / "thresholds.json", 0o664)
    p = VolumeModeParams(lo, hi)
    for r in rows:
        r["label"] = scan_label(r["value"], r["n_chars"], p)
    write_tsv(OUT / "dev_scans.tsv.gz", ["inv", "cls", "scan_id", "scan_order", "value", "n_chars", "labelled", "label"], rows)
    vol = []
    for cls, invs in (("hand", DEV_HAND), ("print", DEV_PRINT)):
        for inv in invs:
            sc = [r for r in rows if r["inv"] == inv and r["labelled"]]
            n = len(sc)
            npr = sum(r["label"] == "print" for r in sc)
            nh = sum(r["label"] == "hand" for r in sc)
            nu = n - npr - nh
            share_all = npr / n if n else None
            vm = volume_mode_from_features(load_regions("1.01.02", inv), p)
            vol.append({"inv": inv, "cls": cls, "n_labelled": n, "n_print": npr, "n_hand": nh, "n_uncertain": nu,
                        "share_incl_uncertain": None if share_all is None else round(share_all, 4),
                        "ref_share": REF_SHARE[inv],
                        "diff": None if share_all is None else round(share_all - REF_SHARE[inv], 4),
                        "print_scan_share": vm["print_scan_share"], "mode": vm["mode"], "method": vm["method"],
                        "class_ok": vm["mode"] == cls})
    write_tsv(OUT / "dev_volumes.tsv", list(vol[0]), vol)
    print(json.dumps(res, indent=1))
    for v in vol:
        print(v)


# ---------------------------------------------------------------- evaluate
def wilson(k, n, z=1.959964):
    if n == 0:
        return (None, None)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(c - h, 4), round(c + h, 4))


def cmd_evaluate(a):
    params, t = load_params()
    meta = {}
    if a.set == "dev":
        items = [("1.01.02", i, {"silver": c}) for c, l in (("hand", DEV_HAND), ("print", DEV_PRINT)) for i in l]
    elif a.set == "transfer":
        items = [("1.10.94", i, {}) for i in TRANSFER]
    elif a.set == "sample":
        items = [("1.01.02", r["inv"], r) for r in tsv_rows(PLAN / "silver_sample.tsv")]
    else:
        items = [("1.01.02", r["inv"], r) for r in tsv_rows(P15 / "staging.tsv")]
    lab15p = {r["inv"]: r["label"] for r in tsv_rows(P15 / "inventories.tsv")}
    vols, scans = [], []
    for arch, inv, m in items:
        try:
            feats = load_regions(arch, inv)
            pre = json.load(open(CACHE / arch / "pre" / f"{inv}.json"))
        except FileNotFoundError:
            vols.append({"inv": inv, "mode": "", "method": "missing"})
            continue
        vm = volume_mode_from_features(feats, params)
        lab = [s for s in vm["scans"] if s["label"] != "unlabelled"]
        near = sum(abs(s["value"] - (params.t_lo + params.t_hi) / 2) <= 0.05 for s in lab)
        n_unl = vm["n_scans"] - vm["n_scans_labelled"]
        share_all = vm["n_scans_print"] / vm["n_scans_labelled"] if vm["n_scans_labelled"] else None
        row = {"inv": inv, "mode": vm["mode"], "method": vm["method"], "print_scan_share": vm["print_scan_share"],
               "share_incl_uncertain": share_all, "n_scans": pre["n_scans"], "n_parse_fail": pre["n_parse_fail"],
               "n_scans_labelled": vm["n_scans_labelled"], "n_print": vm["n_scans_print"], "n_hand": vm["n_scans_hand"],
               "n_uncertain": vm["n_scans_uncertain"], "n_unlabelled": n_unl,
               "frac_near_cut": None if not lab else round(near / len(lab), 4),
               "silver": m.get("silver", ""), "stratum": m.get("stratum", ""), "stratum_size": m.get("stratum_size", ""),
               "label15p": lab15p.get(inv, m.get("label", ""))}
        row["agree"] = (row["mode"] == row["silver"]) if (row["silver"] and vm["method"] == "geometry") else (
            False if row["silver"] else "")
        vols.append(row)
        for s in vm["scans"]:
            scans.append({"inv": inv, **s})
    cols = ["inv", "mode", "method", "print_scan_share", "share_incl_uncertain", "n_scans", "n_parse_fail",
            "n_scans_labelled", "n_print", "n_hand", "n_uncertain", "n_unlabelled", "frac_near_cut", "silver",
            "stratum", "stratum_size", "label15p", "agree"]
    tag = "dev_eval" if a.set == "dev" else a.set    # fit owns dev_scans.tsv.gz / dev_volumes.tsv
    write_tsv(OUT / f"{tag}_volumes.tsv", cols, vols)
    write_tsv(OUT / f"{tag}_scans.tsv.gz", ["inv", "scan_id", "n_regions_feat", "n_chars", "value", "label"], scans)
    for v in vols:
        print({k: v.get(k) for k in ("inv", "mode", "method", "print_scan_share", "silver", "stratum", "agree")})
    if a.set == "sample":
        strata, agg = {}, {}
        for v in vols:
            strata.setdefault(v["stratum"], []).append(v)
        for name, vs in strata.items():
            k = sum(1 for v in vs if v["agree"] is True)
            n = len(vs)
            agg[name] = {"silver": vs[0]["silver"], "stratum_size": int(vs[0]["stratum_size"]), "n_sample": n,
                         "n_agree": k, "agreement": k / n, "wilson95": wilson(k, n)}
        out = {"thresholds": t, "per_stratum": agg}
        for cls in ("print", "hand"):
            ss = [s for s in agg.values() if s["silver"] == cls]
            w = sum(s["stratum_size"] for s in ss)
            out[f"weighted_agreement_{cls}"] = sum(s["stratum_size"] * s["agreement"] for s in ss) / w
        out["pass"] = out["weighted_agreement_print"] >= 0.90 and out["weighted_agreement_hand"] >= 0.90
        with open(OUT / "stop_rule.json", "w") as f:
            json.dump(out, f, indent=1)
        os.chmod(OUT / "stop_rule.json", 0o664)
        print(json.dumps(out, indent=1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepass")
    p.add_argument("--archive", required=True, choices=["1.01.02", "1.10.94"])
    p.add_argument("--src", required=True)
    p.add_argument("--inventories")
    p.add_argument("--dump-lines")
    p.add_argument("--workers", type=int, default=1)
    sub.add_parser("fit")
    p = sub.add_parser("evaluate")
    p.add_argument("--set", required=True, choices=["dev", "transfer", "sample", "staged"])
    a = ap.parse_args()
    {"prepass": cmd_prepass, "fit": cmd_fit, "evaluate": cmd_evaluate}[a.cmd](a)


if __name__ == "__main__":
    main()
