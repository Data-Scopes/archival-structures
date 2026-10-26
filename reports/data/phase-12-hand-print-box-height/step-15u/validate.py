"""Step 15u (phase 12): box-height volume mode. Fit on 76 volumes, one test on a fresh sample.

Subcommands:
  fit                         the 76 fit volumes from the read-only 15q caches -> fit_volumes.tsv, cut.json
                              (never overwritten; recomputed and asserted equal if it exists).
  prepass --set {test|diagnostic} --workers N
                              refuses to run without cut.json. Parses the PageXML tars (read only) of the set's
                              sample rows (reserve substitution for a missing tar -> substitutions.tsv) and writes
                              data/hand_print_geom/15u-a/1.01.02/{regions,pre,scans}/<inv>.*.
                              Launch through coreshare (see the 15u brief).
  evaluate --set {test|diagnostic}
                              box_height_mode with the frozen cut -> <set>_volumes.tsv, <set>_scans.tsv.gz
                              (test: stop_rule.json).
Silver labels are used only by `evaluate` for the test set.
"""
import argparse
import csv
import gzip
import json
import math
import os
import statistics
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from archival_structures.analysis.hand_print import (  # noqa: E402
    BoxHeightParams, box_height_mode, fit_box_height_cut, scan_box_heights, volume_box_height)

REPO = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
P10 = REPO / "reports" / "data" / "phase-10-hand-print-per-region"
PLAN = OUT.parent / "planning"
CACHE_Q = REPO / "data" / "hand_print_geom" / "15q-a"
CACHE = REPO / "data" / "hand_print_geom" / "15u-a"
SHARED = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml")
ARCH = "1.01.02"
DEV_HAND = ["3349", "3350", "3351", "3352", "3353", "3354", "11182", "11281"]
DEV_PRINT = ["3771", "3772", "3773", "3821", "3822", "3823"]
TRANSFER = ["455", "456"]
REGION_COLS = ["region_id", "scan_id", "scan_order", "n_lines", "n_chars", "h_med", "pitch_med", "hrel", "text_head"]
SCAN_COLS = ["scan_id", "scan_order", "page_w", "page_h", "n_lines_text"]
MIN_SCAN_CHARS = 200


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


def load_regions(base, archive, inv):
    rows = tsv_rows(base / archive / "regions" / f"{inv}.tsv.gz")
    for r in rows:
        for c in ("h_med", "pitch_med", "hrel"):
            r[c] = float(r[c]) if r[c] != "" else None
        for c in ("n_lines", "n_chars", "scan_order"):
            r[c] = int(r[c])
    return rows


# ---------------------------------------------------------------- fit
def fit_set():
    items = [("dev_eval", ARCH, i, "hand") for i in DEV_HAND] + [("dev_eval", ARCH, i, "print") for i in DEV_PRINT]
    items += [("transfer", "1.10.94", i, "print") for i in TRANSFER]
    for r in tsv_rows(P10 / "planning-15q" / "silver_sample.tsv"):
        items.append(("sample", ARCH, r["inv"], r["silver"]))
    return items


def cmd_fit(a):
    rows = []
    for st, arch, inv, cls in fit_set():
        v, n, k = volume_box_height(load_regions(CACHE_Q, arch, inv))
        rows.append({"set": st, "inv": inv, "cls": cls, "value": v, "n_chars": n, "n_regions": k, "archive": arch})
    assert len(rows) == 76 and len({(r["archive"], r["inv"]) for r in rows}) == 76
    hand = [r for r in rows if r["cls"] == "hand"]
    prt = [r for r in rows if r["cls"] == "print"]
    cut = fit_box_height_cut([r["value"] for r in hand], [r["value"] for r in prt])
    mp = max(prt, key=lambda r: r["value"])
    mh = min(hand, key=lambda r: r["value"])
    n_ok = sum(r["value"] <= cut for r in prt) + sum(r["value"] > cut for r in hand)
    res = {"cut": cut, "max_print": mp["value"], "max_print_inv": mp["inv"], "min_hand": mh["value"],
           "min_hand_inv": mh["inv"], "separable": mp["value"] < mh["value"], "n_print": len(prt),
           "n_hand": len(hand), "fit_agreement": f"{n_ok}/{len(rows)}",
           "params": BoxHeightParams(cut=cut).to_dict()}
    if (OUT / "cut.json").exists():
        assert json.load(open(OUT / "cut.json")) == res, "fit no longer reproduces the frozen cut.json"
    else:
        with open(OUT / "cut.json", "w") as f:
            json.dump(res, f, indent=1)
        os.chmod(OUT / "cut.json", 0o664)
    write_tsv(OUT / "fit_volumes.tsv", ["set", "inv", "cls", "value", "n_chars", "n_regions"], rows)
    # acceptance 2 comparison against the planning values
    ref = {(r["set"], r["inv"]): float(r["h_w"]) for r in tsv_rows(P10 / "review" / "h_by_volume.tsv")
           if r["set"] in ("dev_eval", "transfer", "sample")}
    diffs = [abs(r["value"] - ref[(r["set"], r["inv"])]) for r in rows]
    print(json.dumps(res, indent=1))
    print("n compared", len(diffs), "max |diff|", max(diffs), "455", rows[14]["value"], "456", rows[15]["value"])


# ---------------------------------------------------------------- prepass
def _prepass_one(args):
    inv, source = args
    from pagexml.parser import parse_pagexml_file
    from archival_structures.analysis.hand_print import region_features
    from archival_structures.text_layer.extract_v2 import extract_scan_v2
    from archival_structures.text_layer.io import iter_pagexml
    from archival_structures.text_layer.presets import PRESETS
    preset = PRESETS["sg_1.01.02_v3"]
    rows, srows, n_scans, n_fail, order = [], [], 0, 0, {}
    for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):
        n_scans += 1
        try:
            scan = parse_pagexml_file(member, pagexml_data=data)
        except Exception:
            n_fail += 1
            continue
        s_row, r_rows, l_rows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
        rows += l_rows
        srows.append({k: s_row.get(k) for k in ("scan_id", "page_w", "page_h", "n_lines_text")})
    feats = region_features(rows)
    texts = {}
    for r in rows:
        if r["text_raw"]:
            texts.setdefault(r["region_id"], []).append(r["text_raw"])
    for f in feats:
        order.setdefault(f["scan_id"], len(order))
        f["scan_order"] = order[f["scan_id"]]
        f["text_head"] = " / ".join(texts.get(f["region_id"], []))[:240]
    for i, s in enumerate(srows):
        s["scan_order"] = i
    base = CACHE / ARCH
    for sub in ("regions", "pre", "scans"):
        mkdir(base / sub)
    write_tsv(base / "regions" / f"{inv}.tsv.gz", REGION_COLS, feats)
    write_tsv(base / "scans" / f"{inv}.tsv.gz", SCAN_COLS, srows)
    with open(base / "pre" / f"{inv}.json", "w") as f:
        json.dump({"inv": inv, "source": str(source), "n_scans": n_scans, "n_parse_fail": n_fail,
                   "n_regions": len(feats), "n_line_rows": len(rows)}, f, indent=1)
    os.chmod(base / "pre" / f"{inv}.json", 0o664)
    return inv, n_scans, n_fail, len(feats)


def cmd_prepass(a):
    from archival_structures.text_layer.io import discover_inventories_v2
    from archival_structures.text_layer.presets import PRESETS
    if not (OUT / "cut.json").exists():
        sys.exit("cut.json missing: run `fit` first (part B must precede the prepass)")
    preset = PRESETS["sg_1.01.02_v3"]
    found = discover_inventories_v2(SHARED, preset.source_pattern)
    rows = [r for r in tsv_rows(PLAN / "test_sample.tsv") if r["set"] == a.set]
    used, subs, wanted = set(), [], []
    for r in rows:
        if r["role"] == "sample":
            used.add(r["inv"])
    for r in rows:
        if r["role"] != "sample":
            continue
        if r["inv"] in found:
            wanted.append(r["inv"])
            continue
        res = [x for x in rows if x["stratum"] == r["stratum"] and x["role"].startswith("reserve")
               and x["inv"] not in used]
        res.sort(key=lambda x: int(x["role"][len("reserve"):]))
        res = [x for x in res if x["inv"] in found]
        if not res:
            sys.exit(f"no reserve available for {r['inv']} in {r['stratum']}")
        used.add(res[0]["inv"])
        subs.append({"set": a.set, "stratum": r["stratum"], "missing": r["inv"], "replacement": res[0]["inv"]})
        wanted.append(res[0]["inv"])
    write_tsv(OUT / f"substitutions_{a.set}.tsv", ["set", "stratum", "missing", "replacement"], subs)
    print(f"{a.set}: {len(wanted)} volumes, {len(subs)} substitutions", flush=True)
    mkdir(CACHE)
    jobs = [(i, found[i]) for i in wanted]
    with ProcessPoolExecutor(max_workers=max(1, a.workers)) as ex:
        for inv, n, nf, nr in ex.map(_prepass_one, jobs):
            print(f"{inv}: {n} scans, {nf} parse failures, {nr} regions", flush=True)


# ---------------------------------------------------------------- evaluate
def wilson(k, n, z=1.959964):
    if n == 0:
        return (None, None)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(c - h, 4), round(c + h, 4))


def evaluated_invs(set_):
    """sample rows of the set after substitutions (the replacement inherits the stratum row's metadata
    of the reserve itself)."""
    rows = [r for r in tsv_rows(PLAN / "test_sample.tsv") if r["set"] == set_]
    byinv = {r["inv"]: r for r in rows}
    subs = {r["missing"]: r["replacement"] for r in tsv_rows(OUT / f"substitutions_{set_}.tsv")}
    out = []
    for r in rows:
        if r["role"] != "sample":
            continue
        inv = subs.get(r["inv"], r["inv"])
        m = dict(byinv[inv])
        m["role"] = byinv[inv]["role"]
        out.append(m)
    return out


def cmd_evaluate(a):
    cut = json.load(open(OUT / "cut.json"))
    params = BoxHeightParams.from_dict(cut["params"])
    lab15p_ = {}
    vols, scans = [], []
    for m in evaluated_invs(a.set):
        inv = m["inv"]
        try:
            feats = load_regions(CACHE, ARCH, inv)
            pre = json.load(open(CACHE / ARCH / "pre" / f"{inv}.json"))
            srows = tsv_rows(CACHE / ARCH / "scans" / f"{inv}.tsv.gz")
        except FileNotFoundError:
            vols.append({"inv": inv, "stratum": m["stratum"], "silver": m["silver"], "mode": "", "method": "missing",
                         "agree": False})
            continue
        bm = box_height_mode(feats, params)
        sc = scan_box_heights(feats, params.min_region_chars)
        sinfo = {s["scan_id"]: s for s in srows}
        order = {f["scan_id"]: f["scan_order"] for f in feats}
        sided = []
        for s in sc:
            side = ""
            if s["value"] is not None and s["n_chars"] >= MIN_SCAN_CHARS:
                side = "print" if s["value"] <= params.cut else "hand"
                sided.append(side)
            si = sinfo.get(s["scan_id"], {})
            scans.append({"inv": inv, "scan_id": s["scan_id"], "scan_order": order.get(s["scan_id"]),
                          "value": s["value"], "n_chars": s["n_chars"], "n_regions": s["n_regions"], "side": side,
                          "page_w": si.get("page_w"), "page_h": si.get("page_h")})
        minority = (sum(x != bm["mode"] for x in sided) / len(sided)) if sided else None
        pw = [float(s["page_w"]) for s in srows if s["page_w"] not in ("", None)]
        ph = [float(s["page_h"]) for s in srows if s["page_h"] not in ("", None)]
        vols.append({"inv": inv, "stratum": m["stratum"], "silver": m["silver"], "stratum_size": m["stratum_size"],
                     "role": m["role"], "mode": bm["mode"], "method": bm["method"], "value": bm["value"],
                     "margin": bm["margin"], "n_chars": bm["n_chars"], "n_regions": bm["n_regions"],
                     "n_scans": pre["n_scans"], "n_parse_fail": pre["n_parse_fail"],
                     "scan_minority_share": None if minority is None else round(minority, 4),
                     "median_page_w": statistics.median(pw) if pw else None,
                     "median_page_h": statistics.median(ph) if ph else None,
                     "label15p": m["label15p"],
                     "agree": (bm["mode"] == m["silver"] and bm["method"] == "box_height") if m["silver"] else ""})
    cols = ["inv", "stratum", "silver", "stratum_size", "role", "mode", "method", "value", "margin", "n_chars",
            "n_regions", "n_scans", "n_parse_fail", "scan_minority_share", "median_page_w", "median_page_h",
            "label15p", "agree"]
    write_tsv(OUT / f"{a.set}_volumes.tsv", cols, vols)
    write_tsv(OUT / f"{a.set}_scans.tsv.gz", ["inv", "scan_id", "scan_order", "value", "n_chars", "n_regions", "side",
                                              "page_w", "page_h"], scans)
    for v in vols:
        print({k: v.get(k) for k in ("inv", "stratum", "silver", "mode", "method", "value", "margin", "agree")})
    if a.set == "test":
        strata, agg = {}, {}
        for v in vols:
            strata.setdefault(v["stratum"], []).append(v)
        for name, vs in strata.items():
            k = sum(1 for v in vs if v["agree"] is True)
            n = len(vs)
            agg[name] = {"silver": vs[0]["silver"], "stratum_size": int(vs[0]["stratum_size"]), "n_sample": n,
                         "n_agree": k, "agreement": k / n, "wilson95": wilson(k, n)}
        out = {"cut": cut, "per_stratum": agg}
        for cls in ("print", "hand"):
            ss = [s for s in agg.values() if s["silver"] == cls]
            w = sum(s["stratum_size"] for s in ss)
            out[f"weighted_agreement_{cls}"] = sum(s["stratum_size"] * s["agreement"] for s in ss) / w
        out["n_volumes"] = len(vols)
        out["n_parse_fail_total"] = sum(int(v.get("n_parse_fail") or 0) for v in vols)
        out["pass"] = out["weighted_agreement_print"] >= 0.90 and out["weighted_agreement_hand"] >= 0.90
        with open(OUT / "stop_rule.json", "w") as f:
            json.dump(out, f, indent=1)
        os.chmod(OUT / "stop_rule.json", 0o664)
        print(json.dumps(out, indent=1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("fit")
    p = sub.add_parser("prepass")
    p.add_argument("--set", required=True, choices=["test", "diagnostic"])
    p.add_argument("--workers", type=int, default=1)
    p = sub.add_parser("evaluate")
    p.add_argument("--set", required=True, choices=["test", "diagnostic"])
    a = ap.parse_args()
    {"fit": cmd_fit, "prepass": cmd_prepass, "evaluate": cmd_evaluate}[a.cmd](a)


if __name__ == "__main__":
    main()
