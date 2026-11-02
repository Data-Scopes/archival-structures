"""Step 23c: VOC folio-map options (phase 17). Single process. Run from the AS root (stages write only into this directory):

    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23c/eval_map.py --stage sg --head-dir SCRATCH
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23c/eval_map.py --stage dev
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23c/eval_map.py --stage test     # ONCE, needs step-23a/gold/folio_gold.done
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23c/eval_map.py --stage context  # dev fixes/breaks (+ test misses if test ran)

--head-dir: an empty-or-existing directory in which `git show HEAD:archival_structures/analysis/folio_map.py` is saved
as folio_map_head.py (the pre-23c module, loaded with importlib).

Reads (read only): data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5 (dev; link to T7_Data),
data/text_layer_v2_dev/19e-a/1.01.02/... (sg check), phase-16 planning/gm_anchors.tsv + gm_split.tsv, step-22b/check_result.tsv,
step-22a/sides.tsv. Test stage only: data/text_layer_voc_23a/... and step-23a/gold/folio_gold.tsv.
"""
import argparse
import csv
import dataclasses
import datetime
import importlib.util
import itertools
import json
import math
import random
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np

sys.path.insert(0, ".")
from archival_structures.analysis import folio_map as new_fm  # noqa: E402
from archival_structures.analysis.folio_map import FOLIO_MAP_V2, FolioMapParams, build_folio_map  # noqa: E402
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs  # noqa: E402
from archival_structures.analysis.page_numbering import interpolate_folio_to_scan  # noqa: E402

DEV_LAYER = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
TEST_LAYER = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
SG_LAYER = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
P16 = Path("reports/data/phase-16-voc-generalisation")
PLAN = P16 / "planning"
SIDES = P16 / "step-22a/sides.tsv"
CHECK = P16 / "step-22b/check_result.tsv"
A23 = Path("reports/data/phase-17-voc-rules-v1/step-23a")
GOLD_DIR = A23 / "gold"
OUT = Path(__file__).parent
TAGS = ("page-number",)
SEED = 20261101
GRID = list(itertools.product((False, True), (None, "first", "last"), (None, 3.0), (False, True), (6, 30)))


def cell_params(c):
    pick, tie, clean, pieces, ext = c
    return FolioMapParams(pick_by_chain=pick, tie_keep=tie, clean_tol=clean, pieces=pieces, extrapolate=ext)


def cname(c):
    pick, tie, clean, pieces, ext = c
    return f"p{int(pick)}_t{tie or 0}_c{int(clean or 0)}_g{int(pieces)}_e{ext}"


BASE = (False, None, None, False, 6)
CNAMES = {c: cname(c) for c in GRID}


def n_nondefault(c):
    return sum([c[0], c[1] is not None, c[2] is not None, c[3], c[4] != 6])


def wilson(k, n, z=1.96):
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def mcnemar(b, c):
    n = b + c
    if n == 0:
        return 1.0
    tail = sum(math.comb(n, k) for k in range(0, min(b, c) + 1))
    return min(1.0, 2 * tail / 2 ** n)


def loo(chain):
    ok = tot = 0
    for i in range(1, len(chain) - 1):
        s, _, _ = interpolate_folio_to_scan(chain[i, 1], np.delete(chain, i, axis=0))
        tot += 1
        ok += math.floor(s + 0.5) == int(chain[i, 0])
    return ok, tot


def read_tsv(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_tsv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        w.writerows(rows)


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def load_volume(layer, inv, tags):
    """-> (VolumeInputs, excluded sides) or None. Exclude = sides up to the SG front_index_run end, or flagged."""
    try:
        v = read_volume_inputs(layer, inv, number_tags=tags)
    except FileNotFoundError:
        return None
    run = front_index_run(v.flags)
    excl = {s for i, s in enumerate(v.sides) if (run and i <= run[1]) or v.flags[i]}
    return v, excl


def side_layout(v):
    c = Counter(p for _, p in v.sides)
    n = sum(c.values())
    return "openings" if n and (c["verso"] + c["recto"]) >= 0.5 * n else "single"


def dev_side_layouts():
    out = {}
    for r in read_tsv(SIDES):
        s, v, rc = int(r["single"]), int(r["verso"]), int(r["recto"])
        out[r["inv"]] = "openings" if (v + rc) >= 0.5 * (s + v + rc) else "single"
    return out


def locate_row(m, v, folio):
    t = m.locate(folio)
    if t is None:
        return "", "none"
    return t.front[0], t.method


def hit(v, gold, pred):
    if not pred:
        return 0, 0
    pos = v.scan_ids.index(pred)
    leaf = {v.scan_ids[pos]} | ({v.scan_ids[pos + 1]} if pos + 1 < len(v.scan_ids) else set())
    return int(gold in leaf), int(gold == pred)


# --------------------------------------------------------------------------------------------------- sg
def stage_sg(head_dir):
    head_dir = Path(head_dir)
    head_path = head_dir / "folio_map_head.py"
    src = subprocess.run(["git", "show", "HEAD:archival_structures/analysis/folio_map.py"], capture_output=True,
                         text=True, check=True).stdout
    head_path.write_text(src)
    spec = importlib.util.spec_from_file_location("folio_map_head", head_path)
    old = importlib.util.module_from_spec(spec)
    sys.modules["folio_map_head"] = old
    spec.loader.exec_module(old)
    ld = load_volume(SG_LAYER, "3444", ())
    assert ld is not None, "SG volume 3444 missing"
    v, excl = ld
    res = {"inv": "3444", "n_sides": len(v.sides), "head_sha_source": "git show HEAD (uncommitted work excluded)"}
    ndiff = 0
    for name, newp, oldp in (("FolioMapParams()", new_fm.FolioMapParams(), old.FolioMapParams()),
                             ("FOLIO_MAP_V2", new_fm.FOLIO_MAP_V2, old.FOLIO_MAP_V2)):
        a = new_fm.build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=newp)
        b = old.build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=oldp)
        diffs = []
        if a.mode != b.mode or a.n_anchors != b.n_anchors or a.n_dup_dropped != b.n_dup_dropped \
                or a.chain_cut != b.chain_cut or not np.array_equal(a.chain, b.chain):
            diffs.append("map")
        for n in range(0, 1500):
            ta, tb = a.locate(n), b.locate(n)
            if (ta is None) != (tb is None) or (ta is not None and dataclasses.astuple(ta) != dataclasses.astuple(tb)):
                diffs.append(n)
        res[name] = dict(mode=a.mode, chain_len=len(a.chain), n_locate=1500, n_diff=len(diffs), diffs=diffs[:20])
        ndiff += len(diffs)
    res["total_differences"] = ndiff
    res["time_utc"] = now_utc()
    json.dump(res, open(OUT / "sg_check.json", "w"), indent=1)
    print(json.dumps(res, indent=1))
    if ndiff:
        sys.exit("sg_check: differences -> stop the step")


# --------------------------------------------------------------------------------------------------- dev
def corrected_anchors():
    corr = {}
    for r in read_tsv(CHECK):
        if r["correct_scan"]:
            corr[(r["inv"], r["folio"], int(r["gold_scan"].rsplit("_", 1)[1]))] = int(r["correct_scan"].rsplit("_", 1)[1])
    rows, n_corr = [], 0
    for r in read_tsv(PLAN / "gm_anchors.tsv"):
        r = dict(r)
        r["scan_orig"] = r["scan"]
        k = (r["inv"], r["folio"], int(r["scan"]))
        r["corrected"] = "0"
        if k in corr:
            r["scan"] = str(corr[k])
            r["corrected"] = "1"
            n_corr += 1
        rows.append(r)
    return rows, n_corr, len(corr)


def stage_dev():
    t0 = time.time()
    rows, n_corr, n_corr_expected = corrected_anchors()
    split = {r["inv"]: r["split"] for r in read_tsv(PLAN / "gm_split.tsv")}
    write_tsv(OUT / "dev_anchors.tsv", ["inv", "gm_id", "kind", "folio", "scan", "layout", "split", "scan_orig", "corrected"],
              [[r[k] for k in ("inv", "gm_id", "kind", "folio", "scan", "layout", "split", "scan_orig", "corrected")] for r in rows])
    print(f"anchors {len(rows)}, corrected {n_corr} (of {n_corr_expected} corrections)")
    slay = dev_side_layouts()
    by_inv = {}
    for r in rows:
        by_inv.setdefault(r["inv"], []).append(r)
    stats = {c: dict(leaf=0, strict=0, n=0, lay={}, meth={}, loo_ok=0, loo_tot=0) for c in GRID}
    unc = {"dev": [0, 0], "test": [0, 0]}  # BASE on the uncorrected rows per phase-16 split
    unc_hits = {}
    detail, dropped, nomap = [], 0, 0
    for k, inv in enumerate(sorted(by_inv), 1):
        ld = load_volume(DEV_LAYER, inv, TAGS)
        rs = by_inv[inv]
        if ld is None:
            nomap += len(rs)
            for r in rs:
                for c in GRID:
                    pass
            ld = None
        maps = {}
        if ld is not None:
            v, excl = ld
            ids = set(v.scan_ids)
            for c in GRID:
                maps[c] = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=cell_params(c))
                ok, tot = loo(maps[c].chain) if len(maps[c].chain) > 2 else (0, 0)
                stats[c]["loo_ok"] += ok
                stats[c]["loo_tot"] += tot
        for r in rs:
            gold = f"NL-HaNA_1.04.02_{inv}_{int(r['scan']):04d}"
            gold0 = f"NL-HaNA_1.04.02_{inv}_{int(r['scan_orig']):04d}"
            sp = r["split"]
            lay = slay.get(inv, "?")
            if ld is not None and gold not in ids:
                dropped += 1
                continue
            drow = dict(inv=inv, split=sp, gm_layout=r["layout"], side_layout=lay, kind=r["kind"], folio=int(r["folio"]),
                        gold_scan=gold)
            for c in GRID:
                if ld is None:
                    pred, meth = "", "no_pages"
                    lf = st = 0
                else:
                    pred, meth = locate_row(maps[c], v, int(r["folio"]))
                    lf, st = hit(v, gold, pred)
                s = stats[c]
                s["n"] += 1
                s["leaf"] += lf
                s["strict"] += st
                a = s["lay"].setdefault(lay, [0, 0, 0])
                a[0] += 1; a[1] += lf; a[2] += st
                m = s["meth"].setdefault(meth, [0, 0, 0])
                m[0] += 1; m[1] += lf; m[2] += st
                drow[f"pred_{CNAMES[c]}"] = pred
                drow[f"meth_{CNAMES[c]}"] = meth
                drow[f"leaf_{CNAMES[c]}"] = lf
                drow[f"strict_{CNAMES[c]}"] = st
            # BASE on the uncorrected rows (22b reproduction)
            if gold0 in ids if ld is not None else True:
                if ld is not None:
                    pred0, _ = locate_row(maps[BASE], v, int(r["folio"]))
                    lf0, _ = hit(v, gold0, pred0)
                else:
                    lf0 = 0
                unc[split[inv]][0] += lf0
                unc[split[inv]][1] += 1
            detail.append(drow)
        if k % 100 == 0:
            print(k, "volumes", round(time.time() - t0), "s", flush=True)
    print(f"scored {len(detail)}, dropped (gold scan not in layer) {dropped}, volume missing anchors {nomap}")
    print("BASE on uncorrected rows:", {k: tuple(v) for k, v in unc.items()})
    layouts = ["openings", "single"]
    methods = sorted({m for c in GRID for m in stats[c]["meth"]})
    header = (["cell", "pick_by_chain", "tie_keep", "clean_tol", "pieces", "extrapolate", "n_nondefault", "n", "leaf",
               "leaf_share", "strict", "strict_share"]
              + [f"{lab}_{x}" for lab in layouts for x in ("n", "leaf_share", "strict_share")]
              + [f"meth_{m}_{x}" for m in methods for x in ("n", "leaf_share")] + ["loo_ok", "loo_tot", "loo_share"])
    out = []
    for c in GRID:
        s = stats[c]
        row = [CNAMES[c], int(c[0]), c[1] or "", c[2] or "", int(c[3]), c[4], n_nondefault(c), s["n"], s["leaf"],
               f"{s['leaf'] / s['n']:.4f}", s["strict"], f"{s['strict'] / s['n']:.4f}"]
        for lab in layouts:
            n, lf, st = s["lay"].get(lab, [0, 0, 0])
            row += [n, f"{lf / n:.4f}" if n else "nan", f"{st / n:.4f}" if n else "nan"]
        for m in methods:
            n, lf, st = s["meth"].get(m, [0, 0, 0])
            row += [n, f"{lf / n:.4f}" if n else "nan"]
        row += [s["loo_ok"], s["loo_tot"], f"{s['loo_ok'] / s['loo_tot']:.4f}" if s["loo_tot"] else "nan"]
        out.append(row)
    write_tsv(OUT / "dev_grid.tsv", header, out)
    write_tsv(OUT / "dev_detail.tsv", list(detail[0]), [list(r.values()) for r in detail])
    best = max(stats[c]["leaf"] / stats[c]["n"] for c in GRID)
    cand = [c for c in GRID if stats[c]["leaf"] / stats[c]["n"] >= best - 0.002]
    cand.sort(key=lambda c: (n_nondefault(c), c[4] != 6, -stats[c]["leaf"]))
    sel = cand[0]
    s = stats[sel]
    json.dump(dict(cell=dict(pick_by_chain=sel[0], tie_keep=sel[1], clean_tol=sel[2], pieces=sel[3], extrapolate=sel[4]),
                   name=CNAMES[sel], dev_n=s["n"], dev_leaf=s["leaf"], dev_strict=s["strict"],
                   dev_leaf_share=s["leaf"] / s["n"], dev_strict_share=s["strict"] / s["n"],
                   dev_loo=[s["loo_ok"], s["loo_tot"], s["loo_ok"] / s["loo_tot"]],
                   base=dict(leaf=stats[BASE]["leaf"], strict=stats[BASE]["strict"],
                             loo=[stats[BASE]["loo_ok"], stats[BASE]["loo_tot"]]),
                   best_dev_leaf_share=best, tie_candidates=[CNAMES[c] for c in cand],
                   base_uncorrected_phase16_splits={k: dict(hits=v[0], n=v[1]) for k, v in unc.items()},
                   n_corrected=n_corr, n_dropped=dropped, seconds=round(time.time() - t0, 1), time_utc=now_utc()),
              open(OUT / "select.json", "w"), indent=1)
    top = sorted(GRID, key=lambda c: (-stats[c]["leaf"], n_nondefault(c)))[:5]
    print("top5:", [(CNAMES[c], stats[c]["leaf"]) for c in top])
    print("selected", CNAMES[sel], stats[sel]["leaf"], stats[sel]["strict"], "of", stats[sel]["n"],
          "loo", stats[sel]["loo_ok"], stats[sel]["loo_tot"], "| BASE", stats[BASE]["leaf"], stats[BASE]["strict"],
          "loo", stats[BASE]["loo_ok"], stats[BASE]["loo_tot"])


# --------------------------------------------------------------------------------------------------- test
def sel_cell():
    s = json.load(open(OUT / "select.json"))["cell"]
    return (s["pick_by_chain"], s["tie_keep"], s["clean_tol"], s["pieces"], s["extrapolate"])


def test_guard():
    """True when the test stage may run; False (clean stop) when the gold is not there yet."""
    if not (OUT / "select.json").exists():
        sys.exit("select.json missing: run the dev stage first")
    if (OUT / "test_done.json").exists():
        sys.exit("test_done.json exists: the test runs once")
    if (GOLD_DIR / "folio_gold.done").exists():
        return True
    if (A23 / "folio_verdicts.json").exists():
        print("folio_verdicts.json exists, folio_gold.done does not: running 23a's summarise_folio.py")
        subprocess.run([sys.executable, "-I", str(A23 / "summarise_folio.py")], check=True)
        if (GOLD_DIR / "folio_gold.done").exists():
            return True
    print("waiting on UA-061: step-23a/gold/folio_gold.done does not exist; test stage not run")
    return False


def stage_test():
    if not test_guard():
        return
    t0 = time.time()
    sel = sel_cell()
    json.dump(dict(status="started", selected=CNAMES[sel], time_utc=now_utc()), open(OUT / "test_done.json", "w"), indent=1)
    gold = read_tsv(GOLD_DIR / "folio_gold.tsv")
    anchors = []
    for g in gold:
        if g["status"] != "number":
            continue
        for kind, col in (("main", "number"), ("left", "left_number")):
            if g[col].strip().isdigit():
                anchors.append(dict(item_id=g["item_id"], inv=g["inv"], scan_id=g["scan_id"], kind=kind,
                                    folio=int(g[col]), layered=int(bool(g["beneath_number"].strip())), note=g["note"]))
    cells = [BASE] if sel == BASE else [BASE, sel]
    cache, det = {}, []
    for a in anchors:
        inv = a["inv"]
        if inv not in cache:
            ld = load_volume(TEST_LAYER, inv, TAGS)
            cache[inv] = None if ld is None else (ld, {c: build_folio_map(ld[0].scan_ids, ld[0].sides, ld[0].numbers,
                                                                          exclude=ld[1], params=cell_params(c)) for c in cells})
        for c in cells:
            base = dict(item_id=a["item_id"], inv=inv, kind=a["kind"], layered=a["layered"], folio=a["folio"],
                        gold_scan=a["scan_id"], cell=CNAMES[c])
            if cache[inv] is None:
                det.append({**base, "side_layout": "?", "pred_scan": "", "method": "no_pages", "leaf": 0, "strict": 0})
                continue
            (v, _), maps = cache[inv]
            pred, meth = locate_row(maps[c], v, a["folio"])
            lf, st = hit(v, a["scan_id"], pred) if a["scan_id"] in v.scan_ids else (0, 0)
            det.append({**base, "side_layout": side_layout(v), "pred_scan": pred, "method": meth, "leaf": lf, "strict": st})
    write_tsv(OUT / "test_eval.tsv", list(det[0]), [list(r.values()) for r in det])
    summ = {}
    for c in cells:
        d = [r for r in det if r["cell"] == CNAMES[c]]
        n, k, s = len(d), sum(r["leaf"] for r in d), sum(r["strict"] for r in d)
        e = dict(n=n, leaf=k, leaf_share=k / n, wilson=wilson(k, n), strict=s, strict_share=s / n,
                 strict_wilson=wilson(s, n), bar_f_pass=k / n >= 0.95)
        for key in ("side_layout", "kind", "method", "layered"):
            e[key] = {}
            for lab in sorted({str(r[key]) for r in d}):
                dd = [r for r in d if str(r[key]) == lab]
                kk = sum(r["leaf"] for r in dd)
                e[key][lab] = dict(n=len(dd), leaf=kk, strict=sum(r["strict"] for r in dd), wilson=wilson(kk, len(dd)))
        summ[CNAMES[c]] = e
    bl = {(r["item_id"], r["kind"]): r["leaf"] for r in det if r["cell"] == CNAMES[BASE]}
    sl = {(r["item_id"], r["kind"]): r["leaf"] for r in det if r["cell"] == CNAMES[sel]}
    b = sum(bl[k] and not sl[k] for k in bl)
    cc = sum(sl[k] and not bl[k] for k in bl)
    out = dict(status="done", selected=CNAMES[sel], n_anchors=len(anchors), cells=summ,
               mcnemar=dict(b_base_only=b, c_sel_only=cc, p=mcnemar(b, cc)),
               verdict="bar F passed" if summ[CNAMES[sel]]["bar_f_pass"] else "bar F missed",
               seconds=round(time.time() - t0, 1), time_utc=now_utc())
    json.dump(out, open(OUT / "test_done.json", "w"), indent=1)
    print(json.dumps({k: {a: b_ for a, b_ in v.items() if a in ("n", "leaf", "leaf_share", "wilson", "strict")}
                      for k, v in summ.items()}, indent=1), out["mcnemar"], out["verdict"])


# --------------------------------------------------------------------------------------------------- context
def dump_context(path, items, layer, tags, slay_fn):
    """items: dicts with inv, folio, gold_scan, pred_scan(optional), cell params `sel`; page-number texts around."""
    L = []
    cache = {}
    for i, r in enumerate(items, 1):
        inv = r["inv"]
        if inv not in cache:
            cache[inv] = load_volume(layer, inv, tags)
        v, excl = cache[inv]
        gs = int(r["gold_scan"].rsplit("_", 1)[1])
        preds = {k: r[k] for k in r if k.startswith("pred_") and r[k]}
        pn = {k: int(x.rsplit("_", 1)[1]) for k, x in preds.items()}
        L.append(f"#{i} [{r.get('tag', '')}] inv {inv} {r.get('side_layout', '')} folio {r['folio']} gold {gs} "
                 f"{' '.join(f'{k} {x}' for k, x in pn.items())} (n_scans {len(v.scan_ids)})")
        for c, mp in r.get("maps", {}).items():
            ch = mp.chain
            L.append(f"   map {c}: mode {mp.mode} chain_len {len(ch)} first/last "
                     f"{ch[0].tolist() if len(ch) else '-'} {ch[-1].tolist() if len(ch) else '-'} pieces {len(mp.pieces)}")
        near = set(range(gs - 3, gs + 4))
        for x in pn.values():
            near |= {x - 1, x, x + 1}
        for sid, page in v.sides:
            sn = int(sid.rsplit("_", 1)[1])
            if sn in near:
                mark = "*" if sn == gs else " "
                pm = "".join(f" <{k[5:]}" for k, x in pn.items() if x == sn)
                L.append(f"   {sn}{mark} {page}{' EXCL' if (sid, page) in excl else ''}{pm}: {v.numbers.get((sid, page), [])}")
    open(path, "w").write("\n".join(L))


def stage_context():
    sel = sel_cell()
    sn, bn = CNAMES[sel], CNAMES[BASE]
    det = read_tsv(OUT / "dev_detail.tsv")
    fixes = [r for r in det if r[f"leaf_{sn}"] == "1" and r[f"leaf_{bn}"] == "0"]
    breaks = [r for r in det if r[f"leaf_{sn}"] == "0" and r[f"leaf_{bn}"] == "1"]
    print(f"dev discordant: fixes {len(fixes)}, breaks {len(breaks)}")
    rng = random.Random(SEED)
    pick = [("fix", r) for r in rng.sample(fixes, min(15, len(fixes)))] + \
           [("break", r) for r in rng.sample(breaks, min(15, len(breaks)))]
    # which single option (of SEL's non-default ones) alone flips the anchor
    singles = {}
    if sel[0]:
        singles["pick"] = (True, None, None, False, 6)
    if sel[1]:
        singles["tie_" + sel[1]] = (False, sel[1], None, False, 6)
    if sel[2]:
        singles["clean"] = (False, None, sel[2], False, 6)
    if sel[3]:
        singles["pieces"] = (False, None, None, True, 6)
    if sel[4] != 6:
        singles["extrap"] = (False, None, None, False, sel[4])
    cache, items, rows = {}, [], []
    for tag, r in pick:
        inv = r["inv"]
        if inv not in cache:
            ld = load_volume(DEV_LAYER, inv, TAGS)
            v, excl = ld
            cache[inv] = (ld, {c: build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=cell_params(c))
                               for c in set([BASE, sel] + list(singles.values()))})
        (v, excl), maps = cache[inv]
        alone = []
        for nm, c in singles.items():
            pred, _ = locate_row(maps[c], v, int(r["folio"]))
            lf, _ = hit(v, r["gold_scan"], pred)
            if lf == (1 if tag == "fix" else 0) and lf != int(r[f"leaf_{bn}"]):
                alone.append(nm)
        rows.append([tag, inv, r["folio"], r["gold_scan"], r[f"pred_{bn}"], r[f"meth_{bn}"], r[f"pred_{sn}"], r[f"meth_{sn}"],
                     "+".join(alone) or "combination", "", ""])
        items.append(dict(inv=inv, folio=r["folio"], gold_scan=r["gold_scan"], tag=tag, side_layout=r["side_layout"],
                          **{f"pred_BASE": r[f"pred_{bn}"], "pred_SEL": r[f"pred_{sn}"]},
                          maps={"BASE": maps[BASE], "SEL": maps[sel]}))
    write_tsv(OUT / "misses_read.tsv", ["tag", "inv", "folio", "gold_scan", "base_pred", "base_method", "sel_pred",
                                        "sel_method", "options_alone_flip", "category", "real_fix_or_break_note"], rows)
    dump_context(OUT / "context_dev.txt", items, DEV_LAYER, TAGS, None)
    print("wrote misses_read.tsv, context_dev.txt")
    if (OUT / "test_done.json").exists() and json.load(open(OUT / "test_done.json")).get("status") == "done":
        te = read_tsv(OUT / "test_eval.tsv")
        miss = [r for r in te if r["cell"] == sn and r["leaf"] == "0"]
        random.Random(SEED).shuffle(miss)
        items = [dict(r, pred_SEL=r["pred_scan"], tag="test miss") for r in miss[:30]]
        dump_context(OUT / "context_test.txt", items, TEST_LAYER, TAGS, None)
        print("wrote context_test.txt", len(items), "of", len(miss))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["sg", "dev", "test", "context"])
    ap.add_argument("--head-dir")
    a = ap.parse_args()
    if a.stage == "sg":
        stage_sg(a.head_dir)
    else:
        {"dev": stage_dev, "test": stage_test, "context": stage_context}[a.stage]()
